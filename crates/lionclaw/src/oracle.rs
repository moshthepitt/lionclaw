//! The engine-run oracle: a worker-independent, reproducible check the
//! engine executes itself. An agent never self-reports an authoritative
//! pass — this path is the only source of `OracleRunCompleted`, and it runs
//! against a complete Git checkout of the judged commit mounted read-only.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use lionclaw_confinement::{MountAccess, MountSpec, RuntimeProgramSpec};
use lionclaw_runtime_api::{
    RuntimeAuthRegistry, RuntimeProgramExecutor, TypedFailure, TypedFailureEvidence,
};
use tokio::sync::Mutex;

use crate::authority::{compile_role_plan, oracle_authority, MissionMounts, RolePlanRequest};
use crate::config::MissionRuntimeProfile;
use crate::ports::{ExecutionControl, OracleOutcome, OracleRunRequest, OracleRunner};
use crate::runner::{
    await_controlled, prepare_inputs, EffectDirs, MissionProgramExecutor, PreparedInputs,
    SCRATCH_MOUNT_TARGET,
};
use crate::workspace;

const ORACLE_MOUNT_TARGET: &str = "/mission/oracle";

pub struct OciOracleRunner {
    profile: MissionRuntimeProfile,
    repo_lock: Arc<Mutex<()>>,
    input_lock: Arc<Mutex<()>>,
}

impl OciOracleRunner {
    pub fn new(profile: MissionRuntimeProfile) -> Self {
        Self {
            profile,
            repo_lock: Arc::new(Mutex::new(())),
            input_lock: Arc::new(Mutex::new(())),
        }
    }
}

fn fail(detail: impl Into<String>) -> TypedFailure {
    TypedFailure::permanent("oracle.infrastructure", detail)
}

#[async_trait]
impl OracleRunner for OciOracleRunner {
    async fn run(&self, request: OracleRunRequest) -> Result<OracleOutcome, TypedFailure> {
        let dirs = EffectDirs::prepare(
            &request.state_dir,
            request.mission_id.as_str(),
            &request.effect_id,
        )
        .map_err(|e| fail(format!("failed to prepare oracle dirs: {e}")))?;

        // Keep every fallible stage in one result. The engine owns the one
        // cleanup path after this runner returns, including failures before a
        // run starts and recovery after this process exits.
        let checkout = dirs.root.join("work");
        let result = async {
            // Complete checkout of the judged commit. The oracle receives it
            // read-only, never a worker's live checkout.
            {
                let _guard = self.repo_lock.lock().await;
                workspace::create_checkout(&request.workspace_dir, &checkout, &request.judged_sha)
                    .await
                    .map_err(|e| fail(format!("failed to create judged checkout: {e}")))?;
            }

            // Copy the oracle executable into its own read-only mount.
            let oracle_dir = dirs.root.join("oracle");
            tokio::fs::create_dir_all(&oracle_dir)
                .await
                .map_err(|e| fail(e.to_string()))?;
            let oracle_dest = oracle_dir.join(request.oracle.as_str());
            tokio::fs::copy(&request.oracle_path, &oracle_dest)
                .await
                .map_err(|e| fail(format!("failed to stage oracle executable: {e}")))?;
            workspace::make_executable(&oracle_dest).map_err(|e| fail(e.to_string()))?;

            // Preparation may use its declaration's explicit network policy,
            // but only publishes an immutable cache directory. The oracle
            // below remains on its separate network-off authority and sees
            // those directories read-only.
            let prepared = if request.prepared_inputs.is_empty() {
                PreparedInputs {
                    mounts: Vec::new(),
                    environment: Vec::new(),
                    refs: Vec::new(),
                }
            } else {
                let _guard = self.input_lock.lock().await;
                prepare_inputs(
                    &self.profile,
                    &request.state_dir,
                    &dirs.root,
                    &checkout,
                    &request.prepared_inputs,
                    &request.effect_id,
                )
                .await
                .map_err(|error| fail(format!("failed to prepare mission inputs: {error:#}")))?
            };

            let authority = oracle_authority(request.oracle.as_str());
            let mut extras = vec![
                MountSpec {
                    source: oracle_dir.clone(),
                    target: ORACLE_MOUNT_TARGET.to_string(),
                    access: MountAccess::ReadOnly,
                },
                MountSpec {
                    source: dirs.read_scratch.clone(),
                    target: SCRATCH_MOUNT_TARGET.to_string(),
                    access: MountAccess::ReadWrite,
                },
            ];
            extras.extend(prepared.mounts);
            let mut environment = oracle_environment();
            environment.extend(prepared.environment);
            let judged_roots = [crate::authority::canonical_or_lexical(&checkout)];
            let compiled = compile_role_plan(RolePlanRequest {
                authority: &authority,
                runtime_id: self.profile.name.clone(),
                confinement: self.profile.confinement.clone(),
                mounts: MissionMounts {
                    workspace: checkout.clone(),
                    extras,
                },
                judged_roots: &judged_roots,
                environment,
            })
            .map_err(|e| fail(format!("oracle plan refused to compile (moat): {e}")))?;

            let program = RuntimeProgramSpec {
                executable: format!("{ORACLE_MOUNT_TARGET}/{}", request.oracle),
                args: Vec::new(),
                environment: Vec::new(),
                stdin: String::new(),
                auth: None,
            };
            let mut executor = MissionProgramExecutor::new(
                compiled.plan().clone(),
                RuntimeAuthRegistry::empty(),
                &request.effect_id,
            );
            // Wall-clock duration is recorded evidence, not fold state; measuring
            // it here is a runner concern that never threatens fold purity.
            #[expect(clippy::disallowed_methods)]
            let started = Instant::now();
            let run = executor
                .execute_captured(program)
                .await
                .map_err(|error| fail(format!("oracle failed to run: {error}")));
            let duration_ms = started.elapsed().as_millis() as u64;
            match run {
                Err(failure) => Err(failure),
                Ok(output) => Ok(OracleOutcome {
                    exit_code: output.exit_code.unwrap_or(-1),
                    exit_signal: output.exit_signal,
                    stdout: output.stdout,
                    stderr: output.stderr,
                    prepared_inputs: prepared.refs,
                    duration_ms,
                }),
            }
        };
        await_controlled(result, request.control.clone(), |control| match control {
            ExecutionControl::RunUntil(_) => None,
            ExecutionControl::DeadlineExhausted => {
                let mut evidence = TypedFailureEvidence::new(
                    Some("oracle.deadline".to_string()),
                    "oracle exceeded its recorded effect deadline",
                );
                evidence.stop_reason = Some("effect deadline exhausted".into());
                Some(TypedFailure::DeadlineExhausted {
                    evidence: Box::new(evidence),
                })
            }
            ExecutionControl::Stop(reason) => {
                let mut evidence = TypedFailureEvidence::new(
                    Some("oracle.stopped".into()),
                    "oracle stopped by operator",
                );
                evidence.stop_reason = Some(reason.clone());
                Some(TypedFailure::OperatorStopped {
                    evidence: Box::new(evidence),
                })
            }
            ExecutionControl::Abort(reason) => {
                let mut evidence = TypedFailureEvidence::new(
                    Some("oracle.aborted".into()),
                    "oracle cancelled because the mission was aborted",
                );
                evidence.stop_reason = Some(reason.clone());
                Some(TypedFailure::OperatorAborted {
                    evidence: Box::new(evidence),
                })
            }
        })
        .await
    }
}

/// Oracle env: cargo/target under the writable scratch mount; nothing agent-
/// or auth-related (oracles have no agent and no network).
fn oracle_environment() -> Vec<(String, String)> {
    vec![
        ("HOME".to_string(), SCRATCH_MOUNT_TARGET.to_string()),
        ("TMPDIR".to_string(), "/tmp".to_string()),
        ("GIT_OPTIONAL_LOCKS".to_string(), "0".to_string()),
        (
            "CARGO_HOME".to_string(),
            format!("{SCRATCH_MOUNT_TARGET}/cargo"),
        ),
        (
            "CARGO_TARGET_DIR".to_string(),
            format!("{SCRATCH_MOUNT_TARGET}/target"),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RuntimeProfiles;
    use crate::model::{EffectId, MissionId, OracleName};

    #[tokio::test]
    async fn production_oracle_observes_stop_before_checkout_or_runtime_launch() {
        let temp = tempfile::tempdir().unwrap();
        let profile = RuntimeProfiles::from_toml(
            "[runtimes.test]\ndriver = \"acp\"\ncommand = \"never-launched\"\n",
            temp.path(),
        )
        .unwrap()
        .get("test")
        .unwrap();
        let runner = OciOracleRunner::new(profile);
        let (_control_tx, control) =
            tokio::sync::watch::channel(ExecutionControl::Stop("operator stop".into()));
        let result = runner
            .run(OracleRunRequest {
                mission_id: MissionId::parse("m123456789abc").unwrap(),
                effect_id: EffectId::for_parts(&["oracle", "pre-start-stop"]),
                oracle: OracleName::new("checks").unwrap(),
                oracle_path: temp.path().join("must-not-be-read"),
                judged_sha: "must-not-be-resolved".into(),
                workspace_dir: temp.path().join("must-not-be-cloned"),
                state_dir: temp.path().join("state"),
                prepared_inputs: Vec::new(),
                deadline_ms: 10,
                control,
            })
            .await;

        let failure = result.expect_err("pre-start stop wins before setup");
        assert!(matches!(failure, TypedFailure::OperatorStopped { .. }));
        assert_eq!(
            failure.evidence().stop_reason.as_deref(),
            Some("operator stop")
        );
    }
}
