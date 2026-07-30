//! The engine-run oracle: a worker-independent, reproducible check the
//! engine executes itself. An agent never self-reports an authoritative
//! pass — this path is the only source of `OracleRunCompleted`, and it runs
//! against a complete Git checkout of the judged commit mounted read-only.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use lionclaw_confinement::{MountAccess, MountSpec, RuntimeProgramSpec};
use lionclaw_runtime_api::{RuntimeProgramExecutor, TypedFailure, TypedFailureEvidence};
use tokio::sync::Mutex;

use crate::authority::{
    compile_role_plan, oracle_authority_with_network, MissionMounts, RolePlanRequest,
};
use crate::config::MissionRuntimeProfile;
use crate::ports::{ExecutionControl, OracleOutcome, OracleRunRequest, OracleRunner};
use crate::resources::MissionDirs;
use crate::runner::{
    await_controlled, prepare_inputs, MissionProgramExecutor, PreparedInputs, SCRATCH_MOUNT_TARGET,
};
use crate::workspace;

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

    fn profile_for(&self, environment_digest: &str) -> MissionRuntimeProfile {
        let mut profile = self.profile.clone();
        profile.confinement.oci_mut().image = Some(environment_digest.to_string());
        profile
    }
}

fn fail(detail: impl Into<String>) -> TypedFailure {
    TypedFailure::permanent("oracle.infrastructure", detail)
}

enum ControlFailureKind {
    Deadline,
    Stop,
    Abort,
}

fn control_failure(control: &ExecutionControl) -> Option<TypedFailure> {
    let (kind, code, detail, reason) = match control {
        ExecutionControl::RunUntil(_) => return None,
        ExecutionControl::DeadlineExhausted => (
            ControlFailureKind::Deadline,
            "oracle.deadline",
            "oracle exceeded its recorded effect deadline",
            "effect deadline exhausted".to_string(),
        ),
        ExecutionControl::Stop(reason) => (
            ControlFailureKind::Stop,
            "oracle.stopped",
            "oracle stopped by operator",
            reason.clone(),
        ),
        ExecutionControl::Abort(reason) => (
            ControlFailureKind::Abort,
            "oracle.aborted",
            "oracle cancelled because the mission was aborted",
            reason.clone(),
        ),
    };
    let mut evidence = TypedFailureEvidence::new(Some(code.to_string()), detail);
    evidence.stop_reason = Some(reason);
    Some(match kind {
        ControlFailureKind::Deadline => TypedFailure::DeadlineExhausted {
            evidence: Box::new(evidence),
        },
        ControlFailureKind::Stop => TypedFailure::OperatorStopped {
            evidence: Box::new(evidence),
        },
        ControlFailureKind::Abort => TypedFailure::OperatorAborted {
            evidence: Box::new(evidence),
        },
    })
}

#[async_trait]
impl OracleRunner for OciOracleRunner {
    async fn run(&self, request: OracleRunRequest) -> Result<OracleOutcome, TypedFailure> {
        if let Some(failure) = control_failure(&request.control.borrow().clone()) {
            return Err(failure);
        }
        let actual_digest = crate::model::OracleSpec::Command(request.command.clone()).digest();
        if actual_digest != request.spec_digest {
            return Err(TypedFailure::permanent(
                "oracle.spec_mismatch",
                "resolved command does not match the recorded oracle spec digest",
            ));
        }
        let dirs = MissionDirs::new(&request.state_dir, &request.mission_id)
            .effect(&request.effect_id)
            .oracle();
        let profile = self.profile_for(&request.environment_digest);
        dirs.prepare()
            .map_err(|e| fail(format!("failed to prepare oracle dirs: {e}")))?;

        // Keep every fallible stage in one result. The engine owns the one
        // cleanup path after this runner returns, including failures before a
        // run starts and recovery after this process exits.
        let checkout = dirs.work().to_path_buf();
        let result = async {
            // Complete checkout of the judged commit. The oracle receives it
            // read-only, never a worker's live checkout.
            {
                let _guard = self.repo_lock.lock().await;
                workspace::create_checkout(&request.workspace_dir, &checkout, &request.judged_sha)
                    .await
                    .map_err(|e| fail(format!("failed to create judged checkout: {e}")))?;
            }

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
                    &profile,
                    &request.state_dir,
                    dirs.root(),
                    &checkout,
                    &request.prepared_inputs,
                    &request.effect_id,
                )
                .await
                .map_err(|error| fail(format!("failed to prepare mission inputs: {error:#}")))?
            };

            let authority = oracle_authority_with_network(
                request.oracle.as_str(),
                request.command.grants.devices.clone(),
                request.command.grants.network.clone(),
            );
            let mut extras = vec![MountSpec {
                source: dirs.scratch().to_path_buf(),
                target: SCRATCH_MOUNT_TARGET.to_string(),
                access: MountAccess::ReadWrite,
            }];
            extras.extend(prepared.mounts);
            let environment =
                oracle_environment(&request.command.environment, prepared.environment);
            let working_dir = if request.command.cwd.as_str() == "." {
                checkout.clone()
            } else {
                checkout.join(request.command.cwd.as_str())
            };
            let metadata = tokio::fs::metadata(&working_dir).await.map_err(|error| {
                fail(format!(
                    "oracle working directory '{}' is unavailable: {error}",
                    request.command.cwd.as_str()
                ))
            })?;
            if !metadata.is_dir() {
                return Err(fail(format!(
                    "oracle working directory '{}' is not a directory",
                    request.command.cwd.as_str()
                )));
            }
            let judged_roots = [crate::authority::canonical_or_lexical(&checkout)];
            let compiled = compile_role_plan(RolePlanRequest {
                authority: &authority,
                runtime_id: profile.name.clone(),
                confinement: profile.confinement.clone(),
                mounts: MissionMounts {
                    workspace: checkout.clone(),
                    extras,
                },
                working_dir,
                judged_roots: &judged_roots,
                environment,
                resources: request.command.resources.clone(),
                resource_ceilings: &request.resource_ceilings,
                runtime_network: crate::model::NetworkGrant::Deny,
            })
            .map_err(|e| fail(format!("oracle plan refused to compile (moat): {e}")))?;

            let program = command_program(&request.command)?;
            let mut executor = MissionProgramExecutor::new(
                compiled.plan().clone(),
                None,
                &request.effect_id,
                None,
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
        await_controlled(Box::pin(result), request.control.clone(), control_failure).await
    }
}

/// Kernel-owned execution coordinates for an oracle. Prepared inputs are the
/// final explicit overlay because they may publish a content-addressed cache.
fn oracle_environment(
    declared: &std::collections::BTreeMap<String, String>,
    prepared_input: Vec<(String, String)>,
) -> Vec<(String, String)> {
    let mut environment = declared.clone();
    environment.extend(prepared_input);
    environment.extend([
        ("HOME".to_string(), SCRATCH_MOUNT_TARGET.to_string()),
        ("TMPDIR".to_string(), "/tmp".to_string()),
        ("GIT_OPTIONAL_LOCKS".to_string(), "0".to_string()),
    ]);
    environment.into_iter().collect()
}

fn command_program(
    command: &crate::model::CommandOracle,
) -> Result<RuntimeProgramSpec, TypedFailure> {
    let Some((executable, args)) = command.argv.split_first() else {
        return Err(fail("oracle argv is empty"));
    };
    Ok(RuntimeProgramSpec {
        executable: executable.clone(),
        args: args.to_vec(),
        environment: Vec::new(),
        stdin: String::new(),
        auth: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RuntimeProfiles;
    use crate::model::{
        AuthorityGrants, CommandOracle, EffectId, MissionId, OracleName, WorkspaceRelativeDir,
    };
    use std::collections::BTreeMap;

    #[test]
    fn oracle_prepared_input_overrides_domain_cache_location() {
        let environment = BTreeMap::from_iter(oracle_environment(
            &BTreeMap::from([
                ("BUILD_OUTPUT".to_string(), "/scratch/build".to_string()),
                ("TOOL_HOME".to_string(), "/scratch/tool".to_string()),
            ]),
            vec![("TOOL_HOME".to_string(), "/inputs/tool".to_string())],
        ));
        assert_eq!(environment["HOME"], "/scratch");
        assert_eq!(environment["BUILD_OUTPUT"], "/scratch/build");
        assert_eq!(environment["TOOL_HOME"], "/inputs/tool");
    }

    #[test]
    fn shell_syntax_is_a_literal_argument() {
        let program = command_program(&CommandOracle {
            argv: vec![
                "printf".into(),
                "$(touch /workspace/escaped)".into(),
                "a; echo b".into(),
            ],
            cwd: WorkspaceRelativeDir::new(".").unwrap(),
            environment: BTreeMap::new(),
            timeout_secs: 30,
            grants: AuthorityGrants::default(),
            resources: Default::default(),
        })
        .unwrap();

        assert_eq!(program.executable, "printf");
        assert_eq!(
            program.args,
            vec!["$(touch /workspace/escaped)", "a; echo b"]
        );
    }

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
                spec_digest: "spec".to_string(),
                command: CommandOracle {
                    argv: vec!["true".into()],
                    cwd: WorkspaceRelativeDir::new(".").unwrap(),
                    environment: BTreeMap::new(),
                    timeout_secs: 30,
                    grants: AuthorityGrants::default(),
                    resources: Default::default(),
                },
                judged_sha: "must-not-be-resolved".into(),
                environment_digest: "sha256:oracle-test".into(),
                workspace_dir: temp.path().join("must-not-be-cloned"),
                state_dir: temp.path().join("state"),
                prepared_inputs: Vec::new(),
                resource_ceilings: Default::default(),
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
