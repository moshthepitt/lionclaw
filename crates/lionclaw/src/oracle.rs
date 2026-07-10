//! The engine-run oracle: a worker-independent, reproducible check the
//! engine executes itself. An agent never self-reports an authoritative
//! pass — this path is the only source of `OracleRunCompleted`, and it runs
//! against a read-only snapshot of the judged commit.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use lionclaw_confinement::{MountAccess, MountSpec, RuntimeProgramSpec, WORKSPACE_MOUNT_TARGET};
use lionclaw_runtime_api::{RuntimeAuthRegistry, RuntimeProgramExecutor};
use tokio::sync::Mutex;

use crate::authority::{compile_role_plan, oracle_authority, MissionMounts, RolePlanRequest};
use crate::config::MissionRuntimeProfile;
use crate::ports::{OracleFailure, OracleOutcome, OracleRunRequest, OracleRunner};
use crate::runner::{AttemptDirs, MissionProgramExecutor, SCRATCH_MOUNT_TARGET};
use crate::workspace;

const ORACLE_MOUNT_TARGET: &str = "/mission/oracle";

pub struct OciOracleRunner {
    profile: MissionRuntimeProfile,
    repo_lock: Arc<Mutex<()>>,
}

impl OciOracleRunner {
    pub fn new(profile: MissionRuntimeProfile) -> Self {
        Self {
            profile,
            repo_lock: Arc::new(Mutex::new(())),
        }
    }
}

fn fail(detail: impl Into<String>) -> OracleFailure {
    OracleFailure {
        detail: detail.into(),
    }
}

#[async_trait]
impl OracleRunner for OciOracleRunner {
    async fn run(&self, request: OracleRunRequest) -> Result<OracleOutcome, OracleFailure> {
        let attempt_tag = format!(
            "oracle-{}-{}",
            request.oracle,
            &request.judged_sha[..12.min(request.judged_sha.len())]
        );
        let dirs = AttemptDirs::prepare(
            &request.state_dir,
            request.mission_id.as_str(),
            &attempt_tag,
        )
        .map_err(|e| fail(format!("failed to prepare oracle dirs: {e}")))?;

        // Everything after the attempt dirs exist runs inside one block so the
        // whole attempt directory is reclaimed on every exit path — the snapshot
        // (a full checkout), the moat-compile, and the staging steps can all fail
        // before the run.
        let snapshot = dirs.root.join("snapshot");
        let result = async {
            // Read-only snapshot of the judged commit — never the worker's live
            // clone, so a worker cannot influence the verdict it is judged by.
            {
                let _guard = self.repo_lock.lock().await;
                workspace::create_snapshot(&request.workspace_dir, &snapshot, &request.judged_sha)
                    .await
                    .map_err(|e| fail(format!("failed to snapshot judged commit: {e}")))?;
            }

            // Copy the oracle executable into its own read-only mount.
            let oracle_dir = dirs.root.join("oracle");
            std::fs::create_dir_all(&oracle_dir).map_err(|e| fail(e.to_string()))?;
            let oracle_dest = oracle_dir.join(request.oracle.as_str());
            std::fs::copy(&request.oracle_path, &oracle_dest)
                .map_err(|e| fail(format!("failed to stage oracle executable: {e}")))?;
            workspace::make_executable(&oracle_dest).map_err(|e| fail(e.to_string()))?;

            let authority = oracle_authority(request.oracle.as_str());
            let workspace_mount = MountSpec {
                source: snapshot.clone(),
                target: WORKSPACE_MOUNT_TARGET.to_string(),
                access: MountAccess::ReadOnly,
            };
            let extras = vec![
                MountSpec {
                    source: oracle_dir.clone(),
                    target: ORACLE_MOUNT_TARGET.to_string(),
                    access: MountAccess::ReadOnly,
                },
                MountSpec {
                    source: dirs.scratch.clone(),
                    target: SCRATCH_MOUNT_TARGET.to_string(),
                    access: MountAccess::ReadWrite,
                },
            ];
            let judged_roots = [crate::authority::canonical_or_lexical(&snapshot)];
            let compiled = compile_role_plan(RolePlanRequest {
                authority: &authority,
                runtime_id: self.profile.name.clone(),
                confinement: self.profile.confinement.clone(),
                skill_projection: None,
                mounts: MissionMounts {
                    workspace: workspace_mount,
                    extras,
                },
                judged_roots: &judged_roots,
                environment: oracle_environment(),
                hard_timeout: self.profile.oracle_timeout,
            })
            .map_err(|e| fail(format!("oracle plan refused to compile (moat): {e}")))?;

            let program = RuntimeProgramSpec {
                executable: format!("{ORACLE_MOUNT_TARGET}/{}", request.oracle),
                args: Vec::new(),
                environment: Vec::new(),
                stdin: String::new(),
                auth: None,
            };
            let mut executor =
                MissionProgramExecutor::new(compiled.plan().clone(), RuntimeAuthRegistry::empty());
            // Wall-clock duration is recorded evidence, not fold state; measuring
            // it here is a runner concern that never threatens fold purity.
            #[expect(clippy::disallowed_methods)]
            let started = Instant::now();
            let run = tokio::time::timeout(
                self.profile.oracle_timeout,
                executor.execute_captured(program),
            )
            .await;
            let duration_ms = started.elapsed().as_millis() as u64;
            match run {
                Err(_) => Err(fail(format!(
                    "oracle exceeded {:?}",
                    self.profile.oracle_timeout
                ))),
                Ok(Err(err)) => Err(fail(format!("oracle failed to run: {err}"))),
                Ok(Ok(output)) => Ok(OracleOutcome {
                    exit_code: output.exit_code.unwrap_or(-1),
                    exit_signal: output.exit_signal,
                    stdout: output.stdout,
                    stderr: output.stderr,
                    duration_ms,
                }),
            }
        }
        .await;

        // Reap the whole attempt directory (snapshot checkout, staged oracle,
        // scratch target dir) on every exit path; the outcome is already in
        // `result` and the verdict is minted from it in the fold.
        workspace::remove_dir(&dirs.root).await;
        result
    }
}

/// Oracle env: cargo/target under the writable scratch mount; nothing agent-
/// or auth-related (oracles have no agent and no network).
fn oracle_environment() -> Vec<(String, String)> {
    vec![
        ("HOME".to_string(), SCRATCH_MOUNT_TARGET.to_string()),
        ("TMPDIR".to_string(), "/tmp".to_string()),
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
