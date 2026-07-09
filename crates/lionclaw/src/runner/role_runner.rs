//! `OciRoleRunner`: the production [`RoleRunner`]. One dispatch =
//! isolate the workspace, compile the plan through the moat, run one full
//! agent turn under confinement, capture the handoff (and, for writers, the
//! resulting commit). Timeouts are enforced here (`process.rs` does not);
//! `kill_on_drop` reaps the container when a timed-out future is dropped.

use std::sync::Arc;

use async_trait::async_trait;
use lionclaw_confinement::{MountAccess, MountSpec, WORKSPACE_MOUNT_TARGET};
use lionclaw_runtime_acp::AcpRuntimeDriver;
use lionclaw_runtime_api::{
    RuntimeAuthRegistry, RuntimeDriverConfig, RuntimeDriverProvider, RuntimeProgramTurnExecution,
    RuntimeSessionReady, RuntimeSessionStartInput, RuntimeTurnInput,
};
use lionclaw_runtime_codex::CodexRuntimeDriver;
use tokio::sync::Mutex;

use crate::authority::{
    compile_authority, compile_role_plan, AuthorityCeiling, MissionMounts, RolePlanRequest,
};
use crate::config::MissionRuntimeProfile;
use crate::model::{ArtifactOutcome, OutputSemantics, RunErrorKind};
use crate::ports::{RoleRunFailure, RoleRunOutcome, RoleRunRequest, RoleRunner};

use super::executor::{mission_execution_context, MissionProgramExecutor};
use super::handoff::read_handoff;
use super::{AttemptDirs, SCRATCH_MOUNT_TARGET};
use crate::workspace;

pub struct OciRoleRunner {
    profile: MissionRuntimeProfile,
    ceiling: AuthorityCeiling,
    /// Serializes git worktree/clone operations per process (one target repo
    /// per mission in the walking skeleton).
    repo_lock: Arc<Mutex<()>>,
}

impl OciRoleRunner {
    pub fn new(profile: MissionRuntimeProfile, ceiling: AuthorityCeiling) -> Self {
        Self {
            profile,
            ceiling,
            repo_lock: Arc::new(Mutex::new(())),
        }
    }

    fn driver(&self) -> Result<Box<dyn RuntimeDriverProvider>, RoleRunFailure> {
        match self.profile.driver.as_str() {
            "codex" => Ok(Box::new(CodexRuntimeDriver)),
            "acp" => Ok(Box::new(AcpRuntimeDriver)),
            other => Err(launch(format!("unknown runtime driver '{other}'"))),
        }
    }
}

fn launch(detail: String) -> RoleRunFailure {
    RoleRunFailure {
        kind: RunErrorKind::Launch,
        detail,
    }
}

#[async_trait]
impl RoleRunner for OciRoleRunner {
    async fn run(&self, request: RoleRunRequest) -> Result<RoleRunOutcome, RoleRunFailure> {
        // A per-role `runtime` override must name the mission's runtime — the
        // engine wires a single profile, so a mismatch is a fail-closed error
        // rather than a silently-ignored knob.
        if let Some(requested) = &request.role.runtime {
            if requested != &self.profile.name {
                return Err(launch(format!(
                    "role requests runtime '{requested}' but this mission runs '{}'",
                    self.profile.name
                )));
            }
        }

        let attempt_tag = format!("{}-a{}", request.task_id, request.attempt_no);
        let dirs = AttemptDirs::prepare(
            &request.state_dir,
            request.mission_id.as_str(),
            &attempt_tag,
        )
        .map_err(|e| launch(format!("failed to prepare attempt dirs: {e}")))?;

        let is_writer = request.role.output == OutputSemantics::ProducesArtifact;

        // Everything after the attempt dirs exist runs inside one block whose
        // Result is captured, so the teardown below reaps the whole attempt
        // directory on EVERY exit path — a failure in workspace isolation or
        // moat compilation, not only after the turn has run.
        let result: Result<RoleRunOutcome, RoleRunFailure> = async {
            // Isolate the workspace. Writers get a clone (committable), everyone
            // else a read-only snapshot of the base commit.
            let (workspace_source, workspace_access, worker_clone) = {
                let _guard = self.repo_lock.lock().await;
                if is_writer {
                    let clone = workspace::create_worker_clone(
                        &request.workspace_dir,
                        &dirs.root.join("work"),
                        request.mission_id.as_str(),
                        &attempt_tag,
                        &request.base_sha,
                    )
                    .await
                    .map_err(|e| launch(format!("failed to create worker clone: {e}")))?;
                    (clone.dir.clone(), MountAccess::ReadWrite, Some(clone))
                } else {
                    let snapshot = dirs.root.join("snapshot");
                    workspace::create_snapshot(
                        &request.workspace_dir,
                        &snapshot,
                        &request.base_sha,
                    )
                    .await
                    .map_err(|e| launch(format!("failed to snapshot workspace: {e}")))?;
                    (snapshot, MountAccess::ReadOnly, None)
                }
            };

            // Compile the plan through the moat. Judged roots = the workspace
            // the verdict is about (only meaningful for verdict roles, but the
            // predicate is applied uniformly).
            let authority = compile_authority(&request.role, &self.ceiling)
                .map_err(|e| launch(format!("authority refused to compile: {e}")))?;
            let workspace_mount = MountSpec {
                source: workspace_source.clone(),
                target: WORKSPACE_MOUNT_TARGET.to_string(),
                access: workspace_access,
            };
            let extras = dirs.agent_mounts();
            let environment = mission_environment(&dirs);
            let judged_roots = [crate::authority::canonical_or_lexical(&workspace_source)];
            let compiled = compile_role_plan(RolePlanRequest {
                authority: &authority,
                runtime_id: self.profile.name.clone(),
                confinement: self.profile.confinement.clone(),
                mounts: MissionMounts {
                    workspace: workspace_mount,
                    extras,
                },
                judged_roots: &judged_roots,
                environment,
                idle_timeout: self.profile.idle_timeout,
                hard_timeout: self.profile.hard_timeout,
            })
            .map_err(|e| launch(format!("plan refused to compile (moat): {e}")))?;

            // Run the agent turn under confinement, then capture the artifact
            // (writer only) before the workspace is torn down.
            let model_id = self.run_turn(&request, compiled.plan().clone()).await?;
            let handoff = read_handoff(&dirs.handoff, request.role.output)?;
            let artifact = if let Some(clone) = &worker_clone {
                let _guard = self.repo_lock.lock().await;
                workspace::capture_worker_result(&request.workspace_dir, clone)
                    .await
                    .map_err(|e| RoleRunFailure {
                        kind: RunErrorKind::DirtyWorktree,
                        detail: e.to_string(),
                    })?
                    .map(|head_sha| ArtifactOutcome {
                        base_sha: request.base_sha.clone(),
                        head_sha,
                    })
            } else {
                None
            };
            Ok(RoleRunOutcome {
                handoff,
                artifact,
                model_id,
            })
        }
        .await;

        // Unconditional teardown of the whole attempt directory (workspace
        // clone/snapshot, handoff, scratch=CARGO_TARGET_DIR, runtime homes) on
        // every exit path: the handoff is already read into `result` and the
        // worker's commit already survives in the target repo's mission ref, so
        // nothing here is load-bearing once the run has settled.
        workspace::remove_dir(&dirs.root).await;
        result
    }
}

impl OciRoleRunner {
    async fn run_turn(
        &self,
        request: &RoleRunRequest,
        plan: lionclaw_confinement::EffectiveExecutionPlan,
    ) -> Result<Option<String>, RoleRunFailure> {
        let driver = self.driver()?;
        // The auth kind a driver requires is fixed per driver; codex is the
        // only one that needs host auth synced in.
        let auth_kind = match driver.auth_provider() {
            Some(provider) => Some(
                lionclaw_runtime_api::RuntimeAuthKind::new(provider.kind())
                    .map_err(|e| launch(format!("invalid auth kind: {e}")))?,
            ),
            None => None,
        };
        let config = RuntimeDriverConfig {
            runtime_id: self.profile.name.clone(),
            executable: self.profile.command.clone(),
            args: self.profile.args.clone(),
            environment: self.profile.environment.clone(),
            model: self.profile.model.clone(),
            mode: None,
            auth: auth_kind.clone(),
            terminal: Default::default(),
        };
        driver
            .validate_config(&config)
            .map_err(|e| launch(format!("driver config invalid: {e}")))?;
        let auth_registry =
            RuntimeAuthRegistry::new(driver.auth_provider().into_iter().collect::<Vec<_>>());
        let adapter = driver.create_adapter(config);

        // Compute the fallible execution context *before* opening a session,
        // so a failure here cannot leak a started session.
        let context = mission_execution_context(&plan)
            .map_err(|e| launch(format!("execution context failed: {e}")))?;

        let state_root = plan
            .mounts
            .iter()
            .find(|m| m.target == lionclaw_confinement::RUNTIME_MOUNT_TARGET)
            .map(|m| m.source.clone());
        let handle = adapter
            .session_start(RuntimeSessionStartInput {
                session_id: uuid_from_key(&request.idempotency_key),
                working_dir: Some(WORKSPACE_MOUNT_TARGET.to_string()),
                environment: plan.environment.clone(),
                runtime_skill_ids: Vec::new(),
                runtime_state_root: state_root,
                runtime_session_ready: RuntimeSessionReady::not_ready(),
            })
            .await
            .map_err(|e| launch(format!("session_start failed: {e}")))?;

        let (journal_tx, mut journal_rx) =
            tokio::sync::mpsc::unbounded_channel::<lionclaw_runtime_api::TurnEvent>();
        let drain = tokio::spawn(async move {
            let mut last_error = None;
            while let Some(event) = journal_rx.recv().await {
                if let lionclaw_runtime_api::RuntimeEvent::Error { text, .. } = &event.event {
                    last_error = Some(text.clone());
                }
            }
            last_error
        });

        let turn = adapter.program_backed_turn(
            RuntimeProgramTurnExecution {
                input: RuntimeTurnInput {
                    runtime_session_id: handle.runtime_session_id.clone(),
                    prompt: request.prompt.clone(),
                    fresh_prompt: None,
                    runtime_skill_ids: Vec::new(),
                },
                context,
                executor: Box::new(MissionProgramExecutor::new(plan, auth_registry)),
            },
            journal_tx,
        );

        let result = tokio::time::timeout(self.profile.hard_timeout, turn).await;
        let _ = adapter.close(&handle).await;
        let last_error = drain.await.ok().flatten();

        match result {
            Err(_) => Err(RoleRunFailure {
                kind: RunErrorKind::Timeout,
                detail: format!("agent turn exceeded {:?}", self.profile.hard_timeout),
            }),
            Ok(Err(err)) => Err(RoleRunFailure {
                kind: RunErrorKind::TurnFailed,
                detail: match last_error {
                    Some(e) => format!("{err}: {e}"),
                    None => err.to_string(),
                },
            }),
            Ok(Ok(_)) => Ok(self.profile.model.clone()),
        }
    }
}

/// Env for a mission container: HOME/XDG under the runtime home, TMPDIR, and
/// (for writers) cargo/npm under scratch so installs stay in the writable
/// area. Kept minimal and mission-specific rather than importing the kernel
/// planner's env builder.
fn mission_environment(dirs: &AttemptDirs) -> Vec<(String, String)> {
    let home = lionclaw_confinement::RUNTIME_HOME_MOUNT_TARGET;
    vec![
        ("HOME".to_string(), home.to_string()),
        ("XDG_CONFIG_HOME".to_string(), format!("{home}/.config")),
        ("XDG_CACHE_HOME".to_string(), format!("{home}/.cache")),
        ("XDG_DATA_HOME".to_string(), format!("{home}/.local/share")),
        ("XDG_STATE_HOME".to_string(), format!("{home}/.local/state")),
        ("TMPDIR".to_string(), "/tmp".to_string()),
        (
            "CARGO_HOME".to_string(),
            format!("{SCRATCH_MOUNT_TARGET}/cargo"),
        ),
        (
            "CARGO_TARGET_DIR".to_string(),
            format!("{SCRATCH_MOUNT_TARGET}/target"),
        ),
        (
            "LIONCLAW_WORKSPACE_DIR".to_string(),
            WORKSPACE_MOUNT_TARGET.to_string(),
        ),
    ]
    .into_iter()
    .chain(std::iter::once((
        "MISSION_ATTEMPT".to_string(),
        dirs.root.to_string_lossy().into_owned(),
    )))
    .collect()
}

/// Deterministic session UUID derived from the idempotency key (no RNG).
fn uuid_from_key(key: &str) -> uuid::Uuid {
    let digest = <sha2::Sha256 as sha2::Digest>::digest(key.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    uuid::Uuid::from_bytes(bytes)
}
