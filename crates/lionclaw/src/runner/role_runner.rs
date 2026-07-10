//! `OciRoleRunner`: the production [`RoleRunner`]. One dispatch =
//! isolate the workspace, compile the plan through the moat, run one full
//! agent turn under confinement, capture the handoff (and, for writers, the
//! resulting commit). Timeouts are enforced here (`process.rs` does not);
//! `kill_on_drop` reaps the container when a timed-out future is dropped.

use std::sync::Arc;

use async_trait::async_trait;
use lionclaw_confinement::{
    inherited_skill_mounts, skill_mount_target, MountAccess, MountSpec, WORKSPACE_MOUNT_TARGET,
};
use lionclaw_runtime_acp::AcpRuntimeDriver;
use lionclaw_runtime_api::{
    RuntimeAuthProvider, RuntimeAuthRegistry, RuntimeDriverConfig, RuntimeDriverProvider,
    RuntimeProgramTurnExecution, RuntimeSessionReady, RuntimeSessionStartInput, RuntimeTurnInput,
};
use lionclaw_runtime_codex::{
    CodexRuntimeAuthProvider, CodexRuntimeDriver, CODEX_RUNTIME_AUTH_KIND,
};
use tokio::sync::Mutex;

use crate::authority::{
    compile_authority, compile_role_plan, AuthorityCeiling, MissionMounts, RolePlanRequest,
};
use crate::config::{MissionRuntimeProfile, RuntimeAuthConfig, RuntimeProfiles};
use crate::mission_type::SkillPackage;
use crate::model::{ArtifactOutcome, OutputSemantics, RunErrorKind};
use crate::ports::{RoleRunFailure, RoleRunOutcome, RoleRunRequest, RoleRunner};

use super::executor::{mission_execution_context, MissionProgramExecutor};
use super::handoff::read_handoff;
use super::native_home_auth::NativeHomeAuthProvider;
use super::{AttemptDirs, SCRATCH_MOUNT_TARGET};
use crate::workspace;

pub struct OciRoleRunner {
    profiles: RuntimeProfiles,
    image_id: String,
    ceiling: AuthorityCeiling,
    /// Serializes git worktree/clone operations inside this process; the store
    /// lease owns cross-process coordination.
    repo_lock: Arc<Mutex<()>>,
}

impl OciRoleRunner {
    pub fn new(profiles: RuntimeProfiles, image_id: String, ceiling: AuthorityCeiling) -> Self {
        Self {
            profiles,
            image_id,
            ceiling,
            repo_lock: Arc::new(Mutex::new(())),
        }
    }

    fn profile(&self, runtime: &str) -> Result<MissionRuntimeProfile, RoleRunFailure> {
        let mut profile = self
            .profiles
            .get(runtime)
            .map_err(|err| launch(err.to_string()))?;
        profile.confinement.oci_mut().image = Some(self.image_id.clone());
        Ok(profile)
    }

    fn driver(profile: &MissionRuntimeProfile) -> anyhow::Result<Box<dyn RuntimeDriverProvider>> {
        match profile.driver.as_str() {
            "codex" => Ok(Box::new(CodexRuntimeDriver)),
            "acp" => Ok(Box::new(AcpRuntimeDriver)),
            other => anyhow::bail!("unknown runtime driver '{other}'"),
        }
    }

    fn auth_registry(profile: &MissionRuntimeProfile) -> anyhow::Result<RuntimeAuthRegistry> {
        let Some(auth) = &profile.auth else {
            return Ok(RuntimeAuthRegistry::empty());
        };
        let provider: Arc<dyn RuntimeAuthProvider> = match auth {
            RuntimeAuthConfig::Provider(kind) if kind == CODEX_RUNTIME_AUTH_KIND => {
                Arc::new(CodexRuntimeAuthProvider)
            }
            RuntimeAuthConfig::Provider(kind) => {
                anyhow::bail!(
                    "runtime '{}' configures unsupported auth provider '{kind}'",
                    profile.name
                )
            }
            RuntimeAuthConfig::NativeHome(config) => {
                Arc::new(NativeHomeAuthProvider::new(config.clone()))
            }
        };
        Ok(RuntimeAuthRegistry::new([provider]))
    }

    fn driver_config(profile: &MissionRuntimeProfile) -> anyhow::Result<RuntimeDriverConfig> {
        let auth = profile
            .auth
            .as_ref()
            .map(|auth| lionclaw_runtime_api::RuntimeAuthKind::new(auth.kind()))
            .transpose()
            .map_err(anyhow::Error::msg)?;
        Ok(RuntimeDriverConfig {
            runtime_id: profile.name.clone(),
            executable: profile.command.clone(),
            args: profile.args.clone(),
            environment: profile.environment.clone(),
            model: profile.model.clone(),
            mode: profile.mode.clone(),
            auth,
            terminal: Default::default(),
        })
    }

    pub(crate) fn validate_profile(profile: &MissionRuntimeProfile) -> anyhow::Result<()> {
        let driver = Self::driver(profile)?;
        let config = Self::driver_config(profile)?;
        Self::auth_registry(profile)?;
        driver.validate_config(&config)
    }
}

fn launch(detail: String) -> RoleRunFailure {
    RoleRunFailure {
        kind: RunErrorKind::Launch,
        detail,
    }
}

fn role_skill_mounts(
    skills: &[SkillPackage],
    projection: Option<&lionclaw_confinement::RuntimeSkillProjectionConfig>,
) -> anyhow::Result<Vec<MountSpec>> {
    if !skills.is_empty() && projection.is_none() {
        anyhow::bail!("runtime profile has no skill projection for mission-assigned skills");
    }
    let mut mounts = skills
        .iter()
        .map(|skill| MountSpec {
            source: skill.root.clone(),
            target: skill_mount_target(&skill.name),
            access: MountAccess::ReadOnly,
        })
        .collect::<Vec<_>>();
    mounts.extend(inherited_skill_mounts(projection)?);
    Ok(mounts)
}

#[async_trait]
impl RoleRunner for OciRoleRunner {
    async fn run(&self, request: RoleRunRequest) -> Result<RoleRunOutcome, RoleRunFailure> {
        if let Some(declared) = &request.role.runtime {
            if declared != &request.runtime {
                return Err(launch(format!(
                    "role declares runtime '{declared}' but its request resolved '{}'",
                    request.runtime
                )));
            }
        }
        let profile = self.profile(&request.runtime)?;
        let skill_mounts = role_skill_mounts(&request.skills, profile.skill_projection.as_ref())
            .map_err(|err| launch(format!("failed to resolve role skills: {err:#}")))?;

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
            let mut extras = dirs.agent_mounts();
            extras.extend(skill_mounts);
            let environment = mission_environment(&dirs);
            let judged_roots = [crate::authority::canonical_or_lexical(&workspace_source)];
            let compiled = compile_role_plan(RolePlanRequest {
                authority: &authority,
                runtime_id: profile.name.clone(),
                confinement: profile.confinement.clone(),
                skill_projection: profile.skill_projection.clone(),
                mounts: MissionMounts {
                    workspace: workspace_mount,
                    extras,
                },
                judged_roots: &judged_roots,
                environment,
                hard_timeout: profile.hard_timeout,
            })
            .map_err(|e| launch(format!("plan refused to compile (moat): {e}")))?;

            // Run the agent turn under confinement, then capture the artifact
            // (writer only) before the workspace is torn down.
            let model_id = self
                .run_turn(&profile, &request, compiled.plan().clone())
                .await?;
            let handoff = read_handoff(&dirs.handoff, request.role.output)?;
            let artifact = if let Some(clone) = &worker_clone {
                let _guard = self.repo_lock.lock().await;
                let head_sha = workspace::capture_worker_result(&request.workspace_dir, clone)
                    .await
                    .map_err(|e| RoleRunFailure {
                        // Only an uncommitted tree is agent behavior; git infra or
                        // a moved HEAD is infrastructure.
                        kind: match e {
                            workspace::CaptureError::DirtyWorktree(_) => {
                                RunErrorKind::DirtyWorktree
                            }
                            workspace::CaptureError::Infra(_) => RunErrorKind::Infra,
                        },
                        detail: e.to_string(),
                    })?;
                Some(ArtifactOutcome {
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
        profile: &MissionRuntimeProfile,
        request: &RoleRunRequest,
        plan: lionclaw_confinement::EffectiveExecutionPlan,
    ) -> Result<Option<String>, RoleRunFailure> {
        let driver = Self::driver(profile)
            .map_err(|err| launch(format!("runtime profile invalid: {err:#}")))?;
        let config = Self::driver_config(profile)
            .map_err(|err| launch(format!("runtime profile invalid: {err:#}")))?;
        let auth_registry = Self::auth_registry(profile)
            .map_err(|err| launch(format!("runtime profile invalid: {err:#}")))?;
        driver
            .validate_config(&config)
            .map_err(|e| launch(format!("driver config invalid: {e}")))?;
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
                },
                context,
                executor: Box::new(MissionProgramExecutor::new(plan, auth_registry)),
            },
            journal_tx,
        );

        let result = tokio::time::timeout(profile.hard_timeout, turn).await;
        let _ = adapter.close(&handle).await;
        let last_error = drain.await.ok().flatten();

        match result {
            Err(_) => Err(RoleRunFailure {
                kind: RunErrorKind::Timeout,
                detail: format!("agent turn exceeded {:?}", profile.hard_timeout),
            }),
            Ok(Err(err)) => Err(RoleRunFailure {
                kind: RunErrorKind::TurnFailed,
                detail: match last_error {
                    Some(e) => format!("{err}: {e}"),
                    None => err.to_string(),
                },
            }),
            Ok(Ok(_)) => Ok(profile.model.clone()),
        }
    }
}

/// Env for a mission container: HOME/XDG under the runtime home, TMPDIR, and
/// cargo (CARGO_HOME/CARGO_TARGET_DIR) under the writable scratch mount so
/// builds stay out of the read-only rootfs. Kept minimal and mission-specific
/// rather than importing the kernel planner's env builder.
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

#[cfg(test)]
mod tests {
    use super::*;
    use lionclaw_confinement::{InheritedSkillRoot, RuntimeSkillProjectionConfig};
    use std::path::Path;

    #[test]
    fn acp_profile_mode_reaches_the_driver_config() {
        let profiles = RuntimeProfiles::from_toml(
            r#"
            [runtimes.example]
            driver = "acp"
            command = "example"
            mode = "autonomous"
            "#,
            Path::new("/home/alice"),
        )
        .expect("profiles");
        let profile = profiles.get("example").expect("profile");

        let config = OciRoleRunner::driver_config(&profile).expect("driver config");

        assert_eq!(config.mode.as_deref(), Some("autonomous"));
    }

    #[test]
    fn role_skill_mounts_combine_mission_and_inherited_packages_read_only() {
        let temp = tempfile::tempdir().unwrap();
        let inherited_root = temp.path().join("inherited");
        let inherited_skill = inherited_root.join("human-skill");
        std::fs::create_dir_all(&inherited_skill).unwrap();
        std::fs::write(inherited_skill.join("SKILL.md"), "fixture").unwrap();
        let mut projection = RuntimeSkillProjectionConfig::native_dir(".agents/skills");
        projection.inherited_roots_mut().push(InheritedSkillRoot {
            source: inherited_root,
            target: ".native/skills".to_string(),
            optional: false,
        });

        let mounts = role_skill_mounts(
            &[SkillPackage {
                name: "mission-skill".to_string(),
                root: temp.path().join("mission-skill"),
                description: "mission skill".to_string(),
            }],
            Some(&projection),
        )
        .unwrap();

        assert_eq!(mounts.len(), 2);
        assert!(mounts
            .iter()
            .all(|mount| mount.access == MountAccess::ReadOnly));
        assert!(mounts
            .iter()
            .any(|mount| mount.target == "/lionclaw/skills/mission-skill"));
        assert!(mounts
            .iter()
            .any(|mount| { mount.target == "/lionclaw/inherited-skills/0/human-skill" }));
    }

    #[test]
    fn mission_skills_require_a_runtime_projection() {
        let err = role_skill_mounts(
            &[SkillPackage {
                name: "mission-skill".to_string(),
                root: "/mission-type/skills/mission-skill".into(),
                description: "mission skill".to_string(),
            }],
            None,
        )
        .expect_err("missing projection");

        assert!(err.to_string().contains("no skill projection"));
        assert!(role_skill_mounts(&[], None).unwrap().is_empty());
    }
}
