//! `OciRoleRunner`: the production [`RoleRunner`]. One dispatch =
//! isolate the workspace, compile the plan through the moat, run one full
//! agent turn under confinement, capture the handoff (and, for writers, the
//! resulting commit). Timeouts are enforced here (`process.rs` does not);
//! `kill_on_drop` reaps the container when a timed-out future is dropped.

use std::sync::Arc;

use async_trait::async_trait;
use lionclaw_confinement::WORKSPACE_MOUNT_TARGET;
use lionclaw_runtime_acp::AcpRuntimeDriver;
use lionclaw_runtime_api::{
    RuntimeAuthProvider, RuntimeAuthRegistry, RuntimeDriverConfig, RuntimeDriverProvider,
    RuntimeProgramTurnExecution, RuntimeSessionReady, RuntimeSessionStartInput, RuntimeTurnInput,
    TypedFailure, TypedFailureEvidence,
};
use lionclaw_runtime_codex::{
    CodexRuntimeAuthProvider, CodexRuntimeDriver, CODEX_RUNTIME_AUTH_KIND,
};
use tokio::sync::Mutex;

use crate::authority::{
    compile_authority, compile_role_plan, AuthorityCeiling, MissionMounts, RolePlanRequest,
};
use crate::config::{MissionRuntimeProfile, RuntimeAuthConfig, RuntimeProfiles};
use crate::model::{ArtifactOutcome, OutputSemantics};
use crate::ports::{ExecutionControl, RoleRunOutcome, RoleRunRequest, RoleRunner};

use super::executor::{mission_execution_context, MissionProgramExecutor};
use super::handoff::read_handoff;
use super::native_home_auth::NativeHomeAuthProvider;
use super::{prepare_skill_mounts, EffectDirs, TaskDirs, SCRATCH_MOUNT_TARGET};
use crate::workspace;

pub struct OciRoleRunner {
    profiles: RuntimeProfiles,
    image_id: String,
    ceiling: AuthorityCeiling,
    /// Serializes Git checkout/capture operations inside this process. The
    /// mission driver lock provides cross-process coordination.
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

    fn profile(&self, runtime: &str) -> Result<MissionRuntimeProfile, TypedFailure> {
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

fn launch(detail: String) -> TypedFailure {
    TypedFailure::permanent("kernel.launch", detail)
}

async fn prepare_writer_checkout(
    repo: &std::path::Path,
    workspace: &std::path::Path,
    base_sha: &str,
    recreate_workspace: bool,
) -> Result<(), TypedFailure> {
    if workspace.exists() && !recreate_workspace {
        return Ok(());
    }
    if workspace.exists()
        && workspace::is_dirty(workspace)
            .await
            .map_err(|e| launch(format!("failed to inspect retained checkout: {e}")))?
    {
        return Err(launch(
            "refusing to recreate a dirty task workspace on a moved base".to_string(),
        ));
    }
    workspace::create_checkout(repo, workspace, base_sha)
        .await
        .map_err(|e| launch(format!("failed to create checkout: {e}")))
}

#[async_trait]
impl RoleRunner for OciRoleRunner {
    async fn run(&self, request: RoleRunRequest) -> Result<RoleRunOutcome, TypedFailure> {
        if let Some(declared) = &request.role.runtime {
            if declared != &request.runtime {
                return Err(launch(format!(
                    "role declares runtime '{declared}' but its request resolved '{}'",
                    request.runtime
                )));
            }
        }
        let profile = self.profile(&request.runtime)?;

        let dirs = EffectDirs::prepare(
            &request.state_dir,
            request.mission_id.as_str(),
            &request.effect_id,
        )
        .map_err(|e| launch(format!("failed to prepare attempt dirs: {e}")))?;

        // Keep every fallible stage in one result. The engine owns the one
        // cleanup path after this runner returns, including failures before a
        // turn starts and recovery after this process exits.
        let result: Result<RoleRunOutcome, TypedFailure> = async {
            let skill_mounts = prepare_skill_mounts(
                &dirs.runtime_home,
                &request.skills,
                profile.skills_dir.as_ref(),
            )
            .map_err(|err| launch(format!("failed to prepare role skills: {err:#}")))?;
            let authority = compile_authority(&request.role, &self.ceiling)
                .map_err(|e| launch(format!("authority refused to compile: {e}")))?;
            let is_writer = authority.output() == OutputSemantics::ProducesArtifact;
            let (workspace_source, scratch_source) = if is_writer {
                let task_dirs = TaskDirs::prepare(
                    &request.state_dir,
                    request.mission_id.as_str(),
                    &request.task_id,
                )
                .map_err(|e| launch(format!("failed to prepare task dirs: {e}")))?;
                (task_dirs.work.clone(), task_dirs.scratch.clone())
            } else {
                (dirs.root.join("work"), dirs.read_scratch.clone())
            };
            {
                let _guard = self.repo_lock.lock().await;
                if is_writer {
                    prepare_writer_checkout(
                        &request.workspace_dir,
                        &workspace_source,
                        &request.base_sha,
                        request.recreate_workspace,
                    )
                    .await?;
                } else {
                    workspace::create_checkout(
                        &request.workspace_dir,
                        &workspace_source,
                        &request.base_sha,
                    )
                    .await
                    .map_err(|e| launch(format!("failed to create checkout: {e}")))?;
                }
            }
            // Compile the plan through the moat. Judged roots = the workspace
            // the verdict is about (only meaningful for verdict roles, but the
            // predicate is applied uniformly).
            let mut extras = dirs.effect_mounts(&scratch_source);
            extras.extend(skill_mounts);
            let environment = mission_environment(&dirs);
            let judged_roots = [crate::authority::canonical_or_lexical(&workspace_source)];
            let compiled = compile_role_plan(RolePlanRequest {
                authority: &authority,
                runtime_id: profile.name.clone(),
                confinement: profile.confinement.clone(),
                mounts: MissionMounts {
                    workspace: workspace_source.clone(),
                    extras,
                },
                judged_roots: &judged_roots,
                environment,
            })
            .map_err(|e| launch(format!("plan refused to compile (moat): {e}")))?;

            // Run the agent turn under confinement, then capture the artifact
            // (writer only) before the workspace is torn down.
            let (applied, final_response) = self
                .run_turn(&profile, &request, compiled.plan().clone())
                .await?;
            let handoff =
                read_handoff(&dirs.handoff, request.role.output).map_err(|mut failure| {
                    failure.evidence_mut().final_response = final_response.clone();
                    failure.evidence_mut().configuration = applied.clone();
                    failure
                })?;
            let artifact = if is_writer {
                let _guard = self.repo_lock.lock().await;
                let head_sha = workspace::capture_worker_result(
                    &request.workspace_dir,
                    &workspace_source,
                    request.mission_id.as_str(),
                    &request.effect_id,
                )
                .await
                .map_err(|e| {
                    let mut failure = match e {
                        workspace::CaptureError::DirtyWorktree(_) => {
                            TypedFailure::invalid("workspace.dirty", e.to_string())
                        }
                        workspace::CaptureError::Infra(_) => {
                            TypedFailure::permanent("workspace.capture", e.to_string())
                        }
                    };
                    failure.evidence_mut().final_response = final_response.clone();
                    failure.evidence_mut().configuration = applied.clone();
                    failure
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
                runtime_configuration: crate::model::RuntimeConfigurationEvidence {
                    requested_model: applied.requested_model,
                    applied_model: applied.applied_model,
                    requested_mode: applied.requested_mode,
                    applied_mode: applied.applied_mode,
                },
                final_response,
            })
        }
        .await;

        result
    }
}

impl OciRoleRunner {
    async fn run_turn(
        &self,
        profile: &MissionRuntimeProfile,
        request: &RoleRunRequest,
        plan: lionclaw_confinement::EffectiveExecutionPlan,
    ) -> Result<(lionclaw_runtime_api::AppliedRuntimeConfiguration, String), TypedFailure> {
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
                session_id: uuid_from_key(request.effect_id.as_str()),
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
            let mut final_response = String::new();
            while let Some(event) = journal_rx.recv().await {
                match &event.event {
                    lionclaw_runtime_api::RuntimeEvent::Error { text, .. } => {
                        last_error = Some(text.clone());
                    }
                    lionclaw_runtime_api::RuntimeEvent::MessageDelta {
                        lane: lionclaw_runtime_api::RuntimeMessageLane::Answer,
                        text,
                    } => {
                        lionclaw_runtime_api::append_streamed_text_delta(&mut final_response, text)
                    }
                    lionclaw_runtime_api::RuntimeEvent::MessageBoundary {
                        lane: lionclaw_runtime_api::RuntimeMessageLane::Answer,
                    } => lionclaw_runtime_api::append_streamed_text_boundary(&mut final_response),
                    _ => {}
                }
            }
            (last_error, final_response.trim_end().to_string())
        });

        let mut turn = Box::pin(adapter.program_backed_turn(
            RuntimeProgramTurnExecution {
                input: RuntimeTurnInput {
                    runtime_session_id: handle.runtime_session_id.clone(),
                    prompt: request.prompt.clone(),
                    fresh_prompt: None,
                },
                context,
                executor: Box::new(MissionProgramExecutor::new(
                    plan,
                    auth_registry,
                    &request.effect_id,
                )),
            },
            journal_tx,
        ));
        let mut control = request.control.clone();
        let result = loop {
            let current_control = control.borrow().clone();
            let deadline_ms = match current_control {
                ExecutionControl::RunUntil(deadline_ms) => deadline_ms,
                ExecutionControl::Stop(reason) => {
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        adapter.cancel(&handle, Some(reason.clone())),
                    )
                    .await;
                    let mut evidence = turn_failure_evidence(
                        profile,
                        "agent turn stopped by operator".into(),
                        String::new(),
                        String::new(),
                    );
                    evidence.stop_reason = Some(reason);
                    break Err(TypedFailure::OperatorStopped {
                        evidence: Box::new(evidence),
                    });
                }
            };
            tokio::select! {
                completed = &mut turn => break completed.map_err(|err| {
                    err.downcast_ref::<TypedFailure>()
                        .cloned()
                        .unwrap_or_else(|| TypedFailure::permanent("runtime.unknown", err.to_string()))
                }),
                changed = control.changed() => {
                    if changed.is_err() {
                        continue;
                    }
                }
                () = tokio::time::sleep(crate::ports::remaining_until(deadline_ms)) => {
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        adapter.cancel(&handle, Some("effect deadline exhausted".into())),
                    ).await;
                    break Err(TypedFailure::DeadlineExhausted {
                        evidence: Box::new(turn_failure_evidence(
                            profile,
                            "agent turn exceeded its recorded effect deadline".into(),
                            String::new(),
                            String::new(),
                        )),
                    });
                }
            }
        };
        drop(turn);
        let _ = adapter.close(&handle).await;
        let (last_error, final_response) = drain.await.unwrap_or_default();

        match result {
            Err(mut failure) => {
                let evidence = failure.evidence_mut();
                evidence.stderr =
                    lionclaw_runtime_api::bounded_text(last_error.as_deref().unwrap_or_default());
                evidence.final_response = lionclaw_runtime_api::bounded_text(&final_response);
                evidence.configuration.requested_model = profile.model.clone();
                evidence.configuration.requested_mode = profile.mode.clone();
                Err(failure.projected())
            }
            Ok(result) => {
                let configuration = result.configuration;
                if configuration.requested_model != profile.model
                    || configuration.requested_mode != profile.mode
                    || profile.model.is_some() && configuration.applied_model.is_none()
                    || profile.mode.is_some() && configuration.applied_mode.is_none()
                {
                    return Err(launch(format!(
                        "runtime did not prove requested configuration was applied: requested model={:?} mode={:?}, evidence={configuration:?}",
                        profile.model, profile.mode
                    )));
                }
                Ok((configuration, final_response))
            }
        }
    }
}

fn turn_failure_evidence(
    profile: &MissionRuntimeProfile,
    detail: String,
    stderr: String,
    final_response: String,
) -> TypedFailureEvidence {
    TypedFailureEvidence {
        detail: lionclaw_runtime_api::bounded_text(&detail),
        stderr: lionclaw_runtime_api::bounded_text(&stderr),
        final_response: lionclaw_runtime_api::bounded_text(&final_response),
        configuration: lionclaw_runtime_api::AppliedRuntimeConfiguration {
            requested_model: profile.model.clone(),
            requested_mode: profile.mode.clone(),
            ..Default::default()
        },
        ..Default::default()
    }
}

/// Env for a mission container: HOME/XDG under the runtime home, TMPDIR, and
/// cargo (CARGO_HOME/CARGO_TARGET_DIR) under the writable scratch mount so
/// builds stay out of the read-only rootfs. Kept minimal and mission-specific
/// rather than importing the kernel planner's env builder.
fn mission_environment(dirs: &EffectDirs) -> Vec<(String, String)> {
    let home = lionclaw_confinement::RUNTIME_HOME_MOUNT_TARGET;
    vec![
        ("HOME".to_string(), home.to_string()),
        ("XDG_CONFIG_HOME".to_string(), format!("{home}/.config")),
        ("XDG_CACHE_HOME".to_string(), format!("{home}/.cache")),
        ("XDG_DATA_HOME".to_string(), format!("{home}/.local/share")),
        ("XDG_STATE_HOME".to_string(), format!("{home}/.local/state")),
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
        (
            "LIONCLAW_WORKSPACE_DIR".to_string(),
            WORKSPACE_MOUNT_TARGET.to_string(),
        ),
    ]
    .into_iter()
    .chain(std::iter::once((
        "MISSION_EFFECT".to_string(),
        dirs.root.to_string_lossy().into_owned(),
    )))
    .collect()
}

/// Deterministic session UUID derived from the effect ID (no RNG).
fn uuid_from_key(key: &str) -> uuid::Uuid {
    let digest = <sha2::Sha256 as sha2::Digest>::digest(key.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    uuid::Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mission_type::SkillPackage;
    use lionclaw_confinement::MountAccess;
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
    fn role_skill_mounts_use_the_runtime_native_directory_read_only() {
        let temp = tempfile::tempdir().unwrap();
        let profiles = RuntimeProfiles::from_toml(
            "[runtimes.example]\ndriver = \"acp\"\ncommand = \"example\"\nskills-dir = \".native/skills\"\n",
            Path::new("/home/alice"),
        )
        .unwrap();
        let profile = profiles.get("example").unwrap();

        let runtime_home = temp.path().join("runtime-home");
        let mounts = prepare_skill_mounts(
            &runtime_home,
            &[SkillPackage {
                name: "mission-skill".to_string(),
                root: temp.path().join("mission-skill"),
                description: "mission skill".to_string(),
            }],
            profile.skills_dir.as_ref(),
        )
        .unwrap();

        assert_eq!(mounts.len(), 1);
        assert!(mounts
            .iter()
            .all(|mount| mount.access == MountAccess::ReadOnly));
        assert!(mounts
            .iter()
            .any(|mount| mount.target == "/runtime/home/.native/skills/mission-skill"));
        assert!(runtime_home.join(".native/skills/mission-skill").is_dir());
    }

    #[test]
    fn mission_skills_require_a_runtime_skills_directory() {
        let err = prepare_skill_mounts(
            Path::new("/runtime-home"),
            &[SkillPackage {
                name: "mission-skill".to_string(),
                root: "/mission-type/skills/mission-skill".into(),
                description: "mission skill".to_string(),
            }],
            None,
        )
        .expect_err("missing projection");

        assert!(err.to_string().contains("no skills-dir"));
        assert!(prepare_skill_mounts(Path::new("/runtime-home"), &[], None)
            .unwrap()
            .is_empty());
    }

    async fn git(repo: &Path, args: &[&str]) -> String {
        let output = tokio::process::Command::new("git")
            .current_dir(repo)
            .args(args)
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }

    #[tokio::test]
    async fn writer_workspace_reuse_preserves_dirty_work_and_clean_rebase_moves_head() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir(&repo).unwrap();
        git(&repo, &["init", "-q"]).await;
        git(&repo, &["config", "user.name", "test"]).await;
        git(&repo, &["config", "user.email", "test@local"]).await;
        git(&repo, &["config", "commit.gpgsign", "false"]).await;
        std::fs::write(repo.join("tracked"), "base\n").unwrap();
        git(&repo, &["add", "tracked"]).await;
        git(&repo, &["commit", "-q", "-m", "base"]).await;
        let base = git(&repo, &["rev-parse", "HEAD"]).await;
        let task_work = temp.path().join("task/work");
        prepare_writer_checkout(&repo, &task_work, &base, true)
            .await
            .unwrap();

        std::fs::write(task_work.join("substantial-uncommitted"), "preserve me\n").unwrap();
        prepare_writer_checkout(&repo, &task_work, &base, false)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(task_work.join("substantial-uncommitted")).unwrap(),
            "preserve me\n"
        );

        std::fs::remove_file(task_work.join("substantial-uncommitted")).unwrap();
        std::fs::write(repo.join("tracked"), "moved\n").unwrap();
        git(&repo, &["add", "tracked"]).await;
        git(&repo, &["commit", "-q", "-m", "moved"]).await;
        let moved = git(&repo, &["rev-parse", "HEAD"]).await;
        prepare_writer_checkout(&repo, &task_work, &moved, true)
            .await
            .unwrap();
        assert_eq!(workspace::head_sha(&task_work).await.unwrap(), moved);
    }

    #[tokio::test]
    async fn moved_base_never_recreates_a_dirty_writer_workspace() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir(&repo).unwrap();
        git(&repo, &["init", "-q"]).await;
        git(&repo, &["config", "user.name", "test"]).await;
        git(&repo, &["config", "user.email", "test@local"]).await;
        git(&repo, &["config", "commit.gpgsign", "false"]).await;
        std::fs::write(repo.join("tracked"), "base\n").unwrap();
        git(&repo, &["add", "tracked"]).await;
        git(&repo, &["commit", "-q", "-m", "base"]).await;
        let base = git(&repo, &["rev-parse", "HEAD"]).await;
        let task_work = temp.path().join("task/work");
        prepare_writer_checkout(&repo, &task_work, &base, true)
            .await
            .unwrap();
        std::fs::write(task_work.join("substantial-uncommitted"), "preserve me\n").unwrap();

        let error = prepare_writer_checkout(&repo, &task_work, &base, true)
            .await
            .unwrap_err();
        assert!(error.detail().contains("refusing to recreate"));
        assert_eq!(
            std::fs::read_to_string(task_work.join("substantial-uncommitted")).unwrap(),
            "preserve me\n"
        );
    }
}
