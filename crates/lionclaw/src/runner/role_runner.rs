//! `OciRoleRunner`: the production [`RoleRunner`]. One dispatch =
//! isolate the workspace, compile the plan through the moat, run one full
//! agent turn under confinement, capture the handoff (and, for writers, the
//! resulting commit). The engine owns deadlines; this boundary observes its
//! control channel throughout setup, execution, and capture.

use std::sync::Arc;

use async_trait::async_trait;
use lionclaw_confinement::WORKSPACE_MOUNT_TARGET;
use lionclaw_runtime_acp::AcpRuntimeDriver;
use lionclaw_runtime_api::{
    RuntimeAuthProvider, RuntimeAuthRegistry, RuntimeDriverConfig, RuntimeDriverProvider,
    RuntimeSessionReady, RuntimeSessionStartInput, TurnExecution, TurnInput, TypedFailure,
    TypedFailureEvidence,
};
use lionclaw_runtime_codex::{
    CodexRuntimeAuthProvider, CodexRuntimeDriver, CODEX_RUNTIME_AUTH_KIND,
};
use tokio::sync::Mutex;

use crate::authority::{
    compile_authority, compile_role_plan, AuthorityCeiling, MissionMounts, RolePlanRequest,
};
use crate::config::{MissionRuntimeProfile, RuntimeAuthConfig, RuntimeProfiles};
use crate::model::OutputSemantics;
use crate::ports::{ExecutionControl, RoleRunOutcome, RoleRunRequest, RoleRunner};

use super::executor::{mission_execution_context, MissionProgramExecutor};
use super::handoff::read_handoff;
use super::native_home_auth::NativeHomeAuthProvider;
use super::{
    await_controlled, prepare_skill_mounts, ConversationDirs, EffectDirs, SCRATCH_MOUNT_TARGET,
};
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

#[derive(Clone, Copy)]
enum CancellationKind {
    Deadline,
    Stop,
    Abort,
}

impl CancellationKind {
    fn setup_detail(self) -> &'static str {
        match self {
            Self::Deadline => "effect deadline reached before or after the runtime turn",
            Self::Stop => "effect stopped before or after the runtime turn",
            Self::Abort => "mission aborted before or after the runtime turn",
        }
    }

    fn setup_code(self) -> &'static str {
        match self {
            Self::Deadline => "kernel.deadline",
            Self::Stop => "kernel.stopped",
            Self::Abort => "kernel.aborted",
        }
    }

    fn active_detail(self) -> &'static str {
        match self {
            Self::Deadline => "agent turn exceeded its recorded effect deadline",
            Self::Stop => "agent turn stopped by operator",
            Self::Abort => "mission aborted by operator",
        }
    }

    fn failure(self, evidence: lionclaw_runtime_api::TypedFailureEvidence) -> TypedFailure {
        match self {
            Self::Deadline => TypedFailure::DeadlineExhausted {
                evidence: Box::new(evidence),
            },
            Self::Stop => TypedFailure::OperatorStopped {
                evidence: Box::new(evidence),
            },
            Self::Abort => TypedFailure::OperatorAborted {
                evidence: Box::new(evidence),
            },
        }
    }
}

fn setup_control_failure(
    profile: &MissionRuntimeProfile,
    control: &ExecutionControl,
) -> Option<TypedFailure> {
    let (kind, reason) = match control {
        ExecutionControl::RunUntil(_) => return None,
        ExecutionControl::DeadlineExhausted => (
            CancellationKind::Deadline,
            "effect deadline exhausted".to_string(),
        ),
        ExecutionControl::Stop(reason) => (CancellationKind::Stop, reason.clone()),
        ExecutionControl::Abort(reason) => (CancellationKind::Abort, reason.clone()),
    };
    let mut evidence = turn_failure_evidence(
        profile,
        kind.setup_detail().into(),
        String::new(),
        String::new(),
    );
    evidence.code = Some(kind.setup_code().into());
    evidence.stop_reason = Some(reason);
    Some(kind.failure(evidence))
}

async fn cancellation_acknowledged<A, T, O>(
    acknowledgement: A,
    mut turn: std::pin::Pin<&mut T>,
    timeout: std::time::Duration,
) -> (bool, Option<O>)
where
    A: std::future::Future<Output = anyhow::Result<lionclaw_runtime_api::RuntimeCancellation>>,
    T: std::future::Future<Output = O>,
{
    tokio::pin!(acknowledgement);
    let mut cancellation_acknowledged = false;
    let mut turn_result = None;
    let completed_in_time = tokio::time::timeout(timeout, async {
        while !cancellation_acknowledged || turn_result.is_none() {
            tokio::select! {
                result = &mut acknowledgement, if !cancellation_acknowledged => {
                    match result {
                        Ok(lionclaw_runtime_api::RuntimeCancellation::Acknowledged) => {
                            cancellation_acknowledged = true;
                        }
                        Ok(lionclaw_runtime_api::RuntimeCancellation::NoActiveTurn) | Err(_) => {
                            return;
                        }
                    }
                }
                result = turn.as_mut(), if turn_result.is_none() => turn_result = Some(result),
            }
        }
    })
    .await
    .is_ok();
    (
        completed_in_time && cancellation_acknowledged && turn_result.is_some(),
        turn_result,
    )
}

fn completed_turn_evidence(
    completed: Option<anyhow::Result<lionclaw_runtime_api::TurnResult>>,
) -> Option<(lionclaw_runtime_api::AppliedRuntimeConfiguration, String)> {
    completed.and_then(|completed| match completed {
        Ok(result) => Some((result.configuration, result.final_response)),
        Err(error) => error.downcast_ref::<TypedFailure>().map(|failure| {
            (
                failure.evidence().configuration.clone(),
                failure.evidence().final_response.clone(),
            )
        }),
    })
}

async fn prepare_writer_checkout(
    repo: &std::path::Path,
    workspace: &std::path::Path,
    observer_index: &std::path::Path,
    base_sha: &str,
    recreate_workspace: bool,
) -> Result<(), TypedFailure> {
    let mut replace = !workspace.exists();
    if workspace.exists() {
        let head = workspace::task_head_sha(workspace)
            .await
            .map_err(|e| launch(format!("failed to inspect retained checkout HEAD: {e}")))?;
        if !recreate_workspace {
            if workspace::task_commit_exists(workspace, base_sha).await
                && workspace::task_is_ancestor(workspace, base_sha, &head)
                    .await
                    .map_err(|e| {
                        launch(format!("failed to compare retained checkout ancestry: {e}"))
                    })?
            {
                workspace::prepare_task_observer_index(repo, observer_index, base_sha, false)
                    .await
                    .map_err(|e| launch(format!("failed to prepare workspace observer: {e}")))?;
                return Ok(());
            }
            return Err(launch(format!(
                "retained task workspace HEAD {head} does not descend from its recorded base {base_sha}"
            )));
        }
        if head == base_sha {
            replace = false;
        } else {
            if workspace::task_is_dirty(workspace)
                .await
                .map_err(|e| launch(format!("failed to inspect retained checkout: {e}")))?
            {
                return Err(launch(
                    "refusing to recreate a dirty task workspace on a moved base".to_string(),
                ));
            }
            if !workspace::commit_exists(repo, &head).await
                || !workspace::is_ancestor(repo, &head, base_sha)
                    .await
                    .map_err(|e| {
                        launch(format!("failed to compare retained checkout ancestry: {e}"))
                    })?
            {
                return Err(launch(format!(
                    "refusing to recreate task workspace with uncaptured commits at {head}"
                )));
            }
            replace = true;
        }
    }
    if replace {
        workspace::replace_checkout(repo, workspace, base_sha)
            .await
            .map_err(|e| launch(format!("failed to create checkout: {e}")))?;
    }
    workspace::prepare_task_observer_index(
        repo,
        observer_index,
        base_sha,
        recreate_workspace || replace,
    )
    .await
    .map_err(|e| launch(format!("failed to prepare workspace observer: {e}")))
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
        let namespace = match request.role.output {
            OutputSemantics::ProposesPlan | OutputSemantics::ProducesReport => {
                crate::model::TaskNamespace::Planning
            }
            OutputSemantics::ProducesArtifact
            | OutputSemantics::EmitsVerdict
            | OutputSemantics::EmitsGapVerdict => crate::model::TaskNamespace::Execution,
        };
        let conversation_id = crate::model::ConversationId::for_role_instance(
            &request.mission_id,
            namespace,
            &request.task_id,
            &request.role.name,
            request.assignment_epoch,
        );
        let conversation_dirs = ConversationDirs::prepare(
            &request.state_dir,
            request.mission_id.as_str(),
            &conversation_id,
        )
        .map_err(|e| launch(format!("failed to prepare conversation dirs: {e}")))?;

        let dirs = EffectDirs::prepare(
            &request.state_dir,
            request.mission_id.as_str(),
            &request.effect_id,
        )
        .map_err(|e| launch(format!("failed to prepare attempt dirs: {e}")))?;

        let setup = async {
            let skill_mounts = prepare_skill_mounts(
                &dirs.runtime_home,
                &request.skills,
                profile.skills_dir.as_ref(),
            )
            .map_err(|err| launch(format!("failed to prepare role skills: {err:#}")))?;
            let authority = compile_authority(&request.role, &self.ceiling)
                .map_err(|e| launch(format!("authority refused to compile: {e}")))?;
            let is_writer = authority.output() == OutputSemantics::ProducesArtifact;
            let (workspace_source, scratch_source, observer_index) = if is_writer {
                let capture = request.artifact_capture.as_ref().ok_or_else(|| {
                    launch("artifact-producing role has no capture authority".into())
                })?;
                if capture.checkout_dir() != conversation_dirs.work {
                    return Err(launch(
                        "artifact capture authority names a different task checkout".into(),
                    ));
                }
                (
                    capture.checkout_dir().to_path_buf(),
                    conversation_dirs.scratch.clone(),
                    Some(conversation_dirs.observer_index.clone()),
                )
            } else {
                if request.artifact_capture.is_some() {
                    return Err(launch(
                        "read-only role received artifact capture authority".into(),
                    ));
                }
                (
                    conversation_dirs.work.clone(),
                    conversation_dirs.scratch.clone(),
                    None,
                )
            };
            {
                let _guard = self.repo_lock.lock().await;
                if is_writer {
                    prepare_writer_checkout(
                        &request.workspace_dir,
                        &workspace_source,
                        observer_index.as_deref().expect("writer observer index"),
                        &request.base_sha,
                        request.recreate_workspace,
                    )
                    .await?;
                    request
                        .updates
                        .send(crate::ports::RoleRunUpdate::WorkspacePrepared {
                            base_sha: request.base_sha.clone(),
                            assignment_epoch: request.assignment_epoch,
                        })
                        .await
                        .map_err(|_| launch("kernel role update receiver closed".into()))?;
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
            if let Some(runtime) = extras
                .iter_mut()
                .find(|mount| mount.target == lionclaw_confinement::RUNTIME_MOUNT_TARGET)
            {
                runtime.source = conversation_dirs.runtime.clone();
            }
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
            Ok((is_writer, compiled.plan().clone()))
        };
        let (is_writer, plan) = await_controlled(setup, request.control.clone(), |control| {
            setup_control_failure(&profile, control)
        })
        .await?;

        // The adapter owns cancellation acknowledgement while its turn is
        // live. Setup and capture use the same engine control, but are simply
        // dropped: their child processes are kill-on-drop and task work is not.
        let (applied, final_response) = self.run_turn(&profile, &request, plan).await?;
        let cancellation_configuration = applied.clone();
        let cancellation_response = final_response.clone();
        let finish = async {
            let handoff =
                read_handoff(&dirs.handoff, request.role.output).map_err(|mut failure| {
                    failure.evidence_mut().final_response = final_response.clone();
                    failure.evidence_mut().configuration = applied.clone();
                    failure
                })?;
            let artifact = if is_writer {
                let _guard = self.repo_lock.lock().await;
                let artifact = request
                    .artifact_capture
                    .as_ref()
                    .expect("writer capture authority was checked during setup")
                    .capture()
                    .await
                    .map_err(|e| {
                        let mut failure = match e {
                            workspace::CaptureError::DirtyWorktree(_) => {
                                TypedFailure::invalid("workspace.dirty", e.to_string())
                            }
                            workspace::CaptureError::HistoryDiverged { .. } => {
                                TypedFailure::permanent("workspace.history", e.to_string())
                            }
                            workspace::CaptureError::Infra(_) => {
                                TypedFailure::permanent("workspace.capture", e.to_string())
                            }
                        };
                        failure.evidence_mut().final_response = final_response.clone();
                        failure.evidence_mut().configuration = applied.clone();
                        failure
                    })?;
                Some(artifact)
            } else {
                None
            };
            Ok(RoleRunOutcome {
                handoff,
                artifact,
                runtime_configuration: crate::model::RuntimeConfigurationEvidence {
                    requested_model: applied.requested_model,
                    applied_model: applied.applied_model,
                    model_confirmation: applied.model_confirmation,
                    requested_mode: applied.requested_mode,
                    applied_mode: applied.applied_mode,
                    mode_confirmation: applied.mode_confirmation,
                },
                final_response,
            })
        };
        await_controlled(finish, request.control.clone(), |control| {
            setup_control_failure(&profile, control).map(|mut failure| {
                failure.evidence_mut().configuration = cancellation_configuration.clone();
                failure.evidence_mut().final_response = cancellation_response.clone();
                failure
            })
        })
        .await
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

        let state_root = profile
            .native_resume
            .then(|| {
                plan.mounts
                    .iter()
                    .find(|m| m.target == lionclaw_confinement::RUNTIME_MOUNT_TARGET)
                    .map(|m| m.source.clone())
            })
            .flatten();
        let runtime_session_ready = state_root
            .as_deref()
            .map(RuntimeSessionReady::from_runtime_state_root)
            .transpose()
            .map_err(|e| launch(format!("native session state invalid: {e}")))?
            .unwrap_or_else(RuntimeSessionReady::not_ready);
        let start = async {
            adapter
                .session_start(RuntimeSessionStartInput {
                    session_id: uuid_from_key(request.effect_id.as_str()),
                    working_dir: Some(WORKSPACE_MOUNT_TARGET.to_string()),
                    environment: plan.environment.clone(),
                    resume: match state_root.clone() {
                        Some(state_root) => lionclaw_runtime_api::RuntimeResume::Native {
                            state_root,
                            ready: runtime_session_ready,
                        },
                        None => lionclaw_runtime_api::RuntimeResume::Reconstruct,
                    },
                })
                .await
                .map_err(|e| launch(format!("session_start failed: {e}")))
        };
        let handle = await_controlled(start, request.control.clone(), |control| {
            setup_control_failure(profile, control)
        })
        .await?;

        let (journal_tx, journal_rx) = tokio::sync::mpsc::channel::<lionclaw_runtime_api::TurnEvent>(
            lionclaw_runtime_api::RUNTIME_TURN_JOURNAL_CAPACITY,
        );
        let updates = request.updates.clone();
        let activity = request.activity.clone();
        let activity_effect_id = request.effect_id.clone();
        let drain = tokio::spawn(drain_runtime_journal(
            journal_rx,
            updates,
            activity,
            activity_effect_id,
        ));

        let mut turn = Box::pin(adapter.turn(
            TurnExecution {
                input: TurnInput {
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
        enum TurnEnd {
            Completed(anyhow::Result<lionclaw_runtime_api::TurnResult>),
            Cancel {
                reason: String,
                kind: CancellationKind,
            },
        }
        let end = loop {
            let current_control = control.borrow().clone();
            match current_control {
                ExecutionControl::RunUntil(_) => {}
                ExecutionControl::DeadlineExhausted => {
                    break TurnEnd::Cancel {
                        reason: "effect deadline exhausted".into(),
                        kind: CancellationKind::Deadline,
                    };
                }
                ExecutionControl::Stop(reason) => {
                    break TurnEnd::Cancel {
                        reason,
                        kind: CancellationKind::Stop,
                    };
                }
                ExecutionControl::Abort(reason) => {
                    break TurnEnd::Cancel {
                        reason,
                        kind: CancellationKind::Abort,
                    };
                }
            }
            tokio::select! {
                biased;
                changed = control.changed() => {
                    if changed.is_err() {
                        continue;
                    }
                }
                completed = &mut turn => break TurnEnd::Completed(completed),
            }
        };
        let result = match end {
            TurnEnd::Completed(completed) => completed.map_err(|err| {
                err.downcast_ref::<TypedFailure>()
                    .cloned()
                    .unwrap_or_else(|| TypedFailure::permanent("runtime.unknown", err.to_string()))
            }),
            TurnEnd::Cancel { reason, kind } => {
                let (acknowledged, completed) = cancellation_acknowledged(
                    adapter.cancel(&handle, Some(reason.clone())),
                    turn.as_mut(),
                    std::time::Duration::from_secs(5),
                )
                .await;
                let mut evidence = turn_failure_evidence(
                    profile,
                    kind.active_detail().into(),
                    String::new(),
                    String::new(),
                );
                if let Some((configuration, final_response)) = completed_turn_evidence(completed) {
                    evidence.configuration = configuration;
                    evidence.final_response = final_response;
                }
                evidence.code = Some(
                    if acknowledged {
                        "runtime.cancel_acknowledged"
                    } else {
                        "runtime.cancel_forced"
                    }
                    .into(),
                );
                evidence.stop_reason = Some(reason);
                Err(kind.failure(evidence))
            }
        };
        drop(turn);
        let _ = adapter.close(&handle).await;
        let fallback_final_response = drain.await.unwrap_or_default();

        // Native identity is conversation state, not successful-effect state.
        // Once a turn has launched, retain whatever opaque identity the
        // adapter durably recorded even when the delivered outcome is a
        // failure, interruption, or deadline. Adapters with no saved identity
        // truthfully reconstruct on the next request.
        if let Some(root) = state_root {
            std::fs::write(
                root.join(lionclaw_runtime_api::RUNTIME_SESSION_READY_MARKER),
                b"ready\n",
            )
            .map_err(|e| launch(format!("failed to commit native session state: {e}")))?;
        }

        match result {
            Err(failure) => Err(project_turn_failure(
                profile,
                failure,
                &fallback_final_response,
            )),
            Ok(result) => {
                let result = validate_completed_turn(profile, result)?;
                Ok(result)
            }
        }
    }
}

async fn drain_runtime_journal(
    mut journal: tokio::sync::mpsc::Receiver<lionclaw_runtime_api::TurnEvent>,
    updates: tokio::sync::mpsc::Sender<crate::ports::RoleRunUpdate>,
    activity: tokio::sync::watch::Sender<
        Option<(crate::model::EffectId, lionclaw_runtime_api::TurnEvent)>,
    >,
    effect_id: crate::model::EffectId,
) -> String {
    let mut final_response = String::new();
    while let Some(event) = journal.recv().await {
        lionclaw_runtime_api::observe_final_response(&mut final_response, event.event());
        if let lionclaw_runtime_api::RuntimeEvent::Configuration { configuration } = event.event() {
            let _ = updates
                .send(crate::ports::RoleRunUpdate::RuntimeConfigured(
                    configuration.clone(),
                ))
                .await;
        }
        activity.send_replace(Some((effect_id.clone(), event)));
    }
    final_response.trim_end().to_string()
}

fn project_turn_failure(
    profile: &MissionRuntimeProfile,
    mut failure: TypedFailure,
    fallback_final_response: &str,
) -> TypedFailure {
    let evidence = failure.evidence_mut();
    evidence.configuration.requested_model = profile.model.clone();
    evidence.configuration.requested_mode = profile.mode.clone();
    if evidence.final_response.is_empty() {
        evidence.final_response = fallback_final_response.to_string();
    }
    failure.projected()
}

fn validate_completed_turn(
    profile: &MissionRuntimeProfile,
    result: lionclaw_runtime_api::TurnResult,
) -> Result<(lionclaw_runtime_api::AppliedRuntimeConfiguration, String), TypedFailure> {
    let result = result.projected();
    let configuration = result.configuration;
    let final_response = result.final_response;
    if configuration.requested_model != profile.model
        || configuration.requested_mode != profile.mode
        || profile.model.is_some() && configuration.applied_model.is_none()
        || profile.model.is_some() && configuration.model_confirmation.is_none()
        || profile.mode.is_some() && configuration.applied_mode.is_none()
        || profile.mode.is_some() && configuration.mode_confirmation.is_none()
    {
        let mut failure = launch(format!(
            "runtime did not prove requested configuration was applied: requested model={:?} mode={:?}, evidence={configuration:?}",
            profile.model, profile.mode
        ));
        failure.evidence_mut().configuration = configuration;
        failure.evidence_mut().final_response = final_response;
        return Err(project_turn_failure(profile, failure, ""));
    }
    Ok((configuration, final_response))
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

    async fn prepare_test_writer(
        repo: &Path,
        task_work: &Path,
        base: &str,
        recreate: bool,
    ) -> Result<(), TypedFailure> {
        let observer_index = task_work
            .parent()
            .expect("test task work has a parent")
            .join("observer.index");
        prepare_writer_checkout(repo, task_work, &observer_index, base, recreate).await
    }

    #[tokio::test]
    async fn cancellation_polls_the_turn_that_must_deliver_its_acknowledgement() {
        let (sent, received) = tokio::sync::oneshot::channel();
        let mut sent = Some(sent);
        let mut turn = std::future::poll_fn(move |_| {
            if let Some(sent) = sent.take() {
                let _ = sent.send(());
            }
            std::task::Poll::Ready(())
        });
        let acknowledgement = async move {
            received.await.map_err(anyhow::Error::from)?;
            Ok(lionclaw_runtime_api::RuntimeCancellation::Acknowledged)
        };

        let (acknowledged, turn_result) = cancellation_acknowledged(
            acknowledgement,
            std::pin::Pin::new(&mut turn),
            std::time::Duration::from_millis(100),
        )
        .await;
        assert!(acknowledged);
        assert_eq!(turn_result, Some(()));
    }

    #[tokio::test]
    async fn cancellation_without_an_active_turn_drops_setup_immediately() {
        let mut turn = Box::pin(async {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            "provider turn started"
        });
        let started = tokio::time::Instant::now();
        let (acknowledged, turn_result) = cancellation_acknowledged(
            std::future::ready(Ok(lionclaw_runtime_api::RuntimeCancellation::NoActiveTurn)),
            turn.as_mut(),
            std::time::Duration::from_secs(5),
        )
        .await;

        assert!(!acknowledged);
        assert_eq!(turn_result, None);
        assert!(started.elapsed() < std::time::Duration::from_millis(100));
    }

    #[test]
    fn cancellation_reclassification_preserves_runtime_evidence() {
        let expected = lionclaw_runtime_api::AppliedRuntimeConfiguration {
            requested_mode: Some("build".into()),
            applied_mode: Some("build".into()),
            mode_confirmation: Some(
                lionclaw_runtime_api::RuntimeConfigurationConfirmation::Observed,
            ),
            ..Default::default()
        };
        let mut failure = TypedFailure::permanent("runtime.cancelled", "cancelled");
        failure.evidence_mut().configuration = expected.clone();
        failure.evidence_mut().final_response = "work before stop".into();
        assert_eq!(
            completed_turn_evidence(Some(Err(anyhow::Error::new(failure)))),
            Some((expected, "work before stop".into()))
        );
    }

    #[test]
    fn completed_turn_accepts_only_confirmed_adapter_canonicalization() {
        let profiles = RuntimeProfiles::from_toml(
            "[runtimes.example]\ndriver = \"acp\"\ncommand = \"example\"\nmodel = \"requested\"\n\
             [runtimes.mode]\ndriver = \"acp\"\ncommand = \"example\"\nmode = \"build\"\n",
            Path::new("/home/alice"),
        )
        .unwrap();
        let profile = profiles.get("example").unwrap();
        let canonical = validate_completed_turn(
            &profile,
            lionclaw_runtime_api::TurnResult {
                configuration: lionclaw_runtime_api::AppliedRuntimeConfiguration {
                    requested_model: Some("requested".into()),
                    applied_model: Some("provider:requested".into()),
                    model_confirmation: Some(
                        lionclaw_runtime_api::RuntimeConfigurationConfirmation::Acknowledged,
                    ),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .expect("the adapter owns advertised name-to-ID equivalence");
        assert_eq!(
            canonical.0.applied_model.as_deref(),
            Some("provider:requested")
        );

        let oversized_applied = "x".repeat(lionclaw_runtime_api::FAILURE_TEXT_LIMIT + 1);
        let bounded = validate_completed_turn(
            &profile,
            lionclaw_runtime_api::TurnResult {
                configuration: lionclaw_runtime_api::AppliedRuntimeConfiguration {
                    requested_model: Some("requested".into()),
                    applied_model: Some(oversized_applied),
                    model_confirmation: Some(
                        lionclaw_runtime_api::RuntimeConfigurationConfirmation::Acknowledged,
                    ),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .expect("the durable outcome boundary bounds adapter evidence");
        assert!(bounded.0.applied_model.unwrap().len() <= lionclaw_runtime_api::FAILURE_TEXT_LIMIT);

        let failure = validate_completed_turn(
            &profile,
            lionclaw_runtime_api::TurnResult {
                configuration: lionclaw_runtime_api::AppliedRuntimeConfiguration {
                    requested_model: Some("requested".into()),
                    applied_model: None,
                    ..Default::default()
                },
                final_response: "useful work before configuration rejection".into(),
            },
        )
        .unwrap_err();

        assert_eq!(
            failure.evidence().final_response,
            "useful work before configuration rejection"
        );
        assert_eq!(
            failure.evidence().configuration.applied_model.as_deref(),
            None
        );

        let unconfirmed = validate_completed_turn(
            &profile,
            lionclaw_runtime_api::TurnResult {
                configuration: lionclaw_runtime_api::AppliedRuntimeConfiguration {
                    requested_model: Some("requested".into()),
                    applied_model: Some("unrelated-fallback".into()),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .unwrap_err();
        assert_eq!(
            unconfirmed
                .evidence()
                .configuration
                .applied_model
                .as_deref(),
            Some("unrelated-fallback")
        );

        let mode_profile = profiles.get("mode").unwrap();
        let unconfirmed_mode = validate_completed_turn(
            &mode_profile,
            lionclaw_runtime_api::TurnResult {
                configuration: lionclaw_runtime_api::AppliedRuntimeConfiguration {
                    requested_mode: Some("build".into()),
                    applied_mode: Some("build".into()),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .unwrap_err();
        assert_eq!(
            unconfirmed_mode
                .evidence()
                .configuration
                .applied_mode
                .as_deref(),
            Some("build")
        );
    }

    #[test]
    fn runner_projection_preserves_adapter_process_stderr() {
        let profiles = RuntimeProfiles::from_toml(
            "[runtimes.example]\ndriver = \"acp\"\ncommand = \"example\"\n",
            Path::new("/home/alice"),
        )
        .unwrap();
        let profile = profiles.get("example").unwrap();
        let mut failure = TypedFailure::permanent("runtime.process", "process failed");
        failure.evidence_mut().stderr = "adapter-captured stderr".into();

        let projected = project_turn_failure(&profile, failure, "");

        assert_eq!(projected.evidence().stderr, "adapter-captured stderr");
    }

    #[tokio::test]
    async fn journal_drain_retains_a_bounded_response_for_forced_cancellation() {
        let (journal_tx, journal_rx) = tokio::sync::mpsc::channel(1);
        let (updates, _update_rx) = tokio::sync::mpsc::channel(1);
        let (activity, _activity_rx) = tokio::sync::watch::channel(None);
        journal_tx
            .send(lionclaw_runtime_api::TurnEvent::canonical(
                lionclaw_runtime_api::RuntimeEvent::MessageDelta {
                    lane: lionclaw_runtime_api::RuntimeMessageLane::Answer,
                    text: "partial response before forced stop".into(),
                },
            ))
            .await
            .unwrap();
        drop(journal_tx);

        let response = drain_runtime_journal(
            journal_rx,
            updates,
            activity,
            crate::model::EffectId::for_parts(&["test", "forced-response"]),
        )
        .await;

        assert_eq!(response, "partial response before forced stop");
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
        prepare_test_writer(&repo, &task_work, &base, true)
            .await
            .unwrap();

        std::fs::write(task_work.join("substantial-uncommitted"), "preserve me\n").unwrap();
        prepare_test_writer(&repo, &task_work, &base, false)
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
        prepare_test_writer(&repo, &task_work, &moved, true)
            .await
            .unwrap();
        assert_eq!(workspace::task_head_sha(&task_work).await.unwrap(), moved);
    }

    #[tokio::test]
    async fn retry_reuses_a_clean_committed_descendant_of_the_recorded_base() {
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
        prepare_test_writer(&repo, &task_work, &base, true)
            .await
            .unwrap();

        std::fs::write(task_work.join("committed-rework"), "preserve this commit\n").unwrap();
        git(&task_work, &["add", "committed-rework"]).await;
        git(&task_work, &["commit", "-q", "-m", "partial rework"]).await;
        let partial = git(&task_work, &["rev-parse", "HEAD"]).await;

        prepare_test_writer(&repo, &task_work, &base, false)
            .await
            .unwrap();
        assert_eq!(workspace::task_head_sha(&task_work).await.unwrap(), partial);
        assert_eq!(
            std::fs::read_to_string(task_work.join("committed-rework")).unwrap(),
            "preserve this commit\n"
        );
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
        prepare_test_writer(&repo, &task_work, &base, true)
            .await
            .unwrap();
        std::fs::write(task_work.join("substantial-uncommitted"), "preserve me\n").unwrap();

        std::fs::write(repo.join("tracked"), "moved\n").unwrap();
        git(&repo, &["add", "tracked"]).await;
        git(&repo, &["commit", "-q", "-m", "moved"]).await;
        let moved = git(&repo, &["rev-parse", "HEAD"]).await;

        let error = prepare_test_writer(&repo, &task_work, &moved, true)
            .await
            .unwrap_err();
        assert!(error.detail().contains("refusing to recreate"));
        assert_eq!(
            std::fs::read_to_string(task_work.join("substantial-uncommitted")).unwrap(),
            "preserve me\n"
        );
    }

    #[tokio::test]
    async fn moved_base_never_discards_clean_uncaptured_commits() {
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
        prepare_test_writer(&repo, &task_work, &base, true)
            .await
            .unwrap();

        std::fs::write(task_work.join("worker-only"), "committed work\n").unwrap();
        git(&task_work, &["add", "worker-only"]).await;
        git(&task_work, &["commit", "-q", "-m", "uncaptured"]).await;
        let uncaptured = git(&task_work, &["rev-parse", "HEAD"]).await;

        std::fs::write(repo.join("tracked"), "other task\n").unwrap();
        git(&repo, &["add", "tracked"]).await;
        git(&repo, &["commit", "-q", "-m", "other-task"]).await;
        let moved = git(&repo, &["rev-parse", "HEAD"]).await;
        let error = prepare_test_writer(&repo, &task_work, &moved, true)
            .await
            .unwrap_err();
        assert!(error.detail().contains("uncaptured commits"));
        assert_eq!(
            workspace::task_head_sha(&task_work).await.unwrap(),
            uncaptured
        );
        assert_eq!(
            std::fs::read_to_string(task_work.join("worker-only")).unwrap(),
            "committed work\n"
        );
    }

    #[tokio::test]
    async fn stale_reuse_intent_fails_closed_instead_of_replacing_disk_state() {
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
        prepare_test_writer(&repo, &task_work, &base, true)
            .await
            .unwrap();

        std::fs::write(repo.join("tracked"), "moved\n").unwrap();
        git(&repo, &["add", "tracked"]).await;
        git(&repo, &["commit", "-q", "-m", "moved"]).await;
        let moved = git(&repo, &["rev-parse", "HEAD"]).await;
        let error = prepare_test_writer(&repo, &task_work, &moved, false)
            .await
            .unwrap_err();
        assert!(error
            .detail()
            .contains("does not descend from its recorded base"));
        assert_eq!(workspace::task_head_sha(&task_work).await.unwrap(), base);
    }
}
