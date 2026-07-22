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
    RuntimeAdapter, RuntimeAuthProvider, RuntimeAuthRegistry, RuntimeDriverConfig,
    RuntimeDriverProvider, RuntimeDriverRegistry, RuntimeNativeReopenOutcome,
    RuntimeNativeReopenRecovery, RuntimeResume, RuntimeResumeMode, RuntimeSessionHandle,
    RuntimeSessionReady, RuntimeSessionStartInput, TurnExecution, TurnInput, TypedFailure,
    TypedFailureEvidence,
};
use lionclaw_runtime_codex::{CodexRuntimeAuthProvider, CodexRuntimeDriver};
use tokio::sync::Mutex;

use crate::authority::{
    compile_authority, compile_role_plan, AuthorityCeiling, MissionMounts, RolePlanRequest,
};
use crate::config::{MissionRuntimeProfile, RuntimeAuthConfig, RuntimeProfiles};
use crate::model::{OutputSemantics, RoleResourceLifetime};
use crate::ports::{ExecutionControl, RoleRunOutcome, RoleRunRequest, RoleRunner};
use crate::resources::MissionDirs;

use super::executor::{mission_execution_context, MissionProgramExecutor};
use super::handoff::read_optional_handoff;
use super::native_home_auth::NativeHomeAuthProvider;
use super::{await_controlled, effect_mounts, prepare_skill_mounts};
use crate::workspace;

pub struct OciRoleRunner {
    profiles: RuntimeProfiles,
    image_id: String,
    ceiling: AuthorityCeiling,
    drivers: RuntimeDriverRegistry,
    auth_providers: RuntimeAuthRegistry,
    /// Serializes Git checkout/capture operations inside this process. The
    /// mission driver lock provides cross-process coordination.
    repo_lock: Arc<Mutex<()>>,
}

impl OciRoleRunner {
    pub fn new(profiles: RuntimeProfiles, image_id: String, ceiling: AuthorityCeiling) -> Self {
        let drivers = RuntimeDriverRegistry::new([
            Arc::new(CodexRuntimeDriver) as Arc<dyn RuntimeDriverProvider>,
            Arc::new(AcpRuntimeDriver) as Arc<dyn RuntimeDriverProvider>,
        ]);
        let auth_providers = RuntimeAuthRegistry::new([
            Arc::new(CodexRuntimeAuthProvider) as Arc<dyn RuntimeAuthProvider>
        ]);
        Self::with_registries(profiles, image_id, ceiling, drivers, auth_providers)
    }

    /// Construct the production runner with protocol registries supplied by
    /// the caller. This is the transport seam used by production-path tests:
    /// workspace, authority, session, handoff, and capture behavior remains
    /// the real runner while only the external native-runtime transport is
    /// substituted.
    pub fn with_registries(
        profiles: RuntimeProfiles,
        image_id: String,
        ceiling: AuthorityCeiling,
        drivers: RuntimeDriverRegistry,
        auth_providers: RuntimeAuthRegistry,
    ) -> Self {
        Self {
            profiles,
            image_id,
            ceiling,
            drivers,
            auth_providers,
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

    fn driver(
        &self,
        profile: &MissionRuntimeProfile,
    ) -> anyhow::Result<Arc<dyn RuntimeDriverProvider>> {
        self.drivers.get(&profile.driver).ok_or_else(|| {
            anyhow::anyhow!(
                "unknown runtime driver '{}' (registered: {})",
                profile.driver,
                self.drivers.names().collect::<Vec<_>>().join(", ")
            )
        })
    }

    fn auth_registry(
        &self,
        profile: &MissionRuntimeProfile,
    ) -> anyhow::Result<RuntimeAuthRegistry> {
        let Some(auth) = &profile.auth else {
            return Ok(RuntimeAuthRegistry::empty());
        };
        match auth {
            RuntimeAuthConfig::Provider(kind) => self
                .auth_providers
                .get_kind(kind)
                .map(|provider| RuntimeAuthRegistry::new([provider]))
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "runtime '{}' configures unsupported auth provider '{kind}'",
                        profile.name
                    )
                }),
            RuntimeAuthConfig::NativeHome(config) => {
                Ok(RuntimeAuthRegistry::new([
                    Arc::new(NativeHomeAuthProvider::new(config.clone()))
                        as Arc<dyn RuntimeAuthProvider>,
                ]))
            }
        }
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
        let runner = Self::new(
            RuntimeProfiles::single(profile.clone()),
            String::new(),
            AuthorityCeiling::default(),
        );
        let driver = runner.driver(profile)?;
        let config = Self::driver_config(profile)?;
        runner.auth_registry(profile)?;
        driver.validate_config(&config)
    }

    pub(crate) fn validate_profile_with_registries(
        profile: &MissionRuntimeProfile,
        drivers: RuntimeDriverRegistry,
        auth: RuntimeAuthRegistry,
    ) -> anyhow::Result<()> {
        let runner = Self::with_registries(
            RuntimeProfiles::single(profile.clone()),
            String::new(),
            AuthorityCeiling::default(),
            drivers,
            auth,
        );
        let driver = runner.driver(profile)?;
        let config = Self::driver_config(profile)?;
        runner.auth_registry(profile)?;
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
    observer_index: &lionclaw_durable_fs::RootedDirectory,
    base_sha: &str,
    recreate_workspace: bool,
) -> Result<(), TypedFailure> {
    let mut replace = !workspace.exists();
    if workspace.exists() {
        let head = workspace::checkout_head_sha(workspace)
            .await
            .map_err(|e| launch(format!("failed to inspect retained checkout HEAD: {e}")))?;
        if !recreate_workspace {
            if workspace::checkout_commit_exists(workspace, base_sha).await
                && workspace::checkout_is_ancestor(workspace, base_sha, &head)
                    .await
                    .map_err(|e| {
                        launch(format!("failed to compare retained checkout ancestry: {e}"))
                    })?
            {
                workspace::prepare_checkout_observer_index(repo, observer_index, base_sha, false)
                    .await
                    .map_err(|e| launch(format!("failed to prepare workspace observer: {e}")))?;
                return Ok(());
            }
            return Err(launch(format!(
                "retained conversation checkout HEAD {head} does not descend from its recorded base {base_sha}"
            )));
        }
        if head == base_sha {
            replace = false;
        } else {
            if workspace::checkout_is_dirty(workspace)
                .await
                .map_err(|e| launch(format!("failed to inspect retained checkout: {e}")))?
            {
                return Err(launch(
                    "refusing to recreate a dirty conversation checkout on a moved base"
                        .to_string(),
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
                    "refusing to recreate conversation checkout with uncaptured commits at {head}"
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
    workspace::prepare_checkout_observer_index(
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
        if let Some(failure) = setup_control_failure(&profile, &request.control.borrow().clone()) {
            return Err(failure);
        }
        let mission_dirs = MissionDirs::new(&request.state_dir, &request.mission_id);
        let dirs = mission_dirs.effect(&request.effect_id).role();
        let lifetime = request.role.output.resource_lifetime();
        let (role_state, state_observer_index) = match lifetime {
            RoleResourceLifetime::Conversation => {
                let conversation_id = crate::model::ConversationId::for_role_instance(
                    &request.mission_id,
                    request.namespace,
                    &request.task_id,
                    &request.role.name,
                    request.assignment_epoch,
                );
                let conversation = mission_dirs.conversation(&conversation_id);
                (
                    conversation.role_state().clone(),
                    Some(conversation.files().map_err(|error| {
                        launch(format!(
                            "invalid conversation resource authority: {error:#}"
                        ))
                    })?),
                )
            }
            RoleResourceLifetime::Effect => (dirs.role_state().clone(), None),
        };
        dirs.prepare()
            .map_err(|e| launch(format!("failed to prepare role effect dirs: {e}")))?;
        role_state
            .prepare()
            .map_err(|e| launch(format!("failed to prepare role state dirs: {e}")))?;

        let setup = async {
            let skill_mounts = prepare_skill_mounts(
                dirs.runtime_home(),
                &request.skills,
                profile.skills_dir.as_ref(),
            )
            .map_err(|err| launch(format!("failed to prepare role skills: {err:#}")))?;
            let authority = compile_authority(&request.role, &self.ceiling)
                .map_err(|e| launch(format!("authority refused to compile: {e}")))?;
            let is_writer = authority.output() == OutputSemantics::ProducesArtifact;
            let (workspace_source, observer_index) = if is_writer {
                debug_assert_eq!(lifetime, RoleResourceLifetime::Conversation);
                let capture = request.artifact_capture.as_ref().ok_or_else(|| {
                    launch("artifact-producing role has no capture authority".into())
                })?;
                if capture.checkout_dir() != role_state.work() {
                    return Err(launch(
                        "artifact capture authority names a different conversation checkout".into(),
                    ));
                }
                (
                    capture.checkout_dir().to_path_buf(),
                    Some(
                        state_observer_index
                            .clone()
                            .expect("writer conversation has an observer index"),
                    ),
                )
            } else {
                if request.artifact_capture.is_some() {
                    return Err(launch(
                        "read-only role received artifact capture authority".into(),
                    ));
                }
                (role_state.work().to_path_buf(), None)
            };
            {
                let _guard = self.repo_lock.lock().await;
                if is_writer {
                    prepare_writer_checkout(
                        &request.workspace_dir,
                        &workspace_source,
                        observer_index.as_ref().expect("writer observer index"),
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
            let mut extras = effect_mounts(&dirs, &role_state);
            extras.extend(skill_mounts);
            let environment = mission_environment(&dirs, &request.environment);
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
        let (is_writer, plan) =
            await_controlled(Box::pin(setup), request.control.clone(), |control| {
                setup_control_failure(&profile, control)
            })
            .await?;

        // The adapter owns cancellation acknowledgement while its turn is
        // live. Setup and capture use the same engine control, but are simply
        // dropped: child processes are kill-on-drop and retained conversation
        // state stays outside disposable effect resources.
        let (applied, final_response) = self.run_turn(&profile, &request, plan).await?;
        let cancellation_configuration = applied.clone();
        let cancellation_response = final_response.clone();
        let finish = async {
            let handoff = read_optional_handoff(dirs.handoff(), request.role.output).map_err(
                |mut failure| {
                    failure.evidence_mut().final_response = final_response.clone();
                    failure.evidence_mut().configuration = applied.clone();
                    failure
                },
            )?;
            // A writer may pause for lead input without handing off. Such a
            // dialogue checkpoint publishes neither an artifact nor capture
            // authority; the same conversation workspace remains available
            // for the next turn.
            let artifact = if is_writer && handoff.is_some() {
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
        await_controlled(Box::pin(finish), request.control.clone(), |control| {
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
        self.run_turn_with_context(profile, request, plan, |plan| {
            mission_execution_context(plan, &request.state_dir)
        })
        .await
    }

    async fn run_turn_with_context(
        &self,
        profile: &MissionRuntimeProfile,
        request: &RoleRunRequest,
        plan: lionclaw_confinement::EffectiveExecutionPlan,
        context_builder: impl Fn(
            &lionclaw_confinement::EffectiveExecutionPlan,
        )
            -> anyhow::Result<lionclaw_runtime_api::RuntimeExecutionContext>,
    ) -> Result<(lionclaw_runtime_api::AppliedRuntimeConfiguration, String), TypedFailure> {
        let driver = self
            .driver(profile)
            .map_err(|err| launch(format!("runtime profile invalid: {err:#}")))?;
        let config = Self::driver_config(profile)
            .map_err(|err| launch(format!("runtime profile invalid: {err:#}")))?;
        let auth_registry = self
            .auth_registry(profile)
            .map_err(|err| launch(format!("runtime profile invalid: {err:#}")))?;
        driver
            .validate_config(&config)
            .map_err(|e| launch(format!("driver config invalid: {e}")))?;
        let adapter = driver.create_adapter(config);

        // Compute the fallible execution context *before* opening a session,
        // so a failure here cannot leak a started session.
        let context =
            context_builder(&plan).map_err(|e| launch(format!("execution context failed: {e}")))?;

        let runtime_state = profile
            .native_resume
            .then(|| context.runtime_state.clone())
            .flatten();
        let runtime_session_ready = runtime_state
            .as_ref()
            .map(RuntimeSessionReady::from_state_dir)
            .transpose()
            .map_err(|e| launch(format!("native session state invalid: {e}")))?
            .unwrap_or_else(RuntimeSessionReady::not_ready);
        let start = async {
            adapter
                .session_start(RuntimeSessionStartInput {
                    session_id: uuid_from_key(request.effect_id.as_str()),
                    working_dir: Some(WORKSPACE_MOUNT_TARGET.to_string()),
                    environment: plan.environment.clone(),
                    resume: match runtime_state.clone() {
                        Some(state) => RuntimeResume::Native {
                            state,
                            ready: runtime_session_ready,
                        },
                        None => RuntimeResume::Reconstruct,
                    },
                })
                .await
                .map_err(|e| launch(format!("session_start failed: {e}")))
        };
        let handle = await_controlled(Box::pin(start), request.control.clone(), |control| {
            setup_control_failure(profile, control)
        })
        .await?;
        let mut resume_mode = handle.resume_mode;
        let (mut result, mut fallback_final_response) = execute_turn_attempt(
            Arc::clone(&adapter),
            &handle,
            profile,
            request,
            context,
            plan.clone(),
            auth_registry.clone(),
        )
        .await;

        let recoverable_reopen = match &result {
            Err(failure) => {
                adapter.native_reopen_outcome(&handle, failure)
                    == RuntimeNativeReopenOutcome::Recoverable
            }
            Ok(_) => false,
        };
        if recoverable_reopen
            && resume_mode == RuntimeResumeMode::Resumed
            && adapter.native_reopen_recovery() == RuntimeNativeReopenRecovery::ForgetAndReconstruct
        {
            let reopen_failure = match result {
                Err(failure) => failure,
                Ok(_) => unreachable!("recoverable reopen requires a failed turn"),
            };
            if let Err(error) = adapter.forget_native_reopen(&handle).await {
                let _ = adapter.close(&handle).await;
                return Err(recovery_failure(
                    &reopen_failure,
                    "forget",
                    &error.to_string(),
                ));
            }
            let _ = adapter.close(&handle).await;
            let reconstruction = adapter
                .session_start(RuntimeSessionStartInput {
                    session_id: uuid_from_key(request.effect_id.as_str()),
                    working_dir: Some(WORKSPACE_MOUNT_TARGET.to_string()),
                    environment: plan.environment.clone(),
                    resume: match runtime_state.clone() {
                        Some(state) => RuntimeResume::Native {
                            state,
                            ready: RuntimeSessionReady::not_ready(),
                        },
                        None => RuntimeResume::Reconstruct,
                    },
                })
                .await
                .map_err(|error| {
                    recovery_failure(&reopen_failure, "reconstruction_start", &error.to_string())
                })?;
            resume_mode = reconstruction.resume_mode;
            let reconstruction_context = match context_builder(&plan) {
                Ok(context) => context,
                Err(error) => {
                    let _ = adapter.close(&reconstruction).await;
                    return Err(recovery_failure(
                        &reopen_failure,
                        "reconstruction_context",
                        &error.to_string(),
                    ));
                }
            };
            let (reconstructed, reconstructed_response) = execute_turn_attempt(
                Arc::clone(&adapter),
                &reconstruction,
                profile,
                request,
                reconstruction_context,
                plan.clone(),
                auth_registry,
            )
            .await;
            let _ = adapter.close(&reconstruction).await;
            fallback_final_response = reconstructed_response;
            result =
                reconstructed.map_err(|failure| double_recovery_failure(&reopen_failure, &failure));
        } else {
            let _ = adapter.close(&handle).await;
        }

        // Native identity is conversation state, not successful-effect state.
        // Once a turn has launched, retain whatever opaque identity the
        // adapter durably recorded even when the delivered outcome is a
        // failure, interruption, or deadline. Adapters with no saved identity
        // truthfully reconstruct on the next request.
        if let Some(state) = runtime_state {
            lionclaw_runtime_api::record_runtime_resume_mode(&state, resume_mode)
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

async fn execute_turn_attempt(
    adapter: Arc<dyn RuntimeAdapter>,
    handle: &RuntimeSessionHandle,
    profile: &MissionRuntimeProfile,
    request: &RoleRunRequest,
    context: lionclaw_runtime_api::RuntimeExecutionContext,
    plan: lionclaw_confinement::EffectiveExecutionPlan,
    auth_registry: RuntimeAuthRegistry,
) -> (
    Result<lionclaw_runtime_api::TurnResult, TypedFailure>,
    String,
) {
    let (journal_tx, journal_rx) =
        tokio::sync::mpsc::channel(lionclaw_runtime_api::RUNTIME_TURN_JOURNAL_CAPACITY);
    let drain = tokio::spawn(drain_runtime_journal(
        journal_rx,
        request.updates.clone(),
        request.activity.clone(),
        request.effect_id.clone(),
    ));
    let mut turn = Box::pin(adapter.turn(
        TurnExecution {
            input: TurnInput {
                runtime_session_id: handle.runtime_session_id.clone(),
                prompt: request.prompt.clone(),
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
        match control.borrow().clone() {
            ExecutionControl::RunUntil(_) => {}
            ExecutionControl::DeadlineExhausted => {
                break TurnEnd::Cancel {
                    reason: "effect deadline exhausted".into(),
                    kind: CancellationKind::Deadline,
                }
            }
            ExecutionControl::Stop(reason) => {
                break TurnEnd::Cancel {
                    reason,
                    kind: CancellationKind::Stop,
                }
            }
            ExecutionControl::Abort(reason) => {
                break TurnEnd::Cancel {
                    reason,
                    kind: CancellationKind::Abort,
                }
            }
        }
        tokio::select! {
            biased;
            changed = control.changed() => if changed.is_err() { continue; },
            completed = &mut turn => break TurnEnd::Completed(completed),
        }
    };
    let result = match end {
        TurnEnd::Completed(completed) => completed.map_err(|error| {
            error
                .downcast_ref::<TypedFailure>()
                .cloned()
                .unwrap_or_else(|| TypedFailure::permanent("runtime.unknown", error.to_string()))
        }),
        TurnEnd::Cancel { reason, kind } => {
            let (acknowledged, completed) = cancellation_acknowledged(
                adapter.cancel(handle, Some(reason.clone())),
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
    (result, drain.await.unwrap_or_default())
}

fn recovery_failure(first: &TypedFailure, stage: &str, detail: &str) -> TypedFailure {
    TypedFailure::permanent(
        "runtime.native_reopen_recovery",
        format!("native_reopen={}; {stage}={detail}", first.detail()),
    )
    .projected()
}

fn double_recovery_failure(first: &TypedFailure, second: &TypedFailure) -> TypedFailure {
    let first_prefix = format!("native_reopen[{}]=", first.category());
    let second_prefix = format!("; canonical_reconstruction[{}]=", second.category());
    let detail_budget = lionclaw_runtime_api::FAILURE_TEXT_LIMIT
        .saturating_sub(first_prefix.len() + second_prefix.len());
    let first_detail = bounded_stage_detail(first.detail(), detail_budget / 2);
    let second_detail = bounded_stage_detail(second.detail(), detail_budget - first_detail.len());
    TypedFailure::permanent(
        "runtime.native_reopen_reconstruction_failed",
        format!("{first_prefix}{first_detail}{second_prefix}{second_detail}"),
    )
    .projected()
}

fn bounded_stage_detail(detail: &str, limit: usize) -> String {
    const MARKER: &str = "...[truncated]";
    if detail.len() <= limit {
        return detail.to_string();
    }
    let mut cut = limit.saturating_sub(MARKER.len());
    while !detail.is_char_boundary(cut) {
        cut = cut.saturating_sub(1);
    }
    format!("{}{MARKER}", &detail[..cut])
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

/// Kernel-owned execution coordinates for a role, composed with the pinned
/// mission type's domain environment.
fn mission_environment(
    dirs: &crate::resources::RoleEffectDirs,
    declared: &std::collections::BTreeMap<String, String>,
) -> Vec<(String, String)> {
    let home = lionclaw_confinement::RUNTIME_HOME_MOUNT_TARGET;
    crate::mission_type::execution_environment(
        [
            ("HOME".to_string(), home.to_string()),
            ("XDG_CONFIG_HOME".to_string(), format!("{home}/.config")),
            ("XDG_CACHE_HOME".to_string(), format!("{home}/.cache")),
            ("XDG_DATA_HOME".to_string(), format!("{home}/.local/share")),
            ("XDG_STATE_HOME".to_string(), format!("{home}/.local/state")),
            ("TMPDIR".to_string(), "/tmp".to_string()),
            ("GIT_OPTIONAL_LOCKS".to_string(), "0".to_string()),
            (
                "LIONCLAW_WORKSPACE_DIR".to_string(),
                WORKSPACE_MOUNT_TARGET.to_string(),
            ),
            (
                "MISSION_EFFECT".to_string(),
                dirs.root().to_string_lossy().into_owned(),
            ),
        ],
        declared,
        std::iter::empty(),
    )
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
    use crate::ports::{EffectCleaner, EffectCleanupRequest};
    use crate::resources::MissionDirs;
    use lionclaw_confinement::{
        ConfinementConfig, EffectiveExecutionPlan, ExecutionLimits, InstallPolicy, MountAccess,
        MountSpec, NetworkMode, OciConfinementConfig, WorkspaceAccess,
    };
    use lionclaw_runtime_api::{
        RuntimeAdapterInfo, RuntimeCancellation, RuntimeDriverProvider, RuntimeTurnJournalSender,
        TurnResult,
    };
    use std::collections::{BTreeMap, BTreeSet, VecDeque};
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::sync::Mutex as StdMutex;

    #[derive(Debug, Clone, Copy)]
    enum FallbackTurn {
        Stale,
        Success,
        Failure,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum FallbackFailurePoint {
        None,
        ReconstructionStart,
        ReconstructionContext,
    }

    #[derive(Debug, Default)]
    struct FallbackObservations {
        starts: Vec<(bool, bool)>,
        prompts: Vec<String>,
        events: Vec<String>,
        closed: Vec<String>,
        drained: Vec<String>,
        native_identity: Option<String>,
        identity_before_reconstructed_success: Option<String>,
    }

    struct FallbackAdapter {
        turns: StdMutex<VecDeque<FallbackTurn>>,
        observations: Arc<StdMutex<FallbackObservations>>,
        reopen_outcome: RuntimeNativeReopenOutcome,
        failure_point: FallbackFailurePoint,
    }

    #[async_trait]
    impl RuntimeAdapter for FallbackAdapter {
        async fn info(&self) -> RuntimeAdapterInfo {
            RuntimeAdapterInfo {
                id: "fallback-boundary".into(),
                version: "1".into(),
                healthy: true,
            }
        }

        fn native_reopen_recovery(&self) -> RuntimeNativeReopenRecovery {
            RuntimeNativeReopenRecovery::ForgetAndReconstruct
        }

        fn native_reopen_outcome(
            &self,
            _handle: &RuntimeSessionHandle,
            _failure: &TypedFailure,
        ) -> RuntimeNativeReopenOutcome {
            self.reopen_outcome
        }

        async fn forget_native_reopen(&self, handle: &RuntimeSessionHandle) -> anyhow::Result<()> {
            let mut observations = self.observations.lock().unwrap();
            observations.native_identity = None;
            observations
                .events
                .push(format!("forget:{}", handle.runtime_session_id));
            Ok(())
        }

        async fn session_start(
            &self,
            input: RuntimeSessionStartInput,
        ) -> anyhow::Result<RuntimeSessionHandle> {
            let ready = matches!(
                &input.resume,
                RuntimeResume::Native { ready, .. } if ready.is_ready()
            );
            let mut observations = self.observations.lock().unwrap();
            let ordinal = observations.starts.len() + 1;
            observations.starts.push((ready, !ready));
            observations.events.push(format!("start:{ordinal}"));
            if ordinal == 2 && self.failure_point == FallbackFailurePoint::ReconstructionStart {
                anyhow::bail!("scripted reconstruction session start failure");
            }
            Ok(RuntimeSessionHandle {
                runtime_session_id: format!("session-{ordinal}"),
                resume_mode: if ready {
                    RuntimeResumeMode::Resumed
                } else {
                    RuntimeResumeMode::Reconstructed
                },
            })
        }

        async fn turn(
            &self,
            execution: TurnExecution,
            journal: RuntimeTurnJournalSender,
        ) -> anyhow::Result<TurnResult> {
            let turn = self
                .turns
                .lock()
                .unwrap()
                .pop_front()
                .expect("no third turn");
            {
                let mut observations = self.observations.lock().unwrap();
                if matches!(turn, FallbackTurn::Success)
                    && execution.input.runtime_session_id == "session-2"
                {
                    observations.identity_before_reconstructed_success =
                        observations.native_identity.clone();
                    observations.native_identity = Some("native-thread-2".into());
                }
                observations.prompts.push(execution.input.prompt);
                observations
                    .events
                    .push(format!("turn:{}", execution.input.runtime_session_id));
            }
            journal
                .send(lionclaw_runtime_api::TurnEvent::canonical(
                    lionclaw_runtime_api::RuntimeEvent::MessageDelta {
                        lane: lionclaw_runtime_api::RuntimeMessageLane::Answer,
                        text: format!("journal-{turn:?}"),
                    },
                ))
                .await?;
            let configuration = lionclaw_runtime_api::AppliedRuntimeConfiguration {
                requested_model: Some(format!("journal-{turn:?}")),
                ..Default::default()
            };
            journal
                .send(lionclaw_runtime_api::TurnEvent::canonical(
                    lionclaw_runtime_api::RuntimeEvent::Configuration { configuration },
                ))
                .await?;
            match turn {
                FallbackTurn::Stale => Err(anyhow::Error::new(TypedFailure::transient(
                    "codex.thread_rollout",
                    "stale native thread",
                    None,
                ))),
                FallbackTurn::Success => Ok(TurnResult {
                    final_response: "authoritative reconstructed completion".into(),
                    ..Default::default()
                }),
                FallbackTurn::Failure => Err(anyhow::Error::new(TypedFailure::permanent(
                    "codex.process_exit",
                    "reconstruction failed ".repeat(lionclaw_runtime_api::FAILURE_TEXT_LIMIT),
                ))),
            }
        }

        async fn cancel(
            &self,
            _handle: &RuntimeSessionHandle,
            _reason: Option<String>,
        ) -> anyhow::Result<RuntimeCancellation> {
            Ok(RuntimeCancellation::Acknowledged)
        }

        async fn close(&self, handle: &RuntimeSessionHandle) -> anyhow::Result<()> {
            self.observations
                .lock()
                .unwrap()
                .closed
                .push(handle.runtime_session_id.clone());
            Ok(())
        }
    }

    struct FallbackProvider {
        turns: Vec<FallbackTurn>,
        observations: Arc<StdMutex<FallbackObservations>>,
        reopen_outcome: RuntimeNativeReopenOutcome,
        failure_point: FallbackFailurePoint,
    }

    impl RuntimeDriverProvider for FallbackProvider {
        fn driver(&self) -> &'static str {
            "codex"
        }

        fn create_adapter(&self, _config: RuntimeDriverConfig) -> Arc<dyn RuntimeAdapter> {
            Arc::new(FallbackAdapter {
                turns: StdMutex::new(self.turns.iter().copied().collect()),
                observations: self.observations.clone(),
                reopen_outcome: self.reopen_outcome,
                failure_point: self.failure_point,
            })
        }
    }

    fn fallback_plan(temp: &Path) -> EffectiveExecutionPlan {
        EffectiveExecutionPlan {
            runtime_id: "codex".into(),
            preset_name: "test".into(),
            confinement: ConfinementConfig::Oci(OciConfinementConfig::default()),
            workspace_access: WorkspaceAccess::ReadOnly,
            network_mode: NetworkMode::None,
            install_policy: InstallPolicy::None,
            root_in_userns: false,
            working_dir: Some(temp.join("workspace").to_string_lossy().into_owned()),
            environment: vec![("CANONICAL_DIALOGUE".into(), "folded-message-1".into())],
            mcp_servers: Vec::new(),
            mounts: vec![
                MountSpec {
                    source: temp.join("workspace"),
                    target: WORKSPACE_MOUNT_TARGET.into(),
                    access: MountAccess::ReadOnly,
                },
                MountSpec {
                    source: temp.join("runtime"),
                    target: lionclaw_confinement::RUNTIME_MOUNT_TARGET.into(),
                    access: MountAccess::ReadWrite,
                },
            ],
            mount_runtime_secrets: false,
            escape_classes: BTreeSet::new(),
            limits: ExecutionLimits::default(),
        }
    }

    fn fallback_request(temp: &Path) -> RoleRunRequest {
        let (control_tx, control) =
            tokio::sync::watch::channel(ExecutionControl::RunUntil(i64::MAX));
        let _control_tx = Box::leak(Box::new(control_tx));
        let (updates, _updates_rx) = tokio::sync::mpsc::channel(8);
        let (activity, _activity_rx) = tokio::sync::watch::channel(None);
        RoleRunRequest {
            mission_id: crate::model::MissionId::for_creation("/workspace", "fallback", 1),
            namespace: crate::model::TaskNamespace::Execution,
            task_id: crate::model::TaskId::new("fallback-boundary").unwrap(),
            attempt_no: 1,
            effect_id: crate::model::EffectId::for_parts(&["fallback-boundary"]),
            role: crate::mission_type::RoleDefinition {
                name: crate::model::RoleName::new("implementer").unwrap(),
                output: OutputSemantics::ProducesArtifact,
                runtime: Some("codex".into()),
                timeout_secs: None,
                network: false,
                secrets: false,
                skills: Vec::new(),
                prompt_body: String::new(),
            },
            environment: Default::default(),
            runtime: "codex".into(),
            skills: Vec::new(),
            prompt: "canonical current prompt\nfolded user message\nfolded assistant message"
                .into(),
            base_sha: "unused".into(),
            assignment_epoch: 1,
            recreate_workspace: false,
            deadline_ms: i64::MAX,
            control,
            updates,
            activity,
            workspace_dir: temp.join("workspace"),
            state_dir: temp.join("state"),
            artifact_capture: None,
        }
    }

    #[test]
    fn role_environment_composes_domain_policy_without_kernel_tool_assumptions() {
        let temp = tempfile::tempdir().unwrap();
        let mission_id = crate::model::MissionId::for_creation("/workspace", "environment", 1);
        let effect_id = crate::model::EffectId::for_parts(&["environment"]);
        let dirs = crate::resources::MissionDirs::new(temp.path(), &mission_id)
            .effect(&effect_id)
            .role();
        let environment = BTreeMap::from_iter(mission_environment(
            &dirs,
            &BTreeMap::from([("BUILD_OUTPUT".to_string(), "/scratch/build".to_string())]),
        ));

        assert_eq!(environment["HOME"], "/runtime/home");
        assert_eq!(environment["BUILD_OUTPUT"], "/scratch/build");
        assert_eq!(environment["MISSION_EFFECT"], dirs.root().to_string_lossy());
    }

    #[tokio::test]
    async fn durable_control_precedes_role_resource_setup() {
        let temp = tempfile::tempdir().unwrap();
        let runner = OciRoleRunner::new(
            RuntimeProfiles::built_in().unwrap(),
            "unused-image".into(),
            AuthorityCeiling::default(),
        );
        for (control, expected) in [
            (ExecutionControl::Stop("stop".into()), "stop"),
            (ExecutionControl::Abort("abort".into()), "abort"),
            (ExecutionControl::DeadlineExhausted, "deadline"),
        ] {
            let mut request = fallback_request(temp.path());
            request.control = tokio::sync::watch::channel(control).1;
            let failure = runner
                .run(request)
                .await
                .expect_err("durable control must win before missing state resources");
            assert!(
                matches!(
                    (&failure, expected),
                    (TypedFailure::OperatorStopped { .. }, "stop")
                        | (TypedFailure::OperatorAborted { .. }, "abort")
                        | (TypedFailure::DeadlineExhausted { .. }, "deadline")
                ),
                "expected {expected}, got {failure:?}"
            );
        }
        assert!(!temp.path().join("state").exists());
    }

    async fn run_fallback_boundary(
        turns: Vec<FallbackTurn>,
        reopen_outcome: RuntimeNativeReopenOutcome,
        failure_point: FallbackFailurePoint,
    ) -> (
        Result<(lionclaw_runtime_api::AppliedRuntimeConfiguration, String), TypedFailure>,
        Arc<StdMutex<FallbackObservations>>,
        tempfile::TempDir,
    ) {
        let temp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(temp.path().join("workspace")).unwrap();
        std::fs::create_dir_all(temp.path().join("runtime")).unwrap();
        let runtime_state =
            lionclaw_runtime_api::RuntimeStateDir::new(temp.path(), temp.path().join("runtime"))
                .unwrap();
        lionclaw_runtime_api::record_runtime_resume_mode(
            &runtime_state,
            RuntimeResumeMode::Resumed,
        )
        .unwrap();
        let observations = Arc::new(StdMutex::new(FallbackObservations {
            native_identity: Some("stale-native-thread-1".into()),
            ..FallbackObservations::default()
        }));
        let profiles = RuntimeProfiles::from_toml(
            "[runtimes.codex]\ndriver = \"codex\"\ncommand = \"codex\"\nnative-resume = true\n",
            temp.path(),
        )
        .unwrap();
        let runner = OciRoleRunner::with_registries(
            profiles,
            "unused-test-image".into(),
            AuthorityCeiling::default(),
            RuntimeDriverRegistry::new([Arc::new(FallbackProvider {
                turns,
                observations: observations.clone(),
                reopen_outcome,
                failure_point,
            }) as Arc<dyn RuntimeDriverProvider>]),
            RuntimeAuthRegistry::empty(),
        );
        let profile = runner.profile("codex").unwrap();
        let mut request = fallback_request(temp.path());
        let (updates, mut updates_rx) = tokio::sync::mpsc::channel(8);
        request.updates = updates;
        let drain_observations = observations.clone();
        let drain_observer = tokio::spawn(async move {
            while let Some(update) = updates_rx.recv().await {
                if let crate::ports::RoleRunUpdate::RuntimeConfigured(configuration) = update {
                    if let Some(observation) = configuration.requested_model {
                        drain_observations.lock().unwrap().drained.push(observation);
                    }
                }
            }
        });
        let context_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls = context_calls.clone();
        let state_anchor = temp.path().to_path_buf();
        let result = runner
            .run_turn_with_context(
                &profile,
                &request,
                fallback_plan(temp.path()),
                move |plan| {
                    let call = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if failure_point == FallbackFailurePoint::ReconstructionContext && call == 1 {
                        anyhow::bail!("scripted reconstruction context failure");
                    }
                    mission_execution_context(plan, &state_anchor)
                },
            )
            .await;
        drop(request);
        drain_observer.await.unwrap();
        (result, observations, temp)
    }

    #[tokio::test]
    async fn production_runner_recovers_one_stale_reopen_with_canonical_reconstruction() {
        let (result, observations, temp) = run_fallback_boundary(
            vec![FallbackTurn::Stale, FallbackTurn::Success],
            RuntimeNativeReopenOutcome::Recoverable,
            FallbackFailurePoint::None,
        )
        .await;
        assert_eq!(result.unwrap().1, "authoritative reconstructed completion");
        let observations = observations.lock().unwrap();
        assert_eq!(observations.starts, vec![(true, false), (false, true)]);
        assert_eq!(observations.prompts.len(), 2);
        assert_eq!(observations.prompts[0], observations.prompts[1]);
        assert_eq!(
            observations.prompts[0],
            "canonical current prompt\nfolded user message\nfolded assistant message"
        );
        assert_eq!(
            observations.events,
            vec![
                "start:1",
                "turn:session-1",
                "forget:session-1",
                "start:2",
                "turn:session-2"
            ]
        );
        assert_eq!(observations.closed, vec!["session-1", "session-2"]);
        assert_eq!(
            observations.drained,
            vec!["journal-Stale", "journal-Success"]
        );
        assert_eq!(observations.identity_before_reconstructed_success, None);
        assert_eq!(
            observations.native_identity.as_deref(),
            Some("native-thread-2")
        );
        drop(observations);
        assert!(
            lionclaw_runtime_api::RuntimeSessionReady::from_state_dir(
                &lionclaw_runtime_api::RuntimeStateDir::new(
                    temp.path(),
                    temp.path().join("runtime"),
                )
                .unwrap(),
            )
            .unwrap()
            .is_ready(),
            "the reconstructed observation must be eligible for native resume"
        );
    }

    #[tokio::test]
    async fn production_runner_bounds_failed_reconstruction_and_settles_both_attempts() {
        let (result, observations, _temp) = run_fallback_boundary(
            vec![FallbackTurn::Stale, FallbackTurn::Failure],
            RuntimeNativeReopenOutcome::Recoverable,
            FallbackFailurePoint::None,
        )
        .await;
        let failure = result.unwrap_err();
        assert_eq!(
            failure.evidence().code.as_deref(),
            Some("runtime.native_reopen_reconstruction_failed")
        );
        assert!(failure
            .detail()
            .contains("native_reopen[transient_runtime]="));
        assert!(failure
            .detail()
            .contains("canonical_reconstruction[permanent_runtime]="));
        assert!(failure.detail().len() <= lionclaw_runtime_api::FAILURE_TEXT_LIMIT);
        let observations = observations.lock().unwrap();
        assert_eq!(observations.starts.len(), 2, "no third session attempt");
        assert_eq!(observations.prompts.len(), 2, "no third turn attempt");
        assert_eq!(observations.closed, vec!["session-1", "session-2"]);
        assert_eq!(
            observations.drained,
            vec!["journal-Stale", "journal-Failure"]
        );
    }

    #[tokio::test]
    async fn production_runner_settles_original_handle_when_reconstruction_start_fails() {
        let (result, observations, _temp) = run_fallback_boundary(
            vec![FallbackTurn::Stale],
            RuntimeNativeReopenOutcome::Recoverable,
            FallbackFailurePoint::ReconstructionStart,
        )
        .await;
        let failure = result.unwrap_err();
        assert_eq!(
            failure.evidence().code.as_deref(),
            Some("runtime.native_reopen_recovery")
        );
        assert!(failure.detail().contains("reconstruction_start="));
        let observations = observations.lock().unwrap();
        assert_eq!(observations.starts.len(), 2);
        assert_eq!(
            observations.prompts.len(),
            1,
            "failed start cannot launch a turn"
        );
        assert_eq!(observations.closed, vec!["session-1"]);
        assert_eq!(observations.drained, vec!["journal-Stale"]);
    }

    #[tokio::test]
    async fn production_runner_closes_reconstruction_when_context_setup_fails() {
        let (result, observations, _temp) = run_fallback_boundary(
            vec![FallbackTurn::Stale],
            RuntimeNativeReopenOutcome::Recoverable,
            FallbackFailurePoint::ReconstructionContext,
        )
        .await;
        let failure = result.unwrap_err();
        assert!(failure.detail().contains("reconstruction_context="));
        let observations = observations.lock().unwrap();
        assert_eq!(observations.starts.len(), 2);
        assert_eq!(observations.prompts.len(), 1);
        assert_eq!(observations.closed, vec!["session-1", "session-2"]);
        assert_eq!(observations.drained, vec!["journal-Stale"]);
    }

    #[tokio::test]
    async fn production_runner_settles_non_recoverable_original_turn_failure() {
        let (result, observations, _temp) = run_fallback_boundary(
            vec![FallbackTurn::Failure],
            RuntimeNativeReopenOutcome::NotReopenFailure,
            FallbackFailurePoint::None,
        )
        .await;
        assert!(result.is_err());
        let observations = observations.lock().unwrap();
        assert_eq!(observations.starts.len(), 1);
        assert_eq!(observations.closed, vec!["session-1"]);
        assert_eq!(observations.drained, vec!["journal-Failure"]);
    }

    #[test]
    fn double_reopen_failure_evidence_is_exact_and_bounded() {
        let reopen = TypedFailure::transient(
            "codex.thread_rollout",
            "y".repeat(lionclaw_runtime_api::FAILURE_TEXT_LIMIT * 2),
            None,
        );
        let reconstruction = TypedFailure::permanent(
            "codex.process_exit",
            "x".repeat(lionclaw_runtime_api::FAILURE_TEXT_LIMIT * 2),
        );

        let failure = double_recovery_failure(&reopen, &reconstruction);

        assert_eq!(
            failure.evidence().code.as_deref(),
            Some("runtime.native_reopen_reconstruction_failed")
        );
        assert!(failure
            .detail()
            .starts_with("native_reopen[transient_runtime]="));
        assert!(failure
            .detail()
            .contains("; canonical_reconstruction[permanent_runtime]="));
        assert!(failure.detail().len() <= lionclaw_runtime_api::FAILURE_TEXT_LIMIT);
        assert!(!failure.detail().contains("third"));
    }

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

    #[tokio::test]
    async fn codex_profile_runner_and_cleaner_keep_only_conversation_state_for_every_outcome() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        let profiles = RuntimeProfiles::built_in().unwrap();
        let runner = OciRoleRunner::new(
            profiles,
            "slice4-test-image".into(),
            AuthorityCeiling::default(),
        );
        let profile = runner.profile("codex").expect("resolve codex profile");
        assert_eq!(profile.name, "codex");
        assert_eq!(profile.driver, "codex");
        assert!(profile.native_resume);
        OciRoleRunner::validate_profile(&profile).expect("construct codex driver");

        let mission_id = crate::model::MissionId::for_creation("/workspace", "slice4", 1);
        let conversation_id = crate::model::ConversationId::for_role_instance(
            &mission_id,
            crate::model::TaskNamespace::Execution,
            &crate::model::TaskId::new("runtime-boundary").unwrap(),
            &crate::model::RoleName::new("implementer").unwrap(),
            1,
        );
        let mission_dirs = MissionDirs::new(temp.path(), &mission_id);
        let conversation = mission_dirs.conversation(&conversation_id);
        conversation.role_state().prepare().unwrap();
        std::fs::create_dir_all(conversation.work()).unwrap();
        std::fs::write(conversation.runtime().join("opaque-session"), b"private").unwrap();
        std::fs::write(conversation.work().join("workspace-identity"), b"stable").unwrap();

        let engine = temp.path().join("fake-oci");
        std::fs::write(&engine, "#!/bin/sh\nexit 0\n").unwrap();
        let mut permissions = std::fs::metadata(&engine).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&engine, permissions).unwrap();
        let cleaner =
            crate::effect_cleanup::LocalEffectCleaner::new(engine.to_string_lossy().into_owned());

        for outcome in [
            "success",
            "role-failure",
            "retry",
            "interruption",
            "deadline",
            "stop",
            "continue",
            "restart",
        ] {
            // Re-open from the durable identity on every iteration, as a new
            // engine process does after loading the store. Effect identity is
            // deliberately absent from this lookup.
            let reopened = mission_dirs.conversation(&conversation_id);
            reopened.role_state().prepare().unwrap();
            assert_eq!(reopened.work(), conversation.work());
            assert_eq!(reopened.runtime(), conversation.runtime());
            let effect_id = crate::model::EffectId::for_parts(&["outcome", outcome]);
            let effect = mission_dirs.effect(&effect_id).role();
            effect.prepare().unwrap();
            for relative in [
                "handoff/handoff.json",
                "runtime-home/credentials",
                "runtime/authorization",
                "effect-work",
            ] {
                let path = effect.root().join(relative);
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).unwrap();
                }
                std::fs::write(path, b"effect-private").unwrap();
            }
            cleaner
                .cleanup(EffectCleanupRequest {
                    mission_id: mission_id.clone(),
                    effect_id,
                    workspace_dir: workspace.clone(),
                    state_dir: temp.path().to_path_buf(),
                    discard_artifact: false,
                })
                .await
                .unwrap();
            assert!(!effect.root().exists(), "effect leaked after {outcome}");
            assert_eq!(
                std::fs::read(conversation.runtime().join("opaque-session")).unwrap(),
                b"private"
            );
            assert_eq!(
                std::fs::read(conversation.work().join("workspace-identity")).unwrap(),
                b"stable"
            );
        }

        std::fs::write(
            &engine,
            "#!/bin/sh\nmarker=\"${0%/*}/failed-once\"\nif [ ! -e \"$marker\" ]; then touch \"$marker\"; exit 1; fi\nexit 0\n",
        )
        .unwrap();
        let retry_cleaner =
            crate::effect_cleanup::LocalEffectCleaner::new(engine.to_string_lossy().into_owned());
        let retry_effect_id = crate::model::EffectId::for_parts(&["outcome", "cleanup-retry"]);
        let retry_effect = mission_dirs.effect(&retry_effect_id).role();
        retry_effect.prepare().unwrap();
        std::fs::write(retry_effect.runtime_home().join("credential"), b"private").unwrap();
        let retry_request = EffectCleanupRequest {
            mission_id: mission_id.clone(),
            effect_id: retry_effect_id,
            workspace_dir: workspace,
            state_dir: temp.path().to_path_buf(),
            discard_artifact: false,
        };
        retry_cleaner
            .cleanup(retry_request.clone())
            .await
            .expect_err("fault-injected container cleanup must block settlement");
        // Local projections are removed even when container removal fails;
        // the retry is solely for the still-unsettled external cleanup.
        assert!(!retry_effect.root().exists());
        assert!(!retry_effect.runtime_home().join("credential").exists());
        assert!(conversation.runtime().join("opaque-session").is_file());
        retry_cleaner.cleanup(retry_request).await.unwrap();
        assert!(!retry_effect.root().exists());
        assert!(conversation.runtime().join("opaque-session").is_file());
        assert!(conversation.work().join("workspace-identity").is_file());

        // A replacement assignment is a new role instance and therefore
        // cannot inherit the old native identity or conversation checkout.
        let replacement_id = crate::model::ConversationId::for_role_instance(
            &mission_id,
            crate::model::TaskNamespace::Execution,
            &crate::model::TaskId::new("runtime-boundary").unwrap(),
            &crate::model::RoleName::new("implementer").unwrap(),
            2,
        );
        assert_ne!(replacement_id, conversation_id);
        let replacement = mission_dirs.conversation(&replacement_id);
        replacement.role_state().prepare().unwrap();
        assert!(!replacement.runtime().join("opaque-session").exists());
        assert!(!replacement.work().join("workspace-identity").exists());
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
        let observer_root = task_work.parent().expect("test task work has a parent");
        let observer_index =
            lionclaw_durable_fs::RootedDirectory::new(observer_root, observer_root).unwrap();
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
        assert_eq!(
            workspace::checkout_head_sha(&task_work).await.unwrap(),
            moved
        );
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
        assert_eq!(
            workspace::checkout_head_sha(&task_work).await.unwrap(),
            partial
        );
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
            workspace::checkout_head_sha(&task_work).await.unwrap(),
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
        assert_eq!(
            workspace::checkout_head_sha(&task_work).await.unwrap(),
            base
        );
    }
}
