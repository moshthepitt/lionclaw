//! `OciRoleRunner`: the production [`RoleRunner`]. One dispatch =
//! isolate the workspace, compile the plan through the moat, run one full
//! agent turn under confinement, capture the handoff (and, for writers, the
//! resulting commit). The engine owns deadlines; this boundary observes its
//! control channel throughout setup, execution, and capture.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use lionclaw_confinement::WORKSPACE_MOUNT_TARGET;
use lionclaw_runtime_acp::AcpRuntimeDriver;
use lionclaw_runtime_api::{
    RuntimeAdapter, RuntimeAuthContext, RuntimeAuthMaterialization, RuntimeAuthPreparation,
    RuntimeAuthProvider, RuntimeAuthRegistry, RuntimeDriverConfig, RuntimeDriverProvider,
    RuntimeDriverRegistry, RuntimeNativeReopenRecovery, RuntimeNativeSessionObservation,
    RuntimeResume, RuntimeSessionHandle, RuntimeSessionReady, RuntimeSessionStartInput,
    TurnExecution, TurnInput, TypedFailure, TypedFailureEvidence,
};
use lionclaw_runtime_codex::{CodexRuntimeAuthProvider, CodexRuntimeDriver};
use tokio::sync::Mutex;

use crate::authority::{
    compile_authority, compile_role_plan, AuthorityCeiling, MissionMounts, RolePlanRequest,
};
use crate::config::{MissionRuntimeProfile, RuntimeAuthConfig, RuntimeProfiles};
use crate::model::{OutputSemantics, RoleResourceLifetime};
use crate::ports::{ExecutionControl, RoleRunner, RoleTurnOutcome, RoleTurnRequest};
use crate::resources::MissionDirs;

use super::executor::{mission_execution_context, MissionProgramExecutor};
use super::handoff::read_optional_handoff;
use super::native_home_auth::NativeHomeAuthProvider;
use super::{
    await_controlled, effect_mounts, prepare_inputs, prepare_skill_mounts, PreparedInputs,
};
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
    input_lock: Arc<Mutex<()>>,
}

#[derive(Clone)]
struct RuntimeTurnAuth {
    materialization: Option<RuntimeAuthMaterialization>,
    staging_root: PathBuf,
}

impl RuntimeTurnAuth {
    fn identity(&self) -> Option<&str> {
        self.materialization
            .as_ref()
            .map(|auth| auth.identity().as_str())
    }
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
            input_lock: Arc::new(Mutex::new(())),
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

    async fn materialize_runtime_auth(
        &self,
        profile: &MissionRuntimeProfile,
        network_mode: lionclaw_runtime_api::NetworkMode,
        staging_root: PathBuf,
    ) -> anyhow::Result<RuntimeTurnAuth> {
        let registry = self.auth_registry(profile)?;
        let context = RuntimeAuthContext::default();
        let materialization = match &profile.auth {
            None => None,
            Some(auth) => {
                let provider = registry.get_kind(auth.kind()).ok_or_else(|| {
                    anyhow::anyhow!(
                        "validated runtime auth registry lost provider '{}'",
                        auth.kind()
                    )
                })?;
                let materialization = provider
                    .prepare(RuntimeAuthPreparation {
                        runtime_id: &profile.name,
                        network_mode,
                        auth_staging_root: Some(&staging_root),
                        host_context: &context,
                    })
                    .await?;
                if materialization.kind().as_str() != auth.kind() {
                    anyhow::bail!(
                        "runtime '{}' configured auth kind '{}' but provider materialized '{}'",
                        profile.name,
                        auth.kind(),
                        materialization.kind()
                    );
                }
                Some(materialization)
            }
        };
        Ok(RuntimeTurnAuth {
            materialization,
            staging_root,
        })
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
    workspace_preparation: &crate::model::WorkspacePreparation,
    archive_checkout: Option<&std::path::Path>,
) -> Result<(), TypedFailure> {
    if matches!(
        workspace_preparation,
        crate::model::WorkspacePreparation::ArchiveAndReset { .. }
    ) {
        let archive_checkout = archive_checkout
            .ok_or_else(|| launch("workspace recreation has no archive authority".into()))?;
        workspace::archive_and_replace_checkout(repo, workspace, archive_checkout, base_sha)
            .await
            .map_err(|e| launch(format!("failed to archive and recreate checkout: {e}")))?;
        return workspace::prepare_checkout_observer_index(repo, observer_index, base_sha, true)
            .await
            .map_err(|e| launch(format!("failed to prepare workspace observer: {e}")));
    }

    let mut replace = !workspace.exists();
    if workspace.exists() {
        let head = workspace::checkout_head_sha(workspace)
            .await
            .map_err(|e| launch(format!("failed to inspect retained checkout HEAD: {e}")))?;
        if !workspace_preparation.resets_workspace() {
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
        workspace_preparation.resets_workspace() || replace,
    )
    .await
    .map_err(|e| launch(format!("failed to prepare workspace observer: {e}")))
}

#[async_trait]
impl RoleRunner for OciRoleRunner {
    async fn run(&self, request: RoleTurnRequest) -> Result<RoleTurnOutcome, TypedFailure> {
        let profile = self.profile(&request.role.runtime)?;
        if let Some(failure) = setup_control_failure(&profile, &request.control.borrow().clone()) {
            return Err(failure);
        }
        let mission_dirs = MissionDirs::new(&request.state_dir, &request.mission_id);
        let dirs = mission_dirs.effect(&request.effect_id).role();
        let lifetime = request.role.output.resource_lifetime();
        if request.workspace_preparation.resets_workspace()
            && request.role.output != OutputSemantics::ProducesArtifact
        {
            return Err(launch(
                "workspace preparation requires an artifact-producing role".into(),
            ));
        }
        let role_state = match lifetime {
            RoleResourceLifetime::Conversation => {
                let role = mission_dirs.role(&request.role.id);
                role.role_state().clone()
            }
            RoleResourceLifetime::Effect => dirs.role_state().clone(),
        };
        let task_dirs = request
            .task_id
            .as_ref()
            .map(|task_id| mission_dirs.task(task_id));
        let state_observer_index = task_dirs
            .as_ref()
            .map(|task| task.files())
            .transpose()
            .map_err(|error| launch(format!("invalid task resource authority: {error:#}")))?;
        dirs.prepare()
            .map_err(|e| launch(format!("failed to prepare role effect dirs: {e}")))?;
        role_state
            .prepare()
            .map_err(|e| launch(format!("failed to prepare role state dirs: {e}")))?;
        let authority = compile_authority(&request.role, &self.ceiling)
            .map_err(|e| launch(format!("authority refused to compile: {e}")))?;
        let auth = await_controlled(
            Box::pin(async {
                self.materialize_runtime_auth(
                    &profile,
                    authority.preset().network_mode,
                    dirs.auth_staging().to_path_buf(),
                )
                .await
                .map_err(|err| launch(format!("runtime auth materialization is invalid: {err:#}")))
            }),
            request.control.clone(),
            |control| setup_control_failure(&profile, control),
        )
        .await?;
        let native_state_key = profile.native_state_key(auth.identity());
        // No adapter session exists yet. The scheduler serializes role effects,
        // so this metadata walk has no LionClaw-owned runtime writer.
        let admission = role_state
            .admit_runtime_profile_async(native_state_key.clone())
            .await
            .map_err(|e| launch(format!("retained runtime state admission refused: {e:#}")));
        prefer_terminal_control(&profile, &request, admission, None, "")?;
        let runtime_profile = role_state
            .runtime_profile(&native_state_key)
            .map_err(|e| launch(format!("invalid runtime profile resource authority: {e:#}")))?;
        runtime_profile
            .prepare()
            .map_err(|e| launch(format!("failed to prepare runtime profile dirs: {e}")))?;
        let runtime_state = runtime_profile.runtime_state().clone();
        let archive_checkout = request
            .workspace_preparation
            .archived_effect()
            .map(|parked_effect| {
                let task = task_dirs
                    .as_ref()
                    .ok_or_else(|| launch("workspace recreation has no task authority".into()))?;
                task.prepare_workspace_archives()
                    .map_err(|e| launch(format!("failed to prepare workspace archives: {e}")))?;
                let canonical = task.workspace_archive(parked_effect);
                let issued = request
                    .artifact_capture
                    .as_ref()
                    .and_then(crate::workspace::ArtifactCapture::archive_checkout)
                    .ok_or_else(|| {
                        launch("workspace recreation has no archive authority".into())
                    })?;
                if issued != canonical {
                    return Err(launch(
                        "artifact capture archive disagrees with task resources".into(),
                    ));
                }
                Ok(canonical)
            })
            .transpose()?;

        let skill_mounts = prepare_skill_mounts(
            &runtime_profile,
            &request.skills,
            profile.skills_dir.as_ref(),
        )
        .map_err(|err| launch(format!("failed to prepare role skills: {err:#}")))?;
        // Skill mountpoint creation is complete and no checkout, compiled
        // plan, or adapter session has started. Join the blocking accountant
        // before observing control so cancellation cannot detach it into
        // cleanup.
        let prelaunch = role_state
            .assess_runtime_retention_async()
            .await
            .map_err(|error| {
                launch(format!(
                    "retained runtime state prelaunch check refused: {error:#}"
                ))
            });
        prefer_terminal_control(&profile, &request, prelaunch, None, "")?;

        let setup = async {
            let is_writer = authority.output() == OutputSemantics::ProducesArtifact;
            let (workspace_source, observer_index) = if is_writer {
                debug_assert_eq!(lifetime, RoleResourceLifetime::Conversation);
                let capture = request.artifact_capture.as_ref().ok_or_else(|| {
                    launch("artifact-producing role has no capture authority".into())
                })?;
                let task = task_dirs.as_ref().ok_or_else(|| {
                    launch("artifact-producing role has no task workspace authority".into())
                })?;
                if capture.checkout_dir() != task.work() {
                    return Err(launch(
                        "artifact capture authority names a different task checkout".into(),
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
                        &request.workspace_preparation,
                        archive_checkout.as_deref(),
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
            let mut extras = effect_mounts(&dirs, &runtime_profile);
            extras.extend(skill_mounts);
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
                    &workspace_source,
                    &request.prepared_inputs,
                    &request.effect_id,
                )
                .await
                .map_err(|error| {
                    launch(format!(
                        "failed to prepare granted mission inputs: {error:#}"
                    ))
                })?
            };
            extras.extend(prepared.mounts);
            let environment =
                mission_environment(&dirs, &request.environment, prepared.environment);
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
                resources: request.role.resources.clone(),
                resource_ceilings: &request.resource_ceilings,
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
        let turn = self
            .run_turn(&profile, &request, plan, runtime_state, auth)
            .await;
        let (applied, final_response) = turn?;
        if let Err(error) = role_state.assess_runtime_retention_async().await {
            let mut failure = TypedFailure::permanent(
                "runtime.native_state_limit",
                format!("retained runtime state post-turn check refused: {error:#}"),
            );
            failure.evidence_mut().configuration = applied.clone();
            failure.evidence_mut().final_response = final_response.clone();
            return Err(failure);
        }
        let cancellation_configuration = applied.clone();
        let cancellation_response = final_response.clone();
        let finish = async {
            let handoff = match read_optional_handoff(dirs.handoff(), request.role.output) {
                Ok(handoff) => handoff,
                Err(mut failure) => {
                    failure.evidence_mut().final_response = final_response.clone();
                    failure.evidence_mut().configuration = applied.clone();
                    return Err(failure);
                }
            };
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
            Ok(RoleTurnOutcome {
                handoff,
                artifact,
                runtime_configuration: role_runtime_configuration(&applied),
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
        request: &RoleTurnRequest,
        plan: lionclaw_confinement::EffectiveExecutionPlan,
        runtime_state: lionclaw_runtime_api::RuntimeStateDir,
        auth: RuntimeTurnAuth,
    ) -> Result<(lionclaw_runtime_api::AppliedRuntimeConfiguration, String), TypedFailure> {
        self.run_turn_with_context(profile, request, plan, auth, |plan| {
            mission_execution_context(plan, Some(runtime_state.clone()))
        })
        .await
    }

    async fn run_turn_with_context(
        &self,
        profile: &MissionRuntimeProfile,
        request: &RoleTurnRequest,
        plan: lionclaw_confinement::EffectiveExecutionPlan,
        auth: RuntimeTurnAuth,
        context_builder: impl Fn(
            &lionclaw_confinement::EffectiveExecutionPlan,
        )
            -> anyhow::Result<lionclaw_runtime_api::RuntimeExecutionContext>,
    ) -> Result<(lionclaw_runtime_api::AppliedRuntimeConfiguration, String), TypedFailure> {
        if let Some(failure) = current_control_failure(profile, request, None, "") {
            return Err(failure);
        }
        let driver = self
            .driver(profile)
            .map_err(|err| launch(format!("runtime profile invalid: {err:#}")))?;
        let config = Self::driver_config(profile)
            .map_err(|err| launch(format!("runtime profile invalid: {err:#}")))?;
        driver
            .validate_config(&config)
            .map_err(|e| launch(format!("driver config invalid: {e}")))?;
        let adapter = driver.create_adapter(config);

        // Compute the fallible execution context exactly once before opening a
        // session, so reconstruction cannot drift or leak another session.
        let context = prefer_terminal_control(
            profile,
            request,
            context_builder(&plan).map_err(|e| launch(format!("execution context failed: {e}"))),
            None,
            "",
        )?;
        let runtime_state = profile
            .native_resume
            .then(|| context.runtime_state.clone())
            .flatten();
        let attempt = prefer_terminal_control(
            profile,
            request,
            runtime_state
                .as_ref()
                .map(lionclaw_runtime_api::begin_runtime_session_attempt)
                .transpose()
                .map_err(|e| launch(format!("native session state invalid: {e}"))),
            None,
            "",
        )?;
        let handle = adapter.session_start(RuntimeSessionStartInput {
            session_id: uuid_from_key(request.effect_id.as_str()),
            working_dir: Some(WORKSPACE_MOUNT_TARGET.to_string()),
            environment: plan.environment.clone(),
            resume: match (runtime_state.clone(), attempt.as_ref()) {
                (Some(state), Some(attempt)) => RuntimeResume::Native {
                    state,
                    ready: attempt.previous_ready(),
                },
                (None, None) => RuntimeResume::Reconstruct,
                _ => unreachable!("native state and attempt are created together"),
            },
        });
        if let Some(control_failure) = current_control_failure(profile, request, None, "") {
            if let Ok(handle) = &handle {
                settle_runtime_session(adapter.as_ref(), handle);
            }
            return Err(control_failure);
        }
        let handle = handle.map_err(|e| launch(format!("session_start failed: {e}")))?;
        let (mut result, mut fallback_final_response) = execute_turn_attempt(
            Arc::clone(&adapter),
            &handle,
            profile,
            request,
            context.clone(),
            plan.clone(),
            auth.clone(),
        )
        .await;

        let mut observation = observe_native_session(adapter.as_ref(), &handle);
        let settled;
        if result.is_err()
            && observation.is_some_and(RuntimeNativeSessionObservation::is_reopen_failure)
            && adapter.native_reopen_recovery() == RuntimeNativeReopenRecovery::ForgetAndReconstruct
        {
            if let Some(control_failure) = current_control_failure(
                profile,
                request,
                Some(AttemptEvidence::from_result(&result)),
                &fallback_final_response,
            ) {
                settle_runtime_session(adapter.as_ref(), &handle);
                return Err(control_failure);
            }
            let reopen_failure = match result {
                Err(failure) => failure,
                Ok(_) => unreachable!("recoverable reopen requires a failed turn"),
            };
            let forget_result = adapter.forget_native_reopen(&handle);
            if let Some(control_failure) = current_control_failure(
                profile,
                request,
                Some(AttemptEvidence::Failure(&reopen_failure)),
                &fallback_final_response,
            ) {
                settle_runtime_session(adapter.as_ref(), &handle);
                return Err(control_failure);
            }
            if let Err(error) = forget_result {
                settled = settle_runtime_session(adapter.as_ref(), &handle);
                if let Some(control_failure) = current_control_failure(
                    profile,
                    request,
                    Some(AttemptEvidence::Failure(&reopen_failure)),
                    &fallback_final_response,
                ) {
                    return Err(control_failure);
                }
                result = Err(recovery_failure(&reopen_failure, "forget", &error));
            } else if !settle_runtime_session(adapter.as_ref(), &handle) {
                settled = false;
                observation = None;
                result = Err(reopen_failure);
            } else {
                if let Some(control_failure) = current_control_failure(
                    profile,
                    request,
                    Some(AttemptEvidence::Failure(&reopen_failure)),
                    &fallback_final_response,
                ) {
                    return Err(control_failure);
                }
                let reconstruction = adapter.session_start(RuntimeSessionStartInput {
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
                });
                if let Some(control_failure) = current_control_failure(
                    profile,
                    request,
                    Some(AttemptEvidence::Failure(&reopen_failure)),
                    &fallback_final_response,
                ) {
                    if let Ok(reconstruction) = &reconstruction {
                        settle_runtime_session(adapter.as_ref(), reconstruction);
                    }
                    return Err(control_failure);
                }
                let reconstruction = reconstruction.map_err(|error| {
                    recovery_failure(&reopen_failure, "reconstruction_start", &error)
                })?;
                let (reconstructed, reconstructed_response) = execute_turn_attempt(
                    Arc::clone(&adapter),
                    &reconstruction,
                    profile,
                    request,
                    context,
                    plan.clone(),
                    auth,
                )
                .await;
                observation = observe_native_session(adapter.as_ref(), &reconstruction);
                settled = settle_runtime_session(adapter.as_ref(), &reconstruction);
                fallback_final_response = reconstructed_response;
                if let Some(control_failure) = current_control_failure(
                    profile,
                    request,
                    Some(AttemptEvidence::from_result(&reconstructed)),
                    &fallback_final_response,
                ) {
                    return Err(control_failure);
                }
                result = reconstructed
                    .map_err(|failure| double_recovery_failure(&reopen_failure, &failure));
            }
        } else {
            settled = settle_runtime_session(adapter.as_ref(), &handle);
        }

        if let Some(control_failure) = current_control_failure(
            profile,
            request,
            Some(AttemptEvidence::from_result(&result)),
            &fallback_final_response,
        ) {
            return Err(control_failure);
        }

        if settled {
            if let Some(attempt) = attempt {
                let persistence = match observation {
                    Some(observation) => attempt.commit(observation),
                    None if result.as_ref().is_err_and(|failure| failure.is_transient()) => {
                        attempt.restore_previous()
                    }
                    None => Ok(()),
                };
                if let Err(error) = persistence {
                    tracing::warn!(
                        error = %error,
                        "failed to persist native session readiness; the next turn will reconstruct"
                    );
                }
            }
        }

        if let Some(control_failure) = current_control_failure(
            profile,
            request,
            Some(AttemptEvidence::from_result(&result)),
            &fallback_final_response,
        ) {
            return Err(control_failure);
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

#[derive(Clone, Copy)]
enum AttemptEvidence<'a> {
    Success(&'a lionclaw_runtime_api::TurnResult),
    Failure(&'a TypedFailure),
}

impl<'a> AttemptEvidence<'a> {
    fn from_result(result: &'a Result<lionclaw_runtime_api::TurnResult, TypedFailure>) -> Self {
        match result {
            Ok(result) => Self::Success(result),
            Err(failure) => Self::Failure(failure),
        }
    }
}

fn current_control_failure(
    profile: &MissionRuntimeProfile,
    request: &RoleTurnRequest,
    observed: Option<AttemptEvidence<'_>>,
    fallback_final_response: &str,
) -> Option<TypedFailure> {
    let control = request.control.borrow().clone();
    control_failure(profile, &control, observed, fallback_final_response)
}

/// A durable terminal decision outranks the result of a fallible host
/// operation completed before the runner could observe that decision.
fn prefer_terminal_control<T>(
    profile: &MissionRuntimeProfile,
    request: &RoleTurnRequest,
    result: Result<T, TypedFailure>,
    observed: Option<AttemptEvidence<'_>>,
    fallback_final_response: &str,
) -> Result<T, TypedFailure> {
    match current_control_failure(profile, request, observed, fallback_final_response) {
        Some(failure) => Err(failure),
        None => result,
    }
}

fn control_failure(
    profile: &MissionRuntimeProfile,
    control: &ExecutionControl,
    observed: Option<AttemptEvidence<'_>>,
    fallback_final_response: &str,
) -> Option<TypedFailure> {
    let mut failure = setup_control_failure(profile, control)?;
    let evidence = failure.evidence_mut();
    match observed {
        Some(AttemptEvidence::Success(observed)) => {
            evidence.configuration = observed.configuration.clone().projected();
            evidence.final_response = lionclaw_runtime_api::bounded_text(&observed.final_response);
        }
        Some(AttemptEvidence::Failure(observed)) => {
            let source = observed.evidence();
            evidence.exit_code = source.exit_code;
            evidence.stderr.clone_from(&source.stderr);
            evidence.final_response.clone_from(&source.final_response);
            evidence.configuration.clone_from(&source.configuration);
        }
        None => {}
    }
    if evidence.final_response.is_empty() {
        evidence.final_response = lionclaw_runtime_api::bounded_text(fallback_final_response);
    }
    Some(failure)
}

fn settle_runtime_session(adapter: &dyn RuntimeAdapter, handle: &RuntimeSessionHandle) -> bool {
    match adapter.close(handle) {
        Ok(()) => true,
        Err(error) => {
            tracing::warn!(
                runtime_session_id = handle.runtime_session_id,
                error = %error,
                "failed to close native runtime session; readiness will remain uncommitted"
            );
            false
        }
    }
}

fn observe_native_session(
    adapter: &dyn RuntimeAdapter,
    handle: &RuntimeSessionHandle,
) -> Option<RuntimeNativeSessionObservation> {
    match adapter.native_session_observation(handle) {
        Ok(observation) => observation,
        Err(error) => {
            tracing::warn!(
                runtime_session_id = handle.runtime_session_id,
                error = %error,
                "failed to read native session observation; the next turn will reconstruct"
            );
            None
        }
    }
}

async fn execute_turn_attempt(
    adapter: Arc<dyn RuntimeAdapter>,
    handle: &RuntimeSessionHandle,
    profile: &MissionRuntimeProfile,
    request: &RoleTurnRequest,
    context: lionclaw_runtime_api::RuntimeExecutionContext,
    plan: lionclaw_confinement::EffectiveExecutionPlan,
    auth: RuntimeTurnAuth,
) -> (
    Result<lionclaw_runtime_api::TurnResult, TypedFailure>,
    String,
) {
    let (journal_tx, journal_rx) =
        tokio::sync::mpsc::channel(lionclaw_runtime_api::RUNTIME_TURN_JOURNAL_CAPACITY);
    let drain = tokio::spawn(drain_runtime_journal(
        journal_rx,
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
                auth.materialization,
                &request.effect_id,
                Some(auth.staging_root),
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
    match drain.await {
        Ok(Ok(final_response)) => (result, final_response),
        Ok(Err(mut journal_failure)) => {
            if let Err(turn_failure) = &result {
                let evidence = journal_failure.evidence_mut();
                if evidence.final_response.is_empty() {
                    evidence
                        .final_response
                        .clone_from(&turn_failure.evidence().final_response);
                }
                evidence.configuration = turn_failure.evidence().configuration.clone();
            }
            (Err(journal_failure), String::new())
        }
        Err(error) => (
            Err(TypedFailure::permanent(
                "runtime.journal",
                format!("runtime journal observer failed: {error}"),
            )),
            String::new(),
        ),
    }
}

fn recovery_failure(first: &TypedFailure, stage: &str, error: &anyhow::Error) -> TypedFailure {
    let failure = error
        .downcast_ref::<TypedFailure>()
        .cloned()
        .unwrap_or_else(|| {
            TypedFailure::permanent("runtime.native_reopen_recovery", error.to_string())
        });
    augment_recovery_failure(first, failure, "runtime.native_reopen_recovery", stage)
}

fn double_recovery_failure(first: &TypedFailure, second: &TypedFailure) -> TypedFailure {
    augment_recovery_failure(
        first,
        second.clone(),
        "runtime.native_reopen_reconstruction_failed",
        "canonical_reconstruction",
    )
}

fn augment_recovery_failure(
    first: &TypedFailure,
    mut current: TypedFailure,
    code: &str,
    current_stage: &str,
) -> TypedFailure {
    let first_prefix = format!("native_reopen[{}]=", first.category());
    let current_prefix = format!("; {current_stage}[{}]=", current.category());
    let detail_budget = lionclaw_runtime_api::FAILURE_TEXT_LIMIT
        .saturating_sub(first_prefix.len() + current_prefix.len());
    let first_detail = bounded_stage_detail(first.detail(), detail_budget / 2);
    let current_detail = bounded_stage_detail(
        current.detail(),
        detail_budget.saturating_sub(first_detail.len()),
    );
    let evidence = current.evidence_mut();
    evidence.code = Some(code.to_string());
    evidence.detail = format!("{first_prefix}{first_detail}{current_prefix}{current_detail}");
    current.projected()
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
    activity: tokio::sync::watch::Sender<
        Option<(crate::model::EffectId, lionclaw_runtime_api::TurnEvent)>,
    >,
    effect_id: crate::model::EffectId,
) -> Result<String, TypedFailure> {
    let mut final_response = String::new();
    while let Some(event) = journal.recv().await {
        lionclaw_runtime_api::observe_final_response(&mut final_response, event.event());
        activity.send_replace(Some((effect_id.clone(), event)));
    }
    Ok(final_response.trim_end().to_string())
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

fn role_runtime_configuration(
    applied: &lionclaw_runtime_api::AppliedRuntimeConfiguration,
) -> crate::model::RuntimeConfigurationEvidence {
    crate::model::RuntimeConfigurationEvidence {
        requested_model: applied.requested_model.clone(),
        applied_model: applied.applied_model.clone(),
        model_confirmation: applied.model_confirmation,
        requested_mode: applied.requested_mode.clone(),
        applied_mode: applied.applied_mode.clone(),
        mode_confirmation: applied.mode_confirmation,
    }
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
    prepared_input: impl IntoIterator<Item = (String, String)>,
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
        prepared_input,
    )
}

/// Deterministic session UUID derived from the effect ID (no RNG).
fn uuid_from_key(key: &str) -> uuid::Uuid {
    let digest = <sha2::Sha256 as sha2::Digest>::digest(key.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    uuid::Uuid::from_bytes(bytes)
}
