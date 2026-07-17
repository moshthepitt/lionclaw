//! The impure shell around the pure core: lock → load → recover → drive
//! effects → step → append, until the mission parks (durable interrupt) or
//! reaches a terminal phase. Structure ported from Zenith (Apache-2.0,
//! Intelligent Internet) `controller.py::advance_project` /
//! `coordinator.py::step`, re-based onto the event-sourced store.
//!
//! A request without an outcome belongs to a previous driver process. Resume
//! reaps its resources and records an interrupted failure; it never guesses
//! whether an external turn completed and never silently replays one.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use lionclaw_runtime_api::TypedFailure;
use sha2::{Digest, Sha256};

use crate::driver_lock::DriverGuard;
use crate::mission_type::MissionType;
use crate::model::{
    step, validate_plan_proposal, EffectId, Handoff, InflightEffect, MissionEvent, MissionId,
    MissionPhase, MissionState, OracleDispatchIntent, OracleRunSuccess, PayloadRef, PlanProposal,
    ProposalError, RoleDispatchIntent, RoleRunSuccess, StepDecision, TaskId, TaskNamespace,
    TerminalReviewDispatchIntent, TerminalReviewSuccess,
};
use crate::ports::{
    Clock, EffectCleaner, EffectCleanupRequest, ExecutionControl, OracleRunRequest, OracleRunner,
    RoleRunRequest, RoleRunUpdate, RoleRunner,
};
use crate::prompt::{
    assemble_planning_prompt, assemble_role_prompt, assemble_terminal_review_prompt,
    PlanningPromptContext, PlanningPromptInput, PlanningPromptRefinement, PromptContext,
    TerminalReviewPromptContext,
};
use crate::store::{AppendError, MissionStore, NewEvent};

pub struct Engine {
    store: MissionStore,
    mission_type: MissionType,
    /// The runtime profile id and pinned confinement image this engine runs
    /// under; recorded on `MissionCreated`.
    runtime: String,
    image_id: String,
    role_runner: Arc<dyn RoleRunner>,
    oracle_runner: Arc<dyn OracleRunner>,
    effect_cleaner: Arc<dyn EffectCleaner>,
    clock: Arc<dyn Clock>,
}

pub struct EngineServices {
    role_runner: Arc<dyn RoleRunner>,
    oracle_runner: Arc<dyn OracleRunner>,
    effect_cleaner: Arc<dyn EffectCleaner>,
    clock: Arc<dyn Clock>,
}

struct ActivityReporter {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl ActivityReporter {
    fn start(
        store: MissionStore,
        mission_id: MissionId,
        activity: tokio::sync::watch::Receiver<Option<(EffectId, lionclaw_runtime_api::TurnEvent)>>,
    ) -> Self {
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = &mut stopped => return,
                    _ = interval.tick() => {}
                }
                let Ok(state) = store.require_state(&mission_id).await else {
                    continue;
                };
                let mission_dir = store.mission_dir(&mission_id);
                let observation = activity.borrow().clone();
                match crate::activity::publish_observed(
                    store
                        .lionclaw_dir()
                        .parent()
                        .expect(".lionclaw directory has a workspace parent"),
                    &mission_dir,
                    &state,
                    crate::activity::now_ms(),
                    observation.as_ref(),
                )
                .await
                {
                    Ok(()) => {}
                    Err(error) => {
                        tracing::warn!(%mission_id, %error, "failed to update non-authoritative activity projection")
                    }
                }
            }
        });
        Self {
            stop: Some(stop),
            task: Some(task),
        }
    }

    async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for ActivityReporter {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

impl EngineServices {
    pub fn new(
        role_runner: Arc<dyn RoleRunner>,
        oracle_runner: Arc<dyn OracleRunner>,
        effect_cleaner: Arc<dyn EffectCleaner>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            role_runner,
            oracle_runner,
            effect_cleaner,
            clock,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionDisposition {
    Ready,
    Running,
    AwaitingPlan,
    Parked,
    CleanupBlocked,
    Terminal,
}

impl MissionDisposition {
    pub const fn slug(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Running => "running",
            Self::AwaitingPlan => "awaiting_plan",
            Self::Parked => "parked",
            Self::CleanupBlocked => "cleanup_blocked",
            Self::Terminal => "terminal",
        }
    }
}

#[derive(Debug, Clone)]
pub struct MissionView {
    pub state: MissionState,
    pub disposition: MissionDisposition,
}

impl MissionView {
    fn from_state(state: MissionState, driver_running: bool) -> Self {
        let disposition = if driver_running {
            MissionDisposition::Running
        } else if state
            .cleanup_failure
            .as_ref()
            .is_some_and(|failure| state.inflight.contains_key(&failure.effect_id))
        {
            MissionDisposition::CleanupBlocked
        } else if state.phase.is_terminal() {
            MissionDisposition::Terminal
        } else if !state.open_attention.is_empty() {
            MissionDisposition::Parked
        } else if state.phase == MissionPhase::Planning
            && state.config.planning.tasks.is_empty()
            && state.proposal.is_none()
        {
            MissionDisposition::AwaitingPlan
        } else {
            MissionDisposition::Ready
        };
        Self { state, disposition }
    }

    pub fn next_actions(&self) -> Vec<&'static str> {
        match self.disposition {
            MissionDisposition::Ready => vec!["mission advance"],
            MissionDisposition::Running => vec!["mission status"],
            MissionDisposition::AwaitingPlan => vec!["mission plan propose"],
            MissionDisposition::Parked if !self.state.parked_effects.is_empty() => {
                vec!["mission continue", "mission decide"]
            }
            MissionDisposition::Parked => vec!["mission decide"],
            MissionDisposition::CleanupBlocked => vec!["mission advance", "mission log"],
            MissionDisposition::Terminal => vec!["mission report", "mission apply"],
        }
    }
}

pub async fn load_mission_view(
    store: &MissionStore,
    mission_id: &MissionId,
) -> Result<MissionView> {
    let guard = DriverGuard::try_acquire(&store.driver_lock_path(mission_id))?;
    let driver_running = guard.is_none();
    drop(guard);
    let state = store.require_state(mission_id).await?;
    Ok(MissionView::from_state(state, driver_running))
}

#[derive(Debug, thiserror::Error)]
pub enum ProposeError {
    #[error("mission is busy (an effect is in flight); retry once it quiesces")]
    MissionBusy,
    #[error(transparent)]
    Rejected(#[from] ProposalError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

const MAX_LOOP_ITERATIONS: usize = 10_000;

pub use crate::model::TERMINAL_REVIEW_TASK_TAG;

impl Engine {
    fn resolve_role_skills(
        &self,
        role: &crate::mission_type::RoleDefinition,
    ) -> std::result::Result<Vec<crate::mission_type::SkillPackage>, String> {
        role.skills
            .iter()
            .map(|name| {
                self.mission_type.skills.get(name).cloned().ok_or_else(|| {
                    format!("role '{}' references missing skill '{name}'", role.name)
                })
            })
            .collect()
    }

    pub fn new(
        store: MissionStore,
        mission_type: MissionType,
        runtime: String,
        image_id: String,
        services: EngineServices,
    ) -> Self {
        Self {
            store,
            mission_type,
            runtime,
            image_id,
            role_runner: services.role_runner,
            oracle_runner: services.oracle_runner,
            effect_cleaner: services.effect_cleaner,
            clock: services.clock,
        }
    }

    pub fn store(&self) -> &MissionStore {
        &self.store
    }

    pub fn mission_type(&self) -> &MissionType {
        &self.mission_type
    }

    /// The mission type this engine runs, pinned by name + content digest.
    fn mission_type_ref(&self) -> crate::model::MissionTypeRef {
        crate::model::MissionTypeRef {
            name: self.mission_type.name.clone(),
            digest: self.mission_type.digest.clone(),
        }
    }

    /// Create a mission. `base_sha` is the target repo's HEAD, observed by
    /// the caller (git stays out of the engine core). The mission type digest,
    /// runtime, and pinned image id are recorded from the engine's own config.
    pub async fn create_mission(
        &self,
        workspace_dir: &str,
        objective: &str,
        base_sha: &str,
        config: crate::model::MissionConfig,
    ) -> Result<MissionId> {
        let now_ms = self.clock.now_ms();
        let mission_id = MissionId::for_creation(workspace_dir, objective, now_ms);
        self.create_mission_with_id(
            mission_id,
            now_ms,
            workspace_dir,
            objective,
            base_sha,
            config,
        )
        .await
    }

    pub(crate) async fn create_mission_with_id(
        &self,
        mission_id: MissionId,
        now_ms: i64,
        workspace_dir: &str,
        objective: &str,
        base_sha: &str,
        config: crate::model::MissionConfig,
    ) -> Result<MissionId> {
        config
            .execution
            .validate()
            .map_err(|error| anyhow::anyhow!("invalid execution policy: {error}"))?;
        // The loader enforces both rules for mission types; enforce them here
        // too so no direct caller can mint a config the closing gate cannot
        // honor (the fold is total and cannot refuse the config).
        if config.stop == crate::model::StopBar::Reviewed && config.terminal_review.is_none() {
            bail!(
                "a reviewed-bar mission requires a terminal review: \
                 the reviewed bar is defined by an independent closing review"
            );
        }
        if let Some(review) = &config.terminal_review {
            match self.mission_type.roles.get(&review.role) {
                Some(role) if role.output == crate::model::OutputSemantics::EmitsGapVerdict => {}
                Some(role) => bail!(
                    "terminal-review role '{}' must be emits-gap-verdict, got {}",
                    review.role,
                    role.output.slug()
                ),
                None => bail!(
                    "terminal-review role '{}' is not provided by mission type '{}'",
                    review.role,
                    self.mission_type.name
                ),
            }
        }
        let created = NewEvent::new(MissionEvent::MissionCreated {
            objective: objective.to_string(),
            mission_type: self.mission_type_ref(),
            runtime: self.runtime.clone(),
            image_id: self.image_id.clone(),
            workspace_dir: workspace_dir.to_string(),
            base_sha: base_sha.to_string(),
            config,
        });
        self.store
            .create_mission(&mission_id, workspace_dir, objective, created, now_ms)
            .await
            .context("failed to create mission")?;
        Ok(mission_id)
    }

    /// Validate and record one complete plan proposal. Initial and revised
    /// plans use the same boundary; invalid or stale proposals append nothing.
    pub async fn propose_plan(
        &self,
        mission_id: &MissionId,
        proposal: PlanProposal,
    ) -> Result<(), ProposeError> {
        let state = self.load_state(mission_id).await?;
        if !state.inflight.is_empty() {
            return Err(ProposeError::MissionBusy);
        }
        validate_plan_proposal(&state, &proposal, &self.mission_type.inventory())?;
        let proposal_json = serde_json::to_string(&proposal).map_err(anyhow::Error::from)?;
        let plan_hash = hex::encode(Sha256::digest(proposal_json.as_bytes()));
        let event = NewEvent::new(MissionEvent::PlanProposed {
            proposal,
            plan_hash,
        });
        self.store
            .append(mission_id, state.head, &[event], self.clock.now_ms())
            .await
            .map_err(|e| ProposeError::Other(e.into()))?;
        Ok(())
    }

    /// Record a decision resolving an open attention item (a durable
    /// interrupt). Validated fail-closed: an illegal decision records nothing.
    pub async fn decide(
        &self,
        mission_id: &MissionId,
        attention_id: &str,
        action: crate::model::DecisionAction,
        justification: &str,
    ) -> Result<()> {
        record_decision(
            &self.store,
            self.clock.now_ms(),
            mission_id,
            attention_id,
            action,
            justification,
        )
        .await
    }

    pub async fn load_state(&self, mission_id: &MissionId) -> Result<MissionState> {
        let state = self.store.require_state(mission_id).await?;
        // The instrument of judgment is pinned: this verifies the mission type's
        // content digest against the one recorded at start, so a mutated role or
        // oracle cannot advance this mission (the fake-green vector). Every method
        // that loads the pinned type — propose_plan and advance/drive —
        // funnels here. `decide`/`record_decision` are deliberately store-only
        // (no type loaded): a decision mints no verdict, and the next `advance`
        // re-verifies the digest before any oracle can run.
        if state.mission_type.digest != self.mission_type.digest {
            bail!(
                "mission type '{}' changed since this mission started \
                 (recorded {}, on-disk {}); start a fresh mission",
                state.mission_type.name,
                crate::model::short_hex(&state.mission_type.digest),
                crate::model::short_hex(&self.mission_type.digest),
            );
        }
        Ok(state)
    }

    /// Drive the mission until it parks, terminates, or awaits input.
    pub async fn advance(&self, mission_id: &MissionId) -> Result<MissionView> {
        self.advance_with_handshake(mission_id, None).await
    }

    pub async fn advance_with_handshake(
        &self,
        mission_id: &MissionId,
        handshake: Option<&std::path::Path>,
    ) -> Result<MissionView> {
        let lock_path = self.store.driver_lock_path(mission_id);
        let started = tokio::time::Instant::now();
        let _guard = loop {
            if let Some(guard) = DriverGuard::try_acquire(&lock_path)? {
                break guard;
            }
            if handshake.is_none() || started.elapsed() >= Duration::from_secs(2) {
                let state = self.store.require_state(mission_id).await?;
                return Ok(MissionView::from_state(state, true));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        if let Some(path) = handshake {
            let temporary = path.with_extension("tmp");
            std::fs::write(&temporary, b"ready\n")
                .with_context(|| format!("writing driver handshake '{}'", temporary.display()))?;
            std::fs::rename(&temporary, path)
                .with_context(|| format!("publishing driver handshake '{}'", path.display()))?;
        }
        if !self.recover_interrupted(mission_id).await? {
            return Ok(MissionView::from_state(
                self.load_state(mission_id).await?,
                false,
            ));
        }
        let (activity, observed_activity) = tokio::sync::watch::channel(None);
        let reporter =
            ActivityReporter::start(self.store.clone(), mission_id.clone(), observed_activity);
        let drive_result = self.drive(mission_id, activity).await;
        // Persist a fold snapshot before parking or exiting so the next
        // invocation resumes without re-folding the whole log.
        let state = match drive_result {
            Ok(()) => {
                let state = self.load_state(mission_id).await?;
                self.store
                    .save_snapshot(&state, self.clock.now_ms())
                    .await?;
                state
            }
            Err(error) => {
                reporter.shutdown().await;
                return Err(error);
            }
        };
        // Projection latency is deliberately outside all authoritative state
        // transitions, deadline decisions, and cleanup.
        reporter.shutdown().await;
        Ok(MissionView::from_state(state, false))
    }

    async fn drive(
        &self,
        mission_id: &MissionId,
        activity: tokio::sync::watch::Sender<Option<(EffectId, lionclaw_runtime_api::TurnEvent)>>,
    ) -> Result<()> {
        for _ in 0..MAX_LOOP_ITERATIONS {
            let state = self.load_state(mission_id).await?;
            if !state.inflight.is_empty() {
                if self.drive_one(&state, activity.clone()).await? {
                    continue;
                }
                return Ok(());
            }
            match step(&state) {
                StepDecision::Idle => {
                    return match state.phase {
                        MissionPhase::Planning => Ok(()),
                        phase => bail!("engine idle in unexpected phase {phase:?}"),
                    };
                }
                StepDecision::Park | StepDecision::Terminal => return Ok(()),
                StepDecision::DispatchRole(intent) => {
                    self.materialize_role_request(&state, intent).await?;
                }
                StepDecision::RunOracles(intents) => {
                    self.materialize_oracle_requests(&state, intents).await?;
                }
                StepDecision::ReviewTerminal(intent) => {
                    self.materialize_terminal_review_request(&state, intent)
                        .await?;
                }
            }
        }
        bail!("advance exceeded {MAX_LOOP_ITERATIONS} iterations; aborting as a safety stop")
    }

    /// Abandon requests inherited from a previous driver process. The lock
    /// proves no live driver still owns them; cleanup must finish before the
    /// interrupted outcome is recorded.
    async fn recover_interrupted(&self, mission_id: &MissionId) -> Result<bool> {
        loop {
            let state = self.load_state(mission_id).await?;
            let Some((effect_id, effect)) = state.inflight.iter().next() else {
                return Ok(true);
            };
            if !self.cleanup_effect(&state, effect_id, true).await? {
                return Ok(false);
            }
            if self
                .append_outcome(
                    &state.mission_id,
                    effect_id,
                    interrupted_outcome(effect_id, effect),
                    true,
                )
                .await?
                .is_none()
            {
                return Ok(false);
            }
        }
    }

    /// Execute one request materialized by this driver, clean its transient
    /// resources, then durably record the outcome.
    async fn drive_one(
        &self,
        state: &MissionState,
        activity: tokio::sync::watch::Sender<Option<(EffectId, lionclaw_runtime_api::TurnEvent)>>,
    ) -> Result<bool> {
        let Some(effect_id) = state.inflight.keys().next() else {
            return Ok(false);
        };
        let wait_ms = state
            .inflight
            .get(effect_id)
            .expect("effect id came from the same map")
            .not_before_ms()
            .saturating_sub(self.clock.now_ms())
            .max(0) as u64;
        let start_at = tokio::time::Instant::now() + Duration::from_millis(wait_ms);
        let effect = loop {
            let current = self.load_state(&state.mission_id).await?;
            let Some(active) = current.inflight.get(effect_id) else {
                return Ok(true);
            };
            if let Some(reason) = current.stop_requests.get(effect_id) {
                if !self.cleanup_effect(&current, effect_id, true).await? {
                    return Ok(false);
                }
                if self
                    .append_outcome(
                        &current.mission_id,
                        effect_id,
                        stopped_before_start_outcome(effect_id, active, reason),
                        true,
                    )
                    .await?
                    .is_none()
                {
                    return Ok(false);
                }
                return Ok(has_owned_oracle_sibling(&current, effect_id, active));
            }
            let now = tokio::time::Instant::now();
            if now >= start_at {
                break active.clone();
            }
            tokio::time::sleep((start_at - now).min(Duration::from_millis(100))).await;
        };
        let (control_tx, control_rx) =
            tokio::sync::watch::channel(ExecutionControl::RunUntil(effect.deadline_ms()));
        let execution = async {
            match &effect {
                InflightEffect::RoleRun { .. } => {
                    self.execute_role_run(state, effect_id, &effect, control_rx, activity.clone())
                        .await
                }
                InflightEffect::OracleRun { .. } => {
                    self.execute_oracle_run(state, effect_id, &effect, control_rx)
                        .await
                }
                InflightEffect::TerminalReview { .. } => {
                    self.execute_terminal_review(
                        state,
                        effect_id,
                        &effect,
                        control_rx,
                        activity.clone(),
                    )
                    .await
                }
            }
        };
        tokio::pin!(execution);
        let outcome = loop {
            tokio::select! {
                outcome = &mut execution => break outcome?,
                () = tokio::time::sleep(Duration::from_millis(100)) => {
                    let current = self.load_state(&state.mission_id).await?;
                    let Some(active) = current.inflight.get(effect_id) else {
                        bail!("active effect '{effect_id}' disappeared without an outcome");
                    };
                    if current.reached_deadlines.contains_key(effect_id) {
                        control_tx.send_replace(ExecutionControl::DeadlineExhausted);
                        continue;
                    }
                    if let Some(reason) = current.stop_requests.get(effect_id) {
                        control_tx.send_replace(ExecutionControl::Stop(reason.clone()));
                        continue;
                    }
                    let mut deadline_ms = active.deadline_ms();
                    let now_ms = self.clock.now_ms();
                    let extension_step_ms = i64::try_from(
                        current.config.execution.extension_step_secs.saturating_mul(1_000),
                    )
                    .unwrap_or(i64::MAX);
                    if now_ms.saturating_add(extension_step_ms) >= deadline_ms {
                        if let Some(budget_deadline_ms) = active.budget_deadline_ms() {
                            if deadline_ms < budget_deadline_ms {
                                let new_deadline_ms = deadline_ms
                                    .saturating_add(extension_step_ms)
                                    .min(budget_deadline_ms);
                                self.append_fact(
                                    &current.mission_id,
                                    current.head,
                                    NewEvent::new(MissionEvent::ControlRequested {
                                        effect_id: effect_id.clone(),
                                        action: crate::model::ControlAction::ExtendDeadline {
                                            old_deadline_ms: deadline_ms,
                                            new_deadline_ms,
                                            automatic: true,
                                        },
                                        reason: "mission execution policy time budget".into(),
                                    }),
                                ).await?;
                                deadline_ms = new_deadline_ms;
                            }
                        }
                    }
                    if now_ms >= deadline_ms {
                        self.append_fact(
                            &current.mission_id,
                            current.head,
                            NewEvent::new(MissionEvent::EffectDeadlineReached {
                                effect_id: effect_id.clone(),
                                deadline_ms,
                            }),
                        )
                        .await?;
                        let latest = self.load_state(&current.mission_id).await?;
                        if latest.reached_deadlines.get(effect_id) == Some(&deadline_ms) {
                            control_tx.send_replace(ExecutionControl::DeadlineExhausted);
                        }
                    } else {
                        control_tx.send_replace(ExecutionControl::RunUntil(deadline_ms));
                    }
                }
            }
        };
        let discard_artifact = !matches!(
            outcome.event,
            MissionEvent::RoleRunCompleted { outcome: Ok(_), .. }
        );
        if !self
            .cleanup_effect(state, effect_id, discard_artifact)
            .await?
        {
            return Ok(false);
        }
        let Some(outcome) = self
            .append_outcome(&state.mission_id, effect_id, outcome, discard_artifact)
            .await?
        else {
            return Ok(false);
        };
        let checkpoint = checkpoint_after(&outcome.event, &state.config.execution);
        let Some((automatic, reason)) = checkpoint else {
            return Ok(true);
        };
        if !automatic {
            // Oracle requests are materialized as one owned batch. Yield only
            // after every sibling has run; otherwise recovery would falsely
            // classify an unstarted sibling as a crashed effect.
            if has_owned_oracle_sibling(state, effect_id, &effect) {
                return Ok(true);
            }
            return Ok(false);
        }
        let current = self.load_state(&state.mission_id).await?;
        self.append_fact(
            &current.mission_id,
            current.head,
            NewEvent::new(MissionEvent::ControlRequested {
                effect_id: effect_id.clone(),
                action: crate::model::ControlAction::Continue { automatic: true },
                reason: reason.into(),
            }),
        )
        .await?;
        Ok(true)
    }

    async fn cleanup_effect(
        &self,
        state: &MissionState,
        effect_id: &EffectId,
        discard_artifact: bool,
    ) -> Result<bool> {
        let request = EffectCleanupRequest {
            mission_id: state.mission_id.clone(),
            effect_id: effect_id.clone(),
            workspace_dir: state.workspace_dir.clone().into(),
            state_dir: self.store.lionclaw_dir().to_path_buf(),
            discard_artifact,
        };
        match self.effect_cleaner.cleanup(request).await {
            Ok(()) => Ok(true),
            Err(error) => {
                let event = NewEvent::new(MissionEvent::EffectCleanupFailed {
                    effect_id: effect_id.clone(),
                    resource: error.resource,
                    failure: TypedFailure::permanent("cleanup.infrastructure", error.detail),
                });
                self.append_fact(&state.mission_id, state.head, event)
                    .await?;
                Ok(false)
            }
        }
    }

    async fn append_outcome(
        &self,
        mission_id: &MissionId,
        effect_id: &EffectId,
        mut outcome: NewEvent,
        mut artifact_discarded: bool,
    ) -> Result<Option<NewEvent>> {
        for _ in 0..MAX_LOOP_ITERATIONS {
            let state = self.load_state(mission_id).await?;
            let Some(effect) = state.inflight.get(effect_id) else {
                bail!("effect '{effect_id}' settled before this driver could append its outcome");
            };
            if let Some(failure) = settlement_failure(&state, effect_id, &outcome) {
                outcome = failed_outcome(effect_id, effect, failure);
                if !artifact_discarded {
                    if !self.cleanup_effect(&state, effect_id, true).await? {
                        return Ok(None);
                    }
                    artifact_discarded = true;
                    continue;
                }
            }
            match self
                .store
                .append(
                    mission_id,
                    state.head,
                    std::slice::from_ref(&outcome),
                    self.clock.now_ms(),
                )
                .await
            {
                Ok(_) | Err(AppendError::Duplicate { .. }) => return Ok(Some(outcome)),
                Err(AppendError::Conflict { .. }) => continue,
                Err(err) => return Err(err.into()),
            }
        }
        bail!("outcome append kept conflicting after {MAX_LOOP_ITERATIONS} retries")
    }

    async fn append_fact(
        &self,
        mission_id: &MissionId,
        initial_head: u64,
        event: NewEvent,
    ) -> Result<()> {
        let mut head = initial_head;
        for _ in 0..MAX_LOOP_ITERATIONS {
            match self
                .store
                .append(
                    mission_id,
                    head,
                    std::slice::from_ref(&event),
                    self.clock.now_ms(),
                )
                .await
            {
                Ok(_) => return Ok(()),
                Err(AppendError::Conflict { .. }) => head = self.load_state(mission_id).await?.head,
                Err(err) => return Err(err.into()),
            }
        }
        bail!("event append kept conflicting after {MAX_LOOP_ITERATIONS} retries")
    }

    async fn run_role_observed(
        &self,
        state: &MissionState,
        effect_id: &EffectId,
        request: RoleRunRequest,
        mut updates: tokio::sync::mpsc::Receiver<RoleRunUpdate>,
        record_workspace: bool,
    ) -> Result<std::result::Result<crate::ports::RoleRunOutcome, TypedFailure>> {
        let run = self.role_runner.run(request);
        tokio::pin!(run);
        loop {
            tokio::select! {
                result = &mut run => {
                    while let Ok(update) = updates.try_recv() {
                        self.record_role_update(state, effect_id, update, record_workspace).await?;
                    }
                    return Ok(result);
                }
                update = updates.recv() => {
                    if let Some(update) = update {
                        self.record_role_update(state, effect_id, update, record_workspace).await?;
                    }
                }
            }
        }
    }

    async fn record_role_update(
        &self,
        state: &MissionState,
        effect_id: &EffectId,
        update: RoleRunUpdate,
        record_workspace: bool,
    ) -> Result<()> {
        match update {
            RoleRunUpdate::WorkspacePrepared {
                base_sha,
                assignment_epoch,
            } if record_workspace => {
                let task_id = match state.inflight.get(effect_id) {
                    Some(InflightEffect::RoleRun { task_id, .. }) => task_id.clone(),
                    _ => return Ok(()),
                };
                self.append_fact(
                    &state.mission_id,
                    state.head,
                    NewEvent::new(MissionEvent::TaskWorkspacePrepared {
                        task_id,
                        effect_id: effect_id.clone(),
                        base_sha,
                        assignment_epoch,
                    }),
                )
                .await
            }
            RoleRunUpdate::WorkspacePrepared { .. } => Ok(()),
            RoleRunUpdate::RuntimeConfigured(configuration) => {
                let configuration = configuration.projected();
                self.append_fact(
                    &state.mission_id,
                    state.head,
                    NewEvent::new(MissionEvent::EffectRuntimeConfigured {
                        effect_id: effect_id.clone(),
                        configuration: crate::model::RuntimeConfigurationEvidence {
                            requested_model: configuration.requested_model,
                            applied_model: configuration.applied_model,
                            model_confirmation: configuration.model_confirmation,
                            requested_mode: configuration.requested_mode,
                            applied_mode: configuration.applied_mode,
                            mode_confirmation: configuration.mode_confirmation,
                        },
                    }),
                )
                .await
            }
        }
    }

    /// Append events, treating a `Conflict`/`Duplicate` as an idempotent no-op.
    /// Safe only for request appends: a Conflict means a non-driver decision
    /// moved the head, and the next fold re-derives the same dispatch.
    async fn append_idempotent(
        &self,
        mission_id: &MissionId,
        head: u64,
        events: &[NewEvent],
    ) -> Result<()> {
        match self
            .store
            .append(mission_id, head, events, self.clock.now_ms())
            .await
        {
            Ok(_) | Err(AppendError::Duplicate { .. }) | Err(AppendError::Conflict { .. }) => {
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn execute_role_run(
        &self,
        state: &MissionState,
        effect_id: &EffectId,
        effect: &InflightEffect,
        control: tokio::sync::watch::Receiver<ExecutionControl>,
        activity: tokio::sync::watch::Sender<Option<(EffectId, lionclaw_runtime_api::TurnEvent)>>,
    ) -> Result<NewEvent> {
        let InflightEffect::RoleRun {
            namespace,
            task_id,
            attempt_no,
            role: role_name,
            runtime,
            prompt,
            base_sha,
            assignment_epoch,
            recreate_workspace,
            ..
        } = effect
        else {
            bail!("execute_role_run called with a non-role effect");
        };
        let attempt_no = *attempt_no;
        let completed = |outcome| {
            NewEvent::new(MissionEvent::RoleRunCompleted {
                namespace: *namespace,
                task_id: task_id.clone(),
                attempt_no,
                effect_id: effect_id.clone(),
                outcome,
            })
        };
        let Some(role) = self.mission_type.roles.get(role_name) else {
            return Ok(completed(Err(TypedFailure::permanent(
                "role.missing",
                format!("role '{role_name}' is no longer provided by the mission type"),
            ))));
        };
        let prompt_text = self.store.blobs().resolve(prompt)?;
        let skills = match self.resolve_role_skills(role) {
            Ok(skills) => skills,
            Err(detail) => {
                return Ok(completed(Err(TypedFailure::permanent(
                    "skills.resolve",
                    detail,
                ))))
            }
        };
        let (updates, update_rx) = tokio::sync::mpsc::channel(8);
        let request = RoleRunRequest {
            mission_id: state.mission_id.clone(),
            task_id: task_id.clone(),
            attempt_no,
            effect_id: effect_id.clone(),
            role: role.clone(),
            runtime: runtime.clone(),
            skills,
            prompt: prompt_text,
            base_sha: base_sha.to_string(),
            assignment_epoch: *assignment_epoch,
            recreate_workspace: *recreate_workspace,
            deadline_ms: effect.deadline_ms(),
            control,
            updates,
            activity,
            workspace_dir: state.workspace_dir.clone().into(),
            state_dir: self.store.lionclaw_dir().to_path_buf(),
        };
        let previous_task = state.tasks_in(*namespace).get(task_id);
        match self
            .run_role_observed(state, effect_id, request, update_rx, true)
            .await?
        {
            Ok(mut outcome) => {
                outcome.runtime_configuration = outcome.runtime_configuration.projected();
                outcome.final_response =
                    lionclaw_runtime_api::bounded_text(&outcome.final_response);
                let incomplete = match &outcome.handoff {
                    Handoff::Work { done: false, .. } => Some("role reported done=false"),
                    Handoff::Plan { done: false, .. } => {
                        Some("planning author reported done=false")
                    }
                    _ => None,
                };
                if let Some(detail) = incomplete {
                    return Ok(completed(Err(invalid_role_outcome(
                        "handoff.incomplete",
                        detail,
                        &outcome,
                    ))));
                }
                // A planning author's proposal is validated fail-closed before
                // it is recorded, exactly like a manually proposed plan — an
                // invalid proposal is a failed attempt, never a bad contract.
                if let Handoff::Plan {
                    done: true,
                    proposal,
                    ..
                } = &outcome.handoff
                {
                    let Some(proposal) = proposal else {
                        return Ok(completed(Err(invalid_role_outcome(
                            "plan.missing",
                            "planning author reported done but proposed no plan",
                            &outcome,
                        ))));
                    };
                    if let Err(error) =
                        validate_plan_proposal(state, proposal, &self.mission_type.inventory())
                    {
                        return Ok(completed(Err(invalid_role_outcome(
                            "plan.invalid",
                            format!("proposed plan is invalid: {error}"),
                            &outcome,
                        ))));
                    }
                }
                let handoff = self.externalize_handoff(outcome.handoff)?;
                let settlement_evidence = lionclaw_runtime_api::TypedFailureEvidence {
                    final_response: outcome.final_response.clone(),
                    configuration: runtime_configuration_evidence(&outcome.runtime_configuration),
                    ..Default::default()
                };
                Ok(completed(Ok(RoleRunSuccess {
                    handoff,
                    artifact: outcome.artifact,
                    final_response: self
                        .store
                        .blobs()
                        .externalize(PayloadRef::inline(outcome.final_response))?,
                    runtime_configuration: outcome.runtime_configuration,
                }))
                .with_settlement_evidence(settlement_evidence))
            }
            Err(mut failure) => {
                if failure.is_transient() {
                    let delay_ms = transient_backoff_ms(
                        previous_task.map_or(0, |task| task.consecutive_failures),
                        failure.retry_after_ms(),
                    );
                    failure.set_next_eligible_at_ms(
                        self.clock.now_ms().saturating_add(delay_ms as i64),
                    );
                }
                Ok(completed(Err(failure.projected())))
            }
        }
    }

    async fn execute_oracle_run(
        &self,
        state: &MissionState,
        effect_id: &EffectId,
        effect: &InflightEffect,
        control: tokio::sync::watch::Receiver<ExecutionControl>,
    ) -> Result<NewEvent> {
        let InflightEffect::OracleRun {
            assertion_ids,
            oracle,
            judged_sha,
            attempt_no,
            ..
        } = effect
        else {
            bail!("execute_oracle_run called with a non-oracle effect");
        };
        let attempt_no = *attempt_no;
        let completed = |outcome| {
            NewEvent::new(MissionEvent::OracleRunCompleted {
                assertion_ids: assertion_ids.to_vec(),
                oracle: oracle.clone(),
                judged_sha: judged_sha.to_string(),
                attempt_no,
                effect_id: effect_id.clone(),
                outcome,
            })
        };
        let Some(oracle_path) = self.mission_type.oracles.get(oracle) else {
            return Ok(completed(Err(TypedFailure::permanent(
                "oracle.missing",
                format!("oracle '{oracle}' is no longer provided by the mission type"),
            ))));
        };
        let request = OracleRunRequest {
            mission_id: state.mission_id.clone(),
            effect_id: effect_id.clone(),
            oracle: oracle.clone(),
            oracle_path: oracle_path.clone(),
            judged_sha: judged_sha.to_string(),
            workspace_dir: state.workspace_dir.clone().into(),
            state_dir: self.store.lionclaw_dir().to_path_buf(),
            prepared_inputs: self.mission_type.inputs.values().cloned().collect(),
            deadline_ms: effect.deadline_ms(),
            control,
        };
        match self.oracle_runner.run(request).await {
            Ok(outcome) => {
                let settlement_evidence = lionclaw_runtime_api::TypedFailureEvidence {
                    exit_code: Some(outcome.exit_code),
                    stderr: String::from_utf8_lossy(&outcome.stderr).into_owned(),
                    ..Default::default()
                };
                Ok(completed(Ok(OracleRunSuccess {
                    exit_code: outcome.exit_code,
                    exit_signal: outcome.exit_signal,
                    stdout: self.store.blobs().payload_from_bytes(&outcome.stdout)?,
                    stderr: self.store.blobs().payload_from_bytes(&outcome.stderr)?,
                    prepared_inputs: outcome.prepared_inputs,
                    duration_ms: outcome.duration_ms,
                }))
                .with_settlement_evidence(settlement_evidence))
            }
            Err(mut failure) => {
                if failure.is_transient() {
                    let delay_ms = transient_backoff_ms(attempt_no, failure.retry_after_ms());
                    failure.set_next_eligible_at_ms(
                        self.clock.now_ms().saturating_add(delay_ms as i64),
                    );
                }
                Ok(completed(Err(failure.projected())))
            }
        }
    }

    /// Execute the closing review: one confined `emits-gap-verdict` role run
    /// against a complete checkout of the judged commit mounted read-only. The
    /// handoff is translated here — never trusted raw: the echoed nonce must
    /// match (a worker-planted script executed by the reviewer can write the
    /// handoff file but cannot read the prompt), `done=false` is "the review itself
    /// did not complete" (an unfinished review is not a verdict), and the
    /// typed gaps are size-capped because they land inline in the event log.
    async fn execute_terminal_review(
        &self,
        state: &MissionState,
        effect_id: &EffectId,
        effect: &InflightEffect,
        control: tokio::sync::watch::Receiver<ExecutionControl>,
        activity: tokio::sync::watch::Sender<Option<(EffectId, lionclaw_runtime_api::TurnEvent)>>,
    ) -> Result<NewEvent> {
        let InflightEffect::TerminalReview {
            attempt_no,
            role: role_name,
            runtime,
            prompt,
            judged_sha,
            nonce,
            ..
        } = effect
        else {
            bail!("execute_terminal_review called with a non-review effect");
        };
        let attempt_no = *attempt_no;
        let completed = |outcome| {
            NewEvent::new(MissionEvent::TerminalReviewCompleted {
                attempt_no,
                effect_id: effect_id.clone(),
                judged_sha: judged_sha.clone(),
                outcome,
            })
        };
        let Some(role) = self.mission_type.roles.get(role_name) else {
            return Ok(completed(Err(TypedFailure::permanent(
                "role.missing",
                format!(
                    "terminal-review role '{role_name}' is no longer provided by the mission type"
                ),
            ))));
        };
        let skills = match self.resolve_role_skills(role) {
            Ok(skills) => skills,
            Err(detail) => {
                return Ok(completed(Err(TypedFailure::permanent(
                    "skills.resolve",
                    detail,
                ))))
            }
        };
        let prompt_text = self.store.blobs().resolve(prompt)?;
        let (updates, update_rx) = tokio::sync::mpsc::channel(8);
        let request = RoleRunRequest {
            mission_id: state.mission_id.clone(),
            task_id: TaskId::new(TERMINAL_REVIEW_TASK_TAG).expect("valid literal task id"),
            attempt_no,
            effect_id: effect_id.clone(),
            role: role.clone(),
            runtime: runtime.clone(),
            skills,
            prompt: prompt_text,
            base_sha: judged_sha.clone(),
            assignment_epoch: attempt_no,
            recreate_workspace: true,
            deadline_ms: effect.deadline_ms(),
            control,
            updates,
            activity,
            workspace_dir: state.workspace_dir.clone().into(),
            state_dir: self.store.lionclaw_dir().to_path_buf(),
        };
        let outcome = match self
            .run_role_observed(state, effect_id, request, update_rx, false)
            .await?
        {
            Ok(outcome) => outcome,
            Err(mut failure) => {
                if failure.is_transient() {
                    let delay_ms = transient_backoff_ms(
                        state.terminal_review.consecutive_failures,
                        failure.retry_after_ms(),
                    );
                    failure.set_next_eligible_at_ms(
                        self.clock.now_ms().saturating_add(delay_ms as i64),
                    );
                }
                return Ok(completed(Err(failure.projected())));
            }
        };
        let Handoff::Review {
            done,
            report,
            passed,
            gaps,
            nonce: echoed,
        } = &outcome.handoff
        else {
            // Unreachable via the runner's schema check; fail closed anyway.
            return Ok(completed(Err(invalid_role_outcome(
                "handoff.review_shape",
                "terminal reviewer handed back a non-review handoff".to_string(),
                &outcome,
            ))));
        };
        if echoed != nonce {
            return Ok(completed(Err(invalid_role_outcome(
                "handoff.nonce",
                "handoff nonce mismatch: the handoff was not written by the reviewer".to_string(),
                &outcome,
            ))));
        }
        if !done {
            return Ok(completed(Err(invalid_role_outcome(
                "handoff.incomplete",
                "reviewer handed off done=false: the review itself did not complete".to_string(),
                &outcome,
            ))));
        }
        let settlement_evidence = lionclaw_runtime_api::TypedFailureEvidence {
            final_response: outcome.final_response.clone(),
            configuration: runtime_configuration_evidence(&outcome.runtime_configuration),
            ..Default::default()
        };
        Ok(completed(Ok(TerminalReviewSuccess {
            passed: *passed,
            gaps: gaps.clone(),
            report: self.store.blobs().externalize(report.clone())?,
            final_response: self
                .store
                .blobs()
                .externalize(PayloadRef::inline(outcome.final_response))?,
            runtime_configuration: outcome.runtime_configuration,
        }))
        .with_settlement_evidence(settlement_evidence))
    }

    /// Resolve the `last_report` blobs of a task's dependencies (in either era's
    /// task map) — the upstream context threaded into a role's prompt.
    fn resolve_upstream_reports(
        &self,
        tasks: &std::collections::BTreeMap<crate::model::TaskId, crate::model::TaskRuntimeState>,
        depends_on: &[crate::model::TaskId],
    ) -> Result<Vec<String>> {
        let mut reports = Vec::new();
        for dep in depends_on {
            if let Some(report) = tasks.get(dep).and_then(|t| t.last_report.as_ref()) {
                reports.push(self.store.blobs().resolve(report)?);
            }
        }
        Ok(reports)
    }

    fn resolve_task_feedback(&self, task: &crate::model::TaskRuntimeState) -> Result<Vec<String>> {
        let mut feedback = Vec::new();
        if let Some(failure) = &task.last_failure {
            feedback.push(format!(
                "Previous attempt failed ({}): {}",
                failure.category(),
                failure.detail()
            ));
        }
        for item in &task.feedback {
            feedback.push(crate::evidence::render_feedback(self.store.blobs(), item)?);
        }
        Ok(feedback)
    }

    /// Assemble an execution role's prompt.
    fn assemble_execution_request(
        &self,
        state: &MissionState,
        role: &crate::mission_type::RoleDefinition,
        intent: &RoleDispatchIntent,
    ) -> Result<String> {
        let plan = state
            .plan
            .as_ref()
            .context("execution dispatch without a plan")?;
        let targets: Vec<_> = plan
            .assertions
            .iter()
            .filter(|a| intent.targets.contains(&a.id))
            .collect();
        let task = plan
            .tasks
            .iter()
            .find(|t| t.id == intent.task_id)
            .context("dispatched task not in plan")?;
        let upstream_reports = self.resolve_upstream_reports(&state.tasks, &task.depends_on)?;
        let feedback = state
            .tasks
            .get(&task.id)
            .map(|runtime| self.resolve_task_feedback(runtime))
            .transpose()?
            .unwrap_or_default();
        let skills = self.resolve_role_skills(role).map_err(anyhow::Error::msg)?;
        let prompt = assemble_role_prompt(
            role,
            &PromptContext {
                objective: &state.objective,
                task_body: &intent.body,
                targets: &targets,
                upstream_reports: &upstream_reports,
                skills: &skills,
                feedback: &feedback,
            },
        );
        Ok(prompt)
    }

    /// Assemble a planning role's prompt. Threads the mission type's playbook
    /// and execution-role/oracle inventories through a separate assembler.
    fn assemble_planning_request(
        &self,
        state: &MissionState,
        role: &crate::mission_type::RoleDefinition,
        intent: &RoleDispatchIntent,
    ) -> Result<String> {
        let task = state
            .config
            .planning
            .tasks
            .iter()
            .find(|t| t.id == intent.task_id)
            .context("dispatched planning task not in the DAG")?;
        let upstream_reports =
            self.resolve_upstream_reports(&state.planning.tasks, &task.depends_on)?;
        let task_feedback = state
            .planning
            .tasks
            .get(&task.id)
            .map(|runtime| self.resolve_task_feedback(runtime))
            .transpose()?
            .unwrap_or_default();
        let planning_input = self.resolve_planning_prompt_input(state)?;
        let oracle_inventory: Vec<String> = self
            .mission_type
            .oracles
            .keys()
            .map(|o| o.as_str().to_string())
            .collect();
        let skills = self.resolve_role_skills(role).map_err(anyhow::Error::msg)?;
        let prompt = assemble_planning_prompt(
            role,
            &PlanningPromptContext {
                objective: &state.objective,
                base_revision: state.planning_base_revision.unwrap_or(state.revision),
                input: planning_input,
                playbook: self.mission_type.playbook.as_deref(),
                roles: &self.mission_type.roles,
                oracle_inventory: &oracle_inventory,
                task_body: &intent.body,
                upstream_reports: &upstream_reports,
                skills: &skills,
                task_feedback: &task_feedback,
            },
        );
        Ok(prompt)
    }

    fn resolve_planning_prompt_input<'a>(
        &self,
        state: &'a MissionState,
    ) -> Result<PlanningPromptInput<'a>> {
        let refinement = match state.planning_input.refinement.as_ref() {
            Some(crate::model::PlanningRefinement::Guidance(guidance)) => {
                Some(PlanningPromptRefinement::HumanGuidance(guidance))
            }
            Some(crate::model::PlanningRefinement::FailureEvidence(feedback)) => {
                Some(PlanningPromptRefinement::FailureEvidence(
                    crate::evidence::render_feedback(self.store.blobs(), feedback)?,
                ))
            }
            None => None,
        };
        Ok(PlanningPromptInput {
            accepted_plan: state.plan.as_ref(),
            latest_rejected_candidate: state.planning_input.latest_rejected_proposal.as_ref(),
            refinement,
        })
    }

    /// Turn a role-dispatch intent into a recorded request: assemble the
    /// prompt (engine-owned), persist it, derive the effect ID.
    async fn materialize_role_request(
        &self,
        state: &MissionState,
        intent: RoleDispatchIntent,
    ) -> Result<()> {
        let role = self
            .mission_type
            .roles
            .get(&intent.role)
            .with_context(|| format!("role '{}' missing from the mission type", intent.role))?;
        // Planning and execution assemble prompts and namespace effect IDs
        // separately, so a planning report can never reach an execution judge and
        // a planning effect can never collide with an execution one.
        let (prompt_text, effect_namespace) = match intent.namespace {
            TaskNamespace::Planning => (
                self.assemble_planning_request(state, role, &intent)?,
                TaskNamespace::Planning.slug(),
            ),
            TaskNamespace::Execution => (
                self.assemble_execution_request(state, role, &intent)?,
                TaskNamespace::Execution.slug(),
            ),
        };
        let prompt_hash = hex::encode(Sha256::digest(prompt_text.as_bytes()));
        let prompt = self
            .store
            .blobs()
            .externalize(PayloadRef::inline(prompt_text))?;
        let (base_sha, assignment_epoch, recreate_workspace) = resolve_task_assignment(
            state.tasks_in(intent.namespace).get(&intent.task_id),
            &intent.base_sha,
            state.config.recovery.max_attempts,
        );
        let effect_id = effect_id_for(&[
            effect_namespace,
            state.mission_id.as_str(),
            intent.task_id.as_str(),
            &intent.attempt_no.to_string(),
            &assignment_epoch.to_string(),
            &prompt_hash,
        ]);
        let requested_at_ms = self.clock.now_ms();
        let not_before_ms = retry_not_before(
            requested_at_ms,
            state
                .tasks_in(intent.namespace)
                .get(&intent.task_id)
                .and_then(|task| task.last_failure.as_ref()),
        );
        let initial_secs = role
            .timeout_secs
            .unwrap_or(state.config.execution.default_timeout_secs);
        let event = NewEvent::new(MissionEvent::RoleRunRequested {
            namespace: intent.namespace,
            task_id: intent.task_id,
            attempt_no: intent.attempt_no,
            effect_id,
            role: intent.role,
            runtime: role
                .runtime
                .clone()
                .unwrap_or_else(|| state.runtime.clone()),
            prompt,
            base_sha,
            assignment_epoch,
            recreate_workspace,
            requested_at_ms,
            not_before_ms,
            deadline_ms: resolved_deadline(not_before_ms, initial_secs)?,
            budget_deadline_ms: resolved_deadline(
                not_before_ms,
                initial_secs.max(state.config.execution.max_task_time_secs),
            )?,
        })
        .with_prompt_hash(prompt_hash);
        self.append_idempotent(&state.mission_id, state.head, &[event])
            .await
    }

    /// Turn a terminal-review intent into a recorded request
    /// (`("terminal-review", …)` effect namespace). Fresh-context by
    /// construction: the assembler takes only the objective. The nonce is
    /// random per materialization — never derived from a deterministic recipe
    /// that worker-planted code could precompute — so, unlike role runs, the
    /// effect ID deliberately excludes the prompt hash. The identity names the
    /// logical review attempt; the recorded request remains the source of its
    /// random nonce.
    async fn materialize_terminal_review_request(
        &self,
        state: &MissionState,
        intent: TerminalReviewDispatchIntent,
    ) -> Result<()> {
        let effect_id = effect_id_for(&[
            "terminal-review",
            state.mission_id.as_str(),
            &intent.judged_sha,
            &intent.attempt_no.to_string(),
        ]);
        let role = self.mission_type.roles.get(&intent.role).with_context(|| {
            format!(
                "terminal-review role '{}' is not provided by the pinned mission type",
                intent.role
            )
        })?;
        // The nonce is recorded on the event and only ever *copied* by the
        // fold, so randomness here never threatens fold purity — same
        // discipline as the oracle's wall-clock duration. It must be random
        // (never derived): a deterministic recipe could be precomputed by
        // worker-planted code, which is the exact forgery this token defeats.
        #[expect(clippy::disallowed_methods)]
        let nonce = uuid::Uuid::new_v4().simple().to_string();
        let skills = self.resolve_role_skills(role).map_err(anyhow::Error::msg)?;
        let limitations: Vec<String> = state
            .plan
            .iter()
            .flat_map(|plan| &plan.requirements)
            .filter_map(|requirement| match &requirement.disposition {
                crate::model::RequirementDisposition::Limitation { rationale } => Some(format!(
                    "{}: {} ({rationale})",
                    requirement.id, requirement.prose
                )),
                crate::model::RequirementDisposition::Covered { .. } => None,
            })
            .collect();
        let prompt_text = assemble_terminal_review_prompt(
            role,
            &TerminalReviewPromptContext {
                objective: &state.objective,
                limitations: &limitations,
                nonce: &nonce,
                skills: &skills,
            },
        );
        let prompt_hash = hex::encode(Sha256::digest(prompt_text.as_bytes()));
        let prompt = self
            .store
            .blobs()
            .externalize(PayloadRef::inline(prompt_text))?;
        let requested_at_ms = self.clock.now_ms();
        let previous_failure = match state.terminal_review.outcome.as_ref() {
            Some(crate::model::ReviewOutcome::Failed { failure }) => Some(failure),
            _ => None,
        };
        let not_before_ms = retry_not_before(requested_at_ms, previous_failure);
        let initial_secs = role
            .timeout_secs
            .unwrap_or(state.config.execution.default_timeout_secs);
        let event = NewEvent::new(MissionEvent::TerminalReviewRequested {
            attempt_no: intent.attempt_no,
            effect_id,
            role: intent.role,
            runtime: role
                .runtime
                .clone()
                .unwrap_or_else(|| state.runtime.clone()),
            prompt,
            judged_sha: intent.judged_sha,
            nonce,
            requested_at_ms,
            not_before_ms,
            deadline_ms: resolved_deadline(not_before_ms, initial_secs)?,
            budget_deadline_ms: resolved_deadline(
                not_before_ms,
                initial_secs.max(state.config.execution.max_task_time_secs),
            )?,
        })
        .with_prompt_hash(prompt_hash);
        self.append_idempotent(&state.mission_id, state.head, &[event])
            .await
    }

    async fn materialize_oracle_requests(
        &self,
        state: &MissionState,
        intents: Vec<OracleDispatchIntent>,
    ) -> Result<()> {
        let events: Vec<NewEvent> = intents
            .into_iter()
            .map(|intent| {
                let effect_id = effect_id_for(&[
                    "oracle",
                    state.mission_id.as_str(),
                    intent.oracle.as_str(),
                    &intent.judged_sha,
                    &intent.attempt_no.to_string(),
                ]);
                let requested_at_ms = self.clock.now_ms();
                let not_before_ms =
                    retry_not_before(requested_at_ms, state.oracle_failures.get(&intent.oracle));
                Ok(NewEvent::new(MissionEvent::OracleRunRequested {
                    assertion_ids: intent.assertion_ids,
                    oracle: intent.oracle,
                    judged_sha: intent.judged_sha,
                    attempt_no: intent.attempt_no,
                    effect_id,
                    requested_at_ms,
                    not_before_ms,
                    deadline_ms: resolved_deadline(
                        not_before_ms,
                        state.config.execution.default_timeout_secs,
                    )?,
                }))
            })
            .collect::<Result<Vec<_>>>()?;
        self.append_idempotent(&state.mission_id, state.head, &events)
            .await
    }

    fn externalize_handoff(&self, handoff: Handoff) -> Result<Handoff> {
        Ok(match handoff {
            Handoff::Work {
                done,
                report,
                request_attention,
            } => Handoff::Work {
                done,
                report: self.store.blobs().externalize(report)?,
                request_attention,
            },
            Handoff::Validate {
                done,
                report,
                items,
                passed,
                request_attention,
            } => Handoff::Validate {
                done,
                report: self.store.blobs().externalize(report)?,
                items,
                passed,
                request_attention,
            },
            Handoff::Review {
                done,
                report,
                passed,
                gaps,
                nonce,
            } => Handoff::Review {
                done,
                report: self.store.blobs().externalize(report)?,
                passed,
                // Typed and KB-scale, like `proposal` below — stays inline.
                gaps,
                nonce,
            },
            Handoff::Plan {
                done,
                report,
                proposal,
                request_attention,
            } => Handoff::Plan {
                done,
                report: self.store.blobs().externalize(report)?,
                // The proposal stays inline (KB-scale, typed); only the prose
                // report is externalized above the blob threshold.
                proposal,
                request_attention,
            },
        })
    }
}

fn resolve_task_assignment(
    previous: Option<&crate::model::TaskRuntimeState>,
    required_base: &str,
    max_attempts: u32,
) -> (String, u32, bool) {
    let retrying_failure =
        previous.is_some_and(|task| task.automatic_retry_remaining(max_attempts));
    let base_sha = if retrying_failure {
        previous
            .and_then(|task| task.workspace_base_sha.clone())
            .unwrap_or_else(|| required_base.to_string())
    } else {
        required_base.to_string()
    };
    let previous_epoch = previous.map_or(0, |task| task.assignment_epoch);
    let recreate = previous.and_then(|task| task.workspace_base_sha.as_deref())
        != Some(base_sha.as_str())
        && !retrying_failure;
    let epoch = match (previous_epoch, recreate) {
        (0, _) => 1,
        (epoch, true) => epoch.saturating_add(1),
        (epoch, false) => epoch,
    };
    (base_sha, epoch, recreate)
}

/// Content-derived effect identity: stable across resume, unique per
/// logical effect.
fn effect_id_for(parts: &[&str]) -> EffectId {
    EffectId::for_parts(parts)
}

fn interrupted_outcome(effect_id: &EffectId, effect: &InflightEffect) -> NewEvent {
    let mut evidence = lionclaw_runtime_api::TypedFailureEvidence::new(
        Some("driver.interrupted".to_string()),
        "the previous mission driver exited before recording an outcome; its resources were cleaned and the effect was not replayed",
    );
    evidence.stop_reason = Some("mission driver exited".into());
    match effect {
        InflightEffect::RoleRun {
            runtime_configuration,
            ..
        }
        | InflightEffect::TerminalReview {
            runtime_configuration,
            ..
        } => {
            if let Some(configuration) = runtime_configuration {
                evidence.configuration = runtime_configuration_evidence(configuration);
            }
        }
        InflightEffect::OracleRun { .. } => {}
    }
    let failure = TypedFailure::Interrupted {
        evidence: Box::new(evidence),
    };
    failed_outcome(effect_id, effect, failure)
}

fn failed_outcome(
    effect_id: &EffectId,
    effect: &InflightEffect,
    failure: TypedFailure,
) -> NewEvent {
    NewEvent::new(match effect {
        InflightEffect::RoleRun {
            namespace,
            task_id,
            attempt_no,
            ..
        } => MissionEvent::RoleRunCompleted {
            namespace: *namespace,
            task_id: task_id.clone(),
            attempt_no: *attempt_no,
            effect_id: effect_id.clone(),
            outcome: Err(failure),
        },
        InflightEffect::OracleRun {
            assertion_ids,
            oracle,
            judged_sha,
            attempt_no,
            ..
        } => MissionEvent::OracleRunCompleted {
            assertion_ids: assertion_ids.clone(),
            oracle: oracle.clone(),
            judged_sha: judged_sha.clone(),
            attempt_no: *attempt_no,
            effect_id: effect_id.clone(),
            outcome: Err(failure),
        },
        InflightEffect::TerminalReview {
            attempt_no,
            judged_sha,
            ..
        } => MissionEvent::TerminalReviewCompleted {
            attempt_no: *attempt_no,
            effect_id: effect_id.clone(),
            judged_sha: judged_sha.clone(),
            outcome: Err(failure),
        },
    })
}

fn stopped_before_start_outcome(
    effect_id: &EffectId,
    effect: &InflightEffect,
    reason: &str,
) -> NewEvent {
    let mut evidence = lionclaw_runtime_api::TypedFailureEvidence::new(
        Some("control.stopped_before_start".into()),
        "operator stopped the effect during its recorded retry backoff",
    );
    evidence.stop_reason = Some(reason.to_string());
    let failure = TypedFailure::OperatorStopped {
        evidence: Box::new(evidence),
    };
    failed_outcome(effect_id, effect, failure)
}

enum SettlementKind {
    Abort,
    Stop,
    Deadline,
}

fn settlement_failure(
    state: &MissionState,
    effect_id: &EffectId,
    outcome: &NewEvent,
) -> Option<TypedFailure> {
    let (code, detail, reason, category) = match &state.phase {
        MissionPhase::Aborted { reason } => (
            "control.aborted_before_settlement",
            "mission abort became durable before the effect outcome",
            reason.clone(),
            SettlementKind::Abort,
        ),
        _ => {
            if let Some(reason) = state.stop_requests.get(effect_id) {
                (
                    "control.stopped_before_settlement",
                    "operator stop became durable before the effect outcome",
                    reason.clone(),
                    SettlementKind::Stop,
                )
            } else if let Some(deadline_ms) = state.reached_deadlines.get(effect_id) {
                (
                    "control.deadline_before_settlement",
                    "the recorded effect deadline became durable before the effect outcome",
                    format!("deadline reached at {deadline_ms}"),
                    SettlementKind::Deadline,
                )
            } else {
                return None;
            }
        }
    };
    let mut evidence = outcome
        .settlement_evidence
        .clone()
        .unwrap_or_else(|| outcome_failure_evidence(&outcome.event));
    evidence.code = Some(code.into());
    evidence.detail = detail.into();
    evidence.stop_reason = Some(reason);
    Some(
        match category {
            SettlementKind::Abort => TypedFailure::OperatorAborted {
                evidence: Box::new(evidence),
            },
            SettlementKind::Stop => TypedFailure::OperatorStopped {
                evidence: Box::new(evidence),
            },
            SettlementKind::Deadline => TypedFailure::DeadlineExhausted {
                evidence: Box::new(evidence),
            },
        }
        .projected(),
    )
}

fn outcome_failure_evidence(outcome: &MissionEvent) -> lionclaw_runtime_api::TypedFailureEvidence {
    match outcome {
        MissionEvent::RoleRunCompleted { outcome, .. } => match outcome {
            Ok(success) => lionclaw_runtime_api::TypedFailureEvidence {
                final_response: inline_payload(&success.final_response),
                configuration: runtime_configuration_evidence(&success.runtime_configuration),
                ..Default::default()
            },
            Err(failure) => failure.evidence().clone(),
        },
        MissionEvent::OracleRunCompleted { outcome, .. } => match outcome {
            Ok(success) => lionclaw_runtime_api::TypedFailureEvidence {
                exit_code: Some(success.exit_code),
                stderr: inline_payload(&success.stderr),
                ..Default::default()
            },
            Err(failure) => failure.evidence().clone(),
        },
        MissionEvent::TerminalReviewCompleted { outcome, .. } => match outcome {
            Ok(success) => lionclaw_runtime_api::TypedFailureEvidence {
                final_response: inline_payload(&success.final_response),
                configuration: runtime_configuration_evidence(&success.runtime_configuration),
                ..Default::default()
            },
            Err(failure) => failure.evidence().clone(),
        },
        _ => lionclaw_runtime_api::TypedFailureEvidence::default(),
    }
}

fn inline_payload(payload: &PayloadRef) -> String {
    match payload {
        PayloadRef::Inline { text } => text.clone(),
        PayloadRef::Blob(_) => String::new(),
    }
}

/// Record a decision without a full engine (the CLI's `decide` needs
/// only the store). Folds current state, validates fail-closed, appends.
pub async fn record_decision(
    store: &MissionStore,
    now_ms: i64,
    mission_id: &MissionId,
    attention_id: &str,
    action: crate::model::DecisionAction,
    justification: &str,
) -> Result<()> {
    let state = store.require_state(mission_id).await?;
    // Preserve the typed `DecisionError` as the error source (its `Display` is
    // already specific: unknown item vs illegal action for the item's kind), so
    // a JSON caller sees the real reason, not a flattened string.
    crate::model::validate_decision(&state, attention_id, &action, justification)?;
    let event = NewEvent::new(MissionEvent::DecisionRecorded {
        attention_id: attention_id.to_string(),
        action: action.clone(),
        justification: justification.to_string(),
    });
    let mut events = vec![event];
    if action == crate::model::DecisionAction::Abort {
        events.push(NewEvent::new(MissionEvent::MissionAborted {
            reason: justification.to_string(),
        }));
    }
    store
        .append(mission_id, state.head, &events, now_ms)
        .await?;
    Ok(())
}

/// Validate and append a control against the exact replayed effect generation.
/// A concurrent outcome makes the optimistic append conflict, so stale races
/// fail instead of leaking onto a successor.
pub async fn record_control(
    store: &MissionStore,
    now_ms: i64,
    mission_id: &MissionId,
    effect_id: &EffectId,
    action: crate::model::ControlAction,
    reason: &str,
) -> Result<()> {
    if reason.trim().is_empty() {
        bail!("control reason must not be empty");
    }
    let state = store.require_state(mission_id).await?;
    if state.phase.is_terminal() {
        bail!("mission '{mission_id}' is terminal; controls are not legal");
    }
    match &action {
        crate::model::ControlAction::Stop => {
            if !state.inflight.contains_key(effect_id) {
                bail!("effect '{effect_id}' is not active; control is stale");
            }
            if state.reached_deadlines.contains_key(effect_id) {
                bail!("effect '{effect_id}' cancellation already began; control is stale");
            }
        }
        crate::model::ControlAction::ExtendDeadline {
            old_deadline_ms,
            new_deadline_ms,
            automatic,
        } => {
            if *automatic {
                bail!("automatic controls are engine-owned");
            }
            let Some(effect) = state.inflight.get(effect_id) else {
                bail!("effect '{effect_id}' is not active; control is stale");
            };
            if state.reached_deadlines.contains_key(effect_id) {
                bail!("effect '{effect_id}' cancellation already began; control is stale");
            }
            if effect.deadline_ms() != *old_deadline_ms {
                bail!("effect '{effect_id}' deadline changed; control is stale");
            }
            if *new_deadline_ms < *old_deadline_ms || *new_deadline_ms <= now_ms {
                bail!("extended deadline must be finite, nondecreasing, and in the future");
            }
        }
        crate::model::ControlAction::Continue { automatic } => {
            if *automatic {
                bail!("automatic controls are engine-owned");
            }
            if !state.parked_effects.contains_key(effect_id) {
                bail!("effect '{effect_id}' is not parked; control is stale");
            }
        }
    }
    store
        .append(
            mission_id,
            state.head,
            &[NewEvent::new(MissionEvent::ControlRequested {
                effect_id: effect_id.clone(),
                action,
                reason: reason.trim().to_string(),
            })],
            now_ms,
        )
        .await?;
    Ok(())
}

fn transient_backoff_ms(consecutive_failures: u32, adapter_retry_after_ms: Option<u64>) -> u64 {
    const MAX_BACKOFF_MS: u64 = 30_000;
    let policy_ms = 1_000_u64 << consecutive_failures.min(4);
    policy_ms
        .max(adapter_retry_after_ms.unwrap_or_default())
        .min(MAX_BACKOFF_MS)
}

fn retry_not_before(now_ms: i64, failure: Option<&TypedFailure>) -> i64 {
    failure
        .filter(|failure| failure.is_transient())
        .and_then(TypedFailure::next_eligible_at_ms)
        .unwrap_or(now_ms)
        .max(now_ms)
}

/// Checkpoint selection is pure: recorded state policy plus the just-produced
/// outcome. The driver only performs the returned action.
fn checkpoint_after(
    event: &MissionEvent,
    policy: &crate::model::ExecutionPolicy,
) -> Option<(bool, &'static str)> {
    match event {
        MissionEvent::RoleRunCompleted {
            outcome: Ok(success),
            ..
        } if success.artifact.is_some() => Some((
            policy.auto_continue_candidate,
            "mission policy auto-continued captured candidate",
        )),
        MissionEvent::RoleRunCompleted {
            outcome:
                Ok(crate::model::RoleRunSuccess {
                    handoff: Handoff::Validate { .. },
                    ..
                }),
            ..
        } => Some((
            policy.auto_continue_proof,
            "mission policy auto-continued advisory proof completion",
        )),
        MissionEvent::RoleRunCompleted { outcome: Ok(_), .. } => {
            Some((false, "agent response checkpoint"))
        }
        MissionEvent::OracleRunCompleted { outcome: Ok(_), .. }
        | MissionEvent::TerminalReviewCompleted { outcome: Ok(_), .. } => Some((
            policy.auto_continue_proof,
            "mission policy auto-continued proof completion",
        )),
        _ => None,
    }
}

fn has_owned_oracle_sibling(
    state: &MissionState,
    effect_id: &EffectId,
    effect: &InflightEffect,
) -> bool {
    matches!(effect, InflightEffect::OracleRun { .. })
        && state.inflight.iter().any(|(sibling_id, sibling)| {
            sibling_id != effect_id && matches!(sibling, InflightEffect::OracleRun { .. })
        })
}

fn resolved_deadline(requested_at_ms: i64, duration_secs: u64) -> Result<i64> {
    let duration_ms = duration_secs
        .checked_mul(1_000)
        .context("execution duration overflows milliseconds")?;
    let duration_ms = i64::try_from(duration_ms).context("execution duration is too large")?;
    requested_at_ms
        .checked_add(duration_ms)
        .context("execution deadline overflows epoch milliseconds")
}

fn runtime_configuration_evidence(
    evidence: &crate::model::RuntimeConfigurationEvidence,
) -> lionclaw_runtime_api::AppliedRuntimeConfiguration {
    lionclaw_runtime_api::AppliedRuntimeConfiguration {
        requested_model: evidence.requested_model.clone(),
        applied_model: evidence.applied_model.clone(),
        model_confirmation: evidence.model_confirmation,
        requested_mode: evidence.requested_mode.clone(),
        applied_mode: evidence.applied_mode.clone(),
        mode_confirmation: evidence.mode_confirmation,
    }
}

fn invalid_role_outcome(
    code: impl Into<String>,
    detail: impl Into<String>,
    outcome: &crate::ports::RoleRunOutcome,
) -> TypedFailure {
    let mut failure = TypedFailure::invalid(code, detail);
    failure.evidence_mut().final_response = outcome.final_response.clone();
    failure.evidence_mut().configuration =
        runtime_configuration_evidence(&outcome.runtime_configuration);
    failure.projected()
}

#[cfg(test)]
mod assignment_tests {
    use super::*;
    use crate::model::{TaskRuntimeState, TaskStatus};

    fn task(status: TaskStatus, base: &str, epoch: u32) -> TaskRuntimeState {
        TaskRuntimeState {
            status,
            attempts: epoch,
            consecutive_failures: 0,
            last_report: None,
            last_failure: None,
            feedback: Vec::new(),
            last_runtime_configuration: None,
            workspace_base_sha: Some(base.to_string()),
            assignment_epoch: epoch,
            final_response: None,
        }
    }

    #[test]
    fn fresh_assignment_rebases_only_when_the_required_deliverable_moved() {
        assert_eq!(
            resolve_task_assignment(None, "h1", 3),
            ("h1".into(), 1, true)
        );
        let pending = task(TaskStatus::Pending, "h1", 1);
        assert_eq!(
            resolve_task_assignment(Some(&pending), "h2", 3),
            ("h2".into(), 2, true)
        );
        assert_eq!(
            resolve_task_assignment(Some(&pending), "h1", 3),
            ("h1".into(), 1, false)
        );
    }

    #[test]
    fn retry_retains_the_original_workspace_base_and_epoch() {
        let mut failed = task(TaskStatus::Failed, "h1", 4);
        failed.consecutive_failures = 1;
        failed.last_failure = Some(TypedFailure::transient(
            "runtime.fixture",
            "driver died",
            None,
        ));
        assert_eq!(
            resolve_task_assignment(Some(&failed), "h2", 3),
            ("h1".into(), 4, false)
        );
    }

    #[test]
    fn transient_backoff_is_bounded_and_honors_structured_retry_after() {
        assert_eq!(transient_backoff_ms(0, None), 1_000);
        assert_eq!(transient_backoff_ms(1, Some(2_500)), 2_500);
        assert_eq!(transient_backoff_ms(99, Some(90_000)), 30_000);
    }

    #[test]
    fn largest_representable_duration_materializes_at_the_zero_epoch() {
        assert_eq!(
            resolved_deadline(0, crate::model::MAX_EXECUTION_DURATION_SECS).unwrap(),
            (crate::model::MAX_EXECUTION_DURATION_SECS * 1_000) as i64
        );
    }
}
