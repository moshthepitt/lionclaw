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
use sha2::{Digest, Sha256};

use crate::driver_lock::DriverGuard;
use crate::mission_type::MissionType;
use crate::model::{
    step, validate_plan_proposal, EffectId, Handoff, InflightEffect, MissionEvent, MissionId,
    MissionPhase, MissionState, OracleDispatchIntent, PayloadRef, PlanProposal, ProposalError,
    RoleDispatchIntent, RunErrorKind, RunFailure, StepDecision, TaskId,
    TerminalReviewDispatchIntent,
};
use crate::ports::{
    Clock, EffectCleaner, EffectCleanupRequest, OracleRunRequest, OracleRunner, RoleRunRequest,
    RoleRunner,
};
use crate::prompt::{
    assemble_planning_prompt, assemble_role_prompt, assemble_terminal_review_prompt,
    PlanningPromptContext, PromptContext, TerminalReviewPromptContext,
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
        let Some(_guard) = DriverGuard::try_acquire(&self.store.driver_lock_path(mission_id))?
        else {
            let state = self.store.require_state(mission_id).await?;
            return Ok(MissionView::from_state(state, true));
        };
        if !self.recover_interrupted(mission_id).await? {
            return Ok(MissionView::from_state(
                self.load_state(mission_id).await?,
                false,
            ));
        }
        self.drive(mission_id).await?;
        // Persist a fold snapshot before parking or exiting so the next
        // invocation resumes without re-folding the whole log.
        let state = self.load_state(mission_id).await?;
        self.store
            .save_snapshot(&state, self.clock.now_ms())
            .await?;
        Ok(MissionView::from_state(state, false))
    }

    async fn drive(&self, mission_id: &MissionId) -> Result<()> {
        for _ in 0..MAX_LOOP_ITERATIONS {
            let state = self.load_state(mission_id).await?;
            if !state.inflight.is_empty() {
                if self.drive_one(&state).await? {
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
            self.append_outcome(
                &state.mission_id,
                state.head,
                interrupted_outcome(effect_id, effect),
            )
            .await?;
        }
    }

    /// Execute one request materialized by this driver, clean its transient
    /// resources, then durably record the outcome.
    async fn drive_one(&self, state: &MissionState) -> Result<bool> {
        let Some((effect_id, effect)) = state.inflight.iter().next() else {
            return Ok(false);
        };
        let outcome = match effect {
            InflightEffect::RoleRun { .. } => {
                self.execute_role_run(state, effect_id, effect).await?
            }
            InflightEffect::OracleRun { .. } => {
                self.execute_oracle_run(state, effect_id, effect).await?
            }
            InflightEffect::TerminalReview { .. } => {
                self.execute_terminal_review(state, effect_id, effect)
                    .await?
            }
        };
        let discard_artifact = !matches!(outcome.event, MissionEvent::RoleRunCompleted { .. });
        if !self
            .cleanup_effect(state, effect_id, discard_artifact)
            .await?
        {
            return Ok(false);
        }
        self.append_outcome(&state.mission_id, state.head, outcome)
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
                    failure: RunFailure {
                        kind: RunErrorKind::Infra,
                        detail: error.detail,
                    },
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
        initial_head: u64,
        outcome: NewEvent,
    ) -> Result<()> {
        let mut head = initial_head;
        for _ in 0..MAX_LOOP_ITERATIONS {
            match self
                .store
                .append(
                    mission_id,
                    head,
                    std::slice::from_ref(&outcome),
                    self.clock.now_ms(),
                )
                .await
            {
                Ok(_) | Err(AppendError::Duplicate { .. }) => return Ok(()),
                Err(AppendError::Conflict { .. }) => {
                    head = self.load_state(mission_id).await?.head;
                }
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
    ) -> Result<NewEvent> {
        let InflightEffect::RoleRun {
            task_id,
            attempt_no,
            role: role_name,
            runtime,
            prompt,
            base_sha,
            ..
        } = effect
        else {
            bail!("execute_role_run called with a non-role effect");
        };
        let attempt_no = *attempt_no;
        let failed = |kind: RunErrorKind, detail: String| {
            NewEvent::new(MissionEvent::RoleRunFailed {
                task_id: task_id.clone(),
                attempt_no,
                effect_id: effect_id.clone(),
                failure: RunFailure { kind, detail },
            })
        };
        let Some(role) = self.mission_type.roles.get(role_name) else {
            return Ok(failed(
                RunErrorKind::Launch,
                format!("role '{role_name}' is no longer provided by the mission type"),
            ));
        };
        let prompt_text = self.store.blobs().resolve(prompt)?;
        let skills = match self.resolve_role_skills(role) {
            Ok(skills) => skills,
            Err(detail) => return Ok(failed(RunErrorKind::Launch, detail)),
        };
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
            workspace_dir: state.workspace_dir.clone().into(),
            state_dir: self.store.lionclaw_dir().to_path_buf(),
        };
        let previous_failure = state
            .planning
            .tasks
            .get(task_id)
            .or_else(|| state.tasks.get(task_id))
            .and_then(|task| task.last_failure.as_ref());
        if attempt_no > 1 && previous_failure.is_some_and(crate::model::RunFailure::transient) {
            let seconds = 1_u64 << (attempt_no.saturating_sub(2)).min(2);
            tokio::time::sleep(Duration::from_secs(seconds)).await;
        }
        match self.role_runner.run(request).await {
            Ok(outcome) => {
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
                        return Ok(failed(
                            RunErrorKind::HandoffInvalid,
                            "planning author reported done but proposed no plan".to_string(),
                        ));
                    };
                    if let Err(error) =
                        validate_plan_proposal(state, proposal, &self.mission_type.inventory())
                    {
                        return Ok(failed(
                            RunErrorKind::HandoffInvalid,
                            format!("proposed plan is invalid: {error}"),
                        ));
                    }
                }
                let handoff = self.externalize_handoff(outcome.handoff)?;
                Ok(NewEvent::new(MissionEvent::RoleRunCompleted {
                    task_id: task_id.clone(),
                    attempt_no,
                    effect_id: effect_id.clone(),
                    handoff,
                    artifact: outcome.artifact,
                })
                .with_model_id(outcome.model_id))
            }
            Err(failure) => Ok(failed(failure.kind, failure.detail)),
        }
    }

    async fn execute_oracle_run(
        &self,
        state: &MissionState,
        effect_id: &EffectId,
        effect: &InflightEffect,
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
        let failed = |detail: String| {
            NewEvent::new(MissionEvent::OracleRunFailed {
                assertion_ids: assertion_ids.to_vec(),
                oracle: oracle.clone(),
                judged_sha: judged_sha.to_string(),
                attempt_no,
                effect_id: effect_id.clone(),
                failure: RunFailure {
                    kind: RunErrorKind::Infra,
                    detail,
                },
            })
        };
        let Some(oracle_path) = self.mission_type.oracles.get(oracle) else {
            return Ok(failed(format!(
                "oracle '{oracle}' is no longer provided by the mission type"
            )));
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
        };
        match self.oracle_runner.run(request).await {
            Ok(outcome) => Ok(NewEvent::new(MissionEvent::OracleRunCompleted {
                assertion_ids: assertion_ids.to_vec(),
                oracle: oracle.clone(),
                judged_sha: judged_sha.to_string(),
                attempt_no,
                effect_id: effect_id.clone(),
                exit_code: outcome.exit_code,
                exit_signal: outcome.exit_signal,
                stdout: self.store.blobs().payload_from_bytes(&outcome.stdout)?,
                stderr: self.store.blobs().payload_from_bytes(&outcome.stderr)?,
                prepared_inputs: outcome.prepared_inputs,
                duration_ms: outcome.duration_ms,
            })),
            Err(failure) => Ok(failed(failure.detail)),
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
        let failed = |kind: RunErrorKind, detail: String| {
            NewEvent::new(MissionEvent::TerminalReviewFailed {
                attempt_no,
                effect_id: effect_id.clone(),
                judged_sha: judged_sha.clone(),
                failure: RunFailure { kind, detail },
            })
        };
        let Some(role) = self.mission_type.roles.get(role_name) else {
            return Ok(failed(
                RunErrorKind::Launch,
                format!(
                    "terminal-review role '{role_name}' is no longer provided by the mission type"
                ),
            ));
        };
        let skills = match self.resolve_role_skills(role) {
            Ok(skills) => skills,
            Err(detail) => return Ok(failed(RunErrorKind::Launch, detail)),
        };
        let prompt_text = self.store.blobs().resolve(prompt)?;
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
            workspace_dir: state.workspace_dir.clone().into(),
            state_dir: self.store.lionclaw_dir().to_path_buf(),
        };
        let outcome = match self.role_runner.run(request).await {
            Ok(outcome) => outcome,
            Err(failure) => return Ok(failed(failure.kind, failure.detail)),
        };
        let Handoff::Review {
            done,
            report,
            passed,
            gaps,
            nonce: echoed,
        } = outcome.handoff
        else {
            // Unreachable via the runner's schema check; fail closed anyway.
            return Ok(failed(
                RunErrorKind::HandoffInvalid,
                "terminal reviewer handed back a non-review handoff".to_string(),
            ));
        };
        if echoed != *nonce {
            return Ok(failed(
                RunErrorKind::HandoffInvalid,
                "handoff nonce mismatch: the handoff was not written by the reviewer".to_string(),
            ));
        }
        if !done {
            return Ok(failed(
                RunErrorKind::TurnFailed,
                "reviewer handed off done=false: the review itself did not complete".to_string(),
            ));
        }
        Ok(NewEvent::new(MissionEvent::TerminalReviewCompleted {
            attempt_no,
            effect_id: effect_id.clone(),
            judged_sha: judged_sha.clone(),
            passed,
            gaps,
            report: self.store.blobs().externalize(report)?,
        })
        .with_model_id(outcome.model_id))
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
                "Previous attempt failed ({:?}): {}",
                failure.kind, failure.detail
            ));
        }
        for item in &task.feedback {
            feedback.push(crate::evidence::render_feedback(self.store.blobs(), item)?);
        }
        Ok(feedback)
    }

    /// Assemble an execution role's prompt (`("role", …)` effect namespace).
    fn assemble_execution_request(
        &self,
        state: &MissionState,
        role: &crate::mission_type::RoleDefinition,
        intent: &RoleDispatchIntent,
    ) -> Result<(String, &'static str)> {
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
        Ok((prompt, "role"))
    }

    /// Assemble a planning role's prompt (`("plan-role", …)` namespace). Threads
    /// the mission type's playbook + execution-role/oracle inventories +
    /// upstream planning reports through a separate assembler.
    fn assemble_planning_request(
        &self,
        state: &MissionState,
        role: &crate::mission_type::RoleDefinition,
        intent: &RoleDispatchIntent,
    ) -> Result<(String, &'static str)> {
        let task = state
            .config
            .planning
            .tasks
            .iter()
            .find(|t| t.id == intent.task_id)
            .context("dispatched planning task not in the DAG")?;
        let upstream_reports =
            self.resolve_upstream_reports(&state.planning.tasks, &task.depends_on)?;
        let mut feedback = state
            .planning
            .tasks
            .get(&task.id)
            .map(|runtime| self.resolve_task_feedback(runtime))
            .transpose()?
            .unwrap_or_default();
        if let Some(refinement) = &state.planning_input.refinement {
            feedback.push(crate::evidence::render_planning_refinement(
                self.store.blobs(),
                refinement,
            )?);
        }
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
                current_plan: state.plan.as_ref(),
                latest_rejected_plan: state
                    .planning_input
                    .latest_rejected_proposal
                    .as_ref()
                    .map(|proposal| &proposal.plan),
                playbook: self.mission_type.playbook.as_deref(),
                roles: &self.mission_type.roles,
                oracle_inventory: &oracle_inventory,
                task_body: &intent.body,
                upstream_reports: &upstream_reports,
                skills: &skills,
                feedback: &feedback,
            },
        );
        Ok((prompt, "plan-role"))
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
        let (prompt_text, effect_namespace) = if state.planning_base_revision.is_some() {
            self.assemble_planning_request(state, role, &intent)?
        } else {
            self.assemble_execution_request(state, role, &intent)?
        };
        let prompt_hash = hex::encode(Sha256::digest(prompt_text.as_bytes()));
        let prompt = self
            .store
            .blobs()
            .externalize(PayloadRef::inline(prompt_text))?;
        let effect_id = effect_id_for(&[
            effect_namespace,
            state.mission_id.as_str(),
            intent.task_id.as_str(),
            &intent.attempt_no.to_string(),
            &prompt_hash,
        ]);
        let event = NewEvent::new(MissionEvent::RoleRunRequested {
            task_id: intent.task_id,
            attempt_no: intent.attempt_no,
            effect_id,
            role: intent.role,
            runtime: role
                .runtime
                .clone()
                .unwrap_or_else(|| state.runtime.clone()),
            prompt,
            base_sha: intent.base_sha,
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
                NewEvent::new(MissionEvent::OracleRunRequested {
                    assertion_ids: intent.assertion_ids,
                    oracle: intent.oracle,
                    judged_sha: intent.judged_sha,
                    attempt_no: intent.attempt_no,
                    effect_id,
                })
            })
            .collect();
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

/// Content-derived effect identity: stable across resume, unique per
/// logical effect.
fn effect_id_for(parts: &[&str]) -> EffectId {
    EffectId::for_parts(parts)
}

fn interrupted_outcome(effect_id: &EffectId, effect: &InflightEffect) -> NewEvent {
    let failure = RunFailure {
        kind: RunErrorKind::Interrupted,
        detail: "the previous mission driver exited before recording an outcome; its resources were cleaned and the effect was not replayed".to_string(),
    };
    NewEvent::new(match effect {
        InflightEffect::RoleRun {
            task_id,
            attempt_no,
            ..
        } => MissionEvent::RoleRunFailed {
            task_id: task_id.clone(),
            attempt_no: *attempt_no,
            effect_id: effect_id.clone(),
            failure,
        },
        InflightEffect::OracleRun {
            assertion_ids,
            oracle,
            judged_sha,
            attempt_no,
            ..
        } => MissionEvent::OracleRunFailed {
            assertion_ids: assertion_ids.clone(),
            oracle: oracle.clone(),
            judged_sha: judged_sha.clone(),
            attempt_no: *attempt_no,
            effect_id: effect_id.clone(),
            failure,
        },
        InflightEffect::TerminalReview {
            attempt_no,
            judged_sha,
            ..
        } => MissionEvent::TerminalReviewFailed {
            attempt_no: *attempt_no,
            effect_id: effect_id.clone(),
            judged_sha: judged_sha.clone(),
            failure,
        },
    })
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
