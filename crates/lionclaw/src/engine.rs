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

use anyhow::{bail, ensure, Context, Result};
use lionclaw_runtime_api::TypedFailure;
use sha2::{Digest, Sha256};

use crate::driver_lock::DriverGuard;
use crate::mission_type::MissionType;
use crate::model::{
    step, validate_mission_proposal, EffectEventClass, EffectId, Handoff, InflightEffect,
    MissionEvent, MissionId, MissionPhase, MissionProposal, MissionState, OracleDispatchIntent,
    OracleRunSuccess, PayloadRef, ProposalError, RoleDispatchIntent, RoleInstance, RoleTurnSuccess,
    StepDecision, TaskId, MAX_ROLE_REPORT_BYTES,
};
use crate::ports::{
    ArtifactCapture, Clock, EffectCleaner, EffectCleanupRequest, ExecutionControl,
    OracleRunRequest, OracleRunner, RoleRunner, RoleTurnRequest,
};
use crate::prompt::{
    render, ExecutionContext, GapReviewPromptContext, JudgmentContext, PlanningPromptContext,
    PlanningPromptInput, PlanningPromptRefinement, TurnContext,
};
use crate::resources::MissionDirs;
use crate::store::{AppendError, MissionStore, NewEvent};

pub struct Engine {
    store: MissionStore,
    mission_type: MissionType,
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

enum EffectCleanupDisposition {
    Complete(Option<TypedFailure>),
    Blocked,
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
                let observation = activity.borrow().clone();
                match crate::activity::publish_observed(
                    &store,
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
    AwaitingLead,
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
            Self::AwaitingLead => "awaiting_lead",
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
    /// Project one replayed state into its operator disposition and actions.
    pub fn from_state(state: MissionState, driver_running: bool) -> Self {
        let terminal = state.phase.is_terminal();
        let disposition = if terminal && state.inflight.is_empty() {
            MissionDisposition::Terminal
        } else if driver_running {
            MissionDisposition::Running
        } else if state
            .cleanup_failure
            .as_ref()
            .is_some_and(|failure| state.inflight.contains_key(&failure.effect_id))
            || terminal
        {
            MissionDisposition::CleanupBlocked
        } else if state.conversations.iter().any(|(id, conversation)| {
            conversation.lifecycle == crate::model::ConversationLifecycle::AwaitingLead
                && state.conversation_is_messageable(id)
        }) {
            MissionDisposition::AwaitingLead
        } else if !state.open_attention.is_empty() {
            MissionDisposition::Parked
        } else if state.phase == MissionPhase::Planning && state.proposal.is_none() {
            MissionDisposition::AwaitingPlan
        } else {
            MissionDisposition::Ready
        };
        Self { state, disposition }
    }

    pub fn next_actions(&self) -> Vec<&'static str> {
        let can_send = self
            .state
            .conversations
            .keys()
            .any(|id| self.state.conversation_accepts_message(id));
        let can_decide = !self.state.open_attention.is_empty();
        let can_preserve = self.state.parked_effects.keys().any(|effect_id| {
            self.state
                .parked_continue_is_legal(effect_id, crate::model::ContinueMode::Preserve)
        });
        let can_recreate = self.state.parked_effects.keys().any(|effect_id| {
            self.state
                .parked_continue_is_legal(effect_id, crate::model::ContinueMode::RecreateWorkspace)
        });
        let continue_actions = [
            can_preserve.then_some("mission continue"),
            can_recreate.then_some("mission continue --recreate"),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        let mut actions = match self.disposition {
            MissionDisposition::Ready => vec!["mission advance"],
            MissionDisposition::Running => vec!["mission status"],
            MissionDisposition::AwaitingLead if can_send => vec!["mission send"],
            MissionDisposition::AwaitingLead => Vec::new(),
            MissionDisposition::AwaitingPlan => vec!["mission plan propose"],
            MissionDisposition::Parked if !continue_actions.is_empty() => {
                let mut actions = continue_actions.clone();
                actions.push("mission decide");
                actions
            }
            MissionDisposition::Parked => vec!["mission decide"],
            MissionDisposition::CleanupBlocked => vec!["mission advance", "mission log"],
            MissionDisposition::Terminal => {
                let mut actions = vec!["mission report"];
                if matches!(self.state.phase, MissionPhase::Done { .. })
                    && self.state.deliverable_head() != self.state.base_sha
                {
                    actions.push("mission apply");
                }
                actions
            }
        };
        if self.disposition == MissionDisposition::AwaitingLead {
            actions.extend(continue_actions);
        }
        if self.disposition == MissionDisposition::AwaitingLead && can_decide {
            actions.push("mission decide");
        }
        if can_send && !actions.contains(&"mission send") && !self.state.phase.is_terminal() {
            actions.push("mission send");
        }
        if !self.state.phase.is_terminal() {
            actions.push("mission abort");
        }
        actions
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

/// Reconcile disposable resources after any current mission driver releases
/// ownership. The state is reloaded only after acquiring the lock, so an event
/// appended between a driver's final fold and lock release cannot be missed.
pub(crate) async fn reconcile_disposable_conversation_resources(
    store: &MissionStore,
    mission_id: &MissionId,
) -> Result<()> {
    let lock_path = store.driver_lock_path(mission_id);
    let guard = tokio::task::spawn_blocking(move || DriverGuard::acquire(&lock_path))
        .await
        .context("joining disposable resource lock waiter")??;
    reconcile_disposable_conversation_resources_with_guard(store, mission_id, &guard).await
}

/// Reconcile only if no driver currently owns the mission. This is used by
/// nonblocking operator observations such as plain `mission advance`; a live
/// driver remains responsible for its own final cleanup.
pub(crate) async fn reconcile_disposable_conversation_resources_if_idle(
    store: &MissionStore,
    mission_id: &MissionId,
) -> Result<()> {
    let Some(guard) = DriverGuard::try_acquire(&store.driver_lock_path(mission_id))? else {
        return Ok(());
    };
    reconcile_disposable_conversation_resources_with_guard(store, mission_id, &guard).await
}

async fn reconcile_disposable_conversation_resources_with_guard(
    store: &MissionStore,
    mission_id: &MissionId,
    guard: &DriverGuard,
) -> Result<()> {
    let state = store.require_state(mission_id).await?;
    cleanup_settled_conversation_scratch(store, &state, guard).await
}

/// Remove only disposable scratch for conversations whose folded lifecycle is
/// settled and which have no inflight role owner. The driver guard makes the
/// folded ownership check stable for the duration of the removal.
async fn cleanup_settled_conversation_scratch(
    store: &MissionStore,
    state: &MissionState,
    _driver_guard: &DriverGuard,
) -> Result<()> {
    let mission_dirs = MissionDirs::new(store.lionclaw_dir(), &state.mission_id);
    for (role_instance, conversation) in &state.conversations {
        if !matches!(
            conversation.lifecycle,
            crate::model::ConversationLifecycle::Completed
                | crate::model::ConversationLifecycle::Retired
        ) || state.inflight.values().any(|effect| {
            matches!(
                effect,
                InflightEffect::RoleTurn {
                    role_instance: active,
                    ..
                } if active == role_instance
            )
        }) {
            continue;
        }
        mission_dirs
            .role(role_instance)
            .remove_disposable_scratch()
            .await
            .with_context(|| {
                format!("cleaning disposable scratch for role instance '{role_instance}'")
            })?;
    }
    Ok(())
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

impl Engine {
    /// Reconstruct a role turn from durable identities without making its
    /// transient prose part of the log or any projection.
    async fn reconstruct_role_prompt(
        &self,
        mission_id: &MissionId,
        effect_id: &EffectId,
    ) -> Result<String> {
        let events = self.store.load(mission_id).await?;
        let request = events.iter().find(|envelope| {
            matches!(&envelope.event, MissionEvent::RoleTurnRequested { effect_id: id, .. } if id == effect_id)
        }).context("role request not found")?;
        let request_seq = request.sequence_no;
        let (role_instance, team_revision, template, expected_hash) = match &request.event {
            MissionEvent::RoleTurnRequested {
                role_instance,
                team_revision,
                prompt_template,
                prompt_hash,
                ..
            } => (
                role_instance.clone(),
                *team_revision,
                *prompt_template,
                prompt_hash.clone(),
            ),
            _ => unreachable!(),
        };
        let prefix: Vec<_> = events
            .into_iter()
            .filter(|event| event.sequence_no < request_seq)
            .collect();
        let state =
            crate::model::fold(prefix).context("role request prefix has no creation event")?;
        let StepDecision::DispatchRole(intent) = step(&state) else {
            bail!("role request prefix no longer reconstructs its dispatch")
        };
        ensure!(
            intent.role_instance == role_instance && intent.team_revision == team_revision,
            "role request prefix reconstructed a different team assignment"
        );
        let role = state
            .team_history
            .get(&team_revision)
            .and_then(|team| team.role(&role_instance))
            .context("role instance missing from recorded team revision")?;
        let dialogue =
            materialize_conversation_messages(self, &state, &role_instance, state.head).await?;
        let prompt = self.assemble_role_request(&state, role, &intent, &dialogue)?;
        let actual_template = crate::model::role_prompt_template(role.output);
        let actual_hash = hex::encode(Sha256::digest(prompt.as_bytes()));
        if template != actual_template || expected_hash != actual_hash {
            bail!("canonical role prompt drift")
        }
        Ok(prompt)
    }

    #[cfg(feature = "testing")]
    pub async fn reconstruct_role_prompt_for_testing(
        &self,
        mission_id: &MissionId,
        effect_id: &EffectId,
    ) -> Result<String> {
        self.reconstruct_role_prompt(mission_id, effect_id).await
    }
    fn resolve_role_skills(
        &self,
        state: &MissionState,
        role: &RoleInstance,
    ) -> std::result::Result<Vec<crate::mission_type::SkillPackage>, String> {
        role.skills
            .iter()
            .map(|name| {
                if let Some(package) = self.mission_type.skills.get(name) {
                    return Ok(package.clone());
                }
                let recorded = state.skills.get(name).ok_or_else(|| {
                    format!("role '{}' references missing skill '{name}'", role.id)
                })?;
                let root = self.store.mission_skills_dir(&state.mission_id).join(name);
                let (package, digest) = crate::mission_type::load_skill_package(&root)
                    .map_err(|error| format!("mission skill '{name}' is unavailable: {error}"))?;
                if package.name != recorded.name
                    || package.description != recorded.description
                    || digest != recorded.digest
                {
                    return Err(format!(
                        "mission skill '{name}' differs from its SkillAdded fact"
                    ));
                }
                Ok(package)
            })
            .collect()
    }

    pub fn new(
        store: MissionStore,
        mission_type: MissionType,
        image_id: String,
        services: EngineServices,
    ) -> Self {
        Self {
            store,
            mission_type,
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
            digest: self.mission_type.digest().to_string(),
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
    ) -> Result<MissionId> {
        let now_ms = self.clock.now_ms();
        let mission_id = MissionId::for_creation(workspace_dir, objective, now_ms);
        self.create_mission_with_id(mission_id, now_ms, workspace_dir, objective, base_sha)
            .await
    }

    pub(crate) async fn create_mission_with_id(
        &self,
        mission_id: MissionId,
        now_ms: i64,
        workspace_dir: &str,
        objective: &str,
        base_sha: &str,
    ) -> Result<MissionId> {
        self.mission_type.validate_at(now_ms)?;
        let config = self.mission_type.mission_config();
        let created = NewEvent::new(MissionEvent::MissionCreated {
            objective: objective.to_string(),
            mission_type: self.mission_type_ref(),
            image_id: self.image_id.clone(),
            workspace_dir: workspace_dir.to_string(),
            base_sha: base_sha.to_string(),
            config,
        });
        self.store
            .create_mission_with_events(
                &mission_id,
                workspace_dir,
                objective,
                &[
                    created,
                    NewEvent::new(MissionEvent::TeamConfigured {
                        team: self.mission_type.default_team.clone(),
                    }),
                ],
                now_ms,
            )
            .await
            .context("failed to create mission")?;
        Ok(mission_id)
    }

    /// Validate and record one complete plan proposal. Initial and revised
    /// plans use the same boundary; invalid or stale proposals append nothing.
    pub async fn propose_plan(
        &self,
        mission_id: &MissionId,
        proposal: MissionProposal,
    ) -> Result<(), ProposeError> {
        let state = self.load_state(mission_id).await?;
        if !state.inflight.is_empty() {
            return Err(ProposeError::MissionBusy);
        }
        validate_mission_proposal(&state, &proposal)?;
        let proposal_json = serde_json::to_string(&proposal).map_err(anyhow::Error::from)?;
        let proposal_hash = hex::encode(Sha256::digest(proposal_json.as_bytes()));
        let event = NewEvent::new(MissionEvent::ProposalRecorded {
            proposal: Box::new(proposal),
            proposal_hash,
        });
        self.store
            .append(mission_id, state.head, &[event], self.clock.now_ms())
            .await
            .map_err(|e| ProposeError::Other(e.into()))?;
        Ok(())
    }

    /// Append one complete lead-direct team revision. Revisions that weaken
    /// an accepted proof panel must travel through ProposalRecorded + an
    /// explicit approval instead.
    pub async fn configure_team(
        &self,
        mission_id: &MissionId,
        team: crate::model::TeamRevision,
    ) -> Result<()> {
        let state = self.load_state(mission_id).await?;
        let proposal = MissionProposal {
            plan: None,
            team: Some(team.clone()),
        };
        validate_mission_proposal(&state, &proposal)
            .map_err(|error| anyhow::anyhow!("team revision rejected: {error}"))?;
        if let Some(current) = &state.team {
            if team_weakens_proof(current, &team, state.config.requires_gap_review) {
                bail!(
                    "team revision weakens the accepted proof bar; propose it for explicit approval"
                );
            }
        }
        for role in team.roles.values() {
            for skill in &role.skills {
                if !self.mission_type.skills.contains_key(skill)
                    && !state.skills.contains_key(skill)
                {
                    bail!(
                        "role instance '{}' references missing skill '{skill}'",
                        role.id
                    );
                }
            }
        }
        self.store
            .append(
                mission_id,
                state.head,
                &[NewEvent::new(MissionEvent::TeamConfigured { team })],
                self.clock.now_ms(),
            )
            .await?;
        Ok(())
    }

    pub async fn add_mission_skill(
        &self,
        mission_id: &MissionId,
        skill: crate::model::MissionSkill,
    ) -> Result<()> {
        let state = self.load_state(mission_id).await?;
        if let Some(existing) = state.skills.get(&skill.name) {
            if existing == &skill {
                return Ok(());
            }
            bail!(
                "mission skill '{}' already exists with different content",
                skill.name
            );
        }
        self.store
            .append(
                mission_id,
                state.head,
                &[NewEvent::new(MissionEvent::SkillAdded { skill })],
                self.clock.now_ms(),
            )
            .await?;
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

    /// Abort any nonterminal mission. The closure fact is appended before an
    /// active driver observes it and begins cancellation.
    pub async fn abort(&self, mission_id: &MissionId, reason: &str) -> Result<()> {
        record_abort(&self.store, self.clock.now_ms(), mission_id, reason).await
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
        if state.mission_type.digest != self.mission_type.digest() {
            bail!(
                "mission type '{}' changed since this mission started \
                 (recorded {}, on-disk {}); start a fresh mission",
                state.mission_type.name,
                crate::model::short_hex(&state.mission_type.digest),
                crate::model::short_hex(self.mission_type.digest()),
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
        let guard = loop {
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
        let drive_result = async {
            self.drive(mission_id, activity).await?;
            let state = self.load_state(mission_id).await?;
            cleanup_settled_conversation_scratch(&self.store, &state, &guard).await?;
            // Persist a fold snapshot before parking or exiting so the next
            // invocation resumes without re-folding the whole log.
            self.store
                .save_snapshot(&state, self.clock.now_ms())
                .await?;
            Ok::<_, anyhow::Error>(state)
        }
        .await;
        // Projection latency is deliberately outside all authoritative state
        // transitions, deadline decisions, and cleanup.
        reporter.shutdown().await;
        let state = drive_result?;
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
                    return if state.phase == MissionPhase::Planning
                        || state.conversations.values().any(|conversation| {
                            conversation.lifecycle
                                == crate::model::ConversationLifecycle::AwaitingLead
                        }) {
                        Ok(())
                    } else {
                        bail!("engine idle in unexpected phase {:?}", state.phase)
                    };
                }
                StepDecision::Park | StepDecision::Terminal => return Ok(()),
                StepDecision::DispatchRole(intent) => {
                    self.materialize_role_turn_request(&state, intent).await?;
                }
                StepDecision::RunOracles(intents) => {
                    self.materialize_oracle_requests(&state, intents).await?;
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
            let Some((effect_id, _)) = state.inflight.iter().next() else {
                return Ok(true);
            };
            let recovered_failure = match self.cleanup_effect(&state, effect_id, true).await? {
                EffectCleanupDisposition::Complete(failure) => failure,
                EffectCleanupDisposition::Blocked => return Ok(false),
            };
            let current = self.load_state(mission_id).await?;
            let Some(effect) = current.inflight.get(effect_id) else {
                continue;
            };
            if let Some(cancellation) = current.durable_cancellation(effect_id) {
                let failure = self.recover_role_failure(
                    current.role_attempt_receipts.get(effect_id),
                    cancellation.into_failure(Default::default()),
                );
                let outcome = failed_outcome(effect_id, effect, failure);
                if self
                    .append_outcome(&current.mission_id, effect_id, outcome, true)
                    .await?
                    .is_none()
                {
                    return Ok(false);
                }
                continue;
            }
            if let Some(failure) = recovered_failure {
                let outcome = failed_outcome(effect_id, effect, failure);
                if self
                    .append_outcome(&current.mission_id, effect_id, outcome, true)
                    .await?
                    .is_none()
                {
                    return Ok(false);
                }
                continue;
            }
            let failure = current
                .role_attempt_receipts
                .get(effect_id)
                .and_then(crate::model::RoleAttemptReceipt::rejection)
                .cloned()
                .unwrap_or_else(interrupted_failure);
            let failure =
                self.recover_role_failure(current.role_attempt_receipts.get(effect_id), failure);
            let outcome = failed_outcome(effect_id, effect, failure);
            if self
                .append_outcome(&current.mission_id, effect_id, outcome, true)
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
            if let MissionPhase::Aborted { reason } = &current.phase {
                if matches!(
                    self.cleanup_effect(&current, effect_id, true).await?,
                    EffectCleanupDisposition::Blocked
                ) {
                    return Ok(false);
                }
                if self
                    .append_outcome(
                        &current.mission_id,
                        effect_id,
                        aborted_before_start_outcome(effect_id, active, reason),
                        true,
                    )
                    .await?
                    .is_none()
                {
                    return Ok(false);
                }
                return Ok(true);
            }
            if let Some(reason) = current.stop_requests.get(effect_id) {
                if matches!(
                    self.cleanup_effect(&current, effect_id, true).await?,
                    EffectCleanupDisposition::Blocked
                ) {
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
                InflightEffect::RoleTurn { .. } => {
                    self.execute_role_turn(state, effect_id, &effect, control_rx, activity.clone())
                        .await
                }
                InflightEffect::OracleRun { .. } => {
                    self.execute_oracle_run(state, effect_id, &effect, control_rx)
                        .await
                }
            }
        };
        // Each effect type carries its complete bounded execution path. Pin
        // the sum once on the heap instead of embedding its largest variant
        // in the driver thread's stack frame.
        let mut execution = Box::pin(execution);
        let mut outcome = loop {
            tokio::select! {
                outcome = &mut execution => break outcome?,
                () = tokio::time::sleep(Duration::from_millis(100)) => {
                    let current = self.load_state(&state.mission_id).await?;
                    let Some(active) = current.inflight.get(effect_id) else {
                        bail!("active effect '{effect_id}' disappeared without an outcome");
                    };
                    if let MissionPhase::Aborted { reason } = &current.phase {
                        control_tx.send_replace(ExecutionControl::Abort(reason.clone()));
                        continue;
                    }
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
                            NewEvent::new(MissionEvent::ControlRequested {
                                effect_id: effect_id.clone(),
                                action: crate::model::ControlAction::DeadlineReached {
                                    deadline_ms,
                                },
                                reason: "effect reached its configured deadline".into(),
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
        let mut discard_artifact = !matches!(
            outcome.event,
            MissionEvent::RoleTurnCompleted { outcome: Ok(_), .. }
        );
        match self
            .cleanup_effect(state, effect_id, discard_artifact)
            .await?
        {
            EffectCleanupDisposition::Complete(Some(failure))
                if matches!(
                    &outcome.event,
                    MissionEvent::RoleTurnCompleted { outcome: Ok(_), .. }
                ) =>
            {
                outcome = failed_outcome(effect_id, &effect, failure);
                discard_artifact = true;
            }
            EffectCleanupDisposition::Complete(Some(_)) => {}
            EffectCleanupDisposition::Complete(None) => {}
            EffectCleanupDisposition::Blocked => return Ok(false),
        }
        let Some(outcome) = self
            .append_outcome(&state.mission_id, effect_id, outcome, discard_artifact)
            .await?
        else {
            return Ok(false);
        };
        let checkpoint = checkpoint_after(&outcome.event, &effect, &state.config.execution);
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
                action: crate::model::ControlAction::Continue {
                    automatic: true,
                    mode: crate::model::ContinueMode::Preserve,
                },
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
    ) -> Result<EffectCleanupDisposition> {
        let request = EffectCleanupRequest {
            mission_id: state.mission_id.clone(),
            effect_id: effect_id.clone(),
            workspace_dir: state.workspace_dir.clone().into(),
            state_dir: self.store.lionclaw_dir().to_path_buf(),
            discard_artifact,
        };
        if let Err(error) = self.effect_cleaner.quiesce(&request).await {
            self.record_cleanup_failure(state, effect_id, error).await?;
            return Ok(EffectCleanupDisposition::Blocked);
        }
        match self.effect_cleaner.cleanup(request).await {
            Ok(()) => Ok(EffectCleanupDisposition::Complete(None)),
            Err(error) => {
                self.record_cleanup_failure(state, effect_id, error).await?;
                Ok(EffectCleanupDisposition::Blocked)
            }
        }
    }

    async fn record_cleanup_failure(
        &self,
        state: &MissionState,
        effect_id: &EffectId,
        error: crate::ports::EffectCleanupFailure,
    ) -> Result<()> {
        self.append_fact(
            &state.mission_id,
            state.head,
            NewEvent::new(MissionEvent::EffectCleanupFailed {
                effect_id: effect_id.clone(),
                resource: error.resource,
                failure: TypedFailure::permanent("cleanup.infrastructure", error.detail),
            }),
        )
        .await
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
                    if matches!(
                        self.cleanup_effect(&state, effect_id, true).await?,
                        EffectCleanupDisposition::Blocked
                    ) {
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
                Ok(_) => return Ok(Some(outcome)),
                Err(AppendError::Duplicate {
                    effect_id: duplicate,
                }) => {
                    if self
                        .duplicate_effects_applied(mission_id, std::slice::from_ref(&outcome))
                        .await?
                    {
                        return Ok(Some(outcome));
                    }
                    bail!(
                        "effect identity collision for '{duplicate}': the durable outcome was not applied by the event fold"
                    );
                }
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
            Ok(_) | Err(AppendError::Conflict { .. }) => Ok(()),
            Err(AppendError::Duplicate { effect_id }) => {
                if self.duplicate_effects_applied(mission_id, events).await? {
                    Ok(())
                } else {
                    bail!(
                        "effect identity collision for '{effect_id}': the durable request was not applied by the event fold"
                    )
                }
            }
            Err(err) => Err(err.into()),
        }
    }

    /// A duplicate effect identity is idempotent only when the event log holds
    /// the exact same fact and the pure fold actually applied it. This keeps a
    /// malformed inert row from reserving a deterministic generation forever.
    async fn duplicate_effects_applied(
        &self,
        mission_id: &MissionId,
        expected: &[NewEvent],
    ) -> Result<bool> {
        let log = self.store.load(mission_id).await?;
        for expected in expected {
            let Some((class, effect_id)) = expected.event.effect_identity() else {
                return Ok(false);
            };
            if !log.iter().any(|stored| {
                stored.event.effect_identity() == Some((class, effect_id))
                    && stored.event == expected.event
                    && stored.stamps == expected.stamps
            }) {
                return Ok(false);
            }
        }
        let Some(state) = crate::model::fold(log) else {
            return Ok(false);
        };
        for expected in expected {
            let Some((class, effect_id)) = expected.event.effect_identity() else {
                return Ok(false);
            };
            let remains_inflight = state
                .inflight
                .keys()
                .any(|candidate| candidate.as_str() == effect_id);
            let applied = match class {
                EffectEventClass::Request => remains_inflight,
                EffectEventClass::Outcome => !remains_inflight,
            };
            if !applied {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn execute_role_turn(
        &self,
        state: &MissionState,
        effect_id: &EffectId,
        effect: &InflightEffect,
        control: tokio::sync::watch::Receiver<ExecutionControl>,
        activity: tokio::sync::watch::Sender<Option<(EffectId, lionclaw_runtime_api::TurnEvent)>>,
    ) -> Result<NewEvent> {
        let InflightEffect::RoleTurn {
            role_instance,
            team_revision,
            task_id,
            attempt_no,
            output,
            base_sha,
            assignment_epoch,
            workspace_preparation,
            ..
        } = effect
        else {
            bail!("execute_role_turn called with a non-role effect");
        };
        let attempt_no = *attempt_no;
        let completed = |outcome: Result<crate::model::RoleTurnSuccess, TypedFailure>| {
            NewEvent::new(MissionEvent::RoleTurnCompleted {
                effect_id: effect_id.clone(),
                outcome: outcome.map_err(without_role_turn_evidence),
            })
        };
        if let Err(reason) = state.active_role_conversation(effect_id) {
            return Ok(completed(Err(TypedFailure::permanent(
                "role.authority",
                reason,
            ))));
        }
        let Some(role) = state
            .team_history
            .get(team_revision)
            .and_then(|team| team.role(role_instance))
        else {
            return Ok(completed(Err(TypedFailure::permanent(
                "role.missing",
                format!(
                    "role instance '{role_instance}' is absent from team revision {team_revision}"
                ),
            ))));
        };
        if role.output != *output {
            return Ok(completed(Err(TypedFailure::permanent(
                "role.output_contract",
                "the pinned mission role no longer matches the effect output contract",
            ))));
        }
        let prompt_text = match self
            .reconstruct_role_prompt(&state.mission_id, effect_id)
            .await
        {
            Ok(prompt) => prompt,
            Err(error) => {
                return Ok(completed(Err(TypedFailure::permanent(
                    "role.prompt_drift",
                    format!("canonical turn reconstruction failed: {error:#}"),
                ))))
            }
        };
        let skills = match self.resolve_role_skills(state, role) {
            Ok(skills) => skills,
            Err(detail) => {
                return Ok(completed(Err(TypedFailure::permanent(
                    "skills.resolve",
                    detail,
                ))))
            }
        };
        let artifact_capture = if *output == crate::model::OutputSemantics::ProducesArtifact {
            task_id.as_ref().map(|task_id| {
                let task =
                    MissionDirs::new(self.store.lionclaw_dir(), &state.mission_id).task(task_id);
                let checkout = task.work().to_path_buf();
                let archive_checkout = workspace_preparation
                    .archived_effect()
                    .map(|archived| task.workspace_archive(archived));
                ArtifactCapture::new(
                    state.workspace_dir.clone().into(),
                    checkout,
                    base_sha.to_string(),
                    state.mission_id.clone(),
                    effect_id.clone(),
                    archive_checkout,
                )
            })
        } else {
            None
        };
        let mut environment = self.mission_type.environment.clone();
        environment.extend(role.environment.clone());
        let request = RoleTurnRequest {
            mission_id: state.mission_id.clone(),
            task_id: task_id.clone(),
            attempt_no,
            effect_id: effect_id.clone(),
            role: role.clone(),
            environment,
            skills,
            prompt: prompt_text.clone(),
            base_sha: base_sha.to_string(),
            assignment_epoch: *assignment_epoch,
            workspace_preparation: workspace_preparation.clone(),
            deadline_ms: effect.deadline_ms(),
            control,
            activity,
            workspace_dir: state.workspace_dir.clone().into(),
            state_dir: self.store.lionclaw_dir().to_path_buf(),
            artifact_capture,
        };
        let previous_task = task_id
            .as_ref()
            .and_then(|task_id| state.tasks.get(task_id));
        match self
            .role_runner
            .run(request)
            .await
            .map(crate::ports::RoleTurnOutcome::projected)
            .map_err(TypedFailure::projected)
        {
            Ok(outcome) => {
                let outcome = match validated_role_success(
                    outcome,
                    *output,
                    base_sha,
                    &state.mission_id,
                    effect_id,
                ) {
                    Ok(outcome) => outcome,
                    Err(failure) => return Ok(completed(Err(failure))),
                };
                // A planning author's proposal is validated fail-closed before
                // it is recorded, exactly like a manually proposed plan — an
                // invalid proposal is a failed attempt, never a bad contract.
                if let Some(Handoff::Plan {
                    done: true,
                    proposal,
                    ..
                }) = &outcome.handoff
                {
                    let Some(proposal) = proposal else {
                        return Ok(completed(Err(invalid_role_outcome(
                            "plan.missing",
                            "planning author reported done but proposed no plan",
                            &outcome,
                        ))));
                    };
                    if let Err(error) = validate_mission_proposal(state, proposal) {
                        return Ok(completed(Err(invalid_role_outcome(
                            "plan.invalid",
                            format!("proposed plan is invalid: {error}"),
                            &outcome,
                        ))));
                    }
                }
                if let Some(Handoff::Review { nonce, .. }) = &outcome.handoff {
                    let expected = crate::prompt::handoff_nonce(&prompt_text).unwrap_or_default();
                    if nonce != expected {
                        return Ok(completed(Err(invalid_role_outcome(
                            "handoff.nonce",
                            "handoff nonce does not match the reviewed turn",
                            &outcome,
                        ))));
                    }
                }
                let handoff = match outcome
                    .handoff
                    .as_ref()
                    .map(|handoff| self.externalize_handoff(handoff))
                    .transpose()
                {
                    Ok(handoff) => handoff,
                    Err(failure) => {
                        return Ok(completed(Err(with_role_outcome_evidence(
                            failure, &outcome,
                        ))));
                    }
                };
                let final_response = match self
                    .externalize_role_payload(PayloadRef::inline(outcome.final_response.clone()))
                {
                    Ok(response) => response,
                    Err(failure) => {
                        return Ok(completed(Err(with_role_outcome_evidence(
                            failure, &outcome,
                        ))));
                    }
                };
                let settlement_evidence = lionclaw_runtime_api::TypedFailureEvidence {
                    final_response: outcome.final_response.clone(),
                    configuration: runtime_configuration_evidence(&outcome.runtime_configuration),
                    ..Default::default()
                };
                Ok(completed(Ok(RoleTurnSuccess {
                    handoff,
                    artifact: outcome
                        .artifact
                        .map(crate::ports::CapturedArtifact::into_outcome),
                    final_response,
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
        let completed = |outcome: Result<crate::model::OracleRunSuccess, TypedFailure>| {
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
            environment: self.mission_type.environment.clone(),
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

    fn resolve_upstream_reports(
        &self,
        state: &MissionState,
        depends_on: &[crate::model::TaskId],
    ) -> Result<Vec<String>> {
        const MAX_UPSTREAM_REPORT_BYTES: usize =
            crate::model::MAX_TASK_DEPENDENCIES * MAX_ROLE_REPORT_BYTES;
        let mut reports = Vec::new();
        let mut remaining = MAX_UPSTREAM_REPORT_BYTES;
        for dep in depends_on {
            let Some(outcome) = state
                .tasks
                .get(dep)
                .and_then(crate::model::TaskRuntimeState::cleared_outcome)
            else {
                continue;
            };
            let receipt = state
                .role_attempt_receipts
                .get(outcome.effect_id())
                .context("cleared dependency points to a missing role-attempt receipt")?;
            let (prefix, report) = match outcome {
                crate::model::TaskAttemptOutcome::Accepted { .. } => (
                    String::new(),
                    Some(receipt.accepted_report().context(
                        "accepted dependency outcome does not contain an accepted handoff",
                    )?),
                ),
                crate::model::TaskAttemptOutcome::Failed { .. } => {
                    let failure = receipt
                        .failure()
                        .context("failed dependency receipt has no failure")?;
                    (
                        format!(
                            "Lead accepted failed dependency '{dep}' ({}): {}",
                            failure.category(),
                            failure.detail()
                        ),
                        receipt.accepted_report(),
                    )
                }
            };
            if prefix.len() > remaining {
                bail!("accepted upstream outcomes exceeded their aggregate prompt budget");
            }
            remaining -= prefix.len();
            let Some(report) = report else {
                reports.push(prefix);
                continue;
            };
            let resolved = self
                .store
                .blobs()
                .resolve_bounded(report, remaining)
                .context("accepted upstream reports exceeded their aggregate prompt budget")?;
            remaining -= resolved.len();
            reports.push(if prefix.is_empty() {
                resolved
            } else {
                format!("{prefix}\nRetained handoff report:\n{resolved}")
            });
        }
        Ok(reports)
    }

    fn resolve_task_feedback(&self, state: &MissionState, task_id: &TaskId) -> Result<Vec<String>> {
        let mut feedback = Vec::new();
        if let Some(failure) = state.task_last_failure(task_id) {
            feedback.push(format!(
                "Previous attempt failed ({}): {}",
                failure.category(),
                failure.detail()
            ));
        }
        let Some(task) = state.tasks.get(task_id) else {
            return Ok(feedback);
        };
        for item in &task.feedback {
            feedback.push(crate::evidence::render_feedback(
                self.store.blobs(),
                state,
                item,
            )?);
        }
        Ok(feedback)
    }

    /// Assemble an execution role's prompt.
    fn assemble_execution_request(
        &self,
        state: &MissionState,
        role: &RoleInstance,
        intent: &RoleDispatchIntent,
        dialogue: &[String],
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
            .find(|task| Some(&task.id) == intent.task_id.as_ref())
            .context("dispatched task not in plan")?;
        let upstream_reports = self.resolve_upstream_reports(state, &task.depends_on)?;
        let mut feedback = state
            .tasks
            .contains_key(&task.id)
            .then(|| self.resolve_task_feedback(state, &task.id))
            .transpose()?
            .unwrap_or_default();
        feedback.extend_from_slice(dialogue);
        let context = TurnContext::Execution(
            role,
            ExecutionContext {
                objective: &state.objective,
                task_body: &intent.body,
                targets: &targets,
                upstream_reports: &upstream_reports,
                guidance: state
                    .team
                    .as_ref()
                    .and_then(|team| team.guidance.as_ref())
                    .map_or("", |guidance| guidance.text.as_str()),
                feedback: &feedback,
            },
        );
        Ok(render(context))
    }

    /// Assemble a planning role's prompt. Threads the mission type's playbook
    /// and execution-role/oracle inventories through a separate assembler.
    fn assemble_planning_request(
        &self,
        state: &MissionState,
        role: &RoleInstance,
        intent: &RoleDispatchIntent,
        dialogue: &[String],
    ) -> Result<String> {
        let upstream_reports = Vec::new();
        let mut task_feedback = Vec::new();
        task_feedback.extend_from_slice(dialogue);
        let planning_input = self.resolve_planning_prompt_input(state)?;
        let oracle_inventory: Vec<String> = self
            .mission_type
            .oracles
            .keys()
            .map(|o| o.as_str().to_string())
            .collect();
        let prompt = render(TurnContext::Planning(
            role,
            PlanningPromptContext {
                objective: &state.objective,
                generation: intent.team_revision,
                base_revision: state.revision,
                input: planning_input,
                playbook: self.mission_type.playbook.as_deref(),
                team: state
                    .team
                    .as_ref()
                    .context("planning dispatch without a team")?,
                oracle_inventory: &oracle_inventory,
                task_body: &intent.body,
                upstream_reports: &upstream_reports,
                guidance: state
                    .team
                    .as_ref()
                    .and_then(|team| team.guidance.as_ref())
                    .map_or("", |guidance| guidance.text.as_str()),
                task_feedback: &task_feedback,
            },
        ));
        Ok(prompt)
    }

    fn assemble_role_request(
        &self,
        state: &MissionState,
        role: &RoleInstance,
        intent: &RoleDispatchIntent,
        dialogue: &[String],
    ) -> Result<String> {
        let plan = state.plan.as_ref();
        let targets = plan
            .map(|plan| {
                plan.assertions
                    .iter()
                    .filter(|assertion| intent.targets.contains(&assertion.id))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        match role.output {
            crate::model::OutputSemantics::ProducesArtifact => {
                self.assemble_execution_request(state, role, intent, dialogue)
            }
            crate::model::OutputSemantics::EmitsVerdict => Ok(render(TurnContext::Judgment(
                role,
                JudgmentContext {
                    objective: &state.objective,
                    task_body: &intent.body,
                    targets: &targets,
                    feedback: dialogue,
                },
            ))),
            crate::model::OutputSemantics::ProducesReport
            | crate::model::OutputSemantics::ProposesPlan => {
                self.assemble_planning_request(state, role, intent, dialogue)
            }
            crate::model::OutputSemantics::EmitsGapVerdict => {
                let limitations = plan
                    .into_iter()
                    .flat_map(|plan| &plan.requirements)
                    .filter_map(|requirement| match &requirement.disposition {
                        crate::model::RequirementDisposition::Limitation { rationale } => {
                            Some(format!("{}: {rationale}", requirement.prose))
                        }
                        crate::model::RequirementDisposition::Covered { .. } => None,
                    })
                    .collect::<Vec<_>>();
                let nonce = EffectId::for_parts(&[
                    state.mission_id.as_str(),
                    role.id.as_str(),
                    &intent.team_revision.to_string(),
                    &intent.attempt_no.to_string(),
                    &intent.base_sha,
                    "gap-review-nonce",
                ]);
                Ok(render(TurnContext::GapReview(
                    role,
                    GapReviewPromptContext {
                        objective: &state.objective,
                        limitations: &limitations,
                        nonce: nonce.as_str(),
                    },
                )))
            }
        }
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
                    crate::evidence::render_feedback(self.store.blobs(), state, feedback)?,
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
    async fn materialize_role_turn_request(
        &self,
        state: &MissionState,
        intent: RoleDispatchIntent,
    ) -> Result<()> {
        let role = state
            .team
            .as_ref()
            .filter(|team| team.revision == intent.team_revision)
            .and_then(|team| team.role(&intent.role_instance))
            .with_context(|| {
                format!(
                    "role instance '{}' missing from team revision {}",
                    intent.role_instance, intent.team_revision
                )
            })?;
        ensure!(
            role.output == intent.output,
            "folded output contract for role instance '{}' differs from its team revision",
            intent.role_instance
        );
        let assignment = crate::model::resolve_role_assignment(
            &intent.role_instance,
            intent.team_revision,
            crate::model::RoleAssignmentContext {
                previous: intent
                    .task_id
                    .as_ref()
                    .and_then(|task_id| state.tasks.get(task_id)),
                required_base: &intent.base_sha,
                lifecycle_generation: state.revision.max(1),
                retrying_failure: intent
                    .task_id
                    .as_ref()
                    .is_some_and(|task_id| state.task_automatic_retry_remaining(task_id)),
                output: intent.output,
            },
        );
        let mut message_boundary = state.head;
        let (dialogue, unavailable_reference) = match materialize_conversation_messages(
            self,
            state,
            &intent.role_instance,
            message_boundary,
        )
        .await
        {
            Ok(dialogue) => (dialogue, None),
            Err(error)
                if error
                    .downcast_ref::<UnavailableConversationReference>()
                    .is_some() =>
            {
                let unavailable = error
                    .downcast::<UnavailableConversationReference>()
                    .expect("guarded unavailable reference error");
                message_boundary = unavailable.message_sequence;
                (Vec::new(), Some(unavailable))
            }
            Err(error) => return Err(error),
        };
        let presented_messages =
            state
                .conversations
                .get(&intent.role_instance)
                .map_or_else(Vec::new, |conversation| {
                    conversation
                        .queued
                        .iter()
                        .filter(|message| {
                            message.sequence_no <= message_boundary
                                && message.marker != crate::model::DeliveryMarker::Undeliverable
                        })
                        .map(|message| message.sequence_no)
                        .collect()
                });
        let prompt_text = if let Some(unavailable) = &unavailable_reference {
            format!(
                "queued message {} could not materialize reference {:?}: {:?}",
                unavailable.message_sequence, unavailable.reference, unavailable.cause
            )
        } else {
            self.assemble_role_request(state, role, &intent, &dialogue)?
        };
        let prompt_hash = hex::encode(Sha256::digest(prompt_text.as_bytes()));
        let base_sha = assignment.base_sha;
        let assignment_epoch = assignment.generation;
        let workspace_preparation = assignment.workspace_preparation;
        let effect_id = EffectId::for_role_turn(
            &state.mission_id,
            &intent.role_instance,
            intent.team_revision,
            intent.task_id.as_ref(),
            intent.attempt_no,
            assignment_epoch,
            &prompt_hash,
        );
        let requested_at_ms = self.clock.now_ms();
        let not_before_ms = retry_not_before(
            requested_at_ms,
            intent
                .task_id
                .as_ref()
                .and_then(|task_id| state.task_last_failure(task_id)),
        );
        let initial_secs = role
            .deadline_secs
            .unwrap_or(state.config.execution.default_timeout_secs);
        let event = NewEvent::new(MissionEvent::RoleTurnRequested {
            role_instance: intent.role_instance.clone(),
            team_revision: intent.team_revision,
            task_id: intent.task_id.clone(),
            assertion_ids: intent.targets.clone(),
            attempt_no: intent.attempt_no,
            effect_id: effect_id.clone(),
            prompt_template: crate::model::role_prompt_template(intent.output),
            prompt_hash: prompt_hash.clone(),
            base_sha,
            assignment_epoch,
            message_boundary,
            presented_messages,
            workspace_preparation,
            requested_at_ms,
            deadline_ms: resolved_deadline(not_before_ms, initial_secs)?,
            budget_deadline_ms: resolved_deadline(
                not_before_ms,
                initial_secs.max(state.config.execution.max_task_time_secs),
            )?,
        })
        .with_prompt_hash(prompt_hash);
        if let Some(unavailable) = unavailable_reference {
            let failure = TypedFailure::permanent(
                "message.reference_unavailable",
                format!(
                    "queued message {} reference {:?} is unavailable: {:?}",
                    unavailable.message_sequence, unavailable.reference, unavailable.cause
                ),
            );
            let completed = NewEvent::new(MissionEvent::RoleTurnCompleted {
                effect_id,
                outcome: Err(failure),
            });
            self.append_idempotent(&state.mission_id, state.head, &[event, completed])
                .await
        } else {
            self.append_idempotent(&state.mission_id, state.head, &[event])
                .await
        }
    }

    async fn materialize_oracle_requests(
        &self,
        state: &MissionState,
        intents: Vec<OracleDispatchIntent>,
    ) -> Result<()> {
        let events: Vec<NewEvent> = intents
            .into_iter()
            .map(|intent| {
                let effect_id = EffectId::for_oracle_request(
                    &state.mission_id,
                    &intent.oracle,
                    &intent.judged_sha,
                    intent.attempt_no,
                );
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

    fn externalize_handoff(&self, handoff: &Handoff) -> Result<Handoff, TypedFailure> {
        Ok(match handoff {
            Handoff::Work {
                done,
                report,
                request_attention,
            } => Handoff::Work {
                done: *done,
                report: self.externalize_handoff_report(report.clone())?,
                request_attention: *request_attention,
            },
            Handoff::Validate {
                done,
                report,
                items,
                passed,
                request_attention,
            } => Handoff::Validate {
                done: *done,
                report: self.externalize_handoff_report(report.clone())?,
                items: items.clone(),
                passed: *passed,
                request_attention: *request_attention,
            },
            Handoff::Review {
                done,
                report,
                passed,
                gaps,
                nonce,
            } => Handoff::Review {
                done: *done,
                report: self.externalize_handoff_report(report.clone())?,
                passed: *passed,
                // Typed and KB-scale, like `proposal` below — stays inline.
                gaps: gaps.clone(),
                nonce: nonce.clone(),
            },
            Handoff::Plan {
                done,
                report,
                proposal,
                request_attention,
            } => Handoff::Plan {
                done: *done,
                report: self.externalize_handoff_report(report.clone())?,
                // The proposal stays inline (KB-scale, typed); only the prose
                // report is externalized above the blob threshold.
                proposal: proposal.clone(),
                request_attention: *request_attention,
            },
        })
    }

    fn externalize_handoff_report(&self, payload: PayloadRef) -> Result<PayloadRef, TypedFailure> {
        if let PayloadRef::Inline { text } = &payload {
            if text.len() > MAX_ROLE_REPORT_BYTES {
                return Err(TypedFailure::invalid(
                    "handoff.report_too_large",
                    format!(
                        "handoff report is {} bytes; the limit is {MAX_ROLE_REPORT_BYTES}",
                        text.len()
                    ),
                ));
            }
        }
        self.externalize_role_payload(payload)
    }

    fn externalize_role_payload(&self, payload: PayloadRef) -> Result<PayloadRef, TypedFailure> {
        if matches!(payload, PayloadRef::Blob(_)) {
            return Err(TypedFailure::invalid(
                "handoff.payload_ref",
                "role output must provide inline text; only the engine may mint blob references",
            ));
        }
        self.store.blobs().externalize(payload).map_err(|error| {
            TypedFailure::permanent(
                "kernel.blob_store",
                format!("failed to persist role output: {error}"),
            )
        })
    }

    fn recover_role_failure(
        &self,
        _receipt: Option<&crate::model::RoleAttemptReceipt>,
        fallback: TypedFailure,
    ) -> TypedFailure {
        fallback.projected()
    }
}

fn team_weakens_proof(
    current: &crate::model::TeamRevision,
    next: &crate::model::TeamRevision,
    requires_gap_review: bool,
) -> bool {
    let panel_weakened = current
        .judgment_assignments
        .iter()
        .any(|(assertion, panel)| {
            next.judgment_assignments
                .get(assertion)
                .is_none_or(|next_panel| panel.iter().any(|judge| !next_panel.contains(judge)))
        });
    let gap_removed = requires_gap_review
        && current.gap_review_assignment.is_some()
        && next.gap_review_assignment.is_none();
    panel_weakened || gap_removed
}

fn interrupted_failure() -> TypedFailure {
    let mut evidence = lionclaw_runtime_api::TypedFailureEvidence::new(
        Some("driver.interrupted".to_string()),
        "the previous mission driver exited before recording an outcome; its resources were cleaned and the effect was not replayed",
    );
    evidence.stop_reason = Some("mission driver exited".into());
    TypedFailure::Interrupted {
        evidence: Box::new(evidence),
    }
}

fn failed_outcome(
    effect_id: &EffectId,
    effect: &InflightEffect,
    failure: TypedFailure,
) -> NewEvent {
    NewEvent::new(match effect {
        InflightEffect::RoleTurn { .. } => MissionEvent::RoleTurnCompleted {
            effect_id: effect_id.clone(),
            outcome: Err(without_role_turn_evidence(failure)),
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

fn aborted_before_start_outcome(
    effect_id: &EffectId,
    effect: &InflightEffect,
    reason: &str,
) -> NewEvent {
    let mut evidence = lionclaw_runtime_api::TypedFailureEvidence::new(
        Some("control.aborted_before_start".into()),
        "mission aborted before the effect runtime started",
    );
    evidence.stop_reason = Some(reason.to_string());
    failed_outcome(
        effect_id,
        effect,
        TypedFailure::OperatorAborted {
            evidence: Box::new(evidence),
        },
    )
}

fn settlement_failure(
    state: &MissionState,
    effect_id: &EffectId,
    outcome: &NewEvent,
) -> Option<TypedFailure> {
    let cancellation = state.durable_cancellation(effect_id)?;
    let evidence = outcome
        .settlement_evidence
        .clone()
        .or_else(|| outcome.event.outcome_failure_evidence())
        .unwrap_or_default();
    Some(cancellation.into_failure(evidence))
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
    let requirement_changes = if action == crate::model::DecisionAction::Approve {
        state
            .open_attention
            .get(attention_id)
            .filter(|item| item.kind == crate::model::AttentionKind::PlanProposal)
            .and(state.proposal.as_ref())
            .and_then(|proposal| proposal.plan.as_ref())
            .map(|proposal| proposal.requirement_changes.clone())
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let decision = NewEvent::new(MissionEvent::DecisionRecorded {
        attention_id: attention_id.to_string(),
        action: action.clone(),
        justification: justification.to_string(),
        requirement_changes,
    });
    let mut events = vec![decision];
    if action == crate::model::DecisionAction::Approve {
        if let Some(team) = state
            .open_attention
            .get(attention_id)
            .filter(|item| item.kind == crate::model::AttentionKind::PlanProposal)
            .and(state.proposal.as_ref())
            .and_then(|proposal| proposal.team.clone())
        {
            events.push(NewEvent::new(MissionEvent::TeamConfigured { team }));
        }
    }
    store
        .append(mission_id, state.head, &events, now_ms)
        .await?;
    reconcile_disposable_conversation_resources(store, mission_id)
        .await
        .with_context(|| {
            format!(
                "decision was recorded durably for mission '{mission_id}', but disposable resource cleanup failed; run 'lionclaw mission advance {mission_id}' to retry"
            )
        })?;
    Ok(())
}

/// Record mission closure without requiring or resolving an attention item.
/// Abort never approves, accepts, verifies, or deletes retained mission evidence.
pub async fn record_abort(
    store: &MissionStore,
    now_ms: i64,
    mission_id: &MissionId,
    reason: &str,
) -> Result<()> {
    if reason.trim().is_empty() {
        bail!("abort requires a non-empty reason");
    }
    let state = store.require_state(mission_id).await?;
    if state.phase.is_terminal() {
        bail!("mission '{mission_id}' is terminal; abort is not legal");
    }
    store
        .append(
            mission_id,
            state.head,
            &[NewEvent::new(MissionEvent::MissionAborted {
                reason: reason.to_string(),
            })],
            now_ms,
        )
        .await?;
    reconcile_disposable_conversation_resources(store, mission_id)
        .await
        .with_context(|| {
            format!(
                "mission '{mission_id}' was aborted durably, but disposable resource cleanup failed; run 'lionclaw mission advance {mission_id}' to retry"
            )
        })?;
    Ok(())
}

async fn materialize_conversation_messages(
    engine: &Engine,
    state: &MissionState,
    role_instance: &crate::model::RoleInstanceId,
    message_boundary: u64,
) -> Result<Vec<String>> {
    let Some(conversation) = state.conversations.get(role_instance) else {
        return Ok(Vec::new());
    };
    let events = engine.store.load(&state.mission_id).await?;
    let repo = std::path::Path::new(&state.workspace_dir);
    let messages = conversation
        .queued
        .iter()
        .filter(|message| message_is_materializable(message, message_boundary))
        .collect::<Vec<_>>();
    let materializer = crate::reference_materialization::ReferenceMaterializer::new(
        state,
        &events,
        engine.store.blobs(),
        repo,
        messages
            .iter()
            .flat_map(|message| message.references.iter()),
    )
    .map_err(|error| unavailable_conversation_reference(&messages, error))?;
    let mut rendered = Vec::with_capacity(messages.len());
    for message in messages {
        let expanded = materializer
            .materialize(&message.references)
            .await
            .map_err(|error| UnavailableConversationReference {
                message_sequence: message.sequence_no,
                reference: error.reference,
                cause: error.cause,
            })
            .map_err(anyhow::Error::new)?;
        rendered.push(render_conversation_message(message, &expanded));
    }
    Ok(rendered)
}

fn unavailable_conversation_reference(
    messages: &[&crate::model::QueuedMessage],
    error: crate::reference_materialization::ReferenceMaterializationError,
) -> anyhow::Error {
    let message_sequence = messages
        .iter()
        .find(|message| message.references.contains(&error.reference))
        .map(|message| message.sequence_no)
        .expect("authority construction errors name a requested reference");
    anyhow::Error::new(UnavailableConversationReference {
        message_sequence,
        reference: error.reference,
        cause: error.cause,
    })
}

fn message_is_materializable(message: &crate::model::QueuedMessage, message_boundary: u64) -> bool {
    message.sequence_no <= message_boundary
        && message.marker != crate::model::DeliveryMarker::Undeliverable
}

#[derive(Debug, thiserror::Error)]
#[error("queued message {message_sequence} has an unavailable reference")]
struct UnavailableConversationReference {
    message_sequence: u64,
    reference: crate::model::MessageReference,
    cause: crate::model::UnavailableReferenceCause,
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
        crate::model::ControlAction::DeadlineReached { .. } => {
            bail!("deadline observations are engine-owned");
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
        crate::model::ControlAction::Continue { automatic, mode } => {
            if *automatic {
                bail!("automatic controls are engine-owned");
            }
            if !state.continue_is_legal(effect_id, *automatic, *mode) {
                bail!("effect '{effect_id}' does not allow the requested continuation mode");
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

/// Atomically resolve and record one sender-free lead message.  Resolution is
/// performed against one folded head and is never repeated after the CAS.
pub struct MessageCommand {
    pub selectors: Vec<String>,
    pub all: bool,
    pub body: String,
    pub references: Vec<crate::model::MessageReference>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "role instance {role_instance} has {retained_messages} retained messages; the limit is {limit}"
)]
pub struct ConversationQueueFull {
    pub role_instance: crate::model::RoleInstanceId,
    pub retained_messages: usize,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReferenceRejectionReason {
    #[error(
        "message references are not permitted for recipient {role_instance} with output semantics {}",
        output.slug()
    )]
    Disallowed {
        role_instance: crate::model::RoleInstanceId,
        output: crate::model::OutputSemantics,
    },
    #[error(
        "message references cannot be sent to a mixture of permitted and disallowed recipients"
    )]
    MixedRecipients,
    #[error("message reference {reference:?} is stale")]
    Stale {
        reference: crate::model::MessageReference,
    },
    #[error("message reference {reference:?} is missing")]
    Missing {
        reference: crate::model::MessageReference,
    },
    #[error("message reference {reference:?} is unreadable")]
    Unreadable {
        reference: crate::model::MessageReference,
    },
    #[error("message reference {reference:?} has invalid content")]
    InvalidContent {
        reference: crate::model::MessageReference,
    },
    #[error("message reference {reference:?} is malformed or belongs to another mission")]
    MalformedOrForeign {
        reference: crate::model::MessageReference,
    },
    #[error("message reference expansion is oversized")]
    Oversized {
        reference: Option<crate::model::MessageReference>,
    },
}

fn validate_reference_eligibility(
    state: &MissionState,
    recipients: &[crate::model::RoleInstanceId],
    references: &[crate::model::MessageReference],
) -> std::result::Result<(), ReferenceRejectionReason> {
    if references.is_empty() {
        return Ok(());
    }
    match state.reference_recipient_policy(recipients) {
        crate::model::ReferenceRecipientPolicy::Permitted => {}
        crate::model::ReferenceRecipientPolicy::Disallowed {
            role_instance,
            output,
        } => {
            return Err(ReferenceRejectionReason::Disallowed {
                role_instance,
                output,
            });
        }
        crate::model::ReferenceRecipientPolicy::Mixed => {
            return Err(ReferenceRejectionReason::MixedRecipients);
        }
        crate::model::ReferenceRecipientPolicy::Invalid => {
            return Err(ReferenceRejectionReason::MalformedOrForeign {
                reference: references[0].clone(),
            });
        }
    }
    Ok(())
}

fn resolve_message_recipients(
    state: &MissionState,
    current: &[crate::model::RoleInstanceId],
    selectors: &[String],
    all: bool,
) -> Result<Vec<crate::model::RoleInstanceId>> {
    if all && !selectors.is_empty() {
        bail!("--all cannot be mixed with explicit recipients");
    }
    if !all && selectors.is_empty() {
        bail!("recipient set is empty");
    }
    let selected = if all {
        current.to_vec()
    } else {
        let mut selected = Vec::new();
        for selector in selectors {
            if let Some(exact) = current
                .iter()
                .find(|recipient| recipient.as_str() == selector)
            {
                selected.push(exact.clone());
                continue;
            }
            let matches: Vec<_> =
                current
                    .iter()
                    .filter(|recipient| {
                        state.team.as_ref().and_then(|team| {
                            team.task_assignments.get(&TaskId::new(selector).ok()?)
                        }) == Some(*recipient)
                    })
                    .collect();
            match matches.as_slice() {
                [] => bail!("recipient '{selector}' is not a current role instance or task"),
                [one] => selected.push((*one).clone()),
                _ => bail!("task name '{selector}' is ambiguous; use a role instance id"),
            }
        }
        selected
    };
    if selected.is_empty() {
        bail!("recipient set is empty");
    }
    if selected.len() > crate::model::MAX_MESSAGE_RECIPIENTS {
        bail!("message has too many recipients");
    }
    let mut ids = std::collections::BTreeSet::new();
    if selected
        .iter()
        .any(|recipient| !ids.insert(recipient.clone()))
    {
        bail!("recipient set contains a duplicate role instance");
    }
    Ok(selected)
}

pub async fn record_message(
    store: &MissionStore,
    repo: &std::path::Path,
    mission_id: &MissionId,
    command: MessageCommand,
    now_ms: i64,
) -> Result<()> {
    let MessageCommand {
        selectors,
        all,
        body,
        references,
    } = command;
    if body.len() > crate::model::MAX_MESSAGE_BYTES {
        bail!("message exceeds {} bytes", crate::model::MAX_MESSAGE_BYTES);
    }
    if references.len() > crate::model::MAX_MESSAGE_REFERENCES {
        return Err(ReferenceRejectionReason::Oversized { reference: None }.into());
    }
    let events = store.load(mission_id).await?;
    let state = crate::model::fold(events.clone()).context("mission has no creation event")?;
    let current: Vec<_> = state
        .conversations
        .iter()
        .filter(|(role_instance, _)| state.conversation_is_messageable(role_instance))
        .map(|(role_instance, _)| role_instance.clone())
        .collect();
    let recipients = resolve_message_recipients(&state, &current, &selectors, all)?;
    if let Some(recipient) = recipients
        .iter()
        .find(|recipient| !state.conversation_accepts_message(recipient))
    {
        return Err(ConversationQueueFull {
            role_instance: recipient.clone(),
            retained_messages: state.conversations[recipient].queued.len(),
            limit: crate::model::MAX_QUEUED_MESSAGES_PER_CONVERSATION,
        }
        .into());
    }
    validate_reference_eligibility(&state, &recipients, &references)?;
    for reference in &references {
        let valid = match reference {
            crate::model::MessageReference::AuthoritativeReceipt { effect_id } => {
                state.authoritative_receipts.contains(effect_id)
            }
            crate::model::MessageReference::ParkEvidence { effect_id } => {
                state.parked_effects.contains_key(effect_id)
            }
            crate::model::MessageReference::ReachableCommit { sha } => {
                state.reachable_commits.contains(sha)
            }
        };
        if !valid {
            let known =
                crate::reference_materialization::reference_was_authoritative(&events, reference)?;
            let reason = if known {
                ReferenceRejectionReason::Stale {
                    reference: reference.clone(),
                }
            } else {
                ReferenceRejectionReason::MalformedOrForeign {
                    reference: reference.clone(),
                }
            };
            return Err(reason.into());
        }
    }
    if let Err(error) = crate::reference_materialization::materialize_references(
        &state,
        &events,
        store.blobs(),
        repo,
        &references,
    )
    .await
    {
        let reason = match error.cause {
            crate::model::UnavailableReferenceCause::ExpansionLimitExceeded => {
                ReferenceRejectionReason::Oversized {
                    reference: Some(error.reference),
                }
            }
            crate::model::UnavailableReferenceCause::SourceMissing => {
                ReferenceRejectionReason::Missing {
                    reference: error.reference,
                }
            }
            crate::model::UnavailableReferenceCause::SourceUnreadable => {
                ReferenceRejectionReason::Unreadable {
                    reference: error.reference,
                }
            }
            crate::model::UnavailableReferenceCause::InvalidContent => {
                ReferenceRejectionReason::InvalidContent {
                    reference: error.reference,
                }
            }
        };
        return Err(reason.into());
    }
    store
        .append(
            mission_id,
            state.head,
            &[NewEvent::new(MissionEvent::MessageSent {
                recipients,
                body,
                references,
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

fn without_role_turn_evidence(mut failure: TypedFailure) -> TypedFailure {
    failure = failure.projected();
    failure.evidence_mut().final_response.clear();
    failure.evidence_mut().configuration = crate::model::RuntimeConfigurationEvidence::default();
    failure
}

/// Checkpoint selection is pure: recorded state policy plus the just-produced
/// outcome. The driver only performs the returned action.
fn checkpoint_after(
    event: &MissionEvent,
    effect: &InflightEffect,
    policy: &crate::model::ExecutionPolicy,
) -> Option<(bool, &'static str)> {
    match (event, effect) {
        (
            MissionEvent::RoleTurnCompleted { outcome: Ok(_), .. },
            InflightEffect::RoleTurn {
                output: crate::model::OutputSemantics::ProducesArtifact,
                ..
            },
        ) => Some((
            policy.auto_continue_candidate,
            "mission policy auto-continued writer completion",
        )),
        (
            MissionEvent::RoleTurnCompleted { outcome: Ok(_), .. },
            InflightEffect::RoleTurn {
                output: crate::model::OutputSemantics::EmitsVerdict,
                ..
            },
        ) => Some((
            policy.auto_continue_proof,
            "mission policy auto-continued advisory proof completion",
        )),
        (
            MissionEvent::RoleTurnCompleted { outcome: Ok(_), .. },
            InflightEffect::RoleTurn { .. },
        ) => Some((false, "agent response checkpoint")),
        (
            MissionEvent::OracleRunCompleted { outcome: Ok(_), .. },
            InflightEffect::OracleRun { .. },
        ) => Some((
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
    crate::model::resolve_execution_deadline_ms(requested_at_ms, duration_secs)
        .map_err(anyhow::Error::msg)
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

fn validated_role_success(
    outcome: crate::ports::RoleTurnOutcome,
    output: crate::model::OutputSemantics,
    base_sha: &str,
    mission_id: &crate::model::MissionId,
    effect_id: &crate::model::EffectId,
) -> std::result::Result<crate::ports::RoleTurnOutcome, TypedFailure> {
    let outcome = outcome.projected();
    if let Some(handoff) = &outcome.handoff {
        if let Err(failure) = crate::runner::validate_handoff(handoff, output) {
            return Err(with_role_outcome_evidence(failure, &outcome));
        }
    }
    if let Some(artifact) = &outcome.artifact {
        if let Err(detail) = artifact.validate_binding(mission_id, effect_id, base_sha) {
            return Err(invalid_role_outcome(
                "workspace.capture_authority",
                detail,
                &outcome,
            ));
        }
    }
    if let Some(handoff) = &outcome.handoff {
        if let Some(detail) = crate::model::role_success_contract_error(
            output,
            handoff,
            outcome
                .artifact
                .as_ref()
                .map(crate::ports::CapturedArtifact::as_outcome),
            base_sha,
        ) {
            return Err(invalid_role_outcome(
                "role.success_contract",
                detail,
                &outcome,
            ));
        }
    } else if let Some(failure) = missing_handoff_failure(output) {
        return Err(with_role_outcome_evidence(failure, &outcome));
    } else if outcome.artifact.is_some() {
        return Err(invalid_role_outcome(
            "role.success_contract",
            "a dialogue checkpoint without a handoff cannot publish an artifact",
            &outcome,
        ));
    }
    Ok(outcome)
}

fn render_conversation_message(
    message: &crate::model::QueuedMessage,
    expanded: &[crate::reference_materialization::MaterializedReference],
) -> String {
    let marker = match message.marker {
        crate::model::DeliveryMarker::Queued => "queued",
        crate::model::DeliveryMarker::PreviouslyDelivered => "previously delivered",
        crate::model::DeliveryMarker::PossiblyDelivered => "possibly delivered",
        crate::model::DeliveryMarker::Undeliverable => "undeliverable",
    };
    let references = expanded
        .iter()
        .map(|reference| {
            format!(
                "{} {}:\n{}",
                reference.label, reference.identity, reference.content
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    if references.is_empty() {
        format!(
            "Lead message [{}; sequence {}]: {}",
            marker, message.sequence_no, message.body
        )
    } else {
        format!(
            "Lead message [{}; sequence {}; references: {}]: {}",
            marker, message.sequence_no, references, message.body
        )
    }
}

fn missing_handoff_failure(output: crate::model::OutputSemantics) -> Option<TypedFailure> {
    output.requires_handoff().then(|| {
        TypedFailure::invalid(
            "handoff.missing",
            "this output contract requires a typed handoff",
        )
    })
}

fn invalid_role_outcome(
    code: impl Into<String>,
    detail: impl Into<String>,
    outcome: &crate::ports::RoleTurnOutcome,
) -> TypedFailure {
    with_role_outcome_evidence(TypedFailure::invalid(code, detail), outcome)
}

fn with_role_outcome_evidence(
    mut failure: TypedFailure,
    outcome: &crate::ports::RoleTurnOutcome,
) -> TypedFailure {
    failure.evidence_mut().final_response = outcome.final_response.clone();
    failure.evidence_mut().configuration =
        runtime_configuration_evidence(&outcome.runtime_configuration);
    failure.projected()
}
