//! The impure shell around the pure core: load → fold → reconcile → drive
//! effects → step → append, until the mission parks (durable interrupt) or
//! reaches a terminal phase. Structure ported from Zenith (Apache-2.0,
//! Intelligent Internet) `controller.py::advance_project` /
//! `coordinator.py::step`, re-based onto the event-sourced store.
//!
//! Every effect flows through the leased ledger; every outcome is recorded
//! in the same transaction that settles its effect row. Resume reconciles
//! before retrying: an unfinished LLM role run is synthesized as failed
//! (its outcome is unknowable without probing — Slice 3 adds probes), an
//! unfinished oracle run is simply re-queued (engine-run, reproducible).

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};

use crate::mission_type::MissionType;
use crate::model::{
    step, validate_plan_amendment, validate_plan_submission, AmendmentError, AmendmentOps,
    AttentionItem, Handoff, InflightEffect, MissionEvent, MissionId, MissionPhase, MissionState,
    OracleDispatchIntent, PayloadRef, PlanSubmission, PlanValidationError, RoleDispatchIntent,
    RunErrorKind, StepDecision,
};
use crate::ports::{Clock, OracleRunRequest, OracleRunner, RoleRunRequest, RoleRunner};
use crate::prompt::{assemble_role_prompt, PromptContext};
use crate::store::{AppendError, MissionStore, NewEvent};

pub struct Engine {
    store: MissionStore,
    mission_type: MissionType,
    role_runner: Arc<dyn RoleRunner>,
    oracle_runner: Arc<dyn OracleRunner>,
    clock: Arc<dyn Clock>,
    worker_id: String,
}

#[derive(Debug)]
pub enum AdvanceOutcome {
    /// Mission is in `Planning`; submit a plan to proceed.
    AwaitingPlan,
    /// Parked on open attention (durable interrupt, zero compute).
    Parked { attention: Vec<AttentionItem> },
    /// Effects are in flight under live leases held by another driver; this
    /// invocation has nothing to do. The next advance resumes.
    Busy,
    /// Done or aborted.
    Terminal { phase: MissionPhase },
}

#[derive(Debug, thiserror::Error)]
pub enum SubmitError {
    #[error("plan rejected:\n{}", .0.iter().map(ToString::to_string).collect::<Vec<_>>().join("\n"))]
    Invalid(Vec<PlanValidationError>),
    #[error("mission is not awaiting a plan (phase: {0})")]
    WrongPhase(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum AmendError {
    #[error("mission is busy (an effect is in flight); retry once it quiesces")]
    MissionBusy,
    #[error("mission is not amendable (phase: {0}); amend a running or parked mission")]
    WrongPhase(String),
    #[error(
        "stale amendment: you targeted revision {targeted}, the plan is now revision {current}"
    )]
    StaleRevision { targeted: u32, current: u32 },
    #[error(transparent)]
    Rejected(#[from] AmendmentError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

const EFFECT_LEASE_MS: i64 = 4 * 60 * 60 * 1000;
const MAX_LOOP_ITERATIONS: usize = 10_000;

impl Engine {
    pub fn new(
        store: MissionStore,
        mission_type: MissionType,
        role_runner: Arc<dyn RoleRunner>,
        oracle_runner: Arc<dyn OracleRunner>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let worker_id = format!("mission-engine-{}", std::process::id());
        Self {
            store,
            mission_type,
            role_runner,
            oracle_runner,
            clock,
            worker_id,
        }
    }

    pub fn store(&self) -> &MissionStore {
        &self.store
    }

    pub fn mission_type(&self) -> &MissionType {
        &self.mission_type
    }

    /// Create a mission. `base_sha` is the target repo's HEAD, observed by
    /// the caller (git stays out of the engine core).
    pub async fn create_mission(
        &self,
        workspace_dir: &str,
        objective: &str,
        base_sha: &str,
        config: crate::model::MissionConfig,
    ) -> Result<MissionId> {
        let now_ms = self.clock.now_ms();
        let mission_id = MissionId::from_digest_prefix(&hex::encode(Sha256::digest(
            format!("{workspace_dir}\u{1f}{objective}\u{1f}{now_ms}").as_bytes(),
        )));
        let created = NewEvent::new(MissionEvent::MissionCreated {
            objective: objective.to_string(),
            mission_type_name: self.mission_type.name.clone(),
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

    /// Validate and record a plan. Fail-closed: an invalid submission
    /// appends nothing.
    pub async fn submit_plan(
        &self,
        mission_id: &MissionId,
        submission: PlanSubmission,
    ) -> Result<(), SubmitError> {
        let state = self.load_state(mission_id).await?;
        if !matches!(state.phase, MissionPhase::Planning) {
            return Err(SubmitError::WrongPhase(format!("{:?}", state.phase)));
        }
        let errors = validate_plan_submission(&submission, &self.mission_type.inventory());
        if !errors.is_empty() {
            return Err(SubmitError::Invalid(errors));
        }
        let plan_json = serde_json::to_string(&submission).map_err(anyhow::Error::from)?;
        let plan_hash = hex::encode(Sha256::digest(plan_json.as_bytes()));
        let event = NewEvent::new(MissionEvent::PlanSubmitted {
            plan: submission,
            plan_hash,
        });
        self.store
            .append(mission_id, state.head, &[event], self.clock.now_ms())
            .await
            .map_err(|e| SubmitError::Other(e.into()))?;
        Ok(())
    }

    /// Amend a running mission's plan (add / supersede / cancel tasks,
    /// strengthen the contract). Fail-closed and quiescent: reconcile crashed
    /// leases first, then refuse (`MissionBusy`) if any effect is still in
    /// flight; refuse a stale amendment (`StaleRevision`); validate the whole
    /// resulting plan; append one `PlanAmended` fact event under the head
    /// guard. An invalid amendment records nothing.
    pub async fn amend_plan(
        &self,
        mission_id: &MissionId,
        ops: AmendmentOps,
        actor: &str,
        justification: &str,
        base_revision: u32,
    ) -> Result<(), AmendError> {
        // Check the cheap guards BEFORE reconciling, so a rejected amendment
        // has no side effects (reconcile appends synthesized-failure events).
        // Neither guard can be invalidated by reconcile: it never changes the
        // revision, nor moves a Running/AttentionNeeded mission out of those
        // phases (it only synthesizes a failure or re-queues an oracle).
        let mut state = self.load_state(mission_id).await?;
        if !matches!(
            state.phase,
            MissionPhase::Running | MissionPhase::AttentionNeeded
        ) {
            return Err(AmendError::WrongPhase(format!("{:?}", state.phase)));
        }
        if base_revision != state.revision {
            return Err(AmendError::StaleRevision {
                targeted: base_revision,
                current: state.revision,
            });
        }
        // Quiesce: reconcile crashed leases so only genuinely-live effects
        // block, then require an empty in-flight set (whole-mission quiesce).
        while !state.inflight.is_empty() {
            if self.reconcile(&state).await? {
                state = self.load_state(mission_id).await?;
            } else {
                break;
            }
        }
        if !state.inflight.is_empty() {
            return Err(AmendError::MissionBusy);
        }
        validate_plan_amendment(&state, &ops, &self.mission_type.inventory())?;
        let event = NewEvent::new(MissionEvent::PlanAmended {
            base_revision,
            ops,
            actor: actor.to_string(),
            justification: justification.to_string(),
        });
        self.store
            .append(mission_id, state.head, &[event], self.clock.now_ms())
            .await
            .map_err(|e| AmendError::Other(e.into()))?;
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
        actor: &str,
    ) -> Result<()> {
        record_decision(
            &self.store,
            self.clock.now_ms(),
            mission_id,
            attention_id,
            action,
            justification,
            actor,
        )
        .await
    }

    pub async fn load_state(&self, mission_id: &MissionId) -> Result<MissionState> {
        self.store
            .load_state_snapshotted(mission_id)
            .await?
            .with_context(|| format!("mission {mission_id} not found"))
    }

    /// Drive the mission until it parks, terminates, or awaits input.
    pub async fn advance(&self, mission_id: &MissionId) -> Result<AdvanceOutcome> {
        let outcome = self.drive(mission_id).await?;
        // Persist a fold snapshot before parking or exiting so the next
        // invocation resumes without re-folding the whole log.
        let state = self.load_state(mission_id).await?;
        self.store
            .save_snapshot(&state, self.clock.now_ms())
            .await?;
        Ok(outcome)
    }

    async fn drive(&self, mission_id: &MissionId) -> Result<AdvanceOutcome> {
        for _ in 0..MAX_LOOP_ITERATIONS {
            let state = self.load_state(mission_id).await?;
            if !state.inflight.is_empty() {
                if self.reconcile(&state).await? {
                    continue; // reconcile appended an outcome; refold
                }
                if self.drive_one(&state).await? {
                    continue; // drove an effect; refold
                }
                // Inflight effects remain but none were reconcilable or due:
                // they are held by live leases (another driver is running
                // them). Return rather than spin; the next advance resumes.
                return Ok(AdvanceOutcome::Busy);
            }
            match step(&state) {
                StepDecision::Idle => {
                    return match state.phase {
                        MissionPhase::Planning => Ok(AdvanceOutcome::AwaitingPlan),
                        phase => bail!("engine idle in unexpected phase {phase:?}"),
                    };
                }
                StepDecision::Park => {
                    return Ok(AdvanceOutcome::Parked {
                        attention: state.open_attention.values().cloned().collect(),
                    });
                }
                StepDecision::Terminal => {
                    return Ok(AdvanceOutcome::Terminal { phase: state.phase });
                }
                StepDecision::DispatchRole(intent) => {
                    self.materialize_role_request(&state, intent).await?;
                }
                StepDecision::RunOracles(intents) => {
                    self.materialize_oracle_requests(&state, intents).await?;
                }
            }
        }
        bail!("advance exceeded {MAX_LOOP_ITERATIONS} iterations; aborting as a safety stop")
    }

    /// Settle inflight effects whose ledger row is no longer runnable: a
    /// role run mid-crash is unknowable → synthesized failure (zenith's
    /// `_reconcile_pending_attempts` discipline); an oracle or review run is
    /// reproducible → re-queued. Returns true when an event was appended
    /// (caller must refold before driving).
    ///
    /// A **live (unexpired) lease is left alone**: another driver still owns
    /// the effect, and stealing it would fabricate failure over a running LLM
    /// or double-run an oracle. Only expired or never-leased effects are
    /// reconciled — mirroring `pull_due`'s eligibility.
    async fn reconcile(&self, state: &MissionState) -> Result<bool> {
        let now_ms = self.clock.now_ms();
        for (key, effect) in &state.inflight {
            let Some(status) = self.store.effect_status(key).await? else {
                continue; // ledger row missing; nothing to reconcile
            };
            if status.status == "queued" {
                continue; // normal path: drive_one will lease it
            }
            if status.is_live_lease(now_ms) {
                continue; // a concurrent driver owns it; do not disturb
            }
            match effect {
                InflightEffect::RoleRun {
                    task_id,
                    attempt_no,
                    ..
                } => {
                    let event = NewEvent::new(MissionEvent::RoleRunFailed {
                        task_id: task_id.clone(),
                        attempt_no: *attempt_no,
                        idempotency_key: key.clone(),
                        error_kind: RunErrorKind::Infra,
                        detail: "resumed with an expired role-run lease; outcome unknowable"
                            .to_string(),
                        synthesized: true,
                    });
                    match self
                        .store
                        .append(&state.mission_id, state.head, &[event], now_ms)
                        .await
                    {
                        Ok(_)
                        | Err(AppendError::Duplicate { .. })
                        | Err(AppendError::Conflict { .. }) => {}
                        Err(err) => return Err(err.into()),
                    }
                    // One reconcile action per pass; refold before the next.
                    return Ok(true);
                }
                InflightEffect::OracleRun { .. } => {
                    self.store.requeue_effect(key, now_ms).await?;
                }
            }
        }
        Ok(false)
    }

    /// Lease and execute one due effect, recording its outcome. Returns
    /// whether an effect was driven (false = nothing was due to lease).
    /// Serial by design in the walking skeleton; validators parallelize later.
    async fn drive_one(&self, state: &MissionState) -> Result<bool> {
        let now_ms = self.clock.now_ms();
        let leases = self
            .store
            .pull_due(
                &state.mission_id,
                &self.worker_id,
                1,
                EFFECT_LEASE_MS,
                now_ms,
            )
            .await?;
        let Some(lease) = leases.into_iter().next() else {
            return Ok(false);
        };
        let outcome = match &lease.request {
            InflightEffect::RoleRun { .. } => {
                self.execute_role_run(state, &lease.effect_id, &lease.request)
                    .await?
            }
            InflightEffect::OracleRun { .. } => {
                self.execute_oracle_run(state, &lease.effect_id, &lease.request)
                    .await?
            }
        };
        match self
            .store
            .append(
                &state.mission_id,
                state.head,
                &[outcome],
                self.clock.now_ms(),
            )
            .await
        {
            Ok(_) => Ok(true),
            // Outcome already recorded (concurrent driver) — reconcile wins.
            Err(AppendError::Duplicate { .. }) | Err(AppendError::Conflict { .. }) => Ok(true),
            Err(err) => Err(err.into()),
        }
    }

    async fn execute_role_run(
        &self,
        state: &MissionState,
        idempotency_key: &str,
        effect: &InflightEffect,
    ) -> Result<NewEvent> {
        let InflightEffect::RoleRun {
            task_id,
            attempt_no,
            role: role_name,
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
                idempotency_key: idempotency_key.to_string(),
                error_kind: kind,
                detail,
                synthesized: false,
            })
        };
        let Some(role) = self.mission_type.roles.get(role_name) else {
            return Ok(failed(
                RunErrorKind::Launch,
                format!("role '{role_name}' is no longer provided by the mission type"),
            ));
        };
        let prompt_text = self.store.blobs().resolve(prompt)?;
        let request = RoleRunRequest {
            mission_id: state.mission_id.clone(),
            task_id: task_id.clone(),
            attempt_no,
            idempotency_key: idempotency_key.to_string(),
            role: role.clone(),
            prompt: prompt_text,
            base_sha: base_sha.to_string(),
            workspace_dir: state.workspace_dir.clone().into(),
            state_dir: self.store.lionclaw_dir().to_path_buf(),
        };
        match self.role_runner.run(request).await {
            Ok(outcome) => {
                let handoff = self.externalize_handoff(outcome.handoff)?;
                Ok(NewEvent::new(MissionEvent::RoleRunCompleted {
                    task_id: task_id.clone(),
                    attempt_no,
                    idempotency_key: idempotency_key.to_string(),
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
        idempotency_key: &str,
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
                idempotency_key: idempotency_key.to_string(),
                detail,
                synthesized: false,
            })
        };
        let Some(oracle_path) = self.mission_type.oracles.get(oracle) else {
            return Ok(failed(format!(
                "oracle '{oracle}' is no longer provided by the mission type"
            )));
        };
        let request = OracleRunRequest {
            mission_id: state.mission_id.clone(),
            oracle: oracle.clone(),
            oracle_path: oracle_path.clone(),
            assertion_ids: assertion_ids.to_vec(),
            judged_sha: judged_sha.to_string(),
            workspace_dir: state.workspace_dir.clone().into(),
            state_dir: self.store.lionclaw_dir().to_path_buf(),
        };
        match self.oracle_runner.run(request).await {
            Ok(outcome) => Ok(NewEvent::new(MissionEvent::OracleRunCompleted {
                assertion_ids: assertion_ids.to_vec(),
                oracle: oracle.clone(),
                judged_sha: judged_sha.to_string(),
                attempt_no,
                idempotency_key: idempotency_key.to_string(),
                exit_code: outcome.exit_code,
                exit_signal: outcome.exit_signal,
                stdout: self.store.blobs().payload_from_bytes(&outcome.stdout)?,
                stderr: self.store.blobs().payload_from_bytes(&outcome.stderr)?,
                duration_ms: outcome.duration_ms,
            })),
            Err(failure) => Ok(failed(failure.detail)),
        }
    }

    /// Turn a role-dispatch intent into a recorded request: assemble the
    /// prompt (engine-owned), persist it, derive the idempotency key.
    async fn materialize_role_request(
        &self,
        state: &MissionState,
        intent: RoleDispatchIntent,
    ) -> Result<()> {
        let plan = state.plan.as_ref().context("dispatch without a plan")?;
        let targets: Vec<_> = plan
            .assertions
            .iter()
            .filter(|a| intent.targets.contains(&a.id))
            .collect();
        let role = self
            .mission_type
            .roles
            .get(&intent.role)
            .with_context(|| format!("role '{}' missing from the mission type", intent.role))?;
        // Thread the reports of this task's dependencies in (resolved from
        // blob refs). Prompt assembly excludes them for verdict roles.
        let task = plan
            .tasks
            .iter()
            .find(|t| t.id == intent.task_id)
            .context("dispatched task not in plan")?;
        let mut upstream_reports = Vec::new();
        for dep in &task.depends_on {
            if let Some(report) = state.tasks.get(dep).and_then(|t| t.last_report.as_ref()) {
                upstream_reports.push(self.store.blobs().resolve(report)?);
            }
        }
        let prompt_text = assemble_role_prompt(
            role,
            &PromptContext {
                objective: &state.objective,
                task_body: &intent.body,
                targets: &targets,
                upstream_reports: &upstream_reports,
            },
        );
        let prompt_hash = hex::encode(Sha256::digest(prompt_text.as_bytes()));
        let prompt = self
            .store
            .blobs()
            .externalize(PayloadRef::inline(prompt_text))?;
        let idempotency_key = idem_key(&[
            "role",
            state.mission_id.as_str(),
            intent.task_id.as_str(),
            &intent.attempt_no.to_string(),
            &prompt_hash,
        ]);
        let event = NewEvent::new(MissionEvent::RoleRunRequested {
            task_id: intent.task_id,
            attempt_no: intent.attempt_no,
            idempotency_key,
            role: intent.role,
            prompt,
            base_sha: intent.base_sha,
        })
        .with_prompt_hash(prompt_hash);
        match self
            .store
            .append(&state.mission_id, state.head, &[event], self.clock.now_ms())
            .await
        {
            Ok(_) => Ok(()),
            Err(AppendError::Duplicate { .. }) | Err(AppendError::Conflict { .. }) => Ok(()),
            Err(err) => Err(err.into()),
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
                let idempotency_key = idem_key(&[
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
                    idempotency_key,
                })
            })
            .collect();
        match self
            .store
            .append(&state.mission_id, state.head, &events, self.clock.now_ms())
            .await
        {
            Ok(_) => Ok(()),
            Err(AppendError::Duplicate { .. }) | Err(AppendError::Conflict { .. }) => Ok(()),
            Err(err) => Err(err.into()),
        }
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
        })
    }
}

/// Content-derived idempotency key: stable across resume, unique per
/// logical effect.
fn idem_key(parts: &[&str]) -> String {
    hex::encode(Sha256::digest(parts.join("\u{1f}").as_bytes()))
}

/// Record a decision without a full engine (the CLI's `ratify`/`decide` need
/// only the store). Folds current state, validates fail-closed, appends.
pub async fn record_decision(
    store: &MissionStore,
    now_ms: i64,
    mission_id: &MissionId,
    attention_id: &str,
    action: crate::model::DecisionAction,
    justification: &str,
    actor: &str,
) -> Result<()> {
    let state = store
        .load_state_snapshotted(mission_id)
        .await?
        .with_context(|| format!("mission {mission_id} not found"))?;
    crate::model::validate_decision(&state, attention_id, &action)
        .map_err(|e| anyhow::anyhow!("decision rejected: {e}"))?;
    let event = NewEvent::new(MissionEvent::DecisionRecorded {
        attention_id: attention_id.to_string(),
        action,
        justification: justification.to_string(),
        actor: actor.to_string(),
    });
    store
        .append(mission_id, state.head, &[event], now_ms)
        .await?;
    Ok(())
}
