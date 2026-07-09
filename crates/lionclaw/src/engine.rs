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
use crate::prompt::{
    assemble_planning_prompt, assemble_role_prompt, PlanningPromptContext, PromptContext,
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
    clock: Arc<dyn Clock>,
    worker_id: String,
}

#[derive(Debug)]
pub enum AdvanceOutcome {
    /// Mission is in `Planning` with no runnable planning DAG; submit a plan to
    /// proceed.
    AwaitingPlan,
    /// Parked on open attention (durable interrupt, zero compute).
    Parked { attention: Vec<AttentionItem> },
    /// Effects are in flight under live leases held by another driver; this
    /// invocation has nothing to do. The next advance resumes.
    Busy,
    /// Done or aborted.
    Terminal { phase: MissionPhase },
}

impl AdvanceOutcome {
    /// The stable snake_case name for `--json` output.
    pub const fn slug(&self) -> &'static str {
        match self {
            Self::AwaitingPlan => "awaiting_plan",
            Self::Parked { .. } => "parked",
            Self::Busy => "busy",
            Self::Terminal { .. } => "terminal",
        }
    }
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
        runtime: String,
        image_id: String,
        role_runner: Arc<dyn RoleRunner>,
        oracle_runner: Arc<dyn OracleRunner>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let worker_id = format!("mission-engine-{}", std::process::id());
        Self {
            store,
            mission_type,
            runtime,
            image_id,
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
        let mission_id = MissionId::from_digest_prefix(&hex::encode(Sha256::digest(
            format!("{workspace_dir}\u{1f}{objective}\u{1f}{now_ms}").as_bytes(),
        )));
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
        let state = self.store.require_state(mission_id).await?;
        // The instrument of judgment is pinned: this verifies the mission type's
        // content digest against the one recorded at start, so a mutated role or
        // oracle cannot advance this mission (the fake-green vector). Every method
        // that loads the pinned type — submit_plan, amend_plan, advance/drive —
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
    /// `_reconcile_pending_attempts` discipline); an oracle run is
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
                    self.append_idempotent(&state.mission_id, state.head, &[event])
                        .await?;
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
    /// One driver turn claims one effect; durable leases coordinate concurrent
    /// drivers without requiring an in-process scheduler.
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
        // The computed outcome is unique — for a role run, not reproducible — so a
        // stale head must NOT discard it (unlike the request/reconcile appends). A
        // concurrent driver settling a DIFFERENT effect moved the head, but our
        // outcome is still unrecorded, so re-append at the refreshed head. A
        // Duplicate means it is already in the log (we hold the lease, so only a
        // zombie re-drive), which is genuinely done.
        let mut head = state.head;
        for _ in 0..MAX_LOOP_ITERATIONS {
            match self
                .store
                .append(
                    &state.mission_id,
                    head,
                    std::slice::from_ref(&outcome),
                    self.clock.now_ms(),
                )
                .await
            {
                Ok(_) | Err(AppendError::Duplicate { .. }) => return Ok(true),
                Err(AppendError::Conflict { .. }) => {
                    head = self.load_state(&state.mission_id).await?.head;
                }
                Err(err) => return Err(err.into()),
            }
        }
        bail!("outcome append kept conflicting after {MAX_LOOP_ITERATIONS} retries")
    }

    /// Append events, treating a `Conflict`/`Duplicate` as an idempotent no-op.
    /// Safe only for *request* and *reconcile* appends: a Conflict means a
    /// concurrent driver moved the head, and the next fold re-derives the same
    /// dispatch. Outcome appends do NOT use this — `drive_one` must re-append its
    /// unique computed outcome rather than discard it.
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
                // A planning author's proposal is validated fail-closed before
                // it is recorded, exactly like a manually submitted plan — an
                // invalid proposal is a failed attempt, never a bad contract.
                if let Handoff::Plan {
                    done: true,
                    proposal,
                    ..
                } = &outcome.handoff
                {
                    let Some(plan) = proposal else {
                        return Ok(failed(
                            RunErrorKind::HandoffInvalid,
                            "planning author reported done but proposed no plan".to_string(),
                        ));
                    };
                    let errors = validate_plan_submission(plan, &self.mission_type.inventory());
                    if !errors.is_empty() {
                        let detail = errors
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join("; ");
                        return Ok(failed(
                            RunErrorKind::HandoffInvalid,
                            format!("proposed plan is invalid: {detail}"),
                        ));
                    }
                }
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

    /// Assemble an execution role's prompt (`("role", …)` idempotency namespace).
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
        let prompt = assemble_role_prompt(
            role,
            &PromptContext {
                objective: &state.objective,
                task_body: &intent.body,
                targets: &targets,
                upstream_reports: &upstream_reports,
            },
        );
        Ok((prompt, "role"))
    }

    /// Assemble a planning role's prompt (`("plan-role", …)` namespace). Threads
    /// the mission type's playbook + oracle inventory + upstream planning reports
    /// through a separate assembler.
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
        let oracle_inventory: Vec<String> = self
            .mission_type
            .oracles
            .keys()
            .map(|o| o.as_str().to_string())
            .collect();
        let prompt = assemble_planning_prompt(
            role,
            &PlanningPromptContext {
                objective: &state.objective,
                playbook: self.mission_type.playbook.as_deref(),
                oracle_inventory: &oracle_inventory,
                task_body: &intent.body,
                upstream_reports: &upstream_reports,
            },
        );
        Ok((prompt, "plan-role"))
    }

    /// Turn a role-dispatch intent into a recorded request: assemble the
    /// prompt (engine-owned), persist it, derive the idempotency key.
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
        // Planning and execution assemble prompts and namespace idempotency keys
        // separately, so a planning report can never reach an execution judge and
        // a planning id can never collide with an execution one.
        let (prompt_text, idem_namespace) = if state.plan.is_none() {
            self.assemble_planning_request(state, role, &intent)?
        } else {
            self.assemble_execution_request(state, role, &intent)?
        };
        let prompt_hash = hex::encode(Sha256::digest(prompt_text.as_bytes()));
        let prompt = self
            .store
            .blobs()
            .externalize(PayloadRef::inline(prompt_text))?;
        let idempotency_key = idem_key(&[
            idem_namespace,
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
    let state = store.require_state(mission_id).await?;
    // Preserve the typed `DecisionError` as the error source (its `Display` is
    // already specific: unknown item vs illegal action for the item's kind), so
    // a JSON caller sees the real reason, not a flattened string.
    crate::model::validate_decision(&state, attention_id, &action)?;
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
