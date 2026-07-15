//! The pure fold: `state = fold(log)`. No I/O, no clock, no RNG — enforced
//! by the model's dependency wall and the crate clippy config. Every
//! engine-deterministic transition happens here; the phase (including the
//! finish class) is re-derived after every event, never stored.
//!
//! Handoff application semantics ported from Zenith (Apache-2.0,
//! Intelligent Internet) `coordinator.py::_apply_handoff_collect`: a work
//! task that isn't `done` fails and raises attention; a validate task always
//! clears and folds its per-assertion verdicts in with sticky passes.

use std::collections::{BTreeMap, BTreeSet};

use super::event::{
    EventEnvelope, GapSeverity, Handoff, MissionEvent, PayloadRef, RuntimeConfigurationEvidence,
};
use super::ids::{AssertionId, TaskId};
use super::plan::Assertion;
use super::state::{
    AdvisoryStatus, AssertionState, AttentionItem, AttentionKind, InflightEffect, MissionPhase,
    MissionState, PlanningInput, PlanningRefinement, PlanningState, ReviewAcceptance,
    ReviewAcceptanceKind, ReviewOutcome, TaskRuntimeState, TaskStatus, TerminalReviewVerdict,
};
use super::verdict::{classify_finish, AuthoritativeVerdict};

/// Bump when fold semantics change; snapshots with a different version are
/// discarded and rebuilt from sequence zero.
pub const REDUCER_VERSION: u32 = 9;

/// Fold a mission's event stream. `None` until a `MissionCreated` arrives.
pub fn fold(events: impl IntoIterator<Item = EventEnvelope>) -> Option<MissionState> {
    let mut state: Option<MissionState> = None;
    for envelope in events {
        match state.as_mut() {
            None => state = bootstrap(&envelope),
            Some(state) => apply(state, &envelope),
        }
    }
    state
}

fn bootstrap(envelope: &EventEnvelope) -> Option<MissionState> {
    let MissionEvent::MissionCreated {
        objective,
        mission_type,
        runtime,
        image_id,
        workspace_dir,
        base_sha,
        config,
    } = &envelope.event
    else {
        return None;
    };
    // Seed the planning DAG's nodes as Pending so they are runnable from event 1.
    let planning_tasks = config
        .planning
        .tasks
        .iter()
        .map(|t| (t.id.clone(), pending_task()))
        .collect();
    Some(MissionState {
        mission_id: envelope.mission_id.clone(),
        objective: objective.clone(),
        mission_type: mission_type.clone(),
        runtime: runtime.clone(),
        image_id: image_id.clone(),
        workspace_dir: workspace_dir.clone(),
        base_sha: base_sha.clone(),
        config: config.clone(),
        phase: MissionPhase::Planning,
        plan: None,
        contract: Default::default(),
        tasks: Default::default(),
        planning: PlanningState {
            tasks: planning_tasks,
        },
        planning_base_revision: (!config.planning.tasks.is_empty()).then_some(0),
        planning_input: PlanningInput {
            latest_rejected_proposal: None,
            refinement: None,
        },
        proposal: None,
        current_sha: base_sha.clone(),
        oracle_attempts: Default::default(),
        inflight: Default::default(),
        cleanup_failure: None,
        open_attention: Default::default(),
        proposal_approved: false,
        revision: 0,
        acknowledged_gates: Default::default(),
        flagged_nodes: Default::default(),
        oracle_failures: Default::default(),
        waived_oracles: Default::default(),
        terminal_review: Default::default(),
        head: envelope.sequence_no,
    })
}

/// Apply one event. Total: impossible transitions are deterministic no-ops,
/// never panics — the log is the source of truth even when an older engine
/// wrote it.
pub fn apply(state: &mut MissionState, envelope: &EventEnvelope) {
    let seq = envelope.sequence_no;
    match &envelope.event {
        MissionEvent::MissionCreated { .. } => {}
        MissionEvent::PlanProposed { proposal, .. } => {
            if super::plan_validation::validate_plan_transition(state, proposal).is_ok() {
                state.proposal = Some(proposal.clone());
                state.planning_base_revision = None;
                state.proposal_approved = false;
            }
        }
        MissionEvent::RoleRunRequested {
            task_id,
            attempt_no,
            base_sha,
            assignment_epoch,
            ..
        } => {
            let tasks = state.active_tasks_mut();
            let task = tasks.entry(task_id.clone()).or_insert_with(pending_task);
            task.status = TaskStatus::Running;
            task.attempts = *attempt_no;
            task.workspace_base_sha = Some(base_sha.clone());
            task.assignment_epoch = *assignment_epoch;
            track_inflight(state, &envelope.event, seq);
        }
        MissionEvent::RoleRunCompleted {
            task_id,
            attempt_no,
            effect_id,
            outcome,
        } => {
            state.inflight.remove(effect_id);
            clear_cleanup_failure(state, effect_id);
            match outcome {
                Ok(success) => {
                    if let Some(artifact) = &success.artifact {
                        state.current_sha = artifact.head_sha.clone();
                    }
                    if let Some(task) = state.active_tasks_mut().get_mut(task_id) {
                        task.attempts = task.attempts.max(*attempt_no);
                    }
                    apply_handoff(state, task_id, &success.handoff);
                    if let Some(task) = state.active_tasks_mut().get_mut(task_id) {
                        task.last_runtime_configuration =
                            Some(success.runtime_configuration.clone());
                        task.final_response = Some(success.final_response.clone());
                        if task.status == TaskStatus::Failed {
                            task.consecutive_failures = task.consecutive_failures.saturating_add(1);
                        } else {
                            task.last_failure = None;
                            task.consecutive_failures = 0;
                        }
                    }
                }
                Err(failure) => {
                    if let Some(task) = state.active_tasks_mut().get_mut(task_id) {
                        task.attempts = task.attempts.max(*attempt_no);
                        task.status = TaskStatus::Failed;
                        task.last_failure = Some(failure.clone());
                        let evidence = failure.evidence();
                        task.final_response = (!evidence.final_response.is_empty())
                            .then(|| PayloadRef::inline(evidence.final_response.clone()));
                        task.last_runtime_configuration = Some(RuntimeConfigurationEvidence {
                            requested_model: evidence.configuration.requested_model.clone(),
                            applied_model: evidence.configuration.applied_model.clone(),
                            requested_mode: evidence.configuration.requested_mode.clone(),
                            applied_mode: evidence.configuration.applied_mode.clone(),
                        });
                        task.consecutive_failures = task.consecutive_failures.saturating_add(1);
                    }
                }
            }
        }
        MissionEvent::OracleRunRequested {
            oracle, attempt_no, ..
        } => {
            state.oracle_attempts.insert(oracle.clone(), *attempt_no);
            track_inflight(state, &envelope.event, seq);
        }
        MissionEvent::OracleRunCompleted {
            assertion_ids,
            oracle,
            judged_sha,
            effect_id,
            outcome,
            ..
        } => {
            state.inflight.remove(effect_id);
            clear_cleanup_failure(state, effect_id);
            match outcome {
                Ok(success) => {
                    state.oracle_failures.remove(oracle);
                    let verdict = AuthoritativeVerdict::from_oracle_outcome(
                        oracle.clone(),
                        judged_sha.clone(),
                        success.exit_code,
                        success.exit_signal,
                        success.stdout.clone(),
                        success.stderr.clone(),
                        success.prepared_inputs.clone(),
                    );
                    for assertion_id in assertion_ids {
                        if let Some(assertion) = state.contract.get_mut(assertion_id) {
                            assertion.last_authoritative = Some(verdict.clone());
                        }
                    }
                }
                Err(failure) => {
                    state
                        .oracle_failures
                        .insert(oracle.clone(), failure.clone());
                }
            }
        }
        // `attempts` is the highest attempt number the log has seen — from
        // ANY review event, not just requests: the materialize fallback
        // records a `Failed` without a `Requested`, and a retry must still
        // dispatch under a fresh attempt (and a fresh effect ID), never
        // spin the drive loop re-appending a duplicate.
        MissionEvent::TerminalReviewRequested { attempt_no, .. } => {
            state.terminal_review.attempts = (*attempt_no).max(state.terminal_review.attempts);
            track_inflight(state, &envelope.event, seq);
        }
        MissionEvent::TerminalReviewCompleted {
            attempt_no,
            effect_id,
            judged_sha,
            outcome,
        } => {
            state.inflight.remove(effect_id);
            clear_cleanup_failure(state, effect_id);
            state.terminal_review.attempts = (*attempt_no).max(state.terminal_review.attempts);
            match outcome {
                Ok(success) => {
                    state.terminal_review.consecutive_failures = 0;
                    state.terminal_review.outcome =
                        Some(ReviewOutcome::Verdict(TerminalReviewVerdict {
                            judged_sha: judged_sha.clone(),
                            passed: success.passed,
                            gaps: success.gaps.clone(),
                            report: success.report.clone(),
                        }));
                }
                Err(failure) => {
                    state.terminal_review.consecutive_failures =
                        state.terminal_review.consecutive_failures.saturating_add(1);
                    state.terminal_review.outcome = Some(ReviewOutcome::Failed {
                        failure: failure.clone(),
                    });
                }
            }
        }
        MissionEvent::MissionAborted { reason, .. } => {
            state.phase = MissionPhase::Aborted {
                reason: reason.clone(),
            };
        }
        MissionEvent::DecisionRecorded {
            attention_id,
            action,
            justification,
        } => {
            apply_decision(state, attention_id, action, justification);
        }
        MissionEvent::EffectCleanupFailed {
            effect_id,
            resource,
            failure,
        } => {
            state.cleanup_failure = Some(super::EffectCleanupFailure {
                effect_id: effect_id.clone(),
                resource: *resource,
                failure: failure.clone(),
            });
        }
    }
    state.head = seq;
    // Promotion runs first: a just-approved proposal must seed the contract
    // before gates/attention/phase are derived this same fold (a plan whose only
    // sink is a gate would otherwise hang one event behind).
    derive_promotion(state);
    derive_gates(state);
    derive_attention(state);
    derive_phase(state);
}

fn clear_cleanup_failure(state: &mut MissionState, effect_id: &super::EffectId) {
    if state
        .cleanup_failure
        .as_ref()
        .is_some_and(|failure| &failure.effect_id == effect_id)
    {
        state.cleanup_failure = None;
    }
}

/// Promote one complete, approved proposal. Retained task ids are immutable;
/// omitted tasks become audit tombstones and new task ids start pending.
fn derive_promotion(state: &mut MissionState) {
    if state.proposal.is_none() || !state.proposal_approved {
        return;
    }
    let proposal = state.proposal.as_ref().expect("proposal present");
    if super::plan_validation::validate_plan_transition(state, proposal).is_err() {
        return;
    }
    let proposal = state
        .proposal
        .take()
        .expect("proposal present (checked above)");
    let next = proposal.plan;
    let live: BTreeSet<_> = next.tasks.iter().map(|task| &task.id).collect();
    let retired: Vec<_> = state
        .plan
        .iter()
        .flat_map(|plan| &plan.tasks)
        .filter(|task| !live.contains(&task.id))
        .map(|task| task.id.clone())
        .collect();
    for task_id in retired {
        if let Some(task) = state.tasks.get_mut(&task_id) {
            task.status = TaskStatus::Superseded;
        }
        state.acknowledged_gates.remove(&task_id);
        state.flagged_nodes.remove(&task_id);
        for assertion in state.contract.values_mut() {
            if assertion.last_advisory.remove(&task_id).is_some() {
                assertion.advisory = recompute_advisory(&assertion.last_advisory);
            }
        }
    }
    for assertion in &next.assertions {
        seed_assertion(&mut state.contract, assertion);
        if let Some(existing) = state.contract.get_mut(&assertion.id) {
            existing.oracle = assertion.oracle.clone();
        }
    }
    for task in &next.tasks {
        seed_task(&mut state.tasks, &task.id);
    }
    state.plan = Some(next);
    state.revision += 1;
}

/// The sticky advisory status implied by a set of per-validator verdicts: a
/// pass anywhere wins (sticky), else a fail, else pending. Matches the
/// accumulation in `apply_handoff`, used to re-derive an assertion's advisory
/// after a re-planned validator's verdict is scrubbed.
fn recompute_advisory(last_advisory: &BTreeMap<TaskId, bool>) -> AdvisoryStatus {
    if last_advisory.values().any(|&p| p) {
        AdvisoryStatus::Passed
    } else if !last_advisory.is_empty() {
        AdvisoryStatus::Failed
    } else {
        AdvisoryStatus::Pending
    }
}

/// Seed a task's runtime as `Pending` (idempotent — keeps any existing entry).
/// Shared by initial and revision proposal folds.
/// A freshly-seeded task: pending, no attempts, no report.
fn pending_task() -> TaskRuntimeState {
    TaskRuntimeState {
        status: TaskStatus::Pending,
        attempts: 0,
        consecutive_failures: 0,
        last_report: None,
        last_failure: None,
        feedback: Vec::new(),
        last_runtime_configuration: None,
        workspace_base_sha: None,
        assignment_epoch: 0,
        final_response: None,
    }
}

fn seed_task(tasks: &mut BTreeMap<TaskId, TaskRuntimeState>, id: &TaskId) {
    tasks.entry(id.clone()).or_insert_with(pending_task);
}

/// Seed an assertion's runtime (idempotent — keeps any existing verdicts).
fn seed_assertion(contract: &mut BTreeMap<AssertionId, AssertionState>, assertion: &Assertion) {
    contract
        .entry(assertion.id.clone())
        .or_insert_with(|| AssertionState {
            oracle: assertion.oracle.clone(),
            advisory: AdvisoryStatus::Pending,
            last_advisory: Default::default(),
            last_authoritative: None,
        });
}

/// Gate status is derived, never an event: a gate whose dependencies are all
/// cleared evaluates its upstream validators (AND semantics). A cleared gate
/// still raises a checkpoint (zenith's discipline — a human confirms before
/// the mission proceeds past it); a failed gate raises `gate_failed`. Both
/// pause the mission until a human decision resolves them.
fn derive_gates(state: &mut MissionState) {
    let Some(plan) = state.plan.clone() else {
        return;
    };
    for task in &plan.tasks {
        if task.kind != super::plan::TaskKind::Gate {
            continue;
        }
        let status = state.tasks.get(&task.id).map(|t| t.status);
        if status != Some(TaskStatus::Pending) {
            continue; // already resolved this fold-run or superseded
        }
        let deps_cleared = task
            .depends_on
            .iter()
            .all(|dep| state.tasks.get(dep).map(|t| t.status) == Some(TaskStatus::Cleared));
        if !deps_cleared {
            continue;
        }
        let new_status = match super::gate::evaluate_gate(state, &plan, &task.id) {
            super::gate::GateResult::Cleared => TaskStatus::Cleared,
            super::gate::GateResult::Blocked { .. } => TaskStatus::Failed,
        };
        if let Some(entry) = state.tasks.get_mut(&task.id) {
            entry.status = new_status;
        }
    }
}

/// Apply a decision to the state it resolves. Reads the *previous* fold's
/// derived attention (still in `open_attention` at this point) to learn the
/// item's kind and node, so no id parsing is needed. Invalid (action, kind)
/// pairs are rejected before recording (see `decision::validate_decision`);
/// here they are no-ops.
fn apply_decision(
    state: &mut MissionState,
    attention_id: &str,
    action: &super::event::DecisionAction,
    justification: &str,
) {
    use super::event::DecisionAction;
    let Some(item) = state.open_attention.get(attention_id).cloned() else {
        return; // unknown or already-resolved item
    };
    // Planning and execution ids live in separate maps. The active era owns
    // every node decision, even when both maps contain the same id.
    let node_status = |state: &mut MissionState, task_id, status| {
        if let Some(task) = state.active_tasks_mut().get_mut(task_id) {
            task.status = status;
        }
    };
    match (action, item.kind) {
        (DecisionAction::Approve, AttentionKind::PlanProposal) => {
            state.proposal_approved = true;
            state.planning_input.latest_rejected_proposal = None;
            state.planning_input.refinement = None;
        }
        (DecisionAction::Revise, AttentionKind::PlanProposal) => {
            state.planning_input.latest_rejected_proposal = state.proposal.take();
            state.planning_input.refinement =
                Some(PlanningRefinement::Guidance(justification.to_string()));
            start_replanning(state);
        }
        (DecisionAction::Retry, AttentionKind::NodeFailed) => {
            if let Some(task_id) = &item.task_id {
                node_status(state, task_id, TaskStatus::Pending);
                if let Some(task) = state.active_tasks_mut().get_mut(task_id) {
                    task.consecutive_failures = 0;
                }
                state.flagged_nodes.remove(task_id);
            }
        }
        (DecisionAction::Accept, AttentionKind::NodeFailed) => {
            if let Some(task_id) = &item.task_id {
                node_status(state, task_id, TaskStatus::Cleared); // accept the failure
                if let Some(task) = state.active_tasks_mut().get_mut(task_id) {
                    task.consecutive_failures = 0;
                }
            }
        }
        (DecisionAction::Accept, AttentionKind::NodeAttention) => {
            if let Some(task_id) = &item.task_id {
                state.flagged_nodes.remove(task_id);
            }
        }
        (DecisionAction::Approve, AttentionKind::GateCheckpoint)
        | (DecisionAction::Accept, AttentionKind::GateFailed) => {
            if let Some(task_id) = &item.task_id {
                state.acknowledged_gates.insert(task_id.clone());
                // Accepting a gate (cleared checkpoint or blocked gate) lets
                // the mission proceed past it: mark it cleared so downstream
                // tasks become runnable instead of wedging forever.
                if let Some(task) = state.tasks.get_mut(task_id) {
                    task.status = TaskStatus::Cleared;
                }
            }
        }
        (DecisionAction::Retry, AttentionKind::OracleFailed) => {
            // Re-open the obligation: step will re-request the oracle.
            if let Some(oracle) = &item.oracle {
                state.oracle_failures.remove(oracle);
            }
        }
        (DecisionAction::Retry, AttentionKind::OracleVerdictFailed) => {
            for assertion_id in &item.assertion_ids {
                if let Some(assertion) = state.contract.get_mut(assertion_id) {
                    assertion.last_authoritative = None;
                }
            }
        }
        (DecisionAction::Repair, AttentionKind::OracleVerdictFailed) => {
            let feedback = super::state::FailureFeedback {
                summary: item.report.clone(),
                evidence: item.evidence.clone(),
                details: item.details.clone(),
                justification: justification.to_string(),
            };
            for assertion_id in &item.assertion_ids {
                if let Some(assertion) = state.contract.get_mut(assertion_id) {
                    assertion.last_authoritative = None;
                }
            }
            if let Some(plan) = &state.plan {
                for task in plan.tasks.iter().filter(|task| {
                    task.kind == super::plan::TaskKind::Work
                        && task
                            .targets
                            .iter()
                            .any(|target| item.assertion_ids.contains(target))
                }) {
                    if let Some(runtime) = state.tasks.get_mut(&task.id) {
                        runtime.status = TaskStatus::Pending;
                        runtime.consecutive_failures = 0;
                        runtime.feedback.push(feedback.clone());
                    }
                }
            }
            state.terminal_review.outcome = None;
            state.terminal_review.accepted = None;
            state.terminal_review.consecutive_failures = 0;
        }
        (DecisionAction::Accept, AttentionKind::OracleFailed) => {
            // Accept the infra failure: waive the obligation so the mission
            // can finish (never verified — there is no authoritative verdict).
            if let Some(oracle) = &item.oracle {
                state.oracle_failures.remove(oracle);
                state.waived_oracles.insert(oracle.clone());
            }
        }
        (DecisionAction::Accept, AttentionKind::OracleVerdictFailed) => {
            if let Some(oracle) = &item.oracle {
                state.waived_oracles.insert(oracle.clone());
            }
        }
        (DecisionAction::Accept, AttentionKind::TerminalReviewGaps) => {
            // Acknowledge the blocking verdict AT ITS SHA (the gap item only
            // raises fresh, so the verdict's sha is the current head): the
            // mission may close with these gaps on record; a later head move
            // re-opens the review — the acknowledgment is never inherited.
            if let Some(ReviewOutcome::Verdict(v)) = &state.terminal_review.outcome {
                state.terminal_review.accepted = Some(ReviewAcceptance {
                    kind: ReviewAcceptanceKind::AcknowledgedGaps,
                    judged_sha: v.judged_sha.clone(),
                    justification: justification.to_string(),
                });
            }
        }
        (
            DecisionAction::Retry,
            AttentionKind::TerminalReviewGaps | AttentionKind::TerminalReviewFailed,
        ) => {
            // Discard the outcome and re-roll a fresh-context reviewer.
            // Attempts are preserved, so the re-dispatch gets a fresh
            // effect ID. Any prior acceptance goes with the discarded
            // outcome — the receipt must never cite a decision this retry
            // just walked away from.
            state.terminal_review.outcome = None;
            state.terminal_review.accepted = None;
        }
        (DecisionAction::Accept, AttentionKind::TerminalReviewFailed) => {
            // Accept the infra failure: waive the review AT THIS HEAD so the
            // mission can close without a verdict. Later work stales the
            // waiver and re-opens the review — the instrument may have
            // recovered, and the human never saw the new tree.
            state.terminal_review.outcome = None;
            state.terminal_review.consecutive_failures = 0;
            state.terminal_review.accepted = Some(ReviewAcceptance {
                kind: ReviewAcceptanceKind::Waived,
                judged_sha: state.deliverable_head().to_string(),
                justification: justification.to_string(),
            });
        }
        (
            DecisionAction::Revise,
            AttentionKind::OracleVerdictFailed
            | AttentionKind::GateFailed
            | AttentionKind::TerminalReviewGaps,
        ) => {
            state.terminal_review.accepted = None;
            state.proposal = None;
            state.proposal_approved = false;
            state.planning_input.refinement = Some(PlanningRefinement::FailureEvidence(Box::new(
                super::state::FailureFeedback {
                    summary: item.report,
                    evidence: item.evidence,
                    details: item.details,
                    justification: justification.to_string(),
                },
            )));
            start_replanning(state);
        }
        (DecisionAction::Abort, _) => {
            state.phase = MissionPhase::Aborted {
                reason: justification.to_string(),
            };
        }
        _ => {}
    }
}

fn start_replanning(state: &mut MissionState) {
    state.planning_base_revision = Some(state.revision);
    for (id, task) in &mut state.planning.tasks {
        task.status = TaskStatus::Pending;
        task.last_report = None;
        task.last_failure = None;
        task.consecutive_failures = 0;
        task.feedback.clear();
        state.flagged_nodes.remove(id);
    }
}

/// Rebuild the open-attention set from scratch: the approval gate, failed
/// nodes, human-flagged nodes, and gate results — minus anything a decision
/// resolved. Attention is a pure function of state, so a decision that
/// changed a task's status or set a flag removes its item automatically.
fn derive_attention(state: &mut MissionState) {
    // An aborted mission is over — it carries no open attention even if a
    // node was left failed. (Done is set later, in `derive_phase`, and closes
    // only when attention is already empty.)
    if matches!(state.phase, MissionPhase::Aborted { .. }) {
        state.open_attention.clear();
        return;
    }
    let mut attention: BTreeMap<String, AttentionItem> = BTreeMap::new();
    let mut raise = |kind: AttentionKind,
                     task_id: Option<TaskId>,
                     oracle: Option<super::ids::OracleName>,
                     assertion_ids: Vec<AssertionId>,
                     evidence: Option<super::state::FailureEvidence>,
                     report: String| {
        let anchor = task_id
            .as_ref()
            .map(|id| id.to_string())
            .or_else(|| oracle.as_ref().map(|o| o.to_string()))
            .unwrap_or_else(|| "mission".to_string());
        // The kind's stable slug — the anchor (a case-sensitive task or oracle
        // id) stays verbatim so two ids differing only by case never collide
        // into one attention item.
        let id = format!("{}:{anchor}", kind.slug());
        attention.insert(
            id.clone(),
            AttentionItem {
                id,
                kind,
                task_id,
                oracle,
                assertion_ids,
                evidence,
                details: None,
                report,
            },
        );
    };
    let failure_report = |label: &str, task_id: &TaskId, task: &TaskRuntimeState| {
        task.last_failure.as_ref().map_or_else(
            || format!("{label} '{task_id}' failed"),
            |failure| {
                format!(
                    "{label} '{task_id}' failed ({}): {}",
                    failure.category(),
                    failure.detail()
                )
            },
        )
    };

    if state.proposal.is_some() && !state.proposal_approved {
        raise(
            AttentionKind::PlanProposal,
            None,
            None,
            Vec::new(),
            None,
            format!(
                "approve the complete plan proposed against revision {}",
                state.revision
            ),
        );
    }

    // Replanning is its own attention era. Rejected execution facts remain in
    // state as durable prompt evidence, but they cannot compete with the
    // planning DAG or its replacement proposal for dispatch. This also keeps a
    // failed-gate `revise` from requiring an administrative accept on the
    // rejected plan before the strategist can run.
    if state.planning_base_revision.is_some() {
        for (task_id, rt) in &state.planning.tasks {
            if rt.status == TaskStatus::Failed
                && !rt.automatic_retry_remaining(state.config.recovery.max_attempts)
            {
                raise(
                    AttentionKind::NodeFailed,
                    Some(task_id.clone()),
                    None,
                    Vec::new(),
                    None,
                    failure_report("planning task", task_id, rt),
                );
            } else if state.flagged_nodes.contains(task_id) {
                raise(
                    AttentionKind::NodeAttention,
                    Some(task_id.clone()),
                    None,
                    Vec::new(),
                    None,
                    format!("planning task '{task_id}' asks for a look"),
                );
            }
        }
        state.open_attention = attention;
        return;
    }
    if state.proposal.is_some() && !state.proposal_approved {
        state.open_attention = attention;
        return;
    }

    // Oracle infrastructure failures: park rather than re-request forever.
    for (oracle, failure) in &state.oracle_failures {
        raise(
            AttentionKind::OracleFailed,
            None,
            Some(oracle.clone()),
            Vec::new(),
            None,
            format!("oracle '{oracle}' failed to run: {}", failure.detail()),
        );
    }

    // A fresh nonzero exit is a valid authoritative verdict, but it is not a
    // terminal dead end. Park it on an explicit repair path with the exact
    // assertion set and evidence references the oracle produced.
    let mut failed_by_oracle: BTreeMap<_, (Vec<AssertionId>, super::state::FailureEvidence)> =
        BTreeMap::new();
    for (assertion_id, assertion) in &state.contract {
        let Some(verdict) = assertion
            .last_authoritative
            .as_ref()
            .filter(|verdict| verdict.is_fresh_at(state.deliverable_head()) && !verdict.passed())
        else {
            continue;
        };
        if state.waived_oracles.contains(verdict.oracle()) {
            continue;
        }
        let (stdout, stderr) = verdict.evidence();
        failed_by_oracle
            .entry(verdict.oracle().clone())
            .or_insert_with(|| {
                (
                    Vec::new(),
                    super::state::FailureEvidence {
                        exit_code: verdict.exit_code(),
                        exit_signal: verdict.exit_signal(),
                        stdout: stdout.clone(),
                        stderr: stderr.clone(),
                    },
                )
            })
            .0
            .push(assertion_id.clone());
    }
    for (oracle, (assertion_ids, evidence)) in failed_by_oracle {
        let targets = assertion_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        raise(
            AttentionKind::OracleVerdictFailed,
            None,
            Some(oracle),
            assertion_ids,
            Some(evidence.clone()),
            format!(
                "authoritative oracle failed assertions [{targets}] with exit {}",
                evidence.exit_code
            ),
        );
    }

    let Some(plan) = state.plan.clone() else {
        state.open_attention = attention;
        return;
    };
    for task in &plan.tasks {
        let status = state.tasks.get(&task.id).map(|t| t.status);
        match task.kind {
            super::plan::TaskKind::Gate => match status {
                Some(TaskStatus::Cleared) if !state.acknowledged_gates.contains(&task.id) => raise(
                    AttentionKind::GateCheckpoint,
                    Some(task.id.clone()),
                    None,
                    Vec::new(),
                    None,
                    format!("gate '{}' cleared; confirm to proceed", task.id),
                ),
                Some(TaskStatus::Failed) if !state.acknowledged_gates.contains(&task.id) => raise(
                    AttentionKind::GateFailed,
                    Some(task.id.clone()),
                    None,
                    Vec::new(),
                    None,
                    format!(
                        "gate '{}' is blocked by dissenting or missing verdicts",
                        task.id
                    ),
                ),
                _ => {}
            },
            super::plan::TaskKind::Work | super::plan::TaskKind::Validate => {
                if status == Some(TaskStatus::Failed)
                    && !state.tasks[&task.id]
                        .automatic_retry_remaining(state.config.recovery.max_attempts)
                {
                    raise(
                        AttentionKind::NodeFailed,
                        Some(task.id.clone()),
                        None,
                        Vec::new(),
                        None,
                        failure_report("task", &task.id, &state.tasks[&task.id]),
                    );
                } else if state.flagged_nodes.contains(&task.id) {
                    raise(
                        AttentionKind::NodeAttention,
                        Some(task.id.clone()),
                        None,
                        Vec::new(),
                        None,
                        format!("task '{}' asked for a human look", task.id),
                    );
                }
            }
        }
    }

    // Terminal review (config-gated so pre-feature logs never raise these).
    if state.config.terminal_review.is_some() {
        match &state.terminal_review.outcome {
            // Infra failure: park rather than re-request forever (the
            // OracleFailed mirror; raised until a decision retries or waives).
            Some(ReviewOutcome::Failed { failure })
                if !failure.automatically_retryable()
                    || state.terminal_review.consecutive_failures
                        >= state.config.recovery.max_attempts =>
            {
                raise(
                    AttentionKind::TerminalReviewFailed,
                    None,
                    None,
                    Vec::new(),
                    None,
                    format!(
                        "terminal review failed to run: {}; retry to re-run \
                     the review, accept to waive it, or abort",
                        failure.detail()
                    ),
                )
            }
            Some(ReviewOutcome::Failed { .. }) => {}
            // A fresh blocking verdict parks — but only when the mission
            // would otherwise close. While remediation tasks / inflight
            // effects / owed oracles are live the park auto-clears: an
            // a revision resumes the mission without a second decision, and
            // the head move re-opens the review for free.
            Some(ReviewOutcome::Verdict(v)) => {
                let acknowledged = state.terminal_review.acknowledges(v);
                if v.is_fresh_at(state.deliverable_head())
                    && v.blocking()
                    && !acknowledged
                    && !work_outstanding(state)
                {
                    let blocking_count = v
                        .gaps
                        .iter()
                        .filter(|g| g.severity == GapSeverity::Blocking)
                        .count();
                    // No blocking gap ⇒ the park came from the reviewer's
                    // fail bit (blocking() dominance): say so — never
                    // "0 blocking gap(s)" on a parked mission.
                    let finding = if blocking_count == 0 {
                        format!(
                            "terminal review failed the product ({} gap(s) recorded; see its report)",
                            v.gaps.len()
                        )
                    } else {
                        format!(
                            "terminal review found {blocking_count} blocking gap(s) of {} total",
                            v.gaps.len()
                        )
                    };
                    raise(
                        AttentionKind::TerminalReviewGaps,
                        None,
                        None,
                        Vec::new(),
                        None,
                        format!(
                            "{finding} at {} (attempt {}); revise the plan to \
                             remediate, retry to re-run the review, accept to \
                             acknowledge and close, or abort",
                            super::ids::short_hex(&v.judged_sha),
                            state.terminal_review.attempts,
                        ),
                    );
                    if let Some(item) = attention.get_mut("terminal_review_gaps:mission") {
                        item.details = Some(v.report.clone());
                    }
                }
            }
            None => {}
        }
    }
    state.open_attention = attention;
}

fn track_inflight(state: &mut MissionState, event: &MissionEvent, seq: u64) {
    if let Some((key, effect)) = InflightEffect::from_request(event, seq) {
        state.inflight.insert(key, effect);
    }
}

fn apply_handoff(state: &mut MissionState, task_id: &super::ids::TaskId, handoff: &Handoff) {
    match handoff {
        Handoff::Work {
            done,
            report,
            request_attention,
        } => {
            let status = if *done {
                TaskStatus::Cleared
            } else {
                TaskStatus::Failed
            };
            // Work runs in either era (a planning report role or an execution
            // artifact role), so route by era; Plan is planning-only and Validate
            // execution-only, and address their maps directly below.
            if let Some(task) = state.active_tasks_mut().get_mut(task_id) {
                task.status = status;
                task.last_report = Some(report.clone());
                task.last_failure = (!done).then(|| {
                    lionclaw_runtime_api::TypedFailure::invalid(
                        "handoff.incomplete",
                        "role reported done=false",
                    )
                });
            }
            // A done task that asks for a look is flagged (derived into a
            // node_attention item); a not-done task is Failed (derived into a
            // node_failed item).
            if *done && *request_attention {
                state.flagged_nodes.insert(task_id.clone());
            }
        }
        Handoff::Plan {
            done,
            report,
            proposal,
            request_attention,
        } => {
            // Planning-only: the author's handoff. A `done` proposal (the shell
            // has already validated it) becomes the gradeless `state.proposal`;
            // it seeds the contract only after approval (`derive_promotion`).
            let status = if *done {
                TaskStatus::Cleared
            } else {
                TaskStatus::Failed
            };
            if let Some(task) = state.planning.tasks.get_mut(task_id) {
                task.status = status;
                task.last_report = Some(report.clone());
                task.last_failure = (!done).then(|| {
                    lionclaw_runtime_api::TypedFailure::invalid(
                        "handoff.incomplete",
                        "planning author reported done=false",
                    )
                });
            }
            if *done {
                if let Some(proposal) = proposal {
                    if state.proposal.is_none()
                        && state.planning_base_revision == Some(proposal.base_revision)
                    {
                        state.proposal = Some(proposal.clone());
                        state.planning_base_revision = None;
                        state.proposal_approved = false;
                    }
                }
                if *request_attention {
                    state.flagged_nodes.insert(task_id.clone());
                }
            }
        }
        Handoff::Validate {
            report,
            items,
            request_attention,
            ..
        } => {
            // Execution-only: validators always clear — they ran; their verdicts
            // are data folded into the contract.
            if let Some(task) = state.tasks.get_mut(task_id) {
                task.status = TaskStatus::Cleared;
                task.last_report = Some(report.clone());
                task.last_failure = None;
            }
            for item in items {
                if let Some(assertion) = state.contract.get_mut(&item.item_id) {
                    assertion.last_advisory.insert(task_id.clone(), item.passed);
                    if item.passed {
                        assertion.advisory = AdvisoryStatus::Passed; // sticky
                    } else if assertion.advisory != AdvisoryStatus::Passed {
                        assertion.advisory = AdvisoryStatus::Failed;
                    }
                }
            }
            if *request_attention {
                state.flagged_nodes.insert(task_id.clone());
            }
        }
        // Terminal reviews are recorded as dedicated terminal-review events;
        // a plan task can never consume this handoff contract. A hostile log
        // that claims otherwise fails the task instead of leaving it running
        // forever with no inflight effect.
        Handoff::Review { .. } => {
            if let Some(task) = state.tasks.get_mut(task_id) {
                task.status = TaskStatus::Failed;
            }
        }
    }
}

/// Re-derive the phase from scratch. Abort is the one sticky, event-anchored
/// exception (the fact isn't reconstructible from other fields).
fn derive_phase(state: &mut MissionState) {
    if matches!(state.phase, MissionPhase::Aborted { .. }) {
        return;
    }
    state.phase = if !state.open_attention.is_empty() {
        MissionPhase::AttentionNeeded
    } else if state.plan.is_none() || state.planning_base_revision.is_some() {
        MissionPhase::Planning
    } else if work_outstanding(state) || terminal_review_outstanding(state) {
        MissionPhase::Running
    } else {
        MissionPhase::Done {
            finish: classify_finish(state),
        }
    };
}

fn tasks_active(state: &MissionState) -> bool {
    state.tasks.values().any(|task| {
        matches!(task.status, TaskStatus::Pending | TaskStatus::Running)
            || task.automatic_retry_remaining(state.config.recovery.max_attempts)
    })
}

/// Work the mission still owes before it could close: active tasks, inflight
/// effects, or unfresh oracle obligations. One definition shared by the phase
/// derivation and the terminal-review park ("would the mission otherwise
/// close"), so the two can never drift.
fn work_outstanding(state: &MissionState) -> bool {
    tasks_active(state) || !state.inflight.is_empty() || oracle_obligation_outstanding(state)
}

/// An oracle-bound assertion without a verdict at the current artifact commit
/// still owes the engine a run (a fresh *fail* settles the obligation — retry
/// is a human decision, not an engine loop).
pub(crate) fn oracle_obligation_outstanding(state: &MissionState) -> bool {
    state.contract.values().any(|assertion| {
        let Some(oracle) = &assertion.oracle else {
            return false;
        };
        // A waived oracle owes nothing (the mission just can't be verified).
        !state.waived_oracles.contains(oracle)
            && assertion
                .last_authoritative
                .as_ref()
                .is_none_or(|v| !v.is_fresh_at(state.deliverable_head()))
    })
}

/// A configured terminal review with no verdict at the current artifact
/// commit still owes the engine a run. A fresh verdict — clean or blocking —
/// settles the obligation (the park on gaps is attention's job, exactly as a
/// fresh oracle *fail* settles the oracle obligation: retry is a human
/// decision, not an engine loop). A waiver granted at this head owes nothing —
/// but later work stales it, exactly like an acknowledgment: acceptance is
/// never inherited by a tree the human never saw. A finish already below the
/// stop bar closes without burning a review — the review is the last gate on
/// an otherwise-passing mission.
pub(crate) fn terminal_review_outstanding(state: &MissionState) -> bool {
    if state.config.terminal_review.is_none() {
        return false; // config-gated: pre-feature logs derive identically
    }
    if state.terminal_review.waived_at(state.deliverable_head()) {
        return false;
    }
    if !state.config.stop.satisfied_by(classify_finish(state)) {
        return false;
    }
    !matches!(
        &state.terminal_review.outcome,
        Some(ReviewOutcome::Verdict(v)) if v.is_fresh_at(state.deliverable_head())
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::super::event::{
        ArtifactOutcome, MissionConfig, OracleRunSuccess, PayloadRef, RoleRunSuccess,
        RuntimeConfigurationEvidence, TerminalReviewSuccess, ValidationItem,
    };
    use super::super::ids::{AssertionId, EffectId, MissionId, OracleName, RoleName, TaskId};
    use super::super::plan::{Assertion, Plan, PlanProposal, PlanningTask, Task, TaskKind};
    use super::super::verdict::FinishClass;
    use super::*;
    use lionclaw_runtime_api::{TypedFailure, TypedFailureEvidence};

    fn tid(raw: &str) -> TaskId {
        TaskId::new(raw).expect("task id")
    }

    fn aid(raw: &str) -> AssertionId {
        AssertionId::new(raw).expect("assertion id")
    }

    fn oracle(raw: &str) -> OracleName {
        OracleName::new(raw).expect("oracle name")
    }

    fn envelope(sequence_no: u64, event: MissionEvent) -> EventEnvelope {
        EventEnvelope {
            mission_id: MissionId::from_digest_prefix("abcdef0123456789"),
            sequence_no,
            recorded_at_ms: 0,
            stamps: Default::default(),
            event,
        }
    }

    /// Fold hand-built events with sequence numbers assigned by position.
    fn fold_log(events: Vec<MissionEvent>) -> Option<MissionState> {
        let events = events.into_iter().flat_map(|event| {
            let approve = matches!(&event, MissionEvent::PlanProposed { .. }).then(|| {
                decision(
                    "plan_proposal:mission",
                    super::super::event::DecisionAction::Approve,
                )
            });
            std::iter::once(event).chain(approve)
        });
        fold(
            events
                .enumerate()
                .map(|(i, event)| envelope(i as u64, event)),
        )
    }

    fn created() -> MissionEvent {
        MissionEvent::MissionCreated {
            objective: "objective".into(),
            mission_type: crate::model::MissionTypeRef {
                name: "mt".into(),
                digest: "d".into(),
            },
            runtime: "codex".into(),
            image_id: "img".into(),
            workspace_dir: "/w".into(),
            base_sha: "base".into(),
            config: MissionConfig {
                recovery: super::super::event::RecoveryConfig { max_attempts: 1 },
                ..Default::default()
            },
        }
    }

    fn plan_proposed(assertions: Vec<Assertion>, tasks: Vec<Task>) -> MissionEvent {
        MissionEvent::PlanProposed {
            proposal: PlanProposal {
                base_revision: 0,
                plan: Plan {
                    requirements: vec![],
                    assertions,
                    tasks,
                },
            },
            plan_hash: "hash".into(),
        }
    }

    fn assertion(id: &str, oracle_name: Option<&str>) -> Assertion {
        Assertion {
            id: aid(id),
            prose: "claim".into(),
            oracle: oracle_name.map(oracle),
        }
    }

    fn work_task(id: &str) -> Task {
        Task {
            id: tid(id),
            kind: TaskKind::Work,
            body: "do".into(),
            targets: vec![],
            role: Some(RoleName::new("implementer").expect("role name")),
            depends_on: vec![],
        }
    }

    fn validate_task(id: &str) -> Task {
        Task {
            id: tid(id),
            kind: TaskKind::Validate,
            body: "check".into(),
            targets: vec![],
            role: Some(RoleName::new("reviewer").expect("role name")),
            depends_on: vec![],
        }
    }

    fn work_handoff(done: bool, request_attention: bool) -> Handoff {
        Handoff::Work {
            done,
            report: PayloadRef::inline("report"),
            request_attention,
        }
    }

    fn validate_handoff(items: &[(&str, bool)]) -> Handoff {
        Handoff::Validate {
            done: true,
            report: PayloadRef::inline("checked"),
            items: items
                .iter()
                .map(|(id, passed)| ValidationItem {
                    item_id: aid(id),
                    passed: *passed,
                })
                .collect(),
            passed: items.iter().all(|(_, passed)| *passed),
            request_attention: false,
        }
    }

    fn role_requested(task: &str, key: &str) -> MissionEvent {
        MissionEvent::RoleRunRequested {
            task_id: tid(task),
            attempt_no: 1,
            effect_id: EffectId::for_parts(&["test", key]),
            role: RoleName::new("implementer").expect("role name"),
            runtime: "codex".into(),
            prompt: PayloadRef::inline("prompt"),
            base_sha: "base".into(),
            assignment_epoch: 1,
            recreate_workspace: true,
        }
    }

    fn role_completed(
        task: &str,
        key: &str,
        handoff: Handoff,
        artifact: Option<ArtifactOutcome>,
    ) -> MissionEvent {
        MissionEvent::RoleRunCompleted {
            task_id: tid(task),
            attempt_no: 1,
            effect_id: EffectId::for_parts(&["test", key]),
            outcome: Ok(RoleRunSuccess {
                handoff,
                artifact,
                final_response: PayloadRef::inline("final response"),
                runtime_configuration: RuntimeConfigurationEvidence::default(),
            }),
        }
    }

    fn oracle_requested(assertion_id: &str, judged: &str, key: &str) -> MissionEvent {
        MissionEvent::OracleRunRequested {
            assertion_ids: vec![aid(assertion_id)],
            oracle: oracle("cargo-test"),
            judged_sha: judged.into(),
            attempt_no: 1,
            effect_id: EffectId::for_parts(&["test", key]),
        }
    }

    fn oracle_completed(
        assertion_id: &str,
        judged: &str,
        key: &str,
        exit_code: i32,
    ) -> MissionEvent {
        MissionEvent::OracleRunCompleted {
            assertion_ids: vec![aid(assertion_id)],
            oracle: oracle("cargo-test"),
            judged_sha: judged.into(),
            attempt_no: 1,
            effect_id: EffectId::for_parts(&["test", key]),
            outcome: Ok(OracleRunSuccess {
                exit_code,
                exit_signal: None,
                stdout: PayloadRef::inline("out"),
                stderr: PayloadRef::inline("err"),
                prepared_inputs: Vec::new(),
                duration_ms: 5,
            }),
        }
    }

    fn decision(item: &str, action: super::super::event::DecisionAction) -> MissionEvent {
        MissionEvent::DecisionRecorded {
            attention_id: item.into(),
            action,
            justification: "j".into(),
        }
    }

    fn gate_task(id: &str, targets: &[&str], deps: &[&str]) -> Task {
        Task {
            id: tid(id),
            kind: TaskKind::Gate,
            body: "".into(),
            targets: targets.iter().map(|t| aid(t)).collect(),
            role: None,
            depends_on: deps.iter().map(|d| tid(d)).collect(),
        }
    }

    // Regression (review): a fresh authoritative FAIL must dominate a green
    // advisory verdict — Unverified, never InternallyConsistent.
    #[test]
    fn fresh_oracle_fail_dominates_green_advisory() {
        use super::super::verdict::FinishClass;
        let state = fold_log(vec![
            created(),
            plan_proposed(
                vec![assertion("A1", Some("cargo-test"))],
                vec![work_task("w"), validate_task("v")],
            ),
            role_completed(
                "w",
                "kw",
                work_handoff(true, false),
                Some(ArtifactOutcome {
                    base_sha: "base".into(),
                    head_sha: "sha-1".into(),
                }),
            ),
            // Validator says pass (advisory becomes sticky Passed).
            role_completed("v", "kv", validate_handoff(&[("A1", true)]), None),
            // Oracle runs at the current commit and FAILS.
            oracle_requested("A1", "sha-1", "ko"),
            oracle_completed("A1", "sha-1", "ko", 1),
        ])
        .expect("state");
        assert_eq!(classify_finish(&state), FinishClass::Unverified);
        assert_eq!(state.phase, MissionPhase::AttentionNeeded);
        assert!(state
            .open_attention
            .contains_key("oracle_verdict_failed:cargo-test"));
    }

    // Regression (review): accepting a GateFailed must let downstream
    // proceed, not wedge the mission in Running forever.
    #[test]
    fn accept_on_failed_gate_unblocks_downstream() {
        let base = vec![
            created(),
            plan_proposed(
                vec![assertion("AA", None)],
                vec![
                    work_task("w"),
                    validate_task("v"),
                    gate_task("g", &["AA"], &["v"]),
                    {
                        let mut w2 = work_task("w2");
                        w2.depends_on = vec![tid("g")];
                        w2
                    },
                ],
            ),
            role_completed("w", "kw", work_handoff(true, false), None),
            // Validator dissents → gate g latches Failed → GateFailed parks.
            role_completed("v", "kv", validate_handoff(&[("AA", false)]), None),
        ];
        let parked = fold_log(base.clone()).expect("state");
        assert_eq!(parked.tasks[&tid("g")].status, TaskStatus::Failed);
        assert!(parked.open_attention.contains_key("gate_failed:g"));

        let mut resolved = base;
        resolved.push(decision(
            "gate_failed:g",
            super::super::event::DecisionAction::Accept,
        ));
        let state = fold_log(resolved).expect("state");
        // Gate accepted → cleared → downstream w2 is runnable, not wedged.
        assert_eq!(state.tasks[&tid("g")].status, TaskStatus::Cleared);
        assert_eq!(state.tasks[&tid("w2")].status, TaskStatus::Pending);
        assert!(state.open_attention.is_empty());
        assert_eq!(
            super::super::step::step(&state),
            super::super::step::StepDecision::DispatchRole(
                super::super::step::RoleDispatchIntent {
                    task_id: tid("w2"),
                    role: RoleName::new("implementer").unwrap(),
                    attempt_no: 1,
                    body: "do".into(),
                    targets: vec![],
                    base_sha: state.deliverable_head().to_string(),
                }
            )
        );
    }

    #[test]
    fn revise_failed_gate_suppresses_rejected_execution_attention() {
        let mut created = created();
        let MissionEvent::MissionCreated { config, .. } = &mut created else {
            unreachable!();
        };
        config.planning.tasks = vec![super::super::plan::PlanningTask {
            id: tid("strategist"),
            role: RoleName::new("strategist").unwrap(),
            body: "replace the rejected plan".into(),
            depends_on: vec![],
        }];
        let base = vec![
            created,
            plan_proposed(
                vec![assertion("AA", None)],
                vec![
                    work_task("w"),
                    validate_task("v"),
                    gate_task("g", &["AA"], &["v"]),
                ],
            ),
            role_completed("w", "kw", work_handoff(true, false), None),
            role_completed("v", "kv", validate_handoff(&[("AA", false)]), None),
        ];
        let parked = fold_log(base.clone()).expect("state");
        let gate = parked
            .open_attention
            .get("gate_failed:g")
            .expect("failed gate attention");
        let exact_failure = gate.report.clone();

        let mut replanning = base;
        replanning.push(decision(
            "gate_failed:g",
            super::super::event::DecisionAction::Revise,
        ));
        let state = fold_log(replanning).expect("state");

        assert_eq!(state.phase, MissionPhase::Planning);
        assert!(state.open_attention.is_empty());
        assert_eq!(state.tasks[&tid("g")].status, TaskStatus::Failed);
        let Some(PlanningRefinement::FailureEvidence(feedback)) =
            state.planning_input.refinement.as_ref()
        else {
            panic!("failed gate evidence must feed replanning");
        };
        assert_eq!(feedback.summary, exact_failure);
        assert_eq!(feedback.justification, "j");
        assert!(matches!(
            super::super::step::step(&state),
            super::super::step::StepDecision::DispatchRole(intent)
                if intent.task_id == tid("strategist")
        ));
    }

    // Regression (review): accepting an OracleFailed waives the obligation
    // so the mission can finish (unverified) instead of looping forever.
    #[test]
    fn accept_on_oracle_failure_waives_and_finishes() {
        use super::super::verdict::FinishClass;
        let base = vec![
            created(),
            plan_proposed(
                vec![assertion("A1", Some("cargo-test"))],
                vec![work_task("w")],
            ),
            role_completed(
                "w",
                "kw",
                work_handoff(true, false),
                Some(ArtifactOutcome {
                    base_sha: "base".into(),
                    head_sha: "sha-1".into(),
                }),
            ),
            oracle_requested("A1", "sha-1", "ko"),
            MissionEvent::OracleRunCompleted {
                assertion_ids: vec![aid("A1")],
                oracle: oracle("cargo-test"),
                judged_sha: "sha-1".into(),
                attempt_no: 1,
                effect_id: EffectId::for_parts(&["test", "ko"]),
                outcome: Err(TypedFailure::permanent("oracle.spawn", "binary missing")),
            },
        ];
        let parked = fold_log(base.clone()).expect("state");
        assert!(parked
            .open_attention
            .contains_key("oracle_failed:cargo-test"));

        let mut resolved = base;
        resolved.push(decision(
            "oracle_failed:cargo-test",
            super::super::event::DecisionAction::Accept,
        ));
        let state = fold_log(resolved).expect("state");
        assert!(state.waived_oracles.contains(&oracle("cargo-test")));
        // No outstanding obligation → mission closes, but never verified.
        assert_eq!(
            state.phase,
            MissionPhase::Done {
                finish: FinishClass::Unverified
            }
        );
    }

    // Regression (review): attention ids must not collapse task ids that
    // differ only by case (lowercase only the kind, not the anchor).
    #[test]
    fn attention_ids_preserve_anchor_case() {
        // Two gates differing only by case, both blocked by one dissent.
        let state = fold_log(vec![
            created(),
            plan_proposed(
                vec![assertion("AA", None)],
                vec![
                    work_task("w"),
                    validate_task("v"),
                    gate_task("Check", &["AA"], &["v"]),
                    gate_task("check", &["AA"], &["v"]),
                ],
            ),
            role_completed("w", "kw", work_handoff(true, false), None),
            role_completed("v", "kv", validate_handoff(&[("AA", false)]), None),
        ])
        .expect("state");
        // Distinct ids — no collision collapsing two gates into one item.
        assert!(state.open_attention.contains_key("gate_failed:Check"));
        assert!(state.open_attention.contains_key("gate_failed:check"));
        assert_eq!(
            state
                .open_attention
                .keys()
                .filter(|k| k.starts_with("gate_failed:"))
                .count(),
            2
        );
    }

    #[test]
    fn fold_bootstraps_only_on_mission_created() {
        let state = fold_log(vec![created()]).expect("created bootstraps");
        assert_eq!(state.phase, MissionPhase::Planning);
        assert_eq!(state.base_sha, "base");
        assert_eq!(state.current_sha, "base");
        assert_eq!(state.head, 0);
        assert!(state.plan.is_none());
        assert!(state.tasks.is_empty() && state.contract.is_empty());

        let non_created = [
            plan_proposed(vec![], vec![]),
            role_completed("t1", "k1", work_handoff(true, false), None),
            MissionEvent::MissionAborted {
                reason: "stop".into(),
            },
        ];
        for event in non_created {
            let name = event.event_type();
            assert!(
                fold_log(vec![event]).is_none(),
                "first event {name} must not bootstrap"
            );
        }
    }

    #[test]
    fn plan_proposed_initializes_contract_and_tasks() {
        let state = fold_log(vec![
            created(),
            plan_proposed(
                vec![
                    assertion("TESTS-PASS", Some("cargo-test")),
                    assertion("NO-ORACLE", None),
                ],
                vec![work_task("t1"), validate_task("v1")],
            ),
        ])
        .expect("state");
        assert!(state.plan.is_some());
        let bound = &state.contract[&aid("TESTS-PASS")];
        assert_eq!(bound.oracle, Some(oracle("cargo-test")));
        assert_eq!(bound.advisory, AdvisoryStatus::Pending);
        assert!(bound.last_advisory.is_empty());
        assert!(bound.last_authoritative.is_none());
        assert_eq!(state.contract[&aid("NO-ORACLE")].oracle, None);
        for id in ["t1", "v1"] {
            let task = &state.tasks[&tid(id)];
            assert_eq!(task.status, TaskStatus::Pending);
            assert_eq!(task.attempts, 0);
        }
        assert_eq!(state.phase, MissionPhase::Running);
    }

    #[test]
    fn work_handoff_status_and_attention() {
        struct Case {
            name: &'static str,
            done: bool,
            request_attention: bool,
            expect_status: TaskStatus,
            expect_kind: Option<AttentionKind>,
        }
        let cases = [
            Case {
                name: "done clears",
                done: true,
                request_attention: false,
                expect_status: TaskStatus::Cleared,
                expect_kind: None,
            },
            Case {
                name: "not done fails and raises node_failed",
                done: false,
                request_attention: false,
                expect_status: TaskStatus::Failed,
                expect_kind: Some(AttentionKind::NodeFailed),
            },
            Case {
                name: "done with request_attention parks as node_attention",
                done: true,
                request_attention: true,
                expect_status: TaskStatus::Cleared,
                expect_kind: Some(AttentionKind::NodeAttention),
            },
            Case {
                name: "not done wins over request_attention",
                done: false,
                request_attention: true,
                expect_status: TaskStatus::Failed,
                expect_kind: Some(AttentionKind::NodeFailed),
            },
        ];
        for case in cases {
            let state = fold_log(vec![
                created(),
                plan_proposed(vec![], vec![work_task("t1")]),
                role_completed(
                    "t1",
                    "k1",
                    work_handoff(case.done, case.request_attention),
                    None,
                ),
            ])
            .expect(case.name);
            assert_eq!(
                state.tasks[&tid("t1")].status,
                case.expect_status,
                "{}",
                case.name
            );
            match case.expect_kind {
                None => {
                    assert!(state.open_attention.is_empty(), "{}", case.name);
                    assert!(
                        matches!(state.phase, MissionPhase::Done { .. }),
                        "{}",
                        case.name
                    );
                }
                Some(kind) => {
                    assert_eq!(state.open_attention.len(), 1, "{}", case.name);
                    let item = state.open_attention.values().next().expect(case.name);
                    assert_eq!(item.kind, kind, "{}", case.name);
                    assert_eq!(item.task_id, Some(tid("t1")), "{}", case.name);
                    assert_eq!(state.phase, MissionPhase::AttentionNeeded, "{}", case.name);
                }
            }
        }
    }

    #[test]
    fn a_review_handoff_on_a_plan_task_fails_instead_of_wedging() {
        let state = fold_log(vec![
            created(),
            plan_proposed(vec![], vec![work_task("t1")]),
            role_completed(
                "t1",
                "k1",
                Handoff::Review {
                    done: true,
                    report: PayloadRef::inline("wrong channel"),
                    passed: true,
                    gaps: vec![],
                    nonce: "n".into(),
                },
                None,
            ),
        ])
        .expect("state");
        assert_eq!(state.tasks[&tid("t1")].status, TaskStatus::Failed);
        assert_eq!(state.open_attention.len(), 1);
        assert_eq!(
            state.open_attention.values().next().unwrap().kind,
            AttentionKind::NodeFailed
        );
    }

    #[test]
    fn validate_handoff_always_clears_and_folds_sticky_advisory() {
        struct Case {
            name: &'static str,
            /// (validator task, verdict) applied in order.
            verdicts: &'static [(&'static str, bool)],
            expect_advisory: AdvisoryStatus,
            expect_last: &'static [(&'static str, bool)],
        }
        let cases = [
            Case {
                name: "pass then later fail stays passed, last_advisory records the false",
                verdicts: &[("v1", true), ("v2", false)],
                expect_advisory: AdvisoryStatus::Passed,
                expect_last: &[("v1", true), ("v2", false)],
            },
            Case {
                name: "same validator flips pass to fail: sticky pass, latest verdict recorded",
                verdicts: &[("v1", true), ("v1", false)],
                expect_advisory: AdvisoryStatus::Passed,
                expect_last: &[("v1", false)],
            },
            Case {
                name: "fail then pass lands passed",
                verdicts: &[("v1", false), ("v2", true)],
                expect_advisory: AdvisoryStatus::Passed,
                expect_last: &[("v1", false), ("v2", true)],
            },
            Case {
                name: "single fail lands failed",
                verdicts: &[("v1", false)],
                expect_advisory: AdvisoryStatus::Failed,
                expect_last: &[("v1", false)],
            },
        ];
        for case in cases {
            let mut events = vec![
                created(),
                plan_proposed(
                    vec![assertion("A1", None)],
                    case.verdicts
                        .iter()
                        .map(|(v, _)| validate_task(v))
                        .collect(),
                ),
            ];
            for (i, (validator, passed)) in case.verdicts.iter().enumerate() {
                events.push(role_completed(
                    validator,
                    &format!("k{i}"),
                    validate_handoff(&[("A1", *passed)]),
                    None,
                ));
            }
            let state = fold_log(events).expect(case.name);
            let a = &state.contract[&aid("A1")];
            assert_eq!(a.advisory, case.expect_advisory, "{}", case.name);
            let expect_last: BTreeMap<TaskId, bool> = case
                .expect_last
                .iter()
                .map(|(v, passed)| (tid(v), *passed))
                .collect();
            assert_eq!(a.last_advisory, expect_last, "{}", case.name);
            // Validators always clear — even the ones that reported a fail.
            for (validator, _) in case.verdicts {
                assert_eq!(
                    state.tasks[&tid(validator)].status,
                    TaskStatus::Cleared,
                    "{}",
                    case.name
                );
            }
            assert!(state.open_attention.is_empty(), "{}", case.name);
        }
    }

    #[test]
    fn validator_reporting_not_done_still_clears() {
        // "Validators always clear" includes a validator that reports
        // done: false — it ran, its verdicts are data; only *work* tasks
        // fail on not-done. No attention is raised either.
        let state = fold_log(vec![
            created(),
            plan_proposed(vec![assertion("A1", None)], vec![validate_task("v1")]),
            role_completed(
                "v1",
                "k1",
                Handoff::Validate {
                    done: false,
                    report: PayloadRef::inline("ran out of budget"),
                    items: vec![ValidationItem {
                        item_id: aid("A1"),
                        passed: false,
                    }],
                    passed: false,
                    request_attention: false,
                },
                None,
            ),
        ])
        .expect("state");
        assert_eq!(state.tasks[&tid("v1")].status, TaskStatus::Cleared);
        assert_eq!(state.contract[&aid("A1")].advisory, AdvisoryStatus::Failed);
        assert!(state.open_attention.is_empty());
        assert!(matches!(state.phase, MissionPhase::Done { .. }));
    }

    #[test]
    fn role_run_failed_fails_task_and_raises_attention() {
        let state = fold_log(vec![
            created(),
            plan_proposed(vec![], vec![work_task("t1")]),
            role_requested("t1", "k1"),
            MissionEvent::RoleRunCompleted {
                task_id: tid("t1"),
                attempt_no: 1,
                effect_id: EffectId::for_parts(&["test", "k1"]),
                outcome: Err(TypedFailure::DeadlineExhausted {
                    evidence: Box::new(TypedFailureEvidence {
                        detail: "took too long".into(),
                        final_response: "partial but useful response".into(),
                        ..Default::default()
                    }),
                }),
            },
        ])
        .expect("state");
        let task = &state.tasks[&tid("t1")];
        assert_eq!(task.status, TaskStatus::Failed);
        assert_eq!(
            task.final_response,
            Some(PayloadRef::inline("partial but useful response"))
        );
        assert_eq!(task.attempts, 1);
        assert!(state.inflight.is_empty(), "outcome settles the request");
        let item = state
            .open_attention
            .values()
            .next()
            .expect("attention item");
        assert_eq!(item.kind, AttentionKind::NodeFailed);
        assert_eq!(item.task_id, Some(tid("t1")));
        assert!(item.report.contains("t1"), "{}", item.report);
        assert_eq!(state.phase, MissionPhase::AttentionNeeded);
    }

    #[test]
    fn typed_failure_classes_drive_only_bounded_correctable_recovery() {
        let cases = [
            (TypedFailure::invalid("handoff.schema", "bad shape"), true),
            (
                TypedFailure::transient("runtime.busy", "busy", Some(10)),
                true,
            ),
            (TypedFailure::permanent("runtime.auth", "denied"), false),
            (
                TypedFailure::DeadlineExhausted {
                    evidence: Box::new(TypedFailureEvidence::new(None, "deadline")),
                },
                false,
            ),
            (
                TypedFailure::Interrupted {
                    evidence: Box::new(TypedFailureEvidence::new(None, "driver died")),
                },
                false,
            ),
            (
                TypedFailure::OperatorStopped {
                    evidence: Box::new(TypedFailureEvidence::new(None, "stop requested")),
                },
                false,
            ),
            (
                TypedFailure::OperatorAborted {
                    evidence: Box::new(TypedFailureEvidence::new(None, "abort requested")),
                },
                false,
            ),
        ];

        for (index, (failure, retryable)) in cases.into_iter().enumerate() {
            let mut mission_created = created();
            let MissionEvent::MissionCreated { config, .. } = &mut mission_created else {
                unreachable!()
            };
            config.recovery.max_attempts = 3;
            let state = fold_log(vec![
                mission_created,
                plan_proposed(vec![], vec![work_task("t1")]),
                role_requested("t1", &format!("request-{index}")),
                MissionEvent::RoleRunCompleted {
                    task_id: tid("t1"),
                    attempt_no: 1,
                    effect_id: EffectId::for_parts(&["test", &format!("request-{index}")]),
                    outcome: Err(failure.clone()),
                },
            ])
            .expect("state");

            assert_eq!(
                state.open_attention.is_empty(),
                retryable,
                "unexpected attention behavior for {}",
                failure.category()
            );
            assert_eq!(
                state.phase,
                if retryable {
                    MissionPhase::Running
                } else {
                    MissionPhase::AttentionNeeded
                },
                "unexpected phase for {}",
                failure.category()
            );
        }
    }

    #[test]
    fn node_decisions_target_the_active_task_era_when_ids_overlap() {
        let mut created = created();
        let MissionEvent::MissionCreated { config, .. } = &mut created else {
            unreachable!("created() builds MissionCreated");
        };
        config.planning.tasks.push(PlanningTask {
            id: tid("same-id"),
            role: RoleName::new("planner").expect("role name"),
            body: "plan".into(),
            depends_on: vec![],
        });
        let state = fold_log(vec![
            created,
            role_requested("same-id", "planning"),
            role_completed("same-id", "planning", work_handoff(true, false), None),
            plan_proposed(vec![], vec![work_task("same-id")]),
            role_requested("same-id", "execution"),
            MissionEvent::RoleRunCompleted {
                task_id: tid("same-id"),
                attempt_no: 1,
                effect_id: EffectId::for_parts(&["test", "execution"]),
                outcome: Err(TypedFailure::DeadlineExhausted {
                    evidence: Box::new(TypedFailureEvidence::new(None, "took too long")),
                }),
            },
            decision(
                "node_failed:same-id",
                super::super::event::DecisionAction::Retry,
            ),
        ])
        .expect("state");

        assert_eq!(
            state.planning.tasks[&tid("same-id")].status,
            TaskStatus::Cleared
        );
        assert_eq!(state.tasks[&tid("same-id")].status, TaskStatus::Pending);
        assert_eq!(state.tasks[&tid("same-id")].consecutive_failures, 0);
        assert!(state.open_attention.is_empty());
    }

    #[test]
    fn artifact_outcome_moves_current_sha() {
        let state = fold_log(vec![
            created(),
            plan_proposed(vec![], vec![work_task("t1")]),
            role_completed(
                "t1",
                "k1",
                work_handoff(true, false),
                Some(ArtifactOutcome {
                    base_sha: "base".into(),
                    head_sha: "head1".into(),
                }),
            ),
        ])
        .expect("state");
        assert_eq!(state.current_sha, "head1");
        assert_eq!(state.base_sha, "base", "base is immutable");
    }

    #[test]
    fn oracle_run_completed_mints_verdict_and_settles_inflight() {
        for (exit_code, expect_passed, expect_finish) in [
            (0, true, FinishClass::Verified),
            (1, false, FinishClass::Unverified),
            (101, false, FinishClass::Unverified),
        ] {
            let mut events = vec![
                created(),
                plan_proposed(
                    vec![assertion("TESTS-PASS", Some("cargo-test"))],
                    vec![work_task("t1")],
                ),
                role_completed("t1", "kr", work_handoff(true, false), None),
            ];
            // Task cleared, nothing inflight: the unsettled oracle
            // obligation alone keeps the mission running.
            let owed = fold_log(events.clone()).expect("owed state");
            assert!(owed.inflight.is_empty());
            assert!(!tasks_active(&owed));
            assert_eq!(owed.phase, MissionPhase::Running);

            events.push(oracle_requested("TESTS-PASS", "base", "ko"));
            let mid = fold_log(events.clone()).expect("mid state");
            assert!(
                mid.inflight
                    .contains_key(&EffectId::for_parts(&["test", "ko"])),
                "request is inflight"
            );
            assert_eq!(mid.phase, MissionPhase::Running);

            events.push(oracle_completed("TESTS-PASS", "base", "ko", exit_code));
            let state = fold_log(events).expect("state");
            assert!(state.inflight.is_empty(), "exit {exit_code}");
            assert_eq!(state.oracle_attempts[&oracle("cargo-test")], 1);
            let verdict = state.contract[&aid("TESTS-PASS")]
                .last_authoritative
                .as_ref()
                .expect("verdict minted");
            assert_eq!(verdict.passed(), expect_passed, "exit {exit_code}");
            assert_eq!(verdict.exit_code(), exit_code);
            assert_eq!(verdict.judged_sha(), "base");
            assert_eq!(classify_finish(&state), expect_finish);
            if expect_passed {
                assert!(matches!(state.phase, MissionPhase::Done { .. }));
            } else {
                assert_eq!(state.phase, MissionPhase::AttentionNeeded);
                let item = &state.open_attention["oracle_verdict_failed:cargo-test"];
                assert_eq!(item.evidence.as_ref().unwrap().exit_code, exit_code);
            }
        }
    }

    // Regression (QA): the freshness filter in classify_finish. A fresh
    // authoritative pass verifies; if the artifact head then moves past the
    // judged commit, the SAME pass is stale and must no longer count toward
    // Verified. Removing the `is_fresh_at` filter would keep this Verified.
    #[test]
    fn classify_finish_ignores_a_stale_authoritative_pass() {
        let mut state = fold_log(vec![
            created(),
            plan_proposed(
                vec![assertion("TESTS-PASS", Some("cargo-test"))],
                vec![work_task("t1")],
            ),
            role_completed("t1", "kr", work_handoff(true, false), None),
            oracle_completed("TESTS-PASS", "base", "ko", 0),
        ])
        .expect("state");
        assert_eq!(
            classify_finish(&state),
            FinishClass::Verified,
            "a fresh pass verifies"
        );
        // The head advances past the judged commit: the verdict is now stale.
        state.current_sha = "moved-on".into();
        assert_eq!(
            classify_finish(&state),
            FinishClass::Unverified,
            "a stale pass must not verify"
        );
    }

    // Regression (QA): a signal-killed oracle (clean exit_code 0 but a signal)
    // is NOT a pass — the honesty floor requires no signal. Pins the
    // `exit_signal.is_none()` clause so it can't silently regress.
    #[test]
    fn signal_killed_oracle_is_not_a_pass() {
        let state = fold_log(vec![
            created(),
            plan_proposed(
                vec![assertion("TESTS-PASS", Some("cargo-test"))],
                vec![work_task("t1")],
            ),
            role_completed("t1", "kr", work_handoff(true, false), None),
            oracle_requested("TESTS-PASS", "base", "ko"),
            MissionEvent::OracleRunCompleted {
                assertion_ids: vec![aid("TESTS-PASS")],
                oracle: oracle("cargo-test"),
                judged_sha: "base".into(),
                attempt_no: 1,
                effect_id: EffectId::for_parts(&["test", "ko"]),
                outcome: Ok(OracleRunSuccess {
                    exit_code: 0,
                    exit_signal: Some(9),
                    stdout: PayloadRef::inline("out"),
                    stderr: PayloadRef::inline("err"),
                    prepared_inputs: Vec::new(),
                    duration_ms: 5,
                }),
            },
        ])
        .expect("state");
        let verdict = state.contract[&aid("TESTS-PASS")]
            .last_authoritative
            .as_ref()
            .expect("verdict minted");
        assert!(!verdict.passed(), "a signal-killed oracle must not pass");
        assert_eq!(classify_finish(&state), FinishClass::Unverified);
        assert_eq!(state.phase, MissionPhase::AttentionNeeded);
    }

    #[test]
    fn oracle_run_failed_raises_attention_without_verdict() {
        let state = fold_log(vec![
            created(),
            plan_proposed(vec![assertion("TESTS-PASS", Some("cargo-test"))], vec![]),
            oracle_requested("TESTS-PASS", "base", "ko"),
            MissionEvent::OracleRunCompleted {
                assertion_ids: vec![aid("TESTS-PASS")],
                oracle: oracle("cargo-test"),
                judged_sha: "base".into(),
                attempt_no: 1,
                effect_id: EffectId::for_parts(&["test", "ko"]),
                outcome: Err(TypedFailure::permanent("oracle.spawn", "spawn failed")),
            },
        ])
        .expect("state");
        assert!(state.inflight.is_empty());
        assert!(state.contract[&aid("TESTS-PASS")]
            .last_authoritative
            .is_none());
        let item = state
            .open_attention
            .values()
            .next()
            .expect("attention item");
        assert_eq!(item.kind, AttentionKind::OracleFailed);
        assert_eq!(item.task_id, None);
        assert_eq!(item.oracle, Some(oracle("cargo-test")));
        assert_eq!(state.phase, MissionPhase::AttentionNeeded);
    }

    #[test]
    fn mission_aborted_is_sticky() {
        let state = fold_log(vec![
            created(),
            plan_proposed(vec![], vec![work_task("t1")]),
            MissionEvent::MissionAborted {
                reason: "operator stop".into(),
            },
            role_completed("t1", "k1", work_handoff(true, false), None),
        ])
        .expect("state");
        assert_eq!(
            state.phase,
            MissionPhase::Aborted {
                reason: "operator stop".into()
            }
        );
        // Later facts still fold; only the phase is pinned.
        assert_eq!(state.tasks[&tid("t1")].status, TaskStatus::Cleared);
        assert_eq!(state.head, 4);
    }

    #[test]
    fn attention_ids_are_kind_and_anchor_scoped_and_stable_across_refolds() {
        let events = vec![
            created(),
            plan_proposed(vec![], vec![work_task("t1")]),
            role_completed("t1", "k1", work_handoff(false, false), None),
        ];
        let once = fold_log(events.clone()).expect("first fold");
        let twice = fold_log(events).expect("second fold");
        let keys: Vec<&str> = once.open_attention.keys().map(String::as_str).collect();
        // Derived ids are stable (kind:anchor), not seq-embedded, so a
        // decision can name them across resumes.
        assert_eq!(keys, vec!["node_failed:t1"]);
        assert_eq!(
            keys,
            twice
                .open_attention
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>()
        );
        assert_eq!(once, twice, "the fold is deterministic");
    }

    #[test]
    fn unknown_ids_in_events_are_tolerated() {
        let state = fold_log(vec![
            created(),
            plan_proposed(vec![assertion("KNOWN-1", None)], vec![]),
            // Task never declared by any plan.
            role_completed("ghost", "k1", work_handoff(true, false), None),
            // Validator verdict for an assertion the contract never heard of.
            role_completed(
                "phantom",
                "k2",
                validate_handoff(&[("UNKNOWN-1", true)]),
                None,
            ),
            MissionEvent::RoleRunCompleted {
                task_id: tid("specter"),
                attempt_no: 1,
                effect_id: EffectId::for_parts(&["test", "k3"]),
                outcome: Err(TypedFailure::permanent("runtime.gone", "gone")),
            },
        ])
        .expect("state");
        // No panics and no phantom rows: outcomes only touch declared ids.
        // Because attention is derived from the declared plan, an outcome for
        // an undeclared task raises nothing (it cannot, and should not).
        assert!(state.tasks.is_empty());
        assert_eq!(state.contract.len(), 1);
        assert_eq!(
            state.contract[&aid("KNOWN-1")].advisory,
            AdvisoryStatus::Pending
        );
        assert!(state.open_attention.is_empty());
    }

    // ---- Terminal review: the closing obligation, its parks, and staleness ----

    use super::super::event::{Gap, GapSeverity, TerminalReviewConfig};
    use super::super::state::{ReviewAcceptanceKind, ReviewOutcome};

    /// The acceptance a `decision(...)` builder produces for exact-equality
    /// assertions.
    fn accepted(kind: ReviewAcceptanceKind, judged_sha: &str) -> ReviewAcceptance {
        ReviewAcceptance {
            kind,
            judged_sha: judged_sha.into(),
            justification: "j".into(),
        }
    }

    fn created_with_review() -> MissionEvent {
        let mut event = created();
        let MissionEvent::MissionCreated { config, .. } = &mut event else {
            unreachable!("created() builds MissionCreated");
        };
        config.terminal_review = Some(TerminalReviewConfig {
            role: RoleName::new("gap-reviewer").expect("role name"),
        });
        event
    }

    fn gap(severity: GapSeverity) -> Gap {
        Gap {
            id: None,
            severity,
            requirement: "the objective's behavior".into(),
            expected: "it works".into(),
            observed: "it does not".into(),
            evidence: "ran it; saw it".into(),
        }
    }

    fn review_requested(attempt_no: u32, key: &str, judged: &str) -> MissionEvent {
        MissionEvent::TerminalReviewRequested {
            attempt_no,
            effect_id: EffectId::for_parts(&["test", key]),
            role: RoleName::new("gap-reviewer").expect("role name"),
            runtime: "codex".into(),
            prompt: PayloadRef::inline("review prompt"),
            judged_sha: judged.into(),
            nonce: "n0".into(),
        }
    }

    fn review_completed_at(
        attempt_no: u32,
        key: &str,
        judged: &str,
        passed: bool,
        gaps: Vec<Gap>,
    ) -> MissionEvent {
        MissionEvent::TerminalReviewCompleted {
            attempt_no,
            effect_id: EffectId::for_parts(&["test", key]),
            judged_sha: judged.into(),
            outcome: Ok(TerminalReviewSuccess {
                passed,
                gaps,
                report: PayloadRef::inline("requirement map + observations"),
                final_response: PayloadRef::inline("review complete"),
                runtime_configuration: RuntimeConfigurationEvidence::default(),
            }),
        }
    }

    fn review_completed(key: &str, judged: &str, passed: bool, gaps: Vec<Gap>) -> MissionEvent {
        review_completed_at(1, key, judged, passed, gaps)
    }

    fn review_failed_at(attempt_no: u32, key: &str, judged: &str, detail: &str) -> MissionEvent {
        MissionEvent::TerminalReviewCompleted {
            attempt_no,
            effect_id: EffectId::for_parts(&["test", key]),
            judged_sha: judged.into(),
            outcome: Err(TypedFailure::DeadlineExhausted {
                evidence: Box::new(TypedFailureEvidence::new(None, detail)),
            }),
        }
    }

    fn review_failed(key: &str, judged: &str, detail: &str) -> MissionEvent {
        review_failed_at(1, key, judged, detail)
    }

    fn review_transient_failed_at(
        attempt_no: u32,
        key: &str,
        judged: &str,
        detail: &str,
    ) -> MissionEvent {
        MissionEvent::TerminalReviewCompleted {
            attempt_no,
            effect_id: EffectId::for_parts(&["test", key]),
            judged_sha: judged.into(),
            outcome: Err(TypedFailure::transient("runtime.busy", detail, None)),
        }
    }

    /// A review-configured mission driven to the brink of closure: work
    /// committed (head → "h1"), the oracle fresh-passing there. Only the
    /// review obligation remains.
    fn events_to_the_brink() -> Vec<MissionEvent> {
        vec![
            created_with_review(),
            plan_proposed(
                vec![assertion("TESTS-PASS", Some("cargo-test"))],
                vec![work_task("fix")],
            ),
            role_completed(
                "fix",
                "k1",
                work_handoff(true, false),
                Some(ArtifactOutcome {
                    base_sha: "base".into(),
                    head_sha: "h1".into(),
                }),
            ),
            oracle_completed("TESTS-PASS", "h1", "ko", 0),
        ]
    }

    #[test]
    fn terminal_review_holds_running_until_a_fresh_verdict_then_closes_clean() {
        // Work settled + oracle fresh-passing: the review obligation is the
        // sole reason the mission has not closed.
        let mut events = events_to_the_brink();
        let owed = fold_log(events.clone()).expect("owed state");
        assert!(owed.inflight.is_empty());
        assert!(owed.open_attention.is_empty());
        assert!(terminal_review_outstanding(&owed));
        assert_eq!(owed.phase, MissionPhase::Running);

        // A clean fresh verdict closes with no park — zero mental tax — and
        // never touches the oracle-minted finish grade.
        events.push(review_requested(1, "kr", "h1"));
        events.push(review_completed("kr", "h1", true, vec![]));
        let state = fold_log(events).expect("state");
        assert!(state.open_attention.is_empty());
        assert_eq!(state.terminal_review.attempts, 1);
        assert_eq!(
            state.phase,
            MissionPhase::Done {
                finish: FinishClass::Verified
            }
        );
    }

    #[test]
    fn a_log_without_terminal_review_config_derives_exactly_as_before() {
        // The same brink under a config-less mission closes immediately …
        let mut events = events_to_the_brink();
        events[0] = created();
        let state = fold_log(events.clone()).expect("state");
        assert!(!terminal_review_outstanding(&state));
        assert_eq!(
            state.phase,
            MissionPhase::Done {
                finish: FinishClass::Verified
            }
        );

        // … and even hostile injected review events gate nothing: the fold
        // records them (never trust the writer, but never drop facts), while
        // every derivation stays config-gated.
        events.push(review_completed(
            "kr",
            "h1",
            false,
            vec![gap(GapSeverity::Blocking)],
        ));
        let state = fold_log(events).expect("state");
        assert!(state.terminal_review.outcome.is_some());
        assert!(state.open_attention.is_empty());
        assert_eq!(
            state.phase,
            MissionPhase::Done {
                finish: FinishClass::Verified
            }
        );
    }

    #[test]
    fn minor_gaps_do_not_park_a_passed_review() {
        let mut events = events_to_the_brink();
        events.push(review_completed(
            "kr",
            "h1",
            true,
            vec![gap(GapSeverity::Major), gap(GapSeverity::Minor)],
        ));
        let state = fold_log(events).expect("state");
        assert!(state.open_attention.is_empty());
        assert!(matches!(state.phase, MissionPhase::Done { .. }));
        // The gaps stay on record for the receipt.
        let Some(ReviewOutcome::Verdict(v)) = &state.terminal_review.outcome else {
            panic!("verdict recorded");
        };
        assert_eq!(v.gaps.len(), 2);
        assert!(!v.blocking());
    }

    #[test]
    fn blocking_gaps_park_and_accept_acknowledges_at_the_judged_sha() {
        let mut events = events_to_the_brink();
        events.push(review_completed(
            "kr",
            "h1",
            false,
            vec![gap(GapSeverity::Blocking), gap(GapSeverity::Minor)],
        ));
        let parked = fold_log(events.clone()).expect("parked state");
        assert_eq!(parked.phase, MissionPhase::AttentionNeeded);
        let item = &parked.open_attention["terminal_review_gaps:mission"];
        assert_eq!(item.kind, AttentionKind::TerminalReviewGaps);
        assert!(item.report.contains("1 blocking gap(s) of 2 total"));

        events.push(decision(
            "terminal_review_gaps:mission",
            super::super::event::DecisionAction::Accept,
        ));
        let state = fold_log(events).expect("state");
        assert_eq!(
            state.terminal_review.accepted,
            Some(accepted(ReviewAcceptanceKind::AcknowledgedGaps, "h1"))
        );
        assert!(state.open_attention.is_empty());
        assert!(matches!(state.phase, MissionPhase::Done { .. }));
    }

    #[test]
    fn a_passed_verdict_with_a_blocking_gap_still_parks() {
        // Fail-closed dominance: the reviewer's own summary bit cannot wave a
        // blocking gap through.
        let mut events = events_to_the_brink();
        events.push(review_completed(
            "kr",
            "h1",
            true,
            vec![gap(GapSeverity::Blocking)],
        ));
        let state = fold_log(events).expect("state");
        assert_eq!(state.phase, MissionPhase::AttentionNeeded);
    }

    #[test]
    fn an_unstructured_fail_still_parks() {
        // passed=false with zero typed gaps is an honest fail; the report is
        // the evidence.
        let mut events = events_to_the_brink();
        events.push(review_completed("kr", "h1", false, vec![]));
        let state = fold_log(events).expect("state");
        assert_eq!(state.phase, MissionPhase::AttentionNeeded);
        let item = &state.open_attention["terminal_review_gaps:mission"];
        assert!(item.report.contains("see its report"));
    }

    #[test]
    fn retry_discards_the_verdict_and_reopens_the_obligation() {
        let mut events = events_to_the_brink();
        events.push(review_requested(1, "kr", "h1"));
        events.push(review_completed(
            "kr",
            "h1",
            false,
            vec![gap(GapSeverity::Blocking)],
        ));
        events.push(decision(
            "terminal_review_gaps:mission",
            super::super::event::DecisionAction::Retry,
        ));
        let state = fold_log(events).expect("state");
        assert_eq!(state.terminal_review.outcome, None);
        assert_eq!(state.terminal_review.accepted, None);
        // Attempts are preserved (the next dispatch is attempt 2 under a
        // fresh effect ID) and the obligation holds the phase.
        assert_eq!(state.terminal_review.attempts, 1);
        assert!(terminal_review_outstanding(&state));
        assert_eq!(state.phase, MissionPhase::Running);
    }

    #[test]
    fn a_head_move_stales_both_verdict_and_acknowledgment() {
        let mut events = events_to_the_brink();
        events.push(review_completed(
            "kr",
            "h1",
            false,
            vec![gap(GapSeverity::Blocking)],
        ));
        events.push(decision(
            "terminal_review_gaps:mission",
            super::super::event::DecisionAction::Accept,
        ));
        assert!(matches!(
            fold_log(events.clone()).expect("state").phase,
            MissionPhase::Done { .. }
        ));

        // New work moves the head: the h1 verdict and its acknowledgment are
        // both stale — the mission re-opens and a blocking verdict at h2
        // parks again (the acknowledgment is keyed, never inherited).
        events.push(role_completed(
            "fix",
            "k2",
            work_handoff(true, false),
            Some(ArtifactOutcome {
                base_sha: "h1".into(),
                head_sha: "h2".into(),
            }),
        ));
        events.push(oracle_completed("TESTS-PASS", "h2", "ko2", 0));
        let reopened = fold_log(events.clone()).expect("reopened state");
        assert!(terminal_review_outstanding(&reopened));
        assert_eq!(reopened.phase, MissionPhase::Running);

        events.push(review_completed(
            "kr2",
            "h2",
            false,
            vec![gap(GapSeverity::Blocking)],
        ));
        let state = fold_log(events).expect("state");
        assert_eq!(
            state.terminal_review.accepted,
            Some(accepted(ReviewAcceptanceKind::AcknowledgedGaps, "h1"))
        );
        assert_eq!(state.phase, MissionPhase::AttentionNeeded);
    }

    #[test]
    fn terminal_review_failure_parks_then_retry_reopens() {
        let mut events = events_to_the_brink();
        events.push(review_requested(1, "kr", "h1"));
        events.push(review_failed("kr", "h1", "agent timed out"));
        let parked = fold_log(events.clone()).expect("parked state");
        assert_eq!(parked.phase, MissionPhase::AttentionNeeded);
        let item = &parked.open_attention["terminal_review_failed:mission"];
        assert_eq!(item.kind, AttentionKind::TerminalReviewFailed);
        assert!(item.report.contains("agent timed out"));

        events.push(decision(
            "terminal_review_failed:mission",
            super::super::event::DecisionAction::Retry,
        ));
        let state = fold_log(events).expect("state");
        assert_eq!(state.terminal_review.outcome, None);
        assert!(terminal_review_outstanding(&state));
        assert_eq!(state.phase, MissionPhase::Running);
    }

    #[test]
    fn prior_successful_reviews_do_not_consume_the_recovery_budget() {
        use super::super::step::{step, StepDecision};

        let mut events = events_to_the_brink();
        let MissionEvent::MissionCreated { config, .. } = &mut events[0] else {
            unreachable!("events_to_the_brink starts with MissionCreated");
        };
        config.recovery.max_attempts = 3;
        events.push(review_requested(4, "kr4", "h1"));
        events.push(review_completed_at(4, "kr4", "h1", true, vec![]));
        events.push(role_completed(
            "fix",
            "k2",
            work_handoff(true, false),
            Some(ArtifactOutcome {
                base_sha: "h1".into(),
                head_sha: "h2".into(),
            }),
        ));
        events.push(oracle_completed("TESTS-PASS", "h2", "ko2", 0));
        events.push(review_requested(5, "kr5", "h2"));
        events.push(review_transient_failed_at(
            5,
            "kr5",
            "h2",
            "temporary overload",
        ));

        let state = fold_log(events).expect("state");
        assert_eq!(state.terminal_review.attempts, 5);
        assert_eq!(state.terminal_review.consecutive_failures, 1);
        assert!(state.open_attention.is_empty());
        assert_eq!(state.phase, MissionPhase::Running);
        assert!(matches!(
            step(&state),
            StepDecision::ReviewTerminal(intent) if intent.attempt_no == 6
        ));
    }

    #[test]
    fn waiving_a_failed_review_closes_and_is_distinct_from_acknowledgment() {
        let mut events = events_to_the_brink();
        events.push(review_failed("kr", "h1", "agent timed out"));
        events.push(decision(
            "terminal_review_failed:mission",
            super::super::event::DecisionAction::Accept,
        ));
        let state = fold_log(events).expect("state");
        // The waiver kind keeps the receipt honest: no verdict exists, and
        // the provenance (who, why) is folded into state.
        assert_eq!(
            state.terminal_review.accepted,
            Some(accepted(ReviewAcceptanceKind::Waived, "h1"))
        );
        assert_eq!(state.terminal_review.outcome, None);
        assert!(!terminal_review_outstanding(&state));
        // The finish grade comes from the oracle facts alone.
        assert_eq!(
            state.phase,
            MissionPhase::Done {
                finish: FinishClass::Verified
            }
        );
    }

    #[test]
    fn a_head_move_stales_a_waiver_and_reopens_the_review() {
        // A waiver is granted at a head, never inherited: new work after a
        // waived close re-opens the review obligation (the instrument may
        // have recovered, and the human never saw the new tree).
        let mut events = events_to_the_brink();
        events.push(review_failed("kr", "h1", "agent timed out"));
        events.push(decision(
            "terminal_review_failed:mission",
            super::super::event::DecisionAction::Accept,
        ));
        assert!(matches!(
            fold_log(events.clone()).expect("state").phase,
            MissionPhase::Done { .. }
        ));

        events.push(role_completed(
            "fix",
            "k2",
            work_handoff(true, false),
            Some(ArtifactOutcome {
                base_sha: "h1".into(),
                head_sha: "h2".into(),
            }),
        ));
        events.push(oracle_completed("TESTS-PASS", "h2", "ko2", 0));
        let state = fold_log(events).expect("state");
        assert!(terminal_review_outstanding(&state));
        assert_eq!(state.phase, MissionPhase::Running);
    }

    #[test]
    fn retry_after_failure_then_completion_records_the_verdict() {
        let mut events = events_to_the_brink();
        events.push(review_requested(1, "kr", "h1"));
        events.push(review_failed("kr", "h1", "agent timed out"));
        events.push(decision(
            "terminal_review_failed:mission",
            super::super::event::DecisionAction::Retry,
        ));
        events.push(review_requested(2, "kr2", "h1"));
        events.push(review_completed("kr2", "h1", true, vec![]));
        let state = fold_log(events).expect("state");
        assert_eq!(state.terminal_review.attempts, 2);
        assert!(matches!(
            state.terminal_review.outcome,
            Some(ReviewOutcome::Verdict(_))
        ));
        assert!(matches!(state.phase, MissionPhase::Done { .. }));
    }

    #[test]
    fn terminal_review_decision_legality() {
        use super::super::decision::{validate_decision, DecisionError};
        use super::super::event::DecisionAction;
        // A gaps park and a failure park, each straight from the fold.
        let mut gaps_events = events_to_the_brink();
        gaps_events.push(review_completed(
            "kr",
            "h1",
            false,
            vec![gap(GapSeverity::Blocking)],
        ));
        let gaps_state = fold_log(gaps_events).expect("state");
        let mut failed_events = events_to_the_brink();
        failed_events.push(review_failed("kr", "h1", "boom"));
        let failed_state = fold_log(failed_events).expect("state");

        for (state, item, allowed) in [
            (
                &gaps_state,
                "terminal_review_gaps:mission",
                &[
                    DecisionAction::Retry,
                    DecisionAction::Revise,
                    DecisionAction::Accept,
                    DecisionAction::Abort,
                ][..],
            ),
            (
                &failed_state,
                "terminal_review_failed:mission",
                &[
                    DecisionAction::Retry,
                    DecisionAction::Accept,
                    DecisionAction::Abort,
                ][..],
            ),
        ] {
            for action in allowed {
                assert!(
                    validate_decision(state, item, action, "accepted").is_ok(),
                    "{item} must accept {action:?}"
                );
            }
            assert!(
                matches!(
                    validate_decision(state, item, &DecisionAction::Approve, ""),
                    Err(DecisionError::InvalidAction { .. })
                ),
                "{item} must refuse Approve"
            );
        }
    }

    #[test]
    fn failed_authoritative_verdict_parks_for_repair_without_review() {
        // A fresh authoritative FAIL is proof that the judged artifact needs
        // work, so it parks on the repair path without burning a reviewer
        // turn. The review remains the last gate on a passing artifact only.
        let mut events = events_to_the_brink();
        events.pop(); // replace the passing oracle run …
        events.push(oracle_completed("TESTS-PASS", "h1", "ko", 1)); // … with a fail
        let state = fold_log(events).expect("state");
        assert!(!terminal_review_outstanding(&state));
        assert_eq!(state.phase, MissionPhase::AttentionNeeded);
        assert!(state
            .open_attention
            .contains_key("oracle_verdict_failed:cargo-test"));
    }
}
