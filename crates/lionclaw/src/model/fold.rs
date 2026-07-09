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

use super::event::{AmendmentOps, EventEnvelope, Handoff, MissionEvent};
use super::ids::{AssertionId, OracleName, TaskId};
use super::plan::{Assertion, PlanSubmission};
use super::state::{
    AdvisoryStatus, AssertionState, AttentionItem, AttentionKind, InflightEffect, MissionPhase,
    MissionState, PlanningState, TaskRuntimeState, TaskStatus,
};
use super::verdict::{classify_finish, AuthoritativeVerdict};

/// Bump when fold semantics change; snapshots with a different version are
/// discarded and rebuilt from sequence zero.
pub const REDUCER_VERSION: u32 = 3;

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
        .map(|t| {
            (
                t.id.clone(),
                TaskRuntimeState {
                    status: TaskStatus::Pending,
                    attempts: 0,
                    last_report: None,
                },
            )
        })
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
        proposal: None,
        current_sha: base_sha.clone(),
        oracle_attempts: Default::default(),
        inflight: Default::default(),
        open_attention: Default::default(),
        ratified: false,
        revision: 0,
        acknowledged_gates: Default::default(),
        flagged_nodes: Default::default(),
        oracle_failures: Default::default(),
        waived_oracles: Default::default(),
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
        MissionEvent::PlanSubmitted { plan, .. } => {
            for assertion in &plan.assertions {
                seed_assertion(&mut state.contract, assertion);
            }
            for task in &plan.tasks {
                seed_task(&mut state.tasks, &task.id);
            }
            state.plan = Some(plan.clone());
            state.revision = 1;
        }
        MissionEvent::RoleRunRequested {
            task_id,
            attempt_no,
            ..
        } => {
            let tasks = era_tasks_mut(state);
            let task = tasks.entry(task_id.clone()).or_insert(TaskRuntimeState {
                status: TaskStatus::Pending,
                attempts: 0,
                last_report: None,
            });
            task.status = TaskStatus::Running;
            task.attempts = *attempt_no;
            track_inflight(state, &envelope.event, seq);
        }
        MissionEvent::RoleRunCompleted {
            task_id,
            idempotency_key,
            handoff,
            artifact,
            ..
        } => {
            state.inflight.remove(idempotency_key);
            if let Some(artifact) = artifact {
                state.current_sha = artifact.head_sha.clone();
            }
            apply_handoff(state, task_id, handoff);
        }
        MissionEvent::RoleRunFailed {
            task_id,
            idempotency_key,
            ..
        } => {
            state.inflight.remove(idempotency_key);
            if let Some(task) = era_tasks_mut(state).get_mut(task_id) {
                task.status = TaskStatus::Failed;
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
            idempotency_key,
            exit_code,
            exit_signal,
            stdout,
            stderr,
            ..
        } => {
            state.inflight.remove(idempotency_key);
            state.oracle_failures.remove(oracle); // the oracle ran; recovered
            let verdict = AuthoritativeVerdict::from_oracle_outcome(
                oracle.clone(),
                judged_sha.clone(),
                *exit_code,
                *exit_signal,
                stdout.clone(),
                stderr.clone(),
            );
            for assertion_id in assertion_ids {
                if let Some(assertion) = state.contract.get_mut(assertion_id) {
                    assertion.last_authoritative = Some(verdict.clone());
                }
            }
        }
        MissionEvent::OracleRunFailed {
            oracle,
            idempotency_key,
            detail,
            ..
        } => {
            state.inflight.remove(idempotency_key);
            state.oracle_failures.insert(oracle.clone(), detail.clone());
        }
        MissionEvent::MissionAborted { reason, .. } => {
            state.phase = MissionPhase::Aborted {
                reason: reason.clone(),
            };
        }
        MissionEvent::DecisionRecorded {
            attention_id,
            action,
            ..
        } => {
            apply_decision(state, attention_id, action);
        }
        MissionEvent::PlanAmended {
            base_revision, ops, ..
        } => {
            apply_amendment(state, *base_revision, ops);
        }
    }
    state.head = seq;
    // Promotion runs first: a just-ratified proposal must seed the contract
    // before gates/attention/phase are derived this same fold (a plan whose only
    // sink is a gate would otherwise hang one event behind).
    derive_promotion(state);
    derive_gates(state);
    derive_attention(state);
    derive_phase(state);
}

/// Seed the execution contract + task DAG from a ratified *proposal* (the
/// in-engine author flow) — the sole promotion path, guarded by
/// `plan.is_none()`. A proposal is gradeless until here; an engine-authored one
/// (or any, under the ratification gate) needs a human `Ratify` first. A
/// manually submitted plan is the other way the contract is seeded — directly,
/// via the `PlanSubmitted` arm, gated instead by the ratification attention.
fn derive_promotion(state: &mut MissionState) {
    if state.plan.is_some() || state.proposal.is_none() {
        return;
    }
    // Every proposal is engine-authored, so it always needs a human `Ratify`
    // first (regardless of `--yes`/ratification_gate, which govern only the
    // manual PlanSubmitted path).
    if !state.ratified {
        return;
    }
    let proposal = state
        .proposal
        .take()
        .expect("proposal present (checked above)");
    for assertion in &proposal.assertions {
        seed_assertion(&mut state.contract, assertion);
    }
    for task in &proposal.tasks {
        seed_task(&mut state.tasks, &task.id);
    }
    state.plan = Some(proposal);
    state.revision = 1;
}

/// Apply an amendment (ADR 0011). The fold guards the *honesty-relevant*
/// invariants against a bad writer: the whole event no-ops unless it targets
/// the current revision and every `bind_oracle` strengthens (never unbinds).
/// It does NOT re-run the engine's structural validation (coverage, acyclicity,
/// materiality) — those bound liveness, not honesty, and a structurally-broken
/// amendment can at worst leave the mission unverifiable, never falsely
/// Verified. Honesty needs no sealing check either: an amendment only ever
/// *strengthens* the contract, and the oracle re-judges the real tree at the
/// final head, so it cannot launder a verdict.
fn apply_amendment(state: &mut MissionState, base_revision: u32, ops: &AmendmentOps) {
    if state.plan.is_none() || base_revision != state.revision {
        return;
    }
    for b in &ops.bind_oracle {
        if !bind_strengthens(&state.contract, &b.assertion, &b.oracle) {
            return;
        }
    }

    // Reconcile the runtime maps: seed added tasks/assertions, tombstone
    // retired tasks, apply oracle bindings. (Re-adding a retired id is rejected
    // by validation, so `seed_task` never collides with a tombstone here.)
    for task in &ops.add {
        seed_task(&mut state.tasks, &task.id);
    }
    for old in ops
        .supersede
        .iter()
        .map(|s| &s.old)
        .chain(ops.cancel.iter())
    {
        if let Some(rt) = state.tasks.get_mut(old) {
            rt.status = TaskStatus::Superseded;
        }
    }
    for assertion in &ops.add_assertion {
        seed_assertion(&mut state.contract, assertion);
    }
    for b in &ops.bind_oracle {
        if let Some(a) = state.contract.get_mut(&b.assertion) {
            if a.oracle.is_none() {
                a.oracle = Some(b.oracle.clone());
            }
        }
    }

    // Replace the plan with the resulting *live* plan (retired tasks removed,
    // dependencies reconciled) — the same transform validation ran.
    let old_plan = state.plan.as_ref().expect("plan present");
    let new_plan = resulting_plan(old_plan, ops);
    // Re-open the advisory nodes whose basis this amendment invalidated. A
    // Validate/Gate that is transitively downstream of a retired task judged a
    // now-discarded tree, so reset it to Pending: derive_gates re-derives a
    // Pending gate, and step re-dispatches a Pending validator against the new
    // head. (Work is cumulative — no rewind — so only advisory nodes stale;
    // the oracle re-judges the real tree regardless, so Verified is untouched.)
    let retired: BTreeSet<&TaskId> = ops
        .supersede
        .iter()
        .map(|s| &s.old)
        .chain(ops.cancel.iter())
        .collect();
    if !retired.is_empty() {
        let live: BTreeSet<&TaskId> = new_plan.tasks.iter().map(|t| &t.id).collect();
        let to_reset: Vec<TaskId> = old_plan
            .tasks
            .iter()
            .filter(|t| {
                matches!(
                    t.kind,
                    super::plan::TaskKind::Validate | super::plan::TaskKind::Gate
                )
            })
            .filter(|t| live.contains(&t.id))
            .filter(|t| depends_on_retired(old_plan, &t.id, &retired))
            .map(|t| t.id.clone())
            .collect();
        for id in to_reset {
            if let Some(rt) = state.tasks.get_mut(&id) {
                rt.status = TaskStatus::Pending;
            }
            // Scrub every latch that pinned the node's stale judgment, so the
            // re-run re-establishes them against the new tree: the gate
            // acknowledgement, the attention flag, and the advisory verdicts it
            // recorded. Removing a verdict also re-derives the assertion's
            // sticky advisory from the *surviving* validators — else a re-run
            // that now fails would leave a stale `Passed` and finish
            // InternallyConsistent on a tree the validator just rejected. Gate
            // ids are absent from the flag/verdict maps, so this is uniform.
            state.acknowledged_gates.remove(&id);
            state.flagged_nodes.remove(&id);
            for assertion in state.contract.values_mut() {
                if assertion.last_advisory.remove(&id).is_some() {
                    assertion.advisory = recompute_advisory(&assertion.last_advisory);
                }
            }
        }
    }
    state.plan = Some(new_plan);
    state.revision += 1;
    // A material amendment re-opens the ratification gate for the new revision
    // (ADR 0006); with the gate off this is a no-op.
    if state.config.ratification_gate {
        state.ratified = false;
    }
}

/// The live plan after applying `ops`: add new tasks, retire superseded/
/// cancelled tasks (removed from the live plan, downstream `depends_on`
/// rewritten old→new for supersede / dropped for cancel), add assertions, and
/// bind oracles (None→Some). Pure and shared by the fold and amendment
/// validation so the two can never disagree about the resulting plan.
pub(crate) fn resulting_plan(current: &PlanSubmission, ops: &AmendmentOps) -> PlanSubmission {
    let mut plan = current.clone();
    plan.tasks.extend(ops.add.iter().cloned());
    for s in &ops.supersede {
        plan.tasks.retain(|t| t.id != s.old);
        for task in &mut plan.tasks {
            for dep in &mut task.depends_on {
                if *dep == s.old {
                    *dep = s.new.clone();
                }
            }
        }
    }
    for old in &ops.cancel {
        plan.tasks.retain(|t| &t.id != old);
        for task in &mut plan.tasks {
            task.depends_on.retain(|dep| dep != old);
        }
    }
    // A supersede old→new can duplicate a dependency (a task that depended on
    // both). Dedup, preserving order, so the plan has no redundant edges.
    for task in &mut plan.tasks {
        let mut seen = BTreeSet::new();
        task.depends_on.retain(|dep| seen.insert(dep.clone()));
    }
    plan.assertions.extend(ops.add_assertion.iter().cloned());
    for b in &ops.bind_oracle {
        if let Some(a) = plan.assertions.iter_mut().find(|a| a.id == b.assertion) {
            if a.oracle.is_none() {
                a.oracle = Some(b.oracle.clone());
            }
        }
    }
    plan
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

/// Whether `start` transitively depends (over `plan.depends_on`) on any task
/// in `retired` — i.e. a retired task sits in its upstream closure.
fn depends_on_retired(plan: &PlanSubmission, start: &TaskId, retired: &BTreeSet<&TaskId>) -> bool {
    let mut stack = vec![start];
    let mut seen = BTreeSet::new();
    while let Some(id) = stack.pop() {
        if !seen.insert(id) {
            continue;
        }
        let Some(task) = plan.tasks.iter().find(|t| &t.id == id) else {
            continue;
        };
        for dep in &task.depends_on {
            if retired.contains(dep) {
                return true;
            }
            stack.push(dep);
        }
    }
    false
}

/// Seed a task's runtime as `Pending` (idempotent — keeps any existing entry).
/// Shared by the initial submission and amendment folds.
fn seed_task(tasks: &mut BTreeMap<TaskId, TaskRuntimeState>, id: &TaskId) {
    tasks.entry(id.clone()).or_insert(TaskRuntimeState {
        status: TaskStatus::Pending,
        attempts: 0,
        last_report: None,
    });
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

/// A `bind_oracle` op strengthens (never weakens) iff the target assertion
/// exists and is currently unbound, or re-binds the identical oracle
/// (idempotent). Shared by the fold's defensive re-check and validation so the
/// two can never disagree.
pub(crate) fn bind_strengthens(
    contract: &BTreeMap<AssertionId, AssertionState>,
    assertion: &AssertionId,
    oracle: &OracleName,
) -> bool {
    contract
        .get(assertion)
        .is_some_and(|a| a.oracle.is_none() || a.oracle.as_ref() == Some(oracle))
}

/// Gate status is derived, never an event: a gate whose dependencies are all
/// cleared evaluates its upstream validators (AND semantics). A cleared gate
/// still raises a checkpoint (zenith's discipline — a human confirms before
/// the mission proceeds past it); a failed gate raises `gate_failed`. Both
/// pause the mission until a human decision (`decide … continue`) resolves them.
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
) {
    use super::event::DecisionAction;
    let Some(item) = state.open_attention.get(attention_id).cloned() else {
        return; // unknown or already-resolved item
    };
    // Planning nodes live in `planning.tasks`, execution nodes in `tasks`; a
    // node decision resets whichever era the mission is in.
    let node_status = |state: &mut MissionState, task_id, status| {
        if let Some(task) = era_tasks_mut(state).get_mut(task_id) {
            task.status = status;
        }
    };
    match (action, item.kind) {
        (DecisionAction::Ratify, AttentionKind::Ratify)
        | (DecisionAction::Ratify, AttentionKind::RatifyProposal) => state.ratified = true,
        (DecisionAction::Retry, AttentionKind::RatifyProposal) => {
            // Reject the proposal and re-run the whole planning DAG. Attempts are
            // preserved, so a re-dispatched node gets a fresh idempotency key.
            // Scrub any `request_attention` flags the discarded run left, or a
            // stale node_attention item would re-park the mission and block the
            // re-plan (mirrors the NodeFailed retry scrub below).
            state.proposal = None;
            for (id, task) in &mut state.planning.tasks {
                task.status = TaskStatus::Pending;
                state.flagged_nodes.remove(id);
            }
        }
        (DecisionAction::Retry, AttentionKind::NodeFailed) => {
            if let Some(task_id) = &item.task_id {
                node_status(state, task_id, TaskStatus::Pending);
                state.flagged_nodes.remove(task_id);
            }
        }
        (DecisionAction::Continue, AttentionKind::NodeFailed) => {
            if let Some(task_id) = &item.task_id {
                node_status(state, task_id, TaskStatus::Cleared); // accept the failure
            }
        }
        (DecisionAction::Continue, AttentionKind::NodeAttention) => {
            if let Some(task_id) = &item.task_id {
                state.flagged_nodes.remove(task_id);
            }
        }
        (DecisionAction::Continue, AttentionKind::GateCheckpoint | AttentionKind::GateFailed) => {
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
        (DecisionAction::Continue, AttentionKind::OracleFailed) => {
            // Accept the infra failure: waive the obligation so the mission
            // can finish (never verified — there is no authoritative verdict).
            if let Some(oracle) = &item.oracle {
                state.oracle_failures.remove(oracle);
                state.waived_oracles.insert(oracle.clone());
            }
        }
        (DecisionAction::Abort, _) => {
            state.phase = MissionPhase::Aborted {
                reason: "aborted by decision".to_string(),
            };
        }
        _ => {}
    }
}

/// Rebuild the open-attention set from scratch: the ratification gate, failed
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
                report,
            },
        );
    };

    // Ratification gate: park before any work until the plan is approved.
    if state.plan.is_some() && state.config.ratification_gate && !state.ratified {
        raise(
            AttentionKind::Ratify,
            None,
            None,
            "ratify the plan and contract before work begins".to_string(),
        );
    }

    // Oracle infrastructure failures: park rather than re-request forever.
    for (oracle, detail) in &state.oracle_failures {
        raise(
            AttentionKind::OracleFailed,
            None,
            Some(oracle.clone()),
            format!("oracle '{oracle}' failed to run: {detail}"),
        );
    }

    // Planning phase (no plan yet): surface failed/flagged planning nodes, and
    // the ratify-proposal gate once the author has proposed.
    if state.plan.is_none() {
        for (task_id, rt) in &state.planning.tasks {
            if rt.status == TaskStatus::Failed {
                raise(
                    AttentionKind::NodeFailed,
                    Some(task_id.clone()),
                    None,
                    format!("planning task '{task_id}' failed"),
                );
            } else if state.flagged_nodes.contains(task_id) {
                raise(
                    AttentionKind::NodeAttention,
                    Some(task_id.clone()),
                    None,
                    format!("planning task '{task_id}' asks for a look"),
                );
            }
        }
        if state.proposal.is_some() && !state.ratified {
            raise(
                AttentionKind::RatifyProposal,
                None,
                None,
                "ratify the proposed contract before execution begins".to_string(),
            );
        }
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
                    format!("gate '{}' cleared; confirm to proceed", task.id),
                ),
                Some(TaskStatus::Failed) if !state.acknowledged_gates.contains(&task.id) => raise(
                    AttentionKind::GateFailed,
                    Some(task.id.clone()),
                    None,
                    format!(
                        "gate '{}' is blocked by dissenting or missing verdicts",
                        task.id
                    ),
                ),
                _ => {}
            },
            super::plan::TaskKind::Work | super::plan::TaskKind::Validate => {
                if status == Some(TaskStatus::Failed) {
                    raise(
                        AttentionKind::NodeFailed,
                        Some(task.id.clone()),
                        None,
                        format!("task '{}' failed", task.id),
                    );
                } else if state.flagged_nodes.contains(&task.id) {
                    raise(
                        AttentionKind::NodeAttention,
                        Some(task.id.clone()),
                        None,
                        format!("task '{}' asked for a human look", task.id),
                    );
                }
            }
        }
    }
    state.open_attention = attention;
}

fn track_inflight(state: &mut MissionState, event: &MissionEvent, seq: u64) {
    if let Some((key, effect)) = InflightEffect::from_request(event, seq) {
        state.inflight.insert(key, effect);
    }
}

/// The task map for the mission's current era: planning nodes live in
/// `planning.tasks` until a plan exists, execution nodes in `tasks` after. The
/// two id spaces are disjoint because `plan` goes `None → Some` monotonically —
/// so this one routing decision has a single name, not a copy at every site.
fn era_tasks_mut(state: &mut MissionState) -> &mut BTreeMap<TaskId, TaskRuntimeState> {
    if state.plan.is_none() {
        &mut state.planning.tasks
    } else {
        &mut state.tasks
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
            if let Some(task) = era_tasks_mut(state).get_mut(task_id) {
                task.status = status;
                task.last_report = Some(report.clone());
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
            // it seeds the contract only after ratification (`derive_promotion`).
            let status = if *done {
                TaskStatus::Cleared
            } else {
                TaskStatus::Failed
            };
            if let Some(task) = state.planning.tasks.get_mut(task_id) {
                task.status = status;
                task.last_report = Some(report.clone());
            }
            if *done {
                if let Some(plan) = proposal {
                    if state.plan.is_none() && state.proposal.is_none() {
                        state.proposal = Some(plan.clone());
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
    } else if state.plan.is_none() {
        MissionPhase::Planning
    } else if tasks_active(state)
        || !state.inflight.is_empty()
        || oracle_obligation_outstanding(state)
    {
        MissionPhase::Running
    } else {
        MissionPhase::Done {
            finish: classify_finish(state),
        }
    };
}

fn tasks_active(state: &MissionState) -> bool {
    state
        .tasks
        .values()
        .any(|t| matches!(t.status, TaskStatus::Pending | TaskStatus::Running))
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
                .is_none_or(|v| !v.is_fresh_at(&state.current_sha))
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::super::event::{
        ArtifactOutcome, MissionConfig, PayloadRef, RunErrorKind, Supersession, ValidationItem,
    };
    use super::super::ids::{AssertionId, MissionId, OracleName, RoleName, TaskId};
    use super::super::plan::{Assertion, PlanSubmission, Task, TaskKind};
    use super::super::verdict::FinishClass;
    use super::*;

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
        fold(
            events
                .into_iter()
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
                ratification_gate: false,
                ..Default::default()
            },
        }
    }

    fn plan_submitted(assertions: Vec<Assertion>, tasks: Vec<Task>) -> MissionEvent {
        MissionEvent::PlanSubmitted {
            plan: PlanSubmission { assertions, tasks },
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
            idempotency_key: key.into(),
            role: RoleName::new("implementer").expect("role name"),
            prompt: PayloadRef::inline("prompt"),
            base_sha: "base".into(),
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
            idempotency_key: key.into(),
            handoff,
            artifact,
        }
    }

    fn oracle_requested(assertion_id: &str, judged: &str, key: &str) -> MissionEvent {
        MissionEvent::OracleRunRequested {
            assertion_ids: vec![aid(assertion_id)],
            oracle: oracle("cargo-test"),
            judged_sha: judged.into(),
            attempt_no: 1,
            idempotency_key: key.into(),
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
            idempotency_key: key.into(),
            exit_code,
            exit_signal: None,
            stdout: PayloadRef::inline("out"),
            stderr: PayloadRef::inline("err"),
            duration_ms: 5,
        }
    }

    fn decision(item: &str, action: super::super::event::DecisionAction) -> MissionEvent {
        MissionEvent::DecisionRecorded {
            attention_id: item.into(),
            action,
            justification: "j".into(),
            actor: "test".into(),
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

    fn covering_work(id: &str, target: &str, deps: &[&str]) -> Task {
        Task {
            id: tid(id),
            kind: TaskKind::Work,
            body: "do".into(),
            targets: vec![aid(target)],
            role: Some(RoleName::new("implementer").expect("role")),
            depends_on: deps.iter().map(|d| tid(d)).collect(),
        }
    }

    fn reviewer_of(id: &str, target: &str, deps: &[&str]) -> Task {
        Task {
            id: tid(id),
            kind: TaskKind::Validate,
            body: "check".into(),
            targets: vec![aid(target)],
            role: Some(RoleName::new("reviewer").expect("role")),
            depends_on: deps.iter().map(|d| tid(d)).collect(),
        }
    }

    fn commit(head: &str) -> Option<ArtifactOutcome> {
        Some(ArtifactOutcome {
            base_sha: "base".into(),
            head_sha: head.into(),
        })
    }

    fn plan_amended(base_revision: u32, ops: AmendmentOps) -> MissionEvent {
        MissionEvent::PlanAmended {
            base_revision,
            ops,
            actor: "test".into(),
            justification: "j".into(),
        }
    }

    fn supersede(old: &str, new: &str) -> Supersession {
        Supersession {
            old: tid(old),
            new: tid(new),
        }
    }

    // Honesty (review G1): re-planning ALREADY-VERIFIED work cannot launder —
    // the oracle re-runs at the new head and the grade follows the real tree.
    // The automated stand-in for the removed seal-enforcement test.
    #[test]
    fn superseding_verified_work_re_judges_at_the_new_head() {
        // A is oracle-verified at h1; a pending `keep` holds the mission Running
        // (so the amendment is reachable, exactly as in a multi-node mission).
        let verified = vec![
            created(),
            plan_submitted(
                vec![assertion("AA", Some("cargo-test"))],
                vec![covering_work("wa", "AA", &[]), work_task("keep")],
            ),
            role_completed("wa", "kwa", work_handoff(true, false), commit("h1")),
            oracle_completed("AA", "h1", "koa", 0),
        ];
        let mid = fold_log(verified.clone()).expect("state");
        assert_eq!(mid.phase, MissionPhase::Running, "keep pending → not Done");
        assert_eq!(
            mid.contract[&aid("AA")]
                .last_authoritative
                .as_ref()
                .map(|v| v.passed()),
            Some(true),
            "A is verified at h1"
        );

        // Supersede the verified work with a regression; the oracle re-judges h2.
        let mut events = verified;
        events.extend([
            plan_amended(
                1,
                AmendmentOps {
                    add: vec![covering_work("wa2", "AA", &[])],
                    supersede: vec![supersede("wa", "wa2")],
                    ..Default::default()
                },
            ),
            role_completed("wa2", "kwa2", work_handoff(true, false), commit("h2")),
            role_completed("keep", "kk", work_handoff(true, false), None),
            oracle_completed("AA", "h2", "koa2", 1),
        ]);
        let state = fold_log(events).expect("state");
        assert_eq!(
            state.phase,
            MissionPhase::Done {
                finish: FinishClass::Unverified
            },
            "regressed re-plan of verified work drops to Unverified"
        );
    }

    // Honesty (review #1): superseding a validator's upstream work resets the
    // validator AND re-derives the assertion's sticky advisory — a re-review
    // that now fails must not finish InternallyConsistent on the discarded tree.
    #[test]
    fn re_reviewed_advisory_re_derives_and_cannot_launder() {
        let base = vec![
            created(),
            plan_submitted(
                vec![assertion("STYLE-OK", None)],
                vec![
                    covering_work("w", "STYLE-OK", &[]),
                    reviewer_of("v", "STYLE-OK", &["w"]),
                ],
            ),
            role_completed("w", "kw", work_handoff(true, false), commit("h1")),
            role_completed("v", "kv", validate_handoff(&[("STYLE-OK", true)]), None),
        ];
        assert_eq!(
            fold_log(base.clone()).expect("state").phase,
            MissionPhase::Done {
                finish: FinishClass::InternallyConsistent
            },
            "v passed → advisory-green"
        );

        // Supersede the reviewed work; v re-reviews the new tree as FAILING.
        let mut events = base;
        events.extend([
            plan_amended(
                1,
                AmendmentOps {
                    add: vec![covering_work("w2", "STYLE-OK", &[])],
                    supersede: vec![supersede("w", "w2")],
                    ..Default::default()
                },
            ),
            role_completed("w2", "kw2", work_handoff(true, false), commit("h2")),
            role_completed("v", "kv2", validate_handoff(&[("STYLE-OK", false)]), None),
        ]);
        let state = fold_log(events).expect("state");
        assert_eq!(
            state.contract[&aid("STYLE-OK")].advisory,
            AdvisoryStatus::Failed,
            "sticky advisory re-derived from the failing re-review"
        );
        assert_eq!(
            state.phase,
            MissionPhase::Done {
                finish: FinishClass::Unverified
            },
            "must not finish InternallyConsistent on the rejected tree"
        );
    }

    #[test]
    fn supersede_dedups_downstream_dependencies() {
        // A downstream task depending on both the superseded task and its
        // replacement ends with a single, deduped edge.
        let plan = PlanSubmission {
            assertions: Vec::new(),
            tasks: vec![work_task("a"), work_task("b"), {
                let mut c = work_task("c");
                c.depends_on = vec![tid("a"), tid("b")];
                c
            }],
        };
        let ops = AmendmentOps {
            supersede: vec![supersede("a", "b")],
            ..Default::default()
        };
        let result = resulting_plan(&plan, &ops);
        let c = result.tasks.iter().find(|t| t.id == tid("c")).expect("c");
        assert_eq!(c.depends_on, vec![tid("b")]);
    }

    // Regression (review): a fresh authoritative FAIL must dominate a green
    // advisory verdict — Unverified, never InternallyConsistent.
    #[test]
    fn fresh_oracle_fail_dominates_green_advisory() {
        use super::super::verdict::FinishClass;
        let state = fold_log(vec![
            created(),
            plan_submitted(
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
        assert_eq!(
            state.phase,
            MissionPhase::Done {
                finish: FinishClass::Unverified
            },
            "a real oracle failure is not laundered to internally-consistent"
        );
    }

    // Regression (review): Continue on a GateFailed must let downstream
    // proceed, not wedge the mission in Running forever.
    #[test]
    fn continue_on_failed_gate_unblocks_downstream() {
        let base = vec![
            created(),
            plan_submitted(
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
            super::super::event::DecisionAction::Continue,
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
                    base_sha: state.current_sha.clone(),
                }
            )
        );
    }

    // Regression (review): Continue on an OracleFailed waives the obligation
    // so the mission can finish (unverified) instead of looping forever.
    #[test]
    fn continue_on_oracle_failure_waives_and_finishes() {
        use super::super::verdict::FinishClass;
        let base = vec![
            created(),
            plan_submitted(
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
            MissionEvent::OracleRunFailed {
                assertion_ids: vec![aid("A1")],
                oracle: oracle("cargo-test"),
                judged_sha: "sha-1".into(),
                attempt_no: 1,
                idempotency_key: "ko".into(),
                detail: "binary missing".into(),
                synthesized: false,
            },
        ];
        let parked = fold_log(base.clone()).expect("state");
        assert!(parked
            .open_attention
            .contains_key("oracle_failed:cargo-test"));

        let mut resolved = base;
        resolved.push(decision(
            "oracle_failed:cargo-test",
            super::super::event::DecisionAction::Continue,
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
            plan_submitted(
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
            plan_submitted(vec![], vec![]),
            role_completed("t1", "k1", work_handoff(true, false), None),
            MissionEvent::MissionAborted {
                reason: "stop".into(),
                actor: "human".into(),
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
    fn plan_submitted_initializes_contract_and_tasks() {
        let state = fold_log(vec![
            created(),
            plan_submitted(
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
                plan_submitted(vec![], vec![work_task("t1")]),
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
                plan_submitted(
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
            plan_submitted(vec![assertion("A1", None)], vec![validate_task("v1")]),
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
            plan_submitted(vec![], vec![work_task("t1")]),
            role_requested("t1", "k1"),
            MissionEvent::RoleRunFailed {
                task_id: tid("t1"),
                attempt_no: 1,
                idempotency_key: "k1".into(),
                error_kind: RunErrorKind::Timeout,
                detail: "took too long".into(),
                synthesized: false,
            },
        ])
        .expect("state");
        let task = &state.tasks[&tid("t1")];
        assert_eq!(task.status, TaskStatus::Failed);
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
    fn artifact_outcome_moves_current_sha() {
        let state = fold_log(vec![
            created(),
            plan_submitted(vec![], vec![work_task("t1")]),
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
                plan_submitted(
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
            assert!(mid.inflight.contains_key("ko"), "request is inflight");
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
            // A fresh verdict — pass or fail — settles the obligation.
            assert_eq!(
                state.phase,
                MissionPhase::Done {
                    finish: expect_finish
                },
                "exit {exit_code}"
            );
        }
    }

    // Regression (QA): a signal-killed oracle (clean exit_code 0 but a signal)
    // is NOT a pass — the honesty floor requires no signal. Pins the
    // `exit_signal.is_none()` clause so it can't silently regress.
    #[test]
    fn signal_killed_oracle_is_not_a_pass() {
        let state = fold_log(vec![
            created(),
            plan_submitted(
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
                idempotency_key: "ko".into(),
                exit_code: 0,
                exit_signal: Some(9), // SIGKILL (timeout/OOM) despite exit 0
                stdout: PayloadRef::inline("out"),
                stderr: PayloadRef::inline("err"),
                duration_ms: 5,
            },
        ])
        .expect("state");
        let verdict = state.contract[&aid("TESTS-PASS")]
            .last_authoritative
            .as_ref()
            .expect("verdict minted");
        assert!(!verdict.passed(), "a signal-killed oracle must not pass");
        assert_eq!(
            state.phase,
            MissionPhase::Done {
                finish: FinishClass::Unverified
            }
        );
    }

    #[test]
    fn oracle_run_failed_raises_attention_without_verdict() {
        let state = fold_log(vec![
            created(),
            plan_submitted(vec![assertion("TESTS-PASS", Some("cargo-test"))], vec![]),
            oracle_requested("TESTS-PASS", "base", "ko"),
            MissionEvent::OracleRunFailed {
                assertion_ids: vec![aid("TESTS-PASS")],
                oracle: oracle("cargo-test"),
                judged_sha: "base".into(),
                attempt_no: 1,
                idempotency_key: "ko".into(),
                detail: "spawn failed".into(),
                synthesized: false,
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
            MissionEvent::MissionAborted {
                reason: "operator stop".into(),
                actor: "human".into(),
            },
            plan_submitted(vec![], vec![work_task("t1")]),
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
        assert_eq!(state.head, 3);
    }

    #[test]
    fn attention_ids_are_kind_and_anchor_scoped_and_stable_across_refolds() {
        let events = vec![
            created(),
            plan_submitted(vec![], vec![work_task("t1")]),
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
            plan_submitted(vec![assertion("KNOWN-1", None)], vec![]),
            // Task never declared by any plan.
            role_completed("ghost", "k1", work_handoff(true, false), None),
            // Validator verdict for an assertion the contract never heard of.
            role_completed(
                "phantom",
                "k2",
                validate_handoff(&[("UNKNOWN-1", true)]),
                None,
            ),
            MissionEvent::RoleRunFailed {
                task_id: tid("specter"),
                attempt_no: 1,
                idempotency_key: "k3".into(),
                error_kind: RunErrorKind::Infra,
                detail: "gone".into(),
                synthesized: false,
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
}
