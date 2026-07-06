//! The pure fold: `state = fold(log)`. No I/O, no clock, no RNG — enforced
//! by the model's dependency wall and the crate clippy config. Every
//! engine-deterministic transition happens here; the phase (including the
//! finish class) is re-derived after every event, never stored.
//!
//! Handoff application semantics ported from Zenith (Apache-2.0,
//! Intelligent Internet) `coordinator.py::_apply_handoff_collect`: a work
//! task that isn't `done` fails and raises attention; a validate task always
//! clears and folds its per-assertion verdicts in with sticky passes.

use super::event::{EventEnvelope, Handoff, MissionEvent};
use super::state::{
    AdvisoryStatus, AssertionState, AttentionItem, AttentionKind, InflightEffect, MissionPhase,
    MissionState, TaskRuntimeState, TaskStatus,
};
use super::verdict::{classify_finish, AuthoritativeVerdict};

/// Bump when fold semantics change; snapshots with a different version are
/// discarded and rebuilt from sequence zero.
pub const REDUCER_VERSION: u32 = 1;

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
        plugin_name,
        workspace_dir,
        base_sha,
        config,
    } = &envelope.event
    else {
        return None;
    };
    Some(MissionState {
        mission_id: envelope.mission_id.clone(),
        objective: objective.clone(),
        plugin_name: plugin_name.clone(),
        workspace_dir: workspace_dir.clone(),
        base_sha: base_sha.clone(),
        config: config.clone(),
        phase: MissionPhase::Planning,
        plan: None,
        contract: Default::default(),
        tasks: Default::default(),
        current_sha: base_sha.clone(),
        oracle_attempts: Default::default(),
        terminal_review_attempts: 0,
        terminal_review_done: None,
        inflight: Default::default(),
        open_attention: Default::default(),
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
                state
                    .contract
                    .entry(assertion.id.clone())
                    .or_insert_with(|| AssertionState {
                        oracle: assertion.oracle.clone(),
                        advisory: AdvisoryStatus::Pending,
                        last_advisory: Default::default(),
                        last_authoritative: None,
                    });
            }
            for task in &plan.tasks {
                state
                    .tasks
                    .entry(task.id.clone())
                    .or_insert(TaskRuntimeState {
                        status: TaskStatus::Pending,
                        attempts: 0,
                    });
            }
            state.plan = Some(plan.clone());
        }
        MissionEvent::RoleRunRequested {
            task_id, attempt_no, ..
        } => {
            let task = state.tasks.entry(task_id.clone()).or_insert(TaskRuntimeState {
                status: TaskStatus::Pending,
                attempts: 0,
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
            apply_handoff(state, task_id, handoff, seq);
        }
        MissionEvent::RoleRunFailed {
            task_id,
            idempotency_key,
            error_kind,
            detail,
            ..
        } => {
            state.inflight.remove(idempotency_key);
            if let Some(task) = state.tasks.get_mut(task_id) {
                task.status = TaskStatus::Failed;
            }
            push_attention(
                state,
                AttentionKind::NodeFailed,
                Some(task_id.clone()),
                format!("role run failed ({error_kind:?}): {detail}"),
                seq,
            );
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
            let verdict = AuthoritativeVerdict::from_oracle_outcome(
                oracle.clone(),
                judged_sha.clone(),
                *exit_code,
                *exit_signal,
                stdout.clone(),
                stderr.clone(),
                seq,
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
            push_attention(
                state,
                AttentionKind::NodeFailed,
                None,
                format!("oracle '{oracle}' failed to run: {detail}"),
                seq,
            );
        }
        MissionEvent::TerminalReviewRequested { attempt_no, .. } => {
            state.terminal_review_attempts = *attempt_no;
            track_inflight(state, &envelope.event, seq);
        }
        MissionEvent::TerminalReviewCompleted {
            idempotency_key,
            done,
            ..
        } => {
            state.inflight.remove(idempotency_key);
            state.terminal_review_done = Some(*done);
        }
        MissionEvent::MissionAborted { reason, .. } => {
            state.phase = MissionPhase::Aborted {
                reason: reason.clone(),
            };
        }
    }
    state.head = seq;
    derive_phase(state);
}

fn track_inflight(state: &mut MissionState, event: &MissionEvent, seq: u64) {
    if let Some((key, effect)) = InflightEffect::from_request(event, seq) {
        state.inflight.insert(key, effect);
    }
}

fn apply_handoff(
    state: &mut MissionState,
    task_id: &super::ids::TaskId,
    handoff: &Handoff,
    seq: u64,
) {
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
            if let Some(task) = state.tasks.get_mut(task_id) {
                task.status = status;
            }
            if !*done {
                push_attention(
                    state,
                    AttentionKind::NodeFailed,
                    Some(task_id.clone()),
                    format!("work task reported not done: {}", summarize(report)),
                    seq,
                );
            } else if *request_attention {
                push_attention(
                    state,
                    AttentionKind::NodeAttention,
                    Some(task_id.clone()),
                    summarize(report),
                    seq,
                );
            }
        }
        Handoff::Validate {
            items,
            request_attention,
            report,
            ..
        } => {
            // Validators always clear — they ran; their verdicts are data.
            if let Some(task) = state.tasks.get_mut(task_id) {
                task.status = TaskStatus::Cleared;
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
                push_attention(
                    state,
                    AttentionKind::NodeAttention,
                    Some(task_id.clone()),
                    summarize(report),
                    seq,
                );
            }
        }
    }
}

fn summarize(report: &super::event::PayloadRef) -> String {
    match report {
        super::event::PayloadRef::Inline { text } => text.clone(),
        super::event::PayloadRef::Blob(blob) => format!("(report blob {})", blob.hex),
    }
}

fn push_attention(
    state: &mut MissionState,
    kind: AttentionKind,
    task_id: Option<super::ids::TaskId>,
    report: String,
    seq: u64,
) {
    let anchor = task_id
        .as_ref()
        .map(|id| id.to_string())
        .unwrap_or_else(|| "engine".to_string());
    let id = format!("{kind:?}:{anchor}:{seq}").to_lowercase();
    state
        .open_attention
        .insert(id.clone(), AttentionItem { id, kind, task_id, report });
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
        assertion.oracle.is_some()
            && assertion
                .last_authoritative
                .as_ref()
                .is_none_or(|v| v.judged_sha() != state.current_sha)
    })
}
