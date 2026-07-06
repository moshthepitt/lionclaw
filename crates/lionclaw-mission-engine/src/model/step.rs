//! The drive-loop decision function, ported from Zenith (Apache-2.0,
//! Intelligent Internet) `coordinator.py` (`step`, `_step_mission`,
//! `_all_runnable_tasks`) onto the event-sourced core: `step()` is pure and
//! returns dispatch *intents*; the engine shell materializes them into
//! `…Requested` events (prompt assembly and blob writes are I/O and live in
//! the shell).
//!
//! Deliberate divergences from zenith, both consequences of enforcing the
//! isolation zenith disabled:
//! - **Writers serialize.** Each artifact-producing run gets its own
//!   worktree stacked on the previous artifact commit, so at most one work
//!   task dispatches at a time. Validators and oracles parallelize freely.
//! - **Auto-close.** There is no interactive orchestrator process to call
//!   `end_mission`; when nothing is runnable, inflight, or owed, the phase
//!   derivation closes the mission. Attention parks keep the human pauses.

use std::collections::BTreeMap;

use super::fold::oracle_obligation_outstanding;
use super::ids::{AssertionId, OracleName, RoleName, TaskId};
use super::plan::TaskKind;
use super::state::{InflightEffect, MissionPhase, MissionState, TaskStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepDecision {
    /// Nothing for the loop to start (waiting on inflight effects, or on a
    /// plan submission).
    Idle,
    /// Open attention — park at zero compute (durable interrupt).
    Park,
    /// Terminal phase; nothing will ever run again.
    Terminal,
    /// Dispatch one artifact-producing role run (writers serialize).
    DispatchRole(RoleDispatchIntent),
    /// Run engine oracles (parallelizable).
    RunOracles(Vec<OracleDispatchIntent>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleDispatchIntent {
    pub task_id: TaskId,
    pub role: RoleName,
    pub attempt_no: u32,
    pub body: String,
    pub targets: Vec<AssertionId>,
    /// Commit the role's workspace is created at.
    pub base_sha: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleDispatchIntent {
    pub oracle: OracleName,
    pub assertion_ids: Vec<AssertionId>,
    pub judged_sha: String,
    pub attempt_no: u32,
}

pub fn step(state: &MissionState) -> StepDecision {
    match &state.phase {
        MissionPhase::Planning => StepDecision::Idle,
        MissionPhase::AttentionNeeded => StepDecision::Park,
        MissionPhase::Done { .. } | MissionPhase::Aborted { .. } => StepDecision::Terminal,
        MissionPhase::Running => step_running(state),
    }
}

fn step_running(state: &MissionState) -> StepDecision {
    // Effects already requested own the turn until their outcomes fold in.
    if !state.inflight.is_empty() {
        return StepDecision::Idle;
    }
    let Some(plan) = &state.plan else {
        return StepDecision::Idle;
    };

    // A running task without an inflight effect can only mean an outcome is
    // about to be reconciled; never double-dispatch.
    let any_running = state
        .tasks
        .values()
        .any(|t| t.status == TaskStatus::Running);
    if any_running {
        return StepDecision::Idle;
    }

    // Runnable = non-gate, pending, all deps cleared — in plan order
    // (zenith `_all_runnable_tasks`; list order is the topo tie-break).
    let status_of = |id: &TaskId| state.tasks.get(id).map(|t| t.status);
    let mut runnable_work = plan.tasks.iter().filter(|task| {
        task.kind == TaskKind::Work
            && status_of(&task.id) == Some(TaskStatus::Pending)
            && task
                .depends_on
                .iter()
                .all(|dep| status_of(dep) == Some(TaskStatus::Cleared))
    });
    if let Some(task) = runnable_work.next() {
        let attempt_no = state.tasks.get(&task.id).map_or(0, |t| t.attempts) + 1;
        return StepDecision::DispatchRole(RoleDispatchIntent {
            task_id: task.id.clone(),
            role: task
                .role
                .clone()
                .expect("plan validation guarantees work tasks carry a role"),
            attempt_no,
            body: task.body.clone(),
            targets: task.targets.clone(),
            base_sha: state.current_sha.clone(),
        });
    }

    // No work left to start: settle oracle obligations against the current
    // artifact commit, batched per oracle.
    if oracle_obligation_outstanding(state) {
        let mut by_oracle: BTreeMap<OracleName, Vec<AssertionId>> = BTreeMap::new();
        for (id, assertion) in &state.contract {
            let Some(oracle) = &assertion.oracle else {
                continue;
            };
            let fresh = assertion
                .last_authoritative
                .as_ref()
                .is_some_and(|v| v.judged_sha() == state.current_sha);
            if !fresh {
                by_oracle.entry(oracle.clone()).or_default().push(id.clone());
            }
        }
        let intents = by_oracle
            .into_iter()
            .map(|(oracle, assertion_ids)| {
                let attempt_no = state.oracle_attempts.get(&oracle).copied().unwrap_or(0) + 1;
                OracleDispatchIntent {
                    oracle,
                    assertion_ids,
                    judged_sha: state.current_sha.clone(),
                    attempt_no,
                }
            })
            .collect();
        return StepDecision::RunOracles(intents);
    }

    // Phase derivation would have closed the mission if nothing were owed;
    // reaching here means an outcome is still folding in.
    StepDecision::Idle
}

/// Reconcile targets on resume: inflight `…Requested` entries whose outcome
/// was never recorded (zenith `_reconcile_pending_attempts`). Pure selector;
/// the shell probes the world and appends real or synthesized outcomes.
pub fn unreconciled(state: &MissionState) -> Vec<(&String, &InflightEffect)> {
    state.inflight.iter().collect()
}
