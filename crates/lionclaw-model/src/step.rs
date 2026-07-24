//! Pure team-driven dispatch.

use super::fold::{gap_review_outstanding, oracle_obligation_outstanding};
use super::ids::{AssertionId, OracleName, RoleInstanceId, TaskId};
use super::state::{ConversationLifecycle, MissionPhase, MissionState, ReviewOutcome, TaskStatus};
use crate::prelude::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepDecision {
    Idle,
    Park,
    Terminal,
    DispatchRole(RoleDispatchIntent),
    RunOracles(Vec<OracleDispatchIntent>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleDispatchIntent {
    pub role_instance: RoleInstanceId,
    pub team_revision: u32,
    pub task_id: Option<TaskId>,
    pub output: super::OutputSemantics,
    pub attempt_no: u32,
    pub body: String,
    pub targets: Vec<AssertionId>,
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
        MissionPhase::AttentionNeeded => StepDecision::Park,
        MissionPhase::Done { .. } | MissionPhase::Aborted { .. } => StepDecision::Terminal,
        MissionPhase::Planning => step_planning(state),
        MissionPhase::Running => step_running(state),
    }
}

fn next_attempt(state: &MissionState, role_instance: &RoleInstanceId) -> u32 {
    state
        .role_attempt_receipts
        .values()
        .filter(|receipt| {
            matches!(
                &receipt.source,
                super::RoleEffectSource::Turn { request, .. }
                    if &request.role_instance == role_instance
            )
        })
        .count()
        .saturating_add(1) as u32
}

fn role_intent(
    state: &MissionState,
    role_instance: &RoleInstanceId,
    task_id: Option<TaskId>,
    body: String,
    targets: Vec<AssertionId>,
) -> Option<RoleDispatchIntent> {
    let team = state.team.as_ref()?;
    let role = team.role(role_instance)?;
    Some(RoleDispatchIntent {
        role_instance: role_instance.clone(),
        team_revision: team.revision,
        task_id,
        output: role.output,
        attempt_no: next_attempt(state, role_instance),
        body,
        targets,
        base_sha: state.deliverable_head().to_string(),
    })
}

fn step_planning(state: &MissionState) -> StepDecision {
    if !state.inflight.is_empty() {
        return StepDecision::Idle;
    }
    let Some(team) = state.team.as_ref() else {
        return StepDecision::Idle;
    };
    let planner = &team.planning_assignment;
    if !state.taskless_assignment_dispatchable(planner, &[]) {
        return StepDecision::Idle;
    }
    if state
        .conversations
        .get(planner)
        .is_some_and(|conversation| {
            conversation.lifecycle == ConversationLifecycle::AwaitingLead
                && conversation.queued.is_empty()
        })
    {
        return StepDecision::Idle;
    }
    role_intent(
        state,
        planner,
        None,
        "Propose the complete mission plan.".to_string(),
        Vec::new(),
    )
    .map_or(StepDecision::Idle, StepDecision::DispatchRole)
}

fn step_running(state: &MissionState) -> StepDecision {
    if !state.inflight.is_empty() {
        return StepDecision::Idle;
    }
    let Some(plan) = &state.plan else {
        return StepDecision::Idle;
    };

    for (role_id, conversation) in &state.conversations {
        if conversation.queued.is_empty()
            || !state.conversation_is_messageable(role_id)
            || conversation.lifecycle == ConversationLifecycle::Completed
        {
            continue;
        }
        let Some(team) = state.team.as_ref() else {
            return StepDecision::Idle;
        };
        let task = state.tasks.iter().find_map(|(task_id, task)| {
            (matches!(task.status, TaskStatus::Running | TaskStatus::Failed)
                && (task.status != TaskStatus::Failed
                    || state.task_automatic_retry_remaining(task_id))
                && task.role_assignment.as_ref().is_some_and(|assignment| {
                    assignment.role_instance == *role_id
                        && assignment.team_revision == team.revision
                }))
            .then_some(task_id)
        });
        let (task_id, body, targets) = match task.and_then(|id| {
            plan.tasks
                .iter()
                .find(|task| &task.id == id)
                .map(|task| (id.clone(), task.body.clone(), task.targets.clone()))
        }) {
            Some((id, body, targets)) => (Some(id), body, targets),
            None => (None, "Continue the conversation.".to_string(), Vec::new()),
        };
        if let Some(intent) = role_intent(state, role_id, task_id, body, targets) {
            return StepDecision::DispatchRole(intent);
        }
    }

    let status_of = |id: &TaskId| state.tasks.get(id).map(|task| task.status);
    for task in &plan.tasks {
        let runnable = matches!(
            status_of(&task.id),
            Some(TaskStatus::Pending) | Some(TaskStatus::Failed)
        ) && task
            .depends_on
            .iter()
            .all(|dependency| status_of(dependency) == Some(TaskStatus::Cleared));
        if !runnable {
            continue;
        }
        let Some(team) = state.team.as_ref() else {
            return StepDecision::Idle;
        };
        let Some(role_id) = team.task_assignments.get(&task.id) else {
            return StepDecision::Idle;
        };
        if let Some(intent) = role_intent(
            state,
            role_id,
            Some(task.id.clone()),
            task.body.clone(),
            task.targets.clone(),
        ) {
            return StepDecision::DispatchRole(intent);
        }
    }

    let Some(team) = state.team.as_ref() else {
        return StepDecision::Idle;
    };
    for (assertion_id, panel) in &team.judgment_assignments {
        for role_id in panel {
            let already_settled = state
                .contract
                .get(assertion_id)
                .and_then(|assertion| assertion.last_advisory.get(role_id))
                .and_then(|effect| state.advisory_receipt(assertion_id, role_id, effect))
                .is_some();
            if !already_settled
                && state
                    .taskless_assignment_dispatchable(role_id, core::slice::from_ref(assertion_id))
            {
                if let Some(intent) = role_intent(
                    state,
                    role_id,
                    None,
                    "Judge the assigned assertions.".to_string(),
                    vec![assertion_id.clone()],
                ) {
                    return StepDecision::DispatchRole(intent);
                }
            }
        }
    }

    if oracle_obligation_outstanding(state) {
        let mut by_oracle: BTreeMap<OracleName, Vec<AssertionId>> = BTreeMap::new();
        for assertion in state.contract.values() {
            let Some(oracle) = &assertion.oracle else {
                continue;
            };
            if !state.oracle_dispatchable(oracle) || by_oracle.contains_key(oracle) {
                continue;
            }
            let owed = state.owed_assertions_for_oracle(oracle);
            if !owed.is_empty() {
                by_oracle.insert(oracle.clone(), owed);
            }
        }
        return StepDecision::RunOracles(
            by_oracle
                .into_iter()
                .map(|(oracle, assertion_ids)| OracleDispatchIntent {
                    attempt_no: state.oracle_attempts.get(&oracle).copied().unwrap_or(0) + 1,
                    oracle,
                    assertion_ids,
                    judged_sha: state.deliverable_head().to_string(),
                })
                .collect(),
        );
    }

    if gap_review_outstanding(state) {
        let retryable = match &state.gap_review.outcome {
            Some(ReviewOutcome::Failed { effect_id }) => state
                .role_attempt_receipts
                .get(effect_id)
                .and_then(super::RoleAttemptReceipt::failure)
                .is_some_and(|failure| {
                    failure.automatically_retryable()
                        && state.gap_review.consecutive_failures
                            < state.config.recovery.max_attempts
                }),
            Some(ReviewOutcome::Verdict { .. }) | None => true,
        };
        if retryable {
            if let Some(role_id) = &team.gap_review_assignment {
                if let Some(intent) = role_intent(
                    state,
                    role_id,
                    None,
                    "Review the delivered product against the objective.".to_string(),
                    Vec::new(),
                ) {
                    return StepDecision::DispatchRole(intent);
                }
            }
        }
    }

    StepDecision::Idle
}
