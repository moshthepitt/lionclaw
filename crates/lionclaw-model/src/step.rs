//! Pure team-driven dispatch.

use super::fold::gap_review_outstanding;
use super::ids::{AssertionId, OracleName, RoleInstanceId, TaskId};
use super::state::{
    ConversationLifecycle, DeliveryMarker, InflightEffect, MissionPhase, MissionState,
    ReviewOutcome, TaskStatus,
};
use super::verdict::{proof_readiness, ProofReadiness, ProofSource};
use crate::prelude::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepDecision {
    Idle,
    Park,
    Terminal,
    DispatchRole(RoleDispatchIntent),
    DispatchRoles(Vec<RoleDispatchIntent>),
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
    pub dependency_refs: Vec<super::TaskCandidateRef>,
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
    base_sha: String,
    dependency_refs: Vec<super::TaskCandidateRef>,
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
        base_sha,
        dependency_refs,
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
        state.deliverable_head().to_string(),
        Vec::new(),
    )
    .map_or(StepDecision::Idle, StepDecision::DispatchRole)
}

fn step_running(state: &MissionState) -> StepDecision {
    let Some(plan) = &state.plan else {
        return StepDecision::Idle;
    };
    let capacity = state.config.execution.effect_capacity as usize;
    let remaining_capacity = capacity.saturating_sub(state.inflight.len());
    if remaining_capacity == 0 {
        return StepDecision::Idle;
    }

    if state.inflight.is_empty() {
        for (role_id, conversation) in &state.conversations {
            if !conversation
                .queued
                .iter()
                .any(|message| message.marker != DeliveryMarker::Undeliverable)
                || !state.conversation_is_messageable(role_id)
                || conversation.lifecycle == ConversationLifecycle::Completed
            {
                continue;
            }
            let Some(team) = state.team.as_ref() else {
                return StepDecision::Idle;
            };
            let task = state.tasks.iter().find_map(|(task_id, task)| {
                (matches!(
                    task.status,
                    TaskStatus::Pending | TaskStatus::Running | TaskStatus::Failed
                ) && (task.status != TaskStatus::Failed
                    || state.task_automatic_retry_remaining(task_id))
                    && task.role_assignment.as_ref().is_some_and(|assignment| {
                        assignment.role_instance == *role_id
                            && assignment.team_revision == team.revision
                    }))
                .then_some(task_id)
            });
            let (task_id, body, targets, base_sha, dependency_refs) = match task.and_then(|id| {
                let task = plan.tasks.iter().find(|task| &task.id == id)?;
                let base_sha = state.task_required_base(id)?;
                let dependency_refs = state.task_dependency_refs(id)?;
                Some((
                    id.clone(),
                    task.body.clone(),
                    task.targets.clone(),
                    base_sha,
                    dependency_refs,
                ))
            }) {
                Some((id, body, targets, base_sha, dependency_refs)) => {
                    (Some(id), body, targets, base_sha, dependency_refs)
                }
                None => (
                    None,
                    "Continue the conversation.".to_string(),
                    Vec::new(),
                    state.deliverable_head().to_string(),
                    Vec::new(),
                ),
            };
            if let Some(intent) = role_intent(
                state,
                role_id,
                task_id,
                body,
                targets,
                base_sha,
                dependency_refs,
            ) {
                return StepDecision::DispatchRole(intent);
            }
        }
    }

    let status_of = |id: &TaskId| state.tasks.get(id).map(|task| task.status);
    let active_roles: BTreeSet<_> = state
        .inflight
        .values()
        .filter_map(|effect| match effect {
            InflightEffect::RoleTurn { role_instance, .. } => Some(role_instance.clone()),
            InflightEffect::OracleRun { .. } => None,
        })
        .collect();
    let active_tasks: BTreeSet<_> = state
        .inflight
        .values()
        .filter_map(|effect| match effect {
            InflightEffect::RoleTurn {
                task_id: Some(task_id),
                ..
            } => Some(task_id.clone()),
            InflightEffect::RoleTurn { task_id: None, .. } | InflightEffect::OracleRun { .. } => {
                None
            }
        })
        .collect();
    let mut reserved_roles = active_roles;
    let mut reserved_tasks = active_tasks;
    let mut intents = Vec::new();
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
        if reserved_tasks.contains(&task.id) || reserved_roles.contains(role_id) {
            continue;
        }
        let Some(base_sha) = state.task_required_base(&task.id) else {
            continue;
        };
        let Some(dependency_refs) = state.task_dependency_refs(&task.id) else {
            continue;
        };
        if let Some(intent) = role_intent(
            state,
            role_id,
            Some(task.id.clone()),
            task.body.clone(),
            task.targets.clone(),
            base_sha,
            dependency_refs,
        ) {
            reserved_tasks.insert(task.id.clone());
            reserved_roles.insert(role_id.clone());
            intents.push(intent);
            if intents.len() == remaining_capacity {
                break;
            }
        }
    }
    match intents.len() {
        0 => {}
        1 => return StepDecision::DispatchRole(intents.remove(0)),
        _ => return StepDecision::DispatchRoles(intents),
    }
    if plan
        .tasks
        .iter()
        .any(|task| status_of(&task.id) != Some(TaskStatus::Cleared))
    {
        return StepDecision::Idle;
    }

    let Some(team) = state.team.as_ref() else {
        return StepDecision::Idle;
    };
    let pending_proof = match proof_readiness(state) {
        ProofReadiness::Pending(sources) => sources,
        ProofReadiness::Failed(_) => return StepDecision::Idle,
        ProofReadiness::Satisfied(_) => Vec::new(),
    };
    let proof_pending = !pending_proof.is_empty();
    let mut judgment_intents = Vec::new();
    let mut reserved_judges = reserved_roles;
    for source in &pending_proof {
        let ProofSource::Judgment {
            role_instance,
            assertion_ids,
        } = source
        else {
            continue;
        };
        if reserved_judges.contains(role_instance)
            || !state.taskless_assignment_dispatchable(role_instance, assertion_ids)
        {
            continue;
        }
        if let Some(intent) = role_intent(
            state,
            role_instance,
            None,
            "Judge the assigned assertions.".to_string(),
            assertion_ids.clone(),
            state.deliverable_head().to_string(),
            Vec::new(),
        ) {
            reserved_judges.insert(role_instance.clone());
            judgment_intents.push(intent);
            if judgment_intents.len() == remaining_capacity {
                break;
            }
        }
    }
    match judgment_intents.len() {
        0 => {}
        1 => return StepDecision::DispatchRole(judgment_intents.remove(0)),
        _ => return StepDecision::DispatchRoles(judgment_intents),
    }

    if !state.inflight.is_empty() {
        return StepDecision::Idle;
    }

    let mut by_oracle: BTreeMap<OracleName, Vec<AssertionId>> = BTreeMap::new();
    for source in pending_proof {
        let ProofSource::Command {
            oracle,
            assertion_ids,
        } = source
        else {
            continue;
        };
        if state.oracle_dispatchable(&oracle) {
            by_oracle.insert(oracle, assertion_ids);
            if by_oracle.len() == remaining_capacity {
                break;
            }
        }
    }
    if !by_oracle.is_empty() {
        let intents = by_oracle
            .into_iter()
            .filter_map(|(oracle, assertion_ids)| {
                Some(OracleDispatchIntent {
                    attempt_no: state.next_oracle_attempt(&oracle)?,
                    oracle,
                    assertion_ids,
                    judged_sha: state.deliverable_head().to_string(),
                })
            })
            .collect::<Vec<_>>();
        if !intents.is_empty() {
            return StepDecision::RunOracles(intents);
        }
    }

    if proof_pending {
        return StepDecision::Idle;
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
                    state.deliverable_head().to_string(),
                    Vec::new(),
                ) {
                    return StepDecision::DispatchRole(intent);
                }
            }
        }
    }

    StepDecision::Idle
}
