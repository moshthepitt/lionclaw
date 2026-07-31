//! Pure mission workflow projection.
//!
//! The event fold stores durable facts. This module derives the complete live
//! state machine from those facts: what the kernel may run and which operator
//! choices are legal.

use serde::Serialize;

use super::ids::{AssertionId, EffectId, OracleName, RoleInstanceId, TaskId};
use super::state::{
    ConversationLifecycle, DeliveryMarker, InflightEffect, MissionState, TaskStatus, TerminalState,
};
use super::verdict::{proof_readiness, FinishClass, ProofFailure, ProofReadiness, ProofSource};
use crate::prelude::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Next {
    pub effects: Vec<EffectIntent>,
    pub choices: Vec<Choice>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EffectIntent {
    /// Resolve one already-materialized request under current driver ownership.
    /// A live driver completes it; a later driver recovers it without replay.
    ResolveEffect {
        effect_id: EffectId,
    },
    /// Remove disposable scratch after one exact role attempt settles.
    CleanupConversation {
        role_instance: RoleInstanceId,
        effect_id: EffectId,
    },
    DispatchRole(RoleDispatchIntent),
    DispatchOracle(OracleDispatchIntent),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub report_refs: Vec<super::ReportEvidenceRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OracleDispatchIntent {
    pub oracle: OracleName,
    pub spec_digest: String,
    pub assertion_ids: Vec<AssertionId>,
    pub judged_sha: String,
    pub attempt_no: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Choice {
    ProposePlan {
        base_revision: u32,
    },
    ConfigureTeam {
        revision: u32,
    },
    AddMissionSkill,
    AssignEnvironment {
        team_revision: u32,
    },
    Decide {
        id: String,
        action: super::DecisionAction,
    },
    SendMessage {
        role_instance: RoleInstanceId,
    },
    Stop {
        effect_id: EffectId,
    },
    ExtendDeadline {
        effect_id: EffectId,
        old_deadline_ms: i64,
    },
    Continue {
        effect_id: EffectId,
        mode: super::ContinueMode,
    },
    Finish {
        finish: FinishClass,
    },
    Abort,
    Apply {
        branch: String,
        sha: String,
    },
}

impl Choice {
    /// Whether this exact operator choice authorizes the requested control.
    /// Automatic controls are engine-owned and never match a `Choice`.
    pub fn authorizes_control(&self, effect_id: &EffectId, action: &super::ControlAction) -> bool {
        match (self, action) {
            (Self::Stop { effect_id: legal }, super::ControlAction::Stop) => legal == effect_id,
            (
                Self::ExtendDeadline {
                    effect_id: legal,
                    old_deadline_ms: legal_deadline,
                },
                super::ControlAction::ExtendDeadline {
                    old_deadline_ms,
                    automatic: false,
                    ..
                },
            ) => legal == effect_id && legal_deadline == old_deadline_ms,
            (
                Self::Continue {
                    effect_id: legal,
                    mode: legal_mode,
                },
                super::ControlAction::Continue {
                    automatic: false,
                    mode,
                },
            ) => legal == effect_id && legal_mode == mode,
            _ => false,
        }
    }
}

pub fn next(state: &MissionState) -> Next {
    let cleanup = conversation_cleanup_intents(state);
    if !cleanup.is_empty() {
        let mut choices = active_control_choices(state);
        if state.terminal.is_none() {
            choices.push(Choice::Abort);
        }
        return Next {
            effects: cleanup,
            choices,
        };
    }
    if state.terminal.is_some() {
        let effects = active_effects(state);
        let choices = terminal_choices(state);
        return Next { effects, choices };
    }

    let mut choices = Vec::new();
    let effects = active_effects(state);
    if !effects.is_empty() {
        choices.extend(active_control_choices(state));
        return complete_nonterminal(state, effects, choices);
    }

    let checkpoints = checkpoint_choices(state);
    choices.extend(checkpoints);

    let proposal = proposal_choices(state);
    if !proposal.is_empty() {
        choices.extend(proposal);
        return complete_nonterminal(state, Vec::new(), choices);
    }

    let failures = failure_choices(state);
    if !failures.is_empty() {
        choices.extend(failures);
        return complete_nonterminal(state, Vec::new(), choices);
    }

    let effects = dispatch_intents(state);
    if !effects.is_empty() {
        return complete_nonterminal(state, effects, choices);
    }

    if let Some(finish) = finish_class(state) {
        choices.push(Choice::Finish { finish });
        choices.push(Choice::Decide {
            id: "mission".to_string(),
            action: super::DecisionAction::Revise,
        });
    }
    complete_nonterminal(state, effects, choices)
}

fn complete_nonterminal(
    state: &MissionState,
    effects: Vec<EffectIntent>,
    mut choices: Vec<Choice>,
) -> Next {
    choices.extend(
        state
            .conversations
            .keys()
            .filter(|role_instance| state.conversation_accepts_message(role_instance))
            .cloned()
            .map(|role_instance| Choice::SendMessage { role_instance }),
    );
    if state.inflight.is_empty() && !proposal_awaits_decision(state) {
        choices.extend(administrative_choices(state));
    }
    choices.push(Choice::Abort);
    Next { effects, choices }
}

fn active_effects(state: &MissionState) -> Vec<EffectIntent> {
    state
        .inflight
        .keys()
        .cloned()
        .map(|effect_id| EffectIntent::ResolveEffect { effect_id })
        .collect()
}

fn conversation_cleanup_intents(state: &MissionState) -> Vec<EffectIntent> {
    state
        .conversations
        .iter()
        .filter(|(role_instance, conversation)| {
            matches!(
                conversation.lifecycle,
                ConversationLifecycle::Completed | ConversationLifecycle::Retired
            ) && !state.inflight.values().any(|effect| {
                matches!(
                    effect,
                    InflightEffect::RoleTurn {
                        role_instance: active,
                        ..
                    } if active == *role_instance
                )
            })
        })
        .filter_map(|(role_instance, conversation)| {
            conversation
                .disposable_resource_owner
                .clone()
                .map(|effect_id| EffectIntent::CleanupConversation {
                    role_instance: role_instance.clone(),
                    effect_id,
                })
        })
        .collect()
}

fn administrative_choices(state: &MissionState) -> Vec<Choice> {
    let mut choices = vec![
        Choice::ProposePlan {
            base_revision: state.revision,
        },
        Choice::ConfigureTeam {
            revision: state
                .team
                .as_ref()
                .map_or(0, |team| team.revision.saturating_add(1)),
        },
        Choice::AddMissionSkill,
    ];
    if let Some(team) = &state.team {
        choices.push(Choice::AssignEnvironment {
            team_revision: team.revision,
        });
    }
    choices
}

fn active_control_choices(state: &MissionState) -> Vec<Choice> {
    if state.cleanup_failure.is_some() || state.terminal.is_some() {
        return Vec::new();
    }
    let mut choices = Vec::new();
    for (effect_id, effect) in &state.inflight {
        if state.reached_deadlines.contains_key(effect_id) {
            continue;
        }
        choices.push(Choice::Stop {
            effect_id: effect_id.clone(),
        });
        if effect.budget_deadline_ms().is_some() {
            choices.push(Choice::ExtendDeadline {
                effect_id: effect_id.clone(),
                old_deadline_ms: effect.deadline_ms(),
            });
        }
    }
    choices
}

fn checkpoint_choices(state: &MissionState) -> Vec<Choice> {
    if state.cleanup_failure.is_some() {
        return Vec::new();
    }
    let mut choices = Vec::new();
    for effect_id in state.parked_effects.keys() {
        if state.parked_continue_is_legal(effect_id, super::ContinueMode::Preserve) {
            choices.push(Choice::Continue {
                effect_id: effect_id.clone(),
                mode: super::ContinueMode::Preserve,
            });
        }
        if state.parked_continue_is_legal(effect_id, super::ContinueMode::RecreateWorkspace) {
            choices.push(Choice::Continue {
                effect_id: effect_id.clone(),
                mode: super::ContinueMode::RecreateWorkspace,
            });
        }
    }
    choices
}

fn terminal_choices(state: &MissionState) -> Vec<Choice> {
    match state.terminal {
        Some(TerminalState::Done { .. })
            if state.inflight.is_empty()
                && state.applied_result.is_none()
                && state.deliverable_head() != state.base_sha =>
        {
            vec![Choice::Apply {
                branch: format!("lionclaw/{}", state.mission_id),
                sha: state.deliverable_head().to_string(),
            }]
        }
        Some(TerminalState::Done { .. } | TerminalState::Aborted { .. }) | None => Vec::new(),
    }
}

fn push_decisions(
    choices: &mut Vec<Choice>,
    id: String,
    actions: impl IntoIterator<Item = super::DecisionAction>,
) {
    choices.extend(actions.into_iter().map(|action| Choice::Decide {
        id: id.clone(),
        action,
    }));
}

fn proposal_choices(state: &MissionState) -> Vec<Choice> {
    if !proposal_awaits_decision(state) {
        return Vec::new();
    }
    let mut choices = Vec::new();
    push_decisions(
        &mut choices,
        "plan_proposal:mission".to_string(),
        [
            super::DecisionAction::Approve,
            super::DecisionAction::Revise,
        ],
    );
    choices
}

fn proposal_awaits_decision(state: &MissionState) -> bool {
    state.proposal.is_some()
}

pub(crate) fn task_failure_id(task_id: &TaskId) -> String {
    format!("node_failed:{task_id}")
}

pub(crate) fn role_failure_id(
    role_instance: &RoleInstanceId,
    assertion_ids: &[AssertionId],
) -> String {
    let scope = assertion_ids.first().map_or_else(
        || role_instance.to_string(),
        |id| format!("{role_instance}:{id}"),
    );
    format!("node_failed:{scope}")
}

pub(crate) fn oracle_failure_id(oracle: &OracleName) -> String {
    format!("oracle_failed:{oracle}")
}

pub(crate) fn proof_failure_id(failure: &ProofFailure) -> String {
    match failure {
        ProofFailure::Receipt {
            source: ProofSource::Command { oracle, .. },
            ..
        } => format!("proof_failed:oracle:{oracle}"),
        ProofFailure::Receipt {
            source:
                ProofSource::Judgment {
                    role_instance,
                    assertion_ids,
                },
            ..
        } => {
            let anchor = assertion_ids
                .first()
                .map_or_else(|| role_instance.to_string(), ToString::to_string);
            format!("proof_failed:judgment:{role_instance}:{anchor}")
        }
        ProofFailure::Receipt {
            source: ProofSource::Review { role_instance },
            ..
        } => format!("proof_failed:review:{role_instance}"),
        ProofFailure::StopBar { .. } => "proof_failed:mission".to_string(),
    }
}

fn task_accept_is_legal(state: &MissionState, task_id: &TaskId) -> bool {
    state
        .tasks
        .get(task_id)
        .and_then(|task| task.candidate_sha.as_ref())
        .is_some()
        || state
            .task_dependency_refs(task_id)
            .is_some_and(|dependencies| dependencies.len() <= 1)
}

fn failure_choices(state: &MissionState) -> Vec<Choice> {
    let mut choices = Vec::new();
    if proposal_awaits_decision(state) {
        return choices;
    }
    for (task_id, task) in &state.tasks {
        if task.status == TaskStatus::Failed && !state.task_automatic_retry_remaining(task_id) {
            let mut actions = vec![super::DecisionAction::Retry, super::DecisionAction::Revise];
            if task_accept_is_legal(state, task_id) {
                actions.push(super::DecisionAction::Accept);
            }
            push_decisions(&mut choices, task_failure_id(task_id), actions);
        }
    }
    if let Some(team) = &state.team {
        let mut assignments = vec![(team.planning_assignment.clone(), Vec::new())];
        for (assertion_id, panel) in &team.judgment_assignments {
            for role_instance in panel {
                assignments.push((role_instance.clone(), vec![assertion_id.clone()]));
            }
        }
        for (role_instance, assertion_ids) in assignments {
            let Some((_effect_id, failure, consecutive)) =
                state.taskless_assignment_failure(&role_instance, &assertion_ids)
            else {
                continue;
            };
            if failure.automatically_retryable() && consecutive < state.config.recovery.max_attempts
            {
                continue;
            }
            push_decisions(
                &mut choices,
                role_failure_id(&role_instance, &assertion_ids),
                [super::DecisionAction::Retry, super::DecisionAction::Revise],
            );
        }
    }
    for oracle in state.oracle_failures.keys() {
        if !state.oracle_automatic_retry_remaining(oracle) {
            push_decisions(
                &mut choices,
                oracle_failure_id(oracle),
                [super::DecisionAction::Retry, super::DecisionAction::Revise],
            );
        }
    }
    if let ProofReadiness::Failed(failures) = proof_readiness(state) {
        for failure in failures {
            let mut actions = Vec::new();
            if failure.retry_available(state) {
                actions.push(super::DecisionAction::Retry);
            }
            if failure.effect_id().is_some() {
                actions.push(super::DecisionAction::Repair);
            }
            actions.push(super::DecisionAction::Revise);
            push_decisions(&mut choices, proof_failure_id(&failure), actions);
        }
    }
    choices
}

fn finish_class(state: &MissionState) -> Option<FinishClass> {
    if state.terminal.is_some()
        || state.plan.is_none()
        || !state.inflight.is_empty()
        || !proposal_choices(state).is_empty()
        || !failure_choices(state).is_empty()
    {
        return None;
    }
    let tasks_settled = state
        .tasks
        .values()
        .filter(|task| task.status != TaskStatus::Superseded)
        .all(|task| task.status == TaskStatus::Cleared);
    if !tasks_settled {
        return None;
    }
    match proof_readiness(state) {
        ProofReadiness::Satisfied(finish) => Some(finish),
        ProofReadiness::Pending(_) | ProofReadiness::Failed(_) => None,
    }
}

fn dispatch_intents(state: &MissionState) -> Vec<EffectIntent> {
    let mut intents = Vec::new();
    if state.plan.is_none()
        || (state.planning_input.refinement.is_some() && state.proposal.is_none())
    {
        if let Some(intent) = planning_intent(state) {
            intents.push(EffectIntent::DispatchRole(intent));
        }
        return intents;
    }

    let Some(plan) = &state.plan else {
        return intents;
    };
    let capacity = state.config.execution.effect_capacity as usize;
    let remaining_capacity = capacity.saturating_sub(state.inflight.len());
    if remaining_capacity == 0 {
        return intents;
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
                return Vec::new();
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
                intents.push(EffectIntent::DispatchRole(intent));
                return intents;
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
            return Vec::new();
        };
        let Some(role_id) = team.task_assignments.get(&task.id) else {
            return Vec::new();
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
            intents.push(EffectIntent::DispatchRole(intent));
            if intents.len() == remaining_capacity {
                break;
            }
        }
    }
    if !intents.is_empty() {
        return intents;
    }

    if plan
        .tasks
        .iter()
        .any(|task| status_of(&task.id) != Some(TaskStatus::Cleared))
    {
        return intents;
    }

    if state.team.is_none() {
        return intents;
    }
    let pending_proof = match proof_readiness(state) {
        ProofReadiness::Pending(sources) => sources,
        ProofReadiness::Failed(_) => return intents,
        ProofReadiness::Satisfied(_) => Vec::new(),
    };
    let proof_pending = !pending_proof.is_empty();
    let mut reserved_judges = reserved_roles;
    for source in &pending_proof {
        let (role_instance, assertion_ids, body, dispatchable) = match source {
            ProofSource::Judgment {
                role_instance,
                assertion_ids,
            } => (
                role_instance,
                assertion_ids.clone(),
                "Judge the assigned assertions.".to_string(),
                state.taskless_assignment_dispatchable(role_instance, assertion_ids),
            ),
            ProofSource::Review { role_instance } => (
                role_instance,
                Vec::new(),
                "Review the delivered product against the objective.".to_string(),
                state.taskless_assignment_dispatchable(role_instance, &[]),
            ),
            ProofSource::Command { .. } => continue,
        };
        if reserved_judges.contains(role_instance) || !dispatchable {
            continue;
        }
        if let Some(intent) = role_intent(
            state,
            role_instance,
            None,
            body,
            assertion_ids,
            state.deliverable_head().to_string(),
            Vec::new(),
        ) {
            reserved_judges.insert(role_instance.clone());
            intents.push(EffectIntent::DispatchRole(intent));
            if intents.len() == remaining_capacity {
                break;
            }
        }
    }
    if !intents.is_empty() || !state.inflight.is_empty() {
        return intents;
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
    for intent in by_oracle.into_iter().filter_map(|(oracle, assertion_ids)| {
        Some(OracleDispatchIntent {
            attempt_no: state.next_oracle_attempt(&oracle)?,
            spec_digest: state.oracles.get(&oracle)?.digest(),
            oracle,
            assertion_ids,
            judged_sha: state.deliverable_head().to_string(),
        })
    }) {
        intents.push(EffectIntent::DispatchOracle(intent));
    }
    if !intents.is_empty() || proof_pending {
        return intents;
    }

    intents
}

fn planning_intent(state: &MissionState) -> Option<RoleDispatchIntent> {
    if !state.inflight.is_empty() || state.proposal.is_some() {
        return None;
    }
    let team = state.team.as_ref()?;
    let planner = &team.planning_assignment;
    if !state.taskless_assignment_dispatchable(planner, &[]) {
        return None;
    }
    if state
        .conversations
        .get(planner)
        .is_some_and(|conversation| {
            conversation.lifecycle == ConversationLifecycle::AwaitingLead
                && conversation.queued.is_empty()
        })
    {
        return None;
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
    let report_refs = if role.output == super::OutputSemantics::EmitsVerdict && task_id.is_none() {
        state.judgment_report_refs(&targets)?
    } else {
        Vec::new()
    };
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
        report_refs,
    })
}
