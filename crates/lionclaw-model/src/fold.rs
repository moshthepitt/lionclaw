//! Pure event fold for the team-owned runtime model.

use super::event::{ControlAction, EventEnvelope, Handoff, MissionEvent};
use super::ids::{AssertionId, RoleInstanceId};
use super::state::{
    ActiveDelivery, AssertionState, AttentionItem, AttentionKind, ConversationLifecycle,
    ConversationState, DeliveryMarker, InflightEffect, MissionPhase, MissionState, ParkedEffect,
    PlanningInput, PlanningRefinement, ReviewAcceptance, ReviewAcceptanceKind, ReviewOutcome,
    RoleAttemptDisposition, RoleAttemptReceipt, SettledHandoff, TaskAttemptOutcome,
    TaskRoleAssignment, TaskRuntimeState, TaskStatus,
};
use super::verdict::{classify_finish, AuthoritativeVerdict};
use crate::prelude::*;
use crate::{TypedFailure, TypedFailureEvidence};

/// Reducer 39 restores durable planning refinement input across team-owned
/// proposal revision and failure-driven replanning.
pub const REDUCER_VERSION: u32 = 39;

pub fn fold(events: impl IntoIterator<Item = EventEnvelope>) -> Option<MissionState> {
    let mut state = None;
    for envelope in events {
        match &mut state {
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
        image_id,
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
        mission_type: mission_type.clone(),
        image_id: image_id.clone(),
        workspace_dir: workspace_dir.clone(),
        base_sha: base_sha.clone(),
        config: config.clone(),
        team: None,
        team_history: BTreeMap::new(),
        skills: BTreeMap::new(),
        phase: MissionPhase::Planning,
        plan: None,
        contract: BTreeMap::new(),
        superseded_assertions: Vec::new(),
        tasks: BTreeMap::new(),
        planning_input: PlanningInput {
            latest_rejected_proposal: None,
            refinement: None,
        },
        proposal: None,
        current_sha: base_sha.clone(),
        oracle_attempts: BTreeMap::new(),
        inflight: BTreeMap::new(),
        role_attempt_receipts: BTreeMap::new(),
        conversations: BTreeMap::new(),
        retained_workspace_archives: BTreeMap::new(),
        authoritative_receipts: BTreeSet::new(),
        reachable_commits: BTreeSet::from([base_sha.clone()]),
        stop_requests: BTreeMap::new(),
        reached_deadlines: BTreeMap::new(),
        parked_effects: BTreeMap::new(),
        cleanup_failure: None,
        open_attention: BTreeMap::new(),
        proposal_approved: false,
        revision: 0,
        acknowledged_gates: BTreeSet::new(),
        flagged_tasks: BTreeSet::new(),
        oracle_failures: BTreeMap::new(),
        waived_oracles: BTreeSet::new(),
        gap_review: Default::default(),
        head: envelope.sequence_no,
    })
}

pub fn apply(state: &mut MissionState, envelope: &EventEnvelope) {
    if envelope.sequence_no <= state.head {
        return;
    }
    let seq = envelope.sequence_no;
    match &envelope.event {
        MissionEvent::MissionCreated { .. } => {}
        MissionEvent::TeamConfigured { team } => apply_team(state, team),
        MissionEvent::SkillAdded { skill } => {
            if valid_skill(skill) {
                state.skills.insert(skill.name.clone(), skill.clone());
            }
        }
        MissionEvent::ProposalRecorded { proposal, .. } => {
            if valid_proposal(state, proposal) {
                state.proposal = Some((**proposal).clone());
                state.proposal_approved = false;
            }
        }
        MissionEvent::RoleTurnRequested { .. } => apply_role_request(state, envelope),
        MissionEvent::MessageSent {
            recipients,
            body,
            references,
        } => apply_message(state, seq, recipients, body, references),
        MissionEvent::RoleTurnCompleted { effect_id, outcome } => {
            apply_role_outcome(state, effect_id, outcome)
        }
        MissionEvent::OracleRunRequested {
            assertion_ids,
            oracle,
            judged_sha,
            attempt_no,
            effect_id,
            requested_at_ms,
            deadline_ms,
        } => {
            let canonical = effect_id
                == &super::EffectId::for_oracle_request(
                    &state.mission_id,
                    oracle,
                    judged_sha,
                    *attempt_no,
                )
                && judged_sha == state.deliverable_head()
                && !assertion_ids.is_empty()
                && assertion_ids == &state.owed_assertions_for_oracle(oracle);
            if canonical && !state.inflight.contains_key(effect_id) {
                state.oracle_attempts.insert(oracle.clone(), *attempt_no);
                state.inflight.insert(
                    effect_id.clone(),
                    InflightEffect::OracleRun {
                        assertion_ids: assertion_ids.clone(),
                        oracle: oracle.clone(),
                        judged_sha: judged_sha.clone(),
                        attempt_no: *attempt_no,
                        requested_at_ms: *requested_at_ms,
                        not_before_ms: state
                            .oracle_failures
                            .get(oracle)
                            .filter(|failure| failure.is_transient())
                            .and_then(TypedFailure::next_eligible_at_ms)
                            .unwrap_or(*requested_at_ms)
                            .max(*requested_at_ms),
                        deadline_ms: *deadline_ms,
                        requested_seq: seq,
                    },
                );
            }
        }
        MissionEvent::OracleRunCompleted {
            assertion_ids,
            oracle,
            judged_sha,
            attempt_no,
            effect_id,
            outcome,
        } => apply_oracle_outcome(
            state,
            assertion_ids,
            oracle,
            judged_sha,
            *attempt_no,
            effect_id,
            outcome,
        ),
        MissionEvent::ControlRequested {
            effect_id,
            action,
            reason,
        } => apply_control(state, effect_id, action, reason),
        MissionEvent::EffectCleanupFailed {
            effect_id,
            resource,
            failure,
        } => {
            if state.inflight.contains_key(effect_id) {
                state.cleanup_failure = Some(super::EffectCleanupFailure {
                    effect_id: effect_id.clone(),
                    resource: *resource,
                    failure: failure.clone(),
                });
            }
        }
        MissionEvent::MissionAborted { reason } => {
            if !state.phase.is_terminal() && !reason.trim().is_empty() {
                state.phase = MissionPhase::Aborted {
                    reason: reason.clone(),
                };
            }
        }
        MissionEvent::DecisionRecorded {
            attention_id,
            action,
            justification,
            ..
        } => apply_decision(state, attention_id, action, justification),
    }
    state.head = seq;
    derive(state);
}

fn valid_skill(skill: &super::MissionSkill) -> bool {
    !skill.name.is_empty()
        && !skill.description.trim().is_empty()
        && skill.digest.len() == 64
        && skill
            .digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_proposal(state: &MissionState, proposal: &super::MissionProposal) -> bool {
    super::validate_mission_proposal(state, proposal).is_ok()
}

fn apply_team(state: &mut MissionState, team: &super::TeamRevision) {
    let expected = state
        .team
        .as_ref()
        .map_or(0, |current| current.revision.saturating_add(1));
    let plan = state
        .proposal
        .as_ref()
        .filter(|proposal| state.proposal_approved && proposal.team.as_ref() == Some(team))
        .and_then(|proposal| proposal.plan.as_ref())
        .map(|proposal| &proposal.plan)
        .or(state.plan.as_ref());
    if team.revision != expected
        || team.validate_shape().is_err()
        || team
            .roles
            .values()
            .any(|role| !role.grants.within(&state.config.ceilings))
        || plan.is_some_and(|plan| !super::validate_plan(plan, team, &state.config).is_empty())
    {
        return;
    }
    if let Some(previous) = &state.team {
        for id in previous.roles.keys() {
            if !team.roles.contains_key(id) {
                if let Some(conversation) = state.conversations.get_mut(id) {
                    retire_conversation(conversation);
                }
            }
        }
    }
    state.team_history.insert(team.revision, team.clone());
    state.team = Some(team.clone());
    if state.proposal_approved
        && state
            .proposal
            .as_ref()
            .and_then(|proposal| proposal.team.as_ref())
            == Some(team)
    {
        promote_proposal_plan(state);
    }
}

fn apply_role_request(state: &mut MissionState, envelope: &EventEnvelope) {
    let MissionEvent::RoleTurnRequested {
        role_instance,
        team_revision,
        task_id,
        assertion_ids,
        attempt_no,
        effect_id,
        prompt_template,
        prompt_hash,
        base_sha,
        assignment_epoch,
        message_boundary,
        presented_messages,
        requested_at_ms,
        ..
    } = &envelope.event
    else {
        return;
    };
    let Some(team) = state.team_history.get(team_revision) else {
        return;
    };
    let Some(role) = team.role(role_instance) else {
        return;
    };
    let canonical = effect_id
        == &super::EffectId::for_role_turn(
            &state.mission_id,
            role_instance,
            *team_revision,
            task_id.as_ref(),
            *attempt_no,
            *assignment_epoch,
            prompt_hash,
        )
        && *prompt_template == super::role_prompt_template(role.output)
        && state.role_dispatch_contract_matches(
            role_instance,
            *team_revision,
            task_id.as_ref(),
            assertion_ids,
        )
        && *message_boundary <= envelope.sequence_no.saturating_sub(1)
        && !state.inflight.contains_key(effect_id);
    if !canonical {
        return;
    }
    let not_before_ms = task_id
        .as_ref()
        .and_then(|task_id| state.task_last_failure(task_id))
        .filter(|failure| failure.is_transient())
        .and_then(TypedFailure::next_eligible_at_ms)
        .unwrap_or(*requested_at_ms)
        .max(*requested_at_ms);
    let conversation = state
        .conversations
        .entry(role_instance.clone())
        .or_insert_with(|| ConversationState {
            role_instance: role_instance.clone(),
            lifecycle: ConversationLifecycle::Ready,
            queued: Vec::new(),
            consumed_through: 0,
            active_delivery: None,
            final_response: None,
            invalid_handoff_reworks: 0,
        });
    let expected_messages = conversation
        .queued
        .iter()
        .filter(|message| {
            message.sequence_no <= *message_boundary
                && message.marker != DeliveryMarker::Undeliverable
        })
        .map(|message| message.sequence_no)
        .collect::<Vec<_>>();
    if expected_messages != *presented_messages {
        return;
    }
    let Some((_, inflight)) = InflightEffect::from_request(
        &envelope.event,
        envelope.sequence_no,
        &state.team_history,
        not_before_ms,
    ) else {
        return;
    };
    if let Some(task_id) = task_id {
        let task = state
            .tasks
            .entry(task_id.clone())
            .or_insert_with(pending_task);
        task.status = TaskStatus::Running;
        task.attempts = *attempt_no;
        task.role_assignment = Some(TaskRoleAssignment {
            role_instance: role_instance.clone(),
            team_revision: *team_revision,
            base_sha: base_sha.clone(),
            assignment_epoch: *assignment_epoch,
        });
    }
    conversation.lifecycle = ConversationLifecycle::Running;
    conversation.active_delivery = Some(ActiveDelivery {
        effect_id: effect_id.clone(),
        message_boundary: *message_boundary,
        presented_messages: presented_messages.clone(),
    });
    state.role_attempt_receipts.insert(
        effect_id.clone(),
        inflight
            .role_attempt_receipt(effect_id, state.revision)
            .expect("role turn"),
    );
    state.inflight.insert(effect_id.clone(), inflight);
}

fn apply_message(
    state: &mut MissionState,
    sequence_no: u64,
    recipients: &[RoleInstanceId],
    body: &str,
    references: &[super::MessageReference],
) {
    if body.len() > super::MAX_MESSAGE_BYTES
        || recipients.is_empty()
        || recipients.len() > super::MAX_MESSAGE_RECIPIENTS
        || references.len() > super::MAX_MESSAGE_REFERENCES
        || recipients
            .iter()
            .any(|recipient| !state.conversation_accepts_message(recipient))
    {
        return;
    }
    for recipient in recipients {
        if let Some(conversation) = state.conversations.get_mut(recipient) {
            conversation.queued.push(super::QueuedMessage {
                sequence_no,
                body: body.to_string(),
                references: references.to_vec(),
                marker: DeliveryMarker::Queued,
            });
        }
    }
}

fn apply_role_outcome(
    state: &mut MissionState,
    effect_id: &super::EffectId,
    outcome: &Result<super::RoleTurnSuccess, TypedFailure>,
) {
    let Some(effect @ InflightEffect::RoleTurn { .. }) = state.inflight.remove(effect_id) else {
        return;
    };
    let Some(request) = effect.role_turn_provenance() else {
        return;
    };
    if let Some(cancellation) = state.durable_cancellation(effect_id) {
        settle_role_failure(
            state,
            effect_id,
            &request,
            cancellation.into_failure(TypedFailureEvidence::default()),
        );
        return;
    }
    match outcome {
        Err(failure)
            if failure.evidence().code.as_deref() == Some("message.reference_unavailable") =>
        {
            settle_delivery(state, &request.role_instance, true);
            if let Some(receipt) = state.role_attempt_receipts.get_mut(effect_id) {
                receipt.disposition = RoleAttemptDisposition::Failed {
                    failure: failure.clone(),
                };
            }
            if let Some(task_id) = &request.task_id {
                if let Some(task) = state.tasks.get_mut(task_id) {
                    task.status = TaskStatus::Pending;
                }
            }
            if let Some(conversation) = state.conversations.get_mut(&request.role_instance) {
                conversation.lifecycle = ConversationLifecycle::Ready;
            }
        }
        Err(failure) => settle_role_failure(state, effect_id, &request, failure.clone()),
        Ok(success) => {
            let Some(team) = state.team_history.get(&request.team_revision) else {
                return;
            };
            let Some(role) = team.role(&request.role_instance) else {
                return;
            };
            let output = role.output;
            if success.handoff.as_ref().is_some_and(|handoff| {
                !role_handoff_matches_request(&request, output, handoff, success.artifact.as_ref())
            }) || (output.requires_handoff() && success.handoff.is_none())
            {
                settle_role_failure(
                    state,
                    effect_id,
                    &request,
                    TypedFailure::invalid(
                        "handoff.contract",
                        "role result does not satisfy its team-owned output contract",
                    ),
                );
                return;
            }
            settle_delivery(state, &request.role_instance, true);
            if let Some(conversation) = state.conversations.get_mut(&request.role_instance) {
                conversation.final_response = Some(success.final_response.clone());
            }
            if let Some(receipt) = state.role_attempt_receipts.get_mut(effect_id) {
                receipt.runtime_configuration = Some(success.runtime_configuration.clone());
                receipt.final_response = Some(success.final_response.clone());
                receipt.handoff = success.handoff.clone();
                receipt.disposition = RoleAttemptDisposition::Succeeded {
                    handoff: success
                        .handoff
                        .as_ref()
                        .map(SettledHandoff::from_handoff)
                        .map(Box::new),
                    artifact: success.artifact.clone(),
                };
            }
            if output == super::OutputSemantics::ProducesArtifact {
                if let Ok((task_id, provenance)) =
                    state.expected_active_workspace_provenance(effect_id)
                {
                    if let Some(archived) = provenance.archived_effect_id.as_ref() {
                        state
                            .retained_workspace_archives
                            .entry(task_id.clone())
                            .or_default()
                            .insert(archived.clone());
                    }
                    if let Some(task) = state.tasks.get_mut(&task_id) {
                        task.workspace_provenance = Some(provenance);
                        task.pending_workspace_recreation = None;
                    }
                }
            }
            apply_success_handoff(state, effect_id, &request, output, success);
        }
    }
}

fn role_handoff_matches_request(
    request: &super::RoleTurnProvenance,
    output: super::OutputSemantics,
    handoff: &Handoff,
    artifact: Option<&super::ArtifactOutcome>,
) -> bool {
    if super::role_success_contract_error(output, handoff, artifact, &request.base_sha).is_some() {
        return false;
    }
    let Handoff::Validate { items, passed, .. } = handoff else {
        return true;
    };
    let expected: BTreeSet<_> = request.assertion_ids.iter().collect();
    let actual: BTreeSet<_> = items.iter().map(|item| &item.item_id).collect();
    items.len() == actual.len()
        && actual == expected
        && *passed == items.iter().all(|item| item.passed)
}

fn apply_success_handoff(
    state: &mut MissionState,
    effect_id: &super::EffectId,
    request: &super::RoleTurnProvenance,
    output: super::OutputSemantics,
    success: &super::RoleTurnSuccess,
) {
    match (output, &success.handoff) {
        (super::OutputSemantics::ProducesArtifact, None)
        | (super::OutputSemantics::ProducesReport, None)
        | (super::OutputSemantics::ProposesPlan, None) => {
            if let Some(conversation) = state.conversations.get_mut(&request.role_instance) {
                conversation.lifecycle = ConversationLifecycle::AwaitingLead;
            }
        }
        (super::OutputSemantics::ProducesArtifact, Some(Handoff::Work { .. })) => {
            if let Some(task_id) = &request.task_id {
                if let Some(task) = state.tasks.get_mut(task_id) {
                    task.status = TaskStatus::Cleared;
                    task.consecutive_failures = 0;
                    task.last_outcome = Some(TaskAttemptOutcome::Accepted {
                        effect_id: effect_id.clone(),
                    });
                }
                if let Some(artifact) = &success.artifact {
                    state.current_sha = artifact.head_sha.clone();
                    state.reachable_commits.insert(artifact.head_sha.clone());
                }
            }
            retire_role_conversation(state, &request.role_instance);
        }
        (super::OutputSemantics::ProposesPlan, Some(Handoff::Plan { proposal, .. })) => {
            if let Some(proposal) = proposal {
                if valid_proposal(state, proposal) {
                    state.proposal = Some((**proposal).clone());
                    state.proposal_approved = false;
                }
            }
            if let Some(conversation) = state.conversations.get_mut(&request.role_instance) {
                conversation.lifecycle = ConversationLifecycle::AwaitingLead;
            }
        }
        (super::OutputSemantics::EmitsVerdict, Some(Handoff::Validate { items, .. })) => {
            for assertion_id in &request.assertion_ids {
                if items.iter().any(|item| &item.item_id == assertion_id) {
                    if let Some(assertion) = state.contract.get_mut(assertion_id) {
                        assertion
                            .last_advisory
                            .insert(request.role_instance.clone(), effect_id.clone());
                    }
                }
            }
            retire_role_conversation(state, &request.role_instance);
        }
        (super::OutputSemantics::EmitsGapVerdict, Some(Handoff::Review { .. })) => {
            state.gap_review.attempts = request.attempt_no;
            state.gap_review.consecutive_failures = 0;
            state.gap_review.outcome = Some(ReviewOutcome::Verdict {
                effect_id: effect_id.clone(),
            });
            retire_role_conversation(state, &request.role_instance);
        }
        (super::OutputSemantics::ProducesReport, Some(Handoff::Work { .. })) => {
            retire_role_conversation(state, &request.role_instance);
        }
        _ => {}
    }
}

fn settle_role_failure(
    state: &mut MissionState,
    effect_id: &super::EffectId,
    request: &super::RoleTurnProvenance,
    failure: TypedFailure,
) {
    settle_delivery(state, &request.role_instance, false);
    if let Some(receipt) = state.role_attempt_receipts.get_mut(effect_id) {
        receipt.runtime_configuration = Some(failure.evidence().configuration.clone());
        if !failure.evidence().final_response.is_empty() {
            receipt.final_response = Some(super::PayloadRef::inline(
                failure.evidence().final_response.clone(),
            ));
        }
        receipt.disposition = RoleAttemptDisposition::Failed {
            failure: failure.clone(),
        };
    }
    if let Some(task_id) = &request.task_id {
        if let Some(task) = state.tasks.get_mut(task_id) {
            task.status = TaskStatus::Failed;
            task.consecutive_failures = task.consecutive_failures.saturating_add(1);
            task.last_outcome = Some(TaskAttemptOutcome::Failed {
                effect_id: effect_id.clone(),
            });
        }
    }
    let output = state
        .team_history
        .get(&request.team_revision)
        .and_then(|team| team.role(&request.role_instance))
        .map(|role| role.output);
    if output == Some(super::OutputSemantics::EmitsGapVerdict) {
        state.gap_review.attempts = request.attempt_no;
        state.gap_review.consecutive_failures =
            state.gap_review.consecutive_failures.saturating_add(1);
        state.gap_review.outcome = Some(ReviewOutcome::Failed {
            effect_id: effect_id.clone(),
        });
    }
    state.parked_effects.insert(
        effect_id.clone(),
        ParkedEffect::RoleTurn {
            role_instance: request.role_instance.clone(),
            task_id: request.task_id.clone(),
        },
    );
    if let Some(conversation) = state.conversations.get_mut(&request.role_instance) {
        conversation.lifecycle = ConversationLifecycle::Ready;
    }
}

fn settle_delivery(state: &mut MissionState, role_instance: &RoleInstanceId, success: bool) {
    let Some(conversation) = state.conversations.get_mut(role_instance) else {
        return;
    };
    let Some(delivery) = conversation.active_delivery.take() else {
        return;
    };
    for message in &mut conversation.queued {
        if delivery.presented_messages.contains(&message.sequence_no) {
            message.marker = if success {
                DeliveryMarker::Undeliverable
            } else {
                DeliveryMarker::PreviouslyDelivered
            };
        }
    }
    if success {
        conversation.consumed_through = delivery.message_boundary;
        conversation
            .queued
            .retain(|message| message.sequence_no > delivery.message_boundary);
    }
}

fn apply_oracle_outcome(
    state: &mut MissionState,
    assertion_ids: &[AssertionId],
    oracle: &super::OracleName,
    judged_sha: &str,
    attempt_no: u32,
    effect_id: &super::EffectId,
    outcome: &Result<super::OracleRunSuccess, TypedFailure>,
) {
    let Some(InflightEffect::OracleRun {
        assertion_ids: expected_assertions,
        oracle: expected_oracle,
        judged_sha: expected_sha,
        attempt_no: expected_attempt,
        ..
    }) = state.inflight.remove(effect_id)
    else {
        return;
    };
    if expected_assertions != assertion_ids
        || expected_oracle != *oracle
        || expected_sha != judged_sha
        || expected_attempt != attempt_no
    {
        return;
    }
    match outcome {
        Err(failure) => {
            state
                .oracle_failures
                .insert(oracle.clone(), failure.clone());
            state.parked_effects.insert(
                effect_id.clone(),
                ParkedEffect::OracleRun {
                    oracle: oracle.clone(),
                },
            );
        }
        Ok(success) => {
            state.oracle_failures.remove(oracle);
            let verdict = AuthoritativeVerdict::from_oracle_outcome(
                oracle.clone(),
                judged_sha.to_string(),
                success.exit_code,
                success.exit_signal,
                success.stdout.clone(),
                success.stderr.clone(),
                success.prepared_inputs.clone(),
            );
            for assertion_id in assertion_ids {
                if let Some(assertion) = state.contract.get_mut(assertion_id) {
                    if assertion.oracle.as_ref() == Some(oracle) {
                        assertion.last_authoritative = Some(verdict.clone());
                    }
                }
            }
            state.authoritative_receipts.insert(effect_id.clone());
        }
    }
}

fn apply_control(
    state: &mut MissionState,
    effect_id: &super::EffectId,
    action: &ControlAction,
    reason: &str,
) {
    if reason.trim().is_empty() {
        return;
    }
    match action {
        ControlAction::Stop if state.inflight.contains_key(effect_id) => {
            state
                .stop_requests
                .insert(effect_id.clone(), reason.to_string());
        }
        ControlAction::DeadlineReached { deadline_ms }
            if state
                .inflight
                .get(effect_id)
                .is_some_and(|effect| effect.deadline_ms() == *deadline_ms) =>
        {
            state
                .reached_deadlines
                .insert(effect_id.clone(), *deadline_ms);
        }
        ControlAction::ExtendDeadline {
            old_deadline_ms,
            new_deadline_ms,
            ..
        } if new_deadline_ms >= old_deadline_ms => {
            if let Some(effect) = state.inflight.get_mut(effect_id) {
                if effect.deadline_ms() == *old_deadline_ms {
                    effect.set_deadline_ms(*new_deadline_ms);
                }
            }
        }
        ControlAction::Continue { mode, .. }
            if state.parked_continue_is_legal(effect_id, *mode) =>
        {
            if *mode == super::ContinueMode::RecreateWorkspace {
                if let Some(task_id) = state.parked_workspace_recreation(effect_id) {
                    if let Some(task) = state.tasks.get_mut(&task_id) {
                        task.pending_workspace_recreation = Some(effect_id.clone());
                    }
                }
            }
            if let Some(parked) = state.parked_effects.remove(effect_id) {
                match parked {
                    ParkedEffect::RoleTurn {
                        role_instance,
                        task_id,
                    } => {
                        if let Some(task_id) = task_id {
                            if let Some(task) = state.tasks.get_mut(&task_id) {
                                task.status = TaskStatus::Pending;
                            }
                        }
                        if let Some(conversation) = state.conversations.get_mut(&role_instance) {
                            conversation.lifecycle = ConversationLifecycle::Ready;
                        }
                    }
                    ParkedEffect::OracleRun { oracle } => {
                        state.oracle_failures.remove(&oracle);
                    }
                }
            }
        }
        _ => {}
    }
}

fn apply_decision(
    state: &mut MissionState,
    attention_id: &str,
    action: &super::DecisionAction,
    justification: &str,
) {
    let Some(item) = state.open_attention.get(attention_id).cloned() else {
        return;
    };
    if justification.trim().is_empty() {
        return;
    }
    match (action, item.kind) {
        (super::DecisionAction::Approve, AttentionKind::PlanProposal) => {
            state.proposal_approved = true;
            state.planning_input.latest_rejected_proposal = None;
            state.planning_input.refinement = None;
            if state
                .proposal
                .as_ref()
                .is_some_and(|proposal| proposal.team.is_none())
            {
                promote_proposal_plan(state);
            }
        }
        (
            super::DecisionAction::Retry,
            AttentionKind::GapReviewGaps | AttentionKind::GapReviewFailed,
        ) => {
            if let Some(outcome) = state.gap_review.outcome.take() {
                state.parked_effects.remove(outcome.effect_id());
            }
            state.gap_review.accepted = None;
        }
        (super::DecisionAction::Retry, AttentionKind::NodeFailed) => {
            retry_failed_node(state, &item);
        }
        (super::DecisionAction::Retry, AttentionKind::OracleVerdictFailed) => {
            clear_authoritative_verdicts(state, &item.assertion_ids);
        }
        (super::DecisionAction::Repair, AttentionKind::OracleVerdictFailed) => {
            let feedback = super::FailureFeedback {
                summary: item.report.clone(),
                evidence: item.evidence.clone(),
                justification: justification.to_string(),
            };
            clear_authoritative_verdicts(state, &item.assertion_ids);
            if let Some(plan) = &state.plan {
                for task in plan.tasks.iter().filter(|task| {
                    task.targets
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
            state.gap_review = Default::default();
        }
        (super::DecisionAction::Repair, _) => {}
        (super::DecisionAction::Revise, AttentionKind::PlanProposal) => {
            state.planning_input.latest_rejected_proposal = state.proposal.take();
            state.planning_input.refinement =
                Some(PlanningRefinement::Guidance(justification.to_string()));
            state.phase = MissionPhase::Planning;
            state.proposal_approved = false;
            state.gap_review.accepted = None;
            ready_planning_conversation(state);
        }
        (super::DecisionAction::Revise, _) => {
            if item.kind == AttentionKind::NodeFailed {
                retry_failed_node(state, &item);
            }
            state.phase = MissionPhase::Planning;
            state.proposal = None;
            state.proposal_approved = false;
            state.gap_review.accepted = None;
            state.planning_input.refinement = Some(PlanningRefinement::FailureEvidence(Box::new(
                super::FailureFeedback {
                    summary: item.report,
                    evidence: item.evidence,
                    justification: justification.to_string(),
                },
            )));
            ready_planning_conversation(state);
        }
        (super::DecisionAction::Accept, AttentionKind::GapReviewGaps) => {
            let judged_sha = state
                .gap_review_receipt()
                .map(|receipt| match &receipt.source {
                    super::RoleEffectSource::Turn { request, .. } => &request.base_sha,
                })
                .cloned();
            if let Some(judged_sha) = judged_sha {
                state.gap_review.accepted = Some(ReviewAcceptance {
                    kind: ReviewAcceptanceKind::AcknowledgedGaps,
                    judged_sha,
                    justification: justification.to_string(),
                });
            }
        }
        (super::DecisionAction::Accept, AttentionKind::GapReviewFailed) => {
            if let Some(outcome) = state.gap_review.outcome.take() {
                state.parked_effects.remove(outcome.effect_id());
            }
            state.gap_review.consecutive_failures = 0;
            state.gap_review.accepted = Some(ReviewAcceptance {
                kind: ReviewAcceptanceKind::Waived,
                judged_sha: state.deliverable_head().to_string(),
                justification: justification.to_string(),
            });
        }
        (super::DecisionAction::Accept, _) => {
            if let Some(task_id) = &item.task_id {
                if let Some(task) = state.tasks.get_mut(task_id) {
                    task.status = TaskStatus::Cleared;
                }
            }
            if let Some(oracle) = &item.oracle {
                state.waived_oracles.insert(oracle.clone());
            }
        }
        _ => {}
    }
    state.open_attention.remove(attention_id);
}

fn ready_planning_conversation(state: &mut MissionState) {
    let Some(planner) = state
        .team
        .as_ref()
        .map(|team| team.planning_assignment.clone())
    else {
        return;
    };
    if let Some(conversation) = state.conversations.get_mut(&planner) {
        conversation.lifecycle = ConversationLifecycle::Ready;
        conversation.active_delivery = None;
    }
}

fn clear_authoritative_verdicts(state: &mut MissionState, assertion_ids: &[AssertionId]) {
    for assertion_id in assertion_ids {
        if let Some(assertion) = state.contract.get_mut(assertion_id) {
            assertion.last_authoritative = None;
        }
    }
}

fn retry_failed_node(state: &mut MissionState, item: &AttentionItem) {
    if let Some(task_id) = &item.task_id {
        if let Some(task) = state.tasks.get_mut(task_id) {
            task.status = TaskStatus::Pending;
            task.consecutive_failures = 0;
        }
    }
    for effect_id in item.evidence.role_attempts() {
        if let Some(ParkedEffect::RoleTurn { role_instance, .. }) =
            state.parked_effects.remove(effect_id)
        {
            if let Some(conversation) = state.conversations.get_mut(&role_instance) {
                conversation.lifecycle = ConversationLifecycle::Ready;
            }
        }
    }
}

fn promote_proposal_plan(state: &mut MissionState) {
    let Some(proposal) = state.proposal.clone() else {
        return;
    };
    let Some(plan_proposal) = proposal.plan else {
        state.proposal = None;
        state.proposal_approved = false;
        return;
    };
    let Some(team) = state.team.as_ref() else {
        return;
    };
    if !super::validate_plan(&plan_proposal.plan, team, &state.config).is_empty() {
        return;
    }
    let supersessions: BTreeMap<_, _> = plan_proposal
        .assertion_supersessions
        .iter()
        .map(|entry| (entry.assertion_id.clone(), entry.replacement_ids.clone()))
        .collect();
    if let Some(current) = &state.plan {
        for assertion in &current.assertions {
            let Some(replacement_ids) = supersessions.get(&assertion.id) else {
                continue;
            };
            if let Some(assertion_state) = state.contract.remove(&assertion.id) {
                state
                    .superseded_assertions
                    .push(super::SupersededAssertion {
                        assertion: assertion.clone(),
                        state: assertion_state,
                        replacement_ids: replacement_ids.clone(),
                        superseded_at_revision: state.revision.saturating_add(1),
                    });
            }
        }
    }
    state.revision = state.revision.saturating_add(1);
    state.plan = Some(plan_proposal.plan.clone());
    state.contract = plan_proposal
        .plan
        .assertions
        .iter()
        .map(|assertion| {
            (
                assertion.id.clone(),
                AssertionState {
                    oracle: assertion.oracle.clone(),
                    last_advisory: BTreeMap::new(),
                    last_authoritative: None,
                },
            )
        })
        .collect();
    let current_ids: BTreeSet<_> = plan_proposal
        .plan
        .tasks
        .iter()
        .map(|task| task.id.clone())
        .collect();
    for task in state.tasks.values_mut() {
        task.status = TaskStatus::Superseded;
    }
    for task_id in current_ids {
        state
            .tasks
            .entry(task_id)
            .or_insert_with(pending_task)
            .status = TaskStatus::Pending;
    }
    state.proposal = None;
    state.proposal_approved = false;
    state.phase = MissionPhase::Running;
}

fn derive(state: &mut MissionState) {
    if state.phase.is_terminal() {
        return;
    }
    let mut attention = BTreeMap::new();
    if state.proposal.is_some() && !state.proposal_approved {
        attention.insert(
            "plan_proposal:mission".to_string(),
            AttentionItem {
                id: "plan_proposal:mission".to_string(),
                kind: AttentionKind::PlanProposal,
                task_id: None,
                oracle: None,
                assertion_ids: Vec::new(),
                evidence: super::DecisionEvidence::None,
                report: "A complete plan or team proposal awaits approval.".to_string(),
            },
        );
    }
    for (task_id, task) in &state.tasks {
        if task.status == TaskStatus::Failed && !state.task_automatic_retry_remaining(task_id) {
            let id = format!("node_failed:{task_id}");
            attention.insert(
                id.clone(),
                AttentionItem {
                    id,
                    kind: AttentionKind::NodeFailed,
                    task_id: Some(task_id.clone()),
                    oracle: None,
                    assertion_ids: Vec::new(),
                    evidence: super::DecisionEvidence::None,
                    report: format!("Task '{task_id}' is parked."),
                },
            );
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
            let Some((effect_id, failure, consecutive)) =
                state.taskless_assignment_failure(&role_instance, &assertion_ids)
            else {
                continue;
            };
            if failure.automatically_retryable() && consecutive < state.config.recovery.max_attempts
            {
                continue;
            }
            let scope = assertion_ids.first().map_or_else(
                || role_instance.to_string(),
                |id| format!("{role_instance}:{id}"),
            );
            let id = format!("node_failed:{scope}");
            attention.insert(
                id.clone(),
                AttentionItem {
                    id,
                    kind: AttentionKind::NodeFailed,
                    task_id: None,
                    oracle: None,
                    assertion_ids,
                    evidence: super::DecisionEvidence::RoleAttempts {
                        effect_ids: vec![effect_id.clone()],
                    },
                    report: format!("Role instance '{role_instance}' is parked."),
                },
            );
        }
    }
    for oracle in state.oracle_failures.keys() {
        if !state.oracle_automatic_retry_remaining(oracle) {
            let id = format!("oracle_failed:{oracle}");
            attention.insert(
                id.clone(),
                AttentionItem {
                    id,
                    kind: AttentionKind::OracleFailed,
                    task_id: None,
                    oracle: Some(oracle.clone()),
                    assertion_ids: state.owed_assertions_for_oracle(oracle),
                    evidence: super::DecisionEvidence::None,
                    report: format!("Oracle '{oracle}' is parked."),
                },
            );
        }
    }
    let mut failed_by_oracle: BTreeMap<
        super::OracleName,
        (Vec<AssertionId>, super::FailureEvidence),
    > = BTreeMap::new();
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
                    super::FailureEvidence {
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
        let id = format!("oracle_verdict_failed:{oracle}");
        attention.insert(
            id.clone(),
            AttentionItem {
                id,
                kind: AttentionKind::OracleVerdictFailed,
                task_id: None,
                oracle: Some(oracle),
                assertion_ids,
                evidence: super::DecisionEvidence::OracleVerdict { evidence },
                report: "An authoritative oracle verdict failed.".to_string(),
            },
        );
    }
    if state.config.requires_gap_review {
        match &state.gap_review.outcome {
            Some(ReviewOutcome::Failed { effect_id })
                if state
                    .role_attempt_receipts
                    .get(effect_id)
                    .and_then(RoleAttemptReceipt::failure)
                    .is_some_and(|failure| !failure.automatically_retryable())
                    || state.gap_review.consecutive_failures
                        >= state.config.recovery.max_attempts =>
            {
                let id = "gap_review_failed:mission".to_string();
                attention.insert(
                    id.clone(),
                    AttentionItem {
                        id,
                        kind: AttentionKind::GapReviewFailed,
                        task_id: None,
                        oracle: None,
                        assertion_ids: Vec::new(),
                        evidence: super::DecisionEvidence::RoleAttempts {
                            effect_ids: vec![effect_id.clone()],
                        },
                        report: "Gap review failed to run.".to_string(),
                    },
                );
            }
            Some(ReviewOutcome::Verdict { effect_id }) => {
                let blocking = state
                    .role_attempt_receipts
                    .get(effect_id)
                    .and_then(
                        |receipt| match (&receipt.source, receipt.settled_handoff()) {
                            (
                                super::RoleEffectSource::Turn { request, .. },
                                Some(SettledHandoff::Review { passed, gaps }),
                            ) if request.base_sha == state.deliverable_head() => Some(
                                !passed
                                    || gaps
                                        .iter()
                                        .any(|gap| gap.severity == super::GapSeverity::Blocking),
                            ),
                            _ => None,
                        },
                    )
                    .unwrap_or(false);
                let work_settled = state
                    .tasks
                    .values()
                    .filter(|task| task.status != TaskStatus::Superseded)
                    .all(|task| task.status == TaskStatus::Cleared)
                    && state.inflight.is_empty()
                    && !oracle_obligation_outstanding(state);
                if blocking
                    && work_settled
                    && !state.gap_review.acknowledges_sha(state.deliverable_head())
                {
                    let id = "gap_review_gaps:mission".to_string();
                    attention.insert(
                        id.clone(),
                        AttentionItem {
                            id,
                            kind: AttentionKind::GapReviewGaps,
                            task_id: None,
                            oracle: None,
                            assertion_ids: Vec::new(),
                            evidence: super::DecisionEvidence::RoleAttempts {
                                effect_ids: vec![effect_id.clone()],
                            },
                            report: "Gap review found blocking gaps.".to_string(),
                        },
                    );
                }
            }
            Some(ReviewOutcome::Failed { .. }) => {}
            None => {}
        }
    }
    state.open_attention = attention;
    if !state.open_attention.is_empty() {
        state.phase = MissionPhase::AttentionNeeded;
        return;
    }
    if state.plan.is_none()
        || (state.planning_input.refinement.is_some() && state.proposal.is_none())
    {
        state.phase = MissionPhase::Planning;
        return;
    }
    state.phase = MissionPhase::Running;
    let tasks_settled = state
        .tasks
        .values()
        .filter(|task| task.status != TaskStatus::Superseded)
        .all(|task| task.status == TaskStatus::Cleared);
    if tasks_settled
        && state.inflight.is_empty()
        && !oracle_obligation_outstanding(state)
        && !advisory_obligation_outstanding(state)
        && !gap_review_outstanding(state)
    {
        state.phase = MissionPhase::Done {
            finish: classify_finish(state),
        };
    }
}

pub(crate) fn advisory_obligation_outstanding(state: &MissionState) -> bool {
    state
        .contract
        .keys()
        .any(|assertion_id| state.advisory_status(assertion_id) == super::AdvisoryStatus::Pending)
}

pub(crate) fn oracle_obligation_outstanding(state: &MissionState) -> bool {
    state.contract.iter().any(|(_, assertion)| {
        assertion.oracle.as_ref().is_some_and(|oracle| {
            !state.waived_oracles.contains(oracle)
                && assertion
                    .last_authoritative
                    .as_ref()
                    .is_none_or(|verdict| !verdict.is_fresh_at(state.deliverable_head()))
        })
    })
}

pub(crate) fn gap_review_outstanding(state: &MissionState) -> bool {
    if !state.config.requires_gap_review {
        return false;
    }
    if state.gap_review.waived_at(state.deliverable_head())
        || state.gap_review.acknowledges_sha(state.deliverable_head())
    {
        return false;
    }
    let Some(team) = state.team.as_ref() else {
        return true;
    };
    let Some(role_id) = team.gap_review_assignment.as_ref() else {
        return true;
    };
    let Some(ReviewOutcome::Verdict { effect_id }) = &state.gap_review.outcome else {
        return true;
    };
    state
        .role_attempt_receipts
        .get(effect_id)
        .and_then(
            |receipt| match (&receipt.source, receipt.settled_handoff()) {
                (
                    super::RoleEffectSource::Turn { request, .. },
                    Some(SettledHandoff::Review { passed, gaps }),
                ) if &request.role_instance == role_id
                    && request.base_sha == state.deliverable_head() =>
                {
                    Some(
                        *passed
                            && gaps
                                .iter()
                                .all(|gap| gap.severity != super::GapSeverity::Blocking),
                    )
                }
                _ => None,
            },
        )
        != Some(true)
}

fn pending_task() -> TaskRuntimeState {
    TaskRuntimeState {
        status: TaskStatus::Pending,
        attempts: 0,
        consecutive_failures: 0,
        last_outcome: None,
        feedback: Vec::new(),
        role_assignment: None,
        workspace_provenance: None,
        pending_workspace_recreation: None,
    }
}

fn retire_role_conversation(state: &mut MissionState, role_instance: &RoleInstanceId) {
    if let Some(conversation) = state.conversations.get_mut(role_instance) {
        retire_conversation(conversation);
    }
}

fn retire_conversation(conversation: &mut ConversationState) {
    conversation.lifecycle = ConversationLifecycle::Retired;
    conversation.active_delivery = None;
    for message in &mut conversation.queued {
        message.marker = DeliveryMarker::Undeliverable;
    }
}
