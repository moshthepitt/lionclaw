//! Pure event fold for the team-owned runtime model.

use super::event::{ControlAction, EventEnvelope, Handoff, MissionEvent};
use super::ids::{AssertionId, RoleInstanceId, TaskId};
use super::state::{
    ActiveDelivery, AssertionState, AttentionItem, AttentionKind, ConversationLifecycle,
    ConversationState, DeliveryMarker, InflightEffect, MissionPhase, MissionState, ParkedEffect,
    PlanningInput, PlanningRefinement, ReviewAcceptance, ReviewAcceptanceKind, ReviewOutcome,
    RoleAttemptDisposition, RoleAttemptReceipt, SettledHandoff, TaskAttemptOutcome,
    TaskRoleAssignment, TaskRuntimeState, TaskStatus,
};
use super::verdict::{classify_finish, AuthoritativeVerdict, FinishClass};
use crate::prelude::*;
use crate::{TypedFailure, TypedFailureEvidence};

/// Reducer 61 parks failed required judgments and permits closure only when the
/// derived finish class satisfies the mission's declared stop bar.
pub const REDUCER_VERSION: u32 = 61;

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
        environment_history: Vec::new(),
        workspace_dir: workspace_dir.clone(),
        base_sha: base_sha.clone(),
        config: config.clone(),
        team: None,
        team_history: BTreeMap::new(),
        runtime_identity_history: BTreeMap::new(),
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
        MissionEvent::TeamConfigured {
            team,
            runtime_identities,
        } => apply_team(state, team, runtime_identities),
        MissionEvent::SkillAdded { skill } => {
            if valid_skill(skill) {
                state.skills.insert(skill.name.clone(), skill.clone());
            }
        }
        MissionEvent::EnvironmentAssigned {
            image_ref,
            image_id,
            preflight,
            team_revision,
            reason,
        } => apply_environment_assignment(
            state,
            image_ref,
            image_id,
            preflight,
            *team_revision,
            reason,
        ),
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
            environment_digest,
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
                && environment_digest == state.environment_digest()
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
                        environment_digest: environment_digest.clone(),
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
                for conversation in state.conversations.values_mut() {
                    retire_conversation(conversation);
                }
            }
        }
        MissionEvent::MissionFinished { finish, reason } => {
            if !state.phase.is_terminal()
                && !reason.trim().is_empty()
                && ready_to_finish(state) == Some(*finish)
            {
                state.phase = MissionPhase::Done { finish: *finish };
                for conversation in state.conversations.values_mut() {
                    retire_conversation(conversation);
                }
            }
        }
        MissionEvent::ResultApplied {
            branch,
            sha,
            reason,
        } => {
            let _valid = matches!(state.phase, MissionPhase::Done { .. })
                && !branch.trim().is_empty()
                && sha == state.deliverable_head()
                && !reason.trim().is_empty();
        }
        MissionEvent::DecisionRecorded {
            attention_id,
            action,
            justification,
            requirement_changes,
        } => apply_decision(
            state,
            attention_id,
            action,
            justification,
            requirement_changes,
        ),
    }
    if state
        .cleanup_failure
        .as_ref()
        .is_some_and(|failure| !state.inflight.contains_key(&failure.effect_id))
    {
        state.cleanup_failure = None;
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

fn apply_environment_assignment(
    state: &mut MissionState,
    image_ref: &str,
    image_id: &str,
    preflight: &super::EnvironmentPreflight,
    team_revision: Option<u32>,
    reason: &str,
) {
    if state.phase.is_terminal()
        || !state.inflight.is_empty()
        || reason.trim().is_empty()
        || !valid_environment_image_ref(image_ref)
        || !valid_environment_image_id(image_id)
        || preflight.image_ref != image_ref
        || preflight.image_id != image_id
        || preflight.engine.trim().is_empty()
        || team_revision != state.team.as_ref().map(|team| team.revision)
    {
        return;
    }
    let revision = state.environment_history.len().saturating_add(1) as u32;
    state.image_id = image_id.to_string();
    state
        .environment_history
        .push(super::EnvironmentAssignment {
            revision,
            image_ref: image_ref.to_string(),
            image_id: image_id.to_string(),
            preflight: preflight.clone(),
            team_revision,
            reason: reason.to_string(),
        });
}

fn valid_environment_image_ref(image_ref: &str) -> bool {
    let image_ref = image_ref.trim();
    if let Some(hex) = image_ref.strip_prefix("sha256:") {
        return valid_sha256_hex(hex);
    }
    image_ref
        .rsplit_once("@sha256:")
        .is_some_and(|(name, hex)| !name.trim().is_empty() && valid_sha256_hex(hex))
}

fn valid_environment_image_id(image_id: &str) -> bool {
    let image_id = image_id.trim();
    if let Some(hex) = image_id.strip_prefix("sha256:") {
        valid_sha256_hex(hex)
    } else {
        valid_sha256_hex(image_id)
    }
}

fn valid_sha256_hex(hex: &str) -> bool {
    hex.len() == 64
        && hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_proposal(state: &MissionState, proposal: &super::MissionProposal) -> bool {
    super::validate_mission_proposal(state, proposal).is_ok()
}

fn apply_team(
    state: &mut MissionState,
    team: &super::TeamRevision,
    runtime_identities: &BTreeMap<RoleInstanceId, super::RuntimeInstrumentIdentity>,
) {
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
        || team.roles.values().any(|role| {
            role.resources
                .within(&state.config.resource_ceilings)
                .is_err()
        })
        || !runtime_identities_match_team(team, runtime_identities)
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
    state
        .runtime_identity_history
        .insert(team.revision, runtime_identities.clone());
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

fn runtime_identities_match_team(
    team: &super::TeamRevision,
    runtime_identities: &BTreeMap<RoleInstanceId, super::RuntimeInstrumentIdentity>,
) -> bool {
    if runtime_identities.len() != team.roles.len() {
        return false;
    }
    team.roles.iter().all(|(id, role)| {
        runtime_identities.get(id).is_some_and(|identity| {
            identity.runtime == role.runtime
                && !identity.runtime.trim().is_empty()
                && identity
                    .model
                    .as_ref()
                    .is_none_or(|model| !model.trim().is_empty())
                && identity
                    .mode
                    .as_ref()
                    .is_none_or(|mode| !mode.trim().is_empty())
        })
    })
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
        environment_digest,
        instrument_identity,
        dependency_refs,
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
        && environment_digest == state.environment_digest()
        && state.role_instrument_identity_for_revision(role_instance, *team_revision)
            == Some(instrument_identity.clone())
        && state.role_dispatch_contract_matches(
            role_instance,
            *team_revision,
            task_id.as_ref(),
            assertion_ids,
        )
        && task_id.as_ref().is_none_or(|task_id| {
            state.task_lineage_request_matches(task_id, base_sha, dependency_refs)
        })
        && task_id.as_ref().is_none_or(|task_id| {
            !state.inflight.values().any(|effect| {
                matches!(
                    effect,
                    InflightEffect::RoleTurn {
                        task_id: Some(active),
                        ..
                    } if active == task_id
                )
            })
        })
        && !state.inflight.values().any(|effect| {
            matches!(
                effect,
                InflightEffect::RoleTurn {
                    role_instance: active,
                    ..
                } if active == role_instance
            )
        })
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
            if conversation.lifecycle == ConversationLifecycle::AwaitingLead {
                conversation.lifecycle = ConversationLifecycle::Ready;
            }
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
            settle_unavailable_delivery(state, &request.role_instance);
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
            let workspace_provenance = (output == super::OutputSemantics::ProducesArtifact)
                .then(|| {
                    request.task_id.as_ref().map(|task_id| {
                        (
                            task_id.clone(),
                            super::TaskWorkspaceProvenance {
                                effect_id: effect_id.clone(),
                                base_sha: request.base_sha.clone(),
                                assignment_epoch: request.assignment_epoch,
                                archived_effect_id: request
                                    .workspace_preparation
                                    .archived_effect()
                                    .cloned(),
                            },
                        )
                    })
                })
                .flatten();
            settle_delivery(state, &request.role_instance, true);
            if let Some(conversation) = state.conversations.get_mut(&request.role_instance) {
                conversation.final_response = Some(success.final_response.clone());
            }
            if let Some(receipt) = state.role_attempt_receipts.get_mut(effect_id) {
                receipt.runtime_configuration = Some(success.runtime_configuration.clone());
                receipt.runtime_usage = success.runtime_usage.clone();
                receipt.prepared_inputs = success.prepared_inputs.clone();
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
                if let Some((task_id, provenance)) = workspace_provenance {
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
    if output == super::OutputSemantics::ProducesArtifact
        && matches!(handoff, Handoff::Work { .. })
        && artifact.is_none()
        && request.dependency_refs.len() > 1
    {
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
                conversation.lifecycle = if conversation
                    .queued
                    .iter()
                    .any(|message| message.marker != DeliveryMarker::Undeliverable)
                {
                    ConversationLifecycle::Ready
                } else {
                    ConversationLifecycle::AwaitingLead
                };
            }
        }
        (super::OutputSemantics::ProducesArtifact, Some(Handoff::Work { .. })) => {
            if let Some(task_id) = &request.task_id {
                if let Some(task) = state.tasks.get_mut(task_id) {
                    task.status = TaskStatus::Cleared;
                    task.consecutive_failures = 0;
                    task.candidate_sha = Some(
                        success
                            .artifact
                            .as_ref()
                            .map(|artifact| artifact.head_sha.clone())
                            .unwrap_or_else(|| request.base_sha.clone()),
                    );
                    task.pending_base_sha = None;
                    task.last_outcome = Some(TaskAttemptOutcome::Accepted {
                        effect_id: effect_id.clone(),
                    });
                }
                if let Some(artifact) = &success.artifact {
                    state.reachable_commits.insert(artifact.head_sha.clone());
                }
                mark_downstream_stale(state, task_id);
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
                conversation.lifecycle = if conversation
                    .queued
                    .iter()
                    .any(|message| message.marker != DeliveryMarker::Undeliverable)
                {
                    ConversationLifecycle::Ready
                } else {
                    ConversationLifecycle::AwaitingLead
                };
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
    let max_recovery_attempts = state.config.recovery.max_attempts;
    settle_failed_delivery(state, &request.role_instance, &failure);
    if let Some(receipt) = state.role_attempt_receipts.get_mut(effect_id) {
        receipt.runtime_configuration = Some(failure.evidence().configuration.clone());
        receipt.runtime_usage = failure.evidence().runtime_usage.clone();
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
        if !failure.evidence().final_response.is_empty() {
            conversation.final_response = Some(super::PayloadRef::inline(
                failure.evidence().final_response.clone(),
            ));
        }
        if state.phase.is_terminal() {
            retire_conversation(conversation);
        } else if failure.is_invalid_output()
            && conversation.invalid_handoff_reworks < max_recovery_attempts
        {
            conversation.invalid_handoff_reworks += 1;
            conversation.lifecycle = ConversationLifecycle::ReworkingInvalidHandoff;
        } else {
            conversation.lifecycle = ConversationLifecycle::Ready;
        }
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
        conversation.queued.retain(|message| {
            (message.marker == DeliveryMarker::Undeliverable
                && !delivery.presented_messages.contains(&message.sequence_no))
                || message.sequence_no > delivery.message_boundary
        });
    }
}

fn settle_failed_delivery(
    state: &mut MissionState,
    role_instance: &RoleInstanceId,
    failure: &TypedFailure,
) {
    let Some(conversation) = state.conversations.get_mut(role_instance) else {
        return;
    };
    let Some(delivery) = conversation.active_delivery.take() else {
        return;
    };
    let observation = if failure.evidence().code.as_deref() == Some("kernel.launch") {
        DeliveryObservation::NotDelivered
    } else if failure.is_invalid_output() || !failure.evidence().final_response.is_empty() {
        DeliveryObservation::Delivered
    } else {
        DeliveryObservation::Uncertain
    };
    for message in conversation
        .queued
        .iter_mut()
        .filter(|message| delivery.presented_messages.contains(&message.sequence_no))
    {
        message.marker = match (observation, message.marker) {
            (DeliveryObservation::Delivered, DeliveryMarker::Queued)
            | (DeliveryObservation::Delivered, DeliveryMarker::PossiblyDelivered) => {
                DeliveryMarker::PreviouslyDelivered
            }
            (DeliveryObservation::Uncertain, DeliveryMarker::Queued) => {
                DeliveryMarker::PossiblyDelivered
            }
            (_, marker) => marker,
        };
    }
}

#[derive(Clone, Copy)]
enum DeliveryObservation {
    NotDelivered,
    Uncertain,
    Delivered,
}

fn settle_unavailable_delivery(state: &mut MissionState, role_instance: &RoleInstanceId) {
    let Some(conversation) = state.conversations.get_mut(role_instance) else {
        return;
    };
    let Some(delivery) = conversation.active_delivery.take() else {
        return;
    };
    for message in &mut conversation.queued {
        if delivery.presented_messages.contains(&message.sequence_no) {
            message.marker = DeliveryMarker::Undeliverable;
        }
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
        environment_digest,
        attempt_no: expected_attempt,
        ..
    }) = state.inflight.get(effect_id)
    else {
        return;
    };
    if expected_assertions != assertion_ids
        || *expected_oracle != *oracle
        || expected_sha != judged_sha
        || *expected_attempt != attempt_no
    {
        return;
    }
    let environment_digest = environment_digest.clone();
    state.inflight.remove(effect_id);
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
            state.parked_effects.retain(|_, parked| {
                !matches!(
                    parked,
                    ParkedEffect::OracleRun { oracle: parked_oracle }
                        if parked_oracle == oracle
                )
            });
            let verdict = AuthoritativeVerdict::from_oracle_success(
                oracle.clone(),
                judged_sha.to_string(),
                environment_digest,
                success,
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
    requirement_changes: &[super::RequirementId],
) {
    let Some(item) = state.open_attention.get(attention_id).cloned() else {
        return;
    };
    if justification.trim().is_empty() {
        return;
    }
    match (action, item.kind) {
        (super::DecisionAction::Approve, AttentionKind::PlanProposal) => {
            let expected = state
                .proposal
                .as_ref()
                .and_then(|proposal| proposal.plan.as_ref())
                .map(|proposal| proposal.requirement_changes.as_slice())
                .unwrap_or(&[]);
            if expected != requirement_changes {
                return;
            }
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
        (super::DecisionAction::Retry, AttentionKind::ProofBarUnmet) => {
            retry_unmet_proof(state, &item);
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
            let repair_base = state.deliverable_head().to_string();
            clear_authoritative_verdicts(state, &item.assertion_ids);
            let mut repaired_tasks = Vec::new();
            if let Some(plan) = &state.plan {
                for task in plan.tasks.iter().filter(|task| {
                    task.targets
                        .iter()
                        .any(|target| item.assertion_ids.contains(target))
                }) {
                    repaired_tasks.push(task.id.clone());
                    if let Some(runtime) = state.tasks.get_mut(&task.id) {
                        runtime.status = TaskStatus::Pending;
                        runtime.consecutive_failures = 0;
                        runtime.candidate_sha = None;
                        runtime.pending_base_sha = Some(repair_base.clone());
                        runtime.feedback.push(feedback.clone());
                    }
                }
            }
            for task_id in repaired_tasks {
                mark_downstream_stale(state, &task_id);
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
            } else if item.kind == AttentionKind::ProofBarUnmet {
                retry_unmet_proof(state, &item);
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
            let freshness = state
                .gap_review_receipt()
                .map(|receipt| match &receipt.source {
                    super::RoleEffectSource::Turn { request, .. } => request.freshness(),
                });
            if let Some(freshness) = freshness {
                state.gap_review.accepted = Some(ReviewAcceptance {
                    kind: ReviewAcceptanceKind::AcknowledgedGaps,
                    freshness,
                    justification: justification.to_string(),
                });
            }
        }
        (super::DecisionAction::Accept, AttentionKind::GapReviewFailed) => {
            let freshness = state
                .gap_review_receipt()
                .map(|receipt| match &receipt.source {
                    super::RoleEffectSource::Turn { request, .. } => request.freshness(),
                });
            if let Some(outcome) = state.gap_review.outcome.take() {
                state.parked_effects.remove(outcome.effect_id());
            }
            state.gap_review.consecutive_failures = 0;
            if let Some(freshness) = freshness {
                state.gap_review.accepted = Some(ReviewAcceptance {
                    kind: ReviewAcceptanceKind::Waived,
                    freshness,
                    justification: justification.to_string(),
                });
            }
        }
        (super::DecisionAction::Accept, _) => {
            if let Some(task_id) = &item.task_id {
                if !task_accept_candidate_is_lineage_complete(state, task_id) {
                    return;
                }
                if let Some(task) = state.tasks.get_mut(task_id) {
                    task.status = TaskStatus::Cleared;
                    if task.candidate_sha.is_none() {
                        task.candidate_sha = task
                            .role_assignment
                            .as_ref()
                            .map(|assignment| assignment.base_sha.clone());
                    }
                    task.pending_base_sha = None;
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

fn task_accept_candidate_is_lineage_complete(
    state: &MissionState,
    task_id: &super::TaskId,
) -> bool {
    if state
        .tasks
        .get(task_id)
        .and_then(|task| task.candidate_sha.as_ref())
        .is_some()
    {
        return true;
    }
    state
        .task_dependency_refs(task_id)
        .is_some_and(|dependencies| dependencies.len() <= 1)
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

fn retry_unmet_proof(state: &mut MissionState, item: &AttentionItem) {
    let failed_effects = item
        .evidence
        .role_attempts()
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    for assertion_id in &item.assertion_ids {
        let retry_confined = state
            .plan
            .as_ref()
            .is_some_and(|plan| plan.assertion_requires_confined_proof(assertion_id));
        let retry_judged = state
            .plan
            .as_ref()
            .is_some_and(|plan| plan.assertion_requires_judged_proof(assertion_id));
        let oracle = retry_confined
            .then(|| {
                state
                    .contract
                    .get(assertion_id)
                    .and_then(|assertion| assertion.oracle.clone())
            })
            .flatten();
        if let Some(assertion) = state.contract.get_mut(assertion_id) {
            if retry_judged {
                assertion
                    .last_advisory
                    .retain(|_, effect_id| !failed_effects.contains(effect_id));
            }
            if retry_confined {
                assertion.last_authoritative = None;
            }
        }
        if let Some(oracle) = oracle {
            state.waived_oracles.remove(&oracle);
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
    let prior_deliverable = state.deliverable_head().to_string();
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
    for task in state.tasks.values_mut() {
        task.status = TaskStatus::Superseded;
    }
    for task in &plan_proposal.plan.tasks {
        let runtime = state
            .tasks
            .entry(task.id.clone())
            .or_insert_with(pending_task);
        runtime.status = TaskStatus::Pending;
        if task.depends_on.is_empty()
            && runtime.candidate_sha.is_none()
            && prior_deliverable != state.base_sha
        {
            runtime.pending_base_sha = Some(prior_deliverable.clone());
        }
    }
    state.proposal = None;
    state.proposal_approved = false;
    state.phase = MissionPhase::Running;
    recompute_current_sha(state);
}

fn derive(state: &mut MissionState) {
    recompute_current_sha(state);
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
            let evidence = match &task.last_outcome {
                Some(TaskAttemptOutcome::Failed { effect_id }) => {
                    super::DecisionEvidence::RoleAttempts {
                        effect_ids: vec![effect_id.clone()],
                    }
                }
                _ => super::DecisionEvidence::None,
            };
            attention.insert(
                id.clone(),
                AttentionItem {
                    id,
                    kind: AttentionKind::NodeFailed,
                    task_id: Some(task_id.clone()),
                    oracle: None,
                    assertion_ids: Vec::new(),
                    evidence,
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
            .filter(|verdict| verdict.is_fresh_at(state) && !verdict.passed())
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
    if attention.is_empty() {
        if let Some(finish) = settled_finish_candidate(state)
            .filter(|finish| !state.config.stop.satisfied_by(*finish))
        {
            let (assertion_ids, effect_ids) = unmet_proof_evidence(state);
            let id = "proof_bar_unmet:mission".to_string();
            attention.insert(
                id.clone(),
                AttentionItem {
                    id,
                    kind: AttentionKind::ProofBarUnmet,
                    task_id: None,
                    oracle: None,
                    assertion_ids,
                    evidence: super::DecisionEvidence::RoleAttempts { effect_ids },
                    report: format!(
                        "Settled proof class '{}' is below the declared stop bar '{}'.",
                        finish.slug(),
                        state.config.stop.slug()
                    ),
                },
            );
        }
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
                            ) if request.is_fresh_at(state) => Some(
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
                    && !state
                        .gap_review
                        .acknowledges_sha(state, state.deliverable_head())
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
}

fn unmet_proof_evidence(state: &MissionState) -> (Vec<AssertionId>, Vec<super::EffectId>) {
    let Some(plan) = &state.plan else {
        return (Vec::new(), Vec::new());
    };
    let mut assertion_ids = Vec::new();
    let mut effect_ids = BTreeSet::new();
    for (assertion_id, assertion) in &state.contract {
        let confined_unmet = plan.assertion_requires_confined_proof(assertion_id)
            && assertion
                .last_authoritative
                .as_ref()
                .filter(|verdict| verdict.is_fresh_at(state))
                .is_none_or(|verdict| !verdict.passed());
        let judged_unmet = plan.assertion_requires_judged_proof(assertion_id)
            && state.advisory_status(assertion_id) != super::AdvisoryStatus::Passed;
        if !confined_unmet && !judged_unmet {
            continue;
        }
        assertion_ids.push(assertion_id.clone());
        let Some(panel) = state
            .team
            .as_ref()
            .and_then(|team| team.judgment_assignments.get(assertion_id))
        else {
            continue;
        };
        for validator in panel {
            let Some(effect_id) = assertion.last_advisory.get(validator) else {
                continue;
            };
            if state
                .advisory_receipt(assertion_id, validator, effect_id)
                .is_some_and(|(_, passed)| !passed)
            {
                effect_ids.insert(effect_id.clone());
            }
        }
    }
    (assertion_ids, effect_ids.into_iter().collect())
}

fn settled_finish_candidate(state: &MissionState) -> Option<FinishClass> {
    if state.phase.is_terminal()
        || state.plan.is_none()
        || !state.inflight.is_empty()
        || oracle_obligation_outstanding(state)
        || advisory_obligation_pending(state)
    {
        return None;
    }
    let tasks_settled = state
        .tasks
        .values()
        .filter(|task| task.status != TaskStatus::Superseded)
        .all(|task| task.status == TaskStatus::Cleared);
    tasks_settled.then(|| classify_finish(state))
}

pub fn ready_to_finish(state: &MissionState) -> Option<FinishClass> {
    if !state.open_attention.is_empty() || gap_review_outstanding(state) {
        return None;
    }
    settled_finish_candidate(state).filter(|finish| state.config.stop.satisfied_by(*finish))
}

fn advisory_obligation_pending(state: &MissionState) -> bool {
    let Some(plan) = &state.plan else {
        return false;
    };
    state.contract.keys().any(|assertion_id| {
        plan.assertion_requires_judged_proof(assertion_id)
            && state.advisory_status(assertion_id) == super::AdvisoryStatus::Pending
    })
}

pub(crate) fn oracle_obligation_outstanding(state: &MissionState) -> bool {
    let Some(plan) = &state.plan else {
        return false;
    };
    state.contract.iter().any(|(assertion_id, assertion)| {
        plan.assertion_requires_confined_proof(assertion_id)
            && assertion.oracle.as_ref().is_some_and(|oracle| {
                !state.waived_oracles.contains(oracle)
                    && assertion
                        .last_authoritative
                        .as_ref()
                        .is_none_or(|verdict| !verdict.is_fresh_at(state))
            })
    })
}

pub(crate) fn gap_review_outstanding(state: &MissionState) -> bool {
    if !state.config.requires_gap_review {
        return false;
    }
    if state.gap_review.waived_at(state)
        || state
            .gap_review
            .acknowledges_sha(state, state.deliverable_head())
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
                ) if &request.role_instance == role_id && request.is_fresh_at(state) => Some(
                    *passed
                        && gaps
                            .iter()
                            .all(|gap| gap.severity != super::GapSeverity::Blocking),
                ),
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
        candidate_sha: None,
        pending_base_sha: None,
        feedback: Vec::new(),
        role_assignment: None,
        workspace_provenance: None,
        pending_workspace_recreation: None,
    }
}

fn mark_downstream_stale(state: &mut MissionState, changed_task: &TaskId) {
    let Some(plan) = &state.plan else {
        return;
    };
    let mut descendants = BTreeSet::new();
    let mut frontier = vec![changed_task.clone()];
    while let Some(parent) = frontier.pop() {
        for task in plan
            .tasks
            .iter()
            .filter(|task| task.depends_on.contains(&parent))
        {
            if descendants.insert(task.id.clone()) {
                frontier.push(task.id.clone());
            }
        }
    }
    for task_id in descendants {
        if let Some(runtime) = state.tasks.get_mut(&task_id) {
            if runtime.status != TaskStatus::Superseded {
                runtime.status = TaskStatus::Pending;
                runtime.consecutive_failures = 0;
                runtime.last_outcome = None;
                runtime.candidate_sha = None;
                runtime.pending_base_sha = None;
            }
        }
    }
    state.gap_review = Default::default();
    recompute_current_sha(state);
}

fn recompute_current_sha(state: &mut MissionState) {
    let Some(plan) = &state.plan else {
        state.current_sha = state.base_sha.clone();
        return;
    };
    let cleared_candidates: BTreeMap<_, _> = plan
        .tasks
        .iter()
        .filter_map(|task| {
            let runtime = state.tasks.get(&task.id)?;
            (runtime.status == TaskStatus::Cleared)
                .then_some(runtime.candidate_sha.as_ref())
                .flatten()
                .map(|sha| (task.id.clone(), sha.clone()))
        })
        .collect();
    if cleared_candidates.is_empty() {
        return;
    }
    let depended_on_by_cleared: BTreeSet<_> = plan
        .tasks
        .iter()
        .filter(|task| cleared_candidates.contains_key(&task.id))
        .flat_map(|task| task.depends_on.iter().cloned())
        .filter(|dependency| cleared_candidates.contains_key(dependency))
        .collect();
    let leaves: Vec<_> = cleared_candidates
        .iter()
        .filter(|(task_id, _)| !depended_on_by_cleared.contains(*task_id))
        .map(|(_, sha)| sha.clone())
        .collect();
    state.current_sha = match leaves.as_slice() {
        [sha] => sha.clone(),
        _ => state.base_sha.clone(),
    };
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
