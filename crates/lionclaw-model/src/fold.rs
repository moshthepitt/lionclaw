//! Pure event fold for the team-owned runtime model.

use alloc::collections::btree_map::Entry;

use super::event::{ControlAction, EventEnvelope, Handoff, MissionEvent};
use super::ids::{RoleInstanceId, TaskId};
use super::state::{
    ActiveDelivery, AppliedResult, AssertionState, ConversationLifecycle, ConversationState,
    DeliveryMarker, InflightEffect, MissionState, ParkedEffect, PlanningInput, PlanningRefinement,
    RoleAttemptDisposition, SettledHandoff, TaskAttemptOutcome, TaskRoleAssignment,
    TaskRuntimeState, TaskStatus, TerminalState,
};
use super::verdict::{
    proof_readiness, AuthoritativeVerdict, ProofFailure, ProofReadiness, ProofSource,
};
use crate::prelude::*;
use crate::{TypedFailure, TypedFailureEvidence};

/// Reducer 77 folds child requests, bindings, terminal receipts, and cleanup
/// while keeping child proof separate from parent authority.
pub const REDUCER_VERSION: u32 = 77;

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
        lineage,
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
        lineage: lineage.clone(),
        team: None,
        team_history: BTreeMap::new(),
        runtime_identity_history: BTreeMap::new(),
        skills: BTreeMap::new(),
        terminal: None,
        applied_result: None,
        plan: None,
        contract: BTreeMap::new(),
        superseded_assertions: Vec::new(),
        tasks: BTreeMap::new(),
        planning_input: PlanningInput {
            latest_rejected_proposal: None,
            refinement: None,
        },
        proposal: None,
        oracles: BTreeMap::new(),
        current_sha: base_sha.clone(),
        oracle_attempts: BTreeMap::new(),
        inflight: BTreeMap::new(),
        role_attempt_receipts: BTreeMap::new(),
        conversations: BTreeMap::new(),
        retained_workspace_archives: BTreeMap::new(),
        authoritative_receipts: BTreeMap::new(),
        child_mission_receipts: BTreeMap::new(),
        cleaned_child_missions: BTreeSet::new(),
        reachable_commits: BTreeSet::from([base_sha.clone()]),
        stop_requests: BTreeMap::new(),
        reached_deadlines: BTreeMap::new(),
        parked_effects: BTreeMap::new(),
        cleanup_failure: None,
        revision: 0,
        acknowledged_gates: BTreeSet::new(),
        flagged_tasks: BTreeSet::new(),
        oracle_failures: BTreeMap::new(),
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
            if super::next(state)
                .choices
                .contains(&super::Choice::AddMissionSkill)
                && valid_skill(skill)
            {
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
            if super::next(state).choices.contains(&super::Choice::ProposePlan {
                base_revision: state.revision,
            }) && valid_proposal(state, proposal)
            {
                state.proposal = Some((**proposal).clone());
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
            spec_digest,
            judged_sha,
            environment_digest,
            attempt_no,
            effect_id,
            requested_at_ms,
            deadline_ms,
        } => {
            let spec = state
                .oracles
                .get(oracle)
                .filter(|spec| spec.digest() == *spec_digest);
            let not_before_ms = state
                .oracle_failures
                .get(oracle)
                .filter(|failure| failure.is_transient())
                .and_then(TypedFailure::next_eligible_at_ms)
                .unwrap_or(*requested_at_ms)
                .max(*requested_at_ms);
            let canonical = effect_id
                == &super::EffectId::for_oracle_request(
                    &state.mission_id,
                    oracle,
                    spec_digest,
                    judged_sha,
                    *attempt_no,
                )
                && spec.is_some()
                && judged_sha == state.deliverable_head()
                && environment_digest == state.environment_digest()
                && state.next_oracle_attempt(oracle) == Some(*attempt_no)
                && !assertion_ids.is_empty()
                && assertion_ids == &state.owed_assertions_for_oracle(oracle)
                && !state.authoritative_receipts.contains_key(effect_id)
                && spec
                    .and_then(|spec| {
                        super::resolve_execution_deadline_ms(not_before_ms, spec.timeout_secs())
                            .ok()
                    })
                    == Some(*deadline_ms);
            if canonical && !state.inflight.contains_key(effect_id) {
                state
                    .oracle_attempts
                    .entry(oracle.clone())
                    .or_default()
                    .insert(spec_digest.clone(), *attempt_no);
                state.inflight.insert(
                    effect_id.clone(),
                    InflightEffect::OracleRun {
                        assertion_ids: assertion_ids.clone(),
                        oracle: oracle.clone(),
                        spec_digest: spec_digest.clone(),
                        judged_sha: judged_sha.clone(),
                        environment_digest: environment_digest.clone(),
                        attempt_no: *attempt_no,
                        requested_at_ms: *requested_at_ms,
                        not_before_ms,
                        deadline_ms: *deadline_ms,
                        requested_seq: seq,
                    },
                );
            }
        }
        event @ MissionEvent::OracleRunCompleted { .. } => apply_oracle_outcome(state, event),
        MissionEvent::ChildMissionRequested { .. } => apply_child_request(state, envelope),
        MissionEvent::ChildMissionBound {
            effect_id,
            child_mission_id,
        } => {
            if let Some(InflightEffect::ChildMission { request, bound, .. }) =
                state.inflight.get_mut(effect_id)
            {
                if request.child_mission_id == *child_mission_id {
                    *bound = true;
                }
            }
        }
        MissionEvent::ChildMissionCompleted { effect_id, receipt } => {
            apply_child_outcome(state, effect_id, receipt)
        }
        MissionEvent::ChildMissionCleaned {
            effect_id,
            child_mission_id,
        } => {
            if super::next(state).effects.iter().any(|intent| {
                matches!(
                    intent,
                    super::EffectIntent::CleanupChildMission {
                        effect_id: legal_effect,
                        child_mission_id: legal_child,
                    } if legal_effect == effect_id && legal_child == child_mission_id
                )
            }) {
                state.cleaned_child_missions.insert(effect_id.clone());
            }
        }
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
        MissionEvent::ConversationResourcesCleaned {
            role_instance,
            effect_id,
        } => {
            if super::next(state).effects.iter().any(|intent| {
                matches!(
                    intent,
                    super::EffectIntent::CleanupConversation {
                        role_instance: legal_role,
                        effect_id: legal_effect,
                    } if legal_role == role_instance && legal_effect == effect_id
                )
            }) {
                if let Some(conversation) = state.conversations.get_mut(role_instance) {
                    conversation.disposable_resource_owner = None;
                }
            }
        }
        MissionEvent::MissionAborted { reason } => {
            if state.terminal.is_none() && !reason.trim().is_empty() {
                state.terminal = Some(TerminalState::Aborted {
                    reason: reason.clone(),
                });
                for conversation in state.conversations.values_mut() {
                    retire_conversation(conversation);
                }
            }
        }
        MissionEvent::MissionFinished { finish, reason } => {
            if state.terminal.is_none()
                && !reason.trim().is_empty()
                && super::next(state).choices.iter().any(
                    |choice| matches!(choice, super::Choice::Finish { finish: legal } if legal == finish),
                )
            {
                state.terminal = Some(TerminalState::Done { finish: *finish });
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
            if !reason.trim().is_empty()
                && super::next(state).choices.iter().any(|choice| {
                    matches!(
                        choice,
                        super::Choice::Apply {
                            branch: legal_branch,
                            sha: legal_sha,
                        } if legal_branch == branch && legal_sha == sha
                    )
                })
            {
                state.applied_result = Some(AppliedResult {
                    branch: branch.clone(),
                    sha: sha.clone(),
                    reason: reason.clone(),
                });
            }
        }
        MissionEvent::DecisionRecorded {
            attention_id,
            action,
            justification,
            requirement_changes,
            proposal_runtime_identities,
        } => apply_decision(
            state,
            attention_id,
            action,
            justification,
            requirement_changes,
            proposal_runtime_identities,
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
    recompute_current_sha(state);
}

fn apply_child_request(state: &mut MissionState, envelope: &EventEnvelope) {
    let MissionEvent::ChildMissionRequested {
        request,
        requested_at_ms,
        deadline_ms,
        budget_deadline_ms,
    } = &envelope.event
    else {
        return;
    };
    if request.parent_mission_id != state.mission_id
        || !super::next(state)
            .effects
            .contains(&super::EffectIntent::ChildMission((**request).clone()))
        || state.inflight.contains_key(&request.parent_effect_id)
        || state
            .child_mission_receipts
            .contains_key(&request.parent_effect_id)
    {
        return;
    }
    let initial_secs = state
        .config
        .execution
        .default_timeout_secs
        .min(request.assignment.deadline_secs);
    if super::resolve_execution_deadline_ms(*requested_at_ms, initial_secs) != Ok(*deadline_ms)
        || super::resolve_execution_deadline_ms(*requested_at_ms, request.assignment.deadline_secs)
            != Ok(*budget_deadline_ms)
    {
        return;
    }
    let Some((effect_id, inflight)) = InflightEffect::from_request(
        &envelope.event,
        envelope.sequence_no,
        &state.team_history,
        *requested_at_ms,
    ) else {
        return;
    };
    let task = state
        .tasks
        .entry(request.task_id.clone())
        .or_insert_with(pending_task);
    task.status = TaskStatus::Running;
    task.attempts = request.attempt_no;
    state.inflight.insert(effect_id, inflight);
}

fn apply_child_outcome(
    state: &mut MissionState,
    effect_id: &super::EffectId,
    receipt: &super::ChildMissionReceipt,
) {
    let Some(InflightEffect::ChildMission { request, bound, .. }) = state.inflight.get(effect_id)
    else {
        return;
    };
    if !*bound
        || !receipt.matches_request(request)
        || state.child_mission_receipts.contains_key(effect_id)
    {
        return;
    }
    let request = (**request).clone();
    let success_candidate = if receipt.succeeded() {
        match receipt.output.as_ref() {
            Some(super::ChildMissionOutput::Artifact { artifact })
                if artifact.base_sha == request.input_artifact =>
            {
                Some(artifact.head_sha.clone())
            }
            Some(super::ChildMissionOutput::Report {
                report,
                report_sha256,
            }) if report.content_sha256().as_deref() == Some(report_sha256) => {
                Some(request.input_artifact.clone())
            }
            Some(_) | None => None,
        }
    } else {
        None
    };
    state.inflight.remove(effect_id);
    state
        .child_mission_receipts
        .insert(effect_id.clone(), receipt.clone());
    if let Some(candidate) = success_candidate {
        clear_task_with_candidate(state, &request.task_id, effect_id, candidate);
    } else if let Some(task) = state.tasks.get_mut(&request.task_id) {
        task.status = TaskStatus::Failed;
        task.consecutive_failures = task.consecutive_failures.saturating_add(1);
        task.last_outcome = Some(TaskAttemptOutcome::Failed {
            effect_id: effect_id.clone(),
        });
    }
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
    let authorized = team_revision.is_some_and(|revision| {
        super::next(state)
            .choices
            .contains(&super::Choice::AssignEnvironment {
                team_revision: revision,
            })
    });
    if !authorized
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
    let plan = state.plan.as_ref();
    if !super::next(state)
        .choices
        .contains(&super::Choice::ConfigureTeam { revision: expected })
        || team.revision != expected
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
        || plan.is_some_and(|plan| {
            !super::validate_plan(plan, team, &state.oracles, &state.config).is_empty()
        })
    {
        return;
    }
    replace_team(state, team, runtime_identities);
}

fn replace_team(
    state: &mut MissionState,
    team: &super::TeamRevision,
    runtime_identities: &BTreeMap<RoleInstanceId, super::RuntimeInstrumentIdentity>,
) {
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
        report_refs,
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
        && *prompt_template
            == super::role_assignment_prompt_template(role.output, task_id.is_some())
        && environment_digest == state.environment_digest()
        && state.role_instrument_identity_for_revision(role_instance, *team_revision)
            == Some(instrument_identity.clone())
        && state.role_dispatch_contract_matches(
            role_instance,
            *team_revision,
            task_id.as_ref(),
            assertion_ids,
        )
        && state.role_report_refs_match(
            role_instance,
            *team_revision,
            task_id.as_ref(),
            assertion_ids,
            report_refs,
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
            disposable_resource_owner: None,
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
    conversation.disposable_resource_owner = Some(effect_id.clone());
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
                let candidate_sha = success
                    .artifact
                    .as_ref()
                    .map(|artifact| artifact.head_sha.clone())
                    .unwrap_or_else(|| request.base_sha.clone());
                clear_task_with_candidate(state, task_id, effect_id, candidate_sha);
                if let Some(artifact) = &success.artifact {
                    state.reachable_commits.insert(artifact.head_sha.clone());
                }
            }
            retire_role_conversation(state, &request.role_instance);
        }
        (super::OutputSemantics::ProposesPlan, Some(Handoff::Plan { proposal, .. })) => {
            if let Some(proposal) = proposal {
                if valid_proposal(state, proposal) {
                    state.proposal = Some((**proposal).clone());
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
        (super::OutputSemantics::EmitsGapVerdict, Some(Handoff::Review { passed, gaps, .. })) => {
            let obsolete: Vec<_> = state
                .parked_effects
                .iter()
                .filter_map(|(parked_id, parked)| {
                    matches!(
                        parked,
                        ParkedEffect::RoleTurn {
                            role_instance,
                            task_id: None,
                        } if role_instance == &request.role_instance
                    )
                    .then_some(parked_id.clone())
                })
                .collect();
            for parked_id in obsolete {
                state.parked_effects.remove(&parked_id);
            }
            if !passed
                || gaps
                    .iter()
                    .any(|gap| gap.severity == super::GapSeverity::Blocking)
            {
                state.parked_effects.insert(
                    effect_id.clone(),
                    ParkedEffect::RoleTurn {
                        role_instance: request.role_instance.clone(),
                        task_id: None,
                    },
                );
            }
            retire_role_conversation(state, &request.role_instance);
        }
        (super::OutputSemantics::ProducesReport, Some(Handoff::Work { .. })) => {
            if let Some(task_id) = &request.task_id {
                clear_task_with_candidate(state, task_id, effect_id, request.base_sha.clone());
            }
            retire_role_conversation(state, &request.role_instance);
        }
        _ => {}
    }
}

fn clear_task_with_candidate(
    state: &mut MissionState,
    task_id: &super::TaskId,
    effect_id: &super::EffectId,
    candidate_sha: String,
) {
    if let Some(task) = state.tasks.get_mut(task_id) {
        task.status = TaskStatus::Cleared;
        task.consecutive_failures = 0;
        task.candidate_sha = Some(candidate_sha);
        task.pending_base_sha = None;
        task.last_outcome = Some(TaskAttemptOutcome::Accepted {
            effect_id: effect_id.clone(),
        });
    }
    mark_downstream_stale(state, task_id);
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
    state.parked_effects.insert(
        effect_id.clone(),
        ParkedEffect::RoleTurn {
            role_instance: request.role_instance.clone(),
            task_id: request.task_id.clone(),
        },
    );
    let terminal = state.is_terminal();
    if let Some(conversation) = state.conversations.get_mut(&request.role_instance) {
        if !failure.evidence().final_response.is_empty() {
            conversation.final_response = Some(super::PayloadRef::inline(
                failure.evidence().final_response.clone(),
            ));
        }
        if terminal {
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

fn apply_oracle_outcome(state: &mut MissionState, event: &MissionEvent) {
    let MissionEvent::OracleRunCompleted {
        assertion_ids,
        oracle,
        spec_digest,
        judged_sha,
        attempt_no,
        effect_id,
        outcome,
    } = event
    else {
        unreachable!("oracle outcome fold only receives completion events");
    };
    let Some(InflightEffect::OracleRun {
        assertion_ids: expected_assertions,
        oracle: expected_oracle,
        spec_digest: expected_spec_digest,
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
        || expected_spec_digest != spec_digest
        || expected_sha != judged_sha
        || *expected_attempt != *attempt_no
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
            let verdict = AuthoritativeVerdict::from_oracle_success(
                assertion_ids.to_vec(),
                oracle.clone(),
                spec_digest.to_string(),
                judged_sha.to_string(),
                environment_digest,
                *attempt_no,
                success,
            );
            let Entry::Vacant(receipt) = state.authoritative_receipts.entry(effect_id.clone())
            else {
                return;
            };
            receipt.insert(verdict);
            state.oracle_failures.remove(oracle);
            state.parked_effects.retain(|_, parked| {
                !matches!(
                    parked,
                    ParkedEffect::OracleRun { oracle: parked_oracle }
                        if parked_oracle == oracle
                )
            });
            for assertion_id in assertion_ids {
                if let Some(assertion) = state.contract.get_mut(assertion_id) {
                    if assertion.oracle.as_ref() == Some(oracle) {
                        assertion.last_authoritative_receipt = Some(effect_id.clone());
                    }
                }
            }
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
    let requires_choice = match action {
        ControlAction::Stop => true,
        ControlAction::ExtendDeadline { automatic, .. }
        | ControlAction::Continue { automatic, .. } => !automatic,
        ControlAction::DeadlineReached { .. } => false,
    };
    if requires_choice
        && !super::next(state)
            .choices
            .iter()
            .any(|choice| choice.authorizes_control(effect_id, action))
    {
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
    decision_id: &str,
    action: &super::DecisionAction,
    justification: &str,
    requirement_changes: &[super::RequirementId],
    proposal_runtime_identities: &BTreeMap<RoleInstanceId, super::RuntimeInstrumentIdentity>,
) {
    if super::validate_decision(state, decision_id, action, justification).is_err() {
        return;
    }

    if decision_id == "mission" && action == &super::DecisionAction::Revise {
        if !proposal_runtime_identities.is_empty() {
            return;
        }
        state.proposal = None;
        state.planning_input.refinement =
            Some(PlanningRefinement::Guidance(justification.to_string()));
        ready_planning_conversation(state);
        return;
    }

    if decision_id == "plan_proposal:mission" {
        match action {
            super::DecisionAction::Approve => {
                let expected = state
                    .proposal
                    .as_ref()
                    .and_then(|proposal| proposal.plan.as_ref())
                    .map(|proposal| proposal.requirement_changes.as_slice())
                    .unwrap_or(&[]);
                if expected != requirement_changes {
                    return;
                }
                accept_mission_proposal(state, proposal_runtime_identities);
            }
            super::DecisionAction::Revise => {
                if !proposal_runtime_identities.is_empty() {
                    return;
                }
                state.planning_input.latest_rejected_proposal = state.proposal.take();
                state.planning_input.refinement =
                    Some(PlanningRefinement::Guidance(justification.to_string()));
                ready_planning_conversation(state);
            }
            super::DecisionAction::Retry
            | super::DecisionAction::Repair
            | super::DecisionAction::Accept => {}
        }
        return;
    }

    if !proposal_runtime_identities.is_empty() {
        return;
    }

    if let Some((task_id, effect_id)) = failed_node(state, decision_id) {
        match action {
            super::DecisionAction::Retry => {
                retry_failed_node(state, task_id.as_ref(), effect_id.as_ref());
            }
            super::DecisionAction::Revise => {
                revise_from_failure(state, decision_id, justification);
            }
            super::DecisionAction::Accept => {
                if let Some(task_id) = task_id {
                    if let Some(task) = state.tasks.get_mut(&task_id) {
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
            }
            super::DecisionAction::Approve | super::DecisionAction::Repair => {}
        }
        return;
    }

    if let Some(oracle) = failed_oracle(state, decision_id) {
        match action {
            super::DecisionAction::Retry => retry_failed_oracle(state, &oracle),
            super::DecisionAction::Revise => {
                revise_from_failure(state, decision_id, justification);
            }
            super::DecisionAction::Approve
            | super::DecisionAction::Repair
            | super::DecisionAction::Accept => {}
        }
        return;
    }

    if let Some(failure) = proof_failure(state, decision_id) {
        match action {
            super::DecisionAction::Retry | super::DecisionAction::Repair => {
                apply_proof_recovery(state, &failure, action, justification);
            }
            super::DecisionAction::Revise => {
                revise_from_failure(state, decision_id, justification);
            }
            super::DecisionAction::Approve | super::DecisionAction::Accept => {}
        }
    }
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

fn failed_node(
    state: &MissionState,
    decision_id: &str,
) -> Option<(Option<TaskId>, Option<super::EffectId>)> {
    for (task_id, task) in &state.tasks {
        if super::workflow::task_failure_id(task_id) != decision_id {
            continue;
        }
        let effect_id = match &task.last_outcome {
            Some(TaskAttemptOutcome::Failed { effect_id }) => Some(effect_id.clone()),
            Some(TaskAttemptOutcome::Accepted { .. }) | None => None,
        };
        return Some((Some(task_id.clone()), effect_id));
    }
    let team = state.team.as_ref()?;
    let mut assignments = vec![(team.planning_assignment.clone(), Vec::new())];
    for (assertion_id, panel) in &team.judgment_assignments {
        for role_instance in panel {
            assignments.push((role_instance.clone(), vec![assertion_id.clone()]));
        }
    }
    for (role_instance, assertion_ids) in assignments {
        if super::workflow::role_failure_id(&role_instance, &assertion_ids) != decision_id {
            continue;
        }
        let (effect_id, _, _) =
            state.taskless_assignment_failure(&role_instance, &assertion_ids)?;
        return Some((None, Some(effect_id.clone())));
    }
    None
}

fn retry_failed_node(
    state: &mut MissionState,
    task_id: Option<&TaskId>,
    effect_id: Option<&super::EffectId>,
) {
    if let Some(task_id) = task_id {
        if let Some(task) = state.tasks.get_mut(task_id) {
            task.status = TaskStatus::Pending;
            task.consecutive_failures = 0;
        }
    }
    if let Some(effect_id) = effect_id {
        if let Some(ParkedEffect::RoleTurn { role_instance, .. }) =
            state.parked_effects.remove(effect_id)
        {
            if let Some(conversation) = state.conversations.get_mut(&role_instance) {
                conversation.lifecycle = ConversationLifecycle::Ready;
            }
        }
    }
}

fn failed_oracle(state: &MissionState, decision_id: &str) -> Option<super::OracleName> {
    state
        .oracle_failures
        .keys()
        .find(|oracle| super::workflow::oracle_failure_id(oracle) == decision_id)
        .cloned()
}

fn retry_failed_oracle(state: &mut MissionState, oracle: &super::OracleName) {
    state.oracle_failures.remove(oracle);
    state.parked_effects.retain(
        |_, parked| !matches!(parked, ParkedEffect::OracleRun { oracle: parked } if parked == oracle),
    );
}

fn proof_failure(state: &MissionState, decision_id: &str) -> Option<ProofFailure> {
    let ProofReadiness::Failed(failures) = proof_readiness(state) else {
        return None;
    };
    failures
        .into_iter()
        .find(|failure| super::workflow::proof_failure_id(failure) == decision_id)
}

fn apply_proof_recovery(
    state: &mut MissionState,
    failure: &ProofFailure,
    action: &super::DecisionAction,
    justification: &str,
) {
    let feedback = proof_failure_feedback(state, failure, justification);
    clear_failed_proof(state, failure);
    if action != &super::DecisionAction::Repair {
        return;
    }

    let repair_base = state.deliverable_head().to_string();
    let repaired_tasks: Vec<_> = state.plan.as_ref().map_or_else(Vec::new, |plan| {
        if matches!(
            failure,
            ProofFailure::Receipt {
                source: ProofSource::Review { .. },
                ..
            }
        ) {
            deliverable_sink(plan).into_iter().collect()
        } else {
            plan.tasks
                .iter()
                .filter(|task| {
                    task.targets
                        .iter()
                        .any(|target| failure.assertion_ids().contains(target))
                })
                .map(|task| task.id.clone())
                .collect()
        }
    });
    for task_id in &repaired_tasks {
        if let Some(runtime) = state.tasks.get_mut(task_id) {
            runtime.status = TaskStatus::Pending;
            runtime.consecutive_failures = 0;
            runtime.candidate_sha = None;
            runtime.pending_base_sha = Some(repair_base.clone());
            runtime.feedback.push(feedback.clone());
        }
    }
    for task_id in repaired_tasks {
        mark_downstream_stale(state, &task_id);
    }
}

fn deliverable_sink(plan: &super::Plan) -> Option<TaskId> {
    let depended_on: BTreeSet<_> = plan
        .tasks
        .iter()
        .flat_map(|task| task.depends_on.iter())
        .collect();
    plan.tasks
        .iter()
        .find(|task| !depended_on.contains(&task.id))
        .map(|task| task.id.clone())
}

fn revise_from_failure(state: &mut MissionState, decision_id: &str, justification: &str) {
    let feedback = revision_feedback(state, decision_id, justification);
    if let Some((task_id, effect_id)) = failed_node(state, decision_id) {
        retry_failed_node(state, task_id.as_ref(), effect_id.as_ref());
    } else if let Some(oracle) = failed_oracle(state, decision_id) {
        retry_failed_oracle(state, &oracle);
    } else if proof_failure(state, decision_id).is_some() {
        let ProofReadiness::Failed(failures) = proof_readiness(state) else {
            return;
        };
        for failure in &failures {
            clear_failed_proof(state, failure);
        }
    }
    state.proposal = None;
    state.planning_input.refinement = Some(PlanningRefinement::FailureEvidence(feedback));
    ready_planning_conversation(state);
}

fn revision_feedback(
    state: &MissionState,
    decision_id: &str,
    justification: &str,
) -> Vec<super::FailureFeedback> {
    let mut feedback = match &state.planning_input.refinement {
        Some(PlanningRefinement::FailureEvidence(feedback)) => feedback.clone(),
        Some(PlanningRefinement::Guidance(_)) | None => Vec::new(),
    };
    if proof_failure(state, decision_id).is_some() {
        if let ProofReadiness::Failed(failures) = proof_readiness(state) {
            feedback.extend(
                failures
                    .iter()
                    .map(|failure| proof_failure_feedback(state, failure, justification)),
            );
        }
        return feedback;
    }
    if let Some(item) = decision_feedback(state, decision_id, justification) {
        feedback.push(item);
    }
    feedback
}

fn decision_feedback(
    state: &MissionState,
    decision_id: &str,
    justification: &str,
) -> Option<super::FailureFeedback> {
    if let Some((task_id, effect_id)) = failed_node(state, decision_id) {
        let summary = task_id.as_ref().map_or_else(
            || "A taskless role assignment is parked.".to_string(),
            |task_id| format!("Task '{task_id}' is parked."),
        );
        let evidence = effect_id.map_or(super::DecisionEvidence::None, |effect_id| {
            super::DecisionEvidence::RoleAttempts {
                effect_ids: vec![effect_id],
            }
        });
        return Some(super::FailureFeedback {
            summary,
            evidence,
            justification: justification.to_string(),
        });
    }
    if let Some(oracle) = failed_oracle(state, decision_id) {
        let failure = state.oracle_failures.get(&oracle)?.clone();
        return Some(super::FailureFeedback {
            summary: format!("Oracle '{oracle}' is parked."),
            evidence: super::DecisionEvidence::OracleRuntimeFailure { failure },
            justification: justification.to_string(),
        });
    }
    None
}

fn proof_failure_feedback(
    state: &MissionState,
    failure: &ProofFailure,
    justification: &str,
) -> super::FailureFeedback {
    let (summary, evidence) = match failure {
        ProofFailure::Receipt {
            source: ProofSource::Command { oracle, .. },
            effect_id,
        } => (
            format!("Required command proof '{oracle}' failed."),
            super::DecisionEvidence::AuthoritativeReceipts {
                effect_ids: vec![effect_id.clone()],
            },
        ),
        ProofFailure::Receipt {
            source: ProofSource::Judgment { role_instance, .. },
            effect_id,
        } => (
            format!("Required judgment by '{role_instance}' failed."),
            super::DecisionEvidence::RoleAttempts {
                effect_ids: vec![effect_id.clone()],
            },
        ),
        ProofFailure::Receipt {
            source: ProofSource::Review { role_instance },
            effect_id,
        } => (
            format!("Required review by '{role_instance}' found blocking gaps or failed to run."),
            super::DecisionEvidence::RoleAttempts {
                effect_ids: vec![effect_id.clone()],
            },
        ),
        ProofFailure::StopBar { finish, .. } => (
            format!(
                "Settled proof class '{}' is below the declared stop bar '{}'.",
                finish.slug(),
                state.config.stop.slug()
            ),
            super::DecisionEvidence::None,
        ),
    };
    super::FailureFeedback {
        summary,
        evidence,
        justification: justification.to_string(),
    }
}

fn clear_failed_proof(state: &mut MissionState, failure: &ProofFailure) {
    let ProofFailure::Receipt { source, effect_id } = failure else {
        return;
    };
    if let ProofSource::Review { role_instance } = source {
        state.parked_effects.remove(effect_id);
        if let Some(conversation) = state.conversations.get_mut(role_instance) {
            conversation.lifecycle = ConversationLifecycle::Ready;
            conversation.active_delivery = None;
        }
        return;
    }
    for assertion_id in source.assertion_ids() {
        let Some(assertion) = state.contract.get_mut(assertion_id) else {
            continue;
        };
        match source {
            ProofSource::Command { .. }
                if assertion.last_authoritative_receipt.as_ref() == Some(effect_id) =>
            {
                assertion.last_authoritative_receipt = None;
            }
            ProofSource::Judgment { role_instance, .. }
                if assertion.last_advisory.get(role_instance) == Some(effect_id) =>
            {
                assertion.last_advisory.remove(role_instance);
            }
            ProofSource::Command { .. } | ProofSource::Judgment { .. } => {}
            ProofSource::Review { .. } => unreachable!("review handled above"),
        }
    }
}

fn accept_mission_proposal(
    state: &mut MissionState,
    runtime_identities: &BTreeMap<RoleInstanceId, super::RuntimeInstrumentIdentity>,
) {
    let Some(proposal) = state.proposal.clone() else {
        return;
    };
    if super::validate_mission_proposal(state, &proposal).is_err() {
        return;
    }
    match &proposal.team {
        Some(team) if !runtime_identities_match_team(team, runtime_identities) => return,
        None if !runtime_identities.is_empty() => return,
        Some(_) | None => {}
    }

    state.planning_input.latest_rejected_proposal = None;
    state.planning_input.refinement = None;
    if let Some(team) = &proposal.team {
        replace_team(state, team, runtime_identities);
    }
    if let Some(plan_proposal) = proposal.plan {
        promote_plan(state, plan_proposal, proposal.oracles);
    } else {
        state.proposal = None;
    }
}

fn promote_plan(
    state: &mut MissionState,
    plan_proposal: super::PlanProposal,
    oracles: Option<BTreeMap<super::OracleName, super::OracleSpec>>,
) {
    let Some(_team) = state.team.as_ref() else {
        return;
    };
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
    if let Some(oracles) = oracles {
        state.oracles = oracles;
    }
    state.parked_effects.clear();
    state.oracle_failures.clear();
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
                    last_authoritative_receipt: None,
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
    recompute_current_sha(state);
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
