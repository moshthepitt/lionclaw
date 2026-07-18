//! The pure fold: `state = fold(log)`. No I/O, no clock, no RNG — enforced
//! by the model's dependency wall and the crate clippy config. Every
//! engine-deterministic transition happens here; the phase (including the
//! finish class) is re-derived after every event, never stored.
//!
//! Handoff application semantics ported from Zenith (Apache-2.0,
//! Intelligent Internet) `coordinator.py::_apply_handoff_collect`: a work
//! task that isn't `done` fails and raises attention; a validate task always
//! clears and folds its per-assertion verdicts in with sticky passes.

use super::event::{
    ControlAction, EventEnvelope, GapSeverity, Handoff, MissionEvent, PayloadRef,
    RuntimeConfigurationEvidence,
};
use super::ids::{AssertionId, TaskId};
use super::plan::Assertion;
use super::state::{
    AdvisoryStatus, AssertionState, AttentionItem, AttentionKind, InflightEffect, MissionPhase,
    MissionState, ParkedEffect, PlanningInput, PlanningRefinement, PlanningState, ReviewAcceptance,
    ReviewAcceptanceKind, ReviewOutcome, TaskAddress, TaskRuntimeState, TaskStatus,
    TerminalReviewVerdict,
};
use super::verdict::{classify_finish, AuthoritativeVerdict};
use crate::prelude::*;
use crate::{RoleName, TypedFailure};

/// Bump when fold semantics change; snapshots with a different version are
/// discarded and rebuilt from sequence zero.
/// Bumped because durable request ingress now verifies model-derived effect
/// identity and task-assignment generation before reserving an effect.
pub const REDUCER_VERSION: u32 = 25;

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
        conversations: Default::default(),
        authoritative_receipts: Default::default(),
        reachable_commits: BTreeSet::from([base_sha.clone()]),
        stop_requests: Default::default(),
        reached_deadlines: Default::default(),
        parked_effects: Default::default(),
        cleanup_failure: None,
        open_attention: Default::default(),
        proposal_approved: false,
        revision: 0,
        acknowledged_gates: Default::default(),
        flagged_tasks: Default::default(),
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
    if let Some(effect_id) = envelope.event.outcome_effect_id() {
        let Some(effect) = state.inflight.get(effect_id).cloned() else {
            finish_apply(state, seq);
            return;
        };
        if !effect.matches_outcome(&envelope.event) {
            finish_apply(state, seq);
            return;
        }
        if let Some(cancellation) = state.durable_cancellation(effect_id) {
            let already_classified = envelope
                .event
                .outcome_failure()
                .is_some_and(|failure| cancellation.matches_failure(failure));
            if !already_classified {
                let evidence = envelope
                    .event
                    .outcome_failure_evidence()
                    .unwrap_or_default();
                let final_response = envelope.event.outcome_final_response();
                settle_effect_failure(
                    state,
                    effect_id,
                    effect,
                    cancellation.into_failure(evidence),
                    final_response,
                );
                finish_apply(state, seq);
                return;
            }
        }
    }
    match &envelope.event {
        MissionEvent::MissionCreated { .. } => {}
        MissionEvent::PlanProposed { proposal, .. } => {
            if super::plan_validation::validate_plan_proposal(state, proposal).is_ok() {
                state.proposal = Some(proposal.clone());
                state.planning_base_revision = None;
                state.proposal_approved = false;
            }
        }
        MissionEvent::RoleRunRequested {
            conversation_id,
            namespace,
            task_id,
            attempt_no,
            role,
            base_sha,
            assignment_epoch,
            message_boundary,
            presented_messages,
            effect_id,
            ..
        } => {
            if !role_request_matches_dispatch(state, envelope) {
                finish_apply(state, seq);
                return;
            }
            state.parked_effects.retain(|_, parked| {
                !matches!(parked, ParkedEffect::RoleRun { namespace: parked_namespace, task_id: parked_task } if parked_namespace == namespace && parked_task == task_id)
            });
            let task = state
                .tasks_in_mut(*namespace)
                .entry(task_id.clone())
                .or_insert_with(pending_task);
            task.status = TaskStatus::Running;
            task.attempts = *attempt_no;
            let expected_conversation_id = crate::ConversationId::for_role_instance(
                &state.mission_id,
                *namespace,
                task_id,
                role,
                *assignment_epoch,
            );
            let conversation = state
                .conversations
                .entry(expected_conversation_id.clone())
                .or_insert_with(|| super::state::ConversationState {
                    role: role.clone(),
                    namespace: *namespace,
                    task_id: task_id.clone(),
                    assignment_epoch: *assignment_epoch,
                    workspace_base_sha: base_sha.clone(),
                    lifecycle: super::state::ConversationLifecycle::Ready,
                    queued: Vec::new(),
                    consumed_through: 0,
                    active_delivery: None,
                    invalid_handoff_reworks: 0,
                });
            let expected_presented: Vec<_> = conversation
                .queued
                .iter()
                .filter(|message| message.sequence_no <= *message_boundary)
                .map(|message| message.sequence_no)
                .collect();
            if conversation_id != &expected_conversation_id
                || *message_boundary != seq.saturating_sub(1)
                || presented_messages != &expected_presented
            {
                finish_apply(state, seq);
                return;
            }
            conversation.lifecycle = super::state::ConversationLifecycle::Running;
            conversation.active_delivery = Some(super::state::ActiveDelivery {
                effect_id: effect_id.clone(),
                message_boundary: *message_boundary,
                presented_messages: presented_messages.clone(),
            });
            track_inflight(state, &envelope.event, seq);
        }
        MissionEvent::MessageSent {
            recipients,
            body,
            references,
        } => {
            if body.len() > crate::MAX_MESSAGE_BYTES
                || recipients.is_empty()
                || recipients.len() > crate::MAX_MESSAGE_RECIPIENTS
                || references.len() > crate::MAX_MESSAGE_REFERENCES
                || recipients
                    .iter()
                    .any(|recipient| !recipient.validate(&state.mission_id))
                || references.iter().any(|reference| match reference {
                    super::event::MessageReference::AuthoritativeReceipt { effect_id } => {
                        !state.authoritative_receipts.contains(effect_id)
                    }
                    super::event::MessageReference::ParkEvidence { effect_id } => {
                        !state.parked_effects.contains_key(effect_id)
                    }
                    super::event::MessageReference::ReachableCommit { sha } => {
                        !state.reachable_commits.contains(sha)
                    }
                })
            {
                finish_apply(state, seq);
                return;
            }
            let unique: BTreeSet<_> = recipients.iter().map(|r| &r.conversation_id).collect();
            if unique.len() != recipients.len()
                || recipients.iter().any(|r| {
                    !state
                        .conversations
                        .get(&r.conversation_id)
                        .is_some_and(|c| {
                            c.role == r.role
                                && c.namespace == r.namespace
                                && c.task_id == r.task_id
                                && c.assignment_epoch == r.assignment_epoch
                                && c.lifecycle != super::state::ConversationLifecycle::Completed
                        })
                })
            {
                finish_apply(state, seq);
                return;
            }
            let mut resumed = Vec::new();
            for recipient in recipients {
                let conversation = state
                    .conversations
                    .get_mut(&recipient.conversation_id)
                    .expect("validated above");
                conversation.queued.push(super::state::QueuedMessage {
                    sequence_no: seq,
                    body: body.clone(),
                    references: references.clone(),
                    marker: super::state::DeliveryMarker::Queued,
                });
                if conversation.lifecycle == super::state::ConversationLifecycle::AwaitingLead
                    || conversation.lifecycle
                        == super::state::ConversationLifecycle::ReworkingInvalidHandoff
                {
                    conversation.lifecycle = super::state::ConversationLifecycle::Ready;
                    resumed.push((conversation.namespace, conversation.task_id.clone()));
                }
            }
            for (namespace, task_id) in resumed {
                if let Some(task) = state.tasks_in_mut(namespace).get_mut(&task_id) {
                    task.status = TaskStatus::Pending;
                }
            }
        }
        MissionEvent::TaskWorkspacePrepared {
            task_id,
            effect_id,
            base_sha,
            assignment_epoch,
        } => {
            let namespace = state
                .inflight
                .get(effect_id)
                .and_then(|effect| match effect {
                    InflightEffect::RoleRun {
                        namespace,
                        task_id: active_task,
                        base_sha: active_base,
                        assignment_epoch: active_epoch,
                        ..
                    } if active_task == task_id
                        && active_base == base_sha
                        && active_epoch == assignment_epoch =>
                    {
                        Some(*namespace)
                    }
                    _ => None,
                });
            if let Some(namespace) = namespace {
                if let Some(task) = state.tasks_in_mut(namespace).get_mut(task_id) {
                    task.workspace_base_sha = Some(base_sha.clone());
                    task.assignment_epoch = *assignment_epoch;
                }
            }
        }
        MissionEvent::EffectRuntimeConfigured {
            effect_id,
            configuration,
        } => {
            let role_task = state
                .inflight
                .get_mut(effect_id)
                .and_then(|effect| match effect {
                    InflightEffect::RoleRun {
                        namespace,
                        task_id,
                        runtime_configuration,
                        ..
                    } => {
                        *runtime_configuration = Some(configuration.clone());
                        Some((*namespace, task_id.clone()))
                    }
                    InflightEffect::TerminalReview {
                        runtime_configuration,
                        ..
                    } => {
                        *runtime_configuration = Some(configuration.clone());
                        None
                    }
                    InflightEffect::OracleRun { .. } => None,
                });
            if let Some((namespace, task_id)) = role_task {
                if let Some(task) = state.tasks_in_mut(namespace).get_mut(&task_id) {
                    task.last_runtime_configuration = Some(configuration.clone());
                }
            }
        }
        MissionEvent::RoleRunCompleted {
            effect_id,
            request,
            outcome,
        } => {
            let namespace = &request.namespace;
            let task_id = &request.task_id;
            let attempt_no = &request.attempt_no;
            let (observed_configuration, expected_contract) = state
                .inflight
                .get(effect_id)
                .and_then(|effect| {
                    if let InflightEffect::RoleRun {
                        output,
                        base_sha,
                        runtime_configuration,
                        ..
                    } = effect
                    {
                        Some((runtime_configuration.clone(), (*output, base_sha.clone())))
                    } else {
                        None
                    }
                })
                .map_or((None, None), |(configuration, contract)| {
                    (configuration, Some(contract))
                });
            state.inflight.remove(effect_id);
            state.stop_requests.remove(effect_id);
            state.reached_deadlines.remove(effect_id);
            clear_cleanup_failure(state, effect_id);
            match outcome {
                Ok(success) => {
                    let contract_error = if success.final_response.declared_len()
                        > crate::MAX_FINAL_RESPONSE_BYTES
                    {
                        Some("role final response exceeds the durable bound")
                    } else if success.handoff.is_none() && success.artifact.is_some() {
                        Some("a checkpoint without a handoff cannot return an artifact")
                    } else {
                        success.handoff.as_ref().and_then(|handoff| {
                            expected_contract.as_ref().map_or(
                                Some("role outcome has no active request"),
                                |(output, base_sha)| {
                                    super::event::role_success_contract_error(
                                        *output,
                                        handoff,
                                        success.artifact.as_ref(),
                                        base_sha,
                                    )
                                },
                            )
                        })
                    };
                    if contract_error.is_none() {
                        if let Some(artifact) = &success.artifact {
                            state.current_sha = artifact.head_sha.clone();
                            state.reachable_commits.insert(artifact.head_sha.clone());
                        }
                        if let Some(task) = state.tasks_in_mut(*namespace).get_mut(task_id) {
                            task.attempts = task.attempts.max(*attempt_no);
                        }
                        if let Some(handoff) = &success.handoff {
                            apply_handoff(state, *namespace, task_id, handoff);
                        }
                        if let Some(task) = state.tasks_in_mut(*namespace).get_mut(task_id) {
                            task.last_runtime_configuration =
                                Some(success.runtime_configuration.clone());
                            task.final_response = Some(success.final_response.clone());
                            if task.status == TaskStatus::Failed {
                                task.consecutive_failures =
                                    task.consecutive_failures.saturating_add(1);
                            } else {
                                task.last_failure = None;
                                task.consecutive_failures = 0;
                            }
                        }
                        settle_conversation_delivery(
                            state,
                            *namespace,
                            task_id,
                            effect_id,
                            success.handoff.is_some(),
                        );
                    } else {
                        let mut failure = TypedFailure::invalid(
                            "role.success_contract",
                            contract_error.unwrap_or("role success contract mismatch"),
                        );
                        failure.evidence_mut().configuration =
                            success.runtime_configuration.clone();
                        apply_role_failure(
                            state,
                            TaskAddress::new(*namespace, task_id.clone()),
                            *attempt_no,
                            effect_id,
                            failure,
                            observed_configuration.as_ref(),
                            Some(&success.final_response),
                        );
                    }
                }
                Err(failure) => {
                    settle_failed_conversation_delivery(
                        state, *namespace, task_id, effect_id, failure,
                    );
                    apply_role_failure(
                        state,
                        TaskAddress::new(*namespace, task_id.clone()),
                        *attempt_no,
                        effect_id,
                        failure.clone(),
                        observed_configuration.as_ref(),
                        None,
                    );
                }
            }
        }
        MissionEvent::OracleRunRequested {
            assertion_ids,
            oracle,
            judged_sha,
            attempt_no,
            effect_id,
            ..
        } => {
            if !oracle_request_matches_obligation(
                state,
                assertion_ids,
                oracle,
                judged_sha,
                *attempt_no,
                effect_id,
            ) {
                finish_apply(state, seq);
                return;
            }
            state.parked_effects.retain(|_, parked| {
                !matches!(parked, ParkedEffect::OracleRun { oracle: parked_oracle } if parked_oracle == oracle)
            });
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
            state.stop_requests.remove(effect_id);
            state.reached_deadlines.remove(effect_id);
            clear_cleanup_failure(state, effect_id);
            match outcome {
                Ok(success) => {
                    state.authoritative_receipts.insert(effect_id.clone());
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
                    if !state.oracle_automatic_retry_remaining(oracle) {
                        state.parked_effects.insert(
                            effect_id.clone(),
                            ParkedEffect::OracleRun {
                                oracle: oracle.clone(),
                            },
                        );
                    }
                }
            }
        }
        // A legal request owns the next attempt number. Orphan outcomes are
        // filtered at ingress and cannot advance recovery identity.
        MissionEvent::TerminalReviewRequested {
            attempt_no,
            effect_id,
            role,
            judged_sha,
            ..
        } => {
            if !terminal_review_request_matches_obligation(
                state,
                *attempt_no,
                effect_id,
                role,
                judged_sha,
            ) {
                finish_apply(state, seq);
                return;
            }
            state
                .parked_effects
                .retain(|_, parked| !matches!(parked, ParkedEffect::TerminalReview));
            state.terminal_review.attempts = (*attempt_no).max(state.terminal_review.attempts);
            track_inflight(state, &envelope.event, seq);
        }
        MissionEvent::TerminalReviewCompleted {
            attempt_no,
            effect_id,
            judged_sha,
            outcome,
        } => {
            let observed_configuration = state.inflight.get(effect_id).and_then(|effect| {
                if let InflightEffect::TerminalReview {
                    runtime_configuration,
                    ..
                } = effect
                {
                    runtime_configuration.clone()
                } else {
                    None
                }
            });
            state.inflight.remove(effect_id);
            state.stop_requests.remove(effect_id);
            state.reached_deadlines.remove(effect_id);
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
                    let failure =
                        merge_failure_configuration(failure, observed_configuration.as_ref());
                    state
                        .parked_effects
                        .insert(effect_id.clone(), ParkedEffect::TerminalReview);
                    state.terminal_review.consecutive_failures =
                        state.terminal_review.consecutive_failures.saturating_add(1);
                    state.terminal_review.outcome = Some(ReviewOutcome::Failed { failure });
                }
            }
        }
        MissionEvent::ControlRequested {
            effect_id,
            action,
            reason,
        } => match action {
            ControlAction::Stop => {
                if state.inflight.contains_key(effect_id)
                    && !state.reached_deadlines.contains_key(effect_id)
                {
                    state
                        .stop_requests
                        .insert(effect_id.clone(), reason.clone());
                }
            }
            ControlAction::ExtendDeadline {
                old_deadline_ms,
                new_deadline_ms,
                ..
            } => {
                if !state.reached_deadlines.contains_key(effect_id) {
                    if let Some(effect) = state.inflight.get_mut(effect_id) {
                        if effect.deadline_ms() == *old_deadline_ms
                            && new_deadline_ms >= old_deadline_ms
                        {
                            effect.set_deadline_ms(*new_deadline_ms);
                        }
                    }
                }
            }
            ControlAction::Continue { .. } => {
                if state.parked_effect_is_continuable(effect_id) {
                    let parked = state
                        .parked_effects
                        .remove(effect_id)
                        .expect("continuable parked effect exists");
                    match parked {
                        ParkedEffect::RoleRun { namespace, task_id } => {
                            if let Some(task) = state.tasks_in_mut(namespace).get_mut(&task_id) {
                                task.status = TaskStatus::Pending;
                                task.consecutive_failures = 0;
                            }
                        }
                        ParkedEffect::OracleRun { oracle } => {
                            state.oracle_failures.remove(&oracle);
                        }
                        ParkedEffect::TerminalReview => {
                            state.terminal_review.outcome = None;
                            state.terminal_review.consecutive_failures = 0;
                        }
                    }
                }
            }
        },
        MissionEvent::EffectDeadlineReached {
            effect_id,
            deadline_ms,
        } => {
            if !state.stop_requests.contains_key(effect_id)
                && state
                    .inflight
                    .get(effect_id)
                    .is_some_and(|effect| effect.deadline_ms() == *deadline_ms)
            {
                state
                    .reached_deadlines
                    .insert(effect_id.clone(), *deadline_ms);
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
    finish_apply(state, seq);
}

fn finish_apply(state: &mut MissionState, seq: u64) {
    state.head = seq;
    // Promotion runs first: a just-approved proposal must seed the contract
    // before gates/attention/phase are derived this same fold (a plan whose only
    // sink is a gate would otherwise hang one event behind).
    derive_promotion(state);
    prune_uncontinuable_parked_effects(state);
    derive_gates(state);
    derive_attention(state);
    derive_phase(state);
}

fn settle_effect_failure(
    state: &mut MissionState,
    effect_id: &super::EffectId,
    effect: InflightEffect,
    failure: TypedFailure,
    final_response: Option<&PayloadRef>,
) {
    state.inflight.remove(effect_id);
    state.stop_requests.remove(effect_id);
    state.reached_deadlines.remove(effect_id);
    clear_cleanup_failure(state, effect_id);
    let observed_configuration = effect.runtime_configuration().cloned();
    let failure = merge_failure_configuration(&failure, observed_configuration.as_ref());
    match effect {
        InflightEffect::RoleRun {
            namespace,
            task_id,
            attempt_no,
            ..
        } => apply_role_failure(
            state,
            TaskAddress::new(namespace, task_id),
            attempt_no,
            effect_id,
            failure,
            None,
            final_response,
        ),
        InflightEffect::OracleRun { oracle, .. } => {
            state.oracle_failures.insert(oracle.clone(), failure);
            state
                .parked_effects
                .insert(effect_id.clone(), ParkedEffect::OracleRun { oracle });
        }
        InflightEffect::TerminalReview { attempt_no, .. } => {
            state.terminal_review.attempts = state.terminal_review.attempts.max(attempt_no);
            state.terminal_review.consecutive_failures =
                state.terminal_review.consecutive_failures.saturating_add(1);
            state.terminal_review.outcome = Some(ReviewOutcome::Failed { failure });
            state
                .parked_effects
                .insert(effect_id.clone(), ParkedEffect::TerminalReview);
        }
    }
}

fn merge_runtime_configuration(
    previous: Option<&RuntimeConfigurationEvidence>,
    current: &RuntimeConfigurationEvidence,
) -> RuntimeConfigurationEvidence {
    let previous = previous.cloned().unwrap_or_default();
    RuntimeConfigurationEvidence {
        requested_model: current.requested_model.clone().or(previous.requested_model),
        applied_model: current.applied_model.clone().or(previous.applied_model),
        model_confirmation: current.model_confirmation.or(previous.model_confirmation),
        requested_mode: current.requested_mode.clone().or(previous.requested_mode),
        applied_mode: current.applied_mode.clone().or(previous.applied_mode),
        mode_confirmation: current.mode_confirmation.or(previous.mode_confirmation),
    }
}

fn settle_conversation_delivery(
    state: &mut MissionState,
    namespace: super::TaskNamespace,
    task_id: &TaskId,
    effect_id: &crate::EffectId,
    has_handoff: bool,
) {
    let Some((_, conversation)) = state.conversations.iter_mut().find(|(_, conversation)| {
        conversation.namespace == namespace
            && &conversation.task_id == task_id
            && conversation.lifecycle == super::state::ConversationLifecycle::Running
            && conversation
                .active_delivery
                .as_ref()
                .is_some_and(|delivery| &delivery.effect_id == effect_id)
    }) else {
        return;
    };
    let boundary = conversation
        .active_delivery
        .take()
        .expect("matched above")
        .message_boundary;
    conversation.consumed_through = conversation.consumed_through.max(boundary);
    conversation
        .queued
        .retain(|message| message.sequence_no > boundary);
    conversation.lifecycle = if has_handoff {
        super::state::ConversationLifecycle::Completed
    } else {
        super::state::ConversationLifecycle::AwaitingLead
    };
}

fn settle_failed_conversation_delivery(
    state: &mut MissionState,
    namespace: super::TaskNamespace,
    task_id: &TaskId,
    effect_id: &crate::EffectId,
    failure: &TypedFailure,
) {
    let Some((_, conversation)) = state.conversations.iter_mut().find(|(_, conversation)| {
        conversation.namespace == namespace
            && &conversation.task_id == task_id
            && conversation.lifecycle == super::state::ConversationLifecycle::Running
            && conversation
                .active_delivery
                .as_ref()
                .is_some_and(|delivery| &delivery.effect_id == effect_id)
    }) else {
        return;
    };
    let boundary = conversation
        .active_delivery
        .take()
        .expect("matched above")
        .message_boundary;
    if failure.evidence().code.as_deref() == Some("kernel.launch") {
        conversation.lifecycle = super::state::ConversationLifecycle::Ready;
        return;
    }
    let invalid_handoff = failure.evidence().code.as_deref() == Some("handoff.schema");
    let marker = if invalid_handoff {
        super::state::DeliveryMarker::PreviouslyDelivered
    } else {
        super::state::DeliveryMarker::PossiblyDelivered
    };
    for message in conversation
        .queued
        .iter_mut()
        .filter(|message| message.sequence_no <= boundary)
    {
        message.marker = marker;
    }
    if invalid_handoff && conversation.invalid_handoff_reworks < state.config.recovery.max_attempts
    {
        conversation.invalid_handoff_reworks += 1;
        conversation.lifecycle = super::state::ConversationLifecycle::ReworkingInvalidHandoff;
    } else {
        conversation.lifecycle = super::state::ConversationLifecycle::Ready;
    }
}

fn merge_failure_configuration(
    failure: &TypedFailure,
    observed: Option<&RuntimeConfigurationEvidence>,
) -> TypedFailure {
    let mut failure = failure.clone();
    let evidence = failure.evidence_mut();
    let reported = RuntimeConfigurationEvidence {
        requested_model: evidence.configuration.requested_model.clone(),
        applied_model: evidence.configuration.applied_model.clone(),
        model_confirmation: evidence.configuration.model_confirmation,
        requested_mode: evidence.configuration.requested_mode.clone(),
        applied_mode: evidence.configuration.applied_mode.clone(),
        mode_confirmation: evidence.configuration.mode_confirmation,
    };
    let merged = merge_runtime_configuration(observed, &reported);
    evidence.configuration = crate::AppliedRuntimeConfiguration {
        requested_model: merged.requested_model,
        applied_model: merged.applied_model,
        model_confirmation: merged.model_confirmation,
        requested_mode: merged.requested_mode,
        applied_mode: merged.applied_mode,
        mode_confirmation: merged.mode_confirmation,
    };
    failure
}

fn role_request_matches_dispatch(state: &MissionState, envelope: &EventEnvelope) -> bool {
    let MissionEvent::RoleRunRequested {
        namespace,
        task_id,
        attempt_no,
        effect_id,
        role,
        output,
        prompt_template: _,
        prompt_hash,
        base_sha,
        assignment_epoch,
        conversation_id,
        message_boundary,
        presented_messages,
        recreate_workspace,
        ..
    } = &envelope.event
    else {
        return false;
    };
    let output_matches = match namespace {
        super::TaskNamespace::Planning => state
            .config
            .planning
            .tasks
            .iter()
            .find(|task| &task.id == task_id)
            .is_some_and(|task| &task.role == role && task.output == *output),
        super::TaskNamespace::Execution => state
            .plan
            .as_ref()
            .and_then(|plan| plan.tasks.iter().find(|task| &task.id == task_id))
            .is_some_and(|task| {
                task.role.as_ref() == Some(role) && output.execution_task_kind() == Some(task.kind)
            }),
    };
    let super::step::StepDecision::DispatchRole(intent) = super::step::step(state) else {
        return false;
    };
    let (expected_base, expected_epoch, expected_recreate) = super::state::resolve_task_assignment(
        state.tasks_in(*namespace).get(task_id),
        &intent.base_sha,
        state.config.recovery.max_attempts,
    );
    if envelope.stamps.prompt_hash.as_deref() != Some(prompt_hash.as_str()) {
        return false;
    }
    let expected_effect = super::EffectId::for_role_request(
        *namespace,
        &state.mission_id,
        task_id,
        *attempt_no,
        *assignment_epoch,
        prompt_hash,
    );
    let expected_conversation = crate::ConversationId::for_role_instance(
        &state.mission_id,
        *namespace,
        task_id,
        role,
        *assignment_epoch,
    );
    let expected_presented: Vec<_> =
        state
            .conversations
            .get(&expected_conversation)
            .map_or_else(Vec::new, |conversation| {
                conversation
                    .queued
                    .iter()
                    .filter(|message| message.sequence_no <= *message_boundary)
                    .map(|message| message.sequence_no)
                    .collect()
            });
    output_matches
        && intent.namespace == *namespace
        && &intent.task_id == task_id
        && intent.attempt_no == *attempt_no
        && &intent.role == role
        && expected_base.as_str() == base_sha
        && expected_epoch == *assignment_epoch
        && expected_recreate == *recreate_workspace
        && expected_effect == *effect_id
        && expected_conversation == *conversation_id
        && *message_boundary == envelope.sequence_no.saturating_sub(1)
        && expected_presented == *presented_messages
}

fn oracle_request_matches_obligation(
    state: &MissionState,
    assertion_ids: &[AssertionId],
    oracle: &super::OracleName,
    judged_sha: &str,
    attempt_no: u32,
    effect_id: &super::EffectId,
) -> bool {
    let owed_assertions = state.owed_assertions_for_oracle(oracle);
    let only_oracles_inflight = state
        .inflight
        .values()
        .all(|effect| matches!(effect, InflightEffect::OracleRun { .. }));
    let same_oracle_inflight = state.inflight.values().any(
        |effect| matches!(effect, InflightEffect::OracleRun { oracle: active, .. } if active == oracle),
    );
    only_oracles_inflight
        && !same_oracle_inflight
        && state.oracle_dispatchable(oracle)
        && judged_sha == state.deliverable_head()
        && attempt_no
            == state
                .oracle_attempts
                .get(oracle)
                .copied()
                .unwrap_or_default()
                + 1
        && !owed_assertions.is_empty()
        && assertion_ids == owed_assertions
        && effect_id
            == &super::EffectId::for_oracle_request(
                &state.mission_id,
                oracle,
                judged_sha,
                attempt_no,
            )
}

fn terminal_review_request_matches_obligation(
    state: &MissionState,
    attempt_no: u32,
    effect_id: &super::EffectId,
    role: &RoleName,
    judged_sha: &str,
) -> bool {
    state.inflight.is_empty()
        && !work_outstanding(state)
        && terminal_review_outstanding(state)
        && state
            .config
            .terminal_review
            .as_ref()
            .is_some_and(|config| &config.role == role)
        && judged_sha == state.deliverable_head()
        && attempt_no == state.terminal_review.attempts + 1
        && effect_id
            == &super::EffectId::for_terminal_review_request(
                &state.mission_id,
                judged_sha,
                attempt_no,
            )
}

fn apply_role_failure(
    state: &mut MissionState,
    task: TaskAddress,
    attempt_no: u32,
    effect_id: &super::EffectId,
    failure: TypedFailure,
    observed_configuration: Option<&RuntimeConfigurationEvidence>,
    final_response: Option<&PayloadRef>,
) {
    let failure = merge_failure_configuration(&failure, observed_configuration);
    let max_attempts = state.config.recovery.max_attempts;
    let should_park =
        if let Some(runtime) = state.tasks_in_mut(task.namespace).get_mut(&task.task_id) {
            runtime.attempts = runtime.attempts.max(attempt_no);
            runtime.status = TaskStatus::Failed;
            runtime.last_failure = Some(failure.clone());
            let evidence = failure.evidence();
            runtime.final_response = final_response.cloned().or_else(|| {
                (!evidence.final_response.is_empty())
                    .then(|| PayloadRef::inline(evidence.final_response.clone()))
            });
            runtime.last_runtime_configuration = Some(merge_runtime_configuration(
                runtime.last_runtime_configuration.as_ref(),
                &evidence.configuration,
            ));
            runtime.consecutive_failures = runtime.consecutive_failures.saturating_add(1);
            !failure.automatically_retryable() || runtime.consecutive_failures >= max_attempts
        } else {
            false
        };
    if should_park {
        state.parked_effects.insert(
            effect_id.clone(),
            ParkedEffect::RoleRun {
                namespace: task.namespace,
                task_id: task.task_id,
            },
        );
    }
}

fn prune_uncontinuable_parked_effects(state: &mut MissionState) {
    let parked = core::mem::take(&mut state.parked_effects);
    state.parked_effects = parked
        .into_iter()
        .filter(|(_, effect)| state.parked_effect_remains_continuable(effect))
        .collect();
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
    if super::plan_validation::validate_plan_proposal(state, proposal).is_err() {
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
        state.flagged_tasks.remove(&TaskAddress::new(
            crate::TaskNamespace::Execution,
            task_id.clone(),
        ));
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
    let active_namespace = state.active_task_namespace();
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
                state
                    .flagged_tasks
                    .remove(&TaskAddress::new(active_namespace, task_id.clone()));
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
                state
                    .flagged_tasks
                    .remove(&TaskAddress::new(active_namespace, task_id.clone()));
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
        state.flagged_tasks.remove(&TaskAddress::new(
            crate::TaskNamespace::Planning,
            id.clone(),
        ));
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
    // rejected plan before the replacement planning DAG can run.
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
            } else if state.flagged_tasks.contains(&TaskAddress::new(
                crate::TaskNamespace::Planning,
                task_id.clone(),
            )) {
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
        if state.oracle_automatic_retry_remaining(oracle) {
            continue;
        }
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
                } else if state.flagged_tasks.contains(&TaskAddress::new(
                    crate::TaskNamespace::Execution,
                    task.id.clone(),
                )) {
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

fn apply_handoff(
    state: &mut MissionState,
    namespace: crate::TaskNamespace,
    task_id: &super::ids::TaskId,
    handoff: &Handoff,
) {
    match handoff {
        Handoff::Work {
            done,
            report,
            request_attention,
        } => {
            debug_assert!(*done, "incomplete work was rejected at effect settlement");
            // Work runs in either era (a planning report role or an execution
            // artifact role), so route by era; Plan is planning-only and Validate
            // execution-only, and address their maps directly below.
            if let Some(task) = state.tasks_in_mut(namespace).get_mut(task_id) {
                task.status = TaskStatus::Cleared;
                task.last_report = Some(report.clone());
                task.last_failure = None;
            }
            if *request_attention {
                state
                    .flagged_tasks
                    .insert(TaskAddress::new(namespace, task_id.clone()));
            }
        }
        Handoff::Plan {
            done,
            report,
            proposal,
            request_attention,
        } => {
            debug_assert!(*done, "incomplete plan was rejected at effect settlement");
            // Planning-only: the author's handoff. A complete valid proposal
            // becomes the gradeless `state.proposal`; it seeds the contract
            // only after approval (`derive_promotion`).
            let proposal_error = match proposal {
                None => Some("planning author completed without a plan proposal".to_string()),
                Some(proposal) if state.planning_base_revision != Some(proposal.base_revision) => {
                    Some("plan proposal does not target the active planning revision".to_string())
                }
                Some(proposal) => super::plan_validation::validate_plan_proposal(state, proposal)
                    .err()
                    .map(|error| error.to_string()),
            };
            let succeeded = proposal_error.is_none();
            let status = if succeeded {
                TaskStatus::Cleared
            } else {
                TaskStatus::Failed
            };
            if let Some(task) = state.planning.tasks.get_mut(task_id) {
                task.status = status;
                task.last_report = Some(report.clone());
                task.last_failure = proposal_error
                    .as_ref()
                    .map(|detail| TypedFailure::invalid("handoff.invalid_plan", detail.clone()));
            }
            if succeeded {
                if let Some(proposal) = proposal {
                    if state.proposal.is_none() {
                        state.proposal = Some(proposal.clone());
                        state.planning_base_revision = None;
                        state.proposal_approved = false;
                    }
                }
                if *request_attention {
                    state.flagged_tasks.insert(TaskAddress::new(
                        crate::TaskNamespace::Planning,
                        task_id.clone(),
                    ));
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
                state.flagged_tasks.insert(TaskAddress::new(
                    crate::TaskNamespace::Execution,
                    task_id.clone(),
                ));
            }
        }
        // Rejected by `handoff_matches_namespace` before this function.
        Handoff::Review { .. } => {}
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
    use std::collections::{BTreeMap, BTreeSet};

    use super::super::event::{
        ArtifactOutcome, MissionConfig, OracleRunSuccess, PayloadRef, RoleRunSuccess,
        RuntimeConfigurationEvidence, TerminalReviewSuccess, ValidationItem,
    };
    use super::super::ids::{AssertionId, EffectId, MissionId, OracleName, RoleName, TaskId};
    use super::super::plan::{
        Assertion, OutputSemantics, Plan, PlanInventory, PlanProposal, PlanningTask, Requirement,
        RequirementDisposition, RequirementKind, Task, TaskKind,
    };
    use super::super::verdict::FinishClass;
    use super::*;
    use crate::TypedFailureEvidence;

    const TEST_PROMPT_HASH: &str =
        "cf07194ee232eb531e15f690000d19846dea69cf05504782658afcfacb9228a2";

    fn mission_id() -> MissionId {
        MissionId::from_digest_prefix("abcdef0123456789")
    }

    fn role_effect(
        namespace: crate::TaskNamespace,
        task: &str,
        attempt_no: u32,
        assignment_epoch: u32,
    ) -> EffectId {
        EffectId::for_role_request(
            namespace,
            &mission_id(),
            &tid(task),
            attempt_no,
            assignment_epoch,
            TEST_PROMPT_HASH,
        )
    }

    fn oracle_effect(judged: &str, attempt_no: u32) -> EffectId {
        oracle_effect_for("cargo-test", judged, attempt_no)
    }

    fn oracle_effect_for(name: &str, judged: &str, attempt_no: u32) -> EffectId {
        EffectId::for_oracle_request(&mission_id(), &oracle(name), judged, attempt_no)
    }

    fn review_effect(judged: &str, attempt_no: u32) -> EffectId {
        EffectId::for_terminal_review_request(&mission_id(), judged, attempt_no)
    }

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
        let mut stamps = super::super::event::VersionStamps::default();
        if matches!(event, MissionEvent::RoleRunRequested { .. }) {
            stamps.prompt_hash = Some(TEST_PROMPT_HASH.to_string());
        }
        EventEnvelope {
            mission_id: mission_id(),
            sequence_no,
            recorded_at_ms: 0,
            stamps,
            event,
        }
    }

    /// Fold hand-built events with sequence numbers assigned by position.
    fn fold_log(events: Vec<MissionEvent>) -> Option<MissionState> {
        let mut known_tasks = BTreeSet::new();
        let mut requested_effects = BTreeSet::new();
        let mut synthesized_role_requests = BTreeSet::new();
        let mut valid_log = Vec::new();
        for event in events {
            match &event {
                MissionEvent::MissionCreated { config, .. } => {
                    known_tasks.extend(config.planning.tasks.iter().map(|task| {
                        TaskAddress::new(crate::TaskNamespace::Planning, task.id.clone())
                    }));
                }
                MissionEvent::PlanProposed { proposal, .. } => {
                    known_tasks.retain(|task| task.namespace == crate::TaskNamespace::Planning);
                    known_tasks.extend(proposal.plan.tasks.iter().filter_map(|task| {
                        task.role.as_ref().map(|_| {
                            TaskAddress::new(crate::TaskNamespace::Execution, task.id.clone())
                        })
                    }));
                }
                _ => {}
            }
            match &event {
                MissionEvent::RoleRunRequested { effect_id, .. }
                | MissionEvent::OracleRunRequested { effect_id, .. }
                | MissionEvent::TerminalReviewRequested { effect_id, .. } => {
                    requested_effects.insert(effect_id.clone());
                }
                _ => {}
            }
            if let MissionEvent::RoleRunCompleted {
                request,
                effect_id,
                outcome: Ok(success),
            } = &event
            {
                if known_tasks.contains(&TaskAddress::new(
                    request.namespace,
                    request.task_id.clone(),
                )) && requested_effects.insert(effect_id.clone())
                {
                    synthesized_role_requests.insert(effect_id.clone());
                    let (role, output) = match success.handoff.as_ref().expect("test log handoff") {
                        Handoff::Work { .. }
                            if request.namespace == crate::TaskNamespace::Planning =>
                        {
                            (
                                RoleName::new("planner").expect("role name"),
                                OutputSemantics::ProducesReport,
                            )
                        }
                        Handoff::Work { .. } => (
                            RoleName::new("implementer").expect("role name"),
                            OutputSemantics::ProducesArtifact,
                        ),
                        Handoff::Validate { .. } => (
                            RoleName::new("reviewer").expect("role name"),
                            OutputSemantics::EmitsVerdict,
                        ),
                        Handoff::Plan { .. } => (
                            RoleName::new("planner").expect("role name"),
                            OutputSemantics::ProposesPlan,
                        ),
                        Handoff::Review { .. } => (
                            RoleName::new("implementer").expect("role name"),
                            OutputSemantics::ProducesArtifact,
                        ),
                    };
                    valid_log.push(MissionEvent::RoleRunRequested {
                        conversation_id: crate::ConversationId::for_role_instance(
                            &mission_id(),
                            request.namespace,
                            &request.task_id,
                            &role,
                            1,
                        ),
                        namespace: request.namespace,
                        task_id: request.task_id.clone(),
                        attempt_no: request.attempt_no,
                        effect_id: effect_id.clone(),
                        role,
                        output,
                        runtime: "codex".into(),
                        prompt_template: crate::RolePromptTemplate::Execution,
                        prompt_hash: PayloadRef::inline("prompt").content_sha256().unwrap(),
                        base_sha: success
                            .artifact
                            .as_ref()
                            .map_or_else(|| "base".into(), |artifact| artifact.base_sha.clone()),
                        assignment_epoch: 1,
                        message_boundary: 0,
                        presented_messages: vec![],
                        recreate_workspace: true,
                        requested_at_ms: 0,
                        not_before_ms: 0,
                        deadline_ms: 100_000,
                        budget_deadline_ms: 100_000,
                    });
                }
            }
            if let MissionEvent::OracleRunCompleted {
                assertion_ids,
                oracle,
                judged_sha,
                attempt_no,
                effect_id,
                ..
            } = &event
            {
                if requested_effects.insert(effect_id.clone()) {
                    valid_log.push(MissionEvent::OracleRunRequested {
                        assertion_ids: assertion_ids.clone(),
                        oracle: oracle.clone(),
                        judged_sha: judged_sha.clone(),
                        attempt_no: *attempt_no,
                        effect_id: effect_id.clone(),
                        requested_at_ms: 0,
                        not_before_ms: 0,
                        deadline_ms: 100_000,
                    });
                }
            }
            if let MissionEvent::TerminalReviewCompleted {
                attempt_no,
                effect_id,
                judged_sha,
                ..
            } = &event
            {
                if requested_effects.insert(effect_id.clone()) {
                    valid_log.push(MissionEvent::TerminalReviewRequested {
                        attempt_no: *attempt_no,
                        effect_id: effect_id.clone(),
                        role: RoleName::new("gap-reviewer").expect("role name"),
                        runtime: "codex".into(),
                        prompt: PayloadRef::inline("review prompt"),
                        judged_sha: judged_sha.clone(),
                        nonce: "n0".into(),
                        requested_at_ms: 0,
                        not_before_ms: 0,
                        deadline_ms: 100_000,
                        budget_deadline_ms: 100_000,
                    });
                }
            }
            valid_log.push(event);
        }
        let events = valid_log.into_iter().flat_map(|event| {
            let approve = matches!(&event, MissionEvent::PlanProposed { .. }).then(|| {
                decision(
                    "plan_proposal:mission",
                    super::super::event::DecisionAction::Approve,
                )
            });
            std::iter::once(event).chain(approve)
        });
        let mut queued: BTreeMap<crate::ConversationId, Vec<u64>> = BTreeMap::new();
        let mut active_roles: BTreeMap<EffectId, crate::RoleRunRequestIdentity> = BTreeMap::new();
        fold(events.enumerate().map(|(i, mut event)| {
            let sequence_no = i as u64;
            match &mut event {
                MissionEvent::MessageSent { recipients, .. } => {
                    for recipient in recipients {
                        queued
                            .entry(recipient.conversation_id.clone())
                            .or_default()
                            .push(sequence_no);
                    }
                }
                MissionEvent::RoleRunRequested {
                    effect_id,
                    conversation_id,
                    namespace,
                    task_id,
                    attempt_no,
                    role,
                    output,
                    runtime,
                    prompt_template,
                    prompt_hash,
                    base_sha,
                    assignment_epoch,
                    message_boundary,
                    presented_messages,
                    recreate_workspace,
                    ..
                } => {
                    *conversation_id = crate::ConversationId::for_role_instance(
                        &mission_id(),
                        *namespace,
                        task_id,
                        role,
                        *assignment_epoch,
                    );
                    *message_boundary = sequence_no.saturating_sub(1);
                    *presented_messages = queued.get(conversation_id).cloned().unwrap_or_default();
                    active_roles.insert(
                        effect_id.clone(),
                        crate::RoleRunRequestIdentity {
                            conversation_id: conversation_id.clone(),
                            namespace: *namespace,
                            task_id: task_id.clone(),
                            attempt_no: *attempt_no,
                            assignment_epoch: *assignment_epoch,
                            role: role.clone(),
                            output: *output,
                            runtime: runtime.clone(),
                            prompt_template: *prompt_template,
                            prompt_hash: prompt_hash.clone(),
                            base_sha: base_sha.clone(),
                            recreate_workspace: *recreate_workspace,
                            message_boundary: *message_boundary,
                            presented_messages: presented_messages.clone(),
                        },
                    );
                }
                MissionEvent::RoleRunCompleted {
                    effect_id, request, ..
                } => {
                    if let Some(active) = active_roles.get(effect_id) {
                        if synthesized_role_requests.contains(effect_id) {
                            **request = active.clone();
                            return envelope(sequence_no, event);
                        }
                        let mut normalized = (**request).clone();
                        normalized.message_boundary = active.message_boundary;
                        normalized.presented_messages = active.presented_messages.clone();
                        if normalized == *active {
                            **request = active.clone();
                        }
                    }
                }
                _ => {}
            }
            envelope(sequence_no, event)
        }))
    }

    fn created() -> MissionEvent {
        let plan_inventory = PlanInventory {
            roles: BTreeMap::from([
                (
                    RoleName::new("implementer").expect("role name"),
                    OutputSemantics::ProducesArtifact,
                ),
                (
                    RoleName::new("reviewer").expect("role name"),
                    OutputSemantics::EmitsVerdict,
                ),
            ]),
            oracles: ["cargo-test", "different-oracle", "lint"]
                .into_iter()
                .map(oracle)
                .collect(),
        };
        MissionEvent::MissionCreated {
            objective: "objective".into(),
            mission_type: crate::MissionTypeRef {
                name: "mt".into(),
                digest: "d".into(),
            },
            runtime: "codex".into(),
            image_id: "img".into(),
            workspace_dir: "/w".into(),
            base_sha: "base".into(),
            config: MissionConfig {
                plan_inventory,
                recovery: super::super::event::RecoveryConfig { max_attempts: 1 },
                ..Default::default()
            },
        }
    }

    fn plan_proposed(mut assertions: Vec<Assertion>, mut tasks: Vec<Task>) -> MissionEvent {
        if assertions.is_empty() {
            assertions.push(assertion("FIXTURE-CONTRACT", Some("cargo-test")));
        }
        let assertion_ids = assertions
            .iter()
            .map(|assertion| assertion.id.clone())
            .collect::<Vec<_>>();
        let covered = tasks
            .iter()
            .filter(|task| task.kind == TaskKind::Work)
            .flat_map(|task| task.targets.iter().cloned())
            .collect::<BTreeSet<_>>();
        if let Some(first_writer) = tasks.iter_mut().find(|task| task.kind == TaskKind::Work) {
            first_writer.targets.extend(
                assertion_ids
                    .iter()
                    .filter(|id| !covered.contains(*id))
                    .cloned(),
            );
        }
        for validator in tasks
            .iter_mut()
            .filter(|task| task.kind == TaskKind::Validate && task.targets.is_empty())
        {
            validator.targets.clone_from(&assertion_ids);
        }
        let requirements = assertions
            .iter()
            .enumerate()
            .map(|(index, assertion)| Requirement {
                id: super::super::ids::RequirementId::new(format!("REQ-{}", index + 1))
                    .expect("requirement id"),
                kind: RequirementKind::Capability,
                prose: format!("fixture requirement for {}", assertion.id),
                disposition: RequirementDisposition::Covered {
                    assertion_ids: vec![assertion.id.clone()],
                },
            })
            .collect();
        MissionEvent::PlanProposed {
            proposal: PlanProposal {
                base_revision: 0,
                plan: Plan {
                    requirements,
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

    #[test]
    fn configuration_less_failure_does_not_clobber_observed_runtime_evidence() {
        let observed = RuntimeConfigurationEvidence {
            requested_model: Some("requested".into()),
            applied_model: Some("applied".into()),
            model_confirmation: Some(crate::RuntimeConfigurationConfirmation::Observed),
            requested_mode: Some("build".into()),
            applied_mode: Some("build".into()),
            mode_confirmation: Some(crate::RuntimeConfigurationConfirmation::Observed),
        };

        assert_eq!(
            merge_runtime_configuration(Some(&observed), &RuntimeConfigurationEvidence::default()),
            observed
        );
    }

    #[test]
    fn noncanonical_effect_id_cannot_reserve_a_future_generation() {
        let mut request = role_requested("w", "noncanonical");
        let MissionEvent::RoleRunRequested { effect_id, .. } = &mut request else {
            unreachable!("role_requested returns a role request")
        };
        *effect_id = EffectId::for_parts(&[
            "oracle",
            MissionId::from_digest_prefix("abcdef0123456789").as_str(),
            "cargo-test",
            "future-head",
            "1",
        ]);

        let state = fold_log(vec![
            created(),
            plan_proposed(vec![], vec![work_task("w")]),
            request,
        ])
        .expect("state");

        assert!(state.inflight.is_empty());
        assert_eq!(state.tasks[&tid("w")].status, TaskStatus::Pending);
    }

    #[test]
    fn a_future_assignment_epoch_cannot_reserve_its_canonical_effect_id() {
        let mut request = role_requested("w", "future-epoch");
        let MissionEvent::RoleRunRequested {
            assignment_epoch,
            effect_id,
            ..
        } = &mut request
        else {
            unreachable!("role_requested returns a role request")
        };
        *assignment_epoch = 2;
        *effect_id = role_effect(crate::TaskNamespace::Execution, "w", 1, 2);

        let state = fold_log(vec![
            created(),
            plan_proposed(vec![], vec![work_task("w")]),
            request,
        ])
        .expect("state");

        assert!(state.inflight.is_empty());
        assert_eq!(state.tasks[&tid("w")].assignment_epoch, 0);
        assert_eq!(state.tasks[&tid("w")].status, TaskStatus::Pending);
    }

    #[test]
    fn a_role_request_without_its_prompt_stamp_is_inert() {
        let events = vec![
            created(),
            plan_proposed(vec![], vec![work_task("w")]),
            decision(
                "plan_proposal:mission",
                super::super::event::DecisionAction::Approve,
            ),
            role_requested("w", "unstamped"),
        ];
        let state = fold(events.into_iter().enumerate().map(|(sequence, event)| {
            let mut envelope = envelope(sequence as u64, event);
            if matches!(&envelope.event, MissionEvent::RoleRunRequested { .. }) {
                envelope.stamps.prompt_hash = None;
            }
            envelope
        }))
        .expect("state");

        assert!(state.inflight.is_empty());
        assert_eq!(state.tasks[&tid("w")].status, TaskStatus::Pending);
    }

    #[test]
    fn a_role_request_with_a_prompt_that_does_not_match_its_stamp_is_inert() {
        let mut request = role_requested("w", "mismatched-prompt");
        let MissionEvent::RoleRunRequested { prompt_hash, .. } = &mut request else {
            unreachable!("role_requested returns a role request")
        };
        *prompt_hash = PayloadRef::inline("different prompt")
            .content_sha256()
            .unwrap();

        let state = fold_log(vec![
            created(),
            plan_proposed(vec![], vec![work_task("w")]),
            request,
        ])
        .expect("state");

        assert!(state.inflight.is_empty());
        assert_eq!(state.tasks[&tid("w")].status, TaskStatus::Pending);
    }

    #[test]
    fn noncanonical_oracle_and_review_ids_cannot_enter_inflight_state() {
        let mut oracle_events = vec![
            created(),
            plan_proposed(
                vec![assertion("TESTS-PASS", Some("cargo-test"))],
                vec![work_task("fix")],
            ),
            role_completed(
                "fix",
                "writer",
                work_handoff(true, false),
                Some(ArtifactOutcome {
                    base_sha: "base".into(),
                    head_sha: "h1".into(),
                }),
            ),
        ];
        let mut oracle_request = oracle_requested("TESTS-PASS", "h1", "forged");
        let MissionEvent::OracleRunRequested { effect_id, .. } = &mut oracle_request else {
            unreachable!("oracle_requested returns an oracle request")
        };
        *effect_id = EffectId::for_parts(&["forged", "oracle"]);
        oracle_events.push(oracle_request);
        let oracle_state = fold_log(oracle_events).expect("oracle state");
        assert!(oracle_state.inflight.is_empty());
        assert!(oracle_state.oracle_attempts.is_empty());

        let mut review_events = events_to_the_brink();
        let mut review_request = review_requested(1, "forged", "h1");
        let MissionEvent::TerminalReviewRequested { effect_id, .. } = &mut review_request else {
            unreachable!("review_requested returns a review request")
        };
        *effect_id = EffectId::for_parts(&["forged", "terminal-review"]);
        review_events.push(review_request);
        let review_state = fold_log(review_events).expect("review state");
        assert!(review_state.inflight.is_empty());
        assert_eq!(review_state.terminal_review.attempts, 0);
        assert!(terminal_review_outstanding(&review_state));
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
        role_requested_in(
            crate::TaskNamespace::Execution,
            task,
            key,
            RoleName::new("implementer").expect("role name"),
            OutputSemantics::ProducesArtifact,
        )
    }

    fn planning_role_requested(task: &str, key: &str, output: OutputSemantics) -> MissionEvent {
        role_requested_in(
            crate::TaskNamespace::Planning,
            task,
            key,
            RoleName::new("planner").expect("role name"),
            output,
        )
    }

    fn role_requested_in(
        namespace: crate::TaskNamespace,
        task: &str,
        _key: &str,
        role: RoleName,
        output: OutputSemantics,
    ) -> MissionEvent {
        MissionEvent::RoleRunRequested {
            conversation_id: crate::ConversationId::for_role_instance(
                &mission_id(),
                namespace,
                &tid(task),
                &role,
                1,
            ),
            namespace,
            task_id: tid(task),
            attempt_no: 1,
            effect_id: role_effect(namespace, task, 1, 1),
            role,
            output,
            runtime: "codex".into(),
            prompt_template: crate::RolePromptTemplate::Execution,
            prompt_hash: PayloadRef::inline("prompt").content_sha256().unwrap(),
            base_sha: "base".into(),
            assignment_epoch: 1,
            message_boundary: 0,
            presented_messages: vec![],
            recreate_workspace: true,
            requested_at_ms: 0,
            not_before_ms: 0,
            deadline_ms: 100_000,
            budget_deadline_ms: 100_000,
        }
    }

    fn role_completed(
        task: &str,
        key: &str,
        handoff: Handoff,
        artifact: Option<ArtifactOutcome>,
    ) -> MissionEvent {
        role_completed_at(
            crate::TaskNamespace::Execution,
            task,
            1,
            key,
            handoff,
            artifact,
        )
    }

    fn planning_role_completed(
        task: &str,
        key: &str,
        handoff: Handoff,
        artifact: Option<ArtifactOutcome>,
    ) -> MissionEvent {
        role_completed_at(
            crate::TaskNamespace::Planning,
            task,
            1,
            key,
            handoff,
            artifact,
        )
    }

    fn role_completed_at(
        namespace: crate::TaskNamespace,
        task: &str,
        attempt_no: u32,
        _key: &str,
        handoff: Handoff,
        artifact: Option<ArtifactOutcome>,
    ) -> MissionEvent {
        let role = if namespace == crate::TaskNamespace::Planning {
            RoleName::new("planner").unwrap()
        } else {
            RoleName::new("implementer").unwrap()
        };
        let output = if namespace == crate::TaskNamespace::Planning {
            match &handoff {
                Handoff::Plan { .. } => OutputSemantics::ProposesPlan,
                Handoff::Validate { .. } => OutputSemantics::EmitsVerdict,
                Handoff::Review { .. } => OutputSemantics::EmitsGapVerdict,
                Handoff::Work { .. } => OutputSemantics::ProducesReport,
            }
        } else {
            OutputSemantics::ProducesArtifact
        };
        MissionEvent::RoleRunCompleted {
            effect_id: role_effect(namespace, task, attempt_no, 1),
            request: role_identity(namespace, task, attempt_no, 1, role, output, "base", true),
            outcome: Ok(RoleRunSuccess {
                handoff: Some(handoff),
                artifact,
                final_response: PayloadRef::inline("final response"),
                runtime_configuration: RuntimeConfigurationEvidence::default(),
            }),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn role_identity(
        namespace: crate::TaskNamespace,
        task: &str,
        attempt_no: u32,
        assignment_epoch: u32,
        role: RoleName,
        output: OutputSemantics,
        base_sha: &str,
        recreate_workspace: bool,
    ) -> Box<crate::RoleRunRequestIdentity> {
        let prompt = PayloadRef::inline("prompt");
        Box::new(crate::RoleRunRequestIdentity {
            conversation_id: crate::ConversationId::for_role_instance(
                &mission_id(),
                namespace,
                &tid(task),
                &role,
                assignment_epoch,
            ),
            namespace,
            task_id: tid(task),
            attempt_no,
            assignment_epoch,
            role,
            output,
            runtime: "codex".into(),
            prompt_hash: prompt.content_sha256().unwrap(),
            prompt_template: crate::RolePromptTemplate::Execution,
            base_sha: base_sha.into(),
            recreate_workspace,
            message_boundary: 0,
            presented_messages: vec![],
        })
    }

    fn oracle_requested(assertion_id: &str, judged: &str, _key: &str) -> MissionEvent {
        MissionEvent::OracleRunRequested {
            assertion_ids: vec![aid(assertion_id)],
            oracle: oracle("cargo-test"),
            judged_sha: judged.into(),
            attempt_no: 1,
            effect_id: oracle_effect(judged, 1),
            requested_at_ms: 0,
            not_before_ms: 0,
            deadline_ms: 100_000,
        }
    }

    fn oracle_completed(
        assertion_id: &str,
        judged: &str,
        key: &str,
        exit_code: i32,
    ) -> MissionEvent {
        oracle_completed_at(assertion_id, judged, 1, key, exit_code)
    }

    fn oracle_completed_at(
        assertion_id: &str,
        judged: &str,
        attempt_no: u32,
        _key: &str,
        exit_code: i32,
    ) -> MissionEvent {
        MissionEvent::OracleRunCompleted {
            assertion_ids: vec![aid(assertion_id)],
            oracle: oracle("cargo-test"),
            judged_sha: judged.into(),
            attempt_no,
            effect_id: oracle_effect(judged, attempt_no),
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

    #[test]
    fn workspace_provenance_advances_only_after_the_exact_runner_confirmation() {
        let effect_id = role_effect(crate::TaskNamespace::Execution, "w", 1, 1);
        let requested = role_requested("w", "workspace");
        let requested_state = fold_log(vec![
            created(),
            plan_proposed(vec![], vec![work_task("w")]),
            requested.clone(),
        ])
        .expect("requested state");
        let task = requested_state.tasks.get(&tid("w")).unwrap();
        assert_eq!(task.workspace_base_sha, None);
        assert_eq!(task.assignment_epoch, 0);

        let stale_state = fold_log(vec![
            created(),
            plan_proposed(vec![], vec![work_task("w")]),
            requested.clone(),
            MissionEvent::TaskWorkspacePrepared {
                task_id: tid("w"),
                effect_id: EffectId::for_parts(&["test", "stale"]),
                base_sha: "base".into(),
                assignment_epoch: 1,
            },
        ])
        .expect("stale state");
        assert_eq!(
            stale_state.tasks.get(&tid("w")).unwrap().workspace_base_sha,
            None
        );

        let confirmed_state = fold_log(vec![
            created(),
            plan_proposed(vec![], vec![work_task("w")]),
            requested,
            MissionEvent::TaskWorkspacePrepared {
                task_id: tid("w"),
                effect_id,
                base_sha: "base".into(),
                assignment_epoch: 1,
            },
        ])
        .expect("confirmed state");
        let task = confirmed_state.tasks.get(&tid("w")).unwrap();
        assert_eq!(task.workspace_base_sha.as_deref(), Some("base"));
        assert_eq!(task.assignment_epoch, 1);
    }

    #[test]
    fn pre_checkout_failure_preserves_the_last_confirmed_workspace_base() {
        let first_effect = role_effect(crate::TaskNamespace::Execution, "w", 1, 1);
        let second_effect = role_effect(crate::TaskNamespace::Execution, "w", 2, 2);
        let state = fold_log(vec![
            created(),
            plan_proposed(vec![], vec![work_task("w")]),
            role_requested("w", "workspace-first"),
            MissionEvent::TaskWorkspacePrepared {
                task_id: tid("w"),
                effect_id: first_effect,
                base_sha: "base".into(),
                assignment_epoch: 1,
            },
            role_completed("w", "workspace-first", work_handoff(true, false), None),
            MissionEvent::RoleRunRequested {
                conversation_id: crate::ConversationId::for_role_instance(
                    &mission_id(),
                    crate::TaskNamespace::Execution,
                    &tid("w"),
                    &RoleName::new("implementer").unwrap(),
                    2,
                ),
                namespace: crate::TaskNamespace::Execution,
                task_id: tid("w"),
                attempt_no: 2,
                effect_id: second_effect.clone(),
                role: RoleName::new("implementer").unwrap(),
                output: OutputSemantics::ProducesArtifact,
                runtime: "codex".into(),
                prompt_template: crate::RolePromptTemplate::Execution,
                prompt_hash: PayloadRef::inline("moved-base prompt")
                    .content_sha256()
                    .unwrap(),
                base_sha: "moved".into(),
                assignment_epoch: 2,
                message_boundary: 0,
                presented_messages: vec![],
                recreate_workspace: true,
                requested_at_ms: 1,
                not_before_ms: 1,
                deadline_ms: 100_001,
                budget_deadline_ms: 100_001,
            },
            MissionEvent::RoleRunCompleted {
                request: role_identity(
                    crate::TaskNamespace::Execution,
                    "w",
                    2,
                    2,
                    RoleName::new("implementer").unwrap(),
                    OutputSemantics::ProducesArtifact,
                    "moved",
                    true,
                ),
                effect_id: second_effect,
                outcome: Err(TypedFailure::permanent(
                    "kernel.launch",
                    "checkout failed before preparation",
                )),
            },
        ])
        .unwrap();
        let task = state.tasks.get(&tid("w")).unwrap();
        assert_eq!(task.workspace_base_sha.as_deref(), Some("base"));
        assert_eq!(task.assignment_epoch, 1);
    }

    #[test]
    fn deadline_and_extension_linearize_by_event_order_without_replay() {
        let effect_id = role_effect(crate::TaskNamespace::Execution, "w", 1, 1);
        let base = vec![
            created(),
            plan_proposed(vec![], vec![work_task("w")]),
            role_requested("w", "deadline-race"),
        ];
        let extend = MissionEvent::ControlRequested {
            effect_id: effect_id.clone(),
            action: ControlAction::ExtendDeadline {
                old_deadline_ms: 100_000,
                new_deadline_ms: 110_000,
                automatic: false,
            },
            reason: "more time".into(),
        };
        let reached = MissionEvent::EffectDeadlineReached {
            effect_id: effect_id.clone(),
            deadline_ms: 100_000,
        };

        let extension_first = fold_log(
            base.iter()
                .cloned()
                .chain([extend.clone(), reached.clone()])
                .collect(),
        )
        .unwrap();
        assert_eq!(extension_first.inflight[&effect_id].deadline_ms(), 110_000);
        assert!(!extension_first.reached_deadlines.contains_key(&effect_id));

        let deadline_first = fold_log(base.into_iter().chain([reached, extend]).collect()).unwrap();
        assert_eq!(deadline_first.inflight[&effect_id].deadline_ms(), 100_000);
        assert_eq!(deadline_first.reached_deadlines[&effect_id], 100_000);

        let stop = MissionEvent::ControlRequested {
            effect_id: effect_id.clone(),
            action: ControlAction::Stop,
            reason: "stop".into(),
        };
        let stop_first = fold_log(vec![
            created(),
            plan_proposed(vec![], vec![work_task("w")]),
            role_requested("w", "deadline-race"),
            stop.clone(),
            MissionEvent::EffectDeadlineReached {
                effect_id: effect_id.clone(),
                deadline_ms: 100_000,
            },
        ])
        .unwrap();
        assert_eq!(stop_first.stop_requests[&effect_id], "stop");
        assert!(!stop_first.reached_deadlines.contains_key(&effect_id));

        let deadline_first = fold_log(vec![
            created(),
            plan_proposed(vec![], vec![work_task("w")]),
            role_requested("w", "deadline-race"),
            MissionEvent::EffectDeadlineReached {
                effect_id: effect_id.clone(),
                deadline_ms: 100_000,
            },
            stop,
        ])
        .unwrap();
        assert_eq!(deadline_first.reached_deadlines[&effect_id], 100_000);
        assert!(!deadline_first.stop_requests.contains_key(&effect_id));
    }

    #[test]
    fn durable_stop_dominates_a_later_exact_role_success() {
        let effect_id = role_effect(crate::TaskNamespace::Execution, "w", 1, 1);
        let state = fold_log(vec![
            created(),
            plan_proposed(vec![], vec![work_task("w")]),
            role_requested("w", "stopped-role"),
            MissionEvent::ControlRequested {
                effect_id: effect_id.clone(),
                action: ControlAction::Stop,
                reason: "operator stop".into(),
            },
            role_completed(
                "w",
                "stopped-role",
                work_handoff(true, false),
                Some(ArtifactOutcome {
                    base_sha: "base".into(),
                    head_sha: "must-not-promote".into(),
                }),
            ),
        ])
        .expect("state");

        assert_eq!(state.deliverable_head(), "base");
        let failure = state.tasks[&tid("w")]
            .last_failure
            .as_ref()
            .expect("stopped role failure");
        assert!(matches!(failure, TypedFailure::OperatorStopped { .. }));
        assert!(state.parked_effects.contains_key(&effect_id));
    }

    #[test]
    fn durable_deadline_dominates_a_later_exact_oracle_success() {
        let effect_id = oracle_effect("h1", 1);
        let state = fold_log(vec![
            created(),
            plan_proposed(
                vec![assertion("A1", Some("cargo-test"))],
                vec![work_task("w")],
            ),
            role_completed(
                "w",
                "work",
                work_handoff(true, false),
                Some(ArtifactOutcome {
                    base_sha: "base".into(),
                    head_sha: "h1".into(),
                }),
            ),
            oracle_requested("A1", "h1", "deadline-oracle"),
            MissionEvent::EffectDeadlineReached {
                effect_id: effect_id.clone(),
                deadline_ms: 100_000,
            },
            oracle_completed("A1", "h1", "deadline-oracle", 0),
        ])
        .expect("state");

        assert!(state.contract[&aid("A1")].last_authoritative.is_none());
        let failure = &state.oracle_failures[&oracle("cargo-test")];
        assert!(matches!(failure, TypedFailure::DeadlineExhausted { .. }));
        assert!(state.parked_effects.contains_key(&effect_id));
    }

    #[test]
    fn durable_abort_dominates_a_later_exact_terminal_review_success() {
        let effect_id = review_effect("h1", 1);
        let mut events = events_to_the_brink();
        events.extend([
            review_requested(1, "aborted-review", "h1"),
            MissionEvent::MissionAborted {
                reason: "operator abort".into(),
            },
            review_completed_at(1, "aborted-review", "h1", true, vec![]),
        ]);
        let state = fold_log(events).expect("state");

        assert_eq!(
            state.phase,
            MissionPhase::Aborted {
                reason: "operator abort".into()
            }
        );
        let Some(ReviewOutcome::Failed { failure }) = state.terminal_review.outcome else {
            panic!("aborted review must settle as a failure");
        };
        assert!(matches!(failure, TypedFailure::OperatorAborted { .. }));
        assert!(state.parked_effects.contains_key(&effect_id));
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
                vec![assertion("AA", Some("cargo-test"))],
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
                    namespace: crate::TaskNamespace::Execution,
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
            output: OutputSemantics::ProposesPlan,
            body: "replace the rejected plan".into(),
            depends_on: vec![],
        }];
        let base = vec![
            created,
            plan_proposed(
                vec![assertion("AA", Some("cargo-test"))],
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
                effect_id: oracle_effect("sha-1", 1),
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
                vec![assertion("AA", Some("cargo-test"))],
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
    fn replay_refuses_a_structurally_invalid_plan() {
        let state = fold_log(vec![
            created(),
            MissionEvent::PlanProposed {
                proposal: PlanProposal {
                    base_revision: 0,
                    plan: Plan {
                        requirements: Vec::new(),
                        assertions: vec![assertion("UNSUPPORTED", Some("cargo-test"))],
                        tasks: vec![work_task("work")],
                    },
                },
                plan_hash: "malformed-but-decodable".into(),
            },
        ])
        .expect("mission creation remains replayable");

        assert!(state.plan.is_none());
        assert!(state.proposal.is_none());
        assert_eq!(state.phase, MissionPhase::Planning);
    }

    #[test]
    fn replay_classifies_an_invalid_planning_handoff_as_failed() {
        let mut mission_created = created();
        let MissionEvent::MissionCreated { config, .. } = &mut mission_created else {
            unreachable!("created() builds MissionCreated");
        };
        config.planning.tasks.push(PlanningTask {
            id: tid("author"),
            role: RoleName::new("planner").expect("role name"),
            output: OutputSemantics::ProposesPlan,
            body: "author a complete plan".into(),
            depends_on: Vec::new(),
        });
        let malformed = PlanProposal {
            base_revision: 0,
            plan: Plan {
                requirements: Vec::new(),
                assertions: vec![assertion("UNSUPPORTED", Some("cargo-test"))],
                tasks: vec![work_task("work")],
            },
        };

        let state = fold_log(vec![
            mission_created,
            planning_role_requested("author", "author", OutputSemantics::ProposesPlan),
            planning_role_completed(
                "author",
                "author",
                Handoff::Plan {
                    done: true,
                    report: PayloadRef::inline("malformed proposal"),
                    proposal: Some(malformed),
                    request_attention: false,
                },
                None,
            ),
        ])
        .expect("mission remains replayable");

        assert!(state.proposal.is_none());
        assert_eq!(
            state.planning.tasks[&tid("author")].status,
            TaskStatus::Failed
        );
        assert!(matches!(
            state.planning.tasks[&tid("author")].last_failure,
            Some(TypedFailure::InvalidOutput { .. })
        ));
        assert!(state.open_attention.contains_key("node_failed:author"));
    }

    #[test]
    fn replay_rejects_an_incomplete_planning_handoff_with_a_valid_proposal() {
        let mut mission_created = created();
        let MissionEvent::MissionCreated { config, .. } = &mut mission_created else {
            unreachable!("created() builds MissionCreated");
        };
        config.planning.tasks.push(PlanningTask {
            id: tid("author"),
            role: RoleName::new("planner").expect("role name"),
            output: OutputSemantics::ProposesPlan,
            body: "author a complete plan".into(),
            depends_on: Vec::new(),
        });
        let MissionEvent::PlanProposed { proposal, .. } =
            plan_proposed(Vec::new(), vec![work_task("work")])
        else {
            unreachable!("plan_proposed() builds PlanProposed");
        };

        let state = fold_log(vec![
            mission_created,
            planning_role_requested("author", "incomplete-author", OutputSemantics::ProposesPlan),
            planning_role_completed(
                "author",
                "incomplete-author",
                Handoff::Plan {
                    done: false,
                    report: PayloadRef::inline("proposal is not ready"),
                    proposal: Some(proposal),
                    request_attention: false,
                },
                None,
            ),
        ])
        .expect("mission remains replayable");

        assert!(state.proposal.is_none());
        assert!(state.plan.is_none());
        assert_eq!(
            state.planning.tasks[&tid("author")].status,
            TaskStatus::Failed
        );
        assert!(matches!(
            state.planning.tasks[&tid("author")].last_failure,
            Some(TypedFailure::InvalidOutput { .. })
        ));
        assert!(state.open_attention.contains_key("node_failed:author"));
    }

    #[test]
    fn plan_proposed_initializes_contract_and_tasks() {
        let state = fold_log(vec![
            created(),
            plan_proposed(
                vec![
                    assertion("TESTS-PASS", Some("cargo-test")),
                    assertion("SECOND-CLAIM", Some("cargo-test")),
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
        assert_eq!(
            state.contract[&aid("SECOND-CLAIM")].oracle,
            Some(oracle("cargo-test"))
        );
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
                        matches!(state.phase, MissionPhase::Running),
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
            let mut tasks = vec![work_task("work")];
            tasks.extend(
                case.verdicts
                    .iter()
                    .map(|(validator, _)| validate_task(validator)),
            );
            let mut events = vec![
                created(),
                plan_proposed(vec![assertion("A1", Some("cargo-test"))], tasks),
                role_completed("work", "work", work_handoff(true, false), None),
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
            plan_proposed(
                vec![assertion("A1", Some("cargo-test"))],
                vec![work_task("work"), validate_task("v1")],
            ),
            role_completed("work", "work", work_handoff(true, false), None),
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
        assert!(matches!(state.phase, MissionPhase::Running));
    }

    #[test]
    fn role_run_failed_fails_task_and_raises_attention() {
        let state = fold_log(vec![
            created(),
            plan_proposed(vec![], vec![work_task("t1")]),
            role_requested("t1", "k1"),
            MissionEvent::RoleRunCompleted {
                request: role_identity(
                    crate::TaskNamespace::Execution,
                    "t1",
                    1,
                    1,
                    RoleName::new("implementer").unwrap(),
                    OutputSemantics::ProducesArtifact,
                    "base",
                    true,
                ),
                effect_id: role_effect(crate::TaskNamespace::Execution, "t1", 1, 1),
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
                    request: role_identity(
                        crate::TaskNamespace::Execution,
                        "t1",
                        1,
                        1,
                        RoleName::new("implementer").unwrap(),
                        OutputSemantics::ProducesArtifact,
                        "base",
                        true,
                    ),
                    effect_id: role_effect(crate::TaskNamespace::Execution, "t1", 1, 1),
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
            output: OutputSemantics::ProducesReport,
            body: "plan".into(),
            depends_on: vec![],
        });
        let state = fold_log(vec![
            created,
            planning_role_requested("same-id", "planning", OutputSemantics::ProducesReport),
            planning_role_completed("same-id", "planning", work_handoff(true, false), None),
            plan_proposed(vec![], vec![work_task("same-id")]),
            role_requested("same-id", "execution"),
            MissionEvent::RoleRunCompleted {
                request: role_identity(
                    crate::TaskNamespace::Execution,
                    "same-id",
                    1,
                    1,
                    RoleName::new("implementer").unwrap(),
                    OutputSemantics::ProducesArtifact,
                    "base",
                    true,
                ),
                effect_id: role_effect(crate::TaskNamespace::Execution, "same-id", 1, 1),
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
    fn plan_handoff_completion_stays_in_its_original_task_era() {
        let mut mission_created = created();
        let MissionEvent::MissionCreated { config, .. } = &mut mission_created else {
            unreachable!("created() builds MissionCreated");
        };
        config.planning.tasks.push(PlanningTask {
            id: tid("same-id"),
            role: RoleName::new("planner").expect("role name"),
            output: OutputSemantics::ProposesPlan,
            body: "replace the rejected plan".into(),
            depends_on: vec![],
        });
        let MissionEvent::PlanProposed {
            proposal: mut replacement,
            ..
        } = plan_proposed(
            vec![assertion("AA", Some("cargo-test"))],
            vec![work_task("replacement")],
        )
        else {
            unreachable!("plan_proposed() builds PlanProposed");
        };
        replacement.base_revision = 1;
        let mut blocked_work = work_task("same-id");
        blocked_work.depends_on = vec![tid("v")];
        let state = fold_log(vec![
            mission_created,
            plan_proposed(
                vec![assertion("AA", Some("cargo-test"))],
                vec![
                    blocked_work,
                    validate_task("v"),
                    gate_task("g", &["AA"], &["v"]),
                ],
            ),
            role_completed("v", "validation", validate_handoff(&[("AA", false)]), None),
            decision("gate_failed:g", super::super::event::DecisionAction::Revise),
            planning_role_requested("same-id", "planning", OutputSemantics::ProposesPlan),
            planning_role_completed(
                "same-id",
                "planning",
                Handoff::Plan {
                    done: true,
                    report: PayloadRef::inline("replacement plan"),
                    proposal: Some(replacement),
                    request_attention: false,
                },
                None,
            ),
        ])
        .expect("state");

        assert_eq!(
            state.planning.tasks[&tid("same-id")].final_response,
            Some(PayloadRef::inline("final response"))
        );
        assert_eq!(state.tasks[&tid("same-id")].final_response, None);
    }

    #[test]
    fn continue_targets_the_parked_effects_original_task_era() {
        let mut mission_created = created();
        let MissionEvent::MissionCreated { config, .. } = &mut mission_created else {
            unreachable!("created() builds MissionCreated");
        };
        config.planning.tasks.push(PlanningTask {
            id: tid("same-id"),
            role: RoleName::new("planner").expect("role name"),
            output: OutputSemantics::ProducesReport,
            body: "replace the rejected plan".into(),
            depends_on: vec![],
        });
        let MissionEvent::PlanProposed {
            proposal: mut replacement,
            ..
        } = plan_proposed(
            vec![assertion("AA", Some("cargo-test"))],
            vec![work_task("same-id")],
        )
        else {
            unreachable!("plan_proposed() builds PlanProposed");
        };
        replacement.base_revision = 1;
        let parked_effect = role_effect(crate::TaskNamespace::Planning, "same-id", 1, 1);
        let state = fold_log(vec![
            mission_created,
            plan_proposed(
                vec![assertion("AA", Some("cargo-test"))],
                vec![
                    work_task("seed"),
                    validate_task("v"),
                    gate_task("g", &["AA"], &["v"]),
                ],
            ),
            role_completed("seed", "seed", work_handoff(true, false), None),
            role_completed("v", "validation", validate_handoff(&[("AA", false)]), None),
            decision("gate_failed:g", super::super::event::DecisionAction::Revise),
            planning_role_requested(
                "same-id",
                "planning-failure",
                OutputSemantics::ProducesReport,
            ),
            MissionEvent::RoleRunCompleted {
                request: role_identity(
                    crate::TaskNamespace::Planning,
                    "same-id",
                    1,
                    1,
                    RoleName::new("planner").unwrap(),
                    OutputSemantics::ProducesReport,
                    "base",
                    true,
                ),
                effect_id: parked_effect.clone(),
                outcome: Err(TypedFailure::permanent("runtime.failed", "failed")),
            },
            MissionEvent::PlanProposed {
                proposal: replacement,
                plan_hash: "replacement".into(),
            },
            role_completed("same-id", "execution", work_handoff(true, false), None),
            MissionEvent::ControlRequested {
                effect_id: parked_effect,
                action: super::super::event::ControlAction::Continue { automatic: false },
                reason: "resume the planning effect".into(),
            },
        ])
        .expect("state");

        assert_eq!(
            state.planning.tasks[&tid("same-id")].status,
            TaskStatus::Pending
        );
        assert_eq!(state.tasks[&tid("same-id")].status, TaskStatus::Cleared);
    }

    #[test]
    fn role_handoffs_must_match_the_request_output_contract() {
        let mut mission_created = created();
        let MissionEvent::MissionCreated { config, .. } = &mut mission_created else {
            unreachable!("created() builds MissionCreated");
        };
        config.planning.tasks.push(PlanningTask {
            id: tid("author"),
            role: RoleName::new("planner").expect("role name"),
            output: OutputSemantics::ProposesPlan,
            body: "plan".into(),
            depends_on: vec![],
        });
        let MissionEvent::PlanProposed { proposal, .. } =
            plan_proposed(vec![], vec![work_task("work")])
        else {
            unreachable!("plan_proposed() builds PlanProposed");
        };
        let state = fold_log(vec![
            mission_created,
            plan_proposed(vec![], vec![work_task("work")]),
            role_requested("work", "wrong-namespace"),
            MissionEvent::RoleRunCompleted {
                request: role_identity(
                    crate::TaskNamespace::Execution,
                    "work",
                    1,
                    1,
                    RoleName::new("implementer").unwrap(),
                    OutputSemantics::ProducesArtifact,
                    "base",
                    true,
                ),
                effect_id: role_effect(crate::TaskNamespace::Execution, "work", 1, 1),
                outcome: Ok(RoleRunSuccess {
                    handoff: Some(Handoff::Plan {
                        done: true,
                        report: PayloadRef::inline("plan"),
                        proposal: Some(proposal),
                        request_attention: false,
                    }),
                    artifact: Some(ArtifactOutcome {
                        base_sha: "base".into(),
                        head_sha: "must-not-promote".into(),
                    }),
                    final_response: PayloadRef::inline("plan"),
                    runtime_configuration: RuntimeConfigurationEvidence::default(),
                }),
            },
        ])
        .expect("state");

        assert_eq!(
            state.planning.tasks[&tid("author")].status,
            TaskStatus::Pending
        );
        assert!(state.proposal.is_none());
        assert_eq!(state.tasks[&tid("work")].status, TaskStatus::Failed);
        assert!(matches!(
            state.tasks[&tid("work")].last_failure,
            Some(TypedFailure::InvalidOutput { .. })
        ));
        assert_eq!(state.current_sha, "base");
        assert!(state.inflight.is_empty());
    }

    #[test]
    fn planning_requests_must_match_the_resolved_role_output() {
        let mut mission_created = created();
        let MissionEvent::MissionCreated { config, .. } = &mut mission_created else {
            unreachable!("created() builds MissionCreated");
        };
        config.planning.tasks.push(PlanningTask {
            id: tid("research"),
            role: RoleName::new("reporter").expect("role name"),
            output: OutputSemantics::ProducesReport,
            body: "research".into(),
            depends_on: vec![],
        });
        let state = fold_log(vec![
            mission_created,
            role_requested_in(
                crate::TaskNamespace::Planning,
                "research",
                "swapped-output",
                RoleName::new("reporter").expect("role name"),
                OutputSemantics::ProposesPlan,
            ),
        ])
        .expect("state");

        assert!(state.inflight.is_empty());
        assert_eq!(
            state.planning.tasks[&tid("research")].status,
            TaskStatus::Pending
        );
        assert!(state.planning.tasks[&tid("research")]
            .last_failure
            .is_none());
    }

    #[test]
    fn a_paired_writer_history_cannot_run_without_a_dispatch_obligation() {
        let forged_effect = EffectId::for_parts(&["test", "forged-writer"]);
        let state = fold_log(vec![
            created(),
            plan_proposed(
                vec![assertion("TESTS-PASS", Some("cargo-test"))],
                vec![work_task("work")],
            ),
            role_completed(
                "work",
                "work",
                work_handoff(true, false),
                Some(ArtifactOutcome {
                    base_sha: "base".into(),
                    head_sha: "h1".into(),
                }),
            ),
            oracle_completed("TESTS-PASS", "h1", "oracle", 0),
            MissionEvent::RoleRunRequested {
                conversation_id: crate::ConversationId::for_role_instance(
                    &mission_id(),
                    crate::TaskNamespace::Execution,
                    &tid("work"),
                    &RoleName::new("implementer").unwrap(),
                    2,
                ),
                namespace: crate::TaskNamespace::Execution,
                task_id: tid("work"),
                attempt_no: 2,
                effect_id: forged_effect.clone(),
                role: RoleName::new("implementer").expect("role name"),
                output: OutputSemantics::ProducesArtifact,
                runtime: "codex".into(),
                prompt_template: crate::RolePromptTemplate::Execution,
                prompt_hash: PayloadRef::inline("forged prompt")
                    .content_sha256()
                    .unwrap(),
                base_sha: "h1".into(),
                assignment_epoch: 2,
                message_boundary: 0,
                presented_messages: vec![],
                recreate_workspace: false,
                requested_at_ms: 0,
                not_before_ms: 0,
                deadline_ms: 100_000,
                budget_deadline_ms: 100_000,
            },
            MissionEvent::RoleRunCompleted {
                request: role_identity(
                    crate::TaskNamespace::Execution,
                    "work",
                    2,
                    1,
                    RoleName::new("implementer").unwrap(),
                    OutputSemantics::ProducesArtifact,
                    "base",
                    true,
                ),
                effect_id: forged_effect,
                outcome: Ok(RoleRunSuccess {
                    handoff: Some(work_handoff(true, false)),
                    artifact: Some(ArtifactOutcome {
                        base_sha: "h1".into(),
                        head_sha: "forged-head".into(),
                    }),
                    final_response: PayloadRef::inline("forged"),
                    runtime_configuration: RuntimeConfigurationEvidence::default(),
                }),
            },
        ])
        .expect("state");

        assert!(state.inflight.is_empty());
        assert_eq!(state.current_sha, "h1");
        assert_eq!(state.tasks[&tid("work")].attempts, 1);
        assert!(matches!(
            state.phase,
            MissionPhase::Done {
                finish: FinishClass::Verified
            }
        ));
    }

    #[test]
    fn role_completion_must_match_the_exact_inflight_identity() {
        let mut mission_created = created();
        let MissionEvent::MissionCreated { config, .. } = &mut mission_created else {
            unreachable!("created() builds MissionCreated");
        };
        config.planning.tasks.push(PlanningTask {
            id: tid("original"),
            role: RoleName::new("planner").expect("role name"),
            output: OutputSemantics::ProducesReport,
            body: "plan".into(),
            depends_on: vec![],
        });
        let base = vec![
            mission_created,
            plan_proposed(vec![], vec![work_task("original"), work_task("claimed")]),
            role_requested("original", "identity"),
        ];
        let mismatches = [
            (crate::TaskNamespace::Planning, "original", 1),
            (crate::TaskNamespace::Execution, "claimed", 1),
            (crate::TaskNamespace::Execution, "original", 2),
        ];

        for (namespace, task_id, attempt_no) in mismatches {
            let mut events = base.clone();
            events.push(MissionEvent::RoleRunCompleted {
                request: role_identity(
                    namespace,
                    task_id,
                    attempt_no,
                    1,
                    RoleName::new("implementer").unwrap(),
                    OutputSemantics::ProducesArtifact,
                    "base",
                    true,
                ),
                effect_id: role_effect(crate::TaskNamespace::Execution, "original", 1, 1),
                outcome: Ok(RoleRunSuccess {
                    handoff: Some(work_handoff(true, false)),
                    artifact: None,
                    final_response: PayloadRef::inline("wrong completion"),
                    runtime_configuration: RuntimeConfigurationEvidence::default(),
                }),
            });
            let state = fold_log(events).expect("state");
            assert_eq!(state.inflight.len(), 1);
            assert_eq!(state.tasks[&tid("original")].status, TaskStatus::Running);
            assert!(state.tasks[&tid("original")].last_failure.is_none());
            assert_eq!(state.tasks[&tid("claimed")].status, TaskStatus::Pending);
            assert_eq!(
                state.planning.tasks[&tid("original")].status,
                TaskStatus::Pending
            );
            assert!(state.parked_effects.is_empty());
        }

        let baseline = fold_log(base).expect("baseline");
        let (effect_id, effect) = baseline.inflight.iter().next().unwrap();
        let valid = effect.role_request_identity().unwrap();
        for dimension in 0..15 {
            let mut request = valid.clone();
            let mut completed_effect_id = effect_id.clone();
            match dimension {
                0 => request.namespace = crate::TaskNamespace::Planning,
                1 => request.task_id = tid("claimed"),
                2 => request.attempt_no += 1,
                3 => request.assignment_epoch += 1,
                4 => {
                    request.conversation_id = crate::ConversationId::parse("f".repeat(64)).unwrap()
                }
                5 => request.role = RoleName::new("reviewer").unwrap(),
                6 => request.output = OutputSemantics::EmitsVerdict,
                7 => request.runtime = "other-profile".into(),
                8 => request.prompt_template = crate::RolePromptTemplate::Planning,
                9 => request.prompt_hash = "0".repeat(64),
                10 => request.base_sha = "other-base".into(),
                11 => request.recreate_workspace = !request.recreate_workspace,
                12 => request.message_boundary += 1,
                13 => request.presented_messages.push(999),
                14 => completed_effect_id = EffectId::for_parts(&["forged", "effect"]),
                _ => unreachable!(),
            }
            let mut rejected = baseline.clone();
            apply(
                &mut rejected,
                &envelope(
                    baseline.head + 1,
                    MissionEvent::RoleRunCompleted {
                        effect_id: completed_effect_id,
                        request: Box::new(request),
                        outcome: Err(TypedFailure::permanent("forged", "forged completion")),
                    },
                ),
            );
            assert_eq!(
                rejected.inflight, baseline.inflight,
                "dimension {dimension}"
            );
            assert_eq!(
                rejected.stop_requests, baseline.stop_requests,
                "dimension {dimension}"
            );
            assert_eq!(
                rejected.reached_deadlines, baseline.reached_deadlines,
                "dimension {dimension}"
            );
            assert_eq!(
                rejected.cleanup_failure, baseline.cleanup_failure,
                "dimension {dimension}"
            );
            assert_eq!(
                rejected.conversations, baseline.conversations,
                "dimension {dimension}"
            );
            assert_eq!(rejected.tasks, baseline.tasks, "dimension {dimension}");
        }
    }

    #[test]
    fn orphan_outcomes_cannot_settle_tasks_or_mint_authority() {
        let mut role_state = fold_log(vec![
            created(),
            plan_proposed(vec![], vec![work_task("work")]),
        ])
        .expect("role state");
        let next = role_state.head + 1;
        apply(
            &mut role_state,
            &envelope(
                next,
                role_completed(
                    "work",
                    "orphan-role",
                    work_handoff(true, false),
                    Some(ArtifactOutcome {
                        base_sha: "base".into(),
                        head_sha: "unrequested".into(),
                    }),
                ),
            ),
        );
        assert_eq!(role_state.tasks[&tid("work")].status, TaskStatus::Pending);
        assert_eq!(role_state.current_sha, "base");

        let mut oracle_state = fold_log(vec![
            created(),
            plan_proposed(
                vec![assertion("TESTS-PASS", Some("cargo-test"))],
                vec![work_task("work")],
            ),
        ])
        .expect("oracle state");
        let next = oracle_state.head + 1;
        apply(
            &mut oracle_state,
            &envelope(
                next,
                oracle_completed("TESTS-PASS", "base", "orphan-oracle", 0),
            ),
        );
        assert!(oracle_state.contract[&aid("TESTS-PASS")]
            .last_authoritative
            .is_none());

        let mut review_state = fold_log(vec![created_with_review()]).expect("review state");
        let next = review_state.head + 1;
        apply(
            &mut review_state,
            &envelope(
                next,
                review_completed_at(1, "orphan-review", "base", true, vec![]),
            ),
        );
        assert!(review_state.terminal_review.outcome.is_none());
    }

    #[test]
    fn artifacts_are_bound_to_writer_output_and_the_request_base() {
        let validator_effect = role_effect(crate::TaskNamespace::Execution, "validator", 1, 1);
        let mut validator_request = role_requested_in(
            crate::TaskNamespace::Execution,
            "validator",
            "validator-artifact",
            RoleName::new("reviewer").expect("role name"),
            OutputSemantics::EmitsVerdict,
        );
        let MissionEvent::RoleRunRequested { base_sha, .. } = &mut validator_request else {
            unreachable!("role_requested_in builds a request");
        };
        *base_sha = "base".into();
        let validator = fold_log(vec![
            created(),
            plan_proposed(vec![], vec![work_task("work"), validate_task("validator")]),
            role_completed("work", "work", work_handoff(true, false), None),
            validator_request,
            MissionEvent::RoleRunCompleted {
                request: role_identity(
                    crate::TaskNamespace::Execution,
                    "validator",
                    1,
                    1,
                    RoleName::new("reviewer").unwrap(),
                    OutputSemantics::EmitsVerdict,
                    "base",
                    true,
                ),
                effect_id: validator_effect,
                outcome: Ok(RoleRunSuccess {
                    handoff: Some(validate_handoff(&[])),
                    artifact: Some(ArtifactOutcome {
                        base_sha: "base".into(),
                        head_sha: "must-not-promote".into(),
                    }),
                    final_response: PayloadRef::inline("invalid artifact"),
                    runtime_configuration: RuntimeConfigurationEvidence::default(),
                }),
            },
        ])
        .expect("validator state");
        assert_eq!(
            validator.tasks[&tid("validator")].status,
            TaskStatus::Failed
        );
        assert_eq!(validator.current_sha, "base");

        let writer = fold_log(vec![
            created(),
            plan_proposed(vec![], vec![work_task("writer")]),
            role_requested("writer", "wrong-base"),
            role_completed(
                "writer",
                "wrong-base",
                work_handoff(true, false),
                Some(ArtifactOutcome {
                    base_sha: "different-base".into(),
                    head_sha: "must-not-promote".into(),
                }),
            ),
        ])
        .expect("writer state");
        assert_eq!(writer.tasks[&tid("writer")].status, TaskStatus::Failed);
        assert_eq!(writer.current_sha, "base");
    }

    #[test]
    fn incomplete_writer_outcomes_cannot_advance_the_deliverable_head() {
        let effect_id = role_effect(crate::TaskNamespace::Execution, "writer", 1, 1);
        let state = fold_log(vec![
            created(),
            plan_proposed(vec![], vec![work_task("writer")]),
            role_requested("writer", "incomplete-writer"),
            MissionEvent::RoleRunCompleted {
                request: role_identity(
                    crate::TaskNamespace::Execution,
                    "writer",
                    1,
                    1,
                    RoleName::new("implementer").unwrap(),
                    OutputSemantics::ProducesArtifact,
                    "base",
                    true,
                ),
                effect_id: effect_id.clone(),
                outcome: Ok(RoleRunSuccess {
                    handoff: Some(work_handoff(false, false)),
                    artifact: Some(ArtifactOutcome {
                        base_sha: "base".into(),
                        head_sha: "must-not-promote".into(),
                    }),
                    final_response: PayloadRef::inline("work is incomplete"),
                    runtime_configuration: RuntimeConfigurationEvidence::default(),
                }),
            },
        ])
        .expect("state");

        assert_eq!(state.tasks[&tid("writer")].status, TaskStatus::Failed);
        assert_eq!(state.current_sha, "base");
        assert!(state.parked_effects.contains_key(&effect_id));
    }

    #[test]
    fn stale_continue_cannot_resurrect_a_task_retired_by_plan_promotion() {
        let parked_effect = role_effect(crate::TaskNamespace::Execution, "retired", 1, 1);
        let MissionEvent::PlanProposed {
            proposal: mut replacement,
            ..
        } = plan_proposed(
            vec![assertion("AA", Some("cargo-test"))],
            vec![work_task("replacement")],
        )
        else {
            unreachable!("plan_proposed() builds PlanProposed");
        };
        replacement.base_revision = 1;

        let state = fold_log(vec![
            created(),
            plan_proposed(
                vec![assertion("AA", Some("cargo-test"))],
                vec![
                    work_task("retired"),
                    validate_task("v"),
                    gate_task("g", &["AA"], &["v"]),
                ],
            ),
            role_requested("retired", "retired-writer"),
            MissionEvent::RoleRunCompleted {
                request: role_identity(
                    crate::TaskNamespace::Execution,
                    "retired",
                    1,
                    1,
                    RoleName::new("implementer").unwrap(),
                    OutputSemantics::ProducesArtifact,
                    "base",
                    true,
                ),
                effect_id: parked_effect.clone(),
                outcome: Err(TypedFailure::permanent("runtime.failed", "failed")),
            },
            role_completed("v", "validator", validate_handoff(&[("AA", false)]), None),
            decision("gate_failed:g", super::super::event::DecisionAction::Revise),
            MissionEvent::PlanProposed {
                proposal: replacement,
                plan_hash: "replacement".into(),
            },
            MissionEvent::ControlRequested {
                effect_id: parked_effect.clone(),
                action: super::super::event::ControlAction::Continue { automatic: false },
                reason: "stale operator view".into(),
            },
        ])
        .expect("state");

        assert_eq!(state.tasks[&tid("retired")].status, TaskStatus::Superseded);
        assert!(!state.parked_effects.contains_key(&parked_effect));
        assert_eq!(state.tasks[&tid("replacement")].status, TaskStatus::Pending);
    }

    #[test]
    fn oracle_completion_identity_is_bound_to_its_request() {
        let effect_id = oracle_effect("base", 1);
        let state = fold_log(vec![
            created(),
            plan_proposed(
                vec![assertion("TESTS-PASS", Some("cargo-test"))],
                vec![work_task("work")],
            ),
            oracle_requested("TESTS-PASS", "base", "oracle-identity"),
            MissionEvent::OracleRunCompleted {
                assertion_ids: vec![aid("TESTS-PASS")],
                oracle: oracle("different-oracle"),
                judged_sha: "different-head".into(),
                attempt_no: 2,
                effect_id: effect_id.clone(),
                outcome: Ok(OracleRunSuccess {
                    exit_code: 0,
                    exit_signal: None,
                    stdout: PayloadRef::inline("out"),
                    stderr: PayloadRef::inline("err"),
                    prepared_inputs: Vec::new(),
                    duration_ms: 1,
                }),
            },
        ])
        .expect("state");

        assert_eq!(state.inflight.len(), 1);
        assert!(!state.oracle_failures.contains_key(&oracle("cargo-test")));
        assert!(state.contract[&aid("TESTS-PASS")]
            .last_authoritative
            .is_none());
        assert!(!state.parked_effects.contains_key(&effect_id));
    }

    #[test]
    fn oracle_requests_cannot_substitute_another_oracles_assertions() {
        let forged_effect = oracle_effect_for("lint", "h1", 1);
        let forged_request = MissionEvent::OracleRunRequested {
            assertion_ids: vec![aid("TESTS-PASS")],
            oracle: oracle("lint"),
            judged_sha: "h1".into(),
            attempt_no: 1,
            effect_id: forged_effect.clone(),
            requested_at_ms: 0,
            not_before_ms: 0,
            deadline_ms: 100_000,
        };
        let forged_completion = MissionEvent::OracleRunCompleted {
            assertion_ids: vec![aid("TESTS-PASS")],
            oracle: oracle("lint"),
            judged_sha: "h1".into(),
            attempt_no: 1,
            effect_id: forged_effect,
            outcome: Ok(OracleRunSuccess {
                exit_code: 0,
                exit_signal: None,
                stdout: PayloadRef::inline("not the bound oracle"),
                stderr: PayloadRef::inline(""),
                prepared_inputs: Vec::new(),
                duration_ms: 1,
            }),
        };
        let state = fold_log(vec![
            created(),
            plan_proposed(
                vec![
                    assertion("TESTS-PASS", Some("cargo-test")),
                    assertion("LINT-PASS", Some("lint")),
                ],
                vec![work_task("work")],
            ),
            role_completed(
                "work",
                "work",
                work_handoff(true, false),
                Some(ArtifactOutcome {
                    base_sha: "base".into(),
                    head_sha: "h1".into(),
                }),
            ),
            forged_request,
            forged_completion,
        ])
        .expect("state");

        assert!(state.inflight.is_empty());
        assert!(!state.oracle_attempts.contains_key(&oracle("lint")));
        assert!(state
            .contract
            .values()
            .all(|assertion| { assertion.last_authoritative.is_none() }));
        assert_eq!(state.phase, MissionPhase::Running);
    }

    #[test]
    fn planning_attention_cannot_leak_to_an_execution_task_with_the_same_id() {
        let mut mission_created = created();
        let MissionEvent::MissionCreated { config, .. } = &mut mission_created else {
            unreachable!("created() builds MissionCreated");
        };
        config.planning.tasks.push(PlanningTask {
            id: tid("same-id"),
            role: RoleName::new("planner").expect("role name"),
            output: OutputSemantics::ProposesPlan,
            body: "plan".into(),
            depends_on: vec![],
        });
        let MissionEvent::PlanProposed { proposal, .. } =
            plan_proposed(vec![], vec![work_task("same-id")])
        else {
            unreachable!("plan_proposed() builds PlanProposed");
        };
        let state = fold_log(vec![
            mission_created,
            planning_role_requested("same-id", "attention", OutputSemantics::ProposesPlan),
            planning_role_completed(
                "same-id",
                "attention",
                Handoff::Plan {
                    done: true,
                    report: PayloadRef::inline("plan"),
                    proposal: Some(proposal),
                    request_attention: true,
                },
                None,
            ),
            decision(
                "plan_proposal:mission",
                super::super::event::DecisionAction::Approve,
            ),
        ])
        .expect("state");

        assert_eq!(state.tasks[&tid("same-id")].status, TaskStatus::Pending);
        assert!(!state.open_attention.contains_key("node_attention:same-id"));
    }

    #[test]
    fn terminal_review_completion_identity_is_bound_to_its_request() {
        let effect_id = review_effect("h1", 1);
        let observed = RuntimeConfigurationEvidence {
            applied_model: Some("observed-model".into()),
            ..Default::default()
        };
        let mut events = events_to_the_brink();
        events.extend([
            review_requested(1, "review-identity", "h1"),
            MissionEvent::EffectRuntimeConfigured {
                effect_id: effect_id.clone(),
                configuration: observed.clone(),
            },
            MissionEvent::TerminalReviewCompleted {
                attempt_no: 2,
                effect_id: effect_id.clone(),
                judged_sha: "different-head".into(),
                outcome: Ok(TerminalReviewSuccess {
                    passed: true,
                    gaps: vec![],
                    report: PayloadRef::inline("mismatched review"),
                    final_response: PayloadRef::inline("mismatched review"),
                    runtime_configuration: RuntimeConfigurationEvidence::default(),
                }),
            },
        ]);
        let state = fold_log(events).expect("state");

        assert_eq!(state.inflight.len(), 1);
        assert!(state.terminal_review.outcome.is_none());
        assert!(!state.parked_effects.contains_key(&effect_id));
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
                mid.inflight.contains_key(&oracle_effect("base", 1)),
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
                effect_id: oracle_effect("base", 1),
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
            plan_proposed(
                vec![assertion("TESTS-PASS", Some("cargo-test"))],
                vec![work_task("work")],
            ),
            role_completed("work", "work", work_handoff(true, false), None),
            oracle_requested("TESTS-PASS", "base", "ko"),
            MissionEvent::OracleRunCompleted {
                assertion_ids: vec![aid("TESTS-PASS")],
                oracle: oracle("cargo-test"),
                judged_sha: "base".into(),
                attempt_no: 1,
                effect_id: oracle_effect("base", 1),
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
        // Later facts remain in the log, but an aborted mission has no legal
        // dispatch obligation, so the paired role history gains no authority.
        assert_eq!(state.tasks[&tid("t1")].status, TaskStatus::Pending);
        assert_eq!(state.head, 5);
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
            plan_proposed(
                vec![assertion("KNOWN-1", Some("cargo-test"))],
                vec![work_task("known")],
            ),
            role_completed("known", "known", work_handoff(true, false), None),
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
                request: role_identity(
                    crate::TaskNamespace::Execution,
                    "specter",
                    1,
                    1,
                    RoleName::new("implementer").unwrap(),
                    OutputSemantics::ProducesArtifact,
                    "base",
                    true,
                ),
                effect_id: EffectId::for_parts(&["test", "k3"]),
                outcome: Err(TypedFailure::permanent("runtime.gone", "gone")),
            },
        ])
        .expect("state");
        // No panics and no phantom rows: outcomes only touch declared ids.
        // Because attention is derived from the declared plan, an outcome for
        // an undeclared task raises nothing (it cannot, and should not).
        assert_eq!(state.tasks.len(), 1);
        assert!(state.tasks.contains_key(&tid("known")));
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

    fn review_requested(attempt_no: u32, _key: &str, judged: &str) -> MissionEvent {
        MissionEvent::TerminalReviewRequested {
            attempt_no,
            effect_id: review_effect(judged, attempt_no),
            role: RoleName::new("gap-reviewer").expect("role name"),
            runtime: "codex".into(),
            prompt: PayloadRef::inline("review prompt"),
            judged_sha: judged.into(),
            nonce: "n0".into(),
            requested_at_ms: 0,
            not_before_ms: 0,
            deadline_ms: 100_000,
            budget_deadline_ms: 100_000,
        }
    }

    fn review_completed_at(
        attempt_no: u32,
        _key: &str,
        judged: &str,
        passed: bool,
        gaps: Vec<Gap>,
    ) -> MissionEvent {
        MissionEvent::TerminalReviewCompleted {
            attempt_no,
            effect_id: review_effect(judged, attempt_no),
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

    fn review_failed_at(attempt_no: u32, _key: &str, judged: &str, detail: &str) -> MissionEvent {
        MissionEvent::TerminalReviewCompleted {
            attempt_no,
            effect_id: review_effect(judged, attempt_no),
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
        _key: &str,
        judged: &str,
        detail: &str,
    ) -> MissionEvent {
        MissionEvent::TerminalReviewCompleted {
            attempt_no,
            effect_id: review_effect(judged, attempt_no),
            judged_sha: judged.into(),
            outcome: Err(TypedFailure::transient("runtime.busy", detail, None)),
        }
    }

    fn move_head_with_fresh_proof(state: &mut MissionState, head: &str) {
        state.current_sha = head.to_string();
        for assertion in state.contract.values_mut() {
            let oracle = assertion.oracle.clone().expect("oracle-bound assertion");
            assertion.last_authoritative = Some(AuthoritativeVerdict::from_oracle_outcome(
                oracle,
                head.to_string(),
                0,
                None,
                PayloadRef::inline("pass"),
                PayloadRef::inline(""),
                Vec::new(),
            ));
        }
        derive_attention(state);
        derive_phase(state);
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

        // … and hostile injected review events remain durable facts but gain
        // no authority when the pinned config did not request a review.
        events.push(review_completed(
            "kr",
            "h1",
            false,
            vec![gap(GapSeverity::Blocking)],
        ));
        let state = fold_log(events).expect("state");
        assert!(state.terminal_review.outcome.is_none());
        assert!(state.open_attention.is_empty());
        assert_eq!(
            state.phase,
            MissionPhase::Done {
                finish: FinishClass::Verified
            }
        );
    }

    #[test]
    fn terminal_review_request_cannot_substitute_the_configured_reviewer() {
        let mut events = events_to_the_brink();
        let effect_id = review_effect("h1", 1);
        events.push(MissionEvent::TerminalReviewRequested {
            attempt_no: 1,
            effect_id: effect_id.clone(),
            role: RoleName::new("imposter").expect("role name"),
            runtime: "codex".into(),
            prompt: PayloadRef::inline("forged review prompt"),
            judged_sha: "h1".into(),
            nonce: "forged".into(),
            requested_at_ms: 0,
            not_before_ms: 0,
            deadline_ms: 100_000,
            budget_deadline_ms: 100_000,
        });
        events.push(review_completed_at(
            1,
            "substituted-reviewer",
            "h1",
            true,
            vec![],
        ));

        let state = fold_log(events).expect("state");
        assert!(state.inflight.is_empty());
        assert_eq!(state.terminal_review.attempts, 0);
        assert!(state.terminal_review.outcome.is_none());
        assert_eq!(state.phase, MissionPhase::Running);
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
        let mut reopened = fold_log(events).expect("state");
        assert!(matches!(reopened.phase, MissionPhase::Done { .. }));

        // Model a later deliverable with fresh proof. The review and its
        // acknowledgment are both keyed to h1, so the closing review reopens.
        move_head_with_fresh_proof(&mut reopened, "h2");
        assert!(terminal_review_outstanding(&reopened));
        assert_eq!(reopened.phase, MissionPhase::Running);

        let request_seq = reopened.head + 1;
        apply(
            &mut reopened,
            &envelope(request_seq, review_requested(2, "kr2", "h2")),
        );
        apply(
            &mut reopened,
            &envelope(
                request_seq + 1,
                review_completed_at(2, "kr2", "h2", false, vec![gap(GapSeverity::Blocking)]),
            ),
        );
        assert_eq!(
            reopened.terminal_review.accepted,
            Some(accepted(ReviewAcceptanceKind::AcknowledgedGaps, "h1"))
        );
        assert_eq!(reopened.phase, MissionPhase::AttentionNeeded);
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
    fn terminal_review_failure_retains_effect_scoped_runtime_configuration() {
        let mut events = events_to_the_brink();
        events.push(review_requested(1, "kr", "h1"));
        events.push(MissionEvent::EffectRuntimeConfigured {
            effect_id: review_effect("h1", 1),
            configuration: RuntimeConfigurationEvidence {
                requested_model: Some("requested".into()),
                applied_model: Some("applied".into()),
                model_confirmation: Some(crate::RuntimeConfigurationConfirmation::Observed),
                requested_mode: Some("build".into()),
                applied_mode: Some("build".into()),
                mode_confirmation: Some(crate::RuntimeConfigurationConfirmation::Observed),
            },
        });
        events.push(review_failed("kr", "h1", "forced cancellation"));

        let state = fold_log(events).expect("state");
        let Some(ReviewOutcome::Failed { failure }) = state.terminal_review.outcome else {
            panic!("terminal review failure must be retained");
        };
        assert_eq!(
            failure.evidence().configuration.applied_model.as_deref(),
            Some("applied")
        );
        assert_eq!(
            failure.evidence().configuration.applied_mode.as_deref(),
            Some("build")
        );
    }

    #[test]
    fn prior_successful_reviews_do_not_consume_the_recovery_budget() {
        use super::super::step::{step, StepDecision};

        let mut events = events_to_the_brink();
        let MissionEvent::MissionCreated { config, .. } = &mut events[0] else {
            unreachable!("events_to_the_brink starts with MissionCreated");
        };
        config.recovery.max_attempts = 3;
        events.push(review_requested(1, "kr1", "h1"));
        events.push(review_completed_at(1, "kr1", "h1", true, vec![]));
        let mut state = fold_log(events).expect("state");
        move_head_with_fresh_proof(&mut state, "h2");
        let request_seq = state.head + 1;
        apply(
            &mut state,
            &envelope(request_seq, review_requested(2, "kr2", "h2")),
        );
        apply(
            &mut state,
            &envelope(
                request_seq + 1,
                review_transient_failed_at(2, "kr2", "h2", "temporary overload"),
            ),
        );
        assert_eq!(state.terminal_review.attempts, 2);
        assert_eq!(state.terminal_review.consecutive_failures, 1);
        assert!(state.open_attention.is_empty());
        assert_eq!(state.phase, MissionPhase::Running);
        assert!(matches!(
            step(&state),
            StepDecision::ReviewTerminal(intent) if intent.attempt_no == 3
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
        let mut state = fold_log(events).expect("state");
        assert!(matches!(state.phase, MissionPhase::Done { .. }));
        move_head_with_fresh_proof(&mut state, "h2");
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
        events.push(review_completed_at(2, "kr2", "h1", true, vec![]));
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

    #[test]
    fn checkpoint_and_atomic_message_routing_are_conversation_scoped() {
        let mut checkpoint = role_completed("w", "checkpoint", work_handoff(true, false), None);
        let MissionEvent::RoleRunCompleted {
            outcome: Ok(success),
            ..
        } = &mut checkpoint
        else {
            unreachable!()
        };
        success.handoff = None;
        let mut events = vec![
            created(),
            plan_proposed(vec![], vec![work_task("w")]),
            role_requested("w", "first"),
            checkpoint,
        ];
        let checkpoint_state = fold_log(events.clone()).expect("checkpoint state");
        let (conversation_id, conversation) = checkpoint_state
            .conversations
            .iter()
            .next()
            .expect("conversation");
        assert_eq!(
            conversation.lifecycle,
            super::super::state::ConversationLifecycle::AwaitingLead
        );
        assert_eq!(
            checkpoint_state.tasks[&tid("w")].status,
            TaskStatus::Running
        );

        let recipient = super::super::event::ConversationRecipient {
            conversation_id: conversation_id.clone(),
            role: conversation.role.clone(),
            namespace: conversation.namespace,
            task_id: conversation.task_id.clone(),
            assignment_epoch: conversation.assignment_epoch,
        };
        events.push(MissionEvent::MessageSent {
            recipients: vec![recipient.clone()],
            body: "continue".into(),
            references: vec![super::super::event::MessageReference::ReachableCommit {
                sha: "base".into(),
            }],
        });
        let resumed = fold_log(events.clone()).expect("resumed state");
        assert_eq!(resumed.tasks[&tid("w")].status, TaskStatus::Pending);
        assert_eq!(resumed.conversations[conversation_id].queued.len(), 1);

        let mut forged = recipient;
        forged.conversation_id = crate::ConversationId::parse("f".repeat(64)).unwrap();
        events.push(MissionEvent::MessageSent {
            recipients: vec![forged],
            body: "must be atomic".into(),
            references: vec![],
        });
        events.push(MissionEvent::MessageSent {
            recipients: vec![super::super::event::ConversationRecipient {
                conversation_id: conversation_id.clone(),
                role: conversation.role.clone(),
                namespace: conversation.namespace,
                task_id: conversation.task_id.clone(),
                assignment_epoch: conversation.assignment_epoch,
            }],
            body: "invalid reference".into(),
            references: vec![super::super::event::MessageReference::ReachableCommit {
                sha: "foreign".into(),
            }],
        });
        let rejected = fold_log(events).expect("rejected state");
        assert_eq!(rejected.conversations[conversation_id].queued.len(), 1);
    }

    #[test]
    fn request_boundary_consumes_only_prior_messages_and_mismatch_cannot_advance() {
        let mut checkpoint = role_completed("w", "checkpoint", work_handoff(true, false), None);
        let MissionEvent::RoleRunCompleted {
            outcome: Ok(success),
            ..
        } = &mut checkpoint
        else {
            unreachable!()
        };
        success.handoff = None;
        let seed = vec![
            created(),
            plan_proposed(vec![], vec![work_task("w")]),
            role_requested("w", "first"),
            checkpoint,
        ];
        let state = fold_log(seed.clone()).unwrap();
        let (conversation_id, conversation) = state.conversations.iter().next().unwrap();
        let recipient = super::super::event::ConversationRecipient {
            conversation_id: conversation_id.clone(),
            role: conversation.role.clone(),
            namespace: conversation.namespace,
            task_id: conversation.task_id.clone(),
            assignment_epoch: 1,
        };
        let mut events = seed;
        events.push(MissionEvent::MessageSent {
            recipients: vec![recipient.clone()],
            body: "before".into(),
            references: vec![],
        });
        let mut request = role_requested("w", "second");
        if let MissionEvent::RoleRunRequested {
            attempt_no,
            effect_id,
            ..
        } = &mut request
        {
            *attempt_no = 2;
            *effect_id = role_effect(crate::TaskNamespace::Execution, "w", 2, 1);
        }
        events.push(request);
        events.push(MissionEvent::MessageSent {
            recipients: vec![recipient],
            body: "during".into(),
            references: vec![],
        });
        let mut forged = role_completed_at(
            crate::TaskNamespace::Execution,
            "w",
            2,
            "forged",
            work_handoff(true, false),
            None,
        );
        if let MissionEvent::RoleRunCompleted { request, .. } = &mut forged {
            request.task_id = tid("other");
        }
        events.push(forged);
        let mismatched = fold_log(events.clone()).unwrap();
        assert_eq!(mismatched.conversations[conversation_id].queued.len(), 2);
        assert_eq!(mismatched.inflight.len(), 1);
        assert!(mismatched.conversations[conversation_id]
            .active_delivery
            .is_some());
        assert_eq!(mismatched.tasks[&tid("w")].status, TaskStatus::Running);

        for forged in [
            {
                let mut event = role_completed_at(
                    crate::TaskNamespace::Execution,
                    "w",
                    2,
                    "forged-attempt",
                    work_handoff(true, false),
                    None,
                );
                if let MissionEvent::RoleRunCompleted { request, .. } = &mut event {
                    request.attempt_no = 1;
                }
                event
            },
            role_completed_at(
                crate::TaskNamespace::Planning,
                "w",
                2,
                "forged-namespace",
                work_handoff(true, false),
                None,
            ),
        ] {
            let mut forged_log = events.clone();
            forged_log.pop();
            forged_log.push(forged);
            let rejected = fold_log(forged_log).unwrap();
            assert_eq!(rejected.inflight.len(), 1);
            assert_eq!(rejected.conversations[conversation_id].queued.len(), 2);
            assert!(rejected.conversations[conversation_id]
                .active_delivery
                .is_some());
        }

        events.pop();
        events.push(role_completed_at(
            crate::TaskNamespace::Execution,
            "w",
            2,
            "valid",
            work_handoff(true, false),
            None,
        ));
        let completed = fold_log(events).unwrap();
        let queued = &completed.conversations[conversation_id].queued;
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].body, "during");
    }

    #[test]
    fn delivery_failure_state_machine_preserves_boundary_and_reports_retry_uncertainty() {
        let mut checkpoint = role_completed("w", "checkpoint", work_handoff(true, false), None);
        let MissionEvent::RoleRunCompleted {
            outcome: Ok(success),
            ..
        } = &mut checkpoint
        else {
            unreachable!()
        };
        success.handoff = None;
        let seed = vec![
            created(),
            plan_proposed(vec![], vec![work_task("w")]),
            role_requested("w", "first"),
            checkpoint,
        ];
        let state = fold_log(seed.clone()).unwrap();
        let (conversation_id, conversation) = state.conversations.iter().next().unwrap();
        let initially_consumed_through = conversation.consumed_through;
        let recipient = super::super::event::ConversationRecipient {
            conversation_id: conversation_id.clone(),
            role: conversation.role.clone(),
            namespace: conversation.namespace,
            task_id: conversation.task_id.clone(),
            assignment_epoch: conversation.assignment_epoch,
        };

        let requested = |events: &mut Vec<MissionEvent>| {
            events.push(MissionEvent::MessageSent {
                recipients: vec![recipient.clone()],
                body: "before".into(),
                references: vec![],
            });
            let mut request = role_requested("w", "second");
            if let MissionEvent::RoleRunRequested {
                attempt_no,
                effect_id,
                ..
            } = &mut request
            {
                *attempt_no = 2;
                *effect_id = role_effect(crate::TaskNamespace::Execution, "w", 2, 1);
            }
            events.push(request);
            events.push(MissionEvent::MessageSent {
                recipients: vec![recipient.clone()],
                body: "during".into(),
                references: vec![],
            });
        };
        let failed = |failure: TypedFailure| {
            let mut completed = role_completed_at(
                crate::TaskNamespace::Execution,
                "w",
                2,
                "failure",
                work_handoff(true, false),
                None,
            );
            let MissionEvent::RoleRunCompleted { outcome, .. } = &mut completed else {
                unreachable!()
            };
            *outcome = Err(failure);
            completed
        };

        let mut launch_events = seed.clone();
        requested(&mut launch_events);
        launch_events.push(failed(TypedFailure::permanent(
            "kernel.launch",
            "session was never opened",
        )));
        let launch = fold_log(launch_events).unwrap();
        let launch_delivery = &launch.conversations[conversation_id];
        assert_eq!(launch_delivery.queued.len(), 2);
        assert!(launch_delivery
            .queued
            .iter()
            .all(|message| message.marker == super::super::state::DeliveryMarker::Queued));
        assert_eq!(launch_delivery.consumed_through, initially_consumed_through);
        assert!(launch_delivery.active_delivery.is_none());

        for (failure, expected) in [
            (
                TypedFailure::invalid("handoff.schema", "delivered malformed handoff"),
                super::super::state::DeliveryMarker::PreviouslyDelivered,
            ),
            (
                TypedFailure::Interrupted {
                    evidence: Box::new(TypedFailureEvidence::new(
                        None,
                        "driver died after delivery",
                    )),
                },
                super::super::state::DeliveryMarker::PossiblyDelivered,
            ),
        ] {
            let mut events = seed.clone();
            requested(&mut events);
            events.push(failed(failure));
            let state = fold_log(events).unwrap();
            let delivery = &state.conversations[conversation_id];
            assert_eq!(delivery.queued.len(), 2);
            assert_eq!(delivery.queued[0].marker, expected);
            assert_eq!(
                delivery.queued[1].marker,
                super::super::state::DeliveryMarker::Queued,
                "a message beyond the immutable request boundary was not delivered"
            );
            assert_eq!(delivery.consumed_through, initially_consumed_through);
            assert!(delivery.active_delivery.is_none());
        }
    }
}
