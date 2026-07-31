//! Phase 0 control-plane liveness laws over generated valid event prefixes.

mod common;

use common::{
    approve_plan, harness, proposal, proposal_from_plan, review_runner, simple_plan, BASE_SHA,
};
use lionclaw::engine::MissionView;
use lionclaw::model::{
    apply, fold, AssertionSupersession, Choice, DecisionAction, EffectIntent, EventEnvelope,
    MissionEvent, PlanProposal, TerminalState, VersionStamps, SCHEMA_VERSION,
};
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};
use lionclaw_runtime_api::TypedFailure;

const HISTORICAL_WEDGES: [&str; 5] = [
    "host-proof-without-evidence-ingestion",
    "monotonic-plan-invalid-refinement",
    "retained-workspace-ancestry-failure",
    "unreachable-simultaneously-current-contract",
    "superseded-task-message-strand",
];

fn envelope(state: &lionclaw::model::MissionState, event: MissionEvent) -> EventEnvelope {
    EventEnvelope {
        mission_id: state.mission_id.clone(),
        sequence_no: state.head + 1,
        recorded_at_ms: 0,
        stamps: VersionStamps {
            schema_version: SCHEMA_VERSION,
            engine_version: env!("CARGO_PKG_VERSION").to_string(),
            prompt_hash: None,
        },
        event,
    }
}

fn has_current_recipient(state: &lionclaw::model::MissionState) -> bool {
    state
        .conversations
        .keys()
        .any(|conversation_id| state.conversation_accepts_message(conversation_id))
}

fn assert_advertised_actions_are_legal(view: &MissionView) {
    for effect in &view.next.effects {
        match effect {
            EffectIntent::ResolveEffect { effect_id } => {
                assert!(view.state.inflight.contains_key(effect_id));
            }
            EffectIntent::CleanupConversation {
                role_instance,
                effect_id,
            } => {
                let conversation = &view.state.conversations[role_instance];
                assert_eq!(
                    conversation.disposable_resource_owner.as_ref(),
                    Some(effect_id)
                );
                assert!(matches!(
                    conversation.lifecycle,
                    lionclaw::model::ConversationLifecycle::Completed
                        | lionclaw::model::ConversationLifecycle::Retired
                ));
                assert!(!view.state.inflight.values().any(|effect| {
                    matches!(
                        effect,
                        lionclaw::model::InflightEffect::RoleTurn {
                            role_instance: active,
                            ..
                        } if active == role_instance
                    )
                }));
            }
            EffectIntent::DispatchRole(_) | EffectIntent::DispatchOracle(_) => {
                assert!(!view.state.is_terminal());
                assert!(view.state.inflight.is_empty());
            }
        }
    }
    for choice in &view.next.choices {
        match choice {
            Choice::ProposePlan { base_revision } => {
                assert_eq!(*base_revision, view.state.revision);
                assert!(!view.state.is_terminal());
                assert!(view.state.inflight.is_empty());
            }
            Choice::ConfigureTeam { revision } => {
                assert_eq!(
                    *revision,
                    view.state
                        .team
                        .as_ref()
                        .map_or(0, |team| team.revision.saturating_add(1))
                );
                assert!(!view.state.is_terminal());
                assert!(view.state.inflight.is_empty());
            }
            Choice::AddMissionSkill => {
                assert!(!view.state.is_terminal());
                assert!(view.state.inflight.is_empty());
            }
            Choice::AssignEnvironment { team_revision } => {
                assert_eq!(
                    Some(*team_revision),
                    view.state.team.as_ref().map(|team| team.revision)
                );
                assert!(!view.state.is_terminal());
                assert!(view.state.inflight.is_empty());
            }
            Choice::Decide { id, action } => {
                lionclaw::model::validate_decision(&view.state, id, action, "liveness probe")
                    .expect("advertised decision must validate");
            }
            Choice::SendMessage { role_instance } => {
                assert!(view.state.conversation_accepts_message(role_instance));
                assert!(has_current_recipient(&view.state));
            }
            Choice::Stop { effect_id } => {
                assert!(view.state.inflight.contains_key(effect_id));
                assert!(!view.state.reached_deadlines.contains_key(effect_id));
            }
            Choice::ExtendDeadline {
                effect_id,
                old_deadline_ms,
            } => {
                assert_eq!(
                    view.state.inflight[effect_id].deadline_ms(),
                    *old_deadline_ms
                );
                assert!(!view.state.reached_deadlines.contains_key(effect_id));
            }
            Choice::Continue { effect_id, mode } => {
                assert!(view.state.parked_continue_is_legal(effect_id, *mode));
            }
            Choice::Finish { finish } => {
                let mut finished = view.state.clone();
                apply(
                    &mut finished,
                    &envelope(
                        &view.state,
                        MissionEvent::MissionFinished {
                            finish: *finish,
                            reason: "liveness probe".to_string(),
                        },
                    ),
                );
                assert_eq!(finished.finish(), Some(*finish));
            }
            Choice::Abort => assert!(!view.state.is_terminal()),
            Choice::Apply { branch, sha } => {
                assert!(matches!(
                    view.state.terminal,
                    Some(TerminalState::Done { .. })
                ));
                assert_eq!(sha, view.state.deliverable_head());
                assert_eq!(branch, &format!("lionclaw/{}", view.state.mission_id));
            }
        }
    }
}

fn assert_decisions_change_authority(state: &lionclaw::model::MissionState) -> bool {
    let mut found = false;
    for choice in lionclaw::model::next(state).choices {
        if let Choice::Decide { id, action } = choice {
            found = true;
            let requirement_changes = if action == DecisionAction::Approve {
                state
                    .proposal
                    .as_ref()
                    .and_then(|proposal| proposal.plan.as_ref())
                    .map(|proposal| proposal.requirement_changes.clone())
                    .unwrap_or_default()
            } else {
                vec![]
            };
            let proposal_runtime_identities = if action == DecisionAction::Approve {
                state
                    .proposal
                    .as_ref()
                    .and_then(|proposal| proposal.team.as_ref())
                    .map(|team| {
                        team.roles
                            .iter()
                            .map(|(role_id, role)| {
                                (
                                    role_id.clone(),
                                    lionclaw::model::RuntimeInstrumentIdentity {
                                        runtime: role.runtime.clone(),
                                        model: None,
                                        mode: None,
                                        model_network: lionclaw::model::NetworkGrant::Deny,
                                    },
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            } else {
                Default::default()
            };
            let mut changed = state.clone();
            apply(
                &mut changed,
                &envelope(
                    state,
                    MissionEvent::DecisionRecorded {
                        attention_id: id.clone(),
                        action: action.clone(),
                        justification: "liveness probe".to_string(),
                        requirement_changes,
                        proposal_runtime_identities,
                    },
                ),
            );
            changed.head = state.head;
            assert_ne!(
                &changed, state,
                "advertised {action:?} did not change '{}'",
                id
            );
        }
    }
    found
}

fn assert_abort_preserves_authority(state: &lionclaw::model::MissionState) {
    let mut aborted = state.clone();
    apply(
        &mut aborted,
        &envelope(
            state,
            MissionEvent::MissionAborted {
                reason: "liveness probe".to_string(),
            },
        ),
    );
    assert!(matches!(
        aborted.terminal,
        Some(TerminalState::Aborted { .. })
    ));
    assert_eq!(aborted.plan, state.plan);
    assert_eq!(aborted.proposal, state.proposal);
    assert_eq!(aborted.revision, state.revision);
    assert_eq!(aborted.current_sha, state.current_sha);
    assert_eq!(aborted.contract, state.contract);
    assert_eq!(aborted.superseded_assertions, state.superseded_assertions);
    assert_eq!(aborted.tasks, state.tasks);
    assert_eq!(aborted.conversations.len(), state.conversations.len());
    for (id, before) in &state.conversations {
        let after = &aborted.conversations[id];
        assert_eq!(after.role_instance, before.role_instance);
        assert_eq!(after.final_response, before.final_response);
        assert_eq!(
            after.invalid_handoff_reworks,
            before.invalid_handoff_reworks
        );
        assert_eq!(after.queued.len(), before.queued.len());
        assert_eq!(
            after.lifecycle,
            lionclaw::model::ConversationLifecycle::Retired
        );
        assert!(after.active_delivery.is_none());
        for (after_message, before_message) in after.queued.iter().zip(&before.queued) {
            assert_eq!(after_message.sequence_no, before_message.sequence_no);
            assert_eq!(after_message.body, before_message.body);
            assert_eq!(after_message.references, before_message.references);
            assert_eq!(
                after_message.marker,
                lionclaw::model::DeliveryMarker::Undeliverable
            );
        }
    }
    assert_eq!(aborted.authoritative_receipts, state.authoritative_receipts);
    assert_eq!(aborted.reachable_commits, state.reachable_commits);
    assert_eq!(aborted.parked_effects, state.parked_effects);
    for driver_running in [false, true] {
        let view = MissionView::from_state(aborted.clone(), driver_running);
        assert_eq!(view.driver_running, driver_running);
        assert_advertised_actions_are_legal(&view);
        assert!(!view
            .next
            .choices
            .iter()
            .any(|choice| matches!(choice, Choice::Abort)));
    }
    for effect_id in state.inflight.keys() {
        let mut forged = aborted.clone();
        apply(
            &mut forged,
            &envelope(
                &aborted,
                MissionEvent::ControlRequested {
                    effect_id: effect_id.clone(),
                    action: lionclaw::model::ControlAction::Stop,
                    reason: "forged terminal stop".to_string(),
                },
            ),
        );
        assert_eq!(
            forged.stop_requests, aborted.stop_requests,
            "an unadvertised terminal control must be inert"
        );
    }
}

fn assert_prefix_liveness(name: &str, events: &[EventEnvelope]) {
    assert!(!events.is_empty(), "{name} has no events");
    for end in 1..=events.len() {
        let prefix = events[..end].to_vec();
        let state = fold(prefix.clone()).expect("valid scenario prefix");
        assert_eq!(fold(prefix.clone()).unwrap(), state, "{name} repeat fold");
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(
            serde_json::from_str::<lionclaw::model::MissionState>(&json).unwrap(),
            state,
            "{name} snapshot form"
        );
        for split in 1..end {
            let mut incremental = fold(prefix[..split].to_vec()).unwrap();
            for event in &prefix[split..] {
                apply(&mut incremental, event);
            }
            assert_eq!(incremental, state, "{name} incremental split {split}");
        }

        let view = MissionView::from_state(state.clone(), false);
        assert_advertised_actions_are_legal(&view);
        if state.is_terminal() {
            assert!(!view
                .next
                .choices
                .iter()
                .any(|choice| matches!(choice, Choice::Abort)));
            continue;
        }

        let deterministic_progress = !view.next.effects.is_empty()
            || !state.inflight.is_empty()
            || state.cleanup_failure.is_some();
        let repair_changes_state = assert_decisions_change_authority(&state);
        let abort_is_legal = view
            .next
            .choices
            .iter()
            .any(|choice| matches!(choice, Choice::Abort));
        assert!(
            deterministic_progress || repair_changes_state || abort_is_legal,
            "{name} prefix {end} has no legal state-changing exit"
        );
        assert!(abort_is_legal, "{name} prefix {end} hid universal abort");
        assert_abort_preserves_authority(&state);
    }
}

#[tokio::test]
async fn every_historical_wedge_seed_has_a_replay_safe_exit() {
    let correction_dir = tempfile::tempdir().unwrap();
    let correction = harness(
        correction_dir.path(),
        review_runner(vec![]),
        MockOracleRunner::exiting(1),
    )
    .await;
    let correction_id = correction
        .engine
        .create_mission(
            correction_dir.path().to_str().unwrap(),
            "correct an accepted assertion",
            BASE_SHA,
        )
        .await
        .unwrap();
    correction
        .engine
        .propose_plan(&correction_id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&correction.engine, &correction_id).await;
    correction.engine.advance(&correction_id).await.unwrap();
    let mut corrected = simple_plan();
    corrected.assertions[0].prose = "the corrected test contract exits zero".to_string();
    let assertion_id = corrected.assertions[0].id.clone();
    correction
        .engine
        .propose_plan(
            &correction_id,
            proposal_from_plan(
                PlanProposal {
                    base_revision: 1,
                    requirement_changes: vec![],
                    assertion_supersessions: vec![AssertionSupersession {
                        assertion_id: assertion_id.clone(),
                        replacement_ids: vec![assertion_id],
                    }],
                    plan: corrected,
                },
                false,
            ),
        )
        .await
        .unwrap();
    approve_plan(&correction.engine, &correction_id).await;
    let correction_events = correction
        .engine
        .store()
        .load(&correction_id)
        .await
        .unwrap();

    let failure_dir = tempfile::tempdir().unwrap();
    let failed = harness(
        failure_dir.path(),
        MockRoleRunner::new(Box::new(|_| {
            Err(TypedFailure::permanent(
                "workspace.ancestry",
                "retained workspace does not descend from its recorded base".to_string(),
            ))
        })),
        MockOracleRunner::exiting(0),
    )
    .await;
    let failed_id = failed
        .engine
        .create_mission(
            failure_dir.path().to_str().unwrap(),
            "replace failed work",
            BASE_SHA,
        )
        .await
        .unwrap();
    failed
        .engine
        .propose_plan(&failed_id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&failed.engine, &failed_id).await;
    failed.engine.advance(&failed_id).await.unwrap();
    let failure_events = failed.engine.store().load(&failed_id).await.unwrap();
    let failed_state = failed.engine.load_state(&failed_id).await.unwrap();
    let attention_id = common::decision_id_with_prefix(&failed_state, "node_failed:");
    assert!(failed_state
        .task_last_role_attempt(&lionclaw::model::TaskId::new("fix").unwrap())
        .is_some());
    failed
        .engine
        .decide(
            &failed_id,
            &attention_id,
            DecisionAction::Revise,
            "replace the failed assignment",
        )
        .await
        .unwrap();
    let replanning_state = failed.engine.load_state(&failed_id).await.unwrap();
    assert!(lionclaw::model::next(&replanning_state)
        .effects
        .iter()
        .any(|effect| matches!(effect, EffectIntent::DispatchRole(_))));
    assert!(matches!(
        replanning_state.planning_input.refinement,
        Some(lionclaw::model::PlanningRefinement::FailureEvidence(ref feedback))
            if feedback.iter().any(|item| matches!(
                item.evidence,
                lionclaw::model::DecisionEvidence::RoleAttempts { .. }
            ))
    ));
    let replanning_events = failed.engine.store().load(&failed_id).await.unwrap();

    let seeds = [
        (HISTORICAL_WEDGES[0], correction_events.as_slice()),
        (HISTORICAL_WEDGES[1], correction_events.as_slice()),
        (HISTORICAL_WEDGES[2], failure_events.as_slice()),
        (HISTORICAL_WEDGES[3], replanning_events.as_slice()),
        (HISTORICAL_WEDGES[4], replanning_events.as_slice()),
    ];
    for (name, events) in seeds {
        assert_prefix_liveness(name, events);
    }
}

#[tokio::test]
async fn automatic_recovery_is_bounded_and_abort_preserves_retained_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(
        dir.path(),
        MockRoleRunner::new(Box::new(|_| {
            Err(TypedFailure::transient(
                "runtime.fixture",
                "repeatable provider failure".to_string(),
                None,
            ))
        })),
        MockOracleRunner::exiting(0),
    )
    .await;
    let id = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "bounded recovery", BASE_SHA)
        .await
        .unwrap();
    h.engine
        .propose_plan(&id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&h.engine, &id).await;
    let parked = h.engine.advance(&id).await.unwrap();
    assert!(common::has_decision(&parked.state, "node_failed:fix"));
    let attempts = parked.state.tasks.values().next().unwrap().attempts;
    assert_eq!(attempts, parked.state.config.recovery.max_attempts);
    let head = parked.state.head;
    h.engine.advance(&id).await.unwrap();
    assert_eq!(h.engine.load_state(&id).await.unwrap().head, head);

    let conversation_id = parked.state.conversations.keys().next().unwrap();
    let retained = h
        .engine
        .store()
        .lionclaw_dir()
        .join("missions")
        .join(id.as_str())
        .join("conversations")
        .join(conversation_id.as_str())
        .join("work")
        .join("RETained-evidence.txt");
    std::fs::create_dir_all(retained.parent().unwrap()).unwrap();
    std::fs::write(&retained, b"preserve this exact evidence").unwrap();
    h.engine
        .abort(&id, "bounded failure is not worth repeating")
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(&retained).unwrap(),
        b"preserve this exact evidence"
    );
    assert_prefix_liveness(
        "bounded-recovery-abort",
        &h.engine.store().load(&id).await.unwrap(),
    );
}
