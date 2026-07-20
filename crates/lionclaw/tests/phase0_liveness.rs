//! Phase 0 control-plane liveness laws over generated valid event prefixes.

mod common;

use common::{approve_plan, harness, proposal, simple_plan, BASE_SHA, HEAD_SHA};
use lionclaw::engine::{MissionDisposition, MissionView};
use lionclaw::model::{
    apply, fold, step, AssertionSupersession, DecisionAction, EventEnvelope, MissionEvent,
    MissionPhase, PlanProposal, StepDecision, VersionStamps, SCHEMA_VERSION,
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
        .any(|conversation_id| state.conversation_is_messageable(conversation_id))
}

fn assert_advertised_actions_are_legal(view: &MissionView) {
    for action in view.next_actions() {
        match action {
            "mission advance" => assert!(!view.state.phase.is_terminal()),
            "mission status" => assert_eq!(view.disposition, MissionDisposition::Running),
            "mission send" => assert!(has_current_recipient(&view.state)),
            "mission plan propose" => {
                assert_eq!(view.disposition, MissionDisposition::AwaitingPlan)
            }
            "mission continue" => assert!(view
                .state
                .parked_effects
                .keys()
                .any(|effect_id| view.state.parked_effect_is_continuable(effect_id))),
            "mission decide" => assert!(!view.state.open_attention.is_empty()),
            "mission abort" => assert!(!view.state.phase.is_terminal()),
            "mission log" => assert_eq!(view.disposition, MissionDisposition::CleanupBlocked),
            "mission report" => assert_eq!(view.disposition, MissionDisposition::Terminal),
            "mission apply" => {
                assert!(matches!(view.state.phase, MissionPhase::Done { .. }));
                assert_ne!(view.state.deliverable_head(), view.state.base_sha);
            }
            unexpected => panic!("unclassified advertised action: {unexpected}"),
        }
    }
}

fn assert_decisions_change_authority(state: &lionclaw::model::MissionState) -> bool {
    let mut found = false;
    for item in state.open_attention.values() {
        for action in lionclaw::model::decision::allowed_actions(item.kind) {
            found = true;
            let requirement_changes = if action == &DecisionAction::Approve {
                state
                    .proposal
                    .as_ref()
                    .map(|proposal| proposal.requirement_changes.clone())
                    .unwrap_or_default()
            } else {
                vec![]
            };
            let mut changed = state.clone();
            apply(
                &mut changed,
                &envelope(
                    state,
                    MissionEvent::DecisionRecorded {
                        attention_id: item.id.clone(),
                        action: action.clone(),
                        justification: "liveness probe".to_string(),
                        requirement_changes,
                    },
                ),
            );
            changed.head = state.head;
            assert_ne!(
                &changed, state,
                "advertised {action:?} did not change '{}'",
                item.id
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
    assert!(matches!(aborted.phase, MissionPhase::Aborted { .. }));
    assert_eq!(aborted.plan, state.plan);
    assert_eq!(aborted.proposal, state.proposal);
    assert_eq!(aborted.revision, state.revision);
    assert_eq!(aborted.current_sha, state.current_sha);
    assert_eq!(aborted.contract, state.contract);
    assert_eq!(aborted.superseded_assertions, state.superseded_assertions);
    assert_eq!(aborted.tasks, state.tasks);
    assert_eq!(aborted.planning, state.planning);
    assert_eq!(aborted.conversations, state.conversations);
    assert_eq!(aborted.authoritative_receipts, state.authoritative_receipts);
    assert_eq!(aborted.reachable_commits, state.reachable_commits);
    assert_eq!(aborted.parked_effects, state.parked_effects);
    for driver_running in [false, true] {
        let view = MissionView::from_state(aborted.clone(), driver_running);
        assert_eq!(view.disposition, MissionDisposition::Terminal);
        assert_advertised_actions_are_legal(&view);
        assert!(!view.next_actions().contains(&"mission abort"));
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
        if state.phase.is_terminal() {
            assert!(!view.next_actions().contains(&"mission abort"));
            continue;
        }

        let deterministic_progress = !matches!(
            step(&state),
            StepDecision::Idle | StepDecision::Park | StepDecision::Terminal
        ) || !state.inflight.is_empty()
            || state.cleanup_failure.is_some();
        let repair_changes_state = assert_decisions_change_authority(&state)
            || view.disposition == MissionDisposition::AwaitingPlan;
        let abort_is_legal = view.next_actions().contains(&"mission abort");
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
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
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
            PlanProposal {
                base_revision: 1,
                requirement_changes: vec![],
                assertion_supersessions: vec![AssertionSupersession {
                    assertion_id: assertion_id.clone(),
                    replacement_ids: vec![assertion_id],
                }],
                plan: corrected,
            },
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
    let item = failed_state
        .open_attention
        .values()
        .find(|item| item.kind == lionclaw::model::AttentionKind::NodeFailed)
        .unwrap();
    assert!(matches!(
        item.failure,
        Some(TypedFailure::PermanentRuntime { .. })
    ));
    failed
        .engine
        .decide(
            &failed_id,
            &item.id,
            DecisionAction::Revise,
            "replace the failed assignment",
        )
        .await
        .unwrap();
    let replanning_state = failed.engine.load_state(&failed_id).await.unwrap();
    assert_eq!(
        replanning_state.tasks.values().next().unwrap().status,
        lionclaw::model::TaskStatus::Superseded
    );
    assert!(matches!(
        replanning_state.planning_input.refinement,
        Some(lionclaw::model::PlanningRefinement::FailureEvidence(ref feedback))
            if matches!(feedback.failure, Some(TypedFailure::PermanentRuntime { .. }))
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
    assert_eq!(parked.disposition, MissionDisposition::Parked);
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
