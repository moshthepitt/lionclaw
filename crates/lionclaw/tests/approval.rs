//! Slice 4: plan approval parks the mission before any work; an approve
//! decision lets it proceed; an unrelated/invalid decision is
//! refused; abort terminates.

mod common;

use std::sync::Arc;

use common::{proposal, simple_plan, test_mission_type, BASE_SHA, HEAD_SHA};
use lionclaw::engine::{Engine, EngineServices, MissionDisposition};
use lionclaw::model::{DecisionAction, FinishClass, MissionConfig, MissionEvent, MissionPhase};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockClock, MockOracleRunner, MockRoleRunner, NoopEffectCleaner};

async fn gated_engine(dir: &std::path::Path) -> Engine {
    let store = MissionStore::open(dir).await.expect("store");
    Engine::new(
        store,
        test_mission_type(),
        "codex".to_string(),
        "test-image".to_string(),
        EngineServices::new(
            Arc::new(MockRoleRunner::happy(HEAD_SHA)),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    )
}

#[tokio::test]
async fn every_plan_parks_until_approved_then_proceeds_to_verified() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = gated_engine(dir.path()).await;
    let mission_id = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "gated mission",
            BASE_SHA,
            MissionConfig::default(),
        )
        .await
        .expect("create");
    engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");

    // Advance parks at the approval gate — no work has run.
    let parked = engine.advance(&mission_id).await.expect("advance");
    assert_eq!(parked.disposition, MissionDisposition::Parked);
    let attention: Vec<_> = parked.state.open_attention.values().collect();
    assert_eq!(attention.len(), 1);
    assert_eq!(attention[0].id, "plan_proposal:mission");

    // The model contract rejects an empty reason even when the action itself
    // is legal; callers cannot bypass the CLI's required flag.
    assert!(engine
        .decide(
            &mission_id,
            "plan_proposal:mission",
            DecisionAction::Approve,
            "",
        )
        .await
        .is_err());

    // An invalid decision (retry on the approve item) is refused.
    assert!(engine
        .decide(
            &mission_id,
            "plan_proposal:mission",
            DecisionAction::Retry,
            "",
        )
        .await
        .is_err());
    // A decision on a nonexistent item is refused.
    assert!(engine
        .decide(&mission_id, "node_failed:ghost", DecisionAction::Accept, "",)
        .await
        .is_err());

    // Approve, then advance runs the mission to a verified finish.
    engine
        .decide(
            &mission_id,
            "plan_proposal:mission",
            DecisionAction::Approve,
            "ok",
        )
        .await
        .expect("approve");
    let done = engine.advance(&mission_id).await.expect("advance 2");
    assert_eq!(done.disposition, MissionDisposition::Terminal);
    assert!(matches!(
        done.state.phase,
        MissionPhase::Done {
            finish: FinishClass::Verified
        }
    ));
}

#[tokio::test]
async fn abort_decision_terminates_the_mission() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = gated_engine(dir.path()).await;
    let mission_id = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "gated",
            BASE_SHA,
            MissionConfig::default(),
        )
        .await
        .expect("create");
    engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    engine.advance(&mission_id).await.expect("advance");

    engine
        .decide(
            &mission_id,
            "plan_proposal:mission",
            DecisionAction::Abort,
            "stop",
        )
        .await
        .expect("abort");
    let state = engine.load_state(&mission_id).await.expect("state");
    assert!(matches!(
        state.phase,
        MissionPhase::Aborted { ref reason } if reason == "stop"
    ));
    let events = engine.store().load(&mission_id).await.expect("events");
    assert!(matches!(
        &events[events.len() - 2].event,
        MissionEvent::DecisionRecorded {
            attention_id,
            action,
            justification,
        } if attention_id == "plan_proposal:mission"
            && action == &DecisionAction::Abort
            && justification == "stop"
    ));
    assert!(matches!(
        &events[events.len() - 1].event,
        MissionEvent::MissionAborted { reason } if reason == "stop"
    ));
}
