//! Slice 4: the default-on ratification gate parks the mission before any
//! work; a ratify decision lets it proceed; an unrelated/invalid decision is
//! refused; abort terminates.

mod common;

use std::sync::Arc;

use common::{simple_plan, test_plugin, BASE_SHA, HEAD_SHA};
use lionclaw::engine::{AdvanceOutcome, Engine};
use lionclaw::model::{DecisionAction, FinishClass, MissionConfig, MissionPhase};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockClock, MockOracleRunner, MockRoleRunner};

async fn gated_engine(dir: &std::path::Path) -> Engine {
    let store = MissionStore::open(dir).await.expect("store");
    Engine::new(
        store,
        test_plugin(),
        Arc::new(MockRoleRunner::happy(HEAD_SHA)),
        Arc::new(MockOracleRunner::exiting(0)),
        Arc::new(MockClock::default()),
    )
}

#[tokio::test]
async fn ratification_gate_parks_then_ratify_proceeds_to_verified() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = gated_engine(dir.path()).await;
    let mission_id = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "gated mission",
            BASE_SHA,
            MissionConfig::default(), // ratification_gate: true
        )
        .await
        .expect("create");
    engine
        .submit_plan(&mission_id, simple_plan())
        .await
        .expect("submit");

    // Advance parks at the ratification gate — no work has run.
    let parked = engine.advance(&mission_id).await.expect("advance");
    let AdvanceOutcome::Parked { attention } = parked else {
        panic!("expected parked at ratify, got {parked:?}");
    };
    assert_eq!(attention.len(), 1);
    assert_eq!(attention[0].id, "ratify:mission");

    // An invalid decision (retry on the ratify item) is refused.
    assert!(engine
        .decide(
            &mission_id,
            "ratify:mission",
            DecisionAction::Retry,
            "",
            "test"
        )
        .await
        .is_err());
    // A decision on a nonexistent item is refused.
    assert!(engine
        .decide(
            &mission_id,
            "node_failed:ghost",
            DecisionAction::Continue,
            "",
            "test"
        )
        .await
        .is_err());

    // Ratify, then advance runs the mission to a verified finish.
    engine
        .decide(
            &mission_id,
            "ratify:mission",
            DecisionAction::Ratify,
            "ok",
            "test",
        )
        .await
        .expect("ratify");
    let done = engine.advance(&mission_id).await.expect("advance 2");
    assert!(matches!(
        done,
        AdvanceOutcome::Terminal {
            phase: MissionPhase::Done {
                finish: FinishClass::Verified
            }
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
        .submit_plan(&mission_id, simple_plan())
        .await
        .expect("submit");
    engine.advance(&mission_id).await.expect("advance");

    engine
        .decide(
            &mission_id,
            "ratify:mission",
            DecisionAction::Abort,
            "stop",
            "test",
        )
        .await
        .expect("abort");
    let state = engine.load_state(&mission_id).await.expect("state");
    assert!(matches!(state.phase, MissionPhase::Aborted { .. }));
}
