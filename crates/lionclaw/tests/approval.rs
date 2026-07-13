//! Slice 4: the default-on approval gate parks the mission before any
//! work; a approve decision lets it proceed; an unrelated/invalid decision is
//! refused; abort terminates.

mod common;

use std::sync::Arc;

use common::{proposal, simple_plan, test_mission_type, BASE_SHA, HEAD_SHA};
use lionclaw::engine::{AdvanceOutcome, Engine};
use lionclaw::model::{DecisionAction, FinishClass, MissionConfig, MissionPhase};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockClock, MockOracleRunner, MockRoleRunner};

async fn gated_engine(dir: &std::path::Path) -> Engine {
    let store = MissionStore::open(dir).await.expect("store");
    Engine::new(
        store,
        test_mission_type(),
        "codex".to_string(),
        "test-image".to_string(),
        Arc::new(MockRoleRunner::happy(HEAD_SHA)),
        Arc::new(MockOracleRunner::exiting(0)),
        Arc::new(MockClock::default()),
    )
}

#[tokio::test]
async fn approval_required_parks_then_approve_proceeds_to_verified() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = gated_engine(dir.path()).await;
    let mission_id = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "gated mission",
            BASE_SHA,
            MissionConfig::default(), // approval_required: true
        )
        .await
        .expect("create");
    engine
        .propose_plan(
            &mission_id,
            proposal(0, simple_plan()),
            "test",
            "initial plan",
        )
        .await
        .expect("propose");

    // Advance parks at the approval gate — no work has run.
    let parked = engine.advance(&mission_id).await.expect("advance");
    let AdvanceOutcome::Parked { attention } = parked else {
        panic!("expected parked at approve, got {parked:?}");
    };
    assert_eq!(attention.len(), 1);
    assert_eq!(attention[0].id, "plan_proposal:mission");

    // An invalid decision (retry on the approve item) is refused.
    assert!(engine
        .decide(
            &mission_id,
            "plan_proposal:mission",
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
            DecisionAction::Accept,
            "",
            "test"
        )
        .await
        .is_err());

    // Approve, then advance runs the mission to a verified finish.
    engine
        .decide(
            &mission_id,
            "plan_proposal:mission",
            DecisionAction::Approve,
            "ok",
            "test",
        )
        .await
        .expect("approve");
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
        .propose_plan(
            &mission_id,
            proposal(0, simple_plan()),
            "test",
            "initial plan",
        )
        .await
        .expect("propose");
    engine.advance(&mission_id).await.expect("advance");

    engine
        .decide(
            &mission_id,
            "plan_proposal:mission",
            DecisionAction::Abort,
            "stop",
            "test",
        )
        .await
        .expect("abort");
    let state = engine.load_state(&mission_id).await.expect("state");
    assert!(matches!(state.phase, MissionPhase::Aborted { .. }));
}
