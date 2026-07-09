//! Slice-1 core semantics: a trivially-passing mission reaches a verified
//! finish; an oracle failure can never be reported as verified.

mod common;

use common::{default_config, harness, simple_plan, BASE_SHA, HEAD_SHA};
use lionclaw::engine::AdvanceOutcome;
use lionclaw::model::{FinishClass, MissionPhase, TaskStatus};
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};

#[tokio::test]
async fn passing_oracle_yields_verified_finish() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(
            dir.path().to_str().expect("utf8"),
            "make the tests pass",
            BASE_SHA,
            default_config(),
        )
        .await
        .expect("create");
    h.engine
        .submit_plan(&mission_id, simple_plan())
        .await
        .expect("submit");
    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    let AdvanceOutcome::Terminal { phase } = outcome else {
        panic!("expected terminal, got {outcome:?}");
    };
    assert_eq!(
        phase,
        MissionPhase::Done {
            finish: FinishClass::Verified
        }
    );

    let state = h.engine.load_state(&mission_id).await.expect("state");
    assert_eq!(state.current_sha, HEAD_SHA);
    assert!(state
        .tasks
        .values()
        .all(|t| t.status == TaskStatus::Cleared));
    let assertion = state.contract.values().next().expect("assertion");
    let verdict = assertion.last_authoritative.as_ref().expect("verdict");
    assert!(verdict.passed());
    assert_eq!(verdict.judged_sha(), HEAD_SHA);
    assert_eq!(verdict.exit_code(), 0);
    // The oracle judged the artifact commit, exactly once.
    assert_eq!(
        h.oracle_runner.calls.lock().expect("lock").as_slice(),
        &[("cargo-test".to_string(), HEAD_SHA.to_string())]
    );
}

#[tokio::test]
async fn failing_oracle_never_reports_verified() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(1),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(
            dir.path().to_str().expect("utf8"),
            "make the tests pass",
            BASE_SHA,
            default_config(),
        )
        .await
        .expect("create");
    h.engine
        .submit_plan(&mission_id, simple_plan())
        .await
        .expect("submit");
    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    let AdvanceOutcome::Terminal { phase } = outcome else {
        panic!("expected terminal, got {outcome:?}");
    };
    assert_eq!(
        phase,
        MissionPhase::Done {
            finish: FinishClass::Unverified
        }
    );
    let state = h.engine.load_state(&mission_id).await.expect("state");
    let verdict = state
        .contract
        .values()
        .next()
        .expect("assertion")
        .last_authoritative
        .as_ref()
        .expect("verdict");
    assert!(!verdict.passed());
    assert_eq!(verdict.exit_code(), 1);
}

#[tokio::test]
async fn worker_reporting_not_done_parks_with_attention() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        MockRoleRunner::new(Box::new(|_| {
            Ok(lionclaw::ports::RoleRunOutcome {
                handoff: lionclaw::model::Handoff::Work {
                    done: false,
                    report: lionclaw::model::PayloadRef::inline("stuck"),
                    request_attention: false,
                },
                artifact: None,
                model_id: None,
            })
        })),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(
            dir.path().to_str().expect("utf8"),
            "make the tests pass",
            BASE_SHA,
            default_config(),
        )
        .await
        .expect("create");
    h.engine
        .submit_plan(&mission_id, simple_plan())
        .await
        .expect("submit");
    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    let AdvanceOutcome::Parked { attention } = outcome else {
        panic!("expected parked, got {outcome:?}");
    };
    assert_eq!(attention.len(), 1);
    // Parked means parked: no oracle ever ran.
    assert!(h.oracle_runner.calls.lock().expect("lock").is_empty());
}
