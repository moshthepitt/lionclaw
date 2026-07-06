//! Resume semantics: re-running a finished mission touches nothing; a
//! crashed role run is reconciled as a synthesized failure, never re-run;
//! one idempotency key gets at most one outcome, ever.

mod common;

use common::{default_config, harness, simple_plan, BASE_SHA, HEAD_SHA};
use lionclaw_mission_engine::engine::AdvanceOutcome;
use lionclaw_mission_engine::model::{MissionEvent, MissionPhase};
use lionclaw_mission_engine::store::{AppendError, NewEvent};
use lionclaw_mission_engine::testing::{MockOracleRunner, MockRoleRunner};

#[tokio::test]
async fn rerun_after_finish_appends_nothing_and_invokes_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(dir.path().to_str().expect("utf8"), "obj", BASE_SHA, default_config())
        .await
        .expect("create");
    h.engine
        .submit_plan(&mission_id, simple_plan())
        .await
        .expect("submit");
    let first = h.engine.advance(&mission_id).await.expect("advance");
    assert!(matches!(first, AdvanceOutcome::Terminal { .. }));

    let head_before = h.engine.load_state(&mission_id).await.expect("state").head;
    let role_calls_before = h.role_runner.calls.lock().expect("lock").len();
    let oracle_calls_before = h.oracle_runner.calls.lock().expect("lock").len();

    // Resume from the log: terminal, zero new events, zero port invocations.
    let second = h.engine.advance(&mission_id).await.expect("re-advance");
    assert!(matches!(second, AdvanceOutcome::Terminal { .. }));
    let state = h.engine.load_state(&mission_id).await.expect("state");
    assert_eq!(state.head, head_before);
    assert_eq!(h.role_runner.calls.lock().expect("lock").len(), role_calls_before);
    assert_eq!(h.oracle_runner.calls.lock().expect("lock").len(), oracle_calls_before);
    // Per-key ceiling held throughout.
    assert!(h.role_runner.max_invocations_per_key() <= 1);
}

#[tokio::test]
async fn crashed_role_run_synthesizes_failure_without_rerunning_the_llm() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(dir.path().to_str().expect("utf8"), "obj", BASE_SHA, default_config())
        .await
        .expect("create");
    h.engine
        .submit_plan(&mission_id, simple_plan())
        .await
        .expect("submit");

    // Simulate a crash: record the request, lease it, then "die" before any
    // outcome lands.
    let state = h.engine.load_state(&mission_id).await.expect("state");
    let event = NewEvent::new(MissionEvent::RoleRunRequested {
        task_id: lionclaw_mission_engine::model::TaskId::new("fix").expect("task id"),
        attempt_no: 1,
        idempotency_key: "crashed-key".to_string(),
        role: lionclaw_mission_engine::model::RoleName::new("implementer").expect("role"),
        prompt: lionclaw_mission_engine::model::PayloadRef::inline("prompt"),
        base_sha: BASE_SHA.to_string(),
    });
    h.engine
        .store()
        .append(&mission_id, state.head, &[event], 1)
        .await
        .expect("append request");
    let leases = h
        .engine
        .store()
        .pull_due(&mission_id, "dead-worker", 1, 60_000, 2)
        .await
        .expect("lease");
    assert_eq!(leases.len(), 1);

    // Resume: the run's outcome is unknowable → synthesized failure, parked
    // attention, and the role runner is never invoked for the crashed key.
    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    assert!(matches!(outcome, AdvanceOutcome::Parked { .. }), "got {outcome:?}");
    let state = h.engine.load_state(&mission_id).await.expect("state");
    assert!(matches!(state.phase, MissionPhase::AttentionNeeded));
    assert!(state.inflight.is_empty());
    assert_eq!(
        h.role_runner
            .invocations_by_key
            .lock()
            .expect("lock")
            .get("crashed-key"),
        None
    );
    let events = h.engine.store().load(&mission_id).await.expect("load");
    let synthesized: Vec<_> = events
        .iter()
        .filter_map(|e| match &e.event {
            MissionEvent::RoleRunFailed {
                idempotency_key,
                synthesized: true,
                ..
            } if idempotency_key == "crashed-key" => Some(e.sequence_no),
            _ => None,
        })
        .collect();
    assert_eq!(synthesized.len(), 1);
}

#[tokio::test]
async fn a_live_lease_is_not_reconciled_to_failure() {
    // Regression (review): a second driver must not synthesize failure over a
    // role run whose lease is still live (the LLM may be running elsewhere).
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "obj", BASE_SHA, default_config())
        .await
        .expect("create");
    h.engine.submit_plan(&mission_id, simple_plan()).await.expect("submit");

    // Record a role-run request and lease it with a long, still-live lease.
    let state = h.engine.load_state(&mission_id).await.expect("state");
    let event = NewEvent::new(MissionEvent::RoleRunRequested {
        task_id: lionclaw_mission_engine::model::TaskId::new("fix").unwrap(),
        attempt_no: 1,
        idempotency_key: "live-key".to_string(),
        role: lionclaw_mission_engine::model::RoleName::new("implementer").unwrap(),
        prompt: lionclaw_mission_engine::model::PayloadRef::inline("p"),
        base_sha: BASE_SHA.to_string(),
    });
    h.engine.store().append(&mission_id, state.head, &[event], 1_000).await.expect("append");
    // Another worker holds a 1-hour lease as of t=1000.
    let leases = h
        .engine
        .store()
        .pull_due(&mission_id, "other-worker", 1, 3_600_000, 1_000)
        .await
        .expect("lease");
    assert_eq!(leases.len(), 1);

    // This driver advances: the live lease must be left alone — no synthesized
    // failure, and the role runner is never invoked for the live key.
    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    assert!(matches!(outcome, AdvanceOutcome::Busy), "got {outcome:?}");
    let events = h.engine.store().load(&mission_id).await.expect("load");
    assert!(
        !events.iter().any(|e| matches!(&e.event,
            MissionEvent::RoleRunFailed { idempotency_key, .. } if idempotency_key == "live-key")),
        "a live lease must not be reconciled to failure"
    );
    assert_eq!(
        h.role_runner.invocations_by_key.lock().unwrap().get("live-key"),
        None
    );
}

#[tokio::test]
async fn rebuild_cursors_does_not_relaunch_a_crashed_role_run() {
    // Regression (review): rebuild_cursors must not launder a crashed (leased)
    // role run back to 'queued', which would re-invoke the LLM. It reseeds as
    // an expired lease so the next advance synthesizes failure instead.
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "obj", BASE_SHA, default_config())
        .await
        .expect("create");
    h.engine.submit_plan(&mission_id, simple_plan()).await.expect("submit");

    // Record + lease a role run, then "crash" (no outcome recorded).
    let state = h.engine.load_state(&mission_id).await.expect("state");
    let event = NewEvent::new(MissionEvent::RoleRunRequested {
        task_id: lionclaw_mission_engine::model::TaskId::new("fix").unwrap(),
        attempt_no: 1,
        idempotency_key: "crash-key".to_string(),
        role: lionclaw_mission_engine::model::RoleName::new("implementer").unwrap(),
        prompt: lionclaw_mission_engine::model::PayloadRef::inline("p"),
        base_sha: BASE_SHA.to_string(),
    });
    h.engine.store().append(&mission_id, state.head, &[event], 1_000).await.expect("append");
    h.engine.store().pull_due(&mission_id, "dead", 1, 60_000, 1_000).await.expect("lease");

    // Rebuild every derived cursor from the log.
    h.engine.store().rebuild_cursors(&mission_id, 5_000).await.expect("rebuild");

    // The next advance must synthesize failure, NOT re-run the LLM.
    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    assert!(matches!(outcome, AdvanceOutcome::Parked { .. }), "got {outcome:?}");
    assert_eq!(
        h.role_runner.invocations_by_key.lock().unwrap().get("crash-key"),
        None,
        "the crashed role run must never be re-invoked after a rebuild"
    );
    let events = h.engine.store().load(&mission_id).await.expect("load");
    assert!(events.iter().any(|e| matches!(&e.event,
        MissionEvent::RoleRunFailed { idempotency_key, synthesized: true, .. }
            if idempotency_key == "crash-key")));
}

#[tokio::test]
async fn one_outcome_per_idempotency_key_is_a_store_invariant() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(dir.path().to_str().expect("utf8"), "obj", BASE_SHA, default_config())
        .await
        .expect("create");
    h.engine
        .submit_plan(&mission_id, simple_plan())
        .await
        .expect("submit");
    h.engine.advance(&mission_id).await.expect("advance");

    // Try to record a second outcome for the oracle's key: rejected.
    let events = h.engine.store().load(&mission_id).await.expect("load");
    let (key, template) = events
        .iter()
        .find_map(|e| match &e.event {
            MissionEvent::OracleRunCompleted { idempotency_key, .. } => {
                Some((idempotency_key.clone(), e.event.clone()))
            }
            _ => None,
        })
        .expect("oracle outcome exists");
    let head = h.engine.load_state(&mission_id).await.expect("state").head;
    let result = h
        .engine
        .store()
        .append(&mission_id, head, &[NewEvent::new(template)], 99)
        .await;
    assert!(
        matches!(result, Err(AppendError::Duplicate { key: k }) if k == key),
        "duplicate outcome must be rejected"
    );
}
