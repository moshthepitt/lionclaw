//! Crash recovery is derived from the event log: an inherited request is
//! cleaned and marked interrupted, never replayed.

mod common;

use common::{
    approve_plan, default_config, effect_id, harness, proposal, simple_plan, BASE_SHA, HEAD_SHA,
};
use lionclaw::engine::MissionDisposition;
use lionclaw::model::{
    ArtifactOutcome, Handoff, MissionEvent, MissionPhase, OracleName, PayloadRef, RunErrorKind,
    TaskId,
};
use lionclaw::store::{AppendError, NewEvent};
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};

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
        .create_mission(
            dir.path().to_str().expect("utf8"),
            "obj",
            BASE_SHA,
            default_config(),
        )
        .await
        .expect("create");
    h.engine
        .propose_plan(
            &mission_id,
            proposal(0, simple_plan()),
            "test",
            "initial plan",
        )
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    let first = h.engine.advance(&mission_id).await.expect("advance");
    assert_eq!(first.disposition, MissionDisposition::Terminal);

    let head_before = first.state.head;
    let role_calls_before = h.role_runner.calls.lock().expect("lock").len();
    let oracle_calls_before = h.oracle_runner.calls.lock().expect("lock").len();

    let second = h.engine.advance(&mission_id).await.expect("re-advance");
    assert_eq!(second.disposition, MissionDisposition::Terminal);
    assert_eq!(second.state.head, head_before);
    assert_eq!(
        h.role_runner.calls.lock().expect("lock").len(),
        role_calls_before
    );
    assert_eq!(
        h.oracle_runner.calls.lock().expect("lock").len(),
        oracle_calls_before
    );
    assert!(h.role_runner.max_invocations_per_key() <= 1);
}

#[tokio::test]
async fn inherited_role_request_is_interrupted_without_rerunning_the_llm() {
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
            "obj",
            BASE_SHA,
            default_config(),
        )
        .await
        .expect("create");
    h.engine
        .propose_plan(
            &mission_id,
            proposal(0, simple_plan()),
            "test",
            "initial plan",
        )
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    let id = effect_id("crashed-role");
    let state = h.engine.load_state(&mission_id).await.expect("state");
    h.engine
        .store()
        .append(
            &mission_id,
            state.head,
            &[NewEvent::new(MissionEvent::RoleRunRequested {
                task_id: lionclaw::model::TaskId::new("fix").expect("task id"),
                attempt_no: 1,
                effect_id: id.clone(),
                role: lionclaw::model::RoleName::new("implementer").expect("role"),
                runtime: "codex".to_string(),
                prompt: lionclaw::model::PayloadRef::inline("prompt"),
                base_sha: BASE_SHA.to_string(),
            })],
            1,
        )
        .await
        .expect("append request");

    let view = h.engine.advance(&mission_id).await.expect("advance");
    assert_eq!(view.disposition, MissionDisposition::Parked);
    assert!(matches!(view.state.phase, MissionPhase::AttentionNeeded));
    assert!(view.state.inflight.is_empty());
    assert_eq!(
        h.role_runner
            .invocations_by_key
            .lock()
            .expect("lock")
            .get(id.as_str()),
        None
    );
    let events = h.engine.store().load(&mission_id).await.expect("load");
    let failures: Vec<_> = events
        .iter()
        .filter(|event| {
            matches!(&event.event,
                MissionEvent::RoleRunFailed { effect_id, failure, .. }
                    if effect_id == &id && failure.kind == RunErrorKind::Interrupted)
        })
        .collect();
    assert_eq!(failures.len(), 1);
}

#[tokio::test]
async fn inherited_oracle_request_is_interrupted_without_rerunning_the_oracle() {
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
            "obj",
            BASE_SHA,
            default_config(),
        )
        .await
        .expect("create");
    h.engine
        .propose_plan(
            &mission_id,
            proposal(0, simple_plan()),
            "test",
            "initial plan",
        )
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;

    let task_id = TaskId::new("fix").expect("task id");
    let role_effect = effect_id("completed-role");
    let oracle_effect = effect_id("crashed-oracle");
    let oracle = OracleName::new("cargo-test").expect("oracle");
    let state = h.engine.load_state(&mission_id).await.expect("state");
    h.engine
        .store()
        .append(
            &mission_id,
            state.head,
            &[
                NewEvent::new(MissionEvent::RoleRunRequested {
                    task_id: task_id.clone(),
                    attempt_no: 1,
                    effect_id: role_effect.clone(),
                    role: lionclaw::model::RoleName::new("implementer").expect("role"),
                    runtime: "codex".to_string(),
                    prompt: PayloadRef::inline("prompt"),
                    base_sha: BASE_SHA.to_string(),
                }),
                NewEvent::new(MissionEvent::RoleRunCompleted {
                    task_id,
                    attempt_no: 1,
                    effect_id: role_effect,
                    handoff: Handoff::Work {
                        done: true,
                        report: PayloadRef::inline("done"),
                        request_attention: false,
                    },
                    artifact: Some(ArtifactOutcome {
                        base_sha: BASE_SHA.to_string(),
                        head_sha: HEAD_SHA.to_string(),
                    }),
                }),
                NewEvent::new(MissionEvent::OracleRunRequested {
                    assertion_ids: vec![lionclaw::model::AssertionId::new("TESTS-PASS").unwrap()],
                    oracle: oracle.clone(),
                    judged_sha: HEAD_SHA.to_string(),
                    attempt_no: 1,
                    effect_id: oracle_effect.clone(),
                }),
            ],
            1,
        )
        .await
        .expect("append requests");

    let view = h.engine.advance(&mission_id).await.expect("advance");
    assert_eq!(view.disposition, MissionDisposition::Parked);
    assert!(view.state.inflight.is_empty());
    assert!(h.oracle_runner.calls.lock().expect("lock").is_empty());
    assert_eq!(
        view.state.oracle_failures.get(&oracle).unwrap().kind,
        RunErrorKind::Interrupted
    );
    let events = h.engine.store().load(&mission_id).await.expect("load");
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                &event.event,
                MissionEvent::OracleRunFailed { effect_id, failure, .. }
                    if effect_id == &oracle_effect && failure.kind == RunErrorKind::Interrupted
            ))
            .count(),
        1
    );
}

#[tokio::test]
async fn snapshot_rebuild_preserves_an_unfinished_request_for_recovery() {
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
            dir.path().to_str().unwrap(),
            "obj",
            BASE_SHA,
            default_config(),
        )
        .await
        .expect("create");
    let id = effect_id("rebuild-crash");
    let state = h.engine.load_state(&mission_id).await.expect("state");
    h.engine
        .store()
        .append(
            &mission_id,
            state.head,
            &[NewEvent::new(MissionEvent::RoleRunRequested {
                task_id: lionclaw::model::TaskId::new("fix").unwrap(),
                attempt_no: 1,
                effect_id: id.clone(),
                role: lionclaw::model::RoleName::new("implementer").unwrap(),
                runtime: "codex".to_string(),
                prompt: lionclaw::model::PayloadRef::inline("p"),
                base_sha: BASE_SHA.to_string(),
            })],
            1,
        )
        .await
        .expect("append");
    let rebuilt = h
        .engine
        .store()
        .rebuild_cursors(&mission_id, 2)
        .await
        .expect("rebuild");
    assert!(rebuilt.inflight.contains_key(&id));

    h.engine.advance(&mission_id).await.expect("advance");
    assert_eq!(
        h.role_runner
            .invocations_by_key
            .lock()
            .unwrap()
            .get(id.as_str()),
        None
    );
}

#[tokio::test]
async fn one_outcome_per_effect_id_is_a_store_invariant() {
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
            "obj",
            BASE_SHA,
            default_config(),
        )
        .await
        .expect("create");
    h.engine
        .propose_plan(
            &mission_id,
            proposal(0, simple_plan()),
            "test",
            "initial plan",
        )
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    h.engine.advance(&mission_id).await.expect("advance");

    let events = h.engine.store().load(&mission_id).await.expect("load");
    let (key, template) = events
        .iter()
        .find_map(|event| match &event.event {
            MissionEvent::OracleRunCompleted { effect_id, .. } => {
                Some((effect_id.clone(), event.event.clone()))
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
        matches!(result, Err(AppendError::Duplicate { effect_id: duplicate }) if duplicate == key.as_str()),
        "duplicate outcome must be rejected"
    );
}
