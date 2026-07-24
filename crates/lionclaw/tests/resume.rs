//! Crash recovery is derived from schema-24 requests: inherited effects are
//! interrupted once and are never re-invoked.

mod common;

use common::{
    approve_plan, fault_append_events, proposal, review_runner, simple_plan, BASE_SHA, HEAD_SHA,
};
use lionclaw::engine::MissionDisposition;
use lionclaw::model::{
    EffectId, MissionEvent, MissionPhase, OracleName, RoleInstanceId, RolePromptTemplate, TaskId,
    WorkspacePreparation,
};
use lionclaw::store::NewEvent;
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};

const PROMPT_HASH: &str = "cf07194ee232eb531e15f690000d19846dea69cf05504782658afcfacb9228a2";

fn role_request(mission_id: &lionclaw::model::MissionId, head: u64) -> (EffectId, MissionEvent) {
    let role = RoleInstanceId::new("implementer").unwrap();
    let task = TaskId::new("fix").unwrap();
    let effect = EffectId::for_role_turn(mission_id, &role, 1, Some(&task), 1, 1, PROMPT_HASH);
    (
        effect.clone(),
        MissionEvent::RoleTurnRequested {
            role_instance: role,
            team_revision: 1,
            task_id: Some(task),
            assertion_ids: vec![lionclaw::model::AssertionId::new("TESTS-PASS").unwrap()],
            attempt_no: 1,
            effect_id: effect,
            prompt_template: RolePromptTemplate::Execution,
            prompt_hash: PROMPT_HASH.to_string(),
            base_sha: BASE_SHA.to_string(),
            dependency_refs: vec![],
            assignment_epoch: 1,
            message_boundary: head,
            presented_messages: vec![],
            workspace_preparation: WorkspacePreparation::ResetForAssignment,
            requested_at_ms: 0,
            deadline_ms: 100_000,
            budget_deadline_ms: 100_000,
        },
    )
}

#[tokio::test]
async fn rerun_after_finish_appends_nothing_and_invokes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let harness = common::harness(
        dir.path(),
        review_runner(vec![]),
        MockOracleRunner::exiting(0),
    )
    .await;
    let id = harness
        .engine
        .create_mission(dir.path().to_str().unwrap(), "obj", BASE_SHA)
        .await
        .unwrap();
    harness
        .engine
        .propose_plan(&id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&harness.engine, &id).await;
    let first = harness.engine.advance(&id).await.unwrap();
    assert_eq!(first.disposition, MissionDisposition::Terminal);
    let role_calls = harness.role_runner.calls.lock().unwrap().len();
    let oracle_calls = harness.oracle_runner.calls.lock().unwrap().len();
    let second = harness.engine.advance(&id).await.unwrap();
    assert_eq!(second.disposition, MissionDisposition::Terminal);
    assert_eq!(second.state.head, first.state.head);
    assert_eq!(harness.role_runner.calls.lock().unwrap().len(), role_calls);
    assert_eq!(
        harness.oracle_runner.calls.lock().unwrap().len(),
        oracle_calls
    );
}

#[tokio::test]
async fn inherited_role_request_is_interrupted_without_rerunning_the_llm() {
    let dir = tempfile::tempdir().unwrap();
    let harness = common::harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let id = harness
        .engine
        .create_mission(dir.path().to_str().unwrap(), "obj", BASE_SHA)
        .await
        .unwrap();
    harness
        .engine
        .propose_plan(&id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&harness.engine, &id).await;
    let state = harness.engine.load_state(&id).await.unwrap();
    let (effect, request) = role_request(&id, state.head);
    fault_append_events(
        dir.path(),
        &id,
        state.head,
        &[NewEvent::new(request).with_prompt_hash(PROMPT_HASH)],
        1,
    )
    .await;

    let view = harness.engine.advance(&id).await.unwrap();
    assert_eq!(view.disposition, MissionDisposition::Parked);
    assert!(matches!(view.state.phase, MissionPhase::AttentionNeeded));
    assert!(view.state.inflight.is_empty());
    assert!(harness.role_runner.calls.lock().unwrap().is_empty());
    assert!(harness
        .engine
        .store()
        .load(&id)
        .await
        .unwrap()
        .iter()
        .any(|event| matches!(
            &event.event,
            MissionEvent::RoleTurnCompleted {
                effect_id,
                outcome: Err(lionclaw_runtime_api::TypedFailure::Interrupted { .. }),
            } if effect_id == &effect
        )));
}

#[tokio::test]
async fn inherited_oracle_request_is_interrupted_without_rerunning_the_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let mut mission_type = common::test_mission_type();
    mission_type.edit_for_testing(|definition| {
        definition.execution.auto_continue_candidate = false;
        definition.execution.auto_continue_proof = false;
    });
    let harness = common::harness_with_type(
        dir.path(),
        mission_type,
        review_runner(vec![]),
        MockOracleRunner::exiting(0),
    )
    .await;
    let id = harness
        .engine
        .create_mission(dir.path().to_str().unwrap(), "obj", BASE_SHA)
        .await
        .unwrap();
    harness
        .engine
        .propose_plan(&id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&harness.engine, &id).await;
    let ready = harness.engine.advance(&id).await.unwrap().state;
    assert!(ready.inflight.is_empty());
    assert!(ready
        .contract
        .values()
        .all(|assertion| assertion.last_authoritative.is_none()));

    let oracle = OracleName::new("cargo-test").unwrap();
    let effect = EffectId::for_oracle_request(&id, &oracle, HEAD_SHA, 1);
    fault_append_events(
        dir.path(),
        &id,
        ready.head,
        &[NewEvent::new(MissionEvent::OracleRunRequested {
            assertion_ids: vec![lionclaw::model::AssertionId::new("TESTS-PASS").unwrap()],
            oracle: oracle.clone(),
            judged_sha: HEAD_SHA.to_string(),
            attempt_no: 1,
            effect_id: effect.clone(),
            requested_at_ms: 0,
            deadline_ms: 100_000,
        })],
        1,
    )
    .await;
    let before = harness.oracle_runner.calls.lock().unwrap().len();
    let view = harness.engine.advance(&id).await.unwrap();
    assert_eq!(view.disposition, MissionDisposition::Parked);
    assert!(matches!(view.state.phase, MissionPhase::AttentionNeeded));
    assert_eq!(harness.oracle_runner.calls.lock().unwrap().len(), before);
    assert!(view.state.inflight.is_empty());
    assert!(harness
        .engine
        .store()
        .load(&id)
        .await
        .unwrap()
        .iter()
        .any(|event| matches!(
            &event.event,
            MissionEvent::OracleRunCompleted {
                effect_id,
                outcome: Err(lionclaw_runtime_api::TypedFailure::Interrupted { .. }),
                ..
            } if effect_id == &effect
        )));
}

#[tokio::test]
async fn snapshot_rebuild_preserves_an_unfinished_request_for_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let harness = common::harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let id = harness
        .engine
        .create_mission(dir.path().to_str().unwrap(), "obj", BASE_SHA)
        .await
        .unwrap();
    harness
        .engine
        .propose_plan(&id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&harness.engine, &id).await;
    let state = harness.engine.load_state(&id).await.unwrap();
    let (effect, request) = role_request(&id, state.head);
    fault_append_events(
        dir.path(),
        &id,
        state.head,
        &[NewEvent::new(request).with_prompt_hash(PROMPT_HASH)],
        1,
    )
    .await;
    let rebuilt = harness
        .engine
        .store()
        .rebuild_cursors(&id, 2)
        .await
        .unwrap();
    assert!(rebuilt.inflight.contains_key(&effect));
    let view = harness.engine.advance(&id).await.unwrap();
    assert!(view.state.inflight.is_empty());
    assert!(harness.role_runner.calls.lock().unwrap().is_empty());
}
