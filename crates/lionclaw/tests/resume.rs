//! Crash recovery is derived from the event log: an inherited request is
//! cleaned and marked interrupted, never replayed.

mod common;

use common::{
    approve_plan, fault_append_events, harness, proposal, simple_plan, BASE_SHA, HEAD_SHA,
};
use lionclaw::engine::MissionDisposition;
use lionclaw::model::{
    ArtifactOutcome, Handoff, MissionEvent, MissionPhase, OracleName, OutputSemantics, PayloadRef,
    RoleRunSuccess, RuntimeConfigurationEvidence, TaskId,
};
use lionclaw::store::NewEvent;
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};

const CRASHED_PROMPT_HASH: &str =
    "cf07194ee232eb531e15f690000d19846dea69cf05504782658afcfacb9228a2";

fn role_effect(
    mission_id: &lionclaw::model::MissionId,
    task_id: &TaskId,
) -> lionclaw::model::EffectId {
    lionclaw::model::EffectId::for_role_request(
        lionclaw::model::TaskNamespace::Execution,
        mission_id,
        task_id,
        1,
        1,
        CRASHED_PROMPT_HASH,
    )
}

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
        .create_mission(dir.path().to_str().expect("utf8"), "obj", BASE_SHA)
        .await
        .expect("create");
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
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
        .create_mission(dir.path().to_str().expect("utf8"), "obj", BASE_SHA)
        .await
        .expect("create");
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    let task_id = TaskId::new("fix").expect("task id");
    let id = role_effect(&mission_id, &task_id);
    let state = h.engine.load_state(&mission_id).await.expect("state");
    fault_append_events(
        dir.path(),
        &mission_id,
        state.head,
        &[NewEvent::new(MissionEvent::RoleRunRequested {
            conversation_id: lionclaw::model::ConversationId::for_role_instance(
                &mission_id,
                lionclaw::model::TaskNamespace::Execution,
                &task_id,
                &lionclaw::model::RoleName::new("implementer").unwrap(),
                1,
            ),
            namespace: lionclaw::model::TaskNamespace::Execution,
            task_id,
            attempt_no: 1,
            effect_id: id.clone(),
            role: lionclaw::model::RoleName::new("implementer").expect("role"),
            output: OutputSemantics::ProducesArtifact,
            runtime: "codex".to_string(),
            prompt: lionclaw::model::PayloadRef::inline("prompt"),
            base_sha: BASE_SHA.to_string(),
            assignment_epoch: 1,
            message_boundary: state.head,
            presented_messages: vec![],
            recreate_workspace: true,
            requested_at_ms: 0,
            not_before_ms: 0,
            deadline_ms: 100_000,
            budget_deadline_ms: 100_000,
        })
        .with_prompt_hash(CRASHED_PROMPT_HASH)],
        1,
    )
    .await;

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
                MissionEvent::RoleRunCompleted { effect_id, outcome: Err(failure), .. }
                    if effect_id == &id && matches!(failure, lionclaw_runtime_api::TypedFailure::Interrupted { .. }))
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
        .create_mission(dir.path().to_str().expect("utf8"), "obj", BASE_SHA)
        .await
        .expect("create");
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;

    let task_id = TaskId::new("fix").expect("task id");
    let oracle = OracleName::new("cargo-test").expect("oracle");
    let role_effect = role_effect(&mission_id, &task_id);
    let oracle_effect =
        lionclaw::model::EffectId::for_oracle_request(&mission_id, &oracle, HEAD_SHA, 1);
    let state = h.engine.load_state(&mission_id).await.expect("state");
    fault_append_events(
        dir.path(),
        &mission_id,
        state.head,
        &[
            NewEvent::new(MissionEvent::RoleRunRequested {
                conversation_id: lionclaw::model::ConversationId::for_role_instance(
                    &mission_id,
                    lionclaw::model::TaskNamespace::Execution,
                    &task_id,
                    &lionclaw::model::RoleName::new("implementer").unwrap(),
                    1,
                ),
                namespace: lionclaw::model::TaskNamespace::Execution,
                task_id: task_id.clone(),
                attempt_no: 1,
                effect_id: role_effect.clone(),
                role: lionclaw::model::RoleName::new("implementer").expect("role"),
                output: OutputSemantics::ProducesArtifact,
                runtime: "codex".to_string(),
                prompt: PayloadRef::inline("prompt"),
                base_sha: BASE_SHA.to_string(),
                assignment_epoch: 1,
                message_boundary: state.head,
                presented_messages: vec![],
                recreate_workspace: true,
                requested_at_ms: 0,
                not_before_ms: 0,
                deadline_ms: 100_000,
                budget_deadline_ms: 100_000,
            })
            .with_prompt_hash(CRASHED_PROMPT_HASH),
            NewEvent::new(MissionEvent::RoleRunCompleted {
                namespace: lionclaw::model::TaskNamespace::Execution,
                task_id,
                attempt_no: 1,
                effect_id: role_effect,
                outcome: Ok(RoleRunSuccess {
                    handoff: Some(Handoff::Work {
                        done: true,
                        report: PayloadRef::inline("done"),
                        request_attention: false,
                    }),
                    artifact: Some(ArtifactOutcome {
                        base_sha: BASE_SHA.to_string(),
                        head_sha: HEAD_SHA.to_string(),
                    }),
                    final_response: PayloadRef::inline("done"),
                    runtime_configuration: RuntimeConfigurationEvidence::default(),
                }),
            }),
            NewEvent::new(MissionEvent::OracleRunRequested {
                assertion_ids: vec![lionclaw::model::AssertionId::new("TESTS-PASS").unwrap()],
                oracle: oracle.clone(),
                judged_sha: HEAD_SHA.to_string(),
                attempt_no: 1,
                effect_id: oracle_effect.clone(),
                requested_at_ms: 0,
                not_before_ms: 0,
                deadline_ms: 100_000,
            }),
        ],
        1,
    )
    .await;

    let view = h.engine.advance(&mission_id).await.expect("advance");
    assert_eq!(view.disposition, MissionDisposition::Parked);
    assert!(view.state.inflight.is_empty());
    assert!(h.oracle_runner.calls.lock().expect("lock").is_empty());
    assert_eq!(
        view.state.oracle_failures.get(&oracle).unwrap().category(),
        "interrupted"
    );
    let events = h.engine.store().load(&mission_id).await.expect("load");
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                &event.event,
                MissionEvent::OracleRunCompleted { effect_id, outcome: Err(failure), .. }
                    if effect_id == &oracle_effect && matches!(failure, lionclaw_runtime_api::TypedFailure::Interrupted { .. })
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
        .create_mission(dir.path().to_str().unwrap(), "obj", BASE_SHA)
        .await
        .expect("create");
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    let task_id = TaskId::new("fix").unwrap();
    let id = role_effect(&mission_id, &task_id);
    let state = h.engine.load_state(&mission_id).await.expect("state");
    fault_append_events(
        dir.path(),
        &mission_id,
        state.head,
        &[NewEvent::new(MissionEvent::RoleRunRequested {
            conversation_id: lionclaw::model::ConversationId::for_role_instance(
                &mission_id,
                lionclaw::model::TaskNamespace::Execution,
                &task_id,
                &lionclaw::model::RoleName::new("implementer").unwrap(),
                1,
            ),
            namespace: lionclaw::model::TaskNamespace::Execution,
            task_id,
            attempt_no: 1,
            effect_id: id.clone(),
            role: lionclaw::model::RoleName::new("implementer").unwrap(),
            output: OutputSemantics::ProducesArtifact,
            runtime: "codex".to_string(),
            prompt: lionclaw::model::PayloadRef::inline("prompt"),
            base_sha: BASE_SHA.to_string(),
            assignment_epoch: 1,
            message_boundary: state.head,
            presented_messages: vec![],
            recreate_workspace: true,
            requested_at_ms: 0,
            not_before_ms: 0,
            deadline_ms: 100_000,
            budget_deadline_ms: 100_000,
        })
        .with_prompt_hash(CRASHED_PROMPT_HASH)],
        1,
    )
    .await;
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
