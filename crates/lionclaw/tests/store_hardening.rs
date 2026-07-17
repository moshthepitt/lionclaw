//! The event log is the only durable effect queue. There is no second ledger
//! to lease, reseed, or reconcile.

mod common;

use common::{
    approve_plan, effect_id, fault_append_events, harness, proposal, simple_plan, BASE_SHA,
    HEAD_SHA,
};
use lionclaw::model::{MissionEvent, OutputSemantics, PayloadRef, RoleName, TaskId};
use lionclaw::store::NewEvent;
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};

#[tokio::test]
async fn unfinished_request_is_rebuilt_from_the_log_alone() {
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
    let id = effect_id("unfinished");
    let state = h.engine.load_state(&mission_id).await.expect("state");
    fault_append_events(
        dir.path(),
        &mission_id,
        state.head,
        &[NewEvent::new(MissionEvent::RoleRunRequested {
            namespace: lionclaw::model::TaskNamespace::Execution,
            task_id: TaskId::new("fix").unwrap(),
            attempt_no: 1,
            effect_id: id.clone(),
            role: RoleName::new("implementer").unwrap(),
            output: OutputSemantics::ProducesArtifact,
            runtime: "codex".to_string(),
            prompt: PayloadRef::inline("prompt"),
            base_sha: BASE_SHA.to_string(),
            assignment_epoch: 1,
            recreate_workspace: true,
            requested_at_ms: 0,
            not_before_ms: 0,
            deadline_ms: 100_000,
            budget_deadline_ms: 100_000,
        })],
        1,
    )
    .await;

    let before = h.engine.load_state(&mission_id).await.expect("state");
    assert!(before.inflight.contains_key(&id));
    let rebuilt = h
        .engine
        .store()
        .rebuild_cursors(&mission_id, 2)
        .await
        .expect("rebuild");
    assert_eq!(rebuilt, before);

    let database = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}",
        dir.path().join(".lionclaw/mission.db").display()
    ))
    .await
    .expect("open schema");
    let tables: Vec<(String,)> = sqlx::query_as(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'mission_effects'",
    )
    .fetch_all(&database)
    .await
    .expect("schema query");
    assert!(tables.is_empty(), "parallel effect ledger must not exist");
}

#[tokio::test]
async fn an_old_event_schema_is_refused_instead_of_accepted_by_accident() {
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
    let state = h.engine.load_state(&mission_id).await.expect("state");
    let mut old = NewEvent::new(MissionEvent::MissionAborted {
        reason: "old writer".to_string(),
    });
    old.stamps.schema_version = lionclaw::model::SCHEMA_VERSION - 1;
    fault_append_events(dir.path(), &mission_id, state.head, &[old], 2).await;

    let error = h
        .engine
        .store()
        .load(&mission_id)
        .await
        .expect_err("reader must reject the old schema");
    assert!(
        error.to_string().contains("unsupported schema version"),
        "got: {error:#}"
    );

    let database = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}",
        dir.path().join(".lionclaw/mission.db").display()
    ))
    .await
    .expect("open database");
    sqlx::query(
        "UPDATE mission_events SET schema_version = ?1 \
         WHERE mission_id = ?2 AND sequence_no = ?3",
    )
    .bind(i64::from(lionclaw::model::SCHEMA_VERSION))
    .bind(mission_id.as_str())
    .bind((state.head + 1) as i64)
    .execute(&database)
    .await
    .expect("corrupt redundant stamp");
    let error = h
        .engine
        .store()
        .load(&mission_id)
        .await
        .expect_err("conflicting stamps must be rejected");
    assert!(
        error.to_string().contains("conflicting schema stamps"),
        "got: {error:#}"
    );
}
