//! The event log is the only durable effect queue. There is no second ledger
//! to lease, reseed, or reconcile.

mod common;

use common::{default_config, effect_id, harness, BASE_SHA, HEAD_SHA};
use lionclaw::model::{MissionEvent, PayloadRef, RoleName, TaskId};
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
        .create_mission(
            dir.path().to_str().unwrap(),
            "obj",
            BASE_SHA,
            default_config(),
        )
        .await
        .expect("create");
    let id = effect_id("unfinished");
    let state = h.engine.load_state(&mission_id).await.expect("state");
    h.engine
        .store()
        .append(
            &mission_id,
            state.head,
            &[NewEvent::new(MissionEvent::RoleRunRequested {
                task_id: TaskId::new("fix").unwrap(),
                attempt_no: 1,
                effect_id: id.clone(),
                role: RoleName::new("implementer").unwrap(),
                runtime: "codex".to_string(),
                prompt: PayloadRef::inline("prompt"),
                base_sha: BASE_SHA.to_string(),
            })],
            1,
        )
        .await
        .expect("append request");

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
