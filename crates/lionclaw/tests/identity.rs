//! The mission type is pinned by content digest at start and verified on every
//! engine open. A mutated role or oracle (the fake-green vector) must refuse to
//! advance the mission, not run against a changed instrument of judgment.

mod common;

use std::sync::Arc;

use common::{default_config, test_mission_type, BASE_SHA, HEAD_SHA};
use lionclaw::engine::Engine;
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockClock, MockOracleRunner, MockRoleRunner};

#[tokio::test]
async fn opening_a_mission_whose_type_digest_changed_is_refused() {
    let dir = tempfile::tempdir().unwrap();

    // Engine A creates the mission, recording its mission type's digest.
    let store = MissionStore::open(dir.path()).await.unwrap();
    let engine_a = Engine::new(
        store,
        test_mission_type(), // digest "test-digest"
        "codex".to_string(),
        "img".to_string(),
        Arc::new(MockRoleRunner::happy(HEAD_SHA)),
        Arc::new(MockOracleRunner::exiting(0)),
        Arc::new(MockClock::default()),
    );
    let id = engine_a
        .create_mission(
            &dir.path().to_string_lossy(),
            "obj",
            BASE_SHA,
            default_config(),
        )
        .await
        .unwrap();
    // Same engine (matching digest) loads fine.
    engine_a
        .load_state(&id)
        .await
        .expect("matching digest loads");

    // Engine B over the SAME store, but its mission type's digest differs (as if
    // a role or oracle was edited on disk between start and advance).
    let mut changed = test_mission_type();
    changed.digest = "a-different-digest".to_string();
    let store = MissionStore::open(dir.path()).await.unwrap();
    let engine_b = Engine::new(
        store,
        changed,
        "codex".to_string(),
        "img".to_string(),
        Arc::new(MockRoleRunner::happy(HEAD_SHA)),
        Arc::new(MockOracleRunner::exiting(0)),
        Arc::new(MockClock::default()),
    );
    let err = engine_b
        .load_state(&id)
        .await
        .expect_err("a changed mission type must be refused, fail-closed");
    assert!(
        err.to_string()
            .contains("changed since this mission started"),
        "expected a pinned-digest refusal, got: {err}"
    );
}
