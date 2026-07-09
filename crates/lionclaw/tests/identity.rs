//! The mission type is pinned by content digest at start and verified on every
//! engine open. A mutated role or oracle (the fake-green vector) must refuse to
//! advance the mission, not run against a changed instrument of judgment.

mod common;

use std::path::Path;
use std::sync::Arc;

use common::{default_config, test_mission_type, BASE_SHA, HEAD_SHA};
use lionclaw::authority::AuthorityCeiling;
use lionclaw::engine::Engine;
use lionclaw::mission_type::load_mission_type;
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockClock, MockOracleRunner, MockRoleRunner};

fn write_minimal_type(root: &Path) {
    std::fs::create_dir_all(root.join("roles")).unwrap();
    std::fs::create_dir_all(root.join("oracles")).unwrap();
    std::fs::write(
        root.join("mission.toml"),
        "[mission-type]\nname = \"digest-test\"\nstop = \"verified\"\nimage = \"img\"\n",
    )
    .unwrap();
    std::fs::write(
        root.join("roles/implementer.md"),
        "---\noutput: produces-artifact\n---\nDo it.\n",
    )
    .unwrap();
    let oracle = root.join("oracles/cargo-test");
    std::fs::write(&oracle, "#!/bin/sh\nexit 0\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&oracle, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}

/// The pin is only meaningful because the digest is computed over the role and
/// oracle *content*: prove editing either file changes it (the equality check in
/// `opening_a_mission_whose_type_digest_changed_is_refused` below is otherwise
/// vacuous if `compute_digest` ignored content).
#[test]
fn the_digest_tracks_role_and_oracle_content() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("digest-test");
    write_minimal_type(&root);
    let digest = || {
        load_mission_type(&root, &AuthorityCeiling::default())
            .expect("loads")
            .digest
    };
    let base = digest();

    // Editing a role's prose changes the digest, and restoring it restores the
    // digest (so the walk is deterministic, not merely order-varying).
    std::fs::write(
        root.join("roles/implementer.md"),
        "---\noutput: produces-artifact\n---\nDo it differently.\n",
    )
    .unwrap();
    assert_ne!(base, digest(), "a mutated role must change the digest");
    std::fs::write(
        root.join("roles/implementer.md"),
        "---\noutput: produces-artifact\n---\nDo it.\n",
    )
    .unwrap();
    assert_eq!(base, digest(), "restoring the role restores the digest");

    // Editing an oracle's bytes changes the digest — the fake-green vector the
    // pin exists to close.
    std::fs::write(root.join("oracles/cargo-test"), "#!/bin/sh\nexit 1\n").unwrap();
    assert_ne!(base, digest(), "a mutated oracle must change the digest");
}

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
