//! The mission type is pinned by content digest at start and verified on every
//! engine open. A mutated role or playbook must refuse to advance the mission,
//! not run against changed mission instructions.

mod common;

use std::path::Path;
use std::sync::Arc;

use common::{test_mission_type, BASE_SHA, HEAD_SHA};
use lionclaw::authority::AuthorityCeiling;
use lionclaw::engine::{Engine, EngineServices};
use lionclaw::mission_type::load_mission_type;
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockClock, MockOracleRunner, MockRoleRunner, NoopEffectCleaner};

fn write_minimal_type(root: &Path) {
    std::fs::create_dir_all(root.join("roles")).unwrap();
    std::fs::write(
        root.join("mission.toml"),
        "[mission-type]\nname = \"digest-test\"\nstop = \"verified\"\nimage = \"img\"\n\
         \n[team]\nplanning-assignment = \"strategist\"\n\
         \n[ceilings]\nnetwork = true\ninstall = true\nwrites = true\n",
    )
    .unwrap();
    std::fs::write(
        root.join("roles/implementer.md"),
        "---\noutput: produces-artifact\nruntime: codex\n---\nDo it.\n",
    )
    .unwrap();
    std::fs::write(
        root.join("roles/strategist.md"),
        "---\noutput: proposes-plan\nruntime: codex\n---\nPlan it.\n",
    )
    .unwrap();
    std::fs::write(root.join("playbook.md"), "# Digest test\n").unwrap();
}

/// The pin is only meaningful because the digest is computed over the loaded
/// content: prove editing a role or playbook changes it (the equality check in
/// `opening_a_mission_whose_type_digest_changed_is_refused` below is otherwise
/// vacuous if `compute_digest` ignored content).
#[test]
fn the_digest_tracks_role_and_playbook_content() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("digest-test");
    write_minimal_type(&root);
    let digest = || {
        load_mission_type(&root, &AuthorityCeiling::default())
            .expect("loads")
            .digest()
            .to_string()
    };
    let base = digest();

    // Editing a role's prose changes the digest, and restoring it restores the
    // digest (so the walk is deterministic, not merely order-varying).
    std::fs::write(
        root.join("roles/implementer.md"),
        "---\noutput: produces-artifact\nruntime: codex\n---\nDo it differently.\n",
    )
    .unwrap();
    assert_ne!(base, digest(), "a mutated role must change the digest");
    std::fs::write(
        root.join("roles/implementer.md"),
        "---\noutput: produces-artifact\nruntime: codex\n---\nDo it.\n",
    )
    .unwrap();
    assert_eq!(base, digest(), "restoring the role restores the digest");

    std::fs::write(root.join("playbook.md"), "# Changed digest test\n").unwrap();
    assert_ne!(base, digest(), "a mutated playbook must change the digest");
}

#[test]
fn the_digest_tracks_recursive_skill_content() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("digest-test");
    write_minimal_type(&root);
    std::fs::create_dir_all(root.join("skills/rust/references")).unwrap();
    std::fs::write(
        root.join("skills/rust/SKILL.md"),
        "---\nname: rust\ndescription: Rust.\n---\n\n# Rust\n",
    )
    .unwrap();
    std::fs::write(root.join("skills/rust/references/guide.md"), "first\n").unwrap();
    std::fs::write(
        root.join("mission.toml"),
        "[mission-type]\nname = \"digest-test\"\nstop = \"verified\"\nimage = \"img\"\n\
         \n[team]\nplanning-assignment = \"strategist\"\n\
         \n[ceilings]\nnetwork = true\ninstall = true\nwrites = true\n",
    )
    .unwrap();
    std::fs::write(
        root.join("roles/implementer.md"),
        "---\noutput: produces-artifact\nruntime: codex\nskills: [rust]\n---\nDo it.\n",
    )
    .unwrap();

    let digest = || {
        load_mission_type(&root, &AuthorityCeiling::default())
            .expect("loads")
            .digest()
            .to_string()
    };
    let base = digest();
    std::fs::write(root.join("skills/rust/references/guide.md"), "second\n").unwrap();
    assert_ne!(
        base,
        digest(),
        "nested skill resources must change the digest"
    );
}

#[tokio::test]
async fn opening_a_mission_whose_type_digest_changed_is_refused() {
    let dir = tempfile::tempdir().unwrap();

    // Engine A creates the mission, recording its mission type's digest.
    let store = MissionStore::open(dir.path()).await.unwrap();
    let engine_a = Engine::new(
        store,
        test_mission_type(),
        "img".to_string(),
        EngineServices::new(
            Arc::new(MockRoleRunner::happy(HEAD_SHA)),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );
    let original_digest = engine_a.mission_type().digest().to_string();
    let id = engine_a
        .create_mission(&dir.path().to_string_lossy(), "obj", BASE_SHA)
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
    changed.edit_for_testing(|definition| {
        definition
            .default_team
            .roles
            .values_mut()
            .next()
            .expect("test role")
            .instructions
            .push_str(" changed");
    });
    assert_ne!(changed.digest(), original_digest);
    let store = MissionStore::open(dir.path()).await.unwrap();
    let engine_b = Engine::new(
        store,
        changed,
        "img".to_string(),
        EngineServices::new(
            Arc::new(MockRoleRunner::happy(HEAD_SHA)),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
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
