//! The real software-dev plugin loads; the moat rejects a plugin whose
//! verdict role would violate the honesty floor.

use std::path::PathBuf;

use lionclaw::authority::AuthorityCeiling;
use lionclaw::authority::MoatViolation;
use lionclaw::mission_type::{load_mission_type, MissionTypeError};
use lionclaw::model::{OutputSemantics, StopBar};

fn repo_root() -> PathBuf {
    // <crate>/tests/plugin_loading.rs → repo root is three parents up from
    // the crate dir.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf()
}

#[test]
fn software_dev_plugin_loads() {
    let plugin = load_mission_type(
        &repo_root().join("mission-types/software-dev"),
        &AuthorityCeiling::default(),
    )
    .expect("software-dev plugin loads");
    assert_eq!(plugin.name, "software-dev");
    assert_eq!(plugin.stop, StopBar::Verified);
    let implementer = plugin
        .roles
        .values()
        .find(|r| r.output == OutputSemantics::ProducesArtifact)
        .expect("has an implementer role");
    assert_eq!(implementer.runtime.as_deref(), Some("codex"));
    assert!(plugin
        .oracles
        .contains_key(&lionclaw::model::OracleName::new("cargo-test").expect("name")));
}

#[test]
fn writable_judge_plugin_refuses_to_load() {
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/mission-types/writable-judge");
    let err = load_mission_type(&fixture, &AuthorityCeiling::default()).expect_err("must refuse");
    // A verdict role asking for secrets cannot satisfy the moat — a typed
    // moat violation, not a generic role error.
    assert!(
        matches!(
            err,
            MissionTypeError::Moat {
                violation: MoatViolation::SecretsForJudge { .. },
                ..
            }
        ),
        "expected a typed moat violation, got {err:?}"
    );
}

#[test]
fn role_declaring_skills_is_rejected_at_load() {
    // Skill projection isn't wired; a declared skill must fail closed rather
    // than be a silent no-op.
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"skilled\"\nstop = \"verified\"\nimage = \"img\"\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("roles")).unwrap();
    std::fs::write(
        dir.path().join("roles/worker.md"),
        "---\noutput: produces-artifact\nskills: [rust]\n---\nDo it.\n",
    )
    .unwrap();
    let err = load_mission_type(dir.path(), &AuthorityCeiling::default()).expect_err("must refuse");
    assert!(
        matches!(&err, MissionTypeError::Role { detail, .. } if detail.contains("skills")),
        "got {err:?}"
    );
}

#[test]
fn a_planning_dag_naming_an_execution_role_fails_to_load() {
    // The planning DAG must reference only planning roles. An execution role
    // (produces-artifact) named as a planning node is rejected fail-closed.
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"bad-planning\"\nstop = \"reviewed\"\nimage = \"img\"\n\
         \n[[planning.tasks]]\nid = \"author\"\nrole = \"worker\"\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("roles")).unwrap();
    std::fs::write(
        dir.path().join("roles/worker.md"),
        "---\noutput: produces-artifact\n---\nDo it.\n",
    )
    .unwrap();
    let err = load_mission_type(dir.path(), &AuthorityCeiling::default()).expect_err("must refuse");
    assert!(
        matches!(&err, MissionTypeError::Manifest(detail) if detail.contains("planning")),
        "got {err:?}"
    );
}
