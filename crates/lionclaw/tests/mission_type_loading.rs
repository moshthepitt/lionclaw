//! The real software-dev mission type loads; the moat rejects a mission type whose
//! verdict role would violate the honesty floor.

use std::path::PathBuf;

use lionclaw::authority::AuthorityCeiling;
use lionclaw::authority::MoatViolation;
use lionclaw::mission_type::{load_mission_type, MissionTypeError};
use lionclaw::model::StopBar;

fn repo_root() -> PathBuf {
    // <crate>/tests/mission_type_loading.rs → repo root is two parents up from
    // the crate manifest dir (crates/lionclaw → crates → repo root).
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf()
}

#[test]
fn software_dev_mission_type_loads() {
    let mission_type = load_mission_type(
        &repo_root().join("mission-types/software-dev"),
        &AuthorityCeiling::default(),
    )
    .expect("software-dev mission type loads");
    assert_eq!(mission_type.name, "software-dev");
    assert_eq!(mission_type.stop, StopBar::Verified);
    assert!(
        mission_type
            .roles
            .values()
            .all(|role| role.runtime.is_none()),
        "bundled roles must inherit the mission's selected runtime"
    );
    assert!(mission_type
        .oracles
        .contains_key(&lionclaw::model::OracleName::new("cargo-test").expect("name")));
}

#[test]
fn writable_judge_mission_type_refuses_to_load() {
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
fn role_declaring_a_bundled_skill_loads_the_resolved_package() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"skilled\"\nstop = \"verified\"\nimage = \"img\"\n\
         \n[skills.rust]\nsource = { path = \"skills/rust\" }\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("roles")).unwrap();
    std::fs::create_dir_all(dir.path().join("skills/rust")).unwrap();
    std::fs::write(
        dir.path().join("skills/rust/SKILL.md"),
        "---\nname: rust\ndescription: Work effectively in Rust.\n---\n\n# Rust\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("roles/worker.md"),
        "---\noutput: produces-artifact\nskills: [rust]\n---\nDo it.\n",
    )
    .unwrap();
    let mission_type =
        load_mission_type(dir.path(), &AuthorityCeiling::default()).expect("mission type loads");
    let role = mission_type.roles.values().next().expect("worker role");
    assert_eq!(role.skills, ["rust"]);
    assert_eq!(
        mission_type.skills.get("rust").expect("rust package").root,
        dir.path().join("skills/rust")
    );
}

#[test]
fn role_referencing_an_undeclared_skill_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"skilled\"\nstop = \"verified\"\nimage = \"img\"\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("roles")).unwrap();
    std::fs::write(
        dir.path().join("roles/worker.md"),
        "---\noutput: produces-artifact\nskills: [missing]\n---\nDo it.\n",
    )
    .unwrap();

    let err = load_mission_type(dir.path(), &AuthorityCeiling::default()).expect_err("must refuse");
    assert!(
        matches!(&err, MissionTypeError::Role { detail, .. } if detail.contains("undeclared skill 'missing'")),
        "got {err:?}"
    );
}

#[test]
fn a_symlinked_skill_package_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let external = tempfile::tempdir().expect("external skill");
    std::fs::write(
        external.path().join("SKILL.md"),
        "---\nname: rust\ndescription: Rust.\n---\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"skilled\"\nstop = \"verified\"\nimage = \"img\"\n\
         \n[skills.rust]\nsource = { path = \"skills/rust\" }\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("roles")).unwrap();
    std::fs::create_dir_all(dir.path().join("skills")).unwrap();
    std::os::unix::fs::symlink(external.path(), dir.path().join("skills/rust")).unwrap();
    std::fs::write(
        dir.path().join("roles/worker.md"),
        "---\noutput: produces-artifact\nskills: [rust]\n---\nDo it.\n",
    )
    .unwrap();

    let err = load_mission_type(dir.path(), &AuthorityCeiling::default()).expect_err("must refuse");
    assert!(
        matches!(&err, MissionTypeError::Skill { detail, .. } if detail.contains("symlink")),
        "got {err:?}"
    );
}

#[test]
fn a_skill_package_through_an_escaping_parent_symlink_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let external = tempfile::tempdir().expect("external skills");
    let external_skill = external.path().join("rust");
    std::fs::create_dir(&external_skill).unwrap();
    std::fs::write(
        external_skill.join("SKILL.md"),
        "---\nname: rust\ndescription: Rust.\n---\n\n# Rust\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"skilled\"\nstop = \"reviewed\"\nimage = \"img\"\n\
         \n[skills.rust]\nsource = { path = \"skills/rust\" }\n",
    )
    .unwrap();
    std::fs::create_dir(dir.path().join("roles")).unwrap();
    std::os::unix::fs::symlink(external.path(), dir.path().join("skills")).unwrap();
    std::fs::write(
        dir.path().join("roles/worker.md"),
        "---\noutput: produces-artifact\nskills: [rust]\n---\nDo it.\n",
    )
    .unwrap();

    let err = load_mission_type(dir.path(), &AuthorityCeiling::default()).expect_err("must refuse");
    assert!(
        matches!(&err, MissionTypeError::Skill { detail, .. } if detail.contains("outside mission type")),
        "got {err:?}"
    );
}

#[test]
fn skill_frontmatter_name_must_match_the_declared_package() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"skilled\"\nstop = \"reviewed\"\nimage = \"img\"\n\
         \n[skills.expected]\nsource = { path = \"skills/expected\" }\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("roles")).unwrap();
    std::fs::create_dir_all(dir.path().join("skills/expected")).unwrap();
    std::fs::write(
        dir.path().join("skills/expected/SKILL.md"),
        "---\nname: different\ndescription: Wrong name.\n---\n\n# Instructions\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("roles/worker.md"),
        "---\noutput: produces-artifact\nskills: [expected]\n---\nDo it.\n",
    )
    .unwrap();

    let err = load_mission_type(dir.path(), &AuthorityCeiling::default()).expect_err("must refuse");
    assert!(
        matches!(&err, MissionTypeError::Skill { detail, .. } if detail.contains("must match")),
        "got {err:?}"
    );
}

#[test]
fn duplicate_role_skill_references_are_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"skilled\"\nstop = \"reviewed\"\nimage = \"img\"\n\
         \n[skills.rust]\nsource = { path = \"skills/rust\" }\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("roles")).unwrap();
    std::fs::create_dir_all(dir.path().join("skills/rust")).unwrap();
    std::fs::write(
        dir.path().join("skills/rust/SKILL.md"),
        "---\nname: rust\ndescription: Rust.\n---\n\n# Rust\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("roles/worker.md"),
        "---\noutput: produces-artifact\nskills: [rust, rust]\n---\nDo it.\n",
    )
    .unwrap();

    let err = load_mission_type(dir.path(), &AuthorityCeiling::default()).expect_err("must refuse");
    assert!(
        matches!(&err, MissionTypeError::Role { detail, .. } if detail.contains("more than once")),
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

// ---- Loader fail-closed guards (each with a failing-first fault injection) ----

fn write_oracle(path: &std::path::Path, contents: &str, executable: bool) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::write(path, contents).unwrap();
    // Set the mode explicitly — `fs::write` preserves an existing file's bits.
    let mode = if executable { 0o755 } else { 0o644 };
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

/// A minimal, loadable mission type: one artifact role + one valid oracle.
fn write_valid_type(root: &std::path::Path) {
    std::fs::create_dir_all(root.join("roles")).unwrap();
    std::fs::create_dir_all(root.join("oracles")).unwrap();
    std::fs::write(
        root.join("mission.toml"),
        "[mission-type]\nname = \"guarded\"\nstop = \"verified\"\nimage = \"img\"\n",
    )
    .unwrap();
    std::fs::write(
        root.join("roles/implementer.md"),
        "---\noutput: produces-artifact\n---\nDo it.\n",
    )
    .unwrap();
    write_oracle(
        &root.join("oracles/cargo-test"),
        "#!/bin/sh\nexit 0\n",
        true,
    );
}

fn load_err(root: &std::path::Path) -> MissionTypeError {
    load_mission_type(root, &AuthorityCeiling::default()).expect_err("must refuse")
}

#[test]
fn the_minimal_type_loads() {
    let dir = tempfile::tempdir().unwrap();
    write_valid_type(dir.path());
    load_mission_type(dir.path(), &AuthorityCeiling::default()).expect("valid type loads");
}

#[test]
fn a_non_executable_oracle_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    write_valid_type(dir.path());
    write_oracle(
        &dir.path().join("oracles/cargo-test"),
        "#!/bin/sh\nexit 0\n",
        false,
    );
    assert!(
        matches!(&load_err(dir.path()), MissionTypeError::Oracle { detail, .. } if detail.contains("executable")),
    );
}

#[test]
fn an_oracle_without_a_shebang_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    write_valid_type(dir.path());
    write_oracle(&dir.path().join("oracles/cargo-test"), "exit 0\n", true);
    assert!(
        matches!(&load_err(dir.path()), MissionTypeError::Oracle { detail, .. } if detail.contains("shebang")),
    );
}

#[test]
fn a_symlinked_oracle_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    write_valid_type(dir.path());
    let oracle = dir.path().join("oracles/cargo-test");
    std::fs::remove_file(&oracle).unwrap();
    std::os::unix::fs::symlink("/bin/sh", &oracle).unwrap();
    assert!(
        matches!(&load_err(dir.path()), MissionTypeError::Oracle { detail, .. } if detail.contains("regular file")),
    );
}

#[test]
fn a_mission_type_with_no_roles_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    write_valid_type(dir.path());
    std::fs::remove_file(dir.path().join("roles/implementer.md")).unwrap();
    assert!(matches!(load_err(dir.path()), MissionTypeError::NoRoles(_)));
}

#[test]
fn a_path_unsafe_mission_type_name_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    write_valid_type(dir.path());
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"../evil\"\nstop = \"verified\"\nimage = \"img\"\n",
    )
    .unwrap();
    assert!(
        matches!(&load_err(dir.path()), MissionTypeError::Manifest(d) if d.contains("path-safe")),
    );
}
