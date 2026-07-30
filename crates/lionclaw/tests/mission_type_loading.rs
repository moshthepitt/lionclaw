//! The real software-dev mission type loads; the moat rejects a mission type whose
//! verdict role would violate the honesty floor.

use std::path::PathBuf;

use lionclaw::authority::AuthorityCeiling;
use lionclaw::authority::MoatViolation;
use lionclaw::mission_type::{load_mission_type, MissionType, MissionTypeError};
use lionclaw::model::{RoleInstanceId, StopBar};

fn repo_root() -> PathBuf {
    // <crate>/tests/mission_type_loading.rs → repo root is two parents up from
    // the crate manifest dir (crates/lionclaw → crates → repo root).
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf()
}

fn write_minimal_bundle(root: &std::path::Path) {
    std::fs::create_dir_all(root.join("roles")).unwrap();
    std::fs::write(
        root.join("mission.toml"),
        "[mission-type]\nname = \"bounded\"\nstop = \"verified\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"worker\"\n",
    )
    .unwrap();
    std::fs::write(
        root.join("roles/worker.md"),
        "---\noutput: proposes-plan\nruntime: codex\n---\nDo it.\n",
    )
    .unwrap();
    std::fs::write(root.join("playbook.md"), "# Bounded\n").unwrap();
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
            .default_team
            .roles
            .values()
            .all(|role| role.runtime == "codex"),
        "bundled roles pin their team-owned runtime"
    );
    assert_eq!(
        mission_type.resource_ceilings.tmpfs,
        ["/tmp:rw,size=1g".to_string()]
    );
    let planner = &mission_type.default_team.planning_assignment;
    assert_eq!(planner.as_str(), "planner");
    assert_eq!(
        mission_type.default_team.roles[planner].output,
        lionclaw::model::OutputSemantics::ProposesPlan
    );
}

#[test]
fn exactly_five_generic_mission_types_load() {
    let root = repo_root().join("mission-types");
    let mut names = std::fs::read_dir(&root)
        .expect("mission-types directory")
        .map(|entry| entry.expect("mission-type entry").file_name())
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(
        names,
        [
            "design",
            "optimization",
            "research",
            "review",
            "software-dev"
        ]
    );

    for name in ["design", "optimization", "research", "review"] {
        let mission_type = load_mission_type(&root.join(name), &AuthorityCeiling::default())
            .unwrap_or_else(|error| panic!("{name} mission type must load: {error:#}"));
        assert_eq!(mission_type.name, name);
        assert_eq!(mission_type.stop, StopBar::Attested);
        assert!(
            mission_type.default_team.roles.len() <= 4,
            "{name} must keep a minimal default team"
        );
        let planner = &mission_type.default_team.planning_assignment;
        assert_eq!(
            mission_type.default_team.roles[planner].output,
            lionclaw::model::OutputSemantics::ProposesPlan
        );
    }

    assert!(!root.join("metric-driven").exists());
}

#[test]
fn review_method_can_produce_a_report_without_mutating_the_judged_product() {
    let mission_type = load_mission_type(
        &repo_root().join("mission-types/review"),
        &AuthorityCeiling::default(),
    )
    .expect("review mission type loads");
    let investigator =
        &mission_type.default_team.roles[&RoleInstanceId::new("investigator").unwrap()];
    assert_eq!(
        investigator.output,
        lionclaw::model::OutputSemantics::ProducesReport
    );
    assert!(!investigator.grants.writes);
    assert!(!mission_type.ceilings.writes);
    assert!(investigator.instructions.contains("review report"));
    assert!(investigator.instructions.contains("read-only"));
}

#[test]
fn mission_type_loads_role_resource_declarations_within_ceiling() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_minimal_bundle(dir.path());
    std::fs::write(
        dir.path().join("mission.toml"),
        r#"[mission-type]
name = "bounded"
stop = "verified"
image = "img"

[team]
planning-assignment = "worker"

[resource-ceilings]
tmpfs = ["/tmp:rw,size=2g"]
"#,
    )
    .unwrap();
    std::fs::write(
        dir.path().join("roles/worker.md"),
        "---\noutput: proposes-plan\nruntime: codex\ntmpfs: [\"/tmp:rw,size=1g\"]\n---\nDo it.\n",
    )
    .unwrap();
    let mission_type =
        load_mission_type(dir.path(), &AuthorityCeiling::default()).expect("mission type loads");
    let worker = mission_type
        .default_team
        .roles
        .get(&RoleInstanceId::new("worker").unwrap())
        .expect("worker role");
    assert_eq!(worker.resources.tmpfs, ["/tmp:rw,size=1g".to_string()]);
}

#[test]
fn mission_type_rejects_resource_declaration_above_ceiling() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_minimal_bundle(dir.path());
    std::fs::write(
        dir.path().join("mission.toml"),
        r#"[mission-type]
name = "bounded"
stop = "verified"
image = "img"

[team]
planning-assignment = "worker"

[resource-ceilings]
tmpfs = ["/tmp:rw,size=1g"]
"#,
    )
    .unwrap();
    std::fs::write(
        dir.path().join("roles/worker.md"),
        "---\noutput: proposes-plan\nruntime: codex\ntmpfs: [\"/tmp:rw,size=2g\"]\n---\nDo it.\n",
    )
    .unwrap();

    let error = load_mission_type(dir.path(), &AuthorityCeiling::default())
        .expect_err("over-ceiling resources must fail closed");
    assert!(
        error.to_string().contains("outside mission ceilings"),
        "got {error:?}"
    );
}

#[test]
fn mission_environment_cannot_replace_kernel_coordinates() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_minimal_bundle(dir.path());
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"bounded\"\nstop = \"verified\"\nimage = \"img\"\nenvironment = { HOME = \"/elsewhere\" }\n\n[team]\nplanning-assignment = \"worker\"\n",
    )
    .unwrap();

    let error = load_mission_type(dir.path(), &AuthorityCeiling::default())
        .expect_err("kernel coordinate override must fail closed");
    assert!(
        matches!(&error, MissionTypeError::Manifest(detail) if detail.contains("owned by the LionClaw kernel")),
        "got {error:?}"
    );
}

#[test]
fn mission_bundle_depth_is_bounded_before_semantic_loading() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_minimal_bundle(dir.path());
    let mut nested = dir.path().join("resources");
    for _ in 0..64 {
        nested.push("nested");
    }
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::write(nested.join("leaf"), "leaf").unwrap();

    let error = load_mission_type(dir.path(), &AuthorityCeiling::default())
        .expect_err("deep bundle must be rejected");
    assert!(error.to_string().contains("depth limit"), "got {error:?}");
}

#[test]
fn mission_bundle_entry_count_is_bounded_before_semantic_loading() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_minimal_bundle(dir.path());
    let resources = dir.path().join("resources");
    std::fs::create_dir(&resources).unwrap();
    for index in 0..4_100 {
        std::fs::write(resources.join(format!("entry-{index:04}")), []).unwrap();
    }

    let error = load_mission_type(dir.path(), &AuthorityCeiling::default())
        .expect_err("oversized bundle inventory must be rejected");
    assert!(error.to_string().contains("entry limit"), "got {error:?}");
}

#[test]
fn mission_bundle_bytes_are_bounded_before_snapshot_copy() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_minimal_bundle(dir.path());
    std::fs::File::create(dir.path().join("oversized-resource"))
        .unwrap()
        .set_len(256 * 1024 * 1024 + 1)
        .unwrap();

    let error = load_mission_type(dir.path(), &AuthorityCeiling::default())
        .expect_err("oversized bundle must be rejected before copying");
    assert!(error.to_string().contains("byte limit"), "got {error:?}");
}

#[test]
fn reserved_mission_lock_path_must_be_a_regular_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_minimal_bundle(dir.path());
    std::fs::create_dir(dir.path().join("mission.lock.toml")).unwrap();

    let error = load_mission_type(dir.path(), &AuthorityCeiling::default())
        .expect_err("a directory cannot stand in for the optional lock file");
    assert!(
        error.to_string().contains("must be a regular file"),
        "got {error:?}"
    );
}

#[test]
fn skill_control_text_is_bounded_before_whole_file_loading() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_minimal_bundle(dir.path());
    let skill = dir.path().join("skills/large");
    std::fs::create_dir_all(&skill).unwrap();
    let skill_md = skill.join("SKILL.md");
    std::fs::write(
        &skill_md,
        "---\nname: large\ndescription: Bounded instructions.\n---\n\nInstructions.\n",
    )
    .unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&skill_md)
        .unwrap()
        .set_len(2 * 1024 * 1024)
        .unwrap();
    std::fs::write(
        dir.path().join("roles/worker.md"),
        "---\noutput: proposes-plan\nruntime: codex\nskills: [large]\n---\nDo it.\n",
    )
    .unwrap();

    let error = load_mission_type(dir.path(), &AuthorityCeiling::default())
        .expect_err("oversized SKILL.md must be rejected");
    assert!(error.to_string().contains("text limit"), "got {error:?}");
}

#[test]
fn mission_control_text_has_one_aggregate_budget() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_minimal_bundle(dir.path());
    let mut assigned = Vec::new();
    for index in 0..5 {
        let name = format!("large-{index}");
        assigned.push(name.clone());
        let skill = dir.path().join("skills").join(&name);
        std::fs::create_dir_all(&skill).unwrap();
        let skill_md = skill.join("SKILL.md");
        std::fs::write(
            &skill_md,
            format!(
                "---\nname: {name}\ndescription: Aggregate bounded instructions.\n---\n\nInstructions.\n"
            ),
        )
        .unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&skill_md)
            .unwrap()
            .set_len(900 * 1024)
            .unwrap();
    }
    std::fs::write(
        dir.path().join("roles/worker.md"),
        format!(
            "---\noutput: proposes-plan\nruntime: codex\nskills: [{}]\n---\nDo it.\n",
            assigned.join(", ")
        ),
    )
    .unwrap();

    let error = load_mission_type(dir.path(), &AuthorityCeiling::default())
        .expect_err("aggregate control text must be rejected");
    assert!(
        error.to_string().contains("aggregate limit"),
        "got {error:?}"
    );
}

#[test]
fn mission_digest_seals_runtime_visible_empty_skill_directories() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_minimal_bundle(dir.path());
    let skill = dir.path().join("skills/visible");
    std::fs::create_dir_all(&skill).unwrap();
    std::fs::write(
        skill.join("SKILL.md"),
        "---\nname: visible\ndescription: Visible tree.\n---\n\nInstructions.\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("roles/worker.md"),
        "---\noutput: proposes-plan\nruntime: codex\nskills: [visible]\n---\nDo it.\n",
    )
    .unwrap();

    let before = load_mission_type(dir.path(), &AuthorityCeiling::default())
        .expect("initial bundle")
        .digest()
        .to_string();
    std::fs::create_dir(skill.join("runtime-control")).unwrap();
    let after = load_mission_type(dir.path(), &AuthorityCeiling::default())
        .expect("mutated bundle")
        .digest()
        .to_string();

    assert_ne!(before, after, "every mounted tree entry must be sealed");
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
        "[mission-type]\nname = \"skilled\"\nstop = \"verified\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"worker\"\n",
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
        "---\noutput: proposes-plan\nruntime: codex\nskills: [rust]\n---\nDo it.\n",
    )
    .unwrap();
    std::fs::write(dir.path().join("playbook.md"), "# Skilled\n").unwrap();
    let mission_type =
        load_mission_type(dir.path(), &AuthorityCeiling::default()).expect("mission type loads");
    let role =
        &mission_type.default_team.roles[&RoleInstanceId::new("worker").expect("role instance")];
    assert_eq!(role.skills, ["rust"]);
    assert!(
        !mission_type
            .skills
            .get("rust")
            .expect("rust package")
            .root
            .starts_with(dir.path()),
        "runtime skill paths must belong to LionClaw's owned snapshot"
    );
}

#[test]
fn manifest_skill_declarations_are_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(dir.path().join("roles")).unwrap();
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"skilled\"\nstop = \"verified\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"worker\"\n\n[skills.rust]\nsource = { path = \"skills/rust\" }\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("roles/worker.md"),
        "---\noutput: proposes-plan\nruntime: codex\n---\nDo it.\n",
    )
    .unwrap();
    std::fs::write(dir.path().join("playbook.md"), "# Planning\n").unwrap();

    let err = load_mission_type(dir.path(), &AuthorityCeiling::default()).expect_err("must refuse");
    assert!(matches!(err, MissionTypeError::Manifest(_)));
}

#[test]
fn role_referencing_a_missing_skill_package_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"skilled\"\nstop = \"verified\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"worker\"\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("roles")).unwrap();
    std::fs::write(
        dir.path().join("roles/worker.md"),
        "---\noutput: proposes-plan\nruntime: codex\nskills: [missing]\n---\nDo it.\n",
    )
    .unwrap();

    let err = load_mission_type(dir.path(), &AuthorityCeiling::default()).expect_err("must refuse");
    assert!(
        matches!(&err, MissionTypeError::Role { detail, .. } if detail.contains("missing skill package 'missing'")),
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
        "[mission-type]\nname = \"skilled\"\nstop = \"verified\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"worker\"\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("roles")).unwrap();
    std::fs::create_dir_all(dir.path().join("skills")).unwrap();
    std::os::unix::fs::symlink(external.path(), dir.path().join("skills/rust")).unwrap();
    std::fs::write(
        dir.path().join("roles/worker.md"),
        "---\noutput: proposes-plan\nruntime: codex\nskills: [rust]\n---\nDo it.\n",
    )
    .unwrap();

    let err = load_mission_type(dir.path(), &AuthorityCeiling::default()).expect_err("must refuse");
    assert!(
        matches!(&err, MissionTypeError::Manifest(detail) if detail.contains("symlink")),
        "got {err:?}"
    );
}

#[test]
fn a_symlink_anywhere_in_the_bundle_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let external = tempfile::tempdir().expect("external");
    std::fs::create_dir_all(dir.path().join("roles")).unwrap();
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"closed\"\nstop = \"verified\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"worker\"\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("roles/worker.md"),
        "---\noutput: proposes-plan\nruntime: codex\n---\nDo it.\n",
    )
    .unwrap();
    std::os::unix::fs::symlink(external.path(), dir.path().join("unrelated-link")).unwrap();

    let err = load_mission_type(dir.path(), &AuthorityCeiling::default()).expect_err("must refuse");
    assert!(
        matches!(&err, MissionTypeError::Manifest(detail) if detail.contains("symlink")),
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
        "[mission-type]\nname = \"skilled\"\nstop = \"attested\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"worker\"\n",
    )
    .unwrap();
    std::fs::create_dir(dir.path().join("roles")).unwrap();
    std::os::unix::fs::symlink(external.path(), dir.path().join("skills")).unwrap();
    std::fs::write(
        dir.path().join("roles/worker.md"),
        "---\noutput: proposes-plan\nruntime: codex\nskills: [rust]\n---\nDo it.\n",
    )
    .unwrap();

    let err = load_mission_type(dir.path(), &AuthorityCeiling::default()).expect_err("must refuse");
    assert!(
        matches!(&err, MissionTypeError::Manifest(detail) if detail.contains("symlink")),
        "got {err:?}"
    );
}

#[test]
fn skill_frontmatter_name_must_match_the_declared_package() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"skilled\"\nstop = \"attested\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"worker\"\n",
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
        "---\noutput: proposes-plan\nruntime: codex\nskills: [expected]\n---\nDo it.\n",
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
        "[mission-type]\nname = \"skilled\"\nstop = \"attested\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"worker\"\n",
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
        "---\noutput: proposes-plan\nruntime: codex\nskills: [rust, rust]\n---\nDo it.\n",
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
        "[mission-type]\nname = \"bad-planning\"\nstop = \"verified\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"worker\"\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("roles")).unwrap();
    std::fs::write(
        dir.path().join("roles/worker.md"),
        "---\noutput: produces-artifact\nruntime: codex\n---\nDo it.\n",
    )
    .unwrap();
    std::fs::write(dir.path().join("playbook.md"), "# Planning\n").unwrap();
    let err = load_mission_type(dir.path(), &AuthorityCeiling::default()).expect_err("must refuse");
    assert!(
        matches!(&err, MissionTypeError::Manifest(detail) if detail.contains("planning")),
        "got {err:?}"
    );
}

// ---- Loader fail-closed guards (each with a failing-first fault injection) ----

fn write_program(path: &std::path::Path, contents: &str, executable: bool) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::write(path, contents).unwrap();
    // Set the mode explicitly — `fs::write` preserves an existing file's bits.
    let mode = if executable { 0o755 } else { 0o644 };
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

/// A minimal, loadable mission type with one planning role.
fn write_valid_type(root: &std::path::Path) {
    std::fs::create_dir_all(root.join("roles")).unwrap();
    std::fs::write(
        root.join("mission.toml"),
        "[mission-type]\nname = \"guarded\"\nstop = \"verified\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"implementer\"\n",
    )
    .unwrap();
    std::fs::write(
        root.join("roles/implementer.md"),
        "---\noutput: proposes-plan\nruntime: codex\n---\nDo it.\n",
    )
    .unwrap();
    std::fs::write(root.join("playbook.md"), "# Guarded\n").unwrap();
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
fn an_invalid_execution_policy_refuses_to_load() {
    let dir = tempfile::tempdir().unwrap();
    write_valid_type(dir.path());
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"guarded\"\nstop = \"verified\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"implementer\"\n\
         \n[execution]\ndefault-timeout-secs = 0\nmax-task-time-secs = 1\nextension-step-secs = 1\n",
    )
    .unwrap();

    assert!(matches!(
        &load_err(dir.path()),
        MissionTypeError::Manifest(detail) if detail.contains("[execution]")
    ));
}

fn add_input_program(root: &std::path::Path, name: &str) {
    std::fs::create_dir_all(root.join("inputs")).unwrap();
    write_program(
        &root.join("inputs").join(name),
        "#!/bin/sh\ncp /workspace/Cargo.lock /output/Cargo.lock\n",
        true,
    );
}

#[test]
fn prepared_input_declarations_load_as_plain_mission_type_data() {
    let dir = tempfile::tempdir().unwrap();
    write_valid_type(dir.path());
    add_input_program(dir.path(), "cargo-home");
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"guarded\"\nstop = \"verified\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"implementer\"\n\
         \n[ceilings]\nnetwork = { mode = \"allow\", destinations = [{ host = \"cache.example.com\", ports = [443] }] }\n\
         \n[[inputs]]\nname = \"cargo-home\"\nnetwork = { mode = \"allow\", destinations = [{ host = \"cache.example.com\", ports = [443] }] }\nkey-files = [\"Cargo.lock\"]\nenvironment = { CARGO_HOME = \"/inputs/cargo-home\" }\n",
    )
    .unwrap();

    let mission_type =
        load_mission_type(dir.path(), &AuthorityCeiling::default()).expect("valid input");
    let input = &mission_type.inputs[&lionclaw::model::InputName::new("cargo-home").unwrap()];
    assert!(input.network.allows("cache.example.com", 443));
    assert_eq!(input.key_files, [PathBuf::from("Cargo.lock")]);
    assert_eq!(input.environment["CARGO_HOME"], "/inputs/cargo-home");
}

#[test]
fn prepared_input_environment_cannot_replace_kernel_coordinates() {
    let dir = tempfile::tempdir().unwrap();
    write_valid_type(dir.path());
    add_input_program(dir.path(), "cache");
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"guarded\"\nstop = \"verified\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"implementer\"\n\
         \n[[inputs]]\nname = \"cache\"\nnetwork = { mode = \"deny\" }\nkey-files = [\"Cargo.lock\"]\nenvironment = { TMPDIR = \"/inputs/cache\" }\n",
    )
    .unwrap();

    assert!(matches!(
        &load_err(dir.path()),
        MissionTypeError::Input { detail, .. }
            if detail.contains("TMPDIR") && detail.contains("owned by the LionClaw kernel")
    ));
}

#[test]
fn loaded_runtime_files_survive_removal_of_the_source_bundle() {
    let dir = tempfile::tempdir().unwrap();
    write_valid_type(dir.path());
    add_input_program(dir.path(), "cargo-home");
    std::fs::create_dir_all(dir.path().join("skills/rust")).unwrap();
    std::fs::write(
        dir.path().join("skills/rust/SKILL.md"),
        "---\nname: rust\ndescription: Owned source.\n---\n\n# Rust\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"guarded\"\nstop = \"verified\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"implementer\"\n\
         \n[[inputs]]\nname = \"cargo-home\"\nnetwork = { mode = \"deny\" }\nkey-files = [\"Cargo.lock\"]\n",
    )
    .unwrap();

    let mission_type =
        load_mission_type(dir.path(), &AuthorityCeiling::default()).expect("valid bundle");
    let input = &mission_type.inputs[&lionclaw::model::InputName::new("cargo-home").unwrap()];
    let skill = &mission_type.skills["rust"].root;
    for path in [&input.program, skill] {
        assert!(!path.starts_with(dir.path()));
    }

    std::fs::remove_dir_all(dir.path()).unwrap();
    assert!(std::fs::read_to_string(&input.program)
        .unwrap()
        .starts_with("#!"));
    assert!(std::fs::read_to_string(skill.join("SKILL.md"))
        .unwrap()
        .contains("Owned source"));
}

#[test]
fn oversized_prepared_input_programs_fail_from_metadata() {
    let dir = tempfile::tempdir().unwrap();
    write_valid_type(dir.path());
    add_input_program(dir.path(), "cargo-home");
    std::fs::OpenOptions::new()
        .write(true)
        .open(dir.path().join("inputs/cargo-home"))
        .unwrap()
        .set_len(64 * 1024 * 1024 + 1)
        .unwrap();
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"guarded\"\nstop = \"verified\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"implementer\"\n\
         \n[[inputs]]\nname = \"cargo-home\"\nkey-files = [\"Cargo.lock\"]\n",
    )
    .unwrap();

    assert!(matches!(
        &load_err(dir.path()),
        MissionTypeError::Input { detail, .. } if detail.contains("byte limit")
    ));
}

#[test]
fn prepared_inputs_require_safe_keys() {
    let dir = tempfile::tempdir().unwrap();
    write_valid_type(dir.path());
    add_input_program(dir.path(), "cargo-home");
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"guarded\"\nstop = \"verified\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"implementer\"\n\n[[inputs]]\nname = \"cargo-home\"\nnetwork = { mode = \"deny\" }\nkey-files = [\"../Cargo.lock\"]\n",
    )
    .unwrap();
    assert!(load_mission_type(dir.path(), &AuthorityCeiling::default()).is_err());
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
        "[mission-type]\nname = \"../evil\"\nstop = \"verified\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"worker\"\n",
    )
    .unwrap();
    assert!(
        matches!(&load_err(dir.path()), MissionTypeError::Manifest(d) if d.contains("path-safe")),
    );
}

// ---- Terminal review declaration (fail-closed like the planning DAG) ----

/// `write_valid_type` plus a typed terminal-review role.
fn write_reviewer_role(root: &std::path::Path) {
    std::fs::write(
        root.join("roles/gap-reviewer.md"),
        "---\noutput: emits-gap-verdict\nruntime: codex\n---\nHunt gaps.\n",
    )
    .unwrap();
}

#[test]
fn a_terminal_review_declaration_loads_and_pins_the_role() {
    let dir = tempfile::tempdir().unwrap();
    write_valid_type(dir.path());
    write_reviewer_role(dir.path());
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"guarded\"\nstop = \"verified\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"implementer\"\ngap-review-assignment = \"gap-reviewer\"\nrequires-gap-review = true\n",
    )
    .unwrap();
    let mission_type = load_mission_type(dir.path(), &AuthorityCeiling::default()).expect("loads");
    assert_eq!(
        mission_type.default_team.gap_review_assignment,
        Some(RoleInstanceId::new("gap-reviewer").unwrap())
    );
    assert!(mission_type.requires_gap_review);
}

#[test]
fn a_attested_bar_without_terminal_review_refuses_to_load() {
    let dir = tempfile::tempdir().unwrap();
    write_valid_type(dir.path());
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"guarded\"\nstop = \"attested\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"implementer\"\nrequires-gap-review = true\n",
    )
    .unwrap();
    assert!(matches!(
        &load_err(dir.path()),
        MissionTypeError::Manifest(d) if d.contains("gap-review assignment")
    ));
}

#[test]
fn a_terminal_review_naming_a_missing_role_refuses_to_load() {
    let dir = tempfile::tempdir().unwrap();
    write_valid_type(dir.path());
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"guarded\"\nstop = \"verified\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"implementer\"\ngap-review-assignment = \"ghost\"\nrequires-gap-review = true\n",
    )
    .unwrap();
    assert!(matches!(
        &load_err(dir.path()),
        MissionTypeError::Manifest(d) if d.contains("does not name a role instance")
    ));
}

#[test]
fn a_terminal_review_naming_a_non_verdict_role_refuses_to_load() {
    let dir = tempfile::tempdir().unwrap();
    write_valid_type(dir.path());
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"guarded\"\nstop = \"verified\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"implementer\"\ngap-review-assignment = \"implementer\"\nrequires-gap-review = true\n",
    )
    .unwrap();
    assert!(matches!(
        &load_err(dir.path()),
        MissionTypeError::Manifest(d) if d.contains("must emit gap verdicts")
    ));
}

#[test]
fn editing_the_terminal_review_declaration_changes_the_digest() {
    let dir = tempfile::tempdir().unwrap();
    write_valid_type(dir.path());
    write_reviewer_role(dir.path());
    let before = load_mission_type(dir.path(), &AuthorityCeiling::default())
        .expect("loads")
        .digest()
        .to_string();
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"guarded\"\nstop = \"verified\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"implementer\"\ngap-review-assignment = \"gap-reviewer\"\nrequires-gap-review = true\n",
    )
    .unwrap();
    let after = load_mission_type(dir.path(), &AuthorityCeiling::default())
        .expect("loads")
        .digest()
        .to_string();
    // The declaration is part of the pinned instrument: adding it mid-mission
    // trips the digest check on the next engine open.
    assert_ne!(before, after);
}

// ---- SKILL-DESCRIPTIONS-AND-PROMPTS loader assertions ----

#[test]
fn skill_description_is_loaded_and_trimmed_into_the_package() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"skilled\"\nstop = \"verified\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"worker\"\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("roles")).unwrap();
    std::fs::create_dir_all(dir.path().join("skills/rust")).unwrap();
    std::fs::write(
        dir.path().join("skills/rust/SKILL.md"),
        "---\nname: rust\ndescription:   Work effectively in Rust.  \n---\n\n# Rust\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("roles/worker.md"),
        "---\noutput: proposes-plan\nruntime: codex\nskills: [rust]\n---\nDo it.\n",
    )
    .unwrap();
    std::fs::write(dir.path().join("playbook.md"), "# Skilled\n").unwrap();
    let mission_type = load_mission_type(dir.path(), &AuthorityCeiling::default()).expect("loads");
    let pkg = mission_type.skills.get("rust").expect("rust package");
    // Leading and trailing whitespace trimmed, inner spacing preserved.
    assert_eq!(pkg.description, "Work effectively in Rust.");
}

#[cfg(unix)]
#[test]
fn a_non_utf8_skill_package_name_is_rejected_without_panicking() {
    use std::os::unix::ffi::OsStringExt;

    let dir = tempfile::tempdir().expect("tempdir");
    write_minimal_bundle(dir.path());
    std::fs::create_dir(dir.path().join("skills")).unwrap();
    std::fs::create_dir(
        dir.path()
            .join("skills")
            .join(std::ffi::OsString::from_vec(vec![0x80])),
    )
    .unwrap();

    let error = load_mission_type(dir.path(), &AuthorityCeiling::default())
        .expect_err("skill package names must be UTF-8");
    assert!(error.to_string().contains("non-UTF-8"), "got {error:#}");
}

fn load_skill_with_description(
    description_line: Option<&str>,
) -> Result<MissionType, MissionTypeError> {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("mission.toml"),
        "[mission-type]\nname = \"skilled\"\nstop = \"verified\"\nimage = \"img\"\n\n[team]\nplanning-assignment = \"worker\"\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("skills/rust")).unwrap();
    std::fs::create_dir_all(dir.path().join("roles")).unwrap();
    std::fs::write(
        dir.path().join("roles/worker.md"),
        "---\noutput: proposes-plan\nruntime: codex\nskills: [rust]\n---\nDo it.\n",
    )
    .unwrap();
    std::fs::write(dir.path().join("playbook.md"), "# Skilled\n").unwrap();
    let description = description_line
        .map(|value| format!("description: {value}\n"))
        .unwrap_or_default();
    std::fs::write(
        dir.path().join("skills/rust/SKILL.md"),
        format!("---\nname: rust\n{description}---\n\n# Rust\n"),
    )
    .unwrap();
    load_mission_type(dir.path(), &AuthorityCeiling::default())
}

#[test]
fn skill_description_is_required() {
    assert!(load_skill_with_description(None).is_err());
}

#[test]
fn skill_description_rejects_whitespace_only() {
    assert!(load_skill_with_description(Some("   ")).is_err());
}

#[test]
fn skill_description_accepts_exactly_1024_utf8_bytes() {
    let description = "é".repeat(512);
    let mission_type = load_skill_with_description(Some(&description)).expect("1024 bytes loads");
    assert_eq!(mission_type.skills["rust"].description.len(), 1024);
}

#[test]
fn skill_description_rejects_more_than_1024_utf8_bytes() {
    let description = format!("{}a", "é".repeat(512));
    assert!(load_skill_with_description(Some(&description)).is_err());
}
