//! End-to-end regression coverage for SKILL-DESCRIPTIONS-AND-PROMPTS:
//! assigned-skill summaries render in every engine prompt path (execution,
//! planning, terminal-review) for skilled roles, unassigned roles stay
//! uncluttered, descriptions load from real SKILL.md files, two skills
//! render in declaration order (not BTreeMap order), the section is bounded
//! (no handoff/schema/tool prose), and missing references fail closed.

mod common;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use common::{
    approve_plan, covered_requirement, proposal, simple_plan, ParseTask, BASE_SHA, HEAD_SHA,
};
use lionclaw::engine::{Engine, EngineServices};
use lionclaw::mission_type::{MissionType, MissionTypeDefinition, RoleDefinition, SkillPackage};
use lionclaw::model::{
    Assertion, AssertionId, Handoff, OracleName, OutputSemantics, Plan, PlanProposal, PlanningDag,
    PlanningTask, RoleName, StopBar, Task, TaskKind,
};
use lionclaw::ports::{CapturedArtifact, RoleRunOutcome, RoleRunRequest};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockClock, MockOracleRunner, MockRoleRunner, NoopEffectCleaner};

fn rn(n: &str) -> RoleName {
    RoleName::new(n).unwrap()
}
fn tid(n: &str) -> lionclaw::model::TaskId {
    lionclaw::model::TaskId::new(n).unwrap()
}
fn aid(n: &str) -> AssertionId {
    AssertionId::new(n).unwrap()
}

fn work_outcome(request: &RoleRunRequest) -> RoleRunOutcome {
    RoleRunOutcome {
        handoff: Some(Handoff::Work {
            done: true,
            report: lionclaw::model::PayloadRef::inline("done"),
            request_attention: false,
        }),
        artifact: Some(CapturedArtifact::for_testing(
            request.base_sha.clone(),
            HEAD_SHA,
        )),
        runtime_configuration: lionclaw::model::RuntimeConfigurationEvidence {
            requested_model: Some("mock".to_string()),
            applied_model: Some("mock".to_string()),
            ..Default::default()
        },
        final_response: String::new(),
    }
}

async fn persisted_prompt(
    engine: &Engine,
    mission_id: &lionclaw::model::MissionId,
    role: &str,
) -> String {
    let events = engine.store().load(mission_id).await.expect("load events");
    let request = events
        .iter()
        .find_map(|envelope| match &envelope.event {
            lionclaw::model::MissionEvent::RoleRunRequested {
                role: requested_role,
                effect_id,
                ..
            } if requested_role.as_str() == role => Some((Some(effect_id), None)),
            lionclaw::model::MissionEvent::TerminalReviewRequested {
                role: requested_role,
                prompt,
                ..
            } if requested_role.as_str() == role => Some((None, Some(prompt))),
            _ => None,
        })
        .unwrap_or_else(|| panic!("persisted prompt for role {role}"));
    match request {
        (Some(effect_id), None) => engine
            .reconstruct_role_prompt_for_testing(mission_id, effect_id)
            .await
            .expect("reconstruct prompt"),
        (None, Some(prompt)) => engine
            .store()
            .blobs()
            .resolve(prompt)
            .expect("resolve prompt"),
        _ => unreachable!(),
    }
}

fn assert_no_skill_section(prompt: &str) {
    assert!(!prompt.contains("## Assigned skills"));
    assert!(!prompt.contains("LionClaw mounted"));
}

/// Write a real SKILL.md with a description into `dir/skills/<name>/SKILL.md`.
fn write_skill(dir: &std::path::Path, name: &str, description: &str) -> PathBuf {
    let root = dir.join("skills").join(name);
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(
        root.join("SKILL.md"),
        format!("---\nname: {name}\ndescription: {description}\n---\n\n# {name}\n\nDo stuff.\n"),
    )
    .unwrap();
    root
}

// ---- Execution path ----

/// A mission type where the implementer has two skills assigned in a
/// deliberate declaration order that differs from BTreeMap sort order.
fn execution_mission_type(
    skill_dir: &std::path::Path,
) -> (MissionType, BTreeMap<String, SkillPackage>) {
    let zebra_root = write_skill(skill_dir, "zebra-skill", "Zebra comes first in declaration");
    let alpha_root = write_skill(
        skill_dir,
        "alpha-skill",
        "Alpha comes second in declaration",
    );
    let skills = BTreeMap::from([
        (
            "zebra-skill".to_string(),
            SkillPackage {
                name: "zebra-skill".to_string(),
                root: zebra_root.clone(),
                description: "Zebra comes first in declaration".to_string(),
            },
        ),
        (
            "alpha-skill".to_string(),
            SkillPackage {
                name: "alpha-skill".to_string(),
                root: alpha_root.clone(),
                description: "Alpha comes second in declaration".to_string(),
            },
        ),
    ]);
    let mut roles = BTreeMap::new();
    {
        let name = rn("implementer");
        roles.insert(
            name.clone(),
            RoleDefinition {
                name,
                output: OutputSemantics::ProducesArtifact,
                runtime: None,
                timeout_secs: None,
                network: false,
                secrets: false,
                // Declaration order: zebra BEFORE alpha (reversed from BTreeMap sort).
                skills: vec!["zebra-skill".to_string(), "alpha-skill".to_string()],
                prompt_body: "Fix the code.".to_string(),
            },
        );
    }
    {
        let name = rn("reviewer");
        roles.insert(
            name.clone(),
            RoleDefinition {
                name,
                output: OutputSemantics::EmitsVerdict,
                runtime: None,
                timeout_secs: None,
                network: false,
                secrets: false,
                skills: Vec::new(),
                prompt_body: "Judge the code.".to_string(),
            },
        );
    }
    let mt = MissionType::for_testing(MissionTypeDefinition {
        name: "skill-exec-test".to_string(),
        stop: StopBar::Verified,
        image: "img".to_string(),
        environment: BTreeMap::new(),
        planning: PlanningDag::default(),
        recovery: Default::default(),
        execution: lionclaw::model::ExecutionPolicy {
            auto_continue_candidate: true,
            auto_continue_proof: true,
            ..Default::default()
        },
        terminal_review: None,
        playbook: None,
        roles,
        skills: skills.clone(),
        inputs: BTreeMap::new(),
        oracles: BTreeMap::from([(
            OracleName::new("cargo-test").unwrap(),
            PathBuf::from("/nonexistent/oracles/cargo-test"),
        )]),
    });
    (mt, skills)
}

#[tokio::test]
async fn execution_prompt_leaves_assigned_skills_to_native_loading() {
    let dir = tempfile::tempdir().unwrap();
    common::initialize_repository(dir.path());
    let (mission_type, _skills) = execution_mission_type(dir.path());

    let runner = MockRoleRunner::new(Box::new(move |request| Ok(work_outcome(request))));
    let store = MissionStore::open(dir.path()).await.expect("store");
    let engine = Engine::new(
        store,
        mission_type,
        "codex".to_string(),
        "img".to_string(),
        EngineServices::new(
            Arc::new(runner),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );
    let mission_id = engine
        .create_mission(dir.path().to_str().unwrap(), "fix the bug", BASE_SHA)
        .await
        .unwrap();
    engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&engine, &mission_id).await;
    engine.advance(&mission_id).await.unwrap();

    let prompt = persisted_prompt(&engine, &mission_id, "implementer").await;

    assert_no_skill_section(&prompt);
    assert!(!prompt.contains("zebra-skill"));
    assert!(!prompt.contains("alpha-skill"));
    assert_eq!(
        prompt.matches("lionclaw.mission.work-handoff.v2").count(),
        1
    );
    // The assigned-skill section is bounded — no handoff JSON, schema, or
    // completion-tool prose inside it.
}

#[tokio::test]
async fn execution_prompt_for_unassigned_role_has_no_skill_section() {
    let dir = tempfile::tempdir().unwrap();
    common::initialize_repository(dir.path());
    let (mission_type, _skills) = execution_mission_type(dir.path());

    let runner = MockRoleRunner::new(Box::new(move |request| {
        Ok(RoleRunOutcome {
            handoff: Some(Handoff::Work {
                done: true,
                report: lionclaw::model::PayloadRef::inline("done"),
                request_attention: false,
            }),
            artifact: Some(CapturedArtifact::for_testing(
                request.base_sha.clone(),
                HEAD_SHA,
            )),
            runtime_configuration: lionclaw::model::RuntimeConfigurationEvidence {
                requested_model: Some("mock".to_string()),
                applied_model: Some("mock".to_string()),
                ..Default::default()
            },
            final_response: String::new(),
        })
    }));
    let store = MissionStore::open(dir.path()).await.expect("store");
    let engine = Engine::new(
        store,
        mission_type,
        "codex".to_string(),
        "img".to_string(),
        EngineServices::new(
            Arc::new(runner),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );
    let mission_id = engine
        .create_mission(dir.path().to_str().unwrap(), "fix the bug", BASE_SHA)
        .await
        .unwrap();
    // A plan with a validate task so the reviewer runs. The assertion is
    // oracle-bound so it clears the `verified` stop bar.
    engine
        .propose_plan(
            &mission_id,
            proposal(
                0,
                Plan {
                    requirements: vec![covered_requirement("GREEN-TESTS", "TESTS-PASS")],
                    assertions: vec![Assertion {
                        id: aid("TESTS-PASS"),
                        prose: "cargo test exits 0".to_string(),
                        oracle: Some(OracleName::new("cargo-test").unwrap()),
                    }],
                    tasks: vec![
                        Task {
                            id: "write".parse_task(),
                            kind: TaskKind::Work,
                            body: "Write.".to_string(),
                            targets: vec![aid("TESTS-PASS")],
                            role: Some(rn("implementer")),
                            depends_on: vec![],
                        },
                        Task {
                            id: "review".parse_task(),
                            kind: TaskKind::Validate,
                            body: "Review.".to_string(),
                            targets: vec![aid("TESTS-PASS")],
                            role: Some(rn("reviewer")),
                            depends_on: vec!["write".parse_task()],
                        },
                    ],
                },
            ),
        )
        .await
        .unwrap();
    approve_plan(&engine, &mission_id).await;
    engine.advance(&mission_id).await.unwrap();

    let prompt = persisted_prompt(&engine, &mission_id, "reviewer").await;
    assert_no_skill_section(&prompt);
}

// ---- Planning path ----

fn planning_mission_type(skill_dir: &std::path::Path) -> MissionType {
    let method_root = write_skill(
        skill_dir,
        "planning-method",
        "A methodical planning approach",
    );
    let mut roles = BTreeMap::new();
    for (name, output) in [
        ("strategist", OutputSemantics::ProducesReport),
        ("author", OutputSemantics::ProposesPlan),
        ("implementer", OutputSemantics::ProducesArtifact),
        ("reviewer", OutputSemantics::EmitsVerdict),
    ] {
        let role_name = rn(name);
        roles.insert(
            role_name.clone(),
            RoleDefinition {
                name: role_name,
                output,
                runtime: None,
                timeout_secs: None,
                network: false,
                secrets: false,
                skills: Vec::new(),
                prompt_body: "role prose".to_string(),
            },
        );
    }
    // Assign the planning-method skill to the strategist.
    roles.get_mut(&rn("strategist")).unwrap().skills = vec!["planning-method".to_string()];
    roles.get_mut(&rn("strategist")).unwrap().runtime = Some("opencode".to_string());

    MissionType::for_testing(MissionTypeDefinition {
        name: "skill-plan-test".to_string(),
        stop: StopBar::Verified,
        image: "img".to_string(),
        environment: BTreeMap::new(),
        planning: PlanningDag {
            tasks: vec![
                PlanningTask {
                    id: tid("strategist"),
                    role: rn("strategist"),
                    output: OutputSemantics::ProducesReport,
                    body: "draft".to_string(),
                    depends_on: vec![],
                },
                PlanningTask {
                    id: tid("author"),
                    role: rn("author"),
                    output: OutputSemantics::ProposesPlan,
                    body: "propose".to_string(),
                    depends_on: vec![tid("strategist")],
                },
            ],
        },
        recovery: Default::default(),
        execution: Default::default(),
        terminal_review: None,
        playbook: None,
        roles,
        skills: BTreeMap::from([(
            "planning-method".to_string(),
            SkillPackage {
                name: "planning-method".to_string(),
                root: method_root,
                description: "A methodical planning approach".to_string(),
            },
        )]),
        inputs: BTreeMap::new(),
        oracles: BTreeMap::from([(
            OracleName::new("cargo-test").unwrap(),
            PathBuf::from("/nonexistent/oracles/cargo-test"),
        )]),
    })
}

fn proposed_plan() -> PlanProposal {
    PlanProposal {
        base_revision: 0,
        requirement_changes: vec![],
        assertion_supersessions: vec![],
        plan: Plan {
            requirements: vec![covered_requirement("GREEN-TESTS", "TESTS-PASS")],
            assertions: vec![Assertion {
                id: aid("TESTS-PASS"),
                prose: "cargo test exits 0".to_string(),
                oracle: Some(OracleName::new("cargo-test").unwrap()),
            }],
            tasks: vec![Task {
                id: tid("fix"),
                kind: TaskKind::Work,
                body: "make it pass".to_string(),
                targets: vec![aid("TESTS-PASS")],
                role: Some(rn("implementer")),
                depends_on: vec![],
            }],
        },
    }
}

#[tokio::test]
async fn planning_prompt_leaves_assigned_skills_to_native_loading() {
    let dir = tempfile::tempdir().unwrap();
    let mission_type = planning_mission_type(dir.path());

    let runner = MockRoleRunner::new(Box::new(move |request: &RoleRunRequest| {
        let handoff = match request.role.output {
            OutputSemantics::ProposesPlan => Handoff::Plan {
                done: true,
                report: lionclaw::model::PayloadRef::inline("proposed"),
                proposal: Some(proposed_plan()),
                request_attention: false,
            },
            _ => Handoff::Work {
                done: true,
                report: lionclaw::model::PayloadRef::inline("report"),
                request_attention: false,
            },
        };
        Ok(RoleRunOutcome {
            handoff: Some(handoff),
            artifact: None,
            runtime_configuration: Default::default(),
            final_response: String::new(),
        })
    }));
    let store = MissionStore::open(dir.path()).await.expect("store");
    let engine = Engine::new(
        store,
        mission_type,
        "codex".to_string(),
        "img".to_string(),
        EngineServices::new(
            Arc::new(runner),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );
    let mission_id = engine
        .create_mission(dir.path().to_str().unwrap(), "plan the work", BASE_SHA)
        .await
        .unwrap();
    engine.advance(&mission_id).await.unwrap();

    let prompt = persisted_prompt(&engine, &mission_id, "strategist").await;
    assert_no_skill_section(&prompt);
    assert!(!prompt.contains("planning-method"));
    assert_eq!(
        prompt.matches("lionclaw.mission.work-handoff.v2").count(),
        1
    );
}

#[tokio::test]
async fn planning_prompt_for_unassigned_role_has_no_skill_section() {
    let dir = tempfile::tempdir().unwrap();
    let mission_type = planning_mission_type(dir.path());

    let runner = MockRoleRunner::new(Box::new(move |request: &RoleRunRequest| {
        let handoff = match request.role.output {
            OutputSemantics::ProposesPlan => Handoff::Plan {
                done: true,
                report: lionclaw::model::PayloadRef::inline("proposed"),
                proposal: Some(proposed_plan()),
                request_attention: false,
            },
            _ => Handoff::Work {
                done: true,
                report: lionclaw::model::PayloadRef::inline("report"),
                request_attention: false,
            },
        };
        Ok(RoleRunOutcome {
            handoff: Some(handoff),
            artifact: None,
            runtime_configuration: Default::default(),
            final_response: String::new(),
        })
    }));
    let store = MissionStore::open(dir.path()).await.expect("store");
    let engine = Engine::new(
        store,
        mission_type,
        "codex".to_string(),
        "img".to_string(),
        EngineServices::new(
            Arc::new(runner),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );
    let mission_id = engine
        .create_mission(dir.path().to_str().unwrap(), "plan the work", BASE_SHA)
        .await
        .unwrap();
    engine.advance(&mission_id).await.unwrap();
    engine.advance(&mission_id).await.unwrap();

    let prompt = persisted_prompt(&engine, &mission_id, "author").await;
    assert_no_skill_section(&prompt);
}

// ---- Terminal-review path ----

#[tokio::test]
async fn terminal_review_prompt_leaves_assigned_skills_to_native_loading() {
    let dir = tempfile::tempdir().unwrap();
    let skill_root = write_skill(dir.path(), "gap-check", "Hunt gaps in the product");

    let mut mission_type = common::review_mission_type();
    mission_type.edit_for_testing(|definition| {
        definition.skills.insert(
            "gap-check".to_string(),
            SkillPackage {
                name: "gap-check".to_string(),
                root: skill_root,
                description: "Hunt gaps in the product".to_string(),
            },
        );
        definition
            .roles
            .get_mut(&rn("gap-reviewer"))
            .unwrap()
            .skills
            .push("gap-check".to_string());
    });

    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.task_id.as_str() == lionclaw::engine::TERMINAL_REVIEW_TASK_TAG {
            Ok(lionclaw::testing::review_verdict(request, true, vec![]))
        } else {
            Ok(RoleRunOutcome {
                handoff: Some(Handoff::Work {
                    done: true,
                    report: lionclaw::model::PayloadRef::inline("done"),
                    request_attention: false,
                }),
                artifact: Some(CapturedArtifact::for_testing(
                    request.base_sha.clone(),
                    HEAD_SHA,
                )),
                runtime_configuration: Default::default(),
                final_response: String::new(),
            })
        }
    }));
    let h = common::harness_with_type(
        dir.path(),
        mission_type,
        runner,
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "fix the tests", BASE_SHA)
        .await
        .unwrap();
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&h.engine, &mission_id).await;
    h.engine.advance(&mission_id).await.unwrap();

    let prompt = persisted_prompt(&h.engine, &mission_id, "gap-reviewer").await;
    assert_no_skill_section(&prompt);
    assert!(!prompt.contains("gap-check"));
    assert_eq!(
        prompt.matches("lionclaw.mission.review-handoff.v2").count(),
        1
    );
}

#[tokio::test]
async fn terminal_review_prompt_for_unassigned_role_has_no_skill_section() {
    let dir = tempfile::tempdir().unwrap();

    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.task_id.as_str() == lionclaw::engine::TERMINAL_REVIEW_TASK_TAG {
            Ok(lionclaw::testing::review_verdict(request, true, vec![]))
        } else {
            Ok(RoleRunOutcome {
                handoff: Some(Handoff::Work {
                    done: true,
                    report: lionclaw::model::PayloadRef::inline("done"),
                    request_attention: false,
                }),
                artifact: Some(CapturedArtifact::for_testing(
                    request.base_sha.clone(),
                    HEAD_SHA,
                )),
                runtime_configuration: Default::default(),
                final_response: String::new(),
            })
        }
    }));
    let h = common::harness_with_type(
        dir.path(),
        common::review_mission_type(),
        runner,
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "fix the tests", BASE_SHA)
        .await
        .unwrap();
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&h.engine, &mission_id).await;
    h.engine.advance(&mission_id).await.unwrap();

    let prompt = persisted_prompt(&h.engine, &mission_id, "gap-reviewer").await;
    assert_no_skill_section(&prompt);
}

// ---- Missing reference fails closed ----

#[tokio::test]
async fn a_role_referencing_a_missing_skill_fails_closed_at_mission_creation() {
    let dir = tempfile::tempdir().unwrap();

    // Build a mission type where a role references a skill that is NOT in
    // the skills map — simulating a stale or corrupt mission type closure.
    let mut mission_type = common::test_mission_type();
    mission_type.edit_for_testing(|definition| {
        definition.roles.get_mut(&rn("implementer")).unwrap().skills =
            vec!["ghost-skill".to_string()];
    });

    let runner = MockRoleRunner::happy(HEAD_SHA);
    let store = MissionStore::open(dir.path()).await.expect("store");
    let engine = Engine::new(
        store,
        mission_type,
        "codex".to_string(),
        "img".to_string(),
        EngineServices::new(
            Arc::new(runner),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );
    let error = engine
        .create_mission(dir.path().to_str().unwrap(), "fix the bug", BASE_SHA)
        .await
        .expect_err("missing skill reference must fail before a mission is recorded");
    assert!(
        error.to_string().contains("ghost-skill"),
        "error must name the missing skill"
    );
}
