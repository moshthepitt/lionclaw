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
    approve_plan, covered_requirement, default_config, proposal, simple_plan, ParseTask, BASE_SHA,
    HEAD_SHA,
};
use lionclaw::engine::{Engine, EngineServices};
use lionclaw::mission_type::{MissionType, RoleDefinition, SkillPackage};
use lionclaw::model::{
    ArtifactOutcome, Assertion, AssertionId, Handoff, MissionConfig, OracleName, OutputSemantics,
    Plan, PlanProposal, PlanningDag, PlanningTask, RoleName, StopBar, Task, TaskKind,
};
use lionclaw::ports::{RoleRunOutcome, RoleRunRequest};
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
        handoff: Handoff::Work {
            done: true,
            report: lionclaw::model::PayloadRef::inline("done"),
            request_attention: false,
        },
        artifact: Some(ArtifactOutcome {
            base_sha: request.base_sha.clone(),
            head_sha: HEAD_SHA.to_string(),
        }),
        model_id: Some("mock".to_string()),
    }
}

/// Extract the `## Assigned skills` section from a prompt (up to the next
/// `## ` heading or end of prompt). Returns an empty string if absent.
fn assigned_skill_section(prompt: &str) -> &str {
    let start = prompt.find("## Assigned skills").map(|i| {
        // Skip past the heading line.
        let after = &prompt[i..];
        after.find('\n').map(|n| i + n + 1).unwrap_or(prompt.len())
    });
    let Some(start) = start else {
        return "";
    };
    let rest = &prompt[start..];
    // The section ends at the next `## ` heading.
    let end = rest
        .find("\n## ")
        .map(|e| start + e)
        .unwrap_or(prompt.len());
    &prompt[start..end]
}

async fn persisted_prompt(
    engine: &Engine,
    mission_id: &lionclaw::model::MissionId,
    role: &str,
) -> String {
    let events = engine.store().load(mission_id).await.expect("load events");
    let prompt = events
        .iter()
        .find_map(|envelope| match &envelope.event {
            lionclaw::model::MissionEvent::RoleRunRequested {
                role: requested_role,
                prompt,
                ..
            }
            | lionclaw::model::MissionEvent::TerminalReviewRequested {
                role: requested_role,
                prompt,
                ..
            } if requested_role.as_str() == role => Some(prompt),
            _ => None,
        })
        .unwrap_or_else(|| panic!("persisted prompt for role {role}"));
    engine
        .store()
        .blobs()
        .resolve(prompt)
        .expect("resolve prompt")
}

fn assert_no_skill_section(prompt: &str) {
    assert!(!prompt.contains("## Assigned skills"));
    assert!(!prompt.contains("LionClaw mounted"));
}

fn assert_skill_section(prompt: &str, skills: &[(&str, &str)], schema: &str) {
    assert_eq!(prompt.matches("## Assigned skills").count(), 1);
    assert_eq!(prompt.matches("LionClaw mounted").count(), 1);
    let section = assigned_skill_section(prompt);
    for (name, description) in skills {
        assert_eq!(section.matches(name).count(), 1, "skill name {name}");
        assert_eq!(
            section.matches(description).count(),
            1,
            "skill description {description}"
        );
    }
    assert!(!section.contains("lionclaw.mission."));
    assert!(!section.contains("\"schema\""));
    assert!(!section.contains("handoff.json"));
    assert_eq!(prompt.matches(schema).count(), 1);
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
                network: false,
                secrets: false,
                skills: Vec::new(),
                prompt_body: "Judge the code.".to_string(),
            },
        );
    }
    let mt = MissionType {
        name: "skill-exec-test".to_string(),
        digest: "test-digest".to_string(),
        stop: StopBar::Verified,
        image: "img".to_string(),
        planning: PlanningDag::default(),
        recovery: Default::default(),
        terminal_review: None,
        playbook: None,
        roles,
        skills: skills.clone(),
        inputs: BTreeMap::new(),
        oracles: BTreeMap::from([(
            OracleName::new("cargo-test").unwrap(),
            PathBuf::from("/nonexistent/oracles/cargo-test"),
        )]),
    };
    (mt, skills)
}

#[tokio::test]
async fn execution_prompt_lists_assigned_skills_in_declaration_order() {
    let dir = tempfile::tempdir().unwrap();
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
        .create_mission(
            dir.path().to_str().unwrap(),
            "fix the bug",
            BASE_SHA,
            default_config(),
        )
        .await
        .unwrap();
    engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&engine, &mission_id).await;
    engine.advance(&mission_id).await.unwrap();

    let prompt = persisted_prompt(&engine, &mission_id, "implementer").await;

    assert_skill_section(
        &prompt,
        &[
            ("zebra-skill", "Zebra comes first in declaration"),
            ("alpha-skill", "Alpha comes second in declaration"),
        ],
        "lionclaw.mission.work-handoff.v1",
    );
    // Declaration order: zebra before alpha (NOT BTreeMap order where alpha < zebra).
    let zebra_pos = prompt.find("zebra-skill").unwrap();
    let alpha_pos = prompt.find("alpha-skill").unwrap();
    assert!(zebra_pos < alpha_pos, "declaration order must be preserved");
    // The assigned-skill section is bounded — no handoff JSON, schema, or
    // completion-tool prose inside it.
}

#[tokio::test]
async fn execution_prompt_for_unassigned_role_has_no_skill_section() {
    let dir = tempfile::tempdir().unwrap();
    let (mission_type, _skills) = execution_mission_type(dir.path());

    let runner = MockRoleRunner::new(Box::new(move |request| {
        Ok(RoleRunOutcome {
            handoff: Handoff::Work {
                done: true,
                report: lionclaw::model::PayloadRef::inline("done"),
                request_attention: false,
            },
            artifact: Some(ArtifactOutcome {
                base_sha: request.base_sha.clone(),
                head_sha: HEAD_SHA.to_string(),
            }),
            model_id: Some("mock".to_string()),
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
        .create_mission(
            dir.path().to_str().unwrap(),
            "fix the bug",
            BASE_SHA,
            default_config(),
        )
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

    MissionType {
        name: "skill-plan-test".to_string(),
        digest: "test-digest".to_string(),
        stop: StopBar::Verified,
        image: "img".to_string(),
        planning: PlanningDag {
            tasks: vec![
                PlanningTask {
                    id: tid("strategist"),
                    role: rn("strategist"),
                    body: "draft".to_string(),
                    depends_on: vec![],
                },
                PlanningTask {
                    id: tid("author"),
                    role: rn("author"),
                    body: "propose".to_string(),
                    depends_on: vec![tid("strategist")],
                },
            ],
        },
        recovery: Default::default(),
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
    }
}

fn proposed_plan() -> PlanProposal {
    PlanProposal {
        base_revision: 0,
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
async fn planning_prompt_lists_assigned_skills_for_skilled_role() {
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
            handoff,
            artifact: None,
            model_id: None,
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
        .create_mission(
            dir.path().to_str().unwrap(),
            "plan the work",
            BASE_SHA,
            MissionConfig {
                stop: StopBar::Verified,
                planning: PlanningDag {
                    tasks: vec![
                        PlanningTask {
                            id: tid("strategist"),
                            role: rn("strategist"),
                            body: "draft".to_string(),
                            depends_on: vec![],
                        },
                        PlanningTask {
                            id: tid("author"),
                            role: rn("author"),
                            body: "propose".to_string(),
                            depends_on: vec![tid("strategist")],
                        },
                    ],
                },
                recovery: Default::default(),
                terminal_review: None,
            },
        )
        .await
        .unwrap();
    engine.advance(&mission_id).await.unwrap();

    let prompt = persisted_prompt(&engine, &mission_id, "strategist").await;
    assert_skill_section(
        &prompt,
        &[("planning-method", "A methodical planning approach")],
        "lionclaw.mission.work-handoff.v1",
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
            handoff,
            artifact: None,
            model_id: None,
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
        .create_mission(
            dir.path().to_str().unwrap(),
            "plan the work",
            BASE_SHA,
            MissionConfig {
                stop: StopBar::Verified,
                planning: PlanningDag {
                    tasks: vec![
                        PlanningTask {
                            id: tid("strategist"),
                            role: rn("strategist"),
                            body: "draft".to_string(),
                            depends_on: vec![],
                        },
                        PlanningTask {
                            id: tid("author"),
                            role: rn("author"),
                            body: "propose".to_string(),
                            depends_on: vec![tid("strategist")],
                        },
                    ],
                },
                recovery: Default::default(),
                terminal_review: None,
            },
        )
        .await
        .unwrap();
    engine.advance(&mission_id).await.unwrap();

    let prompt = persisted_prompt(&engine, &mission_id, "author").await;
    assert_no_skill_section(&prompt);
}

// ---- Terminal-review path ----

#[tokio::test]
async fn terminal_review_prompt_lists_assigned_skills() {
    let dir = tempfile::tempdir().unwrap();
    let skill_root = write_skill(dir.path());

    let mut mission_type = common::review_mission_type();
    mission_type.skills.insert(
        "gap-check".to_string(),
        SkillPackage {
            name: "gap-check".to_string(),
            root: skill_root,
            description: "Hunt gaps in the product".to_string(),
        },
    );
    mission_type
        .roles
        .get_mut(&rn("gap-reviewer"))
        .unwrap()
        .skills
        .push("gap-check".to_string());

    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.task_id.as_str() == lionclaw::engine::TERMINAL_REVIEW_TASK_TAG {
            Ok(lionclaw::testing::review_verdict(request, true, vec![]))
        } else {
            Ok(RoleRunOutcome {
                handoff: Handoff::Work {
                    done: true,
                    report: lionclaw::model::PayloadRef::inline("done"),
                    request_attention: false,
                },
                artifact: Some(ArtifactOutcome {
                    base_sha: request.base_sha.clone(),
                    head_sha: HEAD_SHA.to_string(),
                }),
                model_id: None,
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
        .create_mission(
            dir.path().to_str().unwrap(),
            "fix the tests",
            BASE_SHA,
            common::review_config(),
        )
        .await
        .unwrap();
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&h.engine, &mission_id).await;
    h.engine.advance(&mission_id).await.unwrap();

    let prompt = persisted_prompt(&h.engine, &mission_id, "gap-reviewer").await;
    assert_skill_section(
        &prompt,
        &[("gap-check", "Hunt gaps in the product")],
        "lionclaw.mission.review-handoff.v1",
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
                handoff: Handoff::Work {
                    done: true,
                    report: lionclaw::model::PayloadRef::inline("done"),
                    request_attention: false,
                },
                artifact: Some(ArtifactOutcome {
                    base_sha: request.base_sha.clone(),
                    head_sha: HEAD_SHA.to_string(),
                }),
                model_id: None,
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
        .create_mission(
            dir.path().to_str().unwrap(),
            "fix the tests",
            BASE_SHA,
            common::review_config(),
        )
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
async fn a_role_referencing_a_missing_skill_fails_closed_at_prompt_materialization() {
    let dir = tempfile::tempdir().unwrap();

    // Build a mission type where a role references a skill that is NOT in
    // the skills map — simulating a stale or corrupt mission type closure.
    let mut mission_type = common::test_mission_type();
    mission_type
        .roles
        .get_mut(&rn("implementer"))
        .unwrap()
        .skills = vec!["ghost-skill".to_string()];

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
    let mission_id = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "fix the bug",
            BASE_SHA,
            default_config(),
        )
        .await
        .unwrap();
    engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&engine, &mission_id).await;

    // Advance must error — the missing skill reference cannot be resolved
    // at prompt materialization, so the mission fails closed.
    let err = engine.advance(&mission_id).await;
    assert!(err.is_err(), "missing skill reference must fail closed");
    assert!(
        err.unwrap_err().to_string().contains("ghost-skill"),
        "error must name the missing skill"
    );
}
