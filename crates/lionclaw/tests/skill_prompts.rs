//! End-to-end proof that native runtime skills stay out of every kernel-owned
//! prompt path while stale role skill references fail closed.

mod common;

use std::path::PathBuf;

use common::{
    approve_plan, proposal_with_team, review_proposal, review_runner, simple_plan, BASE_SHA,
};
use lionclaw::mission_type::{MissionType, SkillPackage};
use lionclaw::model::{Handoff, OutputSemantics, PayloadRef, RoleInstanceId};
use lionclaw::ports::{RoleTurnOutcome, RoleTurnRequest};
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};

fn role(name: &str) -> RoleInstanceId {
    RoleInstanceId::new(name).unwrap()
}

fn write_skill(dir: &std::path::Path, name: &str, description: &str) -> PathBuf {
    let root = dir.join("skills").join(name);
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(
        root.join("SKILL.md"),
        format!("---\nname: {name}\ndescription: {description}\n---\n\n# {name}\n"),
    )
    .unwrap();
    root
}

fn mission_type_with_skill(
    dir: &std::path::Path,
    reviewed: bool,
    assigned_role: Option<&str>,
) -> MissionType {
    let root = write_skill(dir, "test-method", "A bounded native method");
    let mut mission_type = if reviewed {
        common::review_mission_type()
    } else {
        common::test_mission_type()
    };
    mission_type.edit_for_testing(|definition| {
        definition.skills.insert(
            "test-method".to_string(),
            SkillPackage {
                name: "test-method".to_string(),
                root,
                description: "A bounded native method".to_string(),
            },
        );
        if let Some(assigned_role) = assigned_role {
            definition
                .default_team
                .roles
                .get_mut(&role(assigned_role))
                .unwrap()
                .skills
                .push("test-method".to_string());
        }
    });
    mission_type
}

async fn persisted_prompt(
    engine: &lionclaw::engine::Engine,
    mission_id: &lionclaw::model::MissionId,
    expected_role: &str,
) -> String {
    let events = engine.store().load(mission_id).await.unwrap();
    let effect_id = events
        .iter()
        .find_map(|event| match &event.event {
            lionclaw::model::MissionEvent::RoleTurnRequested {
                role_instance,
                effect_id,
                ..
            } if role_instance.as_str() == expected_role => Some(effect_id),
            _ => None,
        })
        .unwrap_or_else(|| panic!("persisted prompt for {expected_role}"));
    engine
        .reconstruct_role_prompt_for_testing(mission_id, effect_id)
        .await
        .unwrap()
}

fn assert_no_skill_section(prompt: &str) {
    assert!(!prompt.contains("## Assigned skills"));
    assert!(!prompt.contains("LionClaw mounted"));
    assert!(!prompt.contains("A bounded native method"));
}

fn planning_runner(proposal: lionclaw::model::MissionProposal) -> MockRoleRunner {
    MockRoleRunner::new(Box::new(move |request: &RoleTurnRequest| {
        assert_eq!(request.role.output, OutputSemantics::ProposesPlan);
        Ok(RoleTurnOutcome {
            handoff: Some(Handoff::Plan {
                done: true,
                report: PayloadRef::inline("proposed"),
                proposal: Some(Box::new(proposal.clone())),
                request_attention: false,
            }),
            artifact: None,
            runtime_configuration: Default::default(),
            runtime_usage: Default::default(),
            final_response: "proposed".to_string(),
        })
    }))
}

async fn execution_prompt(dir: &std::path::Path, assigned: bool) -> String {
    let mission_type = mission_type_with_skill(dir, false, assigned.then_some("implementer"));
    let next_team = mission_type.default_team.clone();
    let harness = common::harness_with_type(
        dir,
        mission_type,
        review_runner(vec![]),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = harness
        .engine
        .create_mission(dir.to_str().unwrap(), "fix the bug", BASE_SHA)
        .await
        .unwrap();
    harness
        .engine
        .propose_plan(&mission_id, proposal_with_team(0, simple_plan(), next_team))
        .await
        .unwrap();
    approve_plan(&harness.engine, &mission_id).await;
    harness.engine.advance(&mission_id).await.unwrap();
    persisted_prompt(&harness.engine, &mission_id, "implementer").await
}

async fn planning_prompt(dir: &std::path::Path, assigned: bool) -> String {
    let mission_type = mission_type_with_skill(dir, false, assigned.then_some("strategist"));
    let proposal = proposal_with_team(0, simple_plan(), mission_type.default_team.clone());
    let harness = common::harness_with_type(
        dir,
        mission_type,
        planning_runner(proposal),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = harness
        .engine
        .create_mission(dir.to_str().unwrap(), "plan the work", BASE_SHA)
        .await
        .unwrap();
    harness.engine.advance(&mission_id).await.unwrap();
    persisted_prompt(&harness.engine, &mission_id, "strategist").await
}

async fn gap_review_prompt(dir: &std::path::Path, assigned: bool) -> String {
    let mission_type = mission_type_with_skill(dir, true, assigned.then_some("gap-reviewer"));
    let harness = common::harness_with_type(
        dir,
        mission_type,
        review_runner(vec![(true, vec![])]),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = harness
        .engine
        .create_mission(dir.to_str().unwrap(), "fix the tests", BASE_SHA)
        .await
        .unwrap();
    harness
        .engine
        .propose_plan(&mission_id, review_proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&harness.engine, &mission_id).await;
    harness.engine.advance(&mission_id).await.unwrap();
    persisted_prompt(&harness.engine, &mission_id, "gap-reviewer").await
}

#[tokio::test]
async fn execution_prompt_leaves_assigned_skills_to_native_loading() {
    let dir = tempfile::tempdir().unwrap();
    let prompt = execution_prompt(dir.path(), true).await;
    assert_no_skill_section(&prompt);
    assert!(!prompt.contains("test-method"));
    assert_eq!(
        prompt.matches("lionclaw.mission.work-handoff.v2").count(),
        1
    );
}

#[tokio::test]
async fn execution_prompt_for_unassigned_role_has_no_skill_section() {
    let dir = tempfile::tempdir().unwrap();
    assert_no_skill_section(&execution_prompt(dir.path(), false).await);
}

#[tokio::test]
async fn planning_prompt_leaves_assigned_skills_to_native_loading() {
    let dir = tempfile::tempdir().unwrap();
    let prompt = planning_prompt(dir.path(), true).await;
    assert_no_skill_section(&prompt);
    assert_eq!(prompt.matches("test-method").count(), 1);
    assert_eq!(
        prompt.matches("lionclaw.mission.plan-handoff.v2").count(),
        1
    );
}

#[tokio::test]
async fn planning_prompt_for_unassigned_role_has_no_skill_section() {
    let dir = tempfile::tempdir().unwrap();
    assert_no_skill_section(&planning_prompt(dir.path(), false).await);
}

#[tokio::test]
async fn terminal_review_prompt_leaves_assigned_skills_to_native_loading() {
    let dir = tempfile::tempdir().unwrap();
    let prompt = gap_review_prompt(dir.path(), true).await;
    assert_no_skill_section(&prompt);
    assert!(!prompt.contains("test-method"));
    assert_eq!(
        prompt.matches("lionclaw.mission.review-handoff.v2").count(),
        1
    );
}

#[tokio::test]
async fn terminal_review_prompt_for_unassigned_role_has_no_skill_section() {
    let dir = tempfile::tempdir().unwrap();
    assert_no_skill_section(&gap_review_prompt(dir.path(), false).await);
}

#[tokio::test]
async fn a_role_referencing_a_missing_skill_fails_closed_at_mission_creation() {
    let dir = tempfile::tempdir().unwrap();
    let mut mission_type = common::test_mission_type();
    mission_type.edit_for_testing(|definition| {
        definition
            .default_team
            .roles
            .get_mut(&role("implementer"))
            .unwrap()
            .skills = vec!["ghost-skill".to_string()];
    });
    let harness = common::harness_with_type(
        dir.path(),
        mission_type,
        MockRoleRunner::happy(common::HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let error = harness
        .engine
        .create_mission(dir.path().to_str().unwrap(), "fix the bug", BASE_SHA)
        .await
        .expect_err("missing skill reference must fail before mission creation");
    assert!(error.to_string().contains("ghost-skill"));
}
