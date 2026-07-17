mod common;

use common::{
    approve_plan, harness_with_type, proposal, simple_plan, test_mission_type, BASE_SHA, HEAD_SHA,
};
use lionclaw::mission_type::SkillPackage;
use lionclaw::model::{ArtifactOutcome, Handoff, PayloadRef, RoleName};
use lionclaw::ports::RoleRunOutcome;
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};

#[tokio::test]
async fn engine_resolves_declared_packages_before_role_dispatch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let package_root = dir.path().join("skills/engineering");
    std::fs::create_dir_all(&package_root).unwrap();
    std::fs::write(package_root.join("SKILL.md"), "fixture").unwrap();

    let mut mission_type = test_mission_type();
    mission_type.skills.insert(
        "engineering".to_string(),
        SkillPackage {
            name: "engineering".to_string(),
            root: package_root.clone(),
            description: "engineering skill".to_string(),
        },
    );
    let implementer = mission_type
        .roles
        .get_mut(&RoleName::new("implementer").unwrap())
        .unwrap();
    implementer.runtime = Some("opencode".to_string());
    implementer.skills = vec!["engineering".to_string()];

    let expected_root = package_root.clone();
    let runner = MockRoleRunner::new(Box::new(move |request| {
        assert_eq!(request.runtime, "opencode");
        assert_eq!(request.skills.len(), 1);
        assert_eq!(request.skills[0].name, "engineering");
        assert_eq!(request.skills[0].root, expected_root);
        Ok(RoleRunOutcome {
            handoff: Handoff::Work {
                done: true,
                report: PayloadRef::inline("done"),
                request_attention: false,
            },
            artifact: Some(ArtifactOutcome {
                base_sha: request.base_sha.clone(),
                head_sha: HEAD_SHA.to_string(),
            }),
            runtime_configuration: lionclaw::model::RuntimeConfigurationEvidence {
                requested_model: Some("mock".to_string()),
                applied_model: Some("mock".to_string()),
                ..Default::default()
            },
            final_response: String::new(),
        })
    }));
    let harness = harness_with_type(
        dir.path(),
        mission_type,
        runner,
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = harness
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "use the configured skill",
            BASE_SHA,
        )
        .await
        .unwrap();
    harness
        .engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&harness.engine, &mission_id).await;

    harness.engine.advance(&mission_id).await.unwrap();
    assert_eq!(
        harness
            .role_runner
            .invocations_by_key
            .lock()
            .unwrap()
            .values()
            .sum::<u32>(),
        1
    );
}

#[tokio::test]
async fn role_without_skills_dispatches_an_empty_package_set() {
    let dir = tempfile::tempdir().expect("tempdir");
    let runner = MockRoleRunner::new(Box::new(|request| {
        assert_eq!(request.runtime, "codex");
        assert!(request.skills.is_empty());
        Ok(RoleRunOutcome {
            handoff: Handoff::Work {
                done: true,
                report: PayloadRef::inline("done"),
                request_attention: false,
            },
            artifact: Some(ArtifactOutcome {
                base_sha: request.base_sha.clone(),
                head_sha: HEAD_SHA.to_string(),
            }),
            runtime_configuration: lionclaw::model::RuntimeConfigurationEvidence {
                requested_model: Some("mock".to_string()),
                applied_model: Some("mock".to_string()),
                ..Default::default()
            },
            final_response: String::new(),
        })
    }));
    let harness = harness_with_type(
        dir.path(),
        test_mission_type(),
        runner,
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = harness
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "run without mission skills",
            BASE_SHA,
        )
        .await
        .unwrap();
    harness
        .engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&harness.engine, &mission_id).await;

    harness.engine.advance(&mission_id).await.unwrap();
}
