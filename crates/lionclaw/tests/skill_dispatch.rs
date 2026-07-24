mod common;

use common::{
    approve_plan, harness_with_type, proposal_with_team, simple_plan, test_mission_type, BASE_SHA,
    HEAD_SHA,
};
use lionclaw::mission_type::SkillPackage;
use lionclaw::model::{Handoff, OutputSemantics, PayloadRef, RoleInstanceId, ValidationItem};
use lionclaw::ports::{CapturedArtifact, RoleTurnOutcome};
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};

#[tokio::test]
async fn engine_resolves_declared_packages_before_role_dispatch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let package_root = dir.path().join("skills/engineering");
    std::fs::create_dir_all(&package_root).unwrap();
    std::fs::write(package_root.join("SKILL.md"), "fixture").unwrap();

    let mut mission_type = test_mission_type();
    mission_type.edit_for_testing(|definition| {
        definition.skills.insert(
            "engineering".to_string(),
            SkillPackage {
                name: "engineering".to_string(),
                root: package_root.clone(),
                description: "engineering skill".to_string(),
            },
        );
        let implementer = definition
            .default_team
            .roles
            .get_mut(&RoleInstanceId::new("implementer").unwrap())
            .unwrap();
        implementer.runtime = "opencode".to_string();
        implementer.skills = vec!["engineering".to_string()];
    });
    let proposed_team = mission_type.default_team.clone();

    let expected_root = package_root.clone();
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.role.output == OutputSemantics::EmitsVerdict {
            assert!(request.skills.is_empty());
            return Ok(RoleTurnOutcome {
                handoff: Some(Handoff::Validate {
                    done: true,
                    report: PayloadRef::inline("judged"),
                    items: request
                        .assertion_ids
                        .iter()
                        .cloned()
                        .map(|item_id| ValidationItem {
                            item_id,
                            passed: true,
                        })
                        .collect(),
                    passed: true,
                    request_attention: false,
                }),
                artifact: None,
                runtime_configuration: Default::default(),
                final_response: "judged".into(),
            });
        }
        assert_eq!(request.role.runtime, "opencode");
        assert_eq!(request.skills.len(), 1);
        assert_eq!(request.skills[0].name, "engineering");
        assert_eq!(request.skills[0].root, expected_root);
        Ok(RoleTurnOutcome {
            handoff: Some(Handoff::Work {
                done: true,
                report: PayloadRef::inline("done"),
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
    let proposed = proposal_with_team(0, simple_plan(), proposed_team);
    harness
        .engine
        .propose_plan(&mission_id, proposed)
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
        2
    );
}

#[tokio::test]
async fn role_without_skills_dispatches_an_empty_package_set() {
    let dir = tempfile::tempdir().expect("tempdir");
    let runner = MockRoleRunner::new(Box::new(|request| {
        if request.role.output == OutputSemantics::EmitsVerdict {
            return Ok(RoleTurnOutcome {
                handoff: Some(Handoff::Validate {
                    done: true,
                    report: PayloadRef::inline("judged"),
                    items: request
                        .assertion_ids
                        .iter()
                        .cloned()
                        .map(|item_id| ValidationItem {
                            item_id,
                            passed: true,
                        })
                        .collect(),
                    passed: true,
                    request_attention: false,
                }),
                artifact: None,
                runtime_configuration: Default::default(),
                final_response: "judged".into(),
            });
        }
        assert_eq!(request.role.runtime, "codex");
        assert!(request.skills.is_empty());
        Ok(RoleTurnOutcome {
            handoff: Some(Handoff::Work {
                done: true,
                report: PayloadRef::inline("done"),
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
    let mission_type = test_mission_type();
    let proposed_team = mission_type.default_team.clone();
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
            "run without mission skills",
            BASE_SHA,
        )
        .await
        .unwrap();
    let proposed = proposal_with_team(0, simple_plan(), proposed_team);
    harness
        .engine
        .propose_plan(&mission_id, proposed)
        .await
        .unwrap();
    approve_plan(&harness.engine, &mission_id).await;

    harness.engine.advance(&mission_id).await.unwrap();
}
