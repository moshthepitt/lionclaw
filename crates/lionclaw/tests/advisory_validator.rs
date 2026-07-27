//! Team judgment remains advisory: it routes work and contributes evidence,
//! but only an oracle can mint authoritative verification.

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use common::{advisory_plan, approve_plan, proposal, ParseTask, BASE_SHA, HEAD_SHA};
use lionclaw::engine::Engine;
use lionclaw::mission_type::SkillSource;
use lionclaw::model::{
    AdvisoryStatus, Assertion, AssertionId, DecisionAction, FinishClass, Handoff, MissionPhase,
    MissionProposal, MissionSkill, OutputSemantics, PayloadRef, Plan, RoleAttemptDisposition,
    RoleEffectSource, RoleInstanceId, RuntimeInstrumentIdentity, Task, ValidationItem,
};
use lionclaw::ports::{CapturedArtifact, RoleTurnOutcome, RoleTurnRequest};
use lionclaw::store::NewEvent;
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};

fn role_aware_runner(reviewer_passes: bool) -> MockRoleRunner {
    MockRoleRunner::new(Box::new(move |request: &RoleTurnRequest| {
        match request.role.output {
            OutputSemantics::ProducesArtifact => Ok(RoleTurnOutcome {
                handoff: Some(Handoff::Work {
                    done: true,
                    report: PayloadRef::inline("wrote it"),
                    request_attention: false,
                }),
                artifact: Some(CapturedArtifact::for_testing(
                    request.base_sha.clone(),
                    HEAD_SHA,
                )),
                prepared_inputs: Vec::new(),
                runtime_configuration: Default::default(),
                runtime_usage: Default::default(),
                final_response: "wrote it".to_string(),
            }),
            OutputSemantics::EmitsVerdict => Ok(RoleTurnOutcome {
                handoff: Some(Handoff::Validate {
                    done: true,
                    report: PayloadRef::inline("reviewed"),
                    items: request
                        .assertion_ids
                        .iter()
                        .cloned()
                        .map(|item_id| ValidationItem {
                            item_id,
                            passed: reviewer_passes,
                        })
                        .collect(),
                    passed: reviewer_passes,
                    request_attention: false,
                }),
                artifact: None,
                prepared_inputs: Vec::new(),
                runtime_configuration: Default::default(),
                runtime_usage: Default::default(),
                final_response: "reviewed".to_string(),
            }),
            other => panic!("unexpected output {other:?}"),
        }
    }))
}

async fn run(reviewer_passes: bool) -> lionclaw::model::MissionState {
    let dir = tempfile::tempdir().unwrap();
    let mut mission_type = common::test_mission_type();
    mission_type
        .edit_for_testing(|definition| definition.stop = lionclaw::model::StopBar::Attested);
    let harness = common::harness_with_type(
        dir.path(),
        mission_type,
        role_aware_runner(reviewer_passes),
        MockOracleRunner::exiting(0),
    )
    .await;
    let id = harness
        .engine
        .create_mission(dir.path().to_str().unwrap(), "advisory mission", BASE_SHA)
        .await
        .unwrap();
    harness
        .engine
        .propose_plan(&id, proposal(0, advisory_plan()))
        .await
        .unwrap();
    approve_plan(&harness.engine, &id).await;
    harness.engine.advance(&id).await.unwrap().state
}

fn runtime_identities(codex_model: &str) -> BTreeMap<String, RuntimeInstrumentIdentity> {
    let mut identities = common::default_runtime_identities();
    identities.insert(
        "codex".to_string(),
        RuntimeInstrumentIdentity {
            runtime: "codex".to_string(),
            model: Some(codex_model.to_string()),
            mode: None,
        },
    );
    identities
}

fn plan_with_oracle_obligation(mut plan: Plan) -> Plan {
    let tests_pass = AssertionId::new("TESTS-PASS").unwrap();
    plan.requirements.push(common::covered_requirement(
        "GREEN-TESTS",
        tests_pass.as_str(),
    ));
    plan.assertions.push(Assertion {
        id: tests_pass.clone(),
        prose: "cargo test exits 0".to_string(),
        oracle: Some(lionclaw::model::OracleName::new("cargo-test").unwrap()),
    });
    plan.tasks[0].targets.push(tests_pass);
    plan
}

fn two_reviewer_plan_with_oracle_obligation() -> Plan {
    let style = AssertionId::new("STYLE-OK").unwrap();
    let docs = AssertionId::new("DOCS-OK").unwrap();
    let tests = AssertionId::new("TESTS-PASS").unwrap();
    Plan {
        requirements: vec![
            common::reviewer_checkable_requirement("READABLE-CODE", style.as_str()),
            common::reviewer_checkable_requirement("DOCS-CLEAR", docs.as_str()),
            common::covered_requirement("GREEN-TESTS", tests.as_str()),
        ],
        assertions: vec![
            Assertion {
                id: style.clone(),
                prose: "the code reads cleanly".to_string(),
                oracle: None,
            },
            Assertion {
                id: docs.clone(),
                prose: "the docs are clear".to_string(),
                oracle: None,
            },
            Assertion {
                id: tests.clone(),
                prose: "cargo test exits 0".to_string(),
                oracle: Some(lionclaw::model::OracleName::new("cargo-test").unwrap()),
            },
        ],
        tasks: vec![Task {
            id: "write".parse_task(),
            body: "Write the code and docs.".to_string(),
            targets: vec![style, docs, tests],
            depends_on: Vec::new(),
        }],
    }
}

fn work_outcome(request: &RoleTurnRequest) -> RoleTurnOutcome {
    RoleTurnOutcome {
        handoff: Some(Handoff::Work {
            done: true,
            report: PayloadRef::inline("wrote it"),
            request_attention: false,
        }),
        artifact: Some(CapturedArtifact::for_testing(
            request.base_sha.clone(),
            HEAD_SHA,
        )),
        prepared_inputs: Vec::new(),
        runtime_configuration: Default::default(),
        runtime_usage: Default::default(),
        final_response: "wrote it".to_string(),
    }
}

fn validate_outcome(request: &RoleTurnRequest, report: &str) -> RoleTurnOutcome {
    RoleTurnOutcome {
        handoff: Some(Handoff::Validate {
            done: true,
            report: PayloadRef::inline(report),
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
        prepared_inputs: Vec::new(),
        runtime_configuration: Default::default(),
        runtime_usage: Default::default(),
        final_response: report.to_string(),
    }
}

fn oracle_attention_id(state: &lionclaw::model::MissionState) -> String {
    state
        .open_attention
        .values()
        .find(|item| item.kind == lionclaw::model::AttentionKind::OracleVerdictFailed)
        .expect("oracle verdict attention")
        .id
        .clone()
}

fn write_skill_package(root: &Path, name: &str, description: &str, body: &str) {
    let package = root.join(name);
    std::fs::create_dir_all(&package).expect("create skill package");
    std::fs::write(
        package.join("SKILL.md"),
        format!("---\nname: {name}\ndescription: {description}\n---\n{body}\n"),
    )
    .expect("write skill package");
}

async fn install_mission_skill(
    engine: &Engine,
    mission_id: &lionclaw::model::MissionId,
    source: &Path,
) -> MissionSkill {
    let (_, package) = lionclaw::mission_type::add_mission_skill(
        &mission_skills_dir(engine, mission_id),
        SkillSource::Path(source.to_path_buf()),
    )
    .await
    .expect("install mission skill package");
    MissionSkill {
        name: package.name,
        digest: package.digest,
        description: package.description,
    }
}

fn mission_skills_dir(engine: &Engine, mission_id: &lionclaw::model::MissionId) -> PathBuf {
    engine
        .store()
        .lionclaw_dir()
        .join("missions")
        .join(mission_id.as_str())
        .join("skills")
}

#[tokio::test]
async fn advisory_pass_is_attested_never_verified() {
    let state = run(true).await;
    let assertion = lionclaw::model::AssertionId::new("STYLE-OK").unwrap();
    assert_eq!(state.advisory_status(&assertion), AdvisoryStatus::Passed);
    assert_eq!(
        state.phase,
        MissionPhase::Done {
            finish: FinishClass::Attested
        }
    );
}

#[tokio::test]
async fn advisory_receipt_is_stale_after_environment_digest_change() {
    let state = run(true).await;
    let assertion = lionclaw::model::AssertionId::new("STYLE-OK").unwrap();
    assert_eq!(state.advisory_status(&assertion), AdvisoryStatus::Passed);

    let mut changed_environment = state.clone();
    changed_environment.image_id = format!("sha256:{}", "b".repeat(64));

    assert_eq!(
        changed_environment.advisory_status(&assertion),
        AdvisoryStatus::Pending
    );
}

#[tokio::test]
async fn judged_receipt_reopens_after_model_identity_change() {
    let dir = tempfile::tempdir().unwrap();
    let reviewer_attempts = Arc::new(AtomicUsize::new(0));
    let runner = MockRoleRunner::new(Box::new({
        let reviewer_attempts = reviewer_attempts.clone();
        move |request: &RoleTurnRequest| match request.role.output {
            OutputSemantics::ProducesArtifact => Ok(work_outcome(request)),
            OutputSemantics::EmitsVerdict => {
                reviewer_attempts.fetch_add(1, Ordering::SeqCst);
                Ok(validate_outcome(request, "reviewed under model identity"))
            }
            other => panic!("unexpected output {other:?}"),
        }
    }));
    let mut mission_type = common::test_mission_type();
    mission_type
        .edit_for_testing(|definition| definition.stop = lionclaw::model::StopBar::Attested);
    let reopened_type = mission_type.clone();
    let harness = common::harness_with_type_and_runtime_identities(
        dir.path(),
        mission_type,
        runner,
        MockOracleRunner::exiting(1),
        runtime_identities("model-a"),
    )
    .await;
    let id = harness
        .engine
        .create_mission(dir.path().to_str().unwrap(), "model freshness", BASE_SHA)
        .await
        .unwrap();
    harness
        .engine
        .propose_plan(
            &id,
            proposal(0, plan_with_oracle_obligation(advisory_plan())),
        )
        .await
        .unwrap();
    approve_plan(&harness.engine, &id).await;
    let proven = harness.engine.advance(&id).await.unwrap().state;
    let assertion = AssertionId::new("STYLE-OK").unwrap();
    let reviewer = RoleInstanceId::new("reviewer").unwrap();
    let original_effect = proven.contract[&assertion].last_advisory[&reviewer].clone();
    assert_eq!(proven.advisory_status(&assertion), AdvisoryStatus::Passed);
    assert_eq!(reviewer_attempts.load(Ordering::SeqCst), 1);

    let changed_engine = common::engine_with_runtime_identities(
        dir.path(),
        reopened_type,
        harness.role_runner.clone(),
        harness.oracle_runner.clone(),
        runtime_identities("model-b"),
    )
    .await;
    let mut changed_team = proven.team.clone().unwrap();
    changed_team.revision += 1;
    changed_engine
        .configure_team(&id, changed_team)
        .await
        .unwrap();
    let pending = changed_engine.load_state(&id).await.unwrap();
    assert_eq!(pending.advisory_status(&assertion), AdvisoryStatus::Pending);

    let attention_id = oracle_attention_id(&pending);
    changed_engine
        .decide(
            &id,
            &attention_id,
            DecisionAction::Repair,
            "rerun under changed model",
        )
        .await
        .unwrap();
    let reproved = changed_engine.advance(&id).await.unwrap().state;
    let new_effect = reproved.contract[&assertion].last_advisory[&reviewer].clone();
    assert_ne!(new_effect, original_effect);
    assert_eq!(reviewer_attempts.load(Ordering::SeqCst), 2);
    assert_eq!(reproved.advisory_status(&assertion), AdvisoryStatus::Passed);
}

#[tokio::test]
async fn skill_added_reopens_only_receipts_judged_with_that_skill() {
    let dir = tempfile::tempdir().unwrap();
    let skilled_attempts = Arc::new(AtomicUsize::new(0));
    let plain_attempts = Arc::new(AtomicUsize::new(0));
    let runner = MockRoleRunner::new(Box::new({
        let skilled_attempts = skilled_attempts.clone();
        let plain_attempts = plain_attempts.clone();
        move |request: &RoleTurnRequest| match request.role.output {
            OutputSemantics::ProducesArtifact => Ok(work_outcome(request)),
            OutputSemantics::EmitsVerdict => {
                if request.role.id.as_str() == "reviewer" {
                    assert_eq!(request.skills.len(), 1);
                    assert_eq!(request.skills[0].name, "rubric");
                    skilled_attempts.fetch_add(1, Ordering::SeqCst);
                    Ok(validate_outcome(request, "skilled review"))
                } else if request.role.id.as_str() == "reviewer-plain" {
                    assert!(request.skills.is_empty());
                    plain_attempts.fetch_add(1, Ordering::SeqCst);
                    Ok(validate_outcome(request, "plain review"))
                } else {
                    panic!("unexpected verdict role {}", request.role.id)
                }
            }
            other => panic!("unexpected output {other:?}"),
        }
    }));
    let mut mission_type = common::test_mission_type();
    mission_type
        .edit_for_testing(|definition| definition.stop = lionclaw::model::StopBar::Attested);
    let harness = common::harness_with_type(
        dir.path(),
        mission_type,
        runner,
        MockOracleRunner::exiting(1),
    )
    .await;
    let id = harness
        .engine
        .create_mission(dir.path().to_str().unwrap(), "skill freshness", BASE_SHA)
        .await
        .unwrap();

    let skill_sources = dir.path().join("skill-sources");
    write_skill_package(&skill_sources, "rubric", "Rubric v1", "Check style.");
    let skill_v1 = install_mission_skill(&harness.engine, &id, &skill_sources.join("rubric")).await;
    harness
        .engine
        .add_mission_skill(&id, skill_v1)
        .await
        .unwrap();

    let plan = two_reviewer_plan_with_oracle_obligation();
    let mut team = common::team(1, Some(&plan), false);
    team.roles
        .get_mut(&RoleInstanceId::new("reviewer").unwrap())
        .unwrap()
        .skills = vec!["rubric".to_string()];
    team.roles.insert(
        RoleInstanceId::new("reviewer-plain").unwrap(),
        common::role("reviewer-plain", OutputSemantics::EmitsVerdict),
    );
    team.judgment_assignments.insert(
        AssertionId::new("STYLE-OK").unwrap(),
        vec![RoleInstanceId::new("reviewer").unwrap()],
    );
    team.judgment_assignments.insert(
        AssertionId::new("DOCS-OK").unwrap(),
        vec![RoleInstanceId::new("reviewer-plain").unwrap()],
    );
    harness
        .engine
        .propose_plan(
            &id,
            MissionProposal {
                team: Some(team),
                plan: Some(lionclaw::model::PlanProposal {
                    base_revision: 0,
                    requirement_changes: Vec::new(),
                    assertion_supersessions: Vec::new(),
                    plan,
                }),
            },
        )
        .await
        .unwrap();
    approve_plan(&harness.engine, &id).await;
    let proven = harness.engine.advance(&id).await.unwrap().state;
    let style = AssertionId::new("STYLE-OK").unwrap();
    let docs = AssertionId::new("DOCS-OK").unwrap();
    let skilled = RoleInstanceId::new("reviewer").unwrap();
    let plain = RoleInstanceId::new("reviewer-plain").unwrap();
    let style_effect = proven.contract[&style].last_advisory[&skilled].clone();
    let docs_effect = proven.contract[&docs].last_advisory[&plain].clone();
    assert_eq!(proven.advisory_status(&style), AdvisoryStatus::Passed);
    assert_eq!(proven.advisory_status(&docs), AdvisoryStatus::Passed);
    assert_eq!(skilled_attempts.load(Ordering::SeqCst), 1);
    assert_eq!(plain_attempts.load(Ordering::SeqCst), 1);

    std::fs::remove_dir_all(mission_skills_dir(&harness.engine, &id).join("rubric")).unwrap();
    let replacement_sources = dir.path().join("skill-replacements");
    write_skill_package(
        &replacement_sources,
        "rubric",
        "Rubric v2",
        "Check style and clarity.",
    );
    let skill_v2 =
        install_mission_skill(&harness.engine, &id, &replacement_sources.join("rubric")).await;
    common::fault_append_events(
        dir.path(),
        &id,
        proven.head,
        &[NewEvent::new(lionclaw::model::MissionEvent::SkillAdded {
            skill: skill_v2,
        })],
        42,
    )
    .await;
    let pending = harness.engine.load_state(&id).await.unwrap();
    assert_eq!(pending.advisory_status(&style), AdvisoryStatus::Pending);
    assert_eq!(pending.advisory_status(&docs), AdvisoryStatus::Passed);
    assert_eq!(
        pending.contract[&docs].last_advisory[&plain], docs_effect,
        "unaffected receipt should remain the current evidence"
    );

    let attention_id = oracle_attention_id(&pending);
    harness
        .engine
        .decide(
            &id,
            &attention_id,
            DecisionAction::Repair,
            "rerun after rubric change",
        )
        .await
        .unwrap();
    let reproved = harness.engine.advance(&id).await.unwrap().state;
    let new_style_effect = reproved.contract[&style].last_advisory[&skilled].clone();
    assert_ne!(new_style_effect, style_effect);
    assert_eq!(reproved.contract[&docs].last_advisory[&plain], docs_effect);
    assert_eq!(skilled_attempts.load(Ordering::SeqCst), 2);
    assert_eq!(plain_attempts.load(Ordering::SeqCst), 1);
    assert_eq!(reproved.advisory_status(&style), AdvisoryStatus::Passed);
    assert_eq!(reproved.advisory_status(&docs), AdvisoryStatus::Passed);
}

#[tokio::test]
async fn advisory_fail_parks_for_generic_recovery() {
    let state = run(false).await;
    let assertion = lionclaw::model::AssertionId::new("STYLE-OK").unwrap();
    assert_eq!(state.advisory_status(&assertion), AdvisoryStatus::Failed);
    assert_eq!(state.phase, MissionPhase::AttentionNeeded);
    assert_eq!(lionclaw::model::ready_to_finish(&state), None);
    let attention = &state.open_attention["proof_bar_unmet:mission"];
    assert_eq!(
        attention.kind,
        lionclaw::model::AttentionKind::ProofBarUnmet
    );
    assert_eq!(attention.assertion_ids, [assertion]);
    let actions = lionclaw::engine::MissionView::from_state(state, false).next_actions();
    assert_eq!(actions, ["mission decide", "mission abort"]);
}

#[tokio::test]
async fn read_only_validator_artifacts_are_rejected_before_the_fold() {
    let dir = tempfile::tempdir().unwrap();
    let runner = MockRoleRunner::new(Box::new(|request: &RoleTurnRequest| {
        if request.role.output == OutputSemantics::ProducesArtifact {
            return Ok(RoleTurnOutcome {
                handoff: Some(Handoff::Work {
                    done: true,
                    report: PayloadRef::inline("wrote it"),
                    request_attention: false,
                }),
                artifact: Some(CapturedArtifact::for_testing(
                    request.base_sha.clone(),
                    HEAD_SHA,
                )),
                prepared_inputs: Vec::new(),
                runtime_configuration: Default::default(),
                runtime_usage: Default::default(),
                final_response: "wrote it".to_string(),
            });
        }
        Ok(RoleTurnOutcome {
            handoff: Some(Handoff::Validate {
                done: true,
                report: PayloadRef::inline("reviewed"),
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
            artifact: Some(CapturedArtifact::for_testing(
                request.base_sha.clone(),
                "forged-validator-head",
            )),
            prepared_inputs: Vec::new(),
            runtime_configuration: Default::default(),
            runtime_usage: Default::default(),
            final_response: "reviewed".to_string(),
        })
    }));
    let mut mission_type = common::test_mission_type();
    mission_type
        .edit_for_testing(|definition| definition.stop = lionclaw::model::StopBar::Attested);
    let harness = common::harness_with_type(
        dir.path(),
        mission_type,
        runner,
        MockOracleRunner::exiting(0),
    )
    .await;
    let id = harness
        .engine
        .create_mission(dir.path().to_str().unwrap(), "reject artifact", BASE_SHA)
        .await
        .unwrap();
    harness
        .engine
        .propose_plan(&id, proposal(0, advisory_plan()))
        .await
        .unwrap();
    approve_plan(&harness.engine, &id).await;
    let state = harness.engine.advance(&id).await.unwrap().state;
    assert_eq!(state.current_sha, HEAD_SHA);
    assert!(matches!(state.phase, MissionPhase::AttentionNeeded));
    assert!(harness
        .engine
        .store()
        .load(&id)
        .await
        .unwrap()
        .iter()
        .any(|event| matches!(
            &event.event,
            lionclaw::model::MissionEvent::RoleTurnCompleted {
                outcome: Err(failure),
                ..
            } if failure.evidence().code.as_deref() == Some("workspace.capture_authority")
        )));
}

#[tokio::test]
async fn replacement_validator_requires_new_receipt_and_retains_prior_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let runner = MockRoleRunner::new(Box::new({
        let attempts = attempts.clone();
        move |request: &RoleTurnRequest| match request.role.output {
            OutputSemantics::ProducesArtifact => Ok(RoleTurnOutcome {
                handoff: Some(Handoff::Work {
                    done: true,
                    report: PayloadRef::inline("wrote it"),
                    request_attention: false,
                }),
                artifact: Some(CapturedArtifact::for_testing(
                    request.base_sha.clone(),
                    HEAD_SHA,
                )),
                prepared_inputs: Vec::new(),
                runtime_configuration: Default::default(),
                runtime_usage: Default::default(),
                final_response: "wrote it".to_string(),
            }),
            OutputSemantics::EmitsVerdict => {
                let passed = attempts.fetch_add(1, Ordering::SeqCst) == 0;
                Ok(RoleTurnOutcome {
                    handoff: Some(Handoff::Validate {
                        done: true,
                        report: PayloadRef::inline(if passed {
                            "original validator passed"
                        } else {
                            "replacement validator failed"
                        }),
                        items: request
                            .assertion_ids
                            .iter()
                            .cloned()
                            .map(|item_id| ValidationItem { item_id, passed })
                            .collect(),
                        passed,
                        request_attention: false,
                    }),
                    artifact: None,
                    prepared_inputs: Vec::new(),
                    runtime_configuration: Default::default(),
                    runtime_usage: Default::default(),
                    final_response: "reviewed".to_string(),
                })
            }
            other => panic!("unexpected output {other:?}"),
        }
    }));
    let mut mission_type = common::test_mission_type();
    mission_type
        .edit_for_testing(|definition| definition.stop = lionclaw::model::StopBar::Attested);
    let harness = common::harness_with_type(
        dir.path(),
        mission_type,
        runner,
        MockOracleRunner::exiting(1),
    )
    .await;
    let id = harness
        .engine
        .create_mission(dir.path().to_str().unwrap(), "replace judge", BASE_SHA)
        .await
        .unwrap();
    let mut plan = advisory_plan();
    let tests_pass = lionclaw::model::AssertionId::new("TESTS-PASS").unwrap();
    plan.requirements.push(common::covered_requirement(
        "GREEN-TESTS",
        tests_pass.as_str(),
    ));
    plan.assertions.push(lionclaw::model::Assertion {
        id: tests_pass.clone(),
        prose: "cargo test exits 0".to_string(),
        oracle: Some(lionclaw::model::OracleName::new("cargo-test").unwrap()),
    });
    plan.tasks[0].targets.push(tests_pass);
    harness
        .engine
        .propose_plan(&id, proposal(0, plan))
        .await
        .unwrap();
    approve_plan(&harness.engine, &id).await;
    let original = harness.engine.advance(&id).await.unwrap().state;
    let assertion = lionclaw::model::AssertionId::new("STYLE-OK").unwrap();
    let original_effect = original.contract[&assertion].last_advisory
        [&lionclaw::model::RoleInstanceId::new("reviewer").unwrap()]
        .clone();
    let original_receipt = original.role_attempt_receipts[&original_effect].clone();

    let mut next_team = original.team.clone().unwrap();
    next_team.revision += 1;
    let old = lionclaw::model::RoleInstanceId::new("reviewer").unwrap();
    let replacement = lionclaw::model::RoleInstanceId::new("reviewer-v2").unwrap();
    next_team.roles.remove(&old);
    next_team.roles.insert(
        replacement.clone(),
        common::role("reviewer-v2", OutputSemantics::EmitsVerdict),
    );
    next_team
        .judgment_assignments
        .insert(assertion.clone(), vec![replacement.clone()]);
    for panel in next_team.judgment_assignments.values_mut() {
        for role in panel {
            if *role == old {
                *role = replacement.clone();
            }
        }
    }
    harness
        .engine
        .propose_plan(
            &id,
            MissionProposal {
                plan: None,
                team: Some(next_team),
            },
        )
        .await
        .unwrap();
    approve_plan(&harness.engine, &id).await;
    let pending = harness.engine.load_state(&id).await.unwrap();
    assert_eq!(pending.advisory_status(&assertion), AdvisoryStatus::Pending);
    assert_eq!(
        pending.role_attempt_receipts.get(&original_effect),
        Some(&original_receipt)
    );

    let oracle_attention = pending
        .open_attention
        .values()
        .find(|item| item.kind == lionclaw::model::AttentionKind::OracleVerdictFailed)
        .unwrap()
        .id
        .clone();
    harness
        .engine
        .decide(
            &id,
            &oracle_attention,
            DecisionAction::Repair,
            "rerun under replacement judge",
        )
        .await
        .unwrap();
    let settled = harness.engine.advance(&id).await.unwrap().state;
    let replacement_effect = settled.contract[&assertion].last_advisory[&replacement].clone();
    assert_ne!(replacement_effect, original_effect);
    assert_eq!(
        settled.role_attempt_receipts.get(&original_effect),
        Some(&original_receipt)
    );
    assert!(matches!(
        settled.role_attempt_receipts[&replacement_effect].source,
        RoleEffectSource::Turn {
            plan_revision: 1,
            ..
        }
    ));
    assert!(matches!(
        settled.role_attempt_receipts[&replacement_effect].disposition,
        RoleAttemptDisposition::Succeeded { .. }
    ));
    assert_eq!(settled.advisory_status(&assertion), AdvisoryStatus::Failed);
}
