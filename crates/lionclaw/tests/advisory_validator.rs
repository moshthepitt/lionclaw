//! Team judgment remains advisory: it routes work and contributes evidence,
//! but only an oracle can mint authoritative verification.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use common::{advisory_plan, approve_plan, proposal, BASE_SHA, HEAD_SHA};
use lionclaw::model::{
    AdvisoryStatus, DecisionAction, FinishClass, Handoff, MissionPhase, MissionProposal,
    OutputSemantics, PayloadRef, RoleAttemptDisposition, RoleEffectSource, ValidationItem,
};
use lionclaw::ports::{CapturedArtifact, RoleTurnOutcome, RoleTurnRequest};
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
async fn advisory_fail_is_unverified() {
    let state = run(false).await;
    let assertion = lionclaw::model::AssertionId::new("STYLE-OK").unwrap();
    assert_eq!(state.advisory_status(&assertion), AdvisoryStatus::Failed);
    assert_eq!(
        state.phase,
        MissionPhase::Done {
            finish: FinishClass::Unverified
        }
    );
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
