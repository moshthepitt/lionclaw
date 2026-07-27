//! Slice-1 core semantics: a trivially-passing mission reaches a verified
//! finish; an oracle failure can never be reported as verified.

mod common;

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use clap::Parser;
use common::{
    approve_plan, covered_requirement, harness, proposal, review_runner, simple_plan,
    test_mission_type, BASE_SHA, HEAD_SHA,
};
use lionclaw::engine::MissionDisposition;
use lionclaw::engine::{Engine, EngineServices};
use lionclaw::mission_type::PreparedInput;
use lionclaw::model::{
    Assertion, AssertionId, DecisionAction, FinishClass, InputName, MissionPhase, OracleName,
    OutputSemantics, RoleInstanceId, RuntimeUsage, RuntimeUsageCost, RuntimeUsageCostScope,
    RuntimeUsageDetails, TaskStatus,
};
use lionclaw::testing::{MockClock, NoopEffectCleaner};
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};
use lionclaw::{cli, store::MissionStore};

#[tokio::test]
async fn question_checkpoint_resumes_after_cli_feedback_and_restart_then_completes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let turns = Arc::new(AtomicUsize::new(0));
    let scripted_turns = turns.clone();
    let role_runner = MockRoleRunner::new(Box::new(move |request| {
        if request.role.output == OutputSemantics::EmitsVerdict {
            return Ok(lionclaw::ports::RoleTurnOutcome {
                handoff: Some(lionclaw::model::Handoff::Validate {
                    done: true,
                    report: lionclaw::model::PayloadRef::inline("judged"),
                    items: request
                        .assertion_ids
                        .iter()
                        .cloned()
                        .map(|item_id| lionclaw::model::ValidationItem {
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
                final_response: "judged".into(),
            });
        }
        match scripted_turns.fetch_add(1, Ordering::SeqCst) {
            0 => Ok(lionclaw::ports::RoleTurnOutcome {
                handoff: None,
                artifact: None,
                prepared_inputs: Vec::new(),
                runtime_configuration: Default::default(),
                runtime_usage: Default::default(),
                final_response: "Which behavior should I preserve?".into(),
            }),
            1 => {
                let mut failure = lionclaw_runtime_api::TypedFailure::invalid(
                    "handoff.schema",
                    "handoff is missing the 'done' boolean",
                );
                failure.evidence_mut().final_response = "I attempted the requested repair".into();
                Err(failure)
            }
            _ => Ok(lionclaw::ports::RoleTurnOutcome {
                handoff: Some(lionclaw::model::Handoff::Work {
                    done: true,
                    report: lionclaw::model::PayloadRef::inline("preserved compatibility"),
                    request_attention: false,
                }),
                artifact: Some(lionclaw::ports::CapturedArtifact::for_testing(
                    request.base_sha.clone(),
                    HEAD_SHA,
                )),
                prepared_inputs: Vec::new(),
                runtime_configuration: Default::default(),
                runtime_usage: Default::default(),
                final_response: "Implemented and verified.".into(),
            }),
        }
    }));
    let h = common::harness(dir.path(), role_runner, MockOracleRunner::exiting(0)).await;
    let mission_id = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "preserve behavior", BASE_SHA)
        .await
        .unwrap();
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&h.engine, &mission_id).await;

    let checkpoint = h.engine.advance(&mission_id).await.unwrap();
    assert_eq!(checkpoint.disposition, MissionDisposition::AwaitingLead);
    let (role_instance, conversation) = checkpoint.state.conversations.iter().next().unwrap();
    assert_eq!(
        conversation.final_response,
        Some(lionclaw::model::PayloadRef::inline(
            "Which behavior should I preserve?"
        ))
    );
    let role_instance = role_instance.clone();
    let assignment_epoch = checkpoint
        .state
        .tasks
        .values()
        .next()
        .unwrap()
        .role_assignment
        .as_ref()
        .unwrap()
        .assignment_epoch;
    let send = cli::Cli::try_parse_from([
        "lionclaw",
        "mission",
        "send",
        "--mission-id",
        mission_id.as_str(),
        "--to",
        "fix",
        "--repo",
        dir.path().to_str().unwrap(),
        "Preserve compatibility.",
    ])
    .expect("real CLI parser accepts task-name recipient sugar");
    assert_eq!(
        cli::run(send).await.unwrap(),
        std::process::ExitCode::SUCCESS
    );

    // Reconstruct the production engine over the durable store, as a new CLI
    // process would. The same role runner stands in for the profile-selected
    // native adapter and lets the test inspect continuity without provider
    // branching.
    let restarted = Engine::new(
        MissionStore::open(dir.path()).await.unwrap(),
        test_mission_type(),
        "localhost/lionclaw-runtime-dev:v1".into(),
        EngineServices::new(
            h.role_runner.clone(),
            h.oracle_runner.clone(),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );
    let outcome = restarted.advance(&mission_id).await.unwrap();
    assert_eq!(outcome.disposition, MissionDisposition::Terminal);
    assert_eq!(
        outcome.state.phase,
        MissionPhase::Done {
            finish: FinishClass::Verified
        }
    );
    let resumed = outcome.state.conversations.get(&role_instance).unwrap();
    assert_eq!(resumed.role_instance, role_instance);
    let task = outcome.state.tasks.values().next().unwrap();
    assert_eq!(
        task.role_assignment.as_ref().unwrap().assignment_epoch,
        assignment_epoch
    );
    assert!(resumed.queued.is_empty());
    assert_eq!(turns.load(Ordering::SeqCst), 3);

    let events = MissionStore::open(dir.path())
        .await
        .unwrap()
        .load(&mission_id)
        .await
        .unwrap();
    let invalid = events
        .iter()
        .position(|event| {
            matches!(
                &event.event,
                lionclaw::model::MissionEvent::RoleTurnCompleted { outcome: Err(failure), .. }
                    if failure.evidence().code.as_deref() == Some("handoff.schema")
                        && failure.detail() == "handoff is missing the 'done' boolean"
            )
        })
        .expect("exact schema failure is durable");
    let message_sequence = events
        .iter()
        .find(|event| {
            matches!(
                event.event,
                lionclaw::model::MissionEvent::MessageSent { .. }
            )
        })
        .expect("CLI feedback is durable")
        .sequence_no;
    assert!(resumed.consumed_through >= message_sequence);
    let valid = events
        .iter()
        .position(|event| {
            matches!(
                event.event,
                lionclaw::model::MissionEvent::RoleTurnCompleted { outcome: Ok(_), .. }
            ) && event.sequence_no > events[invalid].sequence_no
        })
        .expect("valid repair follows invalid handoff");
    let receipt = events
        .iter()
        .position(|event| {
            matches!(
                event.event,
                lionclaw::model::MissionEvent::OracleRunCompleted { .. }
            ) && event.sequence_no > events[valid].sequence_no
        })
        .expect("fresh oracle receipt follows the valid repair");
    assert!(
        invalid < valid && valid < receipt,
        "invalid={invalid}, valid={valid}, receipt={receipt}"
    );
}

#[tokio::test]
async fn passing_oracle_yields_verified_finish() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        review_runner(vec![]),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(
            dir.path().to_str().expect("utf8"),
            "make the tests pass",
            BASE_SHA,
        )
        .await
        .expect("create");
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    assert_eq!(outcome.disposition, MissionDisposition::Terminal);
    assert_eq!(
        outcome.state.phase,
        MissionPhase::Done {
            finish: FinishClass::Verified
        }
    );

    let state = h.engine.load_state(&mission_id).await.expect("state");
    assert_eq!(state.current_sha, HEAD_SHA);
    assert!(state
        .tasks
        .values()
        .all(|t| t.status == TaskStatus::Cleared));
    let assertion = state.contract.values().next().expect("assertion");
    let verdict = state.authoritative_verdict(assertion).expect("verdict");
    assert!(verdict.passed());
    assert_eq!(verdict.judged_sha(), HEAD_SHA);
    assert_eq!(verdict.exit_code(), 0);
    // The oracle judged the artifact commit, exactly once.
    assert_eq!(
        h.oracle_runner.calls.lock().expect("lock").as_slice(),
        &[("cargo-test".to_string(), HEAD_SHA.to_string())]
    );
}

#[tokio::test]
async fn direct_engine_creation_rejects_invalid_mission_type_policy() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut mission_type = test_mission_type();
    mission_type.edit_for_testing(|definition| definition.recovery.max_attempts = 0);
    let h = common::harness_with_type(
        dir.path(),
        mission_type,
        review_runner(vec![]),
        MockOracleRunner::exiting(0),
    )
    .await;
    let error = h
        .engine
        .create_mission("/repo", "invalid recovery", BASE_SHA)
        .await
        .expect_err("direct creation must enforce mission-type recovery policy");
    assert!(error.to_string().contains("max-attempts"), "got {error:#}");

    let dir = tempfile::tempdir().expect("tempdir");
    let mut mission_type = test_mission_type();
    mission_type.edit_for_testing(|definition| {
        definition.default_team.planning_assignment =
            RoleInstanceId::new("missing-planner").expect("role instance id");
    });
    let h = common::harness_with_type(
        dir.path(),
        mission_type,
        review_runner(vec![]),
        MockOracleRunner::exiting(0),
    )
    .await;
    let error = h
        .engine
        .create_mission("/repo", "invalid planning", BASE_SHA)
        .await
        .expect_err("direct creation must validate the planning DAG");
    assert!(
        error.to_string().contains("missing-planner"),
        "got {error:#}"
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let mut mission_type = test_mission_type();
    mission_type.edit_for_testing(|definition| {
        definition
            .default_team
            .roles
            .get_mut(&RoleInstanceId::new("reviewer").expect("role name"))
            .expect("reviewer role")
            .grants
            .secrets = true;
    });
    let h = common::harness_with_type(
        dir.path(),
        mission_type,
        review_runner(vec![]),
        MockOracleRunner::exiting(0),
    )
    .await;
    let error = h
        .engine
        .create_mission("/repo", "invalid authority", BASE_SHA)
        .await
        .expect_err("direct creation must enforce the non-writer authority moat");
    assert!(
        error.to_string().contains("may not mount runtime secrets"),
        "got {error:#}"
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let program = dir.path().join("prepared-input");
    std::fs::write(&program, "#!/bin/sh\nexit 0\n").expect("write input program");
    std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755))
        .expect("make input executable");
    let input_name = InputName::new("prepared-input").expect("input name");
    let mut mission_type = test_mission_type();
    mission_type.edit_for_testing(|definition| {
        definition.inputs.insert(
            input_name.clone(),
            PreparedInput {
                name: input_name,
                program,
                network: false,
                key_files: vec!["../outside".into()],
                environment: BTreeMap::new(),
            },
        );
    });
    let h = common::harness_with_type(
        dir.path(),
        mission_type,
        review_runner(vec![]),
        MockOracleRunner::exiting(0),
    )
    .await;
    let error = h
        .engine
        .create_mission("/repo", "invalid prepared input", BASE_SHA)
        .await
        .expect_err("direct creation must enforce prepared-input path safety");
    assert!(
        error.to_string().contains("without traversal"),
        "got {error:#}"
    );
}

#[tokio::test]
async fn durable_role_outcomes_bound_adapter_configuration_evidence() {
    let dir = tempfile::tempdir().expect("tempdir");
    let oversized = "x".repeat(lionclaw_runtime_api::FAILURE_TEXT_LIMIT + 1);
    let h = harness(
        dir.path(),
        MockRoleRunner::new(Box::new(move |request| {
            Ok(lionclaw::ports::RoleTurnOutcome {
                handoff: Some(lionclaw::model::Handoff::Work {
                    done: true,
                    report: lionclaw::model::PayloadRef::inline("done"),
                    request_attention: false,
                }),
                artifact: Some(lionclaw::ports::CapturedArtifact::for_testing(
                    request.base_sha.clone(),
                    HEAD_SHA,
                )),
                prepared_inputs: Vec::new(),
                runtime_configuration: lionclaw::model::RuntimeConfigurationEvidence {
                    applied_model: Some(oversized.clone()),
                    ..Default::default()
                }
                .projected(),
                runtime_usage: Default::default(),
                final_response: "done".into(),
            })
        })),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(
            dir.path().to_str().expect("utf8"),
            "bound runtime evidence",
            BASE_SHA,
        )
        .await
        .expect("create");
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    h.engine.advance(&mission_id).await.expect("advance");

    let state = h.engine.load_state(&mission_id).await.expect("state");
    let task_id = state.tasks.keys().next().expect("task");
    let applied_model = state
        .task_last_role_attempt(task_id)
        .and_then(|receipt| receipt.runtime_configuration.as_ref())
        .and_then(|configuration| configuration.applied_model.as_ref())
        .expect("applied model evidence");
    assert!(applied_model.len() <= lionclaw_runtime_api::FAILURE_TEXT_LIMIT);
}

#[tokio::test]
async fn role_prepared_input_refs_are_durable_receipt_evidence() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input_ref = lionclaw::model::PreparedInputRef {
        name: InputName::new("cargo-home").unwrap(),
        digest: "a".repeat(64),
    };
    let expected = input_ref.clone();
    let h = harness(
        dir.path(),
        MockRoleRunner::new(Box::new(move |request| {
            Ok(lionclaw::ports::RoleTurnOutcome {
                handoff: Some(lionclaw::model::Handoff::Work {
                    done: true,
                    report: lionclaw::model::PayloadRef::inline("done"),
                    request_attention: false,
                }),
                artifact: Some(lionclaw::ports::CapturedArtifact::for_testing(
                    request.base_sha.clone(),
                    HEAD_SHA,
                )),
                prepared_inputs: vec![input_ref.clone()],
                runtime_configuration: Default::default(),
                runtime_usage: Default::default(),
                final_response: "done".into(),
            })
        })),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(
            dir.path().to_str().expect("utf8"),
            "retain input receipt",
            BASE_SHA,
        )
        .await
        .expect("create");
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    let done = h.engine.advance(&mission_id).await.expect("advance");
    let receipt = done
        .state
        .role_attempt_receipts
        .values()
        .find(|receipt| !receipt.prepared_inputs.is_empty())
        .expect("role prepared-input receipt");
    assert_eq!(receipt.prepared_inputs, vec![expected.clone()]);
    let events = h.engine.store().load(&mission_id).await.expect("events");
    assert!(events.iter().any(|event| matches!(
        &event.event,
        lionclaw::model::MissionEvent::RoleTurnCompleted {
            outcome: Ok(success),
            ..
        } if success.prepared_inputs == vec![expected.clone()]
    )));
}

#[tokio::test]
async fn durable_role_outcomes_preserve_and_report_runtime_usage() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        MockRoleRunner::new(Box::new(move |request| {
            Ok(lionclaw::ports::RoleTurnOutcome {
                handoff: Some(lionclaw::model::Handoff::Work {
                    done: true,
                    report: lionclaw::model::PayloadRef::inline("done"),
                    request_attention: false,
                }),
                artifact: Some(lionclaw::ports::CapturedArtifact::for_testing(
                    request.base_sha.clone(),
                    HEAD_SHA,
                )),
                prepared_inputs: Vec::new(),
                runtime_configuration: Default::default(),
                runtime_usage: RuntimeUsage::from_details(RuntimeUsageDetails {
                    input_tokens: Some(10),
                    output_tokens: Some(2),
                    total_tokens: Some(12),
                    cost: Some(RuntimeUsageCost {
                        amount: "0.0042".to_string(),
                        currency: "USD".to_string(),
                        scope: RuntimeUsageCostScope::Turn,
                    }),
                    ..Default::default()
                }),
                final_response: "done".into(),
            })
        })),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(
            dir.path().to_str().expect("utf8"),
            "preserve runtime usage",
            BASE_SHA,
        )
        .await
        .expect("create");
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    h.engine.advance(&mission_id).await.expect("advance");

    let state = h.engine.load_state(&mission_id).await.expect("state");
    let task_id = state.tasks.keys().next().expect("task");
    let receipt = state
        .task_last_role_attempt(task_id)
        .expect("role attempt receipt");
    let details = receipt.runtime_usage.details().expect("usage details");
    assert_eq!(details.input_tokens, Some(10));
    assert_eq!(details.output_tokens, Some(2));
    assert_eq!(details.total_tokens, Some(12));

    let json =
        lionclaw::evidence::role_attempt_receipt_json(h.engine.store().blobs(), &state, receipt);
    assert_eq!(json["runtime_usage"]["status"], "reported");
    assert_eq!(json["runtime_usage"]["usage"]["total_tokens"], 12);
    assert_eq!(json["runtime_usage"]["usage"]["cost"]["scope"], "turn");

    let rendered =
        lionclaw::evidence::render_role_attempt_receipt(h.engine.store().blobs(), &state, receipt);
    assert!(rendered.contains("runtime usage: input_tokens=10"));
    assert!(rendered.contains("total_tokens=12"));
    assert!(rendered.contains("cost=0.0042 USD (turn)"));
}

#[tokio::test]
async fn manual_proof_checkpoint_drains_the_whole_oracle_batch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut mission_type = test_mission_type();
    mission_type.edit_for_testing(|definition| {
        definition.oracles.insert(
            OracleName::new("lint").unwrap(),
            "/nonexistent-mission-type/oracles/lint".into(),
        );
        definition.execution.auto_continue_candidate = false;
        definition.execution.auto_continue_proof = false;
    });
    let h = common::harness_with_type(
        dir.path(),
        mission_type,
        review_runner(vec![]),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mut plan = simple_plan();
    plan.requirements
        .push(covered_requirement("LINT-GREEN", "LINT-PASS"));
    plan.assertions.push(Assertion {
        id: AssertionId::new("LINT-PASS").unwrap(),
        prose: "lint exits 0".into(),
        oracle: Some(OracleName::new("lint").unwrap()),
    });
    plan.tasks[0]
        .targets
        .push(AssertionId::new("LINT-PASS").unwrap());
    let mission_id = h
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "run the complete proof batch",
            BASE_SHA,
        )
        .await
        .unwrap();
    h.engine
        .propose_plan(&mission_id, proposal(0, plan))
        .await
        .unwrap();
    approve_plan(&h.engine, &mission_id).await;

    let checkpoint = h.engine.advance(&mission_id).await.unwrap();
    assert_eq!(checkpoint.disposition, MissionDisposition::Ready);
    assert!(checkpoint.state.inflight.is_empty());
    assert!(checkpoint
        .state
        .contract
        .values()
        .all(|assertion| assertion.last_authoritative_receipt.is_none()));

    let checkpoint = h.engine.advance(&mission_id).await.unwrap();
    assert_eq!(checkpoint.disposition, MissionDisposition::Ready);
    assert_eq!(
        checkpoint
            .state
            .contract
            .values()
            .filter(|assertion| assertion.last_authoritative_receipt.is_some())
            .count(),
        2
    );
    assert!(checkpoint.state.inflight.is_empty());

    let checkpoint = h.engine.advance(&mission_id).await.unwrap();
    assert_eq!(
        checkpoint.disposition,
        MissionDisposition::Terminal,
        "phase={:?} attention={:?} contract={:?}",
        checkpoint.state.phase,
        checkpoint.state.open_attention,
        checkpoint.state.contract
    );
    assert!(checkpoint.state.inflight.is_empty());
    let events = h.engine.store().load(&mission_id).await.unwrap();
    assert!(!events.iter().any(|event| {
        matches!(
            &event.event,
            lionclaw::model::MissionEvent::OracleRunCompleted {
                outcome: Err(lionclaw_runtime_api::TypedFailure::Interrupted { .. }),
                ..
            }
        )
    }));
}

#[tokio::test]
async fn already_satisfied_work_verifies_without_advancing_head() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        MockRoleRunner::new(Box::new(|request| {
            if request.role.output == OutputSemantics::EmitsVerdict {
                Ok(lionclaw::ports::RoleTurnOutcome {
                    handoff: Some(lionclaw::model::Handoff::Validate {
                        done: true,
                        report: lionclaw::model::PayloadRef::inline("judged"),
                        items: request
                            .assertion_ids
                            .iter()
                            .cloned()
                            .map(|item_id| lionclaw::model::ValidationItem {
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
                    final_response: "judged".into(),
                })
            } else {
                Ok(lionclaw::ports::RoleTurnOutcome {
                    handoff: Some(lionclaw::model::Handoff::Work {
                        done: true,
                        report: lionclaw::model::PayloadRef::inline("already satisfied"),
                        request_attention: false,
                    }),
                    artifact: Some(lionclaw::ports::CapturedArtifact::for_testing(
                        request.base_sha.clone(),
                        BASE_SHA,
                    )),
                    prepared_inputs: Vec::new(),
                    runtime_configuration: Default::default(),
                    runtime_usage: Default::default(),
                    final_response: "already satisfied".into(),
                })
            }
        })),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(
            dir.path().to_str().expect("utf8"),
            "confirm the existing implementation",
            BASE_SHA,
        )
        .await
        .expect("create");
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;

    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    assert_eq!(outcome.disposition, MissionDisposition::Terminal);
    assert!(matches!(
        outcome.state.phase,
        MissionPhase::Done {
            finish: FinishClass::Verified
        }
    ));
    let state = h.engine.load_state(&mission_id).await.expect("state");
    assert_eq!(state.current_sha, BASE_SHA);
    assert_eq!(
        h.oracle_runner.calls.lock().expect("lock").as_slice(),
        &[("cargo-test".to_string(), BASE_SHA.to_string())]
    );
}

#[tokio::test]
async fn failing_required_oracle_cannot_be_accepted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        review_runner(vec![]),
        MockOracleRunner::exiting(1),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(
            dir.path().to_str().expect("utf8"),
            "make the tests pass",
            BASE_SHA,
        )
        .await
        .expect("create");
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    assert_eq!(outcome.disposition, MissionDisposition::Parked);
    let attention: Vec<_> = outcome.state.open_attention.values().collect();
    assert_eq!(attention.len(), 1);
    assert_eq!(attention[0].id, "proof_failed:oracle:cargo-test");
    assert_eq!(attention[0].assertion_ids[0].as_str(), "TESTS-PASS");
    assert!(matches!(
        &attention[0].evidence,
        lionclaw::model::DecisionEvidence::AuthoritativeReceipts { effect_ids }
            if effect_ids.len() == 1
    ));
    let state = h.engine.load_state(&mission_id).await.expect("state");
    let verdict = state
        .contract
        .values()
        .next()
        .and_then(|assertion| state.authoritative_verdict(assertion))
        .expect("verdict");
    assert!(!verdict.passed());
    assert_eq!(verdict.exit_code(), 1);

    let error = h
        .engine
        .decide(
            &mission_id,
            "proof_failed:oracle:cargo-test",
            DecisionAction::Accept,
            "required proof cannot be waived",
        )
        .await
        .expect_err("accept must be rejected for failed required proof");
    assert!(
        error.to_string().contains("not valid"),
        "unexpected error: {error:#}"
    );
    let parked = h
        .engine
        .load_state(&mission_id)
        .await
        .expect("parked state");
    assert_eq!(parked.phase, MissionPhase::AttentionNeeded);
    assert_eq!(lionclaw::model::ready_to_finish(&parked), None);
    assert!(parked
        .open_attention
        .contains_key("proof_failed:oracle:cargo-test"));
}

#[tokio::test]
async fn command_retry_is_reoffered_only_for_a_changed_outcome() {
    let dir = tempfile::tempdir().expect("tempdir");
    let attempts = Arc::new(AtomicUsize::new(0));
    let oracle_attempts = attempts.clone();
    let h = harness(
        dir.path(),
        review_runner(vec![]),
        MockOracleRunner::new(Box::new(move |_| {
            let attempt = oracle_attempts.fetch_add(1, Ordering::SeqCst);
            Ok(lionclaw::ports::OracleOutcome {
                exit_code: 1,
                exit_signal: None,
                stdout: if attempt == 0 {
                    b"first failure".to_vec()
                } else {
                    b"changed failure".to_vec()
                },
                stderr: Vec::new(),
                prepared_inputs: Vec::new(),
                duration_ms: 1,
            })
        })),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(
            dir.path().to_str().expect("utf8"),
            "exercise proof retry",
            BASE_SHA,
        )
        .await
        .expect("create");
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;

    let first = h.engine.advance(&mission_id).await.expect("first failure");
    let attention_id = "proof_failed:oracle:cargo-test";
    assert_eq!(
        lionclaw::model::legal_actions(&first.state, &first.state.open_attention[attention_id]),
        [
            DecisionAction::Retry,
            DecisionAction::Repair,
            DecisionAction::Revise
        ]
    );
    assert_eq!(first.state.authoritative_receipts.len(), 1);

    h.engine
        .decide(
            &mission_id,
            attention_id,
            DecisionAction::Retry,
            "retry the first failure",
        )
        .await
        .expect("first retry");
    let changed = h
        .engine
        .advance(&mission_id)
        .await
        .expect("changed failure");
    assert_eq!(
        lionclaw::model::legal_actions(&changed.state, &changed.state.open_attention[attention_id]),
        [
            DecisionAction::Retry,
            DecisionAction::Repair,
            DecisionAction::Revise
        ]
    );
    assert_eq!(changed.state.authoritative_receipts.len(), 2);

    h.engine
        .decide(
            &mission_id,
            attention_id,
            DecisionAction::Retry,
            "retry the changed failure",
        )
        .await
        .expect("second retry");
    let repeated = h
        .engine
        .advance(&mission_id)
        .await
        .expect("repeated failure");
    assert_eq!(
        lionclaw::model::legal_actions(
            &repeated.state,
            &repeated.state.open_attention[attention_id]
        ),
        [DecisionAction::Repair, DecisionAction::Revise]
    );
    assert_eq!(repeated.state.authoritative_receipts.len(), 3);
    h.engine
        .decide(
            &mission_id,
            attention_id,
            DecisionAction::Retry,
            "forged repeated retry",
        )
        .await
        .expect_err("identical repeated failure must suppress retry");
}

#[tokio::test]
async fn worker_reporting_not_done_parks_with_attention() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        MockRoleRunner::new(Box::new(|_| {
            Ok(lionclaw::ports::RoleTurnOutcome {
                handoff: Some(lionclaw::model::Handoff::Work {
                    done: false,
                    report: lionclaw::model::PayloadRef::inline("stuck"),
                    request_attention: false,
                }),
                artifact: None,
                prepared_inputs: Vec::new(),
                runtime_configuration: Default::default(),
                runtime_usage: Default::default(),
                final_response: String::new(),
            })
        })),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(
            dir.path().to_str().expect("utf8"),
            "make the tests pass",
            BASE_SHA,
        )
        .await
        .expect("create");
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    assert_eq!(outcome.disposition, MissionDisposition::Parked);
    let attention: Vec<_> = outcome.state.open_attention.values().collect();
    assert_eq!(attention.len(), 1);
    // Parked means parked: no oracle ever ran.
    assert!(h.oracle_runner.calls.lock().expect("lock").is_empty());
}

#[tokio::test]
async fn role_runner_cannot_inject_a_durable_blob_reference() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        MockRoleRunner::new(Box::new(|request| {
            Ok(lionclaw::ports::RoleTurnOutcome {
                handoff: Some(lionclaw::model::Handoff::Work {
                    done: true,
                    report: lionclaw::model::PayloadRef::Blob(lionclaw::model::BlobRef {
                        algo: "sha256".into(),
                        hex: "a".repeat(64),
                        len: 1,
                    }),
                    request_attention: false,
                }),
                artifact: Some(lionclaw::ports::CapturedArtifact::for_testing(
                    request.base_sha.clone(),
                    HEAD_SHA,
                )),
                prepared_inputs: Vec::new(),
                runtime_configuration: Default::default(),
                runtime_usage: Default::default(),
                final_response: "attempted injection".into(),
            })
        })),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(
            dir.path().to_str().expect("utf8"),
            "reject blob refs",
            BASE_SHA,
        )
        .await
        .expect("create");
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;

    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    assert_eq!(outcome.disposition, MissionDisposition::Parked);
    assert_eq!(outcome.state.current_sha, BASE_SHA);
    let task_id = outcome.state.tasks.keys().next().unwrap();
    let failure = outcome.state.task_last_failure(task_id).unwrap();
    assert_eq!(
        failure.evidence().code.as_deref(),
        Some("handoff.payload_ref")
    );
    assert!(h.oracle_runner.calls.lock().expect("lock").is_empty());
}

#[tokio::test]
async fn role_runner_oversized_report_is_a_durable_invalid_output() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        MockRoleRunner::new(Box::new(|request| {
            Ok(lionclaw::ports::RoleTurnOutcome {
                handoff: Some(lionclaw::model::Handoff::Work {
                    done: true,
                    report: lionclaw::model::PayloadRef::inline(
                        "x".repeat(lionclaw::model::MAX_ROLE_REPORT_BYTES + 1),
                    ),
                    request_attention: false,
                }),
                artifact: Some(lionclaw::ports::CapturedArtifact::for_testing(
                    request.base_sha.clone(),
                    HEAD_SHA,
                )),
                prepared_inputs: Vec::new(),
                runtime_configuration: Default::default(),
                runtime_usage: Default::default(),
                final_response: "oversized report".into(),
            })
        })),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(
            dir.path().to_str().expect("utf8"),
            "reject oversized reports",
            BASE_SHA,
        )
        .await
        .expect("create");
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;

    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    assert_eq!(outcome.disposition, MissionDisposition::Parked);
    assert_eq!(outcome.state.current_sha, BASE_SHA);
    let task_id = outcome.state.tasks.keys().next().unwrap();
    let failure = outcome.state.task_last_failure(task_id).unwrap();
    assert_eq!(
        failure.evidence().code.as_deref(),
        Some("handoff.report_too_large")
    );
    assert!(h.oracle_runner.calls.lock().expect("lock").is_empty());
}
