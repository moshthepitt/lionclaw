//! Slice-1 core semantics: a trivially-passing mission reaches a verified
//! finish; an oracle failure can never be reported as verified.

mod common;

use common::{
    approve_plan, covered_requirement, harness, proposal, simple_plan, test_mission_type, BASE_SHA,
    HEAD_SHA,
};
use lionclaw::engine::MissionDisposition;
use lionclaw::model::{
    Assertion, AssertionId, FinishClass, MissionPhase, OracleName, OutputSemantics, PlanningDag,
    PlanningTask, RoleName, TaskId, TaskStatus,
};
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};

#[tokio::test]
async fn passing_oracle_yields_verified_finish() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
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
    let verdict = assertion.last_authoritative.as_ref().expect("verdict");
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
    mission_type.recovery.max_attempts = 0;
    let h = common::harness_with_type(
        dir.path(),
        mission_type,
        MockRoleRunner::happy(HEAD_SHA),
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
    mission_type.planning = PlanningDag {
        tasks: vec![PlanningTask {
            id: TaskId::new("author").expect("task id"),
            role: RoleName::new("missing-planner").expect("role name"),
            output: OutputSemantics::ProposesPlan,
            body: "Propose the plan.".into(),
            depends_on: Vec::new(),
        }],
    };
    let h = common::harness_with_type(
        dir.path(),
        mission_type,
        MockRoleRunner::happy(HEAD_SHA),
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
}

#[tokio::test]
async fn durable_role_outcomes_bound_adapter_configuration_evidence() {
    let dir = tempfile::tempdir().expect("tempdir");
    let oversized = "x".repeat(lionclaw_runtime_api::FAILURE_TEXT_LIMIT + 1);
    let h = harness(
        dir.path(),
        MockRoleRunner::new(Box::new(move |request| {
            Ok(lionclaw::ports::RoleRunOutcome {
                handoff: lionclaw::model::Handoff::Work {
                    done: true,
                    report: lionclaw::model::PayloadRef::inline("done"),
                    request_attention: false,
                },
                artifact: Some(lionclaw::model::ArtifactOutcome {
                    base_sha: request.base_sha.clone(),
                    head_sha: HEAD_SHA.into(),
                }),
                runtime_configuration: lionclaw::model::RuntimeConfigurationEvidence {
                    applied_model: Some(oversized.clone()),
                    ..Default::default()
                },
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
    let applied_model = state
        .tasks
        .values()
        .next()
        .and_then(|task| task.last_runtime_configuration.as_ref())
        .and_then(|configuration| configuration.applied_model.as_ref())
        .expect("applied model evidence");
    assert!(applied_model.len() <= lionclaw_runtime_api::FAILURE_TEXT_LIMIT);
}

#[tokio::test]
async fn manual_proof_checkpoint_drains_the_whole_oracle_batch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut mission_type = test_mission_type();
    mission_type.oracles.insert(
        OracleName::new("lint").unwrap(),
        "/nonexistent-mission-type/oracles/lint".into(),
    );
    mission_type.execution.auto_continue_proof = false;
    let h = common::harness_with_type(
        dir.path(),
        mission_type,
        MockRoleRunner::happy(HEAD_SHA),
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
    assert_eq!(checkpoint.disposition, MissionDisposition::Terminal);
    assert!(checkpoint.state.inflight.is_empty());
    assert_eq!(
        checkpoint
            .state
            .contract
            .values()
            .filter(|assertion| assertion.last_authoritative.is_some())
            .count(),
        2
    );
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
        MockRoleRunner::happy(BASE_SHA),
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
async fn failing_oracle_never_reports_verified() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
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
    assert_eq!(attention[0].id, "oracle_verdict_failed:cargo-test");
    assert_eq!(attention[0].assertion_ids[0].as_str(), "TESTS-PASS");
    assert_eq!(attention[0].evidence.as_ref().unwrap().exit_code, 1);
    let state = h.engine.load_state(&mission_id).await.expect("state");
    let verdict = state
        .contract
        .values()
        .next()
        .expect("assertion")
        .last_authoritative
        .as_ref()
        .expect("verdict");
    assert!(!verdict.passed());
    assert_eq!(verdict.exit_code(), 1);
}

#[tokio::test]
async fn worker_reporting_not_done_parks_with_attention() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        MockRoleRunner::new(Box::new(|_| {
            Ok(lionclaw::ports::RoleRunOutcome {
                handoff: lionclaw::model::Handoff::Work {
                    done: false,
                    report: lionclaw::model::PayloadRef::inline("stuck"),
                    request_attention: false,
                },
                artifact: None,
                runtime_configuration: Default::default(),
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
            Ok(lionclaw::ports::RoleRunOutcome {
                handoff: lionclaw::model::Handoff::Work {
                    done: true,
                    report: lionclaw::model::PayloadRef::Blob(lionclaw::model::BlobRef {
                        algo: "sha256".into(),
                        hex: "a".repeat(64),
                        len: 1,
                    }),
                    request_attention: false,
                },
                artifact: Some(lionclaw::model::ArtifactOutcome {
                    base_sha: request.base_sha.clone(),
                    head_sha: HEAD_SHA.into(),
                }),
                runtime_configuration: Default::default(),
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
    let failure = outcome
        .state
        .tasks
        .values()
        .next()
        .unwrap()
        .last_failure
        .as_ref()
        .unwrap();
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
            Ok(lionclaw::ports::RoleRunOutcome {
                handoff: lionclaw::model::Handoff::Work {
                    done: true,
                    report: lionclaw::model::PayloadRef::inline(
                        "x".repeat(lionclaw::runner::MAX_HANDOFF_REPORT_BYTES + 1),
                    ),
                    request_attention: false,
                },
                artifact: Some(lionclaw::model::ArtifactOutcome {
                    base_sha: request.base_sha.clone(),
                    head_sha: HEAD_SHA.into(),
                }),
                runtime_configuration: Default::default(),
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
    let failure = outcome
        .state
        .tasks
        .values()
        .next()
        .unwrap()
        .last_failure
        .as_ref()
        .unwrap();
    assert_eq!(
        failure.evidence().code.as_deref(),
        Some("handoff.report_too_large")
    );
    assert!(h.oracle_runner.calls.lock().expect("lock").is_empty());
}
