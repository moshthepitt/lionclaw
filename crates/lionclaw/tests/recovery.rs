mod common;

use lionclaw_runtime_api::TypedFailure;
use std::sync::{Arc, Mutex};

use common::{
    approve_plan, covered_requirement, fault_append_events, harness, harness_with_type, proposal,
    review_runner, simple_plan, test_mission_type, BASE_SHA, HEAD_SHA,
};
use lionclaw::engine::{record_control, MissionDisposition};
use lionclaw::model::{
    Assertion, AssertionId, ControlAction, DecisionAction, Handoff, MissionEvent, MissionPhase,
    OracleName, OutputSemantics, PayloadRef, RoleAttemptDisposition, RoleEffectSource,
    RoleInstanceId, SettledHandoff,
};
use lionclaw::ports::{CapturedArtifact, OracleOutcome, RoleTurnOutcome};
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};

fn completed_work(base_sha: &str) -> RoleTurnOutcome {
    RoleTurnOutcome {
        handoff: Some(Handoff::Work {
            done: true,
            report: PayloadRef::inline("completed"),
            request_attention: false,
        }),
        artifact: Some(CapturedArtifact::for_testing(base_sha, HEAD_SHA)),
        prepared_inputs: Vec::new(),
        runtime_configuration: Default::default(),
        runtime_usage: Default::default(),
        final_response: String::new(),
    }
}

fn judgment_outcome(request: &lionclaw::ports::RoleTurnRequest) -> Option<RoleTurnOutcome> {
    (request.role.output == OutputSemantics::EmitsVerdict).then(|| RoleTurnOutcome {
        handoff: Some(Handoff::Validate {
            done: true,
            report: PayloadRef::inline("recovery reviewed"),
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
        final_response: "recovery reviewed".into(),
    })
}

#[tokio::test]
async fn an_inert_duplicate_outcome_fails_loudly_without_recovery_replay() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        review_runner(vec![(true, Vec::new())]),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "collided recovery", BASE_SHA)
        .await
        .expect("create");
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;

    let prompt_hash = "cf07194ee232eb531e15f690000d19846dea69cf05504782658afcfacb9228a2";
    let task_id = lionclaw::model::TaskId::new("fix").expect("task id");
    let role_instance = RoleInstanceId::new("implementer").unwrap();
    let effect_id = lionclaw::model::EffectId::for_role_turn(
        &mission_id,
        &role_instance,
        1,
        Some(&task_id),
        1,
        1,
        prompt_hash,
    );
    let interrupted = TypedFailure::Interrupted {
        evidence: Box::new(lionclaw_runtime_api::TypedFailureEvidence {
            code: Some("driver.interrupted".into()),
            detail: "the previous mission driver exited before recording an outcome; its resources were cleaned and the effect was not replayed".into(),
            stop_reason: Some("mission driver exited".into()),
            ..Default::default()
        }),
    };
    let orphan = lionclaw::store::NewEvent::new(MissionEvent::RoleTurnCompleted {
        effect_id: effect_id.clone(),
        outcome: Err(interrupted),
    });
    let state = h.engine.load_state(&mission_id).await.expect("state");
    let head = fault_append_events(dir.path(), &mission_id, state.head, &[orphan], 1).await;
    let database = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}",
        dir.path().join(".lionclaw/mission.db").display()
    ))
    .await
    .expect("open mission database");
    sqlx::query(
        "UPDATE mission_events SET effect_id = ?1, effect_class = 'outcome' \
         WHERE mission_id = ?2 AND sequence_no = ?3",
    )
    .bind(effect_id.as_str())
    .bind(mission_id.as_str())
    .bind(head as i64)
    .execute(&database)
    .await
    .expect("reserve the orphan outcome identity");
    let request = lionclaw::store::NewEvent::new(MissionEvent::RoleTurnRequested {
        role_instance,
        team_revision: 1,
        task_id: Some(task_id),
        assertion_ids: vec![AssertionId::new("TESTS-PASS").unwrap()],
        attempt_no: 1,
        effect_id: effect_id.clone(),
        prompt_template: lionclaw::model::RolePromptTemplate::Execution,
        prompt_hash: prompt_hash.into(),
        base_sha: BASE_SHA.into(),
        environment_digest: state.environment_digest().to_string(),
        dependency_refs: vec![],
        assignment_epoch: 1,
        message_boundary: head,
        presented_messages: vec![],
        workspace_preparation: lionclaw::model::WorkspacePreparation::ResetForAssignment,
        requested_at_ms: 0,
        deadline_ms: 100_000,
        budget_deadline_ms: 100_000,
    })
    .with_prompt_hash(prompt_hash);
    fault_append_events(dir.path(), &mission_id, head, &[request], 1).await;
    assert!(h
        .engine
        .load_state(&mission_id)
        .await
        .expect("state")
        .inflight
        .contains_key(&effect_id));

    let error = h
        .engine
        .advance(&mission_id)
        .await
        .expect_err("an inert duplicate must not be mistaken for settlement");
    assert!(
        error.to_string().contains("effect identity collision"),
        "got {error:#}"
    );
    assert!(h.role_runner.calls.lock().expect("calls").is_empty());
}

#[tokio::test]
async fn invalid_handoff_is_reworked_automatically_with_exact_feedback() {
    let dir = tempfile::tempdir().unwrap();
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let captured = prompts.clone();
    let calls = Arc::new(Mutex::new(0_u32));
    let seen = calls.clone();
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if let Some(outcome) = judgment_outcome(request) {
            return Ok(outcome);
        }
        captured.lock().unwrap().push(request.prompt.clone());
        let mut count = seen.lock().unwrap();
        *count += 1;
        if *count == 1 {
            return Ok(RoleTurnOutcome {
                handoff: Some(Handoff::Work {
                    done: false,
                    report: PayloadRef::inline("unfinished"),
                    request_attention: false,
                }),
                artifact: None,
                prepared_inputs: Vec::new(),
                runtime_configuration: Default::default(),
                runtime_usage: Default::default(),
                final_response: String::new(),
            });
        }
        Ok(completed_work(&request.base_sha))
    }));
    let h = harness(dir.path(), runner, MockOracleRunner::exiting(0)).await;
    let id = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "recover output", BASE_SHA)
        .await
        .unwrap();
    h.engine
        .propose_plan(&id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&h.engine, &id).await;

    let outcome = h.engine.advance(&id).await.unwrap();
    assert_eq!(outcome.disposition, MissionDisposition::Terminal);
    let prompts = prompts.lock().unwrap();
    assert_eq!(prompts.len(), 2);
    assert!(prompts[1].contains("Required rework"));
    assert!(prompts[1].contains("role reported done=false"));
    let calls = h.role_runner.calls.lock().unwrap();
    let task_calls = calls
        .iter()
        .filter(|call| call.0.as_ref() == Some(&lionclaw::model::TaskId::new("fix").unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(
        task_calls.iter().map(|call| call.1).collect::<Vec<_>>(),
        [1, 2]
    );
    assert_ne!(task_calls[0].2, task_calls[1].2);

    let task_id = lionclaw::model::TaskId::new("fix").unwrap();
    let mut receipts = outcome
        .state
        .role_attempt_receipts
        .values()
        .filter_map(|receipt| match &receipt.source {
            RoleEffectSource::Turn { request, .. }
                if request.task_id.as_ref() == Some(&task_id) =>
            {
                Some((request.attempt_no, receipt))
            }
            RoleEffectSource::Turn { .. } => None,
        })
        .collect::<Vec<_>>();
    receipts.sort_by_key(|(attempt_no, _)| *attempt_no);
    assert_eq!(receipts.len(), 2);
    assert_ne!(receipts[0].1.effect_id, receipts[1].1.effect_id);

    let first = receipts[0].1;
    assert!(first.final_response.is_none());
    assert_eq!(first.runtime_configuration, Some(Default::default()));
    assert!(first.handoff.is_none());
    let RoleAttemptDisposition::Failed { failure } = &first.disposition else {
        panic!("attempt 1 must retain its invalid-output disposition");
    };
    assert_eq!(
        failure.evidence().code.as_deref(),
        Some("role.success_contract")
    );
    assert_eq!(failure.detail(), "role reported done=false");

    let second = receipts[1].1;
    assert_eq!(second.final_response, Some(PayloadRef::inline("")));
    assert_eq!(second.runtime_configuration, Some(Default::default()));
    assert!(matches!(
        &second.handoff,
        Some(Handoff::Work { report, .. }) if report == &PayloadRef::inline("completed")
    ));
    let RoleAttemptDisposition::Succeeded {
        handoff: Some(handoff),
        artifact: Some(artifact),
    } = &second.disposition
    else {
        panic!("attempt 2 must retain its successful disposition");
    };
    assert_eq!(
        **handoff,
        SettledHandoff::Work {
            request_attention: false
        }
    );
    assert_eq!(artifact.base_sha, BASE_SHA);
    assert_eq!(artifact.head_sha, HEAD_SHA);
}

#[tokio::test]
async fn wrong_handoff_type_is_recorded_as_invalid_and_reworked() {
    let dir = tempfile::tempdir().unwrap();
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let captured = prompts.clone();
    let calls = Arc::new(Mutex::new(0_u32));
    let seen = calls.clone();
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if let Some(outcome) = judgment_outcome(request) {
            return Ok(outcome);
        }
        captured.lock().unwrap().push(request.prompt.clone());
        let mut count = seen.lock().unwrap();
        *count += 1;
        if *count == 1 {
            return Ok(RoleTurnOutcome {
                handoff: Some(Handoff::Validate {
                    done: true,
                    report: PayloadRef::inline("wrong schema"),
                    items: vec![],
                    passed: true,
                    request_attention: false,
                }),
                artifact: Some(CapturedArtifact::for_testing(
                    request.base_sha.clone(),
                    HEAD_SHA,
                )),
                prepared_inputs: Vec::new(),
                runtime_configuration: Default::default(),
                runtime_usage: Default::default(),
                final_response: "wrong schema response".into(),
            });
        }
        Ok(completed_work(&request.base_sha))
    }));
    let h = harness(dir.path(), runner, MockOracleRunner::exiting(0)).await;
    let id = h
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "recover wrong schema",
            BASE_SHA,
        )
        .await
        .unwrap();
    h.engine
        .propose_plan(&id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&h.engine, &id).await;

    let outcome = h.engine.advance(&id).await.unwrap();
    assert_eq!(outcome.disposition, MissionDisposition::Terminal);
    {
        let prompts = prompts.lock().unwrap();
        assert_eq!(prompts.len(), 2);
        assert!(prompts[1].contains("handoff type does not match this role's output semantics"));
    }
    let events = h.engine.store().load(&id).await.unwrap();
    assert!(events.iter().any(|event| matches!(
        &event.event,
        lionclaw::model::MissionEvent::RoleTurnCompleted {
            outcome: Err(TypedFailure::InvalidOutput { .. }),
            ..
        }
    )));
}

#[tokio::test]
async fn transient_runtime_failure_retries_but_launch_failure_parks_immediately() {
    let dir = tempfile::tempdir().unwrap();
    let attempts = Arc::new(Mutex::new(0_u32));
    let seen = attempts.clone();
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if let Some(outcome) = judgment_outcome(request) {
            return Ok(outcome);
        }
        let mut count = seen.lock().unwrap();
        *count += 1;
        if *count == 1 {
            return Err(TypedFailure::transient(
                "runtime.fixture",
                "provider temporarily unavailable",
                None,
            ));
        }
        assert!(request.prompt.contains("provider temporarily unavailable"));
        Ok(completed_work(&request.base_sha))
    }));
    let h = harness(dir.path(), runner, MockOracleRunner::exiting(0)).await;
    let id = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "retry transient", BASE_SHA)
        .await
        .unwrap();
    h.engine
        .propose_plan(&id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&h.engine, &id).await;
    assert_eq!(
        h.engine.advance(&id).await.unwrap().disposition,
        MissionDisposition::Terminal
    );
    assert_eq!(*attempts.lock().unwrap(), 2);

    let dir = tempfile::tempdir().unwrap();
    let runner = MockRoleRunner::new(Box::new(|request| {
        if let Some(outcome) = judgment_outcome(request) {
            return Ok(outcome);
        }
        Err(TypedFailure::permanent(
            "runtime.launch",
            "runtime profile is invalid",
        ))
    }));
    let h = harness(dir.path(), runner, MockOracleRunner::exiting(0)).await;
    let id = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "fail launch", BASE_SHA)
        .await
        .unwrap();
    h.engine
        .propose_plan(&id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&h.engine, &id).await;
    let view = h.engine.advance(&id).await.unwrap();
    assert_eq!(view.disposition, MissionDisposition::Parked);
    let attention: Vec<_> = view.state.open_attention.values().collect();
    assert_eq!(attention[0].id, "node_failed:fix");
    assert_eq!(h.role_runner.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_scheduled_transient_retry_can_be_stopped_before_runtime_launch() {
    let dir = tempfile::tempdir().unwrap();
    let attempts = Arc::new(Mutex::new(0_u32));
    let seen = attempts.clone();
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if let Some(outcome) = judgment_outcome(request) {
            return Ok(outcome);
        }
        let mut count = seen.lock().unwrap();
        *count += 1;
        if *count == 1 {
            Err(TypedFailure::transient(
                "runtime.fixture",
                "retry after a bounded delay",
                None,
            ))
        } else {
            panic!("stopped scheduled retry must never launch")
        }
    }));
    let h = harness(dir.path(), runner, MockOracleRunner::exiting(0)).await;
    let id = h
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "stop scheduled retry",
            BASE_SHA,
        )
        .await
        .unwrap();
    h.engine
        .propose_plan(&id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&h.engine, &id).await;
    let store = h.engine.store().clone();
    let engine = Arc::new(h.engine);
    let driver = tokio::spawn({
        let engine = engine.clone();
        let id = id.clone();
        async move { engine.advance(&id).await.unwrap() }
    });

    let (effect_id, not_before_ms, deadline_ms) = loop {
        let state = store.require_state(&id).await.unwrap();
        if let Some((effect_id, effect)) = state.inflight.iter().find(|(_, effect)| {
            matches!(
                effect,
                lionclaw::model::InflightEffect::RoleTurn { attempt_no: 2, .. }
            )
        }) {
            break (
                effect_id.clone(),
                effect.not_before_ms(),
                effect.deadline_ms(),
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };
    assert!(
        deadline_ms > not_before_ms,
        "the execution budget starts after backoff"
    );
    record_control(
        &store,
        1,
        &id,
        &effect_id,
        ControlAction::Stop,
        "operator stopped the scheduled retry",
    )
    .await
    .unwrap();

    let parked = driver.await.unwrap();
    assert_eq!(parked.disposition, MissionDisposition::Parked);
    assert_eq!(*attempts.lock().unwrap(), 1);
    let task_id = parked.state.tasks.keys().next().unwrap();
    assert_eq!(
        parked.state.task_last_failure(task_id).unwrap().category(),
        "operator_stopped"
    );
}

#[tokio::test]
async fn stopping_one_scheduled_oracle_retry_does_not_interrupt_its_sibling() {
    let dir = tempfile::tempdir().unwrap();
    let mut mission_type = test_mission_type();
    let lint = OracleName::new("lint").unwrap();
    mission_type.edit_for_testing(|definition| {
        definition
            .oracles
            .insert(lint.clone(), "/nonexistent/oracles/lint".into());
    });
    let attempts = Arc::new(Mutex::new(std::collections::BTreeMap::<String, u32>::new()));
    let seen = attempts.clone();
    let oracle = MockOracleRunner::new(Box::new(move |request| {
        let mut attempts = seen.lock().unwrap();
        let attempt = attempts.entry(request.oracle.to_string()).or_default();
        *attempt += 1;
        if *attempt == 1 {
            return Err(TypedFailure::transient(
                "oracle.fixture",
                "schedule both oracle retries",
                None,
            ));
        }
        Ok(OracleOutcome {
            exit_code: 0,
            exit_signal: None,
            stdout: Vec::new(),
            stderr: Vec::new(),
            prepared_inputs: Vec::new(),
            duration_ms: 1,
        })
    }));
    let h = harness_with_type(
        dir.path(),
        mission_type,
        review_runner(vec![(true, Vec::new())]),
        oracle,
    )
    .await;
    let mut plan = simple_plan();
    plan.requirements
        .push(covered_requirement("LINT-GREEN", "LINT-PASS"));
    plan.assertions.push(Assertion {
        id: AssertionId::new("LINT-PASS").unwrap(),
        prose: "lint exits successfully".into(),
        oracle: Some(lint),
    });
    plan.tasks[0]
        .targets
        .push(AssertionId::new("LINT-PASS").unwrap());
    let id = h
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "stop one scheduled oracle",
            BASE_SHA,
        )
        .await
        .unwrap();
    h.engine.propose_plan(&id, proposal(0, plan)).await.unwrap();
    approve_plan(&h.engine, &id).await;
    let store = h.engine.store().clone();
    let engine = Arc::new(h.engine);
    let driver = tokio::spawn({
        let engine = engine.clone();
        let id = id.clone();
        async move { engine.advance(&id).await.unwrap() }
    });

    let (stopped_effect, stopped_oracle) = loop {
        let state = store.require_state(&id).await.unwrap();
        let retries = state
            .inflight
            .iter()
            .filter_map(|(effect_id, effect)| match effect {
                lionclaw::model::InflightEffect::OracleRun {
                    oracle,
                    attempt_no: 2,
                    ..
                } => Some((effect_id.clone(), oracle.clone())),
                _ => None,
            })
            .collect::<Vec<_>>();
        if retries.len() == 2 {
            break retries[0].clone();
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };
    record_control(
        &store,
        1,
        &id,
        &stopped_effect,
        ControlAction::Stop,
        "stop one scheduled oracle retry",
    )
    .await
    .unwrap();

    let parked = driver.await.unwrap();
    assert_eq!(parked.disposition, MissionDisposition::Parked);
    let attempts = attempts.lock().unwrap();
    for (oracle, count) in attempts.iter() {
        assert_eq!(
            *count,
            if oracle == stopped_oracle.as_str() {
                1
            } else {
                2
            },
            "the stopped oracle must not launch again and its sibling must drain"
        );
    }
    assert!(parked
        .state
        .oracle_failures
        .values()
        .all(|failure| failure.category() != "interrupted"));
}

#[tokio::test]
async fn structured_transient_oracle_failure_uses_the_shared_retry_budget() {
    let dir = tempfile::tempdir().unwrap();
    let attempts = Arc::new(Mutex::new(0_u32));
    let seen = attempts.clone();
    let oracle = MockOracleRunner::new(Box::new(move |_| {
        let mut count = seen.lock().unwrap();
        *count += 1;
        if *count == 1 {
            return Err(TypedFailure::transient(
                "oracle.fixture",
                "oracle runtime temporarily unavailable",
                Some(1),
            ));
        }
        Ok(OracleOutcome {
            exit_code: 0,
            exit_signal: None,
            stdout: Vec::new(),
            stderr: Vec::new(),
            prepared_inputs: Vec::new(),
            duration_ms: 1,
        })
    }));
    let h = harness(dir.path(), review_runner(vec![(true, Vec::new())]), oracle).await;
    let id = h
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "retry transient oracle",
            BASE_SHA,
        )
        .await
        .unwrap();
    h.engine
        .propose_plan(&id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&h.engine, &id).await;

    let view = h.engine.advance(&id).await.unwrap();
    assert_eq!(view.disposition, MissionDisposition::Terminal);
    assert_eq!(*attempts.lock().unwrap(), 2);
    assert!(view.state.oracle_failures.is_empty());
    assert!(view.state.parked_effects.is_empty());
}

#[tokio::test]
async fn oracle_repair_reopens_the_owner_with_both_evidence_streams() {
    let dir = tempfile::tempdir().unwrap();
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let captured = prompts.clone();
    let worker = MockRoleRunner::new(Box::new(move |request| {
        if let Some(outcome) = judgment_outcome(request) {
            return Ok(outcome);
        }
        captured.lock().unwrap().push(request.prompt.clone());
        Ok(completed_work(&request.base_sha))
    }));
    let oracle_runs = Mutex::new(0_u32);
    let oracle = MockOracleRunner::new(Box::new(move |_| {
        let mut count = oracle_runs.lock().unwrap();
        *count += 1;
        Ok(if *count == 1 {
            OracleOutcome {
                exit_code: 1,
                exit_signal: None,
                stdout: b"test output".to_vec(),
                stderr: b"compiler diagnostic".to_vec(),
                prepared_inputs: Vec::new(),
                duration_ms: 1,
            }
        } else {
            OracleOutcome {
                exit_code: 0,
                exit_signal: None,
                stdout: Vec::new(),
                stderr: Vec::new(),
                prepared_inputs: Vec::new(),
                duration_ms: 1,
            }
        })
    }));
    let h = harness(dir.path(), worker, oracle).await;
    let id = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "repair failure", BASE_SHA)
        .await
        .unwrap();
    h.engine
        .propose_plan(&id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&h.engine, &id).await;
    assert_eq!(
        h.engine.advance(&id).await.unwrap().disposition,
        MissionDisposition::Parked
    );
    h.engine
        .decide(
            &id,
            "oracle_verdict_failed:cargo-test",
            DecisionAction::Repair,
            "fix the compiler error",
        )
        .await
        .unwrap();
    let view = h.engine.advance(&id).await.unwrap();
    assert_eq!(view.disposition, MissionDisposition::Terminal);
    assert!(matches!(view.state.phase, MissionPhase::Done { .. }));
    let prompts = prompts.lock().unwrap();
    assert_eq!(prompts.len(), 2);
    for expected in [
        "fix the compiler error",
        "stdout:\ntest output",
        "stderr:\ncompiler diagnostic",
    ] {
        assert!(prompts[1].contains(expected), "missing {expected:?}");
    }
}
