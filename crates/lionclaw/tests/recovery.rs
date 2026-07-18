mod common;

use lionclaw_runtime_api::TypedFailure;
use std::sync::{Arc, Mutex};

use common::{
    approve_plan, covered_requirement, fault_append_events, harness, harness_with_type, proposal,
    simple_plan, test_mission_type, BASE_SHA, HEAD_SHA,
};
use lionclaw::engine::{record_control, MissionDisposition};
use lionclaw::model::{
    Assertion, AssertionId, ControlAction, DecisionAction, Handoff, MissionEvent, MissionPhase,
    OracleName, OutputSemantics, PayloadRef, RoleName, TaskNamespace,
};
use lionclaw::ports::{CapturedArtifact, OracleOutcome, RoleRunOutcome};
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};

fn completed_work(base_sha: &str) -> RoleRunOutcome {
    RoleRunOutcome {
        handoff: Some(Handoff::Work {
            done: true,
            report: PayloadRef::inline("completed"),
            request_attention: false,
        }),
        artifact: Some(CapturedArtifact::for_testing(base_sha, HEAD_SHA)),
        runtime_configuration: Default::default(),
        final_response: String::new(),
    }
}

#[tokio::test]
async fn an_inert_duplicate_outcome_fails_loudly_without_recovery_replay() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
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
    let effect_id = lionclaw::model::EffectId::for_role_request(
        TaskNamespace::Execution,
        &mission_id,
        &task_id,
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
    let orphan = lionclaw::store::NewEvent::new(MissionEvent::RoleRunCompleted {
        namespace: TaskNamespace::Execution,
        task_id: task_id.clone(),
        attempt_no: 1,
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
    let request = lionclaw::store::NewEvent::new(MissionEvent::RoleRunRequested {
        conversation_id: lionclaw::model::ConversationId::for_role_instance(
            &mission_id,
            TaskNamespace::Execution,
            &task_id,
            &RoleName::new("implementer").unwrap(),
            1,
        ),
        namespace: TaskNamespace::Execution,
        task_id,
        attempt_no: 1,
        effect_id: effect_id.clone(),
        role: RoleName::new("implementer").expect("role name"),
        output: OutputSemantics::ProducesArtifact,
        runtime: "codex".into(),
        prompt: PayloadRef::inline("prompt"),
        base_sha: BASE_SHA.into(),
        assignment_epoch: 1,
        message_boundary: head,
        presented_messages: vec![],
        recreate_workspace: true,
        requested_at_ms: 0,
        not_before_ms: 0,
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
        captured.lock().unwrap().push(request.prompt.clone());
        let mut count = seen.lock().unwrap();
        *count += 1;
        if *count == 1 {
            return Ok(RoleRunOutcome {
                handoff: Some(Handoff::Work {
                    done: false,
                    report: PayloadRef::inline("unfinished"),
                    request_attention: false,
                }),
                artifact: None,
                runtime_configuration: Default::default(),
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
    assert_eq!(calls.iter().map(|call| call.1).collect::<Vec<_>>(), [1, 2]);
    assert_ne!(calls[0].2, calls[1].2);
}

#[tokio::test]
async fn wrong_handoff_schema_is_recorded_as_invalid_and_reworked() {
    let dir = tempfile::tempdir().unwrap();
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let captured = prompts.clone();
    let calls = Arc::new(Mutex::new(0_u32));
    let seen = calls.clone();
    let runner = MockRoleRunner::new(Box::new(move |request| {
        captured.lock().unwrap().push(request.prompt.clone());
        let mut count = seen.lock().unwrap();
        *count += 1;
        if *count == 1 {
            return Ok(RoleRunOutcome {
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
                runtime_configuration: Default::default(),
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
        assert!(prompts[1].contains("does not match the effect output contract"));
    }
    let events = h.engine.store().load(&id).await.unwrap();
    assert!(events.iter().any(|event| matches!(
        &event.event,
        lionclaw::model::MissionEvent::RoleRunCompleted {
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
    let runner = MockRoleRunner::new(Box::new(|_| {
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
    let runner = MockRoleRunner::new(Box::new(move |_| {
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
                lionclaw::model::InflightEffect::RoleRun { attempt_no: 2, .. }
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
    assert_eq!(
        parked
            .state
            .tasks
            .values()
            .next()
            .unwrap()
            .last_failure
            .as_ref()
            .unwrap()
            .category(),
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
        MockRoleRunner::happy(HEAD_SHA),
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
    let h = harness(dir.path(), MockRoleRunner::happy(HEAD_SHA), oracle).await;
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
