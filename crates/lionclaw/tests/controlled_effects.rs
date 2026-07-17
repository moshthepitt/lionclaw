mod common;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common::{
    approve_plan, default_config, proposal, simple_plan, test_mission_type, BASE_SHA, HEAD_SHA,
};
use lionclaw::engine::{record_control, Engine, EngineServices, MissionDisposition};
use lionclaw::model::{ArtifactOutcome, ControlAction, Handoff, PayloadRef, TaskStatus};
use lionclaw::ports::{
    EffectCleaner, EffectCleanupFailure, EffectCleanupRequest, ExecutionControl, OracleOutcome,
    RoleRunOutcome, RoleRunRequest, RoleRunner,
};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockClock, MockOracleRunner, NoopEffectCleaner};
use lionclaw_runtime_api::{RuntimeEvent, TurnEvent, TypedFailure, TypedFailureEvidence};
use tokio::sync::{Barrier, Notify};

struct RealClock;

impl lionclaw::ports::Clock for RealClock {
    #[expect(
        clippy::disallowed_methods,
        reason = "exercises a real deadline boundary"
    )]
    fn now_ms(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }
}

struct ControlledRunner {
    started: Arc<Barrier>,
    requests: Arc<Mutex<Vec<(String, u32)>>>,
}

struct SleepingRunner;

struct DeadlineRunner {
    calls: Arc<std::sync::atomic::AtomicUsize>,
}

struct SettlementCleaner {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    discards: Arc<Mutex<Vec<bool>>>,
    pause_on: usize,
}

#[async_trait]
impl EffectCleaner for SettlementCleaner {
    async fn cleanup(&self, request: EffectCleanupRequest) -> Result<(), EffectCleanupFailure> {
        let first = {
            let mut discards = self.discards.lock().unwrap();
            discards.push(request.discard_artifact);
            discards.len() == self.pause_on
        };
        if first {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(())
    }
}

#[async_trait]
impl RoleRunner for DeadlineRunner {
    async fn run(&self, mut request: RoleRunRequest) -> Result<RoleRunOutcome, TypedFailure> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        loop {
            match request.control.borrow().clone() {
                ExecutionControl::DeadlineExhausted => {
                    return Err(TypedFailure::DeadlineExhausted {
                        evidence: Box::new(TypedFailureEvidence::new(
                            Some("test.deadline".into()),
                            "engine delivered the durable deadline control",
                        )),
                    });
                }
                ExecutionControl::Stop(reason) => {
                    let mut evidence =
                        TypedFailureEvidence::new(Some("test.stop".into()), "unexpected stop");
                    evidence.stop_reason = Some(reason);
                    return Err(TypedFailure::OperatorStopped {
                        evidence: Box::new(evidence),
                    });
                }
                ExecutionControl::RunUntil(_) => {}
            }
            request.control.changed().await.unwrap();
        }
    }
}

#[async_trait]
impl RoleRunner for SleepingRunner {
    async fn run(&self, request: RoleRunRequest) -> Result<RoleRunOutcome, TypedFailure> {
        tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
        Ok(RoleRunOutcome {
            handoff: Handoff::Work {
                done: true,
                report: PayloadRef::inline("worked beyond initial deadline"),
                request_attention: false,
            },
            artifact: Some(ArtifactOutcome {
                base_sha: request.base_sha,
                head_sha: HEAD_SHA.into(),
            }),
            runtime_configuration: Default::default(),
            final_response: String::new(),
        })
    }
}

#[async_trait]
impl RoleRunner for ControlledRunner {
    async fn run(&self, mut request: RoleRunRequest) -> Result<RoleRunOutcome, TypedFailure> {
        let invocation = {
            let mut requests = self.requests.lock().unwrap();
            requests.push((request.base_sha.clone(), request.assignment_epoch));
            requests.len()
        };
        if invocation == 1 {
            for index in 0..10_000 {
                request.activity.send_replace(Some((
                    request.effect_id.clone(),
                    TurnEvent::canonical(RuntimeEvent::Status {
                        code: Some("progress".into()),
                        text: format!("event {index}"),
                    }),
                )));
            }
            self.started.wait().await;
            loop {
                let control = request.control.borrow().clone();
                if let ExecutionControl::Stop(reason) = control {
                    let mut evidence = TypedFailureEvidence::new(
                        Some("test.stopped".into()),
                        "controlled runner stopped",
                    );
                    evidence.stop_reason = Some(reason);
                    return Err(TypedFailure::OperatorStopped {
                        evidence: Box::new(evidence),
                    });
                }
                request.control.changed().await.unwrap();
            }
        }
        Ok(RoleRunOutcome {
            handoff: Handoff::Work {
                done: true,
                report: PayloadRef::inline("continued in the same task workspace"),
                request_attention: false,
            },
            artifact: Some(ArtifactOutcome {
                base_sha: request.base_sha,
                head_sha: HEAD_SHA.into(),
            }),
            runtime_configuration: Default::default(),
            final_response: "completed after continue".into(),
        })
    }
}

#[tokio::test]
async fn stop_parks_exact_generation_and_continue_preserves_assignment() {
    let dir = tempfile::tempdir().unwrap();
    let store = MissionStore::open(dir.path()).await.unwrap();
    let started = Arc::new(Barrier::new(2));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let engine = Arc::new(Engine::new(
        store.clone(),
        test_mission_type(),
        "codex".into(),
        "test-image".into(),
        EngineServices::new(
            Arc::new(ControlledRunner {
                started: started.clone(),
                requests: requests.clone(),
            }),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    ));
    let mission_id = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "control one exact effect",
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

    let handshake = dir.path().join("driver.ready");
    let driver = tokio::spawn({
        let engine = engine.clone();
        let mission_id = mission_id.clone();
        let handshake = handshake.clone();
        async move {
            engine
                .advance_with_handshake(&mission_id, Some(&handshake))
                .await
                .unwrap()
        }
    });
    started.wait().await;
    assert!(
        handshake.is_file(),
        "lock-owned startup handshake is published"
    );
    let activity_path = lionclaw::activity::path(
        &store
            .lionclaw_dir()
            .join("missions")
            .join(mission_id.as_str()),
    );
    let activity = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if let Ok(bytes) = std::fs::read(&activity_path) {
                if let Ok(projection) =
                    serde_json::from_slice::<lionclaw::activity::ActivityProjection>(&bytes)
                {
                    if projection
                        .effects
                        .first()
                        .is_some_and(|effect| effect.last_activity == "event 9999")
                    {
                        break projection;
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("background activity reporter publishes the coalesced observation");
    assert_eq!(activity.effects.len(), 1);
    assert_eq!(activity.effects[0].applied_model, None);
    assert_eq!(activity.effects[0].applied_mode, None);
    assert_eq!(activity.effects[0].last_activity, "event 9999");
    assert_eq!(
        activity.effects[0].legal_controls,
        ["stop", "extend_deadline"]
    );
    let active = store.require_state(&mission_id).await.unwrap();
    let (effect_id, effect) = active.inflight.iter().next().unwrap();
    let effect_id = effect_id.clone();
    let original_deadline = effect.deadline_ms();
    assert!(record_control(
        &store,
        1,
        &mission_id,
        &effect_id,
        ControlAction::ExtendDeadline {
            old_deadline_ms: original_deadline,
            new_deadline_ms: original_deadline + 1_000,
            automatic: true,
        },
        "forged policy extension",
    )
    .await
    .unwrap_err()
    .to_string()
    .contains("engine-owned"));
    record_control(
        &store,
        1,
        &mission_id,
        &effect_id,
        ControlAction::ExtendDeadline {
            old_deadline_ms: original_deadline,
            new_deadline_ms: original_deadline + 1_000,
            automatic: false,
        },
        "task is making progress",
    )
    .await
    .unwrap();
    assert!(record_control(
        &store,
        2,
        &mission_id,
        &effect_id,
        ControlAction::ExtendDeadline {
            old_deadline_ms: original_deadline,
            new_deadline_ms: original_deadline + 2_000,
            automatic: false,
        },
        "stale extension",
    )
    .await
    .unwrap_err()
    .to_string()
    .contains("stale"));
    record_control(
        &store,
        3,
        &mission_id,
        &effect_id,
        ControlAction::Stop,
        "operator requested a checkpoint",
    )
    .await
    .unwrap();

    let parked = driver.await.unwrap();
    assert_eq!(parked.disposition, MissionDisposition::Parked);
    assert_eq!(
        parked.state.tasks.values().next().unwrap().status,
        TaskStatus::Failed
    );
    assert!(parked.state.parked_effects.contains_key(&effect_id));
    assert_eq!(
        parked.next_actions(),
        ["mission continue", "mission decide"]
    );
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
    assert!(record_control(
        &store,
        4,
        &mission_id,
        &common::effect_id("successor"),
        ControlAction::Continue { automatic: false },
        "wrong generation",
    )
    .await
    .is_err());

    record_control(
        &store,
        5,
        &mission_id,
        &effect_id,
        ControlAction::Continue { automatic: false },
        "resume preserved work",
    )
    .await
    .unwrap();
    assert_eq!(
        store
            .require_state(&mission_id)
            .await
            .unwrap()
            .tasks
            .values()
            .next()
            .unwrap()
            .status,
        TaskStatus::Pending
    );
    let checkpoint = engine.advance(&mission_id).await.unwrap();
    assert_eq!(checkpoint.disposition, MissionDisposition::Terminal);
    assert_eq!(
        requests.lock().unwrap().as_slice(),
        &[(BASE_SHA.into(), 1), (BASE_SHA.into(), 1)]
    );
    assert!(record_control(
        &store,
        6,
        &mission_id,
        &effect_id,
        ControlAction::Continue { automatic: false },
        "must not reopen a terminal mission",
    )
    .await
    .unwrap_err()
    .to_string()
    .contains("terminal"));
}

#[tokio::test]
async fn durable_stop_wins_the_outcome_append_race_and_discards_the_candidate() {
    let dir = tempfile::tempdir().unwrap();
    let store = MissionStore::open(dir.path()).await.unwrap();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let discards = Arc::new(Mutex::new(Vec::new()));
    let engine = Arc::new(Engine::new(
        store.clone(),
        test_mission_type(),
        "codex".into(),
        "test-image".into(),
        EngineServices::new(
            Arc::new(lionclaw::testing::MockRoleRunner::happy(HEAD_SHA)),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(SettlementCleaner {
                entered: entered.clone(),
                release: release.clone(),
                discards: discards.clone(),
                pause_on: 1,
            }),
            Arc::new(MockClock::default()),
        ),
    ));
    let mission_id = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "linearize a stop against completion",
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

    let driver = tokio::spawn({
        let engine = engine.clone();
        let mission_id = mission_id.clone();
        async move { engine.advance(&mission_id).await.unwrap() }
    });
    entered.notified().await;
    let active = store.require_state(&mission_id).await.unwrap();
    let effect_id = active.inflight.keys().next().unwrap().clone();
    record_control(
        &store,
        1,
        &mission_id,
        &effect_id,
        ControlAction::Stop,
        "stop won before the outcome became durable",
    )
    .await
    .unwrap();
    release.notify_one();

    let parked = driver.await.unwrap();
    assert_eq!(parked.disposition, MissionDisposition::Parked);
    let failure = parked
        .state
        .tasks
        .values()
        .next()
        .unwrap()
        .last_failure
        .as_ref()
        .unwrap();
    assert!(matches!(failure, TypedFailure::OperatorStopped { .. }));
    assert_eq!(
        failure.evidence().stop_reason.as_deref(),
        Some("stop won before the outcome became durable")
    );
    assert_eq!(failure.evidence().final_response, "did the work");
    assert_eq!(
        failure.evidence().configuration.applied_model.as_deref(),
        Some("mock-model")
    );
    assert_eq!(discards.lock().unwrap().as_slice(), &[false, true]);
    let events = store.load(&mission_id).await.unwrap();
    assert!(!events.iter().any(|event| matches!(
        event.event,
        lionclaw::model::MissionEvent::RoleRunCompleted { outcome: Ok(_), .. }
    )));
}

#[tokio::test]
async fn settlement_retains_bounded_blob_backed_oracle_stderr() {
    let dir = tempfile::tempdir().unwrap();
    let store = MissionStore::open(dir.path()).await.unwrap();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let discards = Arc::new(Mutex::new(Vec::new()));
    let oracle = MockOracleRunner::new(Box::new(|_| {
        Ok(OracleOutcome {
            exit_code: 1,
            exit_signal: None,
            stdout: Vec::new(),
            stderr: vec![b'E'; 128 * 1024],
            prepared_inputs: Vec::new(),
            duration_ms: 42,
        })
    }));
    let engine = Arc::new(Engine::new(
        store.clone(),
        test_mission_type(),
        "codex".into(),
        "test-image".into(),
        EngineServices::new(
            Arc::new(lionclaw::testing::MockRoleRunner::happy(HEAD_SHA)),
            Arc::new(oracle),
            Arc::new(SettlementCleaner {
                entered: entered.clone(),
                release: release.clone(),
                discards: discards.clone(),
                pause_on: 2,
            }),
            Arc::new(MockClock::default()),
        ),
    ));
    let mission_id = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "preserve large oracle evidence across settlement",
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

    let driver = tokio::spawn({
        let engine = engine.clone();
        let mission_id = mission_id.clone();
        async move { engine.advance(&mission_id).await.unwrap() }
    });
    entered.notified().await;
    let active = store.require_state(&mission_id).await.unwrap();
    let effect_id = active.inflight.keys().next().unwrap().clone();
    assert!(matches!(
        active.inflight.get(&effect_id),
        Some(lionclaw::model::InflightEffect::OracleRun { .. })
    ));
    record_control(
        &store,
        1,
        &mission_id,
        &effect_id,
        ControlAction::Stop,
        "stop won after large oracle output",
    )
    .await
    .unwrap();
    release.notify_one();

    let parked = driver.await.unwrap();
    assert_eq!(parked.disposition, MissionDisposition::Parked);
    let failure = parked.state.oracle_failures.values().next().unwrap();
    assert!(matches!(failure, TypedFailure::OperatorStopped { .. }));
    assert!(!failure.evidence().stderr.is_empty());
    assert!(failure.evidence().stderr.len() <= lionclaw_runtime_api::FAILURE_TEXT_LIMIT);
    assert!(failure.evidence().stderr.starts_with('E'));
    assert_eq!(discards.lock().unwrap().as_slice(), &[false, true]);
}

#[tokio::test]
async fn deadline_is_durably_linearized_before_one_adapter_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let store = MissionStore::open(dir.path()).await.unwrap();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let engine = Engine::new(
        store.clone(),
        test_mission_type(),
        "codex".into(),
        "test-image".into(),
        EngineServices::new(
            Arc::new(DeadlineRunner {
                calls: calls.clone(),
            }),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(RealClock),
        ),
    );
    let mut config = default_config();
    config.execution.default_timeout_secs = 1;
    config.execution.max_task_time_secs = 1;
    let mission_id = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "linearize deadline",
            BASE_SHA,
            config,
        )
        .await
        .unwrap();
    engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&engine, &mission_id).await;

    let parked = engine.advance(&mission_id).await.unwrap();
    assert_eq!(parked.disposition, MissionDisposition::Parked);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(parked.state.reached_deadlines.is_empty());
    let events = store.load(&mission_id).await.unwrap();
    let reached = events
        .iter()
        .position(|event| {
            matches!(
                event.event,
                lionclaw::model::MissionEvent::EffectDeadlineReached { .. }
            )
        })
        .unwrap();
    let completed = events
        .iter()
        .position(|event| {
            matches!(
                event.event,
                lionclaw::model::MissionEvent::RoleRunCompleted {
                    outcome: Err(TypedFailure::DeadlineExhausted { .. }),
                    ..
                }
            )
        })
        .unwrap();
    assert!(reached < completed);
}

#[tokio::test]
async fn finite_policy_budget_extends_before_the_initial_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let store = MissionStore::open(dir.path()).await.unwrap();
    let engine = Engine::new(
        store.clone(),
        test_mission_type(),
        "codex".into(),
        "test-image".into(),
        EngineServices::new(
            Arc::new(SleepingRunner),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(RealClock),
        ),
    );
    let mission_id = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "exercise finite policy extension",
            BASE_SHA,
            lionclaw::model::MissionConfig {
                execution: lionclaw::model::ExecutionPolicy {
                    default_timeout_secs: 1,
                    max_task_time_secs: 2,
                    extension_step_secs: 1,
                    auto_continue_candidate: false,
                    auto_continue_proof: false,
                },
                ..default_config()
            },
        )
        .await
        .unwrap();
    engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&engine, &mission_id).await;

    let checkpoint = engine.advance(&mission_id).await.unwrap();
    assert_eq!(checkpoint.disposition, MissionDisposition::Ready);
    let events = store.load(&mission_id).await.unwrap();
    assert!(events.iter().any(|event| matches!(
        &event.event,
        lionclaw::model::MissionEvent::ControlRequested {
            action: ControlAction::ExtendDeadline {
                automatic: true,
                ..
            },
            ..
        }
    )));
}

#[tokio::test]
async fn policy_auto_continues_candidate_and_proof_with_recorded_controls() {
    let dir = tempfile::tempdir().unwrap();
    let store = MissionStore::open(dir.path()).await.unwrap();
    let engine = Engine::new(
        store.clone(),
        test_mission_type(),
        "codex".into(),
        "test-image".into(),
        EngineServices::new(
            Arc::new(lionclaw::testing::MockRoleRunner::happy(HEAD_SHA)),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );
    let mission_id = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "record automatic checkpoints",
            BASE_SHA,
            lionclaw::model::MissionConfig {
                execution: lionclaw::model::ExecutionPolicy {
                    auto_continue_candidate: true,
                    auto_continue_proof: true,
                    ..Default::default()
                },
                ..default_config()
            },
        )
        .await
        .unwrap();
    engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&engine, &mission_id).await;

    let finished = engine.advance(&mission_id).await.unwrap();
    assert_eq!(finished.disposition, MissionDisposition::Terminal);
    let automatic = store
        .load(&mission_id)
        .await
        .unwrap()
        .iter()
        .filter(|event| {
            matches!(
                &event.event,
                lionclaw::model::MissionEvent::ControlRequested {
                    action: ControlAction::Continue { automatic: true },
                    ..
                }
            )
        })
        .count();
    assert_eq!(automatic, 2);
}

#[tokio::test]
async fn direct_mission_creation_rejects_an_invalid_execution_policy() {
    let dir = tempfile::tempdir().unwrap();
    let store = MissionStore::open(dir.path()).await.unwrap();
    let engine = Engine::new(
        store,
        test_mission_type(),
        "codex".into(),
        "test-image".into(),
        EngineServices::new(
            Arc::new(lionclaw::testing::MockRoleRunner::happy(HEAD_SHA)),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );
    let mut config = default_config();
    config.execution.default_timeout_secs = 0;

    let error = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "reject invalid policy",
            BASE_SHA,
            config,
        )
        .await
        .expect_err("direct callers must not persist invalid execution policy");

    assert!(error.to_string().contains("invalid execution policy"));
}

#[tokio::test]
async fn mission_creation_rejects_deadlines_unrepresentable_at_its_epoch() {
    let dir = tempfile::tempdir().unwrap();
    let mission_type = test_mission_type();
    let store = MissionStore::open(dir.path()).await.unwrap();
    let engine = Engine::new(
        store,
        mission_type,
        "codex".into(),
        "test-image".into(),
        EngineServices::new(
            Arc::new(lionclaw::testing::MockRoleRunner::happy(HEAD_SHA)),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );
    let mut config = default_config();
    config.execution.max_task_time_secs = lionclaw::model::MAX_EXECUTION_DURATION_SECS;

    let error = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "reject an impossible absolute deadline",
            BASE_SHA,
            config,
        )
        .await
        .expect_err("policy deadline must fit at the mission epoch");
    assert!(
        error.to_string().contains("deadline"),
        "unexpected error: {error:#}"
    );
}

#[tokio::test]
async fn mission_creation_rejects_unrepresentable_role_deadlines() {
    let dir = tempfile::tempdir().unwrap();
    let mut mission_type = test_mission_type();
    mission_type.roles.values_mut().next().unwrap().timeout_secs =
        Some(lionclaw::model::MAX_EXECUTION_DURATION_SECS);
    let store = MissionStore::open(dir.path()).await.unwrap();
    let engine = Engine::new(
        store,
        mission_type,
        "codex".into(),
        "test-image".into(),
        EngineServices::new(
            Arc::new(lionclaw::testing::MockRoleRunner::happy(HEAD_SHA)),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );

    let error = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "reject an impossible role deadline",
            BASE_SHA,
            default_config(),
        )
        .await
        .expect_err("role deadline must fit at the mission epoch");
    assert!(
        error.to_string().contains("deadline"),
        "unexpected error: {error:#}"
    );
}
