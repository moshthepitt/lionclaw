mod common;

use lionclaw::model::TerminalState;
use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use common::{
    approve_plan, covered_requirement, initialize_repository, proposal, simple_plan,
    test_mission_type, BASE_SHA, HEAD_SHA,
};
use lionclaw::engine::{record_control, record_message, Engine, EngineServices, MessageCommand};
use lionclaw::model::{
    fold, Assertion, AssertionId, Choice, ControlAction, DeliveryMarker, EffectIntent, Handoff,
    MessageReference, OracleName, OutputSemantics, PayloadRef, RuntimeConfigurationEvidence,
    TaskId, TaskStatus, REDUCER_VERSION,
};
use lionclaw::ports::{
    EffectCleaner, EffectCleanupFailure, EffectCleanupRequest, ExecutionControl, OracleOutcome,
    OracleRunRequest, OracleRunner, RoleRunner, RoleTurnOutcome, RoleTurnRequest,
};
use lionclaw::store::MissionStore;
use lionclaw::testing::{
    capture_prepared_test_artifact, prepare_test_workspace, MockClock, MockOracleRunner,
    NoopEffectCleaner,
};
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

struct RawSuccessAfterStopRunner {
    started: Arc<Barrier>,
}

struct SleepingRunner;

struct ArtifactlessWriter;

struct DeadlineRunner {
    calls: Arc<std::sync::atomic::AtomicUsize>,
}

#[derive(Clone, Copy, Debug)]
enum SettlementRaceOutcome {
    Success,
    Failure,
    InvalidOutput,
    Question,
}

struct SettlementRaceRunner {
    outcome: SettlementRaceOutcome,
    deadline_barrier: Option<Arc<SettlementDeadlineBarrier>>,
}

struct SettlementDeadlineBarrier {
    runner_ready: Notify,
    release_runner: Notify,
}

struct ControlledDeadlineClock {
    now_ms: AtomicI64,
}

impl ControlledDeadlineClock {
    fn new(now_ms: i64) -> Self {
        Self {
            now_ms: AtomicI64::new(now_ms),
        }
    }

    fn advance_past(&self, deadline_ms: i64) {
        self.now_ms
            .store(deadline_ms.saturating_add(1), Ordering::SeqCst);
    }
}

impl lionclaw::ports::Clock for ControlledDeadlineClock {
    fn now_ms(&self) -> i64 {
        self.now_ms.load(Ordering::SeqCst)
    }
}

async fn confirm_completed_turn(
    _request: &RoleTurnRequest,
    _final_response: &str,
    _configuration: &RuntimeConfigurationEvidence,
) -> Result<(), TypedFailure> {
    Ok(())
}

async fn confirm_failed_turn(
    _request: &RoleTurnRequest,
    _failure: &TypedFailure,
) -> Result<(), TypedFailure> {
    Ok(())
}

fn judgment_outcome(request: &RoleTurnRequest) -> Option<RoleTurnOutcome> {
    (request.role.output == OutputSemantics::EmitsVerdict).then(|| RoleTurnOutcome {
        handoff: Some(Handoff::Validate {
            done: true,
            report: PayloadRef::inline("judged"),
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
}

fn happy_team_runner() -> lionclaw::testing::MockRoleRunner {
    lionclaw::testing::MockRoleRunner::new(Box::new(|request| {
        if let Some(outcome) = judgment_outcome(request) {
            return Ok(outcome);
        }
        Ok(RoleTurnOutcome {
            handoff: Some(Handoff::Work {
                done: true,
                report: PayloadRef::inline("did the work"),
                request_attention: false,
            }),
            artifact: Some(lionclaw::ports::CapturedArtifact::for_testing(
                request.base_sha.clone(),
                HEAD_SHA,
            )),
            prepared_inputs: Vec::new(),
            runtime_configuration: RuntimeConfigurationEvidence {
                requested_model: Some("mock-model".into()),
                applied_model: Some("mock-model".into()),
                ..Default::default()
            },
            runtime_usage: Default::default(),
            final_response: "did the work".into(),
        })
    }))
}

#[async_trait]
impl RoleRunner for SettlementRaceRunner {
    async fn run(&self, request: RoleTurnRequest) -> Result<RoleTurnOutcome, TypedFailure> {
        if let Some(outcome) = judgment_outcome(&request) {
            return Ok(outcome);
        }
        prepare_test_workspace(&request).await?;
        if let Some(barrier) = &self.deadline_barrier {
            barrier.runner_ready.notify_one();
            barrier.release_runner.notified().await;
        }
        let configuration = RuntimeConfigurationEvidence {
            requested_model: Some("requested-race-model".into()),
            applied_model: Some("applied-race-model".into()),
            model_confirmation: Some(lionclaw::model::RuntimeConfigurationConfirmation::Observed),
            requested_mode: Some("requested-race-mode".into()),
            applied_mode: Some("applied-race-mode".into()),
            mode_confirmation: Some(lionclaw::model::RuntimeConfigurationConfirmation::Observed),
        };
        match self.outcome {
            SettlementRaceOutcome::Success => {
                let report = PayloadRef::inline("race success");
                let final_response = "success response retained";
                confirm_completed_turn(&request, final_response, &configuration).await?;
                let artifact = capture_prepared_test_artifact(&request, HEAD_SHA).await?;
                Ok(RoleTurnOutcome {
                    handoff: Some(Handoff::Work {
                        done: true,
                        report,
                        request_attention: false,
                    }),
                    artifact: Some(artifact),
                    prepared_inputs: Vec::new(),
                    runtime_configuration: configuration,
                    runtime_usage: Default::default(),
                    final_response: final_response.into(),
                })
            }
            SettlementRaceOutcome::Question => {
                let final_response = "Which exact target should I use?";
                confirm_completed_turn(&request, final_response, &configuration).await?;
                Ok(RoleTurnOutcome {
                    handoff: None,
                    artifact: None,
                    prepared_inputs: Vec::new(),
                    runtime_configuration: configuration,
                    runtime_usage: Default::default(),
                    final_response: final_response.into(),
                })
            }
            SettlementRaceOutcome::Failure | SettlementRaceOutcome::InvalidOutput => {
                let mut evidence = TypedFailureEvidence::new(
                    Some(
                        match self.outcome {
                            SettlementRaceOutcome::Failure => "race.failure",
                            SettlementRaceOutcome::InvalidOutput => "handoff.schema",
                            SettlementRaceOutcome::Success | SettlementRaceOutcome::Question => {
                                unreachable!()
                            }
                        }
                        .into(),
                    ),
                    match self.outcome {
                        SettlementRaceOutcome::Failure => "ordinary observed failure",
                        SettlementRaceOutcome::InvalidOutput => "observed invalid output",
                        SettlementRaceOutcome::Success | SettlementRaceOutcome::Question => {
                            unreachable!()
                        }
                    },
                );
                evidence.final_response = match self.outcome {
                    SettlementRaceOutcome::Failure => "failure response retained",
                    SettlementRaceOutcome::InvalidOutput => "invalid response retained",
                    SettlementRaceOutcome::Success | SettlementRaceOutcome::Question => {
                        unreachable!()
                    }
                }
                .into();
                evidence.configuration = configuration;
                let failure = if matches!(self.outcome, SettlementRaceOutcome::InvalidOutput) {
                    TypedFailure::InvalidOutput {
                        evidence: Box::new(evidence),
                    }
                } else {
                    TypedFailure::PermanentRuntime {
                        evidence: Box::new(evidence),
                    }
                };
                confirm_failed_turn(&request, &failure).await?;
                Err(failure)
            }
        }
    }
}

fn test_repository() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    initialize_repository(dir.path());
    dir
}

struct AbortOracleRunner {
    blocked_started: Arc<Notify>,
    abort_observed: Arc<std::sync::atomic::AtomicBool>,
    calls: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl OracleRunner for AbortOracleRunner {
    async fn run(&self, mut request: OracleRunRequest) -> Result<OracleOutcome, TypedFailure> {
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            return Ok(OracleOutcome {
                exit_code: 1,
                exit_signal: None,
                stdout: Vec::new(),
                stderr: b"first oracle failed".to_vec(),
                prepared_inputs: Vec::new(),
                duration_ms: 1,
            });
        }

        self.blocked_started.notify_one();
        loop {
            match request.control.borrow().clone() {
                ExecutionControl::Abort(reason) => {
                    self.abort_observed
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    let mut evidence =
                        TypedFailureEvidence::new(Some("test.aborted".into()), "mission aborted");
                    evidence.stop_reason = Some(reason);
                    return Err(TypedFailure::OperatorAborted {
                        evidence: Box::new(evidence),
                    });
                }
                ExecutionControl::DeadlineExhausted => {
                    return Err(TypedFailure::DeadlineExhausted {
                        evidence: Box::new(TypedFailureEvidence::new(
                            Some("test.deadline".into()),
                            "unexpected deadline",
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

struct SettlementCleaner {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    discards: Arc<Mutex<Vec<bool>>>,
    pause_on: usize,
}

#[async_trait]
impl EffectCleaner for SettlementCleaner {
    async fn quiesce(&self, _request: &EffectCleanupRequest) -> Result<(), EffectCleanupFailure> {
        Ok(())
    }

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
    async fn run(&self, mut request: RoleTurnRequest) -> Result<RoleTurnOutcome, TypedFailure> {
        if let Some(outcome) = judgment_outcome(&request) {
            return Ok(outcome);
        }
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
                ExecutionControl::Abort(reason) => {
                    let mut evidence =
                        TypedFailureEvidence::new(Some("test.abort".into()), "mission aborted");
                    evidence.stop_reason = Some(reason);
                    return Err(TypedFailure::OperatorAborted {
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
    async fn run(&self, request: RoleTurnRequest) -> Result<RoleTurnOutcome, TypedFailure> {
        if let Some(outcome) = judgment_outcome(&request) {
            return Ok(outcome);
        }
        tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
        prepare_test_workspace(&request).await?;
        let report = PayloadRef::inline("worked beyond initial deadline");
        let configuration = RuntimeConfigurationEvidence::default();
        confirm_completed_turn(&request, "", &configuration).await?;
        let artifact = capture_prepared_test_artifact(&request, HEAD_SHA).await?;
        Ok(RoleTurnOutcome {
            handoff: Some(Handoff::Work {
                done: true,
                report,
                request_attention: false,
            }),
            artifact: Some(artifact),
            prepared_inputs: Vec::new(),
            runtime_configuration: configuration,
            runtime_usage: Default::default(),
            final_response: String::new(),
        })
    }
}

#[async_trait]
impl RoleRunner for ArtifactlessWriter {
    async fn run(&self, request: RoleTurnRequest) -> Result<RoleTurnOutcome, TypedFailure> {
        if let Some(outcome) = judgment_outcome(&request) {
            return Ok(outcome);
        }
        prepare_test_workspace(&request).await?;
        let report = PayloadRef::inline("the requested work was already satisfied");
        let configuration = RuntimeConfigurationEvidence::default();
        let final_response = "no repository change was needed";
        confirm_completed_turn(&request, final_response, &configuration).await?;
        Ok(RoleTurnOutcome {
            handoff: Some(Handoff::Work {
                done: true,
                report,
                request_attention: false,
            }),
            artifact: None,
            prepared_inputs: Vec::new(),
            runtime_configuration: configuration,
            runtime_usage: Default::default(),
            final_response: final_response.into(),
        })
    }
}

#[async_trait]
impl RoleRunner for ControlledRunner {
    async fn run(&self, mut request: RoleTurnRequest) -> Result<RoleTurnOutcome, TypedFailure> {
        if let Some(outcome) = judgment_outcome(&request) {
            return Ok(outcome);
        }
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
        prepare_test_workspace(&request).await?;
        let report = PayloadRef::inline("continued in the same task workspace");
        let configuration = RuntimeConfigurationEvidence::default();
        let final_response = "completed after continue";
        confirm_completed_turn(&request, final_response, &configuration).await?;
        let artifact = capture_prepared_test_artifact(&request, HEAD_SHA).await?;
        Ok(RoleTurnOutcome {
            handoff: Some(Handoff::Work {
                done: true,
                report,
                request_attention: false,
            }),
            artifact: Some(artifact),
            prepared_inputs: Vec::new(),
            runtime_configuration: configuration,
            runtime_usage: Default::default(),
            final_response: final_response.into(),
        })
    }
}

#[async_trait]
impl RoleRunner for RawSuccessAfterStopRunner {
    async fn run(&self, mut request: RoleTurnRequest) -> Result<RoleTurnOutcome, TypedFailure> {
        if let Some(outcome) = judgment_outcome(&request) {
            return Ok(outcome);
        }
        self.started.wait().await;
        loop {
            if matches!(request.control.borrow().clone(), ExecutionControl::Stop(_)) {
                let report = PayloadRef::inline("raw success report");
                let configuration = RuntimeConfigurationEvidence::default();
                let final_response = "raw success response";
                confirm_completed_turn(&request, final_response, &configuration).await?;
                return Ok(RoleTurnOutcome {
                    handoff: Some(Handoff::Work {
                        done: true,
                        report,
                        request_attention: false,
                    }),
                    artifact: None,
                    prepared_inputs: Vec::new(),
                    runtime_configuration: configuration,
                    runtime_usage: Default::default(),
                    final_response: final_response.into(),
                });
            }
            request.control.changed().await.unwrap();
        }
    }
}

#[tokio::test]
async fn park_reference_materializes_the_fold_authoritative_failure() {
    let dir = test_repository();
    let store = MissionStore::open(dir.path()).await.unwrap();
    let started = Arc::new(Barrier::new(2));
    let engine = Arc::new(Engine::new(
        store.clone(),
        test_mission_type(),
        "localhost/lionclaw-runtime-dev:v1".to_string(),
        EngineServices::new(
            Arc::new(RawSuccessAfterStopRunner {
                started: started.clone(),
            }),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    ));
    let mission = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "park evidence truth",
            BASE_SHA,
        )
        .await
        .unwrap();
    engine
        .propose_plan(&mission, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&engine, &mission).await;

    let driver = {
        let engine = engine.clone();
        let mission = mission.clone();
        tokio::spawn(async move { engine.advance(&mission).await.unwrap() })
    };
    started.wait().await;
    let active = store.require_state(&mission).await.unwrap();
    let effect_id = active.inflight.keys().next().unwrap().clone();
    record_control(
        &store,
        1,
        &mission,
        &effect_id,
        ControlAction::Stop,
        "materialization proof stop",
    )
    .await
    .unwrap();
    let settled = driver.await.unwrap().state;
    assert!(settled.parked_effects.contains_key(&effect_id));

    let materialized = lionclaw::reference_materialization::materialize_references(
        &settled,
        &store.load(&mission).await.unwrap(),
        store.blobs(),
        dir.path(),
        &[MessageReference::ParkEvidence {
            effect_id: effect_id.clone(),
        }],
    )
    .await
    .unwrap();
    let failure: serde_json::Value = serde_json::from_str(&materialized[0].content).unwrap();
    assert_eq!(failure["type"], "operator_stopped");
    assert_eq!(
        failure["evidence"]["stop_reason"],
        "materialization proof stop"
    );
    assert!(failure.get("Ok").is_none());
}

#[tokio::test]
async fn stop_parks_exact_generation_and_continue_preserves_assignment() {
    let dir = test_repository();
    let store = MissionStore::open(dir.path()).await.unwrap();
    let started = Arc::new(Barrier::new(2));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let engine = Arc::new(Engine::new(
        store.clone(),
        test_mission_type(),
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
    let observer = serde_json::to_value(&activity.effects[0]).unwrap();
    for fold_owned in ["role", "task", "runtime", "deadline_ms", "legal_controls"] {
        assert!(
            observer.get(fold_owned).is_none(),
            "observer duplicated fold-owned field {fold_owned}"
        );
    }
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
    assert!(parked
        .next
        .choices
        .iter()
        .any(|choice| matches!(choice, Choice::Continue { .. })));
    assert_eq!(
        parked.state.tasks.values().next().unwrap().status,
        TaskStatus::Failed
    );
    assert!(parked.state.parked_effects.contains_key(&effect_id));
    assert!(parked
        .next
        .choices
        .iter()
        .any(|choice| matches!(choice, Choice::SendMessage { .. })));
    assert!(parked
        .next
        .choices
        .iter()
        .any(|choice| matches!(choice, Choice::Abort)));
    let mut awaiting_and_parked = parked.state.clone();
    awaiting_and_parked
        .conversations
        .values_mut()
        .next()
        .expect("parked role conversation")
        .lifecycle = lionclaw::model::ConversationLifecycle::AwaitingLead;
    let composed = lionclaw::engine::MissionView::from_state(awaiting_and_parked, false);
    assert!(composed
        .next
        .choices
        .iter()
        .any(|choice| matches!(choice, Choice::Continue { .. })));
    assert!(composed
        .next
        .choices
        .iter()
        .any(|choice| matches!(choice, Choice::SendMessage { .. })));
    assert_eq!(
        parked
            .state
            .task_last_failure(&TaskId::new("fix").unwrap())
            .unwrap()
            .category(),
        "operator_stopped"
    );
    assert!(record_control(
        &store,
        4,
        &mission_id,
        &common::effect_id("successor"),
        ControlAction::Continue {
            automatic: false,
            mode: lionclaw::model::ContinueMode::Preserve,
        },
        "wrong generation",
    )
    .await
    .is_err());

    record_control(
        &store,
        5,
        &mission_id,
        &effect_id,
        ControlAction::Continue {
            automatic: false,
            mode: lionclaw::model::ContinueMode::Preserve,
        },
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
    let checkpoint = common::advance_to_finished(&engine, &mission_id).await;
    assert!(checkpoint.state.is_terminal());
    assert_eq!(
        requests.lock().unwrap().as_slice(),
        &[(BASE_SHA.into(), 1), (BASE_SHA.into(), 1)]
    );
    assert!(record_control(
        &store,
        6,
        &mission_id,
        &effect_id,
        ControlAction::Continue {
            automatic: false,
            mode: lionclaw::model::ContinueMode::Preserve,
        },
        "must not reopen a terminal mission",
    )
    .await
    .unwrap_err()
    .to_string()
    .contains("terminal"));
}

#[tokio::test]
async fn abort_cancels_an_active_oracle_while_the_driver_drains_its_batch() {
    let dir = test_repository();
    let store = MissionStore::open(dir.path()).await.unwrap();
    let blocked_started = Arc::new(Notify::new());
    let abort_observed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut mission_type = test_mission_type();
    mission_type.edit_for_testing(|definition| {
        definition.oracles.clear();
        for name in ["oracle-a", "oracle-b"] {
            definition.oracles.insert(
                OracleName::new(name).unwrap(),
                format!("/nonexistent-mission-type/oracles/{name}").into(),
            );
        }
    });
    let engine = Arc::new(Engine::new(
        store.clone(),
        mission_type,
        "test-image".into(),
        EngineServices::new(
            Arc::new(happy_team_runner()),
            Arc::new(AbortOracleRunner {
                blocked_started: blocked_started.clone(),
                abort_observed: abort_observed.clone(),
                calls: std::sync::atomic::AtomicUsize::new(0),
            }),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    ));
    let mission_id = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "abort while draining proof",
            BASE_SHA,
        )
        .await
        .unwrap();
    let mut plan = simple_plan();
    plan.assertions[0].oracle = Some(OracleName::new("oracle-a").unwrap());
    plan.requirements
        .push(covered_requirement("SECOND-CHECK", "SECOND-PASS"));
    plan.assertions.push(Assertion {
        id: AssertionId::new("SECOND-PASS").unwrap(),
        prose: "the second check passes".into(),
        oracle: Some(OracleName::new("oracle-b").unwrap()),
    });
    plan.tasks[0]
        .targets
        .push(AssertionId::new("SECOND-PASS").unwrap());
    engine
        .propose_plan(&mission_id, proposal(0, plan))
        .await
        .unwrap();
    approve_plan(&engine, &mission_id).await;

    let driver = tokio::spawn({
        let engine = engine.clone();
        let mission_id = mission_id.clone();
        async move { engine.advance(&mission_id).await }
    });
    blocked_started.notified().await;
    engine
        .abort(&mission_id, "operator aborted the mission")
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), driver)
        .await
        .expect("abort must cancel the active sibling promptly")
        .expect("driver task")
        .expect("advance");

    assert!(abort_observed.load(std::sync::atomic::Ordering::SeqCst));
    let state = engine.load_state(&mission_id).await.unwrap();
    assert!(matches!(
        state.terminal,
        Some(TerminalState::Aborted { .. })
    ));
    assert!(state.inflight.is_empty());
    assert_eq!(state.current_sha, HEAD_SHA);
    assert!(state.reachable_commits.contains(HEAD_SHA));
    let events = store.load(&mission_id).await.unwrap();
    let abort_sequence = events
        .iter()
        .find(|event| {
            matches!(
                event.event,
                lionclaw::model::MissionEvent::MissionAborted { .. }
            )
        })
        .expect("durable abort fact")
        .sequence_no;
    let cancelled_sequence = events
        .iter()
        .find(|event| {
            matches!(
                &event.event,
                lionclaw::model::MissionEvent::OracleRunCompleted {
                    outcome: Err(failure @ TypedFailure::OperatorAborted { .. }),
                    ..
                } if failure.evidence().code.as_deref()
                    == Some("control.aborted_before_settlement")
            )
        })
        .expect("cancelled oracle outcome")
        .sequence_no;
    assert!(abort_sequence < cancelled_sequence);
}

#[tokio::test]
async fn durable_stop_wins_the_outcome_append_race_and_discards_the_candidate() {
    let dir = test_repository();
    let store = MissionStore::open(dir.path()).await.unwrap();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let discards = Arc::new(Mutex::new(Vec::new()));
    let engine = Arc::new(Engine::new(
        store.clone(),
        test_mission_type(),
        "test-image".into(),
        EngineServices::new(
            Arc::new(happy_team_runner()),
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
    assert!(parked
        .next
        .choices
        .iter()
        .any(|choice| matches!(choice, Choice::Continue { .. })));
    let failure = parked
        .state
        .task_last_failure(&TaskId::new("fix").unwrap())
        .unwrap();
    assert!(matches!(failure, TypedFailure::OperatorStopped { .. }));
    assert_eq!(
        failure.evidence().stop_reason.as_deref(),
        Some("stop won before the outcome became durable")
    );
    assert!(failure.evidence().final_response.is_empty());
    assert_eq!(failure.evidence().configuration, Default::default());
    assert_eq!(discards.lock().unwrap().as_slice(), &[false, true]);
    let events = store.load(&mission_id).await.unwrap();
    assert!(!events.iter().any(|event| matches!(
        event.event,
        lionclaw::model::MissionEvent::RoleTurnCompleted { outcome: Ok(_), .. }
    )));
}

#[derive(Clone, Copy, Debug)]
enum SettlementCancellation {
    Stop,
    Deadline,
    Abort,
}

#[tokio::test]
async fn role_cancellation_matrix_preserves_exact_durable_settlement_evidence() {
    for cancellation in [
        SettlementCancellation::Stop,
        SettlementCancellation::Deadline,
        SettlementCancellation::Abort,
    ] {
        for outcome in [
            SettlementRaceOutcome::Success,
            SettlementRaceOutcome::Failure,
            SettlementRaceOutcome::InvalidOutput,
            SettlementRaceOutcome::Question,
        ] {
            let dir = test_repository();
            let store = MissionStore::open(dir.path()).await.unwrap();
            let entered = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            let mut mission_type = test_mission_type();
            let deadline_race = matches!(cancellation, SettlementCancellation::Deadline);
            if deadline_race {
                mission_type.edit_for_testing(|definition| {
                    definition.execution.default_timeout_secs = 1;
                    definition.execution.max_task_time_secs = 1;
                });
            }
            let deadline_clock =
                deadline_race.then(|| Arc::new(ControlledDeadlineClock::new(1_000_000)));
            let deadline_barrier = deadline_race.then(|| {
                Arc::new(SettlementDeadlineBarrier {
                    runner_ready: Notify::new(),
                    release_runner: Notify::new(),
                })
            });
            let clock: Arc<dyn lionclaw::ports::Clock> = deadline_clock
                .clone()
                .map(|clock| clock as Arc<dyn lionclaw::ports::Clock>)
                .unwrap_or_else(|| Arc::new(MockClock::default()));
            let engine = Arc::new(Engine::new(
                store.clone(),
                mission_type,
                "test-image".into(),
                EngineServices::new(
                    Arc::new(SettlementRaceRunner {
                        outcome,
                        deadline_barrier: deadline_barrier.clone(),
                    }),
                    Arc::new(MockOracleRunner::exiting(0)),
                    Arc::new(SettlementCleaner {
                        entered: entered.clone(),
                        release: release.clone(),
                        discards: Arc::new(Mutex::new(Vec::new())),
                        pause_on: 1,
                    }),
                    clock,
                ),
            ));
            let mission_id = engine
                .create_mission(
                    dir.path().to_str().unwrap(),
                    &format!("{cancellation:?} against {outcome:?}"),
                    BASE_SHA,
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
            if let (Some(clock), Some(barrier)) = (&deadline_clock, &deadline_barrier) {
                barrier.runner_ready.notified().await;
                let started = store.require_state(&mission_id).await.unwrap();
                let (started_effect_id, started_effect) = started.inflight.iter().next().unwrap();
                let started_effect_id = started_effect_id.clone();
                let started_deadline_ms = started_effect.deadline_ms();
                clock.advance_past(started_deadline_ms);
                tokio::time::timeout(std::time::Duration::from_secs(2), async {
                    loop {
                        let current = store.require_state(&mission_id).await.unwrap();
                        if current.reached_deadlines.get(&started_effect_id)
                            == Some(&started_deadline_ms)
                        {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("engine must durably observe the controlled deadline");
                barrier.release_runner.notify_one();
            }
            entered.notified().await;
            let active = store.require_state(&mission_id).await.unwrap();
            let (effect_id, effect) = active.inflight.iter().next().unwrap();
            let effect_id = effect_id.clone();
            let request = effect.role_turn_provenance().unwrap();
            let conversation_id = request.role_instance.clone();
            let assignment_epoch = request.assignment_epoch;
            let message_boundary = request.message_boundary;

            // This is a real reducer-30 snapshot of the active request. All
            // following facts, including settlement, form a nonempty tail.
            let snapshotted = store.rebuild_cursors(&mission_id, 7_000).await.unwrap();
            assert_eq!(snapshotted, active);
            assert_eq!(
                store.snapshot_meta(&mission_id).await.unwrap(),
                Some((active.head, REDUCER_VERSION))
            );
            record_message(
                &store,
                dir.path(),
                &mission_id,
                MessageCommand {
                    selectors: Vec::new(),
                    all: true,
                    body: "arrived beyond the observed delivery boundary".into(),
                    references: Vec::new(),
                },
                7_001,
            )
            .await
            .unwrap();

            let mut abort_reconciliation = None;
            match cancellation {
                SettlementCancellation::Stop => {
                    record_control(
                        &store,
                        7_002,
                        &mission_id,
                        &effect_id,
                        ControlAction::Stop,
                        "matrix stop",
                    )
                    .await
                    .unwrap();
                }
                SettlementCancellation::Deadline => {
                    let current = store.require_state(&mission_id).await.unwrap();
                    assert_eq!(
                        current.reached_deadlines.get(&effect_id),
                        Some(&effect.deadline_ms())
                    );
                }
                SettlementCancellation::Abort => {
                    let abort_engine = engine.clone();
                    let abort_mission = mission_id.clone();
                    abort_reconciliation = Some(tokio::spawn(async move {
                        abort_engine.abort(&abort_mission, "matrix abort").await
                    }));
                    tokio::time::timeout(std::time::Duration::from_secs(2), async {
                        loop {
                            if store
                                .require_state(&mission_id)
                                .await
                                .unwrap()
                                .is_terminal()
                            {
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .expect("abort must be durable before settlement is released");
                }
            }
            release.notify_one();
            let view = driver.await.unwrap();
            if let Some(abort) = abort_reconciliation {
                abort.await.unwrap().unwrap();
            }
            let live = view.state;
            let events = store.load(&mission_id).await.unwrap();
            let replayed = fold(events.clone()).unwrap();
            let reloaded = store.require_state(&mission_id).await.unwrap();
            let snapshot_tail = store
                .load_state_snapshotted(&mission_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                live, replayed,
                "live/full replay: {cancellation:?}/{outcome:?}"
            );
            assert_eq!(
                live, reloaded,
                "live/store reload: {cancellation:?}/{outcome:?}"
            );
            assert_eq!(
                live, snapshot_tail,
                "live/snapshot tail: {cancellation:?}/{outcome:?}"
            );
            assert!(live.head > active.head, "snapshot tail must be nonempty");
            let rebuilt = store.rebuild_cursors(&mission_id, 8_000).await.unwrap();
            assert_eq!(live, rebuilt, "live/reducer-30 rebuild");

            assert_eq!(request.assignment_epoch, assignment_epoch);
            assert_eq!(request.role_instance, conversation_id);
            let conversation = &live.conversations[&conversation_id];
            assert!(conversation.active_delivery.is_none());
            assert_eq!(conversation.role_instance, conversation_id);
            assert_eq!(conversation.queued.len(), 1);
            assert!(conversation.queued[0].sequence_no > message_boundary);
            assert_eq!(
                conversation.queued[0].marker,
                match cancellation {
                    SettlementCancellation::Abort => DeliveryMarker::Undeliverable,
                    SettlementCancellation::Stop | SettlementCancellation::Deadline => {
                        DeliveryMarker::Queued
                    }
                }
            );
            assert_eq!(
                conversation.queued[0].body,
                "arrived beyond the observed delivery boundary"
            );
            let failure = live
                .task_last_failure(&TaskId::new("fix").unwrap())
                .unwrap();
            assert!(matches!(
                (cancellation, failure),
                (
                    SettlementCancellation::Stop,
                    TypedFailure::OperatorStopped { .. }
                ) | (
                    SettlementCancellation::Deadline,
                    TypedFailure::DeadlineExhausted { .. }
                ) | (
                    SettlementCancellation::Abort,
                    TypedFailure::OperatorAborted { .. }
                )
            ));
            let evidence = failure.evidence();
            assert_eq!(evidence.configuration, Default::default());
            assert!(evidence.final_response.is_empty());
            match cancellation {
                SettlementCancellation::Stop => {
                    assert_eq!(evidence.stop_reason.as_deref(), Some("matrix stop"));
                }
                SettlementCancellation::Deadline => assert!(evidence
                    .stop_reason
                    .as_deref()
                    .is_some_and(|reason| reason.starts_with("deadline reached at "))),
                SettlementCancellation::Abort => {
                    assert_eq!(evidence.stop_reason.as_deref(), Some("matrix abort"));
                }
            }
            if matches!(cancellation, SettlementCancellation::Abort) {
                assert!(matches!(live.terminal, Some(TerminalState::Aborted { .. })));
            }
            assert!(matches!(
                live.parked_effects.get(&effect_id),
                Some(lionclaw::model::ParkedEffect::RoleTurn {
                    role_instance,
                    task_id: Some(task_id),
                }) if role_instance == &conversation_id
                    && task_id == &TaskId::new("fix").unwrap()
            ));
        }
    }
}

#[tokio::test]
async fn settlement_retains_bounded_blob_backed_oracle_stderr() {
    let dir = test_repository();
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
        "test-image".into(),
        EngineServices::new(
            Arc::new(happy_team_runner()),
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
    assert!(parked
        .next
        .choices
        .iter()
        .any(|choice| matches!(choice, Choice::Continue { .. })));
    let failure = parked.state.oracle_failures.values().next().unwrap();
    assert!(matches!(failure, TypedFailure::OperatorStopped { .. }));
    assert!(!failure.evidence().stderr.is_empty());
    assert!(failure.evidence().stderr.len() <= lionclaw_runtime_api::FAILURE_TEXT_LIMIT);
    assert!(failure.evidence().stderr.starts_with('E'));
    assert_eq!(discards.lock().unwrap().as_slice(), &[false, true]);
}

#[tokio::test]
async fn deadline_is_durably_linearized_before_one_adapter_cancellation() {
    let dir = test_repository();
    let store = MissionStore::open(dir.path()).await.unwrap();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut mission_type = test_mission_type();
    mission_type.edit_for_testing(|definition| {
        definition.execution.default_timeout_secs = 1;
        definition.execution.max_task_time_secs = 1;
    });
    let engine = Engine::new(
        store.clone(),
        mission_type,
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
    let mission_id = engine
        .create_mission(dir.path().to_str().unwrap(), "linearize deadline", BASE_SHA)
        .await
        .unwrap();
    engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&engine, &mission_id).await;

    let parked = engine.advance(&mission_id).await.unwrap();
    assert!(parked
        .next
        .choices
        .iter()
        .any(|choice| matches!(choice, Choice::Continue { .. })));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(parked.state.reached_deadlines.len(), 1);
    let events = store.load(&mission_id).await.unwrap();
    let reached = events
        .iter()
        .position(|event| {
            matches!(
                event.event,
                lionclaw::model::MissionEvent::ControlRequested {
                    action: ControlAction::DeadlineReached { .. },
                    ..
                }
            )
        })
        .unwrap();
    let completed = events
        .iter()
        .position(|event| {
            matches!(
                event.event,
                lionclaw::model::MissionEvent::RoleTurnCompleted {
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
    let dir = test_repository();
    let store = MissionStore::open(dir.path()).await.unwrap();
    let mut mission_type = test_mission_type();
    mission_type.edit_for_testing(|definition| {
        definition.execution = lionclaw::model::ExecutionPolicy {
            default_timeout_secs: 1,
            max_task_time_secs: 2,
            extension_step_secs: 1,
            effect_capacity: 4,
            auto_continue_candidate: false,
            auto_continue_proof: false,
        };
    });
    let engine = Engine::new(
        store.clone(),
        mission_type,
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
        )
        .await
        .unwrap();
    engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&engine, &mission_id).await;

    let checkpoint = engine.advance(&mission_id).await.unwrap();
    assert!(checkpoint
        .next
        .effects
        .iter()
        .any(|effect| matches!(effect, EffectIntent::DispatchOracle(_))));
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
    let dir = test_repository();
    let store = MissionStore::open(dir.path()).await.unwrap();
    let engine = Engine::new(
        store.clone(),
        test_mission_type(),
        "test-image".into(),
        EngineServices::new(
            Arc::new(happy_team_runner()),
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
        )
        .await
        .unwrap();
    engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&engine, &mission_id).await;

    let finished = common::advance_to_finished(&engine, &mission_id).await;
    assert!(finished.state.is_terminal());
    let events = store.load(&mission_id).await.unwrap();
    let automatic = events
        .iter()
        .filter(|event| {
            matches!(
                &event.event,
                lionclaw::model::MissionEvent::ControlRequested {
                    action: ControlAction::Continue {
                        automatic: true,
                        ..
                    },
                    ..
                }
            )
        })
        .count();
    assert_eq!(automatic, 2);
    assert!(events.iter().any(|event| matches!(
        &event.event,
        lionclaw::model::MissionEvent::MissionFinished { .. }
    )));
}

#[tokio::test]
async fn policy_auto_continues_an_artifactless_writer_success() {
    let dir = test_repository();
    let store = MissionStore::open(dir.path()).await.unwrap();
    let engine = Engine::new(
        store.clone(),
        test_mission_type(),
        "test-image".into(),
        EngineServices::new(
            Arc::new(ArtifactlessWriter),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );
    let mission_id = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "continue a writer that needed no repository change",
            BASE_SHA,
        )
        .await
        .unwrap();
    engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&engine, &mission_id).await;

    let finished = common::advance_to_finished(&engine, &mission_id).await;

    assert!(finished.state.is_terminal());
    assert_eq!(finished.state.deliverable_head(), BASE_SHA);
    let events = store.load(&mission_id).await.unwrap();
    let automatic = events
        .iter()
        .filter(|event| {
            matches!(
                &event.event,
                lionclaw::model::MissionEvent::ControlRequested {
                    action: ControlAction::Continue {
                        automatic: true,
                        ..
                    },
                    ..
                }
            )
        })
        .count();
    assert_eq!(automatic, 2);
    assert!(events.iter().any(|event| matches!(
        &event.event,
        lionclaw::model::MissionEvent::MissionFinished { .. }
    )));
}

#[tokio::test]
async fn direct_mission_creation_rejects_an_invalid_execution_policy() {
    let dir = test_repository();
    let store = MissionStore::open(dir.path()).await.unwrap();
    let mut mission_type = test_mission_type();
    mission_type.edit_for_testing(|definition| {
        definition.execution.default_timeout_secs = 0;
    });
    let engine = Engine::new(
        store,
        mission_type,
        "test-image".into(),
        EngineServices::new(
            Arc::new(happy_team_runner()),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );
    let error = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "reject invalid policy",
            BASE_SHA,
        )
        .await
        .expect_err("direct callers must not persist invalid execution policy");

    assert!(error.to_string().contains("invalid execution policy"));
}

#[tokio::test]
async fn mission_creation_rejects_deadlines_unrepresentable_at_its_epoch() {
    let dir = test_repository();
    let mut mission_type = test_mission_type();
    mission_type.edit_for_testing(|definition| {
        definition.execution.max_task_time_secs = lionclaw::model::MAX_EXECUTION_DURATION_SECS;
    });
    let store = MissionStore::open(dir.path()).await.unwrap();
    let engine = Engine::new(
        store,
        mission_type,
        "test-image".into(),
        EngineServices::new(
            Arc::new(happy_team_runner()),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );
    let error = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "reject an impossible absolute deadline",
            BASE_SHA,
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
    let dir = test_repository();
    let mut mission_type = test_mission_type();
    mission_type.edit_for_testing(|definition| {
        definition
            .default_team
            .roles
            .values_mut()
            .next()
            .unwrap()
            .deadline_secs = Some(lionclaw::model::MAX_EXECUTION_DURATION_SECS);
    });
    let store = MissionStore::open(dir.path()).await.unwrap();
    let engine = Engine::new(
        store,
        mission_type,
        "test-image".into(),
        EngineServices::new(
            Arc::new(happy_team_runner()),
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
        )
        .await
        .expect_err("role deadline must fit at the mission epoch");
    assert!(
        error.to_string().contains("deadline"),
        "unexpected error: {error:#}"
    );
}

#[tokio::test]
async fn mission_creation_rejects_zero_second_role_deadlines() {
    let dir = test_repository();
    let mut mission_type = test_mission_type();
    mission_type.edit_for_testing(|definition| {
        definition
            .default_team
            .roles
            .values_mut()
            .next()
            .unwrap()
            .deadline_secs = Some(0);
    });
    let store = MissionStore::open(dir.path()).await.unwrap();
    let engine = Engine::new(
        store,
        mission_type,
        "test-image".into(),
        EngineServices::new(
            Arc::new(happy_team_runner()),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );

    let error = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "reject a zero-second role deadline",
            BASE_SHA,
        )
        .await
        .expect_err("role deadlines are positive durations");
    assert!(
        error.to_string().contains("deadline must be positive"),
        "unexpected error: {error:#}"
    );
}
