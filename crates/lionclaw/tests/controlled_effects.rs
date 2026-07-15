mod common;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common::{
    approve_plan, default_config, proposal, simple_plan, test_mission_type, BASE_SHA, HEAD_SHA,
};
use lionclaw::engine::{record_control, Engine, EngineServices, MissionDisposition};
use lionclaw::model::{ArtifactOutcome, ControlAction, Handoff, PayloadRef, TaskStatus};
use lionclaw::ports::{ExecutionControl, RoleRunOutcome, RoleRunRequest, RoleRunner};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockClock, MockOracleRunner, NoopEffectCleaner};
use lionclaw_runtime_api::{TypedFailure, TypedFailureEvidence};
use tokio::sync::Barrier;

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
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    let activity: lionclaw::activity::ActivityProjection = serde_json::from_slice(
        &std::fs::read(lionclaw::activity::path(
            &store
                .lionclaw_dir()
                .join("missions")
                .join(mission_id.as_str()),
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(activity.effects.len(), 1);
    assert_eq!(
        activity.effects[0].legal_controls,
        ["stop", "extend_deadline"]
    );
    let active = store.require_state(&mission_id).await.unwrap();
    let (effect_id, effect) = active.inflight.iter().next().unwrap();
    let effect_id = effect_id.clone();
    let original_deadline = effect.deadline_ms();
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
