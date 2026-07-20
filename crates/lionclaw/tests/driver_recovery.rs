//! Mission-driver exclusion and cleanup recovery at the engine boundary.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common::{
    approve_plan, fault_append_events, initialize_repository, proposal, simple_plan,
    test_mission_type, BASE_SHA, HEAD_SHA,
};
use lionclaw::engine::{Engine, EngineServices, MissionDisposition};
use lionclaw::model::{
    EffectResource, Handoff, MissionEvent, PayloadRef, RuntimeConfigurationEvidence,
};
use lionclaw::ports::{
    EffectCleaner, EffectCleanupFailure, EffectCleanupRequest, RoleRunOutcome, RoleRunRequest,
    RoleRunner,
};
use lionclaw::store::{MissionStore, NewEvent};
use lionclaw::testing::{
    capture_test_artifact, MockClock, MockOracleRunner, MockRoleRunner, NoopEffectCleaner,
};
use lionclaw_runtime_api::TypedFailure;
use tokio::sync::Barrier;

struct BlockingRunner {
    started: Arc<Barrier>,
    release: Arc<Barrier>,
    calls: Arc<AtomicUsize>,
}

fn test_repository() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    initialize_repository(dir.path());
    dir
}

#[async_trait]
impl RoleRunner for BlockingRunner {
    async fn run(&self, request: RoleRunRequest) -> Result<RoleRunOutcome, TypedFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.wait().await;
        self.release.wait().await;
        let artifact = capture_test_artifact(&request, HEAD_SHA).await?;
        Ok(RoleRunOutcome {
            handoff: Some(Handoff::Work {
                done: true,
                report: PayloadRef::inline("done"),
                request_attention: false,
            }),
            artifact: Some(artifact),
            runtime_configuration: lionclaw::model::RuntimeConfigurationEvidence {
                requested_model: Some("blocking-test".to_string()),
                applied_model: Some("blocking-test".to_string()),
                ..Default::default()
            },
            final_response: String::new(),
        })
    }
}

#[derive(Default)]
struct FailOnceCleaner {
    calls: Mutex<Vec<EffectCleanupRequest>>,
    attempts: AtomicUsize,
}

#[derive(Default)]
struct AlwaysFailCleaner {
    calls: Mutex<Vec<EffectCleanupRequest>>,
}

#[async_trait]
impl EffectCleaner for AlwaysFailCleaner {
    async fn cleanup(&self, request: EffectCleanupRequest) -> Result<(), EffectCleanupFailure> {
        self.calls.lock().unwrap().push(request);
        Err(EffectCleanupFailure {
            resource: EffectResource::RuntimeSecret,
            detail: "injected persistent runtime-secret cleanup failure".to_string(),
        })
    }
}

#[async_trait]
impl EffectCleaner for FailOnceCleaner {
    async fn cleanup(&self, request: EffectCleanupRequest) -> Result<(), EffectCleanupFailure> {
        self.calls.lock().unwrap().push(request);
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(EffectCleanupFailure {
                resource: EffectResource::EffectDirectory,
                detail: "injected attempt-directory cleanup failure".to_string(),
            });
        }
        Ok(())
    }
}

async fn create_approved_mission(
    engine: &Engine,
    repo: &std::path::Path,
) -> lionclaw::model::MissionId {
    let mission_id = engine
        .create_mission(
            repo.to_str().unwrap(),
            "exercise mission driver ownership",
            BASE_SHA,
        )
        .await
        .unwrap();
    engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(engine, &mission_id).await;
    mission_id
}

#[tokio::test]
async fn concurrent_advance_reports_running_and_never_double_dispatches() {
    let dir = test_repository();
    let started = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let calls = Arc::new(AtomicUsize::new(0));
    let engine = Arc::new(Engine::new(
        MissionStore::open(dir.path()).await.unwrap(),
        test_mission_type(),
        "codex".to_string(),
        "test-image".to_string(),
        EngineServices::new(
            Arc::new(BlockingRunner {
                started: started.clone(),
                release: release.clone(),
                calls: calls.clone(),
            }),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    ));
    let mission_id = create_approved_mission(&engine, dir.path()).await;

    let first = tokio::spawn({
        let engine = engine.clone();
        let mission_id = mission_id.clone();
        async move { engine.advance(&mission_id).await }
    });
    started.wait().await;

    let concurrent = engine.advance(&mission_id).await.unwrap();
    assert_eq!(concurrent.disposition, MissionDisposition::Running);
    assert_eq!(
        concurrent.next_actions(),
        vec!["mission status", "mission send", "mission abort"]
    );
    assert_eq!(concurrent.state.inflight.len(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    release.wait().await;
    let finished = first.await.unwrap().unwrap();
    assert_eq!(finished.disposition, MissionDisposition::Terminal);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn detached_startup_waits_out_a_short_observer_lock_probe() {
    let dir = test_repository();
    let engine = Arc::new(Engine::new(
        MissionStore::open(dir.path()).await.unwrap(),
        test_mission_type(),
        "codex".to_string(),
        "test-image".to_string(),
        EngineServices::new(
            Arc::new(MockRoleRunner::happy(HEAD_SHA)),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    ));
    let mission_id = create_approved_mission(&engine, dir.path()).await;
    let lock_path = engine
        .store()
        .lionclaw_dir()
        .join("missions")
        .join(mission_id.as_str())
        .join("driver.lock");
    std::fs::create_dir_all(lock_path.parent().unwrap()).unwrap();
    let observer_probe = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)
        .unwrap();
    rustix::fs::flock(&observer_probe, rustix::fs::FlockOperation::LockExclusive).unwrap();
    let handshake = dir.path().join("detached.ready");
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
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    assert!(
        !handshake.exists(),
        "handshake requires actual lock ownership"
    );
    rustix::fs::flock(&observer_probe, rustix::fs::FlockOperation::Unlock).unwrap();
    drop(observer_probe);

    let finished = driver.await.unwrap();
    assert!(handshake.exists());
    assert_eq!(finished.disposition, MissionDisposition::Terminal);
}

#[tokio::test]
async fn cleanup_failure_is_truthful_and_retried_without_replaying_the_effect() {
    let dir = test_repository();
    let runner = Arc::new(MockRoleRunner::happy(HEAD_SHA));
    let cleaner = Arc::new(FailOnceCleaner::default());
    let engine = Engine::new(
        MissionStore::open(dir.path()).await.unwrap(),
        test_mission_type(),
        "codex".to_string(),
        "test-image".to_string(),
        EngineServices::new(
            runner.clone(),
            Arc::new(MockOracleRunner::exiting(0)),
            cleaner.clone(),
            Arc::new(MockClock::default()),
        ),
    );
    let mission_id = create_approved_mission(&engine, dir.path()).await;

    let blocked = engine.advance(&mission_id).await.unwrap();
    assert_eq!(blocked.disposition, MissionDisposition::CleanupBlocked);
    assert_eq!(
        blocked.next_actions(),
        vec![
            "mission advance",
            "mission log",
            "mission send",
            "mission abort"
        ]
    );
    let failure = blocked.state.cleanup_failure.as_ref().unwrap();
    assert_eq!(failure.resource, EffectResource::EffectDirectory);
    assert_eq!(
        failure.failure.detail(),
        "injected attempt-directory cleanup failure"
    );
    assert_eq!(runner.calls.lock().unwrap().len(), 1);

    let state = engine.load_state(&mission_id).await.unwrap();
    let effect_id = state.inflight.keys().next().unwrap().clone();
    fault_append_events(
        dir.path(),
        &mission_id,
        state.head,
        &[NewEvent::new(MissionEvent::EffectRuntimeConfigured {
            effect_id,
            configuration: RuntimeConfigurationEvidence {
                requested_model: Some("requested-model".into()),
                applied_model: Some("applied-model".into()),
                model_confirmation: Some(
                    lionclaw_runtime_api::RuntimeConfigurationConfirmation::Observed,
                ),
                requested_mode: Some("build".into()),
                applied_mode: Some("build".into()),
                mode_confirmation: Some(
                    lionclaw_runtime_api::RuntimeConfigurationConfirmation::Observed,
                ),
            },
        })],
        1,
    )
    .await;

    let parked = engine.advance(&mission_id).await.unwrap();
    assert_eq!(parked.disposition, MissionDisposition::Parked);
    assert!(parked.state.cleanup_failure.is_none());
    assert!(parked.state.inflight.is_empty());
    assert_eq!(runner.calls.lock().unwrap().len(), 1);
    let task = parked
        .state
        .tasks
        .get(&lionclaw::model::TaskId::new("fix").unwrap())
        .unwrap();
    assert_eq!(
        task.last_failure.as_ref().unwrap().category(),
        "interrupted"
    );
    let configuration = &task.last_failure.as_ref().unwrap().evidence().configuration;
    assert_eq!(
        configuration.requested_model.as_deref(),
        Some("requested-model")
    );
    assert_eq!(
        configuration.applied_model.as_deref(),
        Some("applied-model")
    );
    assert_eq!(configuration.requested_mode.as_deref(), Some("build"));
    assert_eq!(configuration.applied_mode.as_deref(), Some("build"));
    let attention = parked.state.open_attention.values().next().unwrap();
    assert!(attention.report.contains("previous mission driver exited"));
    assert!(attention.report.contains("effect was not replayed"));

    let calls = cleaner.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].effect_id, calls[1].effect_id);
    assert!(!calls[0].discard_artifact);
    assert!(calls[1].discard_artifact);
}

#[tokio::test]
async fn persistent_cleanup_failure_never_settles_or_replays_the_effect() {
    let dir = test_repository();
    let runner = Arc::new(MockRoleRunner::happy(HEAD_SHA));
    let cleaner = Arc::new(AlwaysFailCleaner::default());
    let engine = Engine::new(
        MissionStore::open(dir.path()).await.unwrap(),
        test_mission_type(),
        "codex".to_string(),
        "test-image".to_string(),
        EngineServices::new(
            runner.clone(),
            Arc::new(MockOracleRunner::exiting(0)),
            cleaner.clone(),
            Arc::new(MockClock::default()),
        ),
    );
    let mission_id = create_approved_mission(&engine, dir.path()).await;

    for _ in 0..3 {
        let blocked = engine.advance(&mission_id).await.unwrap();
        assert_eq!(blocked.disposition, MissionDisposition::CleanupBlocked);
        assert_eq!(blocked.state.inflight.len(), 1);
        assert_eq!(
            blocked
                .state
                .cleanup_failure
                .as_ref()
                .unwrap()
                .failure
                .detail(),
            "injected persistent runtime-secret cleanup failure"
        );
    }

    assert_eq!(runner.calls.lock().unwrap().len(), 1);
    let calls = cleaner.calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    assert!(calls
        .windows(2)
        .all(|pair| pair[0].effect_id == pair[1].effect_id));
    assert!(!calls[0].discard_artifact);
    assert!(calls[1..].iter().all(|request| request.discard_artifact));
}
