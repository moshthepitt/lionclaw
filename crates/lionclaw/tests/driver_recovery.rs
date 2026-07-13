//! Mission-driver exclusion and cleanup recovery at the engine boundary.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common::{
    approve_plan, default_config, proposal, simple_plan, test_mission_type, BASE_SHA, HEAD_SHA,
};
use lionclaw::engine::{Engine, EngineServices, MissionDisposition};
use lionclaw::model::{ArtifactOutcome, EffectResource, Handoff, PayloadRef, RunErrorKind};
use lionclaw::ports::{
    EffectCleaner, EffectCleanupFailure, EffectCleanupRequest, RoleRunFailure, RoleRunOutcome,
    RoleRunRequest, RoleRunner,
};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockClock, MockOracleRunner, MockRoleRunner, NoopEffectCleaner};
use tokio::sync::Barrier;

struct BlockingRunner {
    started: Arc<Barrier>,
    release: Arc<Barrier>,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl RoleRunner for BlockingRunner {
    async fn run(&self, request: RoleRunRequest) -> Result<RoleRunOutcome, RoleRunFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.wait().await;
        self.release.wait().await;
        Ok(RoleRunOutcome {
            handoff: Handoff::Work {
                done: true,
                report: PayloadRef::inline("done"),
                request_attention: false,
            },
            artifact: Some(ArtifactOutcome {
                base_sha: request.base_sha,
                head_sha: HEAD_SHA.to_string(),
            }),
            model_id: Some("blocking-test".to_string()),
        })
    }
}

#[derive(Default)]
struct FailOnceCleaner {
    calls: Mutex<Vec<EffectCleanupRequest>>,
    attempts: AtomicUsize,
}

#[async_trait]
impl EffectCleaner for FailOnceCleaner {
    async fn cleanup(&self, request: EffectCleanupRequest) -> Result<(), EffectCleanupFailure> {
        self.calls.lock().unwrap().push(request);
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(EffectCleanupFailure {
                resource: EffectResource::AttemptDirectory,
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
            default_config(),
        )
        .await
        .unwrap();
    engine
        .propose_plan(
            &mission_id,
            proposal(0, simple_plan()),
            "test",
            "initial plan",
        )
        .await
        .unwrap();
    approve_plan(engine, &mission_id).await;
    mission_id
}

#[tokio::test]
async fn concurrent_advance_reports_running_and_never_double_dispatches() {
    let dir = tempfile::tempdir().unwrap();
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
    assert_eq!(concurrent.next_actions(), vec!["mission status"]);
    assert_eq!(concurrent.state.inflight.len(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    release.wait().await;
    let finished = first.await.unwrap().unwrap();
    assert_eq!(finished.disposition, MissionDisposition::Terminal);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cleanup_failure_is_truthful_and_retried_without_replaying_the_effect() {
    let dir = tempfile::tempdir().unwrap();
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
        vec!["mission advance", "mission log"]
    );
    let failure = blocked.state.cleanup_failure.as_ref().unwrap();
    assert_eq!(failure.resource, EffectResource::AttemptDirectory);
    assert_eq!(
        failure.failure.detail,
        "injected attempt-directory cleanup failure"
    );
    assert_eq!(runner.calls.lock().unwrap().len(), 1);

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
        task.last_failure.as_ref().unwrap().kind,
        RunErrorKind::Interrupted
    );
    let attention = parked.state.open_attention.values().next().unwrap();
    assert!(attention.report.contains("previous mission driver exited"));
    assert!(attention.report.contains("effect was not replayed"));

    let calls = cleaner.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].effect_id, calls[1].effect_id);
    assert!(!calls[0].discard_artifact);
    assert!(calls[1].discard_artifact);
}
