//! Mission-driver exclusion and cleanup recovery at the engine boundary.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier as StdBarrier, Mutex};

use async_trait::async_trait;
use common::{
    advisory_plan, approve_plan, initialize_repository, proposal, review_proposal, review_runner,
    simple_plan, test_mission_type, BASE_SHA, HEAD_SHA,
};
use lionclaw::authority::AuthorityCeiling;
use lionclaw::engine::{Engine, EngineServices, MissionDisposition};
use lionclaw::mission_type::{load_mission_type, materialize_mission_type};
use lionclaw::model::{
    fold, ConversationLifecycle, DecisionAction, EffectId, EffectResource, EventEnvelope, Handoff,
    MissionEvent, MissionId, MissionPhase, MissionState, OutputSemantics, PayloadRef,
    RoleAttemptDisposition, RoleEffectSource, RuntimeConfigurationEvidence, TaskId, ValidationItem,
    REDUCER_VERSION,
};
use lionclaw::ports::{
    EffectCleaner, EffectCleanupFailure, EffectCleanupRequest, EventSink, RoleRunner,
    RoleTurnOutcome, RoleTurnRequest,
};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockClock, MockOracleRunner, NoopEffectCleaner};
use lionclaw_runtime_api::TypedFailure;
use tokio::sync::{Barrier, Notify};

fn software_dev_mission_type_dir() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root")
        .join("mission-types/software-dev")
}

struct BlockingRunner {
    started: Arc<Barrier>,
    release: Arc<Barrier>,
    calls: Arc<AtomicUsize>,
}

const RETAINED_REPORT: &str = "exact report retained across driver cancellation";
const RETAINED_RESPONSE: &str = "exact response retained across driver cancellation";
const RETAINED_MODEL: &str = "retained-role-model";

enum RetainedHandoff {
    Valid,
    Malformed,
    Absent,
}

struct RetainedHandoffRunner {
    started: Arc<Barrier>,
    calls: Arc<AtomicUsize>,
    handoff: RetainedHandoff,
}

struct RetainedStateOverflowRunner {
    started: Arc<Barrier>,
    calls: Arc<AtomicUsize>,
}

#[derive(Clone, Copy)]
enum AdversarialRetentionMode {
    RefusedBeforeHandoff,
    AcceptedThenSuccess,
    AcceptedThenCrash,
    AcceptedThenFailure,
    RejectedThenFailure,
}

struct AdversarialRetentionRunner {
    mode: AdversarialRetentionMode,
    started: Arc<Barrier>,
    calls: Arc<AtomicUsize>,
}

struct CompletedOverflowFailureRunner;

struct UnacknowledgedRunner {
    calls: Arc<AtomicUsize>,
}

struct MissingValidatorHandoffRunner {
    validator_started: Arc<Barrier>,
    calls: AtomicUsize,
    validator_attempts: AtomicUsize,
    validator_assignment: Mutex<Option<u32>>,
}

struct CountingCleaner {
    calls: Arc<AtomicUsize>,
}

#[derive(Default)]
struct ArtifactDiscardCleaner {
    calls: Mutex<Vec<EffectCleanupRequest>>,
}

struct DurableFoldCleaner {
    store: MissionStore,
    deletions: AtomicUsize,
    after_first_deletion: Option<CleanupPause>,
}

struct CleanupPause {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

struct RecoveryCompletionGate {
    category: &'static str,
    entered: Arc<StdBarrier>,
    release: Arc<StdBarrier>,
}

fn test_repository() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    initialize_repository(dir.path());
    dir
}

fn mission_ref_exists(
    repo: &std::path::Path,
    mission_id: &MissionId,
    effect_id: &EffectId,
) -> bool {
    std::process::Command::new("git")
        .current_dir(repo)
        .args([
            "show-ref",
            "--verify",
            "--quiet",
            &format!("refs/mission/{mission_id}/{effect_id}"),
        ])
        .status()
        .unwrap()
        .success()
}

fn effect_dir(request: &EffectCleanupRequest) -> std::path::PathBuf {
    request
        .state_dir
        .join("missions")
        .join(request.mission_id.as_str())
        .join("effects")
        .join(request.effect_id.as_str())
}

fn role_effect_dir(request: &RoleTurnRequest) -> std::path::PathBuf {
    request
        .state_dir
        .join("missions")
        .join(request.mission_id.as_str())
        .join("effects")
        .join(request.effect_id.as_str())
}

fn retained_runtime_dir(request: &RoleTurnRequest) -> std::path::PathBuf {
    request
        .state_dir
        .join("missions")
        .join(request.mission_id.as_str())
        .join("conversations")
        .join(request.role.id.as_str())
        .join("runtime")
}

fn retained_runtime_configuration() -> RuntimeConfigurationEvidence {
    RuntimeConfigurationEvidence {
        requested_model: Some(RETAINED_MODEL.to_string()),
        applied_model: Some(RETAINED_MODEL.to_string()),
        ..Default::default()
    }
}

fn exceed_runtime_retention_limit(request: &RoleTurnRequest) {
    let runtime = retained_runtime_dir(request);
    std::fs::create_dir_all(&runtime).unwrap();
    std::fs::File::create(runtime.join("oversized-native-state"))
        .unwrap()
        .set_len(512 * 1024 * 1024 + 1)
        .unwrap();
}

fn retained_work_handoff() -> Handoff {
    Handoff::Work {
        done: true,
        report: PayloadRef::inline(RETAINED_REPORT),
        request_attention: false,
    }
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
                .map(|item_id| ValidationItem {
                    item_id,
                    passed: true,
                })
                .collect(),
            passed: true,
            request_attention: false,
        }),
        artifact: None,
        runtime_configuration: RuntimeConfigurationEvidence::default(),
        runtime_usage: Default::default(),
        final_response: "judged".into(),
    })
}

#[async_trait]
impl RoleRunner for RetainedHandoffRunner {
    async fn run(&self, request: RoleTurnRequest) -> Result<RoleTurnOutcome, TypedFailure> {
        if let Some(outcome) = judgment_outcome(&request) {
            return Ok(outcome);
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        if matches!(self.handoff, RetainedHandoff::Absent) {
            lionclaw::testing::prepare_test_workspace(&request).await?;
        }
        let handoff_dir = role_effect_dir(&request).join("handoff");
        let handoff = match self.handoff {
            RetainedHandoff::Valid => Some(format!(
                r#"{{"schema":"lionclaw.mission.work-handoff.v2","type":"work","done":true,"report":"{RETAINED_REPORT}","request_attention":false}}"#
            )),
            RetainedHandoff::Malformed => Some(
                r#"{"schema":"wrong.schema","type":"work","done":true,"report":"must not disappear","request_attention":false}"#.to_string(),
            ),
            RetainedHandoff::Absent => None,
        };
        if let Some(handoff) = handoff {
            std::fs::create_dir_all(&handoff_dir).unwrap();
            std::fs::write(handoff_dir.join("handoff.json"), handoff).unwrap();
        } else {
            std::fs::create_dir_all(role_effect_dir(&request)).unwrap();
        }
        self.started.wait().await;
        if matches!(self.handoff, RetainedHandoff::Malformed) {
            Err(TypedFailure::invalid(
                "handoff.schema",
                "retained handoff does not match schema",
            ))
        } else {
            std::future::pending().await
        }
    }
}

#[async_trait]
impl RoleRunner for RetainedStateOverflowRunner {
    async fn run(&self, request: RoleTurnRequest) -> Result<RoleTurnOutcome, TypedFailure> {
        if let Some(outcome) = judgment_outcome(&request) {
            return Ok(outcome);
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        lionclaw::testing::prepare_test_workspace(&request).await?;
        std::fs::create_dir_all(role_effect_dir(&request).join("handoff")).unwrap();
        exceed_runtime_retention_limit(&request);
        self.started.wait().await;
        std::future::pending().await
    }
}

#[async_trait]
impl RoleRunner for AdversarialRetentionRunner {
    async fn run(&self, request: RoleTurnRequest) -> Result<RoleTurnOutcome, TypedFailure> {
        if let Some(outcome) = judgment_outcome(&request) {
            return Ok(outcome);
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        lionclaw::testing::prepare_test_workspace(&request).await?;
        std::fs::create_dir_all(role_effect_dir(&request).join("handoff")).unwrap();
        let configuration = retained_runtime_configuration();
        let handoff = retained_work_handoff();

        if matches!(
            self.mode,
            AdversarialRetentionMode::RefusedBeforeHandoff
                | AdversarialRetentionMode::RejectedThenFailure
        ) {
            exceed_runtime_retention_limit(&request);
        }

        if matches!(self.mode, AdversarialRetentionMode::RejectedThenFailure) {
            let mut failure =
                TypedFailure::invalid("handoff.schema", "adversarial invalid handoff");
            failure.evidence_mut().configuration = configuration;
            failure.evidence_mut().final_response = RETAINED_RESPONSE.into();
            return Err(failure);
        }

        // Deliberately violate the public runner contract after the accepted
        // acknowledgement. Cleanup and crash recovery must still fail closed.
        exceed_runtime_retention_limit(&request);
        let artifact =
            lionclaw::testing::capture_prepared_test_artifact(&request, HEAD_SHA).await?;
        match self.mode {
            AdversarialRetentionMode::AcceptedThenSuccess
            | AdversarialRetentionMode::RefusedBeforeHandoff => Ok(RoleTurnOutcome {
                handoff: Some(handoff),
                artifact: Some(artifact),
                runtime_configuration: configuration,
                runtime_usage: Default::default(),
                final_response: RETAINED_RESPONSE.into(),
            }),
            AdversarialRetentionMode::AcceptedThenCrash => {
                self.started.wait().await;
                std::future::pending().await
            }
            AdversarialRetentionMode::AcceptedThenFailure => {
                let mut failure =
                    TypedFailure::permanent("runtime.original_failure", "ordinary role failure");
                failure.evidence_mut().configuration = configuration;
                failure.evidence_mut().final_response = RETAINED_RESPONSE.into();
                Err(failure)
            }
            AdversarialRetentionMode::RejectedThenFailure => unreachable!(),
        }
    }
}

#[async_trait]
impl RoleRunner for CompletedOverflowFailureRunner {
    async fn run(&self, request: RoleTurnRequest) -> Result<RoleTurnOutcome, TypedFailure> {
        if let Some(outcome) = judgment_outcome(&request) {
            return Ok(outcome);
        }
        lionclaw::testing::prepare_test_workspace(&request).await?;
        std::fs::create_dir_all(role_effect_dir(&request).join("handoff")).unwrap();
        exceed_runtime_retention_limit(&request);
        let configuration = retained_runtime_configuration();
        let mut failure =
            TypedFailure::permanent("runtime.original_failure", "ordinary post-turn failure");
        failure.evidence_mut().configuration = configuration;
        failure.evidence_mut().final_response = RETAINED_RESPONSE.into();
        Err(failure)
    }
}

#[async_trait]
impl RoleRunner for MissingValidatorHandoffRunner {
    async fn run(&self, request: RoleTurnRequest) -> Result<RoleTurnOutcome, TypedFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match request.role.output {
            OutputSemantics::ProducesArtifact => {
                lionclaw::testing::prepare_test_workspace(&request).await?;
                let handoff = Handoff::Work {
                    done: true,
                    report: PayloadRef::inline("writer completed"),
                    request_attention: false,
                };
                let runtime_configuration = RuntimeConfigurationEvidence::default();
                let artifact =
                    lionclaw::testing::capture_prepared_test_artifact(&request, HEAD_SHA).await?;
                Ok(RoleTurnOutcome {
                    handoff: Some(handoff),
                    artifact: Some(artifact),
                    runtime_configuration,
                    runtime_usage: Default::default(),
                    final_response: "writer completed".into(),
                })
            }
            OutputSemantics::EmitsVerdict => {
                let attempt = self.validator_attempts.fetch_add(1, Ordering::SeqCst);
                {
                    let mut assignment = self.validator_assignment.lock().unwrap();
                    match &*assignment {
                        Some(generation) => {
                            assert!(request.task_id.is_none());
                            assert_eq!(
                                request.assignment_epoch, *generation,
                                "invalid-output rework must retain the conversation generation"
                            );
                        }
                        None => {
                            *assignment = Some(request.assignment_epoch);
                        }
                    }
                }

                let runtime_configuration = RuntimeConfigurationEvidence {
                    requested_model: Some(RETAINED_MODEL.into()),
                    applied_model: Some(RETAINED_MODEL.into()),
                    ..Default::default()
                };
                if attempt == 0 {
                    self.validator_started.wait().await;
                    std::future::pending().await
                }

                let handoff = Handoff::Validate {
                    done: true,
                    report: PayloadRef::inline("validator recovered"),
                    items: vec![ValidationItem {
                        item_id: lionclaw::model::AssertionId::new("STYLE-OK").unwrap(),
                        passed: true,
                    }],
                    passed: true,
                    request_attention: false,
                };
                Ok(RoleTurnOutcome {
                    handoff: Some(handoff),
                    artifact: None,
                    runtime_configuration,
                    runtime_usage: Default::default(),
                    final_response: RETAINED_RESPONSE.into(),
                })
            }
            output => panic!("unexpected output semantics in recovery test: {output:?}"),
        }
    }
}

async fn assert_durable_turn_precedes_handoff(
    store: &MissionStore,
    mission_id: &MissionId,
    effect_id: &EffectId,
    state: &MissionState,
) {
    let receipt = state
        .role_attempt_receipts
        .get(effect_id)
        .expect("active role receipt");
    assert_eq!(receipt.disposition, RoleAttemptDisposition::Active);
    assert!(receipt.final_response.is_none());
    assert!(
        receipt.handoff.is_none(),
        "the crash boundary must precede handoff acknowledgement"
    );

    let events = store.load(mission_id).await.expect("durable events");
    assert!(
        events.iter().all(|event| !matches!(
            &event.event,
            MissionEvent::RoleTurnCompleted {
                effect_id: observed_effect,
                ..
            } if observed_effect == effect_id
        )),
        "no atomic role outcome may precede the crash"
    );
}

#[async_trait]
impl RoleRunner for UnacknowledgedRunner {
    async fn run(&self, request: RoleTurnRequest) -> Result<RoleTurnOutcome, TypedFailure> {
        if let Some(outcome) = judgment_outcome(&request) {
            return Ok(outcome);
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        let effect_dir = role_effect_dir(&request);
        std::fs::create_dir_all(effect_dir.join("handoff")).unwrap();
        std::fs::write(effect_dir.join("runner-owned-marker"), b"retain").unwrap();
        std::fs::write(
            effect_dir.join("handoff/handoff.json"),
            r#"{"schema":"lionclaw.mission.work-handoff.v2","type":"work","done":true,"report":"untrusted pre-turn handoff","request_attention":false}"#,
        )
        .unwrap();
        Ok(RoleTurnOutcome {
            handoff: Some(Handoff::Work {
                done: true,
                report: PayloadRef::inline("unacknowledged"),
                request_attention: false,
            }),
            artifact: None,
            runtime_configuration: RuntimeConfigurationEvidence::default(),
            runtime_usage: Default::default(),
            final_response: "must not settle".into(),
        })
    }
}

#[async_trait]
impl EffectCleaner for CountingCleaner {
    async fn quiesce(&self, _request: &EffectCleanupRequest) -> Result<(), EffectCleanupFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn cleanup(&self, request: EffectCleanupRequest) -> Result<(), EffectCleanupFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let exact = effect_dir(&request);
        if exact.exists() {
            tokio::fs::remove_dir_all(exact)
                .await
                .map_err(|error| EffectCleanupFailure {
                    resource: EffectResource::EffectDirectory,
                    detail: error.to_string(),
                })?;
        }
        Ok(())
    }
}

#[async_trait]
impl EffectCleaner for ArtifactDiscardCleaner {
    async fn quiesce(&self, _request: &EffectCleanupRequest) -> Result<(), EffectCleanupFailure> {
        Ok(())
    }

    async fn cleanup(&self, request: EffectCleanupRequest) -> Result<(), EffectCleanupFailure> {
        let exact_effect_dir = effect_dir(&request);
        lionclaw::workspace::remove_dir(&exact_effect_dir)
            .await
            .map_err(|error| EffectCleanupFailure {
                resource: EffectResource::EffectDirectory,
                detail: error.to_string(),
            })?;
        if request.discard_artifact {
            lionclaw::workspace::discard_worker_result(
                &request.workspace_dir,
                request.mission_id.as_str(),
                &request.effect_id,
            )
            .await
            .map_err(|error| EffectCleanupFailure {
                resource: EffectResource::WriterRef,
                detail: error.to_string(),
            })?;
        }
        self.calls.lock().unwrap().push(request);
        Ok(())
    }
}

#[async_trait]
impl EffectCleaner for DurableFoldCleaner {
    async fn quiesce(&self, _request: &EffectCleanupRequest) -> Result<(), EffectCleanupFailure> {
        Ok(())
    }

    async fn cleanup(&self, request: EffectCleanupRequest) -> Result<(), EffectCleanupFailure> {
        let state = self
            .store
            .require_state(&request.mission_id)
            .await
            .expect("durable folded state immediately before cleanup");
        assert!(
            state.inflight.contains_key(&request.effect_id),
            "effect must remain active until cleanup completes"
        );
        let receipt = state
            .role_attempt_receipts
            .get(&request.effect_id)
            .expect("active role receipt must survive until cleanup completes");
        assert_eq!(receipt.effect_id, request.effect_id);
        assert_eq!(receipt.disposition, RoleAttemptDisposition::Active);
        assert!(
            receipt.handoff.is_none(),
            "schema-24 outcomes remain private until atomic completion"
        );

        let exact_effect_dir = effect_dir(&request);
        if exact_effect_dir.exists() {
            tokio::fs::remove_dir_all(&exact_effect_dir)
                .await
                .map_err(|error| EffectCleanupFailure {
                    resource: EffectResource::EffectDirectory,
                    detail: error.to_string(),
                })?;
            assert!(
                !exact_effect_dir.exists(),
                "cleanup must remove the exact effect directory"
            );
            self.deletions.fetch_add(1, Ordering::SeqCst);
            if let Some(pause) = &self.after_first_deletion {
                pause.entered.notify_one();
                pause.release.notified().await;
            }
        }
        Ok(())
    }
}

impl EventSink for RecoveryCompletionGate {
    fn emit(&self, event: &EventEnvelope) {
        if matches!(
            &event.event,
            MissionEvent::RoleTurnCompleted {
                outcome: Err(failure),
                ..
            } if failure.category() == self.category
        ) {
            self.entered.wait();
            self.release.wait();
        }
    }
}

#[async_trait]
impl RoleRunner for BlockingRunner {
    async fn run(&self, request: RoleTurnRequest) -> Result<RoleTurnOutcome, TypedFailure> {
        if let Some(outcome) = judgment_outcome(&request) {
            return Ok(outcome);
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.wait().await;
        self.release.wait().await;
        lionclaw::testing::prepare_test_workspace(&request).await?;
        let handoff = Handoff::Work {
            done: true,
            report: PayloadRef::inline("done"),
            request_attention: false,
        };
        let runtime_configuration = lionclaw::model::RuntimeConfigurationEvidence {
            requested_model: Some("blocking-test".to_string()),
            applied_model: Some("blocking-test".to_string()),
            ..Default::default()
        };
        let artifact =
            lionclaw::testing::capture_prepared_test_artifact(&request, HEAD_SHA).await?;
        Ok(RoleTurnOutcome {
            handoff: Some(handoff),
            artifact: Some(artifact),
            runtime_configuration,
            runtime_usage: Default::default(),
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
    async fn quiesce(&self, _request: &EffectCleanupRequest) -> Result<(), EffectCleanupFailure> {
        Ok(())
    }

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
    async fn quiesce(&self, _request: &EffectCleanupRequest) -> Result<(), EffectCleanupFailure> {
        Ok(())
    }

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

fn adversarial_retention_runner(
    mode: AdversarialRetentionMode,
    started: Arc<Barrier>,
    calls: Arc<AtomicUsize>,
) -> Arc<dyn RoleRunner> {
    Arc::new(AdversarialRetentionRunner {
        mode,
        started,
        calls,
    })
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
        "test-image".to_string(),
        EngineServices::new(
            Arc::new(review_runner(vec![(true, vec![])])),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_role_report_survives_driver_cancellation_and_exact_cleanup() {
    let dir = test_repository();
    let started = Arc::new(Barrier::new(2));
    let calls = Arc::new(AtomicUsize::new(0));
    let completion_entered = Arc::new(StdBarrier::new(2));
    let completion_release = Arc::new(StdBarrier::new(2));
    let store = MissionStore::open(dir.path())
        .await
        .unwrap()
        .with_sink(Arc::new(RecoveryCompletionGate {
            category: "interrupted",
            entered: completion_entered.clone(),
            release: completion_release.clone(),
        }));
    let cleaner = Arc::new(DurableFoldCleaner {
        store: store.clone(),
        deletions: AtomicUsize::new(0),
        after_first_deletion: None,
    });
    let engine = Arc::new(Engine::new(
        store.clone(),
        test_mission_type(),
        "test-image".to_string(),
        EngineServices::new(
            Arc::new(RetainedHandoffRunner {
                started: started.clone(),
                calls: calls.clone(),
                handoff: RetainedHandoff::Valid,
            }),
            Arc::new(MockOracleRunner::exiting(0)),
            cleaner.clone(),
            Arc::new(MockClock::default()),
        ),
    ));
    let mission_id = create_approved_mission(&engine, dir.path()).await;
    let snapshot = store
        .rebuild_cursors(&mission_id, 10)
        .await
        .expect("seed an approved-state snapshot");
    let snapshot_head = snapshot.head;
    assert_eq!(
        store.snapshot_meta(&mission_id).await.unwrap(),
        Some((snapshot_head, REDUCER_VERSION))
    );

    let driver = tokio::spawn({
        let engine = engine.clone();
        let mission_id = mission_id.clone();
        async move { engine.advance(&mission_id).await }
    });
    started.wait().await;
    let active = store.require_state(&mission_id).await.unwrap();
    let (effect_id, _) = active.inflight.iter().next().expect("active role effect");
    let effect_id = effect_id.clone();
    let exact_effect_dir = store
        .lionclaw_dir()
        .join("missions")
        .join(mission_id.as_str())
        .join("effects")
        .join(effect_id.as_str());
    assert!(exact_effect_dir.join("handoff/handoff.json").is_file());
    assert_durable_turn_precedes_handoff(&store, &mission_id, &effect_id, &active).await;
    driver.abort();
    assert!(driver.await.unwrap_err().is_cancelled());

    let recovery = tokio::spawn({
        let engine = engine.clone();
        let mission_id = mission_id.clone();
        async move { engine.advance(&mission_id).await }
    });
    completion_entered.wait();
    recovery.abort();
    completion_release.wait();
    assert!(recovery.await.unwrap_err().is_cancelled());

    assert_eq!(calls.load(Ordering::SeqCst), 1, "role must not rerun");
    assert_eq!(cleaner.deletions.load(Ordering::SeqCst), 1);
    assert!(!exact_effect_dir.exists());

    let events = store.load(&mission_id).await.expect("durable events");
    let completion_count = events
        .iter()
        .filter(|event| {
            matches!(
                &event.event,
                MissionEvent::RoleTurnCompleted {
                    effect_id: completed_effect,
                    outcome: Err(failure),
                    ..
                } if completed_effect == &effect_id && failure.category() == "interrupted"
            )
        })
        .count();
    assert_eq!(completion_count, 1, "recovery settles exactly one outcome");

    let replayed = fold(events).expect("full replay");
    let task_id = TaskId::new("fix").unwrap();
    let receipt = replayed
        .task_last_role_attempt(&task_id)
        .expect("settled recovery receipt");
    assert_eq!(receipt.effect_id, effect_id);
    assert_eq!(receipt.failure().unwrap().category(), "interrupted");
    assert!(receipt.accepted_report().is_none());
    assert!(matches!(
        &receipt.disposition,
        RoleAttemptDisposition::Failed { .. }
    ));
    assert!(replayed.inflight.is_empty());

    let (persisted_snapshot_head, reducer) = store
        .snapshot_meta(&mission_id)
        .await
        .unwrap()
        .expect("approved-state snapshot");
    assert_eq!(persisted_snapshot_head, snapshot_head);
    assert_eq!(reducer, REDUCER_VERSION);
    assert!(
        persisted_snapshot_head < replayed.head,
        "recovery must remain a nonempty snapshot tail"
    );
    let from_snapshot_tail = store
        .load_state_snapshotted(&mission_id)
        .await
        .expect("snapshot-tail load")
        .expect("mission state");
    assert_eq!(from_snapshot_tail, replayed);
    assert!(from_snapshot_tail
        .task_last_role_attempt(&task_id)
        .and_then(lionclaw::model::RoleAttemptReceipt::accepted_report)
        .is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_writer_turn_without_handoff_recovers_as_same_conversation_checkpoint() {
    let dir = test_repository();
    let started = Arc::new(Barrier::new(2));
    let calls = Arc::new(AtomicUsize::new(0));
    let store = MissionStore::open(dir.path()).await.unwrap();
    let cleaner = Arc::new(DurableFoldCleaner {
        store: store.clone(),
        deletions: AtomicUsize::new(0),
        after_first_deletion: None,
    });
    let mut mission_type = test_mission_type();
    mission_type
        .edit_for_testing(|definition| definition.stop = lionclaw::model::StopBar::Attested);
    let engine = Arc::new(Engine::new(
        store.clone(),
        mission_type,
        "test-image".to_string(),
        EngineServices::new(
            Arc::new(RetainedHandoffRunner {
                started: started.clone(),
                calls: calls.clone(),
                handoff: RetainedHandoff::Absent,
            }),
            Arc::new(MockOracleRunner::exiting(0)),
            cleaner.clone(),
            Arc::new(MockClock::default()),
        ),
    ));
    let mission_id = create_approved_mission(&engine, dir.path()).await;
    let snapshot = store
        .rebuild_cursors(&mission_id, 10)
        .await
        .expect("seed an approved-state snapshot");

    let driver = tokio::spawn({
        let engine = engine.clone();
        let mission_id = mission_id.clone();
        async move { engine.advance(&mission_id).await }
    });
    started.wait().await;
    let active = store.require_state(&mission_id).await.unwrap();
    let (effect_id, effect) = active.inflight.iter().next().expect("active writer effect");
    let effect_id = effect_id.clone();
    let role_instance = effect
        .role_turn_provenance()
        .expect("role request identity")
        .role_instance;
    assert_durable_turn_precedes_handoff(&store, &mission_id, &effect_id, &active).await;
    driver.abort();
    assert!(driver.await.unwrap_err().is_cancelled());

    let recovered = engine
        .advance(&mission_id)
        .await
        .expect("recover optional writer checkpoint");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "writer must not rerun");
    assert_eq!(cleaner.deletions.load(Ordering::SeqCst), 1);
    assert_eq!(recovered.disposition, MissionDisposition::Parked);

    let state = store.require_state(&mission_id).await.unwrap();
    assert!(state.inflight.is_empty());
    let receipt = state
        .role_attempt_receipts
        .get(&effect_id)
        .expect("settled writer receipt");
    assert!(matches!(
        receipt.disposition,
        RoleAttemptDisposition::Failed { .. }
    ));
    assert!(receipt.handoff.is_none());
    assert_eq!(
        receipt.failure().map(TypedFailure::category),
        Some("interrupted")
    );
    let conversation = state
        .conversations
        .get(&role_instance)
        .expect("same conversation retained");
    assert_eq!(conversation.lifecycle, ConversationLifecycle::Ready);
    assert!(conversation.final_response.is_none());

    let events = store.load(&mission_id).await.expect("durable events");
    let completion = events
        .iter()
        .find_map(|event| match &event.event {
            MissionEvent::RoleTurnCompleted {
                effect_id: completed_effect,
                outcome: Err(failure),
                ..
            } if completed_effect == &effect_id => Some(failure),
            _ => None,
        })
        .expect("recovered interrupted checkpoint");
    assert_eq!(completion.category(), "interrupted");
    assert_eq!(fold(events).expect("full replay"), state);
    assert!(
        snapshot.head < state.head,
        "recovery must remain a nonempty snapshot tail"
    );
    assert_eq!(
        store
            .load_state_snapshotted(&mission_id)
            .await
            .expect("snapshot-tail load")
            .expect("mission state"),
        state
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_handoff_is_refused_before_recording_over_limit_state() {
    let dir = test_repository();
    let calls = Arc::new(AtomicUsize::new(0));
    let cleaner = Arc::new(ArtifactDiscardCleaner::default());
    let store = MissionStore::open(dir.path()).await.unwrap();
    let engine = Engine::new(
        store.clone(),
        test_mission_type(),
        "test-image".to_string(),
        EngineServices::new(
            adversarial_retention_runner(
                AdversarialRetentionMode::RefusedBeforeHandoff,
                Arc::new(Barrier::new(1)),
                calls.clone(),
            ),
            Arc::new(MockOracleRunner::exiting(0)),
            cleaner.clone(),
            Arc::new(MockClock::default()),
        ),
    );
    let mission_id = create_approved_mission(&engine, dir.path()).await;

    let outcome = engine.advance(&mission_id).await.unwrap();

    assert_eq!(outcome.disposition, MissionDisposition::Terminal);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(outcome.state.current_sha, HEAD_SHA);
    let receipt = outcome
        .state
        .role_attempt_receipts
        .values()
        .find(|receipt| {
            matches!(
                &receipt.source,
                RoleEffectSource::Turn { request, .. }
                    if request.role_instance.as_str() == "implementer"
            )
        })
        .expect("successful writer receipt");
    assert!(mission_ref_exists(
        dir.path(),
        &mission_id,
        &receipt.effect_id
    ));
    {
        let cleanup = cleaner.calls.lock().unwrap();
        assert!(!cleanup.is_empty());
    }
    let events = store.load(&mission_id).await.unwrap();
    assert!(events.iter().any(|event| matches!(
        &event.event,
        MissionEvent::RoleTurnCompleted { effect_id, outcome: Ok(_)}
            if effect_id == &receipt.effect_id
    )));
    assert_eq!(fold(events).unwrap(), outcome.state);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleanup_backstop_discards_artifact_after_noncompliant_post_ack_growth() {
    let dir = test_repository();
    let calls = Arc::new(AtomicUsize::new(0));
    let cleaner = Arc::new(ArtifactDiscardCleaner::default());
    let store = MissionStore::open(dir.path()).await.unwrap();
    let engine = Engine::new(
        store.clone(),
        test_mission_type(),
        "test-image".to_string(),
        EngineServices::new(
            adversarial_retention_runner(
                AdversarialRetentionMode::AcceptedThenSuccess,
                Arc::new(Barrier::new(1)),
                calls.clone(),
            ),
            Arc::new(MockOracleRunner::exiting(0)),
            cleaner.clone(),
            Arc::new(MockClock::default()),
        ),
    );
    let mission_id = create_approved_mission(&engine, dir.path()).await;
    let snapshot = store.rebuild_cursors(&mission_id, 10).await.unwrap();

    let outcome = engine.advance(&mission_id).await.unwrap();

    assert_eq!(outcome.disposition, MissionDisposition::Terminal);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(outcome.state.current_sha, HEAD_SHA);
    let receipt = outcome
        .state
        .role_attempt_receipts
        .values()
        .find(|receipt| {
            matches!(
                &receipt.source,
                RoleEffectSource::Turn { request, .. }
                    if request.role_instance.as_str() == "implementer"
            )
        })
        .expect("successful writer receipt");
    assert_eq!(
        receipt.accepted_report(),
        Some(&PayloadRef::inline(RETAINED_REPORT))
    );
    assert!(mission_ref_exists(
        dir.path(),
        &mission_id,
        &receipt.effect_id
    ));
    {
        let cleanup = cleaner.calls.lock().unwrap();
        assert!(!cleanup.is_empty());
    }
    let events = store.load(&mission_id).await.unwrap();
    assert_eq!(fold(events).unwrap(), outcome.state);
    assert!(snapshot.head < outcome.state.head);
    assert_eq!(
        store
            .load_state_snapshotted(&mission_id)
            .await
            .unwrap()
            .unwrap(),
        outcome.state
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_after_accepted_handoff_recovers_limit_and_discards_artifact() {
    let dir = test_repository();
    let started = Arc::new(Barrier::new(2));
    let calls = Arc::new(AtomicUsize::new(0));
    let cleaner = Arc::new(ArtifactDiscardCleaner::default());
    let store = MissionStore::open(dir.path()).await.unwrap();
    let engine = Arc::new(Engine::new(
        store.clone(),
        test_mission_type(),
        "test-image".to_string(),
        EngineServices::new(
            adversarial_retention_runner(
                AdversarialRetentionMode::AcceptedThenCrash,
                started.clone(),
                calls.clone(),
            ),
            Arc::new(MockOracleRunner::exiting(0)),
            cleaner.clone(),
            Arc::new(MockClock::default()),
        ),
    ));
    let mission_id = create_approved_mission(&engine, dir.path()).await;
    let snapshot = store.rebuild_cursors(&mission_id, 10).await.unwrap();
    let driver = tokio::spawn({
        let engine = engine.clone();
        let mission_id = mission_id.clone();
        async move { engine.advance(&mission_id).await }
    });
    started.wait().await;
    let active = store.require_state(&mission_id).await.unwrap();
    let (effect_id, receipt) = active
        .role_attempt_receipts
        .iter()
        .find(|(_, receipt)| receipt.disposition == RoleAttemptDisposition::Active)
        .expect("active atomic role turn");
    let effect_id = effect_id.clone();
    assert!(receipt.handoff.is_none());
    assert!(mission_ref_exists(dir.path(), &mission_id, &effect_id));
    driver.abort();
    assert!(driver.await.unwrap_err().is_cancelled());

    let recovered = engine.advance(&mission_id).await.unwrap();

    assert_eq!(recovered.disposition, MissionDisposition::Parked);
    assert_eq!(calls.load(Ordering::SeqCst), 1, "role must not rerun");
    assert_eq!(recovered.state.current_sha, BASE_SHA);
    let receipt = recovered
        .state
        .role_attempt_receipts
        .get(&effect_id)
        .expect("recovered attempt receipt");
    assert_eq!(
        receipt.failure().unwrap().evidence().code.as_deref(),
        Some("driver.interrupted")
    );
    assert!(receipt.accepted_report().is_none());
    assert!(!mission_ref_exists(dir.path(), &mission_id, &effect_id));
    {
        let cleanup = cleaner.calls.lock().unwrap();
        assert_eq!(cleanup.len(), 1);
        assert!(cleanup[0].discard_artifact);
    }
    let events = store.load(&mission_id).await.unwrap();
    assert_eq!(fold(events).unwrap(), recovered.state);
    assert!(snapshot.head < recovered.state.head);
    assert_eq!(
        store
            .load_state_snapshotted(&mission_id)
            .await
            .unwrap()
            .unwrap(),
        recovered.state
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handoff_rejection_and_ordinary_failure_outrank_retention_backstop() {
    for (mode, expected_code) in [
        (
            AdversarialRetentionMode::AcceptedThenFailure,
            "runtime.original_failure",
        ),
        (
            AdversarialRetentionMode::RejectedThenFailure,
            "handoff.schema",
        ),
    ] {
        let dir = test_repository();
        let cleaner = Arc::new(ArtifactDiscardCleaner::default());
        let store = MissionStore::open(dir.path()).await.unwrap();
        let engine = Engine::new(
            store.clone(),
            test_mission_type(),
            "test-image".to_string(),
            EngineServices::new(
                adversarial_retention_runner(
                    mode,
                    Arc::new(Barrier::new(1)),
                    Arc::new(AtomicUsize::new(0)),
                ),
                Arc::new(MockOracleRunner::exiting(0)),
                cleaner.clone(),
                Arc::new(MockClock::default()),
            ),
        );
        let mission_id = create_approved_mission(&engine, dir.path()).await;

        let outcome = engine.advance(&mission_id).await.unwrap();

        let receipt = outcome
            .state
            .role_attempt_receipts
            .values()
            .find(|receipt| receipt.failure().is_some())
            .expect("settled failure receipt");
        assert_eq!(
            receipt.failure().unwrap().evidence().code.as_deref(),
            Some(expected_code)
        );
        assert!(receipt.accepted_report().is_none());
        assert_eq!(outcome.state.current_sha, BASE_SHA);
        assert!(!mission_ref_exists(
            dir.path(),
            &mission_id,
            &receipt.effect_id
        ));
        {
            let cleanup = cleaner.calls.lock().unwrap();
            assert!(!cleanup.is_empty());
            assert!(cleanup.iter().all(|request| request.discard_artifact));
        }
        assert_eq!(
            fold(store.load(&mission_id).await.unwrap()).unwrap(),
            outcome.state
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_turn_with_oversized_retained_state_recovers_as_typed_failure() {
    let dir = test_repository();
    let started = Arc::new(Barrier::new(2));
    let calls = Arc::new(AtomicUsize::new(0));
    let store = MissionStore::open(dir.path()).await.unwrap();
    let cleaner = Arc::new(DurableFoldCleaner {
        store: store.clone(),
        deletions: AtomicUsize::new(0),
        after_first_deletion: None,
    });
    let engine = Arc::new(Engine::new(
        store.clone(),
        test_mission_type(),
        "test-image".to_string(),
        EngineServices::new(
            Arc::new(RetainedStateOverflowRunner {
                started: started.clone(),
                calls: calls.clone(),
            }),
            Arc::new(MockOracleRunner::exiting(0)),
            cleaner.clone(),
            Arc::new(MockClock::default()),
        ),
    ));
    let mission_id = create_approved_mission(&engine, dir.path()).await;
    let snapshot = store
        .rebuild_cursors(&mission_id, 10)
        .await
        .expect("seed approved-state snapshot");
    let driver = tokio::spawn({
        let engine = engine.clone();
        let mission_id = mission_id.clone();
        async move { engine.advance(&mission_id).await }
    });
    started.wait().await;
    let active = store.require_state(&mission_id).await.unwrap();
    let (effect_id, _) = active.inflight.iter().next().expect("active writer");
    let effect_id = effect_id.clone();
    assert_durable_turn_precedes_handoff(&store, &mission_id, &effect_id, &active).await;
    driver.abort();
    assert!(driver.await.unwrap_err().is_cancelled());

    let recovered = engine
        .advance(&mission_id)
        .await
        .expect("recover retained-state limit failure");
    assert_eq!(recovered.disposition, MissionDisposition::Parked);
    assert_eq!(calls.load(Ordering::SeqCst), 1, "role must not rerun");
    assert_eq!(cleaner.deletions.load(Ordering::SeqCst), 1);
    let receipt = recovered
        .state
        .role_attempt_receipts
        .get(&effect_id)
        .expect("settled retained-state receipt");
    let failure = receipt.failure().expect("typed retained-state failure");
    assert_eq!(
        failure.evidence().code.as_deref(),
        Some("driver.interrupted")
    );
    assert!(failure.detail().contains("previous mission driver exited"));
    assert!(receipt.handoff.is_none());
    assert!(receipt.final_response.is_none());

    let events = store.load(&mission_id).await.unwrap();
    assert!(events.iter().any(|event| matches!(
        &event.event,
        MissionEvent::RoleTurnCompleted { effect_id: observed, outcome: Err(_) }
            if observed == &effect_id
    )));
    assert_eq!(fold(events).unwrap(), recovered.state);
    assert!(snapshot.head < recovered.state.head);
    assert_eq!(
        store
            .load_state_snapshotted(&mission_id)
            .await
            .unwrap()
            .unwrap(),
        recovered.state
    );

    let json = std::process::Command::new(env!("CARGO_BIN_EXE_lionclaw"))
        .args(["mission", "status", mission_id.as_str(), "--json", "--repo"])
        .arg(dir.path())
        .output()
        .unwrap();
    assert!(json.status.success());
    let json: serde_json::Value = serde_json::from_slice(&json.stdout).unwrap();
    let projected = json["role_attempt_receipts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|receipt| receipt["effect_id"] == effect_id.as_str())
        .unwrap();
    assert_eq!(
        projected["disposition"]["failure"]["evidence"]["code"],
        "driver.interrupted"
    );
    assert_ne!(projected["handoff"]["outcome"], "accepted");
    let human = std::process::Command::new(env!("CARGO_BIN_EXE_lionclaw"))
        .args(["mission", "status", mission_id.as_str(), "--repo"])
        .arg(dir.path())
        .output()
        .unwrap();
    assert!(human.status.success());
    let human = String::from_utf8(human.stdout).unwrap();
    assert!(human.contains("driver.interrupted"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_abort_outranks_recovered_retained_state_failure() {
    let dir = test_repository();
    let started = Arc::new(Barrier::new(2));
    let calls = Arc::new(AtomicUsize::new(0));
    let store = MissionStore::open(dir.path()).await.unwrap();
    let engine = Arc::new(Engine::new(
        store.clone(),
        test_mission_type(),
        "test-image".to_string(),
        EngineServices::new(
            Arc::new(RetainedStateOverflowRunner {
                started: started.clone(),
                calls: calls.clone(),
            }),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    ));
    let mission_id = create_approved_mission(&engine, dir.path()).await;
    let driver = tokio::spawn({
        let engine = engine.clone();
        let mission_id = mission_id.clone();
        async move { engine.advance(&mission_id).await }
    });
    started.wait().await;
    let active = store.require_state(&mission_id).await.unwrap();
    let (effect_id, _) = active.inflight.iter().next().expect("active writer");
    let effect_id = effect_id.clone();
    driver.abort();
    assert!(driver.await.unwrap_err().is_cancelled());
    engine
        .abort(&mission_id, "operator abort after completed turn")
        .await
        .unwrap();

    let recovered = engine.advance(&mission_id).await.unwrap();
    assert!(matches!(
        recovered.state.phase,
        MissionPhase::Aborted { .. }
    ));
    let receipt = recovered
        .state
        .role_attempt_receipts
        .get(&effect_id)
        .unwrap();
    assert!(matches!(
        receipt.failure(),
        Some(TypedFailure::OperatorAborted { .. })
    ));
    assert!(receipt.effective_runtime_configuration().is_some());
    assert!(receipt.final_response.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn ordinary_role_failure_outranks_cleanup_retained_state_finding() {
    let dir = test_repository();
    let store = MissionStore::open(dir.path()).await.unwrap();
    let engine = Engine::new(
        store.clone(),
        test_mission_type(),
        "test-image".to_string(),
        EngineServices::new(
            Arc::new(CompletedOverflowFailureRunner),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );
    let mission_id = create_approved_mission(&engine, dir.path()).await;

    let outcome = engine.advance(&mission_id).await.unwrap();
    let receipt = outcome
        .state
        .role_attempt_receipts
        .values()
        .find(|receipt| receipt.failure().is_some())
        .expect("ordinary failed role receipt");
    assert_eq!(
        receipt.failure().unwrap().evidence().code.as_deref(),
        Some("runtime.original_failure")
    );
    assert_eq!(
        receipt
            .effective_runtime_configuration()
            .and_then(|configuration| configuration.applied_model.as_deref()),
        Some(RETAINED_MODEL)
    );
    assert_eq!(
        receipt.final_response.as_ref(),
        Some(&PayloadRef::inline(RETAINED_RESPONSE))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_validator_turn_without_handoff_recovers_as_typed_rework() {
    let dir = test_repository();
    let validator_started = Arc::new(Barrier::new(2));
    let runner = Arc::new(MissingValidatorHandoffRunner {
        validator_started: validator_started.clone(),
        calls: AtomicUsize::new(0),
        validator_attempts: AtomicUsize::new(0),
        validator_assignment: Mutex::new(None),
    });
    let store = MissionStore::open(dir.path()).await.unwrap();
    let mut mission_type = test_mission_type();
    mission_type
        .edit_for_testing(|definition| definition.stop = lionclaw::model::StopBar::Attested);
    let engine = Arc::new(Engine::new(
        store.clone(),
        mission_type,
        "test-image".to_string(),
        EngineServices::new(
            runner.clone(),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    ));
    let mission_id = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "recover a validator turn without a handoff",
            BASE_SHA,
        )
        .await
        .unwrap();
    engine
        .propose_plan(&mission_id, proposal(0, advisory_plan()))
        .await
        .unwrap();
    approve_plan(&engine, &mission_id).await;
    let snapshot = store
        .rebuild_cursors(&mission_id, 10)
        .await
        .expect("seed an approved-state snapshot");

    let driver = tokio::spawn({
        let engine = engine.clone();
        let mission_id = mission_id.clone();
        async move { engine.advance(&mission_id).await }
    });
    validator_started.wait().await;
    let active = store.require_state(&mission_id).await.unwrap();
    let (effect_id, effect) = active
        .inflight
        .iter()
        .find(|(_, effect)| {
            effect
                .role_turn_provenance()
                .is_some_and(|request| request.role_instance.as_str() == "reviewer")
        })
        .expect("active validator effect");
    let effect_id = effect_id.clone();
    let first_request = effect.role_turn_provenance().expect("validator identity");
    let role_instance = first_request.role_instance.clone();
    assert_durable_turn_precedes_handoff(&store, &mission_id, &effect_id, &active).await;
    driver.abort();
    assert!(driver.await.unwrap_err().is_cancelled());

    let parked = engine
        .advance(&mission_id)
        .await
        .expect("recover and rework mandatory validator output");
    let attention_id = parked
        .state
        .open_attention
        .keys()
        .next()
        .expect("validator recovery attention")
        .clone();
    engine
        .decide(
            &mission_id,
            &attention_id,
            DecisionAction::Retry,
            "retry interrupted validator",
        )
        .await
        .expect("retry validator");
    engine
        .advance(&mission_id)
        .await
        .expect("reworked validator");
    assert_eq!(
        runner.calls.load(Ordering::SeqCst),
        3,
        "writer runs once and validator runs once plus one bounded rework"
    );
    assert_eq!(runner.validator_attempts.load(Ordering::SeqCst), 2);

    let state = store.require_state(&mission_id).await.unwrap();
    let failed = state
        .role_attempt_receipts
        .get(&effect_id)
        .expect("first validator receipt remains inspectable");
    let failure = failed.failure().expect("typed missing-handoff failure");
    assert_eq!(failure.category(), "interrupted");
    assert_eq!(
        failure.evidence().code.as_deref(),
        Some("driver.interrupted")
    );
    assert!(failure.evidence().final_response.is_empty());
    assert!(failed.handoff.is_none());
    assert!(matches!(
        failed.disposition,
        RoleAttemptDisposition::Failed { .. }
    ));
    let RoleEffectSource::Turn {
        request: failed_request,
        ..
    } = &failed.source;
    assert_eq!(failed_request.role_instance, role_instance);

    let current = state
        .role_attempt_receipts
        .values()
        .filter(|receipt| {
            matches!(
                &receipt.source,
                RoleEffectSource::Turn { request, .. }
                    if request.role_instance.as_str() == "reviewer"
                        && request.assertion_ids
                            == [lionclaw::model::AssertionId::new("STYLE-OK").unwrap()]
            )
        })
        .max_by_key(|receipt| match &receipt.source {
            RoleEffectSource::Turn { request, .. } => request.attempt_no,
        })
        .expect("reworked validator receipt");
    let RoleEffectSource::Turn {
        request: current_request,
        ..
    } = &current.source;
    assert_eq!(
        current_request.role_instance, role_instance,
        "bounded invalid-output rework must resume the same conversation"
    );
    assert!(matches!(
        current.disposition,
        RoleAttemptDisposition::Succeeded { .. }
    ));
    let conversation = state
        .conversations
        .get(&role_instance)
        .expect("validator conversation");
    assert_eq!(conversation.invalid_handoff_reworks, 0);

    let events = store.load(&mission_id).await.expect("durable events");
    assert!(events.iter().any(|event| matches!(
        &event.event,
        MissionEvent::RoleTurnCompleted {
            effect_id: completed_effect,
            outcome: Err(failure),
            ..
        } if completed_effect == &effect_id
            && failure.category() == "interrupted"
            && failure.evidence().code.as_deref() == Some("driver.interrupted")
    )));
    assert_eq!(fold(events).expect("full replay"), state);
    assert!(
        snapshot.head < state.head,
        "recovery and rework must remain a nonempty snapshot tail"
    );
    assert_eq!(
        store
            .load_state_snapshotted(&mission_id)
            .await
            .expect("snapshot-tail load")
            .expect("mission state"),
        state
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_retained_handoff_recovers_as_typed_invalid_output() {
    let dir = test_repository();
    let started = Arc::new(Barrier::new(1));
    let calls = Arc::new(AtomicUsize::new(0));
    let store = MissionStore::open(dir.path()).await.unwrap();
    let cleaner = Arc::new(DurableFoldCleaner {
        store: store.clone(),
        deletions: AtomicUsize::new(0),
        after_first_deletion: None,
    });
    let engine = Engine::new(
        store.clone(),
        test_mission_type(),
        "test-image".to_string(),
        EngineServices::new(
            Arc::new(RetainedHandoffRunner {
                started: started.clone(),
                calls: calls.clone(),
                handoff: RetainedHandoff::Malformed,
            }),
            Arc::new(MockOracleRunner::exiting(0)),
            cleaner.clone(),
            Arc::new(MockClock::default()),
        ),
    );
    let mission_id = create_approved_mission(&engine, dir.path()).await;
    let snapshot = store
        .rebuild_cursors(&mission_id, 10)
        .await
        .expect("seed an approved-state snapshot");
    let snapshot_head = snapshot.head;

    let settled = engine.advance(&mission_id).await.unwrap();
    assert_eq!(settled.disposition, MissionDisposition::Parked);
    assert!(settled.state.inflight.is_empty());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "invalid output retries only within the configured recovery budget"
    );
    assert_eq!(cleaner.deletions.load(Ordering::SeqCst), 3);

    let task_id = TaskId::new("fix").unwrap();
    let receipt = settled
        .state
        .task_last_role_attempt(&task_id)
        .expect("settled implementation receipt");
    let effect_id = &receipt.effect_id;
    assert!(matches!(
        &receipt.disposition,
        RoleAttemptDisposition::Failed { .. }
    ));
    let failure = receipt.rejection().expect("typed rejected outcome");
    assert!(matches!(failure, TypedFailure::InvalidOutput { .. }));
    assert_eq!(failure.category(), "invalid_output");
    assert_eq!(failure.evidence().code.as_deref(), Some("handoff.schema"));
    assert!(failure.evidence().detail.contains("does not match"));

    let exact_effect_dir = store
        .lionclaw_dir()
        .join("missions")
        .join(mission_id.as_str())
        .join("effects")
        .join(effect_id.as_str());
    assert!(!exact_effect_dir.exists());
    let task_failure = settled
        .state
        .task_last_failure(&task_id)
        .expect("typed recovery failure");
    assert_eq!(task_failure, failure);

    let events = store.load(&mission_id).await.expect("durable events");
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                &event.event,
                MissionEvent::RoleTurnCompleted {
                    outcome: Err(failure),
                    ..
                } if failure.evidence().code.as_deref() == Some("handoff.schema")
            ))
            .count(),
        3,
        "each bounded retry has one atomic rejected outcome"
    );
    let replayed = fold(events).expect("full replay");
    assert_eq!(replayed, settled.state);
    let (persisted_snapshot_head, reducer) = store
        .snapshot_meta(&mission_id)
        .await
        .unwrap()
        .expect("approved-state snapshot");
    assert!(
        persisted_snapshot_head >= snapshot_head,
        "bounded retries may advance the persisted snapshot"
    );
    assert_eq!(reducer, REDUCER_VERSION);
    assert!(
        persisted_snapshot_head <= replayed.head,
        "snapshot cannot advance beyond the fully replayed head"
    );
    let from_snapshot_tail = store
        .load_state_snapshotted(&mission_id)
        .await
        .expect("snapshot-tail load")
        .expect("mission state");
    assert_eq!(from_snapshot_tail, replayed);
    let reopened = MissionStore::open(dir.path())
        .await
        .expect("reopen mission store")
        .require_state(&mission_id)
        .await
        .expect("reload recovered mission");
    assert_eq!(reopened, replayed);
    let reloaded_failure = reopened.task_last_failure(&task_id).unwrap();
    assert!(matches!(
        reloaded_failure,
        TypedFailure::InvalidOutput { .. }
    ));
    assert_eq!(
        reloaded_failure.evidence().code.as_deref(),
        Some("handoff.schema")
    );
}

#[tokio::test]
async fn unacknowledged_handoff_preserves_the_active_effect_and_resources() {
    let dir = test_repository();
    let cleaner_calls = Arc::new(AtomicUsize::new(0));
    let runner_calls = Arc::new(AtomicUsize::new(0));
    let store = MissionStore::open(dir.path()).await.unwrap();
    let engine = Engine::new(
        store.clone(),
        test_mission_type(),
        "test-image".to_string(),
        EngineServices::new(
            Arc::new(UnacknowledgedRunner {
                calls: runner_calls.clone(),
            }),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(CountingCleaner {
                calls: cleaner_calls.clone(),
            }),
            Arc::new(MockClock::default()),
        ),
    );
    let mission_id = create_approved_mission(&engine, dir.path()).await;

    let settled = engine.advance(&mission_id).await.expect("atomic outcome");
    assert_eq!(settled.disposition, MissionDisposition::Terminal);
    assert_eq!(runner_calls.load(Ordering::SeqCst), 1);
    assert!(settled.state.inflight.is_empty());
    let (effect_id, receipt) = settled
        .state
        .role_attempt_receipts
        .iter()
        .find(|(_, receipt)| receipt.handoff.is_some())
        .expect("settled role receipt");
    assert!(matches!(
        receipt.disposition,
        RoleAttemptDisposition::Succeeded { .. }
    ));
    let exact_effect_dir = store
        .lionclaw_dir()
        .join("missions")
        .join(mission_id.as_str())
        .join("effects")
        .join(effect_id.as_str());
    assert!(!exact_effect_dir.exists());
    let events = store.load(&mission_id).await.unwrap();
    let completed_effects = events
        .iter()
        .filter(|event| {
            matches!(
                &event.event,
                MissionEvent::RoleTurnCompleted { .. } | MissionEvent::OracleRunCompleted { .. }
            )
        })
        .count();
    assert!(completed_effects > 0);
    assert_eq!(
        cleaner_calls.load(Ordering::SeqCst),
        completed_effects * 2,
        "each atomic effect outcome is quiesced and cleaned exactly once"
    );
    let replayed = fold(events).expect("full recovery replay");
    assert_eq!(replayed, settled.state);
}

#[tokio::test]
async fn cleanup_failure_is_truthful_and_retried_without_replaying_the_effect() {
    let dir = test_repository();
    let runner = Arc::new(review_runner(vec![(true, vec![])]));
    let cleaner = Arc::new(FailOnceCleaner::default());
    let engine = Engine::new(
        MissionStore::open(dir.path()).await.unwrap(),
        test_mission_type(),
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

    let mut parked = engine.advance(&mission_id).await.unwrap();
    for _ in 0..20 {
        if parked.state.cleanup_failure.is_none() {
            break;
        }
        parked = engine.advance(&mission_id).await.unwrap();
    }
    assert_eq!(parked.disposition, MissionDisposition::Parked);
    assert!(parked.state.cleanup_failure.is_none());
    assert!(parked.state.inflight.is_empty());
    assert_eq!(runner.calls.lock().unwrap().len(), 1);
    let task_id = TaskId::new("fix").unwrap();
    let failure = parked
        .state
        .task_last_failure(&task_id)
        .expect("interrupted cleanup failure");
    assert_eq!(failure.category(), "interrupted");
    let configuration = &failure.evidence().configuration;
    assert_eq!(
        configuration,
        &RuntimeConfigurationEvidence::default(),
        "crash recovery must not invent runtime evidence absent an atomic outcome"
    );
    let attention = parked.state.open_attention.values().next().unwrap();
    assert_eq!(attention.task_id.as_ref(), Some(&task_id));
    assert_eq!(attention.report, "Task 'fix' is parked.");

    let calls = cleaner.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].effect_id, calls[1].effect_id);
    assert!(!calls[0].discard_artifact);
    assert!(calls[1].discard_artifact);
}

#[tokio::test]
async fn terminal_inflight_cleanup_is_recoverable_through_the_production_cli() {
    let dir = test_repository();
    let started = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let calls = Arc::new(AtomicUsize::new(0));
    let cleaner = Arc::new(FailOnceCleaner::default());
    let mission_type_source = software_dev_mission_type_dir();
    let mission_type = load_mission_type(&mission_type_source, &AuthorityCeiling::default())
        .expect("load software-dev mission type");
    let store = MissionStore::open(dir.path()).await.unwrap();
    let engine = Arc::new(Engine::new(
        store.clone(),
        mission_type,
        "test-image".to_string(),
        EngineServices::new(
            Arc::new(BlockingRunner {
                started: started.clone(),
                release,
                calls: calls.clone(),
            }),
            Arc::new(MockOracleRunner::exiting(0)),
            cleaner,
            Arc::new(MockClock::default()),
        ),
    ));
    let mission_id = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "exercise production cleanup recovery",
            BASE_SHA,
        )
        .await
        .unwrap();
    engine
        .propose_plan(&mission_id, review_proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&engine, &mission_id).await;
    let mission_root = dir
        .path()
        .join(".lionclaw/missions")
        .join(mission_id.as_str());
    std::fs::create_dir_all(&mission_root).expect("mission resource root");
    materialize_mission_type(
        &mission_type_source,
        &mission_root.join("mission-type"),
        &AuthorityCeiling::default(),
    )
    .expect("materialize mission type snapshot");

    let driver = tokio::spawn({
        let engine = engine.clone();
        let mission_id = mission_id.clone();
        async move { engine.advance(&mission_id).await }
    });
    started.wait().await;
    let active = store.require_state(&mission_id).await.unwrap();
    let request = active
        .inflight
        .values()
        .next()
        .and_then(|effect| effect.role_turn_provenance())
        .expect("active role request");
    let conversation_root = mission_root
        .join("conversations")
        .join(request.role_instance.as_str());
    std::fs::create_dir_all(conversation_root.join("scratch")).expect("scratch");
    std::fs::create_dir_all(conversation_root.join("work")).expect("work");
    std::fs::create_dir_all(conversation_root.join("runtime")).expect("runtime");
    std::fs::write(conversation_root.join("scratch/build-output"), "discard\n").unwrap();
    std::fs::write(conversation_root.join("work/retained"), "preserve\n").unwrap();
    std::fs::write(
        conversation_root.join("runtime/native-session"),
        "preserve\n",
    )
    .unwrap();
    std::fs::write(conversation_root.join("observer.index"), "preserve\n").unwrap();

    let abort_store = store.clone();
    let abort_mission = mission_id.clone();
    let abort = tokio::spawn(async move {
        lionclaw::engine::record_abort(
            &abort_store,
            10,
            &abort_mission,
            "driver disappeared while draining abort",
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if store
                .require_state(&mission_id)
                .await
                .unwrap()
                .phase
                .is_terminal()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("abort becomes durable");
    driver.abort();
    assert!(driver.await.unwrap_err().is_cancelled());
    abort.await.unwrap().unwrap();

    let inherited = store.require_state(&mission_id).await.unwrap();
    assert!(matches!(inherited.phase, MissionPhase::Aborted { .. }));
    assert_eq!(inherited.inflight.len(), 1);
    assert_eq!(
        inherited.conversations[&request.role_instance].lifecycle,
        ConversationLifecycle::Retired
    );
    let inbox = std::process::Command::new(env!("CARGO_BIN_EXE_lionclaw"))
        .args(["mission", "inbox", "--repo"])
        .arg(dir.path())
        .output()
        .expect("render inherited cleanup through the production inbox");
    assert!(
        inbox.status.success(),
        "inbox failed: {}",
        String::from_utf8_lossy(&inbox.stderr)
    );
    let inbox = String::from_utf8(inbox.stdout).expect("UTF-8 inbox");
    assert!(inbox.contains("aborted with 1 inherited effect(s) awaiting cleanup"));
    assert!(inbox.contains("no live driver; recovery required"));
    assert!(inbox.contains("next: mission advance | mission log"));

    let blocked = engine.advance(&mission_id).await.unwrap();
    assert_eq!(blocked.disposition, MissionDisposition::CleanupBlocked);
    assert_eq!(blocked.next_actions(), ["mission advance", "mission log"]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(conversation_root.join("scratch/build-output").is_file());

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_lionclaw"))
        .args([
            "mission",
            "advance",
            mission_id.as_str(),
            "--wait",
            "--repo",
        ])
        .arg(dir.path())
        .output()
        .expect("run production CLI recovery");
    assert!(
        output.status.success(),
        "CLI recovery failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let recovered = store.require_state(&mission_id).await.unwrap();
    assert!(matches!(recovered.phase, MissionPhase::Aborted { .. }));
    assert!(recovered.inflight.is_empty());
    assert_eq!(
        recovered.conversations[&request.role_instance].lifecycle,
        ConversationLifecycle::Retired
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "role must not rerun");
    assert!(!conversation_root.join("scratch").exists());
    assert!(conversation_root.join("work/retained").is_file());
    assert!(conversation_root.join("runtime/native-session").is_file());
    assert!(conversation_root.join("observer.index").is_file());
}

#[tokio::test]
async fn persistent_cleanup_failure_never_settles_or_replays_the_effect() {
    let dir = test_repository();
    let runner = Arc::new(review_runner(vec![(true, vec![])]));
    let cleaner = Arc::new(AlwaysFailCleaner::default());
    let engine = Engine::new(
        MissionStore::open(dir.path()).await.unwrap(),
        test_mission_type(),
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
