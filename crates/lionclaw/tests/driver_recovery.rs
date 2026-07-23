//! Mission-driver exclusion and cleanup recovery at the engine boundary.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier as StdBarrier, Mutex};

use async_trait::async_trait;
use common::{
    advisory_plan, approve_plan, fault_append_events, initialize_repository, proposal, simple_plan,
    test_mission_type, BASE_SHA, HEAD_SHA,
};
use lionclaw::authority::AuthorityCeiling;
use lionclaw::engine::{Engine, EngineServices, MissionDisposition};
use lionclaw::mission_type::{load_mission_type, materialize_mission_type};
use lionclaw::model::{
    fold, ConversationLifecycle, EffectId, EffectResource, EventEnvelope, Handoff, MissionEvent,
    MissionId, MissionPhase, MissionState, OutputSemantics, PayloadRef, RoleAttemptDisposition,
    RoleEffectSource, RoleHandoffObservation, RoleTurnObservation, RuntimeConfigurationEvidence,
    TaskId, TaskNamespace, ValidationItem, REDUCER_VERSION,
};
use lionclaw::ports::{
    EffectCleaner, EffectCleanupFailure, EffectCleanupRequest, EventSink, RoleRunOutcome,
    RoleRunRequest, RoleRunner,
};
use lionclaw::store::{MissionStore, NewEvent};
use lionclaw::testing::{MockClock, MockOracleRunner, MockRoleRunner, NoopEffectCleaner};
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

struct UnacknowledgedRunner {
    calls: Arc<AtomicUsize>,
}

struct MissingValidatorHandoffRunner {
    validator_started: Arc<Barrier>,
    calls: AtomicUsize,
    validator_attempts: AtomicUsize,
    validator_assignment: Mutex<Option<(TaskId, u32)>>,
}

struct CountingCleaner {
    calls: Arc<AtomicUsize>,
}

struct DurableFoldCleaner {
    store: MissionStore,
    expected: ExpectedHandoff,
    deletions: AtomicUsize,
    after_first_deletion: Option<CleanupPause>,
}

enum ExpectedHandoff {
    Accepted(PayloadRef),
    Rejected,
    Absent,
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

fn effect_dir(request: &EffectCleanupRequest) -> std::path::PathBuf {
    request
        .state_dir
        .join("missions")
        .join(request.mission_id.as_str())
        .join("effects")
        .join(request.effect_id.as_str())
}

fn role_effect_dir(request: &RoleRunRequest) -> std::path::PathBuf {
    request
        .state_dir
        .join("missions")
        .join(request.mission_id.as_str())
        .join("effects")
        .join(request.effect_id.as_str())
}

#[async_trait]
impl RoleRunner for RetainedHandoffRunner {
    async fn run(&self, request: RoleRunRequest) -> Result<RoleRunOutcome, TypedFailure> {
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
        request
            .confirm_turn_observed(RoleTurnObservation::Completed {
                final_response: PayloadRef::inline(RETAINED_RESPONSE),
                runtime_configuration: RuntimeConfigurationEvidence {
                    requested_model: Some(RETAINED_MODEL.to_string()),
                    applied_model: Some(RETAINED_MODEL.to_string()),
                    ..Default::default()
                },
            })
            .await?;
        self.started.wait().await;
        std::future::pending().await
    }
}

#[async_trait]
impl RoleRunner for MissingValidatorHandoffRunner {
    async fn run(&self, request: RoleRunRequest) -> Result<RoleRunOutcome, TypedFailure> {
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
                request
                    .confirm_turn_observed(RoleTurnObservation::Completed {
                        final_response: PayloadRef::inline("writer completed"),
                        runtime_configuration: runtime_configuration.clone(),
                    })
                    .await?;
                request
                    .confirm_handoff_observed(RoleHandoffObservation::Accepted {
                        report: handoff.report().clone(),
                    })
                    .await?;
                let artifact =
                    lionclaw::testing::capture_prepared_test_artifact(&request, HEAD_SHA).await?;
                Ok(RoleRunOutcome {
                    handoff: Some(handoff),
                    artifact: Some(artifact),
                    runtime_configuration,
                    final_response: "writer completed".into(),
                })
            }
            OutputSemantics::EmitsVerdict => {
                let attempt = self.validator_attempts.fetch_add(1, Ordering::SeqCst);
                {
                    let mut assignment = self.validator_assignment.lock().unwrap();
                    match &*assignment {
                        Some((task_id, generation)) => {
                            assert_eq!(&request.task_id, task_id);
                            assert_eq!(
                                request.assignment_epoch, *generation,
                                "invalid-output rework must retain the conversation generation"
                            );
                        }
                        None => {
                            *assignment = Some((request.task_id.clone(), request.assignment_epoch));
                        }
                    }
                }

                let runtime_configuration = RuntimeConfigurationEvidence {
                    requested_model: Some(RETAINED_MODEL.into()),
                    applied_model: Some(RETAINED_MODEL.into()),
                    ..Default::default()
                };
                request
                    .confirm_turn_observed(RoleTurnObservation::Completed {
                        final_response: PayloadRef::inline(RETAINED_RESPONSE),
                        runtime_configuration: runtime_configuration.clone(),
                    })
                    .await?;
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
                request
                    .confirm_handoff_observed(RoleHandoffObservation::Accepted {
                        report: handoff.report().clone(),
                    })
                    .await?;
                Ok(RoleRunOutcome {
                    handoff: Some(handoff),
                    artifact: None,
                    runtime_configuration,
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
    assert_eq!(
        receipt.turn,
        Some(RoleTurnObservation::Completed {
            final_response: PayloadRef::inline(RETAINED_RESPONSE),
            runtime_configuration: RuntimeConfigurationEvidence {
                requested_model: Some(RETAINED_MODEL.to_string()),
                applied_model: Some(RETAINED_MODEL.to_string()),
                ..Default::default()
            },
        })
    );
    assert!(
        receipt.handoff.is_none(),
        "the crash boundary must precede handoff acknowledgement"
    );

    let events = store.load(mission_id).await.expect("durable events");
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                &event.event,
                MissionEvent::RoleTurnObserved {
                    effect_id: observed_effect,
                    ..
                } if observed_effect == effect_id
            ))
            .count(),
        1,
        "the completed turn is acknowledged exactly once"
    );
    assert!(
        events.iter().all(|event| !matches!(
            &event.event,
            MissionEvent::RoleHandoffObserved {
                effect_id: observed_effect,
                ..
            } | MissionEvent::RoleRunCompleted {
                effect_id: observed_effect,
                ..
            } if observed_effect == effect_id
        )),
        "neither a handoff observation nor final outcome may precede the crash"
    );
}

fn assert_operator_receipt(
    repo: &std::path::Path,
    mission_id: &MissionId,
    effect_id: &EffectId,
    handoff_outcome: &str,
    handoff_evidence: &str,
) {
    let json = std::process::Command::new(env!("CARGO_BIN_EXE_lionclaw"))
        .args(["mission", "status", mission_id.as_str(), "--json", "--repo"])
        .arg(repo)
        .output()
        .expect("render JSON mission status");
    assert!(
        json.status.success(),
        "JSON status failed: {}",
        String::from_utf8_lossy(&json.stderr)
    );
    let json: serde_json::Value =
        serde_json::from_slice(&json.stdout).expect("JSON mission status");
    let receipt = json["role_attempt_receipts"]
        .as_array()
        .expect("role attempt receipts")
        .iter()
        .find(|receipt| receipt["effect_id"] == effect_id.as_str())
        .expect("settled receipt in JSON status");
    assert_eq!(receipt["turn"]["outcome"], "completed");
    assert_eq!(
        receipt["turn"]["final_response"]["content"],
        RETAINED_RESPONSE
    );
    assert_eq!(
        receipt["effective_runtime_configuration"]["requested_model"],
        RETAINED_MODEL
    );
    assert_eq!(
        receipt["effective_runtime_configuration"]["applied_model"],
        RETAINED_MODEL
    );
    assert_eq!(
        receipt
            .to_string()
            .matches("\"effective_runtime_configuration\"")
            .count(),
        1
    );
    assert!(receipt["turn"]
        .as_object()
        .is_some_and(|turn| !turn.contains_key("runtime_configuration")));
    if let Some(evidence) = receipt["disposition"]["failure"]["evidence"].as_object() {
        assert!(!evidence.contains_key("configuration"));
    }
    assert_eq!(receipt["handoff"]["outcome"], handoff_outcome);
    assert!(
        receipt["handoff"].to_string().contains(handoff_evidence),
        "JSON handoff evidence must retain {handoff_evidence}: {}",
        receipt["handoff"]
    );

    let human = std::process::Command::new(env!("CARGO_BIN_EXE_lionclaw"))
        .args(["mission", "status", mission_id.as_str(), "--repo"])
        .arg(repo)
        .output()
        .expect("render human mission status");
    assert!(
        human.status.success(),
        "human status failed: {}",
        String::from_utf8_lossy(&human.stderr)
    );
    let human = String::from_utf8(human.stdout).expect("UTF-8 human status");
    assert!(human.contains(&format!("effect: {effect_id}")));
    assert!(human.contains("turn: completed"));
    assert!(human.contains(RETAINED_RESPONSE));
    assert!(human.contains(&format!("handoff: {handoff_outcome}")));
    assert!(human.contains(handoff_evidence));
}

#[async_trait]
impl RoleRunner for UnacknowledgedRunner {
    async fn run(&self, request: RoleRunRequest) -> Result<RoleRunOutcome, TypedFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let effect_dir = role_effect_dir(&request);
        std::fs::create_dir_all(effect_dir.join("handoff")).unwrap();
        std::fs::write(effect_dir.join("runner-owned-marker"), b"retain").unwrap();
        std::fs::write(
            effect_dir.join("handoff/handoff.json"),
            r#"{"schema":"lionclaw.mission.work-handoff.v2","type":"work","done":true,"report":"untrusted pre-turn handoff","request_attention":false}"#,
        )
        .unwrap();
        Ok(RoleRunOutcome {
            handoff: Some(Handoff::Work {
                done: true,
                report: PayloadRef::inline("unacknowledged"),
                request_attention: false,
            }),
            artifact: None,
            runtime_configuration: RuntimeConfigurationEvidence::default(),
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
        match &self.expected {
            ExpectedHandoff::Accepted(report) => {
                assert_eq!(
                    receipt.accepted_report(),
                    Some(report),
                    "RoleHandoffObserved must be durable before destructive cleanup"
                );
            }
            ExpectedHandoff::Rejected => {
                let failure = receipt
                    .rejection()
                    .expect("malformed retained handoff must be durably rejected");
                assert!(failure.is_invalid_output());
                assert_eq!(failure.evidence().code.as_deref(), Some("handoff.schema"));
            }
            ExpectedHandoff::Absent => {
                assert!(
                    receipt.handoff.is_none(),
                    "an absent handoff must remain absent through quiescence"
                );
            }
        }

        let exact_effect_dir = effect_dir(&request);
        if exact_effect_dir.exists() {
            match &self.expected {
                ExpectedHandoff::Accepted(_) | ExpectedHandoff::Rejected => assert!(
                    exact_effect_dir.join("handoff/handoff.json").is_file(),
                    "retained handoff must still exist while durable state is checked"
                ),
                ExpectedHandoff::Absent => assert!(
                    !exact_effect_dir.join("handoff/handoff.json").exists(),
                    "the optional dialogue checkpoint must not fabricate a handoff"
                ),
            }
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
            assert_eq!(
                self.deletions.fetch_add(1, Ordering::SeqCst),
                0,
                "recovery must delete the exact effect directory once"
            );
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
            MissionEvent::RoleRunCompleted {
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
    async fn run(&self, request: RoleRunRequest) -> Result<RoleRunOutcome, TypedFailure> {
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
        request
            .confirm_turn_observed(RoleTurnObservation::Completed {
                final_response: PayloadRef::inline(""),
                runtime_configuration: runtime_configuration.clone(),
            })
            .await?;
        request
            .confirm_handoff_observed(RoleHandoffObservation::Accepted {
                report: handoff.report().clone(),
            })
            .await?;
        let artifact =
            lionclaw::testing::capture_prepared_test_artifact(&request, HEAD_SHA).await?;
        Ok(RoleRunOutcome {
            handoff: Some(handoff),
            artifact: Some(artifact),
            runtime_configuration,
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
        expected: ExpectedHandoff::Accepted(PayloadRef::inline(RETAINED_REPORT)),
        deletions: AtomicUsize::new(0),
        after_first_deletion: None,
    });
    let engine = Arc::new(Engine::new(
        store.clone(),
        test_mission_type(),
        "codex".to_string(),
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
    let expected_report = PayloadRef::inline(RETAINED_REPORT);
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
    let turn_position = events
        .iter()
        .position(|event| {
            matches!(
                &event.event,
                MissionEvent::RoleTurnObserved {
                    effect_id: observed_effect,
                    observation: RoleTurnObservation::Completed { .. },
                } if observed_effect == &effect_id
            )
        })
        .expect("durable RoleTurnObserved");
    let observed_position = events
        .iter()
        .position(|event| {
            matches!(
                &event.event,
                MissionEvent::RoleHandoffObserved {
                    effect_id: observed_effect,
                    observation: RoleHandoffObservation::Accepted { report },
                } if observed_effect == &effect_id
                    && report == &PayloadRef::inline(RETAINED_REPORT)
            )
        })
        .expect("durable RoleHandoffObserved");
    let completion_position = events
        .iter()
        .position(|event| {
            matches!(
                &event.event,
                MissionEvent::RoleRunCompleted {
                    effect_id: completed_effect,
                    outcome: Err(failure),
                    ..
                } if completed_effect == &effect_id && failure.category() == "interrupted"
            )
        })
        .expect("durable interrupted completion");
    assert!(
        turn_position < observed_position && observed_position < completion_position,
        "turn, report observation, and interrupted completion must remain ordered"
    );

    let replayed = fold(events).expect("full replay");
    let task_id = TaskId::new("fix").unwrap();
    let receipt = replayed
        .task_last_role_attempt(TaskNamespace::Execution, &task_id)
        .expect("settled recovery receipt");
    assert_eq!(receipt.effect_id, effect_id);
    assert_eq!(receipt.failure().unwrap().category(), "interrupted");
    assert_eq!(receipt.accepted_report(), Some(&expected_report));
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
    assert_eq!(
        from_snapshot_tail
            .task_last_role_attempt(TaskNamespace::Execution, &task_id)
            .and_then(lionclaw::model::RoleAttemptReceipt::accepted_report),
        Some(&expected_report)
    );
    assert_operator_receipt(
        dir.path(),
        &mission_id,
        &effect_id,
        "accepted",
        RETAINED_REPORT,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_writer_turn_without_handoff_recovers_as_same_conversation_checkpoint() {
    let dir = test_repository();
    let started = Arc::new(Barrier::new(2));
    let calls = Arc::new(AtomicUsize::new(0));
    let store = MissionStore::open(dir.path()).await.unwrap();
    let cleaner = Arc::new(DurableFoldCleaner {
        store: store.clone(),
        expected: ExpectedHandoff::Absent,
        deletions: AtomicUsize::new(0),
        after_first_deletion: None,
    });
    let engine = Arc::new(Engine::new(
        store.clone(),
        test_mission_type(),
        "codex".to_string(),
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
    let conversation_id = effect
        .role_request_identity()
        .expect("role request identity")
        .conversation_id;
    assert_durable_turn_precedes_handoff(&store, &mission_id, &effect_id, &active).await;
    driver.abort();
    assert!(driver.await.unwrap_err().is_cancelled());

    let recovered = engine
        .advance(&mission_id)
        .await
        .expect("recover optional writer checkpoint");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "writer must not rerun");
    assert_eq!(cleaner.deletions.load(Ordering::SeqCst), 1);
    assert_eq!(recovered.disposition, MissionDisposition::AwaitingLead);
    assert!(recovered.next_actions().contains(&"mission send"));

    let state = store.require_state(&mission_id).await.unwrap();
    assert!(state.inflight.is_empty());
    let receipt = state
        .role_attempt_receipts
        .get(&effect_id)
        .expect("settled writer receipt");
    assert!(matches!(
        receipt.disposition,
        RoleAttemptDisposition::Succeeded {
            handoff: None,
            artifact: None,
        }
    ));
    assert!(receipt.handoff.is_none());
    assert_eq!(
        receipt.turn,
        Some(RoleTurnObservation::Completed {
            final_response: PayloadRef::inline(RETAINED_RESPONSE),
            runtime_configuration: RuntimeConfigurationEvidence {
                requested_model: Some(RETAINED_MODEL.into()),
                applied_model: Some(RETAINED_MODEL.into()),
                ..Default::default()
            },
        })
    );
    let conversation = state
        .conversations
        .get(&conversation_id)
        .expect("same conversation retained");
    assert_eq!(conversation.lifecycle, ConversationLifecycle::AwaitingLead);
    assert_eq!(
        conversation.final_response,
        Some(PayloadRef::inline(RETAINED_RESPONSE))
    );

    let events = store.load(&mission_id).await.expect("durable events");
    let completion = events
        .iter()
        .find_map(|event| match &event.event {
            MissionEvent::RoleRunCompleted {
                effect_id: completed_effect,
                outcome: Ok(success),
                ..
            } if completed_effect == &effect_id => Some(success),
            _ => None,
        })
        .expect("recovered successful checkpoint");
    assert!(completion.handoff.is_none());
    assert!(completion.artifact.is_none());
    assert_eq!(
        completion.final_response,
        PayloadRef::inline(RETAINED_RESPONSE)
    );
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
    let engine = Arc::new(Engine::new(
        store.clone(),
        test_mission_type(),
        "codex".to_string(),
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
    let mut plan = advisory_plan();
    plan.assertions[0].oracle = Some(lionclaw::model::OracleName::new("cargo-test").unwrap());
    engine
        .propose_plan(&mission_id, proposal(0, plan))
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
                .role_request_identity()
                .is_some_and(|request| request.output == OutputSemantics::EmitsVerdict)
        })
        .expect("active validator effect");
    let effect_id = effect_id.clone();
    let first_request = effect.role_request_identity().expect("validator identity");
    let conversation_id = first_request.conversation_id.clone();
    assert_durable_turn_precedes_handoff(&store, &mission_id, &effect_id, &active).await;
    driver.abort();
    assert!(driver.await.unwrap_err().is_cancelled());

    engine
        .advance(&mission_id)
        .await
        .expect("recover and rework mandatory validator output");
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
    assert!(failure.is_invalid_output());
    assert_eq!(failure.evidence().code.as_deref(), Some("handoff.missing"));
    assert_eq!(failure.evidence().final_response, RETAINED_RESPONSE);
    assert_eq!(
        failure.evidence().configuration.applied_model.as_deref(),
        Some(RETAINED_MODEL)
    );
    assert!(failed.handoff.is_none());
    assert!(matches!(
        failed.disposition,
        RoleAttemptDisposition::Failed { .. }
    ));
    let RoleEffectSource::Task {
        request: failed_request,
        ..
    } = &failed.source
    else {
        panic!("validator receipt must have task provenance");
    };
    assert_eq!(failed_request.conversation_id, conversation_id);

    let current = state
        .task_last_role_attempt(TaskNamespace::Execution, &TaskId::new("review").unwrap())
        .expect("reworked validator receipt");
    let RoleEffectSource::Task {
        request: current_request,
        ..
    } = &current.source
    else {
        panic!("validator receipt must have task provenance");
    };
    assert_eq!(
        current_request.conversation_id, conversation_id,
        "bounded invalid-output rework must resume the same conversation"
    );
    assert!(matches!(
        current.disposition,
        RoleAttemptDisposition::Succeeded { .. }
    ));
    let conversation = state
        .conversations
        .get(&conversation_id)
        .expect("validator conversation");
    assert_eq!(conversation.invalid_handoff_reworks, 1);

    let events = store.load(&mission_id).await.expect("durable events");
    assert!(events.iter().any(|event| matches!(
        &event.event,
        MissionEvent::RoleRunCompleted {
            effect_id: completed_effect,
            outcome: Err(failure),
            ..
        } if completed_effect == &effect_id
            && failure.is_invalid_output()
            && failure.evidence().code.as_deref() == Some("handoff.missing")
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
    let started = Arc::new(Barrier::new(2));
    let calls = Arc::new(AtomicUsize::new(0));
    let completion_entered = Arc::new(StdBarrier::new(2));
    let completion_release = Arc::new(StdBarrier::new(2));
    let cleanup_entered = Arc::new(Notify::new());
    let cleanup_release = Arc::new(Notify::new());
    let store = MissionStore::open(dir.path())
        .await
        .unwrap()
        .with_sink(Arc::new(RecoveryCompletionGate {
            category: "invalid_output",
            entered: completion_entered.clone(),
            release: completion_release.clone(),
        }));
    let cleaner = Arc::new(DurableFoldCleaner {
        store: store.clone(),
        expected: ExpectedHandoff::Rejected,
        deletions: AtomicUsize::new(0),
        after_first_deletion: Some(CleanupPause {
            entered: cleanup_entered.clone(),
            release: cleanup_release,
        }),
    });
    let engine = Arc::new(Engine::new(
        store.clone(),
        test_mission_type(),
        "codex".to_string(),
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
    ));
    let mission_id = create_approved_mission(&engine, dir.path()).await;
    let snapshot = store
        .rebuild_cursors(&mission_id, 10)
        .await
        .expect("seed an approved-state snapshot");
    let snapshot_head = snapshot.head;

    let driver = tokio::spawn({
        let engine = engine.clone();
        let mission_id = mission_id.clone();
        async move { engine.advance(&mission_id).await }
    });
    started.wait().await;
    let active = store.require_state(&mission_id).await.unwrap();
    let effect_id = active
        .inflight
        .keys()
        .next()
        .expect("active effect")
        .clone();
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
    cleanup_entered.notified().await;
    let interrupted = store.require_state(&mission_id).await.unwrap();
    assert!(
        interrupted.inflight.contains_key(&effect_id),
        "the effect remains active until its durable outcome is appended"
    );
    let interrupted_receipt = interrupted
        .role_attempt_receipts
        .get(&effect_id)
        .expect("durable active receipt");
    assert_eq!(
        interrupted_receipt.disposition,
        RoleAttemptDisposition::Active
    );
    let rejection = interrupted_receipt
        .rejection()
        .expect("durable rejected observation");
    assert_eq!(rejection.evidence().code.as_deref(), Some("handoff.schema"));
    assert!(!exact_effect_dir.exists());
    assert!(
        store
            .load(&mission_id)
            .await
            .unwrap()
            .iter()
            .all(|event| !matches!(
                &event.event,
                MissionEvent::RoleRunCompleted {
                    effect_id: completed_effect,
                    ..
                } if completed_effect == &effect_id
            )),
        "the first recovery must stop before outcome append"
    );
    recovery.abort();
    assert!(recovery.await.unwrap_err().is_cancelled());

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
    let live = store.require_state(&mission_id).await.unwrap();
    assert!(live.inflight.is_empty());
    let receipt = live
        .role_attempt_receipts
        .get(&effect_id)
        .expect("rejected handoff receipt survives settlement");
    assert!(matches!(
        &receipt.disposition,
        RoleAttemptDisposition::Failed { .. }
    ));
    assert!(receipt.rejection().is_some());
    let task_id = TaskId::new("fix").unwrap();
    let failure = live
        .task_last_failure(TaskNamespace::Execution, &task_id)
        .expect("typed recovery failure");
    assert!(matches!(failure, TypedFailure::InvalidOutput { .. }));
    assert_eq!(failure.category(), "invalid_output");
    assert_eq!(failure.evidence().code.as_deref(), Some("handoff.schema"));
    assert!(
        failure.evidence().detail.contains("does not match"),
        "schema mismatch detail must survive recovery"
    );

    let events = store.load(&mission_id).await.expect("durable events");
    let turn_position = events
        .iter()
        .position(|event| {
            matches!(
                &event.event,
                MissionEvent::RoleTurnObserved {
                    effect_id: observed_effect,
                    observation: RoleTurnObservation::Completed { .. },
                } if observed_effect == &effect_id
            )
        })
        .expect("durable RoleTurnObserved");
    let (rejected_position, rejected) = events
        .iter()
        .enumerate()
        .find_map(|(position, event)| match &event.event {
            MissionEvent::RoleHandoffObserved {
                effect_id: observed_effect,
                observation: RoleHandoffObservation::Rejected { failure },
            } if observed_effect == &effect_id => Some((position, failure)),
            _ => None,
        })
        .expect("malformed retained handoff must be durably rejected");
    assert!(
        turn_position < rejected_position,
        "completed turn must precede the rejected handoff observation"
    );
    assert_eq!(rejected.evidence().code.as_deref(), Some("handoff.schema"));
    let replayed = fold(events).expect("full replay");
    assert_eq!(replayed, live);
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
    let reopened = MissionStore::open(dir.path())
        .await
        .expect("reopen mission store")
        .require_state(&mission_id)
        .await
        .expect("reload recovered mission");
    assert_eq!(reopened, replayed);
    let reloaded_failure = reopened
        .task_last_failure(TaskNamespace::Execution, &task_id)
        .unwrap();
    assert!(matches!(
        reloaded_failure,
        TypedFailure::InvalidOutput { .. }
    ));
    assert_eq!(
        reloaded_failure.evidence().code.as_deref(),
        Some("handoff.schema")
    );
    assert_operator_receipt(
        dir.path(),
        &mission_id,
        &effect_id,
        "rejected",
        "handoff.schema",
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
        "codex".to_string(),
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

    let error = engine
        .advance(&mission_id)
        .await
        .expect_err("runner protocol violation must fail the driver");
    assert!(
        format!("{error:#}").contains("without a durable completed-turn observation"),
        "unexpected protocol error: {error:#}"
    );
    assert_eq!(
        cleaner_calls.load(Ordering::SeqCst),
        0,
        "the engine must not clean resources after an unacknowledged handoff"
    );

    let state = store.require_state(&mission_id).await.unwrap();
    let (effect_id, _) = state.inflight.iter().next().expect("effect remains active");
    let receipt = state
        .role_attempt_receipts
        .get(effect_id)
        .expect("unacknowledged effect retains its active receipt");
    assert_eq!(receipt.disposition, RoleAttemptDisposition::Active);
    assert!(receipt.turn.is_none());
    assert!(receipt.handoff.is_none());
    assert_eq!(
        state.tasks[&lionclaw::model::TaskId::new("fix").unwrap()].status,
        lionclaw::model::TaskStatus::Running
    );
    let exact_effect_dir = store
        .lionclaw_dir()
        .join("missions")
        .join(mission_id.as_str())
        .join("effects")
        .join(effect_id.as_str());
    assert!(exact_effect_dir.join("runner-owned-marker").is_file());
    assert!(exact_effect_dir.join("handoff/handoff.json").is_file());
    assert!(store
        .load(&mission_id)
        .await
        .unwrap()
        .iter()
        .all(|event| !matches!(
            &event.event,
            MissionEvent::RoleTurnObserved { .. }
                | MissionEvent::RoleHandoffObserved { .. }
                | MissionEvent::RoleRunCompleted { .. }
        )));

    let recovered = engine
        .advance(&mission_id)
        .await
        .expect("the next driver must clean and settle the unobserved attempt");
    assert_eq!(recovered.disposition, MissionDisposition::Parked);
    assert_eq!(
        runner_calls.load(Ordering::SeqCst),
        1,
        "effect must not rerun"
    );
    assert_eq!(
        cleaner_calls.load(Ordering::SeqCst),
        2,
        "recovery quiesces and cleans the exact effect once"
    );
    assert!(!exact_effect_dir.exists());
    let recovered_receipt = recovered
        .state
        .role_attempt_receipts
        .get(effect_id)
        .expect("interrupted receipt");
    assert!(recovered_receipt.turn.is_none());
    assert!(recovered_receipt.handoff.is_none());
    assert_eq!(
        recovered_receipt.failure().map(TypedFailure::category),
        Some("interrupted")
    );
    let events = store.load(&mission_id).await.unwrap();
    assert!(events
        .iter()
        .all(|event| !matches!(&event.event, MissionEvent::RoleHandoffObserved { .. })));
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(&event.event, MissionEvent::RoleRunCompleted { .. }))
            .count(),
        1
    );
    let replayed = fold(events).expect("full recovery replay");
    assert_eq!(replayed, recovered.state);
    assert_eq!(
        store.require_state(&mission_id).await.unwrap(),
        recovered.state,
        "store reload and full fold must agree after recovery"
    );

    let stable = engine
        .advance(&mission_id)
        .await
        .expect("a settled recovery must not enter the same loop");
    assert_eq!(stable.disposition, MissionDisposition::Parked);
    assert_eq!(runner_calls.load(Ordering::SeqCst), 1);
    assert_eq!(cleaner_calls.load(Ordering::SeqCst), 2);
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
    let task_id = TaskId::new("fix").unwrap();
    let failure = parked
        .state
        .task_last_failure(TaskNamespace::Execution, &task_id)
        .expect("interrupted cleanup failure");
    assert_eq!(failure.category(), "interrupted");
    let configuration = &failure.evidence().configuration;
    assert_eq!(
        configuration.requested_model.as_deref(),
        Some("mock-model"),
        "the durably observed turn remains authoritative"
    );
    assert_eq!(
        configuration.applied_model.as_deref(),
        Some("mock-model"),
        "a later conflicting runtime fact cannot rewrite turn evidence"
    );
    assert_eq!(configuration.requested_mode, None);
    assert_eq!(configuration.applied_mode, None);
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
        "codex".to_string(),
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
    let mission_id = create_approved_mission(&engine, dir.path()).await;
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
        .and_then(|effect| effect.role_request_identity())
        .expect("active role request");
    let conversation_root = mission_root
        .join("conversations")
        .join(request.conversation_id.as_str());
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
        inherited.conversations[&request.conversation_id].lifecycle,
        ConversationLifecycle::Running
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
        recovered.conversations[&request.conversation_id].lifecycle,
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
