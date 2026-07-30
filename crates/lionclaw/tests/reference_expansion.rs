//! Production ingress/request-boundary coverage for transient message references.

mod common;

use std::process::Command;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use clap::Parser;
use common::{
    approve_plan, harness, initialize_repository, proposal, simple_plan, test_mission_type,
    BASE_SHA,
};
use lionclaw::cli::{self, Cli};
use lionclaw::engine::{Engine, EngineServices, ReferenceRejectionReason};
use lionclaw::model::{
    DecisionAction, DeliveryMarker, Handoff, MessageReference, MissionEvent, PayloadRef,
    RoleInstanceId, TaskId, UnavailableReferenceCause, REDUCER_VERSION, SCHEMA_VERSION,
};
use lionclaw::ports::{
    CapturedArtifact, OracleOutcome, RoleRunner, RoleTurnOutcome, RoleTurnRequest,
};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockClock, MockOracleRunner, MockRoleRunner, NoopEffectCleaner};
use lionclaw_runtime_api::TypedFailure;

fn send_cli(
    repo: &std::path::Path,
    mission: &lionclaw::model::MissionId,
    conversation: &str,
    reference_args: &[&str],
    body: &str,
) -> Cli {
    let mut args = vec![
        "lionclaw",
        "mission",
        "send",
        "--mission-id",
        mission.as_str(),
        "--to",
        conversation,
    ];
    args.extend_from_slice(reference_args);
    args.extend(["--repo", repo.to_str().unwrap(), body]);
    Cli::try_parse_from(args).expect("production mission send parser")
}

fn checkpoint(_request: &RoleTurnRequest) -> RoleTurnOutcome {
    RoleTurnOutcome {
        handoff: None,
        artifact: None,
        prepared_inputs: Vec::new(),
        runtime_configuration: Default::default(),
        runtime_usage: Default::default(),
        final_response: "awaiting lead".into(),
    }
}

fn judgment_outcome(request: &RoleTurnRequest) -> Option<RoleTurnOutcome> {
    (request.role.output == lionclaw::model::OutputSemantics::EmitsVerdict).then(|| {
        RoleTurnOutcome {
            handoff: Some(Handoff::Validate {
                done: true,
                report: PayloadRef::inline("reference behavior reviewed"),
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
            final_response: "reference behavior reviewed".into(),
        }
    })
}

fn reference_failures(
    state: &lionclaw::model::MissionState,
) -> Vec<&lionclaw_runtime_api::TypedFailure> {
    state
        .role_attempt_receipts
        .values()
        .filter_map(lionclaw::model::RoleAttemptReceipt::failure)
        .filter(|failure| {
            failure.evidence().code.as_deref() == Some("message.reference_unavailable")
        })
        .collect()
}

struct CheckpointRunner {
    prompts: Arc<Mutex<Vec<String>>>,
}

struct CheckpointHarness {
    engine: Engine,
}

async fn checkpoint_harness(
    workspace: &std::path::Path,
    prompts: Arc<Mutex<Vec<String>>>,
) -> CheckpointHarness {
    initialize_repository(workspace);
    let engine = Engine::new(
        MissionStore::open(workspace).await.unwrap(),
        test_mission_type(),
        "localhost/lionclaw-runtime-dev:v1".into(),
        EngineServices::new(
            Arc::new(CheckpointRunner { prompts }),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );
    CheckpointHarness { engine }
}

#[async_trait]
impl RoleRunner for CheckpointRunner {
    async fn run(
        &self,
        request: RoleTurnRequest,
    ) -> Result<RoleTurnOutcome, lionclaw_runtime_api::TypedFailure> {
        if let Some(outcome) = judgment_outcome(&request) {
            return Ok(outcome);
        }
        if request.role.output == lionclaw::model::OutputSemantics::ProducesArtifact
            && request.attempt_no == 1
        {
            lionclaw::testing::prepare_test_workspace(&request).await?;
        }
        self.prompts.lock().unwrap().push(request.prompt.clone());
        Ok(checkpoint(&request))
    }
}

#[tokio::test]
async fn reachable_commit_expands_only_at_the_typed_role_request_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let h = checkpoint_harness(dir.path(), prompts.clone()).await;
    let mission = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "references", BASE_SHA)
        .await
        .unwrap();
    h.engine
        .propose_plan(&mission, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&h.engine, &mission).await;
    let awaiting = h.engine.advance(&mission).await.unwrap().state;
    let conversation = awaiting.conversations.keys().next().unwrap().to_string();
    let store = MissionStore::open(dir.path()).await.unwrap();

    cli::run(send_cli(
        dir.path(),
        &mission,
        &conversation,
        &["--commit", BASE_SHA],
        "inspect the cited change",
    ))
    .await
    .unwrap();

    let durable = serde_json::to_string(&store.load(&mission).await.unwrap()).unwrap();
    assert!(
        durable.contains(BASE_SHA),
        "the durable identity is retained"
    );
    assert!(!durable.contains("reachable commit 68416f3d"));
    assert!(!durable.contains("LionClaw test base"));
    assert!(!durable.contains("fixture.txt"));

    h.engine.advance(&mission).await.unwrap();
    let request = prompts
        .lock()
        .unwrap()
        .last()
        .expect("continued typed RoleTurnRequest")
        .clone();
    assert!(request.contains(&format!("reachable commit {BASE_SHA}:")));
    assert!(request.contains("LionClaw test base"));
    assert!(request.contains("fixture.txt"));

    // The expansion is reconstructed prose, never a competing durable event
    // or blob. The source Git object is intentionally outside this scan.
    let events = store.load(&mission).await.unwrap();
    assert!(events.iter().all(|event| match &event.event {
        MissionEvent::MessageSent { references, .. } => references
            .iter()
            .all(|reference| matches!(reference, MessageReference::ReachableCommit { .. })),
        _ => true,
    }));
    let mission_root = dir.path().join(".lionclaw/missions").join(mission.as_str());
    for entry in walkdir(&mission_root) {
        if entry.is_file() {
            let bytes = std::fs::read(&entry).unwrap();
            assert!(!String::from_utf8_lossy(&bytes).contains("LionClaw test base"));
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum CommitObjectFault {
    Missing,
    Corrupt,
}

impl CommitObjectFault {
    fn cause(self) -> UnavailableReferenceCause {
        match self {
            Self::Missing => UnavailableReferenceCause::SourceMissing,
            Self::Corrupt => UnavailableReferenceCause::InvalidContent,
        }
    }

    fn inject(self, object: &std::path::Path) {
        match self {
            Self::Missing => std::fs::remove_file(object).unwrap(),
            Self::Corrupt => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;

                    let mut permissions = std::fs::metadata(object).unwrap().permissions();
                    permissions.set_mode(0o600);
                    std::fs::set_permissions(object, permissions).unwrap();
                }
                std::fs::write(object, b"invalid zlib object").unwrap();
            }
        }
    }
}

#[tokio::test]
async fn accepted_commit_object_fault_settles_once_and_does_not_block_later_messages() {
    for fault in [CommitObjectFault::Missing, CommitObjectFault::Corrupt] {
        prove_commit_object_fault_settles_once(fault).await;
    }
}

async fn prove_commit_object_fault_settles_once(fault: CommitObjectFault) {
    assert_eq!((SCHEMA_VERSION, REDUCER_VERSION), (36, 72));
    let dir = tempfile::tempdir().unwrap();
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let h = checkpoint_harness(dir.path(), prompts.clone()).await;
    let mission = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "references", BASE_SHA)
        .await
        .unwrap();
    h.engine
        .propose_plan(&mission, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&h.engine, &mission).await;
    let awaiting = h.engine.advance(&mission).await.unwrap().state;
    let conversation = awaiting.conversations.keys().next().unwrap().to_string();
    let store = MissionStore::open(dir.path()).await.unwrap();

    cli::run(send_cli(
        dir.path(),
        &mission,
        &conversation,
        &["--commit", BASE_SHA],
        "failed reference message must not be presented",
    ))
    .await
    .unwrap();
    cli::run(send_cli(
        dir.path(),
        &mission,
        &conversation,
        &[],
        "distinct later message must progress",
    ))
    .await
    .unwrap();
    let accepted = store.require_state(&mission).await.unwrap();
    let accepted_conversation = accepted.conversations.values().next().unwrap();
    let unavailable_sequence = accepted_conversation.queued[0].sequence_no;
    let later_sequence = accepted_conversation.queued[1].sequence_no;
    let attempts_before = accepted.tasks.values().next().unwrap().attempts;
    let object = dir
        .path()
        .join(".git/objects")
        .join(&BASE_SHA[..2])
        .join(&BASE_SHA[2..]);
    assert!(object.is_file(), "fault injection requires a loose commit");
    fault.inject(&object);

    let advanced = h.engine.advance(&mission).await.unwrap().state;
    let failures = reference_failures(&advanced);
    assert_eq!(failures.len(), 1);
    let conversation = advanced.conversations.values().next().unwrap();
    assert_eq!(conversation.queued.len(), 1);
    assert_eq!(conversation.queued[0].marker, DeliveryMarker::Undeliverable);
    assert_eq!(conversation.queued[0].sequence_no, unavailable_sequence);
    assert!(conversation.consumed_through > later_sequence);
    assert!(conversation.active_delivery.is_none());
    assert_eq!(
        advanced.tasks.values().next().unwrap().attempts,
        attempts_before + 2
    );
    assert_eq!(
        advanced.tasks.values().next().unwrap().consecutive_failures,
        0
    );
    let detail = &failures[0].evidence().detail;
    assert!(detail.contains(&format!("queued message {unavailable_sequence}")));
    assert!(detail.contains(BASE_SHA));
    assert!(detail.contains(&format!("{:?}", fault.cause())));
    let prompt = prompts.lock().unwrap().last().unwrap().clone();
    assert!(prompt.contains("distinct later message must progress"));
    assert!(!prompt.contains("failed reference message must not be presented"));
    assert!(!prompt.contains("reachable commit"));

    let replayed = lionclaw::model::fold(store.load(&mission).await.unwrap()).unwrap();
    assert_eq!(advanced, replayed);
    assert_eq!(
        store.require_state(&mission).await.unwrap(),
        advanced,
        "reload preserves exact unavailable-reference accounting"
    );
    assert_operator_views(
        dir.path(),
        &mission,
        unavailable_sequence,
        conversation.consumed_through,
    )
    .await;
    let snapshot = store.rebuild_cursors(&mission, 29_000).await.unwrap();
    let snapshot_head = snapshot.head;
    assert_eq!(
        store.snapshot_meta(&mission).await.unwrap(),
        Some((snapshot_head, REDUCER_VERSION))
    );

    cli::run(send_cli(
        dir.path(),
        &mission,
        conversation_id(&snapshot),
        &[],
        "snapshot tail message",
    ))
    .await
    .unwrap();
    let tail_sequence = store
        .require_state(&mission)
        .await
        .unwrap()
        .conversations
        .values()
        .next()
        .unwrap()
        .queued[1]
        .sequence_no;
    let final_state = store.require_state(&mission).await.unwrap();
    assert!(
        final_state.head > snapshot_head,
        "snapshot tail must be nonempty"
    );
    assert_eq!(
        reference_failures(&final_state).len(),
        1,
        "settlement is once-only"
    );
    let final_conversation = final_state.conversations.values().next().unwrap();
    assert_eq!(
        final_conversation.queued[0].marker,
        DeliveryMarker::Undeliverable
    );
    assert_eq!(final_conversation.queued.len(), 2);
    assert_eq!(final_conversation.queued[1].marker, DeliveryMarker::Queued);
    assert_eq!(final_conversation.queued[1].sequence_no, tail_sequence);
    assert_eq!(
        final_conversation.consumed_through,
        conversation.consumed_through
    );
    assert_eq!(
        final_state.tasks.values().next().unwrap().attempts,
        attempts_before + 2
    );
    assert_eq!(
        final_state
            .tasks
            .values()
            .next()
            .unwrap()
            .consecutive_failures,
        0
    );

    let full = lionclaw::model::fold(store.load(&mission).await.unwrap()).unwrap();
    assert_eq!(final_state, full);
    assert_eq!(store.require_state(&mission).await.unwrap(), full);
    assert_eq!(
        store.load_state_snapshotted(&mission).await.unwrap(),
        Some(full.clone())
    );
    assert_eq!(
        store.snapshot_meta(&mission).await.unwrap(),
        Some((snapshot_head, REDUCER_VERSION)),
        "the asserted reducer-30 snapshot must remain behind the final log head"
    );

    let database = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}",
        dir.path().join(".lionclaw/mission.db").display()
    ))
    .await
    .unwrap();
    sqlx::query("UPDATE mission_snapshots SET reducer_version = 28 WHERE mission_id = ?1")
        .bind(mission.as_str())
        .execute(&database)
        .await
        .unwrap();
    assert_eq!(store.require_state(&mission).await.unwrap(), full);
    assert_eq!(store.rebuild_cursors(&mission, 29_001).await.unwrap(), full);
    assert_eq!(
        store.snapshot_meta(&mission).await.unwrap(),
        Some((full.head, REDUCER_VERSION))
    );
}

#[derive(Clone, Copy, Debug)]
enum ReceiptBlobFault {
    Missing,
    Corrupt,
}

impl ReceiptBlobFault {
    fn cause(self) -> UnavailableReferenceCause {
        match self {
            Self::Missing => UnavailableReferenceCause::SourceMissing,
            Self::Corrupt => UnavailableReferenceCause::InvalidContent,
        }
    }

    fn inject(self, blob_path: &std::path::Path) {
        match self {
            Self::Missing => std::fs::remove_file(blob_path).unwrap(),
            Self::Corrupt => {
                let mut corrupted = std::fs::read(blob_path).unwrap();
                corrupted[0] ^= 0xff;
                let mut permissions = std::fs::metadata(blob_path).unwrap().permissions();
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    permissions.set_mode(0o644);
                }
                std::fs::set_permissions(blob_path, permissions).unwrap();
                std::fs::write(blob_path, corrupted).unwrap();
            }
        }
    }
}

#[tokio::test]
async fn accepted_receipt_blob_fault_before_dispatch_settles_once() {
    for fault in [ReceiptBlobFault::Missing, ReceiptBlobFault::Corrupt] {
        prove_receipt_blob_fault_settles_once(fault).await;
    }
}

#[tokio::test]
async fn oversized_authoritative_receipt_is_rejected_with_exact_typed_truth() {
    let dir = tempfile::tempdir().unwrap();
    let runner = MockRoleRunner::new(Box::new(|request| {
        if let Some(outcome) = judgment_outcome(request) {
            return Ok(outcome);
        }
        if request.attempt_no == 1 {
            Ok(RoleTurnOutcome {
                handoff: Some(Handoff::Work {
                    done: true,
                    report: PayloadRef::inline("minted oversized receipt authority"),
                    request_attention: false,
                }),
                artifact: Some(CapturedArtifact::for_testing(
                    request.base_sha.clone(),
                    common::HEAD_SHA.to_string(),
                )),
                prepared_inputs: Vec::new(),
                runtime_configuration: Default::default(),
                runtime_usage: Default::default(),
                final_response: "minted oversized receipt authority".into(),
            })
        } else {
            Ok(checkpoint(request))
        }
    }));
    let oracle = MockOracleRunner::new(Box::new(|_| {
        let mut stdout = vec![b'R'; 128 * 1024];
        stdout.push(0xff);
        Ok(OracleOutcome {
            exit_code: 1,
            exit_signal: None,
            stdout,
            stderr: Vec::new(),
            prepared_inputs: Vec::new(),
            duration_ms: 7,
        })
    }));
    let h = harness(dir.path(), runner, oracle).await;
    let mission = h
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "oversized receipt reference",
            BASE_SHA,
        )
        .await
        .unwrap();
    let mut plan = simple_plan();
    plan.tasks[0].id = TaskId::new("mint-oversized-receipt").unwrap();
    h.engine
        .propose_plan(&mission, proposal(0, plan))
        .await
        .unwrap();
    approve_plan(&h.engine, &mission).await;
    for _ in 0..8 {
        h.engine.advance(&mission).await.unwrap();
        let state = h.engine.load_state(&mission).await.unwrap();
        if !state.authoritative_receipts.is_empty() && !common::decision_ids(&state).is_empty() {
            break;
        }
    }
    let store = MissionStore::open(dir.path()).await.unwrap();
    let failed = store.require_state(&mission).await.unwrap();
    let receipt = failed.authoritative_receipts.keys().next().unwrap().clone();
    let receipt_is_blob_backed = store
        .load(&mission)
        .await
        .unwrap()
        .into_iter()
        .any(|event| {
            matches!(
                event.event,
                MissionEvent::OracleRunCompleted {
                    effect_id,
                    outcome: Ok(lionclaw::model::OracleRunSuccess {
                        stdout: PayloadRef::Blob(_),
                        ..
                    }),
                    ..
                } if effect_id == receipt
            )
        });
    assert!(receipt_is_blob_backed);
    let attention = common::decision_id_with_prefix(&failed, "proof_failed:");
    h.engine
        .decide(
            &mission,
            &attention,
            DecisionAction::Repair,
            "retry recipient",
        )
        .await
        .unwrap();
    h.engine.advance(&mission).await.unwrap();
    let ready = store.require_state(&mission).await.unwrap();
    let conversation = ready
        .conversations
        .iter()
        .find(|(id, _)| ready.conversation_is_messageable(id))
        .map(|(id, _)| id.clone())
        .unwrap();
    let before = store.load(&mission).await.unwrap();
    let error = cli::run(send_cli(
        dir.path(),
        &mission,
        conversation.as_str(),
        &["--receipt", receipt.as_str()],
        "must reject before ingress",
    ))
    .await
    .expect_err("oversized receipt must fail closed");
    assert_eq!(
        error.downcast_ref::<ReferenceRejectionReason>(),
        Some(&ReferenceRejectionReason::Oversized {
            reference: Some(MessageReference::AuthoritativeReceipt { effect_id: receipt }),
        })
    );
    assert_eq!(store.load(&mission).await.unwrap(), before);
}

async fn prove_receipt_blob_fault_settles_once(fault: ReceiptBlobFault) {
    let dir = tempfile::tempdir().unwrap();
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let observed = prompts.clone();
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if let Some(outcome) = judgment_outcome(request) {
            return Ok(outcome);
        }
        observed.lock().unwrap().push(request.prompt.clone());
        if request.attempt_no == 1 {
            Ok(RoleTurnOutcome {
                handoff: Some(Handoff::Work {
                    done: true,
                    report: PayloadRef::inline("minted receipt authority"),
                    request_attention: false,
                }),
                artifact: Some(CapturedArtifact::for_testing(
                    request.base_sha.clone(),
                    common::HEAD_SHA.to_string(),
                )),
                prepared_inputs: Vec::new(),
                runtime_configuration: Default::default(),
                runtime_usage: Default::default(),
                final_response: "minted receipt authority".into(),
            })
        } else {
            Ok(checkpoint(request))
        }
    }));
    let oracle = MockOracleRunner::new(Box::new(|_| {
        let mut stdout = b"REAL-BELOW-LIMIT-RECEIPT\n".to_vec();
        stdout.extend(std::iter::repeat_n(b'R', 10 * 1024));
        stdout.push(0xff);
        Ok(OracleOutcome {
            exit_code: 1,
            exit_signal: None,
            stdout,
            stderr: Vec::new(),
            prepared_inputs: Vec::new(),
            duration_ms: 7,
        })
    }));
    let h = harness(dir.path(), runner, oracle).await;
    let mission = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "receipt reference", BASE_SHA)
        .await
        .unwrap();
    let mut plan = simple_plan();
    plan.tasks[0].id = TaskId::new("mint-receipt").unwrap();
    h.engine
        .propose_plan(&mission, proposal(0, plan))
        .await
        .unwrap();
    approve_plan(&h.engine, &mission).await;
    for _ in 0..8 {
        h.engine.advance(&mission).await.unwrap();
        let state = MissionStore::open(dir.path())
            .await
            .unwrap()
            .require_state(&mission)
            .await
            .unwrap();
        if !state.authoritative_receipts.is_empty() && !common::decision_ids(&state).is_empty() {
            break;
        }
    }
    let store = MissionStore::open(dir.path()).await.unwrap();
    let failed = store.require_state(&mission).await.unwrap();
    let receipt = failed
        .authoritative_receipts
        .keys()
        .next()
        .unwrap_or_else(|| panic!("receipt was not minted: {failed:#?}"))
        .clone();
    let attention = common::decision_id_with_prefix(&failed, "proof_failed:");
    h.engine
        .decide(
            &mission,
            &attention,
            DecisionAction::Repair,
            "retry recipient",
        )
        .await
        .unwrap();
    h.engine.advance(&mission).await.unwrap();
    let accepted = store.require_state(&mission).await.unwrap();
    let conversation = RoleInstanceId::new("implementer").unwrap();
    assert!(accepted.conversation_is_messageable(&conversation));
    cli::run(send_cli(
        dir.path(),
        &mission,
        conversation.as_str(),
        &["--receipt", receipt.as_str()],
        "whole receipt message must not be presented",
    ))
    .await
    .unwrap();
    cli::run(send_cli(
        dir.path(),
        &mission,
        conversation.as_str(),
        &[],
        "later receipt-boundary message progresses",
    ))
    .await
    .unwrap();
    let queued = store.require_state(&mission).await.unwrap();
    let failed_sequence = queued.conversations[&conversation].queued[0].sequence_no;
    let receipt_blob = store
        .load(&mission)
        .await
        .unwrap()
        .into_iter()
        .find_map(|event| match event.event {
            MissionEvent::OracleRunCompleted {
                effect_id,
                outcome: Ok(success),
                ..
            } if effect_id == receipt => match success.stdout {
                PayloadRef::Blob(blob) => Some(blob),
                PayloadRef::Inline { .. } => None,
            },
            _ => None,
        })
        .expect("non-UTF-8 production oracle output is blob-backed");
    let blob_path = store
        .lionclaw_dir()
        .join("blobs/sha256")
        .join(&receipt_blob.hex[..2])
        .join(&receipt_blob.hex[2..4])
        .join(&receipt_blob.hex);
    assert_rejected_receipt_completion_cannot_supply_reference_prose(
        &store,
        &mission,
        &receipt,
        dir.path(),
    )
    .await;
    fault.inject(&blob_path);

    let attempts_before = queued.tasks[&TaskId::new("mint-receipt").unwrap()].attempts;
    let settled = h.engine.advance(&mission).await.unwrap().state;
    let conversation_state = &settled.conversations[&conversation];
    let failures = reference_failures(&settled);
    assert_eq!(failures.len(), 1);
    assert_eq!(conversation_state.queued.len(), 1);
    assert_eq!(
        conversation_state.queued[0].marker,
        DeliveryMarker::Undeliverable
    );
    assert_eq!(conversation_state.queued[0].sequence_no, failed_sequence);
    assert!(conversation_state.active_delivery.is_none());
    let detail = &failures[0].evidence().detail;
    assert!(detail.contains(receipt.as_str()));
    assert!(detail.contains(&format!("{:?}", fault.cause())));
    assert_eq!(
        settled.tasks[&TaskId::new("mint-receipt").unwrap()].attempts,
        attempts_before + 2
    );
    let prompt = prompts.lock().unwrap().last().unwrap().clone();
    assert!(prompt.contains("later receipt-boundary message progresses"));
    assert!(!prompt.contains("whole receipt message must not be presented"));
    assert!(!prompt.contains("REAL-BELOW-LIMIT-RECEIPT"));
    assert_eq!(
        lionclaw::model::fold(store.load(&mission).await.unwrap()).unwrap(),
        settled
    );
    assert_operator_views(
        dir.path(),
        &mission,
        failed_sequence,
        conversation_state.consumed_through,
    )
    .await;
    assert_replay_snapshot_tail(
        dir.path(),
        &store,
        &mission,
        conversation.as_str(),
        &settled,
        &format!("receipt {fault:?} replay tail remains queued"),
    )
    .await;
}

async fn assert_rejected_receipt_completion_cannot_supply_reference_prose(
    store: &MissionStore,
    mission: &lionclaw::model::MissionId,
    receipt: &lionclaw::model::EffectId,
    repo: &std::path::Path,
) {
    let events = store.load(mission).await.unwrap();
    let completion = events
        .iter()
        .position(|envelope| {
            matches!(
                envelope.event,
                MissionEvent::OracleRunCompleted { ref effect_id, .. } if effect_id == receipt
            )
        })
        .unwrap();
    let mut prefix = events[..=completion].to_vec();
    let mut legitimate = prefix.pop().unwrap();
    let mut rejected = legitimate.clone();
    let MissionEvent::OracleRunCompleted {
        judged_sha,
        outcome: Ok(success),
        ..
    } = &mut rejected.event
    else {
        unreachable!()
    };
    *judged_sha = "forged-unjudged-head".into();
    success.stdout = PayloadRef::inline("FORGED-RECEIPT-PROSE");
    legitimate.sequence_no = legitimate.sequence_no.saturating_add(1);
    prefix.push(rejected);
    prefix.push(legitimate);

    let replayed = lionclaw::model::fold(prefix.clone()).unwrap();
    assert!(replayed.authoritative_receipts.contains_key(receipt));
    let materialized = lionclaw::reference_materialization::materialize_references(
        &replayed,
        &prefix,
        store.blobs(),
        repo,
        &[MessageReference::AuthoritativeReceipt {
            effect_id: receipt.clone(),
        }],
    )
    .await
    .unwrap();
    assert!(materialized[0].content.contains("REAL-BELOW-LIMIT-RECEIPT"));
    assert!(!materialized[0].content.contains("FORGED-RECEIPT-PROSE"));
}

#[tokio::test]
async fn accepted_park_reference_survives_legal_clear_from_durable_history() {
    let dir = tempfile::tempdir().unwrap();
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let observed = prompts.clone();
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if let Some(outcome) = judgment_outcome(request) {
            return Ok(outcome);
        }
        observed
            .lock()
            .unwrap()
            .push((request.task_id.clone(), request.prompt.clone()));
        if request
            .task_id
            .as_ref()
            .is_some_and(|task_id| task_id.as_str() == "park-source")
        {
            Err(TypedFailure::transient(
                "reference.park-source",
                "REAL-DURABLE-PARK-EVIDENCE",
                None,
            ))
        } else {
            Ok(checkpoint(request))
        }
    }));
    let h = harness(dir.path(), runner, MockOracleRunner::exiting(0)).await;
    let mission = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "park lifecycle", BASE_SHA)
        .await
        .unwrap();
    let mut plan = simple_plan();
    plan.tasks[0].id = TaskId::new("recipient").unwrap();
    let mut park_source = plan.tasks[0].clone();
    park_source.id = TaskId::new("park-source").unwrap();
    park_source.targets.clear();
    plan.tasks.push(park_source);
    plan.tasks.push(lionclaw::model::Task {
        id: TaskId::new("finalize").unwrap(),
        body: "combine the reference source and recipient work".into(),
        targets: Vec::new(),
        depends_on: vec![
            TaskId::new("recipient").unwrap(),
            TaskId::new("park-source").unwrap(),
        ],
    });
    let mut mission_proposal = proposal(0, plan);
    let source_role = RoleInstanceId::new("source-role").unwrap();
    let team = mission_proposal.team.as_mut().unwrap();
    team.roles.insert(
        source_role.clone(),
        common::role(
            "source-role",
            lionclaw::model::OutputSemantics::ProducesArtifact,
        ),
    );
    team.task_assignments
        .insert(TaskId::new("park-source").unwrap(), source_role);
    h.engine
        .propose_plan(&mission, mission_proposal)
        .await
        .unwrap();
    approve_plan(&h.engine, &mission).await;

    for _ in 0..8 {
        h.engine.advance(&mission).await.unwrap();
        let state = h.engine.load_state(&mission).await.unwrap();
        if !state.parked_effects.is_empty()
            && state.conversation_is_messageable(&RoleInstanceId::new("implementer").unwrap())
        {
            break;
        }
    }
    let store = MissionStore::open(dir.path()).await.unwrap();
    let parked = store.require_state(&mission).await.unwrap();
    let park = parked
        .parked_effects
        .keys()
        .next()
        .unwrap_or_else(|| panic!("real role failure did not park: {parked:#?}"))
        .clone();
    let recipient = RoleInstanceId::new("implementer").unwrap();
    assert!(parked.conversation_is_messageable(&recipient));

    let pre_clear = lionclaw::reference_materialization::materialize_references(
        &parked,
        &store.load(&mission).await.unwrap(),
        store.blobs(),
        dir.path(),
        &[MessageReference::ParkEvidence {
            effect_id: park.clone(),
        }],
    )
    .await
    .unwrap();
    assert_eq!(pre_clear.len(), 1);
    assert!(pre_clear[0].content.contains("REAL-DURABLE-PARK-EVIDENCE"));

    cli::run(send_cli(
        dir.path(),
        &mission,
        recipient.as_str(),
        &["--park", park.as_str()],
        "accepted before clear and delivered after clear",
    ))
    .await
    .unwrap();
    let accepted = store.require_state(&mission).await.unwrap();
    let accepted_message = accepted.conversations[&recipient].queued[0].clone();
    let source_event = store
        .load(&mission)
        .await
        .unwrap()
        .into_iter()
        .find(|event| matches!(&event.event, MissionEvent::RoleTurnCompleted { effect_id, .. } if effect_id == &park))
        .expect("immutable park source event");
    cli::run(
        Cli::try_parse_from([
            "lionclaw",
            "mission",
            "continue",
            mission.as_str(),
            park.as_str(),
            "--reason",
            "clear park through production control",
            "--repo",
            dir.path().to_str().unwrap(),
        ])
        .unwrap(),
    )
    .await
    .unwrap();
    let cleared = store.require_state(&mission).await.unwrap();
    assert!(!cleared.parked_effects.contains_key(&park));
    assert_eq!(
        cleared.conversations[&recipient].queued[0],
        accepted_message
    );

    h.engine.advance(&mission).await.unwrap();
    let delivered = store.require_state(&mission).await.unwrap();
    assert!(reference_failures(&delivered).is_empty());
    assert!(delivered.conversations[&recipient].queued.is_empty());
    let post_clear = prompts
        .lock()
        .unwrap()
        .iter()
        .rev()
        .find(|(task, prompt)| {
            task.as_ref()
                .is_some_and(|task| task.as_str() == "recipient")
                && prompt.contains("accepted before clear and delivered after clear")
        })
        .unwrap()
        .1
        .clone();
    assert!(post_clear.contains(&format!("park evidence {park}:")));
    assert!(post_clear.contains("REAL-DURABLE-PARK-EVIDENCE"));
    let events = store.load(&mission).await.unwrap();
    assert!(events.iter().any(|event| event == &source_event));
    assert!(reference_failures(&delivered).is_empty());

    cli::run(send_cli(
        dir.path(),
        &mission,
        recipient.as_str(),
        &[],
        "distinct later boundary progresses",
    ))
    .await
    .unwrap();
    if let Some(blocking_effect) = store
        .require_state(&mission)
        .await
        .unwrap()
        .parked_effects
        .keys()
        .next()
        .cloned()
    {
        cli::run(
            Cli::try_parse_from([
                "lionclaw",
                "mission",
                "continue",
                mission.as_str(),
                blocking_effect.as_str(),
                "--reason",
                "allow the distinct later boundary",
                "--repo",
                dir.path().to_str().unwrap(),
            ])
            .unwrap(),
        )
        .await
        .unwrap();
    }
    h.engine.advance(&mission).await.unwrap();
    let final_state = store.require_state(&mission).await.unwrap();
    assert!(final_state.conversations[&recipient].queued.is_empty());
    assert!(prompts.lock().unwrap().iter().any(|(task, prompt)| {
        task.as_ref()
            .is_some_and(|task| task.as_str() == "recipient")
            && prompt.contains("distinct later boundary progresses")
    }));
    assert_eq!(
        lionclaw::model::fold(store.load(&mission).await.unwrap()).unwrap(),
        final_state
    );
    assert_park_operator_views(dir.path(), &mission, recipient.as_str()).await;
    assert_replay_snapshot_tail(
        dir.path(),
        &store,
        &mission,
        recipient.as_str(),
        &final_state,
        "park replay tail remains queued",
    )
    .await;
}

async fn assert_replay_snapshot_tail(
    repo: &std::path::Path,
    store: &MissionStore,
    mission: &lionclaw::model::MissionId,
    conversation: &str,
    before_tail: &lionclaw::model::MissionState,
    tail_body: &str,
) {
    assert_eq!(store.require_state(mission).await.unwrap(), *before_tail);
    assert_eq!(
        lionclaw::model::fold(store.load(mission).await.unwrap()).unwrap(),
        *before_tail
    );
    let snapshot = store.rebuild_cursors(mission, 29_100).await.unwrap();
    assert_eq!(snapshot, *before_tail);
    let snapshot_head = snapshot.head;
    assert_eq!(
        store.snapshot_meta(mission).await.unwrap(),
        Some((snapshot_head, REDUCER_VERSION))
    );

    cli::run(send_cli(repo, mission, conversation, &[], tail_body))
        .await
        .unwrap();
    let final_state = store.require_state(mission).await.unwrap();
    assert!(
        final_state.head > snapshot_head,
        "genuine reducer-30 snapshot must have a nonempty durable tail"
    );
    let queued_tail = final_state.conversations[&RoleInstanceId::new(conversation).unwrap()]
        .queued
        .iter()
        .find(|message| message.body == tail_body)
        .expect("snapshot-tail message retained whole");
    assert_eq!(queued_tail.marker, DeliveryMarker::Queued);
    assert_eq!(
        store.snapshot_meta(mission).await.unwrap(),
        Some((snapshot_head, REDUCER_VERSION)),
        "the genuine snapshot head must remain strictly behind the final head"
    );
    let full = lionclaw::model::fold(store.load(mission).await.unwrap()).unwrap();
    assert_eq!(final_state, full);
    assert_eq!(
        MissionStore::open(repo)
            .await
            .unwrap()
            .require_state(mission)
            .await
            .unwrap(),
        full,
        "MissionStore reload must apply the nonempty snapshot tail"
    );
    assert_eq!(
        store.load_state_snapshotted(mission).await.unwrap(),
        Some(full.clone())
    );

    let database = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}",
        repo.join(".lionclaw/mission.db").display()
    ))
    .await
    .unwrap();
    sqlx::query("UPDATE mission_snapshots SET reducer_version = 28 WHERE mission_id = ?1")
        .bind(mission.as_str())
        .execute(&database)
        .await
        .unwrap();
    assert_eq!(store.require_state(mission).await.unwrap(), full);
    assert_eq!(store.rebuild_cursors(mission, 29_101).await.unwrap(), full);
    assert_eq!(
        store.snapshot_meta(mission).await.unwrap(),
        Some((full.head, REDUCER_VERSION))
    );
}

fn conversation_id(state: &lionclaw::model::MissionState) -> &str {
    state.conversations.keys().next().unwrap().as_str()
}

async fn assert_operator_views(
    repo: &std::path::Path,
    mission: &lionclaw::model::MissionId,
    unavailable_sequence: u64,
    delivered_through: u64,
) {
    let state = MissionStore::open(repo)
        .await
        .unwrap()
        .require_state(mission)
        .await
        .unwrap();
    let _failure = reference_failures(&state)
        .into_iter()
        .find(|failure| {
            failure
                .evidence()
                .detail
                .contains(&format!("queued message {unavailable_sequence}"))
        })
        .expect("exact unavailable-reference receipt");
    let (role_instance, conversation) = state
        .conversations
        .iter()
        .find(|(_, conversation)| {
            conversation
                .queued
                .iter()
                .any(|message| message.sequence_no == unavailable_sequence)
        })
        .expect("affected team conversation");
    let unavailable = conversation
        .queued
        .iter()
        .find(|message| message.sequence_no == unavailable_sequence)
        .expect("whole unavailable message retained");
    for args in [
        vec!["mission", "status", mission.as_str(), "--json"],
        vec!["mission", "report", mission.as_str(), "--json"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_lionclaw"))
            .args(args)
            .arg("--repo")
            .arg(repo)
            .output()
            .unwrap();
        assert!(output.status.success());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        let root = json
            .get("missions")
            .and_then(|v| v.as_array())
            .map_or(&json, |v| &v[0]);
        assert_eq!(root["mission_id"], mission.as_str());
        assert!(root.get("unavailable_references").is_none());
        assert!(root.to_string().contains("message.reference_unavailable"));
        let projected = root["conversations"]
            .as_array()
            .unwrap()
            .iter()
            .find(|value| value["id"] == role_instance.as_str())
            .expect("affected conversation is selected by identity");
        assert!(projected.get("assignment_epoch").is_none());
        let queued = projected["queued_messages"].as_array().unwrap();
        let projected_message = queued
            .iter()
            .find(|value| value["sequence_no"] == unavailable_sequence)
            .expect("exact unavailable message is retained");
        assert_eq!(
            projected_message,
            &serde_json::to_value(unavailable).unwrap()
        );
        assert_eq!(projected_message["marker"], "undeliverable");
        assert_eq!(projected["consumed_through"], delivered_through);
        assert!(projected["active_message_boundary"].is_null());
        assert!(projected["presented_messages"].is_null());
    }
    for args in [
        vec!["mission", "status", mission.as_str()],
        vec!["mission", "report", mission.as_str()],
        vec!["mission", "inbox"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_lionclaw"))
            .args(args)
            .arg("--repo")
            .arg(repo)
            .output()
            .unwrap();
        assert!(output.status.success());
        let human = String::from_utf8(output.stdout).unwrap();
        assert!(human.contains(mission.as_str()));
        assert!(human.contains(role_instance.as_str()));
        assert!(human.contains("marker=undeliverable"));
        assert!(human.contains(&format!("delivery_through={delivered_through}")));
        assert!(human.contains("message.reference_unavailable"));
    }
    let inbox = Command::new(env!("CARGO_BIN_EXE_lionclaw"))
        .args(["mission", "inbox", "--json", "--repo"])
        .arg(repo)
        .output()
        .unwrap();
    assert!(inbox.status.success());
    let inbox: serde_json::Value = serde_json::from_slice(&inbox.stdout).unwrap();
    let missions = inbox["missions"].as_array().unwrap();
    let status = cli_json(repo, mission, "status");
    assert_inbox_binding(mission.as_str(), &status, missions);
}

async fn assert_park_operator_views(
    repo: &std::path::Path,
    mission: &lionclaw::model::MissionId,
    conversation_id: &str,
) {
    let state = MissionStore::open(repo)
        .await
        .unwrap()
        .require_state(mission)
        .await
        .unwrap();
    assert!(reference_failures(&state).is_empty());
    let conversation = state
        .conversations
        .iter()
        .find(|(id, _)| id.as_str() == conversation_id)
        .unwrap()
        .1;
    for command in ["status", "report"] {
        let output = Command::new(env!("CARGO_BIN_EXE_lionclaw"))
            .args(["mission", command, mission.as_str(), "--json", "--repo"])
            .arg(repo)
            .output()
            .unwrap();
        assert!(output.status.success());
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        let root = json
            .get("missions")
            .and_then(|value| value.as_array())
            .map_or(&json, |missions| &missions[0]);
        assert_eq!(root["mission_id"], mission.as_str());
        assert!(root.get("unavailable_references").is_none());
        let projected = root["conversations"]
            .as_array()
            .unwrap()
            .iter()
            .find(|value| value["id"] == conversation_id)
            .unwrap();
        assert!(projected.get("assignment_epoch").is_none());
        assert_eq!(projected["consumed_through"], conversation.consumed_through);
        assert_eq!(
            projected["queued_messages"],
            serde_json::json!(conversation.queued)
        );

        let human = Command::new(env!("CARGO_BIN_EXE_lionclaw"))
            .args(["mission", command, mission.as_str(), "--repo"])
            .arg(repo)
            .output()
            .unwrap();
        assert!(human.status.success());
        let human = String::from_utf8(human.stdout).unwrap();
        assert!(human.contains(mission.as_str()));
        assert!(human.contains(&format!(
            "conversation {conversation_id}: lifecycle={} queued={} delivery_through={}",
            serde_json::to_value(conversation.lifecycle)
                .unwrap()
                .as_str()
                .unwrap(),
            conversation.queued.len(),
            conversation.consumed_through
        )));
        assert!(!human.contains("message.reference_unavailable"));
    }
    let inbox = Command::new(env!("CARGO_BIN_EXE_lionclaw"))
        .args(["mission", "inbox", "--json", "--repo"])
        .arg(repo)
        .output()
        .unwrap();
    assert!(inbox.status.success());
    let inbox: serde_json::Value = serde_json::from_slice(&inbox.stdout).unwrap();
    let missions = inbox["missions"].as_array().unwrap();
    assert_eq!(missions.len(), 1);
    assert_eq!(missions[0]["mission_id"], mission.as_str());
    let status = cli_json(repo, mission, "status");
    assert_eq!(missions[0]["next"], status["next"]);
}

fn assert_inbox_binding(mission_id: &str, status: &serde_json::Value, inbox: &[serde_json::Value]) {
    assert_eq!(status["mission_id"], mission_id);
    let selected = inbox
        .iter()
        .filter(|record| record["mission_id"] == mission_id)
        .collect::<Vec<_>>();
    assert_eq!(
        selected.len(),
        inbox.len(),
        "unrelated inbox mission record"
    );
    if let Some(record) = selected.first() {
        assert_eq!(selected.len(), 1);
        assert_eq!(record["mission_id"], status["mission_id"]);
        assert_eq!(record["next"], status["next"]);
        assert_eq!(record["conversations"], status["conversations"]);
        return;
    }

    let choices = status["next"]["choices"].as_array().unwrap();
    assert!(choices.iter().all(|choice| !matches!(
        choice["kind"].as_str(),
        Some("decide" | "send_message" | "continue" | "finish")
    )));
}

#[test]
#[should_panic]
fn inbox_binding_rejects_omitted_mission_that_awaits_lead_input() {
    let status = serde_json::json!({
        "mission_id": "m-awaiting",
        "terminal": null,
        "next": {
            "effects": [],
            "choices": [
                {"kind": "send_message", "role_instance": "implementer"},
                {"kind": "abort"}
            ]
        },
        "conversations": [{
            "lifecycle": "awaiting_lead",
            "queued_messages": []
        }]
    });
    assert_inbox_binding("m-awaiting", &status, &[]);
}

fn cli_json(
    repo: &std::path::Path,
    mission: &lionclaw::model::MissionId,
    command: &str,
) -> serde_json::Value {
    let output = Command::new(env!("CARGO_BIN_EXE_lionclaw"))
        .args(["mission", command, mission.as_str(), "--json", "--repo"])
        .arg(repo)
        .output()
        .unwrap();
    assert!(output.status.success());
    serde_json::from_slice(&output.stdout).unwrap()
}

fn walkdir(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut pending = vec![root.to_path_buf()];
    let mut found = Vec::new();
    while let Some(path) = pending.pop() {
        found.push(path.clone());
        if path.is_dir() {
            pending.extend(
                std::fs::read_dir(path)
                    .unwrap()
                    .map(|entry| entry.unwrap().path()),
            );
        }
    }
    found
}
