//! Production ingress/request-boundary coverage for transient message references.

mod common;

use std::process::Command;
use std::sync::{Arc, Mutex};

use clap::Parser;
use common::{approve_plan, harness, proposal, simple_plan, BASE_SHA};
use lionclaw::cli::{self, Cli};
use lionclaw::model::{
    DecisionAction, DeliveryMarker, Handoff, MessageReference, MissionEvent, PayloadRef, RoleName,
    TaskId, TaskKind, UnavailableReferenceCause, REDUCER_VERSION, SCHEMA_VERSION,
};
use lionclaw::ports::{
    CapturedArtifact, OracleOutcome, RoleRunOutcome, RoleRunRequest, RoleRunUpdate,
};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};
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

fn checkpoint(request: &RoleRunRequest) -> RoleRunOutcome {
    request
        .updates
        .try_send(RoleRunUpdate::WorkspacePrepared {
            base_sha: request.base_sha.clone(),
            assignment_epoch: request.assignment_epoch,
        })
        .unwrap();
    RoleRunOutcome {
        handoff: None,
        artifact: None,
        runtime_configuration: Default::default(),
        final_response: "awaiting lead".into(),
    }
}

#[tokio::test]
async fn reachable_commit_expands_only_at_the_typed_role_request_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let observed = prompts.clone();
    let runner = MockRoleRunner::new(Box::new(move |request| {
        observed.lock().unwrap().push(request.prompt.clone());
        Ok(checkpoint(request))
    }));
    let h = harness(dir.path(), runner, MockOracleRunner::exiting(0)).await;
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
        .expect("continued typed RoleRunRequest")
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

#[tokio::test]
async fn dead_reachable_commit_settles_once_and_does_not_block_a_later_message() {
    assert_eq!((SCHEMA_VERSION, REDUCER_VERSION), (21, 29));
    let dir = tempfile::tempdir().unwrap();
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let observed = prompts.clone();
    let runner = MockRoleRunner::new(Box::new(move |request| {
        observed.lock().unwrap().push(request.prompt.clone());
        Ok(checkpoint(request))
    }));
    let h = harness(dir.path(), runner, MockOracleRunner::exiting(0)).await;
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
    std::fs::remove_file(object).unwrap();

    let advanced = h.engine.advance(&mission).await.unwrap().state;
    assert_eq!(advanced.unavailable_references.len(), 1);
    let conversation = advanced.conversations.values().next().unwrap();
    assert_eq!(conversation.queued.len(), 1);
    assert_eq!(conversation.queued[0].marker, DeliveryMarker::Undeliverable);
    assert_eq!(conversation.queued[0].sequence_no, unavailable_sequence);
    assert!(conversation.consumed_through > later_sequence);
    assert!(conversation.active_delivery.is_none());
    assert_eq!(
        advanced.tasks.values().next().unwrap().attempts,
        attempts_before + 1
    );
    assert_eq!(
        advanced.tasks.values().next().unwrap().consecutive_failures,
        0
    );
    let evidence = &advanced.unavailable_references[0];
    assert_eq!(
        evidence.conversation_id.as_str(),
        conversation_id(&advanced)
    );
    assert_eq!(evidence.assignment_epoch, conversation.assignment_epoch);
    assert_eq!(evidence.message_sequence, unavailable_sequence);
    assert_eq!(
        evidence.reference,
        MessageReference::ReachableCommit {
            sha: BASE_SHA.to_owned()
        }
    );
    assert_eq!(evidence.cause, UnavailableReferenceCause::SourceMissing);
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
        final_state.unavailable_references.len(),
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
        attempts_before + 1
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
        "the asserted reducer-29 snapshot must remain behind the final log head"
    );

    assert_operator_views(
        dir.path(),
        &mission,
        unavailable_sequence,
        conversation.consumed_through,
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

#[tokio::test]
async fn accepted_receipt_blob_unavailable_before_dispatch_settles_once() {
    let dir = tempfile::tempdir().unwrap();
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let observed = prompts.clone();
    let runner = MockRoleRunner::new(Box::new(move |request| {
        observed.lock().unwrap().push(request.prompt.clone());
        if request.attempt_no == 1 {
            Ok(RoleRunOutcome {
                handoff: Some(Handoff::Work {
                    done: true,
                    report: PayloadRef::inline("minted receipt authority"),
                    request_attention: false,
                }),
                artifact: Some(CapturedArtifact::for_testing(
                    request.base_sha.clone(),
                    common::HEAD_SHA.to_string(),
                )),
                runtime_configuration: Default::default(),
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
        if !state.authoritative_receipts.is_empty() && !state.open_attention.is_empty() {
            break;
        }
    }
    let store = MissionStore::open(dir.path()).await.unwrap();
    let failed = store.require_state(&mission).await.unwrap();
    let receipt = failed
        .authoritative_receipts
        .iter()
        .next()
        .unwrap_or_else(|| panic!("receipt was not minted: {failed:#?}"))
        .clone();
    let attention = failed.open_attention.keys().next().unwrap().clone();
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
    let conversation = accepted
        .conversations
        .iter()
        .find(|(id, conversation)| {
            conversation.task_id.as_str() == "mint-receipt"
                && accepted.conversation_is_messageable(id)
        })
        .map(|(id, _)| id.clone())
        .unwrap();
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
    std::fs::remove_file(blob_path).unwrap();

    let attempts_before = queued.tasks[&TaskId::new("mint-receipt").unwrap()].attempts;
    let settled = h.engine.advance(&mission).await.unwrap().state;
    let conversation_state = &settled.conversations[&conversation];
    assert_eq!(settled.unavailable_references.len(), 1);
    assert_eq!(conversation_state.queued.len(), 1);
    assert_eq!(
        conversation_state.queued[0].marker,
        DeliveryMarker::Undeliverable
    );
    assert_eq!(conversation_state.queued[0].sequence_no, failed_sequence);
    assert!(conversation_state.active_delivery.is_none());
    assert_eq!(
        settled.unavailable_references[0].reference,
        MessageReference::AuthoritativeReceipt { effect_id: receipt }
    );
    assert_eq!(
        settled.unavailable_references[0].cause,
        UnavailableReferenceCause::SourceMissing
    );
    assert_eq!(
        settled.tasks[&TaskId::new("mint-receipt").unwrap()].attempts,
        attempts_before + 1
    );
    let prompt = prompts.lock().unwrap().last().unwrap().clone();
    assert!(prompt.contains("later receipt-boundary message progresses"));
    assert!(!prompt.contains("whole receipt message must not be presented"));
    assert!(!prompt.contains("REAL-BELOW-LIMIT-RECEIPT"));
    assert_eq!(
        lionclaw::model::fold(store.load(&mission).await.unwrap()).unwrap(),
        settled
    );
    assert_reference_operator_output(
        dir.path(),
        &mission,
        &["undeliverable"],
        &["REAL-BELOW-LIMIT-RECEIPT"],
    );
    assert_replay_snapshot_tail(
        dir.path(),
        &store,
        &mission,
        conversation.as_str(),
        &settled,
        "receipt replay tail remains queued",
    )
    .await;
}

#[tokio::test]
async fn accepted_park_reference_survives_legal_clear_from_durable_history() {
    let dir = tempfile::tempdir().unwrap();
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let observed = prompts.clone();
    let runner = MockRoleRunner::new(Box::new(move |request| {
        observed
            .lock()
            .unwrap()
            .push((request.task_id.clone(), request.prompt.clone()));
        if request.task_id.as_str() == "park-source" {
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
    park_source.kind = TaskKind::Validate;
    park_source.role = Some(RoleName::new("reviewer").unwrap());
    plan.tasks.push(park_source);
    h.engine
        .propose_plan(&mission, proposal(0, plan))
        .await
        .unwrap();
    approve_plan(&h.engine, &mission).await;

    for _ in 0..8 {
        h.engine.advance(&mission).await.unwrap();
        let state = h.engine.load_state(&mission).await.unwrap();
        if !state.parked_effects.is_empty()
            && state.conversations.iter().any(|(id, conversation)| {
                conversation.task_id.as_str() == "recipient"
                    && state.conversation_is_messageable(id)
            })
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
    let recipient = parked
        .conversations
        .iter()
        .find(|(_, conversation)| conversation.task_id.as_str() == "recipient")
        .map(|(id, _)| id.clone())
        .unwrap();

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
        .find(|event| matches!(&event.event, MissionEvent::RoleRunCompleted { effect_id, .. } if effect_id == &park))
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
    assert!(delivered.unavailable_references.is_empty());
    assert!(delivered.conversations[&recipient].queued.is_empty());
    let post_clear = prompts
        .lock()
        .unwrap()
        .iter()
        .rev()
        .find(|(task, prompt)| {
            task.as_str() == "recipient"
                && prompt.contains("accepted before clear and delivered after clear")
        })
        .unwrap()
        .1
        .clone();
    assert!(post_clear.contains(&format!("park evidence {park}:")));
    assert!(post_clear.contains("REAL-DURABLE-PARK-EVIDENCE"));
    let events = store.load(&mission).await.unwrap();
    assert!(events.iter().any(|event| event == &source_event));
    assert!(!events.iter().any(|event| matches!(
        event.event,
        MissionEvent::MessageReferenceUnavailable { .. }
    )));

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
        task.as_str() == "recipient" && prompt.contains("distinct later boundary progresses")
    }));
    assert_eq!(
        lionclaw::model::fold(store.load(&mission).await.unwrap()).unwrap(),
        final_state
    );
    assert_reference_operator_output(
        dir.path(),
        &mission,
        &["park"],
        &["unavailable reference:", "MessageReferenceUnavailable"],
    );
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
        "genuine reducer-29 snapshot must have a nonempty durable tail"
    );
    let queued_tail = final_state.conversations
        [&lionclaw::model::ConversationId::parse(conversation).unwrap()]
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

fn assert_reference_operator_output(
    repo: &std::path::Path,
    mission: &lionclaw::model::MissionId,
    required: &[&str],
    forbidden: &[&str],
) {
    for args in [
        vec!["mission", "status", mission.as_str(), "--json"],
        vec!["mission", "report", mission.as_str(), "--json"],
        vec!["mission", "inbox", "--json"],
        vec!["mission", "status", mission.as_str()],
        vec!["mission", "report", mission.as_str()],
        vec!["mission", "inbox"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_lionclaw"))
            .args(&args)
            .arg("--repo")
            .arg(repo)
            .output()
            .unwrap();
        assert!(output.status.success());
        assert!(
            !output.stdout.is_empty(),
            "operator output must be non-vacuous"
        );
        let rendered = String::from_utf8(output.stdout).unwrap();
        for forbidden in forbidden {
            assert!(
                !rendered.contains(forbidden),
                "operator output fabricated forbidden reference truth: {forbidden}"
            );
        }
        if !args.contains(&"inbox") {
            for required in required {
                assert!(
                    rendered.contains(required),
                    "operator output omitted reference truth: {required}"
                );
            }
        }
    }
}

fn conversation_id(state: &lionclaw::model::MissionState) -> &str {
    state.conversations.keys().next().unwrap().as_str()
}

fn assert_operator_views(
    repo: &std::path::Path,
    mission: &lionclaw::model::MissionId,
    unavailable_sequence: u64,
    delivered_through: u64,
) {
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
        assert_eq!(
            root["unavailable_references"][0]["message_sequence"],
            unavailable_sequence
        );
        let queued = root["conversations"][0]["queued_messages"]
            .as_array()
            .unwrap();
        assert_eq!(queued[0]["marker"], "undeliverable");
        assert_eq!(queued[0]["sequence_no"], unavailable_sequence);
        assert_eq!(
            root["conversations"][0]["consumed_through"],
            delivered_through
        );
    }
    for args in [
        vec!["mission", "status", mission.as_str()],
        vec!["mission", "report", mission.as_str()],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_lionclaw"))
            .args(args)
            .arg("--repo")
            .arg(repo)
            .output()
            .unwrap();
        assert!(output.status.success());
        let human = String::from_utf8(output.stdout).unwrap();
        assert!(human.contains("marker=undeliverable"));
        assert!(human.contains(&format!("delivery_through={delivered_through}")));
        assert!(human.contains("unavailable reference:"));
        assert!(human.contains("cause=SourceMissing"));
    }
    let inbox = Command::new(env!("CARGO_BIN_EXE_lionclaw"))
        .args(["mission", "inbox", "--json", "--repo"])
        .arg(repo)
        .output()
        .unwrap();
    assert!(inbox.status.success());
    let inbox: serde_json::Value = serde_json::from_slice(&inbox.stdout).unwrap();
    assert_eq!(inbox["missions"], serde_json::json!([]));
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
