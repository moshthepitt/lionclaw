//! Production ingress/request-boundary coverage for transient message references.

mod common;

use std::sync::{Arc, Mutex};

use clap::Parser;
use common::{approve_plan, effect_id, harness, proposal, simple_plan, BASE_SHA};
use lionclaw::cli::{self, Cli};
use lionclaw::model::{
    EventEnvelope, MessageReference, MissionEvent, OracleName, OracleRunSuccess, ParkedEffect,
    PayloadRef,
};
use lionclaw::ports::{RoleRunOutcome, RoleRunRequest, RoleRunUpdate};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};
use lionclaw_runtime_api::TypedFailure;

fn send_cli(
    repo: &std::path::Path,
    mission: &lionclaw::model::MissionId,
    conversation: &str,
    reference_args: &[&str],
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
    args.extend(["--repo", repo.to_str().unwrap(), "inspect the cited change"]);
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
async fn unavailable_references_fail_before_append_or_delivery_advancement() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(
        dir.path(),
        MockRoleRunner::new(Box::new(|request| Ok(checkpoint(request)))),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "invalid references", BASE_SHA)
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
    let before = store.load(&mission).await.unwrap();
    let state_before = store.require_state(&mission).await.unwrap();

    let invalid = [
        MessageReference::ReachableCommit {
            sha: "0".repeat(40),
        },
        MessageReference::AuthoritativeReceipt {
            effect_id: effect_id("foreign-receipt"),
        },
        MessageReference::ParkEvidence {
            effect_id: effect_id("foreign-park"),
        },
    ];
    for reference in invalid {
        let (flag, identity) = match &reference {
            MessageReference::ReachableCommit { sha } => ("--commit", sha.as_str()),
            MessageReference::AuthoritativeReceipt { effect_id } => {
                ("--receipt", effect_id.as_str())
            }
            MessageReference::ParkEvidence { effect_id } => ("--park", effect_id.as_str()),
        };
        let error = cli::run(send_cli(
            dir.path(),
            &mission,
            &conversation,
            &[flag, identity],
        ))
        .await
        .expect_err("foreign or unreachable authority must fail closed");
        assert!(error.to_string().contains("not valid authority"));
        let after = store.load(&mission).await.unwrap();
        assert_eq!(after, before, "failure changed the authoritative log");
        let state = store.require_state(&mission).await.unwrap();
        assert_eq!(state, state_before, "failure changed folded delivery state");
    }
}

#[tokio::test]
async fn receipt_and_park_material_are_labelled_bounded_and_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(
        dir.path(),
        MockRoleRunner::new(Box::new(|request| Ok(checkpoint(request)))),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "materialization", BASE_SHA)
        .await
        .unwrap();
    let store = MissionStore::open(dir.path()).await.unwrap();
    let mut state = store.require_state(&mission).await.unwrap();
    let mut events = store.load(&mission).await.unwrap();
    let stamps = events[0].stamps.clone();
    let receipt = effect_id("distinctive-receipt");
    let park = effect_id("distinctive-park");
    let oracle = OracleName::new("cargo-test").unwrap();
    state.authoritative_receipts.insert(receipt.clone());
    state.parked_effects.insert(
        park.clone(),
        ParkedEffect::OracleRun {
            oracle: oracle.clone(),
        },
    );
    events.push(EventEnvelope {
        mission_id: mission.clone(),
        sequence_no: 2,
        recorded_at_ms: 2,
        stamps: stamps.clone(),
        event: MissionEvent::OracleRunCompleted {
            assertion_ids: vec![],
            oracle: oracle.clone(),
            judged_sha: BASE_SHA.into(),
            attempt_no: 1,
            effect_id: receipt.clone(),
            outcome: Ok(OracleRunSuccess {
                exit_code: 0,
                exit_signal: None,
                stdout: PayloadRef::inline("RECEIPT-PAYLOAD-7d87"),
                stderr: PayloadRef::inline("receipt-stderr"),
                prepared_inputs: vec![],
                duration_ms: 1,
            }),
        },
    });
    events.push(EventEnvelope {
        mission_id: mission.clone(),
        sequence_no: 3,
        recorded_at_ms: 3,
        stamps: stamps.clone(),
        event: MissionEvent::OracleRunCompleted {
            assertion_ids: vec![],
            oracle,
            judged_sha: BASE_SHA.into(),
            attempt_no: 2,
            effect_id: park.clone(),
            outcome: Err(TypedFailure::permanent(
                "transport.distinctive",
                "PARK-PAYLOAD-c12e",
            )),
        },
    });

    let expanded = lionclaw::reference_materialization::materialize_references(
        &state,
        &events,
        store.blobs(),
        dir.path(),
        &[
            MessageReference::AuthoritativeReceipt {
                effect_id: receipt.clone(),
            },
            MessageReference::ParkEvidence {
                effect_id: park.clone(),
            },
        ],
    )
    .await
    .unwrap();
    assert_eq!(expanded[0].label, "authoritative receipt");
    assert_eq!(expanded[0].identity, receipt.to_string());
    assert!(expanded[0].content.contains("RECEIPT-PAYLOAD-7d87"));
    assert_eq!(expanded[1].label, "park evidence");
    assert_eq!(expanded[1].identity, park.to_string());
    assert!(expanded[1].content.contains("PARK-PAYLOAD-c12e"));

    let oversized = effect_id("oversized-receipt");
    state.authoritative_receipts.insert(oversized.clone());
    events.push(EventEnvelope {
        mission_id: mission,
        sequence_no: 4,
        recorded_at_ms: 4,
        stamps,
        event: MissionEvent::OracleRunCompleted {
            assertion_ids: vec![],
            oracle: OracleName::new("cargo-test").unwrap(),
            judged_sha: BASE_SHA.into(),
            attempt_no: 3,
            effect_id: oversized.clone(),
            outcome: Ok(OracleRunSuccess {
                exit_code: 0,
                exit_signal: None,
                stdout: PayloadRef::inline(
                    "x".repeat(lionclaw::reference_materialization::MAX_REFERENCE_BYTES + 1),
                ),
                stderr: PayloadRef::inline(""),
                prepared_inputs: vec![],
                duration_ms: 1,
            }),
        },
    });
    let error = lionclaw::reference_materialization::materialize_references(
        &state,
        &events,
        store.blobs(),
        dir.path(),
        &[MessageReference::AuthoritativeReceipt {
            effect_id: oversized,
        }],
    )
    .await
    .expect_err("oversized receipt must not materialize");
    assert!(error.to_string().contains("materialization bound"));
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
