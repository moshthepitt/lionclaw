//! Production ingress/request-boundary coverage for transient message references.

mod common;

use std::sync::{Arc, Mutex};

use clap::Parser;
use common::{approve_plan, harness, proposal, simple_plan, BASE_SHA};
use lionclaw::cli::{self, Cli};
use lionclaw::model::{MessageReference, MissionEvent};
use lionclaw::ports::{RoleRunOutcome, RoleRunRequest, RoleRunUpdate};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};

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
