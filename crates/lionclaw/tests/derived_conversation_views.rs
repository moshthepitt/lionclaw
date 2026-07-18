//! Production CLI projections of the folded conversation authority.

mod common;

use std::process::{Command, Output};
use std::sync::atomic::{AtomicUsize, Ordering};

use clap::Parser;
use common::{approve_plan, harness, proposal, simple_plan, BASE_SHA};
use lionclaw::cli::{self, Cli};
use lionclaw::model::{DeliveryMarker, MessageReference, MissionEvent, TypedFailure};
use lionclaw::ports::RoleRunOutcome;
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};

fn cli_output(repo: &std::path::Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_lionclaw"))
        .args(args)
        .arg("--repo")
        .arg(repo)
        .output()
        .expect("run production CLI")
}

fn stdout(output: Output) -> String {
    assert!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("UTF-8 CLI output")
}

fn conversation<'a>(value: &'a serde_json::Value, id: &str) -> &'a serde_json::Value {
    value["conversations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|conversation| conversation["id"] == id)
        .expect("conversation in projection")
}

#[tokio::test]
async fn real_cli_views_share_one_folded_conversation_projection() {
    let dir = tempfile::tempdir().unwrap();
    let turns = AtomicUsize::new(0);
    let runner = MockRoleRunner::new(Box::new(move |request| {
        request
            .updates
            .try_send(lionclaw::ports::RoleRunUpdate::WorkspacePrepared {
                base_sha: request.base_sha.clone(),
                assignment_epoch: request.assignment_epoch,
            })
            .unwrap();
        match turns.fetch_add(1, Ordering::SeqCst) {
            0 => Ok(RoleRunOutcome {
                handoff: None,
                artifact: None,
                runtime_configuration: Default::default(),
                final_response: format!("lead checkpoint {}", "x".repeat(80 * 1024)),
            }),
            _ => Err(TypedFailure::invalid(
                "handoff.schema",
                "the delivered response was malformed",
            )),
        }
    }));
    let h = harness(dir.path(), runner, MockOracleRunner::exiting(0)).await;
    let mission = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "derived views", BASE_SHA)
        .await
        .unwrap();
    h.engine
        .propose_plan(&mission, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&h.engine, &mission).await;
    let awaiting = h.engine.advance(&mission).await.unwrap();
    let conversation_id = awaiting.state.conversations.keys().next().unwrap().clone();
    assert_eq!(awaiting.disposition.slug(), "awaiting_lead");

    let runtime_root = dir
        .path()
        .join(".lionclaw/missions")
        .join(mission.as_str())
        .join("conversations")
        .join(conversation_id.as_str())
        .join("runtime");
    std::fs::create_dir_all(&runtime_root).unwrap();
    lionclaw_runtime_api::record_runtime_resume_mode(
        &runtime_root,
        lionclaw_runtime_api::RuntimeResumeMode::Resumed,
    )
    .unwrap();
    let awaiting_inbox: serde_json::Value = serde_json::from_str(&stdout(cli_output(
        dir.path(),
        &["mission", "inbox", "--json"],
    )))
    .unwrap();
    let awaiting_item = &awaiting_inbox["missions"][0];
    let awaiting_conversation = conversation(awaiting_item, conversation_id.as_str());
    assert_eq!(awaiting_item["disposition"], "awaiting_lead");
    assert_eq!(
        awaiting_item["next_actions"],
        serde_json::json!(["mission send"])
    );
    assert_eq!(awaiting_conversation["lifecycle"], "awaiting_lead");
    assert_eq!(
        awaiting_conversation["legal_actions"],
        serde_json::json!(["mission send"])
    );
    assert_eq!(
        awaiting_conversation["runtime_resume_mode"],
        "native_session"
    );
    let awaiting_human = stdout(cli_output(dir.path(), &["mission", "inbox"]));
    assert!(awaiting_human.contains("awaiting lead feedback"));
    assert!(awaiting_human.contains("next: mission send"));

    let send = Cli::try_parse_from([
        "lionclaw",
        "mission",
        "send",
        "--mission-id",
        mission.as_str(),
        "--to",
        conversation_id.as_str(),
        "--commit",
        BASE_SHA,
        "--repo",
        dir.path().to_str().unwrap(),
        "queued lead message",
    ])
    .unwrap();
    cli::run(send).await.unwrap();

    let status_json: serde_json::Value = serde_json::from_str(&stdout(cli_output(
        dir.path(),
        &["mission", "status", mission.as_str(), "--json"],
    )))
    .unwrap();
    let report_json: serde_json::Value = serde_json::from_str(&stdout(cli_output(
        dir.path(),
        &["mission", "report", mission.as_str(), "--json"],
    )))
    .unwrap();
    let store = MissionStore::open(dir.path()).await.unwrap();
    let authoritative = store.require_state(&mission).await.unwrap();
    let folded_conversation = &authoritative.conversations[&conversation_id];
    assert_eq!(folded_conversation.queued.len(), 1);
    assert_eq!(folded_conversation.queued[0].body, "queued lead message");
    assert_eq!(folded_conversation.queued[0].marker, DeliveryMarker::Queued);
    let projections = [
        conversation(&status_json, conversation_id.as_str()),
        conversation(&report_json, conversation_id.as_str()),
    ];
    for projected in projections {
        assert_eq!(projected["lifecycle"], "ready");
        assert_eq!(
            projected["queued_messages"][0]["body"],
            "queued lead message"
        );
        assert_eq!(
            projected["queued_messages"][0]["references"],
            serde_json::to_value([MessageReference::ReachableCommit {
                sha: BASE_SHA.into()
            }])
            .unwrap()
        );
        assert_eq!(projected["queued_messages"][0]["marker"], "queued");
        assert_eq!(projected["runtime_resume_mode"], "native_session");
        assert_eq!(
            projected["legal_actions"],
            serde_json::json!(["mission advance", "mission send"])
        );
        let response = projected["final_response"].as_str().unwrap();
        assert!(response.starts_with("lead checkpoint "));
        assert!(response.len() <= lionclaw::model::MAX_FINAL_RESPONSE_BYTES as usize);
    }
    assert_eq!(status_json["disposition"], "ready");
    assert_eq!(
        status_json["next_actions"],
        serde_json::json!(["mission advance"])
    );
    assert_eq!(status_json["activity"], serde_json::Value::Null);

    let human_status = stdout(cli_output(
        dir.path(),
        &["mission", "status", mission.as_str()],
    ));
    let human_report = stdout(cli_output(
        dir.path(),
        &["mission", "report", mission.as_str()],
    ));
    for human in [&human_status, &human_report] {
        assert!(human.contains(conversation_id.as_str()));
        assert!(human.contains("lifecycle=ready"));
        assert!(human.contains("queued=1"));
        assert!(human.contains("resume=native_session"));
        assert!(human.contains("final response: lead checkpoint"));
    }
    assert!(human_status.contains("next: mission advance"));

    // A malformed delivered turn leaves the same queued message in the fold,
    // now with an honest retry marker. Exercise a second CLI snapshot instead
    // of manufacturing a ConversationState with that marker.
    let failed = h.engine.advance(&mission).await.unwrap();
    let uncertain = &failed.state.conversations[&conversation_id];
    assert_eq!(uncertain.queued.len(), 1);
    assert_eq!(
        uncertain.queued[0].marker,
        DeliveryMarker::PreviouslyDelivered
    );
    let retry_status: serde_json::Value = serde_json::from_str(&stdout(cli_output(
        dir.path(),
        &["mission", "status", mission.as_str(), "--json"],
    )))
    .unwrap();
    let retry_report: serde_json::Value = serde_json::from_str(&stdout(cli_output(
        dir.path(),
        &["mission", "report", mission.as_str(), "--json"],
    )))
    .unwrap();
    for root in [&retry_status, &retry_report] {
        let projected = conversation(root, conversation_id.as_str());
        assert_eq!(
            projected["queued_messages"][0]["marker"],
            "previously_delivered"
        );
        assert_eq!(
            projected["queued_messages"][0]["references"][0]["sha"],
            BASE_SHA
        );
        assert_eq!(
            projected["legal_actions"],
            match projected["lifecycle"].as_str().unwrap() {
                "ready" | "reworking_invalid_handoff" => {
                    serde_json::json!(["mission advance", "mission send"])
                }
                lifecycle => panic!("unexpected retry lifecycle {lifecycle}"),
            }
        );
    }

    let events = store.load(&mission).await.unwrap();
    assert!(events
        .iter()
        .any(|event| matches!(event.event, MissionEvent::MessageSent { .. })));
    let database = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}",
        dir.path().join(".lionclaw/mission.db").display()
    ))
    .await
    .unwrap();
    let competing: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND \
         (name LIKE '%conversation%' OR name LIKE '%inbox%' OR name LIKE '%view%')",
    )
    .fetch_all(&database)
    .await
    .unwrap();
    assert!(
        competing.is_empty(),
        "competing projection tables: {competing:?}"
    );
}
