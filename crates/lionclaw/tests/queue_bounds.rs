//! Production and replay proof for the per-conversation retained-message bound.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use clap::Parser;
use common::{
    approve_plan, covered_requirement, fault_append_events, harness, initialize_repository,
    proposal, simple_plan, test_mission_type, BASE_SHA,
};
use lionclaw::cli::{self, Cli};
use lionclaw::engine::{ConversationQueueFull, Engine, EngineServices};
use lionclaw::model::{
    fold, Assertion, AssertionId, ConversationLifecycle, Handoff, MissionEvent, OracleName,
    OutputSemantics, PayloadRef, Plan, RoleInstanceId, RuntimeConfigurationEvidence, Task, TaskId,
    TaskStatus, ValidationItem, MAX_QUEUED_MESSAGES_PER_CONVERSATION, REDUCER_VERSION,
};
use lionclaw::ports::{RoleRunner, RoleTurnOutcome, RoleTurnRequest};
use lionclaw::store::{AppendError, MissionStore, NewEvent};
use lionclaw::testing::{MockClock, MockOracleRunner, MockRoleRunner, NoopEffectCleaner};
use lionclaw_runtime_api::TypedFailure;

struct GatedRoleRunner {
    blocked_calls: Vec<usize>,
    calls: AtomicUsize,
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
    prompts: Mutex<Vec<String>>,
}

impl GatedRoleRunner {
    fn new(blocked_calls: impl Into<Vec<usize>>) -> Self {
        Self {
            blocked_calls: blocked_calls.into(),
            calls: AtomicUsize::new(0),
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
            prompts: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl RoleRunner for GatedRoleRunner {
    async fn run(&self, request: RoleTurnRequest) -> Result<RoleTurnOutcome, TypedFailure> {
        if request.role.output == OutputSemantics::ProducesArtifact {
            lionclaw::testing::prepare_test_workspace(&request).await?;
        }
        self.prompts.lock().unwrap().push(request.prompt.clone());
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.blocked_calls.contains(&call) {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        match request.role.output {
            OutputSemantics::EmitsVerdict => {
                let report = PayloadRef::inline("queue boundary reviewed");
                let runtime_configuration = RuntimeConfigurationEvidence::default();
                let final_response = "queue boundary reviewed";
                Ok(RoleTurnOutcome {
                    handoff: Some(Handoff::Validate {
                        done: true,
                        report,
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
                    runtime_configuration,
                    runtime_usage: Default::default(),
                    final_response: final_response.into(),
                })
            }
            OutputSemantics::ProducesArtifact | OutputSemantics::ProducesReport => {
                let runtime_configuration = RuntimeConfigurationEvidence::default();
                let final_response = "question";
                Ok(RoleTurnOutcome {
                    handoff: None,
                    artifact: None,
                    runtime_configuration,
                    runtime_usage: Default::default(),
                    final_response: final_response.into(),
                })
            }
            OutputSemantics::ProposesPlan | OutputSemantics::EmitsGapVerdict => {
                panic!("unexpected role output in queue-bound proof")
            }
        }
    }
}

async fn gated_engine(workspace: &std::path::Path, runner: Arc<GatedRoleRunner>) -> Arc<Engine> {
    initialize_repository(workspace);
    Arc::new(Engine::new(
        MissionStore::open(workspace).await.unwrap(),
        test_mission_type(),
        "localhost/lionclaw-runtime-dev:v1".into(),
        EngineServices::new(
            runner,
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    ))
}

fn work_and_independent_validator_plan() -> Plan {
    let assertion = AssertionId::new("QUEUE-BOUND").unwrap();
    Plan {
        requirements: vec![covered_requirement("BOUNDED-MESSAGES", "QUEUE-BOUND")],
        assertions: vec![Assertion {
            id: assertion.clone(),
            prose: "conversation messages remain bounded".into(),
            oracle: Some(OracleName::new("cargo-test").unwrap()),
        }],
        tasks: vec![
            Task {
                id: TaskId::new("writer").unwrap(),
                body: "ask for exact input".into(),
                targets: vec![assertion.clone()],
                depends_on: vec![],
            },
            Task {
                id: TaskId::new("validator").unwrap(),
                body: "validate the independent queue boundary".into(),
                targets: vec![],
                depends_on: vec![],
            },
            Task {
                id: TaskId::new("finalize").unwrap(),
                body: "combine both independently completed changes".into(),
                targets: vec![],
                depends_on: vec![
                    TaskId::new("writer").unwrap(),
                    TaskId::new("validator").unwrap(),
                ],
            },
        ],
    }
}

fn send_cli(
    repo: &std::path::Path,
    mission: &lionclaw::model::MissionId,
    conversation: &RoleInstanceId,
    body: &str,
) -> Cli {
    Cli::try_parse_from([
        "lionclaw",
        "mission",
        "send",
        "--mission-id",
        mission.as_str(),
        "--to",
        conversation.as_str(),
        "--repo",
        repo.to_str().unwrap(),
        body,
    ])
    .unwrap()
}

fn send_all_with_commit_cli(
    repo: &std::path::Path,
    mission: &lionclaw::model::MissionId,
    commit: &str,
    body: &str,
) -> Cli {
    Cli::try_parse_from(vec![
        "lionclaw",
        "mission",
        "send",
        "--mission-id",
        mission.as_str(),
        "--all",
        "--commit",
        commit,
        "--repo",
        repo.to_str().unwrap(),
        body,
    ])
    .unwrap()
}

fn cli_output(repo: &std::path::Path, args: &[&str]) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_lionclaw"))
        .args(args)
        .arg("--repo")
        .arg(repo)
        .output()
        .unwrap()
}

#[tokio::test]
async fn queued_input_after_a_running_boundary_resumes_the_same_generation() {
    let dir = tempfile::tempdir().unwrap();
    let runner = Arc::new(GatedRoleRunner::new(vec![0, 1]));
    let engine = gated_engine(dir.path(), runner.clone()).await;
    let mission = engine
        .create_mission(dir.path().to_str().unwrap(), "late bounded input", BASE_SHA)
        .await
        .unwrap();
    engine
        .propose_plan(&mission, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&engine, &mission).await;
    let driver = tokio::spawn({
        let engine = engine.clone();
        let mission = mission.clone();
        async move { engine.advance(&mission).await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), runner.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();

    let store = MissionStore::open(dir.path()).await.unwrap();
    let running = store.require_state(&mission).await.unwrap();
    let (conversation_id, conversation) = running.conversations.iter().next().unwrap();
    assert_eq!(conversation.lifecycle, ConversationLifecycle::Running);
    let conversation_id = conversation_id.clone();
    for index in 0..MAX_QUEUED_MESSAGES_PER_CONVERSATION {
        cli::run(send_cli(
            dir.path(),
            &mission,
            &conversation_id,
            &format!("late message {index:02}"),
        ))
        .await
        .unwrap();
    }
    let full = store.require_state(&mission).await.unwrap();
    assert_eq!(
        full.conversations[&conversation_id].lifecycle,
        ConversationLifecycle::Running
    );
    assert_eq!(
        full.conversations[&conversation_id].queued.len(),
        MAX_QUEUED_MESSAGES_PER_CONVERSATION
    );
    fault_append_events(
        dir.path(),
        &mission,
        full.head,
        &[NewEvent::new(MissionEvent::MessageSent {
            recipients: vec![conversation_id.clone()],
            body: "forged late overflow".into(),
            references: vec![],
        })],
        31_100,
    )
    .await;
    let snapshot = store.rebuild_cursors(&mission, 31_101).await.unwrap();
    let snapshot_head = snapshot.head;

    runner.release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(5), runner.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let resumed = store.require_state(&mission).await.unwrap();
    assert_eq!(resumed.conversations.len(), 1);
    assert_eq!(
        resumed.conversations[&conversation_id].role_instance,
        conversation_id
    );
    assert_eq!(
        resumed.conversations[&conversation_id].lifecycle,
        ConversationLifecycle::Running
    );
    assert_eq!(
        resumed.tasks[&TaskId::new("fix").unwrap()].status,
        TaskStatus::Running
    );
    {
        let prompts = runner.prompts.lock().unwrap();
        assert_eq!(prompts.len(), 2);
        for index in 0..MAX_QUEUED_MESSAGES_PER_CONVERSATION {
            let body = format!("late message {index:02}");
            assert_eq!(prompts[1].matches(&body).count(), 1);
        }
        assert!(!prompts[1].contains("forged late overflow"));
    }

    runner.release.add_permits(1);
    let settled = driver.await.unwrap().unwrap().state;
    assert!(settled.head > snapshot_head);
    assert_eq!(
        settled.conversations[&conversation_id].lifecycle,
        ConversationLifecycle::AwaitingLead
    );
    assert!(settled.conversations[&conversation_id].queued.is_empty());
    let replayed = fold(store.load(&mission).await.unwrap()).unwrap();
    assert_eq!(settled, replayed);
    assert_eq!(
        store.load_state_snapshotted(&mission).await.unwrap(),
        Some(replayed)
    );
}

#[tokio::test]
async fn production_broadcast_is_atomic_when_one_live_conversation_is_full() {
    let dir = tempfile::tempdir().unwrap();
    let runner = Arc::new(GatedRoleRunner::new(vec![1]));
    let engine = gated_engine(dir.path(), runner.clone()).await;
    let mission = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "atomic bounded broadcast",
            BASE_SHA,
        )
        .await
        .unwrap();
    let mut mission_proposal = proposal(0, work_and_independent_validator_plan());
    let reporter_id = RoleInstanceId::new("second-implementer").unwrap();
    let team = mission_proposal.team.as_mut().unwrap();
    team.roles.insert(
        reporter_id.clone(),
        common::role("second-implementer", OutputSemantics::ProducesArtifact),
    );
    team.task_assignments
        .insert(TaskId::new("validator").unwrap(), reporter_id.clone());
    engine
        .propose_plan(&mission, mission_proposal)
        .await
        .unwrap();
    approve_plan(&engine, &mission).await;
    let driver = tokio::spawn({
        let engine = engine.clone();
        let mission = mission.clone();
        async move { engine.advance(&mission).await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), runner.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();

    let store = MissionStore::open(dir.path()).await.unwrap();
    let active = store.require_state(&mission).await.unwrap();
    assert_eq!(active.conversations.len(), 2);
    let writer_id = RoleInstanceId::new("implementer").unwrap();
    let validator_id = reporter_id;
    assert!(active.conversations.contains_key(&writer_id));
    assert!(active.conversations.contains_key(&validator_id));
    assert!(
        [&writer_id, &validator_id].iter().any(|role_id| {
            active.conversations[*role_id].lifecycle == ConversationLifecycle::Running
        }),
        "one independent task should remain running while the other is live"
    );
    for index in 0..MAX_QUEUED_MESSAGES_PER_CONVERSATION {
        cli::run(send_cli(
            dir.path(),
            &mission,
            &writer_id,
            &format!("writer queue {index}"),
        ))
        .await
        .unwrap();
    }
    let events_before = store.load(&mission).await.unwrap();
    let state_before = store.require_state(&mission).await.unwrap();
    let foreign_commit = "f".repeat(40);
    let rejection = cli::run(send_all_with_commit_cli(
        dir.path(),
        &mission,
        &foreign_commit,
        "broadcast must not partially route",
    ))
    .await
    .unwrap_err();
    assert_eq!(
        rejection.downcast_ref::<ConversationQueueFull>(),
        Some(&ConversationQueueFull {
            role_instance: writer_id.clone(),
            retained_messages: MAX_QUEUED_MESSAGES_PER_CONVERSATION,
            limit: MAX_QUEUED_MESSAGES_PER_CONVERSATION,
        })
    );
    assert_eq!(store.load(&mission).await.unwrap(), events_before);
    assert_eq!(store.require_state(&mission).await.unwrap(), state_before);
    assert!(state_before.conversations[&validator_id].queued.is_empty());
    assert_eq!(
        state_before.conversations[&writer_id].queued.len(),
        MAX_QUEUED_MESSAGES_PER_CONVERSATION
    );

    runner.release.add_permits(1);
    driver.await.unwrap().unwrap();
}

#[tokio::test]
async fn exact_queue_capacity_is_typed_atomic_replay_safe_and_recoverable() {
    let dir = tempfile::tempdir().unwrap();
    let runner = MockRoleRunner::new(Box::new(|request| {
        if request.role.output == OutputSemantics::EmitsVerdict {
            return Ok(lionclaw::ports::RoleTurnOutcome {
                handoff: Some(Handoff::Validate {
                    done: true,
                    report: PayloadRef::inline("queue boundary reviewed"),
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
                runtime_configuration: Default::default(),
                runtime_usage: Default::default(),
                final_response: "reviewed".into(),
            });
        }
        Ok(lionclaw::ports::RoleTurnOutcome {
            handoff: None,
            artifact: None,
            runtime_configuration: Default::default(),
            runtime_usage: Default::default(),
            final_response: "question".into(),
        })
    }));
    let h = harness(dir.path(), runner, MockOracleRunner::exiting(0)).await;
    let mission = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "bound messages", BASE_SHA)
        .await
        .unwrap();
    h.engine
        .propose_plan(&mission, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&h.engine, &mission).await;
    let awaiting = h.engine.advance(&mission).await.unwrap().state;
    let conversation_id = awaiting.conversations.keys().next().unwrap().clone();
    let store = MissionStore::open(dir.path()).await.unwrap();

    for index in 0..MAX_QUEUED_MESSAGES_PER_CONVERSATION {
        cli::run(send_cli(
            dir.path(),
            &mission,
            &conversation_id,
            &format!("bounded message {index}"),
        ))
        .await
        .unwrap();
    }
    let full = store.require_state(&mission).await.unwrap();
    let conversation = &full.conversations[&conversation_id];
    assert_eq!(
        conversation.queued.len(),
        MAX_QUEUED_MESSAGES_PER_CONVERSATION
    );
    assert_eq!(conversation.lifecycle, ConversationLifecycle::Ready);
    assert_eq!(
        full.conversation_legal_actions(&conversation_id),
        vec!["mission advance"]
    );

    // A raw over-cap event advances the log cursor but is fold-inert. Production
    // ingress rejects before append and returns the exact full conversation.
    fault_append_events(
        dir.path(),
        &mission,
        full.head,
        &[NewEvent::new(MissionEvent::MessageSent {
            recipients: vec![conversation_id.clone()],
            body: "forged overflow".into(),
            references: vec![],
        })],
        31_000,
    )
    .await;
    let forged = store.require_state(&mission).await.unwrap();
    assert_eq!(forged.head, full.head + 1);
    assert_eq!(forged.conversations[&conversation_id], *conversation);
    let events_before_rejection = store.load(&mission).await.unwrap();
    let rejection = cli::run(send_cli(
        dir.path(),
        &mission,
        &conversation_id,
        "typed overflow",
    ))
    .await
    .unwrap_err();
    assert_eq!(
        rejection.downcast_ref::<ConversationQueueFull>(),
        Some(&ConversationQueueFull {
            role_instance: conversation_id.clone(),
            retained_messages: MAX_QUEUED_MESSAGES_PER_CONVERSATION,
            limit: MAX_QUEUED_MESSAGES_PER_CONVERSATION,
        })
    );
    assert_eq!(store.load(&mission).await.unwrap(), events_before_rejection);
    assert_eq!(store.require_state(&mission).await.unwrap(), forged);

    let json = cli_output(
        dir.path(),
        &["mission", "status", mission.as_str(), "--json"],
    );
    assert!(json.status.success());
    let json: serde_json::Value = serde_json::from_slice(&json.stdout).unwrap();
    assert_eq!(
        json["conversations"][0]["queued_messages"]
            .as_array()
            .unwrap()
            .len(),
        MAX_QUEUED_MESSAGES_PER_CONVERSATION
    );
    assert_eq!(
        json["conversations"][0]["legal_actions"],
        serde_json::json!(["mission advance"])
    );
    assert_eq!(
        json["next_actions"],
        serde_json::json!(["mission advance", "mission abort"])
    );
    let report_json = cli_output(
        dir.path(),
        &["mission", "report", mission.as_str(), "--json"],
    );
    assert!(report_json.status.success());
    let report_json: serde_json::Value = serde_json::from_slice(&report_json.stdout).unwrap();
    assert_eq!(report_json["next_actions"], json["next_actions"]);
    let inbox_json = cli_output(dir.path(), &["mission", "inbox", "--json"]);
    assert!(inbox_json.status.success());
    let inbox_json: serde_json::Value = serde_json::from_slice(&inbox_json.stdout).unwrap();
    assert!(inbox_json["missions"]
        .as_array()
        .unwrap()
        .iter()
        .all(|record| record["mission_id"] != mission.as_str()));
    for args in [
        vec!["mission", "status", mission.as_str()],
        vec!["mission", "report", mission.as_str()],
        vec!["mission", "inbox"],
    ] {
        let human = cli_output(dir.path(), &args);
        assert!(human.status.success());
        let human = String::from_utf8(human.stdout).unwrap();
        assert!(!human.contains("mission send"));
        if args[1] == "status" {
            assert!(human.contains(&format!("queued={MAX_QUEUED_MESSAGES_PER_CONVERSATION}")));
            assert!(human.contains("legal_actions=mission advance"));
        }
    }

    let snapshot = store.rebuild_cursors(&mission, 31_001).await.unwrap();
    let snapshot_head = snapshot.head;
    assert_eq!(
        store.snapshot_meta(&mission).await.unwrap(),
        Some((snapshot_head, REDUCER_VERSION))
    );
    h.engine.advance(&mission).await.unwrap();
    let settled = store.require_state(&mission).await.unwrap();
    assert!(settled.conversations[&conversation_id].queued.is_empty());
    assert_eq!(
        settled.conversations[&conversation_id].lifecycle,
        ConversationLifecycle::AwaitingLead
    );
    cli::run(send_cli(
        dir.path(),
        &mission,
        &conversation_id,
        "capacity restored after exact settlement",
    ))
    .await
    .unwrap();
    let final_state = store.require_state(&mission).await.unwrap();
    assert!(final_state.head > snapshot_head);
    assert_eq!(final_state.conversations[&conversation_id].queued.len(), 1);

    for index in 1..(MAX_QUEUED_MESSAGES_PER_CONVERSATION - 1) {
        cli::run(send_cli(
            dir.path(),
            &mission,
            &conversation_id,
            &format!("race fill {index}"),
        ))
        .await
        .unwrap();
    }
    let race_before = store.load(&mission).await.unwrap();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
    let mut writers = Vec::new();
    for body in ["racing sender one", "racing sender two"] {
        let barrier = barrier.clone();
        let cli = send_cli(dir.path(), &mission, &conversation_id, body);
        writers.push(tokio::spawn(async move {
            barrier.wait().await;
            cli::run(cli).await
        }));
    }
    let mut admitted = 0;
    for writer in writers {
        match writer.await.unwrap() {
            Ok(_) => admitted += 1,
            Err(error)
                if error.downcast_ref::<ConversationQueueFull>().is_some()
                    || matches!(
                        error.downcast_ref::<AppendError>(),
                        Some(AppendError::Conflict { .. })
                    ) => {}
            Err(error) => panic!("unexpected concurrent send failure: {error:#}"),
        }
    }
    assert_eq!(admitted, 1);
    assert_eq!(
        store.load(&mission).await.unwrap().len(),
        race_before.len() + 1
    );
    let final_state = store.require_state(&mission).await.unwrap();
    assert_eq!(
        final_state.conversations[&conversation_id].queued.len(),
        MAX_QUEUED_MESSAGES_PER_CONVERSATION
    );
    let replayed = fold(store.load(&mission).await.unwrap()).unwrap();
    assert_eq!(final_state, replayed);
    assert_eq!(
        store.load_state_snapshotted(&mission).await.unwrap(),
        Some(replayed)
    );

    let database = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}",
        dir.path().join(".lionclaw/mission.db").display()
    ))
    .await
    .unwrap();
    sqlx::query("UPDATE mission_snapshots SET reducer_version = 30 WHERE mission_id = ?1")
        .bind(mission.as_str())
        .execute(&database)
        .await
        .unwrap();
    assert_eq!(store.require_state(&mission).await.unwrap(), final_state);
    assert_eq!(
        store.rebuild_cursors(&mission, 31_002).await.unwrap(),
        final_state
    );
    assert_eq!(
        store.snapshot_meta(&mission).await.unwrap(),
        Some((final_state.head, REDUCER_VERSION))
    );
}
