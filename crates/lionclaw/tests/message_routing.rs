//! Production ingress coverage for sender-free conversation routing.

mod common;

use std::sync::Arc;

use clap::Parser;
use common::{approve_plan, covered_requirement, harness, proposal, BASE_SHA};
use lionclaw::cli::{self, Cli};
use lionclaw::model::{
    fold, Assertion, AssertionId, ConversationId, ConversationLifecycle, DeliveryMarker,
    MissionEvent, OracleName, Plan, RoleName, Task, TaskId, TaskKind, TaskNamespace,
};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};

fn send_cli(repo: &std::path::Path, mission: &str, args: &[&str]) -> Cli {
    let mut argv = vec!["lionclaw", "mission", "send", "--mission-id", mission];
    argv.extend_from_slice(args);
    argv.extend(["--repo", repo.to_str().unwrap(), "feedback"]);
    Cli::try_parse_from(argv).expect("production send parser")
}

async fn reject_without_mutation(
    store: &MissionStore,
    mission: &lionclaw::model::MissionId,
    command: Cli,
) {
    let events = store.load(mission).await.unwrap();
    let state = store.require_state(mission).await.unwrap();
    assert!(cli::run(command).await.is_err());
    assert_eq!(store.load(mission).await.unwrap(), events);
    assert_eq!(store.require_state(mission).await.unwrap(), state);
}

#[tokio::test]
async fn production_cli_routes_atomically_only_to_explicit_live_conversations() {
    let dir = tempfile::tempdir().unwrap();
    let runner = MockRoleRunner::new(Box::new(|request| {
        request
            .updates
            .try_send(lionclaw::ports::RoleRunUpdate::WorkspacePrepared {
                base_sha: request.base_sha.clone(),
                assignment_epoch: request.assignment_epoch,
            })
            .unwrap();
        Ok(lionclaw::ports::RoleRunOutcome {
            handoff: None,
            artifact: None,
            runtime_configuration: Default::default(),
            final_response: "question".into(),
        })
    }));
    let h = harness(dir.path(), runner, MockOracleRunner::exiting(0)).await;
    let role = RoleName::new("implementer").unwrap();
    let alpha = TaskId::new("alpha").unwrap();
    let mission = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "route", BASE_SHA)
        .await
        .unwrap();
    let task_ids = [alpha.clone(), TaskId::new("beta").unwrap()];
    let tasks = task_ids
        .iter()
        .cloned()
        .zip(["ROUTE-A", "ROUTE-B"])
        .map(|(id, assertion)| Task {
            id,
            kind: TaskKind::Work,
            body: "ask".into(),
            targets: vec![AssertionId::new(assertion).unwrap()],
            role: Some(role.clone()),
            depends_on: vec![],
        })
        .collect();
    h.engine
        .propose_plan(
            &mission,
            proposal(
                0,
                Plan {
                    requirements: [("ROUTING-A", "ROUTE-A"), ("ROUTING-B", "ROUTE-B")]
                        .into_iter()
                        .map(|(requirement, assertion)| covered_requirement(requirement, assertion))
                        .collect(),
                    assertions: ["ROUTE-A", "ROUTE-B"]
                        .into_iter()
                        .map(|id| Assertion {
                            id: AssertionId::new(id).unwrap(),
                            prose: "routes".into(),
                            oracle: Some(OracleName::new("cargo-test").unwrap()),
                        })
                        .collect(),
                    tasks,
                },
            ),
        )
        .await
        .unwrap();
    approve_plan(&h.engine, &mission).await;
    let checkpoint = h.engine.advance(&mission).await.unwrap().state;
    assert_eq!(checkpoint.conversations.len(), 1);
    let current_conversation = checkpoint.conversations.keys().next().unwrap().clone();
    let store = MissionStore::open(dir.path()).await.unwrap();

    // Rejections traverse parser + dispatch + load/fold and append nothing.
    let rejected = [
        vec!["--to", "alpha", "--to", "alpha"],
        vec!["--to", "alpha", "--to", "missing"],
    ];
    for args in rejected {
        reject_without_mutation(
            &store,
            &mission,
            send_cli(dir.path(), mission.as_str(), &args),
        )
        .await;
    }
    assert!(Cli::try_parse_from([
        "lionclaw",
        "mission",
        "send",
        "--mission-id",
        mission.as_str(),
        "--all",
        "--to",
        "alpha",
        "--repo",
        dir.path().to_str().unwrap(),
        "feedback",
    ])
    .is_err());
    for epoch in [0, 2] {
        let id = ConversationId::for_role_instance(
            &mission,
            TaskNamespace::Execution,
            &alpha,
            &role,
            epoch,
        );
        reject_without_mutation(
            &store,
            &mission,
            send_cli(dir.path(), mission.as_str(), &["--to", id.as_str()]),
        )
        .await;
    }

    // Unique task sugar, an explicit id, and --all resolve through the same
    // explicit live-conversation snapshot.
    cli::run(send_cli(dir.path(), mission.as_str(), &["--to", "alpha"]))
        .await
        .unwrap();
    cli::run(send_cli(
        dir.path(),
        mission.as_str(),
        &["--to", current_conversation.as_str()],
    ))
    .await
    .unwrap();
    cli::run(send_cli(dir.path(), mission.as_str(), &["--all"]))
        .await
        .unwrap();

    let events = store.load(&mission).await.unwrap();
    let sent: Vec<_> = events
        .iter()
        .filter_map(|event| match &event.event {
            MissionEvent::MessageSent { recipients, .. } => Some(recipients),
            _ => None,
        })
        .collect();
    assert_eq!(sent.len(), 3);
    assert_eq!(sent[0].len(), 1, "routing is one atomic event");
    assert_eq!(sent[1].len(), 1, "explicit id routes exactly once");
    assert_eq!(sent[2].len(), 1, "--all snapshots every current instance");
    assert!(sent
        .iter()
        .flat_map(|recipients| recipients.iter())
        .all(|recipient| recipient.assignment_epoch == 1));
    assert!(sent
        .iter()
        .flat_map(|recipients| recipients.iter())
        .all(|recipient| {
            recipient.conversation_id
                == ConversationId::for_role_instance(
                    &mission,
                    recipient.namespace,
                    &recipient.task_id,
                    &recipient.role,
                    recipient.assignment_epoch,
                )
        }));

    // Concurrent production CLI writers contend on MissionStore CAS. Each
    // successful command contributes exactly one whole MessageSent event;
    // conflicts contribute none, so partial routing is unrepresentable.
    let race_before = store.load(&mission).await.unwrap();
    let race_state_before = store.require_state(&mission).await.unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(16));
    let mut writers = Vec::new();
    for _ in 0..16 {
        let barrier = barrier.clone();
        let cli = send_cli(dir.path(), mission.as_str(), &["--all"]);
        writers.push(tokio::spawn(async move {
            barrier.wait().await;
            cli::run(cli).await
        }));
    }
    let mut ok = 0;
    let mut conflicts = 0;
    for writer in writers {
        match writer.await.unwrap() {
            Ok(_) => ok += 1,
            Err(error) if error.to_string().contains("append conflict") => conflicts += 1,
            Err(error) => panic!("unexpected send failure: {error:#}"),
        }
    }
    assert!(
        ok > 0 && conflicts > 0,
        "the race must exercise both CAS outcomes"
    );
    let after = store.load(&mission).await.unwrap();
    assert_eq!(after.len(), race_before.len() + ok);
    let race_events: Vec<_> = after
        .iter()
        .rev()
        .take(ok)
        .map(|event| &event.event)
        .collect();
    assert_eq!(race_events.len(), ok);
    assert!(race_events.iter().all(|event| matches!(
        event,
        MissionEvent::MessageSent { recipients, .. } if recipients.len() == 1
    )));
    let race_state_after = store.require_state(&mission).await.unwrap();
    let before_queued = &race_state_before.conversations[&current_conversation].queued;
    let after_queued = &race_state_after.conversations[&current_conversation].queued;
    assert_eq!(after_queued.len(), before_queued.len() + ok);
    assert_eq!(race_state_after.head, race_state_before.head + ok as u64);

    // Accepting a real replacement plan retires the old AwaitingLead
    // conversation before the next role is dispatched. Its queued messages
    // become explicit tombstones and neither an exact stale target nor --all
    // may route back into it.
    let replacement = TaskId::new("replacement").unwrap();
    h.engine
        .propose_plan(
            &mission,
            proposal(
                1,
                Plan {
                    requirements: [("ROUTING-A", "ROUTE-A"), ("ROUTING-B", "ROUTE-B")]
                        .into_iter()
                        .map(|(requirement, assertion)| covered_requirement(requirement, assertion))
                        .collect(),
                    assertions: ["ROUTE-A", "ROUTE-B"]
                        .into_iter()
                        .map(|id| Assertion {
                            id: AssertionId::new(id).unwrap(),
                            prose: "routes".into(),
                            oracle: Some(OracleName::new("cargo-test").unwrap()),
                        })
                        .collect(),
                    tasks: vec![Task {
                        id: replacement.clone(),
                        kind: TaskKind::Work,
                        body: "ask replacement".into(),
                        targets: vec![
                            AssertionId::new("ROUTE-A").unwrap(),
                            AssertionId::new("ROUTE-B").unwrap(),
                        ],
                        role: Some(role.clone()),
                        depends_on: vec![],
                    }],
                },
            ),
        )
        .await
        .unwrap();
    approve_plan(&h.engine, &mission).await;

    let retired = store.require_state(&mission).await.unwrap();
    let old = &retired.conversations[&current_conversation];
    assert_eq!(old.lifecycle, ConversationLifecycle::Retired);
    assert!(old.active_delivery.is_none());
    assert!(old
        .queued
        .iter()
        .all(|message| message.marker == DeliveryMarker::Undeliverable));
    assert!(retired
        .conversation_legal_actions(&current_conversation)
        .is_empty());
    reject_without_mutation(
        &store,
        &mission,
        send_cli(
            dir.path(),
            mission.as_str(),
            &["--to", current_conversation.as_str()],
        ),
    )
    .await;

    let replacement_checkpoint = h.engine.advance(&mission).await.unwrap().state;
    let replacement_conversation = replacement_checkpoint
        .conversations
        .iter()
        .find(|(id, conversation)| {
            conversation.task_id == replacement
                && replacement_checkpoint.conversation_is_messageable(id)
        })
        .map(|(id, _)| id.clone())
        .expect("distinct live replacement conversation");
    assert_ne!(replacement_conversation, current_conversation);

    let before_all = store.load(&mission).await.unwrap();
    cli::run(send_cli(dir.path(), mission.as_str(), &["--all"]))
        .await
        .unwrap();
    let after_all = store.load(&mission).await.unwrap();
    assert_eq!(after_all.len(), before_all.len() + 1);
    let MissionEvent::MessageSent { recipients, .. } = &after_all.last().unwrap().event else {
        panic!("--all must append one atomic MessageSent event")
    };
    assert_eq!(recipients.len(), 1);
    assert_eq!(recipients[0].conversation_id, replacement_conversation);
    assert_eq!(recipients[0].task_id, replacement);
    assert!(recipients
        .iter()
        .all(|recipient| recipient.conversation_id != current_conversation));

    let full = fold(after_all).expect("full replay after replacement send");
    let snapshotted = store
        .load_state_snapshotted(&mission)
        .await
        .expect("snapshot replay")
        .expect("mission state");
    assert_eq!(snapshotted, full);
}
