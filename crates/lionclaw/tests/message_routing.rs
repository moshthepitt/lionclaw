//! Production ingress coverage for sender-free conversation routing.

mod common;

use std::sync::Arc;

use clap::Parser;
use common::{approve_plan, covered_requirement, harness, proposal, team, BASE_SHA};
use lionclaw::cli::{self, Cli};
use lionclaw::engine::ConversationQueueFull;
use lionclaw::model::{
    fold, Assertion, AssertionId, ConversationLifecycle, DeliveryMarker, MissionEvent,
    MissionProposal, OracleName, OutputSemantics, Plan, PlanProposal, RoleInstanceId, Task, TaskId,
};
use lionclaw::store::{AppendError, MissionStore};
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
        if request.role.output == OutputSemantics::EmitsVerdict {
            Ok(lionclaw::ports::RoleTurnOutcome {
                handoff: Some(lionclaw::model::Handoff::Validate {
                    done: true,
                    report: lionclaw::model::PayloadRef::inline("reviewed"),
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
                final_response: "reviewed".into(),
            })
        } else {
            Ok(lionclaw::ports::RoleTurnOutcome {
                handoff: None,
                artifact: None,
                prepared_inputs: Vec::new(),
                runtime_configuration: Default::default(),
                runtime_usage: Default::default(),
                final_response: "question".into(),
            })
        }
    }));
    let h = harness(dir.path(), runner, MockOracleRunner::exiting(0)).await;
    let role = RoleInstanceId::new("implementer").unwrap();
    let alpha = TaskId::new("alpha").unwrap();
    let mission = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "route", BASE_SHA)
        .await
        .unwrap();
    let task_ids = [alpha.clone(), TaskId::new("beta").unwrap()];
    let mut tasks: Vec<_> = task_ids
        .iter()
        .cloned()
        .zip(["ROUTE-A", "ROUTE-B"])
        .map(|(id, assertion)| Task {
            id,
            body: "ask".into(),
            targets: vec![AssertionId::new(assertion).unwrap()],
            depends_on: vec![],
        })
        .collect();
    tasks[1].depends_on.push(alpha.clone());
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
    let current_conversation = role.clone();
    assert_eq!(
        checkpoint.conversations[&current_conversation].lifecycle,
        ConversationLifecycle::AwaitingLead
    );
    assert_eq!(
        checkpoint
            .conversations
            .keys()
            .filter(|id| checkpoint.conversation_is_messageable(id))
            .count(),
        1
    );
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
    for id in ["reviewer", "missing-role"] {
        reject_without_mutation(
            &store,
            &mission,
            send_cli(dir.path(), mission.as_str(), &["--to", id]),
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
        .all(|recipient| recipient == &current_conversation));

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
    let mut full = 0;
    for writer in writers {
        match writer.await.unwrap() {
            Ok(_) => ok += 1,
            Err(error)
                if matches!(
                    error.downcast_ref::<AppendError>(),
                    Some(AppendError::Conflict { .. })
                ) =>
            {
                conflicts += 1
            }
            Err(error) if error.downcast_ref::<ConversationQueueFull>().is_some() => full += 1,
            Err(error) => panic!("unexpected send failure: {error:#}"),
        }
    }
    assert!(ok > 0, "at least one exact recipient snapshot must commit");
    assert_eq!(ok + conflicts + full, 16);
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
    assert!(after_queued.len() <= lionclaw::model::MAX_QUEUED_MESSAGES_PER_CONVERSATION);
    assert_eq!(race_state_after.head, race_state_before.head + ok as u64);

    // Accepting a real replacement plan retires the old AwaitingLead
    // conversation before the next role is dispatched. Its queued messages
    // become explicit tombstones and neither an exact stale target nor --all
    // may route back into it.
    let replacement = TaskId::new("replacement").unwrap();
    let replacement_plan = Plan {
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
            body: "ask replacement".into(),
            targets: vec![
                AssertionId::new("ROUTE-A").unwrap(),
                AssertionId::new("ROUTE-B").unwrap(),
            ],
            depends_on: vec![],
        }],
    };
    let replacement_role = RoleInstanceId::new("replacement-worker").unwrap();
    let mut replacement_team = team(2, Some(&replacement_plan), false);
    replacement_team.roles.remove(&role);
    replacement_team.roles.insert(
        replacement_role.clone(),
        common::role("replacement-worker", OutputSemantics::ProducesArtifact),
    );
    replacement_team
        .task_assignments
        .insert(replacement.clone(), replacement_role.clone());
    h.engine
        .propose_plan(
            &mission,
            MissionProposal {
                team: Some(replacement_team),
                plan: Some(PlanProposal {
                    base_revision: 1,
                    requirement_changes: vec![],
                    assertion_supersessions: vec![],
                    plan: replacement_plan,
                }),
            },
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
    assert!(!lionclaw::model::next(&retired).choices.iter().any(
        |choice| matches!(choice, lionclaw::model::Choice::SendMessage { role_instance } if role_instance == &current_conversation)
    ));
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
        .find(|(id, _)| {
            **id == replacement_role && replacement_checkpoint.conversation_is_messageable(id)
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
    assert_eq!(recipients[0], replacement_conversation);
    assert!(recipients
        .iter()
        .all(|recipient| recipient != &current_conversation));

    let full = fold(after_all).expect("full replay after replacement send");
    let snapshotted = store
        .load_state_snapshotted(&mission)
        .await
        .expect("snapshot replay")
        .expect("mission state");
    assert_eq!(snapshotted, full);
}
