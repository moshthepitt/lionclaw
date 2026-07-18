//! Production ingress coverage for sender-free conversation routing.

mod common;

use std::sync::Arc;

use clap::Parser;
use common::{approve_plan, covered_requirement, harness, proposal, BASE_SHA};
use lionclaw::cli::{self, Cli};
use lionclaw::model::{
    Assertion, AssertionId, ConversationId, MissionEvent, OracleName, Plan, RoleName, Task, TaskId,
    TaskKind, TaskNamespace,
};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};

fn send_cli(repo: &std::path::Path, mission: &str, args: &[&str]) -> Cli {
    let mut argv = vec!["lionclaw", "mission", "send", "--mission-id", mission];
    argv.extend_from_slice(args);
    argv.extend(["--repo", repo.to_str().unwrap(), "feedback"]);
    Cli::try_parse_from(argv).expect("production send parser")
}

async fn event_count(store: &MissionStore, mission: &lionclaw::model::MissionId) -> usize {
    store.load(mission).await.unwrap().len()
}

#[tokio::test]
async fn production_cli_routes_atomically_to_exact_current_role_instances() {
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
    let (mission, alpha_conversation) = loop {
        let mission = h
            .engine
            .create_mission(dir.path().to_str().unwrap(), "route", BASE_SHA)
            .await
            .unwrap();
        let conversation =
            ConversationId::for_role_instance(&mission, TaskNamespace::Execution, &alpha, &role, 1);
        if conversation
            .as_str()
            .starts_with(|character: char| ('b'..='f').contains(&character))
        {
            break (mission, conversation);
        }
    };
    // A selector can legitimately match one conversation id and a different
    // task name. This is the otherwise easy-to-miss ambiguous-sugar case.
    let collision = TaskId::new(alpha_conversation.as_str()).unwrap();
    let task_ids = [alpha.clone(), TaskId::new("beta").unwrap(), collision];
    let tasks = task_ids
        .iter()
        .cloned()
        .zip(["ROUTE-A", "ROUTE-B", "ROUTE-C"])
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
                    requirements: [
                        ("ROUTING-A", "ROUTE-A"),
                        ("ROUTING-B", "ROUTE-B"),
                        ("ROUTING-C", "ROUTE-C"),
                    ]
                    .into_iter()
                    .map(|(requirement, assertion)| covered_requirement(requirement, assertion))
                    .collect(),
                    assertions: ["ROUTE-A", "ROUTE-B", "ROUTE-C"]
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
        let before = event_count(&store, &mission).await;
        assert!(cli::run(send_cli(dir.path(), mission.as_str(), &args))
            .await
            .is_err());
        assert_eq!(event_count(&store, &mission).await, before);
    }
    for epoch in [0, 2] {
        let id = ConversationId::for_role_instance(
            &mission,
            TaskNamespace::Execution,
            &alpha,
            &role,
            epoch,
        );
        let before = event_count(&store, &mission).await;
        assert!(cli::run(send_cli(
            dir.path(),
            mission.as_str(),
            &["--to", id.as_str()]
        ))
        .await
        .is_err());
        assert_eq!(event_count(&store, &mission).await, before);
    }

    // Both task sugar and --all resolve through the same current-instance set.
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
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0].len(), 1, "routing is one atomic event");
    assert_eq!(sent[1].len(), 1, "--all snapshots every current instance");
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
}
