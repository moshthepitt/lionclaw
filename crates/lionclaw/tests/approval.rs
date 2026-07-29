//! Slice 4: plan approval parks the mission before any work; an approve
//! decision lets it proceed; an unrelated/invalid decision is
//! refused; universal abort terminates without accepting work.

mod common;

use lionclaw::model::TerminalState;
use std::sync::Arc;

use common::{
    initialize_repository, proposal, review_runner, simple_plan, test_mission_type, BASE_SHA,
};
use lionclaw::engine::{Engine, EngineServices};
use lionclaw::model::{
    Choice, DecisionAction, FinishClass, MissionEvent, MissionProposal, MissionSkill,
};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockClock, MockOracleRunner, NoopEffectCleaner};

async fn gated_engine(dir: &std::path::Path) -> Engine {
    initialize_repository(dir);
    let store = MissionStore::open(dir).await.expect("store");
    Engine::new(
        store,
        test_mission_type(),
        "test-image".to_string(),
        EngineServices::new(
            Arc::new(review_runner(vec![(true, Vec::new())])),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    )
}

#[tokio::test]
async fn terminal_mission_rejects_every_administrative_mutation_without_appending() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = gated_engine(dir.path()).await;
    let mission_id = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "closed administration",
            BASE_SHA,
        )
        .await
        .expect("create");
    engine
        .abort(&mission_id, "administration is closed")
        .await
        .expect("abort");
    let before = engine
        .load_state(&mission_id)
        .await
        .expect("terminal state");

    assert!(engine
        .propose_plan(
            &mission_id,
            MissionProposal {
                plan: None,
                team: Some({
                    let mut team = before.team.clone().expect("team");
                    team.revision += 1;
                    team
                }),
            },
        )
        .await
        .is_err());

    let mut team = before.team.clone().expect("team");
    team.revision += 1;
    team.guidance = Some(lionclaw::model::MissionGuidance::new(
        "terminal guidance must not land",
    ));
    assert!(engine.configure_team(&mission_id, team).await.is_err());

    assert!(engine
        .add_mission_skill(
            &mission_id,
            MissionSkill {
                name: "closed-skill".to_string(),
                digest: "a".repeat(64),
                description: "must not be recorded after closure".to_string(),
            },
        )
        .await
        .is_err());

    let after = engine
        .load_state(&mission_id)
        .await
        .expect("unchanged state");
    assert_eq!(
        after.head, before.head,
        "rejected commands appended an event"
    );
    assert_eq!(after.proposal, before.proposal);
    assert_eq!(after.team, before.team);
    assert_eq!(after.skills, before.skills);
}

#[tokio::test]
async fn every_plan_parks_until_approved_then_proceeds_to_verified() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = gated_engine(dir.path()).await;
    let mission_id = engine
        .create_mission(dir.path().to_str().unwrap(), "gated mission", BASE_SHA)
        .await
        .expect("create");
    engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");

    // Advance parks at the approval gate — no work has run.
    let parked = engine.advance(&mission_id).await.expect("advance");
    assert_eq!(
        common::decision_actions(&parked.state, "plan_proposal:mission"),
        [DecisionAction::Approve, DecisionAction::Revise]
    );
    let inbox = std::process::Command::new(env!("CARGO_BIN_EXE_lionclaw"))
        .args(["mission", "inbox", "--repo"])
        .arg(dir.path())
        .output()
        .expect("render proposal-ready human inbox");
    assert!(
        inbox.status.success(),
        "inbox failed: {}",
        String::from_utf8_lossy(&inbox.stderr)
    );
    let inbox = String::from_utf8(inbox.stdout).expect("UTF-8 inbox");
    assert!(inbox.contains(&format!(
        "mission decide {mission_id} plan_proposal:mission approve --justification JUSTIFICATION"
    )));
    assert!(inbox.contains(&format!(
        "mission decide {mission_id} plan_proposal:mission revise --feedback-file FEEDBACK_FILE"
    )));

    // The model contract rejects an empty reason even when the action itself
    // is legal; callers cannot bypass the CLI's required flag.
    assert!(engine
        .decide(
            &mission_id,
            "plan_proposal:mission",
            DecisionAction::Approve,
            "",
        )
        .await
        .is_err());

    // An invalid decision (retry on the approve item) is refused.
    assert!(engine
        .decide(
            &mission_id,
            "plan_proposal:mission",
            DecisionAction::Retry,
            "",
        )
        .await
        .is_err());
    // A decision on a nonexistent item is refused.
    assert!(engine
        .decide(&mission_id, "node_failed:ghost", DecisionAction::Accept, "",)
        .await
        .is_err());

    // Approve, then drive to the explicit finish gate without losing the mission
    // from operator inboxes. `Finish` is a choice, not an automatic phase.
    engine
        .decide(
            &mission_id,
            "plan_proposal:mission",
            DecisionAction::Approve,
            "ok",
        )
        .await
        .expect("approve");
    let ready = engine
        .advance(&mission_id)
        .await
        .expect("advance to finish");
    assert!(ready
        .next
        .choices
        .iter()
        .any(|choice| matches!(choice, Choice::Finish { .. })));
    let projected = serde_json::to_value(&ready.next).expect("serialize next");
    assert_eq!(
        projected.as_object().unwrap().keys().collect::<Vec<_>>(),
        ["choices", "effects"],
        "Next must not grow another state or issue projection"
    );
    assert!(
        projected["choices"]
            .as_array()
            .unwrap()
            .iter()
            .any(|choice| choice["kind"] == "propose_plan"),
        "an idle nonterminal mission must retain explicit revision authority"
    );
    assert!(
        ready.next.effects.is_empty(),
        "an available proposal choice must not restart planning implicitly"
    );
    assert!(
        projected["choices"]
            .as_array()
            .unwrap()
            .iter()
            .any(|choice| {
                choice["kind"] == "decide"
                    && choice["id"] == "mission"
                    && choice["action"] == "revise"
            }),
        "a fully green mission must offer explicit revision"
    );
    let inbox = std::process::Command::new(env!("CARGO_BIN_EXE_lionclaw"))
        .args(["mission", "inbox", "--json", "--repo"])
        .arg(dir.path())
        .output()
        .expect("render finish-ready inbox");
    assert!(
        inbox.status.success(),
        "inbox failed: {}",
        String::from_utf8_lossy(&inbox.stderr)
    );
    let inbox: serde_json::Value = serde_json::from_slice(&inbox.stdout).unwrap();
    assert_eq!(inbox["missions"].as_array().unwrap().len(), 1);
    assert_eq!(inbox["missions"][0]["mission_id"], mission_id.as_str());
    assert_eq!(inbox["missions"][0]["next"], projected);
    let inbox = std::process::Command::new(env!("CARGO_BIN_EXE_lionclaw"))
        .args(["mission", "inbox", "--repo"])
        .arg(dir.path())
        .output()
        .expect("render finish-ready human inbox");
    assert!(
        inbox.status.success(),
        "inbox failed: {}",
        String::from_utf8_lossy(&inbox.stderr)
    );
    let inbox = String::from_utf8(inbox.stdout).expect("UTF-8 inbox");
    assert!(inbox.contains("ready to finish"));
    assert!(inbox.contains(&format!("mission finish {mission_id} --reason REASON")));
    assert!(inbox.contains(&format!(
        "mission decide {mission_id} mission revise --feedback-file FEEDBACK_FILE"
    )));
    assert!(inbox.contains(&format!("mission abort {mission_id} --reason REASON")));

    engine.finish(&mission_id, "done").await.expect("finish");
    let done = engine.advance(&mission_id).await.expect("advance terminal");
    assert!(done.state.is_terminal());
    assert_eq!(done.state.finish(), Some(FinishClass::Verified));
}

#[tokio::test]
async fn universal_abort_needs_no_attention_and_records_only_the_abort_fact() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = gated_engine(dir.path()).await;
    let mission_id = engine
        .create_mission(dir.path().to_str().unwrap(), "gated", BASE_SHA)
        .await
        .expect("create");

    engine.abort(&mission_id, "stop").await.expect("abort");
    let state = engine.load_state(&mission_id).await.expect("state");
    assert!(matches!(
        state.terminal,
        Some(TerminalState::Aborted { ref reason }) if reason == "stop"
    ));
    let events = engine.store().load(&mission_id).await.expect("events");
    assert!(matches!(
        &events[events.len() - 1].event,
        MissionEvent::MissionAborted { reason } if reason == "stop"
    ));
    assert!(!events
        .iter()
        .any(|event| matches!(event.event, MissionEvent::DecisionRecorded { .. })));

    assert!(engine.abort(&mission_id, "again").await.is_err());
    assert!(engine.abort(&mission_id, "").await.is_err());
}
