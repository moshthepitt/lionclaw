//! The persistence litmus: state is a pure fold over the log. Folding the
//! same events twice is identical; folding a prefix then applying the tail
//! equals folding everything; state survives a serde roundtrip unchanged
//! (snapshot readiness); and the discardable snapshot rebuilds from the log.

mod common;

use common::{
    approve_plan, blocking_gap, fault_append_events, harness, harness_with_type, proposal,
    review_mission_type, review_proposal, review_runner, simple_plan, TestHarness, BASE_SHA,
    HEAD_SHA,
};
use lionclaw::model::{
    apply, fold, ControlAction, DecisionAction, EffectId, MissionEvent, MissionId, PayloadRef,
    RoleInstanceId, TaskId,
};
use lionclaw::store::NewEvent;
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};
use lionclaw_runtime_api::{TypedFailure, TypedFailureEvidence};

/// The four laws, over whatever log the mission produced: determinism,
/// incremental consistency, serde roundtrip, and cursor agreement/rebuild.
async fn assert_fold_litmus(h: &TestHarness, mission_id: &MissionId) {
    let events = h.engine.store().load(mission_id).await.expect("load");
    assert!(events.len() >= 4, "expected a full mission log");

    // Determinism: same log, same state.
    let once = fold(events.clone()).expect("fold");
    let twice = fold(events.clone()).expect("fold");
    assert_eq!(once, twice);

    // Incremental consistency: fold(prefix) + apply(tail) == fold(all).
    for split in 1..events.len() {
        let mut prefix = fold(events[..split].to_vec()).expect("prefix fold");
        for envelope in &events[split..] {
            apply(&mut prefix, envelope);
        }
        assert_eq!(prefix, once, "split at {split} diverged");
    }

    // Serde roundtrip (snapshot readiness): state == decode(encode(state)).
    let encoded = serde_json::to_string(&once).expect("encode");
    let decoded = serde_json::from_str(&encoded).expect("decode");
    assert_eq!(once, decoded);

    // A finished mission has no unfinished requests.
    assert!(once.inflight.is_empty());

    // Delete the derived snapshot and rebuild from the log alone.
    let rebuilt = h
        .engine
        .store()
        .rebuild_cursors(mission_id, 9_000_000)
        .await
        .expect("rebuild cursors");
    assert_eq!(rebuilt, once, "state diverged after cursor rebuild");
}

fn assert_every_prefix_is_deterministic(events: &[lionclaw::model::EventEnvelope]) {
    for end in 1..=events.len() {
        let prefix = &events[..end];
        let expected = fold(prefix.to_vec()).expect("prefix fold");
        assert_eq!(fold(prefix.to_vec()).expect("repeat fold"), expected);
        let json = serde_json::to_string(&expected).expect("encode prefix state");
        assert_eq!(
            serde_json::from_str::<lionclaw::model::MissionState>(&json)
                .expect("decode prefix state"),
            expected
        );
        for split in 1..end {
            let mut incremental = fold(prefix[..split].to_vec()).expect("incremental prefix");
            for event in &prefix[split..] {
                apply(&mut incremental, event);
            }
            assert_eq!(
                incremental, expected,
                "prefix ending at {end} diverged at split {split}"
            );
        }
    }
}

#[tokio::test]
async fn fold_is_deterministic_incremental_and_serde_stable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(dir.path().to_str().expect("utf8"), "obj", BASE_SHA)
        .await
        .expect("create");
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    h.engine.advance(&mission_id).await.expect("advance");

    assert_fold_litmus(&h, &mission_id).await;
}

#[tokio::test]
async fn a_review_mission_satisfies_the_litmus_through_park_and_acknowledge() {
    // The richest review log: request → blocking verdict → park →
    // acknowledge → Done. Catches a missing #[serde(default)] on any new
    // state field the moment it exists.
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness_with_type(
        dir.path(),
        review_mission_type(),
        review_runner(vec![(false, vec![blocking_gap()])]),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(dir.path().to_str().expect("utf8"), "obj", BASE_SHA)
        .await
        .expect("create");
    h.engine
        .propose_plan(&mission_id, review_proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    h.engine
        .advance(&mission_id)
        .await
        .expect("advance to park");
    h.engine
        .decide(
            &mission_id,
            "gap_review_gaps:mission",
            DecisionAction::Accept,
            "acceptable",
        )
        .await
        .expect("decide");
    h.engine
        .advance(&mission_id)
        .await
        .expect("advance to done");

    assert_fold_litmus(&h, &mission_id).await;
}

#[tokio::test]
async fn snapshot_resume_matches_full_refold() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "obj", BASE_SHA)
        .await
        .expect("create");
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    h.engine.advance(&mission_id).await.expect("advance");

    // Prove the snapshot branch is actually taken (not silently full-refolding
    // via the fallback): a live snapshot row must cover the whole log at the
    // current reducer.
    let events = h.engine.store().load(&mission_id).await.expect("load");
    let head = events.last().expect("events exist").sequence_no;
    let (upto, reducer) = h
        .engine
        .store()
        .snapshot_meta(&mission_id)
        .await
        .expect("meta")
        .expect("advance() must have written a snapshot");
    assert_eq!(upto, head, "snapshot must cover the whole log");
    assert_eq!(reducer, lionclaw::model::REDUCER_VERSION);

    // Loading via the snapshot path must equal a fresh full fold of the log.
    let via_snapshot = h
        .engine
        .store()
        .load_state_snapshotted(&mission_id)
        .await
        .expect("load")
        .expect("state");
    let via_full = fold(events).expect("fold");
    assert_eq!(via_snapshot, via_full);
}

#[tokio::test]
async fn corrupted_snapshot_cursor_is_discarded_before_replay() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "reject snapshot authority",
            BASE_SHA,
        )
        .await
        .expect("create");
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    h.engine.advance(&mission_id).await.expect("advance");

    let events = h.engine.store().load(&mission_id).await.expect("events");
    let expected = fold(events).expect("full replay");
    let database = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}",
        dir.path().join(".lionclaw/mission.db").display()
    ))
    .await
    .expect("open database");
    let mut corrupted = expected.clone();
    corrupted.head -= 1;
    sqlx::query(
        "UPDATE mission_snapshots SET state_json = ?1 \
         WHERE mission_id = ?2",
    )
    .bind(serde_json::to_string(&corrupted).expect("encode corruption"))
    .bind(mission_id.as_str())
    .execute(&database)
    .await
    .expect("corrupt snapshot cursor");

    let resumed = h
        .engine
        .store()
        .load_state_snapshotted(&mission_id)
        .await
        .expect("load must fall back")
        .expect("state");
    assert_eq!(
        resumed, expected,
        "corrupt snapshot must not become authority"
    );

    let mut future = expected.clone();
    future.head += 10;
    sqlx::query(
        "UPDATE mission_snapshots SET upto_sequence_no = ?1, state_json = ?2 \
         WHERE mission_id = ?3",
    )
    .bind(future.head as i64)
    .bind(serde_json::to_string(&future).expect("encode future snapshot"))
    .bind(mission_id.as_str())
    .execute(&database)
    .await
    .expect("inject future snapshot");
    let resumed = h
        .engine
        .store()
        .load_state_snapshotted(&mission_id)
        .await
        .expect("load must reject future cursor")
        .expect("state");
    assert_eq!(resumed, expected, "future snapshot must not hide the log");
}

#[tokio::test]
async fn controlled_effect_log_satisfies_every_prefix_and_snapshot_law() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "controlled replay", BASE_SHA)
        .await
        .unwrap();
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&h.engine, &mission_id).await;

    let task_id = TaskId::new("fix").unwrap();
    let prompt_hash = PayloadRef::inline("prompt").content_sha256().unwrap();
    let role_instance = RoleInstanceId::new("implementer").unwrap();
    let effect_id = EffectId::for_role_turn(
        &mission_id,
        &role_instance,
        1,
        Some(&task_id),
        1,
        1,
        &prompt_hash,
    );
    let mut failure_evidence = TypedFailureEvidence::new(
        Some("runtime.cancel_acknowledged".into()),
        "effect reached its recorded deadline",
    );
    failure_evidence.stop_reason = Some("effect deadline exhausted".into());
    let state = h.engine.load_state(&mission_id).await.unwrap();
    fault_append_events(
        dir.path(),
        &mission_id,
        state.head,
        &[
            NewEvent::new(MissionEvent::RoleTurnRequested {
                role_instance,
                team_revision: 1,
                task_id: Some(task_id),
                assertion_ids: vec![lionclaw::model::AssertionId::new("TESTS-PASS").unwrap()],
                attempt_no: 1,
                effect_id: effect_id.clone(),
                prompt_template: lionclaw::model::RolePromptTemplate::Execution,
                prompt_hash,
                base_sha: BASE_SHA.into(),
                dependency_refs: vec![],
                assignment_epoch: 1,
                message_boundary: state.head,
                presented_messages: vec![],
                workspace_preparation: lionclaw::model::WorkspacePreparation::ResetForAssignment,
                requested_at_ms: 1_000,
                deadline_ms: 2_000,
                budget_deadline_ms: 3_000,
            }),
            NewEvent::new(MissionEvent::ControlRequested {
                effect_id: effect_id.clone(),
                action: ControlAction::ExtendDeadline {
                    old_deadline_ms: 2_000,
                    new_deadline_ms: 3_000,
                    automatic: false,
                },
                reason: "observed progress".into(),
            }),
            NewEvent::new(MissionEvent::ControlRequested {
                effect_id: effect_id.clone(),
                action: ControlAction::DeadlineReached { deadline_ms: 3_000 },
                reason: "extended deadline elapsed".into(),
            }),
            NewEvent::new(MissionEvent::RoleTurnCompleted {
                effect_id: effect_id.clone(),
                outcome: Err(TypedFailure::DeadlineExhausted {
                    evidence: Box::new(failure_evidence),
                }),
            }),
        ],
        3_001,
    )
    .await;

    let events = h.engine.store().load(&mission_id).await.unwrap();
    assert_every_prefix_is_deterministic(&events);
    let expected = fold(events).unwrap();
    assert!(expected.inflight.is_empty());
    assert_eq!(expected.reached_deadlines.get(&effect_id), Some(&3_000));
    assert_eq!(
        h.engine
            .store()
            .rebuild_cursors(&mission_id, 4_000)
            .await
            .unwrap(),
        expected
    );
}

#[tokio::test]
async fn iterative_ratification_and_abort_survive_every_prefix_and_snapshot_generation() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(
            dir.path().to_str().expect("utf8"),
            "ratify repeatedly",
            BASE_SHA,
        )
        .await
        .expect("create");

    for (name, feedback) in [("A", "refine A"), ("B", "refine B")] {
        let mut plan = simple_plan();
        plan.tasks[0].body = format!("candidate {name}");
        h.engine
            .propose_plan(&mission_id, proposal(0, plan))
            .await
            .expect("propose candidate");
        h.engine
            .decide(
                &mission_id,
                "plan_proposal:mission",
                DecisionAction::Revise,
                feedback,
            )
            .await
            .expect("revise candidate");
    }
    let mut plan_c = simple_plan();
    plan_c.tasks[0].body = "candidate C".to_string();
    h.engine
        .propose_plan(&mission_id, proposal(0, plan_c))
        .await
        .expect("propose C");
    h.engine
        .decide(
            &mission_id,
            "plan_proposal:mission",
            DecisionAction::Approve,
            "C is ready",
        )
        .await
        .expect("approve C");

    let events = h.engine.store().load(&mission_id).await.expect("events");
    assert_every_prefix_is_deterministic(&events);
    let expected = fold(events).expect("full fold");
    let rebuilt = h
        .engine
        .store()
        .rebuild_cursors(&mission_id, 10)
        .await
        .expect("snapshot rebuild");
    assert_eq!(rebuilt, expected);

    let database = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}",
        dir.path().join(".lionclaw/mission.db").display()
    ))
    .await
    .expect("open database");
    sqlx::query(
        "UPDATE mission_snapshots SET reducer_version = ?1, state_json = 'not-json' \
         WHERE mission_id = ?2",
    )
    .bind((lionclaw::model::REDUCER_VERSION - 1) as i64)
    .bind(mission_id.as_str())
    .execute(&database)
    .await
    .expect("age snapshot");
    let from_old_snapshot = h
        .engine
        .store()
        .load_state_snapshotted(&mission_id)
        .await
        .expect("fallback load")
        .expect("state");
    assert_eq!(from_old_snapshot, expected);
    h.engine
        .store()
        .rebuild_cursors(&mission_id, 11)
        .await
        .expect("replace stale snapshot from the authoritative log");
    assert_eq!(
        h.engine
            .store()
            .snapshot_meta(&mission_id)
            .await
            .expect("snapshot meta")
            .expect("snapshot")
            .1,
        lionclaw::model::REDUCER_VERSION
    );

    let aborted_id = h
        .engine
        .create_mission(
            dir.path().to_str().expect("utf8"),
            "abort during ratification",
            BASE_SHA,
        )
        .await
        .expect("create aborted mission");
    h.engine
        .propose_plan(&aborted_id, proposal(0, simple_plan()))
        .await
        .expect("propose abort candidate");
    h.engine
        .abort(&aborted_id, "stop here")
        .await
        .expect("abort");
    let aborted_events = h
        .engine
        .store()
        .load(&aborted_id)
        .await
        .expect("aborted events");
    assert_every_prefix_is_deterministic(&aborted_events);
}

#[tokio::test]
async fn failure_driven_replanning_survives_every_prefix_and_snapshot_rebuild() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = harness_with_type(
        dir.path(),
        review_mission_type(),
        review_runner(vec![(false, vec![blocking_gap()])]),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = h
        .engine
        .create_mission(
            dir.path().to_str().expect("utf8"),
            "repair the reviewed behavior",
            BASE_SHA,
        )
        .await
        .expect("create");
    h.engine
        .propose_plan(&mission_id, review_proposal(0, simple_plan()))
        .await
        .expect("propose");
    approve_plan(&h.engine, &mission_id).await;
    h.engine.advance(&mission_id).await.expect("advance to gap");
    h.engine
        .decide(
            &mission_id,
            "gap_review_gaps:mission",
            DecisionAction::Revise,
            "repair what the reviewer observed",
        )
        .await
        .expect("replan");

    let events = h.engine.store().load(&mission_id).await.expect("events");
    assert_every_prefix_is_deterministic(&events);
    let expected = fold(events).expect("full fold");
    let rebuilt = h
        .engine
        .store()
        .rebuild_cursors(&mission_id, 20)
        .await
        .expect("snapshot rebuild");
    assert_eq!(rebuilt, expected);
}
