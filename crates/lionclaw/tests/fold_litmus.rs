//! The persistence litmus: state is a pure fold over the log. Folding the
//! same events twice is identical; folding a prefix then applying the tail
//! equals folding everything; state survives a serde roundtrip unchanged
//! (snapshot readiness); and the discardable snapshot rebuilds from the log.

mod common;

use common::{
    approve_plan, blocking_gap, default_config, harness, harness_with_type, proposal,
    review_config, review_mission_type, review_runner, simple_plan, TestHarness, BASE_SHA,
    HEAD_SHA,
};
use lionclaw::model::{apply, fold, DecisionAction, MissionId};
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};

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
        .create_mission(
            dir.path().to_str().expect("utf8"),
            "obj",
            BASE_SHA,
            default_config(),
        )
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
        .create_mission(
            dir.path().to_str().expect("utf8"),
            "obj",
            BASE_SHA,
            review_config(),
        )
        .await
        .expect("create");
    h.engine
        .propose_plan(&mission_id, proposal(0, simple_plan()))
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
            "terminal_review_gaps:mission",
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
        .create_mission(
            dir.path().to_str().unwrap(),
            "obj",
            BASE_SHA,
            default_config(),
        )
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
