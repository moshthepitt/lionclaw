//! The persistence litmus: state is a pure fold over the log. Folding the
//! same events twice is identical; folding a prefix then applying the tail
//! equals folding everything; state survives a serde roundtrip unchanged
//! (snapshot readiness); and the derived effect ledger agrees with the
//! fold's inflight set.

mod common;

use common::{default_config, harness, simple_plan, BASE_SHA, HEAD_SHA};
use lionclaw_mission_engine::model::{apply, fold};
use lionclaw_mission_engine::testing::{MockOracleRunner, MockRoleRunner};

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
        .create_mission(dir.path().to_str().expect("utf8"), "obj", BASE_SHA, default_config())
        .await
        .expect("create");
    h.engine
        .submit_plan(&mission_id, simple_plan())
        .await
        .expect("submit");
    h.engine.advance(&mission_id).await.expect("advance");

    let events = h.engine.store().load(&mission_id).await.expect("load");
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

    // Cursor agreement: a finished mission has no inflight effects and no
    // runnable ledger rows.
    assert!(once.inflight.is_empty());
    for envelope in &events {
        if let Some((lionclaw_mission_engine::model::IdemClass::Request, key)) =
            envelope.event.idempotency()
        {
            let status = h
                .engine
                .store()
                .effect_status(key)
                .await
                .expect("status")
                .expect("row exists");
            assert!(
                status == "done" || status == "failed",
                "effect {key} left in state {status}"
            );
        }
    }
}
