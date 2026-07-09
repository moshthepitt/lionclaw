//! Slice 3 store hardening: lease expiry lets a crashed worker's claim be
//! reclaimed exactly once; idempotency keeps duplicate requests out.

mod common;

use common::{default_config, harness, simple_plan, BASE_SHA, HEAD_SHA};
use lionclaw::model::{MissionEvent, PayloadRef, RoleName, TaskId};
use lionclaw::store::NewEvent;
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};

#[tokio::test]
async fn expired_lease_is_reclaimable_exactly_once() {
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
        .submit_plan(&mission_id, simple_plan())
        .await
        .expect("submit");
    let store = h.engine.store();

    // Record a request so there is a queued effect to lease.
    let state = h.engine.load_state(&mission_id).await.expect("state");
    let event = NewEvent::new(MissionEvent::RoleRunRequested {
        task_id: TaskId::new("fix").unwrap(),
        attempt_no: 1,
        idempotency_key: "lease-key".to_string(),
        role: RoleName::new("implementer").unwrap(),
        prompt: PayloadRef::inline("p"),
        base_sha: BASE_SHA.to_string(),
    });
    store
        .append(&mission_id, state.head, &[event], 1_000)
        .await
        .expect("append");

    // Worker A leases at t=1000 with a 5s lease (expires 6000).
    let a = store
        .pull_due(&mission_id, "worker-a", 1, 5_000, 1_000)
        .await
        .expect("lease a");
    assert_eq!(a.len(), 1);

    // Before expiry, no one else can lease it.
    let b_early = store
        .pull_due(&mission_id, "worker-b", 1, 5_000, 3_000)
        .await
        .expect("lease b");
    assert!(b_early.is_empty(), "a live lease must not be reclaimable");

    // After expiry (t=7000), worker B reclaims it — exactly once.
    let b_late = store
        .pull_due(&mission_id, "worker-b", 1, 5_000, 7_000)
        .await
        .expect("lease b late");
    assert_eq!(b_late.len(), 1, "expired lease must be reclaimable");
    assert_eq!(b_late[0].effect_id, a[0].effect_id);

    // A second puller at the same instant gets nothing (single reclaim).
    let c = store
        .pull_due(&mission_id, "worker-c", 1, 5_000, 7_000)
        .await
        .expect("lease c");
    assert!(
        c.is_empty(),
        "a freshly reclaimed lease is not double-leased"
    );
}
