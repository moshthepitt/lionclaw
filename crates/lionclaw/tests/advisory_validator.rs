//! Slice 2: a read-only validator produces advisory verdicts alongside (or
//! instead of) an oracle. Advisory verdicts route and rank; they never mark
//! a mission verified — that requires authoritative oracle coverage.

mod common;

use std::sync::Arc;

use common::{advisory_plan, proposal, test_mission_type, BASE_SHA, HEAD_SHA};
use lionclaw::engine::Engine;
use lionclaw::model::{
    AdvisoryStatus, FinishClass, Handoff, MissionPhase, PayloadRef, ValidationItem,
};
use lionclaw::ports::{RoleRunOutcome, RoleRunRequest};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockClock, MockOracleRunner, MockRoleRunner};

/// A role-aware mock: verdict roles return a ValidateHandoff, others a work
/// handoff that "commits" HEAD_SHA.
fn role_aware_runner(reviewer_passes: bool) -> MockRoleRunner {
    MockRoleRunner::new(Box::new(move |req: &RoleRunRequest| {
        use lionclaw::model::OutputSemantics;
        let handoff = if req.role.output == OutputSemantics::EmitsVerdict {
            Handoff::Validate {
                done: true,
                report: PayloadRef::inline("reviewed"),
                items: vec![ValidationItem {
                    item_id: lionclaw::model::AssertionId::new("STYLE-OK").unwrap(),
                    passed: reviewer_passes,
                }],
                passed: reviewer_passes,
                request_attention: false,
            }
        } else {
            Handoff::Work {
                done: true,
                report: PayloadRef::inline("wrote it"),
                request_attention: false,
            }
        };
        let artifact = (req.role.output == OutputSemantics::ProducesArtifact).then(|| {
            lionclaw::model::ArtifactOutcome {
                base_sha: req.base_sha.clone(),
                head_sha: HEAD_SHA.to_string(),
            }
        });
        Ok(RoleRunOutcome {
            handoff,
            artifact,
            model_id: None,
        })
    }))
}

async fn run(reviewer_passes: bool) -> (MissionPhase, AdvisoryStatus) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = MissionStore::open(dir.path()).await.expect("store");
    let engine = Engine::new(
        store,
        test_mission_type(),
        "codex".to_string(),
        "test-image".to_string(),
        Arc::new(role_aware_runner(reviewer_passes)),
        Arc::new(MockOracleRunner::exiting(0)),
        Arc::new(MockClock::default()),
    );
    let mission_id = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "advisory-only mission",
            BASE_SHA,
            lionclaw::model::MissionConfig {
                approval_required: false,
                ..Default::default()
            },
        )
        .await
        .expect("create");
    engine
        .propose_plan(
            &mission_id,
            proposal(0, advisory_plan()),
            "test",
            "initial plan",
        )
        .await
        .expect("propose");
    engine.advance(&mission_id).await.expect("advance");
    let state = engine.load_state(&mission_id).await.expect("state");
    let advisory = state
        .contract
        .get(&lionclaw::model::AssertionId::new("STYLE-OK").unwrap())
        .expect("assertion")
        .advisory;
    (state.phase, advisory)
}

#[tokio::test]
async fn advisory_pass_is_internally_consistent_never_verified() {
    let (phase, advisory) = run(true).await;
    assert_eq!(advisory, AdvisoryStatus::Passed);
    // All-green advisory with no authoritative coverage: internally
    // consistent, NOT verified.
    assert_eq!(
        phase,
        MissionPhase::Done {
            finish: FinishClass::InternallyConsistent
        }
    );
}

#[tokio::test]
async fn advisory_fail_is_unverified() {
    let (phase, advisory) = run(false).await;
    assert_eq!(advisory, AdvisoryStatus::Failed);
    assert_eq!(
        phase,
        MissionPhase::Done {
            finish: FinishClass::Unverified
        }
    );
}
