//! Slice 6 eval — the deterministic (engine-side, no-agent) completion
//! gates: the moat refuses a writable/over-privileged judge, and an
//! advisory-only mission type can never reach a verified finish.

mod common;

use std::path::PathBuf;
use std::sync::Arc;

use common::{covered_requirement, proposal};
use lionclaw::authority::AuthorityCeiling;
use lionclaw::engine::Engine;
use lionclaw::mission_type::load_mission_type;
use lionclaw::model::{
    AssertionId, FinishClass, Handoff, MissionConfig, MissionPhase, OutputSemantics, PayloadRef,
    Plan, RoleName, StopBar, Task, TaskId, TaskKind, ValidationItem,
};
use lionclaw::ports::{RoleRunOutcome, RoleRunRequest};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockClock, MockOracleRunner, MockRoleRunner};

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// Scenario 3 — the moat holds: a mission type whose verdict role over-reaches
/// refuses to load, so no mission can ever start from it.
#[test]
fn moat_refuses_over_privileged_judge_mission_type() {
    let err = load_mission_type(
        &fixtures().join("mission-types/writable-judge"),
        &AuthorityCeiling::default(),
    )
    .expect_err("an over-privileged judge must refuse to load");
    assert!(
        matches!(err, lionclaw::mission_type::MissionTypeError::Moat { .. }),
        "expected a typed moat violation, got {err:?}"
    );
}

/// Scenario 4 — the fold-level honesty cap: a mission type with a reviewer and
/// no oracles finishes internally-consistent even when the reviewer passes
/// everything. Never verified — an agent-only verdict can't mint authority.
/// (Its bar is `reviewed`, so the advisory plan is valid; a `verified`
/// type would reject the oracle-less plan when proposed — see
/// `plan_validation::tests::verified_bar_rejects_an_oracle_less_assertion`.)
#[tokio::test]
async fn advisory_only_mission_type_never_verifies() {
    let mission_type = load_mission_type(
        &fixtures().join("mission-types/advisory-only"),
        &AuthorityCeiling::default(),
    )
    .expect("advisory-only mission type loads");
    assert_eq!(mission_type.stop, StopBar::Reviewed);
    assert!(mission_type.oracles.is_empty(), "fixture has no oracles");

    let dir = tempfile::tempdir().expect("tempdir");
    let store = MissionStore::open(dir.path()).await.expect("store");
    // A reviewer that passes everything (the plan validator per assertion,
    // the terminal reviewer with a clean verdict); a worker that commits.
    let runner = MockRoleRunner::new(Box::new(|req: &RoleRunRequest| {
        if req.task_id.as_str() == lionclaw::engine::TERMINAL_REVIEW_TASK_TAG {
            return Ok(lionclaw::testing::review_verdict(req, true, vec![]));
        }
        let handoff = if req.role.output == OutputSemantics::EmitsVerdict {
            Handoff::Validate {
                done: true,
                report: PayloadRef::inline("looks great to me"),
                items: vec![ValidationItem {
                    item_id: AssertionId::new("READABLE").unwrap(),
                    passed: true,
                }],
                passed: true,
                request_attention: false,
            }
        } else {
            Handoff::Work {
                done: true,
                report: PayloadRef::inline("done"),
                request_attention: false,
            }
        };
        let artifact = (req.role.output == OutputSemantics::ProducesArtifact).then(|| {
            lionclaw::model::ArtifactOutcome {
                base_sha: req.base_sha.clone(),
                head_sha: "head-1".to_string(),
            }
        });
        Ok(RoleRunOutcome {
            handoff,
            artifact,
            model_id: None,
        })
    }));
    // The reviewed bar requires the closing review (create_mission refuses
    // it otherwise), so thread the fixture's declaration like cmd_start does.
    let terminal_review = mission_type.terminal_review.clone();
    let engine = Engine::new(
        store,
        mission_type,
        "codex".to_string(),
        "test-image".to_string(),
        Arc::new(runner),
        Arc::new(MockOracleRunner::exiting(0)),
        Arc::new(MockClock::default()),
    );
    let mission_id = engine
        .create_mission(
            dir.path().to_str().unwrap(),
            "make it readable",
            "base-0",
            MissionConfig {
                approval_required: false,
                stop: StopBar::Reviewed,
                planning: Default::default(),
                recovery: Default::default(),
                terminal_review,
            },
        )
        .await
        .expect("create");
    // One oracle-less assertion, covered by a worker and judged by a reviewer.
    let plan = Plan {
        requirements: vec![covered_requirement("READABLE-CODE", "READABLE")],
        assertions: vec![lionclaw::model::Assertion {
            id: AssertionId::new("READABLE").unwrap(),
            prose: "the code reads cleanly".to_string(),
            oracle: None,
        }],
        tasks: vec![
            Task {
                id: TaskId::new("write").unwrap(),
                kind: TaskKind::Work,
                body: "write it".to_string(),
                targets: vec![AssertionId::new("READABLE").unwrap()],
                role: Some(RoleName::new("implementer").unwrap()),
                depends_on: vec![],
            },
            Task {
                id: TaskId::new("review").unwrap(),
                kind: TaskKind::Validate,
                body: "review it".to_string(),
                targets: vec![AssertionId::new("READABLE").unwrap()],
                role: Some(RoleName::new("reviewer").unwrap()),
                depends_on: vec![TaskId::new("write").unwrap()],
            },
        ],
    };
    engine
        .propose_plan(&mission_id, proposal(0, plan), "test", "initial plan")
        .await
        .expect("propose");
    engine.advance(&mission_id).await.expect("advance");
    let state = engine.load_state(&mission_id).await.expect("state");

    match state.phase {
        MissionPhase::Done { finish } => {
            assert_eq!(
                finish,
                FinishClass::InternallyConsistent,
                "advisory-only all-pass is internally consistent, never verified"
            );
        }
        other => panic!("expected Done, got {other:?}"),
    }
}
