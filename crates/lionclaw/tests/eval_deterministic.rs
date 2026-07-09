//! Slice 6 eval — the deterministic (engine-side, no-agent) completion
//! gates: the moat refuses a writable/over-privileged judge, and an
//! advisory-only plugin can never reach a verified finish.

mod common;

use std::path::PathBuf;
use std::sync::Arc;

use lionclaw::authority::AuthorityCeiling;
use lionclaw::engine::Engine;
use lionclaw::mission_type::load_mission_type;
use lionclaw::model::{
    AssertionId, FinishClass, Handoff, MissionConfig, MissionPhase, OutputSemantics, PayloadRef,
    PlanSubmission, RoleName, StopBar, Task, TaskId, TaskKind, ValidationItem,
};
use lionclaw::ports::{RoleRunOutcome, RoleRunRequest};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockClock, MockOracleRunner, MockRoleRunner};

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// Scenario 3 — the moat holds: a plugin whose verdict role over-reaches
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
/// (Its bar is `reviewed`, so the advisory plan is submittable; a `verified`
/// type would reject the oracle-less plan at submit — see
/// `plan_validation::tests::verified_bar_rejects_an_oracle_less_assertion`.)
#[tokio::test]
async fn advisory_only_mission_type_never_verifies() {
    let plugin = load_mission_type(
        &fixtures().join("mission-types/advisory-only"),
        &AuthorityCeiling::default(),
    )
    .expect("advisory-only mission type loads");
    assert_eq!(plugin.stop, StopBar::Reviewed);
    assert!(plugin.oracles.is_empty(), "fixture has no oracles");

    let dir = tempfile::tempdir().expect("tempdir");
    let store = MissionStore::open(dir.path()).await.expect("store");
    // A reviewer that passes everything; a worker that commits.
    let runner = MockRoleRunner::new(Box::new(|req: &RoleRunRequest| {
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
    let engine = Engine::new(
        store,
        plugin,
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
                ratification_gate: false,
                stop: StopBar::Reviewed,
            },
        )
        .await
        .expect("create");
    // One oracle-less assertion, covered by a worker and judged by a reviewer.
    let plan = PlanSubmission {
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
    engine.submit_plan(&mission_id, plan).await.expect("submit");
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
