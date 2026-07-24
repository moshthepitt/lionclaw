//! Slice 6 eval — the deterministic (engine-side, no-agent) completion
//! gates: the moat refuses a writable/over-privileged judge, and an
//! advisory-only mission type can never reach a verified finish.

mod common;

use std::path::PathBuf;
use std::sync::Arc;

use common::{approve_plan, covered_requirement, proposal_with_team, BASE_SHA};
use lionclaw::authority::AuthorityCeiling;
use lionclaw::engine::{Engine, EngineServices};
use lionclaw::mission_type::load_mission_type;
use lionclaw::model::{
    AssertionId, FinishClass, Handoff, MissionPhase, OutputSemantics, PayloadRef, Plan, StopBar,
    Task, TaskId, ValidationItem,
};
use lionclaw::ports::{CapturedArtifact, RoleTurnOutcome, RoleTurnRequest};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockClock, MockOracleRunner, MockRoleRunner, NoopEffectCleaner};

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
/// (Its bar is `attested`, so the advisory plan is valid; a `verified`
/// type would reject the oracle-less plan when proposed — see
/// `plan_validation::tests::verified_bar_rejects_an_oracle_less_assertion`.)
#[tokio::test]
async fn advisory_only_mission_type_never_verifies() {
    let mission_type = load_mission_type(
        &fixtures().join("mission-types/advisory-only"),
        &AuthorityCeiling::default(),
    )
    .expect("advisory-only mission type loads");
    assert_eq!(mission_type.stop, StopBar::Attested);
    assert!(mission_type.oracles.is_empty(), "fixture has no oracles");
    let proposed_team = mission_type.default_team.clone();

    let dir = tempfile::tempdir().expect("tempdir");
    common::initialize_repository(dir.path());
    let store = MissionStore::open(dir.path()).await.expect("store");
    // A reviewer that passes everything (the plan validator per assertion,
    // the terminal reviewer with a clean verdict); a worker that commits.
    let runner = MockRoleRunner::new(Box::new(|req: &RoleTurnRequest| {
        if req.role.output == OutputSemantics::EmitsGapVerdict {
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
        let artifact = (req.role.output == OutputSemantics::ProducesArtifact)
            .then(|| CapturedArtifact::for_testing(req.base_sha.clone(), "head-1"));
        Ok(RoleTurnOutcome {
            handoff: Some(handoff),
            artifact,
            runtime_configuration: Default::default(),
            runtime_usage: Default::default(),
            final_response: String::new(),
        })
    }));
    let engine = Engine::new(
        store,
        mission_type,
        "test-image".to_string(),
        EngineServices::new(
            Arc::new(runner),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );
    let mission_id = engine
        .create_mission(dir.path().to_str().unwrap(), "make it readable", BASE_SHA)
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
        tasks: vec![Task {
            id: TaskId::new("write").unwrap(),
            body: "write it".to_string(),
            targets: vec![AssertionId::new("READABLE").unwrap()],
            depends_on: vec![],
        }],
    };
    let mut plan = plan;
    plan.requirements[0].disposition = lionclaw::model::RequirementDisposition::ReviewerCheckable {
        assertion_ids: vec![AssertionId::new("READABLE").unwrap()],
    };
    engine
        .propose_plan(&mission_id, proposal_with_team(0, plan, proposed_team))
        .await
        .expect("propose");
    approve_plan(&engine, &mission_id).await;
    let mut state = None;
    for _ in 0..6 {
        let outcome = engine.advance(&mission_id).await.expect("advance");
        if matches!(outcome.state.phase, MissionPhase::Done { .. }) {
            state = Some(outcome.state);
            break;
        }
    }
    let state = state.unwrap_or_else(|| panic!("mission did not finish after bounded advances"));

    match state.phase {
        MissionPhase::Done { finish } => {
            assert_eq!(
                finish,
                FinishClass::Attested,
                "advisory-only all-pass is internally consistent, never verified"
            );
        }
        other => panic!("expected Done, got {other:?}"),
    }
}
