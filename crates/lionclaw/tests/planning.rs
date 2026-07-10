//! Planning-in-phase, end to end: an objective drives a contract-free planning
//! DAG (research → adversary → author), the author's proposal is gradeless and
//! parks for ratification, and only a human `ratify` seeds the contract — after
//! which execution runs and an oracle mints the `Verified` finish.

mod common;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use common::BASE_SHA;
use lionclaw::engine::Engine;
use lionclaw::mission_type::{MissionType, RoleDefinition};
use lionclaw::model::{
    ArtifactOutcome, Assertion, AssertionId, AttentionKind, DecisionAction, Handoff, MissionConfig,
    MissionPhase, OracleName, OutputSemantics, PayloadRef, PlanSubmission, PlanningDag,
    PlanningTask, RoleName, StopBar, Task, TaskKind,
};
use lionclaw::ports::{RoleRunOutcome, RoleRunRequest};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockClock, MockOracleRunner, MockRoleRunner};

fn rn(n: &str) -> RoleName {
    RoleName::new(n).unwrap()
}
fn tid(n: &str) -> lionclaw::model::TaskId {
    lionclaw::model::TaskId::new(n).unwrap()
}
fn aid(n: &str) -> AssertionId {
    AssertionId::new(n).unwrap()
}

fn role(name: &str, output: OutputSemantics) -> RoleDefinition {
    RoleDefinition {
        name: rn(name),
        output,
        runtime: None,
        network: false,
        secrets: false,
        prompt_body: "role prose".to_string(),
    }
}

/// A mission type whose planning DAG is strategist → red-team → author, with
/// execution roles for what the author will propose. Stop bar: verified.
fn planning_mission_type() -> MissionType {
    let mut roles = BTreeMap::new();
    for (name, output) in [
        ("strategist", OutputSemantics::ProducesReport),
        ("red-team", OutputSemantics::ProducesReport),
        ("author", OutputSemantics::ProposesPlan),
        ("implementer", OutputSemantics::ProducesArtifact),
        ("reviewer", OutputSemantics::EmitsVerdict),
    ] {
        roles.insert(rn(name), role(name, output));
    }
    MissionType {
        name: "planning-test".to_string(),
        digest: "test-digest".to_string(),
        stop: StopBar::Verified,
        image: "img".to_string(),
        planning: planning_dag(),
        terminal_review: None,
        playbook: Some("plan carefully".to_string()),
        roles,
        oracles: BTreeMap::from([(
            OracleName::new("cargo-test").unwrap(),
            PathBuf::from("/nonexistent/oracles/cargo-test"),
        )]),
    }
}

fn planning_dag() -> PlanningDag {
    PlanningDag {
        tasks: vec![
            PlanningTask {
                id: tid("strategist"),
                role: rn("strategist"),
                body: "draft".to_string(),
                depends_on: vec![],
            },
            PlanningTask {
                id: tid("red-team"),
                role: rn("red-team"),
                body: "critique".to_string(),
                depends_on: vec![tid("strategist")],
            },
            PlanningTask {
                id: tid("author"),
                role: rn("author"),
                body: "propose".to_string(),
                depends_on: vec![tid("strategist"), tid("red-team")],
            },
        ],
    }
}

/// What the author proposes: one oracle-bound assertion, one implementer work
/// task. (Verified-possible, so it clears the `verified` bar.)
fn proposed_plan() -> PlanSubmission {
    PlanSubmission {
        assertions: vec![Assertion {
            id: aid("TESTS-PASS"),
            prose: "cargo test exits 0".to_string(),
            oracle: Some(OracleName::new("cargo-test").unwrap()),
        }],
        tasks: vec![Task {
            id: tid("fix"),
            kind: TaskKind::Work,
            body: "make it pass".to_string(),
            targets: vec![aid("TESTS-PASS")],
            role: Some(rn("implementer")),
            depends_on: vec![],
        }],
    }
}

/// A role runner that answers per output semantics: reports for the planners,
/// a plan proposal for the author, a committed artifact for the implementer.
fn planning_runner() -> MockRoleRunner {
    MockRoleRunner::new(Box::new(|req: &RoleRunRequest| {
        let handoff = match req.role.output {
            OutputSemantics::ProposesPlan => Handoff::Plan {
                done: true,
                report: PayloadRef::inline("proposed contract"),
                proposal: Some(proposed_plan()),
                request_attention: false,
            },
            OutputSemantics::ProducesReport => Handoff::Work {
                done: true,
                report: PayloadRef::inline("planning report"),
                request_attention: false,
            },
            OutputSemantics::ProducesArtifact => Handoff::Work {
                done: true,
                report: PayloadRef::inline("fixed it"),
                request_attention: false,
            },
            OutputSemantics::EmitsVerdict => Handoff::Validate {
                done: true,
                report: PayloadRef::inline("looks good"),
                items: vec![],
                passed: true,
                request_attention: false,
            },
            OutputSemantics::EmitsGapVerdict => {
                panic!("terminal-review roles are never plan tasks")
            }
        };
        let artifact =
            (req.role.output == OutputSemantics::ProducesArtifact).then(|| ArtifactOutcome {
                base_sha: req.base_sha.clone(),
                head_sha: "head-1".to_string(),
            });
        Ok(RoleRunOutcome {
            handoff,
            artifact,
            model_id: None,
        })
    }))
}

async fn planning_engine(workspace: &std::path::Path) -> Engine {
    let store = MissionStore::open(workspace).await.expect("store");
    Engine::new(
        store,
        planning_mission_type(),
        "codex".to_string(),
        "img".to_string(),
        Arc::new(planning_runner()),
        Arc::new(MockOracleRunner::exiting(0)),
        Arc::new(MockClock::default()),
    )
}

#[tokio::test]
async fn planning_proposes_then_ratify_seeds_the_contract_and_verifies() {
    let dir = tempfile::tempdir().unwrap();
    let engine = planning_engine(dir.path()).await;
    let id = engine
        .create_mission(
            &dir.path().to_string_lossy(),
            "make the tests pass",
            BASE_SHA,
            MissionConfig {
                ratification_gate: true,
                stop: StopBar::Verified,
                planning: planning_dag(),
                terminal_review: None,
            },
        )
        .await
        .unwrap();

    // Drive the planning DAG: strategist → red-team → author → park on the
    // proposal. No contract exists yet — the proposal is gradeless.
    engine.advance(&id).await.unwrap();
    let state = engine.load_state(&id).await.unwrap();
    assert_eq!(state.phase, MissionPhase::AttentionNeeded);
    assert!(state.plan.is_none(), "planning must not seed a plan");
    assert!(state.contract.is_empty(), "no contract before ratification");
    assert!(state.proposal.is_some(), "the author proposed a contract");
    let ratify = state
        .open_attention
        .values()
        .find(|a| a.kind == AttentionKind::RatifyProposal)
        .expect("parked on RatifyProposal");

    // A malformed advance can't launder past ratification: still no contract.
    engine.advance(&id).await.unwrap();
    assert!(engine.load_state(&id).await.unwrap().plan.is_none());

    // Ratify: derive_promotion seeds the contract for the first time.
    engine
        .decide(
            &id,
            &ratify.id,
            DecisionAction::Ratify,
            "looks good",
            "human",
        )
        .await
        .unwrap();
    let state = engine.load_state(&id).await.unwrap();
    assert!(state.plan.is_some(), "ratification seeds the plan");
    assert!(state.contract.contains_key(&aid("TESTS-PASS")));
    assert_eq!(state.revision, 1);

    // Execute: implementer commits, cargo-test passes at the new head → Verified.
    engine.advance(&id).await.unwrap();
    let state = engine.load_state(&id).await.unwrap();
    assert_eq!(
        state.phase,
        MissionPhase::Done {
            finish: lionclaw::model::FinishClass::Verified
        },
        "an oracle pass at the final head is Verified"
    );
    assert_ne!(
        state.current_sha, BASE_SHA,
        "the implementer's commit landed"
    );
}

#[tokio::test]
async fn retrying_a_proposal_rejects_it_and_re_runs_planning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = planning_engine(dir.path()).await;
    let id = engine
        .create_mission(
            &dir.path().to_string_lossy(),
            "make the tests pass",
            BASE_SHA,
            MissionConfig {
                ratification_gate: true,
                stop: StopBar::Verified,
                planning: planning_dag(),
                terminal_review: None,
            },
        )
        .await
        .unwrap();
    engine.advance(&id).await.unwrap();
    let state = engine.load_state(&id).await.unwrap();
    let ratify = state
        .open_attention
        .values()
        .find(|a| a.kind == AttentionKind::RatifyProposal)
        .expect("parked on RatifyProposal");

    // Retry: the proposal is discarded and the planning DAG is re-runnable.
    // Nothing was seeded, so nothing is weakened.
    engine
        .decide(
            &id,
            &ratify.id,
            DecisionAction::Retry,
            "not good enough",
            "human",
        )
        .await
        .unwrap();
    let state = engine.load_state(&id).await.unwrap();
    assert!(
        state.proposal.is_none(),
        "the rejected proposal is discarded"
    );
    assert!(state.plan.is_none(), "still no contract after a rejection");
    assert!(state.contract.is_empty());
    assert_eq!(
        state.phase,
        MissionPhase::Planning,
        "planning is re-runnable"
    );
}

/// A failed planning node (what a crashed run reconciles to — planning is
/// RoleRun, synthesized-failed by the same machinery) must raise a *retryable*
/// NodeFailed, never wedge the mission.
#[tokio::test]
async fn a_failed_planning_node_is_retryable_not_a_wedge() {
    let dir = tempfile::tempdir().unwrap();
    let store = MissionStore::open(dir.path()).await.unwrap();
    // A runner that fails the first planning node.
    let runner = MockRoleRunner::new(Box::new(|req: &RoleRunRequest| {
        if req.role.name.as_str() == "strategist" {
            return Err(lionclaw::ports::RoleRunFailure {
                kind: lionclaw::model::RunErrorKind::Timeout,
                detail: "crashed mid-planning".to_string(),
            });
        }
        Ok(RoleRunOutcome {
            handoff: Handoff::Work {
                done: true,
                report: PayloadRef::inline("report"),
                request_attention: false,
            },
            artifact: None,
            model_id: None,
        })
    }));
    let engine = Engine::new(
        store,
        planning_mission_type(),
        "codex".to_string(),
        "img".to_string(),
        Arc::new(runner),
        Arc::new(MockOracleRunner::exiting(0)),
        Arc::new(MockClock::default()),
    );
    let id = engine
        .create_mission(
            &dir.path().to_string_lossy(),
            "obj",
            BASE_SHA,
            MissionConfig {
                ratification_gate: true,
                stop: StopBar::Verified,
                planning: planning_dag(),
                terminal_review: None,
            },
        )
        .await
        .unwrap();

    engine.advance(&id).await.unwrap();
    let state = engine.load_state(&id).await.unwrap();
    assert_eq!(state.phase, MissionPhase::AttentionNeeded);
    let node_failed = state
        .open_attention
        .values()
        .find(|a| a.kind == AttentionKind::NodeFailed)
        .expect("a failed planning node raises NodeFailed, not a wedge");
    assert_eq!(node_failed.task_id.as_ref(), Some(&tid("strategist")));

    // Retry re-pends the planning node (a fresh attempt would re-dispatch it).
    engine
        .decide(
            &id,
            &node_failed.id,
            DecisionAction::Retry,
            "try again",
            "human",
        )
        .await
        .unwrap();
    let state = engine.load_state(&id).await.unwrap();
    assert_eq!(
        state.phase,
        MissionPhase::Planning,
        "retry re-opens planning"
    );
    assert!(state.plan.is_none());
}

/// Drive planning where the author hands back `author_handoff`, returning the
/// parked state.
async fn park_after_author(
    dir: &std::path::Path,
    author_handoff: Handoff,
) -> lionclaw::model::MissionState {
    let store = MissionStore::open(dir).await.expect("store");
    let runner = MockRoleRunner::new(Box::new(move |req: &RoleRunRequest| {
        let handoff = if req.role.output == OutputSemantics::ProposesPlan {
            author_handoff.clone()
        } else {
            Handoff::Work {
                done: true,
                report: PayloadRef::inline("report"),
                request_attention: false,
            }
        };
        Ok(RoleRunOutcome {
            handoff,
            artifact: None,
            model_id: None,
        })
    }));
    let engine = Engine::new(
        store,
        planning_mission_type(),
        "codex".to_string(),
        "img".to_string(),
        Arc::new(runner),
        Arc::new(MockOracleRunner::exiting(0)),
        Arc::new(MockClock::default()),
    );
    let id = engine
        .create_mission(
            &dir.to_string_lossy(),
            "obj",
            BASE_SHA,
            MissionConfig {
                ratification_gate: true,
                stop: StopBar::Verified,
                planning: planning_dag(),
                terminal_review: None,
            },
        )
        .await
        .unwrap();
    engine.advance(&id).await.unwrap();
    engine.load_state(&id).await.unwrap()
}

fn assert_author_failed_seeding_nothing(state: &lionclaw::model::MissionState) {
    assert_eq!(state.phase, MissionPhase::AttentionNeeded);
    assert!(
        state
            .open_attention
            .values()
            .any(|a| a.kind == AttentionKind::NodeFailed
                && a.task_id.as_ref() == Some(&tid("author"))),
        "the author node failed"
    );
    assert!(state.proposal.is_none(), "a bad handoff seeds no proposal");
    assert!(state.contract.is_empty());
}

/// The engine re-validates the author's Handoff::Plan fail-closed before it can
/// become a ratifiable proposal — neither a done-with-no-proposal nor a proposal
/// that fails plan validation slips through.
#[tokio::test]
async fn a_bad_author_proposal_fails_the_node_and_seeds_nothing() {
    // (a) `done` but proposed nothing.
    let dir = tempfile::tempdir().unwrap();
    let state = park_after_author(
        dir.path(),
        Handoff::Plan {
            done: true,
            report: PayloadRef::inline("no plan"),
            proposal: None,
            request_attention: false,
        },
    )
    .await;
    assert_author_failed_seeding_nothing(&state);

    // (b) a proposal that fails validation under the `verified` stop bar (an
    // oracle-less assertion).
    let invalid = PlanSubmission {
        assertions: vec![Assertion {
            id: aid("UNCHECKABLE"),
            prose: "no oracle can prove this".to_string(),
            oracle: None,
        }],
        tasks: vec![Task {
            id: tid("fix"),
            kind: TaskKind::Work,
            body: "x".to_string(),
            targets: vec![aid("UNCHECKABLE")],
            role: Some(rn("implementer")),
            depends_on: vec![],
        }],
    };
    let dir = tempfile::tempdir().unwrap();
    let state = park_after_author(
        dir.path(),
        Handoff::Plan {
            done: true,
            report: PayloadRef::inline("bad plan"),
            proposal: Some(invalid),
            request_attention: false,
        },
    )
    .await;
    assert_author_failed_seeding_nothing(&state);
}
