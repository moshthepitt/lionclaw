//! Planning-in-phase, end to end: an objective drives a contract-free planning
//! DAG (research → adversary → author), the author's proposal is gradeless and
//! parks for approval, and only a human `approve` seeds the contract — after
//! which execution runs and an oracle mints the `Verified` finish.

mod common;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use common::{covered_requirement, BASE_SHA};
use lionclaw::engine::{Engine, EngineServices};
use lionclaw::mission_type::{MissionType, RoleDefinition, SkillPackage};
use lionclaw::model::{
    ArtifactOutcome, Assertion, AssertionId, AttentionKind, DecisionAction, Handoff, MissionConfig,
    MissionEvent, MissionPhase, OracleName, OutputSemantics, PayloadRef, Plan, PlanProposal,
    PlanningDag, PlanningRefinement, PlanningTask, RoleName, StopBar, Task, TaskKind, TaskStatus,
};
use lionclaw::ports::{RoleRunOutcome, RoleRunRequest};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockClock, MockOracleRunner, MockRoleRunner, NoopEffectCleaner};

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
        skills: Vec::new(),
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
    let strategist = roles.get_mut(&rn("strategist")).unwrap();
    strategist.skills = vec!["planning-method".to_string()];
    strategist.runtime = Some("opencode".to_string());
    MissionType {
        name: "planning-test".to_string(),
        digest: "test-digest".to_string(),
        stop: StopBar::Verified,
        image: "img".to_string(),
        planning: planning_dag(),
        recovery: Default::default(),
        terminal_review: None,
        playbook: Some("plan carefully".to_string()),
        roles,
        skills: BTreeMap::from([(
            "planning-method".to_string(),
            SkillPackage {
                name: "planning-method".to_string(),
                root: PathBuf::from("/mission-type/skills/planning-method"),
                description: "planning method".to_string(),
            },
        )]),
        inputs: BTreeMap::new(),
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
fn proposed_plan() -> PlanProposal {
    PlanProposal {
        base_revision: 0,
        plan: Plan {
            requirements: vec![covered_requirement("GREEN-TESTS", "TESTS-PASS")],
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
        },
    }
}

fn candidate(task_id: &str) -> PlanProposal {
    let mut proposal = proposed_plan();
    proposal.plan.tasks[0].id = tid(task_id);
    proposal.plan.tasks[0].body = format!("make {task_id} pass");
    proposal
}

/// A role runner that answers per output semantics: reports for the planners,
/// a plan proposal for the author, a committed artifact for the implementer.
fn planning_runner() -> MockRoleRunner {
    MockRoleRunner::new(Box::new(|req: &RoleRunRequest| {
        if req.role.name.as_str() == "strategist" {
            assert_eq!(req.runtime, "opencode");
            assert_eq!(req.skills.len(), 1);
            assert_eq!(req.skills[0].name, "planning-method");
        } else {
            assert_eq!(req.runtime, "codex");
            assert!(req.skills.is_empty());
        }
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
        EngineServices::new(
            Arc::new(planning_runner()),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    )
}

#[tokio::test]
async fn planning_proposes_then_approve_seeds_the_contract_and_verifies() {
    let dir = tempfile::tempdir().unwrap();
    let engine = planning_engine(dir.path()).await;
    let id = engine
        .create_mission(
            &dir.path().to_string_lossy(),
            "make the tests pass",
            BASE_SHA,
            MissionConfig {
                stop: StopBar::Verified,
                planning: planning_dag(),
                recovery: Default::default(),
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
    assert!(state.contract.is_empty(), "no contract before approval");
    assert!(state.proposal.is_some(), "the author proposed a contract");
    let approve = state
        .open_attention
        .values()
        .find(|a| a.kind == AttentionKind::PlanProposal)
        .expect("parked on PlanProposal");

    // A malformed advance can't launder past approval: still no contract.
    engine.advance(&id).await.unwrap();
    assert!(engine.load_state(&id).await.unwrap().plan.is_none());

    // Approve: derive_promotion seeds the contract for the first time.
    engine
        .decide(&id, &approve.id, DecisionAction::Approve, "looks good")
        .await
        .unwrap();
    let state = engine.load_state(&id).await.unwrap();
    assert!(state.plan.is_some(), "approval seeds the plan");
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
async fn revising_a_proposal_rejects_it_and_re_runs_planning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = planning_engine(dir.path()).await;
    let id = engine
        .create_mission(
            &dir.path().to_string_lossy(),
            "make the tests pass",
            BASE_SHA,
            MissionConfig {
                stop: StopBar::Verified,
                planning: planning_dag(),
                recovery: Default::default(),
                terminal_review: None,
            },
        )
        .await
        .unwrap();
    engine.advance(&id).await.unwrap();
    let state = engine.load_state(&id).await.unwrap();
    let approve = state
        .open_attention
        .values()
        .find(|a| a.kind == AttentionKind::PlanProposal)
        .expect("parked on PlanProposal");

    // Revise: the proposal is kept as the latest rejected candidate and the
    // planning DAG is re-runnable. Nothing was seeded, so nothing is weakened.
    engine
        .decide(&id, &approve.id, DecisionAction::Revise, "not good enough")
        .await
        .unwrap();
    let state = engine.load_state(&id).await.unwrap();
    assert!(state.proposal.is_none());
    assert_eq!(
        state.planning_input.latest_rejected_proposal.as_ref(),
        Some(&proposed_plan())
    );
    assert_eq!(
        state.planning_input.refinement.as_ref(),
        Some(&PlanningRefinement::Guidance("not good enough".to_string()))
    );
    assert!(state.plan.is_none(), "still no contract after a rejection");
    assert!(state.contract.is_empty());
    assert_eq!(
        state.phase,
        MissionPhase::Planning,
        "planning is re-runnable"
    );
    for runtime in state.planning.tasks.values() {
        assert_eq!(runtime.status, TaskStatus::Pending);
        assert!(runtime.last_report.is_none());
        assert!(runtime.last_failure.is_none());
        assert!(runtime.feedback.is_empty());
    }

    engine.advance(&id).await.unwrap();
    let events = engine.store().load(&id).await.unwrap();
    let second_strategist_prompt = events
        .iter()
        .find_map(|envelope| match &envelope.event {
            MissionEvent::RoleRunRequested {
                task_id,
                attempt_no: 2,
                prompt,
                ..
            } if task_id == &tid("strategist") => Some(prompt),
            _ => None,
        })
        .expect("second strategist prompt");
    let prompt = engine
        .store()
        .blobs()
        .resolve(second_strategist_prompt)
        .unwrap();
    assert!(prompt.contains("Required rework"));
    assert!(prompt.contains("not good enough"));
    assert!(prompt.contains("Latest rejected plan candidate"));
    assert!(prompt.contains("make it pass"));
}

#[tokio::test]
async fn ratification_can_revise_a_to_b_to_c_and_then_approve() {
    let dir = tempfile::tempdir().unwrap();
    let engine = planning_engine(dir.path()).await;
    let id = engine
        .create_mission(
            &dir.path().to_string_lossy(),
            "make the tests pass",
            BASE_SHA,
            MissionConfig {
                stop: StopBar::Verified,
                planning: planning_dag(),
                recovery: Default::default(),
                terminal_review: None,
            },
        )
        .await
        .unwrap();

    engine.propose_plan(&id, candidate("fix-a")).await.unwrap();
    engine.propose_plan(&id, candidate("fix-a2")).await.unwrap();
    let state = engine.load_state(&id).await.unwrap();
    assert_eq!(state.proposal.as_ref(), Some(&candidate("fix-a2")));
    assert!(state.planning_input.latest_rejected_proposal.is_none());
    assert!(state.planning_input.refinement.is_none());
    engine
        .decide(
            &id,
            "plan_proposal:mission",
            DecisionAction::Revise,
            "A needs narrower tasks",
        )
        .await
        .unwrap();
    let state = engine.load_state(&id).await.unwrap();
    assert_eq!(
        state.planning_input.latest_rejected_proposal.as_ref(),
        Some(&candidate("fix-a2"))
    );
    assert_eq!(
        state.planning_input.refinement.as_ref(),
        Some(&PlanningRefinement::Guidance(
            "A needs narrower tasks".to_string()
        ))
    );
    assert!(state.proposal.is_none());
    assert!(state.plan.is_none());
    assert_eq!(state.planning_base_revision, Some(0));

    engine.propose_plan(&id, candidate("fix-b")).await.unwrap();
    let state = engine.load_state(&id).await.unwrap();
    assert_eq!(state.proposal.as_ref(), Some(&candidate("fix-b")));
    assert_eq!(
        state.planning_input.latest_rejected_proposal.as_ref(),
        Some(&candidate("fix-a2"))
    );
    assert_eq!(
        state.planning_input.refinement.as_ref(),
        Some(&PlanningRefinement::Guidance(
            "A needs narrower tasks".to_string()
        ))
    );

    engine
        .decide(
            &id,
            "plan_proposal:mission",
            DecisionAction::Revise,
            "B missed the oracle",
        )
        .await
        .unwrap();
    let state = engine.load_state(&id).await.unwrap();
    assert_eq!(
        state.planning_input.latest_rejected_proposal.as_ref(),
        Some(&candidate("fix-b"))
    );
    assert_eq!(
        state.planning_input.refinement.as_ref(),
        Some(&PlanningRefinement::Guidance(
            "B missed the oracle".to_string()
        ))
    );
    assert!(state.proposal.is_none());
    assert_eq!(state.planning_base_revision, Some(0));

    engine.propose_plan(&id, candidate("fix-c")).await.unwrap();
    engine
        .decide(
            &id,
            "plan_proposal:mission",
            DecisionAction::Approve,
            "C is acceptable",
        )
        .await
        .unwrap();
    let state = engine.load_state(&id).await.unwrap();
    assert_eq!(state.revision, 1);
    assert_eq!(state.plan.as_ref(), Some(&candidate("fix-c").plan));
    assert!(state.proposal.is_none());
    assert!(state.planning_input.latest_rejected_proposal.is_none());
    assert!(state.planning_input.refinement.is_none());
}

#[tokio::test]
async fn ratification_revisions_are_unbounded_and_keep_only_the_newest_input() {
    let dir = tempfile::tempdir().unwrap();
    let engine = planning_engine(dir.path()).await;
    let id = engine
        .create_mission(
            &dir.path().to_string_lossy(),
            "make the tests pass",
            BASE_SHA,
            MissionConfig {
                stop: StopBar::Verified,
                planning: planning_dag(),
                recovery: Default::default(),
                terminal_review: None,
            },
        )
        .await
        .unwrap();

    for i in 0..6 {
        let proposal = candidate(&format!("fix-{i}"));
        engine.propose_plan(&id, proposal.clone()).await.unwrap();
        let guidance = format!("feedback-{i}");
        engine
            .decide(
                &id,
                "plan_proposal:mission",
                DecisionAction::Revise,
                &guidance,
            )
            .await
            .unwrap();
        let state = engine.load_state(&id).await.unwrap();
        assert_eq!(
            state.planning_input.latest_rejected_proposal.as_ref(),
            Some(&proposal)
        );
        assert_eq!(
            state.planning_input.refinement.as_ref(),
            Some(&PlanningRefinement::Guidance(guidance))
        );
        assert!(state.proposal.is_none());
        assert_eq!(state.planning_base_revision, Some(0));
        assert!(state.planning.tasks.values().all(|runtime| {
            runtime.status == TaskStatus::Pending && runtime.last_report.is_none()
        }));
    }

    let events = engine.store().load(&id).await.unwrap();
    for i in 0..6 {
        assert!(events.iter().any(|envelope| matches!(
            &envelope.event,
            MissionEvent::DecisionRecorded { justification, .. } if justification == &format!("feedback-{i}")
        )));
    }
}

#[tokio::test]
async fn revise_guidance_preserves_whitespace_verbatim() {
    let dir = tempfile::tempdir().unwrap();
    let engine = planning_engine(dir.path()).await;
    let id = engine
        .create_mission(
            &dir.path().to_string_lossy(),
            "make the tests pass",
            BASE_SHA,
            MissionConfig {
                stop: StopBar::Verified,
                planning: planning_dag(),
                recovery: Default::default(),
                terminal_review: None,
            },
        )
        .await
        .unwrap();
    engine
        .propose_plan(&id, candidate("fix-space"))
        .await
        .unwrap();

    let guidance = "  keep these exact bytes\n\t";
    engine
        .decide(
            &id,
            "plan_proposal:mission",
            DecisionAction::Revise,
            guidance,
        )
        .await
        .unwrap();
    let state = engine.load_state(&id).await.unwrap();
    assert_eq!(
        state.planning_input.refinement.as_ref(),
        Some(&PlanningRefinement::Guidance(guidance.to_string()))
    );
}

#[tokio::test]
async fn aborting_a_plan_proposal_records_the_generic_decision_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let engine = planning_engine(dir.path()).await;
    let id = engine
        .create_mission(
            &dir.path().to_string_lossy(),
            "make the tests pass",
            BASE_SHA,
            MissionConfig {
                stop: StopBar::Verified,
                planning: planning_dag(),
                recovery: Default::default(),
                terminal_review: None,
            },
        )
        .await
        .unwrap();
    engine
        .propose_plan(&id, candidate("fix-abort"))
        .await
        .unwrap();

    engine
        .decide(
            &id,
            "plan_proposal:mission",
            DecisionAction::Abort,
            "stop exactly here",
        )
        .await
        .unwrap();
    let state = engine.load_state(&id).await.unwrap();
    assert!(matches!(
        state.phase,
        MissionPhase::Aborted { ref reason } if reason == "stop exactly here"
    ));
    assert!(state.open_attention.is_empty());

    let events = engine.store().load(&id).await.unwrap();
    let tail: Vec<_> = events.iter().rev().take(2).collect();
    assert!(matches!(
        &tail[1].event,
        MissionEvent::DecisionRecorded {
            action: DecisionAction::Abort,
            justification,
            ..
        } if justification == "stop exactly here"
    ));
    assert!(matches!(
        &tail[0].event,
        MissionEvent::MissionAborted { reason } if reason == "stop exactly here"
    ));
    assert_eq!(tail[0].sequence_no, tail[1].sequence_no + 1);
}

/// A failed planning node must raise a *retryable* NodeFailed, never wedge the
/// mission.
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
        EngineServices::new(
            Arc::new(runner),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );
    let id = engine
        .create_mission(
            &dir.path().to_string_lossy(),
            "obj",
            BASE_SHA,
            MissionConfig {
                stop: StopBar::Verified,
                planning: planning_dag(),
                recovery: Default::default(),
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
        .decide(&id, &node_failed.id, DecisionAction::Retry, "try again")
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
        EngineServices::new(
            Arc::new(runner),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );
    let id = engine
        .create_mission(
            &dir.to_string_lossy(),
            "obj",
            BASE_SHA,
            MissionConfig {
                stop: StopBar::Verified,
                planning: planning_dag(),
                recovery: Default::default(),
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
/// become a approvable proposal — neither a done-with-no-proposal nor a proposal
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
    let invalid = Plan {
        requirements: vec![covered_requirement("CHECKABLE", "UNCHECKABLE")],
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
            proposal: Some(PlanProposal {
                base_revision: 0,
                plan: invalid,
            }),
            request_attention: false,
        },
    )
    .await;
    assert_author_failed_seeding_nothing(&state);
}
