//! Planning-in-phase, end to end: an objective drives a contract-free planning
//! DAG (research → adversary → author), the author's proposal is gradeless and
//! parks for approval, and only a human `approve` seeds the contract — after
//! which execution runs and an oracle mints the `Verified` finish.

mod common;

use lionclaw_runtime_api::TypedFailure;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use common::{covered_requirement, BASE_SHA};
use lionclaw::engine::{Engine, EngineServices};
use lionclaw::mission_type::{MissionType, MissionTypeDefinition, RoleDefinition, SkillPackage};
use lionclaw::model::{
    Assertion, AssertionId, AttentionKind, ConversationLifecycle, DecisionAction, Handoff,
    MissionEvent, MissionPhase, OracleName, OutputSemantics, PayloadRef, Plan, PlanProposal,
    PlanningDag, PlanningRefinement, PlanningTask, RoleName, StopBar, Task, TaskKind, TaskStatus,
};
use lionclaw::ports::{CapturedArtifact, RoleRunOutcome, RoleRunRequest};
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

fn markdown_section<'a>(prompt: &'a str, heading: &str) -> &'a str {
    let marker = format!("## {heading}\n\n");
    prompt
        .split_once(&marker)
        .unwrap_or_else(|| panic!("missing prompt section '{heading}'"))
        .1
        .split("\n\n## ")
        .next()
        .expect("section body")
}

fn role(name: &str, output: OutputSemantics) -> RoleDefinition {
    RoleDefinition {
        name: rn(name),
        output,
        runtime: None,
        timeout_secs: None,
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
    MissionType::for_testing(MissionTypeDefinition {
        name: "planning-test".to_string(),
        stop: StopBar::Verified,
        image: "img".to_string(),
        environment: BTreeMap::new(),
        planning: planning_dag(),
        recovery: Default::default(),
        execution: Default::default(),
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
    })
}

fn planning_dag() -> PlanningDag {
    PlanningDag {
        tasks: vec![
            PlanningTask {
                id: tid("strategist"),
                role: rn("strategist"),
                output: OutputSemantics::ProducesReport,
                body: "draft".to_string(),
                depends_on: vec![],
            },
            PlanningTask {
                id: tid("red-team"),
                role: rn("red-team"),
                output: OutputSemantics::ProducesReport,
                body: "critique".to_string(),
                depends_on: vec![tid("strategist")],
            },
            PlanningTask {
                id: tid("author"),
                role: rn("author"),
                output: OutputSemantics::ProposesPlan,
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
        requirement_changes: vec![],
        assertion_supersessions: vec![],
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

fn expanded_candidate(base_revision: u32) -> PlanProposal {
    let mut proposal = proposed_plan();
    proposal.base_revision = base_revision;
    proposal
        .plan
        .requirements
        .push(covered_requirement("EXTRA-WORK", "EXTRA-HOLDS"));
    proposal.plan.assertions.push(Assertion {
        id: aid("EXTRA-HOLDS"),
        prose: "the additional behavior works".to_string(),
        oracle: Some(OracleName::new("cargo-test").unwrap()),
    });
    proposal.plan.tasks.push(Task {
        id: tid("extra"),
        kind: TaskKind::Work,
        body: "implement the additional behavior".to_string(),
        targets: vec![aid("EXTRA-HOLDS")],
        role: Some(rn("implementer")),
        depends_on: vec![],
    });
    proposal
}

/// A role runner that answers per output semantics: reports for the planners,
/// a plan proposal for the author, a committed artifact for the implementer.
fn successful_role_outcome(req: &RoleRunRequest) -> RoleRunOutcome {
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
    let artifact = (req.role.output == OutputSemantics::ProducesArtifact)
        .then(|| CapturedArtifact::for_testing(req.base_sha.clone(), "head-1"));
    RoleRunOutcome {
        handoff: Some(handoff),
        artifact,
        runtime_configuration: Default::default(),
        final_response: String::new(),
    }
}

fn planning_runner() -> MockRoleRunner {
    MockRoleRunner::new(Box::new(|req| Ok(successful_role_outcome(req))))
}

async fn planning_engine_with_runner(
    workspace: &std::path::Path,
    runner: MockRoleRunner,
) -> Engine {
    common::initialize_repository(workspace);
    let store = MissionStore::open(workspace).await.expect("store");
    Engine::new(
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
    )
}

async fn planning_engine(workspace: &std::path::Path) -> Engine {
    planning_engine_with_runner(workspace, planning_runner()).await
}

async fn advance_through_checkpoints(
    engine: &Engine,
    id: &lionclaw::model::MissionId,
) -> lionclaw::engine::MissionView {
    for _ in 0..32 {
        let view = engine.advance(id).await.unwrap();
        if view.disposition != lionclaw::engine::MissionDisposition::Ready {
            return view;
        }
    }
    panic!("planning test exceeded checkpoint bound")
}

#[tokio::test]
async fn mission_creation_persists_the_pinned_planning_contract() {
    let dir = tempfile::tempdir().unwrap();
    let engine = planning_engine(dir.path()).await;
    let id = engine
        .create_mission(
            &dir.path().to_string_lossy(),
            "make the tests pass",
            BASE_SHA,
        )
        .await
        .expect("create mission");
    let state = engine.load_state(&id).await.expect("state");

    assert_eq!(state.config, engine.mission_type().mission_config());
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
        )
        .await
        .unwrap();

    // Drive the planning DAG: strategist → red-team → author → park on the
    // proposal. No contract exists yet — the proposal is gradeless.
    advance_through_checkpoints(&engine, &id).await;
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
    advance_through_checkpoints(&engine, &id).await;
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
    advance_through_checkpoints(&engine, &id).await;
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
        )
        .await
        .unwrap();
    advance_through_checkpoints(&engine, &id).await;
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

    let replanned = advance_through_checkpoints(&engine, &id).await;
    assert_eq!(
        replanned.disposition,
        lionclaw::engine::MissionDisposition::Parked,
        "unexpected replanning conversations: {:?}",
        replanned.state.conversations
    );
    let events = engine.store().load(&id).await.unwrap();
    let strategist_effects: Vec<_> = events
        .iter()
        .filter_map(|envelope| match &envelope.event {
            MissionEvent::RoleRunRequested {
                task_id,
                assignment_epoch,
                effect_id,
                ..
            } if task_id == &tid("strategist") => Some((*assignment_epoch, effect_id)),
            _ => None,
        })
        .collect();
    assert_eq!(strategist_effects.len(), 2);
    assert_eq!(strategist_effects[0].0, 1);
    assert_eq!(strategist_effects[1].0, 2);
    assert_ne!(strategist_effects[0].1, strategist_effects[1].1);
    assert_eq!(replanned.state.planning_generation, 2);
    let strategist_conversations: Vec<_> = replanned
        .state
        .conversations
        .iter()
        .filter(|(_, conversation)| conversation.task_id == tid("strategist"))
        .collect();
    assert_eq!(strategist_conversations.len(), 2);
    assert_ne!(strategist_conversations[0].0, strategist_conversations[1].0);
    let old = strategist_conversations
        .iter()
        .find(|(_, conversation)| conversation.assignment_epoch == 1)
        .expect("first planning generation");
    let replacement = strategist_conversations
        .iter()
        .find(|(_, conversation)| conversation.assignment_epoch == 2)
        .expect("replacement planning generation");
    assert_eq!(old.1.lifecycle, ConversationLifecycle::Retired);
    assert_ne!(old.0, replacement.0);
    let second_strategist_effect = strategist_effects[1].1;
    let prompt = engine
        .reconstruct_role_prompt_for_testing(&id, second_strategist_effect)
        .await
        .unwrap();
    assert_eq!(
        markdown_section(&prompt, "Active planning input"),
        "### Human guidance\n\nnot good enough"
    );
    let rejected = markdown_section(&prompt, "Latest rejected plan candidate");
    assert!(rejected.contains("\"base_revision\": 0"));
    assert!(rejected.contains("make it pass"));
}

#[tokio::test]
async fn replanning_prompt_combines_the_accepted_plan_rejected_candidate_and_guidance() {
    let dir = tempfile::tempdir().unwrap();
    let engine = planning_engine(dir.path()).await;
    let id = engine
        .create_mission(
            &dir.path().to_string_lossy(),
            "make the tests pass",
            BASE_SHA,
        )
        .await
        .unwrap();

    engine.propose_plan(&id, proposed_plan()).await.unwrap();
    engine
        .decide(
            &id,
            "plan_proposal:mission",
            DecisionAction::Approve,
            "initial plan is sound",
        )
        .await
        .unwrap();
    let rejected = expanded_candidate(1);
    engine.propose_plan(&id, rejected.clone()).await.unwrap();
    engine
        .decide(
            &id,
            "plan_proposal:mission",
            DecisionAction::Revise,
            "keep the new requirement but use one coherent task",
        )
        .await
        .unwrap();

    advance_through_checkpoints(&engine, &id).await;
    let events = engine.store().load(&id).await.unwrap();
    let effect_id = events
        .iter()
        .find_map(|envelope| match &envelope.event {
            MissionEvent::RoleRunRequested {
                task_id, effect_id, ..
            } if task_id == &tid("strategist") => Some(effect_id),
            _ => None,
        })
        .expect("strategist prompt");
    let prompt = engine
        .reconstruct_role_prompt_for_testing(&id, effect_id)
        .await
        .unwrap();

    let accepted = markdown_section(&prompt, "Current accepted plan");
    assert!(accepted.contains("TESTS-PASS"));
    assert!(!accepted.contains("EXTRA-HOLDS"));
    let rejected_section = markdown_section(&prompt, "Latest rejected plan candidate");
    assert!(rejected_section.contains("\"base_revision\": 1"));
    assert!(rejected_section.contains("EXTRA-HOLDS"));
    assert_eq!(
        markdown_section(&prompt, "Active planning input"),
        "### Human guidance\n\nkeep the new requirement but use one coherent task"
    );
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
async fn successful_refinement_cycles_do_not_consume_the_recovery_budget() {
    let dir = tempfile::tempdir().unwrap();
    let strategist_calls = Arc::new(Mutex::new(0_u32));
    let seen = strategist_calls.clone();
    let runner = MockRoleRunner::new(Box::new(move |request| {
        if request.role.name.as_str() == "strategist" {
            let mut calls = seen.lock().unwrap();
            *calls += 1;
            if *calls == 4 {
                return Err(TypedFailure::transient(
                    "runtime.fixture",
                    "temporary provider timeout".to_string(),
                    None,
                ));
            }
        }
        Ok(successful_role_outcome(request))
    }));
    let engine = planning_engine_with_runner(dir.path(), runner).await;
    let id = engine
        .create_mission(
            &dir.path().to_string_lossy(),
            "refine without spending recovery",
            BASE_SHA,
        )
        .await
        .unwrap();

    for _ in 0..3 {
        advance_through_checkpoints(&engine, &id).await;
        engine
            .decide(
                &id,
                "plan_proposal:mission",
                DecisionAction::Revise,
                "repeat the same refinement",
            )
            .await
            .unwrap();
    }

    let view = advance_through_checkpoints(&engine, &id).await;
    assert_eq!(view.state.phase, MissionPhase::AttentionNeeded);
    assert!(view.state.proposal.is_some());
    assert_eq!(*strategist_calls.lock().unwrap(), 5);
    assert_eq!(view.state.planning_generation, 4);
    assert_eq!(view.state.planning.tasks[&tid("strategist")].attempts, 2);
    assert_eq!(
        view.state.planning.tasks[&tid("strategist")].consecutive_failures,
        0
    );
    let strategist_effects: Vec<_> = engine
        .store()
        .load(&id)
        .await
        .unwrap()
        .iter()
        .filter_map(|event| match &event.event {
            MissionEvent::RoleRunRequested {
                task_id, effect_id, ..
            } if task_id == &tid("strategist") => Some(effect_id.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        strategist_effects
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
            .len(),
        strategist_effects.len(),
        "even identical feedback produces a fresh generation identity"
    );
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
async fn aborting_a_plan_proposal_uses_the_universal_abort_fact() {
    let dir = tempfile::tempdir().unwrap();
    let engine = planning_engine(dir.path()).await;
    let id = engine
        .create_mission(
            &dir.path().to_string_lossy(),
            "make the tests pass",
            BASE_SHA,
        )
        .await
        .unwrap();
    engine
        .propose_plan(&id, candidate("fix-abort"))
        .await
        .unwrap();

    engine.abort(&id, "stop exactly here").await.unwrap();
    let state = engine.load_state(&id).await.unwrap();
    assert!(matches!(
        state.phase,
        MissionPhase::Aborted { ref reason } if reason == "stop exactly here"
    ));
    assert!(state.open_attention.is_empty());

    let events = engine.store().load(&id).await.unwrap();
    let tail = events.last().expect("abort event");
    assert!(matches!(
        &tail.event,
        MissionEvent::MissionAborted { reason } if reason == "stop exactly here"
    ));
    assert!(!events
        .iter()
        .any(|event| matches!(event.event, MissionEvent::DecisionRecorded { .. })));
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
            return Err(TypedFailure::transient(
                "runtime.fixture",
                "crashed mid-planning".to_string(),
                None,
            ));
        }
        Ok(RoleRunOutcome {
            handoff: Some(Handoff::Work {
                done: true,
                report: PayloadRef::inline("report"),
                request_attention: false,
            }),
            artifact: None,
            runtime_configuration: Default::default(),
            final_response: String::new(),
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
        .create_mission(&dir.path().to_string_lossy(), "obj", BASE_SHA)
        .await
        .unwrap();

    advance_through_checkpoints(&engine, &id).await;
    let state = engine.load_state(&id).await.unwrap();
    assert_eq!(state.phase, MissionPhase::AttentionNeeded);
    let node_failed = state
        .open_attention
        .values()
        .find(|a| a.kind == AttentionKind::NodeFailed)
        .expect("a failed planning node raises NodeFailed, not a wedge");
    assert_eq!(node_failed.task_id.as_ref(), Some(&tid("strategist")));
    assert!(
        node_failed.failure.is_some(),
        "typed failure evidence is exact"
    );
    let failed_generation = state.planning_generation;

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
    advance_through_checkpoints(&engine, &id).await;
    let state = engine.load_state(&id).await.unwrap();

    // Replanning retires the failed assignment generation. The next planning
    // pass receives the exact failure plus lead feedback, and cannot dispatch
    // the failed assignment again under its old epoch.
    let node_failed = state
        .open_attention
        .values()
        .find(|a| a.kind == AttentionKind::NodeFailed)
        .expect("retrying the deterministic fixture fails again");
    engine
        .decide(
            &id,
            &node_failed.id,
            DecisionAction::Revise,
            "replace the failed planning approach",
        )
        .await
        .unwrap();
    let state = engine.load_state(&id).await.unwrap();
    assert_eq!(state.planning_generation, failed_generation + 1);
    assert_eq!(
        state.planning.tasks[&tid("strategist")].status,
        TaskStatus::Pending
    );
    assert!(matches!(
        state.planning_input.refinement.as_ref(),
        Some(PlanningRefinement::FailureEvidence(feedback))
            if feedback.failure.is_some()
                && feedback.justification == "replace the failed planning approach"
    ));
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
            handoff: Some(handoff),
            artifact: None,
            runtime_configuration: Default::default(),
            final_response: String::new(),
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
        .create_mission(&dir.to_string_lossy(), "obj", BASE_SHA)
        .await
        .unwrap();
    advance_through_checkpoints(&engine, &id).await;
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
                requirement_changes: vec![],
                assertion_supersessions: vec![],
                plan: invalid,
            }),
            request_attention: false,
        },
    )
    .await;
    assert_author_failed_seeding_nothing(&state);
}
