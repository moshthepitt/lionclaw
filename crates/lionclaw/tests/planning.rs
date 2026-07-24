//! Team-owned planning, ratification, refinement, and recovery proofs.

mod common;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use common::{approve_plan, simple_plan, BASE_SHA, HEAD_SHA};
use lionclaw::engine::{Engine, MissionDisposition};
use lionclaw::model::{
    AttentionKind, DecisionAction, Handoff, MissionPhase, MissionProposal, OutputSemantics,
    PayloadRef, PlanningRefinement, RoleAttemptDisposition, ValidationItem,
};
use lionclaw::ports::{CapturedArtifact, RoleTurnOutcome, RoleTurnRequest};
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};
use lionclaw_runtime_api::TypedFailure;

fn candidate(task_id: &str) -> MissionProposal {
    candidate_at(task_id, 0)
}

fn candidate_at(task_id: &str, base_revision: u32) -> MissionProposal {
    let mut plan = simple_plan();
    plan.tasks[0].id = lionclaw::model::TaskId::new(task_id).unwrap();
    plan.tasks[0].body = format!("make {task_id} pass");
    common::proposal(base_revision, plan)
}

fn scripted_runner(
    proposals: Vec<MissionProposal>,
    fail_work: bool,
) -> (MockRoleRunner, Arc<Mutex<Vec<String>>>) {
    let fallback = proposals.last().cloned().expect("at least one proposal");
    let proposals = Mutex::new(VecDeque::from(proposals));
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let captured = prompts.clone();
    let runner = MockRoleRunner::new(Box::new(move |request: &RoleTurnRequest| {
        captured.lock().unwrap().push(request.prompt.clone());
        match request.role.output {
            OutputSemantics::ProposesPlan => {
                let proposal = proposals
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or_else(|| fallback.clone());
                Ok(RoleTurnOutcome {
                    handoff: Some(Handoff::Plan {
                        done: true,
                        report: PayloadRef::inline("proposed"),
                        proposal: Some(Box::new(proposal)),
                        request_attention: false,
                    }),
                    artifact: None,
                    runtime_configuration: Default::default(),
                    final_response: "proposed".to_string(),
                })
            }
            OutputSemantics::ProducesArtifact if fail_work => Err(TypedFailure::permanent(
                "runtime.fixture",
                "implementation failed",
            )),
            OutputSemantics::ProducesArtifact => Ok(RoleTurnOutcome {
                handoff: Some(Handoff::Work {
                    done: true,
                    report: PayloadRef::inline("fixed"),
                    request_attention: false,
                }),
                artifact: Some(CapturedArtifact::for_testing(
                    request.base_sha.clone(),
                    HEAD_SHA,
                )),
                runtime_configuration: Default::default(),
                final_response: "fixed".to_string(),
            }),
            OutputSemantics::EmitsVerdict => Ok(RoleTurnOutcome {
                handoff: Some(Handoff::Validate {
                    done: true,
                    report: PayloadRef::inline("reviewed"),
                    items: request
                        .assertion_ids
                        .iter()
                        .cloned()
                        .map(|item_id| ValidationItem {
                            item_id,
                            passed: true,
                        })
                        .collect(),
                    passed: true,
                    request_attention: false,
                }),
                artifact: None,
                runtime_configuration: Default::default(),
                final_response: "reviewed".to_string(),
            }),
            other => panic!("unexpected planning fixture output {other:?}"),
        }
    }));
    (runner, prompts)
}

async fn engine(
    dir: &std::path::Path,
    proposals: Vec<MissionProposal>,
    fail_work: bool,
) -> (Engine, Arc<Mutex<Vec<String>>>) {
    let (runner, prompts) = scripted_runner(proposals, fail_work);
    let harness = common::harness(dir, runner, MockOracleRunner::exiting(0)).await;
    (harness.engine, prompts)
}

async fn create(engine: &Engine, dir: &std::path::Path) -> lionclaw::model::MissionId {
    engine
        .create_mission(dir.to_str().unwrap(), "plan the work", BASE_SHA)
        .await
        .unwrap()
}

async fn proposal_attention(engine: &Engine, id: &lionclaw::model::MissionId) -> String {
    let state = engine.load_state(id).await.unwrap();
    state
        .open_attention
        .values()
        .find(|item| item.kind == AttentionKind::PlanProposal)
        .unwrap_or_else(|| {
            panic!(
                "plan proposal attention: phase={:?} attention={:?} proposal={:?} receipts={:?}",
                state.phase, state.open_attention, state.proposal, state.role_attempt_receipts
            )
        })
        .id
        .clone()
}

#[tokio::test]
async fn mission_creation_persists_the_pinned_planning_contract() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = engine(dir.path(), vec![candidate("a")], false).await;
    let id = create(&engine, dir.path()).await;
    let state = engine.load_state(&id).await.unwrap();
    let team = state.team.expect("revision-zero team");
    assert_eq!(team.revision, 0);
    assert_eq!(team.planning_assignment.as_str(), "strategist");
    assert_eq!(
        team.roles[&team.planning_assignment].output,
        OutputSemantics::ProposesPlan
    );
    assert!(state.plan.is_none());
    assert_eq!(state.phase, MissionPhase::Planning);
}

#[tokio::test]
async fn planning_proposes_then_approve_seeds_the_contract_and_verifies() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = engine(dir.path(), vec![candidate("fix")], false).await;
    let id = create(&engine, dir.path()).await;
    let parked = engine.advance(&id).await.unwrap();
    assert_eq!(parked.disposition, MissionDisposition::AwaitingLead);
    assert!(parked.state.plan.is_none());
    approve_plan(&engine, &id).await;
    let finished = engine.advance(&id).await.unwrap();
    assert_eq!(finished.disposition, MissionDisposition::Terminal);
    assert_eq!(finished.state.revision, 1);
    assert!(finished
        .state
        .contract
        .contains_key(&lionclaw::model::AssertionId::new("TESTS-PASS").unwrap()));
}

#[tokio::test]
async fn revising_a_proposal_rejects_it_and_re_runs_planning() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = engine(
        dir.path(),
        vec![candidate("candidate-a"), candidate("candidate-b")],
        false,
    )
    .await;
    let id = create(&engine, dir.path()).await;
    engine.advance(&id).await.unwrap();
    engine
        .decide(
            &id,
            &proposal_attention(&engine, &id).await,
            DecisionAction::Revise,
            "not good enough",
        )
        .await
        .unwrap();
    let state = engine.load_state(&id).await.unwrap();
    assert_eq!(state.phase, MissionPhase::Planning);
    assert_eq!(
        state.planning_input.refinement,
        Some(PlanningRefinement::Guidance("not good enough".to_string()))
    );
    assert!(state.planning_input.latest_rejected_proposal.is_some());
    engine.advance(&id).await.unwrap();
    let state = engine.load_state(&id).await.unwrap();
    assert_eq!(
        state
            .proposal
            .as_ref()
            .unwrap()
            .plan
            .as_ref()
            .unwrap()
            .plan
            .tasks[0]
            .id
            .as_str(),
        "candidate-b"
    );
}

#[tokio::test]
async fn replanning_prompt_combines_the_accepted_plan_rejected_candidate_and_guidance() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, prompts) = engine(
        dir.path(),
        vec![
            candidate("accepted"),
            candidate_at("rejected", 1),
            candidate_at("replacement", 1),
        ],
        true,
    )
    .await;
    let id = create(&engine, dir.path()).await;
    engine.advance(&id).await.unwrap();
    approve_plan(&engine, &id).await;
    let failed = engine.advance(&id).await.unwrap();
    let node = failed
        .state
        .open_attention
        .values()
        .find(|item| item.kind == AttentionKind::NodeFailed)
        .unwrap();
    engine
        .decide(
            &id,
            &node.id,
            DecisionAction::Revise,
            "replace the implementation approach",
        )
        .await
        .unwrap();
    engine.advance(&id).await.unwrap();
    engine
        .decide(
            &id,
            &proposal_attention(&engine, &id).await,
            DecisionAction::Revise,
            "tighten the replacement",
        )
        .await
        .unwrap();
    engine.advance(&id).await.unwrap();
    let planning_prompts: Vec<_> = prompts
        .lock()
        .unwrap()
        .iter()
        .filter(|prompt| prompt.contains("## Proposal base revision"))
        .cloned()
        .collect();
    let prompt = planning_prompts.last().unwrap();
    assert!(prompt.contains("## Current accepted plan"));
    assert!(prompt.contains("\"rejected\""));
    assert!(prompt.contains("tighten the replacement"));
}

#[tokio::test]
async fn ratification_can_revise_a_to_b_to_c_and_then_approve() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = engine(
        dir.path(),
        vec![candidate("a"), candidate("b"), candidate("c")],
        false,
    )
    .await;
    let id = create(&engine, dir.path()).await;
    for guidance in ["revise a", "revise b"] {
        engine.advance(&id).await.unwrap();
        engine
            .decide(
                &id,
                &proposal_attention(&engine, &id).await,
                DecisionAction::Revise,
                guidance,
            )
            .await
            .unwrap();
    }
    engine.advance(&id).await.unwrap();
    approve_plan(&engine, &id).await;
    assert_eq!(engine.load_state(&id).await.unwrap().revision, 1);
}

#[tokio::test]
async fn ratification_revisions_are_unbounded_and_keep_only_the_newest_input() {
    let dir = tempfile::tempdir().unwrap();
    let proposals = (0..12)
        .map(|index| candidate(&format!("candidate-{index}")))
        .collect();
    let (engine, _) = engine(dir.path(), proposals, false).await;
    let id = create(&engine, dir.path()).await;
    for index in 0..11 {
        engine.advance(&id).await.unwrap();
        let guidance = format!("guidance-{index}");
        engine
            .decide(
                &id,
                &proposal_attention(&engine, &id).await,
                DecisionAction::Revise,
                &guidance,
            )
            .await
            .unwrap();
        let state = engine.load_state(&id).await.unwrap();
        assert_eq!(
            state.planning_input.refinement,
            Some(PlanningRefinement::Guidance(guidance))
        );
        assert!(state
            .planning_input
            .latest_rejected_proposal
            .as_ref()
            .unwrap()
            .plan
            .as_ref()
            .unwrap()
            .plan
            .tasks[0]
            .id
            .as_str()
            .ends_with(&index.to_string()));
    }
}

#[tokio::test]
async fn successful_refinement_cycles_do_not_consume_the_recovery_budget() {
    let dir = tempfile::tempdir().unwrap();
    let proposals = (0..4)
        .map(|index| candidate(&format!("candidate-{index}")))
        .collect();
    let (engine, _) = engine(dir.path(), proposals, false).await;
    let id = create(&engine, dir.path()).await;
    for index in 0..3 {
        engine.advance(&id).await.unwrap();
        engine
            .decide(
                &id,
                &proposal_attention(&engine, &id).await,
                DecisionAction::Revise,
                &format!("revision {index}"),
            )
            .await
            .unwrap();
    }
    let state = engine.load_state(&id).await.unwrap();
    assert!(state.role_attempt_receipts.values().all(|receipt| matches!(
        receipt.disposition,
        RoleAttemptDisposition::Succeeded { .. }
    )));
    assert!(state.parked_effects.is_empty());
}

#[tokio::test]
async fn revise_guidance_preserves_whitespace_verbatim() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = engine(dir.path(), vec![candidate("a")], false).await;
    let id = create(&engine, dir.path()).await;
    engine.advance(&id).await.unwrap();
    let guidance = "  keep\n\nexact spacing  ";
    engine
        .decide(
            &id,
            &proposal_attention(&engine, &id).await,
            DecisionAction::Revise,
            guidance,
        )
        .await
        .unwrap();
    assert_eq!(
        engine
            .load_state(&id)
            .await
            .unwrap()
            .planning_input
            .refinement,
        Some(PlanningRefinement::Guidance(guidance.to_string()))
    );
}

#[tokio::test]
async fn aborting_a_plan_proposal_uses_the_universal_abort_fact() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = engine(dir.path(), vec![candidate("a")], false).await;
    let id = create(&engine, dir.path()).await;
    engine.advance(&id).await.unwrap();
    engine.abort(&id, "stop exactly here").await.unwrap();
    let state = engine.load_state(&id).await.unwrap();
    assert!(matches!(
        state.phase,
        MissionPhase::Aborted { ref reason } if reason == "stop exactly here"
    ));
    assert!(engine
        .store()
        .load(&id)
        .await
        .unwrap()
        .iter()
        .any(|event| matches!(
            &event.event,
            lionclaw::model::MissionEvent::MissionAborted { reason }
                if reason == "stop exactly here"
        )));
}

#[tokio::test]
async fn a_failed_planning_node_is_retryable_not_a_wedge() {
    let dir = tempfile::tempdir().unwrap();
    let runner = MockRoleRunner::new(Box::new(|request: &RoleTurnRequest| {
        assert_eq!(request.role.output, OutputSemantics::ProposesPlan);
        Err(TypedFailure::permanent(
            "runtime.fixture",
            "crashed mid-planning",
        ))
    }));
    let harness = common::harness(dir.path(), runner, MockOracleRunner::exiting(0)).await;
    let id = create(&harness.engine, dir.path()).await;
    let failed = harness.engine.advance(&id).await.unwrap();
    let node = failed
        .state
        .open_attention
        .values()
        .find(|item| item.kind == AttentionKind::NodeFailed)
        .expect("failed planner parks");
    let calls = harness.role_runner.calls.lock().unwrap().len();
    harness
        .engine
        .decide(&id, &node.id, DecisionAction::Retry, "try again")
        .await
        .unwrap();
    let failed_again = harness.engine.advance(&id).await.unwrap();
    assert!(failed_again
        .state
        .open_attention
        .values()
        .any(|item| item.kind == AttentionKind::NodeFailed));
    assert_eq!(harness.role_runner.calls.lock().unwrap().len(), calls + 1);
}

#[tokio::test]
async fn a_bad_author_proposal_fails_the_node_and_seeds_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut bad = candidate("bad");
    bad.plan.as_mut().unwrap().plan.tasks.clear();
    bad.team.as_mut().unwrap().task_assignments.clear();
    let (engine, _) = engine(dir.path(), vec![bad], false).await;
    let id = create(&engine, dir.path()).await;
    let failed = engine.advance(&id).await.unwrap();
    assert!(failed
        .state
        .open_attention
        .values()
        .any(|item| item.kind == AttentionKind::NodeFailed));
    assert!(failed.state.plan.is_none());
    assert!(failed.state.contract.is_empty());
    assert_eq!(failed.state.revision, 0);
}
