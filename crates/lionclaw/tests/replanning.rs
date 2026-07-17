//! Complete-plan revision invariants. Replanning has one language: propose the
//! whole next plan against the current revision.

mod common;

use common::{approve_plan, harness, proposal, simple_plan, ParseTask, BASE_SHA, HEAD_SHA};
use lionclaw::engine::ProposeError;
use lionclaw::model::{
    Assertion, AssertionId, DecisionAction, OracleName, PlanProposal, ProposalError, Requirement,
    RequirementDisposition, RequirementId, RequirementKind, RoleName, Task, TaskKind, TaskStatus,
};
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};

fn new_task(id: &str) -> Task {
    Task {
        id: id.parse_task(),
        kind: TaskKind::Work,
        body: format!("implement {id}"),
        targets: vec![AssertionId::new("TESTS-PASS").unwrap()],
        role: Some(RoleName::new("implementer").unwrap()),
        depends_on: vec![],
    }
}

async fn test_harness() -> (tempfile::TempDir, common::TestHarness) {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(
        dir.path(),
        MockRoleRunner::happy(HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    (dir, h)
}

async fn started() -> (
    tempfile::TempDir,
    common::TestHarness,
    lionclaw::model::MissionId,
) {
    let (dir, h) = test_harness().await;
    let id = h
        .engine
        .create_mission("/repo", "revise", BASE_SHA)
        .await
        .unwrap();
    h.engine
        .propose_plan(&id, proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&h.engine, &id).await;
    (dir, h, id)
}

#[tokio::test]
async fn complete_revision_retires_omitted_tasks_and_seeds_new_tasks() {
    let (_dir, h, id) = started().await;
    let mut next = simple_plan();
    next.tasks = vec![new_task("fix2")];
    h.engine.propose_plan(&id, proposal(1, next)).await.unwrap();
    approve_plan(&h.engine, &id).await;

    let state = h.engine.load_state(&id).await.unwrap();
    assert_eq!(state.revision, 2);
    assert_eq!(
        state.tasks[&"fix".parse_task()].status,
        TaskStatus::Superseded
    );
    assert_eq!(
        state.tasks[&"fix2".parse_task()].status,
        TaskStatus::Pending
    );
    assert_eq!(state.plan.unwrap().tasks[0].id, "fix2".parse_task());
}

#[tokio::test]
async fn stale_and_immaterial_proposals_append_nothing() {
    let (_dir, h, id) = started().await;
    let head = h.engine.load_state(&id).await.unwrap().head;

    let stale = h
        .engine
        .propose_plan(&id, proposal(0, simple_plan()))
        .await
        .unwrap_err();
    assert!(matches!(
        stale,
        ProposeError::Rejected(ProposalError::Stale { .. })
    ));

    let same = h
        .engine
        .propose_plan(&id, proposal(1, simple_plan()))
        .await
        .unwrap_err();
    assert!(matches!(
        same,
        ProposeError::Rejected(ProposalError::Immaterial)
    ));
    assert_eq!(h.engine.load_state(&id).await.unwrap().head, head);
}

#[tokio::test]
async fn requirements_cannot_disappear_or_weaken() {
    let (_dir, h, id) = started().await;
    let mut removed = simple_plan();
    removed.requirements = vec![Requirement {
        id: RequirementId::new("NEW-SCOPE").unwrap(),
        kind: RequirementKind::Capability,
        prose: "new scope".into(),
        disposition: RequirementDisposition::Covered {
            assertion_ids: vec![AssertionId::new("TESTS-PASS").unwrap()],
        },
    }];
    assert!(matches!(
        h.engine.propose_plan(&id, proposal(1, removed)).await,
        Err(ProposeError::Rejected(
            ProposalError::RequirementWeakened { .. }
        ))
    ));

    let mut limited = simple_plan();
    limited.requirements[0].disposition = RequirementDisposition::Limitation {
        rationale: "too hard".into(),
    };
    assert!(matches!(
        h.engine.propose_plan(&id, proposal(1, limited)).await,
        Err(ProposeError::Rejected(
            ProposalError::RequirementWeakened { .. }
        ))
    ));
}

#[tokio::test]
async fn a_limitation_may_become_covered_but_not_the_reverse() {
    let (_dir, h) = test_harness().await;
    let id = h
        .engine
        .create_mission("/repo", "limitations", BASE_SHA)
        .await
        .unwrap();
    let mut initial = simple_plan();
    initial.requirements.push(Requirement {
        id: RequirementId::new("EXTRA-SCOPE").unwrap(),
        kind: RequirementKind::Constraint,
        prose: "extra scope".into(),
        disposition: RequirementDisposition::Limitation {
            rationale: "not yet supported".into(),
        },
    });
    h.engine
        .propose_plan(&id, proposal(0, initial.clone()))
        .await
        .unwrap();
    approve_plan(&h.engine, &id).await;

    initial.requirements[1].disposition = RequirementDisposition::Covered {
        assertion_ids: vec![AssertionId::new("TESTS-PASS").unwrap()],
    };
    initial.tasks = vec![new_task("fix2")];
    h.engine
        .propose_plan(&id, proposal(1, initial))
        .await
        .unwrap();
    approve_plan(&h.engine, &id).await;
}

#[tokio::test]
async fn assertions_are_preserved_and_oracle_binding_only_strengthens() {
    let (_dir, h, id) = started().await;
    let mut changed = simple_plan();
    changed.assertions[0].prose = "different claim".into();
    assert!(matches!(
        h.engine.propose_plan(&id, proposal(1, changed)).await,
        Err(ProposeError::Rejected(
            ProposalError::AssertionWeakened { .. }
        ))
    ));

    let mut rebound = simple_plan();
    rebound.assertions[0].oracle = Some(OracleName::new("other-oracle").unwrap());
    assert!(matches!(
        h.engine.propose_plan(&id, proposal(1, rebound)).await,
        Err(ProposeError::Rejected(
            ProposalError::AssertionWeakened { .. }
        ))
    ));
}

#[tokio::test]
async fn retained_task_ids_are_immutable_and_retired_ids_never_revive() {
    let (_dir, h, id) = started().await;
    let mut changed = simple_plan();
    changed.tasks[0].body = "quietly different".into();
    assert!(matches!(
        h.engine.propose_plan(&id, proposal(1, changed)).await,
        Err(ProposeError::Rejected(ProposalError::TaskChanged { .. }))
    ));

    let mut revision = simple_plan();
    revision.tasks = vec![new_task("fix2")];
    h.engine
        .propose_plan(&id, proposal(1, revision.clone()))
        .await
        .unwrap();
    approve_plan(&h.engine, &id).await;
    revision.tasks.push(new_task("fix"));
    assert!(matches!(
        h.engine.propose_plan(&id, proposal(2, revision)).await,
        Err(ProposeError::Rejected(ProposalError::TaskIdReused { .. }))
    ));
}

#[tokio::test]
async fn approval_policy_applies_to_every_revision() {
    let (_dir, h) = test_harness().await;
    let id = h
        .engine
        .create_mission("/repo", "approval", BASE_SHA)
        .await
        .unwrap();
    h.engine
        .propose_plan(&id, proposal(0, simple_plan()))
        .await
        .unwrap();
    let state = h.engine.load_state(&id).await.unwrap();
    assert_eq!(state.revision, 0);
    assert!(state.proposal.is_some());
    h.engine
        .decide(
            &id,
            "plan_proposal:mission",
            DecisionAction::Approve,
            "approved",
        )
        .await
        .unwrap();
    assert_eq!(h.engine.load_state(&id).await.unwrap().revision, 1);

    let mut next = simple_plan();
    next.tasks = vec![new_task("fix2")];
    h.engine.propose_plan(&id, proposal(1, next)).await.unwrap();
    let state = h.engine.load_state(&id).await.unwrap();
    assert_eq!(state.revision, 1);
    assert!(state.proposal.is_some());
}

#[tokio::test]
async fn initial_proposal_must_target_revision_zero() {
    let (_dir, h) = test_harness().await;
    let id = h
        .engine
        .create_mission("/repo", "initial", BASE_SHA)
        .await
        .unwrap();
    let error = h
        .engine
        .propose_plan(
            &id,
            PlanProposal {
                base_revision: 1,
                plan: simple_plan(),
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ProposeError::Rejected(ProposalError::Stale { .. })
    ));
}

#[test]
fn requirement_and_assertion_ids_are_distinct_types() {
    let requirement = RequirementId::new("OBJECTIVE-HOLDS").unwrap();
    let assertion = Assertion {
        id: AssertionId::new("BEHAVIOR-PROVEN").unwrap(),
        prose: "behavior is proven".into(),
        oracle: None,
    };
    assert_ne!(requirement.as_str(), assertion.id.as_str());
}
