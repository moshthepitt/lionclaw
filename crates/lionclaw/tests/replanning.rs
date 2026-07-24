//! Complete-plan revision invariants. Replanning has one language: propose the
//! whole next plan against the current revision.

mod common;

use common::{
    approve_plan, blocking_gap, harness, harness_with_type, proposal, proposal_from_plan,
    review_mission_type, review_proposal, review_runner, simple_plan, ParseTask, BASE_SHA,
};
use lionclaw::engine::ProposeError;
use lionclaw::model::{
    Assertion, AssertionId, AssertionSupersession, DecisionAction, MissionEvent, PlanProposal,
    ProposalError, Requirement, RequirementDisposition, RequirementId, RequirementKind, Task,
    TaskStatus,
};
use lionclaw::testing::MockOracleRunner;

fn new_task(id: &str) -> Task {
    Task {
        id: id.parse_task(),
        body: format!("implement {id}"),
        targets: vec![AssertionId::new("TESTS-PASS").unwrap()],
        depends_on: vec![],
    }
}

fn casualty_plan(
    requirement: Requirement,
    assertion: Assertion,
    task_id: &str,
) -> lionclaw::model::Plan {
    let mut plan = simple_plan();
    plan.requirements.push(requirement);
    plan.assertions.push(assertion.clone());
    plan.tasks = vec![Task {
        id: task_id.parse_task(),
        body: format!("close {task_id} without waiving the objective"),
        targets: vec![AssertionId::new("TESTS-PASS").unwrap(), assertion.id],
        depends_on: vec![],
    }];
    plan
}

fn reviewer_requirement(id: &str, assertion: &str, prose: &str) -> Requirement {
    Requirement {
        id: RequirementId::new(id).unwrap(),
        kind: RequirementKind::Validation,
        prose: prose.into(),
        disposition: RequirementDisposition::ReviewerCheckable {
            assertion_ids: vec![AssertionId::new(assertion).unwrap()],
        },
    }
}

fn reviewer_assertion(id: &str, prose: &str) -> Assertion {
    Assertion {
        id: AssertionId::new(id).unwrap(),
        prose: prose.into(),
        oracle: None,
    }
}

async fn attested_harness() -> (tempfile::TempDir, common::TestHarness) {
    let dir = tempfile::tempdir().unwrap();
    let mut mission_type = common::test_mission_type();
    mission_type.edit_for_testing(|definition| {
        definition.stop = lionclaw::model::StopBar::Attested;
    });
    let h = harness_with_type(
        dir.path(),
        mission_type,
        review_runner(vec![(true, Vec::new())]),
        MockOracleRunner::exiting(0),
    )
    .await;
    (dir, h)
}

async fn test_harness() -> (tempfile::TempDir, common::TestHarness) {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(
        dir.path(),
        review_runner(vec![(true, Vec::new())]),
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
        .create_mission(dir.path().to_str().unwrap(), "revise", BASE_SHA)
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

    let mut stale_proposal = proposal(0, simple_plan());
    stale_proposal.team.as_mut().unwrap().revision = 2;
    let stale = h
        .engine
        .propose_plan(&id, stale_proposal)
        .await
        .unwrap_err();
    assert!(
        matches!(stale, ProposeError::Rejected(ProposalError::Stale { .. })),
        "{stale:?}"
    );

    let same = h
        .engine
        .propose_plan(&id, proposal(1, simple_plan()))
        .await
        .unwrap_err();
    assert!(
        matches!(same, ProposeError::Rejected(ProposalError::Immaterial)),
        "{same:?}"
    );
    assert_eq!(h.engine.load_state(&id).await.unwrap().head, head);
}

#[tokio::test]
async fn covered_requirement_weakening_requires_an_exact_recorded_decision() {
    let (_dir, h, id) = started().await;
    let mut removed = simple_plan();
    let changed_requirement = removed.requirements[0].id.clone();
    removed.requirements = vec![Requirement {
        id: RequirementId::new("NEW-SCOPE").unwrap(),
        kind: RequirementKind::Capability,
        prose: "new scope".into(),
        disposition: RequirementDisposition::ConfinedProvable {
            assertion_ids: vec![AssertionId::new("TESTS-PASS").unwrap()],
        },
    }];
    assert!(matches!(
        h.engine
            .propose_plan(&id, proposal(1, removed.clone()))
            .await,
        Err(ProposeError::Rejected(
            ProposalError::RequirementChangesMismatch { .. }
        ))
    ));

    let declared = PlanProposal {
        base_revision: 1,
        requirement_changes: vec![changed_requirement.clone()],
        assertion_supersessions: vec![],
        plan: removed,
    };
    h.engine
        .propose_plan(&id, proposal_from_plan(declared, false))
        .await
        .unwrap();
    approve_plan(&h.engine, &id).await;

    let events = h.engine.store().load(&id).await.unwrap();
    assert!(events.iter().any(|event| matches!(
        &event.event,
        MissionEvent::DecisionRecorded {
            action: DecisionAction::Approve,
            requirement_changes,
            ..
        } if requirement_changes == std::slice::from_ref(&changed_requirement)
    )));
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

    initial.requirements[1].disposition = RequirementDisposition::ConfinedProvable {
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
async fn host_acceptance_obligations_are_reported_and_do_not_become_confined_work() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(
        dir.path(),
        review_runner(vec![(true, Vec::new())]),
        MockOracleRunner::exiting(0),
    )
    .await;
    let id = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "host acceptance", BASE_SHA)
        .await
        .unwrap();
    let mut plan = simple_plan();
    plan.requirements.push(Requirement {
        id: RequirementId::new("LIVE-HOST-CHECK").unwrap(),
        kind: RequirementKind::Validation,
        prose: "operator verifies the deployed service on the host".into(),
        disposition: RequirementDisposition::HostAcceptance {
            rationale: "the confined runtime cannot observe the host deployment".into(),
        },
    });
    h.engine.propose_plan(&id, proposal(0, plan)).await.unwrap();
    approve_plan(&h.engine, &id).await;

    let outcome = h.engine.advance(&id).await.unwrap();
    assert!(matches!(
        outcome.state.phase,
        lionclaw::model::MissionPhase::Done {
            finish: lionclaw::model::FinishClass::Verified
        }
    ));
    assert_eq!(outcome.state.contract.len(), 1);

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_lionclaw"))
        .args([
            "mission",
            "report",
            id.as_str(),
            "--json",
            "--repo",
            dir.path().to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "report failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        report["host_acceptance_obligations"][0]["id"],
        serde_json::json!("LIVE-HOST-CHECK")
    );
    assert_eq!(
        report["host_acceptance_obligations"][0]["rationale"],
        serde_json::json!("the confined runtime cannot observe the host deployment")
    );
}

#[tokio::test]
async fn corrected_assertions_visibly_supersede_and_stale_prior_receipts() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness_with_type(
        dir.path(),
        review_mission_type(),
        review_runner(vec![(false, vec![blocking_gap()])]),
        MockOracleRunner::exiting(0),
    )
    .await;
    let id = h
        .engine
        .create_mission(dir.path().to_str().unwrap(), "revise", BASE_SHA)
        .await
        .unwrap();
    h.engine
        .propose_plan(&id, review_proposal(0, simple_plan()))
        .await
        .unwrap();
    approve_plan(&h.engine, &id).await;
    h.engine.advance(&id).await.unwrap();
    let state = h.engine.load_state(&id).await.unwrap();
    assert!(state.contract[&AssertionId::new("TESTS-PASS").unwrap()]
        .last_authoritative
        .is_some());

    let mut changed = simple_plan();
    changed.assertions[0].prose = "different claim".into();
    assert!(matches!(
        h.engine
            .propose_plan(&id, proposal(1, changed.clone()))
            .await,
        Err(ProposeError::Rejected(
            ProposalError::AssertionSupersessionsMismatch { .. }
        ))
    ));

    let assertion_id = changed.assertions[0].id.clone();
    h.engine
        .propose_plan(
            &id,
            proposal_from_plan(
                PlanProposal {
                    base_revision: 1,
                    requirement_changes: vec![],
                    assertion_supersessions: vec![AssertionSupersession {
                        assertion_id: assertion_id.clone(),
                        replacement_ids: vec![assertion_id.clone()],
                    }],
                    plan: changed,
                },
                true,
            ),
        )
        .await
        .unwrap();
    approve_plan(&h.engine, &id).await;

    let state = h.engine.load_state(&id).await.unwrap();
    let active = &state.contract[&assertion_id];
    assert!(active.last_authoritative.is_none());
    let retired = state.superseded_assertions.last().unwrap();
    assert_eq!(retired.assertion.prose, "cargo test exits 0");
    assert!(retired.state.last_authoritative.is_some());
    assert_eq!(retired.replacement_ids, vec![assertion_id]);
    assert_eq!(retired.superseded_at_revision, 2);
}

#[tokio::test]
async fn dogfooded_casualty_shapes_have_legal_intent_preserving_revisions() {
    struct Case {
        objective: &'static str,
        requirement_id: &'static str,
        assertion_id: &'static str,
        initial_requirement: &'static str,
        initial_assertion: &'static str,
        revised_requirement: &'static str,
        revised_assertion: Option<&'static str>,
        host_obligation: Option<&'static str>,
    }

    let cases = [
        Case {
            objective: "host proof is reclassified instead of assigned to confined work",
            requirement_id: "HOST-LIVE-PROOF",
            assertion_id: "LIVE-RUNTIME-OBSERVED",
            initial_requirement: "confined worker proves the live host runtime behaved correctly",
            initial_assertion: "the confined worker observed the live host runtime",
            revised_requirement: "operator confirms live runtime behavior after apply",
            revised_assertion: None,
            host_obligation: Some(
                "requires host acceptance after apply because confinement cannot observe the live runtime",
            ),
        },
        Case {
            objective: "one-transition history mistake is corrected in mission",
            requirement_id: "FORWARD-HISTORY",
            assertion_id: "ONE-TRANSITION-HISTORY",
            initial_requirement: "deliver the change as one reconstructed transition",
            initial_assertion: "the task workspace contains exactly one transition from base to head",
            revised_requirement: "deliver forward-only history that descends from the assigned base",
            revised_assertion: Some(
                "the delivered head descends from the assigned base without rewriting retained work",
            ),
            host_obligation: None,
        },
        Case {
            objective: "simultaneously current conversation wording is corrected in mission",
            requirement_id: "CURRENT-CONVERSATION",
            assertion_id: "SIMULTANEOUS-CURRENT-CONVERSATIONS",
            initial_requirement: "prove all simultaneously current conversations are identical",
            initial_assertion: "multiple conversations are simultaneously current and inspectable",
            revised_requirement: "prove only the active team-owned conversation remains messageable",
            revised_assertion: Some("the active team-owned conversation is the only messageable conversation"),
            host_obligation: None,
        },
    ];

    for case in cases {
        let (_dir, h) = attested_harness().await;
        let id = h
            .engine
            .create_mission("/repo", case.objective, BASE_SHA)
            .await
            .unwrap();
        let initial_requirement = reviewer_requirement(
            case.requirement_id,
            case.assertion_id,
            case.initial_requirement,
        );
        let initial_assertion = reviewer_assertion(case.assertion_id, case.initial_assertion);
        h.engine
            .propose_plan(
                &id,
                proposal(
                    0,
                    casualty_plan(initial_requirement, initial_assertion, "fix"),
                ),
            )
            .await
            .unwrap();
        approve_plan(&h.engine, &id).await;

        let requirement_id = RequirementId::new(case.requirement_id).unwrap();
        let assertion_id = AssertionId::new(case.assertion_id).unwrap();
        let mut revised = simple_plan();
        revised.requirements.push(Requirement {
            id: requirement_id.clone(),
            kind: RequirementKind::Validation,
            prose: case.revised_requirement.into(),
            disposition: match case.host_obligation {
                Some(rationale) => RequirementDisposition::HostAcceptance {
                    rationale: rationale.into(),
                },
                None => RequirementDisposition::ReviewerCheckable {
                    assertion_ids: vec![assertion_id.clone()],
                },
            },
        });
        if let Some(assertion_prose) = case.revised_assertion {
            revised.assertions.push(Assertion {
                id: assertion_id.clone(),
                prose: assertion_prose.into(),
                oracle: None,
            });
        }
        revised.tasks = vec![Task {
            id: "fix2".parse_task(),
            body: "continue with the corrected contract".into(),
            targets: revised
                .assertions
                .iter()
                .map(|assertion| assertion.id.clone())
                .collect(),
            depends_on: vec![],
        }];
        let replacements = if case.revised_assertion.is_some() {
            vec![assertion_id.clone()]
        } else {
            vec![AssertionId::new("TESTS-PASS").unwrap()]
        };
        h.engine
            .propose_plan(
                &id,
                proposal_from_plan(
                    PlanProposal {
                        base_revision: 1,
                        requirement_changes: vec![requirement_id.clone()],
                        assertion_supersessions: vec![AssertionSupersession {
                            assertion_id: assertion_id.clone(),
                            replacement_ids: replacements,
                        }],
                        plan: revised,
                    },
                    false,
                ),
            )
            .await
            .unwrap();
        approve_plan(&h.engine, &id).await;

        let state = h.engine.load_state(&id).await.unwrap();
        assert_eq!(state.revision, 2, "{}", case.objective);
        assert!(state
            .contract
            .contains_key(&AssertionId::new("TESTS-PASS").unwrap()));
        assert!(
            case.revised_assertion.is_some() == state.contract.contains_key(&assertion_id),
            "{}",
            case.objective
        );
        let retired = state.superseded_assertions.last().unwrap();
        assert_eq!(retired.assertion.id, assertion_id);
        assert_eq!(retired.superseded_at_revision, 2);
        let events = h.engine.store().load(&id).await.unwrap();
        assert!(events.iter().any(|event| matches!(
            &event.event,
            MissionEvent::DecisionRecorded {
                action: DecisionAction::Approve,
                requirement_changes,
                ..
            } if requirement_changes == std::slice::from_ref(&requirement_id)
        )));
    }
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
        .propose_plan(&id, {
            let mut proposal = proposal_from_plan(
                PlanProposal {
                    base_revision: 1,
                    requirement_changes: vec![],
                    assertion_supersessions: vec![],
                    plan: simple_plan(),
                },
                false,
            );
            proposal.team.as_mut().unwrap().revision = 1;
            proposal
        })
        .await
        .unwrap_err();
    assert!(
        matches!(error, ProposeError::Rejected(ProposalError::Stale { .. })),
        "{error:?}"
    );
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
