mod common;

use std::collections::{BTreeMap, BTreeSet};

use common::{
    approve_plan, default_runtime_identities, engine_with_runtime_identities, fault_append_events,
    initialize_repository, simple_plan, team, test_mission_type, BASE_SHA,
};
use lionclaw::engine::record_control;
use lionclaw::model::{
    next, resolve_execution_deadline_ms, AuthorityCeilings, ChildMissionAssignment,
    ChildMissionOutput, Choice, ControlAction, DecisionAction, EffectIntent, ExecutionPolicy,
    Handoff, MissionConfig, MissionEvent, MissionId, MissionProposal, OutputSemantics, PayloadRef,
    PlanProposal, RecoveryConfig, RoleInstanceId, TaskAssignment, TaskId, TaskStatus, TeamRevision,
};
use lionclaw::ports::RoleTurnOutcome;
use lionclaw::store::{MissionStore, NewEvent};
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};
use lionclaw_runtime_api::TypedFailure;

fn child_team(output: OutputSemantics, plan: &lionclaw::model::Plan) -> TeamRevision {
    let planner = common::role("child-planner", OutputSemantics::ProposesPlan);
    let mut worker = common::role("child-worker", output);
    worker.runtime = "codex".into();
    let reviewer = common::role("child-reviewer", OutputSemantics::EmitsVerdict);
    TeamRevision {
        revision: 0,
        roles: BTreeMap::from([
            (planner.id.clone(), planner),
            (worker.id.clone(), worker),
            (reviewer.id.clone(), reviewer),
        ]),
        planning_assignment: RoleInstanceId::new("child-planner").unwrap(),
        task_assignments: BTreeMap::from([(
            plan.tasks[0].id.clone(),
            RoleInstanceId::new("child-worker").unwrap().into(),
        )]),
        judgment_assignments: BTreeMap::from([(
            plan.assertions[0].id.clone(),
            vec![RoleInstanceId::new("child-reviewer").unwrap()],
        )]),
        gap_review_assignment: None,
        guidance: None,
    }
}

fn child_assignment(output: OutputSemantics) -> ChildMissionAssignment {
    let plan = simple_plan();
    ChildMissionAssignment {
        objective: "run the delegated proof-bearing task".into(),
        output,
        config: MissionConfig {
            stop: lionclaw::model::StopBar::Verified,
            ceilings: AuthorityCeilings {
                writes: output == OutputSemantics::ProducesArtifact,
                ..Default::default()
            },
            runtime_ceilings: BTreeSet::from(["codex".into()]),
            recovery: RecoveryConfig { max_attempts: 2 },
            execution: ExecutionPolicy {
                default_timeout_secs: 60,
                max_task_time_secs: 120,
                extension_step_secs: 30,
                effect_capacity: 1,
                max_child_depth: 3,
                max_descendants: 8,
                auto_continue_candidate: false,
                auto_continue_proof: false,
            },
            ..Default::default()
        },
        proposal: Box::new(MissionProposal {
            plan: Some(PlanProposal {
                base_revision: 0,
                requirement_changes: Vec::new(),
                assertion_supersessions: Vec::new(),
                plan: plan.clone(),
            }),
            team: Some(child_team(output, &plan)),
            oracles: Some(common::oracle_specs(&plan)),
        }),
        deadline_secs: 120,
    }
}

fn parent_proposal(output: OutputSemantics) -> MissionProposal {
    parent_proposal_with_assignment(child_assignment(output))
}

fn parent_proposal_with_assignment(assignment: ChildMissionAssignment) -> MissionProposal {
    let plan = simple_plan();
    let mut parent_team = team(1, Some(&plan), false);
    parent_team.task_assignments.insert(
        plan.tasks[0].id.clone(),
        TaskAssignment::ChildMission {
            mission: Box::new(assignment),
        },
    );
    MissionProposal {
        plan: Some(PlanProposal {
            base_revision: 0,
            requirement_changes: Vec::new(),
            assertion_supersessions: Vec::new(),
            plan: plan.clone(),
        }),
        team: Some(parent_team),
        oracles: Some(common::oracle_specs(&plan)),
    }
}

fn successful_runner() -> MockRoleRunner {
    MockRoleRunner::new(Box::new(|request| {
        let handoff = match request.role.output {
            OutputSemantics::EmitsVerdict => Handoff::Validate {
                done: true,
                report: PayloadRef::inline("independent judgment passed"),
                items: request
                    .assertion_ids
                    .iter()
                    .cloned()
                    .map(|item_id| lionclaw::model::ValidationItem {
                        item_id,
                        passed: true,
                    })
                    .collect(),
                passed: true,
                request_attention: false,
            },
            OutputSemantics::ProducesReport | OutputSemantics::ProducesArtifact => Handoff::Work {
                done: true,
                report: PayloadRef::inline("delegated task output"),
                request_attention: false,
            },
            other => panic!("unexpected child runtime output {other:?}"),
        };
        Ok(RoleTurnOutcome {
            handoff: Some(handoff),
            artifact: None,
            prepared_inputs: Vec::new(),
            runtime_configuration: Default::default(),
            runtime_usage: Default::default(),
            final_response: "completed".into(),
        })
    }))
}

async fn setup(
    output: OutputSemantics,
    runner: MockRoleRunner,
) -> (tempfile::TempDir, common::TestHarness, MissionId) {
    setup_with_proposal(output, runner, parent_proposal(output)).await
}

async fn setup_with_proposal(
    _output: OutputSemantics,
    runner: MockRoleRunner,
    proposal: MissionProposal,
) -> (tempfile::TempDir, common::TestHarness, MissionId) {
    let directory = tempfile::tempdir().unwrap();
    initialize_repository(directory.path());
    let harness = common::harness(directory.path(), runner, MockOracleRunner::exiting(0)).await;
    let mission = harness
        .engine
        .create_mission(
            directory.path().to_str().unwrap(),
            "delegate through the same machine",
            BASE_SHA,
        )
        .await
        .unwrap();
    harness
        .engine
        .propose_plan(&mission, proposal)
        .await
        .unwrap();
    approve_plan(&harness.engine, &mission).await;
    (directory, harness, mission)
}

fn projected_child_request(
    state: &lionclaw::model::MissionState,
) -> lionclaw::model::ChildMissionRequest {
    next(state)
        .effects
        .into_iter()
        .find_map(|intent| match intent {
            EffectIntent::ChildMission(request) => Some(request),
            _ => None,
        })
        .expect("next visibly projects child work")
}

async fn advance_until_finish(engine: &lionclaw::engine::Engine, mission: &MissionId) {
    for _ in 0..16 {
        let view = engine.advance(mission).await.unwrap();
        if view
            .next
            .choices
            .iter()
            .any(|choice| matches!(choice, Choice::Finish { .. }))
        {
            engine
                .finish(mission, "test terminal settlement")
                .await
                .unwrap();
            return;
        }
    }
    panic!("mission {mission} never reached its ordinary finish choice");
}

#[tokio::test]
async fn crash_boundaries_reconnect_exact_child_without_duplicate() {
    let (directory, harness, parent_id) =
        setup(OutputSemantics::ProducesReport, successful_runner()).await;
    let parent = harness.engine.load_state(&parent_id).await.unwrap();
    let request = projected_child_request(&parent);
    let requested_at_ms = 1_000_000;

    fault_append_events(
        directory.path(),
        &parent_id,
        parent.head,
        &[NewEvent::new(MissionEvent::ChildMissionRequested {
            request: Box::new(request.clone()),
            requested_at_ms,
            deadline_ms: resolve_execution_deadline_ms(requested_at_ms, 120).unwrap(),
            budget_deadline_ms: resolve_execution_deadline_ms(requested_at_ms, 120).unwrap(),
        })],
        requested_at_ms,
    )
    .await;
    let store = MissionStore::open(directory.path()).await.unwrap();
    assert_eq!(
        store.list_missions().await.unwrap(),
        vec![parent_id.clone()]
    );

    // Crash after durable request: recovery creates exactly the deterministic child.
    harness.engine.advance(&parent_id).await.unwrap();
    assert_eq!(store.list_missions().await.unwrap().len(), 2);
    let unbound = harness.engine.load_state(&parent_id).await.unwrap();
    assert!(matches!(
        unbound.inflight.get(&request.parent_effect_id),
        Some(lionclaw::model::InflightEffect::ChildMission { bound: false, .. })
    ));
    let child = harness
        .engine
        .load_state(&request.child_mission_id)
        .await
        .unwrap();
    assert_eq!(child.lineage.as_ref().unwrap().parent_mission_id, parent_id);

    // Crash after creation: a fresh engine binds the same child, never another one.
    let restarted = engine_with_runtime_identities(
        directory.path(),
        test_mission_type(),
        harness.role_runner.clone(),
        harness.oracle_runner.clone(),
        default_runtime_identities(),
    )
    .await;
    restarted.advance(&parent_id).await.unwrap();
    let bound = restarted.load_state(&parent_id).await.unwrap();
    assert!(matches!(
        bound.inflight.get(&request.parent_effect_id),
        Some(lionclaw::model::InflightEffect::ChildMission { bound: true, .. })
    ));
    assert_eq!(store.list_missions().await.unwrap().len(), 2);

    // Crash after terminal child truth: the parent reconnects and derives its receipt.
    advance_until_finish(&restarted, &request.child_mission_id).await;
    let child = restarted
        .load_state(&request.child_mission_id)
        .await
        .unwrap();
    assert!(child.terminal.is_some());
    restarted.advance(&parent_id).await.unwrap();
    let received = restarted.load_state(&parent_id).await.unwrap();
    assert!(received
        .child_mission_receipts
        .contains_key(&request.parent_effect_id));
    assert!(!received
        .cleaned_child_missions
        .contains(&request.parent_effect_id));

    // Crash after receipt: cleanup and parent proof resume without re-running the child.
    let restarted_again = engine_with_runtime_identities(
        directory.path(),
        test_mission_type(),
        harness.role_runner.clone(),
        harness.oracle_runner.clone(),
        default_runtime_identities(),
    )
    .await;
    advance_until_finish(&restarted_again, &parent_id).await;
    let settled = restarted_again.load_state(&parent_id).await.unwrap();
    assert!(settled
        .cleaned_child_missions
        .contains(&request.parent_effect_id));
    assert_eq!(
        settled.tasks[&TaskId::new("fix").unwrap()].status,
        TaskStatus::Cleared
    );
    assert_eq!(store.list_missions().await.unwrap().len(), 2);
}

#[tokio::test]
async fn artifact_child_and_parent_abort_use_the_same_normal_task_flow() {
    let (directory, harness, parent_id) =
        setup(OutputSemantics::ProducesArtifact, successful_runner()).await;
    let request = projected_child_request(&harness.engine.load_state(&parent_id).await.unwrap());
    harness.engine.advance(&parent_id).await.unwrap();
    harness.engine.advance(&parent_id).await.unwrap();
    advance_until_finish(&harness.engine, &request.child_mission_id).await;
    harness.engine.advance(&parent_id).await.unwrap();
    let received = harness.engine.load_state(&parent_id).await.unwrap();
    assert!(matches!(
        received.child_mission_receipts[&request.parent_effect_id].output,
        Some(ChildMissionOutput::Artifact { .. })
    ));

    let (abort_dir, abort_harness, abort_parent) =
        setup(OutputSemantics::ProducesReport, successful_runner()).await;
    let abort_request = projected_child_request(
        &abort_harness
            .engine
            .load_state(&abort_parent)
            .await
            .unwrap(),
    );
    abort_harness.engine.advance(&abort_parent).await.unwrap();
    abort_harness.engine.advance(&abort_parent).await.unwrap();
    abort_harness
        .engine
        .abort(&abort_parent, "operator aborted parent")
        .await
        .unwrap();
    for _ in 0..8 {
        let state = abort_harness
            .engine
            .advance(&abort_parent)
            .await
            .unwrap()
            .state;
        if state.inflight.is_empty()
            && state
                .cleaned_child_missions
                .contains(&abort_request.parent_effect_id)
        {
            break;
        }
    }
    let parent = abort_harness
        .engine
        .load_state(&abort_parent)
        .await
        .unwrap();
    let child = abort_harness
        .engine
        .load_state(&abort_request.child_mission_id)
        .await
        .unwrap();
    assert!(parent.inflight.is_empty());
    assert!(child.terminal.is_some());
    assert!(child.inflight.is_empty());
    assert_eq!(
        MissionStore::open(abort_dir.path())
            .await
            .unwrap()
            .list_missions()
            .await
            .unwrap()
            .len(),
        2
    );
    drop(directory);
}

#[tokio::test]
async fn stopping_parent_effect_aborts_and_quiesces_bound_child() {
    let (_directory, harness, parent_id) =
        setup(OutputSemantics::ProducesReport, successful_runner()).await;
    let request = projected_child_request(&harness.engine.load_state(&parent_id).await.unwrap());
    harness.engine.advance(&parent_id).await.unwrap();
    harness.engine.advance(&parent_id).await.unwrap();

    record_control(
        harness.engine.store(),
        1,
        &parent_id,
        &request.parent_effect_id,
        ControlAction::Stop,
        "operator stopped delegated work",
    )
    .await
    .unwrap();
    for _ in 0..8 {
        let state = harness.engine.advance(&parent_id).await.unwrap().state;
        if state
            .child_mission_receipts
            .contains_key(&request.parent_effect_id)
            && state.inflight.is_empty()
        {
            break;
        }
    }

    let parent = harness.engine.load_state(&parent_id).await.unwrap();
    let child = harness
        .engine
        .load_state(&request.child_mission_id)
        .await
        .unwrap();
    let receipt = &parent.child_mission_receipts[&request.parent_effect_id];
    assert_eq!(
        receipt.failure.as_ref().unwrap().category(),
        "operator_stopped"
    );
    assert!(child.terminal.is_some());
    assert!(child.inflight.is_empty());
}

#[tokio::test]
async fn nested_children_execute_with_deterministic_kernel_lineage() {
    let mut leaf = child_assignment(OutputSemantics::ProducesReport);
    leaf.objective = "leaf delegated mission".into();
    leaf.config.execution.max_child_depth = 2;
    leaf.config.execution.max_descendants = 4;
    let mut middle = child_assignment(OutputSemantics::ProducesReport);
    middle.objective = "middle delegated mission".into();
    middle
        .proposal
        .team
        .as_mut()
        .unwrap()
        .task_assignments
        .insert(
            TaskId::new("fix").unwrap(),
            TaskAssignment::ChildMission {
                mission: Box::new(leaf),
            },
        );
    let proposal = parent_proposal_with_assignment(middle);
    let (directory, harness, parent_id) = setup_with_proposal(
        OutputSemantics::ProducesReport,
        successful_runner(),
        proposal,
    )
    .await;

    advance_until_finish(&harness.engine, &parent_id).await;
    let store = MissionStore::open(directory.path()).await.unwrap();
    let mission_ids = store.list_missions().await.unwrap();
    assert_eq!(mission_ids.len(), 3);
    let mut depths = Vec::new();
    for mission_id in mission_ids {
        if mission_id == parent_id {
            continue;
        }
        let state = harness.engine.load_state(&mission_id).await.unwrap();
        depths.push(state.lineage.as_ref().unwrap().depth);
    }
    depths.sort();
    assert_eq!(depths, vec![1, 2]);
}

#[tokio::test]
async fn failed_child_parks_on_existing_retry_decision_and_new_attempt_gets_new_id() {
    let runner = MockRoleRunner::new(Box::new(|request| {
        if request.role.output == OutputSemantics::ProducesReport {
            return Err(TypedFailure::permanent(
                "test.child_failure",
                "unchanged delegated failure",
            ));
        }
        Ok(RoleTurnOutcome {
            handoff: Some(Handoff::Validate {
                done: true,
                report: PayloadRef::inline("judged"),
                items: request
                    .assertion_ids
                    .iter()
                    .cloned()
                    .map(|item_id| lionclaw::model::ValidationItem {
                        item_id,
                        passed: true,
                    })
                    .collect(),
                passed: true,
                request_attention: false,
            }),
            artifact: None,
            prepared_inputs: Vec::new(),
            runtime_configuration: Default::default(),
            runtime_usage: Default::default(),
            final_response: "judged".into(),
        })
    }));
    let (_directory, harness, parent_id) = setup(OutputSemantics::ProducesReport, runner).await;
    let first = projected_child_request(&harness.engine.load_state(&parent_id).await.unwrap());
    for _ in 0..12 {
        let state = harness.engine.advance(&parent_id).await.unwrap().state;
        if state.tasks[&first.task_id].status == TaskStatus::Failed
            && next(&state).effects.is_empty()
        {
            break;
        }
    }
    let failed = harness.engine.load_state(&parent_id).await.unwrap();
    assert_eq!(failed.tasks[&first.task_id].status, TaskStatus::Failed);
    assert!(next(&failed).effects.is_empty());
    assert!(next(&failed).choices.iter().any(|choice| matches!(
        choice,
        Choice::Decide {
            action: DecisionAction::Retry,
            ..
        }
    )));
    harness
        .engine
        .decide(
            &parent_id,
            &format!("node_failed:{}", first.task_id),
            DecisionAction::Retry,
            "explicitly retry the ordinary task",
        )
        .await
        .unwrap();
    let second = projected_child_request(&harness.engine.load_state(&parent_id).await.unwrap());
    assert_eq!(second.attempt_no, 2);
    assert_ne!(second.child_mission_id, first.child_mission_id);
    assert_eq!(second.request_digest, first.request_digest);
}
