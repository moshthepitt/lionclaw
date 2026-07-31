mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use clap::Parser;
use common::{
    approve_plan, default_runtime_identities, engine_with_runtime_identities, fault_append_events,
    initialize_repository, simple_plan, team, test_mission_type, BASE_SHA,
};
use lionclaw::authority::AuthorityCeiling;
use lionclaw::cli;
use lionclaw::config::RuntimeProfiles;
use lionclaw::engine::{record_control, Engine, EngineServices};
use lionclaw::mission_type::load_mission_type;
use lionclaw::model::{
    next, resolve_execution_deadline_ms, AuthorityCeilings, ChildMissionAssignment,
    ChildMissionOutput, Choice, ControlAction, DecisionAction, EffectIntent, ExecutionPolicy,
    Handoff, MissionConfig, MissionEvent, MissionId, MissionProposal, OutputSemantics, PayloadRef,
    PlanProposal, RecoveryConfig, RoleInstanceId, TaskAssignment, TaskId, TaskStatus, TeamRevision,
};
use lionclaw::ports::{ExecutionControl, RoleRunner, RoleTurnOutcome, RoleTurnRequest};
use lionclaw::store::{MissionStore, NewEvent};
use lionclaw::testing::{MockClock, MockOracleRunner, MockRoleRunner, NoopEffectCleaner};
use lionclaw_runtime_api::{
    RuntimeAuthProvider, RuntimeAuthRegistry, RuntimeDriverProvider, RuntimeDriverRegistry,
};
use lionclaw_runtime_api::{TypedFailure, TypedFailureEvidence};
use lionclaw_runtime_codex::{CodexRuntimeAuthProvider, CodexRuntimeDriver};
use tokio::sync::Notify;

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

struct BlockingChildRunner {
    started: Arc<Notify>,
}

#[async_trait]
impl RoleRunner for BlockingChildRunner {
    async fn run(&self, request: RoleTurnRequest) -> Result<RoleTurnOutcome, TypedFailure> {
        if request.role.output != OutputSemantics::ProducesReport {
            return successful_runner().run(request).await;
        }

        self.started.notify_one();
        let mut control = request.control;
        loop {
            let observed = control.borrow().clone();
            let failure = match observed {
                ExecutionControl::RunUntil(_) => None,
                ExecutionControl::DeadlineExhausted => Some(TypedFailure::DeadlineExhausted {
                    evidence: Box::new(TypedFailureEvidence::new(
                        Some("test.child_deadline".into()),
                        "child observed deadline cancellation",
                    )),
                }),
                ExecutionControl::Stop(reason) => {
                    let mut evidence = TypedFailureEvidence::new(
                        Some("test.child_stop".into()),
                        "child observed stop cancellation",
                    );
                    evidence.stop_reason = Some(reason);
                    Some(TypedFailure::OperatorStopped {
                        evidence: Box::new(evidence),
                    })
                }
                ExecutionControl::Abort(reason) => {
                    let mut evidence = TypedFailureEvidence::new(
                        Some("test.child_abort".into()),
                        "child observed abort cancellation",
                    );
                    evidence.stop_reason = Some(reason);
                    Some(TypedFailure::OperatorAborted {
                        evidence: Box::new(evidence),
                    })
                }
            };
            if let Some(failure) = failure {
                return Err(failure);
            }
            control.changed().await.unwrap();
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum LiveParentCancellation {
    Stop,
    Deadline,
    Abort,
}

async fn assert_live_parent_cancellation(cancellation: LiveParentCancellation) {
    let directory = tempfile::tempdir().unwrap();
    initialize_repository(directory.path());
    let started = Arc::new(Notify::new());
    let store = MissionStore::open(directory.path()).await.unwrap();
    let engine = Engine::new(
        store,
        test_mission_type(),
        "localhost/lionclaw-runtime-dev:v1".into(),
        EngineServices::new(
            Arc::new(BlockingChildRunner {
                started: started.clone(),
            }),
            Arc::new(MockOracleRunner::exiting(0)),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        )
        .with_runtime_identities(default_runtime_identities()),
    );
    let parent_id = engine
        .create_mission(
            directory.path().to_str().unwrap(),
            "cancel live delegated work",
            BASE_SHA,
        )
        .await
        .unwrap();
    let mut proposal = parent_proposal(OutputSemantics::ProducesReport);
    for role in proposal.team.as_mut().unwrap().roles.values_mut() {
        role.runtime = "codex".into();
    }
    engine.propose_plan(&parent_id, proposal).await.unwrap();
    approve_plan(&engine, &parent_id).await;
    let request = projected_child_request(&engine.load_state(&parent_id).await.unwrap());
    engine.advance(&parent_id).await.unwrap();
    engine.advance(&parent_id).await.unwrap();

    let advance_engine = engine.clone();
    let advance_parent = parent_id.clone();
    let mut advance = tokio::spawn(async move { advance_engine.advance(&advance_parent).await });
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("child role did not start");
    match cancellation {
        LiveParentCancellation::Stop => record_control(
            engine.store(),
            1,
            &parent_id,
            &request.parent_effect_id,
            ControlAction::Stop,
            "operator stopped live delegated work",
        )
        .await
        .unwrap(),
        LiveParentCancellation::Deadline => {
            let parent = engine.load_state(&parent_id).await.unwrap();
            let deadline_ms = parent.inflight[&request.parent_effect_id].deadline_ms();
            fault_append_events(
                directory.path(),
                &parent_id,
                parent.head,
                &[NewEvent::new(MissionEvent::ControlRequested {
                    effect_id: request.parent_effect_id.clone(),
                    action: ControlAction::DeadlineReached { deadline_ms },
                    reason: "engine observed live delegated deadline".into(),
                })],
                deadline_ms,
            )
            .await;
        }
        LiveParentCancellation::Abort => engine
            .abort(&parent_id, "operator aborted live delegated work")
            .await
            .unwrap(),
    }

    let completed = tokio::time::timeout(Duration::from_secs(2), &mut advance).await;
    if completed.is_err() {
        engine
            .abort(
                &request.child_mission_id,
                "test cleanup after parent cancellation was not propagated",
            )
            .await
            .unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), &mut advance).await;
        panic!("{cancellation:?} did not interrupt the active child driver");
    }
    completed.unwrap().unwrap().unwrap();

    let parent = engine.load_state(&parent_id).await.unwrap();
    let child = engine.load_state(&request.child_mission_id).await.unwrap();
    let receipt = &parent.child_mission_receipts[&request.parent_effect_id];
    let expected_category = match cancellation {
        LiveParentCancellation::Stop => "operator_stopped",
        LiveParentCancellation::Deadline => "deadline_exhausted",
        LiveParentCancellation::Abort => "operator_aborted",
    };
    assert_eq!(
        receipt.failure.as_ref().unwrap().category(),
        expected_category
    );
    assert!(child.terminal.is_some());
    assert!(child.inflight.is_empty());
}

fn write_cli_mission_type(root: &std::path::Path) {
    std::fs::create_dir_all(root.join("roles")).unwrap();
    std::fs::write(
        root.join("playbook.md"),
        "Delegate work through the same machine.\n",
    )
    .unwrap();
    std::fs::write(
        root.join("mission.toml"),
        r#"[mission-type]
name = "child-reopen"
stop = "verified"
image = "child-reopen-image"

[team]
planning-assignment = "strategist"

[ceilings]
writes = true

[recovery]
max-attempts = 3

[execution]
default-timeout-secs = 60
max-task-time-secs = 120
extension-step-secs = 30
effect-capacity = 4
auto-continue-candidate = false
auto-continue-proof = false
"#,
    )
    .unwrap();
    std::fs::write(
        root.join("roles/strategist.md"),
        "---\noutput: proposes-plan\nruntime: codex\n---\nPlan delegated work.\n",
    )
    .unwrap();
}

#[tokio::test]
async fn ordinary_cli_reopens_a_child_from_the_kernel_owned_root_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let repo = directory.path().join("repo");
    std::fs::create_dir(&repo).unwrap();
    initialize_repository(&repo);
    let mission_type_root = directory.path().join("mission-type");
    write_cli_mission_type(&mission_type_root);
    let fake_oci = directory.path().join("fake-oci");
    std::fs::write(
        &fake_oci,
        "#!/bin/sh\nif [ \"$1 $2\" = \"image inspect\" ]; then echo child-reopen-image-id; fi\nexit 0\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_oci, std::fs::Permissions::from_mode(0o755)).unwrap();
    let profiles = RuntimeProfiles::from_toml(
        &format!(
            r#"[runtimes.codex]
driver = "codex"
command = "external-codex"
auth = "codex"
confinement = {{ backend = "podman", engine = "{}", read-only-rootfs = true }}
"#,
            fake_oci.display()
        ),
        directory.path(),
    )
    .unwrap();
    let role_runner = Arc::new(successful_runner());
    let oracle_runner = Arc::new(MockOracleRunner::exiting(0));
    let transports = cli::MissionTransports::external(
        profiles,
        RuntimeDriverRegistry::new(
            [Arc::new(CodexRuntimeDriver) as Arc<dyn RuntimeDriverProvider>],
        ),
        RuntimeAuthRegistry::new([
            Arc::new(CodexRuntimeAuthProvider) as Arc<dyn RuntimeAuthProvider>
        ]),
        oracle_runner.clone(),
    )
    .with_role_transport(role_runner.clone(), Arc::new(NoopEffectCleaner));
    let start = cli::Cli::try_parse_from([
        "lionclaw",
        "mission",
        "start",
        "--type",
        mission_type_root.to_str().unwrap(),
        "--repo",
        repo.to_str().unwrap(),
        "--objective",
        "prove ordinary child reopening",
        "--runtime",
        "codex",
    ])
    .unwrap();
    cli::run_with_transports(start, transports.clone())
        .await
        .unwrap();

    let store = MissionStore::open(&repo).await.unwrap();
    let missions = store.list_missions().await.unwrap();
    let [parent_id] = missions.as_slice() else {
        panic!("mission start did not create exactly one parent")
    };
    let parent_id = parent_id.clone();
    let mission_type = load_mission_type(&mission_type_root, &AuthorityCeiling::default()).unwrap();
    let parent = store.require_state(&parent_id).await.unwrap();
    let engine = Engine::new(
        store,
        mission_type,
        parent.image_id,
        EngineServices::new(
            role_runner,
            oracle_runner,
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        )
        .with_runtime_identities(default_runtime_identities()),
    );
    let mut proposal = parent_proposal(OutputSemantics::ProducesReport);
    for role in proposal.team.as_mut().unwrap().roles.values_mut() {
        role.runtime = "codex".into();
    }
    engine.propose_plan(&parent_id, proposal).await.unwrap();
    approve_plan(&engine, &parent_id).await;
    let request = projected_child_request(&engine.load_state(&parent_id).await.unwrap());
    engine.advance(&parent_id).await.unwrap();
    engine.advance(&parent_id).await.unwrap();

    let advance_child = cli::Cli::try_parse_from([
        "lionclaw",
        "mission",
        "advance",
        request.child_mission_id.as_str(),
        "--repo",
        repo.to_str().unwrap(),
        "--json",
    ])
    .unwrap();
    let code = cli::run_with_transports(advance_child, transports)
        .await
        .unwrap();
    assert_eq!(code, std::process::ExitCode::SUCCESS);
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
async fn cancellation_before_child_creation_never_leaves_an_active_child() {
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
    record_control(
        harness.engine.store(),
        requested_at_ms + 1,
        &parent_id,
        &request.parent_effect_id,
        ControlAction::Stop,
        "operator stopped before child creation",
    )
    .await
    .unwrap();

    harness.engine.advance(&parent_id).await.unwrap();
    let child = harness
        .engine
        .load_state(&request.child_mission_id)
        .await
        .unwrap();
    assert!(child.terminal.is_some());
    assert!(child.inflight.is_empty());

    for _ in 0..8 {
        let parent = harness.engine.advance(&parent_id).await.unwrap().state;
        if parent
            .child_mission_receipts
            .contains_key(&request.parent_effect_id)
        {
            break;
        }
    }
    let parent = harness.engine.load_state(&parent_id).await.unwrap();
    assert_eq!(
        parent.child_mission_receipts[&request.parent_effect_id]
            .failure
            .as_ref()
            .unwrap()
            .category(),
        "operator_stopped"
    );
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
async fn live_parent_stop_deadline_and_abort_interrupt_the_active_child() {
    for cancellation in [
        LiveParentCancellation::Stop,
        LiveParentCancellation::Deadline,
        LiveParentCancellation::Abort,
    ] {
        assert_live_parent_cancellation(cancellation).await;
    }
}

#[tokio::test]
async fn nested_children_execute_with_deterministic_kernel_lineage() {
    let mut leaf = child_assignment(OutputSemantics::ProducesReport);
    leaf.objective = "leaf delegated mission".into();
    leaf.config.execution.max_child_depth = 2;
    leaf.config.execution.max_descendants = 0;
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
            let mut failure =
                TypedFailure::permanent("test.child_failure", "unchanged delegated failure");
            failure.evidence_mut().final_response = "bounded child failure response".into();
            return Err(failure);
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
    let (directory, harness, parent_id) = setup(OutputSemantics::ProducesReport, runner).await;
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
    let first_failure = failed.task_last_failure(&first.task_id).unwrap();
    assert_eq!(
        first_failure.evidence().code.as_deref(),
        Some("test.child_failure")
    );
    assert_eq!(first_failure.detail(), "unchanged delegated failure");
    assert_eq!(
        first_failure.evidence().final_response,
        "bounded child failure response"
    );
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

    for _ in 0..12 {
        let state = harness.engine.advance(&parent_id).await.unwrap().state;
        if state
            .child_mission_receipts
            .contains_key(&second.parent_effect_id)
        {
            break;
        }
    }
    let retried = harness.engine.load_state(&parent_id).await.unwrap();
    assert!(retried
        .child_mission_receipts
        .contains_key(&second.parent_effect_id));
    assert_eq!(
        MissionStore::open(directory.path())
            .await
            .unwrap()
            .list_missions()
            .await
            .unwrap()
            .len(),
        3
    );
}
