//! Slice 9: parallel writer lineages converge through one integration sink.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common::{approve_plan, covered_requirement, initialize_repository, ParseTask, BASE_SHA};
use lionclaw::engine::{Engine, EngineServices, MissionDisposition};
use lionclaw::mission_type::{MissionType, MissionTypeDefinition};
use lionclaw::model::{
    Assertion, AssertionId, AuthorityCeilings, AuthorityGrants, DecisionAction, EffectId,
    EventEnvelope, ExecutionPolicy, FinishClass, Handoff, MissionConfig, MissionEvent,
    MissionPhase, MissionProposal, OracleName, OutputSemantics, PayloadRef, Plan, PlanProposal,
    RoleInstance, RoleInstanceId, RolePromptTemplate, StopBar, Task, TaskCandidateRef,
    TeamRevision, VersionStamps, WorkspacePreparation, SCHEMA_VERSION,
};
use lionclaw::ports::{
    OracleOutcome, OracleRunRequest, OracleRunner, RoleRunner, RoleTurnOutcome, RoleTurnRequest,
};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockClock, NoopEffectCleaner};
use lionclaw_runtime_api::TypedFailure;
use tempfile::TempDir;

const LEFT: &str = "left";
const RIGHT: &str = "right";
const MERGE: &str = "merge";
const REVIEWER: &str = "reviewer";

#[derive(Default)]
struct ParallelLog {
    events: Mutex<Vec<WriterEvent>>,
    workspaces: Mutex<BTreeMap<String, String>>,
    dependency_refs: Mutex<Vec<Vec<TaskCandidateRef>>>,
    calls: Mutex<Vec<RunnerCall>>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum WriterEvent {
    Started(String),
    Ended(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RunnerCall {
    task: String,
    attempt_no: u32,
    base_sha: String,
}

#[derive(Clone)]
struct ParallelRoleRunner {
    first_wave: Arc<tokio::sync::Barrier>,
    log: Arc<ParallelLog>,
    conflict: bool,
}

impl ParallelRoleRunner {
    fn new(conflict: bool) -> Self {
        Self {
            first_wave: Arc::new(tokio::sync::Barrier::new(2)),
            log: Arc::new(ParallelLog::default()),
            conflict,
        }
    }

    fn log(&self) -> Arc<ParallelLog> {
        self.log.clone()
    }
}

#[async_trait]
impl RoleRunner for ParallelRoleRunner {
    async fn run(&self, request: RoleTurnRequest) -> Result<RoleTurnOutcome, TypedFailure> {
        if request.role.output == OutputSemantics::EmitsVerdict {
            return Ok(validate_outcome(&request));
        }
        let task = request
            .task_id
            .as_ref()
            .expect("parallel test dispatches only task writers")
            .to_string();
        self.log.calls.lock().expect("lock").push(RunnerCall {
            task: task.clone(),
            attempt_no: request.attempt_no,
            base_sha: request.base_sha.clone(),
        });
        if let Some(capture) = &request.artifact_capture {
            self.log
                .workspaces
                .lock()
                .expect("lock")
                .insert(task.clone(), capture.checkout_dir().display().to_string());
        }
        lionclaw::testing::prepare_test_workspace(&request).await?;
        self.log
            .events
            .lock()
            .expect("lock")
            .push(WriterEvent::Started(task.clone()));
        if (task == LEFT || task == RIGHT) && request.attempt_no == 1 {
            self.first_wave.wait().await;
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let outcome = if task == MERGE {
            self.merge_and_capture(&request).await
        } else {
            self.write_and_capture(&request, &task).await
        };
        self.log
            .events
            .lock()
            .expect("lock")
            .push(WriterEvent::Ended(task));
        outcome
    }
}

impl ParallelRoleRunner {
    async fn write_and_capture(
        &self,
        request: &RoleTurnRequest,
        task: &str,
    ) -> Result<RoleTurnOutcome, TypedFailure> {
        let checkout = checkout(request)?;
        let file = if self.conflict {
            "shared.txt".to_string()
        } else {
            format!("{task}.txt")
        };
        std::fs::write(
            checkout.join(file),
            format!(
                "{task} attempt {} from {}\n",
                request.attempt_no, request.base_sha
            ),
        )
        .map_err(infra)?;
        git(checkout, &["add", "-A"])?;
        git_commit(
            checkout,
            &format!("{task} candidate {}", request.attempt_no),
            request.attempt_no,
        )?;
        captured_work(request).await
    }

    async fn merge_and_capture(
        &self,
        request: &RoleTurnRequest,
    ) -> Result<RoleTurnOutcome, TypedFailure> {
        self.log
            .dependency_refs
            .lock()
            .expect("lock")
            .push(request.dependency_refs.clone());
        if request.dependency_refs.len() != 2 {
            return Err(TypedFailure::permanent(
                "testing.integration_refs",
                format!(
                    "expected two dependency refs, got {:?}",
                    request.dependency_refs
                ),
            ));
        }
        let checkout = checkout(request)?;
        git_configure(checkout)?;
        for candidate in &request.dependency_refs {
            if candidate.sha == request.base_sha {
                continue;
            }
            git(checkout, &["fetch", "origin", &candidate.sha])?;
            if !git_status(
                checkout,
                &["merge-base", "--is-ancestor", &candidate.sha, "HEAD"],
            )? && !git_status(checkout, &["merge", "--no-ff", "--no-edit", &candidate.sha])?
            {
                return Err(TypedFailure::permanent(
                    "workspace.merge_conflict",
                    format!("failed to merge dependency {}", candidate.task_id),
                ));
            }
        }
        std::fs::write(
            checkout.join(format!("integrated-{}.txt", request.attempt_no)),
            format!("integrated at {}\n", request.attempt_no),
        )
        .map_err(infra)?;
        git(checkout, &["add", "-A"])?;
        git_commit(
            checkout,
            &format!("integrate candidates {}", request.attempt_no),
            request.attempt_no + 10,
        )?;
        captured_work(request).await
    }
}

struct ScriptedOracleRunner {
    calls: Mutex<Vec<(String, String)>>,
    fail_oracle: Option<&'static str>,
    attempts: Mutex<BTreeMap<String, u32>>,
}

impl ScriptedOracleRunner {
    fn passing() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            fail_oracle: None,
            attempts: Mutex::new(BTreeMap::new()),
        }
    }

    fn fail_once(oracle: &'static str) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            fail_oracle: Some(oracle),
            attempts: Mutex::new(BTreeMap::new()),
        }
    }

    fn judged_shas(&self) -> Vec<String> {
        self.calls
            .lock()
            .expect("lock")
            .iter()
            .map(|(_, sha)| sha.clone())
            .collect()
    }
}

#[async_trait]
impl OracleRunner for ScriptedOracleRunner {
    async fn run(&self, request: OracleRunRequest) -> Result<OracleOutcome, TypedFailure> {
        self.calls
            .lock()
            .expect("lock")
            .push((request.oracle.to_string(), request.judged_sha.clone()));
        let mut attempts = self.attempts.lock().expect("lock");
        let attempt = attempts.entry(request.oracle.to_string()).or_default();
        let exit_code = if self.fail_oracle == Some(request.oracle.as_str()) && *attempt == 0 {
            1
        } else {
            0
        };
        *attempt += 1;
        Ok(OracleOutcome {
            exit_code,
            exit_signal: None,
            stdout: format!("oracle {exit_code} at {}", request.judged_sha).into_bytes(),
            stderr: Vec::new(),
            prepared_inputs: Vec::new(),
            duration_ms: 1,
        })
    }
}

struct Harness {
    engine: Engine,
    runner_log: Arc<ParallelLog>,
    oracle: Arc<ScriptedOracleRunner>,
}

async fn harness(dir: &Path, conflict: bool, oracle: ScriptedOracleRunner) -> Harness {
    initialize_repository(dir);
    let runner = Arc::new(ParallelRoleRunner::new(conflict));
    let runner_log = runner.log();
    let oracle = Arc::new(oracle);
    let engine = Engine::new(
        MissionStore::open(dir).await.expect("open store"),
        mission_type(),
        "localhost/lionclaw-runtime-dev:v1".to_string(),
        EngineServices::new(
            runner,
            oracle.clone(),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );
    Harness {
        engine,
        runner_log,
        oracle,
    }
}

#[tokio::test]
async fn parallel_writers_run_concurrently_and_integrate_at_the_deliverable_head() {
    let dir = TempDir::new().expect("tempdir");
    let h = harness(dir.path(), false, ScriptedOracleRunner::passing()).await;
    let mission_id = start_parallel_mission(&h.engine, dir.path()).await;

    let outcome = h.engine.advance(&mission_id).await.expect("advance");
    assert_eq!(outcome.disposition, MissionDisposition::Terminal);
    assert_eq!(
        outcome.state.phase,
        MissionPhase::Done {
            finish: FinishClass::Verified
        }
    );
    assert_writer_overlap(&h.runner_log);
    assert_distinct_workspaces(&h.runner_log);
    let refs = h
        .runner_log
        .dependency_refs
        .lock()
        .expect("lock")
        .last()
        .cloned()
        .expect("integration refs");
    assert_eq!(refs.len(), 2);
    let deliverable = outcome.state.deliverable_head().to_string();
    assert_ne!(deliverable, BASE_SHA);
    for candidate in &refs {
        assert_ancestor(dir.path(), &candidate.sha, &deliverable);
    }
    let judged = h.oracle.judged_shas();
    assert_eq!(judged.len(), 2);
    assert!(judged.iter().all(|judged| judged == &deliverable));
}

#[tokio::test]
async fn repairing_a_non_sink_task_reowes_integration_and_proof_at_the_new_head() {
    let dir = TempDir::new().expect("tempdir");
    let h = harness(
        dir.path(),
        false,
        ScriptedOracleRunner::fail_once("cargo-left"),
    )
    .await;
    let mission_id = start_parallel_mission(&h.engine, dir.path()).await;

    let parked = h.engine.advance(&mission_id).await.expect("first advance");
    assert_eq!(parked.disposition, MissionDisposition::Parked);
    let first_head = parked.state.deliverable_head().to_string();
    h.engine
        .decide(
            &mission_id,
            "oracle_verdict_failed:cargo-left",
            DecisionAction::Repair,
            "repair the left-side assertion",
        )
        .await
        .expect("repair");

    let done = h.engine.advance(&mission_id).await.expect("repair advance");
    assert_eq!(done.disposition, MissionDisposition::Terminal);
    let final_head = done.state.deliverable_head().to_string();
    assert_ne!(final_head, first_head);
    let calls = h.runner_log.calls.lock().expect("lock").clone();
    assert_eq!(calls.iter().filter(|call| call.task == LEFT).count(), 2);
    assert_eq!(calls.iter().filter(|call| call.task == RIGHT).count(), 1);
    assert_eq!(calls.iter().filter(|call| call.task == MERGE).count(), 2);
    let left_repair_base = calls
        .iter()
        .find(|call| call.task == LEFT && call.attempt_no == 2)
        .map(|call| call.base_sha.clone())
        .expect("left repair call");
    assert_eq!(left_repair_base, first_head);
    let final_refs = h
        .runner_log
        .dependency_refs
        .lock()
        .expect("lock")
        .last()
        .cloned()
        .expect("final integration refs");
    for candidate in &final_refs {
        assert_ancestor(dir.path(), &candidate.sha, &final_head);
    }
    assert_eq!(h.oracle.judged_shas().last(), Some(&final_head));
}

#[tokio::test]
async fn integration_merge_conflicts_park_the_sink_task() {
    let dir = TempDir::new().expect("tempdir");
    let h = harness(dir.path(), true, ScriptedOracleRunner::passing()).await;
    let mission_id = start_parallel_mission(&h.engine, dir.path()).await;

    let parked = h.engine.advance(&mission_id).await.expect("advance");
    assert_eq!(parked.disposition, MissionDisposition::Parked);
    assert!(
        parked
            .state
            .open_attention
            .contains_key("node_failed:merge"),
        "expected merge task failure, got {:?}",
        parked.state.open_attention
    );
    let merge = parked
        .state
        .tasks
        .get(&MERGE.parse_task())
        .expect("merge task");
    assert_eq!(merge.status, lionclaw::model::TaskStatus::Failed);
    let failure = parked
        .state
        .role_attempt_receipts
        .values()
        .find_map(|receipt| receipt.failure())
        .expect("merge failure");
    assert_eq!(
        failure.evidence().code.as_deref(),
        Some("workspace.merge_conflict")
    );
}

#[tokio::test]
async fn serial_single_writer_repair_flow_keeps_slice8_projection() {
    let dir = TempDir::new().expect("tempdir");
    initialize_repository(dir.path());
    let runner = Arc::new(SerialRoleRunner::default());
    let oracle = Arc::new(ScriptedOracleRunner::fail_once("cargo-test"));
    let mut mission_type = common::test_mission_type();
    mission_type.edit_for_testing(|definition| {
        definition.execution.effect_capacity = 4;
        definition.execution.auto_continue_candidate = true;
        definition.execution.auto_continue_proof = true;
    });
    let engine = Engine::new(
        MissionStore::open(dir.path()).await.expect("open store"),
        mission_type,
        "localhost/lionclaw-runtime-dev:v1".to_string(),
        EngineServices::new(
            runner.clone(),
            oracle.clone(),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );
    let mission_id = engine
        .create_mission(
            dir.path().to_str().expect("utf8"),
            "serial repair",
            BASE_SHA,
        )
        .await
        .expect("create");
    engine
        .propose_plan(&mission_id, common::proposal(0, common::simple_plan()))
        .await
        .expect("propose");
    approve_plan(&engine, &mission_id).await;

    let parked = engine.advance(&mission_id).await.expect("first advance");
    assert_eq!(parked.disposition, MissionDisposition::Parked);
    let first_head = parked.state.deliverable_head().to_string();
    engine
        .decide(
            &mission_id,
            "oracle_verdict_failed:cargo-test",
            DecisionAction::Repair,
            "repair serial task",
        )
        .await
        .expect("repair");
    let done = engine.advance(&mission_id).await.expect("second advance");
    assert_eq!(done.disposition, MissionDisposition::Terminal);
    let calls = runner.calls.lock().expect("lock").clone();
    assert_eq!(
        calls,
        vec![
            (Some("fix".to_string()), 1, BASE_SHA.to_string()),
            (Some("fix".to_string()), 2, first_head.clone())
        ]
    );
    let events = engine.store().load(&mission_id).await.expect("events");
    let receipts = events
        .iter()
        .filter_map(|event| match &event.event {
            MissionEvent::RoleTurnRequested {
                task_id,
                attempt_no,
                base_sha,
                dependency_refs,
                ..
            } => Some((
                task_id.as_ref().map(ToString::to_string),
                *attempt_no,
                base_sha.clone(),
                dependency_refs.clone(),
            )),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(receipts.len(), 2);
    assert_eq!(
        receipts[0],
        (Some("fix".to_string()), 1, BASE_SHA.to_string(), vec![])
    );
    assert_eq!(
        receipts[1],
        (Some("fix".to_string()), 2, first_head, vec![])
    );
    assert_eq!(
        done.state.tasks[&"fix".parse_task()]
            .candidate_sha
            .as_deref(),
        Some(done.state.deliverable_head())
    );
    assert_eq!(
        oracle.judged_shas().last(),
        Some(&done.state.deliverable_head().to_string())
    );
}

#[test]
fn parallel_writer_completion_order_is_fold_equivalent_and_stale_lineages_are_rejected() {
    let mission_id = lionclaw::model::MissionId::parse("mabc123abc123").expect("mission id");
    let base = BASE_SHA.to_string();
    let plan = parallel_plan(false);
    let team = assigned_team(1, &plan);
    let mut prefix = vec![EventEnvelope {
        mission_id: mission_id.clone(),
        sequence_no: 1,
        recorded_at_ms: 1,
        stamps: stamps(),
        event: MissionEvent::MissionCreated {
            objective: "parallel".to_string(),
            mission_type: lionclaw::model::MissionTypeRef {
                name: "parallel-test".to_string(),
                digest: "digest".to_string(),
            },
            image_id: "image".to_string(),
            workspace_dir: "/repo".to_string(),
            base_sha: base.clone(),
            config: MissionConfig {
                stop: StopBar::Verified,
                execution: ExecutionPolicy {
                    effect_capacity: 2,
                    ..Default::default()
                },
                ceilings: AuthorityCeilings {
                    writes: true,
                    ..Default::default()
                },
                oracles: BTreeSet::from([
                    OracleName::new("cargo-left").unwrap(),
                    OracleName::new("cargo-test").unwrap(),
                ]),
                ..Default::default()
            },
            delegation: Default::default(),
        },
    }];
    prefix.push(envelope(
        &mission_id,
        2,
        MissionEvent::TeamConfigured {
            team: common::team(0, None, false),
        },
    ));
    prefix.push(envelope(
        &mission_id,
        3,
        MissionEvent::ProposalRecorded {
            proposal: Box::new(MissionProposal {
                team: Some(team.clone()),
                plan: Some(PlanProposal {
                    base_revision: 0,
                    requirement_changes: vec![],
                    assertion_supersessions: vec![],
                    plan: plan.clone(),
                }),
            }),
            proposal_hash: "proposal".to_string(),
        },
    ));
    prefix.push(envelope(
        &mission_id,
        4,
        MissionEvent::DecisionRecorded {
            attention_id: "plan_proposal:mission".to_string(),
            action: DecisionAction::Approve,
            justification: "approve".to_string(),
            requirement_changes: vec![],
        },
    ));
    prefix.push(envelope(
        &mission_id,
        5,
        MissionEvent::TeamConfigured { team },
    ));
    let left_request = role_request(&mission_id, 6, LEFT, "left-writer", 1, &base, vec![]);
    let right_request = role_request(&mission_id, 7, RIGHT, "right-writer", 1, &base, vec![]);
    let left_done = role_success(left_request.effect_id(), "left-head");
    let right_done = role_success(right_request.effect_id(), "right-head");

    let mut left_first = prefix.clone();
    left_first.push(left_request);
    left_first.push(right_request.clone());
    left_first.push(envelope(&mission_id, 8, left_done.clone()));
    left_first.push(envelope(&mission_id, 9, right_done.clone()));
    let mut right_first = prefix.clone();
    right_first.push(role_request(
        &mission_id,
        6,
        RIGHT,
        "right-writer",
        1,
        &base,
        vec![],
    ));
    right_first.push(role_request(
        &mission_id,
        7,
        LEFT,
        "left-writer",
        1,
        &base,
        vec![],
    ));
    right_first.push(envelope(&mission_id, 8, right_done));
    right_first.push(envelope(&mission_id, 9, left_done));
    let left_state = lionclaw::model::fold(left_first).expect("left fold");
    let right_state = lionclaw::model::fold(right_first).expect("right fold");
    assert_eq!(left_state.current_sha, base);
    assert_eq!(right_state.current_sha, base);
    assert_eq!(
        left_state.tasks[&LEFT.parse_task()].candidate_sha,
        right_state.tasks[&LEFT.parse_task()].candidate_sha
    );
    assert_eq!(
        left_state.tasks[&RIGHT.parse_task()].candidate_sha,
        right_state.tasks[&RIGHT.parse_task()].candidate_sha
    );

    let stale = role_request(
        &mission_id,
        10,
        MERGE,
        "integrator",
        1,
        "left-head",
        vec![
            TaskCandidateRef {
                task_id: LEFT.parse_task(),
                sha: "older-left".to_string(),
            },
            TaskCandidateRef {
                task_id: RIGHT.parse_task(),
                sha: "right-head".to_string(),
            },
        ],
    );
    let mut stale_state = left_state.clone();
    lionclaw::model::apply(&mut stale_state, &stale);
    assert!(stale_state.inflight.is_empty());
}

#[derive(Default)]
struct SerialRoleRunner {
    calls: Mutex<Vec<(Option<String>, u32, String)>>,
}

#[async_trait]
impl RoleRunner for SerialRoleRunner {
    async fn run(&self, request: RoleTurnRequest) -> Result<RoleTurnOutcome, TypedFailure> {
        self.calls.lock().expect("lock").push((
            request.task_id.as_ref().map(ToString::to_string),
            request.attempt_no,
            request.base_sha.clone(),
        ));
        lionclaw::testing::prepare_test_workspace(&request).await?;
        let checkout = checkout(&request)?;
        std::fs::write(
            checkout.join(format!("serial-{}.txt", request.attempt_no)),
            format!("serial {}\n", request.attempt_no),
        )
        .map_err(infra)?;
        git(checkout, &["add", "-A"])?;
        git_commit(
            checkout,
            &format!("serial candidate {}", request.attempt_no),
            request.attempt_no,
        )?;
        captured_work(&request).await
    }
}

fn mission_type() -> MissionType {
    MissionType::for_testing(MissionTypeDefinition {
        name: "parallel-writer-test".to_string(),
        stop: StopBar::Verified,
        image: "localhost/lionclaw-runtime-dev:v1".to_string(),
        environment: BTreeMap::new(),
        default_team: default_team(),
        ceilings: AuthorityCeilings {
            writes: true,
            ..Default::default()
        },
        resource_ceilings: Default::default(),
        oracle_resources: Default::default(),
        requires_gap_review: false,
        recovery: Default::default(),
        execution: ExecutionPolicy {
            effect_capacity: 2,
            auto_continue_candidate: true,
            auto_continue_proof: true,
            ..Default::default()
        },
        playbook: None,
        skills: BTreeMap::new(),
        inputs: BTreeMap::new(),
        oracles: BTreeMap::from([
            (
                OracleName::new("cargo-left").expect("oracle name"),
                "/nonexistent-left".into(),
            ),
            (
                OracleName::new("cargo-test").expect("oracle name"),
                "/nonexistent".into(),
            ),
        ]),
        oracle_devices: Default::default(),
    })
}

fn default_team() -> TeamRevision {
    TeamRevision {
        revision: 0,
        roles: roles(),
        planning_assignment: RoleInstanceId::new("strategist").unwrap(),
        task_assignments: BTreeMap::new(),
        judgment_assignments: BTreeMap::new(),
        gap_review_assignment: None,
        guidance: None,
    }
}

fn assigned_team(revision: u32, plan: &Plan) -> TeamRevision {
    TeamRevision {
        revision,
        roles: roles(),
        planning_assignment: RoleInstanceId::new("strategist").unwrap(),
        task_assignments: BTreeMap::from([
            (
                LEFT.parse_task(),
                RoleInstanceId::new("left-writer").unwrap(),
            ),
            (
                RIGHT.parse_task(),
                RoleInstanceId::new("right-writer").unwrap(),
            ),
            (
                MERGE.parse_task(),
                RoleInstanceId::new("integrator").unwrap(),
            ),
        ]),
        judgment_assignments: plan
            .assertions
            .iter()
            .map(|assertion| {
                (
                    assertion.id.clone(),
                    vec![RoleInstanceId::new(REVIEWER).unwrap()],
                )
            })
            .collect(),
        gap_review_assignment: None,
        guidance: None,
    }
}

fn roles() -> BTreeMap<RoleInstanceId, RoleInstance> {
    [
        role("strategist", OutputSemantics::ProposesPlan, false),
        role("left-writer", OutputSemantics::ProducesArtifact, true),
        role("right-writer", OutputSemantics::ProducesArtifact, true),
        role("integrator", OutputSemantics::ProducesArtifact, true),
        role(REVIEWER, OutputSemantics::EmitsVerdict, false),
    ]
    .into_iter()
    .map(|role| (role.id.clone(), role))
    .collect()
}

fn role(id: &str, output: OutputSemantics, writes: bool) -> RoleInstance {
    RoleInstance {
        id: RoleInstanceId::new(id).unwrap(),
        purpose: id.replace('-', " "),
        output,
        runtime: "codex".to_string(),
        instructions: format!("Test role {id}."),
        skills: Vec::new(),
        environment: BTreeMap::new(),
        grants: AuthorityGrants {
            writes,
            ..Default::default()
        },
        resources: Default::default(),
        deadline_secs: None,
    }
}

fn parallel_plan(conflict: bool) -> Plan {
    let left = AssertionId::new("LEFT-OK").unwrap();
    let right = AssertionId::new("RIGHT-OK").unwrap();
    let merged = AssertionId::new("MERGED-OK").unwrap();
    Plan {
        requirements: vec![
            covered_requirement("LEFT-DONE", "LEFT-OK"),
            covered_requirement("RIGHT-DONE", "RIGHT-OK"),
            covered_requirement("MERGED-DONE", "MERGED-OK"),
        ],
        assertions: vec![
            assertion(left.clone(), "left work is present", "cargo-left"),
            assertion(right.clone(), "right work is present", "cargo-test"),
            assertion(merged.clone(), "both lineages are integrated", "cargo-test"),
        ],
        tasks: vec![
            Task {
                id: LEFT.parse_task(),
                body: if conflict {
                    "Write the left side with a shared-file conflict.".to_string()
                } else {
                    "Write the left side.".to_string()
                },
                targets: vec![left],
                depends_on: Vec::new(),
            },
            Task {
                id: RIGHT.parse_task(),
                body: if conflict {
                    "Write the right side with a shared-file conflict.".to_string()
                } else {
                    "Write the right side.".to_string()
                },
                targets: vec![right],
                depends_on: Vec::new(),
            },
            Task {
                id: MERGE.parse_task(),
                body: "Merge the left and right candidates forward.".to_string(),
                targets: vec![merged],
                depends_on: vec![LEFT.parse_task(), RIGHT.parse_task()],
            },
        ],
    }
}

fn assertion(id: AssertionId, prose: &str, oracle: &str) -> Assertion {
    Assertion {
        id,
        prose: prose.to_string(),
        oracle: Some(OracleName::new(oracle).unwrap()),
    }
}

async fn start_parallel_mission(engine: &Engine, repo: &Path) -> lionclaw::model::MissionId {
    let plan = parallel_plan(false);
    let mission_id = engine
        .create_mission(repo.to_str().expect("utf8"), "parallel writers", BASE_SHA)
        .await
        .expect("create");
    engine
        .propose_plan(
            &mission_id,
            MissionProposal {
                team: Some(assigned_team(1, &plan)),
                plan: Some(PlanProposal {
                    base_revision: 0,
                    requirement_changes: vec![],
                    assertion_supersessions: vec![],
                    plan,
                }),
            },
        )
        .await
        .expect("propose");
    approve_plan(engine, &mission_id).await;
    mission_id
}

fn validate_outcome(request: &RoleTurnRequest) -> RoleTurnOutcome {
    RoleTurnOutcome {
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
        runtime_configuration: Default::default(),
        runtime_usage: Default::default(),
        final_response: "judged".to_string(),
    }
}

async fn captured_work(request: &RoleTurnRequest) -> Result<RoleTurnOutcome, TypedFailure> {
    let artifact = request
        .artifact_capture
        .as_ref()
        .expect("capture")
        .capture()
        .await
        .map_err(|error| TypedFailure::permanent("testing.capture", error.to_string()))?;
    Ok(RoleTurnOutcome {
        handoff: Some(Handoff::Work {
            done: true,
            report: PayloadRef::inline("done"),
            request_attention: false,
        }),
        artifact: Some(artifact),
        runtime_configuration: Default::default(),
        runtime_usage: Default::default(),
        final_response: "done".to_string(),
    })
}

fn checkout(request: &RoleTurnRequest) -> Result<&Path, TypedFailure> {
    request
        .artifact_capture
        .as_ref()
        .map(|capture| capture.checkout_dir())
        .ok_or_else(|| TypedFailure::permanent("testing.capture", "missing capture authority"))
}

fn git_configure(repo: &Path) -> Result<(), TypedFailure> {
    git(repo, &["config", "user.name", "LionClaw Parallel Test"])?;
    git(repo, &["config", "user.email", "parallel@lionclaw.local"])?;
    git(repo, &["config", "commit.gpgsign", "false"])
}

fn git_commit(repo: &Path, message: &str, offset: u32) -> Result<(), TypedFailure> {
    git_configure(repo)?;
    let date = format!("2000-01-{:02}T00:00:00Z", offset.min(28) + 1);
    let status = std::process::Command::new("git")
        .current_dir(repo)
        .env("GIT_AUTHOR_DATE", &date)
        .env("GIT_COMMITTER_DATE", &date)
        .args(["commit", "--quiet", "-m", message])
        .status()
        .map_err(infra)?;
    if status.success() {
        Ok(())
    } else {
        Err(TypedFailure::permanent(
            "testing.git",
            format!("git commit failed with {status}"),
        ))
    }
}

fn git(repo: &Path, args: &[&str]) -> Result<(), TypedFailure> {
    if git_status(repo, args)? {
        Ok(())
    } else {
        Err(TypedFailure::permanent(
            "testing.git",
            format!("git {} failed", args.join(" ")),
        ))
    }
}

fn git_status(repo: &Path, args: &[&str]) -> Result<bool, TypedFailure> {
    let status = std::process::Command::new("git")
        .current_dir(repo)
        .args(args)
        .status()
        .map_err(infra)?;
    Ok(status.success())
}

fn infra(error: impl std::fmt::Display) -> TypedFailure {
    TypedFailure::permanent("testing.infrastructure", error.to_string())
}

fn assert_writer_overlap(log: &ParallelLog) {
    let events = log.events.lock().expect("lock");
    let first_wave = events.iter().take(2).cloned().collect::<BTreeSet<_>>();
    assert_eq!(
        first_wave,
        BTreeSet::from([
            WriterEvent::Started(LEFT.to_string()),
            WriterEvent::Started(RIGHT.to_string())
        ]),
        "writers did not both start before completion: {events:?}"
    );
    assert!(events.contains(&WriterEvent::Ended(LEFT.to_string())));
    assert!(events.contains(&WriterEvent::Ended(RIGHT.to_string())));
}

fn assert_distinct_workspaces(log: &ParallelLog) {
    let workspaces = log.workspaces.lock().expect("lock");
    let left = workspaces.get(LEFT).expect("left workspace");
    let right = workspaces.get(RIGHT).expect("right workspace");
    assert_ne!(left, right);
}

fn assert_ancestor(repo: &Path, ancestor: &str, descendant: &str) {
    let status = std::process::Command::new("git")
        .current_dir(repo)
        .args(["merge-base", "--is-ancestor", ancestor, descendant])
        .status()
        .expect("git merge-base");
    assert!(
        status.success(),
        "expected {ancestor} to be ancestor of {descendant}"
    );
}

fn envelope(
    mission_id: &lionclaw::model::MissionId,
    sequence_no: u64,
    event: MissionEvent,
) -> EventEnvelope {
    EventEnvelope {
        mission_id: mission_id.clone(),
        sequence_no,
        recorded_at_ms: sequence_no as i64,
        stamps: stamps(),
        event,
    }
}

fn role_request(
    mission_id: &lionclaw::model::MissionId,
    sequence_no: u64,
    task: &str,
    role: &str,
    attempt_no: u32,
    base_sha: &str,
    dependency_refs: Vec<TaskCandidateRef>,
) -> EventEnvelope {
    let role_instance = RoleInstanceId::new(role).unwrap();
    let task_id = Some(task.parse_task());
    let prompt_hash = format!("{task}-{attempt_no}");
    let effect_id = EffectId::for_role_turn(
        mission_id,
        &role_instance,
        1,
        task_id.as_ref(),
        attempt_no,
        1,
        &prompt_hash,
    );
    envelope(
        mission_id,
        sequence_no,
        MissionEvent::RoleTurnRequested {
            role_instance,
            team_revision: 1,
            task_id,
            assertion_ids: vec![],
            attempt_no,
            effect_id,
            prompt_template: RolePromptTemplate::Execution,
            prompt_hash,
            base_sha: base_sha.to_string(),
            dependency_refs,
            assignment_epoch: 1,
            message_boundary: sequence_no - 1,
            presented_messages: vec![],
            workspace_preparation: WorkspacePreparation::ResetForAssignment,
            requested_at_ms: sequence_no as i64,
            deadline_ms: 1000,
            budget_deadline_ms: 1000,
        },
    )
}

trait EffectIdFromEnvelope {
    fn effect_id(&self) -> EffectId;
}

impl EffectIdFromEnvelope for EventEnvelope {
    fn effect_id(&self) -> EffectId {
        match &self.event {
            MissionEvent::RoleTurnRequested { effect_id, .. } => effect_id.clone(),
            _ => panic!("expected request"),
        }
    }
}

fn role_success(effect_id: EffectId, head_sha: &str) -> MissionEvent {
    MissionEvent::RoleTurnCompleted {
        effect_id,
        outcome: Ok(lionclaw::model::RoleTurnSuccess {
            handoff: Some(Handoff::Work {
                done: true,
                report: PayloadRef::inline("done"),
                request_attention: false,
            }),
            artifact: Some(lionclaw::model::ArtifactOutcome {
                base_sha: BASE_SHA.to_string(),
                head_sha: head_sha.to_string(),
            }),
            runtime_configuration: Default::default(),
            runtime_usage: Default::default(),
            final_response: PayloadRef::inline("done"),
        }),
    }
}

fn stamps() -> VersionStamps {
    VersionStamps {
        schema_version: SCHEMA_VERSION,
        engine_version: "test".to_string(),
        prompt_hash: None,
    }
}
