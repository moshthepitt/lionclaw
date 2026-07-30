//! Shared schema-24 integration harness.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use lionclaw::engine::{Engine, EngineServices, MissionView};
use lionclaw::mission_type::{MissionType, MissionTypeDefinition};
use lionclaw::model::{
    Assertion, AssertionId, AuthorityCeilings, AuthorityGrants, Choice, CommandOracle,
    ConfinementResources, FinishClass, MissionId, MissionProposal, OracleName, OracleSpec,
    OutputSemantics, Plan, PlanProposal, Requirement, RequirementDisposition, RequirementId,
    RequirementKind, RoleInstance, RoleInstanceId, RuntimeInstrumentIdentity, StopBar, Task,
    TeamRevision, WorkspaceRelativeDir,
};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockClock, MockOracleRunner, MockRoleRunner, NoopEffectCleaner};

pub const BASE_SHA: &str = "68416f3db8602142a2b732a91cfb8cc86b898f17";
pub const HEAD_SHA: &str = "27c714953fa5f62e2d627b2cd0e7a7e67aabba37";

pub fn initialize_repository(workspace: &Path) {
    if workspace.join(".git").is_dir() {
        return;
    }
    let run = |args: &[&str]| {
        let status = std::process::Command::new("git")
            .current_dir(workspace)
            .args(args)
            .status()
            .expect("run Git for integration repository");
        assert!(status.success(), "git {} failed", args.join(" "));
    };
    run(&["init", "--quiet"]);
    run(&["config", "user.name", "LionClaw Test Base"]);
    run(&["config", "user.email", "test@lionclaw.local"]);
    run(&["config", "commit.gpgsign", "false"]);
    std::fs::write(workspace.join("fixture.txt"), "base\n").expect("write base fixture");
    run(&["add", "fixture.txt"]);
    let status = std::process::Command::new("git")
        .current_dir(workspace)
        .env("GIT_AUTHOR_DATE", "2000-01-01T00:00:00Z")
        .env("GIT_COMMITTER_DATE", "2000-01-01T00:00:00Z")
        .args(["commit", "--quiet", "-m", "LionClaw test base"])
        .status()
        .expect("commit integration repository base");
    assert!(status.success(), "git commit failed");
    let actual = std::process::Command::new("git")
        .current_dir(workspace)
        .args(["rev-parse", "HEAD"])
        .output()
        .expect("resolve integration repository base");
    assert!(actual.status.success());
    assert_eq!(String::from_utf8(actual.stdout).unwrap().trim(), BASE_SHA);
}

/// Test-only event-log fault injection for crash and replay properties.
pub async fn fault_append_events(
    workspace: &Path,
    mission_id: &lionclaw::model::MissionId,
    expected_head: u64,
    events: &[lionclaw::store::NewEvent],
    now_ms: i64,
) -> u64 {
    let database = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}",
        workspace.join(".lionclaw/mission.db").display()
    ))
    .await
    .expect("open mission database for fault injection");
    let mut transaction = database.begin().await.expect("begin fault append");
    let actual: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(sequence_no), 0) FROM mission_events WHERE mission_id = ?1",
    )
    .bind(mission_id.as_str())
    .fetch_one(&mut *transaction)
    .await
    .expect("read fault append head");
    assert_eq!(actual as u64, expected_head, "fault append head changed");

    let mut sequence = expected_head;
    for event in events {
        sequence += 1;
        let payload = serde_json::to_string(&serde_json::json!({
            "stamps": event.stamps,
            "event": event.event,
        }))
        .expect("encode fault event");
        sqlx::query(
            "INSERT INTO mission_events
                 (mission_id, sequence_no, recorded_at_ms, schema_version, payload_json)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(mission_id.as_str())
        .bind(sequence as i64)
        .bind(now_ms)
        .bind(event.stamps.schema_version)
        .bind(payload)
        .execute(&mut *transaction)
        .await
        .expect("inject fault event");
    }
    transaction.commit().await.expect("commit fault events");
    sequence
}

pub fn effect_id(label: &str) -> lionclaw::model::EffectId {
    lionclaw::model::EffectId::for_parts(&["test", label])
}

pub async fn approve_plan(engine: &Engine, mission_id: &lionclaw::model::MissionId) {
    engine
        .decide(
            mission_id,
            "plan_proposal:mission",
            lionclaw::model::DecisionAction::Approve,
            "test approval",
        )
        .await
        .expect("approve plan");
}

pub fn decision_actions(
    state: &lionclaw::model::MissionState,
    id: &str,
) -> Vec<lionclaw::model::DecisionAction> {
    lionclaw::model::next(state)
        .choices
        .into_iter()
        .filter_map(|choice| match choice {
            Choice::Decide {
                id: choice_id,
                action,
            } if choice_id == id => Some(action),
            _ => None,
        })
        .collect()
}

pub fn has_decision(state: &lionclaw::model::MissionState, id: &str) -> bool {
    !decision_actions(state, id).is_empty()
}

pub fn finish_choice(state: &lionclaw::model::MissionState) -> Option<FinishClass> {
    lionclaw::model::next(state)
        .choices
        .into_iter()
        .find_map(|choice| match choice {
            Choice::Finish { finish } => Some(finish),
            _ => None,
        })
}

pub fn decision_ids(state: &lionclaw::model::MissionState) -> Vec<String> {
    lionclaw::model::next(state)
        .choices
        .into_iter()
        .filter_map(|choice| match choice {
            Choice::Decide { id, .. } => Some(id),
            _ => None,
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub fn decision_id_with_prefix(state: &lionclaw::model::MissionState, prefix: &str) -> String {
    let matches = decision_ids(state)
        .into_iter()
        .filter(|id| id.starts_with(prefix))
        .collect::<Vec<_>>();
    assert_eq!(
        matches.len(),
        1,
        "expected one decision starting with {prefix:?}, got {matches:?}"
    );
    matches.into_iter().next().unwrap()
}

pub async fn advance_to_finished(engine: &Engine, mission_id: &MissionId) -> MissionView {
    let ready = engine
        .advance(mission_id)
        .await
        .expect("advance to finish gate");
    assert!(
        ready
            .next
            .choices
            .iter()
            .any(|choice| matches!(choice, Choice::Finish { .. })),
        "mission was not ready to finish: {ready:?}"
    );
    engine
        .finish(mission_id, "test finish")
        .await
        .expect("record finish");
    engine.advance(mission_id).await.expect("advance terminal")
}

pub fn role(id: &str, output: OutputSemantics) -> RoleInstance {
    RoleInstance {
        id: RoleInstanceId::new(id).expect("role instance id"),
        purpose: id.replace('-', " "),
        output,
        runtime: if id == "gap-reviewer" {
            "opencode".to_string()
        } else {
            "codex".to_string()
        },
        instructions: format!("Test role {id}."),
        skills: Vec::new(),
        environment: BTreeMap::new(),
        grants: AuthorityGrants {
            writes: output == OutputSemantics::ProducesArtifact,
            ..Default::default()
        },
        resources: Default::default(),
        deadline_secs: None,
    }
}

pub fn team(revision: u32, plan: Option<&Plan>, gap_review: bool) -> TeamRevision {
    let roles = BTreeMap::from([
        (
            RoleInstanceId::new("strategist").unwrap(),
            role("strategist", OutputSemantics::ProposesPlan),
        ),
        (
            RoleInstanceId::new("implementer").unwrap(),
            role("implementer", OutputSemantics::ProducesArtifact),
        ),
        (
            RoleInstanceId::new("reviewer").unwrap(),
            role("reviewer", OutputSemantics::EmitsVerdict),
        ),
        (
            RoleInstanceId::new("gap-reviewer").unwrap(),
            role("gap-reviewer", OutputSemantics::EmitsGapVerdict),
        ),
    ]);
    TeamRevision {
        revision,
        roles,
        planning_assignment: RoleInstanceId::new("strategist").unwrap(),
        task_assignments: plan
            .into_iter()
            .flat_map(|plan| &plan.tasks)
            .map(|task| (task.id.clone(), RoleInstanceId::new("implementer").unwrap()))
            .collect(),
        judgment_assignments: plan
            .into_iter()
            .flat_map(|plan| &plan.assertions)
            .map(|assertion| {
                (
                    assertion.id.clone(),
                    vec![RoleInstanceId::new("reviewer").unwrap()],
                )
            })
            .collect(),
        gap_review_assignment: gap_review.then(|| RoleInstanceId::new("gap-reviewer").unwrap()),
        guidance: None,
    }
}

fn mission_type(requires_gap_review: bool) -> MissionType {
    MissionType::for_testing(MissionTypeDefinition {
        name: "software-dev-test".to_string(),
        stop: StopBar::Verified,
        image: "localhost/lionclaw-runtime-dev:v1".to_string(),
        environment: BTreeMap::new(),
        default_team: team(0, None, requires_gap_review),
        ceilings: AuthorityCeilings {
            writes: true,
            ..Default::default()
        },
        resource_ceilings: Default::default(),
        requires_gap_review,
        recovery: Default::default(),
        execution: lionclaw::model::ExecutionPolicy {
            auto_continue_candidate: true,
            auto_continue_proof: true,
            ..Default::default()
        },
        playbook: None,
        skills: BTreeMap::new(),
        inputs: BTreeMap::new(),
    })
}

pub fn test_mission_type() -> MissionType {
    mission_type(false)
}

pub fn review_mission_type() -> MissionType {
    mission_type(true)
}

/// One advisory-only assertion with team-owned judgment assignment.
pub fn advisory_plan() -> Plan {
    Plan {
        requirements: vec![reviewer_checkable_requirement("READABLE-CODE", "STYLE-OK")],
        assertions: vec![Assertion {
            id: AssertionId::new("STYLE-OK").expect("assertion id"),
            prose: "the code reads cleanly".to_string(),
            oracle: None,
        }],
        tasks: vec![Task {
            id: "write".parse_task(),
            body: "Write the code.".to_string(),
            targets: vec![AssertionId::new("STYLE-OK").expect("assertion id")],
            depends_on: Vec::new(),
        }],
    }
}

pub fn simple_plan() -> Plan {
    Plan {
        requirements: vec![covered_requirement("GREEN-TESTS", "TESTS-PASS")],
        assertions: vec![Assertion {
            id: AssertionId::new("TESTS-PASS").expect("assertion id"),
            prose: "cargo test exits 0".to_string(),
            oracle: Some(OracleName::new("cargo-test").expect("oracle name")),
        }],
        tasks: vec![Task {
            id: "fix".parse_task(),
            body: "Make the failing test pass.".to_string(),
            targets: vec![AssertionId::new("TESTS-PASS").expect("assertion id")],
            depends_on: Vec::new(),
        }],
    }
}

pub fn covered_requirement(id: &str, assertion: &str) -> Requirement {
    Requirement {
        id: RequirementId::new(id).expect("requirement id"),
        kind: RequirementKind::Capability,
        prose: id.to_ascii_lowercase().replace('-', " "),
        disposition: RequirementDisposition::ConfinedProvable {
            assertion_ids: vec![AssertionId::new(assertion).expect("assertion id")],
        },
    }
}

pub fn reviewer_checkable_requirement(id: &str, assertion: &str) -> Requirement {
    Requirement {
        id: RequirementId::new(id).expect("requirement id"),
        kind: RequirementKind::Capability,
        prose: id.to_ascii_lowercase().replace('-', " "),
        disposition: RequirementDisposition::ReviewerCheckable {
            assertion_ids: vec![AssertionId::new(assertion).expect("assertion id")],
        },
    }
}

pub fn oracle_specs(plan: &Plan) -> BTreeMap<OracleName, OracleSpec> {
    plan.assertions
        .iter()
        .filter_map(|assertion| assertion.oracle.clone())
        .map(|oracle| {
            (
                oracle,
                OracleSpec::Command(CommandOracle {
                    argv: vec!["true".to_string()],
                    cwd: WorkspaceRelativeDir::new(".").unwrap(),
                    environment: BTreeMap::new(),
                    timeout_secs: 60,
                    grants: AuthorityGrants::default(),
                    resources: ConfinementResources::default(),
                }),
            )
        })
        .collect()
}

pub fn proposal(base_revision: u32, plan: Plan) -> MissionProposal {
    let oracles = oracle_specs(&plan);
    MissionProposal {
        team: Some(team(base_revision.saturating_add(1), Some(&plan), false)),
        plan: Some(PlanProposal {
            base_revision,
            requirement_changes: vec![],
            assertion_supersessions: vec![],
            plan,
        }),
        oracles: Some(oracles),
    }
}

pub fn proposal_with_oracle_timeout(
    base_revision: u32,
    plan: Plan,
    timeout_secs: u64,
) -> MissionProposal {
    let mut proposal = proposal(base_revision, plan);
    for spec in proposal
        .oracles
        .as_mut()
        .expect("test proposal declares its complete oracle map")
        .values_mut()
    {
        match spec {
            OracleSpec::Command(command) => command.timeout_secs = timeout_secs,
            OracleSpec::External(_) => {
                panic!("proposal_with_oracle_timeout only supports command oracle fixtures")
            }
        }
    }
    proposal
}

pub fn review_proposal(base_revision: u32, plan: Plan) -> MissionProposal {
    let oracles = oracle_specs(&plan);
    MissionProposal {
        team: Some(team(base_revision.saturating_add(1), Some(&plan), true)),
        plan: Some(PlanProposal {
            base_revision,
            requirement_changes: vec![],
            assertion_supersessions: vec![],
            plan,
        }),
        oracles: Some(oracles),
    }
}

pub fn proposal_with_team(
    base_revision: u32,
    plan: Plan,
    mut next_team: TeamRevision,
) -> MissionProposal {
    next_team.revision = base_revision.saturating_add(1);
    next_team.task_assignments = plan
        .tasks
        .iter()
        .map(|task| (task.id.clone(), RoleInstanceId::new("implementer").unwrap()))
        .collect();
    next_team.judgment_assignments = plan
        .assertions
        .iter()
        .map(|assertion| {
            (
                assertion.id.clone(),
                vec![RoleInstanceId::new("reviewer").unwrap()],
            )
        })
        .collect();
    let oracles = oracle_specs(&plan);
    MissionProposal {
        team: Some(next_team),
        plan: Some(PlanProposal {
            base_revision,
            requirement_changes: vec![],
            assertion_supersessions: vec![],
            plan,
        }),
        oracles: Some(oracles),
    }
}

pub fn proposal_from_plan(plan: PlanProposal, gap_review: bool) -> MissionProposal {
    let oracles = oracle_specs(&plan.plan);
    MissionProposal {
        team: Some(team(
            plan.base_revision.saturating_add(1),
            Some(&plan.plan),
            gap_review,
        )),
        plan: Some(plan),
        oracles: Some(oracles),
    }
}

pub trait ParseTask {
    fn parse_task(&self) -> lionclaw::model::TaskId;
}

impl ParseTask for str {
    fn parse_task(&self) -> lionclaw::model::TaskId {
        lionclaw::model::TaskId::new(self).expect("task id")
    }
}

#[allow(dead_code)]
pub struct TestHarness {
    pub engine: Engine,
    pub role_runner: Arc<MockRoleRunner>,
    pub oracle_runner: Arc<MockOracleRunner>,
}

pub async fn harness(
    workspace: &Path,
    role_runner: MockRoleRunner,
    oracle_runner: MockOracleRunner,
) -> TestHarness {
    harness_with_type(workspace, test_mission_type(), role_runner, oracle_runner).await
}

pub async fn harness_with_type(
    workspace: &Path,
    mission_type: MissionType,
    role_runner: MockRoleRunner,
    oracle_runner: MockOracleRunner,
) -> TestHarness {
    harness_with_type_and_runtime_identities(
        workspace,
        mission_type,
        role_runner,
        oracle_runner,
        default_runtime_identities(),
    )
    .await
}

pub fn default_runtime_identities() -> BTreeMap<String, RuntimeInstrumentIdentity> {
    BTreeMap::from([
        (
            "codex".to_string(),
            RuntimeInstrumentIdentity {
                runtime: "codex".to_string(),
                model: None,
                mode: None,
            },
        ),
        (
            "opencode".to_string(),
            RuntimeInstrumentIdentity {
                runtime: "opencode".to_string(),
                model: None,
                mode: None,
            },
        ),
    ])
}

pub async fn harness_with_type_and_runtime_identities(
    workspace: &Path,
    mission_type: MissionType,
    role_runner: MockRoleRunner,
    oracle_runner: MockOracleRunner,
    runtime_identities: BTreeMap<String, RuntimeInstrumentIdentity>,
) -> TestHarness {
    initialize_repository(workspace);
    let role_runner = Arc::new(role_runner);
    let oracle_runner = Arc::new(oracle_runner);
    let engine = engine_with_runtime_identities(
        workspace,
        mission_type,
        role_runner.clone(),
        oracle_runner.clone(),
        runtime_identities,
    )
    .await;
    TestHarness {
        engine,
        role_runner,
        oracle_runner,
    }
}

pub async fn engine_with_runtime_identities(
    workspace: &Path,
    mission_type: MissionType,
    role_runner: Arc<MockRoleRunner>,
    oracle_runner: Arc<MockOracleRunner>,
    runtime_identities: BTreeMap<String, RuntimeInstrumentIdentity>,
) -> Engine {
    let store = MissionStore::open(workspace).await.expect("open store");
    Engine::new(
        store,
        mission_type,
        "localhost/lionclaw-runtime-dev:v1".to_string(),
        EngineServices::new(
            role_runner,
            oracle_runner,
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        )
        .with_runtime_identities(runtime_identities),
    )
}

pub fn blocking_gap() -> lionclaw::model::Gap {
    lionclaw::model::Gap {
        id: Some("GAP-1".to_string()),
        severity: lionclaw::model::GapSeverity::Blocking,
        requirement: "the objective's behavior".to_string(),
        expected: "it works".to_string(),
        observed: "it does not".to_string(),
        evidence: "ran it; saw it fail".to_string(),
    }
}

/// A writer plus a gap reviewer whose verdicts are scripted per invocation.
pub fn review_runner(verdicts: Vec<(bool, Vec<lionclaw::model::Gap>)>) -> MockRoleRunner {
    use lionclaw::model::{Handoff, PayloadRef};
    use lionclaw::ports::{CapturedArtifact, RoleTurnOutcome};
    let reviews = std::sync::Mutex::new(0usize);
    MockRoleRunner::new(Box::new(move |request| {
        if request.role.output == OutputSemantics::EmitsGapVerdict {
            assert_eq!(request.role.runtime, "opencode");
            let mut seen = reviews.lock().expect("lock");
            let (passed, gaps) = verdicts[(*seen).min(verdicts.len() - 1)].clone();
            *seen += 1;
            Ok(lionclaw::testing::review_verdict(request, passed, gaps))
        } else if request.role.output == OutputSemantics::EmitsVerdict {
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
                final_response: "judged".to_string(),
            })
        } else {
            Ok(RoleTurnOutcome {
                handoff: Some(Handoff::Work {
                    done: true,
                    report: PayloadRef::inline("committed the change"),
                    request_attention: false,
                }),
                artifact: Some(CapturedArtifact::for_testing(
                    request.base_sha.clone(),
                    HEAD_SHA,
                )),
                prepared_inputs: Vec::new(),
                runtime_configuration: Default::default(),
                runtime_usage: Default::default(),
                final_response: String::new(),
            })
        }
    }))
}

pub fn oracle_names() -> BTreeSet<OracleName> {
    BTreeSet::from([OracleName::new("cargo-test").unwrap()])
}
