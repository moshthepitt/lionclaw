//! Shared harness for engine integration tests: tempdir-backed store, a
//! hand-built mission type (loading is exercised elsewhere), scripted mock ports.
//!
//! Each integration test binary compiles this module independently, so any
//! given test uses only a subset of these helpers — dead-code warnings for
//! the rest are expected.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use lionclaw::engine::{Engine, EngineServices};
use lionclaw::mission_type::{MissionType, MissionTypeDefinition, RoleDefinition};
use lionclaw::model::{
    Assertion, AssertionId, OracleName, OutputSemantics, Plan, PlanProposal, Requirement,
    RequirementDisposition, RequirementId, RequirementKind, RoleName, StopBar, Task, TaskKind,
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

/// Test-only fault injection that writes directly to the on-disk log. Raw
/// production append is kernel-private; crash/replay tests deliberately bypass
/// that boundary through SQLite rather than reopening it in the public API.
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

pub fn test_mission_type() -> MissionType {
    let implementer = RoleName::new("implementer").expect("role name");
    let cargo_test = OracleName::new("cargo-test").expect("oracle name");
    MissionType::for_testing(MissionTypeDefinition {
        name: "software-dev-test".to_string(),
        stop: StopBar::Verified,
        image: "localhost/lionclaw-runtime-dev:v1".to_string(),
        planning: Default::default(),
        recovery: Default::default(),
        execution: lionclaw::model::ExecutionPolicy {
            auto_continue_candidate: true,
            auto_continue_proof: true,
            ..Default::default()
        },
        terminal_review: None,
        playbook: None,
        roles: BTreeMap::from([
            (
                implementer.clone(),
                RoleDefinition {
                    name: implementer,
                    output: OutputSemantics::ProducesArtifact,
                    runtime: None,
                    timeout_secs: None,
                    network: false,
                    secrets: false,
                    skills: Vec::new(),
                    prompt_body: "Fix the code.".to_string(),
                },
            ),
            (
                RoleName::new("reviewer").expect("role name"),
                RoleDefinition {
                    name: RoleName::new("reviewer").expect("role name"),
                    output: OutputSemantics::EmitsVerdict,
                    runtime: None,
                    timeout_secs: None,
                    network: false,
                    secrets: false,
                    skills: Vec::new(),
                    prompt_body: "Judge the code.".to_string(),
                },
            ),
        ]),
        skills: BTreeMap::new(),
        inputs: BTreeMap::new(),
        oracles: BTreeMap::from([(
            cargo_test,
            "/nonexistent-mission-type/oracles/cargo-test".into(),
        )]),
    })
}

/// `test_mission_type` plus a declared closing review: a fresh-context
/// `gap-reviewer` judge the engine dispatches once work and oracles settle.
pub fn review_mission_type() -> MissionType {
    let gap_reviewer = RoleName::new("gap-reviewer").expect("role name");
    let mut mission_type = test_mission_type();
    mission_type.edit_for_testing(|definition| {
        definition.roles.insert(
            gap_reviewer.clone(),
            RoleDefinition {
                name: gap_reviewer.clone(),
                output: OutputSemantics::EmitsGapVerdict,
                runtime: Some("opencode".to_string()),
                timeout_secs: None,
                network: false,
                secrets: false,
                skills: Vec::new(),
                prompt_body: "Hunt product gaps against the objective.".to_string(),
            },
        );
        definition.terminal_review =
            Some(lionclaw::model::TerminalReviewConfig { role: gap_reviewer });
    });
    mission_type
}

/// A plan with a work task and a read-only reviewer over one oracle-less
/// assertion — advisory-only, so it can never verify.
pub fn advisory_plan() -> Plan {
    Plan {
        requirements: vec![covered_requirement("READABLE-CODE", "STYLE-OK")],
        assertions: vec![Assertion {
            id: AssertionId::new("STYLE-OK").expect("assertion id"),
            prose: "the code reads cleanly".to_string(),
            oracle: None,
        }],
        tasks: vec![
            Task {
                id: "write".parse_task(),
                kind: TaskKind::Work,
                body: "Write the code.".to_string(),
                targets: vec![AssertionId::new("STYLE-OK").expect("assertion id")],
                role: Some(RoleName::new("implementer").expect("role name")),
                depends_on: Vec::new(),
            },
            Task {
                id: "review".parse_task(),
                kind: TaskKind::Validate,
                body: "Review the code.".to_string(),
                targets: vec![AssertionId::new("STYLE-OK").expect("assertion id")],
                role: Some(RoleName::new("reviewer").expect("role name")),
                depends_on: vec!["write".parse_task()],
            },
        ],
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
            kind: TaskKind::Work,
            body: "Make the failing test pass.".to_string(),
            targets: vec![AssertionId::new("TESTS-PASS").expect("assertion id")],
            role: Some(RoleName::new("implementer").expect("role name")),
            depends_on: Vec::new(),
        }],
    }
}

pub fn covered_requirement(id: &str, assertion: &str) -> Requirement {
    Requirement {
        id: RequirementId::new(id).expect("requirement id"),
        kind: RequirementKind::Capability,
        prose: id.to_ascii_lowercase().replace('-', " "),
        disposition: RequirementDisposition::Covered {
            assertion_ids: vec![AssertionId::new(assertion).expect("assertion id")],
        },
    }
}

pub fn proposal(base_revision: u32, plan: Plan) -> PlanProposal {
    PlanProposal {
        base_revision,
        plan,
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

// Fields are read by some test binaries and not others; each compiles this
// module independently, so unused-field warnings are expected and harmless.
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
    initialize_repository(workspace);
    let store = MissionStore::open(workspace).await.expect("open store");
    let role_runner = Arc::new(role_runner);
    let oracle_runner = Arc::new(oracle_runner);
    let engine = Engine::new(
        store,
        mission_type,
        "codex".to_string(),
        "localhost/lionclaw-runtime-dev:v1".to_string(),
        EngineServices::new(
            role_runner.clone(),
            oracle_runner.clone(),
            Arc::new(NoopEffectCleaner),
            Arc::new(MockClock::default()),
        ),
    );
    TestHarness {
        engine,
        role_runner,
        oracle_runner,
    }
}

/// One blocking gap, fully evidenced.
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

/// A worker that commits `HEAD_SHA` plus a terminal reviewer whose verdicts
/// are scripted per invocation (the last one repeats). The reviewer echoes
/// the prompt's nonce, exactly as a real agent must.
pub fn review_runner(verdicts: Vec<(bool, Vec<lionclaw::model::Gap>)>) -> MockRoleRunner {
    use lionclaw::model::{Handoff, PayloadRef};
    use lionclaw::ports::{CapturedArtifact, RoleRunOutcome};
    let reviews = std::sync::Mutex::new(0usize);
    MockRoleRunner::new(Box::new(move |request| {
        if request.task_id.as_str() == lionclaw::engine::TERMINAL_REVIEW_TASK_TAG {
            assert_eq!(request.runtime, "opencode");
            let mut seen = reviews.lock().expect("lock");
            let (passed, gaps) = verdicts[(*seen).min(verdicts.len() - 1)].clone();
            *seen += 1;
            Ok(lionclaw::testing::review_verdict(request, passed, gaps))
        } else {
            Ok(RoleRunOutcome {
                handoff: Some(Handoff::Work {
                    done: true,
                    report: PayloadRef::inline("committed the change"),
                    request_attention: false,
                }),
                artifact: Some(CapturedArtifact::for_testing(
                    request.base_sha.clone(),
                    HEAD_SHA,
                )),
                runtime_configuration: Default::default(),
                final_response: String::new(),
            })
        }
    }))
}
