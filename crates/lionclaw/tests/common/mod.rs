//! Shared harness for engine integration tests: tempdir-backed store, a
//! hand-built plugin (loading is exercised elsewhere), scripted mock ports.
//!
//! Each integration test binary compiles this module independently, so any
//! given test uses only a subset of these helpers — dead-code warnings for
//! the rest are expected.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use lionclaw::engine::Engine;
use lionclaw::mission_type::{MissionType, RoleDefinition};
use lionclaw::model::{
    Assertion, AssertionId, MissionConfig, OracleName, OutputSemantics, PlanSubmission, RoleName,
    StopBar, Task, TaskKind,
};
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockClock, MockOracleRunner, MockRoleRunner};

pub const BASE_SHA: &str = "0000000000000000000000000000000000000001";
pub const HEAD_SHA: &str = "0000000000000000000000000000000000000002";

pub fn test_mission_type() -> MissionType {
    let implementer = RoleName::new("implementer").expect("role name");
    let cargo_test = OracleName::new("cargo-test").expect("oracle name");
    MissionType {
        name: "software-dev-test".to_string(),
        digest: "test-digest".to_string(),
        // `Reviewed` so the shared harness accepts both oracle-bound and
        // advisory plans; the `Verified` submit-reachability check is exercised
        // in the plan_validation unit tests.
        stop: StopBar::Reviewed,
        image: "localhost/lionclaw-runtime-dev:v1".to_string(),
        root: "/nonexistent-mission-type".into(),
        playbook: None,
        roles: BTreeMap::from([
            (
                implementer.clone(),
                RoleDefinition {
                    name: implementer,
                    output: OutputSemantics::ProducesArtifact,
                    runtime: None,
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
                    network: false,
                    secrets: false,
                    skills: Vec::new(),
                    prompt_body: "Judge the code.".to_string(),
                },
            ),
        ]),
        oracles: BTreeMap::from([(cargo_test, "/nonexistent-plugin/oracles/cargo-test".into())]),
    }
}

/// A plan with a work task and a read-only reviewer over one oracle-less
/// assertion — advisory-only, so it can never verify.
pub fn advisory_plan() -> PlanSubmission {
    PlanSubmission {
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

pub fn simple_plan() -> PlanSubmission {
    PlanSubmission {
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
    let store = MissionStore::open(workspace).await.expect("open store");
    let role_runner = Arc::new(role_runner);
    let oracle_runner = Arc::new(oracle_runner);
    let engine = Engine::new(
        store,
        test_mission_type(),
        "codex".to_string(),
        "localhost/lionclaw-runtime-dev:v1".to_string(),
        role_runner.clone(),
        oracle_runner.clone(),
        Arc::new(MockClock::default()),
    );
    TestHarness {
        engine,
        role_runner,
        oracle_runner,
    }
}

pub fn default_config() -> MissionConfig {
    MissionConfig {
        ratification_gate: false,
        ..Default::default()
    }
}
