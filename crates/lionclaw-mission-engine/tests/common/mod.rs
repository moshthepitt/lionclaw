//! Shared harness for engine integration tests: tempdir-backed store, a
//! hand-built plugin (loading is exercised elsewhere), scripted mock ports.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use lionclaw_mission_engine::engine::Engine;
use lionclaw_mission_engine::model::{
    Assertion, AssertionId, MissionConfig, OracleName, OutputSemantics, PlanSubmission, RoleName,
    StopBar, Task, TaskKind,
};
use lionclaw_mission_engine::plugin::{LoadedPlugin, RoleDefinition};
use lionclaw_mission_engine::store::MissionStore;
use lionclaw_mission_engine::testing::{MockClock, MockOracleRunner, MockRoleRunner};

pub const BASE_SHA: &str = "0000000000000000000000000000000000000001";
pub const HEAD_SHA: &str = "0000000000000000000000000000000000000002";

pub fn test_plugin() -> LoadedPlugin {
    let implementer = RoleName::new("implementer").expect("role name");
    let cargo_test = OracleName::new("cargo-test").expect("oracle name");
    LoadedPlugin {
        name: "software-dev-test".to_string(),
        stop: StopBar::Verified,
        root: "/nonexistent-plugin".into(),
        playbook: None,
        roles: BTreeMap::from([(
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
        )]),
        oracles: BTreeMap::from([(cargo_test, "/nonexistent-plugin/oracles/cargo-test".into())]),
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
    fn parse_task(&self) -> lionclaw_mission_engine::model::TaskId;
}

impl ParseTask for str {
    fn parse_task(&self) -> lionclaw_mission_engine::model::TaskId {
        lionclaw_mission_engine::model::TaskId::new(self).expect("task id")
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
        test_plugin(),
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
