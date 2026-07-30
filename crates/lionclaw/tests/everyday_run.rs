mod common;

use std::collections::VecDeque;
use std::os::unix::fs::FileTypeExt;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use lionclaw::cli::{self, Cli, MissionTransports};
use lionclaw::config::RuntimeProfiles;
use lionclaw::everyday::AttachedRuntimeExecutor;
use lionclaw::store::MissionStore;
use lionclaw::testing::{MockOracleRunner, MockRoleRunner};
use lionclaw_confinement::{ExecutionRequest, MountAccess};
use lionclaw_runtime_api::{
    ExecutionOutput, RuntimeAdapter, RuntimeAdapterInfo, RuntimeAuthIdentity, RuntimeAuthKind,
    RuntimeAuthMaterialization, RuntimeAuthPreparation, RuntimeAuthProjection, RuntimeAuthProvider,
    RuntimeAuthRegistry, RuntimeCancellation, RuntimeDriverConfig, RuntimeDriverProvider,
    RuntimeDriverRegistry, RuntimeProgramSpec, RuntimeSessionHandle, RuntimeSessionStartInput,
    RuntimeTerminalProgramInput, RuntimeTurnJournalSender, TurnExecution, TurnResult,
};

const NONTERMINAL_EXIT: u8 = 2;

#[derive(Default)]
struct Observations {
    validations: usize,
    auth_preparations: usize,
    terminal_inputs: Vec<RuntimeTerminalProgramInput>,
    requests: Vec<ExecutionRequest>,
    context: Vec<String>,
    bridge_socket_mounts: usize,
    projected_clients: Vec<String>,
}

struct FakeDriver {
    observations: Arc<Mutex<Observations>>,
}

impl RuntimeDriverProvider for FakeDriver {
    fn driver(&self) -> &'static str {
        "fake-terminal"
    }

    fn validate_config(&self, config: &RuntimeDriverConfig) -> Result<()> {
        assert_eq!(config.runtime_id, "fake");
        assert_eq!(config.executable, "real-agent");
        assert_eq!(config.auth, Some(RuntimeAuthKind::from_static("fake-auth")));
        self.observations.lock().unwrap().validations += 1;
        Ok(())
    }

    fn create_adapter(&self, _config: RuntimeDriverConfig) -> Arc<dyn RuntimeAdapter> {
        Arc::new(FakeAdapter {
            observations: Arc::clone(&self.observations),
        })
    }
}

struct FakeAdapter {
    observations: Arc<Mutex<Observations>>,
}

#[async_trait]
impl RuntimeAdapter for FakeAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "fake".to_string(),
            version: "test".to_string(),
            healthy: true,
        }
    }

    fn session_start(&self, _input: RuntimeSessionStartInput) -> Result<RuntimeSessionHandle> {
        unreachable!("the everyday terminal path does not open a protocol turn")
    }

    async fn turn(
        &self,
        _execution: TurnExecution,
        _journal: RuntimeTurnJournalSender,
    ) -> Result<TurnResult> {
        unreachable!("the everyday terminal path does not run a protocol turn")
    }

    fn build_terminal_program(
        &self,
        input: RuntimeTerminalProgramInput,
    ) -> Result<RuntimeProgramSpec> {
        let mut args = vec!["tui".to_string()];
        if input.resume {
            args.push("--continue".to_string());
        }
        args.push("--prompt".to_string());
        args.push(input.bootstrap_message.clone());
        self.observations
            .lock()
            .unwrap()
            .terminal_inputs
            .push(input);
        Ok(RuntimeProgramSpec {
            executable: "real-agent".to_string(),
            args,
            environment: Vec::new(),
            stdin: String::new(),
            auth: Some(RuntimeAuthKind::from_static("fake-auth")),
        })
    }

    async fn cancel(
        &self,
        _handle: &RuntimeSessionHandle,
        _reason: Option<String>,
    ) -> Result<RuntimeCancellation> {
        Ok(RuntimeCancellation::NoActiveTurn)
    }

    fn close(&self, _handle: &RuntimeSessionHandle) -> Result<()> {
        Ok(())
    }
}

struct FakeAuth {
    observations: Arc<Mutex<Observations>>,
}

#[async_trait]
impl RuntimeAuthProvider for FakeAuth {
    fn kind(&self) -> &'static str {
        "fake-auth"
    }

    async fn prepare(
        &self,
        input: RuntimeAuthPreparation<'_>,
    ) -> Result<RuntimeAuthMaterialization> {
        assert_eq!(input.runtime_id, "fake");
        assert!(input.auth_staging_root.is_some());
        self.observations.lock().unwrap().auth_preparations += 1;
        Ok(RuntimeAuthMaterialization::new(
            RuntimeAuthKind::from_static("fake-auth"),
            RuntimeAuthIdentity::new("fake-auth-identity").unwrap(),
            RuntimeAuthProjection::default(),
        ))
    }
}

struct FakeAttached {
    observations: Arc<Mutex<Observations>>,
    outputs: Mutex<VecDeque<ExecutionOutput>>,
}

impl FakeAttached {
    fn new(
        observations: Arc<Mutex<Observations>>,
        outputs: impl IntoIterator<Item = ExecutionOutput>,
    ) -> Self {
        Self {
            observations,
            outputs: Mutex::new(outputs.into_iter().collect()),
        }
    }
}

#[async_trait]
impl AttachedRuntimeExecutor for FakeAttached {
    async fn execute(&self, request: ExecutionRequest) -> Result<ExecutionOutput> {
        let runtime_mount = request
            .plan
            .mounts
            .iter()
            .find(|mount| mount.target == "/runtime")
            .expect("runtime mount");
        let context = std::fs::read_to_string(runtime_mount.source.join("AGENTS.generated.md"))
            .expect("neutral bootstrap context");
        let bridge_mount = request
            .plan
            .mounts
            .iter()
            .find(|mount| mount.target == "/runtime/lionclaw/operator.sock")
            .expect("operator bridge socket mount");
        assert!(std::fs::symlink_metadata(&bridge_mount.source)
            .expect("live operator bridge socket")
            .file_type()
            .is_socket());
        let skill_mount = request
            .plan
            .mounts
            .iter()
            .find(|mount| mount.target == "/runtime/home/.agents/skills/lionclaw")
            .expect("standard skill mount");
        let projected_client =
            std::fs::read_to_string(skill_mount.source.join("lionclaw")).unwrap();
        let mut observations = self.observations.lock().unwrap();
        observations.context.push(context);
        observations.bridge_socket_mounts += 1;
        observations.projected_clients.push(projected_client);
        observations.requests.push(request);
        drop(observations);
        Ok(self.outputs.lock().unwrap().pop_front().unwrap_or_default())
    }
}

fn output(code: i32) -> ExecutionOutput {
    ExecutionOutput {
        exit_code: Some(code),
        ..Default::default()
    }
}

fn profiles(home: &Path) -> RuntimeProfiles {
    RuntimeProfiles::from_toml(
        r#"
        [runtimes.fake]
        driver = "fake-terminal"
        command = "real-agent"
        native-resume = true
        auth = "fake-auth"
        skills-dir = ".agents/skills"
        confinement = { backend = "podman", image = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", read-only-rootfs = true, tmpfs = ["/tmp:rw,size=64m"] }
        "#,
        home,
    )
    .expect("fake runtime profile")
}

fn transports(
    home: &Path,
    observations: Arc<Mutex<Observations>>,
    outputs: impl IntoIterator<Item = ExecutionOutput>,
) -> MissionTransports {
    let attached = Arc::new(FakeAttached::new(Arc::clone(&observations), outputs));
    MissionTransports::external(
        profiles(home),
        RuntimeDriverRegistry::new([Arc::new(FakeDriver {
            observations: Arc::clone(&observations),
        }) as Arc<dyn RuntimeDriverProvider>]),
        RuntimeAuthRegistry::new([Arc::new(FakeAuth {
            observations: Arc::clone(&observations),
        }) as Arc<dyn RuntimeAuthProvider>]),
        Arc::new(MockOracleRunner::exiting(0)),
    )
    .with_attached_runtime(attached)
}

fn run_cli(repo: &Path) -> Cli {
    Cli::try_parse_from(["lionclaw", "run", "fake", "--repo", repo.to_str().unwrap()])
        .expect("lionclaw run fake parses")
}

#[tokio::test]
async fn everyday_run_reaches_validated_profile_auth_and_confinement() {
    let temp = tempfile::tempdir().unwrap();
    common::initialize_repository(temp.path());
    let observations = Arc::new(Mutex::new(Observations::default()));

    let code = cli::run_with_transports(
        run_cli(temp.path()),
        transports(temp.path(), Arc::clone(&observations), [output(0)]),
    )
    .await
    .expect("run succeeds at the transport boundary");

    assert_eq!(code, std::process::ExitCode::from(NONTERMINAL_EXIT));
    let observations = observations.lock().unwrap();
    assert!(observations.validations >= 1);
    assert_eq!(observations.auth_preparations, 1);
    assert_eq!(observations.requests.len(), 1);
    let request = &observations.requests[0];
    assert_eq!(request.plan.workspace_access.as_str(), "read-write");
    assert_eq!(request.plan.network_mode.as_str(), "on");
    assert!(request
        .plan
        .environment
        .contains(&("HOME".to_string(), "/runtime/home".to_string())));
    assert!(request.plan.environment.contains(&(
        "XDG_DATA_HOME".to_string(),
        "/runtime/home/.local/share".to_string()
    )));
    assert!(request.runtime_auth.is_some());
    assert!(request.plan.mounts.iter().any(|mount| {
        mount.target == "/runtime/home/.agents/skills/lionclaw"
            && mount.access == MountAccess::ReadOnly
    }));
    assert_eq!(observations.bridge_socket_mounts, 1);
    assert!(observations.projected_clients[0].starts_with("#!/usr/bin/env node\n"));
    assert!(!observations.projected_clients[0]
        .contains(std::env::current_exe().unwrap().to_str().unwrap()));
    assert!(observations.context[0].contains("\"repository\": \"/workspace\""));
    assert!(observations.context[0].contains("\"runtime\": \"fake\""));
    assert!(observations.context[0].contains("\"mission\": null"));
}

#[tokio::test]
async fn runtime_crash_retries_are_bounded_and_cannot_append_a_decision() {
    let temp = tempfile::tempdir().unwrap();
    let harness = common::harness(
        temp.path(),
        MockRoleRunner::happy(common::HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = harness
        .engine
        .create_mission(
            temp.path().to_str().unwrap(),
            "Keep the mission parked",
            common::BASE_SHA,
        )
        .await
        .unwrap();
    let store = MissionStore::open(temp.path()).await.unwrap();
    let before = store.load(&mission_id).await.unwrap();
    let observations = Arc::new(Mutex::new(Observations::default()));

    let code = cli::run_with_transports(
        run_cli(temp.path()),
        transports(
            temp.path(),
            Arc::clone(&observations),
            [output(17), output(17), output(17), output(0)],
        ),
    )
    .await
    .expect("runtime failure is a reported outcome");

    assert_eq!(code, std::process::ExitCode::FAILURE);
    assert_eq!(observations.lock().unwrap().requests.len(), 3);
    assert_eq!(store.load(&mission_id).await.unwrap(), before);
}

#[tokio::test]
async fn restart_resumes_native_conversation_and_reloads_folded_next() {
    let temp = tempfile::tempdir().unwrap();
    let harness = common::harness(
        temp.path(),
        MockRoleRunner::happy(common::HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let mission_id = harness
        .engine
        .create_mission(
            temp.path().to_str().unwrap(),
            "Observe current truth",
            common::BASE_SHA,
        )
        .await
        .unwrap();
    let observations = Arc::new(Mutex::new(Observations::default()));
    let transports = transports(
        temp.path(),
        Arc::clone(&observations),
        [output(0), output(0)],
    );

    let first = cli::run_with_transports(run_cli(temp.path()), transports.clone())
        .await
        .unwrap();
    assert_eq!(first, std::process::ExitCode::from(NONTERMINAL_EXIT));
    harness
        .engine
        .abort(&mission_id, "test transition")
        .await
        .unwrap();
    let second = cli::run_with_transports(run_cli(temp.path()), transports)
        .await
        .unwrap();
    assert_eq!(second, std::process::ExitCode::FAILURE);

    let observations = observations.lock().unwrap();
    assert_eq!(observations.terminal_inputs.len(), 2);
    assert!(!observations.terminal_inputs[0].resume);
    assert!(observations.terminal_inputs[1].resume);
    assert!(observations.context[0].contains("\"kind\": \"propose_plan\""));
    assert!(observations.context[1].contains("\"kind\": \"aborted\""));
    assert!(!observations.context[1].contains("\"kind\": \"propose_plan\""));
}
