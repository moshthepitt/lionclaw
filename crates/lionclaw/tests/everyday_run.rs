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
const IMAGE_A: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const IMAGE_B: &str = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

#[derive(Default)]
struct Observations {
    validations: usize,
    auth_preparations: usize,
    terminal_inputs: Vec<RuntimeTerminalProgramInput>,
    requests: Vec<ExecutionRequest>,
    context: Vec<String>,
    bridge_socket_mounts: usize,
    projected_clients: Vec<String>,
    image_resolutions: Vec<(String, String, String)>,
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
    image_identities: Mutex<VecDeque<String>>,
}

impl FakeAttached {
    fn new(
        observations: Arc<Mutex<Observations>>,
        outputs: impl IntoIterator<Item = ExecutionOutput>,
        image_identities: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            observations,
            outputs: Mutex::new(outputs.into_iter().collect()),
            image_identities: Mutex::new(image_identities.into_iter().collect()),
        }
    }
}

#[async_trait]
impl AttachedRuntimeExecutor for FakeAttached {
    async fn resolve_image_compatibility_identity(
        &self,
        engine: &str,
        image: &str,
    ) -> Result<String> {
        let mut identities = self.image_identities.lock().unwrap();
        let identity = if identities.len() > 1 {
            identities.pop_front().unwrap()
        } else {
            identities.front().cloned().expect("fake image identity")
        };
        self.observations.lock().unwrap().image_resolutions.push((
            engine.to_string(),
            image.to_string(),
            identity.clone(),
        ));
        Ok(identity)
    }

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

fn signal(number: i32) -> ExecutionOutput {
    ExecutionOutput {
        exit_signal: Some(number),
        ..Default::default()
    }
}

fn profiles(home: &Path) -> RuntimeProfiles {
    profiles_with_image(home, IMAGE_A)
}

fn profiles_with_image(home: &Path, image: &str) -> RuntimeProfiles {
    RuntimeProfiles::from_toml(
        &format!(
            r#"
        [runtimes.fake]
        driver = "fake-terminal"
        command = "real-agent"
        native-resume = true
        auth = "fake-auth"
        skills-dir = ".agents/skills"
        confinement = {{ backend = "podman", image = "{image}", read-only-rootfs = true, tmpfs = ["/tmp:rw,size=64m"] }}
        "#
        ),
        home,
    )
    .expect("fake runtime profile")
}

fn transports(
    home: &Path,
    runtime_root: &Path,
    observations: Arc<Mutex<Observations>>,
    outputs: impl IntoIterator<Item = ExecutionOutput>,
) -> MissionTransports {
    transports_with_profiles(
        profiles(home),
        observations,
        outputs,
        [IMAGE_A.to_string()],
        runtime_root,
    )
}

fn transports_with_profiles(
    profiles: RuntimeProfiles,
    observations: Arc<Mutex<Observations>>,
    outputs: impl IntoIterator<Item = ExecutionOutput>,
    image_identities: impl IntoIterator<Item = String>,
    runtime_root: &Path,
) -> MissionTransports {
    let attached = Arc::new(FakeAttached::new(
        Arc::clone(&observations),
        outputs,
        image_identities,
    ));
    MissionTransports::external(
        profiles,
        RuntimeDriverRegistry::new([Arc::new(FakeDriver {
            observations: Arc::clone(&observations),
        }) as Arc<dyn RuntimeDriverProvider>]),
        RuntimeAuthRegistry::new([Arc::new(FakeAuth {
            observations: Arc::clone(&observations),
        }) as Arc<dyn RuntimeAuthProvider>]),
        Arc::new(MockOracleRunner::exiting(0)),
    )
    .with_attached_runtime(attached)
    .with_everyday_runtime_root(runtime_root.to_path_buf())
}

fn run_cli(repo: &Path) -> Cli {
    Cli::try_parse_from(["lionclaw", "run", "fake", "--repo", repo.to_str().unwrap()])
        .expect("lionclaw run fake parses")
}

fn guide_cli(repo: &Path) -> Cli {
    Cli::try_parse_from([
        "lionclaw",
        "mission",
        "guide",
        "--repo",
        repo.to_str().unwrap(),
        "--json",
    ])
    .expect("lionclaw mission guide parses")
}

fn apply_cli(repo: &Path) -> Cli {
    Cli::try_parse_from([
        "lionclaw",
        "mission",
        "apply",
        "--repo",
        repo.to_str().unwrap(),
    ])
    .expect("lionclaw mission apply parses")
}

#[tokio::test]
async fn everyday_run_reaches_validated_profile_auth_and_confinement() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    common::initialize_repository(temp.path());
    let observations = Arc::new(Mutex::new(Observations::default()));

    let code = cli::run_with_transports(
        run_cli(temp.path()),
        transports(
            temp.path(),
            runtime.path(),
            Arc::clone(&observations),
            [output(0)],
        ),
    )
    .await
    .expect("run succeeds at the transport boundary");

    assert_eq!(code, std::process::ExitCode::from(NONTERMINAL_EXIT));
    let observations = observations.lock().unwrap();
    assert!(observations.validations >= 1);
    assert_eq!(observations.auth_preparations, 1);
    assert_eq!(
        observations.image_resolutions,
        [(
            "podman".to_string(),
            IMAGE_A.to_string(),
            IMAGE_A.to_string()
        )]
    );
    assert_eq!(observations.requests.len(), 1);
    let request = &observations.requests[0];
    assert_eq!(request.plan.workspace_access.as_str(), "read-only");
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
    assert!(request
        .plan
        .mounts
        .iter()
        .any(|mount| { mount.target == "/scratch" && mount.access == MountAccess::ReadWrite }));
    assert_eq!(observations.bridge_socket_mounts, 1);
    assert!(observations.projected_clients[0].starts_with("#!/usr/bin/env node\n"));
    assert!(!observations.projected_clients[0]
        .contains(std::env::current_exe().unwrap().to_str().unwrap()));
    assert!(observations.context[0].contains("\"repository\": \"/workspace\""));
    assert!(observations.context[0].contains("\"runtime\": \"fake\""));
    assert!(observations.context[0].contains("\"mission\": null"));
}

#[tokio::test]
async fn ordinary_runtime_failure_is_not_retried_or_allowed_to_append_a_decision() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
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
            runtime.path(),
            Arc::clone(&observations),
            [output(17), output(0)],
        ),
    )
    .await
    .expect("runtime failure is a reported outcome");

    assert_eq!(code, std::process::ExitCode::FAILURE);
    assert_eq!(observations.lock().unwrap().requests.len(), 1);
    assert_eq!(store.load(&mission_id).await.unwrap(), before);
}

#[tokio::test]
async fn runtime_signal_retries_are_bounded_and_cannot_append_a_decision() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
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
            runtime.path(),
            Arc::clone(&observations),
            [signal(9), signal(9), signal(9), output(0)],
        ),
    )
    .await
    .expect("runtime crash is a reported outcome");

    assert_eq!(code, std::process::ExitCode::FAILURE);
    assert_eq!(observations.lock().unwrap().requests.len(), 3);
    assert_eq!(store.load(&mission_id).await.unwrap(), before);
}

#[tokio::test]
async fn restart_resumes_native_conversation_and_releases_a_settled_mission() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
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
        runtime.path(),
        Arc::clone(&observations),
        [output(0), output(0), output(0)],
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
    let second = cli::run_with_transports(run_cli(temp.path()), transports.clone())
        .await
        .unwrap();
    assert_eq!(second, std::process::ExitCode::from(NONTERMINAL_EXIT));
    let next_mission = harness
        .engine
        .create_mission(
            temp.path().to_str().unwrap(),
            "Observe later truth",
            common::BASE_SHA,
        )
        .await
        .unwrap();
    let third = cli::run_with_transports(run_cli(temp.path()), transports)
        .await
        .unwrap();
    assert_eq!(third, std::process::ExitCode::from(NONTERMINAL_EXIT));

    let observations = observations.lock().unwrap();
    assert_eq!(observations.terminal_inputs.len(), 3);
    assert!(!observations.terminal_inputs[0].resume);
    assert!(observations.terminal_inputs[1].resume);
    assert!(observations.terminal_inputs[2].resume);
    assert!(observations.context[0].contains("\"kind\": \"propose_plan\""));
    assert!(observations.context[1].contains("\"mission\": null"));
    assert!(!observations.context[1].contains("\"kind\": \"aborted\""));
    assert!(observations.context[2].contains(next_mission.as_str()));
    assert!(observations.context[2].contains("\"kind\": \"propose_plan\""));
}

#[tokio::test]
async fn durable_binding_preserves_one_exact_live_mission() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let harness = common::harness(
        temp.path(),
        MockRoleRunner::happy(common::HEAD_SHA),
        MockOracleRunner::exiting(0),
    )
    .await;
    let current = harness
        .engine
        .create_mission(
            temp.path().to_str().unwrap(),
            "Keep this mission current",
            common::BASE_SHA,
        )
        .await
        .unwrap();
    let observations = Arc::new(Mutex::new(Observations::default()));
    let transports = transports(
        temp.path(),
        runtime.path(),
        Arc::clone(&observations),
        [output(0), output(0)],
    );
    cli::run_with_transports(run_cli(temp.path()), transports.clone())
        .await
        .unwrap();
    let other = harness
        .engine
        .create_mission(
            temp.path().to_str().unwrap(),
            "A separately created mission",
            common::BASE_SHA,
        )
        .await
        .unwrap();

    cli::run_with_transports(run_cli(temp.path()), transports)
        .await
        .unwrap();
    assert_eq!(
        cli::run_with_transports(guide_cli(temp.path()), MissionTransports::production())
            .await
            .unwrap(),
        std::process::ExitCode::SUCCESS
    );

    let observations = observations.lock().unwrap();
    assert!(observations.context[1].contains(current.as_str()));
    assert!(!observations.context[1].contains(other.as_str()));
    assert!(!observations.context[1].contains("\"kind\": \"ambiguous\""));
}

#[tokio::test]
async fn completed_binding_is_preserved_through_apply_then_released() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let harness = common::harness(
        temp.path(),
        common::review_runner(Vec::new()),
        MockOracleRunner::exiting(0),
    )
    .await;
    let current = harness
        .engine
        .create_mission(
            temp.path().to_str().unwrap(),
            "Finish and apply this mission",
            common::BASE_SHA,
        )
        .await
        .unwrap();
    let observations = Arc::new(Mutex::new(Observations::default()));
    let transports = transports(
        temp.path(),
        runtime.path(),
        Arc::clone(&observations),
        [output(0), output(0), output(0)],
    );
    cli::run_with_transports(run_cli(temp.path()), transports.clone())
        .await
        .unwrap();
    harness
        .engine
        .propose_plan(&current, common::proposal(0, common::simple_plan()))
        .await
        .unwrap();
    common::approve_plan(&harness.engine, &current).await;
    common::advance_to_finished(&harness.engine, &current).await;
    let later = harness
        .engine
        .create_mission(
            temp.path().to_str().unwrap(),
            "Wait until the current mission is applied",
            common::BASE_SHA,
        )
        .await
        .unwrap();

    let completed = cli::run_with_transports(run_cli(temp.path()), transports.clone())
        .await
        .unwrap();
    assert_eq!(completed, std::process::ExitCode::SUCCESS);
    assert_eq!(
        cli::run_with_transports(apply_cli(temp.path()), MissionTransports::production())
            .await
            .unwrap(),
        std::process::ExitCode::SUCCESS
    );
    let released = cli::run_with_transports(run_cli(temp.path()), transports)
        .await
        .unwrap();
    assert_eq!(released, std::process::ExitCode::from(NONTERMINAL_EXIT));

    let observations = observations.lock().unwrap();
    assert!(observations.context[1].contains(current.as_str()));
    assert!(observations.context[1].contains("\"kind\": \"apply\""));
    assert!(!observations.context[1].contains(later.as_str()));
    assert!(observations.context[2].contains(later.as_str()));
    assert!(!observations.context[2].contains(current.as_str()));
}

#[tokio::test]
async fn corrupt_current_mission_binding_fails_before_runtime_execution() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    common::initialize_repository(temp.path());
    let observations = Arc::new(Mutex::new(Observations::default()));
    let transports = transports(
        temp.path(),
        runtime.path(),
        Arc::clone(&observations),
        [output(0), output(0)],
    );
    cli::run_with_transports(run_cli(temp.path()), transports.clone())
        .await
        .unwrap();
    std::fs::write(
        temp.path().join(".lionclaw/everyday/current-mission"),
        b"not-a-mission",
    )
    .unwrap();

    let error = cli::run_with_transports(run_cli(temp.path()), transports)
        .await
        .expect_err("corrupt binding must fail closed");

    assert!(
        format!("{error:#}").contains("current everyday mission identity is invalid"),
        "got {error:#}"
    );
    assert_eq!(observations.lock().unwrap().requests.len(), 1);
}

#[tokio::test]
async fn runtime_state_inside_the_judged_repository_is_rejected() {
    let temp = tempfile::tempdir().unwrap();
    common::initialize_repository(temp.path());
    let observations = Arc::new(Mutex::new(Observations::default()));
    let runtime_root = temp.path().join(".runtime-state");

    let error = cli::run_with_transports(
        run_cli(temp.path()),
        transports(
            temp.path(),
            &runtime_root,
            Arc::clone(&observations),
            [output(0)],
        ),
    )
    .await
    .expect_err("writable runtime state must remain outside the judged tree");

    assert!(
        format!("{error:#}").contains("overlaps judged root"),
        "got {error:#}"
    );
    assert!(!runtime_root.exists());
    let observations = observations.lock().unwrap();
    assert_eq!(observations.auth_preparations, 0);
    assert!(observations.requests.is_empty());
}

#[tokio::test]
async fn mutable_image_tag_is_resolved_before_native_resume_and_execution() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    common::initialize_repository(temp.path());
    let observations = Arc::new(Mutex::new(Observations::default()));
    let mutable_ref = "localhost/lionclaw-runtime-dev:v1";
    let transports = transports_with_profiles(
        profiles_with_image(temp.path(), mutable_ref),
        Arc::clone(&observations),
        [output(0), output(0)],
        [IMAGE_A.to_string(), IMAGE_B.to_string()],
        runtime.path(),
    );

    cli::run_with_transports(run_cli(temp.path()), transports.clone())
        .await
        .unwrap();
    cli::run_with_transports(run_cli(temp.path()), transports)
        .await
        .unwrap();

    let observations = observations.lock().unwrap();
    assert_eq!(
        observations.image_resolutions,
        [
            (
                "podman".to_string(),
                mutable_ref.to_string(),
                IMAGE_A.to_string()
            ),
            (
                "podman".to_string(),
                mutable_ref.to_string(),
                IMAGE_B.to_string()
            ),
        ]
    );
    assert_eq!(
        observations.requests[0]
            .plan
            .confinement
            .oci()
            .image
            .as_deref(),
        Some(IMAGE_A)
    );
    assert_eq!(
        observations.requests[1]
            .plan
            .confinement
            .oci()
            .image
            .as_deref(),
        Some(IMAGE_B)
    );
    assert!(!observations.terminal_inputs[0].resume);
    assert!(!observations.terminal_inputs[1].resume);
    assert_ne!(
        observations.terminal_inputs[0].session_id,
        observations.terminal_inputs[1].session_id
    );
}
