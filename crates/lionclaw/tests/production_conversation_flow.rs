//! The Slice 4 production conversation proof.  Only the native agent and
//! oracle transports are scripted; every boundary around them is production.

use std::collections::VecDeque;
use std::io::{BufRead, BufReader};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use clap::Parser;
use lionclaw::config::RuntimeProfiles;
use lionclaw::engine::{MissionDisposition, ReferenceRejectionReason};
use lionclaw::model::{
    apply, fold, Assertion, AssertionId, FinishClass, MessageReference, MissionEvent, MissionPhase,
    MissionState, OracleName, OutputSemantics, PayloadRef, Plan, PlanProposal, Requirement,
    RequirementDisposition, RequirementId, RequirementKind, RoleName, Task, TaskId, TaskKind,
    TaskStatus, REDUCER_VERSION, SCHEMA_VERSION,
};
use lionclaw::ports::{OracleOutcome, OracleRunRequest, OracleRunner};
use lionclaw::store::MissionStore;
use lionclaw::{cli, workspace};
use lionclaw_runtime_api::{
    RuntimeAdapter, RuntimeAdapterInfo, RuntimeAuthContext, RuntimeAuthPreparation,
    RuntimeAuthProvider, RuntimeAuthRegistry, RuntimeCancellation, RuntimeDriverConfig,
    RuntimeDriverProvider, RuntimeDriverRegistry, RuntimeResumeMode, RuntimeSessionHandle,
    RuntimeSessionStartInput, TurnExecution, TurnResult, TypedFailure,
};
use lionclaw_runtime_codex::CodexRuntimeDriver;

#[derive(Clone, Copy)]
enum DeliveryTurn {
    AwaitLead,
    Fail,
    InvalidHandoff,
    Complete,
    CompleteWithOversizedReference,
    Review,
    Plan,
    Validate,
}

type SessionObservations = Arc<Mutex<Vec<(Option<String>, bool)>>>;
type PromptObservations = Arc<Mutex<Vec<(String, std::path::PathBuf)>>>;

struct TestCodexAuth;

#[async_trait]
impl RuntimeAuthProvider for TestCodexAuth {
    fn kind(&self) -> &'static str {
        "codex"
    }

    async fn validate(&self, _context: &RuntimeAuthContext) -> anyhow::Result<()> {
        Ok(())
    }

    async fn prepare(
        &self,
        _input: RuntimeAuthPreparation<'_>,
    ) -> anyhow::Result<Vec<(String, String)>> {
        anyhow::bail!("test codex auth setup refused launch")
    }
}

fn cli_output(repo: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_lionclaw"))
        .args(args)
        .arg("--repo")
        .arg(repo)
        .output()
        .expect("run parsed production CLI")
}

fn stdout(output: Output) -> String {
    assert!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("UTF-8 CLI output")
}

fn watch_observation(
    repo: &Path,
    mission_id: &str,
    json: bool,
    final_line_contains: Option<&str>,
) -> String {
    let mut command = Command::new(env!("CARGO_BIN_EXE_lionclaw"));
    command
        .args(["mission", "status", mission_id, "--watch"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .arg("--repo")
        .arg(repo);
    if json {
        command.arg("--json");
    }
    let mut child = command.spawn().expect("spawn status watcher");
    let output = child.stdout.take().expect("watch stdout");
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let final_line_contains = final_line_contains.map(str::to_owned);
    let reader = std::thread::spawn(move || {
        let mut reader = BufReader::new(output);
        let mut observation = String::new();
        let result = loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => break Ok(observation),
                Ok(_) => {
                    let complete = final_line_contains
                        .as_ref()
                        .is_none_or(|expected| line.contains(expected));
                    observation.push_str(&line);
                    if complete {
                        break Ok(observation);
                    }
                }
                Err(error) => break Err(error),
            }
        };
        let _ = sender.send(result);
    });
    let observed = receiver.recv_timeout(Duration::from_secs(10));
    let _ = child.kill();
    let output = child.wait_with_output().expect("reap status watcher");
    reader.join().expect("join status watcher reader");
    match observed {
        Ok(Ok(line)) if !line.is_empty() => line,
        Ok(Ok(_)) => panic!(
            "watch exited without an observation: {}",
            String::from_utf8_lossy(&output.stderr)
        ),
        Ok(Err(error)) => panic!("failed to read watch observation: {error}"),
        Err(error) => panic!(
            "watch emitted no observation before its deadlock deadline ({error}): {}",
            String::from_utf8_lossy(&output.stderr)
        ),
    }
}

fn assert_activity_suppressed(repo: &Path, mission_id: &str, label: &str) {
    let status: serde_json::Value = serde_json::from_str(&stdout(cli_output(
        repo,
        &["mission", "status", mission_id, "--json"],
    )))
    .unwrap();
    assert_eq!(status["activity"], serde_json::Value::Null, "{label}");

    for format in [None, Some("--json")] {
        let mut command = Command::new("timeout");
        command
            .args(["0.35", env!("CARGO_BIN_EXE_lionclaw"), "mission", "status"])
            .arg(mission_id)
            .arg("--watch");
        if let Some(format) = format {
            command.arg(format);
        }
        let output = command.arg("--repo").arg(repo).output().unwrap();
        assert!(
            output.stdout.is_empty(),
            "{label} leaked through watch: {}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
}

fn walk_paths(root: &Path) -> Vec<std::path::PathBuf> {
    let mut pending = vec![root.to_path_buf()];
    let mut paths = Vec::new();
    while let Some(path) = pending.pop() {
        if path.is_dir() {
            pending.extend(
                std::fs::read_dir(&path)
                    .unwrap()
                    .map(|entry| entry.unwrap().path()),
            );
        }
        paths.push(path);
    }
    paths
}

async fn assert_reference_send_rejected(
    repo: &Path,
    store: &MissionStore,
    mission: &lionclaw::model::MissionId,
    conversation: &lionclaw::model::ConversationId,
    reference_args: Vec<String>,
    expected: ReferenceRejectionReason,
) {
    let before_events = store.load(mission).await.unwrap();
    let before_state = fold(before_events.clone()).unwrap();
    let before_cursor = before_state.conversations[conversation].consumed_through;
    let before_messages = before_events
        .iter()
        .filter(|event| matches!(event.event, MissionEvent::MessageSent { .. }))
        .count();
    let mut args = vec![
        "lionclaw".to_string(),
        "mission".into(),
        "send".into(),
        "--mission-id".into(),
        mission.to_string(),
        "--to".into(),
        conversation.to_string(),
    ];
    args.extend(reference_args);
    args.extend([
        "--repo".into(),
        repo.display().to_string(),
        "REJECTED-EXPANSION-PROBE".into(),
    ]);
    let error = cli::run(cli::Cli::try_parse_from(args).unwrap())
        .await
        .expect_err("invalid production reference set must fail closed");
    assert_eq!(
        error.downcast_ref::<ReferenceRejectionReason>(),
        Some(&expected),
        "reference rejection lost exact typed truth: {error:#}"
    );
    let after_events = store.load(mission).await.unwrap();
    let after_state = fold(after_events.clone()).unwrap();
    assert_eq!(after_events, before_events, "rejection partially appended");
    assert_eq!(
        after_state, before_state,
        "rejection changed head, recipients, queues, cursors, delivery, presentation, or active boundary"
    );
    assert_eq!(
        after_events
            .iter()
            .filter(|event| matches!(event.event, MissionEvent::MessageSent { .. }))
            .count(),
        before_messages,
        "rejection appended MessageSent"
    );
    assert_eq!(
        after_state.conversations[conversation].consumed_through, before_cursor,
        "rejection advanced the freshly folded delivery cursor"
    );
}

async fn capture_reference_watch(
    repo: &Path,
    store: &MissionStore,
    mission: &lionclaw::model::MissionId,
    conversation: &lionclaw::model::ConversationId,
    transports: &cli::MissionTransports,
    entered: &Arc<tokio::sync::Semaphore>,
    release: &Arc<tokio::sync::Semaphore>,
) -> String {
    cli::run(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "send",
            "--mission-id",
            mission.as_str(),
            "--to",
            conversation.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "hold the current effect for the watch proof",
        ])
        .unwrap(),
    )
    .await
    .unwrap();
    entered.forget_permits(entered.available_permits());
    release.forget_permits(release.available_permits());
    let command = cli::Cli::try_parse_from([
        "lionclaw",
        "mission",
        "driver",
        mission.as_str(),
        "--repo",
        repo.to_str().unwrap(),
        "--handshake",
        store
            .lionclaw_dir()
            .join("references-watch.ready")
            .to_str()
            .unwrap(),
    ])
    .unwrap();
    let active_driver = tokio::spawn({
        let transports = transports.clone();
        async move { cli::run_with_transports(command, transports).await }
    });
    entered.acquire().await.unwrap().forget();
    let current = store.require_state(mission).await.unwrap();
    let mission_root = store.lionclaw_dir().join("missions").join(mission.as_str());
    lionclaw::activity::publish_observed(
        repo,
        &mission_root,
        &current,
        lionclaw::activity::now_ms(),
        None,
    )
    .await
    .unwrap();
    let watched = watch_observation(repo, mission.as_str(), true, None);
    release.add_permits(8);
    assert_eq!(
        active_driver.await.unwrap().unwrap(),
        std::process::ExitCode::SUCCESS
    );
    watched
}

fn projected_conversation<'a>(value: &'a serde_json::Value, id: &str) -> &'a serde_json::Value {
    value["conversations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|conversation| conversation["id"] == id)
        .expect("conversation in projection")
}

struct DeliveryTransport {
    turns: Arc<Mutex<VecDeque<DeliveryTurn>>>,
    entered: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
    sessions: SessionObservations,
    prompts: PromptObservations,
    launch_failures: Arc<Mutex<usize>>,
}

impl DeliveryTransport {
    fn handoff(execution: &TurnExecution) -> std::path::PathBuf {
        let runtime = execution
            .context
            .runtime_state_root
            .as_ref()
            .expect("native state root");
        let mission = runtime
            .parent()
            .and_then(Path::parent)
            .and_then(Path::parent)
            .expect("mission state root");
        std::fs::read_dir(mission.join("effects"))
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path().join("handoff/handoff.json"))
            .find(|path| path.parent().is_some_and(Path::exists))
            .expect("current effect handoff mount")
    }
}

#[async_trait]
impl RuntimeAdapter for DeliveryTransport {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "delivery-native".into(),
            version: "1".into(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        input: RuntimeSessionStartInput,
    ) -> anyhow::Result<RuntimeSessionHandle> {
        let mut launch_failures = self.launch_failures.lock().unwrap();
        if *launch_failures > 0 {
            *launch_failures -= 1;
            anyhow::bail!("scripted session launch failure");
        }
        drop(launch_failures);
        let native_ready = matches!(
            &input.resume,
            lionclaw_runtime_api::RuntimeResume::Native { ready, .. } if ready.is_ready()
        );
        self.sessions
            .lock()
            .unwrap()
            .push((input.working_dir.clone(), native_ready));
        Ok(RuntimeSessionHandle {
            runtime_session_id: input.session_id.to_string(),
            resume_mode: if native_ready {
                RuntimeResumeMode::Resumed
            } else {
                RuntimeResumeMode::Reconstructed
            },
        })
    }

    async fn turn(
        &self,
        execution: TurnExecution,
        _journal: lionclaw_runtime_api::RuntimeTurnJournalSender,
    ) -> anyhow::Result<TurnResult> {
        let turn = self
            .turns
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted turn");
        self.prompts.lock().unwrap().push((
            execution.input.prompt.clone(),
            execution
                .context
                .runtime_state_root
                .clone()
                .expect("native state root"),
        ));
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        if matches!(turn, DeliveryTurn::Fail) {
            anyhow::bail!("scripted external transport failure");
        }
        match turn {
            DeliveryTurn::AwaitLead => {}
            DeliveryTurn::Fail => unreachable!("returned above"),
            DeliveryTurn::InvalidHandoff => {
                std::fs::write(Self::handoff(&execution), b"{}")?;
            }
            DeliveryTurn::Complete | DeliveryTurn::CompleteWithOversizedReference => {
                let runtime = execution.context.runtime_state_root.as_ref().unwrap();
                let work = runtime.parent().unwrap().join("work");
                let artifact = work.join("delivery.txt");
                let contents = if execution.input.prompt.contains("TERMINAL-DIRECT-PROSE") {
                    "reference-bearing retry complete\n"
                } else {
                    "complete\n"
                };
                std::fs::write(&artifact, contents)?;
                git(&work, &["add", "delivery.txt"])?;
                if matches!(turn, DeliveryTurn::CompleteWithOversizedReference) {
                    std::fs::write(work.join("reference-bound.txt"), "B".repeat(70 * 1024))?;
                    git(&work, &["add", "reference-bound.txt"])?;
                }
                git(&work, &["commit", "-q", "-m", "complete delivery proof"])?;
                std::fs::write(
                    Self::handoff(&execution),
                    r#"{"schema":"lionclaw.mission.work-handoff.v2","type":"work","done":true,"report":"delivery complete","request_attention":false}"#,
                )?;
            }
            DeliveryTurn::Review => {
                let nonce = execution
                    .input
                    .prompt
                    .rsplit_once("## Handoff nonce")
                    .expect("terminal review nonce")
                    .1
                    .trim();
                std::fs::write(
                    Self::handoff(&execution),
                    format!(
                        r#"{{"schema":"lionclaw.mission.review-handoff.v2","type":"review","done":true,"report":"delivery state clean","passed":true,"nonce":"{nonce}","gaps":[]}}"#
                    ),
                )?;
            }
            DeliveryTurn::Plan => {
                std::fs::write(
                    Self::handoff(&execution),
                    r#"{"schema":"lionclaw.mission.plan-handoff.v2","type":"plan","done":true,"report":"planned after lead response","proposal":{"base_revision":0,"requirement_changes":[],"assertion_supersessions":[],"plan":{"requirements":[{"id":"PRODUCTION-CONVERSATION","kind":"validation","prose":"production conversation flow works","disposition":{"type":"covered","assertion_ids":["TESTS-CLEAN"]}}],"assertions":[{"id":"TESTS-CLEAN","prose":"the production flow passes tests","oracle":"cargo-test"}],"tasks":[{"id":"integrate","kind":"work","body":"exercise the production flow","targets":["TESTS-CLEAN"],"role":"implementer","depends_on":[]},{"id":"validate","kind":"validate","body":"validate the production flow","targets":["TESTS-CLEAN"],"role":"validator","depends_on":["integrate"]}]}},"request_attention":false}"#,
                )?;
            }
            DeliveryTurn::Validate => {
                std::fs::write(
                    Self::handoff(&execution),
                    r#"{"schema":"lionclaw.mission.validate-handoff.v2","type":"validate","done":true,"report":"validated after lead response","items":[{"item_id":"TESTS-CLEAN","passed":true}],"passed":true,"request_attention":false}"#,
                )?;
            }
        }
        Ok(TurnResult {
            final_response: match turn {
                DeliveryTurn::AwaitLead => format!(
                    "Which release target should I use? {}",
                    "x".repeat(80 * 1024)
                ),
                DeliveryTurn::Fail => unreachable!("returned above"),
                DeliveryTurn::InvalidHandoff => "I supplied an invalid handoff.".into(),
                DeliveryTurn::Complete | DeliveryTurn::CompleteWithOversizedReference => {
                    "The requested production flow is complete.".into()
                }
                DeliveryTurn::Review => "The terminal review is clean.".into(),
                DeliveryTurn::Plan => "The lead response resolved the planning question.".into(),
                DeliveryTurn::Validate => {
                    "The lead response resolved the validation question.".into()
                }
            },
            ..Default::default()
        })
    }

    async fn cancel(
        &self,
        _handle: &RuntimeSessionHandle,
        _reason: Option<String>,
    ) -> anyhow::Result<RuntimeCancellation> {
        Ok(RuntimeCancellation::Acknowledged)
    }

    async fn close(&self, _handle: &RuntimeSessionHandle) -> anyhow::Result<()> {
        Ok(())
    }
}

struct DeliveryProvider {
    turns: Arc<Mutex<VecDeque<DeliveryTurn>>>,
    entered: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
    sessions: SessionObservations,
    prompts: PromptObservations,
    launch_failures: Arc<Mutex<usize>>,
}

struct RenamedDeliveryProvider {
    inner: DeliveryProvider,
}

impl RuntimeDriverProvider for RenamedDeliveryProvider {
    fn driver(&self) -> &'static str {
        "unrelated-provider-identity"
    }

    fn create_adapter(&self, config: RuntimeDriverConfig) -> Arc<dyn RuntimeAdapter> {
        self.inner.create_adapter(config)
    }
}

impl RuntimeDriverProvider for DeliveryProvider {
    fn driver(&self) -> &'static str {
        "codex"
    }

    fn create_adapter(&self, _config: RuntimeDriverConfig) -> Arc<dyn RuntimeAdapter> {
        Arc::new(DeliveryTransport {
            turns: self.turns.clone(),
            entered: self.entered.clone(),
            release: self.release.clone(),
            sessions: self.sessions.clone(),
            prompts: self.prompts.clone(),
            launch_failures: self.launch_failures.clone(),
        })
    }
}

struct NativeTransport {
    turns: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl RuntimeAdapter for NativeTransport {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "scripted-native".into(),
            version: "1".into(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        input: RuntimeSessionStartInput,
    ) -> anyhow::Result<RuntimeSessionHandle> {
        let resume_mode = match input.resume {
            lionclaw_runtime_api::RuntimeResume::Native { ready, .. } if ready.is_ready() => {
                RuntimeResumeMode::Resumed
            }
            _ => RuntimeResumeMode::Reconstructed,
        };
        Ok(RuntimeSessionHandle {
            runtime_session_id: input.session_id.to_string(),
            resume_mode,
        })
    }

    async fn turn(
        &self,
        execution: TurnExecution,
        _journal: lionclaw_runtime_api::RuntimeTurnJournalSender,
    ) -> anyhow::Result<TurnResult> {
        self.turns
            .lock()
            .unwrap()
            .push(execution.input.prompt.clone());
        let runtime = execution
            .context
            .runtime_state_root
            .expect("native state root");
        std::fs::write(runtime.join("transport-session"), b"durable-native-id")?;
        let conversation = runtime.parent().expect("conversation root");
        let mission = conversation
            .parent()
            .and_then(Path::parent)
            .expect("mission state root");
        let handoff = std::fs::read_dir(mission.join("effects"))?
            .filter_map(Result::ok)
            .map(|entry| entry.path().join("handoff/handoff.json"))
            .find(|path| path.parent().is_some_and(Path::exists))
            .expect("current effect handoff mount");

        if execution.input.prompt.contains("## Handoff nonce") {
            let nonce = execution
                .input
                .prompt
                .rsplit_once("## Handoff nonce")
                .unwrap()
                .1
                .trim();
            std::fs::write(
                handoff,
                format!(
                    r#"{{"schema":"lionclaw.mission.review-handoff.v2","type":"review","done":true,"report":"clean production review","passed":true,"nonce":"{nonce}","gaps":[]}}"#
                ),
            )?;
        } else {
            let work = conversation.join("work");
            std::fs::write(work.join("base.txt"), "captured by production runner\n")?;
            git(&work, &["add", "base.txt"])?;
            git(
                &work,
                &["commit", "-q", "-m", "production conversation artifact"],
            )?;
            std::fs::write(
                handoff,
                r#"{"schema":"lionclaw.mission.work-handoff.v2","type":"work","done":true,"report":"production artifact captured","request_attention":false}"#,
            )?;
        }
        Ok(TurnResult {
            final_response: "native transport completed".into(),
            ..Default::default()
        })
    }

    async fn cancel(
        &self,
        _handle: &RuntimeSessionHandle,
        _reason: Option<String>,
    ) -> anyhow::Result<RuntimeCancellation> {
        Ok(RuntimeCancellation::Acknowledged)
    }

    async fn close(&self, _handle: &RuntimeSessionHandle) -> anyhow::Result<()> {
        Ok(())
    }
}

struct NativeProvider {
    turns: Arc<Mutex<Vec<String>>>,
}

impl RuntimeDriverProvider for NativeProvider {
    fn driver(&self) -> &'static str {
        "codex"
    }

    fn create_adapter(&self, config: RuntimeDriverConfig) -> Arc<dyn RuntimeAdapter> {
        assert_eq!(config.runtime_id, "codex");
        Arc::new(NativeTransport {
            turns: self.turns.clone(),
        })
    }
}

struct ExternalOracleTransport {
    calls: Arc<Mutex<Vec<(String, String)>>>,
}

struct ReferenceIsolationOracleTransport {
    attempts: Arc<Mutex<usize>>,
}

#[async_trait]
impl OracleRunner for ReferenceIsolationOracleTransport {
    async fn run(&self, _request: OracleRunRequest) -> Result<OracleOutcome, TypedFailure> {
        let mut attempts = self.attempts.lock().unwrap();
        *attempts += 1;
        Ok(OracleOutcome {
            exit_code: i32::from(*attempts == 1),
            exit_signal: None,
            stdout: b"TERMINAL-RECEIPT-PROSE\n".to_vec(),
            stderr: Vec::new(),
            prepared_inputs: Vec::new(),
            duration_ms: 1,
        })
    }
}

fn scripted_oracle_outcome(oracle: &str) -> (i32, &'static [u8], u64) {
    match oracle {
        "build-release" => (0, b"release build passed\n", 11),
        "cargo-clippy" => (0, b"clippy passed\n", 12),
        "cargo-test" => (0, b"tests passed\n", 13),
        "fmt-check" => (0, b"format check passed\n", 14),
        other => panic!("unexpected external oracle request: {other}"),
    }
}

struct FailingOracleTransport;

#[async_trait]
impl OracleRunner for FailingOracleTransport {
    async fn run(&self, _request: OracleRunRequest) -> Result<OracleOutcome, TypedFailure> {
        let mut stdout = b"AUTHORITATIVE-RECEIPT-CONTENT\n".to_vec();
        stdout.extend(std::iter::repeat_n(b'R', 10 * 1024));
        // A non-UTF-8 byte forces the production store to retain this genuine
        // receipt payload in the content-addressed blob store.
        stdout.push(0xff);
        Ok(OracleOutcome {
            exit_code: 1,
            exit_signal: None,
            stdout,
            stderr: Vec::new(),
            prepared_inputs: Vec::new(),
            duration_ms: 1,
        })
    }
}

#[async_trait]
impl OracleRunner for ExternalOracleTransport {
    async fn run(&self, request: OracleRunRequest) -> Result<OracleOutcome, TypedFailure> {
        assert!(request.workspace_dir.join(".git").exists());
        let (exit_code, stdout, duration_ms) = scripted_oracle_outcome(request.oracle.as_str());
        self.calls
            .lock()
            .unwrap()
            .push((request.oracle.to_string(), request.judged_sha));
        Ok(OracleOutcome {
            exit_code,
            exit_signal: None,
            stdout: stdout.to_vec(),
            stderr: Vec::new(),
            prepared_inputs: Vec::new(),
            duration_ms,
        })
    }
}

fn git(repo: &Path, args: &[&str]) -> anyhow::Result<()> {
    let status = std::process::Command::new("git")
        .current_dir(repo)
        .args(args)
        .status()?;
    anyhow::ensure!(status.success(), "git {args:?} failed");
    Ok(())
}

fn plan() -> Plan {
    let assertions = [
        ("FORMAT-CLEAN", "fmt-check"),
        ("BUILD-CLEAN", "build-release"),
        ("CLIPPY-CLEAN", "cargo-clippy"),
        ("TESTS-CLEAN", "cargo-test"),
    ]
    .map(|(id, oracle)| Assertion {
        id: AssertionId::new(id).unwrap(),
        prose: format!("the captured production artifact passes {oracle}"),
        oracle: Some(OracleName::new(oracle).unwrap()),
    });
    Plan {
        requirements: vec![Requirement {
            id: RequirementId::new("PRODUCTION-CONVERSATION").unwrap(),
            kind: RequirementKind::Validation,
            prose: "production conversation flow works".into(),
            disposition: RequirementDisposition::Covered {
                assertion_ids: assertions
                    .iter()
                    .map(|assertion| assertion.id.clone())
                    .collect(),
            },
        }],
        assertions: assertions.to_vec(),
        tasks: vec![Task {
            id: TaskId::new("integrate").unwrap(),
            kind: TaskKind::Work,
            body: "exercise production workspace and artifact capture".into(),
            targets: assertions
                .iter()
                .map(|assertion| assertion.id.clone())
                .collect(),
            role: Some(RoleName::new("implementer").unwrap()),
            depends_on: Vec::new(),
        }],
    }
}

fn awaiting_lead_validation_plan() -> Plan {
    let assertion = AssertionId::new("VALIDATOR-WHILE-AWAITING-LEAD").unwrap();
    Plan {
        requirements: vec![Requirement {
            id: RequirementId::new("COMPOSITIONAL-TURN-SETTLEMENT").unwrap(),
            kind: RequirementKind::Validation,
            prose: "awaiting lead and parked role effects compose".into(),
            disposition: RequirementDisposition::Covered {
                assertion_ids: vec![assertion.clone()],
            },
        }],
        assertions: vec![Assertion {
            id: assertion.clone(),
            prose: "an independent validator runs while the exact writer awaits the lead".into(),
            oracle: Some(OracleName::new("cargo-test").unwrap()),
        }],
        tasks: vec![
            Task {
                id: TaskId::new("writer-question").unwrap(),
                kind: TaskKind::Work,
                body: "ask the lead for the missing release target".into(),
                targets: vec![assertion.clone()],
                role: Some(RoleName::new("implementer").unwrap()),
                depends_on: vec![],
            },
            Task {
                id: TaskId::new("independent-validator").unwrap(),
                kind: TaskKind::Validate,
                body: "validate the already available evidence".into(),
                targets: vec![assertion],
                role: Some(RoleName::new("renamed-judgment-role").unwrap()),
                depends_on: vec![],
            },
        ],
    }
}

fn reference_plan() -> Plan {
    let first = AssertionId::new("REFERENCE-RECEIPT").unwrap();
    Plan {
        requirements: vec![Requirement {
            id: RequirementId::new("REFERENCES-TRANSIENT").unwrap(),
            kind: RequirementKind::Validation,
            prose: "references remain transient".into(),
            disposition: RequirementDisposition::Covered {
                assertion_ids: vec![first.clone()],
            },
        }],
        assertions: vec![Assertion {
            id: first.clone(),
            prose: "produce genuine receipt authority".into(),
            oracle: Some(OracleName::new("cargo-test").unwrap()),
        }],
        tasks: vec![Task {
            id: TaskId::new("mint-receipt").unwrap(),
            kind: TaskKind::Work,
            body: "create receipt authority then receive transient references".into(),
            targets: vec![first],
            role: Some(RoleName::new("implementer").unwrap()),
            depends_on: vec![],
        }],
    }
}

async fn initialize_repo(repo: &Path) -> String {
    std::fs::create_dir_all(repo).unwrap();
    git(repo, &["init", "-q"]).unwrap();
    git(repo, &["config", "user.name", "Production Flow Test"]).unwrap();
    git(repo, &["config", "user.email", "production@lionclaw.local"]).unwrap();
    git(repo, &["config", "commit.gpgsign", "false"]).unwrap();
    std::fs::write(repo.join("base.txt"), "base\n").unwrap();
    git(repo, &["add", "base.txt"]).unwrap();
    git(repo, &["commit", "-q", "-m", "base"]).unwrap();
    workspace::head_sha(repo).await.unwrap()
}

#[tokio::test]
async fn production_validator_and_park_compose_with_exact_awaiting_writer() {
    assert_eq!((SCHEMA_VERSION, REDUCER_VERSION), (21, 31));
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    let base = initialize_repo(&repo).await;
    let fake_oci = temp.path().join("external-oci-transport");
    std::fs::write(&fake_oci, "#!/bin/sh\nif [ \"$1 $2\" = \"image inspect\" ]; then echo production-image-id; fi\nexit 0\n").unwrap();
    std::fs::set_permissions(&fake_oci, std::fs::Permissions::from_mode(0o755)).unwrap();
    let profiles = RuntimeProfiles::from_toml(
        &format!(
            r#"[runtimes.codex]
driver = "codex"
command = "external-codex"
native-resume = true
confinement = {{ backend = "podman", engine = "{}", read-only-rootfs = true }}
"#,
            fake_oci.display()
        ),
        temp.path(),
    )
    .unwrap();
    let mission_type = temp.path().join("mission-type");
    materialize_mission_type(&mission_type);
    std::fs::write(
        mission_type.join("roles/renamed-judgment-role.md"),
        "---\noutput: emits-verdict\nruntime: codex\n---\nValidate existing evidence.\n",
    )
    .unwrap();
    let turns = Arc::new(Mutex::new(VecDeque::from([
        DeliveryTurn::AwaitLead,
        DeliveryTurn::Validate,
    ])));
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let sessions = Arc::new(Mutex::new(Vec::new()));
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let transports = cli::MissionTransports::external(
        profiles,
        RuntimeDriverRegistry::new([Arc::new(DeliveryProvider {
            turns: turns.clone(),
            entered: entered.clone(),
            release: release.clone(),
            sessions: sessions.clone(),
            prompts: prompts.clone(),
            launch_failures: Arc::new(Mutex::new(0)),
        }) as Arc<dyn RuntimeDriverProvider>]),
        RuntimeAuthRegistry::new([Arc::new(TestCodexAuth) as Arc<dyn RuntimeAuthProvider>]),
        Arc::new(ExternalOracleTransport {
            calls: Arc::new(Mutex::new(Vec::new())),
        }),
    );
    cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "start",
            "--type",
            mission_type.to_str().unwrap(),
            "--repo",
            repo.to_str().unwrap(),
            "--objective",
            "prove composition",
            "--runtime",
            "codex",
        ])
        .unwrap(),
        transports.clone(),
    )
    .await
    .unwrap();
    let store = MissionStore::open(&repo).await.unwrap();
    let mission = store.list_missions().await.unwrap().pop().unwrap();
    let proposal = temp.path().join("plan.json");
    std::fs::write(
        &proposal,
        serde_json::to_vec(&PlanProposal {
            base_revision: 0,
            requirement_changes: vec![],
            assertion_supersessions: vec![],
            plan: awaiting_lead_validation_plan(),
        })
        .unwrap(),
    )
    .unwrap();
    for command in [
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "plan",
            "propose",
            mission.as_str(),
            "--file",
            proposal.to_str().unwrap(),
            "--repo",
            repo.to_str().unwrap(),
        ])
        .unwrap(),
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "decide",
            mission.as_str(),
            "plan_proposal:mission",
            "approve",
            "--justification",
            "approve proof",
            "--repo",
            repo.to_str().unwrap(),
        ])
        .unwrap(),
    ] {
        cli::run_with_transports(command, transports.clone())
            .await
            .unwrap();
    }
    let driver = tokio::spawn({
        let transports = transports.clone();
        let command = cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "driver",
            mission.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "--handshake",
            temp.path().join("ready").to_str().unwrap(),
        ])
        .unwrap();
        async move { cli::run_with_transports(command, transports).await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.acquire())
        .await
        .expect("writer did not enter")
        .unwrap()
        .forget();
    let initial = store.require_state(&mission).await.unwrap();
    let plan_order = initial.plan.as_ref().unwrap().tasks.clone();
    release.add_permits(1);
    assert_eq!(
        driver.await.unwrap().unwrap(),
        std::process::ExitCode::SUCCESS
    );
    let driver = tokio::spawn({
        let transports = transports.clone();
        let command = cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "driver",
            mission.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "--handshake",
            temp.path().join("validator-ready").to_str().unwrap(),
        ])
        .unwrap();
        async move { cli::run_with_transports(command, transports).await }
    });
    let validator_entered =
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.acquire()).await;
    if validator_entered.is_err() {
        panic!(
            "validator did not enter: {:#?}",
            store.require_state(&mission).await.unwrap()
        );
    }
    validator_entered.unwrap().unwrap().forget();
    let active = store.require_state(&mission).await.unwrap();
    let writer_id = TaskId::new("writer-question").unwrap();
    let validator_id = TaskId::new("independent-validator").unwrap();
    assert_eq!(active.tasks[&writer_id].status, TaskStatus::Running);
    assert_eq!(active.tasks[&validator_id].status, TaskStatus::Running);
    let (conversation_id, writer) = active
        .conversations
        .iter()
        .find(|(_, c)| c.task_id == writer_id)
        .unwrap();
    assert_eq!(
        writer.lifecycle,
        lionclaw::model::ConversationLifecycle::AwaitingLead
    );
    assert!(active.conversation_is_messageable(conversation_id));
    assert_eq!(active.inflight.len(), 1, "role effects serialize");
    let (effect_id, effect) = active.inflight.iter().next().unwrap();
    let effect_id = effect_id.clone();
    assert!(matches!(
        effect,
        lionclaw::model::InflightEffect::RoleRun { task_id, .. } if task_id == &validator_id
    ));
    let validator_conversation = active
        .conversations
        .iter()
        .find(|(_, conversation)| conversation.task_id == validator_id)
        .map(|(id, _)| id.clone())
        .unwrap();
    let validator_prompt = prompts.lock().unwrap().last().unwrap().0.clone();
    for producer_controlled in [
        "reachable commit ",
        "park evidence ",
        "authoritative receipt ",
        "base.txt",
    ] {
        assert!(
            !validator_prompt.contains(producer_controlled),
            "reference expansion leaked into renamed EmitsVerdict prompt: {producer_controlled}"
        );
    }
    assert_reference_send_rejected(
        &repo,
        &store,
        &mission,
        &validator_conversation,
        vec!["--commit".into(), base.clone()],
        ReferenceRejectionReason::Disallowed {
            conversation_id: validator_conversation.clone(),
            output: OutputSemantics::EmitsVerdict,
        },
    )
    .await;

    let before_mixed = store.load(&mission).await.unwrap();
    let mixed_error = cli::run(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "send",
            "--mission-id",
            mission.as_str(),
            "--all",
            "--commit",
            base.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "mixed recipients must reject atomically",
        ])
        .unwrap(),
    )
    .await
    .expect_err("a mixed judgment recipient send must fail closed");
    assert_eq!(
        mixed_error.downcast_ref::<ReferenceRejectionReason>(),
        Some(&ReferenceRejectionReason::MixedRecipients),
        "mixed-recipient rejection lost exact typed truth: {mixed_error:#}"
    );
    assert_eq!(
        store.load(&mission).await.unwrap(),
        before_mixed,
        "mixed-recipient rejection appended a partial recipient subset"
    );
    assert_eq!(active.deliverable_head(), base);
    assert_eq!(active.plan.as_ref().unwrap().tasks, plan_order);
    assert_eq!(
        store
            .load(&mission)
            .await
            .unwrap()
            .iter()
            .filter(|e| matches!(e.event, MissionEvent::RoleRunRequested { .. }))
            .count(),
        2,
        "no Work redispatch"
    );
    assert_eq!(
        store.rebuild_cursors(&mission, 28_000).await.unwrap(),
        active
    );
    assert_eq!(
        store.snapshot_meta(&mission).await.unwrap(),
        Some((active.head, REDUCER_VERSION))
    );
    cli::run(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "stop",
            mission.as_str(),
            effect_id.as_str(),
            "--reason",
            "prove cancellation composition",
            "--repo",
            repo.to_str().unwrap(),
        ])
        .unwrap(),
    )
    .await
    .unwrap();
    release.add_permits(1);
    assert_eq!(
        driver.await.unwrap().unwrap(),
        std::process::ExitCode::SUCCESS
    );
    let events = store.load(&mission).await.unwrap();
    let replayed = fold(events).unwrap();
    let settled = MissionStore::open(&repo)
        .await
        .unwrap()
        .load_state_snapshotted(&mission)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(settled, replayed);
    assert!(
        settled.head > active.head,
        "real reducer-30 snapshot has a nonempty tail"
    );
    assert_eq!(settled.deliverable_head(), base);
    assert_eq!(settled.tasks[&writer_id].status, TaskStatus::Running);
    assert_eq!(settled.tasks[&validator_id].status, TaskStatus::Failed);
    assert!(settled.inflight.is_empty());
    assert_eq!(
        (settled.parked_effects.len(), settled.open_attention.len()),
        (1, 1)
    );
    assert_eq!(
        (
            turns.lock().unwrap().len(),
            sessions.lock().unwrap().len(),
            prompts.lock().unwrap().len()
        ),
        (0, 2, 2)
    );
    let view = lionclaw::engine::MissionView::from_state(settled, false);
    assert_eq!(view.disposition, MissionDisposition::AwaitingLead);
    assert_eq!(
        view.next_actions(),
        [
            "mission send",
            "mission continue",
            "mission decide",
            "mission abort"
        ]
    );
    let conversation_id = conversation_id.to_string();
    let status: serde_json::Value = serde_json::from_str(&stdout(cli_output(
        &repo,
        &["mission", "status", mission.as_str(), "--json"],
    )))
    .unwrap();
    let report: serde_json::Value = serde_json::from_str(&stdout(cli_output(
        &repo,
        &["mission", "report", mission.as_str(), "--json"],
    )))
    .unwrap();
    let inbox: serde_json::Value =
        serde_json::from_str(&stdout(cli_output(&repo, &["mission", "inbox", "--json"]))).unwrap();
    for root in [&status, &report, &inbox["missions"][0]] {
        assert_eq!(
            root["next_actions"],
            serde_json::json!([
                "mission send",
                "mission continue",
                "mission decide",
                "mission abort"
            ])
        );
        let conversation = projected_conversation(root, &conversation_id);
        assert_eq!(conversation["lifecycle"], "awaiting_lead");
        assert_eq!(
            conversation["legal_actions"],
            serde_json::json!(["mission send"])
        );
        assert!(conversation["final_response"]
            .as_str()
            .unwrap()
            .starts_with("Which release target should I use?"));
    }
    for args in [
        vec!["mission", "status", mission.as_str()],
        vec!["mission", "report", mission.as_str()],
        vec!["mission", "inbox"],
    ] {
        let human = stdout(cli_output(&repo, &args));
        assert!(human.contains("mission send | mission continue | mission decide | mission abort"));
        assert!(human.contains("lifecycle=awaiting_lead"));
    }
    let database = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}",
        repo.join(".lionclaw/mission.db").display()
    ))
    .await
    .unwrap();
    sqlx::query("UPDATE mission_snapshots SET reducer_version = 28 WHERE mission_id = ?1")
        .bind(mission.as_str())
        .execute(&database)
        .await
        .unwrap();
    assert_eq!(
        MissionStore::open(&repo)
            .await
            .unwrap()
            .require_state(&mission)
            .await
            .unwrap(),
        replayed
    );
    assert_eq!(
        store.rebuild_cursors(&mission, 28_001).await.unwrap(),
        replayed
    );
    assert_eq!(
        store.snapshot_meta(&mission).await.unwrap(),
        Some((replayed.head, REDUCER_VERSION))
    );
}

#[tokio::test]
async fn renamed_terminal_gap_verdict_prompt_excludes_all_reference_prose() {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    let base = initialize_repo(&repo).await;
    let fake_oci = temp.path().join("external-oci-transport");
    std::fs::write(
        &fake_oci,
        "#!/bin/sh\nif [ \"$1 $2\" = \"image inspect\" ]; then echo production-image-id; fi\nexit 0\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_oci, std::fs::Permissions::from_mode(0o755)).unwrap();
    let profiles = RuntimeProfiles::from_toml(
        &format!(
            r#"[runtimes.unrelated-runtime-identity]
driver = "unrelated-provider-identity"
command = "external-agent"
native-resume = true
confinement = {{ backend = "podman", engine = "{}", read-only-rootfs = true }}
"#,
            fake_oci.display()
        ),
        temp.path(),
    )
    .unwrap();
    let mission_type = temp.path().join("mission-type");
    materialize_mission_type(&mission_type);
    let manifest = std::fs::read_to_string(mission_type.join("mission.toml")).unwrap();
    std::fs::write(
        mission_type.join("mission.toml"),
        manifest.replace("gap-reviewer", "renamed-closing-role"),
    )
    .unwrap();
    std::fs::rename(
        mission_type.join("roles/gap-reviewer.md"),
        mission_type.join("roles/renamed-closing-role.md"),
    )
    .unwrap();
    for role in ["implementer.md", "renamed-closing-role.md"] {
        let path = mission_type.join("roles").join(role);
        if path.exists() {
            let contents = std::fs::read_to_string(&path).unwrap();
            std::fs::write(
                &path,
                contents.replace("runtime: codex", "runtime: unrelated-runtime-identity"),
            )
            .unwrap();
        }
    }
    let turns = Arc::new(Mutex::new(VecDeque::from([
        DeliveryTurn::Complete,
        DeliveryTurn::Fail,
        DeliveryTurn::Complete,
        DeliveryTurn::Review,
    ])));
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(16));
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let transports = cli::MissionTransports::external(
        profiles,
        RuntimeDriverRegistry::new([Arc::new(RenamedDeliveryProvider {
            inner: DeliveryProvider {
                turns: turns.clone(),
                entered,
                release,
                sessions: Arc::new(Mutex::new(Vec::new())),
                prompts: prompts.clone(),
                launch_failures: Arc::new(Mutex::new(0)),
            },
        }) as Arc<dyn RuntimeDriverProvider>]),
        RuntimeAuthRegistry::empty(),
        Arc::new(ReferenceIsolationOracleTransport {
            attempts: Arc::new(Mutex::new(0)),
        }),
    );
    cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "start",
            "--type",
            mission_type.to_str().unwrap(),
            "--repo",
            repo.to_str().unwrap(),
            "--objective",
            "prove semantic terminal reference isolation",
            "--runtime",
            "unrelated-runtime-identity",
        ])
        .unwrap(),
        transports.clone(),
    )
    .await
    .unwrap();
    let store = MissionStore::open(&repo).await.unwrap();
    let mission = store.list_missions().await.unwrap().pop().unwrap();
    let proposal = temp.path().join("plan.json");
    std::fs::write(
        &proposal,
        serde_json::to_vec(&PlanProposal {
            base_revision: 0,
            requirement_changes: vec![],
            assertion_supersessions: vec![],
            plan: reference_plan(),
        })
        .unwrap(),
    )
    .unwrap();
    for command in [
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "plan",
            "propose",
            mission.as_str(),
            "--file",
            proposal.to_str().unwrap(),
            "--repo",
            repo.to_str().unwrap(),
        ])
        .unwrap(),
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "decide",
            mission.as_str(),
            "plan_proposal:mission",
            "approve",
            "--justification",
            "approve semantic isolation proof",
            "--repo",
            repo.to_str().unwrap(),
        ])
        .unwrap(),
    ] {
        cli::run_with_transports(command, transports.clone())
            .await
            .unwrap();
    }
    let driver = |label: &str| {
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "driver",
            mission.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "--handshake",
            temp.path().join(label).to_str().unwrap(),
        ])
        .unwrap()
    };
    for attempt in 0..4 {
        cli::run_with_transports(
            driver(&format!("mint-receipt-{attempt}.ready")),
            transports.clone(),
        )
        .await
        .unwrap();
        let state = store.require_state(&mission).await.unwrap();
        if !state.authoritative_receipts.is_empty() && !state.open_attention.is_empty() {
            break;
        }
    }
    let failed = store.require_state(&mission).await.unwrap();
    let attention = failed
        .open_attention
        .keys()
        .next()
        .expect("failed oracle attention");
    cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "decide",
            mission.as_str(),
            attention,
            "repair",
            "--justification",
            "retry through a parked reference-bearing turn",
            "--repo",
            repo.to_str().unwrap(),
        ])
        .unwrap(),
        transports.clone(),
    )
    .await
    .unwrap();
    for attempt in 0..4 {
        cli::run_with_transports(
            driver(&format!("park-retry-{attempt}.ready")),
            transports.clone(),
        )
        .await
        .unwrap();
        if !store
            .require_state(&mission)
            .await
            .unwrap()
            .parked_effects
            .is_empty()
        {
            break;
        }
    }
    let parked = store.require_state(&mission).await.unwrap();
    let (conversation, _) = parked
        .conversations
        .iter()
        .find(|(id, conversation)| {
            conversation.task_id.as_str() == "mint-receipt"
                && parked.conversation_is_messageable(id)
        })
        .unwrap_or_else(|| panic!("parked writer conversation: {parked:#?}"));
    let conversation = conversation.clone();
    let park = parked
        .parked_effects
        .keys()
        .next()
        .expect("park evidence identity")
        .clone();
    let receipt = parked
        .authoritative_receipts
        .iter()
        .next()
        .expect("authoritative validator receipt")
        .clone();
    cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "send",
            "--mission-id",
            mission.as_str(),
            "--to",
            conversation.as_str(),
            "--park",
            park.as_str(),
            "--receipt",
            receipt.as_str(),
            "--commit",
            base.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "TERMINAL-DIRECT-PROSE",
        ])
        .unwrap(),
        transports.clone(),
    )
    .await
    .unwrap();
    cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "continue",
            mission.as_str(),
            park.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "--reason",
            "resume after reference delivery",
        ])
        .unwrap(),
        transports.clone(),
    )
    .await
    .unwrap();
    for attempt in 0..6 {
        cli::run_with_transports(
            driver(&format!("finish-{attempt}.ready")),
            transports.clone(),
        )
        .await
        .unwrap();
        if matches!(
            store.require_state(&mission).await.unwrap().phase,
            MissionPhase::Done { .. }
        ) {
            break;
        }
    }
    let captured = prompts.lock().unwrap().clone();
    let resumed = captured
        .iter()
        .find(|(prompt, _)| prompt.contains("TERMINAL-DIRECT-PROSE"))
        .expect("permitted worker received reference expansions");
    for marker in [
        "TERMINAL-RECEIPT-PROSE",
        "base.txt",
        "scripted external transport failure",
    ] {
        assert!(resumed.0.contains(marker), "worker prompt omitted {marker}");
    }
    let terminal = captured
        .iter()
        .find(|(prompt, _)| prompt.contains("## Handoff nonce"))
        .expect("real terminal RoleRunRequest prompt");
    for producer_controlled in [
        "TERMINAL-DIRECT-PROSE",
        "TERMINAL-RECEIPT-PROSE",
        "base.txt",
        "scripted external transport failure",
    ] {
        assert!(
            !terminal.0.contains(producer_controlled),
            "reference prose leaked into renamed EmitsGapVerdict prompt: {producer_controlled}"
        );
    }
    let terminal_role = store
        .load(&mission)
        .await
        .unwrap()
        .into_iter()
        .find_map(|event| match event.event {
            MissionEvent::TerminalReviewRequested { role, .. } => Some(role),
            _ => None,
        })
        .expect("persisted terminal request");
    assert_eq!(terminal_role.as_str(), "renamed-closing-role");
    assert_eq!(
        store
            .require_state(&mission)
            .await
            .unwrap()
            .config
            .plan_inventory
            .roles[&terminal_role],
        OutputSemantics::EmitsGapVerdict
    );
}

#[tokio::test]
async fn production_planner_resumes_and_missing_validator_verdict_reworks_exact_conversation() {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    initialize_repo(&repo).await;
    let fake_oci = temp.path().join("external-oci-transport");
    std::fs::write(
        &fake_oci,
        "#!/bin/sh\nif [ \"$1 $2\" = \"image inspect\" ]; then echo production-image-id; fi\nexit 0\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_oci, std::fs::Permissions::from_mode(0o755)).unwrap();
    let profiles = RuntimeProfiles::from_toml(
        &format!(
            r#"[runtimes.codex]
driver = "codex"
command = "external-codex"
native-resume = true
confinement = {{ backend = "podman", engine = "{}", read-only-rootfs = true }}
"#,
            fake_oci.display()
        ),
        temp.path(),
    )
    .unwrap();
    let mission_type_dir = temp.path().join("mission-type");
    materialize_planning_validation_mission_type(&mission_type_dir);
    let turns = Arc::new(Mutex::new(VecDeque::from([
        DeliveryTurn::AwaitLead,
        DeliveryTurn::Plan,
        DeliveryTurn::Complete,
        DeliveryTurn::AwaitLead,
        DeliveryTurn::Validate,
        DeliveryTurn::Review,
    ])));
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(32));
    let sessions = Arc::new(Mutex::new(Vec::new()));
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let transports = cli::MissionTransports::external(
        profiles,
        RuntimeDriverRegistry::new([Arc::new(DeliveryProvider {
            turns: turns.clone(),
            entered,
            release,
            sessions: sessions.clone(),
            prompts: prompts.clone(),
            launch_failures: Arc::new(Mutex::new(0)),
        }) as Arc<dyn RuntimeDriverProvider>]),
        RuntimeAuthRegistry::empty(),
        Arc::new(ExternalOracleTransport {
            calls: Arc::new(Mutex::new(Vec::new())),
        }),
    );
    cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "start",
            "--type",
            mission_type_dir.to_str().unwrap(),
            "--repo",
            repo.to_str().unwrap(),
            "--objective",
            "prove planner and validator conversation identity",
            "--runtime",
            "codex",
        ])
        .unwrap(),
        transports.clone(),
    )
    .await
    .unwrap();
    let store = MissionStore::open(&repo).await.unwrap();
    let mission_id = store.list_missions().await.unwrap().pop().unwrap();
    let run_driver = |label: &str| {
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "driver",
            mission_id.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "--handshake",
            temp.path().join(label).to_str().unwrap(),
        ])
        .unwrap()
    };
    cli::run_with_transports(run_driver("planner-await.ready"), transports.clone())
        .await
        .unwrap();

    let planner_state = MissionStore::open(&repo)
        .await
        .unwrap()
        .require_state(&mission_id)
        .await
        .unwrap();
    let (planner_id, planner) = planner_state
        .conversations
        .iter()
        .find(|(_, conversation)| conversation.task_id.as_str() == "planner")
        .expect("production planner conversation");
    assert_eq!(
        planner.lifecycle,
        lionclaw::model::ConversationLifecycle::AwaitingLead
    );
    let planner_id = planner_id.clone();
    let planner_generation = planner.assignment_epoch;
    cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "send",
            "--mission-id",
            mission_id.as_str(),
            "--to",
            planner_id.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "Use the exact proposed production contract.",
        ])
        .unwrap(),
        transports.clone(),
    )
    .await
    .unwrap();
    cli::run_with_transports(run_driver("planner-resume.ready"), transports.clone())
        .await
        .unwrap();
    let after_planner = MissionStore::open(&repo)
        .await
        .unwrap()
        .require_state(&mission_id)
        .await
        .unwrap();
    let planner = &after_planner.conversations[&planner_id];
    assert_eq!(planner.assignment_epoch, planner_generation);
    assert!(planner.queued.is_empty());
    assert!(planner.consumed_through > 0);
    assert!(after_planner.proposal.is_some());

    cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "decide",
            mission_id.as_str(),
            "plan_proposal:mission",
            "approve",
            "--justification",
            "exercise validator production routing",
            "--repo",
            repo.to_str().unwrap(),
        ])
        .unwrap(),
        transports.clone(),
    )
    .await
    .unwrap();
    cli::run_with_transports(run_driver("validator-await.ready"), transports.clone())
        .await
        .unwrap();
    // Promotion of the implementer's commit closes that driver pass. A fresh
    // production driver reload dispatches the dependent validator.
    cli::run_with_transports(run_driver("validator-dispatch.ready"), transports.clone())
        .await
        .unwrap();
    let validator_state = MissionStore::open(&repo)
        .await
        .unwrap()
        .require_state(&mission_id)
        .await
        .unwrap();
    let (validator_id, validator) = validator_state
        .conversations
        .iter()
        .find(|(_, conversation)| conversation.task_id.as_str() == "validate")
        .unwrap_or_else(|| panic!("production validator conversation: {validator_state:#?}"));
    assert_ne!(
        validator.lifecycle,
        lionclaw::model::ConversationLifecycle::AwaitingLead,
        "required verdict absence must never become a dialogue checkpoint"
    );
    let validator_id = validator_id.clone();
    let validator_generation = validator.assignment_epoch;
    assert_ne!(planner_id, validator_id);
    cli::run_with_transports(run_driver("terminal-review.ready"), transports.clone())
        .await
        .unwrap();
    let final_events = MissionStore::open(&repo)
        .await
        .unwrap()
        .load(&mission_id)
        .await
        .unwrap();
    let final_state = fold(final_events.clone()).unwrap();
    let validator = &final_state.conversations[&validator_id];
    assert_eq!(validator.assignment_epoch, validator_generation);
    assert!(validator.queued.is_empty());
    assert_eq!(validator.invalid_handoff_reworks, 1);
    let validator_outcomes = final_events
        .iter()
        .filter_map(|event| match &event.event {
            lionclaw::model::MissionEvent::RoleRunCompleted {
                request, outcome, ..
            } if request.conversation_id == validator_id => Some(outcome),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(validator_outcomes.len(), 2);
    assert!(matches!(
        validator_outcomes[0],
        Err(failure) if failure.is_invalid_output()
            && failure.evidence().code.as_deref() == Some("handoff.missing")
    ));
    assert!(validator_outcomes[1].is_ok());
    assert_eq!(fold(final_events).unwrap(), final_state);
    assert!(turns.lock().unwrap().is_empty());
    let sessions = sessions.lock().unwrap();
    assert!(sessions.iter().any(|(_, resumed)| *resumed));
    let prompts = prompts.lock().unwrap();
    assert!(prompts
        .iter()
        .any(|(prompt, _)| { prompt.contains("Use the exact proposed production contract.") }));
    assert_eq!(
        prompts
            .iter()
            .filter(|(_, runtime)| runtime.to_string_lossy().contains(validator_id.as_str()))
            .count(),
        2,
        "invalid output and its repair use the same conversation runtime"
    );
    assert!(prompts
        .iter()
        .any(|(_, runtime)| runtime.to_string_lossy().contains(planner_id.as_str())));
    assert!(prompts
        .iter()
        .any(|(_, runtime)| runtime.to_string_lossy().contains(validator_id.as_str())));
}

#[tokio::test]
async fn production_same_base_revision_retires_awaiting_planner_before_broadcast() {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    let base = initialize_repo(&repo).await;
    let fake_oci = temp.path().join("external-oci-transport");
    std::fs::write(
        &fake_oci,
        "#!/bin/sh\nif [ \"$1 $2\" = \"image inspect\" ]; then echo production-image-id; fi\nexit 0\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_oci, std::fs::Permissions::from_mode(0o755)).unwrap();
    let profiles = RuntimeProfiles::from_toml(
        &format!(
            r#"[runtimes.codex]
driver = "codex"
command = "external-codex"
native-resume = true
confinement = {{ backend = "podman", engine = "{}", read-only-rootfs = true }}
"#,
            fake_oci.display()
        ),
        temp.path(),
    )
    .unwrap();
    let mission_type_dir = temp.path().join("mission-type");
    materialize_planning_validation_mission_type(&mission_type_dir);
    let turns = Arc::new(Mutex::new(VecDeque::from([
        DeliveryTurn::AwaitLead,
        DeliveryTurn::AwaitLead,
    ])));
    let transports = cli::MissionTransports::external(
        profiles,
        RuntimeDriverRegistry::new([Arc::new(DeliveryProvider {
            turns: turns.clone(),
            entered: Arc::new(tokio::sync::Semaphore::new(0)),
            release: Arc::new(tokio::sync::Semaphore::new(8)),
            sessions: Arc::new(Mutex::new(Vec::new())),
            prompts: Arc::new(Mutex::new(Vec::new())),
            launch_failures: Arc::new(Mutex::new(0)),
        }) as Arc<dyn RuntimeDriverProvider>]),
        RuntimeAuthRegistry::empty(),
        Arc::new(ExternalOracleTransport {
            calls: Arc::new(Mutex::new(Vec::new())),
        }),
    );
    cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "start",
            "--type",
            mission_type_dir.to_str().unwrap(),
            "--repo",
            repo.to_str().unwrap(),
            "--objective",
            "prove same-base planner replacement routing",
            "--runtime",
            "codex",
        ])
        .unwrap(),
        transports.clone(),
    )
    .await
    .unwrap();
    let store = MissionStore::open(&repo).await.unwrap();
    let mission_id = store.list_missions().await.unwrap().pop().unwrap();
    let driver = |label: &str| {
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "driver",
            mission_id.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "--handshake",
            temp.path().join(label).to_str().unwrap(),
        ])
        .unwrap()
    };
    cli::run_with_transports(driver("original-planner.ready"), transports.clone())
        .await
        .unwrap();

    let awaiting = MissionStore::open(&repo)
        .await
        .unwrap()
        .require_state(&mission_id)
        .await
        .unwrap();
    assert_eq!(awaiting.current_sha, base);
    let (retired_id, original) = awaiting
        .conversations
        .iter()
        .find(|(_, conversation)| conversation.task_id.as_str() == "planner")
        .expect("real planner conversation");
    assert_eq!(
        original.lifecycle,
        lionclaw::model::ConversationLifecycle::AwaitingLead
    );
    assert!(original.final_response.is_some());
    let retired_id = retired_id.clone();
    let original_generation = original.assignment_epoch;

    cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "send",
            "--mission-id",
            mission_id.as_str(),
            "--to",
            retired_id.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "Retain this queued message only as retired conversation history.",
        ])
        .unwrap(),
        transports.clone(),
    )
    .await
    .unwrap();

    let proposal_path = temp.path().join("same-base-plan.json");
    std::fs::write(
        &proposal_path,
        serde_json::to_vec(&PlanProposal {
            base_revision: 0,
            requirement_changes: vec![],
            assertion_supersessions: vec![],
            plan: plan(),
        })
        .unwrap(),
    )
    .unwrap();
    cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "plan",
            "propose",
            mission_id.as_str(),
            "--file",
            proposal_path.to_str().unwrap(),
            "--repo",
            repo.to_str().unwrap(),
        ])
        .unwrap(),
        transports.clone(),
    )
    .await
    .unwrap();
    let feedback = temp.path().join("revision-feedback.txt");
    std::fs::write(&feedback, "replace the awaiting planner generation\n").unwrap();
    cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "decide",
            mission_id.as_str(),
            "plan_proposal:mission",
            "revise",
            "--feedback-file",
            feedback.to_str().unwrap(),
            "--repo",
            repo.to_str().unwrap(),
        ])
        .unwrap(),
        transports.clone(),
    )
    .await
    .unwrap();

    let retired_events = store.load(&mission_id).await.unwrap();
    let retired = fold(retired_events.clone()).unwrap();
    assert_eq!(retired.current_sha, base, "revision must not move Git base");
    let original = &retired.conversations[&retired_id];
    assert_eq!(
        original.lifecycle,
        lionclaw::model::ConversationLifecycle::Retired
    );
    assert_eq!(original.assignment_epoch, original_generation);
    assert!(original.final_response.is_some());
    assert!(original.active_delivery.is_none());
    assert_eq!(original.queued.len(), 1);
    assert!(original
        .queued
        .iter()
        .all(|message| { message.marker == lionclaw::model::DeliveryMarker::Undeliverable }));
    assert!(retired.planning.tasks[&TaskId::new("planner").unwrap()]
        .last_report
        .is_none());
    assert!(retired.conversation_legal_actions(&retired_id).is_empty());
    assert!(retired
        .parked_effects
        .keys()
        .all(|effect_id| !retired.parked_effect_is_continuable(effect_id)));
    let superseded_tombstones: Vec<_> = retired
        .tasks
        .iter()
        .filter(|(_, task)| task.status == lionclaw::model::TaskStatus::Superseded)
        .map(|(task_id, _)| task_id.clone())
        .collect();

    let before_stale = store.load(&mission_id).await.unwrap();
    cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "send",
            "--mission-id",
            mission_id.as_str(),
            "--to",
            retired_id.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "stale targeted send",
        ])
        .unwrap(),
        transports.clone(),
    )
    .await
    .expect_err("retired planner must reject targeted send");
    assert_eq!(store.load(&mission_id).await.unwrap(), before_stale);

    cli::run_with_transports(driver("replacement-planner.ready"), transports.clone())
        .await
        .unwrap();
    let replacement_state = MissionStore::open(&repo)
        .await
        .unwrap()
        .require_state(&mission_id)
        .await
        .unwrap();
    let (replacement_id, replacement) = replacement_state
        .conversations
        .iter()
        .find(|(id, conversation)| {
            *id != &retired_id
                && conversation.task_id.as_str() == "planner"
                && replacement_state.conversation_is_messageable(id)
        })
        .expect("distinct live replacement planner conversation");
    assert_ne!(replacement_id, &retired_id);
    assert!(replacement.assignment_epoch > original_generation);
    assert!(replacement.queued.is_empty());
    assert!(superseded_tombstones.iter().all(|task_id| {
        replacement_state.tasks[task_id].status == lionclaw::model::TaskStatus::Superseded
    }));

    let before_all = store.load(&mission_id).await.unwrap();
    cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "send",
            "--mission-id",
            mission_id.as_str(),
            "--all",
            "--repo",
            repo.to_str().unwrap(),
            "broadcast only to live replacement conversations",
        ])
        .unwrap(),
        transports,
    )
    .await
    .unwrap();
    let after_all = store.load(&mission_id).await.unwrap();
    assert_eq!(after_all.len(), before_all.len() + 1);
    let MissionEvent::MessageSent { recipients, .. } = &after_all.last().unwrap().event else {
        panic!("--all must append one atomic MessageSent event")
    };
    assert_eq!(recipients.len(), 1);
    assert_eq!(recipients[0].conversation_id, *replacement_id);
    assert!(recipients
        .iter()
        .all(|recipient| recipient.conversation_id != retired_id));

    let full = fold(after_all).expect("full replay after replacement broadcast");
    let fresh = MissionStore::open(&repo)
        .await
        .unwrap()
        .load_state_snapshotted(&mission_id)
        .await
        .unwrap()
        .expect("fresh snapshotted replay");
    assert_eq!(fresh, full);
    assert_eq!(
        full.conversations[&retired_id].final_response,
        original.final_response
    );
    assert!(turns.lock().unwrap().is_empty());
}

#[tokio::test]
async fn production_park_then_base_move_replaces_and_isolates_stale_conversation() {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    let base = initialize_repo(&repo).await;
    let fake_oci = temp.path().join("external-oci-transport");
    std::fs::write(
        &fake_oci,
        "#!/bin/sh\nif [ \"$1 $2\" = \"image inspect\" ]; then echo production-image-id; fi\nexit 0\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_oci, std::fs::Permissions::from_mode(0o755)).unwrap();
    let profiles = RuntimeProfiles::from_toml(
        &format!(
            r#"[runtimes.codex]
driver = "codex"
command = "external-codex"
native-resume = true
confinement = {{ backend = "podman", engine = "{}", read-only-rootfs = true }}
"#,
            fake_oci.display()
        ),
        temp.path(),
    )
    .unwrap();
    let mission_type_dir = temp.path().join("mission-type");
    materialize_mission_type(&mission_type_dir);
    let turns = Arc::new(Mutex::new(VecDeque::from([
        DeliveryTurn::Fail,
        DeliveryTurn::Complete,
        DeliveryTurn::AwaitLead,
    ])));
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(1));
    let transports = cli::MissionTransports::external(
        profiles,
        RuntimeDriverRegistry::new([Arc::new(DeliveryProvider {
            turns: turns.clone(),
            entered: entered.clone(),
            release: release.clone(),
            sessions: Arc::new(Mutex::new(Vec::new())),
            prompts: prompts.clone(),
            launch_failures: Arc::new(Mutex::new(0)),
        }) as Arc<dyn RuntimeDriverProvider>]),
        RuntimeAuthRegistry::empty(),
        Arc::new(FailingOracleTransport),
    );
    cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "start",
            "--type",
            mission_type_dir.to_str().unwrap(),
            "--repo",
            repo.to_str().unwrap(),
            "--objective",
            "prove parked work isolation across an ordinary base move",
            "--runtime",
            "codex",
        ])
        .unwrap(),
        transports.clone(),
    )
    .await
    .unwrap();
    let store = MissionStore::open(&repo).await.unwrap();
    let mission = store.list_missions().await.unwrap().pop().unwrap();
    let proposal_path = temp.path().join("parked-base-move-plan.json");
    std::fs::write(
        &proposal_path,
        serde_json::to_vec(&PlanProposal {
            base_revision: 0,
            requirement_changes: vec![],
            assertion_supersessions: vec![],
            plan: reference_plan(),
        })
        .unwrap(),
    )
    .unwrap();
    for command in [
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "plan",
            "propose",
            mission.as_str(),
            "--file",
            proposal_path.to_str().unwrap(),
            "--repo",
            repo.to_str().unwrap(),
        ])
        .unwrap(),
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "decide",
            mission.as_str(),
            "plan_proposal:mission",
            "approve",
            "--justification",
            "exercise ordinary production promotion",
            "--repo",
            repo.to_str().unwrap(),
        ])
        .unwrap(),
    ] {
        cli::run_with_transports(command, transports.clone())
            .await
            .unwrap();
    }
    let driver = |label: &str| {
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "driver",
            mission.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "--handshake",
            temp.path().join(label).to_str().unwrap(),
        ])
        .unwrap()
    };
    cli::run_with_transports(driver("park-old.ready"), transports.clone())
        .await
        .unwrap();
    entered.acquire().await.unwrap().forget();
    let parked = MissionStore::open(&repo)
        .await
        .unwrap()
        .require_state(&mission)
        .await
        .unwrap();
    let (old_id, old) = parked
        .conversations
        .iter()
        .find(|(_, conversation)| conversation.task_id.as_str() == "mint-receipt")
        .expect("parked production conversation");
    assert_eq!(old.lifecycle, lionclaw::model::ConversationLifecycle::Ready);
    assert_eq!(old.workspace_base_sha, base);
    let old_id = old_id.clone();
    let old_generation = old.assignment_epoch;
    let old_park = parked
        .parked_effects
        .iter()
        .find(|(_, effect)| {
            matches!(
                effect,
                lionclaw::model::ParkedEffect::RoleRun { task_id, .. }
                    if task_id.as_str() == "mint-receipt"
            )
        })
        .map(|(id, _)| id.clone())
        .expect("exact parked production effect");
    cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "send",
            "--mission-id",
            mission.as_str(),
            "--to",
            old_id.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "this message must become explicitly undeliverable",
        ])
        .unwrap(),
        transports.clone(),
    )
    .await
    .unwrap();

    cli::run(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "continue",
            mission.as_str(),
            old_park.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "--reason",
            "complete ordinary work from the unchanged assignment",
        ])
        .unwrap(),
    )
    .await
    .unwrap();
    let continued = MissionStore::open(&repo)
        .await
        .unwrap()
        .require_state(&mission)
        .await
        .unwrap();
    let continued_conversation = &continued.conversations[&old_id];
    assert_eq!(continued_conversation.assignment_epoch, old_generation);
    assert_eq!(continued_conversation.workspace_base_sha, base);

    let move_driver = tokio::spawn({
        let transports = transports.clone();
        let command = driver("move-base.ready");
        async move { cli::run_with_transports(command, transports).await }
    });
    entered.acquire().await.unwrap().forget();
    cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "send",
            "--mission-id",
            mission.as_str(),
            "--to",
            old_id.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "this late message must remain beyond the active boundary",
        ])
        .unwrap(),
        transports.clone(),
    )
    .await
    .unwrap();
    release.add_permits(1);
    move_driver.await.unwrap().unwrap();
    let reloaded_store = MissionStore::open(&repo).await.unwrap();
    assert_ne!(
        reloaded_store
            .require_state(&mission)
            .await
            .unwrap()
            .current_sha,
        base,
        "ordinary work did not move Git base"
    );
    for attempt in 0..4 {
        if !reloaded_store
            .require_state(&mission)
            .await
            .unwrap()
            .open_attention
            .is_empty()
        {
            break;
        }
        cli::run_with_transports(
            driver(&format!("fail-oracle-{attempt}.ready")),
            transports.clone(),
        )
        .await
        .unwrap();
    }
    let moved = reloaded_store.require_state(&mission).await.unwrap();
    let old_response = moved.conversations[&old_id].final_response.clone();
    assert!(old_response.is_some());
    let attention = moved.open_attention.keys().next().unwrap().clone();
    cli::run(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "decide",
            mission.as_str(),
            attention.as_str(),
            "repair",
            "--justification",
            "request the moved-base replacement after ordinary oracle failure",
            "--repo",
            repo.to_str().unwrap(),
        ])
        .unwrap(),
    )
    .await
    .unwrap();
    release.add_permits(1);
    cli::run_with_transports(driver("replacement.ready"), transports.clone())
        .await
        .unwrap();
    let moved = MissionStore::open(&repo)
        .await
        .unwrap()
        .require_state(&mission)
        .await
        .unwrap();
    let retired = &moved.conversations[&old_id];
    assert_eq!(
        retired.lifecycle,
        lionclaw::model::ConversationLifecycle::Retired
    );
    assert_eq!(retired.assignment_epoch, old_generation);
    assert_eq!(retired.final_response, old_response);
    assert!(retired.active_delivery.is_none());
    assert_eq!(retired.queued.len(), 1);
    assert!(retired.queued[0].sequence_no > retired.consumed_through);
    assert!(retired
        .queued
        .iter()
        .all(|message| message.marker == lionclaw::model::DeliveryMarker::Undeliverable));
    assert!(moved.conversation_legal_actions(&old_id).is_empty());
    assert!(!moved.parked_effect_is_continuable(&old_park));
    let (replacement_id, replacement) = moved
        .conversations
        .iter()
        .find(|(id, conversation)| {
            *id != &old_id
                && conversation.task_id.as_str() == "mint-receipt"
                && moved.conversation_is_messageable(id)
        })
        .expect("same-task moved-base replacement conversation");
    assert_ne!(replacement_id, &old_id);
    assert_eq!(replacement.workspace_base_sha, moved.current_sha);
    assert!(replacement.assignment_epoch > old_generation);
    assert!(replacement.queued.is_empty());
    assert!(replacement.active_delivery.is_none());
    assert_ne!(replacement.final_response, old_response);

    let before_stale = reloaded_store.load(&mission).await.unwrap();
    cli::run(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "continue",
            mission.as_str(),
            old_park.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "--reason",
            "stale continuation must fail closed",
        ])
        .unwrap(),
    )
    .await
    .expect_err("retired parked effect must not continue");
    assert_eq!(reloaded_store.load(&mission).await.unwrap(), before_stale);
    cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "send",
            "--mission-id",
            mission.as_str(),
            "--to",
            old_id.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "stale targeted send must fail closed",
        ])
        .unwrap(),
        transports.clone(),
    )
    .await
    .expect_err("retired conversation must reject targeted send");
    assert_eq!(reloaded_store.load(&mission).await.unwrap(), before_stale);

    cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "send",
            "--mission-id",
            mission.as_str(),
            "--all",
            "--repo",
            repo.to_str().unwrap(),
            "broadcast only to the replacement generation",
        ])
        .unwrap(),
        transports,
    )
    .await
    .unwrap();
    let final_events = reloaded_store.load(&mission).await.unwrap();
    let MissionEvent::MessageSent { recipients, .. } = &final_events.last().unwrap().event else {
        panic!("--all must append one atomic message event")
    };
    assert!(recipients
        .iter()
        .all(|recipient| recipient.conversation_id != old_id));
    assert!(recipients
        .iter()
        .any(|recipient| recipient.conversation_id == *replacement_id));
    let replayed = fold(final_events).unwrap();
    let snapshotted = MissionStore::open(&repo)
        .await
        .unwrap()
        .load_state_snapshotted(&mission)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshotted, replayed);
    assert_eq!(replayed.conversations[&old_id].final_response, old_response);
    assert!(replayed.conversations[&old_id].active_delivery.is_none());
    assert!(turns.lock().unwrap().is_empty());
    assert!(prompts
        .lock()
        .unwrap()
        .iter()
        .any(|(_, runtime)| runtime.to_string_lossy().contains(replacement_id.as_str())));
}

fn materialize_mission_type(root: &Path) {
    std::fs::create_dir_all(root.join("roles")).unwrap();
    std::fs::create_dir_all(root.join("oracles")).unwrap();
    std::fs::write(root.join("playbook.md"), "Production flow fixture.\n").unwrap();
    std::fs::write(
        root.join("mission.toml"),
        r#"[mission-type]
name = "production-flow"
stop = "verified"
image = "production-image"

[terminal-review]
role = "gap-reviewer"

[execution]
default-timeout-secs = 60
max-task-time-secs = 120
extension-step-secs = 30
auto-continue-candidate = false
auto-continue-proof = true
"#,
    )
    .unwrap();
    std::fs::write(
        root.join("roles/implementer.md"),
        "---\noutput: produces-artifact\nruntime: codex\n---\nImplement the production flow.\n",
    )
    .unwrap();
    std::fs::write(
        root.join("roles/gap-reviewer.md"),
        "---\noutput: emits-gap-verdict\nruntime: codex\n---\nReview the production flow.\n",
    )
    .unwrap();
    for name in ["build-release", "cargo-clippy", "cargo-test", "fmt-check"] {
        let oracle = root.join("oracles").join(name);
        std::fs::write(&oracle, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&oracle, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}

fn materialize_planning_validation_mission_type(root: &Path) {
    materialize_mission_type(root);
    let manifest = std::fs::read_to_string(root.join("mission.toml")).unwrap();
    std::fs::write(
        root.join("mission.toml"),
        format!(
            "{manifest}\n[[planning.tasks]]\nid = \"planner\"\nrole = \"planner\"\nbody = \"plan the production conversation proof\"\n"
        ),
    )
    .unwrap();
    std::fs::write(
        root.join("roles/planner.md"),
        "---\noutput: proposes-plan\nruntime: codex\n---\nPlan the production flow.\n",
    )
    .unwrap();
    std::fs::write(
        root.join("roles/validator.md"),
        "---\noutput: emits-verdict\nruntime: codex\n---\nValidate the production flow.\n",
    )
    .unwrap();
}

#[tokio::test]
async fn production_conversation_restarts_and_closes_verified() {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    let base = initialize_repo(&repo).await;
    let fake_oci = temp.path().join("external-oci-transport");
    std::fs::write(
        &fake_oci,
        "#!/bin/sh\nif [ \"$1 $2\" = \"image inspect\" ]; then echo production-image-id; fi\nexit 0\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_oci, std::fs::Permissions::from_mode(0o755)).unwrap();
    let profiles = RuntimeProfiles::from_toml(
        &format!(
            r#"[runtimes.codex]
driver = "codex"
command = "external-codex"
native-resume = true
confinement = {{ backend = "podman", engine = "{}", read-only-rootfs = true }}
"#,
            fake_oci.display()
        ),
        temp.path(),
    )
    .unwrap();
    let mission_type_dir = temp.path().join("mission-type");
    materialize_mission_type(&mission_type_dir);
    let turns = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::new(Mutex::new(Vec::new()));
    let transports = cli::MissionTransports::external(
        profiles,
        RuntimeDriverRegistry::new([Arc::new(NativeProvider {
            turns: turns.clone(),
        }) as Arc<dyn RuntimeDriverProvider>]),
        RuntimeAuthRegistry::empty(),
        Arc::new(ExternalOracleTransport {
            calls: calls.clone(),
        }),
    );
    let start = cli::Cli::try_parse_from([
        "lionclaw",
        "mission",
        "start",
        "--type",
        mission_type_dir.to_str().unwrap(),
        "--repo",
        repo.to_str().unwrap(),
        "--objective",
        "prove the production conversation",
        "--runtime",
        "codex",
    ])
    .unwrap();
    cli::run_with_transports(start, transports.clone())
        .await
        .unwrap();
    let store = MissionStore::open(&repo).await.unwrap();
    let missions = store.list_missions().await.unwrap();
    let [mission_id] = missions.as_slice() else {
        panic!("start must create exactly one mission")
    };
    let mission_id = mission_id.clone();
    let proposal_path = temp.path().join("proposal.json");
    std::fs::write(
        &proposal_path,
        serde_json::to_vec(&PlanProposal {
            base_revision: 0,
            requirement_changes: vec![],
            assertion_supersessions: vec![],
            plan: plan(),
        })
        .unwrap(),
    )
    .unwrap();
    let propose = cli::Cli::try_parse_from([
        "lionclaw",
        "mission",
        "plan",
        "propose",
        mission_id.as_str(),
        "--file",
        proposal_path.to_str().unwrap(),
        "--repo",
        repo.to_str().unwrap(),
    ])
    .unwrap();
    cli::run_with_transports(propose, transports.clone())
        .await
        .unwrap();
    let decide = cli::Cli::try_parse_from([
        "lionclaw",
        "mission",
        "decide",
        mission_id.as_str(),
        "plan_proposal:mission",
        "approve",
        "--justification",
        "approve production proof",
        "--repo",
        repo.to_str().unwrap(),
    ])
    .unwrap();
    cli::run_with_transports(decide, transports.clone())
        .await
        .unwrap();

    let status = cli::Cli::try_parse_from([
        "lionclaw",
        "mission",
        "status",
        mission_id.as_str(),
        "--repo",
        repo.to_str().unwrap(),
        "--json",
    ])
    .unwrap();
    assert_eq!(
        cli::run_with_transports(status, transports.clone())
            .await
            .unwrap(),
        std::process::ExitCode::SUCCESS
    );

    let outcome = loop {
        let handshake = temp
            .path()
            .join(format!("driver-{}.ready", turns.lock().unwrap().len()));
        let driver = cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "driver",
            mission_id.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "--handshake",
            handshake.to_str().unwrap(),
        ])
        .unwrap();
        cli::run_with_transports(driver, transports.clone())
            .await
            .unwrap();
        let view = lionclaw::engine::load_mission_view(
            &MissionStore::open(&repo).await.unwrap(),
            &mission_id,
        )
        .await
        .unwrap();
        if view.disposition == MissionDisposition::Terminal {
            break view;
        }
    };
    assert_eq!(
        outcome.state.phase,
        MissionPhase::Done {
            finish: FinishClass::Verified
        }
    );
    for args in [
        vec![
            "lionclaw",
            "mission",
            "report",
            mission_id.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "--json",
        ],
        vec![
            "lionclaw",
            "mission",
            "inbox",
            "--repo",
            repo.to_str().unwrap(),
            "--json",
        ],
    ] {
        let observation = cli::Cli::try_parse_from(args).unwrap();
        assert_eq!(
            cli::run_with_transports(observation, transports.clone())
                .await
                .unwrap(),
            std::process::ExitCode::SUCCESS
        );
    }
    assert_ne!(outcome.state.current_sha, base);
    let oracle_calls = calls.lock().unwrap().clone();
    let mut called_names = oracle_calls
        .iter()
        .map(|(oracle, _)| oracle.as_str())
        .collect::<Vec<_>>();
    called_names.sort_unstable();
    assert_eq!(
        called_names,
        ["build-release", "cargo-clippy", "cargo-test", "fmt-check"]
    );
    assert!(oracle_calls
        .iter()
        .all(|(_, judged_sha)| judged_sha == &outcome.state.current_sha));
    assert_eq!(turns.lock().unwrap().len(), 2);
    let restarted = MissionStore::open(&repo).await.unwrap();
    let events = restarted.load(&mission_id).await.unwrap();
    let replayed = fold(events.clone()).expect("full replay");
    assert_eq!(replayed, outcome.state);
    for split in 1..events.len() {
        let mut from_prefix = fold(events[..split].to_vec()).expect("substantive prefix");
        for event in &events[split..] {
            apply(&mut from_prefix, event);
        }
        assert_eq!(from_prefix, replayed, "prefix split {split} diverged");
    }
    let snapshotted = restarted
        .load_state_snapshotted(&mission_id)
        .await
        .expect("snapshot resume")
        .expect("mission state");
    assert_eq!(snapshotted, replayed);
    let snapshot_json = serde_json::to_string(&snapshotted).expect("encode snapshot state");
    assert_eq!(
        serde_json::from_str::<MissionState>(&snapshot_json).expect("decode snapshot state"),
        replayed
    );
    let rebuilt = restarted
        .rebuild_cursors(&mission_id, 9_000_000)
        .await
        .expect("rebuild snapshot from authoritative log");
    assert_eq!(rebuilt, replayed);
    let role = events
        .iter()
        .position(|event| matches!(event.event, MissionEvent::RoleRunCompleted { .. }))
        .unwrap();
    let oracle_completions = events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| match &event.event {
            MissionEvent::OracleRunCompleted {
                oracle,
                judged_sha,
                outcome: Ok(outcome),
                ..
            } => Some((index, oracle.as_str(), judged_sha.as_str(), outcome)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(oracle_completions.len(), 4);
    assert_eq!(
        oracle_completions
            .iter()
            .map(|(_, oracle, ..)| *oracle)
            .collect::<Vec<_>>(),
        oracle_calls
            .iter()
            .map(|(oracle, _)| oracle.as_str())
            .collect::<Vec<_>>(),
        "completion receipts must retain the serial external transport order"
    );
    for (_, oracle, judged_sha, actual) in &oracle_completions {
        let (exit_code, _, duration_ms) = scripted_oracle_outcome(oracle);
        assert_eq!(*judged_sha, outcome.state.current_sha);
        assert_eq!(actual.exit_code, exit_code);
        assert_eq!(actual.duration_ms, duration_ms);
    }
    let gate = events
        .iter()
        .position(|event| matches!(event.event, MissionEvent::TerminalReviewRequested { .. }))
        .unwrap();
    let review = events
        .iter()
        .position(|event| matches!(event.event, MissionEvent::TerminalReviewCompleted { .. }))
        .unwrap();
    assert!(oracle_completions
        .iter()
        .all(|(oracle, ..)| role < *oracle && *oracle < gate));
    assert!(oracle_completions
        .windows(2)
        .all(|pair| pair[0].0 < pair[1].0));
    assert!(gate < review);
    assert!(repo
        .join(".lionclaw/missions")
        .read_dir()
        .unwrap()
        .all(|entry| {
            !entry
                .unwrap()
                .path()
                .join("effects")
                .read_dir()
                .is_ok_and(|mut entries| entries.next().is_some())
        }));
}

#[tokio::test]
async fn production_delivery_uses_one_exact_immutable_boundary() {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    let base = initialize_repo(&repo).await;
    let fake_oci = temp.path().join("external-oci-transport");
    std::fs::write(
        &fake_oci,
        "#!/bin/sh\nif [ \"$1 $2\" = \"image inspect\" ]; then echo production-image-id; fi\nexit 0\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_oci, std::fs::Permissions::from_mode(0o755)).unwrap();
    let profiles = RuntimeProfiles::from_toml(
        &format!(
            r#"[runtimes.codex]
driver = "codex"
command = "external-codex"
auth = "codex"
native-resume = true
confinement = {{ backend = "podman", engine = "{}", read-only-rootfs = true }}
"#,
            fake_oci.display()
        ),
        temp.path(),
    )
    .unwrap();
    let mission_type_dir = temp.path().join("mission-type");
    materialize_mission_type(&mission_type_dir);
    let turns = Arc::new(Mutex::new(VecDeque::from([
        DeliveryTurn::AwaitLead,
        DeliveryTurn::InvalidHandoff,
        DeliveryTurn::Fail,
        DeliveryTurn::AwaitLead,
        DeliveryTurn::Complete,
        DeliveryTurn::Review,
    ])));
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let sessions = Arc::new(Mutex::new(Vec::new()));
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let launch_failures = Arc::new(Mutex::new(0));
    let oracle_calls = Arc::new(Mutex::new(Vec::new()));
    let transports = cli::MissionTransports::external(
        profiles.clone(),
        RuntimeDriverRegistry::new([Arc::new(DeliveryProvider {
            turns: turns.clone(),
            entered: entered.clone(),
            release: release.clone(),
            sessions: sessions.clone(),
            prompts: prompts.clone(),
            launch_failures: launch_failures.clone(),
        }) as Arc<dyn RuntimeDriverProvider>]),
        RuntimeAuthRegistry::new([Arc::new(TestCodexAuth) as Arc<dyn RuntimeAuthProvider>]),
        Arc::new(ExternalOracleTransport {
            calls: oracle_calls.clone(),
        }),
    );
    let start = cli::Cli::try_parse_from([
        "lionclaw",
        "mission",
        "start",
        "--type",
        mission_type_dir.to_str().unwrap(),
        "--repo",
        repo.to_str().unwrap(),
        "--objective",
        "prove exact delivery",
        "--runtime",
        "codex",
    ])
    .unwrap();
    cli::run_with_transports(start, transports.clone())
        .await
        .unwrap();
    let store = MissionStore::open(&repo).await.unwrap();
    let mission_id = store.list_missions().await.unwrap().pop().unwrap();
    let proposal_path = temp.path().join("delivery-plan.json");
    std::fs::write(
        &proposal_path,
        serde_json::to_vec(&PlanProposal {
            base_revision: 0,
            requirement_changes: vec![],
            assertion_supersessions: vec![],
            plan: plan(),
        })
        .unwrap(),
    )
    .unwrap();
    for cli in [
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "plan",
            "propose",
            mission_id.as_str(),
            "--file",
            proposal_path.to_str().unwrap(),
            "--repo",
            repo.to_str().unwrap(),
        ])
        .unwrap(),
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "decide",
            mission_id.as_str(),
            "plan_proposal:mission",
            "approve",
            "--justification",
            "approve delivery proof",
            "--repo",
            repo.to_str().unwrap(),
        ])
        .unwrap(),
    ] {
        cli::run_with_transports(cli, transports.clone())
            .await
            .unwrap();
    }

    let driver_cli = |handshake: &Path| {
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "driver",
            mission_id.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "--handshake",
            handshake.to_str().unwrap(),
        ])
        .unwrap()
    };
    // The first process reaches a genuine no-handoff checkpoint and exits.
    let first_driver = tokio::spawn({
        let transports = transports.clone();
        let command = driver_cli(&temp.path().join("delivery-first.ready"));
        async move { cli::run_with_transports(command, transports).await }
    });
    entered.acquire().await.unwrap().forget();
    release.add_permits(1);
    let reached_awaiting = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let store = MissionStore::open(&repo).await.unwrap();
            let state = store.require_state(&mission_id).await.unwrap();
            if state.inflight.is_empty()
                && state.conversations.values().any(|conversation| {
                    conversation.lifecycle == lionclaw::model::ConversationLifecycle::AwaitingLead
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    if reached_awaiting.is_err() {
        panic!(
            "first driver did not reach awaiting lead: {:#?}",
            MissionStore::open(&repo)
                .await
                .unwrap()
                .require_state(&mission_id)
                .await
                .unwrap()
        );
    }
    if !first_driver.is_finished() {
        first_driver.abort();
    }
    match first_driver.await {
        Ok(Ok(code)) => assert_eq!(code, std::process::ExitCode::SUCCESS),
        Err(error) => assert!(error.is_cancelled()),
        Ok(Err(error)) => panic!("first driver failed: {error:#}"),
    }
    let awaiting_store = MissionStore::open(&repo).await.unwrap();
    let awaiting = lionclaw::engine::load_mission_view(&awaiting_store, &mission_id)
        .await
        .unwrap();
    assert_eq!(awaiting.disposition, MissionDisposition::AwaitingLead);
    let (conversation_id, conversation) = awaiting.state.conversations.iter().next().unwrap();
    let response = conversation.final_response.as_ref().unwrap();
    let response = awaiting_store.blobs().resolve(response).unwrap();
    assert!(response.starts_with("Which release target should I use?"));
    assert!(response.len() <= lionclaw::model::MAX_FINAL_RESPONSE_BYTES as usize);
    let exact_conversation_id = conversation_id.clone();
    let assignment_generation = conversation.assignment_epoch;
    let conversation_id = conversation_id.to_string();

    // Every user-facing view is parsed and rendered by the production binary
    // after a fresh MissionStore reload. They must agree on the one folded
    // awaiting-lead conversation rather than carrying observer authority.
    let status_json: serde_json::Value = serde_json::from_str(&stdout(cli_output(
        &repo,
        &["mission", "status", mission_id.as_str(), "--json"],
    )))
    .unwrap();
    let report_json: serde_json::Value = serde_json::from_str(&stdout(cli_output(
        &repo,
        &["mission", "report", mission_id.as_str(), "--json"],
    )))
    .unwrap();
    let inbox_json: serde_json::Value =
        serde_json::from_str(&stdout(cli_output(&repo, &["mission", "inbox", "--json"]))).unwrap();
    for root in [&status_json, &report_json, &inbox_json["missions"][0]] {
        let projected = projected_conversation(root, &conversation_id);
        assert_eq!(projected["lifecycle"], "awaiting_lead");
        assert_eq!(
            projected["legal_actions"],
            serde_json::json!(["mission send"])
        );
        assert_eq!(projected["runtime_resume_mode"], "canonical_reconstruction");
        let final_response = projected["final_response"].as_str().unwrap();
        assert!(final_response.starts_with("Which release target should I use?"));
        assert!(final_response.len() <= lionclaw::model::MAX_FINAL_RESPONSE_BYTES as usize);
    }
    // Activity is intentionally projected only while the durable disposition
    // is Running. AwaitingLead plus an empty inflight set is a settled exact
    // delivery boundary, so stale adapter activity must not leak into status.
    assert_eq!(status_json["activity"], serde_json::Value::Null);
    assert_eq!(
        status_json["next_actions"],
        serde_json::json!(["mission send", "mission abort"])
    );
    for args in [
        vec!["mission", "status", mission_id.as_str()],
        vec!["mission", "report", mission_id.as_str()],
        vec!["mission", "inbox"],
    ] {
        let human = stdout(cli_output(&repo, &args));
        assert!(human.contains("lifecycle=awaiting_lead"));
        assert!(human.contains("legal_actions=mission send"));
        assert!(human.contains("final response: Which release target should I use?"));
    }

    // A canonically parsed lead reply is appended after reopening the store.
    // A new driver process then reconstructs the Engine from durable authority.
    let send = |body: &'static str| {
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "send",
            "--mission-id",
            mission_id.as_str(),
            "--to",
            conversation_id.as_str(),
            "--commit",
            base.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            body,
        ])
        .unwrap()
    };
    cli::run_with_transports(
        send("Use the preserved release target."),
        transports.clone(),
    )
    .await
    .unwrap();
    // Use the real Codex provider for this one failure. Its test auth provider
    // refuses setup inside MissionProgramExecutor's interactive launch, before
    // an app-server transport exists and without disrupting OCI cleanup.
    let launch_transports = cli::MissionTransports::external(
        profiles.clone(),
        RuntimeDriverRegistry::new(
            [Arc::new(CodexRuntimeDriver) as Arc<dyn RuntimeDriverProvider>],
        ),
        RuntimeAuthRegistry::new([Arc::new(TestCodexAuth) as Arc<dyn RuntimeAuthProvider>]),
        Arc::new(ExternalOracleTransport {
            calls: oracle_calls.clone(),
        }),
    );

    let launch_driver = tokio::spawn({
        let transports = launch_transports;
        let command = driver_cli(&temp.path().join("delivery-launch-failure.ready"));
        async move { cli::run_with_transports(command, transports).await }
    });
    let active_store = MissionStore::open(&repo).await.unwrap();
    let events = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let events = active_store.load(&mission_id).await.unwrap();
            if events.iter().any(|event| {
                matches!(
                    &event.event,
                    MissionEvent::RoleRunCompleted { outcome: Err(failure), .. }
                        if failure.evidence().code.as_deref() == Some("kernel.launch")
                )
            }) {
                break events;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let events = match events {
        Ok(events) => events,
        Err(_) => panic!(
            "launch failure was not recorded; durable events: {:#?}",
            active_store.load(&mission_id).await.unwrap()
        ),
    };
    if !launch_driver.is_finished() {
        launch_driver.abort();
    }
    match launch_driver.await {
        Ok(Ok(_)) => {}
        Err(error) => assert!(error.is_cancelled()),
        Ok(Err(error)) => panic!("launch driver failed: {error:#}"),
    }
    let launch = events
        .iter()
        .position(|event| {
            matches!(
                &event.event,
                MissionEvent::RoleRunCompleted { outcome: Err(failure), .. }
                    if failure.evidence().code.as_deref() == Some("kernel.launch")
            )
        })
        .expect("production launch failure");
    let launch_failure = match &events[launch].event {
        MissionEvent::RoleRunCompleted {
            outcome: Err(failure),
            ..
        } => failure,
        _ => unreachable!("launch index identifies a failed role run"),
    };
    assert_eq!(
        launch_failure.evidence().code.as_deref(),
        Some("kernel.launch")
    );
    assert!(launch_failure
        .evidence()
        .detail
        .contains("test codex auth setup refused launch"));
    assert_eq!(
        launch_failure.evidence().configuration,
        lionclaw_runtime_api::AppliedRuntimeConfiguration::default()
    );
    assert!(launch_failure.evidence().final_response.is_empty());
    let after_launch = fold(events[..=launch].iter().cloned()).unwrap();
    let delivery = &after_launch.conversations[&exact_conversation_id];
    assert_eq!(delivery.assignment_epoch, assignment_generation);
    assert_eq!(delivery.queued.len(), 1);
    assert_eq!(
        delivery.queued[0].marker,
        lionclaw::model::DeliveryMarker::Queued
    );
    assert!(delivery.active_delivery.is_none());
    let parked = active_store.require_state(&mission_id).await.unwrap();
    assert_eq!(
        parked,
        fold(active_store.load(&mission_id).await.unwrap()).unwrap()
    );
    assert_eq!(
        parked,
        active_store
            .rebuild_cursors(&mission_id, 9_000_000)
            .await
            .expect("launch refusal snapshot-tail rebuild")
    );
    let launch_status: serde_json::Value = serde_json::from_str(&stdout(cli_output(
        &repo,
        &["mission", "status", mission_id.as_str(), "--json"],
    )))
    .unwrap();
    assert_eq!(
        launch_status["tasks"][0]["failure"]["evidence"]["code"],
        "kernel.launch"
    );
    assert!(launch_status["tasks"][0]["failure"]["evidence"]["detail"]
        .as_str()
        .unwrap()
        .contains("test codex auth setup refused launch"));
    assert_ne!(
        projected_conversation(&launch_status, &conversation_id)["lifecycle"],
        "awaiting_lead"
    );
    let launch_human = stdout(cli_output(
        &repo,
        &["mission", "status", mission_id.as_str()],
    ));
    assert!(launch_human.contains("test codex auth setup refused launch"));
    let parked_effect = parked.parked_effects.keys().next().unwrap().to_string();
    cli::run(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "continue",
            mission_id.as_str(),
            parked_effect.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "--reason",
            "retry the launch-class failure",
        ])
        .unwrap(),
    )
    .await
    .unwrap();
    let restarted_driver = tokio::spawn({
        let transports = transports.clone();
        let command = driver_cli(&temp.path().join("delivery-restarted.ready"));
        async move { cli::run_with_transports(command, transports).await }
    });
    entered.acquire().await.unwrap().forget();
    let state = active_store.require_state(&mission_id).await.unwrap();
    let delivery = state.conversations.values().next().unwrap();
    assert_eq!(delivery.queued.len(), 1);
    let active = delivery.active_delivery.as_ref().unwrap();
    let boundary = active.message_boundary;
    assert!(delivery.queued[0].sequence_no <= boundary);

    // The runner has launched with this exact immutable request. Reducer
    // fault-injection mutates this same identity; this production path proves
    // it actually reaches the active runner and stays stable as the log moves.
    let effect_id = active.effect_id.clone();
    let request = state
        .inflight
        .get(&effect_id)
        .unwrap()
        .role_request_identity()
        .unwrap();
    assert_eq!(request.message_boundary, boundary);
    assert_eq!(
        request.presented_messages,
        vec![delivery.queued[0].sequence_no]
    );
    let active_status: serde_json::Value = serde_json::from_str(&stdout(cli_output(
        &repo,
        &["mission", "status", mission_id.as_str(), "--json"],
    )))
    .unwrap();
    let active_projection = projected_conversation(&active_status, &conversation_id);
    assert_eq!(active_projection["lifecycle"], "running");
    assert_eq!(
        active_projection["legal_actions"],
        serde_json::json!(["mission status", "mission send"])
    );
    assert_eq!(
        active_projection["runtime_resume_mode"],
        "canonical_reconstruction"
    );
    assert_eq!(active_projection["queued_messages"][0]["marker"], "queued");
    assert_eq!(
        active_projection["queued_messages"][0]["references"][0]["sha"],
        base
    );
    // Recompute the disposable observer through its production projection from
    // the freshly loaded fold, then require parsed status to render that exact
    // projection without promoting it to persisted mission authority.
    let mission_dir = repo.join(".lionclaw/missions").join(mission_id.as_str());
    lionclaw::activity::publish_observed(
        &repo,
        &mission_dir,
        &state,
        lionclaw::activity::now_ms(),
        None,
    )
    .await
    .unwrap();
    let activity: lionclaw::activity::ActivityProjection =
        serde_json::from_slice(&std::fs::read(lionclaw::activity::path(&mission_dir)).unwrap())
            .unwrap();
    assert_eq!(activity.event_head, state.head);
    assert_eq!(activity.effects.len(), 1);
    assert_eq!(activity.effects[0].effect_id, effect_id.as_str());
    let refreshed_status: serde_json::Value = serde_json::from_str(&stdout(cli_output(
        &repo,
        &["mission", "status", mission_id.as_str(), "--json"],
    )))
    .unwrap();
    assert_eq!(
        refreshed_status["activity"],
        serde_json::to_value(&activity).unwrap()
    );
    let activity_path = lionclaw::activity::path(&mission_dir);
    std::fs::write(&activity_path, b"not json").unwrap();
    assert_activity_suppressed(&repo, mission_id.as_str(), "malformed projection");
    std::fs::remove_file(&activity_path).unwrap();
    assert_activity_suppressed(&repo, mission_id.as_str(), "absent projection");
    std::fs::write(
        &activity_path,
        vec![0; (lionclaw::activity::MAX_PROJECTION_BYTES + 1) as usize],
    )
    .unwrap();
    assert_activity_suppressed(&repo, mission_id.as_str(), "oversized projection");
    std::fs::remove_file(&activity_path).unwrap();
    std::fs::create_dir(&activity_path).unwrap();
    assert_activity_suppressed(&repo, mission_id.as_str(), "non-regular projection");
    std::fs::remove_dir(&activity_path).unwrap();
    std::os::unix::fs::symlink("missing-observer-target", &activity_path).unwrap();
    assert_activity_suppressed(&repo, mission_id.as_str(), "symlink projection");
    std::fs::remove_file(&activity_path).unwrap();
    for (label, field, value) in [
        ("stale version", "version", serde_json::json!(3)),
        (
            "mission mismatch",
            "mission_id",
            serde_json::json!("foreign"),
        ),
        (
            "head mismatch",
            "event_head",
            serde_json::json!(state.head - 1),
        ),
    ] {
        let mut candidate = serde_json::to_value(&activity).unwrap();
        candidate[field] = value;
        std::fs::write(&activity_path, serde_json::to_vec(&candidate).unwrap()).unwrap();
        assert_activity_suppressed(&repo, mission_id.as_str(), label);
    }
    let mut missing = activity.clone();
    missing.effects.clear();
    std::fs::write(&activity_path, serde_json::to_vec(&missing).unwrap()).unwrap();
    assert_activity_suppressed(&repo, mission_id.as_str(), "missing effect");
    for (label, ids) in [
        ("forged effect", vec!["forged-effect"]),
        ("wrong effect", vec!["wrong-effect"]),
        ("extra effect", vec![effect_id.as_str(), "extra-effect"]),
    ] {
        let mut candidate = activity.clone();
        candidate.effects = ids
            .into_iter()
            .map(|id| {
                let mut effect = activity.effects[0].clone();
                effect.effect_id = id.into();
                effect
            })
            .collect();
        std::fs::write(&activity_path, serde_json::to_vec(&candidate).unwrap()).unwrap();
        assert_activity_suppressed(&repo, mission_id.as_str(), label);
    }
    let mut duplicate = activity.clone();
    duplicate.effects.push(activity.effects[0].clone());
    std::fs::write(&activity_path, serde_json::to_vec(&duplicate).unwrap()).unwrap();
    assert_activity_suppressed(&repo, mission_id.as_str(), "duplicate effect");
    std::fs::write(&activity_path, serde_json::to_vec(&activity).unwrap()).unwrap();
    let watched_line = watch_observation(&repo, mission_id.as_str(), true, None);
    let watched_activity: serde_json::Value =
        serde_json::from_str(watched_line.lines().next().expect("watch observation")).unwrap();
    assert_eq!(
        watched_activity["activity"],
        serde_json::to_value(&activity).unwrap()
    );
    assert_eq!(
        watched_activity["conversations"],
        refreshed_status["conversations"]
    );
    assert_eq!(watched_activity["tasks"], refreshed_status["tasks"]);
    assert_eq!(
        watched_activity["next_actions"],
        refreshed_status["next_actions"]
    );
    let watched_human = watch_observation(&repo, mission_id.as_str(), false, Some("marker=queued"));
    assert!(watched_human.contains(&format!("{} ", effect_id.as_str())));
    assert!(watched_human.contains("lifecycle=running"));
    assert!(watched_human.contains("resume=canonical_reconstruction"));
    assert!(watched_human.contains("legal_actions=mission status|mission send"));
    assert!(watched_human.contains("marker=queued"));
    assert!(watched_human.contains(base.as_str()));
    let active_human = stdout(cli_output(
        &repo,
        &["mission", "status", mission_id.as_str()],
    ));
    assert!(active_human.contains("legal_actions=mission status|mission send"));
    assert!(active_human.contains("activity "));

    // This production-ingress append occurs while the request is held inside
    // the native adapter. It is beyond that request's boundary by definition.
    cli::run_with_transports(
        send("This arrived during the active turn."),
        transports.clone(),
    )
    .await
    .unwrap();
    let mid_turn = active_store.require_state(&mission_id).await.unwrap();
    let delivery = mid_turn.conversations.values().next().unwrap();
    assert_eq!(delivery.queued.len(), 2);
    assert!(delivery.queued[1].sequence_no > boundary);
    assert_eq!(
        delivery.active_delivery.as_ref().unwrap().message_boundary,
        boundary
    );
    assert!(active_store
        .load(&mission_id)
        .await
        .unwrap()
        .iter()
        .any(|event| {
            matches!(
                &event.event,
                MissionEvent::RoleRunCompleted { outcome: Err(failure), .. }
                    if failure.evidence().code.as_deref() == Some("kernel.launch")
            )
        }));

    // Invalid handoff proves delivered failure: only the message inside the
    // immutable boundary becomes PreviouslyDelivered; the arrival stays queued.
    release.add_permits(1);
    entered.acquire().await.unwrap().forget();
    let reworking = MissionStore::open(&repo)
        .await
        .unwrap()
        .require_state(&mission_id)
        .await
        .unwrap();
    let delivery = &reworking.conversations[&exact_conversation_id];
    assert_eq!(delivery.assignment_epoch, assignment_generation);
    assert_eq!(
        delivery.queued[0].marker,
        lionclaw::model::DeliveryMarker::PreviouslyDelivered
    );
    assert_eq!(
        delivery.queued[1].marker,
        lionclaw::model::DeliveryMarker::Queued
    );
    let retry_boundary = delivery.active_delivery.as_ref().unwrap().message_boundary;
    assert!(retry_boundary >= boundary);
    assert!(delivery.queued[1].sequence_no <= retry_boundary);

    // A delivered transport failure truthfully makes everything presented by
    // its request only PossiblyDelivered.
    release.add_permits(1);
    restarted_driver.await.unwrap().unwrap();
    let failed = MissionStore::open(&repo)
        .await
        .unwrap()
        .require_state(&mission_id)
        .await
        .unwrap();
    assert_eq!(
        failed.conversations[&exact_conversation_id].assignment_epoch,
        assignment_generation
    );
    assert!(failed.inflight.is_empty());
    let mut completed_effect_projection = activity.clone();
    completed_effect_projection.event_head = failed.head;
    std::fs::write(
        &activity_path,
        serde_json::to_vec(&completed_effect_projection).unwrap(),
    )
    .unwrap();
    assert_activity_suppressed(
        &repo,
        mission_id.as_str(),
        "completed effect at current head",
    );
    let failed_messages = &failed.conversations.values().next().unwrap().queued;
    assert_eq!(
        failed_messages[0].marker,
        lionclaw::model::DeliveryMarker::PreviouslyDelivered,
        "a later uncertain failure must not erase the earlier proof of delivery"
    );
    assert_eq!(
        failed_messages[1].marker,
        lionclaw::model::DeliveryMarker::PossiblyDelivered
    );
    let uncertain_status: serde_json::Value = serde_json::from_str(&stdout(cli_output(
        &repo,
        &["mission", "status", mission_id.as_str(), "--json"],
    )))
    .unwrap();
    let uncertain_report: serde_json::Value = serde_json::from_str(&stdout(cli_output(
        &repo,
        &["mission", "report", mission_id.as_str(), "--json"],
    )))
    .unwrap();
    let uncertain_inbox: serde_json::Value =
        serde_json::from_str(&stdout(cli_output(&repo, &["mission", "inbox", "--json"]))).unwrap();
    for root in [
        &uncertain_status,
        &uncertain_report,
        &uncertain_inbox["missions"][0],
    ] {
        let projected = projected_conversation(root, &conversation_id);
        assert_eq!(
            projected["queued_messages"][0]["marker"],
            "previously_delivered"
        );
        assert_eq!(
            projected["queued_messages"][1]["marker"],
            "possibly_delivered"
        );
        assert_eq!(
            projected["queued_messages"][0]["body"],
            "Use the preserved release target."
        );
        assert_eq!(
            projected["queued_messages"][0]["references"][0]["sha"],
            base
        );
        assert_eq!(projected["runtime_resume_mode"], "native_session");
        assert_eq!(
            projected["legal_actions"],
            serde_json::json!(["mission advance", "mission send"])
        );
    }
    for args in [
        vec!["mission", "status", mission_id.as_str()],
        vec!["mission", "report", mission_id.as_str()],
        vec!["mission", "inbox"],
    ] {
        let human = stdout(cli_output(&repo, &args));
        assert!(human.contains("marker=previously_delivered"));
        assert!(human.contains("marker=possibly_delivered"));
        assert!(human.contains("body=Use the preserved release target."));
        assert!(human.contains("legal_actions=mission advance|mission send"));
    }
    let failed_effect = failed.parked_effects.keys().next().unwrap().to_string();
    cli::run(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "continue",
            mission_id.as_str(),
            failed_effect.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "--reason",
            "retry the delivered transport failure",
        ])
        .unwrap(),
    )
    .await
    .unwrap();
    let interrupted_driver = tokio::spawn({
        let transports = transports.clone();
        let command = driver_cli(&temp.path().join("delivery-interrupted.ready"));
        async move { cli::run_with_transports(command, transports).await }
    });
    entered.acquire().await.unwrap().forget();

    // Drop the process while the next native turn is active. Restart recovery
    // records interruption, and the same production request path re-presents
    // the messages with the conservative marker.
    interrupted_driver.abort();
    assert!(interrupted_driver.await.unwrap_err().is_cancelled());
    let recovered_driver = tokio::spawn({
        let transports = transports.clone();
        let command = driver_cli(&temp.path().join("delivery-recovered.ready"));
        async move { cli::run_with_transports(command, transports).await }
    });
    recovered_driver.await.unwrap().unwrap();
    let recovered = MissionStore::open(&repo)
        .await
        .unwrap()
        .require_state(&mission_id)
        .await
        .unwrap();
    let delivery = &recovered.conversations[&exact_conversation_id];
    assert_eq!(delivery.assignment_epoch, assignment_generation);
    assert_eq!(
        delivery.queued[0].marker,
        lionclaw::model::DeliveryMarker::PreviouslyDelivered
    );
    assert_eq!(
        delivery.queued[1].marker,
        lionclaw::model::DeliveryMarker::PossiblyDelivered
    );
    let interrupted_effect = recovered.parked_effects.keys().next().unwrap().to_string();
    cli::run(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "continue",
            mission_id.as_str(),
            interrupted_effect.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "--reason",
            "retry the interrupted delivery",
        ])
        .unwrap(),
    )
    .await
    .unwrap();
    let completion_driver = tokio::spawn({
        let transports = transports.clone();
        let command = driver_cli(&temp.path().join("delivery-completion.ready"));
        async move { cli::run_with_transports(command, transports).await }
    });
    entered.acquire().await.unwrap().forget();
    let completing = MissionStore::open(&repo)
        .await
        .unwrap()
        .require_state(&mission_id)
        .await
        .unwrap();
    let delivery = completing.conversations.values().next().unwrap();
    let completion_boundary = delivery.active_delivery.as_ref().unwrap().message_boundary;

    cli::run_with_transports(send("Keep this for the next turn."), transports.clone())
        .await
        .unwrap();
    let before_success = MissionStore::open(&repo)
        .await
        .unwrap()
        .require_state(&mission_id)
        .await
        .unwrap();
    assert!(
        before_success.conversations.values().next().unwrap().queued[2].sequence_no
            > completion_boundary
    );
    release.add_permits(2);
    completion_driver.await.unwrap().unwrap();
    let mut completed_store = MissionStore::open(&repo).await.unwrap();
    if completed_store
        .require_state(&mission_id)
        .await
        .unwrap()
        .phase
        != (MissionPhase::Done {
            finish: FinishClass::Verified,
        })
    {
        cli::run_with_transports(
            driver_cli(&temp.path().join("delivery-closing.ready")),
            transports.clone(),
        )
        .await
        .unwrap();
        completed_store = MissionStore::open(&repo).await.unwrap();
    }
    let completed = completed_store.require_state(&mission_id).await.unwrap();
    let delivery = completed.conversations.values().next().unwrap();
    assert_eq!(delivery.queued.len(), 1);
    assert_eq!(delivery.queued[0].body, "Keep this for the next turn.");
    assert_eq!(
        delivery.queued[0].marker,
        lionclaw::model::DeliveryMarker::Undeliverable
    );
    assert!(delivery.queued[0].sequence_no > delivery.consumed_through);
    assert_eq!(
        delivery.lifecycle,
        lionclaw::model::ConversationLifecycle::Retired
    );
    assert!(delivery.active_delivery.is_none());
    assert!(completed
        .conversation_legal_actions(
            completed
                .conversations
                .keys()
                .next()
                .expect("completed conversation")
        )
        .is_empty());
    assert_eq!(
        completed.phase,
        MissionPhase::Done {
            finish: FinishClass::Verified
        }
    );
    let mut completed_effect = activity.clone();
    completed_effect.event_head = completed.head;
    std::fs::write(
        &activity_path,
        serde_json::to_vec(&completed_effect).unwrap(),
    )
    .unwrap();
    let completed_status: serde_json::Value = serde_json::from_str(&stdout(cli_output(
        &repo,
        &["mission", "status", mission_id.as_str(), "--json"],
    )))
    .unwrap();
    assert_eq!(completed_status["activity"], serde_json::Value::Null);

    assert!(turns.lock().unwrap().is_empty());
    {
        let sessions = sessions.lock().unwrap();
        assert_eq!(sessions.len(), 6);
        assert_eq!(sessions[0].0, sessions[1].0, "workspace changed on restart");
        assert_eq!(sessions[1].0, sessions[2].0, "workspace changed on repair");
        assert!(!sessions[0].1);
        assert!(
            sessions[1].1,
            "native session was not eligible after restart"
        );
        assert!(sessions[2].1, "native session was not eligible for repair");
    }
    {
        let prompts = prompts.lock().unwrap();
        assert!(prompts[1].0.contains("Use the preserved release target."));
        assert!(prompts[2]
            .0
            .contains("handoff is missing the 'schema' string"));
        assert_eq!(prompts[0].1, prompts[1].1);
        assert_eq!(prompts[1].1, prompts[2].1);
    }

    let events = completed_store.load(&mission_id).await.unwrap();
    let completions: Vec<_> = events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| {
            matches!(event.event, MissionEvent::RoleRunCompleted { .. }).then_some(index)
        })
        .collect();
    let oracle_completions = events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| match &event.event {
            MissionEvent::OracleRunCompleted {
                oracle,
                judged_sha,
                outcome: Ok(outcome),
                ..
            } => Some((index, oracle.as_str(), judged_sha.as_str(), outcome)),
            _ => None,
        })
        .collect::<Vec<_>>();
    let review_requested = events
        .iter()
        .position(|event| matches!(event.event, MissionEvent::TerminalReviewRequested { .. }))
        .unwrap();
    let review_completed = events
        .iter()
        .position(|event| matches!(event.event, MissionEvent::TerminalReviewCompleted { .. }))
        .unwrap();
    assert_eq!(completions.len(), 6);
    assert!(completions.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(oracle_completions.len(), 4);
    let calls = oracle_calls.lock().unwrap();
    assert_eq!(
        oracle_completions
            .iter()
            .map(|(_, oracle, ..)| *oracle)
            .collect::<Vec<_>>(),
        calls
            .iter()
            .map(|(oracle, _)| oracle.as_str())
            .collect::<Vec<_>>()
    );
    for (index, oracle, judged_sha, actual) in oracle_completions {
        let (exit_code, _, duration_ms) = scripted_oracle_outcome(oracle);
        assert_eq!(judged_sha, completed.current_sha);
        assert_eq!(actual.exit_code, exit_code);
        assert_eq!(actual.duration_ms, duration_ms);
        assert!(completions[5] < index && index < review_requested);
    }
    assert!(review_requested < review_completed);
}

#[test]
fn production_references_are_authoritative_bounded_and_transient() {
    std::thread::Builder::new()
        .name("production-reference-proof".into())
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(production_references_are_authoritative_bounded_and_transient_inner())
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn production_references_are_authoritative_bounded_and_transient_inner() {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    let base = initialize_repo(&repo).await;
    let fake_oci = temp.path().join("external-oci-transport");
    std::fs::write(
        &fake_oci,
        "#!/bin/sh\nif [ \"$1 $2\" = \"image inspect\" ]; then echo production-image-id; fi\nexit 0\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_oci, std::fs::Permissions::from_mode(0o755)).unwrap();
    let profiles = RuntimeProfiles::from_toml(
        &format!(
            r#"[runtimes.codex]
driver = "codex"
command = "external-codex"
native-resume = true
confinement = {{ backend = "podman", engine = "{}", read-only-rootfs = true }}
"#,
            fake_oci.display()
        ),
        temp.path(),
    )
    .unwrap();
    let mission_type_dir = temp.path().join("mission-type");
    materialize_mission_type(&mission_type_dir);
    let turns = Arc::new(Mutex::new(VecDeque::from([
        DeliveryTurn::CompleteWithOversizedReference,
        DeliveryTurn::Fail,
        DeliveryTurn::AwaitLead,
        DeliveryTurn::AwaitLead,
    ])));
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(8));
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let transports = cli::MissionTransports::external(
        profiles,
        RuntimeDriverRegistry::new([Arc::new(DeliveryProvider {
            turns: turns.clone(),
            entered: entered.clone(),
            release: release.clone(),
            sessions: Arc::new(Mutex::new(Vec::new())),
            prompts: prompts.clone(),
            launch_failures: Arc::new(Mutex::new(0)),
        }) as Arc<dyn RuntimeDriverProvider>]),
        RuntimeAuthRegistry::empty(),
        Arc::new(FailingOracleTransport),
    );
    cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "start",
            "--type",
            mission_type_dir.to_str().unwrap(),
            "--repo",
            repo.to_str().unwrap(),
            "--objective",
            "prove transient references",
            "--runtime",
            "codex",
        ])
        .unwrap(),
        transports.clone(),
    )
    .await
    .unwrap();
    let store = MissionStore::open(&repo).await.unwrap();
    let mission = store.list_missions().await.unwrap().pop().unwrap();
    let proposal_path = temp.path().join("reference-plan.json");
    std::fs::write(
        &proposal_path,
        serde_json::to_vec(&PlanProposal {
            base_revision: 0,
            requirement_changes: vec![],
            assertion_supersessions: vec![],
            plan: reference_plan(),
        })
        .unwrap(),
    )
    .unwrap();
    for command in [
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "plan",
            "propose",
            mission.as_str(),
            "--file",
            proposal_path.to_str().unwrap(),
            "--repo",
            repo.to_str().unwrap(),
        ])
        .unwrap(),
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "decide",
            mission.as_str(),
            "plan_proposal:mission",
            "approve",
            "--justification",
            "approve reference proof",
            "--repo",
            repo.to_str().unwrap(),
        ])
        .unwrap(),
    ] {
        cli::run_with_transports(command, transports.clone())
            .await
            .unwrap();
    }
    let driver = |name: &str| {
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "driver",
            mission.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "--handshake",
            temp.path().join(name).to_str().unwrap(),
        ])
        .unwrap()
    };
    for attempt in 0..4 {
        cli::run_with_transports(
            driver(&format!("references-receipt-{attempt}")),
            transports.clone(),
        )
        .await
        .unwrap();
        if !store
            .require_state(&mission)
            .await
            .unwrap()
            .authoritative_receipts
            .is_empty()
        {
            break;
        }
    }
    let failed_oracle = store.require_state(&mission).await.unwrap();
    let receipt = failed_oracle
        .authoritative_receipts
        .iter()
        .next()
        .expect("production oracle receipt")
        .clone();
    let attention = failed_oracle.open_attention.keys().next().unwrap().clone();
    cli::run(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "decide",
            mission.as_str(),
            &attention,
            "repair",
            "--justification",
            "retry to create park evidence",
            "--repo",
            repo.to_str().unwrap(),
        ])
        .unwrap(),
    )
    .await
    .unwrap();
    for attempt in 0..4 {
        cli::run_with_transports(
            driver(&format!("references-parked-{attempt}")),
            transports.clone(),
        )
        .await
        .unwrap();
        if !store
            .require_state(&mission)
            .await
            .unwrap()
            .parked_effects
            .is_empty()
        {
            break;
        }
    }
    let parked = store.require_state(&mission).await.unwrap();
    let park = parked
        .parked_effects
        .keys()
        .next()
        .expect("production failed role park evidence")
        .clone();
    let conversation = parked
        .conversations
        .iter()
        .find(|(id, conversation)| {
            conversation.task_id.as_str() == "mint-receipt"
                && parked.conversation_is_messageable(id)
        })
        .map(|(id, _)| id.clone())
        .expect("current production conversation");

    let oversized_commit = parked
        .reachable_commits
        .iter()
        .find(|sha| sha.as_str() != base)
        .expect("production artifact commit authority")
        .clone();
    let tree = stdout(
        Command::new("git")
            .current_dir(&repo)
            .args(["rev-parse", &format!("{base}^{{tree}}")])
            .output()
            .unwrap(),
    )
    .trim()
    .to_string();
    let unreachable = String::from_utf8(
        Command::new("git")
            .current_dir(&repo)
            .args([
                "commit-tree",
                tree.as_str(),
                "-m",
                "detached reference object",
            ])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_string();
    assert!(repo.join(".git/objects").exists());

    let malformed_sha = "0".repeat(40);
    for (args, expected) in [
        (
            vec!["--commit".into(), malformed_sha.clone()],
            ReferenceRejectionReason::MalformedOrForeign {
                reference: MessageReference::ReachableCommit { sha: malformed_sha },
            },
        ),
        (
            vec!["--commit".into(), unreachable.clone()],
            ReferenceRejectionReason::MalformedOrForeign {
                reference: MessageReference::ReachableCommit {
                    sha: unreachable.clone(),
                },
            },
        ),
        (
            vec!["--commit".into(), oversized_commit.to_string()],
            ReferenceRejectionReason::Oversized {
                reference: Some(MessageReference::ReachableCommit {
                    sha: oversized_commit.to_string(),
                }),
            },
        ),
        (
            vec![
                "--receipt".into(),
                receipt.to_string(),
                "--commit".into(),
                unreachable.clone(),
            ],
            ReferenceRejectionReason::MalformedOrForeign {
                reference: MessageReference::ReachableCommit {
                    sha: unreachable.clone(),
                },
            },
        ),
    ] {
        assert_reference_send_rejected(&repo, &store, &mission, &conversation, args, expected)
            .await;
    }
    let too_many = (0..=lionclaw::model::MAX_MESSAGE_REFERENCES)
        .flat_map(|_| ["--receipt".to_string(), receipt.to_string()])
        .collect();
    assert_reference_send_rejected(
        &repo,
        &store,
        &mission,
        &conversation,
        too_many,
        ReferenceRejectionReason::Oversized { reference: None },
    )
    .await;
    let aggregate = (0..lionclaw::model::MAX_MESSAGE_REFERENCES)
        .flat_map(|_| ["--receipt".to_string(), receipt.to_string()])
        .collect();
    assert_reference_send_rejected(
        &repo,
        &store,
        &mission,
        &conversation,
        aggregate,
        ReferenceRejectionReason::Oversized {
            reference: Some(MessageReference::AuthoritativeReceipt {
                effect_id: receipt.clone(),
            }),
        },
    )
    .await;

    let before_send = store.load(&mission).await.unwrap();
    cli::run(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "send",
            "--mission-id",
            mission.as_str(),
            "--to",
            conversation.as_str(),
            "--receipt",
            receipt.as_str(),
            "--park",
            park.as_str(),
            "--commit",
            base.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "inspect all authoritative references",
        ])
        .unwrap(),
    )
    .await
    .unwrap();
    let after_send = store.load(&mission).await.unwrap();
    assert_eq!(after_send.len(), before_send.len() + 1);
    cli::run(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "continue",
            mission.as_str(),
            park.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "--reason",
            "continue reference proof",
        ])
        .unwrap(),
    )
    .await
    .unwrap();
    assert_reference_send_rejected(
        &repo,
        &store,
        &mission,
        &conversation,
        vec!["--park".into(), park.to_string()],
        ReferenceRejectionReason::Stale {
            reference: MessageReference::ParkEvidence {
                effect_id: park.clone(),
            },
        },
    )
    .await;
    cli::run_with_transports(driver("references-delivered"), transports.clone())
        .await
        .unwrap();
    let prompt = prompts.lock().unwrap().last().unwrap().0.clone();
    assert!(prompt.contains(&format!("authoritative receipt {receipt}:")));
    assert!(prompt.contains("AUTHORITATIVE-RECEIPT-CONTENT"));
    assert!(prompt.contains(&format!("park evidence {park}:")));
    assert!(prompt.contains("scripted external transport failure"));
    assert!(prompt.contains(&format!("reachable commit {base}:")));
    assert!(prompt.contains("base.txt"));

    // Keep a real production effect current while exercising the parsed,
    // bounded watch path. The adapter is the registry-selected codex native
    // transport; only its completion gate is scripted.
    let mission_root = store.lionclaw_dir().join("missions").join(mission.as_str());
    let watched = capture_reference_watch(
        &repo,
        &store,
        &mission,
        &conversation,
        &transports,
        &entered,
        &release,
    )
    .await;

    let durable = serde_json::to_string(&store.load(&mission).await.unwrap()).unwrap();
    let transient_prose = [
        "authoritative receipt ",
        "park evidence ",
        "reachable commit ",
        "inspect all authoritative references\n[",
        "REJECTED-EXPANSION-PROBE",
    ];
    for transient in transient_prose {
        assert!(
            !durable.contains(transient),
            "durable expansion leaked: {transient}"
        );
    }
    let rendered_views = [
        vec!["mission", "status", mission.as_str()],
        vec!["mission", "status", mission.as_str(), "--json"],
        vec!["mission", "inbox"],
        vec!["mission", "inbox", "--json"],
        vec!["mission", "log", mission.as_str()],
        vec!["mission", "report", mission.as_str()],
        vec!["mission", "report", mission.as_str(), "--json"],
    ]
    .into_iter()
    .map(|args| stdout(cli_output(&repo, &args)))
    .collect::<Vec<_>>()
    .join("\n")
        + "\n"
        + &watched;
    for transient in transient_prose {
        assert!(
            !rendered_views.contains(transient),
            "parsed CLI view leaked transient expansion: {transient}"
        );
    }
    for path in walk_paths(&mission_root) {
        if path.is_file() {
            let bytes = std::fs::read(&path).unwrap();
            let content = String::from_utf8_lossy(&bytes);
            for transient in transient_prose {
                assert!(
                    !content.contains(transient),
                    "mission-private file {} leaked transient expansion",
                    path.display()
                );
            }
        }
    }
    // Mint foreign authority through a second real mission. Its receipt and
    // park are genuine, but mission identity keeps both out of this mission's
    // authority set.
    {
        let mut foreign_turns = turns.lock().unwrap();
        foreign_turns.clear();
        foreign_turns.extend([
            DeliveryTurn::Complete,
            DeliveryTurn::Fail,
            DeliveryTurn::AwaitLead,
        ]);
    }
    cli::run_with_transports(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "start",
            "--type",
            mission_type_dir.to_str().unwrap(),
            "--repo",
            repo.to_str().unwrap(),
            "--objective",
            "mint foreign reference authority",
            "--runtime",
            "codex",
        ])
        .unwrap(),
        transports.clone(),
    )
    .await
    .unwrap();
    let foreign = store
        .list_missions()
        .await
        .unwrap()
        .into_iter()
        .find(|candidate| candidate != &mission)
        .unwrap();
    for command in [
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "plan",
            "propose",
            foreign.as_str(),
            "--file",
            proposal_path.to_str().unwrap(),
            "--repo",
            repo.to_str().unwrap(),
        ])
        .unwrap(),
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "decide",
            foreign.as_str(),
            "plan_proposal:mission",
            "approve",
            "--justification",
            "mint foreign authority",
            "--repo",
            repo.to_str().unwrap(),
        ])
        .unwrap(),
    ] {
        cli::run_with_transports(command, transports.clone())
            .await
            .unwrap();
    }
    let foreign_driver = |name: &str| {
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "driver",
            foreign.as_str(),
            "--repo",
            repo.to_str().unwrap(),
            "--handshake",
            temp.path().join(name).to_str().unwrap(),
        ])
        .unwrap()
    };
    for attempt in 0..4 {
        cli::run_with_transports(
            foreign_driver(&format!("foreign-receipt-{attempt}")),
            transports.clone(),
        )
        .await
        .unwrap();
        if !store
            .require_state(&foreign)
            .await
            .unwrap()
            .authoritative_receipts
            .is_empty()
        {
            break;
        }
    }
    let foreign_failed = store.require_state(&foreign).await.unwrap();
    let foreign_receipt = foreign_failed
        .authoritative_receipts
        .iter()
        .next()
        .unwrap()
        .clone();
    let foreign_attention = foreign_failed.open_attention.keys().next().unwrap().clone();
    cli::run(
        cli::Cli::try_parse_from([
            "lionclaw",
            "mission",
            "decide",
            foreign.as_str(),
            foreign_attention.as_str(),
            "repair",
            "--justification",
            "mint foreign park",
            "--repo",
            repo.to_str().unwrap(),
        ])
        .unwrap(),
    )
    .await
    .unwrap();
    for attempt in 0..4 {
        cli::run_with_transports(
            foreign_driver(&format!("foreign-park-{attempt}")),
            transports.clone(),
        )
        .await
        .unwrap();
        if !store
            .require_state(&foreign)
            .await
            .unwrap()
            .parked_effects
            .is_empty()
        {
            break;
        }
    }
    let foreign_park = store
        .require_state(&foreign)
        .await
        .unwrap()
        .parked_effects
        .keys()
        .next()
        .unwrap()
        .clone();
    for (flag, identity, reference) in [
        (
            "--receipt",
            foreign_receipt.to_string(),
            MessageReference::AuthoritativeReceipt {
                effect_id: foreign_receipt.clone(),
            },
        ),
        (
            "--park",
            foreign_park.to_string(),
            MessageReference::ParkEvidence {
                effect_id: foreign_park.clone(),
            },
        ),
    ] {
        assert_reference_send_rejected(
            &repo,
            &store,
            &mission,
            &conversation,
            vec![flag.into(), identity],
            ReferenceRejectionReason::MalformedOrForeign { reference },
        )
        .await;
    }

    let receipt_blob = store
        .load(&mission)
        .await
        .unwrap()
        .into_iter()
        .find_map(|event| match event.event {
            MissionEvent::OracleRunCompleted {
                effect_id,
                outcome: Ok(success),
                ..
            } if effect_id == receipt => match success.stdout {
                PayloadRef::Blob(blob) => Some(blob),
                PayloadRef::Inline { .. } => None,
            },
            _ => None,
        })
        .expect("production receipt stored as a private blob");
    let blob_path = store
        .lionclaw_dir()
        .join("blobs/sha256")
        .join(&receipt_blob.hex[..2])
        .join(&receipt_blob.hex[2..4])
        .join(&receipt_blob.hex);
    let mut permissions = std::fs::metadata(&blob_path).unwrap().permissions();
    permissions.set_mode(0o000);
    std::fs::set_permissions(&blob_path, permissions).unwrap();
    assert_reference_send_rejected(
        &repo,
        &store,
        &mission,
        &conversation,
        vec!["--receipt".into(), receipt.to_string()],
        ReferenceRejectionReason::Unreadable {
            reference: MessageReference::AuthoritativeReceipt {
                effect_id: receipt.clone(),
            },
        },
    )
    .await;
    let mut permissions = std::fs::metadata(&blob_path).unwrap().permissions();
    permissions.set_mode(0o644);
    std::fs::set_permissions(&blob_path, permissions).unwrap();
    let mut corrupted = std::fs::read(&blob_path).unwrap();
    corrupted[0] ^= 1;
    std::fs::write(&blob_path, corrupted).unwrap();
    assert_reference_send_rejected(
        &repo,
        &store,
        &mission,
        &conversation,
        vec!["--receipt".into(), receipt.to_string()],
        ReferenceRejectionReason::InvalidContent {
            reference: MessageReference::AuthoritativeReceipt { effect_id: receipt },
        },
    )
    .await;
}
