use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::Notify;
use uuid::Uuid;

use lionclaw_runtime_api::{
    canonical_events, ExecutionOutput, NetworkGrant, RuntimeAdapter, RuntimeAuthKind,
    RuntimeConfigurationConfirmation, RuntimeEvent, RuntimeExecutionContext, RuntimeMcpServerSpec,
    RuntimeMessageLane, RuntimeNativeSessionObservation, RuntimeNativeStateAvailability,
    RuntimeProgramExecutor, RuntimeProgramSession, RuntimeProgramSpec, RuntimeProgramStdoutSender,
    RuntimeResume, RuntimeSessionHandle, RuntimeSessionReady, RuntimeSessionStartInput,
    RuntimeStateDir, RuntimeTerminalConfig, RuntimeTerminalProgramInput, RuntimeUsage,
    RuntimeUsageCostScope, TurnEvent, TurnExecution, TurnInput, TypedFailure,
    RUNTIME_STATE_VALUE_LIMIT, RUNTIME_TURN_JOURNAL_CAPACITY,
};

use super::{
    acp_permission_denial, acp_turn_events, AcpMessage, AcpRuntimeAdapter, AcpRuntimeConfig,
    ACP_SESSION_ID_STATE_FILE,
};

const TEST_PROFILE_KEY: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
use crate::client::AcpClient;
use crate::protocol::AcpSessionSelections;

fn opencode_acp_config(model: Option<String>, mode: Option<String>) -> AcpRuntimeConfig {
    AcpRuntimeConfig {
        runtime_id: "opencode".to_string(),
        executable: "opencode".to_string(),
        args: vec!["acp".to_string()],
        environment: vec![("OPENCODE_DISABLE_AUTOUPDATE".to_string(), "1".to_string())],
        model,
        mode,
        auth: None,
        terminal: RuntimeTerminalConfig::default(),
        session_id_state_file: ACP_SESSION_ID_STATE_FILE.to_string(),
        default_working_dir: "/workspace".to_string(),
    }
}

fn runtime_not_ready() -> RuntimeSessionReady {
    RuntimeSessionReady::not_ready()
}

#[tokio::test]
async fn acp_adapter_preserves_typed_launch_refusal() {
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(None, None));
    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            resume: RuntimeResume::Reconstruct,
        })
        .expect("start ACP session");
    let (journal, _journal_rx) = tokio::sync::mpsc::channel(4);
    let error = adapter
        .turn(
            TurnExecution {
                input: TurnInput {
                    runtime_session_id: handle.runtime_session_id,
                    prompt: "launch refusal probe".into(),
                },
                context: RuntimeExecutionContext {
                    network: NetworkGrant::Deny,
                    working_dir: None,
                    environment: Vec::new(),
                    runtime_state: None,
                    runtime_path_projections: Vec::new(),
                    mcp_servers: Vec::new(),
                },
                executor: Box::new(LaunchRefusingAcpProgramExecutor),
            },
            journal,
        )
        .await
        .expect_err("ACP setup refusal must fail the turn");
    let failure = error
        .downcast_ref::<TypedFailure>()
        .expect("ACP adapter must preserve shared typed launch evidence");

    assert_eq!(failure.evidence().code.as_deref(), Some("kernel.launch"));
    assert_eq!(
        failure.evidence().detail,
        "ACP confinement setup refused launch"
    );
}

fn runtime_state(runtime_state_root: PathBuf) -> RuntimeStateDir {
    let state = RuntimeStateDir::new(&runtime_state_root, &runtime_state_root, TEST_PROFILE_KEY)
        .expect("test-owned runtime state must be rooted");
    std::fs::create_dir_all(state.control_path()).expect("create test runtime control");
    std::fs::create_dir_all(state.path()).expect("create test runtime profile state");
    state
}

fn runtime_state_value_path(runtime_state_root: &Path, file_name: &str) -> PathBuf {
    RuntimeStateDir::new(runtime_state_root, runtime_state_root, TEST_PROFILE_KEY)
        .expect("test-owned runtime state must be rooted")
        .path()
        .join(file_name)
}

fn write_acp_session_id(runtime_state: &RuntimeStateDir, session_id: &str) {
    std::fs::write(
        runtime_state.path().join(ACP_SESSION_ID_STATE_FILE),
        format!("{session_id}\n"),
    )
    .expect("write ACP session id");
}

fn assert_acp_session_id(runtime_state_root: &Path, expected: &str) {
    assert_eq!(
        std::fs::read_to_string(runtime_state_value_path(
            runtime_state_root,
            ACP_SESSION_ID_STATE_FILE,
        ))
        .expect("read durable ACP session id"),
        format!("{expected}\n")
    );
}

fn mark_runtime_ready(runtime_state: &RuntimeStateDir) -> RuntimeSessionReady {
    lionclaw_runtime_api::begin_runtime_session_attempt(runtime_state)
        .expect("begin marker setup")
        .commit(RuntimeNativeSessionObservation::Reconstructed {
            state: RuntimeNativeStateAvailability::Reopenable,
        })
        .expect("write runtime ready marker");
    lionclaw_runtime_api::begin_runtime_session_attempt(runtime_state)
        .expect("consume runtime ready marker")
        .previous_ready()
}

fn opencode_initialize_response(id: u64) -> String {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "protocolVersion": 1,
            "agentCapabilities": {
                "loadSession": true,
                "mcpCapabilities": {
                    "http": true,
                    "sse": true,
                },
                "promptCapabilities": {
                    "embeddedContext": true,
                    "image": true,
                },
                "sessionCapabilities": {
                    "close": {},
                    "fork": {},
                    "list": {},
                    "resume": {},
                },
            },
            "agentInfo": {
                "name": "OpenCode",
                "version": "1.17.9",
            },
        },
    })
    .to_string()
}

fn resume_only_initialize_response(id: u64) -> String {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "protocolVersion": 1,
            "agentCapabilities": {
                "sessionCapabilities": {
                    "resume": {},
                },
            },
            "agentInfo": {
                "name": "ResumeOnly",
                "version": "1.0.0",
            },
        },
    })
    .to_string()
}

fn load_only_initialize_response(id: u64) -> String {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "protocolVersion": 1,
            "agentCapabilities": {
                "loadSession": true,
            },
            "agentInfo": {
                "name": "LoadOnly",
                "version": "1.0.0",
            },
        },
    })
    .to_string()
}

fn no_reopen_initialize_response(id: u64) -> String {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "protocolVersion": 1,
            "agentInfo": {
                "name": "NoResume",
                "version": "1.0.0",
            },
        },
    })
    .to_string()
}

#[derive(Debug, Deserialize)]
struct AcpFixture {
    raw_in: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct AcpConfigOptionsFixture {
    raw_out: Vec<String>,
    raw_in: Vec<String>,
}

#[derive(Debug, Default)]
struct FakeAcpProgramState {
    sent: Vec<Value>,
}

#[derive(Debug)]
struct FakeAcpProgramExecutor {
    inbound: VecDeque<String>,
    expected_auth: Option<RuntimeAuthKind>,
    state: Arc<Mutex<FakeAcpProgramState>>,
}

#[derive(Debug)]
struct LaunchRefusingAcpProgramExecutor;

#[async_trait::async_trait]
impl RuntimeProgramExecutor for LaunchRefusingAcpProgramExecutor {
    async fn execute_streaming(
        &mut self,
        _program: RuntimeProgramSpec,
        _stdout: RuntimeProgramStdoutSender,
    ) -> anyhow::Result<ExecutionOutput> {
        unreachable!("ACP requires an interactive program")
    }

    async fn execute_captured(
        &mut self,
        _program: RuntimeProgramSpec,
    ) -> anyhow::Result<ExecutionOutput> {
        unreachable!("ACP requires an interactive program")
    }

    async fn spawn(
        &mut self,
        _program: RuntimeProgramSpec,
    ) -> anyhow::Result<Box<dyn RuntimeProgramSession>> {
        Err(anyhow::Error::new(TypedFailure::permanent(
            "kernel.launch",
            "ACP confinement setup refused launch",
        )))
    }
}

#[async_trait::async_trait]
impl RuntimeProgramExecutor for FakeAcpProgramExecutor {
    async fn execute_streaming(
        &mut self,
        _program: RuntimeProgramSpec,
        _stdout: RuntimeProgramStdoutSender,
    ) -> anyhow::Result<ExecutionOutput> {
        unreachable!("ACP driver should spawn an interactive program")
    }

    async fn execute_captured(
        &mut self,
        _program: RuntimeProgramSpec,
    ) -> anyhow::Result<ExecutionOutput> {
        unreachable!("ACP driver should spawn an interactive program")
    }

    async fn spawn(
        &mut self,
        program: RuntimeProgramSpec,
    ) -> anyhow::Result<Box<dyn RuntimeProgramSession>> {
        assert_eq!(program.executable, "opencode");
        assert_eq!(program.args, vec!["acp".to_string()]);
        assert_eq!(
            program.environment,
            vec![("OPENCODE_DISABLE_AUTOUPDATE".to_string(), "1".to_string())]
        );
        assert!(program.stdin.is_empty());
        assert_eq!(program.auth, self.expected_auth);
        Ok(Box::new(FakeAcpProgramSession {
            inbound: std::mem::take(&mut self.inbound),
            output: ExecutionOutput {
                exit_code: Some(0),
                ..ExecutionOutput::default()
            },
            state: Arc::clone(&self.state),
        }))
    }
}

#[derive(Debug)]
struct FakeAcpProgramSession {
    inbound: VecDeque<String>,
    output: ExecutionOutput,
    state: Arc<Mutex<FakeAcpProgramState>>,
}

#[async_trait::async_trait]
impl RuntimeProgramSession for FakeAcpProgramSession {
    async fn write_line(&mut self, line: &str) -> anyhow::Result<()> {
        let value = serde_json::from_str::<Value>(line).expect("driver writes JSON-RPC");
        self.state.lock().expect("fake ACP state").sent.push(value);
        Ok(())
    }

    async fn read_line(&mut self) -> anyhow::Result<Option<String>> {
        Ok(self.inbound.pop_front())
    }

    async fn shutdown(self: Box<Self>) -> anyhow::Result<ExecutionOutput> {
        Ok(self.output)
    }
}

#[derive(Debug)]
struct ReadFailingAcpProgramExecutor {
    inbound: VecDeque<String>,
    state: Arc<Mutex<FakeAcpProgramState>>,
}

#[async_trait::async_trait]
impl RuntimeProgramExecutor for ReadFailingAcpProgramExecutor {
    async fn execute_streaming(
        &mut self,
        _program: RuntimeProgramSpec,
        _stdout: RuntimeProgramStdoutSender,
    ) -> anyhow::Result<ExecutionOutput> {
        unreachable!("ACP driver should spawn an interactive program")
    }

    async fn execute_captured(
        &mut self,
        _program: RuntimeProgramSpec,
    ) -> anyhow::Result<ExecutionOutput> {
        unreachable!("ACP driver should spawn an interactive program")
    }

    async fn spawn(
        &mut self,
        _program: RuntimeProgramSpec,
    ) -> anyhow::Result<Box<dyn RuntimeProgramSession>> {
        Ok(Box::new(ReadFailingAcpProgramSession {
            inbound: std::mem::take(&mut self.inbound),
            state: Arc::clone(&self.state),
        }))
    }
}

#[derive(Debug)]
struct ReadFailingAcpProgramSession {
    inbound: VecDeque<String>,
    state: Arc<Mutex<FakeAcpProgramState>>,
}

#[async_trait::async_trait]
impl RuntimeProgramSession for ReadFailingAcpProgramSession {
    async fn write_line(&mut self, line: &str) -> anyhow::Result<()> {
        let value = serde_json::from_str::<Value>(line).expect("driver writes JSON-RPC");
        self.state.lock().expect("fake ACP state").sent.push(value);
        Ok(())
    }

    async fn read_line(&mut self) -> anyhow::Result<Option<String>> {
        match self.inbound.pop_front() {
            Some(line) => Ok(Some(line)),
            None => Err(anyhow::anyhow!("simulated ACP transport read failure")),
        }
    }

    async fn shutdown(self: Box<Self>) -> anyhow::Result<ExecutionOutput> {
        Ok(ExecutionOutput {
            exit_code: Some(1),
            ..ExecutionOutput::default()
        })
    }
}

#[derive(Debug)]
struct CancelableAcpProgramExecutor {
    state: Arc<CancelableAcpProgramState>,
}

#[derive(Debug)]
struct CancelableAcpProgramState {
    inbound: Mutex<VecDeque<String>>,
    sent: Mutex<Vec<Value>>,
    notify: Notify,
    shutdown: AtomicBool,
}

impl CancelableAcpProgramState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inbound: Mutex::new(VecDeque::from([
                opencode_initialize_response(1),
                r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"ses_cancel","configOptions":[]}}"#.to_string(),
            ])),
            sent: Mutex::new(Vec::new()),
            notify: Notify::new(),
            shutdown: AtomicBool::new(false),
        })
    }

    fn sent_methods(&self) -> Vec<String> {
        self.sent
            .lock()
            .expect("cancelable ACP sent")
            .iter()
            .filter_map(|message| {
                message
                    .get("method")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .collect()
    }

    fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }
}

#[async_trait::async_trait]
impl RuntimeProgramExecutor for CancelableAcpProgramExecutor {
    async fn execute_streaming(
        &mut self,
        _program: RuntimeProgramSpec,
        _stdout: RuntimeProgramStdoutSender,
    ) -> anyhow::Result<ExecutionOutput> {
        unreachable!("ACP driver should spawn an interactive program")
    }

    async fn execute_captured(
        &mut self,
        _program: RuntimeProgramSpec,
    ) -> anyhow::Result<ExecutionOutput> {
        unreachable!("ACP driver should spawn an interactive program")
    }

    async fn spawn(
        &mut self,
        _program: RuntimeProgramSpec,
    ) -> anyhow::Result<Box<dyn RuntimeProgramSession>> {
        Ok(Box::new(CancelableAcpProgramSession {
            state: Arc::clone(&self.state),
        }))
    }
}

#[derive(Debug)]
struct CancelableAcpProgramSession {
    state: Arc<CancelableAcpProgramState>,
}

#[async_trait::async_trait]
impl RuntimeProgramSession for CancelableAcpProgramSession {
    async fn write_line(&mut self, line: &str) -> anyhow::Result<()> {
        let value = serde_json::from_str::<Value>(line).expect("driver writes JSON-RPC");
        if value.get("method").and_then(Value::as_str) == Some("session/cancel") {
            self.state
                .inbound
                .lock()
                .expect("cancelable ACP inbound")
                .push_back(
                    r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"cancelled","_meta":{}}}"#
                        .to_string(),
                );
            self.state.notify.notify_waiters();
        }
        self.state
            .sent
            .lock()
            .expect("cancelable ACP sent")
            .push(value);
        Ok(())
    }

    async fn read_line(&mut self) -> anyhow::Result<Option<String>> {
        loop {
            if let Some(line) = self
                .state
                .inbound
                .lock()
                .expect("cancelable ACP inbound")
                .pop_front()
            {
                return Ok(Some(line));
            }
            self.state.notify.notified().await;
        }
    }

    async fn shutdown(self: Box<Self>) -> anyhow::Result<ExecutionOutput> {
        self.state.shutdown.store(true, Ordering::Release);
        Ok(ExecutionOutput {
            exit_code: Some(0),
            ..ExecutionOutput::default()
        })
    }
}

fn opencode_acp_fixture() -> AcpFixture {
    serde_json::from_str(include_str!(
        "../tests/fixtures/opencode_acp_success_1_17_9.json"
    ))
    .expect("OpenCode ACP fixture JSON")
}

fn opencode_acp_config_options_fixture() -> AcpConfigOptionsFixture {
    serde_json::from_str(include_str!(
        "../tests/fixtures/opencode_acp_config_options_1_17_9.json"
    ))
    .expect("OpenCode ACP config-options fixture JSON")
}

fn project_opencode_acp_fixture_events() -> Vec<RuntimeEvent> {
    opencode_acp_fixture()
        .raw_in
        .iter()
        .flat_map(|raw| {
            let value = serde_json::from_str(raw).expect("fixture raw JSON-RPC line");
            let message = AcpMessage { value };
            acp_turn_events(&message)
        })
        .map(TurnEvent::into_event)
        .collect()
}

fn acp_driver_context(runtime_state_root: PathBuf) -> RuntimeExecutionContext {
    RuntimeExecutionContext {
        network: NetworkGrant::allow_single("api.openai.com", 443).unwrap(),
        working_dir: None,
        environment: Vec::new(),
        runtime_state: Some(runtime_state(runtime_state_root)),
        runtime_path_projections: Vec::new(),
        mcp_servers: Vec::new(),
    }
}

async fn start_ready_acp_session(
    adapter: &AcpRuntimeAdapter,
    runtime_state: RuntimeStateDir,
) -> RuntimeSessionHandle {
    let ready = mark_runtime_ready(&runtime_state);
    adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            resume: RuntimeResume::Native {
                state: runtime_state,
                ready,
            },
        })
        .expect("start ready ACP session")
}

fn assert_native_observation(
    adapter: &AcpRuntimeAdapter,
    handle: &RuntimeSessionHandle,
    expected: Option<RuntimeNativeSessionObservation>,
) {
    assert_eq!(
        adapter
            .native_session_observation(handle)
            .expect("native session observation"),
        expected
    );
}

#[test]
fn opencode_acp_fixture_projects_to_canonical_runtime_events() {
    let events = project_opencode_acp_fixture_events();

    assert_eq!(
        events,
        vec![
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Reasoning,
                text: "The".to_string(),
            },
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Reasoning,
                text: " user".to_string(),
            },
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Reasoning,
                text: " is".to_string(),
            },
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Reasoning,
                text: " asking".to_string(),
            },
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Reasoning,
                text: " me".to_string(),
            },
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Reasoning,
                text: " to".to_string(),
            },
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Reasoning,
                text: " reply".to_string(),
            },
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Reasoning,
                text: " with".to_string(),
            },
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Reasoning,
                text: " exactly".to_string(),
            },
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Reasoning,
                text: " \"".to_string(),
            },
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Reasoning,
                text: "OK".to_string(),
            },
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Reasoning,
                text: "\".".to_string(),
            },
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text: "OK".to_string(),
            },
        ]
    );
}

#[test]
fn current_mode_update_projects_observed_configuration() {
    let events = acp_turn_events(&AcpMessage {
        value: json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "ses_observed",
                "update": {
                    "sessionUpdate": "current_mode_update",
                    "currentModeId": "plan"
                }
            }
        }),
    })
    .into_iter()
    .map(TurnEvent::into_event)
    .collect::<Vec<_>>();

    assert_eq!(
        events,
        vec![RuntimeEvent::Configuration {
            configuration: lionclaw_runtime_api::AppliedRuntimeConfiguration {
                applied_mode: Some("plan".to_string()),
                mode_confirmation: Some(RuntimeConfigurationConfirmation::Observed),
                ..Default::default()
            }
        }]
    );

    let legacy_events = acp_turn_events(&AcpMessage {
        value: json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "ses_observed",
                "update": {
                    "sessionUpdate": "current_mode_update",
                    "modeId": "build"
                }
            }
        }),
    })
    .into_iter()
    .map(TurnEvent::into_event)
    .collect::<Vec<_>>();

    assert_eq!(
        legacy_events,
        vec![RuntimeEvent::Configuration {
            configuration: lionclaw_runtime_api::AppliedRuntimeConfiguration {
                applied_mode: Some("build".to_string()),
                mode_confirmation: Some(RuntimeConfigurationConfirmation::Observed),
                ..Default::default()
            }
        }]
    );
}

#[test]
fn malformed_current_mode_update_is_ignored() {
    let missing = acp_turn_events(&AcpMessage {
        value: json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "ses_observed",
                "update": {
                    "sessionUpdate": "current_mode_update"
                }
            }
        }),
    });
    let empty = acp_turn_events(&AcpMessage {
        value: json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "ses_observed",
                "update": {
                    "sessionUpdate": "current_mode_update",
                    "currentModeId": ""
                }
            }
        }),
    });

    assert!(missing.is_empty());
    assert!(empty.is_empty());
}

#[test]
fn opencode_acp_config_options_fixture_pins_model_and_mode_protocol() {
    let fixture = opencode_acp_config_options_fixture();
    let raw_out = fixture
        .raw_out
        .iter()
        .map(|raw| serde_json::from_str::<Value>(raw).expect("fixture raw_out JSON"))
        .collect::<Vec<_>>();

    assert_eq!(
        raw_out
            .iter()
            .filter_map(|message| message.get("method").and_then(Value::as_str))
            .collect::<Vec<_>>(),
        vec![
            "initialize",
            "session/new",
            "session/set_config_option",
            "session/set_config_option",
        ]
    );
    assert_eq!(raw_out[2]["params"]["configId"], json!("model"));
    assert_eq!(
        raw_out[2]["params"]["value"],
        json!("openai/gpt-5.3-codex-spark")
    );
    assert_eq!(raw_out[3]["params"]["configId"], json!("mode"));
    assert_eq!(raw_out[3]["params"]["value"], json!("plan"));

    let responses = fixture
        .raw_in
        .iter()
        .map(|raw| serde_json::from_str::<Value>(raw).expect("fixture raw_in JSON"))
        .collect::<Vec<_>>();
    let model_response = acp_response_by_id(&responses, 3).expect("model response");
    let mode_response = acp_response_by_id(&responses, 4).expect("mode response");
    assert_eq!(
        acp_config_current_value(model_response, "model"),
        Some("openai/gpt-5.3-codex-spark")
    );
    assert_eq!(
        acp_config_current_value(mode_response, "mode"),
        Some("plan")
    );
}

#[tokio::test]
async fn advertised_first_class_model_and_mode_are_applied_by_typed_methods() {
    let state = Arc::new(Mutex::new(FakeAcpProgramState::default()));
    let session = FakeAcpProgramSession {
        inbound: VecDeque::from([
            r#"{"jsonrpc":"2.0","id":1,"result":{"models":{"currentModelId":"openrouter:gpt-5.5","availableModels":[{"modelId":"openrouter:gpt-5.5","name":"gpt-5.5"}]}}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":2,"result":{"modes":{"currentModeId":"dont_ask","availableModes":[{"id":"dont_ask","name":"Don't Ask"}]}}}"#.to_string(),
        ]),
        output: ExecutionOutput::default(),
        state: Arc::clone(&state),
    };
    let mut client = AcpClient::new(Box::new(session));
    let selections = AcpSessionSelections::from_session_result(&json!({
        "models": {
            "currentModelId": "openrouter:old",
            "availableModels": [
                {"modelId": "openrouter:gpt-5.5", "name": "gpt-5.5"}
            ]
        },
        "modes": {
            "currentModeId": "default",
            "availableModes": [
                {"id": "dont_ask", "name": "Don't Ask"}
            ]
        },
        "configOptions": []
    }));
    let applied = client
        .configure_session(
            &opencode_acp_config(Some("gpt-5.5".into()), Some("dont_ask".into())),
            "session-1",
            &selections,
        )
        .await
        .expect("typed selections apply");

    assert_eq!(applied.requested_model.as_deref(), Some("gpt-5.5"));
    assert_eq!(applied.applied_model.as_deref(), Some("openrouter:gpt-5.5"));
    assert_eq!(
        applied.model_confirmation,
        Some(RuntimeConfigurationConfirmation::Observed)
    );
    assert_eq!(applied.applied_mode.as_deref(), Some("dont_ask"));
    assert_eq!(
        applied.mode_confirmation,
        Some(RuntimeConfigurationConfirmation::Observed)
    );
    let sent = state.lock().unwrap().sent.clone();
    assert_eq!(sent[0]["method"], "session/set_model");
    assert_eq!(sent[0]["params"]["modelId"], "openrouter:gpt-5.5");
    assert_eq!(sent[1]["method"], "session/set_mode");
    assert_eq!(sent[1]["params"]["modeId"], "dont_ask");
}

#[tokio::test]
async fn empty_first_class_setter_result_is_acknowledged_without_stale_selection() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(None, Some("dont_ask".into())));
    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            resume: RuntimeResume::Native {
                state: runtime_state(runtime_state_root.clone()),
                ready: runtime_not_ready(),
            },
        })
        .expect("start");
    let fake_state = Arc::new(Mutex::new(FakeAcpProgramState::default()));
    let executor = FakeAcpProgramExecutor {
        inbound: VecDeque::from([
            opencode_initialize_response(1),
            r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"ses_mode","modes":{"currentModeId":"default","availableModes":[{"id":"default","name":"Default"},{"id":"dont_ask","name":"Don't Ask"}]}}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":3,"result":{}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":4,"result":{"stopReason":"end_turn","_meta":{}}}"#.to_string(),
        ]),
        expected_auth: None,
        state: Arc::clone(&fake_state),
    };
    let (journal_tx, _journal_rx) = tokio::sync::mpsc::channel(RUNTIME_TURN_JOURNAL_CAPACITY);

    let result = adapter
        .turn(
            TurnExecution {
                input: TurnInput {
                    runtime_session_id: handle.runtime_session_id,
                    prompt: "hello".to_string(),
                },
                context: acp_driver_context(runtime_state_root),
                executor: Box::new(executor),
            },
            journal_tx,
        )
        .await
        .expect("empty first-class setter result is a successful acknowledgement");

    assert_eq!(
        result.configuration.requested_mode.as_deref(),
        Some("dont_ask")
    );
    assert_eq!(
        result.configuration.applied_mode.as_deref(),
        Some("dont_ask")
    );
    assert_eq!(
        result.configuration.mode_confirmation,
        Some(RuntimeConfigurationConfirmation::Acknowledged)
    );
    let sent = fake_state.lock().expect("fake ACP state").sent.clone();
    assert_eq!(sent[2]["method"], "session/set_mode");
    assert_eq!(sent[2]["params"]["modeId"], "dont_ask");
}

#[tokio::test]
async fn first_class_setter_preserves_intervening_observed_selection() {
    let state = Arc::new(Mutex::new(FakeAcpProgramState::default()));
    let session = FakeAcpProgramSession {
        inbound: VecDeque::from([
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"session-1","update":{"sessionUpdate":"current_mode_update","currentModeId":"dont_ask"}}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":1,"result":{}}"#.to_string(),
        ]),
        output: ExecutionOutput::default(),
        state,
    };
    let mut client = AcpClient::new(Box::new(session));
    let selections = AcpSessionSelections::from_session_result(&json!({
        "modes": {
            "currentModeId": "default",
            "availableModes": [
                {"id": "default", "name": "Default"},
                {"id": "dont_ask", "name": "Don't Ask"}
            ]
        }
    }));

    let applied = client
        .configure_session(
            &opencode_acp_config(None, Some("dont_ask".into())),
            "session-1",
            &selections,
        )
        .await
        .expect("intervening current-mode update is retained");

    assert_eq!(applied.applied_mode.as_deref(), Some("dont_ask"));
    assert_eq!(
        applied.mode_confirmation,
        Some(RuntimeConfigurationConfirmation::Observed)
    );
}

#[tokio::test]
async fn advertised_current_model_is_recorded_without_requested_model() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(None, None));
    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            resume: RuntimeResume::Native {
                state: runtime_state(runtime_state_root.clone()),
                ready: runtime_not_ready(),
            },
        })
        .expect("start");
    let fake_state = Arc::new(Mutex::new(FakeAcpProgramState::default()));
    let executor = FakeAcpProgramExecutor {
        inbound: VecDeque::from([
            opencode_initialize_response(1),
            r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"ses_model","configOptions":[{"id":"model","currentValue":"opencode/big-pickle","options":[{"value":"opencode/big-pickle"}]}]}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn","_meta":{}}}"#.to_string(),
        ]),
        expected_auth: None,
        state: Arc::clone(&fake_state),
    };
    let (journal_tx, mut journal_rx) = tokio::sync::mpsc::channel(RUNTIME_TURN_JOURNAL_CAPACITY);

    let result = adapter
        .turn(
            TurnExecution {
                input: TurnInput {
                    runtime_session_id: handle.runtime_session_id,
                    prompt: "hello".to_string(),
                },
                context: acp_driver_context(runtime_state_root),
                executor: Box::new(executor),
            },
            journal_tx,
        )
        .await
        .expect("ACP turn");

    assert_eq!(result.configuration.requested_model, None);
    assert_eq!(
        result.configuration.applied_model.as_deref(),
        Some("opencode/big-pickle")
    );
    assert_eq!(
        result.configuration.model_confirmation,
        Some(RuntimeConfigurationConfirmation::Observed)
    );
    assert_eq!(result.runtime_usage, RuntimeUsage::NotReported);

    let mut journal = Vec::new();
    while let Some(record) = journal_rx.recv().await {
        journal.push(record);
    }
    assert_eq!(
        canonical_events(&journal).cloned().collect::<Vec<_>>(),
        vec![
            RuntimeEvent::Configuration {
                configuration: result.configuration.clone(),
            },
            RuntimeEvent::Done,
        ]
    );
    let sent = fake_state.lock().expect("fake ACP state").sent.clone();
    assert_eq!(
        sent.iter()
            .filter_map(|message| message.get("method").and_then(Value::as_str))
            .collect::<Vec<_>>(),
        vec!["initialize", "session/new", "session/prompt"]
    );
}

#[tokio::test]
async fn acp_turn_updates_advertised_model_from_runtime_notification() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(None, None));
    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            resume: RuntimeResume::Native {
                state: runtime_state(runtime_state_root.clone()),
                ready: runtime_not_ready(),
            },
        })
        .expect("start");
    let executor = FakeAcpProgramExecutor {
        inbound: VecDeque::from([
            opencode_initialize_response(1),
            r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"ses_model","configOptions":[{"id":"model","currentValue":"opencode/old","options":[{"value":"opencode/old"},{"value":"opencode/new"}]}]}}"#.to_string(),
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_model","update":{"sessionUpdate":"config_option_update","configOptions":[{"id":"model","currentValue":"opencode/new","options":[{"value":"opencode/old"},{"value":"opencode/new"}]}]}}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn","_meta":{}}}"#.to_string(),
        ]),
        expected_auth: None,
        state: Arc::new(Mutex::new(FakeAcpProgramState::default())),
    };
    let (journal_tx, mut journal_rx) = tokio::sync::mpsc::channel(RUNTIME_TURN_JOURNAL_CAPACITY);

    let result = adapter
        .turn(
            TurnExecution {
                input: TurnInput {
                    runtime_session_id: handle.runtime_session_id,
                    prompt: "hello".to_string(),
                },
                context: acp_driver_context(runtime_state_root),
                executor: Box::new(executor),
            },
            journal_tx,
        )
        .await
        .expect("ACP turn");

    assert_eq!(
        result.configuration.applied_model.as_deref(),
        Some("opencode/new")
    );
    assert_eq!(
        result.configuration.model_confirmation,
        Some(RuntimeConfigurationConfirmation::Observed)
    );

    let mut configuration_events = Vec::new();
    while let Some(record) = journal_rx.recv().await {
        if let RuntimeEvent::Configuration { configuration } = record.into_event() {
            configuration_events.push(configuration);
        }
    }
    assert_eq!(
        configuration_events
            .iter()
            .map(|configuration| configuration.applied_model.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("opencode/old"), Some("opencode/new")]
    );
}

#[tokio::test]
async fn requested_mode_drift_from_runtime_notification_fails_with_observed_evidence() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(None, Some("plan".into())));
    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            resume: RuntimeResume::Native {
                state: runtime_state(runtime_state_root.clone()),
                ready: runtime_not_ready(),
            },
        })
        .expect("start");
    let executor = FakeAcpProgramExecutor {
        inbound: VecDeque::from([
            opencode_initialize_response(1),
            r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"ses_mode","modes":{"currentModeId":"build","availableModes":[{"id":"plan","name":"Plan"},{"id":"build","name":"Build"}]}}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":3,"result":{"modes":{"currentModeId":"plan","availableModes":[{"id":"plan","name":"Plan"},{"id":"build","name":"Build"}]}}}"#.to_string(),
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_mode","update":{"sessionUpdate":"current_mode_update","currentModeId":"build"}}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":4,"result":{"stopReason":"end_turn","_meta":{}}}"#.to_string(),
        ]),
        expected_auth: None,
        state: Arc::new(Mutex::new(FakeAcpProgramState::default())),
    };
    let (journal_tx, _journal_rx) = tokio::sync::mpsc::channel(RUNTIME_TURN_JOURNAL_CAPACITY);

    let error = adapter
        .turn(
            TurnExecution {
                input: TurnInput {
                    runtime_session_id: handle.runtime_session_id,
                    prompt: "hello".to_string(),
                },
                context: acp_driver_context(runtime_state_root),
                executor: Box::new(executor),
            },
            journal_tx,
        )
        .await
        .expect_err("requested mode drift must fail");
    let failure = error
        .downcast_ref::<TypedFailure>()
        .expect("configuration drift is typed");

    assert_eq!(
        failure.evidence().code.as_deref(),
        Some("acp.configuration_drift")
    );
    assert!(failure.evidence().detail.contains("mode requested 'plan'"));
    assert_eq!(
        failure.evidence().configuration.requested_mode.as_deref(),
        Some("plan")
    );
    assert_eq!(
        failure.evidence().configuration.applied_mode.as_deref(),
        Some("build")
    );
    assert_eq!(
        failure.evidence().configuration.mode_confirmation,
        Some(RuntimeConfigurationConfirmation::Observed)
    );
}

#[tokio::test]
async fn partial_configuration_failure_preserves_requested_and_observed_evidence() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(
        Some("gpt-5".into()),
        Some("plan".into()),
    ));
    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            resume: RuntimeResume::Native {
                state: runtime_state(runtime_state_root.clone()),
                ready: runtime_not_ready(),
            },
        })
        .expect("start");
    let executor = FakeAcpProgramExecutor {
        inbound: VecDeque::from([
            opencode_initialize_response(1),
            r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"ses_partial","configOptions":[{"id":"model","currentValue":"old","options":[{"value":"gpt-5"}]},{"id":"mode","currentValue":"build","options":[{"value":"plan"},{"value":"build"}]}]}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":3,"result":{"configOptions":[{"id":"model","currentValue":"gpt-5"}]}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":4,"result":{"configOptions":[{"id":"mode","currentValue":"build"}]}}"#.to_string(),
        ]),
        expected_auth: None,
        state: Arc::new(Mutex::new(FakeAcpProgramState::default())),
    };
    let (journal_tx, _journal_rx) = tokio::sync::mpsc::channel(RUNTIME_TURN_JOURNAL_CAPACITY);

    let error = adapter
        .turn(
            TurnExecution {
                input: TurnInput {
                    runtime_session_id: handle.runtime_session_id,
                    prompt: "never reached".to_string(),
                },
                context: acp_driver_context(runtime_state_root),
                executor: Box::new(executor),
            },
            journal_tx,
        )
        .await
        .expect_err("mode mismatch must fail during configuration");
    let failure = error
        .downcast_ref::<TypedFailure>()
        .expect("configuration failure is typed");

    assert_eq!(failure.evidence().code.as_deref(), Some("acp.runtime"));
    assert!(failure
        .evidence()
        .detail
        .contains("applied mode 'build' instead of requested 'plan'"));
    assert_eq!(
        failure.evidence().configuration.requested_model.as_deref(),
        Some("gpt-5")
    );
    assert_eq!(
        failure.evidence().configuration.applied_model.as_deref(),
        Some("gpt-5")
    );
    assert_eq!(
        failure.evidence().configuration.requested_mode.as_deref(),
        Some("plan")
    );
    assert_eq!(
        failure.evidence().configuration.applied_mode.as_deref(),
        Some("build")
    );
    assert_eq!(failure.evidence().runtime_usage, RuntimeUsage::NotReported);
}

#[tokio::test]
async fn unadvertised_or_unconfirmed_config_option_is_rejected() {
    let selections = AcpSessionSelections::from_session_result(&json!({
        "configOptions": [{
            "id": "model",
            "currentValue": "old",
            "options": [{"value": "advertised"}]
        }]
    }));
    let state = Arc::new(Mutex::new(FakeAcpProgramState::default()));
    let mut client = AcpClient::new(Box::new(FakeAcpProgramSession {
        inbound: VecDeque::new(),
        output: ExecutionOutput::default(),
        state,
    }));
    let error = client
        .configure_session(
            &opencode_acp_config(Some("missing".into()), None),
            "session-1",
            &selections,
        )
        .await
        .expect_err("unadvertised model must fail before an RPC");
    assert!(error
        .to_string()
        .contains("does not advertise requested model"));

    let state = Arc::new(Mutex::new(FakeAcpProgramState::default()));
    let mut client = AcpClient::new(Box::new(FakeAcpProgramSession {
        inbound: VecDeque::from([r#"{"jsonrpc":"2.0","id":1,"result":{}}"#.to_string()]),
        output: ExecutionOutput::default(),
        state,
    }));
    let error = client
        .configure_session(
            &opencode_acp_config(Some("advertised".into()), None),
            "session-1",
            &selections,
        )
        .await
        .expect_err("an ack without applied evidence must fail");
    assert!(error.to_string().contains("did not observe applied model"));
}

fn acp_response_by_id(messages: &[Value], id: u64) -> Option<&Value> {
    messages
        .iter()
        .find(|message| message.get("id").and_then(Value::as_u64) == Some(id))
}

fn acp_config_current_value<'a>(message: &'a Value, config_id: &str) -> Option<&'a str> {
    message
        .pointer("/result/configOptions")?
        .as_array()?
        .iter()
        .find(|option| option.get("id").and_then(Value::as_str) == Some(config_id))?
        .get("currentValue")?
        .as_str()
}

#[tokio::test]
async fn acp_turn_uses_profile_driver_journal() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let expected_auth = RuntimeAuthKind::from_static("test-acp-auth");
    let mut config = opencode_acp_config(Some("gpt-5".to_string()), Some("plan".to_string()));
    config.auth = Some(expected_auth.clone());
    let adapter = AcpRuntimeAdapter::new(config);
    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            resume: RuntimeResume::Native {
                state: runtime_state(runtime_state_root.clone()),
                ready: runtime_not_ready(),
            },
        })
        .expect("start");
    assert_eq!(
        adapter
            .cancel(&handle, Some("pre-start".into()))
            .await
            .unwrap(),
        lionclaw_runtime_api::RuntimeCancellation::NoActiveTurn
    );
    let fake_state = Arc::new(Mutex::new(FakeAcpProgramState::default()));
    let executor = FakeAcpProgramExecutor {
        inbound: VecDeque::from([
            opencode_initialize_response(1),
            r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"ses_program","configOptions":[{"id":"model","currentValue":"old","options":[{"value":"gpt-5"}]},{"id":"mode","currentValue":"build","options":[{"value":"plan"}]}]}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":3,"result":{"configOptions":[{"id":"model","currentValue":"gpt-5"}]}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":4,"result":{"configOptions":[{"id":"mode","currentValue":"plan"}]}}"#.to_string(),
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_program","update":{"sessionUpdate":"agent_thought_chunk","messageId":"msg_1","content":{"type":"text","text":"thinking"}}}}"#.to_string(),
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_program","update":{"sessionUpdate":"agent_message_chunk","messageId":"msg_1","content":{"type":"text","text":"answer"}}}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":5,"result":{"stopReason":"end_turn","_meta":{}}}"#.to_string(),
        ]),
        expected_auth: Some(expected_auth),
        state: Arc::clone(&fake_state),
    };
    let (journal_tx, mut journal_rx) = tokio::sync::mpsc::channel(RUNTIME_TURN_JOURNAL_CAPACITY);
    let mut context = acp_driver_context(runtime_state_root.clone());
    context.working_dir = Some("/workspace/crates/example".to_string());

    let result = adapter
        .turn(
            TurnExecution {
                input: TurnInput {
                    runtime_session_id: handle.runtime_session_id.clone(),
                    prompt: "hello".to_string(),
                },
                context,
                executor: Box::new(executor),
            },
            journal_tx,
        )
        .await
        .expect("ACP turn");
    assert_eq!(
        result.configuration,
        lionclaw_runtime_api::AppliedRuntimeConfiguration {
            requested_model: Some("gpt-5".to_string()),
            applied_model: Some("gpt-5".to_string()),
            model_confirmation: Some(
                lionclaw_runtime_api::RuntimeConfigurationConfirmation::Observed,
            ),
            requested_mode: Some("plan".to_string()),
            applied_mode: Some("plan".to_string()),
            mode_confirmation: Some(
                lionclaw_runtime_api::RuntimeConfigurationConfirmation::Observed,
            ),
        }
    );
    assert_eq!(result.final_response, "answer");

    let mut journal = Vec::new();
    while let Some(record) = journal_rx.recv().await {
        journal.push(record);
    }
    assert_eq!(
        canonical_events(&journal).cloned().collect::<Vec<_>>(),
        vec![
            RuntimeEvent::Configuration {
                configuration: result.configuration.clone(),
            },
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Reasoning,
                text: "thinking".to_string(),
            },
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text: "answer".to_string(),
            },
            RuntimeEvent::Done,
        ]
    );
    assert_eq!(
        std::fs::read_to_string(runtime_state_value_path(
            &runtime_state_root,
            ACP_SESSION_ID_STATE_FILE,
        ))
        .expect("saved session id"),
        "ses_program\n"
    );
    assert_native_observation(
        &adapter,
        &handle,
        Some(RuntimeNativeSessionObservation::Reconstructed {
            state: RuntimeNativeStateAvailability::Reopenable,
        }),
    );
    let sent = fake_state.lock().expect("fake ACP state").sent.clone();
    assert_eq!(
        sent.iter()
            .filter_map(|message| message.get("method").and_then(Value::as_str))
            .collect::<Vec<_>>(),
        vec![
            "initialize",
            "session/new",
            "session/set_config_option",
            "session/set_config_option",
            "session/prompt",
        ]
    );
    assert_eq!(sent[1]["params"]["cwd"], json!("/workspace/crates/example"));
    assert_eq!(sent[2]["params"]["configId"], json!("model"));
    assert_eq!(sent[2]["params"]["value"], json!("gpt-5"));
    assert_eq!(sent[3]["params"]["configId"], json!("mode"));
    assert_eq!(sent[3]["params"]["value"], json!("plan"));
}

#[tokio::test]
async fn acp_turn_captures_reported_runtime_usage() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(None, None));
    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            resume: RuntimeResume::Native {
                state: runtime_state(runtime_state_root.clone()),
                ready: runtime_not_ready(),
            },
        })
        .expect("start");
    let executor = FakeAcpProgramExecutor {
        inbound: VecDeque::from([
            opencode_initialize_response(1),
            r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"ses_usage","configOptions":[]}}"#.to_string(),
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_usage","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"OK"}}}}"#.to_string(),
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_usage","update":{"sessionUpdate":"usage_update","used":9362,"size":200000,"cost":{"amount":0,"currency":"USD"}}}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn","usage":{"inputTokens":9362,"outputTokens":2,"totalTokens":9376,"thoughtTokens":12},"_meta":{}}}"#.to_string(),
        ]),
        expected_auth: None,
        state: Arc::new(Mutex::new(FakeAcpProgramState::default())),
    };
    let (journal_tx, _journal_rx) = tokio::sync::mpsc::channel(RUNTIME_TURN_JOURNAL_CAPACITY);

    let result = adapter
        .turn(
            TurnExecution {
                input: TurnInput {
                    runtime_session_id: handle.runtime_session_id,
                    prompt: "hello".to_string(),
                },
                context: acp_driver_context(runtime_state_root),
                executor: Box::new(executor),
            },
            journal_tx,
        )
        .await
        .expect("ACP turn");

    assert_eq!(result.final_response, "OK");
    let details = result.runtime_usage.details().expect("reported usage");
    assert_eq!(details.input_tokens, Some(9362));
    assert_eq!(details.output_tokens, Some(2));
    assert_eq!(details.total_tokens, Some(9376));
    assert_eq!(details.reasoning_tokens, Some(12));
    assert_eq!(details.context_used_tokens, Some(9362));
    assert_eq!(details.context_window_tokens, Some(200000));
    let cost = details.cost.as_ref().expect("reported cost");
    assert_eq!(cost.amount, "0");
    assert_eq!(cost.currency, "USD");
    assert_eq!(cost.scope, RuntimeUsageCostScope::SessionCumulative);
}

#[tokio::test]
async fn acp_turn_keeps_partial_usage_and_ignores_malformed_fields() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(None, None));
    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            resume: RuntimeResume::Native {
                state: runtime_state(runtime_state_root.clone()),
                ready: runtime_not_ready(),
            },
        })
        .expect("start");
    let executor = FakeAcpProgramExecutor {
        inbound: VecDeque::from([
            opencode_initialize_response(1),
            r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"ses_usage","configOptions":[]}}"#.to_string(),
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_usage","update":{"sessionUpdate":"usage_update","used":"bad","size":4096,"cost":{"amount":false,"currency":"USD"}}}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn","usage":{"inputTokens":5,"outputTokens":"bad","totalTokens":null},"_meta":{}}}"#.to_string(),
        ]),
        expected_auth: None,
        state: Arc::new(Mutex::new(FakeAcpProgramState::default())),
    };
    let (journal_tx, _journal_rx) = tokio::sync::mpsc::channel(RUNTIME_TURN_JOURNAL_CAPACITY);

    let result = adapter
        .turn(
            TurnExecution {
                input: TurnInput {
                    runtime_session_id: handle.runtime_session_id,
                    prompt: "hello".to_string(),
                },
                context: acp_driver_context(runtime_state_root),
                executor: Box::new(executor),
            },
            journal_tx,
        )
        .await
        .expect("ACP turn");

    let details = result.runtime_usage.details().expect("partial usage");
    assert_eq!(details.input_tokens, Some(5));
    assert_eq!(details.output_tokens, None);
    assert_eq!(details.total_tokens, None);
    assert_eq!(details.context_used_tokens, None);
    assert_eq!(details.context_window_tokens, Some(4096));
    assert_eq!(details.cost, None);
}

#[tokio::test]
async fn acp_turn_projects_runtime_mcp_servers() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(None, None));
    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            resume: RuntimeResume::Native {
                state: runtime_state(runtime_state_root.clone()),
                ready: runtime_not_ready(),
            },
        })
        .expect("start");
    let fake_state = Arc::new(Mutex::new(FakeAcpProgramState::default()));
    let executor = FakeAcpProgramExecutor {
        inbound: VecDeque::from([
            opencode_initialize_response(1),
            r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"ses_program","configOptions":[]}}"#
                .to_string(),
            r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn","_meta":{}}}"#.to_string(),
        ]),
        expected_auth: None,
        state: Arc::clone(&fake_state),
    };
    let (journal_tx, _journal_rx) = tokio::sync::mpsc::channel(RUNTIME_TURN_JOURNAL_CAPACITY);
    let mut context = acp_driver_context(runtime_state_root);
    context.mcp_servers = vec![RuntimeMcpServerSpec {
        name: "lionclaw".to_string(),
        command: "node".to_string(),
        args: vec![
            "/runtime/.lionclaw-mcp-stdio-proxy.mjs".to_string(),
            "/runtime/lionclaw/channel-send.sock".to_string(),
        ],
    }];

    adapter
        .turn(
            TurnExecution {
                input: TurnInput {
                    runtime_session_id: handle.runtime_session_id,
                    prompt: "hello".to_string(),
                },
                context,
                executor: Box::new(executor),
            },
            journal_tx,
        )
        .await
        .expect("ACP turn");

    let sent = fake_state.lock().expect("fake ACP state").sent.clone();
    assert_eq!(
        sent[1]["params"]["mcpServers"],
        json!([{
            "name": "lionclaw",
            "command": "node",
            "args": [
                "/runtime/.lionclaw-mcp-stdio-proxy.mjs",
                "/runtime/lionclaw/channel-send.sock"
            ],
            "env": []
        }])
    );
}

#[tokio::test]
async fn acp_terminal_program_uses_native_command_without_protocol_args() {
    let mut config = opencode_acp_config(None, None);
    config.auth = Some(RuntimeAuthKind::from_static("test-acp-auth"));
    config.terminal = RuntimeTerminalConfig {
        args: vec!["--mini".to_string()],
        resume_args: vec!["--continue".to_string()],
        message_arg: Some("--prompt".to_string()),
    };
    let adapter = AcpRuntimeAdapter::new(config);
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");

    let program = adapter
        .build_terminal_program(RuntimeTerminalProgramInput {
            session_id: Uuid::new_v4(),
            runtime_state: runtime_state(runtime_state_root),
            resume: true,
            bootstrap_message: "current facts".to_string(),
        })
        .expect("terminal program");

    assert_eq!(program.executable, "opencode");
    assert_eq!(
        program.args,
        ["--mini", "--continue", "--prompt", "current facts"]
    );
    assert!(!program.args.iter().any(|arg| arg == "acp"));
    assert_eq!(
        program.environment,
        vec![("OPENCODE_DISABLE_AUTOUPDATE".to_string(), "1".to_string())]
    );
    assert_eq!(
        program.auth,
        Some(RuntimeAuthKind::from_static("test-acp-auth"))
    );
}

#[tokio::test]
async fn acp_cancel_sends_session_cancel_for_active_prompt() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let adapter = Arc::new(AcpRuntimeAdapter::new(opencode_acp_config(None, None)));
    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            resume: RuntimeResume::Native {
                state: runtime_state(runtime_state_root.clone()),
                ready: runtime_not_ready(),
            },
        })
        .expect("start");
    let state = CancelableAcpProgramState::new();
    let (journal_tx, mut journal_rx) = tokio::sync::mpsc::channel(RUNTIME_TURN_JOURNAL_CAPACITY);
    let adapter_for_task = Arc::clone(&adapter);
    let handle_for_task = handle.clone();
    let state_for_task = Arc::clone(&state);
    let turn_task = tokio::spawn(async move {
        adapter_for_task
            .turn(
                TurnExecution {
                    input: TurnInput {
                        runtime_session_id: handle_for_task.runtime_session_id,
                        prompt: "cancel me".to_string(),
                    },
                    context: acp_driver_context(runtime_state_root),
                    executor: Box::new(CancelableAcpProgramExecutor {
                        state: state_for_task,
                    }),
                },
                journal_tx,
            )
            .await
    });

    for _ in 0..100 {
        if state
            .sent_methods()
            .iter()
            .any(|method| method == "session/prompt")
        {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        state
            .sent_methods()
            .iter()
            .any(|method| method == "session/prompt"),
        "ACP prompt request was not sent"
    );

    let cancellation = adapter
        .cancel(&handle, Some("operator cancelled".to_string()))
        .await
        .expect("cancel active ACP prompt");
    assert_eq!(
        cancellation,
        lionclaw_runtime_api::RuntimeCancellation::Acknowledged
    );
    assert!(
        state.is_shutdown(),
        "ACP cancel should wait until the prompt response path has shut down the session"
    );
    turn_task.await.expect("turn task").expect("turn completes");

    let mut journal = Vec::new();
    while let Some(record) = journal_rx.recv().await {
        journal.push(record);
    }
    assert_eq!(
        canonical_events(&journal).cloned().collect::<Vec<_>>(),
        vec![RuntimeEvent::Done]
    );
    assert_eq!(
        state.sent_methods(),
        vec![
            "initialize".to_string(),
            "session/new".to_string(),
            "session/prompt".to_string(),
            "session/cancel".to_string(),
        ]
    );
    assert_native_observation(
        adapter.as_ref(),
        &handle,
        Some(RuntimeNativeSessionObservation::Reconstructed {
            state: RuntimeNativeStateAvailability::Reopenable,
        }),
    );
}

#[test]
fn acp_permission_requests_are_denied_by_default() {
    let response = acp_permission_denial(Some(&json!({
        "options": [
            { "optionId": "once", "kind": "allow_once", "name": "Allow once" },
            { "optionId": "reject", "kind": "reject_once", "name": "Reject" }
        ]
    })));

    assert_eq!(
        response,
        json!({
            "outcome": {
                "outcome": "selected",
                "optionId": "reject"
            }
        })
    );
    assert_eq!(
        acp_permission_denial(Some(&json!({ "options": [] }))),
        json!({ "outcome": { "outcome": "cancelled" } })
    );

    assert_eq!(
        acp_permission_denial(Some(&json!({
            "options": [
                {
                    "optionId": "allow",
                    "kind": "allow_once",
                    "name": "Do not deny this request"
                }
            ]
        }))),
        json!({ "outcome": { "outcome": "cancelled" } }),
        "display prose must never turn an allow option into a structured denial"
    );
    assert_eq!(
        acp_permission_denial(Some(&json!({
            "options": [
                { "optionId": "always", "kind": "reject_always" },
                { "optionId": "one", "kind": "reject_once" }
            ]
        }))),
        json!({
            "outcome": {
                "outcome": "selected",
                "optionId": "one"
            }
        }),
        "reject_once is the least-persistent structured denial"
    );
}

#[tokio::test]
async fn acp_session_start_resumes_saved_ready_session() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let runtime_state = runtime_state(runtime_state_root);
    std::fs::write(
        runtime_state.path().join(ACP_SESSION_ID_STATE_FILE),
        "ses_ready\n",
    )
    .expect("write session id");
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(None, None));
    let runtime_session_ready = mark_runtime_ready(&runtime_state);

    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            resume: RuntimeResume::Native {
                state: runtime_state,
                ready: runtime_session_ready,
            },
        })
        .expect("start");
    assert_eq!(
        adapter
            .native_session_observation(&handle)
            .expect("native observation"),
        None,
        "loading an identity is intent, not an observed protocol resume"
    );
}

#[tokio::test]
async fn acp_session_start_rejects_oversized_saved_session() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let runtime_state = runtime_state(runtime_state_root);
    std::fs::write(
        runtime_state.path().join(ACP_SESSION_ID_STATE_FILE),
        vec![b'x'; RUNTIME_STATE_VALUE_LIMIT + 1],
    )
    .expect("write oversized session id");
    let ready = mark_runtime_ready(&runtime_state);
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(None, None));

    let error = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            resume: RuntimeResume::Native {
                state: runtime_state,
                ready,
            },
        })
        .expect_err("oversized saved ACP session must fail closed");

    assert!(error.to_string().contains("4096-byte limit"));
}

#[tokio::test]
async fn acp_prefers_session_resume_and_uses_effective_working_directory() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let runtime_state = runtime_state(runtime_state_root.clone());
    std::fs::write(
        runtime_state.path().join(ACP_SESSION_ID_STATE_FILE),
        "ses_ready\n",
    )
    .expect("write session id");
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(None, None));
    let ready = mark_runtime_ready(&runtime_state);
    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            resume: RuntimeResume::Native {
                state: runtime_state,
                ready,
            },
        })
        .expect("start");
    let fake_state = Arc::new(Mutex::new(FakeAcpProgramState::default()));
    let executor = FakeAcpProgramExecutor {
        inbound: VecDeque::from([
            opencode_initialize_response(1),
            r#"{"jsonrpc":"2.0","id":2,"result":{"configOptions":[]}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn","_meta":{}}}"#.to_string(),
        ]),
        expected_auth: None,
        state: Arc::clone(&fake_state),
    };
    let (journal_tx, mut journal_rx) = tokio::sync::mpsc::channel(RUNTIME_TURN_JOURNAL_CAPACITY);
    let mut context = acp_driver_context(runtime_state_root);
    context.working_dir = Some("/workspace/packages/runtime".to_string());

    adapter
        .turn(
            TurnExecution {
                input: TurnInput {
                    runtime_session_id: handle.runtime_session_id.clone(),
                    prompt: "continue".to_string(),
                },
                context,
                executor: Box::new(executor),
            },
            journal_tx,
        )
        .await
        .expect("ACP resume turn");

    let mut journal = Vec::new();
    while let Some(record) = journal_rx.recv().await {
        journal.push(record);
    }
    assert_eq!(
        canonical_events(&journal).cloned().collect::<Vec<_>>(),
        vec![RuntimeEvent::Done]
    );
    let sent = fake_state.lock().expect("fake ACP state").sent.clone();
    assert_eq!(
        sent.iter()
            .filter_map(|message| message.get("method").and_then(Value::as_str))
            .collect::<Vec<_>>(),
        vec!["initialize", "session/resume", "session/prompt"]
    );
    assert_eq!(sent[1]["params"]["sessionId"], json!("ses_ready"));
    assert_eq!(
        sent[1]["params"]["cwd"],
        json!("/workspace/packages/runtime")
    );
    assert_native_observation(
        &adapter,
        &handle,
        Some(RuntimeNativeSessionObservation::Resumed),
    );
}

#[tokio::test]
async fn acp_uses_session_load_when_resume_is_unsupported() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let runtime_state = runtime_state(runtime_state_root.clone());
    std::fs::write(
        runtime_state.path().join(ACP_SESSION_ID_STATE_FILE),
        "ses_ready\n",
    )
    .expect("write session id");
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(None, None));
    let ready = mark_runtime_ready(&runtime_state);
    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            resume: RuntimeResume::Native {
                state: runtime_state,
                ready,
            },
        })
        .expect("start");
    let fake_state = Arc::new(Mutex::new(FakeAcpProgramState::default()));
    let executor = FakeAcpProgramExecutor {
        inbound: VecDeque::from([
            load_only_initialize_response(1),
            r#"{"jsonrpc":"2.0","id":2,"result":{"configOptions":[]}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn","_meta":{}}}"#.to_string(),
        ]),
        expected_auth: None,
        state: Arc::clone(&fake_state),
    };
    let (journal_tx, mut journal_rx) = tokio::sync::mpsc::channel(RUNTIME_TURN_JOURNAL_CAPACITY);
    let mut context = acp_driver_context(runtime_state_root);
    context.working_dir = Some("/workspace/packages/runtime".to_string());

    adapter
        .turn(
            TurnExecution {
                input: TurnInput {
                    runtime_session_id: handle.runtime_session_id.clone(),
                    prompt: "continue".to_string(),
                },
                context,
                executor: Box::new(executor),
            },
            journal_tx,
        )
        .await
        .expect("ACP resume turn");

    let mut journal = Vec::new();
    while let Some(record) = journal_rx.recv().await {
        journal.push(record);
    }
    assert_eq!(
        canonical_events(&journal).cloned().collect::<Vec<_>>(),
        vec![RuntimeEvent::Done]
    );
    let sent = fake_state.lock().expect("fake ACP state").sent.clone();
    assert_eq!(
        sent.iter()
            .filter_map(|message| message.get("method").and_then(Value::as_str))
            .collect::<Vec<_>>(),
        vec!["initialize", "session/load", "session/prompt"]
    );
    assert_eq!(sent[1]["params"]["sessionId"], json!("ses_ready"));
    assert_eq!(
        sent[1]["params"]["cwd"],
        json!("/workspace/packages/runtime")
    );
    assert_native_observation(
        &adapter,
        &handle,
        Some(RuntimeNativeSessionObservation::Resumed),
    );
}

#[tokio::test]
async fn acp_new_session_without_reopen_capability_clears_stale_session_id() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let runtime_state = runtime_state(runtime_state_root.clone());
    std::fs::write(
        runtime_state.path().join(ACP_SESSION_ID_STATE_FILE),
        "ses_stale\n",
    )
    .expect("write stale session id");
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(None, None));
    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            resume: RuntimeResume::Native {
                state: runtime_state,
                ready: runtime_not_ready(),
            },
        })
        .expect("start");
    let fake_state = Arc::new(Mutex::new(FakeAcpProgramState::default()));
    let executor = FakeAcpProgramExecutor {
        inbound: VecDeque::from([
            no_reopen_initialize_response(1),
            r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"ses_ephemeral","configOptions":[]}}"#
                .to_string(),
            r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn","_meta":{}}}"#.to_string(),
        ]),
        expected_auth: None,
        state: Arc::clone(&fake_state),
    };
    let (journal_tx, mut journal_rx) = tokio::sync::mpsc::channel(RUNTIME_TURN_JOURNAL_CAPACITY);

    adapter
        .turn(
            TurnExecution {
                input: TurnInput {
                    runtime_session_id: handle.runtime_session_id.clone(),
                    prompt: "hello".to_string(),
                },
                context: acp_driver_context(runtime_state_root.clone()),
                executor: Box::new(executor),
            },
            journal_tx,
        )
        .await
        .expect("ACP turn");

    let mut journal = Vec::new();
    while let Some(record) = journal_rx.recv().await {
        journal.push(record);
    }
    assert_eq!(
        canonical_events(&journal).cloned().collect::<Vec<_>>(),
        vec![RuntimeEvent::Done]
    );
    assert!(
        !runtime_state_value_path(&runtime_state_root, ACP_SESSION_ID_STATE_FILE).exists(),
        "unreopenable ACP sessions must not be advertised as resumable state"
    );
    assert_native_observation(
        &adapter,
        &handle,
        Some(RuntimeNativeSessionObservation::Reconstructed {
            state: RuntimeNativeStateAvailability::Unavailable,
        }),
    );
    let sent = fake_state.lock().expect("fake ACP state").sent.clone();
    assert_eq!(
        sent.iter()
            .filter_map(|message| message.get("method").and_then(Value::as_str))
            .collect::<Vec<_>>(),
        vec!["initialize", "session/new", "session/prompt"]
    );
}

#[tokio::test]
async fn acp_ready_session_without_reopen_capability_uses_canonical_prompt() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let runtime_state = runtime_state(runtime_state_root.clone());
    std::fs::write(
        runtime_state.path().join(ACP_SESSION_ID_STATE_FILE),
        "ses_stale\n",
    )
    .expect("write stale session id");
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(None, None));
    let ready = mark_runtime_ready(&runtime_state);
    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            resume: RuntimeResume::Native {
                state: runtime_state,
                ready,
            },
        })
        .expect("start");
    assert_eq!(
        adapter
            .native_session_observation(&handle)
            .expect("native observation"),
        None,
        "loading an identity is not an observed protocol resume"
    );
    let fake_state = Arc::new(Mutex::new(FakeAcpProgramState::default()));
    let executor = FakeAcpProgramExecutor {
        inbound: VecDeque::from([
            no_reopen_initialize_response(1),
            r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"ses_fresh","configOptions":[]}}"#
                .to_string(),
            r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn","_meta":{}}}"#.to_string(),
        ]),
        expected_auth: None,
        state: Arc::clone(&fake_state),
    };
    let (journal_tx, mut journal_rx) = tokio::sync::mpsc::channel(RUNTIME_TURN_JOURNAL_CAPACITY);

    adapter
        .turn(
            TurnExecution {
                input: TurnInput {
                    runtime_session_id: handle.runtime_session_id.clone(),
                    prompt: "canonical prompt".to_string(),
                },
                context: acp_driver_context(runtime_state_root.clone()),
                executor: Box::new(executor),
            },
            journal_tx,
        )
        .await
        .expect("ACP turn");

    let mut journal = Vec::new();
    while let Some(record) = journal_rx.recv().await {
        journal.push(record);
    }
    assert_eq!(
        canonical_events(&journal).cloned().collect::<Vec<_>>(),
        vec![RuntimeEvent::Done]
    );
    assert!(
        !runtime_state_value_path(&runtime_state_root, ACP_SESSION_ID_STATE_FILE).exists(),
        "stale ACP session ids must be cleared when the agent cannot reopen them"
    );
    assert_native_observation(
        &adapter,
        &handle,
        Some(RuntimeNativeSessionObservation::Reconstructed {
            state: RuntimeNativeStateAvailability::Unavailable,
        }),
    );
    let sent = fake_state.lock().expect("fake ACP state").sent.clone();
    assert_eq!(
        sent.iter()
            .filter_map(|message| message.get("method").and_then(Value::as_str))
            .collect::<Vec<_>>(),
        vec!["initialize", "session/new", "session/prompt"]
    );
    assert_eq!(sent[1]["params"]["cwd"], json!("/workspace"));
    assert_eq!(sent[2]["params"]["sessionId"], json!("ses_fresh"));
    assert_eq!(
        sent[2]["params"]["prompt"][0]["text"],
        json!("canonical prompt")
    );
}

#[tokio::test]
async fn matching_json_rpc_reopen_rejection_is_observed_without_forgetting_identity() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let runtime_state = runtime_state(runtime_state_root.clone());
    write_acp_session_id(&runtime_state, "ses_ready");
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(None, None));
    let handle = start_ready_acp_session(&adapter, runtime_state).await;
    let fake_state = Arc::new(Mutex::new(FakeAcpProgramState::default()));
    let executor = FakeAcpProgramExecutor {
        inbound: VecDeque::from([
            opencode_initialize_response(1),
            r#"{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"session unavailable"}}"#
                .to_string(),
        ]),
        expected_auth: None,
        state: Arc::clone(&fake_state),
    };
    let (journal, _journal_rx) = tokio::sync::mpsc::channel(RUNTIME_TURN_JOURNAL_CAPACITY);

    let error = adapter
        .turn(
            TurnExecution {
                input: TurnInput {
                    runtime_session_id: handle.runtime_session_id.clone(),
                    prompt: "continue".to_string(),
                },
                context: acp_driver_context(runtime_state_root.clone()),
                executor: Box::new(executor),
            },
            journal,
        )
        .await
        .expect_err("matching session/resume rejection must fail");

    assert!(matches!(
        error.downcast_ref::<TypedFailure>(),
        Some(TypedFailure::PermanentRuntime { .. })
    ));
    assert_native_observation(
        &adapter,
        &handle,
        Some(RuntimeNativeSessionObservation::ReopenFailed),
    );
    assert_acp_session_id(&runtime_state_root, "ses_ready");
}

#[derive(Debug, Clone, Copy)]
enum RetryableReopenMethod {
    Load,
    Resume,
}

async fn assert_retryable_reopen_failure_retains_identity(method: RetryableReopenMethod) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let runtime_state = runtime_state(runtime_state_root.clone());
    write_acp_session_id(&runtime_state, "ses_ready");
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(None, None));
    let handle = start_ready_acp_session(&adapter, runtime_state).await;
    let fake_state = Arc::new(Mutex::new(FakeAcpProgramState::default()));
    let (initialize, reopen_method) = match method {
        RetryableReopenMethod::Load => (load_only_initialize_response(1), "session/load"),
        RetryableReopenMethod::Resume => (resume_only_initialize_response(1), "session/resume"),
    };
    let executor = FakeAcpProgramExecutor {
        inbound: VecDeque::from([
            initialize,
            r#"{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"provider temporarily unavailable","data":{"retryable":true,"retryAfterMs":25}}}"#.to_string(),
        ]),
        expected_auth: None,
        state: Arc::clone(&fake_state),
    };
    let (journal, _journal_rx) = tokio::sync::mpsc::channel(RUNTIME_TURN_JOURNAL_CAPACITY);

    let error = adapter
        .turn(
            TurnExecution {
                input: TurnInput {
                    runtime_session_id: handle.runtime_session_id.clone(),
                    prompt: "continue".to_string(),
                },
                context: acp_driver_context(runtime_state_root.clone()),
                executor: Box::new(executor),
            },
            journal,
        )
        .await
        .expect_err("retryable reopen failure must fail the attempt");

    assert!(matches!(
        error.downcast_ref::<TypedFailure>(),
        Some(TypedFailure::TransientRuntime {
            retry_after_ms: Some(25),
            ..
        })
    ));
    assert_native_observation(&adapter, &handle, None);
    assert_acp_session_id(&runtime_state_root, "ses_ready");
    assert_eq!(
        fake_state
            .lock()
            .expect("fake ACP state")
            .sent
            .iter()
            .filter_map(|message| message.get("method").and_then(Value::as_str))
            .collect::<Vec<_>>(),
        vec!["initialize", reopen_method]
    );
}

#[tokio::test]
async fn retryable_session_load_failure_retains_native_identity() {
    assert_retryable_reopen_failure_retains_identity(RetryableReopenMethod::Load).await;
}

#[tokio::test]
async fn retryable_session_resume_failure_retains_native_identity() {
    assert_retryable_reopen_failure_retains_identity(RetryableReopenMethod::Resume).await;
}

#[derive(Debug, Clone, Copy)]
enum NonRejectionReopenFailure {
    Eof,
    MalformedJson,
    Transport,
    MalformedEnvelope(&'static str),
}

async fn assert_non_rejection_reopen_failure_retains_identity(failure: NonRejectionReopenFailure) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let runtime_state = runtime_state(runtime_state_root.clone());
    write_acp_session_id(&runtime_state, "ses_ready");
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(None, None));
    let handle = start_ready_acp_session(&adapter, runtime_state).await;
    let fake_state = Arc::new(Mutex::new(FakeAcpProgramState::default()));
    let executor: Box<dyn RuntimeProgramExecutor> = match failure {
        NonRejectionReopenFailure::Eof => Box::new(FakeAcpProgramExecutor {
            inbound: VecDeque::from([opencode_initialize_response(1)]),
            expected_auth: None,
            state: Arc::clone(&fake_state),
        }),
        NonRejectionReopenFailure::MalformedJson => Box::new(FakeAcpProgramExecutor {
            inbound: VecDeque::from([opencode_initialize_response(1), "{not-json".to_string()]),
            expected_auth: None,
            state: Arc::clone(&fake_state),
        }),
        NonRejectionReopenFailure::Transport => Box::new(ReadFailingAcpProgramExecutor {
            inbound: VecDeque::from([opencode_initialize_response(1)]),
            state: Arc::clone(&fake_state),
        }),
        NonRejectionReopenFailure::MalformedEnvelope(response) => {
            Box::new(FakeAcpProgramExecutor {
                inbound: VecDeque::from([opencode_initialize_response(1), response.to_string()]),
                expected_auth: None,
                state: Arc::clone(&fake_state),
            })
        }
    };
    let (journal, _journal_rx) = tokio::sync::mpsc::channel(RUNTIME_TURN_JOURNAL_CAPACITY);

    adapter
        .turn(
            TurnExecution {
                input: TurnInput {
                    runtime_session_id: handle.runtime_session_id.clone(),
                    prompt: "continue".to_string(),
                },
                context: acp_driver_context(runtime_state_root.clone()),
                executor,
            },
            journal,
        )
        .await
        .expect_err("non-rejection reopen failure must fail");

    assert_native_observation(&adapter, &handle, None);
    assert_acp_session_id(&runtime_state_root, "ses_ready");
}

#[tokio::test]
async fn eof_while_reopening_is_not_a_reopen_rejection() {
    assert_non_rejection_reopen_failure_retains_identity(NonRejectionReopenFailure::Eof).await;
}

#[tokio::test]
async fn malformed_json_while_reopening_is_not_a_reopen_rejection() {
    assert_non_rejection_reopen_failure_retains_identity(NonRejectionReopenFailure::MalformedJson)
        .await;
}

#[tokio::test]
async fn transport_error_while_reopening_is_not_a_reopen_rejection() {
    assert_non_rejection_reopen_failure_retains_identity(NonRejectionReopenFailure::Transport)
        .await;
}

#[tokio::test]
async fn malformed_matching_responses_cannot_trigger_reopen_recovery() {
    for response in [
        r#"{"jsonrpc":"2.0","id":2,"error":null}"#,
        r#"{"jsonrpc":"2.0","id":2,"error":"unavailable"}"#,
        r#"{"jsonrpc":"2.0","id":2,"error":{}}"#,
        r#"{"id":2,"error":{"code":-32000,"message":"unavailable"}}"#,
        r#"{"jsonrpc":"1.0","id":2,"error":{"code":-32000,"message":"unavailable"}}"#,
        r#"{"jsonrpc":"2.0","id":2,"error":{"message":"unavailable"}}"#,
        r#"{"jsonrpc":"2.0","id":2,"error":{"code":-32000}}"#,
        r#"{"jsonrpc":"2.0","id":2,"result":{},"error":{"code":-32000,"message":"unavailable"}}"#,
    ] {
        assert_non_rejection_reopen_failure_retains_identity(
            NonRejectionReopenFailure::MalformedEnvelope(response),
        )
        .await;
    }
}

#[tokio::test]
async fn reconstructed_reopenable_observation_and_identity_survive_configuration_failure() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let mut config = opencode_acp_config(Some("missing-model".to_string()), None);
    config.auth = None;
    let adapter = AcpRuntimeAdapter::new(config);
    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            resume: RuntimeResume::Native {
                state: runtime_state(runtime_state_root.clone()),
                ready: runtime_not_ready(),
            },
        })
        .expect("start");
    let fake_state = Arc::new(Mutex::new(FakeAcpProgramState::default()));
    let executor = FakeAcpProgramExecutor {
        inbound: VecDeque::from([
            opencode_initialize_response(1),
            r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"ses_durable","configOptions":[]}}"#
                .to_string(),
        ]),
        expected_auth: None,
        state: Arc::clone(&fake_state),
    };
    let (journal, _journal_rx) = tokio::sync::mpsc::channel(RUNTIME_TURN_JOURNAL_CAPACITY);

    adapter
        .turn(
            TurnExecution {
                input: TurnInput {
                    runtime_session_id: handle.runtime_session_id.clone(),
                    prompt: "never reached".to_string(),
                },
                context: acp_driver_context(runtime_state_root.clone()),
                executor: Box::new(executor),
            },
            journal,
        )
        .await
        .expect_err("unavailable requested model must fail after session/new");

    assert_native_observation(
        &adapter,
        &handle,
        Some(RuntimeNativeSessionObservation::Reconstructed {
            state: RuntimeNativeStateAvailability::Reopenable,
        }),
    );
    assert_acp_session_id(&runtime_state_root, "ses_durable");
}

#[tokio::test]
async fn resumed_observation_and_identity_survive_prompt_failure() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let runtime_state = runtime_state(runtime_state_root.clone());
    write_acp_session_id(&runtime_state, "ses_ready");
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(None, None));
    let handle = start_ready_acp_session(&adapter, runtime_state).await;
    let fake_state = Arc::new(Mutex::new(FakeAcpProgramState::default()));
    let executor = FakeAcpProgramExecutor {
        inbound: VecDeque::from([
            opencode_initialize_response(1),
            r#"{"jsonrpc":"2.0","id":2,"result":{"configOptions":[]}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":3,"error":{"code":-32001,"message":"prompt failed"}}"#
                .to_string(),
        ]),
        expected_auth: None,
        state: Arc::clone(&fake_state),
    };
    let (journal, _journal_rx) = tokio::sync::mpsc::channel(RUNTIME_TURN_JOURNAL_CAPACITY);

    adapter
        .turn(
            TurnExecution {
                input: TurnInput {
                    runtime_session_id: handle.runtime_session_id.clone(),
                    prompt: "continue".to_string(),
                },
                context: acp_driver_context(runtime_state_root.clone()),
                executor: Box::new(executor),
            },
            journal,
        )
        .await
        .expect_err("prompt rejection must fail after successful resume");

    assert_native_observation(
        &adapter,
        &handle,
        Some(RuntimeNativeSessionObservation::Resumed),
    );
    assert_acp_session_id(&runtime_state_root, "ses_ready");
}
