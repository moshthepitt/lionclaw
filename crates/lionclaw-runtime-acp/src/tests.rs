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
    canonical_events, ExecutionOutput, NetworkMode, RuntimeAdapter, RuntimeAuthKind, RuntimeEvent,
    RuntimeExecutionContext, RuntimeMcpServerSpec, RuntimeMessageLane, RuntimeProgramExecutor,
    RuntimeProgramSession, RuntimeProgramSpec, RuntimeProgramStdoutSender,
    RuntimeProgramTurnExecution, RuntimeSessionReady, RuntimeSessionStartInput,
    RuntimeTerminalConfig, RuntimeTerminalProgramInput, RuntimeTurnInput, RuntimeTurnMode,
    RUNTIME_SESSION_READY_MARKER,
};

use super::{
    acp_permission_denial, acp_turn_events, AcpMessage, AcpRuntimeAdapter, AcpRuntimeConfig,
    ACP_SESSION_ID_STATE_FILE,
};

fn opencode_acp_config(model: Option<String>, mode: Option<String>) -> AcpRuntimeConfig {
    AcpRuntimeConfig {
        runtime_id: "opencode".to_string(),
        executable: "opencode".to_string(),
        args: vec!["acp".to_string()],
        environment: vec![("OPENCODE_DISABLE_AUTOUPDATE".to_string(), "1".to_string())],
        model,
        mode,
        auth: None,
        terminal: RuntimeTerminalConfig { args: Vec::new() },
        session_id_state_file: ACP_SESSION_ID_STATE_FILE.to_string(),
        default_working_dir: "/workspace".to_string(),
    }
}

fn runtime_not_ready() -> RuntimeSessionReady {
    RuntimeSessionReady::not_ready()
}

fn mark_runtime_ready(runtime_state_root: &Path) -> RuntimeSessionReady {
    std::fs::write(
        runtime_state_root.join(RUNTIME_SESSION_READY_MARKER),
        "ready\n",
    )
    .expect("write runtime ready marker");
    RuntimeSessionReady::from_runtime_state_root(runtime_state_root)
        .expect("runtime ready marker should be valid")
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
            let message = AcpMessage {
                raw: raw.clone(),
                value,
            };
            acp_turn_events(&message)
        })
        .map(|record| record.event)
        .collect()
}

fn acp_driver_context(runtime_state_root: PathBuf) -> RuntimeExecutionContext {
    RuntimeExecutionContext {
        network_mode: NetworkMode::On,
        working_dir: None,
        environment: Vec::new(),
        runtime_state_root: Some(runtime_state_root),
        runtime_path_projections: Vec::new(),
        mcp_servers: Vec::new(),
    }
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
async fn acp_program_backed_turn_uses_profile_driver_journal() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let expected_auth = RuntimeAuthKind::from_static("test-acp-auth");
    let mut config = opencode_acp_config(Some("gpt-5".to_string()), Some("plan".to_string()));
    config.auth = Some(expected_auth.clone());
    let adapter = AcpRuntimeAdapter::new(config);
    assert_eq!(adapter.turn_mode(), RuntimeTurnMode::ProgramBacked);

    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            runtime_skill_ids: Vec::new(),
            runtime_state_root: Some(runtime_state_root.clone()),
            runtime_session_ready: runtime_not_ready(),
        })
        .await
        .expect("start");
    let fake_state = Arc::new(Mutex::new(FakeAcpProgramState::default()));
    let executor = FakeAcpProgramExecutor {
        inbound: VecDeque::from([
            opencode_initialize_response(1),
            r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"ses_program","configOptions":[]}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":3,"result":{}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":4,"result":{}}"#.to_string(),
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_program","update":{"sessionUpdate":"agent_thought_chunk","messageId":"msg_1","content":{"type":"text","text":"thinking"}}}}"#.to_string(),
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_program","update":{"sessionUpdate":"agent_message_chunk","messageId":"msg_1","content":{"type":"text","text":"answer"}}}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":5,"result":{"stopReason":"end_turn","_meta":{}}}"#.to_string(),
        ]),
        expected_auth: Some(expected_auth),
        state: Arc::clone(&fake_state),
    };
    let (journal_tx, mut journal_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut context = acp_driver_context(runtime_state_root.clone());
    context.working_dir = Some("/workspace/crates/example".to_string());

    adapter
        .program_backed_turn(
            RuntimeProgramTurnExecution {
                input: RuntimeTurnInput {
                    runtime_session_id: handle.runtime_session_id.clone(),
                    prompt: "hello".to_string(),
                    fresh_prompt: None,
                    runtime_skill_ids: Vec::new(),
                },
                context,
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
        vec![
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
    assert!(journal.iter().all(|record| record.raw.is_some()));
    assert_eq!(
        std::fs::read_to_string(runtime_state_root.join(ACP_SESSION_ID_STATE_FILE))
            .expect("saved session id"),
        "ses_program\n"
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
async fn acp_program_backed_turn_projects_runtime_mcp_servers() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(None, None));
    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            runtime_skill_ids: Vec::new(),
            runtime_state_root: Some(runtime_state_root.clone()),
            runtime_session_ready: runtime_not_ready(),
        })
        .await
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
    let (journal_tx, _journal_rx) = tokio::sync::mpsc::unbounded_channel();
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
        .program_backed_turn(
            RuntimeProgramTurnExecution {
                input: RuntimeTurnInput {
                    runtime_session_id: handle.runtime_session_id,
                    prompt: "hello".to_string(),
                    fresh_prompt: None,
                    runtime_skill_ids: Vec::new(),
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
    let adapter = AcpRuntimeAdapter::new(config);
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");

    let program = adapter
        .build_terminal_program(RuntimeTerminalProgramInput {
            session_id: Uuid::new_v4(),
            runtime_state_root,
        })
        .expect("terminal program");

    assert_eq!(program.executable, "opencode");
    assert!(program.args.is_empty());
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
            runtime_skill_ids: Vec::new(),
            runtime_state_root: Some(runtime_state_root.clone()),
            runtime_session_ready: runtime_not_ready(),
        })
        .await
        .expect("start");
    let state = CancelableAcpProgramState::new();
    let (journal_tx, mut journal_rx) = tokio::sync::mpsc::unbounded_channel();
    let adapter_for_task = Arc::clone(&adapter);
    let handle_for_task = handle.clone();
    let state_for_task = Arc::clone(&state);
    let turn_task = tokio::spawn(async move {
        adapter_for_task
            .program_backed_turn(
                RuntimeProgramTurnExecution {
                    input: RuntimeTurnInput {
                        runtime_session_id: handle_for_task.runtime_session_id,
                        prompt: "cancel me".to_string(),
                        fresh_prompt: None,
                        runtime_skill_ids: Vec::new(),
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

    adapter
        .cancel(&handle, Some("operator cancelled".to_string()))
        .await
        .expect("cancel active ACP prompt");
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
}

#[tokio::test]
async fn acp_session_start_resumes_saved_ready_session() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    std::fs::write(
        runtime_state_root.join(ACP_SESSION_ID_STATE_FILE),
        "ses_ready\n",
    )
    .expect("write session id");
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(None, None));
    let runtime_session_ready = mark_runtime_ready(&runtime_state_root);

    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            runtime_skill_ids: Vec::new(),
            runtime_state_root: Some(runtime_state_root),
            runtime_session_ready,
        })
        .await
        .expect("start");
    assert!(handle.resumes_existing_session);
}

#[tokio::test]
async fn acp_resume_uses_effective_working_directory() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    std::fs::write(
        runtime_state_root.join(ACP_SESSION_ID_STATE_FILE),
        "ses_ready\n",
    )
    .expect("write session id");
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(None, None));
    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            runtime_skill_ids: Vec::new(),
            runtime_state_root: Some(runtime_state_root.clone()),
            runtime_session_ready: mark_runtime_ready(&runtime_state_root),
        })
        .await
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
    let (journal_tx, mut journal_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut context = acp_driver_context(runtime_state_root);
    context.working_dir = Some("/workspace/packages/runtime".to_string());

    adapter
        .program_backed_turn(
            RuntimeProgramTurnExecution {
                input: RuntimeTurnInput {
                    runtime_session_id: handle.runtime_session_id,
                    prompt: "continue".to_string(),
                    fresh_prompt: None,
                    runtime_skill_ids: Vec::new(),
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
}

#[tokio::test]
async fn acp_resume_uses_session_resume_when_load_is_unsupported() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    std::fs::write(
        runtime_state_root.join(ACP_SESSION_ID_STATE_FILE),
        "ses_ready\n",
    )
    .expect("write session id");
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(None, None));
    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            runtime_skill_ids: Vec::new(),
            runtime_state_root: Some(runtime_state_root.clone()),
            runtime_session_ready: mark_runtime_ready(&runtime_state_root),
        })
        .await
        .expect("start");
    let fake_state = Arc::new(Mutex::new(FakeAcpProgramState::default()));
    let executor = FakeAcpProgramExecutor {
        inbound: VecDeque::from([
            resume_only_initialize_response(1),
            r#"{"jsonrpc":"2.0","id":2,"result":{"configOptions":[]}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn","_meta":{}}}"#.to_string(),
        ]),
        expected_auth: None,
        state: Arc::clone(&fake_state),
    };
    let (journal_tx, mut journal_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut context = acp_driver_context(runtime_state_root);
    context.working_dir = Some("/workspace/packages/runtime".to_string());

    adapter
        .program_backed_turn(
            RuntimeProgramTurnExecution {
                input: RuntimeTurnInput {
                    runtime_session_id: handle.runtime_session_id,
                    prompt: "continue".to_string(),
                    fresh_prompt: None,
                    runtime_skill_ids: Vec::new(),
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
}

#[tokio::test]
async fn acp_new_session_without_reopen_capability_clears_stale_session_id() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    std::fs::write(
        runtime_state_root.join(ACP_SESSION_ID_STATE_FILE),
        "ses_stale\n",
    )
    .expect("write stale session id");
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(None, None));
    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            runtime_skill_ids: Vec::new(),
            runtime_state_root: Some(runtime_state_root.clone()),
            runtime_session_ready: runtime_not_ready(),
        })
        .await
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
    let (journal_tx, mut journal_rx) = tokio::sync::mpsc::unbounded_channel();

    adapter
        .program_backed_turn(
            RuntimeProgramTurnExecution {
                input: RuntimeTurnInput {
                    runtime_session_id: handle.runtime_session_id,
                    prompt: "hello".to_string(),
                    fresh_prompt: None,
                    runtime_skill_ids: Vec::new(),
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
        !runtime_state_root.join(ACP_SESSION_ID_STATE_FILE).exists(),
        "unreopenable ACP sessions must not be advertised as resumable state"
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
async fn acp_ready_session_without_reopen_capability_falls_back_to_fresh_prompt() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    std::fs::write(
        runtime_state_root.join(ACP_SESSION_ID_STATE_FILE),
        "ses_stale\n",
    )
    .expect("write stale session id");
    let adapter = AcpRuntimeAdapter::new(opencode_acp_config(None, None));
    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            runtime_skill_ids: Vec::new(),
            runtime_state_root: Some(runtime_state_root.clone()),
            runtime_session_ready: mark_runtime_ready(&runtime_state_root),
        })
        .await
        .expect("start");
    assert!(handle.resumes_existing_session);
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
    let (journal_tx, mut journal_rx) = tokio::sync::mpsc::unbounded_channel();

    adapter
        .program_backed_turn(
            RuntimeProgramTurnExecution {
                input: RuntimeTurnInput {
                    runtime_session_id: handle.runtime_session_id,
                    prompt: "resume prompt".to_string(),
                    fresh_prompt: Some("fresh prompt".to_string()),
                    runtime_skill_ids: Vec::new(),
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
        !runtime_state_root.join(ACP_SESSION_ID_STATE_FILE).exists(),
        "stale ACP session ids must be cleared when the agent cannot reopen them"
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
        json!("fresh prompt")
    );
}
