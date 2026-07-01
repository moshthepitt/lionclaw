use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use anyhow::Result;
use async_trait::async_trait;
use lionclaw_runtime_api::{
    append_streamed_text_boundary, append_streamed_text_delta, canonical_events, ExecutionOutput,
    NetworkMode, RawTurnPayload, RuntimeAdapter, RuntimeControlExecution, RuntimeControlInput,
    RuntimeControlOrigin, RuntimeControlOutcome, RuntimeDriverConfig, RuntimeDriverProvider,
    RuntimeEvent, RuntimeExecutionContext, RuntimeFileChangeStatus, RuntimeMcpServerSpec,
    RuntimeMessageLane, RuntimePathProjection, RuntimeProgramExecutor, RuntimeProgramSession,
    RuntimeProgramSpec, RuntimeProgramStdoutSender, RuntimeSessionHandle, RuntimeSessionReady,
    RuntimeSessionStartInput, RuntimeTerminalProgramInput, TurnEvent, RUNTIME_SESSION_READY_MARKER,
};

use crate::codex_runtime_auth_kind;
use serde_json::{json, Value};
use uuid::Uuid;

use super::{
    build_codex_app_server_program, build_codex_terminal_program, codex_mcp_server_override_args,
    extract_app_server_turn_id, response_id, save_thread_id, AppServerMessage, AppServerTransport,
    CodexAppServerClient, CodexRuntimeAdapter, CodexRuntimeConfig, CODEX_THREAD_ID_STATE_FILE,
};

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

fn runtime_home_projection_context(
    runtime_state_root: PathBuf,
    runtime_home_root: PathBuf,
) -> RuntimeExecutionContext {
    RuntimeExecutionContext {
        network_mode: NetworkMode::On,
        working_dir: None,
        environment: Vec::new(),
        runtime_state_root: Some(runtime_state_root.clone()),
        runtime_path_projections: vec![
            RuntimePathProjection::directory("/runtime", runtime_state_root)
                .expect("runtime projection"),
            RuntimePathProjection::directory("/runtime/home", runtime_home_root)
                .expect("runtime home projection"),
        ],
        mcp_servers: Vec::new(),
    }
}

#[derive(Clone)]
struct FakeAppServerTransport {
    incoming: Arc<Mutex<VecDeque<Value>>>,
    sent: Arc<Mutex<Vec<Value>>>,
    shutdowns: Arc<AtomicUsize>,
    output: ExecutionOutput,
}

impl FakeAppServerTransport {
    fn new(incoming: Vec<Value>) -> Self {
        Self {
            incoming: Arc::new(Mutex::new(incoming.into())),
            sent: Arc::new(Mutex::new(Vec::new())),
            shutdowns: Arc::new(AtomicUsize::new(0)),
            output: ExecutionOutput {
                exit_code: Some(0),
                ..ExecutionOutput::default()
            },
        }
    }
}

struct UnusedRuntimeProgramExecutor;

#[async_trait]
impl RuntimeProgramExecutor for UnusedRuntimeProgramExecutor {
    async fn execute_streaming(
        &mut self,
        _program: RuntimeProgramSpec,
        _stdout: RuntimeProgramStdoutSender,
    ) -> Result<ExecutionOutput> {
        anyhow::bail!("test did not expect streaming runtime execution")
    }

    async fn execute_captured(&mut self, _program: RuntimeProgramSpec) -> Result<ExecutionOutput> {
        anyhow::bail!("test did not expect captured runtime execution")
    }

    async fn spawn(
        &mut self,
        _program: RuntimeProgramSpec,
    ) -> Result<Box<dyn RuntimeProgramSession>> {
        anyhow::bail!("test did not expect interactive runtime execution")
    }
}

async fn start_codex_test_session(
    runtime_state_root: Option<PathBuf>,
) -> (
    CodexRuntimeAdapter,
    RuntimeSessionHandle,
    super::CodexThreadState,
) {
    start_codex_test_session_with_config(CodexRuntimeConfig::default(), runtime_state_root).await
}

async fn start_codex_test_session_with_config(
    config: CodexRuntimeConfig,
    runtime_state_root: Option<PathBuf>,
) -> (
    CodexRuntimeAdapter,
    RuntimeSessionHandle,
    super::CodexThreadState,
) {
    let adapter = CodexRuntimeAdapter::new(config);
    let runtime_session_ready = runtime_not_ready();
    let handle = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            runtime_skill_ids: Vec::new(),
            runtime_state_root,
            runtime_session_ready,
        })
        .await
        .expect("start");
    let thread_state = adapter.thread_state_for(&handle.runtime_session_id);
    (adapter, handle, thread_state)
}

fn omits_jsonrpc_header(message: &Value) -> bool {
    message.get("jsonrpc").is_none()
}

fn codex_protocol_fixture(name: &str) -> Value {
    let raw = match name {
        "compact_context_compaction_v2" => {
            include_str!("../tests/fixtures/codex_app_server/compact_context_compaction_v2.json")
        }
        "turn_interrupt_v2" => {
            include_str!("../tests/fixtures/codex_app_server/turn_interrupt_v2.json")
        }
        "successful_turn_v2" => {
            include_str!("../tests/fixtures/codex_app_server/successful_turn_v2.json")
        }
        other => panic!("unknown Codex protocol fixture: {other}"),
    };
    serde_json::from_str(raw).expect("Codex protocol fixture is valid JSON")
}

fn fixture_server_messages(fixture: &Value) -> Vec<Value> {
    fixture
        .get("server")
        .and_then(Value::as_array)
        .expect("fixture server messages")
        .clone()
}

fn fixture_client_request(fixture: &Value) -> (&str, Value) {
    let client = fixture.get("client").expect("fixture client request");
    let method = client
        .get("method")
        .and_then(Value::as_str)
        .expect("fixture client method");
    let params = client.get("params").cloned().unwrap_or(Value::Null);
    (method, params)
}

fn fixture_client_message(fixture: &Value) -> Value {
    fixture
        .get("client")
        .expect("fixture client request")
        .clone()
}

#[async_trait]
impl AppServerTransport for FakeAppServerTransport {
    async fn send(&mut self, message: &Value) -> Result<()> {
        self.sent.lock().expect("sent lock").push(message.clone());
        Ok(())
    }

    async fn recv(&mut self) -> Result<Option<AppServerMessage>> {
        let next = self.incoming.lock().expect("incoming lock").pop_front();
        Ok(next.map(AppServerMessage::from))
    }

    async fn shutdown(&mut self) -> Result<ExecutionOutput> {
        self.shutdowns.fetch_add(1, Ordering::SeqCst);
        Ok(self.output.clone())
    }
}

fn answer_text_from_events(events: impl IntoIterator<Item = RuntimeEvent>) -> String {
    let mut text = String::new();
    for event in events {
        match event {
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text: delta,
            } => append_streamed_text_delta(&mut text, &delta),
            RuntimeEvent::MessageBoundary {
                lane: RuntimeMessageLane::Answer,
            } => append_streamed_text_boundary(&mut text),
            _ => {}
        }
    }
    text
}

#[test]
fn codex_driver_requires_profile_auth() {
    let driver = super::CodexRuntimeDriver;
    let mut config = RuntimeDriverConfig {
        runtime_id: "codex".to_string(),
        executable: "codex".to_string(),
        ..RuntimeDriverConfig::default()
    };

    let err = RuntimeDriverProvider::validate_config(&driver, &config)
        .expect_err("Codex profile auth must be explicit");
    assert!(err.to_string().contains("requires auth kind 'codex'"));

    config.auth = Some(codex_runtime_auth_kind());
    RuntimeDriverProvider::validate_config(&driver, &config)
        .expect("Codex profile with codex auth should validate");
}

#[test]
fn codex_terminal_program_uses_lionclaw_context_and_outer_boundary() {
    let program = build_codex_terminal_program(&CodexRuntimeConfig {
        executable: "codex".to_string(),
        model: Some("gpt-5.5".to_string()),
    });

    assert_eq!(program.executable, "codex");
    assert_eq!(
        program.args,
        vec![
            "--sandbox".to_string(),
            "danger-full-access".to_string(),
            "--ask-for-approval".to_string(),
            "never".to_string(),
            "-c".to_string(),
            "check_for_update_on_startup=false".to_string(),
            "-c".to_string(),
            "projects.\"/workspace\".trust_level=\"trusted\"".to_string(),
            "-c".to_string(),
            "model_instructions_file=\"/runtime/AGENTS.generated.md\"".to_string(),
            "--model".to_string(),
            "gpt-5.5".to_string(),
        ]
    );
    assert_eq!(program.auth, Some(codex_runtime_auth_kind()));
    assert!(program.environment.is_empty());
    assert!(program.stdin.is_empty());
}

#[test]
fn codex_terminal_program_uses_global_options_without_saved_thread_resume() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    save_thread_id(&runtime_state_root, "thr_saved").expect("save thread");

    let adapter = CodexRuntimeAdapter::new(CodexRuntimeConfig {
        executable: "codex".to_string(),
        model: Some("gpt-5.5".to_string()),
    });
    let program = adapter
        .build_terminal_program(RuntimeTerminalProgramInput {
            session_id: Uuid::new_v4(),
            runtime_state_root,
        })
        .expect("terminal program");

    assert_eq!(
        program.args,
        vec![
            "--sandbox".to_string(),
            "danger-full-access".to_string(),
            "--ask-for-approval".to_string(),
            "never".to_string(),
            "-c".to_string(),
            "check_for_update_on_startup=false".to_string(),
            "-c".to_string(),
            "projects.\"/workspace\".trust_level=\"trusted\"".to_string(),
            "-c".to_string(),
            "model_instructions_file=\"/runtime/AGENTS.generated.md\"".to_string(),
            "--model".to_string(),
            "gpt-5.5".to_string(),
        ]
    );
    assert!(!program.args.iter().any(|arg| arg == "resume"));
    assert!(!program.args.iter().any(|arg| arg == "thr_saved"));
}

#[test]
fn codex_app_server_program_uses_default_stdio_transport() {
    let program = build_codex_app_server_program(
        &CodexRuntimeConfig {
            executable: "codex".to_string(),
            model: Some("gpt-5-codex".to_string()),
        },
        &[],
    );

    assert_eq!(program.executable, "codex");
    assert_eq!(
        program.args,
        vec![
            "-c".to_string(),
            "check_for_update_on_startup=false".to_string(),
            "-c".to_string(),
            "projects.\"/workspace\".trust_level=\"trusted\"".to_string(),
            "app-server".to_string(),
        ]
    );
    assert_eq!(program.stdin, "");
    assert_eq!(program.auth, Some(codex_runtime_auth_kind()));
}

#[test]
fn codex_app_server_program_projects_mcp_servers_as_config_overrides() {
    let program = build_codex_app_server_program(
        &CodexRuntimeConfig {
            executable: "codex".to_string(),
            model: None,
        },
        &[RuntimeMcpServerSpec {
            name: "lionclaw".to_string(),
            command: "node".to_string(),
            args: vec![
                "/runtime/.lionclaw-mcp-stdio-proxy.mjs".to_string(),
                "/runtime/lionclaw/channel-send.sock".to_string(),
            ],
        }],
    );

    assert_eq!(
            program.args,
            vec![
                "-c".to_string(),
                "check_for_update_on_startup=false".to_string(),
                "-c".to_string(),
                "projects.\"/workspace\".trust_level=\"trusted\"".to_string(),
                "-c".to_string(),
                "mcp_servers.\"lionclaw\".command=\"node\"".to_string(),
                "-c".to_string(),
                "mcp_servers.\"lionclaw\".args=[\"/runtime/.lionclaw-mcp-stdio-proxy.mjs\",\"/runtime/lionclaw/channel-send.sock\"]".to_string(),
                "app-server".to_string(),
            ]
        );
}

#[test]
fn codex_app_server_program_escapes_mcp_server_config_overrides() {
    let args = codex_mcp_server_override_args(&[RuntimeMcpServerSpec {
        name: "lion\"claw\\tools".to_string(),
        command: "node\nruntime".to_string(),
        args: vec![
            "/runtime/path with spaces".to_string(),
            "quote\"arg".to_string(),
        ],
    }]);

    assert_eq!(
            args,
            vec![
                "-c".to_string(),
                "mcp_servers.\"lion\\\"claw\\\\tools\".command=\"node\\nruntime\"".to_string(),
                "-c".to_string(),
                "mcp_servers.\"lion\\\"claw\\\\tools\".args=[\"/runtime/path with spaces\",\"quote\\\"arg\"]".to_string(),
            ]
        );
}

#[test]
fn app_server_text_preserves_streamed_delta_whitespace() {
    let params = json!({
        "delta": {
            "content": [
                {"text": "Hello"},
                {"text": " world"},
                {"text": "\n"},
            ],
        },
    });

    assert_eq!(
        super::app_server_text(&params, &["delta"]),
        Some("Hello world\n".to_string())
    );
    assert_eq!(
        super::app_server_error_text(&json!({"message": "  failed \n"})),
        "failed"
    );
}

#[test]
fn app_server_item_descriptions_render_operator_activity() {
    assert_eq!(
        super::describe_app_server_item(&json!({
            "item": {
                "type": "commandExecution",
                "command": ["cargo", "test"],
                "cwd": "/workspace/lionclaw",
                "status": "completed",
                "exitCode": 0,
                "durationMs": 1234,
            }
        })),
        Some("codex ran: cargo test  cwd lionclaw  exit 0  1.2s".to_string())
    );
    assert_eq!(
        super::describe_app_server_item(&json!({
            "item": {
                "type": "commandExecution",
                "command": ["rg", "operator console"],
                "status": "completed",
            }
        })),
        Some("codex searched: rg \"operator console\"".to_string())
    );
    let file_change_events = super::app_server_item_events(&json!({
        "item": {
            "id": "edit-1",
            "type": "fileChange",
            "status": "completed",
            "changes": [
                {"path": "/workspace/src/operator/run_tui.rs", "kind": "update"},
                {"path": "Cargo.toml", "kind": "update"}
            ],
        }
    }));
    assert!(matches!(
        file_change_events.as_slice(),
        [RuntimeEvent::FileChange { change }]
            if change.runtime == "codex"
                && change.operation_id.as_deref() == Some("edit-1")
                && change.status == RuntimeFileChangeStatus::Edited
                && change.paths == ["src/operator/run_tui.rs", "Cargo.toml"]
                && change.total_count == 2
    ));
    assert_eq!(
        super::describe_app_server_item(&json!({
            "item": {
                "type": "webSearch",
                "action": {"type": "search", "query": "ratatui scrollbar"}
            }
        })),
        Some("codex searched: ratatui scrollbar".to_string())
    );
    assert_eq!(
        super::describe_app_server_item(&json!({
            "item": {
                "type": "webSearch",
                "query": "",
                "action": {
                    "type": "search",
                    "queries": ["official Ratatui scrollbar docs Scrollbar ratatui widgets"]
                }
            }
        })),
        Some(
            "codex searched: official Ratatui scrollbar docs Scrollbar ratatui widgets".to_string()
        )
    );
    assert_eq!(
        super::describe_app_server_item(&json!({
            "item": {
                "type": "webSearch",
                "query": "",
                "action": {"type": "search", "queries": ["", "  "]}
            }
        })),
        None
    );
    assert_eq!(
        super::describe_app_server_item(&json!({
            "item": {
                "type": "webSearch",
                "action": {
                    "type": "open_page",
                    "url": "https://docs.rs/ratatui/latest/ratatui/widgets/struct.Scrollbar.html"
                }
            }
        })),
        Some(
            "codex opened: https://docs.rs/ratatui/latest/ratatui/widgets/struct.Scrollbar.html"
                .to_string()
        )
    );
    assert_eq!(
        super::describe_app_server_item(&json!({
            "item": {"type": "agentMessage", "text": "hello"}
        })),
        None
    );
}

#[tokio::test]
async fn app_server_agent_message_phases_choose_transcript_lane() {
    let (_adapter, _handle, thread_state) = start_codex_test_session(None).await;
    let mut client = CodexAppServerClient::new(FakeAppServerTransport::new(Vec::new()));
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

    for message in [
        json!({"method": "turn/started", "params": {"threadId": "thr_1", "turnId": "turn_1"}}),
        json!({
            "method": "item/started",
            "params": {
                "item": {
                    "id": "msg_plan",
                    "type": "agentMessage",
                    "phase": "prelude"
                }
            }
        }),
        json!({"method": "item/agentMessage/delta", "params": {"itemId": "msg_plan", "delta": "I'll inspect first."}}),
        json!({
            "method": "item/started",
            "params": {
                "item": {
                    "id": "msg_final",
                    "type": "agentMessage",
                    "phase": "final_answer"
                }
            }
        }),
        json!({"method": "item/agentMessage/delta", "params": {"itemId": "msg_final", "delta": "Final answer."}}),
    ] {
        client
            .dispatch_message(message, &event_tx, &thread_state)
            .await
            .expect("handle message");
    }

    let events: Vec<RuntimeEvent> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeEvent::MessageDelta {
            lane: RuntimeMessageLane::Reasoning,
            text
        } if text == "I'll inspect first."
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeEvent::MessageDelta {
            lane: RuntimeMessageLane::Answer,
            text
        } if text == "Final answer."
    )));
    assert!(!events
        .iter()
        .any(|event| matches!(event, RuntimeEvent::MessageBoundary { .. })));
}

#[tokio::test]
async fn app_server_agent_message_items_emit_answer_boundaries() {
    let (_adapter, _handle, thread_state) = start_codex_test_session(None).await;
    let mut client = CodexAppServerClient::new(FakeAppServerTransport::new(Vec::new()));
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

    for message in [
        json!({"method": "turn/started", "params": {"threadId": "thr_1", "turnId": "turn_1"}}),
        json!({
            "method": "item/started",
            "params": {
                "item": {
                    "id": "msg_intro",
                    "type": "agentMessage",
                    "phase": "final_answer"
                }
            }
        }),
        json!({"method": "item/agentMessage/delta", "params": {"itemId": "msg_intro", "delta": "Intro."}}),
        json!({"method": "item/completed", "params": {"item": {"id": "msg_intro", "type": "agentMessage"}}}),
        json!({
            "method": "item/started",
            "params": {
                "item": {
                    "id": "msg_body",
                    "type": "agentMessage",
                    "phase": "final_answer"
                }
            }
        }),
        json!({"method": "item/agentMessage/delta", "params": {"itemId": "msg_body", "delta": "**Project**"}}),
    ] {
        client
            .dispatch_message(message, &event_tx, &thread_state)
            .await
            .expect("handle message");
    }

    let events: Vec<RuntimeEvent> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
    let message_events = events
        .iter()
        .filter(|event| {
            matches!(
                event,
                RuntimeEvent::MessageDelta { .. } | RuntimeEvent::MessageBoundary { .. }
            )
        })
        .cloned()
        .collect::<Vec<_>>();

    assert!(matches!(
        message_events.as_slice(),
        [
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text: intro
            },
            RuntimeEvent::MessageBoundary {
                lane: RuntimeMessageLane::Answer
            },
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text: body
            }
        ] if intro == "Intro." && body == "**Project**"
    ));
}

#[test]
fn model_list_control_arguments_only_allow_listing_flags() {
    assert_eq!(super::model_list_include_hidden(""), Ok(false));
    assert_eq!(super::model_list_include_hidden("--hidden"), Ok(true));
    assert_eq!(
        super::model_list_include_hidden("--include-hidden"),
        Ok(true)
    );

    let outcome = super::model_list_include_hidden("gpt-5-codex")
        .expect_err("model selection should not be faked as a list request");
    assert!(matches!(
        outcome,
        RuntimeControlOutcome::InteractiveOnly { message } if message.contains("model selection is interactive")
    ));

    let outcome = super::model_list_include_hidden("--unknown")
        .expect_err("unknown model list flags should be rejected");
    assert!(matches!(
        outcome,
        RuntimeControlOutcome::Failed { code, message }
            if code.as_deref() == Some("runtime.control.invalid_arguments")
                && message.contains("--hidden")
    ));
}

#[test]
fn thread_control_argument_validation_rejects_ambiguous_forms() {
    assert!(matches!(
        super::invalid_thread_control_arguments("rename", ""),
        Some(RuntimeControlOutcome::Failed { code, message })
            if code.as_deref() == Some("runtime.control.invalid_arguments")
                && message.contains("non-empty name")
    ));
    assert!(matches!(
        super::invalid_thread_control_arguments("compact", "now"),
        Some(RuntimeControlOutcome::Failed { code, message })
            if code.as_deref() == Some("runtime.control.invalid_arguments")
                && message.contains("does not accept arguments")
    ));
}

#[tokio::test]
async fn review_control_is_unsupported_without_a_verified_native_mapping() {
    let adapter = CodexRuntimeAdapter::new(CodexRuntimeConfig::default());
    let (event_tx, _event_rx) = tokio::sync::mpsc::unbounded_channel();

    let outcome = adapter
        .run_app_server_control(
            RuntimeControlExecution {
                input: RuntimeControlInput {
                    runtime_session_id: "codex-test".to_string(),
                    raw: "/review base main".to_string(),
                    command_name: "review".to_string(),
                    arguments: "base main".to_string(),
                    origin: RuntimeControlOrigin::SessionTurn,
                    runtime_skill_ids: Vec::new(),
                },
                context: RuntimeExecutionContext {
                    network_mode: NetworkMode::On,
                    working_dir: None,
                    environment: Vec::new(),
                    runtime_state_root: None,
                    runtime_path_projections: Vec::new(),
                    mcp_servers: Vec::new(),
                },
                executor: Box::new(UnusedRuntimeProgramExecutor),
            },
            event_tx,
        )
        .await
        .expect("review outcome");

    assert!(matches!(
        outcome,
        RuntimeControlOutcome::Unsupported { message }
            if message.contains("not exposed through LionClaw")
    ));
}

#[test]
fn model_list_description_uses_codex_app_server_display_fields() {
    let response = json!({
        "data": [
            {
                "id": "model-id",
                "model": "gpt-5-codex",
                "displayName": "GPT-5 Codex"
            },
            {
                "id": "fallback-id",
                "model": "fallback-model"
            }
        ]
    });

    assert_eq!(
        super::describe_model_list_response(&response),
        "Available Codex models: GPT-5 Codex, fallback-model."
    );
}

#[test]
fn completed_turn_error_text_only_flags_failed_turns() {
    assert_eq!(
        super::completed_turn_error_text(&json!({
            "turn": {
                "id": "turn_1",
                "status": "completed",
                "error": null
            }
        })),
        None
    );
    assert_eq!(
        super::completed_turn_error_text(&json!({
            "turn": {
                "id": "turn_1",
                "status": "failed",
                "error": {"message": "boom"}
            }
        })),
        Some("boom".to_string())
    );
    assert_eq!(
        super::completed_turn_error_text(&json!({
            "turn": {
                "id": "turn_1",
                "status": "interrupted",
                "error": null
            }
        })),
        Some("codex turn interrupted".to_string())
    );
}

#[tokio::test]
async fn codex_app_server_protocol_streams_turn_and_saves_thread_id() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let (adapter, handle, thread_state) = start_codex_test_session_with_config(
        CodexRuntimeConfig {
            executable: "codex".to_string(),
            model: Some("gpt-5-codex".to_string()),
        },
        Some(runtime_state_root.clone()),
    )
    .await;
    let transport = FakeAppServerTransport::new(vec![
        json!({"id": 1, "result": {"serverInfo": {"name": "codex", "version": "test"}, "userAgent": "codex/test"}}),
        json!({"method": "thread/started", "params": {"threadId": "thr_1"}}),
        json!({"id": 2, "result": {}}),
        json!({"id": 3, "result": {"turn": {"id": "turn_1"}}}),
        json!({"method": "item/agentMessage/delta", "params": {"threadId": "thr_1", "turnId": "turn_1", "delta": "Hello"}}),
        json!({"method": "turn/completed", "params": {"threadId": "thr_1", "turnId": "turn_1"}}),
    ]);
    let sent = transport.sent.clone();
    let mut client = CodexAppServerClient::new(transport);
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

    client
        .initialize(&event_tx, &thread_state)
        .await
        .expect("initialize");
    let thread_id = adapter
        .ensure_app_server_thread(
            &mut client,
            &handle.runtime_session_id,
            &event_tx,
            &thread_state,
        )
        .await
        .expect("thread");
    assert_eq!(thread_id, "thr_1");
    let response = client
        .request(
            "turn/start",
            super::turn_start_params(&thread_id, "hello", Some("gpt-5-codex"), NetworkMode::None),
            &event_tx,
            &thread_state,
        )
        .await
        .expect("turn start");
    client
        .wait_for_turn_completed(
            super::extract_app_server_turn_id(&response).as_deref(),
            None,
            None,
            &event_tx,
            &thread_state,
            None,
        )
        .await
        .expect("turn completed");

    let events: Vec<RuntimeEvent> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeEvent::MessageDelta { lane: RuntimeMessageLane::Answer, text } if text == "Hello"
    )));
    assert_eq!(
        std::fs::read_to_string(runtime_state_root.join(CODEX_THREAD_ID_STATE_FILE))
            .expect("thread state"),
        "thr_1\n"
    );

    let sent = sent.lock().expect("sent lock").clone();
    assert!(sent.iter().all(omits_jsonrpc_header));
    assert_eq!(sent[0]["method"], "initialize");
    assert_eq!(sent[1]["method"], "initialized");
    assert_eq!(sent[2]["method"], "thread/start");
    assert_eq!(sent[3]["method"], "turn/start");
    assert_eq!(
        sent[3]["params"]["sandboxPolicy"]["type"],
        "externalSandbox"
    );
    assert_eq!(
        sent[3]["params"]["sandboxPolicy"]["networkAccess"],
        "restricted"
    );

    adapter.close(&handle).await.expect("close");
}

#[tokio::test]
async fn codex_app_server_separates_distinct_agent_message_items() {
    let (adapter, handle, thread_state) = start_codex_test_session(None).await;
    let transport = FakeAppServerTransport::new(vec![
        json!({"id": 1, "result": {"turn": {"id": "turn_1"}}}),
        json!({"method": "item/agentMessage/delta", "params": {"threadId": "thr_1", "turnId": "turn_1", "itemId": "msg_1", "delta": "First."}}),
        json!({"method": "item/agentMessage/delta", "params": {"threadId": "thr_1", "turnId": "turn_1", "itemId": "msg_2", "delta": "Second."}}),
        json!({"method": "turn/completed", "params": {"threadId": "thr_1", "turnId": "turn_1"}}),
    ]);
    let mut client = CodexAppServerClient::new(transport);
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

    let response = client
        .request("turn/start", json!({}), &event_tx, &thread_state)
        .await
        .expect("turn start");
    client
        .wait_for_turn_completed(
            super::extract_app_server_turn_id(&response).as_deref(),
            Some("thr_1"),
            None,
            &event_tx,
            &thread_state,
            None,
        )
        .await
        .expect("turn completed");

    let events = std::iter::from_fn(|| event_rx.try_recv().ok());
    assert_eq!(answer_text_from_events(events), "First.\n\nSecond.");

    adapter.close(&handle).await.expect("close");
}

async fn assert_image_generation_event_emits_runtime_artifact(event_message: Value) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    let generated_dir = runtime_state_root
        .join("home")
        .join(".codex")
        .join("generated_images")
        .join("thr_1");
    assert_image_generation_event_emits_runtime_artifact_at(
        event_message,
        runtime_state_root,
        generated_dir,
        None,
    )
    .await;
}

async fn assert_image_generation_event_emits_runtime_artifact_at(
    event_message: Value,
    runtime_state_root: PathBuf,
    generated_dir: PathBuf,
    runtime_context: Option<RuntimeExecutionContext>,
) {
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    std::fs::create_dir_all(&generated_dir).expect("create generated image dir");
    std::fs::write(generated_dir.join("ig_1.png"), b"png").expect("write generated image");
    let (adapter, handle, thread_state) =
        start_codex_test_session(Some(runtime_state_root.clone())).await;
    thread_state
        .persist_thread_id("thr_1")
        .expect("persist thread id");
    let transport = FakeAppServerTransport::new(vec![
        json!({"id": 1, "result": {"turn": {"id": "turn_1"}}}),
        event_message,
        json!({"method": "turn/completed", "params": {"threadId": "thr_1", "turnId": "turn_1"}}),
    ]);
    let mut client = match runtime_context {
        Some(runtime_context) => {
            CodexAppServerClient::new_with_runtime_context(transport, runtime_context)
        }
        None => CodexAppServerClient::new(transport),
    };
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

    let response = client
        .request("turn/start", json!({}), &event_tx, &thread_state)
        .await
        .expect("turn start");
    client
        .wait_for_turn_completed(
            super::extract_app_server_turn_id(&response).as_deref(),
            Some("thr_1"),
            None,
            &event_tx,
            &thread_state,
            None,
        )
        .await
        .expect("turn completed");

    let events: Vec<RuntimeEvent> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
    let artifact = events
        .iter()
        .find_map(|event| match event {
            RuntimeEvent::Artifact { artifact } => Some(artifact),
            _ => None,
        })
        .expect("image artifact event");
    assert_eq!(artifact.artifact_id, "codex:image:thr_1:ig_1");
    assert_eq!(artifact.filename.as_deref(), Some("ig_1.png"));
    assert_eq!(artifact.mime_type.as_deref(), Some("image/png"));
    assert_eq!(artifact.path, generated_dir.join("ig_1.png"));

    adapter.close(&handle).await.expect("close");
}

#[tokio::test]
async fn codex_app_server_method_image_generation_event_emits_runtime_artifact() {
    assert_image_generation_event_emits_runtime_artifact(json!({
        "method": "event_msg",
        "params": {
            "payload": {
                "type": "image_generation_end",
                "call_id": "ig_1",
                "status": "completed"
            }
        }
    }))
    .await;
}

#[tokio::test]
async fn codex_app_server_top_level_image_generation_event_emits_runtime_artifact() {
    assert_image_generation_event_emits_runtime_artifact(json!({
        "type": "event_msg",
        "payload": {
            "type": "image_generation_end",
            "call_id": "ig_1",
            "status": "completed",
            "result": "base64 omitted"
        }
    }))
    .await;
}

#[tokio::test]
async fn codex_app_server_image_generation_saved_path_maps_runtime_root() {
    assert_image_generation_event_emits_runtime_artifact(json!({
        "type": "event_msg",
        "payload": {
            "type": "image_generation_end",
            "call_id": "ig_1",
            "status": "generating",
            "saved_path": "/runtime/home/.codex/generated_images/thr_1/ig_1.png",
            "result": "base64 omitted"
        }
    }))
    .await;
}

#[tokio::test]
async fn codex_app_server_image_generation_saved_path_maps_runtime_home_projection() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    let runtime_home_root = temp_dir.path().join("runtime-home");
    let generated_dir = runtime_home_root
        .join(".codex")
        .join("generated_images")
        .join("thr_1");
    let runtime_context =
        runtime_home_projection_context(runtime_state_root.clone(), runtime_home_root);

    assert_image_generation_event_emits_runtime_artifact_at(
        json!({
            "type": "event_msg",
            "payload": {
                "type": "image_generation_end",
                "call_id": "ig_1",
                "status": "generating",
                "saved_path": "/runtime/home/.codex/generated_images/thr_1/ig_1.png",
                "result": "base64 omitted"
            }
        }),
        runtime_state_root,
        generated_dir,
        Some(runtime_context),
    )
    .await;
}

#[tokio::test]
async fn codex_app_server_image_generation_relative_saved_path_maps_runtime_home_projection() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    let runtime_home_root = temp_dir.path().join("runtime-home");
    let generated_dir = runtime_home_root
        .join(".codex")
        .join("generated_images")
        .join("thr_1");
    let runtime_context =
        runtime_home_projection_context(runtime_state_root.clone(), runtime_home_root);

    assert_image_generation_event_emits_runtime_artifact_at(
        json!({
            "type": "event_msg",
            "payload": {
                "type": "image_generation_end",
                "call_id": "ig_1",
                "status": "generating",
                "saved_path": "home/.codex/generated_images/thr_1/ig_1.png",
                "result": "base64 omitted"
            }
        }),
        runtime_state_root,
        generated_dir,
        Some(runtime_context),
    )
    .await;
}

#[tokio::test]
async fn codex_app_server_image_generation_default_path_maps_runtime_home_projection() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    let runtime_home_root = temp_dir.path().join("runtime-home");
    let generated_dir = runtime_home_root
        .join(".codex")
        .join("generated_images")
        .join("thr_1");
    let runtime_context =
        runtime_home_projection_context(runtime_state_root.clone(), runtime_home_root);

    assert_image_generation_event_emits_runtime_artifact_at(
        json!({
            "type": "event_msg",
            "payload": {
                "type": "image_generation_end",
                "call_id": "ig_1",
                "status": "completed",
                "result": "base64 omitted"
            }
        }),
        runtime_state_root,
        generated_dir,
        Some(runtime_context),
    )
    .await;
}

#[test]
fn codex_generated_image_path_rejects_runtime_root_traversal() {
    let runtime_state_root = PathBuf::from("/host/runtime-state");

    assert_eq!(
        super::codex_generated_image_path(
            "/runtime/./home/.codex/generated_images/thr_1/ig_1.png",
            &runtime_state_root,
            None,
        ),
        Some(PathBuf::from(
            "/host/runtime-state/home/.codex/generated_images/thr_1/ig_1.png"
        ))
    );
    assert_eq!(
        super::codex_generated_image_path("/runtime/../outside.png", &runtime_state_root, None),
        None
    );
    assert_eq!(
        super::codex_generated_image_path("../outside.png", &runtime_state_root, None),
        None
    );
    assert_eq!(
        super::codex_generated_image_path("/tmp/outside.png", &runtime_state_root, None),
        None
    );
    assert_eq!(
        super::codex_generated_image_path("/runtime", &runtime_state_root, None),
        None
    );
    assert_eq!(
        super::codex_generated_image_path(".", &runtime_state_root, None),
        None
    );
}

#[test]
fn codex_generated_image_path_uses_runtime_home_projection() {
    let runtime_state_root = PathBuf::from("/host/runtime-state");
    let runtime_home_root = PathBuf::from("/host/runtime-home");
    let context = runtime_home_projection_context(runtime_state_root.clone(), runtime_home_root);

    assert_eq!(
        super::codex_generated_image_path(
            "/runtime/home/.codex/generated_images/thr_1/ig_1.png",
            &runtime_state_root,
            Some(&context),
        ),
        Some(PathBuf::from(
            "/host/runtime-home/.codex/generated_images/thr_1/ig_1.png"
        ))
    );
    assert_eq!(
        super::codex_generated_image_path(
            "home/.codex/generated_images/thr_1/ig_1.png",
            &runtime_state_root,
            Some(&context),
        ),
        Some(PathBuf::from(
            "/host/runtime-home/.codex/generated_images/thr_1/ig_1.png"
        ))
    );
}

#[test]
fn codex_generated_image_path_respects_blocked_runtime_projection() {
    let runtime_state_root = PathBuf::from("/host/runtime-state");
    let context = RuntimeExecutionContext {
        network_mode: NetworkMode::On,
        working_dir: None,
        environment: Vec::new(),
        runtime_state_root: Some(runtime_state_root.clone()),
        runtime_path_projections: vec![
            RuntimePathProjection::directory("/runtime", runtime_state_root.clone())
                .expect("runtime projection"),
            RuntimePathProjection::exact(
                "/runtime/lionclaw/channel-send.sock",
                "/tmp/channel-send.sock",
            )
            .expect("channel send socket projection"),
        ],
        mcp_servers: Vec::new(),
    };

    assert_eq!(
        super::codex_generated_image_path(
            "/runtime/lionclaw/channel-send.sock/hidden.png",
            &runtime_state_root,
            Some(&context),
        ),
        None
    );
    assert_eq!(
        super::codex_generated_image_path(
            "lionclaw/channel-send.sock/hidden.png",
            &runtime_state_root,
            Some(&context),
        ),
        None
    );
    assert_eq!(
        super::codex_generated_image_path(
            "/runtime/lionclaw/channel-send.sock/hidden.png",
            &runtime_state_root,
            None,
        ),
        Some(PathBuf::from(
            "/host/runtime-state/lionclaw/channel-send.sock/hidden.png"
        ))
    );
}

#[test]
fn codex_default_generated_image_path_uses_runtime_home_projection() {
    let runtime_state_root = PathBuf::from("/host/runtime-state");
    let runtime_home_root = PathBuf::from("/host/runtime-home");
    let context =
        runtime_home_projection_context(runtime_state_root.clone(), runtime_home_root.clone());

    assert_eq!(
        super::codex_default_generated_image_path(
            "thr_1",
            "ig_1.png",
            &runtime_state_root,
            Some(&context),
        ),
        runtime_home_root
            .join(".codex")
            .join("generated_images")
            .join("thr_1")
            .join("ig_1.png")
    );
    assert_eq!(
        super::codex_default_generated_image_path("thr_1", "ig_1.png", &runtime_state_root, None,),
        runtime_state_root
            .join("home")
            .join(".codex")
            .join("generated_images")
            .join("thr_1")
            .join("ig_1.png")
    );
}

#[tokio::test]
async fn codex_app_server_image_generation_unsafe_saved_path_does_not_use_default_artifact() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    let generated_dir = runtime_state_root
        .join("home")
        .join(".codex")
        .join("generated_images")
        .join("thr_1");
    std::fs::create_dir_all(&generated_dir).expect("create generated image dir");
    std::fs::write(generated_dir.join("ig_1.png"), b"png").expect("write generated image");
    let (adapter, handle, thread_state) = start_codex_test_session(Some(runtime_state_root)).await;
    thread_state
        .persist_thread_id("thr_1")
        .expect("persist thread id");
    let transport = FakeAppServerTransport::new(vec![
        json!({"id": 1, "result": {"turn": {"id": "turn_1"}}}),
        json!({
            "type": "event_msg",
            "payload": {
                "type": "image_generation_end",
                "call_id": "ig_1",
                "status": "generating",
                "saved_path": "/runtime/../outside.png"
            }
        }),
        json!({"method": "turn/completed", "params": {"threadId": "thr_1", "turnId": "turn_1"}}),
    ]);
    let mut client = CodexAppServerClient::new(transport);
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

    let response = client
        .request("turn/start", json!({}), &event_tx, &thread_state)
        .await
        .expect("turn start");
    client
        .wait_for_turn_completed(
            super::extract_app_server_turn_id(&response).as_deref(),
            Some("thr_1"),
            None,
            &event_tx,
            &thread_state,
            None,
        )
        .await
        .expect("turn completed");

    let artifacts = std::iter::from_fn(|| event_rx.try_recv().ok())
        .filter(|event| matches!(event, RuntimeEvent::Artifact { .. }))
        .count();
    assert_eq!(artifacts, 0);

    adapter.close(&handle).await.expect("close");
}

#[tokio::test]
async fn codex_app_server_image_generation_item_emits_runtime_artifact() {
    assert_image_generation_event_emits_runtime_artifact(json!({
        "method": "item/completed",
        "params": {
            "threadId": "thr_1",
            "turnId": "turn_1",
            "item": {
                "type": "imageGeneration",
                "id": "ig_1",
                "status": "completed"
            }
        }
    }))
    .await;
}

#[tokio::test]
async fn codex_app_server_response_item_image_generation_emits_runtime_artifact() {
    assert_image_generation_event_emits_runtime_artifact(json!({
        "type": "response_item",
        "payload": {
            "type": "image_generation_call",
            "id": "ig_1",
            "status": "completed",
            "result": "base64 omitted"
        }
    }))
    .await;
}

#[tokio::test]
async fn codex_app_server_image_generation_interim_update_does_not_dedupe_completed_path() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    let final_image = runtime_state_root
        .join("home")
        .join(".codex")
        .join("generated_images")
        .join("thr_1")
        .join("ig_1-final.png");
    std::fs::create_dir_all(final_image.parent().expect("final image parent"))
        .expect("create generated image dir");
    std::fs::write(&final_image, b"png").expect("write final generated image");
    let (adapter, handle, thread_state) = start_codex_test_session(Some(runtime_state_root)).await;
    thread_state
        .persist_thread_id("thr_1")
        .expect("persist thread id");
    let transport = FakeAppServerTransport::new(vec![
        json!({"id": 1, "result": {"turn": {"id": "turn_1"}}}),
        json!({
            "method": "item/updated",
            "params": {
                "threadId": "thr_1",
                "turnId": "turn_1",
                "item": {
                    "type": "imageGeneration",
                    "id": "ig_1",
                    "status": "generating"
                }
            }
        }),
        json!({
            "method": "item/updated",
            "params": {
                "threadId": "thr_1",
                "turnId": "turn_1",
                "item": {
                    "type": "imageGeneration",
                    "id": "ig_1",
                    "status": "completed",
                    "savedPath": "/runtime/home/.codex/generated_images/thr_1/ig_1-final.png"
                }
            }
        }),
        json!({"method": "turn/completed", "params": {"threadId": "thr_1", "turnId": "turn_1"}}),
    ]);
    let mut client = CodexAppServerClient::new(transport);
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

    let response = client
        .request("turn/start", json!({}), &event_tx, &thread_state)
        .await
        .expect("turn start");
    client
        .wait_for_turn_completed(
            super::extract_app_server_turn_id(&response).as_deref(),
            Some("thr_1"),
            None,
            &event_tx,
            &thread_state,
            None,
        )
        .await
        .expect("turn completed");

    let artifacts = std::iter::from_fn(|| event_rx.try_recv().ok())
        .filter_map(|event| match event {
            RuntimeEvent::Artifact { artifact } => Some(artifact),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0].artifact_id, "codex:image:thr_1:ig_1");
    assert_eq!(artifacts[0].filename.as_deref(), Some("ig_1-final.png"));
    assert_eq!(artifacts[0].path, final_image);

    adapter.close(&handle).await.expect("close");
}

#[tokio::test]
async fn codex_app_server_protocol_resumes_saved_thread_id() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    std::fs::write(
        runtime_state_root.join(CODEX_THREAD_ID_STATE_FILE),
        "thr_saved\n",
    )
    .expect("write thread id");

    let (adapter, handle, thread_state) = start_codex_ready_test_session(runtime_state_root).await;
    let transport = FakeAppServerTransport::new(vec![
        json!({"id": 1, "result": {"thread": {"id": "thr_saved"}}}),
    ]);
    let sent = transport.sent.clone();
    let mut client = CodexAppServerClient::new(transport);
    let (event_tx, _event_rx) = tokio::sync::mpsc::unbounded_channel();

    let thread_id = adapter
        .ensure_app_server_thread(
            &mut client,
            &handle.runtime_session_id,
            &event_tx,
            &thread_state,
        )
        .await
        .expect("resume thread");

    assert_eq!(thread_id, "thr_saved");
    let sent = sent.lock().expect("sent lock").clone();
    assert_eq!(sent[0]["method"], "thread/resume");
    assert_eq!(sent[0]["params"]["threadId"], "thr_saved");

    adapter.close(&handle).await.expect("close");
}

#[tokio::test]
async fn codex_app_server_unmatched_terminal_notifications_complete_current_waiter() {
    let (adapter, handle, thread_state) = start_codex_test_session(None).await;
    let transport = FakeAppServerTransport::new(vec![
        json!({"id": 1, "result": {"turn": {"id": "turn_1"}}}),
        json!({
            "method": "turn/completed",
            "params": {
                "threadId": "thr_1",
                "turn": {"status": "completed"}
            }
        }),
    ]);
    let mut client = CodexAppServerClient::new(transport);
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

    let response = client
        .request("turn/start", json!({}), &event_tx, &thread_state)
        .await
        .expect("turn start");
    client
        .wait_for_turn_completed(
            super::extract_app_server_turn_id(&response).as_deref(),
            None,
            None,
            &event_tx,
            &thread_state,
            None,
        )
        .await
        .expect("id-less completion should complete the active turn");

    let events: Vec<RuntimeEvent> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
    assert!(events
        .iter()
        .any(|event| matches!(event, RuntimeEvent::Done)));

    adapter.close(&handle).await.expect("close");
}

#[tokio::test]
async fn codex_app_server_compaction_uses_context_compaction_contract_fixture() {
    let fixture = codex_protocol_fixture("compact_context_compaction_v2");
    let (method, params) = fixture_client_request(&fixture);
    let (adapter, handle, thread_state) = start_codex_test_session(None).await;
    let transport = FakeAppServerTransport::new(fixture_server_messages(&fixture));
    let sent = transport.sent.clone();
    let mut client = CodexAppServerClient::new(transport);
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let (interrupt_tx, mut interrupt_rx) = tokio::sync::mpsc::unbounded_channel();

    client
        .request(method, params, &event_tx, &thread_state)
        .await
        .expect("compact start request");
    client
        .wait_for_context_compaction_completed(
            "thr_1",
            interrupt_tx,
            &mut interrupt_rx,
            &event_tx,
            &thread_state,
        )
        .await
        .expect("context compaction should complete through turn notifications");

    let sent = sent.lock().expect("sent lock").clone();
    assert_eq!(sent, vec![fixture_client_message(&fixture)]);

    let events: Vec<RuntimeEvent> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeEvent::Status { text, .. } if text == "codex context compacted"
    )));
    assert!(events
        .iter()
        .any(|event| matches!(event, RuntimeEvent::Done)));

    adapter.close(&handle).await.expect("close");
}

/// Translate a fixture's server notifications into a canonical turn journal,
/// tagging every event with the raw app-server message it came from. The
/// JSON-RPC response is consumed by `request` in the real flow, not
/// translated, so it is skipped here. Returns the client too, so callers can
/// inspect recorded turn state (e.g. failures) the journal does not carry.
async fn codex_fixture_canonical_journal(
    fixture_name: &str,
) -> (Vec<TurnEvent>, CodexAppServerClient<FakeAppServerTransport>) {
    let fixture = codex_protocol_fixture(fixture_name);
    let (_adapter, _handle, thread_state) = start_codex_test_session(None).await;
    let mut client = CodexAppServerClient::new(FakeAppServerTransport::new(Vec::new()));
    let mut journal = Vec::new();
    for message in fixture_server_messages(&fixture) {
        if response_id(&message).is_some() {
            continue;
        }
        let raw = RawTurnPayload {
            driver: "codex-app-server".to_string(),
            payload: message.to_string(),
        };
        let events = client
            .handle_message(message, &thread_state)
            .await
            .expect("handle message");
        journal.extend(
            events
                .into_iter()
                .map(|event| TurnEvent::with_raw(event, raw.clone())),
        );
    }
    (journal, client)
}

#[tokio::test]
async fn codex_app_server_turn_journals_project_to_canonical_events() {
    let (successful, _) = codex_fixture_canonical_journal("successful_turn_v2").await;
    assert_eq!(
        canonical_events(&successful).cloned().collect::<Vec<_>>(),
        vec![
            RuntimeEvent::Status {
                code: None,
                text: "codex turn started".to_string(),
            },
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text: "Hello ".to_string(),
            },
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text: "world!".to_string(),
            },
            RuntimeEvent::Status {
                code: None,
                text: "codex turn completed".to_string(),
            },
            RuntimeEvent::Done,
        ],
    );
    assert!(successful.iter().all(|record| record
        .raw
        .as_ref()
        .is_some_and(|raw| raw.driver == "codex-app-server")));

    let (compaction, _) = codex_fixture_canonical_journal("compact_context_compaction_v2").await;
    assert_eq!(
        canonical_events(&compaction).cloned().collect::<Vec<_>>(),
        vec![
            RuntimeEvent::Status {
                code: None,
                text: "codex turn started".to_string(),
            },
            RuntimeEvent::Status {
                code: None,
                text: "codex context compaction started".to_string(),
            },
            RuntimeEvent::Status {
                code: None,
                text: "codex context compacted".to_string(),
            },
            RuntimeEvent::Status {
                code: None,
                text: "codex turn completed".to_string(),
            },
            RuntimeEvent::Done,
        ],
    );

    // An interrupted turn records a failure instead of emitting events, so
    // its canonical journal is empty and the interruption is recorded as a
    // turn failure (an empty journal alone would not distinguish a recorded
    // interruption from the notification being ignored).
    let (interrupted, client) = codex_fixture_canonical_journal("turn_interrupt_v2").await;
    assert!(interrupted.is_empty());
    assert_eq!(
        client.turn_failure(Some("turn_1")),
        Some("codex turn interrupted")
    );
}

#[tokio::test]
async fn codex_app_server_successful_turn_uses_contract_fixture() {
    let fixture = codex_protocol_fixture("successful_turn_v2");
    let (method, params) = fixture_client_request(&fixture);
    let (adapter, handle, thread_state) = start_codex_test_session(None).await;
    let transport = FakeAppServerTransport::new(fixture_server_messages(&fixture));
    let sent = transport.sent.clone();
    let mut client = CodexAppServerClient::new(transport);
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

    let response = client
        .request(method, params, &event_tx, &thread_state)
        .await
        .expect("turn/start request");
    let turn_id = extract_app_server_turn_id(&response);
    assert_eq!(turn_id.as_deref(), Some("turn_1"));
    client
        .wait_for_turn_completed(
            turn_id.as_deref(),
            Some("thr_1"),
            None,
            &event_tx,
            &thread_state,
            None,
        )
        .await
        .expect("turn should complete through notifications");

    let sent = sent.lock().expect("sent lock").clone();
    assert_eq!(sent, vec![fixture_client_message(&fixture)]);

    let events: Vec<RuntimeEvent> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
    assert_eq!(
        events,
        vec![
            RuntimeEvent::Status {
                code: None,
                text: "codex turn started".to_string(),
            },
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text: "Hello ".to_string(),
            },
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text: "world!".to_string(),
            },
            RuntimeEvent::Status {
                code: None,
                text: "codex turn completed".to_string(),
            },
            RuntimeEvent::Done,
        ],
    );
    assert_eq!(answer_text_from_events(events), "Hello world!");

    adapter.close(&handle).await.expect("close");
}

#[tokio::test]
async fn codex_app_server_turn_interrupt_uses_contract_fixture() {
    let fixture = codex_protocol_fixture("turn_interrupt_v2");
    let (adapter, handle, thread_state) = start_codex_test_session(None).await;
    let transport = FakeAppServerTransport::new(fixture_server_messages(&fixture));
    let sent = transport.sent.clone();
    let mut client = CodexAppServerClient::new(transport);
    let (event_tx, _event_rx) = tokio::sync::mpsc::unbounded_channel();

    client
        .interrupt_turn("thr_1", "turn_1", &event_tx, &thread_state)
        .await
        .expect("turn interrupt should accept interrupted terminal notification");

    let sent = sent.lock().expect("sent lock").clone();
    assert_eq!(sent, vec![fixture_client_message(&fixture)]);

    adapter.close(&handle).await.expect("close");
}

#[tokio::test]
async fn codex_cancel_interrupts_active_app_server_turn() {
    let (adapter, handle, thread_state) = start_codex_test_session(None).await;
    let (interrupt_tx, mut interrupt_rx) = tokio::sync::mpsc::unbounded_channel();
    thread_state
        .set_active_turn("thr_1", "turn_1", interrupt_tx)
        .expect("set active turn");

    let wait_task = tokio::spawn(async move {
        let request = interrupt_rx.recv().await.expect("interrupt request");
        request.ack_tx.send(Ok(())).expect("ack interrupt");
    });

    adapter
        .cancel(&handle, Some("test timeout".to_string()))
        .await
        .expect("cancel sends native Codex interrupt");
    wait_task.await.expect("wait task joins");

    adapter.close(&handle).await.expect("close");
}

#[tokio::test]
async fn codex_turn_started_notification_registers_active_cancel_target() {
    let (adapter, handle, thread_state) = start_codex_test_session(None).await;
    let transport = FakeAppServerTransport::new(Vec::new());
    let mut client = CodexAppServerClient::new(transport);
    let (event_tx, _event_rx) = tokio::sync::mpsc::unbounded_channel();
    let (interrupt_tx, mut interrupt_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut registered_turn_id = None;

    client
        .dispatch_message(
            json!({
                "method": "turn/started",
                "params": {
                    "threadId": "thr_1",
                    "turnId": "turn_1"
                }
            }),
            &event_tx,
            &thread_state,
        )
        .await
        .expect("turn started notification");
    client
        .register_active_wait_turn(
            Some("thr_1"),
            None,
            Some(&interrupt_tx),
            &mut registered_turn_id,
            &thread_state,
        )
        .expect("register active wait turn");

    assert_eq!(registered_turn_id.as_deref(), Some("turn_1"));
    let wait_task = tokio::spawn(async move {
        let request = interrupt_rx.recv().await.expect("interrupt request");
        request.ack_tx.send(Ok(())).expect("ack interrupt");
    });

    adapter
        .cancel(&handle, Some("test timeout".to_string()))
        .await
        .expect("cancel sends native Codex interrupt");
    wait_task.await.expect("wait task joins");

    adapter.close(&handle).await.expect("close");
}

#[tokio::test]
async fn codex_app_server_failed_turn_completion_returns_error() {
    let (adapter, handle, thread_state) = start_codex_test_session(None).await;
    let transport = FakeAppServerTransport::new(vec![
        json!({"id": 1, "result": {"turn": {"id": "turn_1"}}}),
        json!({
            "method": "turn/completed",
            "params": {
                "threadId": "thr_1",
                "turn": {
                    "id": "turn_1",
                    "status": "failed",
                    "error": {"message": "boom from model"}
                }
            }
        }),
    ]);
    let mut client = CodexAppServerClient::new(transport);
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

    let response = client
        .request("turn/start", json!({}), &event_tx, &thread_state)
        .await
        .expect("turn start");
    let err = client
        .wait_for_turn_completed(
            super::extract_app_server_turn_id(&response).as_deref(),
            None,
            None,
            &event_tx,
            &thread_state,
            None,
        )
        .await
        .expect_err("failed turn should be returned as an adapter error");

    assert!(err.to_string().contains("boom from model"));
    let events: Vec<RuntimeEvent> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
    assert!(!events
        .iter()
        .any(|event| matches!(event, RuntimeEvent::Done)));

    adapter.close(&handle).await.expect("close");
}

#[tokio::test]
async fn codex_app_server_unmatched_terminal_error_returns_error() {
    let (adapter, handle, thread_state) = start_codex_test_session(None).await;
    let transport = FakeAppServerTransport::new(vec![
        json!({"id": 1, "result": {"turn": {"id": "turn_1"}}}),
        json!({
            "method": "error",
            "params": {
                "threadId": "thr_1",
                "error": {"message": "global app-server failure"},
                "willRetry": false
            }
        }),
    ]);
    let mut client = CodexAppServerClient::new(transport);
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

    let response = client
        .request("turn/start", json!({}), &event_tx, &thread_state)
        .await
        .expect("turn start");
    let err = client
        .wait_for_turn_completed(
            super::extract_app_server_turn_id(&response).as_deref(),
            None,
            None,
            &event_tx,
            &thread_state,
            None,
        )
        .await
        .expect_err("unmatched terminal errors should fail a specific turn");

    assert!(err.to_string().contains("global app-server failure"));
    let events: Vec<RuntimeEvent> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
    assert!(!events
        .iter()
        .any(|event| matches!(event, RuntimeEvent::Done)));

    adapter.close(&handle).await.expect("close");
}

#[tokio::test]
async fn codex_app_server_retryable_error_notification_does_not_fail_turn() {
    let (adapter, handle, thread_state) = start_codex_test_session(None).await;
    let transport = FakeAppServerTransport::new(vec![
        json!({"id": 1, "result": {"turn": {"id": "turn_1"}}}),
        json!({
            "method": "error",
            "params": {
                "threadId": "thr_1",
                "turnId": "turn_1",
                "message": "temporary overload",
                "willRetry": true
            }
        }),
        json!({
            "method": "turn/completed",
            "params": {
                "threadId": "thr_1",
                "turn": {
                    "id": "turn_1",
                    "status": "completed"
                }
            }
        }),
    ]);
    let mut client = CodexAppServerClient::new(transport);
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

    let response = client
        .request("turn/start", json!({}), &event_tx, &thread_state)
        .await
        .expect("turn start");
    client
        .wait_for_turn_completed(
            super::extract_app_server_turn_id(&response).as_deref(),
            None,
            None,
            &event_tx,
            &thread_state,
            None,
        )
        .await
        .expect("retryable error should not fail a completed turn");

    let events: Vec<RuntimeEvent> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeEvent::Status { code: Some(code), text }
            if code == "runtime.retrying" && text == "temporary overload"
    )));
    assert!(!events
        .iter()
        .any(|event| matches!(event, RuntimeEvent::Error { .. })));
    assert!(events
        .iter()
        .any(|event| matches!(event, RuntimeEvent::Done)));

    adapter.close(&handle).await.expect("close");
}

#[tokio::test]
async fn codex_app_server_protocol_rejects_internal_approval_callbacks() {
    let (adapter, handle, thread_state) = start_codex_test_session(None).await;
    let transport = FakeAppServerTransport::new(vec![
        json!({"id": 1, "result": {}}),
        json!({"id": "approval-1", "method": "item/commandExecution/requestApproval", "params": {}}),
        json!({"method": "turn/completed", "params": {"turnId": "turn_1"}}),
    ]);
    let sent = transport.sent.clone();
    let mut client = CodexAppServerClient::new(transport);
    let (event_tx, _event_rx) = tokio::sync::mpsc::unbounded_channel();

    let response = client
        .request("turn/start", json!({}), &event_tx, &thread_state)
        .await
        .expect("turn start");
    client
        .wait_for_turn_completed(
            super::extract_app_server_turn_id(&response).as_deref(),
            None,
            None,
            &event_tx,
            &thread_state,
            None,
        )
        .await
        .expect("turn completed");

    let sent = sent.lock().expect("sent lock").clone();
    assert!(sent.iter().any(|message| {
        message.get("id").and_then(Value::as_str) == Some("approval-1")
            && omits_jsonrpc_header(message)
            && message.pointer("/error/code").and_then(Value::as_i64) == Some(-32601)
            && message
                .pointer("/error/message")
                .and_then(Value::as_str)
                .is_some_and(|message| message.contains("LionClaw policy"))
    }));

    adapter.close(&handle).await.expect("close");
}

#[tokio::test]
async fn finish_app_server_session_shuts_down_after_protocol_error() {
    let transport = FakeAppServerTransport::new(Vec::new());
    let shutdowns = transport.shutdowns.clone();
    let client = CodexAppServerClient::new(transport);

    let err =
        super::finish_app_server_session::<_, ()>(client, Err(anyhow::anyhow!("protocol failed")))
            .await
            .expect_err("protocol error should be returned");

    assert!(err.to_string().contains("protocol failed"));
    assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn missing_or_invalid_thread_file_starts_fresh_codex_thread() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    std::fs::write(runtime_state_root.join(CODEX_THREAD_ID_STATE_FILE), "\n")
        .expect("write invalid thread id");

    let (adapter, handle, _) = start_codex_test_session(Some(runtime_state_root)).await;
    assert!(!handle.resumes_existing_session);

    adapter.close(&handle).await.expect("close");
}

#[tokio::test]
async fn saved_thread_file_without_ready_marker_starts_fresh_codex_thread() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    std::fs::write(
        runtime_state_root.join(CODEX_THREAD_ID_STATE_FILE),
        "thread-old\n",
    )
    .expect("write thread id");

    let (adapter, handle, _) = start_codex_test_session(Some(runtime_state_root)).await;
    assert!(!handle.resumes_existing_session);

    adapter.close(&handle).await.expect("close");
}

#[tokio::test]
async fn symlinked_thread_file_is_rejected() {
    use std::os::unix::fs::symlink;

    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_state_root = temp_dir.path().join("runtime-state");
    std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
    let target = temp_dir.path().join("thread-id-target");
    std::fs::write(&target, "thread-old\n").expect("write target");
    symlink(&target, runtime_state_root.join(CODEX_THREAD_ID_STATE_FILE)).expect("create symlink");

    let adapter = CodexRuntimeAdapter::new(CodexRuntimeConfig::default());
    let runtime_session_ready = mark_runtime_ready(&runtime_state_root);
    let err = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            runtime_skill_ids: Vec::new(),
            runtime_state_root: Some(runtime_state_root),
            runtime_session_ready,
        })
        .await
        .expect_err("symlinked thread state should fail");
    assert!(err.to_string().contains("cannot be a symlink"));
}

#[tokio::test]
async fn different_lionclaw_sessions_do_not_share_codex_thread_ids() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let runtime_a = temp_dir.path().join("runtime-a");
    let runtime_b = temp_dir.path().join("runtime-b");
    std::fs::create_dir_all(&runtime_a).expect("create runtime a");
    std::fs::create_dir_all(&runtime_b).expect("create runtime b");
    std::fs::write(runtime_a.join(CODEX_THREAD_ID_STATE_FILE), "thread-a\n")
        .expect("write thread a");
    std::fs::write(runtime_b.join(CODEX_THREAD_ID_STATE_FILE), "thread-b\n")
        .expect("write thread b");

    let adapter = CodexRuntimeAdapter::new(CodexRuntimeConfig::default());
    let runtime_a_ready = mark_runtime_ready(&runtime_a);
    let runtime_b_ready = mark_runtime_ready(&runtime_b);
    let handle_a = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            runtime_skill_ids: Vec::new(),
            runtime_state_root: Some(runtime_a),
            runtime_session_ready: runtime_a_ready,
        })
        .await
        .expect("start a");
    let handle_b = adapter
        .session_start(RuntimeSessionStartInput {
            session_id: Uuid::new_v4(),
            working_dir: None,
            environment: Vec::new(),
            runtime_skill_ids: Vec::new(),
            runtime_state_root: Some(runtime_b),
            runtime_session_ready: runtime_b_ready,
        })
        .await
        .expect("start b");

    assert_eq!(
        adapter
            .current_thread_id(&handle_a.runtime_session_id)
            .expect("thread a"),
        Some("thread-a".to_string())
    );
    assert_eq!(
        adapter
            .current_thread_id(&handle_b.runtime_session_id)
            .expect("thread b"),
        Some("thread-b".to_string())
    );

    adapter.close(&handle_a).await.expect("close a");
    adapter.close(&handle_b).await.expect("close b");
}

async fn start_codex_ready_test_session(
    runtime_state_root: PathBuf,
) -> (
    CodexRuntimeAdapter,
    RuntimeSessionHandle,
    super::CodexThreadState,
) {
    let adapter = CodexRuntimeAdapter::new(CodexRuntimeConfig::default());
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
    let thread_state = adapter.thread_state_for(&handle.runtime_session_id);
    (adapter, handle, thread_state)
}
