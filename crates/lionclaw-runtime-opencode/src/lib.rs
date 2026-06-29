#![cfg_attr(
    not(test),
    warn(
        clippy::allow_attributes_without_reason,
        clippy::clone_on_ref_ptr,
        clippy::expect_used,
        clippy::future_not_send,
        clippy::get_unwrap,
        clippy::indexing_slicing,
        clippy::large_futures,
        clippy::large_stack_arrays,
        clippy::large_types_passed_by_value,
        clippy::let_underscore_must_use,
        clippy::mutex_atomic,
        clippy::mutex_integer,
        clippy::panic,
        clippy::panic_in_result_fn,
        clippy::pathbuf_init_then_push,
        clippy::rc_buffer,
        clippy::rc_mutex,
        clippy::redundant_clone,
        clippy::same_name_method,
        clippy::significant_drop_in_scrutinee,
        clippy::significant_drop_tightening,
        clippy::uninlined_format_args,
        clippy::unused_result_ok,
        clippy::unwrap_in_result,
        clippy::unwrap_used,
        reason = "production code follows LionClaw's strict Clippy profile; tests keep fail-fast ergonomics"
    )
)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::time::{timeout_at, Instant};
use tracing::warn;
use uuid::Uuid;

use lionclaw_runtime_api::{
    choose_terminal_transcript_target, load_ready_state_value, load_state_value,
    normalize_terminal_transcript_launch_started_at, save_state_value, ConversationDriver,
    ExecutionOutput, RawTurnPayload, RuntimeAdapter, RuntimeAdapterInfo, RuntimeCapabilityResult,
    RuntimeEvent, RuntimeEventSender, RuntimeMessageLane, RuntimeProgramExecutor,
    RuntimeProgramSession, RuntimeProgramSpec, RuntimeProgramTurnExecution, RuntimeSessionHandle,
    RuntimeSessionReady, RuntimeSessionStartInput, RuntimeTerminalProgramInput,
    RuntimeTerminalTranscript, RuntimeTerminalTranscriptInput,
    RuntimeTerminalTranscriptProgramExecutor, RuntimeTerminalTranscriptWarning,
    RuntimeTerminalTurn, RuntimeTerminalTurnStatus, RuntimeTurnInput, RuntimeTurnJournalSender,
    RuntimeTurnMode, RuntimeTurnResult, TerminalTranscriptCandidate, TerminalTranscriptTarget,
    TerminalTranscriptTimestampPrecision, TurnEvent,
};

const OPENCODE_SESSION_ID_STATE_FILE: &str = ".lionclaw-opencode-session-id";
const OPENCODE_RUNTIME_CONFIG_PATH: &str = "/runtime/opencode.generated.json";
const OPENCODE_ACP_DRIVER: &str = "opencode-acp";
const OPENCODE_ACP_WORKSPACE_PATH: &str = "/workspace";

#[derive(Debug, Clone)]
pub struct OpenCodeRuntimeConfig {
    pub executable: String,
    pub model: Option<String>,
    pub agent: Option<String>,
}

impl Default for OpenCodeRuntimeConfig {
    fn default() -> Self {
        Self {
            executable: "opencode".to_string(),
            model: None,
            agent: None,
        }
    }
}

#[derive(Debug)]
pub struct OpenCodeRuntimeAdapter {
    config: OpenCodeRuntimeConfig,
    sessions: Arc<RwLock<HashMap<String, OpenCodeSessionState>>>,
}

#[derive(Debug, Clone)]
struct OpenCodeSessionState {
    runtime_state_root: Option<PathBuf>,
    session_id: Option<String>,
}

impl OpenCodeRuntimeAdapter {
    pub fn new(config: OpenCodeRuntimeConfig) -> Self {
        Self {
            config,
            sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl RuntimeAdapter for OpenCodeRuntimeAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "opencode".to_string(),
            version: "0.1".to_string(),
            healthy: !self.config.executable.trim().is_empty(),
        }
    }

    fn turn_mode(&self) -> RuntimeTurnMode {
        RuntimeTurnMode::ProgramBacked
    }

    async fn session_start(&self, input: RuntimeSessionStartInput) -> Result<RuntimeSessionHandle> {
        let runtime_session_id = format!("opencode-{}", Uuid::new_v4());
        let session_id = match input.runtime_state_root.as_deref() {
            Some(root) => load_ready_opencode_session_id(root, input.runtime_session_ready)?,
            None => None,
        };
        let resumes_existing_session = session_id.is_some();
        self.sessions
            .write()
            .map_err(|_| anyhow!("opencode runtime session state lock poisoned"))?
            .insert(
                runtime_session_id.clone(),
                OpenCodeSessionState {
                    runtime_state_root: input.runtime_state_root,
                    session_id,
                },
            );

        Ok(RuntimeSessionHandle {
            runtime_session_id,
            resumes_existing_session,
        })
    }

    async fn program_backed_turn(
        &self,
        execution: RuntimeProgramTurnExecution,
        journal: RuntimeTurnJournalSender,
    ) -> Result<RuntimeTurnResult> {
        let RuntimeProgramTurnExecution {
            input, executor, ..
        } = execution;
        let mut driver = OpenCodeAcpConversationDriver {
            config: self.config.clone(),
            sessions: Arc::clone(&self.sessions),
            executor,
        };
        driver.run_turn(input, journal).await
    }

    fn build_terminal_program(
        &self,
        input: RuntimeTerminalProgramInput,
    ) -> Result<RuntimeProgramSpec> {
        Ok(build_opencode_terminal_program(
            &self.config,
            load_ready_opencode_session_id(&input.runtime_state_root, input.runtime_session_ready)?,
        ))
    }

    async fn export_terminal_transcript(
        &self,
        input: RuntimeTerminalTranscriptInput,
        executor: &mut dyn RuntimeTerminalTranscriptProgramExecutor,
    ) -> Result<RuntimeTerminalTranscript> {
        let hard_timeout = executor.hard_timeout();
        let deadline = Instant::now() + hard_timeout;
        export_opencode_terminal_transcript_with_cli(
            &self.config,
            input,
            executor,
            deadline,
            hard_timeout,
        )
        .await
    }

    async fn resolve_capability_requests(
        &self,
        _handle: &RuntimeSessionHandle,
        results: Vec<RuntimeCapabilityResult>,
        events: RuntimeEventSender,
    ) -> Result<()> {
        if !results.is_empty() {
            return Err(anyhow!(
                "opencode adapter does not support runtime-side capability request resolution"
            ));
        }
        drop(events.send(RuntimeEvent::Done));
        Ok(())
    }

    async fn cancel(&self, _handle: &RuntimeSessionHandle, _reason: Option<String>) -> Result<()> {
        Ok(())
    }

    async fn close(&self, handle: &RuntimeSessionHandle) -> Result<()> {
        self.sessions
            .write()
            .map_err(|_| anyhow!("opencode runtime session state lock poisoned"))?
            .remove(&handle.runtime_session_id);
        Ok(())
    }
}

struct OpenCodeAcpConversationDriver {
    config: OpenCodeRuntimeConfig,
    sessions: Arc<RwLock<HashMap<String, OpenCodeSessionState>>>,
    executor: Box<dyn RuntimeProgramExecutor>,
}

#[async_trait]
impl ConversationDriver for OpenCodeAcpConversationDriver {
    fn protocol_name(&self) -> &'static str {
        OPENCODE_ACP_DRIVER
    }

    async fn run_turn(
        &mut self,
        input: RuntimeTurnInput,
        journal: RuntimeTurnJournalSender,
    ) -> Result<RuntimeTurnResult> {
        let runtime_session_id = input.runtime_session_id.clone();
        let session_state = get_runtime_session(&self.sessions, &runtime_session_id)?;
        let program = build_opencode_acp_program(&self.config);
        let session = self.executor.spawn(program).await?;
        let mut client = OpenCodeAcpClient::new(session);

        let result = async {
            client.initialize().await?;
            let session_id = client
                .ensure_session(&self.sessions, &runtime_session_id, &session_state)
                .await?;
            client.configure_session(&self.config, &session_id).await?;
            client.prompt(&session_id, &input.prompt, &journal).await?;
            Ok(RuntimeTurnResult::default())
        }
        .await;

        finish_opencode_acp_session(client, result).await
    }
}

fn build_opencode_acp_program(config: &OpenCodeRuntimeConfig) -> RuntimeProgramSpec {
    RuntimeProgramSpec {
        executable: config.executable.clone(),
        args: vec!["acp".to_string()],
        environment: opencode_runtime_environment(),
        stdin: String::new(),
        auth: None,
    }
}

struct OpenCodeAcpClient {
    session: Option<Box<dyn RuntimeProgramSession>>,
    next_id: u64,
}

#[derive(Debug, Clone)]
struct OpenCodeAcpMessage {
    raw: String,
    value: Value,
}

#[derive(Debug, Clone)]
struct OpenCodeAcpResponse {
    raw: String,
    result: Value,
}

impl OpenCodeAcpClient {
    fn new(session: Box<dyn RuntimeProgramSession>) -> Self {
        Self {
            session: Some(session),
            next_id: 1,
        }
    }

    async fn initialize(&mut self) -> Result<()> {
        self.request(
            "initialize",
            json!({
                "protocolVersion": 1,
                "clientCapabilities": {
                    "fs": {
                        "readTextFile": false,
                        "writeTextFile": false,
                    },
                    "terminal": false,
                },
            }),
            None,
        )
        .await?;
        Ok(())
    }

    async fn ensure_session(
        &mut self,
        sessions: &RwLock<HashMap<String, OpenCodeSessionState>>,
        runtime_session_id: &str,
        session_state: &OpenCodeSessionState,
    ) -> Result<String> {
        if let Some(session_id) = session_state.session_id.as_deref() {
            self.request(
                "session/load",
                json!({
                    "sessionId": session_id,
                    "cwd": OPENCODE_ACP_WORKSPACE_PATH,
                    "mcpServers": [],
                }),
                None,
            )
            .await?;
            return Ok(session_id.to_string());
        }

        let response = self
            .request(
                "session/new",
                json!({
                    "cwd": OPENCODE_ACP_WORKSPACE_PATH,
                    "mcpServers": [],
                }),
                None,
            )
            .await?;
        let session_id = response
            .result
            .get("sessionId")
            .and_then(Value::as_str)
            .and_then(normalize_opencode_session_id)
            .context("OpenCode ACP session/new response is missing sessionId")?;
        remember_opencode_session_id(sessions, runtime_session_id, &session_id)?;

        Ok(session_id)
    }

    async fn configure_session(
        &mut self,
        config: &OpenCodeRuntimeConfig,
        session_id: &str,
    ) -> Result<()> {
        if let Some(model) = config.model.as_deref() {
            self.request(
                "session/set_model",
                json!({
                    "sessionId": session_id,
                    "modelId": model,
                }),
                None,
            )
            .await?;
        }

        if let Some(agent) = config.agent.as_deref() {
            self.request(
                "session/set_config_option",
                json!({
                    "sessionId": session_id,
                    "configId": "mode",
                    "value": agent,
                }),
                None,
            )
            .await?;
        }

        Ok(())
    }

    async fn prompt(
        &mut self,
        session_id: &str,
        prompt: &str,
        journal: &RuntimeTurnJournalSender,
    ) -> Result<()> {
        let response = self
            .request(
                "session/prompt",
                json!({
                    "sessionId": session_id,
                    "prompt": [{
                        "type": "text",
                        "text": prompt,
                    }],
                }),
                Some(journal),
            )
            .await?;
        drop(journal.send(TurnEvent::with_raw(
            RuntimeEvent::Done,
            RawTurnPayload {
                driver: OPENCODE_ACP_DRIVER.to_string(),
                payload: response.raw,
            },
        )));
        Ok(())
    }

    async fn request(
        &mut self,
        method: &str,
        params: Value,
        journal: Option<&RuntimeTurnJournalSender>,
    ) -> Result<OpenCodeAcpResponse> {
        let id = self.next_request_id();
        self.send(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }))
        .await?;

        loop {
            let Some(message) = self.recv().await? else {
                return Err(anyhow!("OpenCode ACP closed before responding to {method}"));
            };
            if opencode_acp_response_id(&message.value).is_some_and(|response_id| response_id == id)
            {
                return parse_opencode_acp_response(message, method);
            }
            self.dispatch_message(message, journal).await?;
        }
    }

    async fn dispatch_message(
        &mut self,
        message: OpenCodeAcpMessage,
        journal: Option<&RuntimeTurnJournalSender>,
    ) -> Result<()> {
        if opencode_acp_is_server_request(&message.value) {
            self.respond_to_server_request(&message.value).await?;
            return Ok(());
        }

        if let Some(journal) = journal {
            for record in opencode_acp_turn_events(&message) {
                drop(journal.send(record));
            }
        }
        Ok(())
    }

    async fn respond_to_server_request(&mut self, request: &Value) -> Result<()> {
        let id = request
            .get("id")
            .cloned()
            .context("OpenCode ACP server request is missing id")?;
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match method {
            "session/request_permission" => {
                self.send(&json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": opencode_acp_permission_denial(request.get("params")),
                }))
                .await
            }
            "fs/read_text_file" | "fs/write_text_file" => {
                self.send(&opencode_acp_error_response(
                    id,
                    -32000,
                    "LionClaw disables OpenCode ACP filesystem access",
                ))
                .await
            }
            _ => {
                self.send(&opencode_acp_error_response(
                    id,
                    -32601,
                    &format!("LionClaw does not support OpenCode ACP request '{method}'"),
                ))
                .await
            }
        }
    }

    async fn send(&mut self, message: &Value) -> Result<()> {
        let session = self
            .session
            .as_mut()
            .context("OpenCode ACP session is already closed")?;
        session.write_line(&serde_json::to_string(message)?).await
    }

    async fn recv(&mut self) -> Result<Option<OpenCodeAcpMessage>> {
        let session = self
            .session
            .as_mut()
            .context("OpenCode ACP session is already closed")?;
        loop {
            let Some(line) = session.read_line().await? else {
                return Ok(None);
            };
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let value = serde_json::from_str(trimmed)
                .with_context(|| format!("invalid OpenCode ACP JSON-RPC line: {trimmed}"))?;
            return Ok(Some(OpenCodeAcpMessage {
                raw: trimmed.to_string(),
                value,
            }));
        }
    }

    async fn shutdown(mut self) -> Result<ExecutionOutput> {
        let Some(session) = self.session.take() else {
            return Ok(ExecutionOutput::default());
        };
        session.shutdown().await
    }

    fn next_request_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

fn remember_opencode_session_id(
    sessions: &RwLock<HashMap<String, OpenCodeSessionState>>,
    runtime_session_id: &str,
    session_id: &str,
) -> Result<()> {
    let runtime_state_root =
        update_runtime_session_id(sessions, runtime_session_id, session_id.to_string())?;
    if let Some(root) = runtime_state_root.as_deref() {
        save_opencode_session_id(root, session_id)?;
    }
    Ok(())
}

fn opencode_acp_response_id(message: &Value) -> Option<u64> {
    if message.get("result").is_none() && message.get("error").is_none() {
        return None;
    }
    message.get("id").and_then(Value::as_u64)
}

fn opencode_acp_is_server_request(message: &Value) -> bool {
    message.get("method").and_then(Value::as_str).is_some()
        && message.get("id").is_some()
        && message.get("result").is_none()
        && message.get("error").is_none()
}

fn parse_opencode_acp_response(
    message: OpenCodeAcpMessage,
    method: &str,
) -> Result<OpenCodeAcpResponse> {
    if let Some(error) = message.value.get("error") {
        return Err(anyhow!(
            "OpenCode ACP {method} failed: {}",
            opencode_acp_error_text(error)
        ));
    }
    Ok(OpenCodeAcpResponse {
        raw: message.raw,
        result: message.value.get("result").cloned().unwrap_or(Value::Null),
    })
}

fn opencode_acp_error_text(error: &Value) -> String {
    let code = error.get("code").and_then(Value::as_i64);
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("unknown error");
    match code {
        Some(code) => format!("{code}: {message}"),
        None => message.to_string(),
    }
}

fn opencode_acp_error_response(id: Value, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message,
        },
    })
}

fn opencode_acp_permission_denial(params: Option<&Value>) -> Value {
    let reject_option = params
        .and_then(|params| params.get("options"))
        .and_then(Value::as_array)
        .and_then(|options| {
            options.iter().find_map(|option| {
                let kind = option
                    .get("kind")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let name = option
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if kind.contains("reject")
                    || kind.contains("deny")
                    || name.to_ascii_lowercase().contains("reject")
                    || name.to_ascii_lowercase().contains("deny")
                {
                    option
                        .get("optionId")
                        .or_else(|| option.get("id"))
                        .and_then(Value::as_str)
                } else {
                    None
                }
            })
        });

    match reject_option {
        Some(option_id) => json!({
            "outcome": {
                "outcome": "selected",
                "optionId": option_id,
            },
        }),
        None => json!({
            "outcome": {
                "outcome": "cancelled",
            },
        }),
    }
}

fn opencode_acp_turn_events(message: &OpenCodeAcpMessage) -> Vec<TurnEvent> {
    if message.value.get("method").and_then(Value::as_str) != Some("session/update") {
        return Vec::new();
    }
    let Some(update) = message
        .value
        .pointer("/params/update")
        .or_else(|| message.value.get("params"))
    else {
        return Vec::new();
    };
    let Some(session_update) = update.get("sessionUpdate").and_then(Value::as_str) else {
        return Vec::new();
    };

    let event = match session_update {
        "agent_message_chunk" => opencode_acp_content_text(update.get("content")).map(|text| {
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text,
            }
        }),
        "agent_thought_chunk" => opencode_acp_content_text(update.get("content")).map(|text| {
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Reasoning,
                text,
            }
        }),
        "tool_call" | "tool_call_update" => {
            opencode_acp_tool_status(update).map(|text| RuntimeEvent::Status { code: None, text })
        }
        _ => None,
    };

    event
        .map(|event| {
            vec![TurnEvent::with_raw(
                event,
                RawTurnPayload {
                    driver: OPENCODE_ACP_DRIVER.to_string(),
                    payload: message.raw.clone(),
                },
            )]
        })
        .unwrap_or_default()
}

fn opencode_acp_content_text(value: Option<&Value>) -> Option<String> {
    let text = match value {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| opencode_acp_content_text(Some(item)))
            .collect::<Vec<_>>()
            .join(""),
        Some(Value::Object(object)) => object
            .get("text")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| opencode_acp_content_text(object.get("content")))
            .or_else(|| opencode_acp_content_text(object.get("parts")))
            .unwrap_or_default(),
        _ => String::new(),
    };
    (!text.is_empty()).then_some(text)
}

fn opencode_acp_tool_status(update: &Value) -> Option<String> {
    let status = update
        .get("status")
        .and_then(Value::as_str)
        .filter(|status| !status.trim().is_empty())
        .unwrap_or("updated");
    let title = update
        .get("title")
        .or_else(|| update.get("kind"))
        .and_then(Value::as_str)
        .filter(|title| !title.trim().is_empty())
        .unwrap_or("tool");
    Some(format!("opencode tool {status}: {title}"))
}

async fn finish_opencode_acp_session<R>(client: OpenCodeAcpClient, result: Result<R>) -> Result<R> {
    let shutdown = client
        .shutdown()
        .await
        .and_then(ensure_opencode_acp_exit_success);

    match (result, shutdown) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(err)) => Err(err),
        (Err(err), Ok(())) => Err(err),
        (Err(err), Err(shutdown_err)) => {
            warn!(
                error = %shutdown_err,
                "OpenCode ACP shutdown failed after runtime error"
            );
            Err(err)
        }
    }
}

fn ensure_opencode_acp_exit_success(output: ExecutionOutput) -> Result<()> {
    if output.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if stderr.is_empty() {
        return Err(anyhow!(
            "OpenCode ACP exited with {}",
            output.status_description()
        ));
    }
    Err(anyhow!(
        "OpenCode ACP exited with {}: {stderr}",
        output.status_description()
    ))
}

fn build_opencode_terminal_program(
    config: &OpenCodeRuntimeConfig,
    session_id: Option<String>,
) -> RuntimeProgramSpec {
    let mut args = Vec::new();
    if let Some(session_id) = session_id {
        args.push("--session".to_string());
        args.push(session_id);
    }
    if let Some(model) = &config.model {
        args.push("--model".to_string());
        args.push(model.clone());
    }
    if let Some(agent) = &config.agent {
        args.push("--agent".to_string());
        args.push(agent.clone());
    }

    RuntimeProgramSpec {
        executable: config.executable.clone(),
        args,
        environment: opencode_terminal_environment(),
        stdin: String::new(),
        auth: None,
    }
}

fn build_opencode_session_list_program(config: &OpenCodeRuntimeConfig) -> RuntimeProgramSpec {
    RuntimeProgramSpec {
        executable: config.executable.clone(),
        args: vec![
            "session".to_string(),
            "list".to_string(),
            "--format".to_string(),
            "json".to_string(),
        ],
        environment: opencode_transcript_export_environment(),
        stdin: String::new(),
        auth: None,
    }
}

fn build_opencode_export_program(
    config: &OpenCodeRuntimeConfig,
    session_id: &str,
) -> RuntimeProgramSpec {
    RuntimeProgramSpec {
        executable: config.executable.clone(),
        args: vec!["export".to_string(), session_id.to_string()],
        environment: opencode_transcript_export_environment(),
        stdin: String::new(),
        auth: None,
    }
}

fn opencode_runtime_environment() -> Vec<(String, String)> {
    vec![("OPENCODE_DISABLE_AUTOUPDATE".to_string(), "1".to_string())]
}

fn opencode_terminal_environment() -> Vec<(String, String)> {
    let mut environment = opencode_runtime_environment();
    environment.push((
        "OPENCODE_CONFIG".to_string(),
        OPENCODE_RUNTIME_CONFIG_PATH.to_string(),
    ));
    environment
}

fn opencode_transcript_export_environment() -> Vec<(String, String)> {
    let mut environment = opencode_runtime_environment();
    environment.push(("OPENCODE_PURE".to_string(), "1".to_string()));
    environment
}

async fn export_opencode_terminal_transcript_with_cli(
    config: &OpenCodeRuntimeConfig,
    input: RuntimeTerminalTranscriptInput,
    executor: &mut dyn RuntimeTerminalTranscriptProgramExecutor,
    deadline: Instant,
    hard_timeout: std::time::Duration,
) -> Result<RuntimeTerminalTranscript> {
    let saved_session_id = load_saved_opencode_session_id(&input.runtime_state_root)?;
    let (listed_sessions, source_selection_reconciled, mut warnings) = match timeout_at(
        deadline,
        executor.execute(build_opencode_session_list_program(config)),
    )
    .await
    {
        Ok(Ok(output)) if output.success() => match parse_opencode_session_list(&output.stdout) {
            Ok(listed_sessions) => (listed_sessions, true, Vec::new()),
            Err(err) if saved_session_id.is_some() => (
                OpenCodeListedSessions::default(),
                false,
                vec![RuntimeTerminalTranscriptWarning::new(
                    "opencode-session-list",
                    err.to_string(),
                )],
            ),
            Err(err) => return Err(err),
        },
        Ok(Ok(output)) => {
            let err = opencode_program_error("session list", &output);
            if saved_session_id.is_none() {
                return Err(err);
            }
            (
                OpenCodeListedSessions::default(),
                false,
                vec![RuntimeTerminalTranscriptWarning::new(
                    "opencode-session-list",
                    err.to_string(),
                )],
            )
        }
        Ok(Err(err)) => {
            let err = anyhow!("failed to list OpenCode sessions through the OpenCode CLI: {err}");
            if saved_session_id.is_none() {
                return Err(err);
            }
            (
                OpenCodeListedSessions::default(),
                false,
                vec![RuntimeTerminalTranscriptWarning::new(
                    "opencode-session-list",
                    err.to_string(),
                )],
            )
        }
        Err(_) => {
            let err = anyhow!(
                "timed out after {}s while listing OpenCode sessions through the OpenCode CLI",
                hard_timeout.as_secs_f32()
            );
            if saved_session_id.is_none() {
                return Err(err);
            }
            (
                OpenCodeListedSessions::default(),
                false,
                vec![RuntimeTerminalTranscriptWarning::new(
                    "opencode-session-list",
                    err.to_string(),
                )],
            )
        }
    };

    let mut turns = Vec::new();
    let target_session_id = choose_terminal_transcript_target(
        saved_session_id.as_deref(),
        listed_sessions.latest.as_ref(),
        normalize_terminal_transcript_launch_started_at(
            input.launch_started_at,
            TerminalTranscriptTimestampPrecision::Milliseconds,
        ),
    );
    let mut target = TerminalTranscriptTarget::default();
    if let Some(session_id) = target_session_id.as_deref() {
        if target.choose_if_empty(session_id) {
            save_opencode_session_id(&input.runtime_state_root, session_id)?;
        }
    }

    for session_id in
        opencode_reconcile_session_ids(target_session_id.as_deref(), saved_session_id.as_deref())
    {
        if Instant::now() >= deadline {
            warnings.push(RuntimeTerminalTranscriptWarning::new(
                "opencode-session-list",
                "OpenCode transcript export deadline reached; returning partial transcript",
            ));
            break;
        }

        let export_output = match timeout_at(
            deadline,
            executor.execute(build_opencode_export_program(config, &session_id)),
        )
        .await
        {
            Ok(Ok(output)) => output,
            Ok(Err(err)) => {
                warnings.push(RuntimeTerminalTranscriptWarning::new(
                    format!("opencode-session:{session_id}"),
                    format!("failed to export OpenCode session through the OpenCode CLI: {err}"),
                ));
                continue;
            }
            Err(_) => {
                warnings.push(RuntimeTerminalTranscriptWarning::new(
                    format!("opencode-session:{session_id}"),
                    format!(
                        "timed out after {}s while exporting OpenCode session; returning partial transcript",
                        hard_timeout.as_secs_f32()
                    ),
                ));
                break;
            }
        };
        if !export_output.success() {
            warnings.push(RuntimeTerminalTranscriptWarning::new(
                format!("opencode-session:{session_id}"),
                opencode_program_error(&format!("export {session_id}"), &export_output).to_string(),
            ));
            continue;
        }
        match parse_opencode_export(&session_id, &export_output.stdout) {
            Ok(session_transcript) => {
                target.record_reconciliation(&session_id, true, session_transcript.resumable);
                turns.extend(session_transcript.turns);
            }
            Err(err) => warnings.push(RuntimeTerminalTranscriptWarning::new(
                format!("opencode-session:{session_id}"),
                err.to_string(),
            )),
        }
    }

    turns.sort_by(|left, right| {
        left.started_at
            .cmp(&right.started_at)
            .then_with(|| left.source_id.cmp(&right.source_id))
    });
    Ok(RuntimeTerminalTranscript::new(
        turns,
        warnings,
        target.transcript_state(source_selection_reconciled),
    ))
}

fn opencode_program_error(action: &str, output: &ExecutionOutput) -> anyhow::Error {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if stderr.is_empty() {
        anyhow!(
            "OpenCode CLI {action} exited with {}",
            output.status_description()
        )
    } else {
        anyhow!(
            "OpenCode CLI {action} exited with {}: {stderr}",
            output.status_description()
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct OpenCodeListedSessions {
    latest: Option<TerminalTranscriptCandidate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OpenCodeTerminalTranscript {
    turns: Vec<RuntimeTerminalTurn>,
    resumable: bool,
}

fn parse_opencode_session_list(stdout: &[u8]) -> Result<OpenCodeListedSessions> {
    let raw = String::from_utf8_lossy(stdout);
    if raw.trim().is_empty() {
        return Ok(OpenCodeListedSessions::default());
    }

    let sessions = serde_json::from_slice::<Vec<Value>>(stdout)
        .context("failed to parse OpenCode session list JSON")?;
    let latest = sessions
        .iter()
        .filter_map(opencode_session_list_candidate)
        .max_by(|left, right| {
            left.updated_at
                .cmp(&right.updated_at)
                .then_with(|| left.id.cmp(&right.id))
        });

    Ok(OpenCodeListedSessions { latest })
}

fn opencode_session_list_candidate(session: &Value) -> Option<TerminalTranscriptCandidate> {
    let id = session.get("id").and_then(Value::as_str)?;
    TerminalTranscriptCandidate::new(id, opencode_session_list_updated_at(session))
}

fn opencode_session_list_updated_at(session: &Value) -> Option<DateTime<Utc>> {
    let ms = session
        .get("updated")
        .and_then(Value::as_i64)
        .or_else(|| session.get("created").and_then(Value::as_i64))?;
    Utc.timestamp_millis_opt(ms).single()
}

fn opencode_reconcile_session_ids(
    target_session_id: Option<&str>,
    saved_session_id: Option<&str>,
) -> Vec<String> {
    let mut ids = Vec::new();
    for id in [target_session_id, saved_session_id].into_iter().flatten() {
        if !ids.iter().any(|existing| existing == id) {
            ids.push(id.to_string());
        }
    }
    ids
}

fn parse_opencode_export(
    requested_session_id: &str,
    stdout: &[u8],
) -> Result<OpenCodeTerminalTranscript> {
    let export = serde_json::from_slice::<OpenCodeExport>(stdout).with_context(|| {
        format!("failed to parse OpenCode export JSON for session {requested_session_id}")
    })?;
    let resumable_pair = opencode_resumable_message_pair(&export.messages)
        .map(|(user, assistant)| (user.info.id.as_str(), assistant.info.id.as_str()));
    let exported_session_id = export
        .info
        .id
        .as_deref()
        .filter(|id| !id.trim().is_empty())
        .unwrap_or(requested_session_id);
    let messages_by_id = export
        .messages
        .iter()
        .map(|message| (message.info.id.as_str(), message))
        .collect::<HashMap<_, _>>();
    let mut turns = Vec::new();
    let mut resumable = false;

    for assistant in export
        .messages
        .iter()
        .filter(|message| message.info.role == "assistant")
    {
        let Some(parent_id) = assistant
            .info
            .parent_id
            .as_deref()
            .filter(|id| !id.trim().is_empty())
        else {
            continue;
        };
        let Some(user) = messages_by_id.get(parent_id) else {
            continue;
        };
        if user.info.role != "user" {
            continue;
        }

        let Some(turn) = opencode_export_turn(exported_session_id, user, assistant)? else {
            continue;
        };
        if resumable_pair.is_some_and(|(user_id, assistant_id)| {
            user.info.id.as_str() == user_id && assistant.info.id.as_str() == assistant_id
        }) {
            resumable = true;
        }
        turns.push(turn);
    }

    Ok(OpenCodeTerminalTranscript { turns, resumable })
}

#[derive(Debug, Deserialize)]
struct OpenCodeExport {
    #[serde(default)]
    info: OpenCodeExportInfo,
    #[serde(default)]
    messages: Vec<OpenCodeExportMessage>,
}

#[derive(Debug, Default, Deserialize)]
struct OpenCodeExportInfo {
    id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OpenCodeExportMessage {
    info: OpenCodeExportMessageInfo,
    #[serde(default)]
    parts: Vec<OpenCodeExportPart>,
}

#[derive(Debug, Deserialize)]
struct OpenCodeExportMessageInfo {
    id: String,
    #[serde(default, rename = "sessionID")]
    session_id: Option<String>,
    role: String,
    time: OpenCodeExportMessageTime,
    #[serde(default, rename = "parentID")]
    parent_id: Option<String>,
    #[serde(default)]
    finish: Option<Value>,
    #[serde(default)]
    error: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct OpenCodeExportMessageTime {
    created: Option<i64>,
    completed: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct OpenCodeExportPart {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    ignored: bool,
    text: Option<String>,
    #[serde(default)]
    state: Option<Value>,
    #[serde(default)]
    metadata: Option<Value>,
}

fn opencode_export_turn(
    exported_session_id: &str,
    user: &OpenCodeExportMessage,
    assistant: &OpenCodeExportMessage,
) -> Result<Option<RuntimeTerminalTurn>> {
    let display_user_text = opencode_message_export_text(user);
    let assistant_text = opencode_message_export_text(assistant);
    if display_user_text.trim().is_empty() || assistant_text.trim().is_empty() {
        return Ok(None);
    }

    let session_id = assistant
        .info
        .session_id
        .as_deref()
        .filter(|id| !id.trim().is_empty())
        .unwrap_or(exported_session_id);
    let (error_code, error_text) = opencode_assistant_error(&assistant.info);

    Ok(Some(RuntimeTerminalTurn {
        source_id: format!(
            "opencode-export:{}:{}:{}",
            session_id, user.info.id, assistant.info.id
        ),
        prompt_user_text: display_user_text.clone(),
        display_user_text,
        assistant_text,
        status: opencode_assistant_status(assistant),
        error_code,
        error_text,
        started_at: opencode_message_time(user, false)?,
        finished_at: opencode_message_time(assistant, true)?,
    }))
}

fn opencode_message_export_text(message: &OpenCodeExportMessage) -> String {
    message
        .parts
        .iter()
        .filter(|part| part.kind == "text")
        .filter(|part| !part.ignored)
        .filter_map(|part| part.text.as_deref().filter(|text| !text.trim().is_empty()))
        .map(str::to_string)
        .collect::<Vec<_>>()
        .join("\n")
}

fn opencode_assistant_status(message: &OpenCodeExportMessage) -> RuntimeTerminalTurnStatus {
    let info = &message.info;
    if let Some(error) = &info.error {
        let name = error
            .get("name")
            .or_else(|| error.get("code"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        return if name.contains("abort") || name.contains("interrupt") {
            RuntimeTerminalTurnStatus::Interrupted
        } else {
            RuntimeTerminalTurnStatus::Failed
        };
    }

    if opencode_assistant_has_pending_tool_followup(message) {
        return RuntimeTerminalTurnStatus::Interrupted;
    }

    if let Some(finish) = opencode_assistant_finish(info) {
        return if opencode_assistant_finish_is_final(finish) {
            RuntimeTerminalTurnStatus::Completed
        } else {
            RuntimeTerminalTurnStatus::Interrupted
        };
    }

    if info.time.completed.is_some() {
        RuntimeTerminalTurnStatus::Completed
    } else {
        RuntimeTerminalTurnStatus::Interrupted
    }
}

fn opencode_resumable_message_pair(
    messages: &[OpenCodeExportMessage],
) -> Option<(&OpenCodeExportMessage, &OpenCodeExportMessage)> {
    let latest_user = opencode_latest_raw_message(messages, "user");
    let latest_assistant = opencode_latest_raw_message(messages, "assistant");
    let (Some(latest_user), Some(latest_assistant)) = (latest_user, latest_assistant) else {
        return None;
    };

    (opencode_raw_message_order(&latest_user.info)
        < opencode_raw_message_order(&latest_assistant.info)
        && latest_assistant.info.parent_id.as_deref() == Some(latest_user.info.id.as_str())
        && latest_assistant.info.error.is_none()
        && opencode_assistant_finish(&latest_assistant.info)
            .is_some_and(opencode_assistant_finish_is_final)
        && !opencode_assistant_has_pending_tool_followup(latest_assistant))
    .then_some((latest_user, latest_assistant))
}

fn opencode_assistant_finish(info: &OpenCodeExportMessageInfo) -> Option<&str> {
    info.finish
        .as_ref()
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|finish| !finish.is_empty())
}

fn opencode_assistant_finish_is_final(finish: &str) -> bool {
    !finish.eq_ignore_ascii_case("tool-calls")
}

fn opencode_assistant_has_pending_tool_followup(message: &OpenCodeExportMessage) -> bool {
    message
        .parts
        .iter()
        .any(opencode_tool_part_needs_model_followup)
}

fn opencode_tool_part_needs_model_followup(part: &OpenCodeExportPart) -> bool {
    part.kind == "tool"
        && !part
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("providerExecuted"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
        && !opencode_tool_part_is_orphaned_interrupted(part)
}

fn opencode_tool_part_is_orphaned_interrupted(part: &OpenCodeExportPart) -> bool {
    part.state.as_ref().is_some_and(|state| {
        state.get("status").and_then(Value::as_str) == Some("error")
            && state
                .pointer("/metadata/interrupted")
                .and_then(Value::as_bool)
                .unwrap_or(false)
    })
}

fn opencode_latest_raw_message<'a>(
    messages: &'a [OpenCodeExportMessage],
    role: &str,
) -> Option<&'a OpenCodeExportMessage> {
    messages
        .iter()
        .filter(|message| message.info.role == role && message.info.time.created.is_some())
        .max_by_key(|message| opencode_raw_message_order(&message.info))
}

fn opencode_raw_message_order(info: &OpenCodeExportMessageInfo) -> (i64, &str) {
    (info.time.created.unwrap_or(i64::MIN), info.id.as_str())
}

fn opencode_assistant_error(info: &OpenCodeExportMessageInfo) -> (Option<String>, Option<String>) {
    let Some(error) = &info.error else {
        return (None, None);
    };
    let code = error
        .get("name")
        .or_else(|| error.get("code"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let text = [
        "/message",
        "/data/message",
        "/cause/message",
        "/error/message",
    ]
    .iter()
    .find_map(|pointer| error.pointer(pointer).and_then(Value::as_str))
    .filter(|message| !message.trim().is_empty())
    .map(str::to_string);

    (code, text)
}

fn opencode_message_time(
    message: &OpenCodeExportMessage,
    prefer_completed: bool,
) -> Result<DateTime<Utc>> {
    let ms = if prefer_completed {
        message.info.time.completed.or(message.info.time.created)
    } else {
        message.info.time.created
    }
    .ok_or_else(|| anyhow!("OpenCode export message is missing a valid timestamp"))?;
    Utc.timestamp_millis_opt(ms)
        .single()
        .ok_or_else(|| anyhow!("invalid OpenCode timestamp {ms}"))
}

fn get_runtime_session(
    sessions: &RwLock<HashMap<String, OpenCodeSessionState>>,
    runtime_session_id: &str,
) -> Result<OpenCodeSessionState> {
    sessions
        .read()
        .map_err(|_| anyhow!("opencode runtime session state lock poisoned"))?
        .get(runtime_session_id)
        .cloned()
        .ok_or_else(|| anyhow!("runtime session '{runtime_session_id}' not found"))
}

fn load_saved_opencode_session_id(root: &Path) -> Result<Option<String>> {
    load_state_value(root, OPENCODE_SESSION_ID_STATE_FILE, "opencode session")
}

fn load_ready_opencode_session_id(
    root: &Path,
    runtime_session_ready: RuntimeSessionReady,
) -> Result<Option<String>> {
    load_ready_state_value(
        root,
        OPENCODE_SESSION_ID_STATE_FILE,
        "opencode session",
        runtime_session_ready,
    )
}

fn save_opencode_session_id(root: &Path, session_id: &str) -> Result<()> {
    save_state_value(
        root,
        OPENCODE_SESSION_ID_STATE_FILE,
        session_id,
        "opencode session",
    )
}

fn normalize_opencode_session_id(session_id: &str) -> Option<String> {
    let session_id = session_id.trim();
    if session_id.is_empty() {
        return None;
    }
    Some(session_id.to_string())
}

fn update_runtime_session_id(
    sessions: &RwLock<HashMap<String, OpenCodeSessionState>>,
    runtime_session_id: &str,
    session_id: String,
) -> Result<Option<PathBuf>> {
    let mut sessions = sessions
        .write()
        .map_err(|_| anyhow!("opencode runtime session state lock poisoned"))?;
    let session = sessions
        .get_mut(runtime_session_id)
        .ok_or_else(|| anyhow!("runtime session '{runtime_session_id}' not found"))?;
    let runtime_state_root = session.runtime_state_root.clone();
    session.session_id = Some(session_id);
    drop(sessions);
    Ok(runtime_state_root)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        future::pending,
        path::{Path, PathBuf},
        sync::{Arc, Mutex},
        time::Duration,
    };

    use lionclaw_runtime_api::{
        canonical_events, ExecutionOutput, NetworkMode, RuntimeAdapter, RuntimeEvent,
        RuntimeExecutionContext, RuntimeMessageLane, RuntimeProgramExecutor, RuntimeProgramSession,
        RuntimeProgramSpec, RuntimeProgramStdoutSender, RuntimeProgramTurnExecution,
        RuntimeSessionReady, RuntimeSessionStartInput, RuntimeTerminalProgramInput,
        RuntimeTerminalTranscriptInput, RuntimeTerminalTranscriptProgramExecutor,
        RuntimeTerminalTurnStatus, RuntimeTurnInput, RuntimeTurnMode, RUNTIME_SESSION_READY_MARKER,
    };

    use super::{
        opencode_acp_permission_denial, opencode_acp_turn_events, opencode_runtime_environment,
        opencode_terminal_environment, parse_opencode_session_list, OpenCodeAcpMessage,
        OpenCodeRuntimeAdapter, OpenCodeRuntimeConfig, OPENCODE_SESSION_ID_STATE_FILE,
    };
    use chrono::{DateTime, TimeZone, Utc};
    use serde::Deserialize;
    use serde_json::{json, Value};
    use uuid::Uuid;

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

    #[derive(Debug)]
    struct FakeTranscriptExecutor {
        outputs: VecDeque<ExecutionOutput>,
        programs: Vec<RuntimeProgramSpec>,
    }

    #[async_trait::async_trait]
    impl RuntimeTerminalTranscriptProgramExecutor for FakeTranscriptExecutor {
        async fn execute(
            &mut self,
            program: RuntimeProgramSpec,
        ) -> anyhow::Result<ExecutionOutput> {
            self.programs.push(program);
            self.outputs
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("missing fake OpenCode output"))
        }
    }

    #[derive(Debug)]
    struct HangingTranscriptExecutor;

    #[async_trait::async_trait]
    impl RuntimeTerminalTranscriptProgramExecutor for HangingTranscriptExecutor {
        fn hard_timeout(&self) -> Duration {
            Duration::from_millis(10)
        }

        async fn execute(
            &mut self,
            _program: RuntimeProgramSpec,
        ) -> anyhow::Result<ExecutionOutput> {
            pending::<()>().await;
            unreachable!("pending transcript executor returned")
        }
    }

    #[derive(Debug)]
    struct HangingAfterOutputsTranscriptExecutor {
        outputs: VecDeque<ExecutionOutput>,
        programs: Vec<RuntimeProgramSpec>,
        hard_timeout: Duration,
    }

    #[async_trait::async_trait]
    impl RuntimeTerminalTranscriptProgramExecutor for HangingAfterOutputsTranscriptExecutor {
        fn hard_timeout(&self) -> Duration {
            self.hard_timeout
        }

        async fn execute(
            &mut self,
            program: RuntimeProgramSpec,
        ) -> anyhow::Result<ExecutionOutput> {
            self.programs.push(program);
            if let Some(output) = self.outputs.pop_front() {
                return Ok(output);
            }
            pending::<()>().await;
            unreachable!("pending transcript executor returned")
        }
    }

    #[derive(Debug, Deserialize)]
    struct OpenCodeAcpFixture {
        raw_in: Vec<String>,
    }

    #[derive(Debug, Default)]
    struct FakeAcpProgramState {
        sent: Vec<Value>,
    }

    #[derive(Debug)]
    struct FakeAcpProgramExecutor {
        inbound: VecDeque<String>,
        state: Arc<Mutex<FakeAcpProgramState>>,
    }

    #[async_trait::async_trait]
    impl RuntimeProgramExecutor for FakeAcpProgramExecutor {
        async fn execute_streaming(
            &mut self,
            _program: RuntimeProgramSpec,
            _stdout: RuntimeProgramStdoutSender,
        ) -> anyhow::Result<ExecutionOutput> {
            unreachable!("OpenCode ACP driver should spawn an interactive program")
        }

        async fn execute_captured(
            &mut self,
            _program: RuntimeProgramSpec,
        ) -> anyhow::Result<ExecutionOutput> {
            unreachable!("OpenCode ACP driver should spawn an interactive program")
        }

        async fn spawn(
            &mut self,
            program: RuntimeProgramSpec,
        ) -> anyhow::Result<Box<dyn RuntimeProgramSession>> {
            assert_eq!(program.executable, "opencode");
            assert_eq!(program.args, vec!["acp".to_string()]);
            assert_eq!(program.environment, opencode_runtime_environment());
            assert!(program.stdin.is_empty());
            assert!(program.auth.is_none());
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

    fn json_output(value: serde_json::Value) -> ExecutionOutput {
        ExecutionOutput {
            stdout: value.to_string().into_bytes(),
            exit_code: Some(0),
            ..ExecutionOutput::default()
        }
    }

    fn bytes_output(bytes: impl Into<Vec<u8>>) -> ExecutionOutput {
        ExecutionOutput {
            stdout: bytes.into(),
            exit_code: Some(0),
            ..ExecutionOutput::default()
        }
    }

    fn opencode_test_timestamp(ms: i64) -> DateTime<Utc> {
        Utc.timestamp_millis_opt(ms)
            .single()
            .expect("valid test timestamp")
    }

    fn opencode_test_precise_timestamp(seconds: i64, nanos: u32) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(seconds, nanos).expect("valid test timestamp")
    }

    fn opencode_acp_fixture() -> OpenCodeAcpFixture {
        serde_json::from_str(include_str!(
            "../tests/fixtures/opencode_acp_success_1_17_9.json"
        ))
        .expect("OpenCode ACP fixture JSON")
    }

    fn project_opencode_acp_fixture_events() -> Vec<RuntimeEvent> {
        opencode_acp_fixture()
            .raw_in
            .iter()
            .flat_map(|raw| {
                let value = serde_json::from_str(raw).expect("fixture raw JSON-RPC line");
                let message = OpenCodeAcpMessage {
                    raw: raw.clone(),
                    value,
                };
                opencode_acp_turn_events(&message)
            })
            .map(|record| record.event)
            .collect()
    }

    fn opencode_acp_driver_context(runtime_state_root: PathBuf) -> RuntimeExecutionContext {
        RuntimeExecutionContext {
            network_mode: NetworkMode::On,
            environment: Vec::new(),
            runtime_state_root: Some(runtime_state_root),
            runtime_path_projections: Vec::new(),
        }
    }

    fn opencode_transcript_input(runtime_state_root: PathBuf) -> RuntimeTerminalTranscriptInput {
        opencode_transcript_input_after(runtime_state_root, None)
    }

    fn opencode_transcript_input_after(
        runtime_state_root: PathBuf,
        launch_started_at: Option<DateTime<Utc>>,
    ) -> RuntimeTerminalTranscriptInput {
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        RuntimeTerminalTranscriptInput {
            session_id: Uuid::new_v4(),
            runtime_state_root,
            launch_started_at,
        }
    }

    fn opencode_session_list_output(session_ids: &[&str]) -> ExecutionOutput {
        let sessions = session_ids
            .iter()
            .enumerate()
            .map(|(index, session_id)| (*session_id, 10_000 - index as i64))
            .collect::<Vec<_>>();
        opencode_session_list_output_with_times(&sessions)
    }

    fn opencode_session_list_output_with_times(sessions: &[(&str, i64)]) -> ExecutionOutput {
        json_output(json!(sessions
            .iter()
            .map(|(session_id, updated)| {
                json!({
                    "id": session_id,
                    "title": session_id,
                    "updated": updated,
                    "created": updated,
                    "projectId": "proj",
                    "directory": "/workspace"
                })
            })
            .collect::<Vec<_>>()))
    }

    fn opencode_completed_export_output(
        session_id: &str,
        user_text: &str,
        assistant_text: &str,
    ) -> ExecutionOutput {
        opencode_export_output(
            session_id,
            vec![
                opencode_user_message(session_id, "msg_user", user_text, 1_000),
                opencode_assistant_message(
                    session_id,
                    "msg_assistant",
                    "msg_user",
                    Some(assistant_text),
                    2_000,
                    Some(3_000),
                    Some("stop"),
                ),
            ],
        )
    }

    fn opencode_export_output(session_id: &str, messages: Vec<Value>) -> ExecutionOutput {
        json_output(json!({
            "info": {
                "id": session_id,
                "title": "native test",
                "time": { "created": 1000, "updated": 3000 },
                "directory": "/workspace"
            },
            "messages": messages
        }))
    }

    fn opencode_user_message(
        session_id: &str,
        message_id: &str,
        text: &str,
        created: i64,
    ) -> Value {
        json!({
            "info": {
                "id": message_id,
                "sessionID": session_id,
                "role": "user",
                "time": { "created": created },
                "agent": "build",
                "model": { "providerID": "openai", "modelID": "gpt-5" }
            },
            "parts": [{
                "id": format!("{message_id}_part"),
                "messageID": message_id,
                "sessionID": session_id,
                "type": "text",
                "text": text
            }]
        })
    }

    fn opencode_assistant_message(
        session_id: &str,
        message_id: &str,
        parent_id: &str,
        text: Option<&str>,
        created: i64,
        completed: Option<i64>,
        finish: Option<&str>,
    ) -> Value {
        let mut time = json!({ "created": created });
        if let Some(completed) = completed {
            time["completed"] = json!(completed);
        }
        let mut info = json!({
            "id": message_id,
            "sessionID": session_id,
            "role": "assistant",
            "time": time,
            "parentID": parent_id,
            "modelID": "gpt-5",
            "providerID": "openai",
            "mode": "build",
            "agent": "build",
            "path": { "cwd": "/workspace", "root": "/workspace" },
            "cost": 0,
            "tokens": {
                "input": 0,
                "output": 0,
                "reasoning": 0,
                "cache": { "read": 0, "write": 0 }
            }
        });
        if let Some(finish) = finish {
            info["finish"] = json!(finish);
        }

        json!({
            "info": info,
            "parts": text.map(|text| {
                vec![json!({
                    "id": format!("{message_id}_part"),
                    "messageID": message_id,
                    "sessionID": session_id,
                    "type": "text",
                    "text": text
                })]
            }).unwrap_or_default()
        })
    }

    fn opencode_tool_part(
        session_id: &str,
        message_id: &str,
        part_id: &str,
        status: &str,
        provider_executed: bool,
        interrupted: bool,
    ) -> Value {
        let mut state = json!({
            "status": status,
            "input": {},
        });
        if interrupted {
            state["metadata"] = json!({ "interrupted": true });
        }
        let mut part = json!({
            "id": part_id,
            "messageID": message_id,
            "sessionID": session_id,
            "type": "tool",
            "callID": format!("{part_id}_call"),
            "tool": "bash",
            "state": state,
        });
        if provider_executed {
            part["metadata"] = json!({ "providerExecuted": true });
        }
        part
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

    #[tokio::test]
    async fn opencode_program_backed_turn_uses_acp_driver_journal() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig {
            executable: "opencode".to_string(),
            model: Some("gpt-5".to_string()),
            agent: Some("plan".to_string()),
        });
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
                r#"{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentInfo":{"name":"OpenCode","version":"1.17.9"}}}"#.to_string(),
                r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"ses_program","configOptions":[]}}"#.to_string(),
                r#"{"jsonrpc":"2.0","id":3,"result":{}}"#.to_string(),
                r#"{"jsonrpc":"2.0","id":4,"result":{}}"#.to_string(),
                r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_program","update":{"sessionUpdate":"agent_thought_chunk","messageId":"msg_1","content":{"type":"text","text":"thinking"}}}}"#.to_string(),
                r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_program","update":{"sessionUpdate":"agent_message_chunk","messageId":"msg_1","content":{"type":"text","text":"answer"}}}}"#.to_string(),
                r#"{"jsonrpc":"2.0","id":5,"result":{"stopReason":"end_turn","_meta":{}}}"#.to_string(),
            ]),
            state: Arc::clone(&fake_state),
        };
        let (journal_tx, mut journal_rx) = tokio::sync::mpsc::unbounded_channel();

        adapter
            .program_backed_turn(
                RuntimeProgramTurnExecution {
                    input: RuntimeTurnInput {
                        runtime_session_id: handle.runtime_session_id.clone(),
                        prompt: "hello".to_string(),
                        fresh_prompt: None,
                        runtime_skill_ids: Vec::new(),
                    },
                    context: opencode_acp_driver_context(runtime_state_root.clone()),
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
            std::fs::read_to_string(runtime_state_root.join(OPENCODE_SESSION_ID_STATE_FILE))
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
                "session/set_model",
                "session/set_config_option",
                "session/prompt",
            ]
        );
        assert_eq!(sent[2]["params"]["modelId"], json!("gpt-5"));
        assert_eq!(sent[3]["params"]["configId"], json!("mode"));
        assert_eq!(sent[3]["params"]["value"], json!("plan"));
    }

    #[test]
    fn opencode_acp_permission_requests_are_denied_by_default() {
        let response = opencode_acp_permission_denial(Some(&json!({
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
            opencode_acp_permission_denial(Some(&json!({ "options": [] }))),
            json!({ "outcome": { "outcome": "cancelled" } })
        );
    }

    #[tokio::test]
    async fn opencode_session_start_resumes_saved_ready_session() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        std::fs::write(
            runtime_state_root.join(OPENCODE_SESSION_ID_STATE_FILE),
            "ses_ready\n",
        )
        .expect("write session id");
        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig::default());
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

    #[test]
    fn opencode_adapter_builds_terminal_program_spec() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig {
            executable: "opencode".to_string(),
            model: Some("gpt-5".to_string()),
            agent: Some("builder".to_string()),
        });

        let program = adapter
            .build_terminal_program(RuntimeTerminalProgramInput {
                session_id: Uuid::new_v4(),
                runtime_state_root,
                runtime_session_ready: runtime_not_ready(),
            })
            .expect("program");

        assert_eq!(program.executable, "opencode");
        assert_eq!(
            program.args,
            vec![
                "--model".to_string(),
                "gpt-5".to_string(),
                "--agent".to_string(),
                "builder".to_string(),
            ]
        );
        assert_eq!(program.environment, opencode_terminal_environment());
        assert!(program.stdin.is_empty());
        assert!(program.auth.is_none());
    }

    #[test]
    fn opencode_adapter_builds_terminal_program_for_saved_session() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        std::fs::write(
            runtime_state_root.join(OPENCODE_SESSION_ID_STATE_FILE),
            "ses_saved\n",
        )
        .expect("write session id");
        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig::default());
        let runtime_session_ready = mark_runtime_ready(&runtime_state_root);

        let program = adapter
            .build_terminal_program(RuntimeTerminalProgramInput {
                session_id: Uuid::new_v4(),
                runtime_state_root,
                runtime_session_ready,
            })
            .expect("program");

        assert_eq!(
            program.args,
            vec!["--session".to_string(), "ses_saved".to_string(),]
        );
    }

    #[test]
    fn opencode_terminal_program_ignores_saved_session_without_ready_marker() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        std::fs::write(
            runtime_state_root.join(OPENCODE_SESSION_ID_STATE_FILE),
            "ses_saved\n",
        )
        .expect("write session id");
        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig::default());

        let program = adapter
            .build_terminal_program(RuntimeTerminalProgramInput {
                session_id: Uuid::new_v4(),
                runtime_state_root,
                runtime_session_ready: runtime_not_ready(),
            })
            .expect("program");

        assert!(!program.args.iter().any(|arg| arg == "--session"));
        assert!(!program.args.iter().any(|arg| arg == "ses_saved"));
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_opencode_session_file_is_rejected() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        let target = temp_dir.path().join("outside-session-id");
        std::fs::write(&target, "ses_outside\n").expect("write target");
        std::os::unix::fs::symlink(
            &target,
            runtime_state_root.join(OPENCODE_SESSION_ID_STATE_FILE),
        )
        .expect("symlink session state");
        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig::default());
        let runtime_session_ready = mark_runtime_ready(&runtime_state_root);

        let err = adapter
            .build_terminal_program(RuntimeTerminalProgramInput {
                session_id: Uuid::new_v4(),
                runtime_state_root,
                runtime_session_ready,
            })
            .expect_err("symlinked session state should be rejected");

        assert!(
            err.to_string().contains("cannot be a symlink"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn opencode_session_list_chooses_newest_timestamp_independent_of_order() {
        let output = opencode_session_list_output_with_times(&[
            ("ses_old", 1_000),
            ("ses_new", 3_000),
            ("ses_mid", 2_000),
        ]);

        let listed = parse_opencode_session_list(&output.stdout).expect("session list");

        let latest = listed.latest.expect("latest session");
        assert_eq!(latest.id, "ses_new");
        assert_eq!(
            latest.updated_at.expect("updated").timestamp_millis(),
            3_000
        );
    }

    #[test]
    fn opencode_session_list_skips_malformed_entries() {
        let output = bytes_output(
            serde_json::to_vec(&json!([
                {"updated": 5_000},
                {"id": "ses_good", "updated": 2_000},
                {"id": "ses_bad_time", "updated": "later"}
            ]))
            .expect("session list JSON"),
        );

        let listed = parse_opencode_session_list(&output.stdout).expect("session list");

        let latest = listed.latest.expect("latest session");
        assert_eq!(latest.id, "ses_good");
        assert_eq!(
            latest.updated_at.expect("updated").timestamp_millis(),
            2_000
        );
    }

    #[tokio::test]
    async fn opencode_terminal_transcript_export_uses_native_cli_export() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        let mut executor = FakeTranscriptExecutor {
            outputs: VecDeque::from([
                opencode_session_list_output(&["ses_opencode_session"]),
                opencode_completed_export_output(
                    "ses_opencode_session",
                    "hello opencode",
                    "hello from native export",
                ),
            ]),
            programs: Vec::new(),
        };

        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig::default());
        let transcript = adapter
            .export_terminal_transcript(
                opencode_transcript_input(runtime_state_root.clone()),
                &mut executor,
            )
            .await
            .expect("export transcript");

        assert!(transcript.warnings.is_empty());
        assert!(transcript.state.is_reconciled());
        assert!(transcript.state.is_resumable());
        let turns = transcript.turns;
        assert_eq!(turns.len(), 1);
        let turn = &turns[0];
        assert_eq!(turn.display_user_text, "hello opencode");
        assert_eq!(turn.prompt_user_text, "hello opencode");
        assert_eq!(turn.assistant_text, "hello from native export");
        assert_eq!(turn.started_at.timestamp_millis(), 1_000);
        assert_eq!(turn.finished_at.timestamp_millis(), 3_000);
        assert!(turn
            .source_id
            .contains("opencode-export:ses_opencode_session:msg_user:msg_assistant"));
        assert_eq!(
            std::fs::read_to_string(
                temp_dir
                    .path()
                    .join("runtime-state")
                    .join(OPENCODE_SESSION_ID_STATE_FILE)
            )
            .expect("saved session id"),
            "ses_opencode_session\n"
        );

        assert_eq!(executor.programs.len(), 2);
        assert_eq!(
            executor.programs[0].args,
            vec![
                "session".to_string(),
                "list".to_string(),
                "--format".to_string(),
                "json".to_string(),
            ]
        );
        assert_eq!(
            executor.programs[1].args,
            vec!["export".to_string(), "ses_opencode_session".to_string()]
        );
        assert_eq!(
            executor.programs[0].environment,
            vec![
                ("OPENCODE_DISABLE_AUTOUPDATE".to_string(), "1".to_string()),
                ("OPENCODE_PURE".to_string(), "1".to_string()),
            ]
        );
        assert_eq!(
            executor.programs[1].environment,
            vec![
                ("OPENCODE_DISABLE_AUTOUPDATE".to_string(), "1".to_string()),
                ("OPENCODE_PURE".to_string(), "1".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn opencode_terminal_transcript_exports_linked_and_current_sessions_only() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        std::fs::write(
            runtime_state_root.join(OPENCODE_SESSION_ID_STATE_FILE),
            "ses_saved\n",
        )
        .expect("write session id");
        let mut executor = FakeTranscriptExecutor {
            outputs: VecDeque::from([
                opencode_session_list_output(&["ses_latest", "ses_ignored"]),
                opencode_completed_export_output("ses_latest", "hello", "answer"),
                opencode_completed_export_output("ses_saved", "before", "saved answer"),
            ]),
            programs: Vec::new(),
        };
        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig::default());

        let transcript = adapter
            .export_terminal_transcript(
                opencode_transcript_input_after(
                    runtime_state_root.clone(),
                    Some(opencode_test_timestamp(9_500)),
                ),
                &mut executor,
            )
            .await
            .expect("export transcript");

        assert_eq!(transcript.turns.len(), 2);
        assert!(transcript.state.is_reconciled());
        assert!(transcript.state.is_resumable());
        assert_eq!(
            transcript.turns[0].source_id,
            "opencode-export:ses_latest:msg_user:msg_assistant"
        );
        assert_eq!(
            transcript.turns[1].source_id,
            "opencode-export:ses_saved:msg_user:msg_assistant"
        );
        assert!(transcript.warnings.is_empty());
        assert_eq!(executor.programs.len(), 3);
        assert_eq!(
            executor.programs[1].args,
            vec!["export".to_string(), "ses_latest".to_string()]
        );
        assert_eq!(
            executor.programs[2].args,
            vec!["export".to_string(), "ses_saved".to_string()]
        );
        assert!(!executor
            .programs
            .iter()
            .any(|program| program.args.iter().any(|arg| arg == "ses_ignored")));
        assert_eq!(
            std::fs::read_to_string(runtime_state_root.join(OPENCODE_SESSION_ID_STATE_FILE))
                .expect("saved session id"),
            "ses_latest\n"
        );
    }

    #[tokio::test]
    async fn opencode_terminal_transcript_keeps_saved_session_when_latest_predates_launch() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        std::fs::write(
            runtime_state_root.join(OPENCODE_SESSION_ID_STATE_FILE),
            "ses_saved\n",
        )
        .expect("write session id");
        let mut executor = FakeTranscriptExecutor {
            outputs: VecDeque::from([
                opencode_session_list_output_with_times(&[("ses_stale", 1_000)]),
                opencode_completed_export_output("ses_saved", "hello", "answer"),
            ]),
            programs: Vec::new(),
        };
        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig::default());

        let transcript = adapter
            .export_terminal_transcript(
                opencode_transcript_input_after(
                    runtime_state_root.clone(),
                    Some(opencode_test_timestamp(1_500)),
                ),
                &mut executor,
            )
            .await
            .expect("export transcript");

        assert_eq!(transcript.turns.len(), 1);
        assert_eq!(
            transcript.turns[0].source_id,
            "opencode-export:ses_saved:msg_user:msg_assistant"
        );
        assert!(transcript.state.is_reconciled());
        assert!(transcript.state.is_resumable());
        assert!(transcript.warnings.is_empty());
        assert_eq!(executor.programs.len(), 2);
        assert_eq!(
            executor.programs[1].args,
            vec!["export".to_string(), "ses_saved".to_string()]
        );
        assert!(!executor
            .programs
            .iter()
            .any(|program| program.args.iter().any(|arg| arg == "ses_stale")));
        assert_eq!(
            std::fs::read_to_string(runtime_state_root.join(OPENCODE_SESSION_ID_STATE_FILE))
                .expect("saved session id"),
            "ses_saved\n"
        );
    }

    #[tokio::test]
    async fn opencode_terminal_transcript_accepts_latest_in_launch_millisecond() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        std::fs::write(
            runtime_state_root.join(OPENCODE_SESSION_ID_STATE_FILE),
            "ses_saved\n",
        )
        .expect("write session id");
        let mut executor = FakeTranscriptExecutor {
            outputs: VecDeque::from([
                opencode_session_list_output_with_times(&[("ses_latest", 1_500)]),
                opencode_completed_export_output("ses_latest", "hello", "answer"),
                opencode_completed_export_output("ses_saved", "before", "saved answer"),
            ]),
            programs: Vec::new(),
        };
        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig::default());

        let transcript = adapter
            .export_terminal_transcript(
                opencode_transcript_input_after(
                    runtime_state_root.clone(),
                    Some(opencode_test_precise_timestamp(1, 500_999_000)),
                ),
                &mut executor,
            )
            .await
            .expect("export transcript");

        assert_eq!(transcript.turns.len(), 2);
        assert!(transcript.state.is_reconciled());
        assert!(transcript.state.is_resumable());
        assert!(transcript.warnings.is_empty());
        assert_eq!(
            executor.programs[1].args,
            vec!["export".to_string(), "ses_latest".to_string()]
        );
        assert_eq!(
            std::fs::read_to_string(runtime_state_root.join(OPENCODE_SESSION_ID_STATE_FILE))
                .expect("saved session id"),
            "ses_latest\n"
        );
    }

    #[tokio::test]
    async fn opencode_terminal_transcript_tracks_failed_current_session_without_resumability() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        std::fs::write(
            runtime_state_root.join(OPENCODE_SESSION_ID_STATE_FILE),
            "ses_good\n",
        )
        .expect("write session id");
        let mut executor = FakeTranscriptExecutor {
            outputs: VecDeque::from([
                opencode_session_list_output(&["ses_bad", "ses_good"]),
                bytes_output(b"not json"),
                opencode_completed_export_output("ses_good", "hello", "answer"),
            ]),
            programs: Vec::new(),
        };
        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig::default());

        let transcript = adapter
            .export_terminal_transcript(
                opencode_transcript_input_after(
                    runtime_state_root.clone(),
                    Some(opencode_test_timestamp(9_500)),
                ),
                &mut executor,
            )
            .await
            .expect("export transcript");

        assert_eq!(transcript.turns.len(), 1);
        assert_eq!(transcript.warnings.len(), 1);
        assert!(!transcript.state.is_reconciled());
        assert!(!transcript.state.is_resumable());
        assert_eq!(
            std::fs::read_to_string(runtime_state_root.join(OPENCODE_SESSION_ID_STATE_FILE))
                .expect("saved session id"),
            "ses_bad\n"
        );
    }

    #[tokio::test]
    async fn opencode_terminal_transcript_resumability_uses_latest_raw_message_state() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        let mut executor = FakeTranscriptExecutor {
            outputs: VecDeque::from([
                opencode_session_list_output(&["ses_good"]),
                opencode_export_output(
                    "ses_good",
                    vec![
                        opencode_user_message("ses_good", "msg_user_1", "hello", 1_000),
                        opencode_assistant_message(
                            "ses_good",
                            "msg_assistant_1",
                            "msg_user_1",
                            Some("answer"),
                            2_000,
                            Some(3_000),
                            Some("stop"),
                        ),
                        opencode_user_message("ses_good", "msg_user_2", "unfinished", 4_000),
                    ],
                ),
            ]),
            programs: Vec::new(),
        };
        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig::default());

        let transcript = adapter
            .export_terminal_transcript(
                opencode_transcript_input(runtime_state_root),
                &mut executor,
            )
            .await
            .expect("export transcript");

        assert_eq!(transcript.turns.len(), 1);
        assert_eq!(
            transcript.turns[0].source_id,
            "opencode-export:ses_good:msg_user_1:msg_assistant_1"
        );
        assert!(transcript.state.is_reconciled());
        assert!(!transcript.state.is_resumable());
        assert!(transcript.warnings.is_empty());
    }

    #[tokio::test]
    async fn opencode_terminal_transcript_marks_tool_call_finish_interrupted() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        let mut executor = FakeTranscriptExecutor {
            outputs: VecDeque::from([
                opencode_session_list_output(&["ses_good"]),
                opencode_export_output(
                    "ses_good",
                    vec![
                        opencode_user_message("ses_good", "msg_user_1", "needs tool", 1_000),
                        opencode_assistant_message(
                            "ses_good",
                            "msg_assistant_1",
                            "msg_user_1",
                            Some("checking"),
                            2_000,
                            Some(3_000),
                            Some("tool-calls"),
                        ),
                    ],
                ),
            ]),
            programs: Vec::new(),
        };
        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig::default());

        let transcript = adapter
            .export_terminal_transcript(
                opencode_transcript_input(runtime_state_root),
                &mut executor,
            )
            .await
            .expect("export transcript");

        assert_eq!(transcript.turns.len(), 1);
        assert_eq!(
            transcript.turns[0].status,
            RuntimeTerminalTurnStatus::Interrupted
        );
        assert!(transcript.state.is_reconciled());
        assert!(!transcript.state.is_resumable());
        assert!(transcript.warnings.is_empty());
    }

    #[tokio::test]
    async fn opencode_terminal_transcript_treats_unknown_finish_as_final() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        let mut executor = FakeTranscriptExecutor {
            outputs: VecDeque::from([
                opencode_session_list_output(&["ses_good"]),
                opencode_export_output(
                    "ses_good",
                    vec![
                        opencode_user_message("ses_good", "msg_user", "hello", 1_000),
                        opencode_assistant_message(
                            "ses_good",
                            "msg_assistant",
                            "msg_user",
                            Some("answer"),
                            2_000,
                            Some(3_000),
                            Some("unknown"),
                        ),
                    ],
                ),
            ]),
            programs: Vec::new(),
        };
        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig::default());

        let transcript = adapter
            .export_terminal_transcript(
                opencode_transcript_input(runtime_state_root),
                &mut executor,
            )
            .await
            .expect("export transcript");

        assert_eq!(transcript.turns.len(), 1);
        assert_eq!(
            transcript.turns[0].status,
            RuntimeTerminalTurnStatus::Completed
        );
        assert!(transcript.state.is_reconciled());
        assert!(transcript.state.is_resumable());
        assert!(transcript.warnings.is_empty());
    }

    #[tokio::test]
    async fn opencode_terminal_transcript_resumability_requires_no_pending_tools() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        let mut assistant = opencode_assistant_message(
            "ses_good",
            "msg_assistant",
            "msg_user",
            Some("I will check."),
            2_000,
            Some(3_000),
            Some("stop"),
        );
        assistant["parts"]
            .as_array_mut()
            .expect("assistant parts")
            .push(opencode_tool_part(
                "ses_good",
                "msg_assistant",
                "tool_part",
                "pending",
                false,
                false,
            ));
        let mut executor = FakeTranscriptExecutor {
            outputs: VecDeque::from([
                opencode_session_list_output(&["ses_good"]),
                opencode_export_output(
                    "ses_good",
                    vec![
                        opencode_user_message("ses_good", "msg_user", "hello", 1_000),
                        assistant,
                    ],
                ),
            ]),
            programs: Vec::new(),
        };
        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig::default());

        let transcript = adapter
            .export_terminal_transcript(
                opencode_transcript_input(runtime_state_root),
                &mut executor,
            )
            .await
            .expect("export transcript");

        assert_eq!(transcript.turns.len(), 1);
        assert_eq!(
            transcript.turns[0].status,
            RuntimeTerminalTurnStatus::Interrupted
        );
        assert!(transcript.state.is_reconciled());
        assert!(!transcript.state.is_resumable());
        assert!(transcript.warnings.is_empty());
    }

    #[tokio::test]
    async fn opencode_terminal_transcript_allows_nonblocking_tool_parts() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        let mut assistant = opencode_assistant_message(
            "ses_good",
            "msg_assistant",
            "msg_user",
            Some("Provider handled it."),
            2_000,
            Some(3_000),
            Some("stop"),
        );
        assistant["parts"]
            .as_array_mut()
            .expect("assistant parts")
            .push(opencode_tool_part(
                "ses_good",
                "msg_assistant",
                "tool_part",
                "completed",
                true,
                false,
            ));
        assistant["parts"]
            .as_array_mut()
            .expect("assistant parts")
            .push(opencode_tool_part(
                "ses_good",
                "msg_assistant",
                "orphaned_tool_part",
                "error",
                false,
                true,
            ));
        let mut executor = FakeTranscriptExecutor {
            outputs: VecDeque::from([
                opencode_session_list_output(&["ses_good"]),
                opencode_export_output(
                    "ses_good",
                    vec![
                        opencode_user_message("ses_good", "msg_user", "hello", 1_000),
                        assistant,
                    ],
                ),
            ]),
            programs: Vec::new(),
        };
        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig::default());

        let transcript = adapter
            .export_terminal_transcript(
                opencode_transcript_input(runtime_state_root),
                &mut executor,
            )
            .await
            .expect("export transcript");

        assert_eq!(transcript.turns.len(), 1);
        assert_eq!(
            transcript.turns[0].status,
            RuntimeTerminalTurnStatus::Completed
        );
        assert!(transcript.state.is_reconciled());
        assert!(transcript.state.is_resumable());
        assert!(transcript.warnings.is_empty());
    }

    #[tokio::test]
    async fn opencode_terminal_transcript_resumability_requires_latest_user_parent() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        let mut executor = FakeTranscriptExecutor {
            outputs: VecDeque::from([
                opencode_session_list_output(&["ses_good"]),
                opencode_export_output(
                    "ses_good",
                    vec![
                        opencode_user_message("ses_good", "msg_user_1", "hello", 1_000),
                        opencode_user_message("ses_good", "msg_user_2", "unfinished", 2_000),
                        opencode_assistant_message(
                            "ses_good",
                            "msg_assistant_old",
                            "msg_user_1",
                            Some("late old answer"),
                            3_000,
                            Some(4_000),
                            Some("stop"),
                        ),
                    ],
                ),
            ]),
            programs: Vec::new(),
        };
        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig::default());

        let transcript = adapter
            .export_terminal_transcript(
                opencode_transcript_input(runtime_state_root),
                &mut executor,
            )
            .await
            .expect("export transcript");

        assert_eq!(transcript.turns.len(), 1);
        assert_eq!(
            transcript.turns[0].source_id,
            "opencode-export:ses_good:msg_user_1:msg_assistant_old"
        );
        assert!(transcript.state.is_reconciled());
        assert!(!transcript.state.is_resumable());
        assert!(transcript.warnings.is_empty());
    }

    #[tokio::test]
    async fn opencode_terminal_transcript_resumability_requires_imported_latest_pair() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        let mut latest_user = opencode_user_message("ses_good", "msg_user_2", "ignored", 4_000);
        latest_user["parts"][0]["ignored"] = json!(true);
        let mut executor = FakeTranscriptExecutor {
            outputs: VecDeque::from([
                opencode_session_list_output(&["ses_good"]),
                opencode_export_output(
                    "ses_good",
                    vec![
                        opencode_user_message("ses_good", "msg_user_1", "hello", 1_000),
                        opencode_assistant_message(
                            "ses_good",
                            "msg_assistant_1",
                            "msg_user_1",
                            Some("answer"),
                            2_000,
                            Some(3_000),
                            Some("stop"),
                        ),
                        latest_user,
                        opencode_assistant_message(
                            "ses_good",
                            "msg_assistant_2",
                            "msg_user_2",
                            Some("latest answer"),
                            5_000,
                            Some(6_000),
                            Some("stop"),
                        ),
                    ],
                ),
            ]),
            programs: Vec::new(),
        };
        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig::default());

        let transcript = adapter
            .export_terminal_transcript(
                opencode_transcript_input(runtime_state_root),
                &mut executor,
            )
            .await
            .expect("export transcript");

        assert_eq!(transcript.turns.len(), 1);
        assert_eq!(
            transcript.turns[0].source_id,
            "opencode-export:ses_good:msg_user_1:msg_assistant_1"
        );
        assert!(transcript.state.is_reconciled());
        assert!(!transcript.state.is_resumable());
        assert!(transcript.warnings.is_empty());
    }

    #[tokio::test]
    async fn opencode_terminal_transcript_timeout_returns_partial_transcript() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        std::fs::write(
            runtime_state_root.join(OPENCODE_SESSION_ID_STATE_FILE),
            "ses_hangs\n",
        )
        .expect("write session id");
        let mut executor = HangingAfterOutputsTranscriptExecutor {
            outputs: VecDeque::from([
                opencode_session_list_output(&["ses_good"]),
                opencode_completed_export_output("ses_good", "hello", "answer"),
            ]),
            programs: Vec::new(),
            hard_timeout: Duration::from_millis(200),
        };
        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig::default());

        let transcript = adapter
            .export_terminal_transcript(
                opencode_transcript_input_after(
                    runtime_state_root,
                    Some(opencode_test_timestamp(0)),
                ),
                &mut executor,
            )
            .await
            .expect("partial transcript");

        assert_eq!(transcript.turns.len(), 1);
        assert!(transcript.state.is_reconciled());
        assert!(transcript.state.is_resumable());
        assert_eq!(transcript.warnings.len(), 1);
        assert_eq!(
            transcript.warnings[0].source_id,
            "opencode-session:ses_hangs"
        );
        assert!(
            transcript.warnings[0].error.contains("timed out"),
            "unexpected warning: {}",
            transcript.warnings[0].error
        );
    }

    #[tokio::test]
    async fn opencode_list_failure_fallback_imports_without_reconciling() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        std::fs::write(
            runtime_state_root.join(OPENCODE_SESSION_ID_STATE_FILE),
            "ses_saved\n",
        )
        .expect("write session id");
        let mut executor = FakeTranscriptExecutor {
            outputs: VecDeque::from([
                ExecutionOutput {
                    stderr: b"list unavailable".to_vec(),
                    exit_code: Some(1),
                    ..ExecutionOutput::default()
                },
                opencode_completed_export_output("ses_saved", "hello", "answer"),
            ]),
            programs: Vec::new(),
        };
        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig::default());

        let transcript = adapter
            .export_terminal_transcript(
                opencode_transcript_input(runtime_state_root),
                &mut executor,
            )
            .await
            .expect("saved session fallback");

        assert_eq!(transcript.turns.len(), 1);
        assert!(!transcript.state.is_reconciled());
        assert!(!transcript.state.is_resumable());
        assert_eq!(transcript.warnings.len(), 1);
        assert_eq!(transcript.warnings[0].source_id, "opencode-session-list");
        assert!(
            transcript.warnings[0].error.contains("list unavailable"),
            "unexpected warning: {}",
            transcript.warnings[0].error
        );
        assert_eq!(
            executor.programs[1].args,
            vec!["export".to_string(), "ses_saved".to_string()]
        );
    }

    #[tokio::test]
    async fn opencode_malformed_list_fallback_imports_without_reconciling() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        std::fs::write(
            runtime_state_root.join(OPENCODE_SESSION_ID_STATE_FILE),
            "ses_saved\n",
        )
        .expect("write session id");
        let mut executor = FakeTranscriptExecutor {
            outputs: VecDeque::from([
                bytes_output(b"{\"unexpected\":true}"),
                opencode_completed_export_output("ses_saved", "hello", "answer"),
            ]),
            programs: Vec::new(),
        };
        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig::default());

        let transcript = adapter
            .export_terminal_transcript(
                opencode_transcript_input(runtime_state_root),
                &mut executor,
            )
            .await
            .expect("saved session fallback");

        assert_eq!(transcript.turns.len(), 1);
        assert!(!transcript.state.is_reconciled());
        assert!(!transcript.state.is_resumable());
        assert_eq!(transcript.warnings.len(), 1);
        assert_eq!(transcript.warnings[0].source_id, "opencode-session-list");
        assert!(
            transcript.warnings[0]
                .error
                .contains("failed to parse OpenCode session list JSON"),
            "unexpected warning: {}",
            transcript.warnings[0].error
        );
        assert_eq!(
            executor.programs[1].args,
            vec!["export".to_string(), "ses_saved".to_string()]
        );
    }

    #[tokio::test]
    async fn opencode_terminal_transcript_export_is_bounded_as_a_whole_pass() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig::default());
        let mut executor = HangingTranscriptExecutor;

        let err = adapter
            .export_terminal_transcript(
                opencode_transcript_input(runtime_state_root),
                &mut executor,
            )
            .await
            .expect_err("expected export timeout");

        let message = err.to_string();
        assert!(
            message.contains("timed out after"),
            "unexpected error: {err}"
        );
        assert!(
            message.contains("listing OpenCode sessions through the OpenCode CLI"),
            "unexpected error: {err}"
        );
    }
}
