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
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::time::timeout;
use tracing::warn;
use uuid::Uuid;

use lionclaw_runtime_api::{
    clear_state_value, load_ready_state_value, save_state_value, ExecutionOutput, RawTurnPayload,
    RuntimeAdapter, RuntimeAdapterInfo, RuntimeAuthKind, RuntimeCapabilityResult,
    RuntimeDriverConfig, RuntimeDriverProvider, RuntimeEvent, RuntimeEventSender,
    RuntimeMcpServerSpec, RuntimeMessageLane, RuntimeProgramExecutor, RuntimeProgramSession,
    RuntimeProgramSpec, RuntimeProgramTurnExecution, RuntimeSessionHandle,
    RuntimeSessionStartInput, RuntimeTerminalConfig, RuntimeTerminalProgramInput, RuntimeTurnInput,
    RuntimeTurnJournalSender, RuntimeTurnMode, RuntimeTurnResult, TurnEvent,
};

pub const ACP_PROTOCOL_NAME: &str = "acp";
pub const ACP_DEFAULT_WORKING_DIR: &str = "/workspace";
pub const ACP_SESSION_ID_STATE_FILE: &str = ".lionclaw-acp-session-id";
const ACP_CANCEL_ACK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcpRuntimeConfig {
    pub runtime_id: String,
    pub executable: String,
    pub args: Vec<String>,
    pub environment: Vec<(String, String)>,
    pub model: Option<String>,
    pub mode: Option<String>,
    pub auth: Option<RuntimeAuthKind>,
    pub terminal: RuntimeTerminalConfig,
    pub session_id_state_file: String,
    pub default_working_dir: String,
}

impl AcpRuntimeConfig {
    fn normalized_runtime_id(&self) -> String {
        let runtime_id = self.runtime_id.trim();
        if runtime_id.is_empty() {
            "acp".to_string()
        } else {
            runtime_id.to_string()
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AcpRuntimeDriver;

impl RuntimeDriverProvider for AcpRuntimeDriver {
    fn driver(&self) -> &'static str {
        ACP_PROTOCOL_NAME
    }

    fn create_adapter(&self, config: RuntimeDriverConfig) -> Arc<dyn RuntimeAdapter> {
        Arc::new(AcpRuntimeAdapter::new(AcpRuntimeConfig {
            runtime_id: config.runtime_id,
            executable: config.executable,
            args: config.args,
            environment: config.environment,
            model: config.model,
            mode: config.mode,
            auth: config.auth,
            terminal: config.terminal,
            session_id_state_file: ACP_SESSION_ID_STATE_FILE.to_string(),
            default_working_dir: ACP_DEFAULT_WORKING_DIR.to_string(),
        }))
    }
}

#[derive(Debug)]
pub struct AcpRuntimeAdapter {
    config: AcpRuntimeConfig,
    sessions: Arc<RwLock<HashMap<String, AcpSessionState>>>,
}

#[derive(Debug, Clone)]
struct AcpSessionState {
    runtime_state_root: Option<PathBuf>,
    session_id: Option<String>,
    active_turn: Option<ActiveAcpTurn>,
}

#[derive(Debug, Clone)]
struct ActiveAcpTurn {
    session_id: String,
    cancel_tx: mpsc::UnboundedSender<AcpCancelRequest>,
    completion: Arc<AcpTurnCompletion>,
}

#[derive(Debug)]
struct AcpCancelRequest {
    sent: oneshot::Sender<Result<(), String>>,
}

#[derive(Debug, Default)]
struct AcpTurnCompletion {
    done: AtomicBool,
    notify: Notify,
}

impl AcpTurnCompletion {
    async fn wait(&self) {
        loop {
            if self.done.load(Ordering::Acquire) {
                return;
            }
            let notified = self.notify.notified();
            if self.done.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    fn mark_done(&self) {
        self.done.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
}

impl AcpRuntimeAdapter {
    pub fn new(config: AcpRuntimeConfig) -> Self {
        Self {
            config,
            sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl RuntimeAdapter for AcpRuntimeAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: self.config.normalized_runtime_id(),
            version: "0.1".to_string(),
            healthy: !self.config.executable.trim().is_empty(),
        }
    }

    fn turn_mode(&self) -> RuntimeTurnMode {
        RuntimeTurnMode::ProgramBacked
    }

    fn build_terminal_program(
        &self,
        _input: RuntimeTerminalProgramInput,
    ) -> Result<RuntimeProgramSpec> {
        Ok(build_acp_terminal_program(&self.config))
    }

    async fn session_start(&self, input: RuntimeSessionStartInput) -> Result<RuntimeSessionHandle> {
        let runtime_id = self.config.normalized_runtime_id();
        let runtime_session_id = format!("{runtime_id}-{}", Uuid::new_v4());
        let session_id = match input.runtime_state_root.as_deref() {
            Some(root) => {
                load_ready_acp_session_id(&self.config, root, input.runtime_session_ready)?
            }
            None => None,
        };
        let resumes_existing_session = session_id.is_some();
        self.sessions
            .write()
            .map_err(|_| anyhow!("ACP runtime session state lock poisoned"))?
            .insert(
                runtime_session_id.clone(),
                AcpSessionState {
                    runtime_state_root: input.runtime_state_root,
                    session_id,
                    active_turn: None,
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
            input,
            context,
            executor,
        } = execution;
        let mut driver = AcpTurnRunner {
            config: self.config.clone(),
            sessions: Arc::clone(&self.sessions),
            working_dir: context
                .working_dir
                .unwrap_or_else(|| self.config.default_working_dir.clone()),
            mcp_servers: context.mcp_servers,
            executor,
        };
        driver.run_turn(input, journal).await
    }

    async fn resolve_capability_requests(
        &self,
        _handle: &RuntimeSessionHandle,
        results: Vec<RuntimeCapabilityResult>,
        events: RuntimeEventSender,
    ) -> Result<()> {
        if !results.is_empty() {
            return Err(anyhow!(
                "ACP adapter does not support runtime-side capability request resolution"
            ));
        }
        drop(events.send(RuntimeEvent::Done));
        Ok(())
    }

    async fn cancel(&self, handle: &RuntimeSessionHandle, _reason: Option<String>) -> Result<()> {
        let active_turn = self
            .sessions
            .read()
            .map_err(|_| anyhow!("ACP runtime session state lock poisoned"))?
            .get(&handle.runtime_session_id)
            .and_then(|state| state.active_turn.clone());
        let Some(active_turn) = active_turn else {
            return Ok(());
        };

        let completion = Arc::clone(&active_turn.completion);
        let (sent, cancel_sent) = oneshot::channel();
        active_turn
            .cancel_tx
            .send(AcpCancelRequest { sent })
            .map_err(|_| {
                anyhow!(
                    "ACP turn '{}' is no longer accepting cancellation",
                    active_turn.session_id
                )
            })?;
        match timeout(ACP_CANCEL_ACK_TIMEOUT, cancel_sent).await {
            Ok(Ok(result)) => result.map_err(anyhow::Error::msg),
            Ok(Err(_)) => Err(anyhow!(
                "ACP turn cancellation send acknowledgement was dropped for '{}'",
                active_turn.session_id
            )),
            Err(_) => {
                warn!(
                    session_id = active_turn.session_id,
                    "timed out waiting for ACP session/cancel send acknowledgement"
                );
                return Ok(());
            }
        }?;

        match timeout(ACP_CANCEL_ACK_TIMEOUT, completion.wait()).await {
            Ok(()) => Ok(()),
            Err(_) => {
                warn!(
                    session_id = active_turn.session_id,
                    "timed out waiting for ACP cancelled turn to finish"
                );
                Ok(())
            }
        }
    }

    async fn close(&self, handle: &RuntimeSessionHandle) -> Result<()> {
        self.sessions
            .write()
            .map_err(|_| anyhow!("ACP runtime session state lock poisoned"))?
            .remove(&handle.runtime_session_id);
        Ok(())
    }
}

struct AcpTurnRunner {
    config: AcpRuntimeConfig,
    sessions: Arc<RwLock<HashMap<String, AcpSessionState>>>,
    working_dir: String,
    mcp_servers: Vec<RuntimeMcpServerSpec>,
    executor: Box<dyn RuntimeProgramExecutor>,
}

impl AcpTurnRunner {
    async fn run_turn(
        &mut self,
        input: RuntimeTurnInput,
        journal: RuntimeTurnJournalSender,
    ) -> Result<RuntimeTurnResult> {
        let runtime_session_id = input.runtime_session_id.clone();
        let session_state = get_runtime_session(&self.sessions, &runtime_session_id)?;
        let program = build_acp_program(&self.config);
        let session = self.executor.spawn(program).await?;
        let mut client = AcpClient::new(session);
        let mut active_turn = None;

        let result = async {
            let session_capabilities = client.initialize().await?;
            let opened_session = client
                .ensure_session(AcpEnsureSession {
                    config: &self.config,
                    sessions: &self.sessions,
                    runtime_session_id: &runtime_session_id,
                    session_state: &session_state,
                    session_capabilities,
                    working_dir: &self.working_dir,
                    mcp_servers: &self.mcp_servers,
                })
                .await?;
            client
                .configure_session(&self.config, &opened_session.session_id)
                .await?;
            let (cancel_tx, mut cancel_rx) = mpsc::unbounded_channel();
            active_turn = Some(register_active_acp_turn(
                &self.sessions,
                &runtime_session_id,
                &opened_session.session_id,
                cancel_tx,
            )?);
            let prompt = if opened_session.resumed_existing {
                &input.prompt
            } else {
                input.fresh_prompt.as_deref().unwrap_or(&input.prompt)
            };
            let prompt_result = client
                .prompt(&opened_session.session_id, prompt, &journal, &mut cancel_rx)
                .await;
            prompt_result?;
            Ok(RuntimeTurnResult::default())
        }
        .await;

        let result = finish_acp_session(client, result).await;
        drop(active_turn);
        result
    }
}

fn build_acp_program(config: &AcpRuntimeConfig) -> RuntimeProgramSpec {
    RuntimeProgramSpec {
        executable: config.executable.clone(),
        args: config.args.clone(),
        environment: config.environment.clone(),
        stdin: String::new(),
        auth: config.auth.clone(),
    }
}

fn build_acp_terminal_program(config: &AcpRuntimeConfig) -> RuntimeProgramSpec {
    RuntimeProgramSpec {
        executable: config.executable.clone(),
        args: config.terminal.args.clone(),
        environment: config.environment.clone(),
        stdin: String::new(),
        auth: config.auth.clone(),
    }
}

fn acp_mcp_servers(servers: &[RuntimeMcpServerSpec]) -> Value {
    Value::Array(
        servers
            .iter()
            .map(|server| {
                json!({
                    "name": server.name,
                    "command": server.command,
                    "args": server.args,
                    "env": [],
                })
            })
            .collect(),
    )
}

struct AcpClient {
    session: Option<Box<dyn RuntimeProgramSession>>,
    next_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AcpOpenedSession {
    session_id: String,
    resumed_existing: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct AcpSessionCapabilities {
    load_session: bool,
    resume_session: bool,
}

impl AcpSessionCapabilities {
    fn from_initialize_result(result: &Value) -> Self {
        let agent_capabilities = result.get("agentCapabilities");
        Self {
            load_session: agent_capabilities
                .and_then(|capabilities| capabilities.get("loadSession"))
                .and_then(Value::as_bool)
                .unwrap_or(false),
            resume_session: agent_capabilities
                .and_then(|capabilities| capabilities.pointer("/sessionCapabilities/resume"))
                .is_some_and(acp_capability_object_enabled),
        }
    }

    fn reopen_method(self) -> Option<&'static str> {
        if self.load_session {
            Some("session/load")
        } else if self.resume_session {
            Some("session/resume")
        } else {
            None
        }
    }
}

fn acp_capability_object_enabled(value: &Value) -> bool {
    value.as_object().is_some() || value.as_bool() == Some(true)
}

#[derive(Debug, Clone)]
struct AcpMessage {
    raw: String,
    value: Value,
}

#[derive(Debug, Clone)]
struct AcpResponse {
    raw: String,
    result: Value,
}

struct AcpCancelWait<'a> {
    session_id: &'a str,
    cancel_rx: &'a mut mpsc::UnboundedReceiver<AcpCancelRequest>,
}

struct AcpEnsureSession<'a> {
    config: &'a AcpRuntimeConfig,
    sessions: &'a RwLock<HashMap<String, AcpSessionState>>,
    runtime_session_id: &'a str,
    session_state: &'a AcpSessionState,
    session_capabilities: AcpSessionCapabilities,
    working_dir: &'a str,
    mcp_servers: &'a [RuntimeMcpServerSpec],
}

impl AcpClient {
    fn new(session: Box<dyn RuntimeProgramSession>) -> Self {
        Self {
            session: Some(session),
            next_id: 1,
        }
    }

    async fn initialize(&mut self) -> Result<AcpSessionCapabilities> {
        let response = self
            .request(
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
        Ok(AcpSessionCapabilities::from_initialize_result(
            &response.result,
        ))
    }

    async fn ensure_session(&mut self, input: AcpEnsureSession<'_>) -> Result<AcpOpenedSession> {
        let mcp_servers = acp_mcp_servers(input.mcp_servers);
        if let Some(session_id) = input.session_state.session_id.as_deref() {
            if let Some(reopen_method) = input.session_capabilities.reopen_method() {
                self.request(
                    reopen_method,
                    json!({
                        "sessionId": session_id,
                        "cwd": input.working_dir,
                        "mcpServers": mcp_servers.clone(),
                    }),
                    None,
                )
                .await?;
                return Ok(AcpOpenedSession {
                    session_id: session_id.to_string(),
                    resumed_existing: true,
                });
            } else {
                forget_acp_session_id(input.config, input.sessions, input.runtime_session_id)?;
            }
        }

        let response = self
            .request(
                "session/new",
                json!({
                    "cwd": input.working_dir,
                    "mcpServers": mcp_servers,
                }),
                None,
            )
            .await?;
        let session_id = response
            .result
            .get("sessionId")
            .and_then(Value::as_str)
            .and_then(normalize_acp_session_id)
            .context("ACP session/new response is missing sessionId")?;
        if input.session_capabilities.reopen_method().is_some() {
            remember_acp_session_id(
                input.config,
                input.sessions,
                input.runtime_session_id,
                &session_id,
            )?;
        } else {
            forget_acp_session_id(input.config, input.sessions, input.runtime_session_id)?;
        }

        Ok(AcpOpenedSession {
            session_id,
            resumed_existing: false,
        })
    }

    async fn configure_session(
        &mut self,
        config: &AcpRuntimeConfig,
        session_id: &str,
    ) -> Result<()> {
        if let Some(model) = config.model.as_deref() {
            self.request(
                "session/set_config_option",
                json!({
                    "sessionId": session_id,
                    "configId": "model",
                    "value": model,
                }),
                None,
            )
            .await?;
        }

        if let Some(mode) = config.mode.as_deref() {
            self.request(
                "session/set_config_option",
                json!({
                    "sessionId": session_id,
                    "configId": "mode",
                    "value": mode,
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
        cancel_rx: &mut mpsc::UnboundedReceiver<AcpCancelRequest>,
    ) -> Result<()> {
        let response = self
            .request_with_cancel(
                "session/prompt",
                json!({
                    "sessionId": session_id,
                    "prompt": [{
                        "type": "text",
                        "text": prompt,
                    }],
                }),
                Some(journal),
                session_id,
                cancel_rx,
            )
            .await?;
        drop(journal.send(TurnEvent::with_raw(
            RuntimeEvent::Done,
            RawTurnPayload {
                driver: ACP_PROTOCOL_NAME.to_string(),
                payload: response.raw,
            },
        )));
        Ok(())
    }

    async fn request_with_cancel(
        &mut self,
        method: &str,
        params: Value,
        journal: Option<&RuntimeTurnJournalSender>,
        session_id: &str,
        cancel_rx: &mut mpsc::UnboundedReceiver<AcpCancelRequest>,
    ) -> Result<AcpResponse> {
        let id = self.next_request_id();
        self.send_request(id, method, params).await?;
        self.wait_for_response(
            id,
            method,
            journal,
            Some(AcpCancelWait {
                session_id,
                cancel_rx,
            }),
        )
        .await
    }

    async fn request(
        &mut self,
        method: &str,
        params: Value,
        journal: Option<&RuntimeTurnJournalSender>,
    ) -> Result<AcpResponse> {
        let id = self.next_request_id();
        self.send_request(id, method, params).await?;
        self.wait_for_response(id, method, journal, None).await
    }

    async fn wait_for_response(
        &mut self,
        id: u64,
        method: &str,
        journal: Option<&RuntimeTurnJournalSender>,
        mut cancel: Option<AcpCancelWait<'_>>,
    ) -> Result<AcpResponse> {
        if let Some(cancel) = cancel.as_mut() {
            loop {
                tokio::select! {
                    maybe_cancel = cancel.cancel_rx.recv() => {
                        match maybe_cancel {
                            Some(cancel_request) => {
                                self.cancel_session(cancel.session_id, cancel_request).await;
                            }
                            None => break,
                        }
                    }
                    maybe_message = self.recv() => {
                        let Some(message) = maybe_message? else {
                            return Err(anyhow!("ACP process closed before responding to {method}"));
                        };
                        if acp_response_id(&message.value).is_some_and(|response_id| response_id == id) {
                            return parse_acp_response(message, method);
                        }
                        self.dispatch_message(message, journal).await?;
                    }
                }
            }
        }

        loop {
            let Some(message) = self.recv().await? else {
                return Err(anyhow!("ACP process closed before responding to {method}"));
            };
            if acp_response_id(&message.value).is_some_and(|response_id| response_id == id) {
                return parse_acp_response(message, method);
            }
            self.dispatch_message(message, journal).await?;
        }
    }

    async fn send_request(&mut self, id: u64, method: &str, params: Value) -> Result<()> {
        self.send(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }))
        .await
    }

    async fn send_notification(&mut self, method: &str, params: Value) -> Result<()> {
        self.send(&json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }))
        .await
    }

    async fn cancel_session(&mut self, session_id: &str, cancel: AcpCancelRequest) {
        let result = self
            .send_notification(
                "session/cancel",
                json!({
                    "sessionId": session_id,
                }),
            )
            .await
            .map_err(|err| err.to_string());
        drop(cancel.sent.send(result));
    }

    async fn dispatch_message(
        &mut self,
        message: AcpMessage,
        journal: Option<&RuntimeTurnJournalSender>,
    ) -> Result<()> {
        if acp_is_server_request(&message.value) {
            self.respond_to_server_request(&message.value).await?;
            return Ok(());
        }

        if let Some(journal) = journal {
            for record in acp_turn_events(&message) {
                drop(journal.send(record));
            }
        }
        Ok(())
    }

    async fn respond_to_server_request(&mut self, request: &Value) -> Result<()> {
        let id = request
            .get("id")
            .cloned()
            .context("ACP server request is missing id")?;
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match method {
            "session/request_permission" => {
                self.send(&json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": acp_permission_denial(request.get("params")),
                }))
                .await
            }
            "fs/read_text_file" | "fs/write_text_file" => {
                self.send(&acp_error_response(
                    id,
                    -32000,
                    "LionClaw disables ACP filesystem access",
                ))
                .await
            }
            _ => {
                self.send(&acp_error_response(
                    id,
                    -32601,
                    &format!("LionClaw does not support ACP request '{method}'"),
                ))
                .await
            }
        }
    }

    async fn send(&mut self, message: &Value) -> Result<()> {
        let session = self
            .session
            .as_mut()
            .context("ACP session is already closed")?;
        session.write_line(&serde_json::to_string(message)?).await
    }

    async fn recv(&mut self) -> Result<Option<AcpMessage>> {
        let session = self
            .session
            .as_mut()
            .context("ACP session is already closed")?;
        loop {
            let Some(line) = session.read_line().await? else {
                return Ok(None);
            };
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let value = serde_json::from_str(trimmed)
                .with_context(|| format!("invalid ACP JSON-RPC line: {trimmed}"))?;
            return Ok(Some(AcpMessage {
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

fn get_runtime_session(
    sessions: &RwLock<HashMap<String, AcpSessionState>>,
    runtime_session_id: &str,
) -> Result<AcpSessionState> {
    sessions
        .read()
        .map_err(|_| anyhow!("ACP runtime session state lock poisoned"))?
        .get(runtime_session_id)
        .cloned()
        .ok_or_else(|| anyhow!("unknown ACP runtime session '{runtime_session_id}'"))
}

fn remember_acp_session_id(
    config: &AcpRuntimeConfig,
    sessions: &RwLock<HashMap<String, AcpSessionState>>,
    runtime_session_id: &str,
    session_id: &str,
) -> Result<()> {
    let runtime_state_root =
        update_runtime_session_id(sessions, runtime_session_id, session_id.to_string())?;
    if let Some(root) = runtime_state_root.as_deref() {
        save_acp_session_id(config, root, session_id)?;
    }
    Ok(())
}

fn forget_acp_session_id(
    config: &AcpRuntimeConfig,
    sessions: &RwLock<HashMap<String, AcpSessionState>>,
    runtime_session_id: &str,
) -> Result<()> {
    let runtime_state_root = clear_runtime_session_id(sessions, runtime_session_id)?;
    if let Some(root) = runtime_state_root.as_deref() {
        clear_state_value(root, &config.session_id_state_file, "ACP session id")?;
    }
    Ok(())
}

fn update_runtime_session_id(
    sessions: &RwLock<HashMap<String, AcpSessionState>>,
    runtime_session_id: &str,
    session_id: String,
) -> Result<Option<PathBuf>> {
    let mut sessions = sessions
        .write()
        .map_err(|_| anyhow!("ACP runtime session state lock poisoned"))?;
    let state = sessions
        .get_mut(runtime_session_id)
        .ok_or_else(|| anyhow!("unknown ACP runtime session '{runtime_session_id}'"))?;
    state.session_id = Some(session_id);
    let runtime_state_root = state.runtime_state_root.clone();
    drop(sessions);
    Ok(runtime_state_root)
}

fn clear_runtime_session_id(
    sessions: &RwLock<HashMap<String, AcpSessionState>>,
    runtime_session_id: &str,
) -> Result<Option<PathBuf>> {
    let mut sessions = sessions
        .write()
        .map_err(|_| anyhow!("ACP runtime session state lock poisoned"))?;
    let state = sessions
        .get_mut(runtime_session_id)
        .ok_or_else(|| anyhow!("unknown ACP runtime session '{runtime_session_id}'"))?;
    state.session_id = None;
    let runtime_state_root = state.runtime_state_root.clone();
    drop(sessions);
    Ok(runtime_state_root)
}

fn register_active_acp_turn(
    sessions: &Arc<RwLock<HashMap<String, AcpSessionState>>>,
    runtime_session_id: &str,
    session_id: &str,
    cancel_tx: mpsc::UnboundedSender<AcpCancelRequest>,
) -> Result<ActiveAcpTurnRegistration> {
    let sessions_ref = Arc::clone(sessions);
    let completion = Arc::new(AcpTurnCompletion::default());
    {
        let mut sessions = sessions
            .write()
            .map_err(|_| anyhow!("ACP runtime session state lock poisoned"))?;
        let state = sessions
            .get_mut(runtime_session_id)
            .ok_or_else(|| anyhow!("unknown ACP runtime session '{runtime_session_id}'"))?;
        state.active_turn = Some(ActiveAcpTurn {
            session_id: session_id.to_string(),
            cancel_tx,
            completion: Arc::clone(&completion),
        });
        drop(sessions);
    }
    Ok(ActiveAcpTurnRegistration {
        sessions: sessions_ref,
        runtime_session_id: runtime_session_id.to_string(),
        completion,
    })
}

struct ActiveAcpTurnRegistration {
    sessions: Arc<RwLock<HashMap<String, AcpSessionState>>>,
    runtime_session_id: String,
    completion: Arc<AcpTurnCompletion>,
}

impl Drop for ActiveAcpTurnRegistration {
    fn drop(&mut self) {
        if let Ok(mut sessions) = self.sessions.write() {
            if let Some(state) = sessions.get_mut(&self.runtime_session_id) {
                if state
                    .active_turn
                    .as_ref()
                    .is_some_and(|turn| Arc::ptr_eq(&turn.completion, &self.completion))
                {
                    state.active_turn = None;
                }
            }
        }
        self.completion.mark_done();
    }
}

fn save_acp_session_id(
    config: &AcpRuntimeConfig,
    runtime_state_root: &std::path::Path,
    session_id: &str,
) -> Result<()> {
    save_state_value(
        runtime_state_root,
        &config.session_id_state_file,
        session_id,
        "ACP session id",
    )
}

fn load_ready_acp_session_id(
    config: &AcpRuntimeConfig,
    runtime_state_root: &std::path::Path,
    runtime_session_ready: lionclaw_runtime_api::RuntimeSessionReady,
) -> Result<Option<String>> {
    load_ready_state_value(
        runtime_state_root,
        &config.session_id_state_file,
        "ACP session id",
        runtime_session_ready,
    )
}

fn normalize_acp_session_id(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn acp_response_id(message: &Value) -> Option<u64> {
    if message.get("result").is_none() && message.get("error").is_none() {
        return None;
    }
    message.get("id").and_then(Value::as_u64)
}

fn acp_is_server_request(message: &Value) -> bool {
    message.get("method").and_then(Value::as_str).is_some()
        && message.get("id").is_some()
        && message.get("result").is_none()
        && message.get("error").is_none()
}

fn parse_acp_response(message: AcpMessage, method: &str) -> Result<AcpResponse> {
    if let Some(error) = message.value.get("error") {
        return Err(anyhow!("ACP {method} failed: {}", acp_error_text(error)));
    }
    Ok(AcpResponse {
        raw: message.raw,
        result: message.value.get("result").cloned().unwrap_or(Value::Null),
    })
}

fn acp_error_text(error: &Value) -> String {
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

fn acp_error_response(id: Value, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message,
        },
    })
}

fn acp_permission_denial(params: Option<&Value>) -> Value {
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

fn acp_turn_events(message: &AcpMessage) -> Vec<TurnEvent> {
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
        "agent_message_chunk" => {
            acp_content_text(update.get("content")).map(|text| RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text,
            })
        }
        "agent_thought_chunk" => {
            acp_content_text(update.get("content")).map(|text| RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Reasoning,
                text,
            })
        }
        "tool_call" | "tool_call_update" => {
            acp_tool_status(update).map(|text| RuntimeEvent::Status { code: None, text })
        }
        _ => None,
    };

    event
        .map(|event| {
            vec![TurnEvent::with_raw(
                event,
                RawTurnPayload {
                    driver: ACP_PROTOCOL_NAME.to_string(),
                    payload: message.raw.clone(),
                },
            )]
        })
        .unwrap_or_default()
}

fn acp_content_text(value: Option<&Value>) -> Option<String> {
    let text = match value {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| acp_content_text(Some(item)))
            .collect::<Vec<_>>()
            .join(""),
        Some(Value::Object(object)) => object
            .get("text")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| acp_content_text(object.get("content")))
            .or_else(|| acp_content_text(object.get("parts")))
            .unwrap_or_default(),
        _ => String::new(),
    };
    (!text.is_empty()).then_some(text)
}

fn acp_tool_status(update: &Value) -> Option<String> {
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
    Some(format!("acp tool {status}: {title}"))
}

async fn finish_acp_session<R>(client: AcpClient, result: Result<R>) -> Result<R> {
    let shutdown = client.shutdown().await.and_then(ensure_acp_exit_success);

    match (result, shutdown) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(err)) => Err(err),
        (Err(err), Ok(())) => Err(err),
        (Err(err), Err(shutdown_err)) => {
            warn!(
                error = %shutdown_err,
                "ACP shutdown failed after runtime error"
            );
            Err(err)
        }
    }
}

fn ensure_acp_exit_success(output: ExecutionOutput) -> Result<()> {
    if output.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if stderr.is_empty() {
        return Err(anyhow!(
            "ACP process exited with {}",
            output.status_description()
        ));
    }
    Err(anyhow!(
        "ACP process exited with {}: {stderr}",
        output.status_description()
    ))
}

#[cfg(test)]
mod tests {
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
        canonical_events, ExecutionOutput, NetworkMode, RuntimeAdapter, RuntimeAuthKind,
        RuntimeEvent, RuntimeExecutionContext, RuntimeMcpServerSpec, RuntimeMessageLane,
        RuntimeProgramExecutor, RuntimeProgramSession, RuntimeProgramSpec,
        RuntimeProgramStdoutSender, RuntimeProgramTurnExecution, RuntimeSessionReady,
        RuntimeSessionStartInput, RuntimeTerminalConfig, RuntimeTerminalProgramInput,
        RuntimeTurnInput, RuntimeTurnMode, RUNTIME_SESSION_READY_MARKER,
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
                r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn","_meta":{}}}"#
                    .to_string(),
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
                r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn","_meta":{}}}"#
                    .to_string(),
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
                r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn","_meta":{}}}"#
                    .to_string(),
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
                r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn","_meta":{}}}"#
                    .to_string(),
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
                r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn","_meta":{}}}"#
                    .to_string(),
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
}
