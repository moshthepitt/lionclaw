use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use serde_json::{json, Value};
use tracing::warn;
use uuid::Uuid;

use crate::kernel::runtime::{
    spawn_interactive, ExecutionOutput, ExecutionRequest, ExecutionSession, NetworkMode,
    RuntimeAdapter, RuntimeAdapterInfo, RuntimeAuthKind, RuntimeCapabilityResult,
    RuntimeControlExecution, RuntimeControlOutcome, RuntimeEvent, RuntimeEventSender,
    RuntimeMessageLane, RuntimeProgramSpec, RuntimeProgramTurnExecution, RuntimeSessionHandle,
    RuntimeSessionStartInput, RuntimeTurnMode, RuntimeTurnResult,
};

#[derive(Debug, Clone)]
pub struct CodexRuntimeConfig {
    pub executable: String,
    pub model: Option<String>,
}

impl Default for CodexRuntimeConfig {
    fn default() -> Self {
        Self {
            executable: "codex".to_string(),
            model: None,
        }
    }
}

#[derive(Debug)]
pub struct CodexRuntimeAdapter {
    config: CodexRuntimeConfig,
    sessions: Arc<RwLock<HashMap<String, CodexSessionState>>>,
}

const CODEX_THREAD_ID_STATE_FILE: &str = ".lionclaw-codex-thread-id";

#[derive(Debug, Clone)]
struct CodexSessionState {
    runtime_state_root: Option<PathBuf>,
    thread_id: Option<String>,
}

#[derive(Clone)]
struct CodexThreadState {
    sessions: Arc<RwLock<HashMap<String, CodexSessionState>>>,
    runtime_session_id: String,
}

impl CodexRuntimeAdapter {
    pub fn new(config: CodexRuntimeConfig) -> Self {
        Self {
            config,
            sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn run_app_server_turn(
        &self,
        execution: RuntimeProgramTurnExecution,
        events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
        let RuntimeProgramTurnExecution {
            input,
            plan,
            runtime_secrets_mount,
            codex_home_override,
        } = execution;
        let network_mode = plan.network_mode;
        let thread_state = self.thread_state_for(&input.runtime_session_id);
        let transport = self
            .start_app_server_transport(plan, runtime_secrets_mount, codex_home_override)
            .await?;
        let mut client = CodexAppServerClient::new(transport);

        let result = async {
            client.initialize(&events, &thread_state).await?;
            let thread_id = self
                .ensure_app_server_thread(
                    &mut client,
                    &input.runtime_session_id,
                    &events,
                    &thread_state,
                )
                .await?;
            let response = client
                .request(
                    "turn/start",
                    turn_start_params(
                        &thread_id,
                        &input.prompt,
                        self.config.model.as_deref(),
                        network_mode,
                    ),
                    &events,
                    &thread_state,
                )
                .await?;
            let turn_id = extract_app_server_turn_id(&response);
            client
                .wait_for_turn_completed(turn_id.as_deref(), &events, &thread_state)
                .await?;
            Ok(RuntimeTurnResult::default())
        }
        .await;

        finish_app_server_session(client, result).await
    }

    async fn run_app_server_control(
        &self,
        execution: RuntimeControlExecution,
        events: RuntimeEventSender,
    ) -> Result<RuntimeControlOutcome> {
        let command = execution.input.command_name.as_str();
        match command {
            "model" | "models" => self.run_model_list_control(execution, events).await,
            "permissions" | "permission" => Ok(RuntimeControlOutcome::InteractiveOnly {
                message:
                    "Codex permission changes must be made through LionClaw runtime policy, not a runtime slash command."
                        .to_string(),
            }),
            "rename" | "name" | "compact" | "review" => {
                self.run_thread_control(execution, events).await
            }
            _ => Ok(RuntimeControlOutcome::Unsupported {
                message: format!("Codex does not support native control command '/{command}'"),
            }),
        }
    }

    async fn run_model_list_control(
        &self,
        execution: RuntimeControlExecution,
        events: RuntimeEventSender,
    ) -> Result<RuntimeControlOutcome> {
        let RuntimeControlExecution {
            plan,
            runtime_secrets_mount,
            codex_home_override,
            input,
        } = execution;
        let thread_state = self.thread_state_for(&input.runtime_session_id);
        let transport = self
            .start_app_server_transport(plan, runtime_secrets_mount, codex_home_override)
            .await?;
        let mut client = CodexAppServerClient::new(transport);
        let result = async {
            client.initialize(&events, &thread_state).await?;
            let response = client
                .request(
                    "model/list",
                    json!({
                        "includeHidden": input.arguments.split_whitespace().any(|arg| arg == "--hidden"),
                    }),
                    &events,
                    &thread_state,
                )
                .await?;

            Ok(RuntimeControlOutcome::Handled {
                message: describe_model_list_response(&response),
            })
        }
        .await;

        finish_app_server_session(client, result).await
    }

    async fn run_thread_control(
        &self,
        execution: RuntimeControlExecution,
        events: RuntimeEventSender,
    ) -> Result<RuntimeControlOutcome> {
        let RuntimeControlExecution {
            plan,
            runtime_secrets_mount,
            codex_home_override,
            input,
        } = execution;
        let Some(saved_thread_id) = self.current_thread_id(&input.runtime_session_id)? else {
            return Ok(RuntimeControlOutcome::Failed {
                code: Some("runtime.control.thread_required".to_string()),
                message: format!(
                    "Codex control '/{}' needs an existing Codex thread; send a prompt first.",
                    input.command_name
                ),
            });
        };

        let thread_state = self.thread_state_for(&input.runtime_session_id);
        let transport = self
            .start_app_server_transport(plan, runtime_secrets_mount, codex_home_override)
            .await?;
        let mut client = CodexAppServerClient::new(transport);

        let result = async {
            client.initialize(&events, &thread_state).await?;
            let thread_id = self
                .resume_app_server_thread(&mut client, &saved_thread_id, &events, &thread_state)
                .await?;

            let outcome = match input.command_name.as_str() {
                "rename" | "name" => {
                    let name = input.arguments.trim();
                    if name.is_empty() {
                        RuntimeControlOutcome::Failed {
                            code: Some("runtime.control.invalid_arguments".to_string()),
                            message: "Codex rename requires a non-empty name.".to_string(),
                        }
                    } else {
                        client
                            .request(
                                "thread/name/set",
                                json!({
                                    "threadId": thread_id,
                                    "name": name,
                                }),
                                &events,
                                &thread_state,
                            )
                            .await?;
                        RuntimeControlOutcome::Handled {
                            message: format!("Renamed Codex thread to '{name}'."),
                        }
                    }
                }
                "compact" => {
                    client
                        .request(
                            "thread/compact/start",
                            json!({
                                "threadId": thread_id,
                            }),
                            &events,
                            &thread_state,
                        )
                        .await?;
                    client
                        .wait_for_thread_compacted(Some(&thread_id), &events, &thread_state)
                        .await?;
                    RuntimeControlOutcome::Handled {
                        message: "Compacted Codex thread.".to_string(),
                    }
                }
                "review" => {
                    let response = client
                        .request(
                            "review/start",
                            json!({
                                "threadId": thread_id,
                                "delivery": "inline",
                                "target": review_target_from_arguments(&input.arguments),
                            }),
                            &events,
                            &thread_state,
                        )
                        .await?;
                    let turn_id = extract_app_server_turn_id(&response);
                    client
                        .wait_for_turn_completed(turn_id.as_deref(), &events, &thread_state)
                        .await?;
                    let answer = client.answer_text().trim().to_string();
                    RuntimeControlOutcome::Handled {
                        message: if answer.is_empty() {
                            "Codex review completed.".to_string()
                        } else {
                            answer
                        },
                    }
                }
                other => RuntimeControlOutcome::Unsupported {
                    message: format!("Codex does not support native control command '/{other}'"),
                },
            };

            Ok(outcome)
        }
        .await;

        finish_app_server_session(client, result).await
    }

    async fn start_app_server_transport(
        &self,
        plan: crate::kernel::runtime::EffectiveExecutionPlan,
        runtime_secrets_mount: Option<crate::kernel::runtime::RuntimeSecretsMount>,
        codex_home_override: Option<PathBuf>,
    ) -> Result<ExecutionSessionTransport> {
        let session = spawn_interactive(ExecutionRequest {
            plan,
            program: build_codex_app_server_program(&self.config),
            runtime_secrets_mount,
            codex_home_override,
        })
        .await?;
        Ok(ExecutionSessionTransport::new(session))
    }

    async fn ensure_app_server_thread<T>(
        &self,
        client: &mut CodexAppServerClient<T>,
        runtime_session_id: &str,
        events: &RuntimeEventSender,
        thread_state: &CodexThreadState,
    ) -> Result<String>
    where
        T: AppServerTransport + Send,
    {
        if let Some(thread_id) = self.current_thread_id(runtime_session_id)? {
            return self
                .resume_app_server_thread(client, &thread_id, events, thread_state)
                .await;
        }

        let response = client
            .request(
                "thread/start",
                thread_start_params(self.config.model.as_deref()),
                events,
                thread_state,
            )
            .await?;
        let thread_id = match extract_app_server_thread_id(&response) {
            Some(thread_id) => thread_id,
            None => self.current_thread_id(runtime_session_id)?.ok_or_else(|| {
                anyhow!("codex app-server thread/start response missing thread id")
            })?,
        };
        thread_state.persist_thread_id(&thread_id)?;
        Ok(thread_id)
    }

    async fn resume_app_server_thread<T>(
        &self,
        client: &mut CodexAppServerClient<T>,
        thread_id: &str,
        events: &RuntimeEventSender,
        thread_state: &CodexThreadState,
    ) -> Result<String>
    where
        T: AppServerTransport + Send,
    {
        let response = client
            .request(
                "thread/resume",
                thread_resume_params(thread_id, self.config.model.as_deref()),
                events,
                thread_state,
            )
            .await?;
        let resolved_thread_id =
            extract_app_server_thread_id(&response).unwrap_or_else(|| thread_id.to_string());
        thread_state.persist_thread_id(&resolved_thread_id)?;
        Ok(resolved_thread_id)
    }

    fn thread_state_for(&self, runtime_session_id: &str) -> CodexThreadState {
        CodexThreadState {
            sessions: Arc::clone(&self.sessions),
            runtime_session_id: runtime_session_id.to_string(),
        }
    }

    fn current_thread_id(&self, runtime_session_id: &str) -> Result<Option<String>> {
        Ok(self.session_state(runtime_session_id)?.thread_id)
    }

    fn session_state(&self, runtime_session_id: &str) -> Result<CodexSessionState> {
        self.sessions
            .read()
            .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?
            .get(runtime_session_id)
            .cloned()
            .ok_or_else(|| anyhow!("runtime session '{runtime_session_id}' not found"))
    }
}

#[async_trait]
impl RuntimeAdapter for CodexRuntimeAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "codex".to_string(),
            version: "0.1".to_string(),
            healthy: !self.config.executable.trim().is_empty(),
        }
    }

    fn turn_mode(&self) -> RuntimeTurnMode {
        RuntimeTurnMode::ProgramBacked
    }

    async fn session_start(&self, input: RuntimeSessionStartInput) -> Result<RuntimeSessionHandle> {
        let runtime_session_id = format!("codex-{}", Uuid::new_v4());
        let thread_id = match input.runtime_state_root.as_deref() {
            Some(root) if runtime_session_is_ready(root)? => load_saved_thread_id(root)?,
            Some(_) | None => None,
        };
        let resumes_existing_session = thread_id.is_some();
        self.sessions
            .write()
            .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?
            .insert(
                runtime_session_id.clone(),
                CodexSessionState {
                    runtime_state_root: input.runtime_state_root,
                    thread_id,
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
        events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
        self.run_app_server_turn(execution, events).await
    }

    async fn runtime_control(
        &self,
        execution: RuntimeControlExecution,
        events: RuntimeEventSender,
    ) -> Result<RuntimeControlOutcome> {
        self.run_app_server_control(execution, events).await
    }

    async fn resolve_capability_requests(
        &self,
        _handle: &RuntimeSessionHandle,
        results: Vec<RuntimeCapabilityResult>,
        events: RuntimeEventSender,
    ) -> Result<()> {
        if !results.is_empty() {
            return Err(anyhow!(
                "codex adapter does not support runtime-side capability request resolution"
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
            .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?
            .remove(&handle.runtime_session_id);
        Ok(())
    }
}

fn build_codex_app_server_program(config: &CodexRuntimeConfig) -> RuntimeProgramSpec {
    RuntimeProgramSpec {
        executable: config.executable.clone(),
        args: vec![
            "app-server".to_string(),
            "--listen".to_string(),
            "stdio://".to_string(),
        ],
        environment: Vec::new(),
        stdin: String::new(),
        auth: Some(RuntimeAuthKind::Codex),
    }
}

#[async_trait]
trait AppServerTransport {
    async fn send(&mut self, message: &Value) -> Result<()>;
    async fn recv(&mut self) -> Result<Option<Value>>;
    async fn shutdown(&mut self) -> Result<ExecutionOutput>;
}

struct ExecutionSessionTransport {
    session: Option<ExecutionSession>,
}

impl ExecutionSessionTransport {
    fn new(session: ExecutionSession) -> Self {
        Self {
            session: Some(session),
        }
    }
}

#[async_trait]
impl AppServerTransport for ExecutionSessionTransport {
    async fn send(&mut self, message: &Value) -> Result<()> {
        let session = self
            .session
            .as_mut()
            .context("codex app-server session is already closed")?;
        session.write_line(&serde_json::to_string(message)?).await
    }

    async fn recv(&mut self) -> Result<Option<Value>> {
        let session = self
            .session
            .as_mut()
            .context("codex app-server session is already closed")?;
        loop {
            let Some(line) = session.read_line().await? else {
                return Ok(None);
            };
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            return serde_json::from_str(trimmed)
                .with_context(|| format!("invalid codex app-server JSON-RPC line: {trimmed}"))
                .map(Some);
        }
    }

    async fn shutdown(&mut self) -> Result<ExecutionOutput> {
        let Some(session) = self.session.take() else {
            return Ok(ExecutionOutput::default());
        };
        session.shutdown().await
    }
}

struct CodexAppServerClient<T> {
    transport: T,
    next_id: u64,
    completed_turns: HashSet<String>,
    unmatched_turn_completed: bool,
    compacted_threads: HashSet<String>,
    unmatched_thread_compacted: bool,
    answer_text: String,
}

impl<T> CodexAppServerClient<T>
where
    T: AppServerTransport + Send,
{
    fn new(transport: T) -> Self {
        Self {
            transport,
            next_id: 1,
            completed_turns: HashSet::new(),
            unmatched_turn_completed: false,
            compacted_threads: HashSet::new(),
            unmatched_thread_compacted: false,
            answer_text: String::new(),
        }
    }

    fn answer_text(&self) -> &str {
        &self.answer_text
    }

    async fn initialize(
        &mut self,
        events: &RuntimeEventSender,
        thread_state: &CodexThreadState,
    ) -> Result<()> {
        self.request(
            "initialize",
            json!({
                "clientInfo": {
                    "name": "lionclaw",
                    "title": "LionClaw",
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "capabilities": {
                    "experimentalApi": true,
                },
            }),
            events,
            thread_state,
        )
        .await?;
        self.notify("initialized", json!({})).await
    }

    async fn notify(&mut self, method: &str, params: Value) -> Result<()> {
        self.transport
            .send(&json!({
                "method": method,
                "params": params,
            }))
            .await
    }

    async fn request(
        &mut self,
        method: &str,
        params: Value,
        events: &RuntimeEventSender,
        thread_state: &CodexThreadState,
    ) -> Result<Value> {
        let id = self.next_request_id();
        self.transport
            .send(&json!({
                "id": id,
                "method": method,
                "params": params,
            }))
            .await?;

        loop {
            let Some(message) = self.transport.recv().await? else {
                bail!("codex app-server closed before responding to {method}");
            };
            if response_id(&message).is_some_and(|response_id| response_id == id) {
                return parse_app_server_response(message, method);
            }
            self.handle_message(message, events, thread_state).await?;
        }
    }

    async fn wait_for_turn_completed(
        &mut self,
        turn_id: Option<&str>,
        events: &RuntimeEventSender,
        thread_state: &CodexThreadState,
    ) -> Result<()> {
        if self.turn_completed(turn_id) {
            return Ok(());
        }

        loop {
            let Some(message) = self.transport.recv().await? else {
                bail!("codex app-server closed before turn completed");
            };
            self.handle_message(message, events, thread_state).await?;
            if self.turn_completed(turn_id) {
                return Ok(());
            }
        }
    }

    async fn wait_for_thread_compacted(
        &mut self,
        thread_id: Option<&str>,
        events: &RuntimeEventSender,
        thread_state: &CodexThreadState,
    ) -> Result<()> {
        if self.thread_compacted(thread_id) {
            return Ok(());
        }

        loop {
            let Some(message) = self.transport.recv().await? else {
                bail!("codex app-server closed before thread compacted");
            };
            self.handle_message(message, events, thread_state).await?;
            if self.thread_compacted(thread_id) {
                return Ok(());
            }
        }
    }

    async fn shutdown(mut self) -> Result<ExecutionOutput> {
        self.transport.shutdown().await
    }

    fn next_request_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn turn_completed(&self, turn_id: Option<&str>) -> bool {
        match turn_id {
            Some(turn_id) => self.completed_turns.contains(turn_id),
            None => self.unmatched_turn_completed || !self.completed_turns.is_empty(),
        }
    }

    fn thread_compacted(&self, thread_id: Option<&str>) -> bool {
        match thread_id {
            Some(thread_id) => self.compacted_threads.contains(thread_id),
            None => self.unmatched_thread_compacted || !self.compacted_threads.is_empty(),
        }
    }

    async fn handle_message(
        &mut self,
        message: Value,
        events: &RuntimeEventSender,
        thread_state: &CodexThreadState,
    ) -> Result<()> {
        if message.get("id").is_some() && message.get("method").is_some() {
            self.respond_to_server_request(&message).await?;
            return Ok(());
        }

        let Some(method) = message.get("method").and_then(Value::as_str) else {
            return Ok(());
        };
        let params = message.get("params").unwrap_or(&Value::Null);
        match method {
            "thread/started" => {
                if let Some(thread_id) = extract_app_server_thread_id(params) {
                    thread_state.persist_thread_id(&thread_id)?;
                    drop(events.send(RuntimeEvent::Status {
                        code: None,
                        text: format!("codex thread started: {thread_id}"),
                    }));
                }
            }
            "thread/name/updated" => {
                drop(events.send(RuntimeEvent::Status {
                    code: None,
                    text: "codex thread renamed".to_string(),
                }));
            }
            "thread/compacted" => {
                if let Some(thread_id) = extract_app_server_thread_id(params) {
                    self.compacted_threads.insert(thread_id);
                } else {
                    self.unmatched_thread_compacted = true;
                }
                drop(events.send(RuntimeEvent::Status {
                    code: None,
                    text: "codex thread compacted".to_string(),
                }));
            }
            "turn/started" => {
                drop(events.send(RuntimeEvent::Status {
                    code: None,
                    text: "codex turn started".to_string(),
                }));
            }
            "turn/completed" => {
                if let Some(turn_id) = extract_app_server_turn_id(params) {
                    self.completed_turns.insert(turn_id);
                } else {
                    self.unmatched_turn_completed = true;
                }
                drop(events.send(RuntimeEvent::Status {
                    code: None,
                    text: "codex turn completed".to_string(),
                }));
                drop(events.send(RuntimeEvent::Done));
            }
            "item/agentMessage/delta" => {
                if let Some(text) = app_server_text(params, &["delta", "text", "content"]) {
                    self.answer_text.push_str(&text);
                    drop(events.send(RuntimeEvent::MessageDelta {
                        lane: RuntimeMessageLane::Answer,
                        text,
                    }));
                }
            }
            "item/agentReasoning/delta" | "item/agentReasoningRawContent/delta" => {
                if let Some(text) = app_server_text(params, &["delta", "text", "content"]) {
                    drop(events.send(RuntimeEvent::MessageDelta {
                        lane: RuntimeMessageLane::Reasoning,
                        text,
                    }));
                }
            }
            "item/started" | "item/completed" | "item/updated" => {
                if let Some(status) = describe_app_server_item(params) {
                    drop(events.send(RuntimeEvent::Status {
                        code: None,
                        text: status,
                    }));
                }
            }
            "error" => {
                drop(events.send(RuntimeEvent::Error {
                    code: Some("runtime.error".to_string()),
                    text: app_server_error_text(params),
                }));
            }
            _ => {}
        }
        Ok(())
    }

    async fn respond_to_server_request(&mut self, message: &Value) -> Result<()> {
        let id = message
            .get("id")
            .cloned()
            .context("codex app-server request missing id")?;
        let method = message.get("method").and_then(Value::as_str).unwrap_or("");
        let message = match method {
            "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
                format!(
                    "LionClaw does not grant runtime-internal approval callback '{method}'; runtime permissions are controlled by LionClaw policy"
                )
            }
            _ => format!("unsupported LionClaw app-server callback '{method}'"),
        };
        let response = json!({
            "id": id,
            "error": {
                "code": -32601,
                "message": message,
            },
        });
        self.transport.send(&response).await
    }
}

fn response_id(message: &Value) -> Option<u64> {
    message.get("id").and_then(Value::as_u64)
}

fn parse_app_server_response(message: Value, method: &str) -> Result<Value> {
    if let Some(error) = message.get("error") {
        bail!(
            "codex app-server {method} failed: {}",
            app_server_error_text(error)
        );
    }
    Ok(message.get("result").cloned().unwrap_or(Value::Null))
}

fn ensure_app_server_exit_success(output: ExecutionOutput) -> Result<()> {
    if output.success() {
        return Ok(());
    }
    let code = output.exit_code.unwrap_or(1);
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if stderr.is_empty() {
        bail!("codex app-server exited with code {code}");
    }
    bail!("codex app-server exited with code {code}: {stderr}");
}

async fn finish_app_server_session<T, R>(
    client: CodexAppServerClient<T>,
    result: Result<R>,
) -> Result<R>
where
    T: AppServerTransport + Send,
{
    let shutdown = client
        .shutdown()
        .await
        .and_then(ensure_app_server_exit_success);

    match (result, shutdown) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(err)) => Err(err),
        (Err(err), Ok(())) => Err(err),
        (Err(err), Err(shutdown_err)) => {
            warn!(
                error = %shutdown_err,
                "codex app-server shutdown failed after runtime error"
            );
            Err(err)
        }
    }
}

fn thread_start_params(model: Option<&str>) -> Value {
    let mut params = json!({
        "approvalPolicy": "never",
        "threadSource": "user",
    });
    insert_optional_model(&mut params, model);
    params
}

fn thread_resume_params(thread_id: &str, model: Option<&str>) -> Value {
    let mut params = json!({
        "threadId": thread_id,
        "approvalPolicy": "never",
    });
    insert_optional_model(&mut params, model);
    params
}

fn turn_start_params(
    thread_id: &str,
    prompt: &str,
    model: Option<&str>,
    network_mode: NetworkMode,
) -> Value {
    let mut params = json!({
        "threadId": thread_id,
        "input": [
            {
                "type": "text",
                "text": prompt,
            }
        ],
        "approvalPolicy": "never",
        "sandboxPolicy": {
            "type": "externalSandbox",
            "networkAccess": match network_mode {
                NetworkMode::On => "enabled",
                NetworkMode::None => "restricted",
            },
        },
    });
    insert_optional_model(&mut params, model);
    params
}

fn insert_optional_model(params: &mut Value, model: Option<&str>) {
    let Some(model) = model else {
        return;
    };
    let Some(params) = params.as_object_mut() else {
        return;
    };
    params.insert("model".to_string(), json!(model));
}

fn review_target_from_arguments(arguments: &str) -> Value {
    let trimmed = arguments.trim();
    if trimmed.is_empty() || trimmed == "changes" || trimmed == "uncommitted" {
        return json!({"type": "uncommittedChanges"});
    }
    if let Some(branch) = trimmed
        .strip_prefix("base ")
        .or_else(|| trimmed.strip_prefix("baseBranch "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return json!({
            "type": "baseBranch",
            "branch": branch,
        });
    }
    if let Some(sha) = trimmed
        .strip_prefix("commit ")
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return json!({
            "type": "commit",
            "sha": sha,
        });
    }
    json!({
        "type": "custom",
        "instructions": trimmed,
    })
}

fn extract_app_server_thread_id(value: &Value) -> Option<String> {
    value
        .pointer("/thread/id")
        .and_then(Value::as_str)
        .or_else(|| value.get("threadId").and_then(Value::as_str))
        .or_else(|| value.get("thread_id").and_then(Value::as_str))
        .or_else(|| value.get("id").and_then(Value::as_str))
        .filter(|thread_id| !thread_id.trim().is_empty())
        .map(|thread_id| thread_id.trim().to_string())
}

fn extract_app_server_turn_id(value: &Value) -> Option<String> {
    value
        .pointer("/turn/id")
        .and_then(Value::as_str)
        .or_else(|| value.get("turnId").and_then(Value::as_str))
        .or_else(|| value.get("turn_id").and_then(Value::as_str))
        .or_else(|| value.get("id").and_then(Value::as_str))
        .filter(|turn_id| !turn_id.trim().is_empty())
        .map(|turn_id| turn_id.trim().to_string())
}

fn app_server_text(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .filter_map(|key| value.get(*key))
        .find_map(|value| collect_codex_text(value, 0))
}

fn app_server_error_text(value: &Value) -> String {
    app_server_text(value, &["message", "text", "details", "error"])
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| "codex app-server error".to_string())
}

fn describe_app_server_item(params: &Value) -> Option<String> {
    let item = params.get("item").unwrap_or(params);
    let item_type = item.get("type").and_then(Value::as_str)?;
    match item_type {
        "enteredReviewMode" => Some("codex review started".to_string()),
        "exitedReviewMode" => Some("codex review completed".to_string()),
        other => Some(format!("codex item: {other}")),
    }
}

fn describe_model_list_response(response: &Value) -> String {
    let models: Vec<String> = response
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(model_display_name)
        .take(20)
        .collect();

    if models.is_empty() {
        return "Codex returned no available models.".to_string();
    }

    format!("Available Codex models: {}.", models.join(", "))
}

fn model_display_name(value: &Value) -> Option<String> {
    value
        .get("id")
        .and_then(Value::as_str)
        .or_else(|| value.get("name").and_then(Value::as_str))
        .or_else(|| value.get("label").and_then(Value::as_str))
        .filter(|name| !name.trim().is_empty())
        .map(|name| name.trim().to_string())
}

fn load_saved_thread_id(root: &Path) -> Result<Option<String>> {
    let path = root.join(CODEX_THREAD_ID_STATE_FILE);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(Into::into(err)),
    };
    if metadata.file_type().is_symlink() {
        return Err(anyhow!(
            "codex thread state file '{}' cannot be a symlink",
            path.display()
        ));
    }
    if !metadata.is_file() {
        return Ok(None);
    }

    let thread_id = fs::read_to_string(&path)
        .with_context(|| format!("failed to read codex thread state '{}'", path.display()))?
        .trim()
        .to_string();
    if thread_id.is_empty() {
        return Ok(None);
    }
    Ok(Some(thread_id))
}

fn save_thread_id(root: &Path, thread_id: &str) -> Result<()> {
    let thread_id = thread_id.trim();
    if thread_id.is_empty() {
        return Ok(());
    }

    let path = root.join(CODEX_THREAD_ID_STATE_FILE);
    match fs::symlink_metadata(&path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                return Err(anyhow!(
                    "codex thread state file '{}' cannot be a symlink",
                    path.display()
                ));
            }
            if !metadata.is_file() {
                return Err(anyhow!(
                    "codex thread state path '{}' must be a regular file",
                    path.display()
                ));
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(Into::into(err)),
    }

    let temp_path = root.join(format!(
        "{}.{}.tmp",
        CODEX_THREAD_ID_STATE_FILE,
        Uuid::new_v4().simple()
    ));
    fs::write(&temp_path, format!("{thread_id}\n"))?;
    fs::rename(&temp_path, &path)?;
    Ok(())
}

fn runtime_session_is_ready(root: &Path) -> Result<bool> {
    Ok(root
        .join(crate::home::RUNTIME_SESSION_READY_MARKER)
        .is_file())
}

impl CodexThreadState {
    fn persist_thread_id(&self, thread_id: &str) -> Result<()> {
        let root = self
            .sessions
            .read()
            .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?
            .get(&self.runtime_session_id)
            .ok_or_else(|| anyhow!("runtime session '{}' not found", self.runtime_session_id))?
            .runtime_state_root
            .clone();

        if let Some(root) = root.as_deref() {
            save_thread_id(root, thread_id)?;
        }

        {
            let mut sessions = self
                .sessions
                .write()
                .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?;
            let session = sessions.get_mut(&self.runtime_session_id).ok_or_else(|| {
                anyhow!("runtime session '{}' not found", self.runtime_session_id)
            })?;
            session.thread_id = Some(thread_id.to_string());
            drop(sessions);
        }
        Ok(())
    }
}

fn collect_codex_text(value: &Value, depth: usize) -> Option<String> {
    if depth > 6 {
        return None;
    }

    match value {
        Value::String(text) if text.is_empty() => None,
        Value::String(text) => Some(text.clone()),
        Value::Array(values) => {
            let parts: Vec<String> = values
                .iter()
                .filter_map(|value| collect_codex_text(value, depth + 1))
                .collect();
            if parts.is_empty() {
                None
            } else {
                Some(parts.join(""))
            }
        }
        Value::Object(map) => {
            for key in [
                "delta",
                "text",
                "content",
                "message",
                "last_agent_message",
                "reasoning_text",
                "raw_content",
                "summary_text",
            ] {
                if let Some(value) = map.get(key) {
                    if let Some(text) = collect_codex_text(value, depth + 1) {
                        return Some(text);
                    }
                }
            }
            None
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
    };

    use crate::kernel::runtime::{
        ExecutionOutput, RuntimeAdapter, RuntimeEvent, RuntimeMessageLane, RuntimeSessionStartInput,
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use serde_json::{json, Value};

    use super::{
        build_codex_app_server_program, AppServerTransport, CodexAppServerClient,
        CodexRuntimeAdapter, CodexRuntimeConfig, CODEX_THREAD_ID_STATE_FILE,
    };
    use uuid::Uuid;

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

    #[async_trait]
    impl AppServerTransport for FakeAppServerTransport {
        async fn send(&mut self, message: &Value) -> Result<()> {
            self.sent.lock().expect("sent lock").push(message.clone());
            Ok(())
        }

        async fn recv(&mut self) -> Result<Option<Value>> {
            Ok(self.incoming.lock().expect("incoming lock").pop_front())
        }

        async fn shutdown(&mut self) -> Result<ExecutionOutput> {
            self.shutdowns.fetch_add(1, Ordering::SeqCst);
            Ok(self.output.clone())
        }
    }

    #[test]
    fn codex_app_server_program_uses_stdio_listener() {
        let program = build_codex_app_server_program(&CodexRuntimeConfig {
            executable: "codex".to_string(),
            model: Some("gpt-5-codex".to_string()),
        });

        assert_eq!(program.executable, "codex");
        assert_eq!(
            program.args,
            vec![
                "app-server".to_string(),
                "--listen".to_string(),
                "stdio://".to_string(),
            ]
        );
        assert_eq!(program.stdin, "");
        assert_eq!(
            program.auth,
            Some(crate::kernel::runtime::RuntimeAuthKind::Codex)
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

    #[tokio::test]
    async fn codex_app_server_protocol_streams_turn_and_saves_thread_id() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        let adapter = CodexRuntimeAdapter::new(CodexRuntimeConfig {
            executable: "codex".to_string(),
            model: Some("gpt-5-codex".to_string()),
        });
        let handle = adapter
            .session_start(RuntimeSessionStartInput {
                session_id: Uuid::new_v4(),
                working_dir: None,
                environment: Vec::new(),
                runtime_skill_ids: Vec::new(),
                runtime_state_root: Some(runtime_state_root.clone()),
            })
            .await
            .expect("start");
        let thread_state = adapter.thread_state_for(&handle.runtime_session_id);
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
                super::turn_start_params(
                    &thread_id,
                    "hello",
                    Some("gpt-5-codex"),
                    crate::kernel::runtime::NetworkMode::None,
                ),
                &event_tx,
                &thread_state,
            )
            .await
            .expect("turn start");
        client
            .wait_for_turn_completed(
                super::extract_app_server_turn_id(&response).as_deref(),
                &event_tx,
                &thread_state,
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
    async fn codex_app_server_protocol_rejects_internal_approval_callbacks() {
        let adapter = CodexRuntimeAdapter::new(CodexRuntimeConfig::default());
        let handle = adapter
            .session_start(RuntimeSessionStartInput {
                session_id: Uuid::new_v4(),
                working_dir: None,
                environment: Vec::new(),
                runtime_skill_ids: Vec::new(),
                runtime_state_root: None,
            })
            .await
            .expect("start");
        let thread_state = adapter.thread_state_for(&handle.runtime_session_id);
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
                &event_tx,
                &thread_state,
            )
            .await
            .expect("turn completed");

        let sent = sent.lock().expect("sent lock").clone();
        assert!(sent.iter().any(|message| {
            message.get("id").and_then(Value::as_str) == Some("approval-1")
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

        let err = super::finish_app_server_session::<_, ()>(
            client,
            Err(anyhow::anyhow!("protocol failed")),
        )
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

        let adapter = CodexRuntimeAdapter::new(CodexRuntimeConfig::default());
        let handle = adapter
            .session_start(RuntimeSessionStartInput {
                session_id: Uuid::new_v4(),
                working_dir: None,
                environment: Vec::new(),
                runtime_skill_ids: Vec::new(),
                runtime_state_root: Some(runtime_state_root),
            })
            .await
            .expect("start");
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

        let adapter = CodexRuntimeAdapter::new(CodexRuntimeConfig::default());
        let handle = adapter
            .session_start(RuntimeSessionStartInput {
                session_id: Uuid::new_v4(),
                working_dir: None,
                environment: Vec::new(),
                runtime_skill_ids: Vec::new(),
                runtime_state_root: Some(runtime_state_root),
            })
            .await
            .expect("start");
        assert!(!handle.resumes_existing_session);

        adapter.close(&handle).await.expect("close");
    }

    #[tokio::test]
    async fn symlinked_thread_file_is_rejected() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        std::fs::write(
            runtime_state_root.join(crate::home::RUNTIME_SESSION_READY_MARKER),
            "",
        )
        .expect("write ready marker");
        let target = temp_dir.path().join("thread-id-target");
        std::fs::write(&target, "thread-old\n").expect("write target");
        symlink(&target, runtime_state_root.join(CODEX_THREAD_ID_STATE_FILE))
            .expect("create symlink");

        let adapter = CodexRuntimeAdapter::new(CodexRuntimeConfig::default());
        let err = adapter
            .session_start(RuntimeSessionStartInput {
                session_id: Uuid::new_v4(),
                working_dir: None,
                environment: Vec::new(),
                runtime_skill_ids: Vec::new(),
                runtime_state_root: Some(runtime_state_root),
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
        std::fs::write(
            runtime_a.join(crate::home::RUNTIME_SESSION_READY_MARKER),
            "",
        )
        .expect("write ready marker a");
        std::fs::write(
            runtime_b.join(crate::home::RUNTIME_SESSION_READY_MARKER),
            "",
        )
        .expect("write ready marker b");
        std::fs::write(runtime_a.join(CODEX_THREAD_ID_STATE_FILE), "thread-a\n")
            .expect("write thread a");
        std::fs::write(runtime_b.join(CODEX_THREAD_ID_STATE_FILE), "thread-b\n")
            .expect("write thread b");

        let adapter = CodexRuntimeAdapter::new(CodexRuntimeConfig::default());
        let handle_a = adapter
            .session_start(RuntimeSessionStartInput {
                session_id: Uuid::new_v4(),
                working_dir: None,
                environment: Vec::new(),
                runtime_skill_ids: Vec::new(),
                runtime_state_root: Some(runtime_a),
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
}
