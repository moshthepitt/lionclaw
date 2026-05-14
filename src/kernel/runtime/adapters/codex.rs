use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};
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
    active_turn: Option<ActiveCodexTurn>,
}

#[derive(Debug, Clone)]
struct ActiveCodexTurn {
    thread_id: String,
    turn_id: String,
    interrupt_tx: mpsc::UnboundedSender<CodexInterruptRequest>,
}

#[derive(Debug)]
struct CodexInterruptRequest {
    ack_tx: oneshot::Sender<Result<()>>,
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
            let (interrupt_tx, mut interrupt_rx) = mpsc::unbounded_channel();
            client
                .wait_for_turn_completed(
                    turn_id.as_deref(),
                    Some(&thread_id),
                    Some(interrupt_tx),
                    &events,
                    &thread_state,
                    Some(&mut interrupt_rx),
                )
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
            "rename" | "name" | "compact" => self.run_thread_control(execution, events).await,
            "review" => Ok(RuntimeControlOutcome::Unsupported {
                message: "Codex review control is not exposed through LionClaw yet.".to_string(),
            }),
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
        let include_hidden = match model_list_include_hidden(&input.arguments) {
            Ok(include_hidden) => include_hidden,
            Err(outcome) => return Ok(outcome),
        };
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
                        "includeHidden": include_hidden,
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
        if let Some(outcome) =
            invalid_thread_control_arguments(&input.command_name, &input.arguments)
        {
            return Ok(outcome);
        }

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
                "compact" => {
                    let (interrupt_tx, mut interrupt_rx) = mpsc::unbounded_channel();
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
                        .wait_for_context_compaction_completed(
                            &thread_id,
                            interrupt_tx,
                            &mut interrupt_rx,
                            &events,
                            &thread_state,
                        )
                        .await?;
                    RuntimeControlOutcome::Handled {
                        message: "Compacted Codex thread.".to_string(),
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

    async fn cancel(&self, handle: &RuntimeSessionHandle, _reason: Option<String>) -> Result<()> {
        let active_turn = self
            .session_state(&handle.runtime_session_id)
            .ok()
            .and_then(|state| state.active_turn);
        let Some(active_turn) = active_turn else {
            return Ok(());
        };

        let (ack_tx, ack_rx) = oneshot::channel();
        active_turn
            .interrupt_tx
            .send(CodexInterruptRequest { ack_tx })
            .map_err(|_| {
                anyhow!(
                    "codex turn interrupt channel closed before turn/interrupt for {}",
                    active_turn.turn_id
                )
            })?;

        tokio::time::timeout(Duration::from_secs(5), ack_rx)
            .await
            .map_err(|_| {
                anyhow!(
                    "timed out waiting for codex turn/interrupt acknowledgement for {}/{}",
                    active_turn.thread_id,
                    active_turn.turn_id
                )
            })?
            .map_err(|_| {
                anyhow!(
                    "codex turn interrupt task dropped acknowledgement for {}/{}",
                    active_turn.thread_id,
                    active_turn.turn_id
                )
            })?
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
        args: vec!["app-server".to_string()],
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
    completed_turn_threads: HashSet<String>,
    unmatched_turn_completed: bool,
    failed_turns: HashMap<String, String>,
    unmatched_turn_failure: Option<String>,
    started_thread_turns: HashMap<String, String>,
    completed_context_compaction_threads: HashSet<String>,
    unmatched_context_compaction_completed: bool,
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
            completed_turn_threads: HashSet::new(),
            unmatched_turn_completed: false,
            failed_turns: HashMap::new(),
            unmatched_turn_failure: None,
            started_thread_turns: HashMap::new(),
            completed_context_compaction_threads: HashSet::new(),
            unmatched_context_compaction_completed: false,
        }
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
            .send(&app_server_notification_message(method, params))
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
            .send(&app_server_request_message(id, method, params))
            .await?;

        loop {
            let Some(message) = self.transport.recv().await? else {
                bail!("codex app-server closed before responding to {method}");
            };
            if response_id(&message).is_some_and(|response_id| response_id == id) {
                return parse_app_server_response(message, method);
            }
            self.handle_message(message, events, thread_state).await?;
            if let Some(message) = self.turn_failure(None) {
                bail!("{message}");
            }
        }
    }

    async fn wait_for_turn_completed(
        &mut self,
        turn_id: Option<&str>,
        thread_id: Option<&str>,
        interrupt_tx: Option<mpsc::UnboundedSender<CodexInterruptRequest>>,
        events: &RuntimeEventSender,
        thread_state: &CodexThreadState,
        mut interrupt_rx: Option<&mut mpsc::UnboundedReceiver<CodexInterruptRequest>>,
    ) -> Result<()> {
        if let Some(message) = self.turn_failure(turn_id) {
            bail!("{message}");
        }
        let mut registered_turn_id = None;
        let result = async {
            self.register_active_wait_turn(
                thread_id,
                turn_id,
                interrupt_tx.as_ref(),
                &mut registered_turn_id,
                thread_state,
            )?;
            if self.wait_turn_completed(turn_id, thread_id) {
                return Ok(());
            }

            loop {
                if let Some(interrupt_rx) = interrupt_rx.as_mut() {
                    tokio::select! {
                        message = self.transport.recv() => {
                            let Some(message) = message? else {
                                bail!("codex app-server closed before turn completed");
                            };
                            self.handle_message(message, events, thread_state).await?;
                        }
                        interrupt = interrupt_rx.recv() => {
                            if let Some(interrupt) = interrupt {
                                self.handle_turn_interrupt(
                                    thread_id,
                                    registered_turn_id.as_deref().or(turn_id),
                                    interrupt,
                                    events,
                                    thread_state,
                                )
                                .await;
                            }
                        }
                    }
                } else {
                    let Some(message) = self.transport.recv().await? else {
                        bail!("codex app-server closed before turn completed");
                    };
                    self.handle_message(message, events, thread_state).await?;
                }
                self.register_active_wait_turn(
                    thread_id,
                    turn_id,
                    interrupt_tx.as_ref(),
                    &mut registered_turn_id,
                    thread_state,
                )?;
                if let Some(message) = self.turn_failure(turn_id) {
                    bail!("{message}");
                }
                if self.wait_turn_completed(turn_id, thread_id) {
                    return Ok(());
                }
            }
        }
        .await;

        if let (Some(thread_id), Some(turn_id)) = (thread_id, registered_turn_id.as_deref()) {
            thread_state.clear_active_turn(thread_id, turn_id)?;
        }
        result
    }

    async fn wait_for_context_compaction_completed(
        &mut self,
        thread_id: &str,
        interrupt_tx: mpsc::UnboundedSender<CodexInterruptRequest>,
        interrupt_rx: &mut mpsc::UnboundedReceiver<CodexInterruptRequest>,
        events: &RuntimeEventSender,
        thread_state: &CodexThreadState,
    ) -> Result<()> {
        if let Some(message) = self.turn_failure(None) {
            bail!("{message}");
        }
        if self.context_compaction_completed(thread_id) && self.thread_turn_completed(thread_id) {
            return Ok(());
        }

        let mut registered_turn_id = None;
        let result = async {
            self.register_active_wait_turn(
                Some(thread_id),
                None,
                Some(&interrupt_tx),
                &mut registered_turn_id,
                thread_state,
            )?;
            loop {
                tokio::select! {
                    message = self.transport.recv() => {
                        let Some(message) = message? else {
                            bail!("codex app-server closed before context compaction completed");
                        };
                        self.handle_message(message, events, thread_state).await?;
                    }
                    interrupt = interrupt_rx.recv() => {
                        if let Some(interrupt) = interrupt {
                            self.handle_turn_interrupt(
                                Some(thread_id),
                                registered_turn_id.as_deref(),
                                interrupt,
                                events,
                                thread_state,
                            )
                            .await;
                        }
                    }
                }
                self.register_active_wait_turn(
                    Some(thread_id),
                    None,
                    Some(&interrupt_tx),
                    &mut registered_turn_id,
                    thread_state,
                )?;
                if let Some(message) = self.turn_failure(None) {
                    bail!("{message}");
                }
                if self.context_compaction_completed(thread_id)
                    && self.thread_turn_completed(thread_id)
                {
                    return Ok(());
                }
            }
        }
        .await;

        if let Some(turn_id) = registered_turn_id.as_deref() {
            thread_state.clear_active_turn(thread_id, turn_id)?;
        }
        result
    }

    async fn handle_turn_interrupt(
        &mut self,
        thread_id: Option<&str>,
        turn_id: Option<&str>,
        interrupt: CodexInterruptRequest,
        events: &RuntimeEventSender,
        thread_state: &CodexThreadState,
    ) {
        let result = match (thread_id, turn_id) {
            (Some(thread_id), Some(turn_id)) => {
                self.interrupt_turn(thread_id, turn_id, events, thread_state)
                    .await
            }
            _ => Err(anyhow!(
                "codex turn/interrupt requested before app-server reported an active turn id"
            )),
        };
        drop(interrupt.ack_tx.send(result));
    }

    async fn interrupt_turn(
        &mut self,
        thread_id: &str,
        turn_id: &str,
        events: &RuntimeEventSender,
        thread_state: &CodexThreadState,
    ) -> Result<()> {
        let method = "turn/interrupt";
        let id = self.next_request_id();
        self.transport
            .send(&app_server_request_message(
                id,
                method,
                json!({
                    "threadId": thread_id,
                    "turnId": turn_id,
                }),
            ))
            .await?;

        loop {
            let Some(message) = self.transport.recv().await? else {
                bail!("codex app-server closed before responding to {method}");
            };
            if response_id(&message).is_some_and(|response_id| response_id == id) {
                parse_app_server_response(message, method)?;
                return Ok(());
            }
            self.handle_message(message, events, thread_state).await?;
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
            Some(turn_id) => {
                self.unmatched_turn_completed || self.completed_turns.contains(turn_id)
            }
            None => self.unmatched_turn_completed || !self.completed_turns.is_empty(),
        }
    }

    fn wait_turn_completed(&self, turn_id: Option<&str>, thread_id: Option<&str>) -> bool {
        match (turn_id, thread_id) {
            (Some(turn_id), _) => self.turn_completed(Some(turn_id)),
            (None, Some(thread_id)) => self.thread_turn_completed(thread_id),
            (None, None) => self.turn_completed(None),
        }
    }

    fn thread_turn_completed(&self, thread_id: &str) -> bool {
        self.unmatched_turn_completed || self.completed_turn_threads.contains(thread_id)
    }

    fn register_active_wait_turn(
        &self,
        thread_id: Option<&str>,
        turn_id: Option<&str>,
        interrupt_tx: Option<&mpsc::UnboundedSender<CodexInterruptRequest>>,
        registered_turn_id: &mut Option<String>,
        thread_state: &CodexThreadState,
    ) -> Result<()> {
        let (Some(thread_id), Some(interrupt_tx)) = (thread_id, interrupt_tx) else {
            return Ok(());
        };
        if registered_turn_id.is_some() {
            return Ok(());
        }
        let turn_id = turn_id
            .map(str::to_string)
            .or_else(|| self.started_thread_turns.get(thread_id).cloned());
        let Some(turn_id) = turn_id else {
            return Ok(());
        };

        thread_state.set_active_turn(thread_id, &turn_id, interrupt_tx.clone())?;
        *registered_turn_id = Some(turn_id);
        Ok(())
    }

    fn turn_failure(&self, turn_id: Option<&str>) -> Option<&str> {
        match turn_id {
            Some(turn_id) => self
                .failed_turns
                .get(turn_id)
                .map(String::as_str)
                .or(self.unmatched_turn_failure.as_deref()),
            None => self
                .unmatched_turn_failure
                .as_deref()
                .or_else(|| self.failed_turns.values().next().map(String::as_str)),
        }
    }

    fn remember_turn_failure(&mut self, params: &Value, message: String) {
        if let Some(turn_id) = extract_app_server_turn_id(params) {
            self.failed_turns.insert(turn_id, message);
        } else {
            self.unmatched_turn_failure = Some(message);
        }
    }

    fn context_compaction_completed(&self, thread_id: &str) -> bool {
        self.unmatched_context_compaction_completed
            || self
                .completed_context_compaction_threads
                .contains(thread_id)
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
            "turn/started" => {
                if let (Some(thread_id), Some(turn_id)) = (
                    extract_app_server_thread_id(params),
                    extract_app_server_turn_id(params),
                ) {
                    self.started_thread_turns.insert(thread_id, turn_id);
                }
                drop(events.send(RuntimeEvent::Status {
                    code: None,
                    text: "codex turn started".to_string(),
                }));
            }
            "turn/completed" => {
                if let Some(message) = completed_turn_error_text(params) {
                    self.remember_turn_failure(params, message);
                } else {
                    if let Some(thread_id) = extract_app_server_thread_id(params) {
                        self.completed_turn_threads.insert(thread_id);
                    }
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
            }
            "item/agentMessage/delta" => {
                if let Some(text) = app_server_text(params, &["delta", "text", "content"]) {
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
                let status = self.record_app_server_item(method, params);
                if let Some(status) = status {
                    drop(events.send(RuntimeEvent::Status {
                        code: None,
                        text: status,
                    }));
                }
            }
            "error" => {
                let message = app_server_error_text(params);
                if error_notification_will_retry(params) {
                    drop(events.send(RuntimeEvent::Status {
                        code: Some("runtime.retrying".to_string()),
                        text: message,
                    }));
                } else {
                    self.remember_turn_failure(params, message);
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn record_app_server_item(&mut self, method: &str, params: &Value) -> Option<String> {
        match (method, app_server_item_type(params)) {
            ("item/started", Some("contextCompaction")) => {
                Some("codex context compaction started".to_string())
            }
            ("item/completed", Some("contextCompaction")) => {
                if let Some(thread_id) = extract_app_server_thread_id(params) {
                    self.completed_context_compaction_threads.insert(thread_id);
                } else {
                    self.unmatched_context_compaction_completed = true;
                }
                Some("codex context compacted".to_string())
            }
            (_, Some(_)) => describe_app_server_item(params),
            (_, None) => None,
        }
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
        let response = app_server_error_response(id, -32601, message);
        self.transport.send(&response).await
    }
}

fn app_server_request_message(id: u64, method: &str, params: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    })
}

fn app_server_notification_message(method: &str, params: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
    })
}

fn app_server_error_response(id: Value, code: i64, message: String) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message,
        },
    })
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

fn invalid_thread_control_arguments(
    command_name: &str,
    arguments: &str,
) -> Option<RuntimeControlOutcome> {
    let arguments = arguments.trim();
    match command_name {
        "rename" | "name" if arguments.is_empty() => Some(invalid_arguments_outcome(
            "Codex rename requires a non-empty name.",
        )),
        "compact" if !arguments.is_empty() => Some(invalid_arguments_outcome(
            "Codex compact does not accept arguments.",
        )),
        _ => None,
    }
}

fn model_list_include_hidden(arguments: &str) -> std::result::Result<bool, RuntimeControlOutcome> {
    let mut include_hidden = false;
    for argument in arguments.split_whitespace() {
        match argument {
            "--hidden" | "--include-hidden" => include_hidden = true,
            argument if argument.starts_with('-') => {
                return Err(invalid_arguments_outcome(
                    "Codex model listing only accepts --hidden or --include-hidden.",
                ));
            }
            _ => {
                return Err(RuntimeControlOutcome::InteractiveOnly {
                    message:
                        "Codex model selection is interactive; configure the LionClaw runtime model instead."
                            .to_string(),
                });
            }
        }
    }
    Ok(include_hidden)
}

fn invalid_arguments_outcome(message: &str) -> RuntimeControlOutcome {
    RuntimeControlOutcome::Failed {
        code: Some("runtime.control.invalid_arguments".to_string()),
        message: message.to_string(),
    }
}

fn extract_app_server_thread_id(value: &Value) -> Option<String> {
    value
        .pointer("/thread/id")
        .and_then(Value::as_str)
        .or_else(|| value.pointer("/turn/threadId").and_then(Value::as_str))
        .or_else(|| value.pointer("/turn/thread/id").and_then(Value::as_str))
        .or_else(|| value.pointer("/item/threadId").and_then(Value::as_str))
        .or_else(|| value.get("threadId").and_then(Value::as_str))
        .or_else(|| value.get("thread_id").and_then(Value::as_str))
        .filter(|thread_id| !thread_id.trim().is_empty())
        .map(|thread_id| thread_id.trim().to_string())
}

fn extract_app_server_turn_id(value: &Value) -> Option<String> {
    value
        .pointer("/turn/id")
        .and_then(Value::as_str)
        .or_else(|| value.pointer("/item/turnId").and_then(Value::as_str))
        .or_else(|| value.get("turnId").and_then(Value::as_str))
        .or_else(|| value.get("turn_id").and_then(Value::as_str))
        .filter(|turn_id| !turn_id.trim().is_empty())
        .map(|turn_id| turn_id.trim().to_string())
}

fn app_server_item_type(params: &Value) -> Option<&str> {
    let item = params.get("item").unwrap_or(params);
    item.get("type").and_then(Value::as_str)
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

fn completed_turn_error_text(params: &Value) -> Option<String> {
    let turn = params.get("turn").unwrap_or(params);
    let status = turn
        .get("status")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|status| !status.is_empty());
    let error = turn
        .get("error")
        .or_else(|| params.get("error"))
        .filter(|error| !error.is_null());
    if status.is_none_or(|status| status.eq_ignore_ascii_case("completed")) && error.is_none() {
        return None;
    }

    let message = error
        .map(app_server_error_text)
        .or_else(|| {
            app_server_text(turn, &["message", "statusMessage", "details", "error"])
                .map(|text| text.trim().to_string())
                .filter(|text| !text.is_empty())
        })
        .or_else(|| status.map(turn_status_error_text))
        .unwrap_or_else(|| "codex turn failed".to_string());
    Some(message)
}

fn turn_status_error_text(status: &str) -> String {
    if status.eq_ignore_ascii_case("failed") {
        "codex turn failed".to_string()
    } else if status.eq_ignore_ascii_case("interrupted") {
        "codex turn interrupted".to_string()
    } else {
        format!("codex turn ended with status '{status}'")
    }
}

fn error_notification_will_retry(params: &Value) -> bool {
    params
        .get("willRetry")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn describe_app_server_item(params: &Value) -> Option<String> {
    match app_server_item_type(params)? {
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
        .get("displayName")
        .and_then(Value::as_str)
        .or_else(|| value.get("model").and_then(Value::as_str))
        .or_else(|| value.get("id").and_then(Value::as_str))
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
    fn set_active_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
        interrupt_tx: mpsc::UnboundedSender<CodexInterruptRequest>,
    ) -> Result<()> {
        let mut sessions = self
            .sessions
            .write()
            .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?;
        let session = sessions
            .get_mut(&self.runtime_session_id)
            .ok_or_else(|| anyhow!("runtime session '{}' not found", self.runtime_session_id))?;
        session.active_turn = Some(ActiveCodexTurn {
            thread_id: thread_id.to_string(),
            turn_id: turn_id.to_string(),
            interrupt_tx,
        });
        drop(sessions);
        Ok(())
    }

    fn clear_active_turn(&self, thread_id: &str, turn_id: &str) -> Result<()> {
        let mut sessions = self
            .sessions
            .write()
            .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?;
        let Some(session) = sessions.get_mut(&self.runtime_session_id) else {
            return Ok(());
        };
        if session
            .active_turn
            .as_ref()
            .is_some_and(|active| active.thread_id == thread_id && active.turn_id == turn_id)
        {
            session.active_turn = None;
        }
        drop(sessions);
        Ok(())
    }

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
        path::PathBuf,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    };

    use crate::kernel::runtime::{
        ConfinementConfig, EffectiveExecutionPlan, ExecutionLimits, ExecutionOutput, NetworkMode,
        OciConfinementConfig, RuntimeAdapter, RuntimeControlExecution, RuntimeControlInput,
        RuntimeControlOrigin, RuntimeControlOutcome, RuntimeEvent, RuntimeMessageLane,
        RuntimeSessionHandle, RuntimeSessionStartInput, WorkspaceAccess,
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

    async fn start_codex_test_session(
        runtime_state_root: Option<PathBuf>,
    ) -> (
        CodexRuntimeAdapter,
        RuntimeSessionHandle,
        super::CodexThreadState,
    ) {
        start_codex_test_session_with_config(CodexRuntimeConfig::default(), runtime_state_root)
            .await
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
        let handle = adapter
            .session_start(RuntimeSessionStartInput {
                session_id: Uuid::new_v4(),
                working_dir: None,
                environment: Vec::new(),
                runtime_skill_ids: Vec::new(),
                runtime_state_root,
            })
            .await
            .expect("start");
        let thread_state = adapter.thread_state_for(&handle.runtime_session_id);
        (adapter, handle, thread_state)
    }

    fn is_jsonrpc_message(message: &Value) -> bool {
        message.get("jsonrpc").and_then(Value::as_str) == Some("2.0")
    }

    fn codex_protocol_fixture(name: &str) -> Value {
        let raw = match name {
            "compact_context_compaction_v2" => include_str!(
                "../../../../tests/fixtures/codex_app_server/compact_context_compaction_v2.json"
            ),
            "turn_interrupt_v2" => {
                include_str!("../../../../tests/fixtures/codex_app_server/turn_interrupt_v2.json")
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
    fn codex_app_server_program_uses_default_stdio_transport() {
        let program = build_codex_app_server_program(&CodexRuntimeConfig {
            executable: "codex".to_string(),
            model: Some("gpt-5-codex".to_string()),
        });

        assert_eq!(program.executable, "codex");
        assert_eq!(program.args, vec!["app-server".to_string()]);
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
                    plan: test_execution_plan(),
                    runtime_secrets_mount: None,
                    codex_home_override: None,
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
        assert!(sent.iter().all(is_jsonrpc_message));
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
    async fn codex_app_server_protocol_resumes_saved_thread_id() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        std::fs::write(
            runtime_state_root.join(crate::home::RUNTIME_SESSION_READY_MARKER),
            "",
        )
        .expect("write ready marker");
        std::fs::write(
            runtime_state_root.join(CODEX_THREAD_ID_STATE_FILE),
            "thr_saved\n",
        )
        .expect("write thread id");

        let (adapter, handle, thread_state) =
            start_codex_test_session(Some(runtime_state_root)).await;
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
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0]["method"], "thread/compact/start");
        assert_eq!(sent[0]["params"]["threadId"], "thr_1");

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

        let (method, params) = fixture_client_request(&fixture);
        let sent = sent.lock().expect("sent lock").clone();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0]["method"], method);
        assert_eq!(sent[0]["params"], params);

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
            .handle_message(
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
                && is_jsonrpc_message(message)
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

    fn test_execution_plan() -> EffectiveExecutionPlan {
        EffectiveExecutionPlan {
            runtime_id: "codex".to_string(),
            preset_name: "everyday".to_string(),
            confinement: ConfinementConfig::Oci(OciConfinementConfig::default()),
            workspace_access: WorkspaceAccess::ReadWrite,
            network_mode: NetworkMode::On,
            working_dir: None,
            environment: Vec::new(),
            idle_timeout: Duration::from_secs(30),
            hard_timeout: Duration::from_secs(90),
            mounts: Vec::new(),
            mount_runtime_secrets: false,
            escape_classes: Default::default(),
            limits: ExecutionLimits::default(),
        }
    }
}
