use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, bail, Context, Result};
use lionclaw_runtime_api::{
    ExecutionOutput, RuntimeArtifact, RuntimeEvent, RuntimeExecutionContext, RuntimeMessageLane,
};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tracing::warn;

use crate::state::{CodexInterruptRequest, CodexThreadState};

use super::event_mapping::{
    agent_message_phase_lane, app_server_error_text, app_server_item_events, app_server_item_type,
    app_server_text, completed_turn_error_text, error_notification_will_retry,
};
use super::generated_artifacts::{
    codex_default_generated_image_path, codex_generated_image_call_id, codex_generated_image_path,
    codex_generated_image_payload, codex_generated_image_saved_path,
};
use super::protocol::{
    app_server_error_response, app_server_notification_message, app_server_request_message,
    extract_app_server_item_id, extract_app_server_thread_id, extract_app_server_turn_id,
    parse_app_server_response, response_id,
};
use super::sink::CodexAppServerEventSink;
use super::transport::{AppServerMessage, AppServerTransport};

pub(crate) struct CodexAppServerClient<T> {
    transport: T,
    runtime_context: Option<RuntimeExecutionContext>,
    next_id: u64,
    completed_turns: HashSet<String>,
    completed_turn_threads: HashSet<String>,
    unmatched_turn_completed: bool,
    failed_turns: HashMap<String, String>,
    unmatched_turn_failure: Option<String>,
    started_thread_turns: HashMap<String, String>,
    agent_message_lanes: HashMap<String, RuntimeMessageLane>,
    active_agent_message_item_id: Option<String>,
    active_answer_item_ids: HashSet<String>,
    last_answer_item_id: Option<String>,
    completed_context_compaction_threads: HashSet<String>,
    unmatched_context_compaction_completed: bool,
    emitted_artifact_ids: HashSet<String>,
}

impl<T> CodexAppServerClient<T>
where
    T: AppServerTransport + Send,
{
    pub(crate) fn new(transport: T) -> Self {
        Self::new_with_optional_runtime_context(transport, None)
    }

    pub(crate) fn new_with_runtime_context(
        transport: T,
        runtime_context: RuntimeExecutionContext,
    ) -> Self {
        Self::new_with_optional_runtime_context(transport, Some(runtime_context))
    }

    pub(crate) fn new_with_optional_runtime_context(
        transport: T,
        runtime_context: Option<RuntimeExecutionContext>,
    ) -> Self {
        Self {
            transport,
            runtime_context,
            next_id: 1,
            completed_turns: HashSet::new(),
            completed_turn_threads: HashSet::new(),
            unmatched_turn_completed: false,
            failed_turns: HashMap::new(),
            unmatched_turn_failure: None,
            started_thread_turns: HashMap::new(),
            agent_message_lanes: HashMap::new(),
            active_agent_message_item_id: None,
            active_answer_item_ids: HashSet::new(),
            last_answer_item_id: None,
            completed_context_compaction_threads: HashSet::new(),
            unmatched_context_compaction_completed: false,
            emitted_artifact_ids: HashSet::new(),
        }
    }

    pub(crate) async fn initialize<'a>(
        &mut self,
        sink: impl Into<CodexAppServerEventSink<'a>>,
        thread_state: &CodexThreadState,
    ) -> Result<()> {
        let sink = sink.into();
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
            sink,
            thread_state,
        )
        .await?;
        self.notify("initialized", json!({})).await
    }

    pub(crate) async fn notify(&mut self, method: &str, params: Value) -> Result<()> {
        self.transport
            .send(&app_server_notification_message(method, params))
            .await
    }

    pub(crate) async fn request<'a>(
        &mut self,
        method: &str,
        params: Value,
        sink: impl Into<CodexAppServerEventSink<'a>>,
        thread_state: &CodexThreadState,
    ) -> Result<Value> {
        let sink = sink.into();
        let id = self.next_request_id();
        self.transport
            .send(&app_server_request_message(id, method, params))
            .await?;

        loop {
            let Some(message) = self.transport.recv().await? else {
                bail!("codex app-server closed before responding to {method}");
            };
            if response_id(message.value()).is_some_and(|response_id| response_id == id) {
                return parse_app_server_response(message.into_value(), method);
            }
            self.dispatch_message(message, sink, thread_state).await?;
            if let Some(message) = self.turn_failure(None) {
                bail!("{message}");
            }
        }
    }

    pub(crate) async fn wait_for_turn_completed<'a>(
        &mut self,
        turn_id: Option<&str>,
        thread_id: Option<&str>,
        interrupt_tx: Option<mpsc::UnboundedSender<CodexInterruptRequest>>,
        sink: impl Into<CodexAppServerEventSink<'a>>,
        thread_state: &CodexThreadState,
        mut interrupt_rx: Option<&mut mpsc::UnboundedReceiver<CodexInterruptRequest>>,
    ) -> Result<()> {
        let sink = sink.into();
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
                            self.dispatch_message(message, sink, thread_state).await?;
                        }
                        interrupt = interrupt_rx.recv() => {
                            if let Some(interrupt) = interrupt {
                                self.handle_turn_interrupt(
                                    thread_id,
                                    registered_turn_id.as_deref().or(turn_id),
                                    interrupt,
                                    sink,
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
                    self.dispatch_message(message, sink, thread_state).await?;
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

    pub(crate) async fn wait_for_context_compaction_completed<'a>(
        &mut self,
        thread_id: &str,
        interrupt_tx: mpsc::UnboundedSender<CodexInterruptRequest>,
        interrupt_rx: &mut mpsc::UnboundedReceiver<CodexInterruptRequest>,
        sink: impl Into<CodexAppServerEventSink<'a>>,
        thread_state: &CodexThreadState,
    ) -> Result<()> {
        let sink = sink.into();
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
                        self.dispatch_message(message, sink, thread_state).await?;
                    }
                    interrupt = interrupt_rx.recv() => {
                        if let Some(interrupt) = interrupt {
                            self.handle_turn_interrupt(
                                Some(thread_id),
                                registered_turn_id.as_deref(),
                                interrupt,
                                sink,
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

    pub(crate) async fn handle_turn_interrupt<'a>(
        &mut self,
        thread_id: Option<&str>,
        turn_id: Option<&str>,
        interrupt: CodexInterruptRequest,
        sink: impl Into<CodexAppServerEventSink<'a>>,
        thread_state: &CodexThreadState,
    ) {
        let sink = sink.into();
        let result = match (thread_id, turn_id) {
            (Some(thread_id), Some(turn_id)) => {
                self.interrupt_turn(thread_id, turn_id, sink, thread_state)
                    .await
            }
            _ => Err(anyhow!(
                "codex turn/interrupt requested before app-server reported an active turn id"
            )),
        };
        drop(interrupt.ack_tx.send(result));
    }

    pub(crate) async fn interrupt_turn<'a>(
        &mut self,
        thread_id: &str,
        turn_id: &str,
        sink: impl Into<CodexAppServerEventSink<'a>>,
        thread_state: &CodexThreadState,
    ) -> Result<()> {
        let sink = sink.into();
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
            if response_id(message.value()).is_some_and(|response_id| response_id == id) {
                parse_app_server_response(message.into_value(), method)?;
                return Ok(());
            }
            self.dispatch_message(message, sink, thread_state).await?;
        }
    }

    pub(crate) async fn shutdown(mut self) -> Result<ExecutionOutput> {
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

    pub(crate) fn register_active_wait_turn(
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

    pub(crate) fn turn_failure(&self, turn_id: Option<&str>) -> Option<&str> {
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

    pub(crate) fn remember_turn_failure(&mut self, params: &Value, message: String) {
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

    /// Translate one app-server message into the canonical `RuntimeEvent`s it
    /// produces, in order. Protocol side effects (responding to server-initiated
    /// requests, persisting the thread id, recording turn state) happen here;
    /// emitting the returned events to a sender or journal is the caller's job.
    pub(crate) async fn handle_message(
        &mut self,
        message: Value,
        thread_state: &CodexThreadState,
    ) -> Result<Vec<RuntimeEvent>> {
        let mut events = Vec::new();
        if message.get("id").is_some() && message.get("method").is_some() {
            self.respond_to_server_request(&message).await?;
            return Ok(events);
        }

        let Some(method) = message.get("method").and_then(Value::as_str) else {
            if let Some(artifact) =
                self.generated_artifact_for_message(None, &message, thread_state)?
            {
                events.push(RuntimeEvent::Artifact { artifact });
            }
            return Ok(events);
        };
        let params = message.get("params").unwrap_or(&Value::Null);
        if let Some(artifact) =
            self.generated_artifact_for_message(Some(method), params, thread_state)?
        {
            events.push(RuntimeEvent::Artifact { artifact });
        }
        match method {
            "thread/started" => {
                if let Some(thread_id) = extract_app_server_thread_id(params) {
                    thread_state.persist_thread_id(&thread_id)?;
                    events.push(RuntimeEvent::Status {
                        code: None,
                        text: format!("codex thread started: {thread_id}"),
                    });
                }
            }
            "thread/name/updated" => {
                events.push(RuntimeEvent::Status {
                    code: None,
                    text: "codex thread renamed".to_string(),
                });
            }
            "turn/started" => {
                self.reset_turn_message_state();
                if let (Some(thread_id), Some(turn_id)) = (
                    extract_app_server_thread_id(params),
                    extract_app_server_turn_id(params),
                ) {
                    self.started_thread_turns.insert(thread_id, turn_id);
                }
                events.push(RuntimeEvent::Status {
                    code: None,
                    text: "codex turn started".to_string(),
                });
            }
            "turn/completed" => {
                self.reset_turn_message_state();
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
                    events.push(RuntimeEvent::Status {
                        code: None,
                        text: "codex turn completed".to_string(),
                    });
                    events.push(RuntimeEvent::Done);
                }
            }
            "item/agentMessage/delta" => {
                if let Some(text) = app_server_text(params, &["delta", "text", "content"]) {
                    let lane = self.agent_message_lane(params);
                    if lane == RuntimeMessageLane::Answer {
                        if let Some(boundary) = self.record_answer_delta_boundary(params) {
                            events.push(boundary);
                        }
                    }
                    events.push(RuntimeEvent::MessageDelta { lane, text });
                }
            }
            "item/agentReasoning/delta" | "item/agentReasoningRawContent/delta" => {
                if let Some(text) = app_server_text(params, &["delta", "text", "content"]) {
                    events.push(RuntimeEvent::MessageDelta {
                        lane: RuntimeMessageLane::Reasoning,
                        text,
                    });
                }
            }
            "item/started" | "item/completed" | "item/updated" => {
                events.extend(self.record_app_server_item(method, params));
            }
            "error" => {
                let message = app_server_error_text(params);
                if error_notification_will_retry(params) {
                    events.push(RuntimeEvent::Status {
                        code: Some("runtime.retrying".to_string()),
                        text: message,
                    });
                } else {
                    self.remember_turn_failure(params, message);
                }
            }
            _ => {}
        }
        Ok(events)
    }

    /// Translate one app-server message and forward its canonical events to the
    /// selected live sink.
    pub(crate) async fn dispatch_message<'a, M>(
        &mut self,
        message: M,
        sink: impl Into<CodexAppServerEventSink<'a>>,
        thread_state: &CodexThreadState,
    ) -> Result<()>
    where
        M: Into<AppServerMessage>,
    {
        let sink = sink.into();
        let message = message.into();
        let raw_payload = message.raw;
        for event in self.handle_message(message.value, thread_state).await? {
            sink.send(event, &raw_payload);
        }
        Ok(())
    }

    fn generated_artifact_for_message(
        &mut self,
        method: Option<&str>,
        message: &Value,
        thread_state: &CodexThreadState,
    ) -> Result<Option<RuntimeArtifact>> {
        let Some(payload) = codex_generated_image_payload(method, message) else {
            return Ok(None);
        };

        let Some(call_id) = codex_generated_image_call_id(payload) else {
            return Ok(None);
        };
        let thread_id = extract_app_server_thread_id(payload)
            .or_else(|| extract_app_server_thread_id(message))
            .or_else(|| thread_state.current_thread_id().ok().flatten());
        let Some(thread_id) = thread_id else {
            return Ok(None);
        };
        let Some(runtime_state_root) = thread_state.runtime_state_root()? else {
            return Ok(None);
        };

        let filename = format!("{call_id}.png");
        let path = if let Some(saved_path) = codex_generated_image_saved_path(payload) {
            let Some(path) = codex_generated_image_path(
                saved_path,
                &runtime_state_root,
                self.runtime_context.as_ref(),
            ) else {
                return Ok(None);
            };
            path
        } else {
            codex_default_generated_image_path(
                &thread_id,
                &filename,
                &runtime_state_root,
                self.runtime_context.as_ref(),
            )
        };
        let artifact_id = format!("codex:image:{thread_id}:{call_id}");
        if !self.emitted_artifact_ids.insert(artifact_id.clone()) {
            return Ok(None);
        }
        let filename = path
            .file_name()
            .and_then(|name| name.to_str())
            .map(str::to_string)
            .unwrap_or(filename);

        Ok(Some(RuntimeArtifact {
            artifact_id,
            path,
            filename: Some(filename),
            mime_type: Some("image/png".to_string()),
        }))
    }

    fn record_app_server_item(&mut self, method: &str, params: &Value) -> Vec<RuntimeEvent> {
        self.record_agent_message_item(method, params);
        self.record_answer_item_completion(method, params);
        match (method, app_server_item_type(params)) {
            ("item/started", Some("contextCompaction")) => {
                vec![RuntimeEvent::Status {
                    code: None,
                    text: "codex context compaction started".to_string(),
                }]
            }
            ("item/completed", Some("contextCompaction")) => {
                if let Some(thread_id) = extract_app_server_thread_id(params) {
                    self.completed_context_compaction_threads.insert(thread_id);
                } else {
                    self.unmatched_context_compaction_completed = true;
                }
                vec![RuntimeEvent::Status {
                    code: None,
                    text: "codex context compacted".to_string(),
                }]
            }
            (_, Some(_)) => app_server_item_events(params),
            (_, None) => Vec::new(),
        }
    }

    fn record_answer_delta_boundary(&mut self, params: &Value) -> Option<RuntimeEvent> {
        let item_id = self.agent_message_item_id(params)?;
        self.begin_answer_item(item_id)
    }

    fn record_answer_item_completion(&mut self, method: &str, params: &Value) {
        if method != "item/completed" || app_server_item_type(params) != Some("agentMessage") {
            return;
        }
        let Some(item_id) = extract_app_server_item_id(params) else {
            return;
        };
        if self
            .agent_message_lanes
            .get(&item_id)
            .copied()
            .unwrap_or(RuntimeMessageLane::Answer)
            == RuntimeMessageLane::Answer
        {
            self.active_answer_item_ids.remove(&item_id);
        }
        self.agent_message_lanes.remove(&item_id);
    }

    fn begin_answer_item(&mut self, item_id: String) -> Option<RuntimeEvent> {
        let boundary = if self.active_answer_item_ids.insert(item_id.clone())
            && self
                .last_answer_item_id
                .as_deref()
                .is_some_and(|last_item_id| last_item_id != item_id)
        {
            Some(RuntimeEvent::MessageBoundary {
                lane: RuntimeMessageLane::Answer,
            })
        } else {
            None
        };
        self.last_answer_item_id = Some(item_id);
        boundary
    }

    fn reset_turn_message_state(&mut self) {
        self.agent_message_lanes.clear();
        self.active_agent_message_item_id = None;
        self.active_answer_item_ids.clear();
        self.last_answer_item_id = None;
    }

    fn record_agent_message_item(&mut self, method: &str, params: &Value) {
        if app_server_item_type(params) != Some("agentMessage") {
            return;
        }
        let Some(item_id) = extract_app_server_item_id(params) else {
            return;
        };
        match method {
            "item/started" | "item/updated" => {
                self.active_agent_message_item_id = Some(item_id.clone());
                if let Some(lane) = agent_message_phase_lane(params) {
                    self.agent_message_lanes.insert(item_id, lane);
                }
            }
            "item/completed"
                if self.active_agent_message_item_id.as_deref() == Some(item_id.as_str()) =>
            {
                self.active_agent_message_item_id = None;
            }
            _ => {}
        }
    }

    fn agent_message_lane(&self, params: &Value) -> RuntimeMessageLane {
        self.agent_message_item_id(params)
            .and_then(|item_id| self.agent_message_lanes.get(&item_id).copied())
            .or_else(|| agent_message_phase_lane(params))
            .unwrap_or(RuntimeMessageLane::Answer)
    }

    fn agent_message_item_id(&self, params: &Value) -> Option<String> {
        extract_app_server_item_id(params).or_else(|| self.active_agent_message_item_id.clone())
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

fn ensure_app_server_exit_success(output: ExecutionOutput) -> Result<()> {
    if output.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if stderr.is_empty() {
        bail!(
            "codex app-server exited with {}",
            output.status_description()
        );
    }
    bail!(
        "codex app-server exited with {}: {stderr}",
        output.status_description()
    );
}

pub(crate) async fn finish_app_server_session<T, R>(
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
