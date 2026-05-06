use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;

use crate::kernel::runtime::{
    ExecutionOutput, RuntimeAdapter, RuntimeAdapterInfo, RuntimeAuthKind, RuntimeCapabilityResult,
    RuntimeEvent, RuntimeEventSender, RuntimeMessageLane, RuntimeProgramOutputParser,
    RuntimeProgramSpec, RuntimeSessionHandle, RuntimeSessionStartInput, RuntimeTurnInput,
    RuntimeTurnMode,
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
    last_launch_resumed_session: bool,
}

#[derive(Default)]
struct CodexOutputParser {
    emitted_answer: bool,
    pending_unphased_agent_message: Option<String>,
    thread_state: Option<CodexThreadState>,
    saw_thread_id: bool,
}

#[derive(Clone)]
struct CodexThreadState {
    sessions: Arc<RwLock<HashMap<String, CodexSessionState>>>,
    runtime_session_id: String,
    expects_new_thread_id: bool,
}

impl RuntimeProgramOutputParser for CodexOutputParser {
    fn parse_line(&mut self, line: &str) -> Vec<RuntimeEvent> {
        parse_codex_output_line(line, self)
    }

    fn finish(&mut self) -> Vec<RuntimeEvent> {
        let mut events = Vec::new();
        flush_pending_agent_message(&mut events, self, RuntimeMessageLane::Answer);
        if let Some(state) = &self.thread_state {
            if state.expects_new_thread_id && !self.saw_thread_id {
                events.push(RuntimeEvent::Status {
                    code: None,
                    text: "codex did not report a thread id; future turns will start fresh"
                        .to_string(),
                });
            }
        }
        events
    }
}

impl CodexRuntimeAdapter {
    pub fn new(config: CodexRuntimeConfig) -> Self {
        Self {
            config,
            sessions: Arc::new(RwLock::new(HashMap::new())),
        }
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
                    last_launch_resumed_session: false,
                },
            );

        Ok(RuntimeSessionHandle {
            runtime_session_id,
            resumes_existing_session,
        })
    }

    fn build_turn_program(&self, input: &RuntimeTurnInput) -> Result<RuntimeProgramSpec> {
        let thread_id = {
            let mut sessions = self
                .sessions
                .write()
                .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?;
            let thread_id = {
                let session = sessions.get_mut(&input.runtime_session_id).ok_or_else(|| {
                    anyhow!("runtime session '{}' not found", input.runtime_session_id)
                })?;
                session.last_launch_resumed_session = session.thread_id.is_some();
                session.thread_id.clone()
            };
            drop(sessions);
            thread_id
        };

        Ok(RuntimeProgramSpec {
            executable: self.config.executable.clone(),
            args: build_codex_exec_args(self.config.model.as_deref(), thread_id.as_deref()),
            environment: Vec::new(),
            stdin: input.prompt.clone(),
            auth: Some(RuntimeAuthKind::Codex),
        })
    }

    fn program_output_parser(
        &self,
        input: &RuntimeTurnInput,
    ) -> Option<Box<dyn RuntimeProgramOutputParser>> {
        let expects_new_thread_id = self
            .sessions
            .read()
            .ok()
            .and_then(|sessions| sessions.get(&input.runtime_session_id).cloned())
            .map(|session| session.thread_id.is_none())
            .unwrap_or(false);

        Some(Box::new(CodexOutputParser {
            thread_state: Some(CodexThreadState {
                sessions: Arc::clone(&self.sessions),
                runtime_session_id: input.runtime_session_id.clone(),
                expects_new_thread_id,
            }),
            ..CodexOutputParser::default()
        }))
    }

    fn parse_program_output_line(&self, line: &str) -> Vec<RuntimeEvent> {
        let mut parser = CodexOutputParser::default();
        parser.parse_line(line)
    }

    fn format_program_exit_error(
        &self,
        output: &ExecutionOutput,
        _observed_error_text: Option<&str>,
    ) -> String {
        let code = output.exit_code.unwrap_or(1);
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if stderr.is_empty() {
            format!("codex exec exited with code {code}")
        } else {
            format!("codex exec exited with code {code}: {stderr}")
        }
    }

    fn prepare_program_retry_after_failure(
        &self,
        input: &RuntimeTurnInput,
        _output: &ExecutionOutput,
        _observed_error_text: Option<&str>,
        events: &RuntimeEventSender,
    ) -> Result<bool> {
        let root_to_clear = {
            let mut sessions = self
                .sessions
                .write()
                .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?;
            let Some(session) = sessions.get_mut(&input.runtime_session_id) else {
                return Ok(false);
            };
            if !session.last_launch_resumed_session {
                return Ok(false);
            }
            session.thread_id = None;
            session.last_launch_resumed_session = false;
            let root = session.runtime_state_root.clone();
            drop(sessions);
            root
        };
        if let Some(root) = root_to_clear.as_deref() {
            clear_saved_thread_id(root)?;
        }
        drop(events.send(RuntimeEvent::Status {
            code: None,
            text: "codex continuity restarted with a fresh thread".to_string(),
        }));
        Ok(true)
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

fn build_codex_exec_args(model: Option<&str>, thread_id: Option<&str>) -> Vec<String> {
    let mut args = vec!["exec".to_string()];

    if thread_id.is_some() {
        args.push("resume".to_string());
    }

    // LionClaw already provides the outer confinement boundary via Podman, so
    // Codex should use its official external-sandbox mode rather than trying
    // to nest bubblewrap inside the container.
    args.push("--dangerously-bypass-approvals-and-sandbox".to_string());

    args.push("--json".to_string());

    if let Some(model) = model {
        args.push("--model".to_string());
        args.push(model.to_string());
    }

    if let Some(thread_id) = thread_id {
        args.push(thread_id.to_string());
        args.push("-".to_string());
    }

    args
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

    let thread_id = match fs::read_to_string(&path) {
        Ok(contents) => contents.trim().to_string(),
        Err(_) => return Ok(None),
    };
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

fn clear_saved_thread_id(root: &Path) -> Result<()> {
    let path = root.join(CODEX_THREAD_ID_STATE_FILE);
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(Into::into(err)),
    }
}

fn runtime_session_is_ready(root: &Path) -> Result<bool> {
    Ok(root
        .join(crate::home::RUNTIME_SESSION_READY_MARKER)
        .is_file())
}

#[cfg(test)]
fn parse_codex_stdout(stdout: &[u8]) -> Vec<RuntimeEvent> {
    let output = String::from_utf8_lossy(stdout);
    let mut events = Vec::new();
    let mut parser = CodexOutputParser::default();

    for line in output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        events.extend(parser.parse_line(line));
    }
    events.extend(parser.finish());

    events
}

fn parse_codex_output_line(line: &str, parser: &mut CodexOutputParser) -> Vec<RuntimeEvent> {
    let line = line.trim();
    if line.is_empty() {
        return Vec::new();
    }

    let mut events = Vec::new();
    if let Ok(json) = serde_json::from_str::<Value>(line) {
        parse_codex_json_line(&mut events, &json, parser);
    } else {
        push_message_delta(
            &mut events,
            parser,
            RuntimeMessageLane::Answer,
            line.to_string(),
        );
    }
    events
}

fn parse_codex_json_line(
    events: &mut Vec<RuntimeEvent>,
    json: &Value,
    parser: &mut CodexOutputParser,
) {
    match json.get("type").and_then(Value::as_str) {
        Some("event_msg") => {
            if let Some(payload) = json.get("payload") {
                parse_codex_json_event(events, payload, parser);
            }
        }
        Some("response_item") => {}
        _ => {
            if codex_event_type(json).is_some() {
                parse_codex_json_event(events, json, parser);
            }
        }
    }
}

fn parse_codex_json_event(
    events: &mut Vec<RuntimeEvent>,
    json: &Value,
    parser: &mut CodexOutputParser,
) {
    let payload = codex_event_payload(json);
    let event_type = codex_event_type(json);
    match event_type {
        Some("thread.started") => {
            if let Some(thread_id) = extract_codex_thread_id(payload) {
                parser.saw_thread_id = true;
                if let Some(state) = &parser.thread_state {
                    if let Err(err) = state.persist_thread_id(thread_id) {
                        events.push(RuntimeEvent::Status {
                            code: None,
                            text: format!("codex thread id could not be saved: {err}"),
                        });
                    }
                }
                events.push(RuntimeEvent::Status {
                    code: None,
                    text: format!("codex thread started: {thread_id}"),
                });
            }
        }
        Some("turn.started") | Some("turn_started") | Some("task_started") => {
            events.push(RuntimeEvent::Status {
                code: None,
                text: "codex turn started".to_string(),
            });
        }
        Some("turn.completed") | Some("turn_complete") => {
            flush_pending_agent_message(events, parser, RuntimeMessageLane::Answer);
            events.push(RuntimeEvent::Status {
                code: None,
                text: "codex turn completed".to_string(),
            });
        }
        Some("task_complete") => {
            if let Some(text) = codex_event_text(payload, &["last_agent_message"]) {
                parser.pending_unphased_agent_message = None;
                if !parser.emitted_answer {
                    push_message_delta(events, parser, RuntimeMessageLane::Answer, text);
                }
            } else if !parser.emitted_answer {
                flush_pending_agent_message(events, parser, RuntimeMessageLane::Answer);
            } else {
                parser.pending_unphased_agent_message = None;
            }
            events.push(RuntimeEvent::Status {
                code: None,
                text: "codex turn completed".to_string(),
            });
            events.push(RuntimeEvent::Done);
        }
        Some("turn.failed") => {
            let message = payload
                .pointer("/error/message")
                .and_then(Value::as_str)
                .unwrap_or("codex turn failed");
            flush_pending_agent_message(events, parser, RuntimeMessageLane::Reasoning);
            events.push(RuntimeEvent::Error {
                code: Some("runtime.error".to_string()),
                text: format!("codex: {message}"),
            });
        }
        Some("turn_aborted") => {
            let message = codex_event_text(payload, &["reason"])
                .unwrap_or_else(|| "codex turn aborted".to_string());
            flush_pending_agent_message(events, parser, RuntimeMessageLane::Reasoning);
            events.push(RuntimeEvent::Error {
                code: Some("runtime.error".to_string()),
                text: format!("codex: {message}"),
            });
        }
        Some("error") => {
            let message = payload
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("codex stream error");
            flush_pending_agent_message(events, parser, RuntimeMessageLane::Reasoning);
            events.push(RuntimeEvent::Error {
                code: Some("runtime.error".to_string()),
                text: format!("codex: {message}"),
            });
        }
        Some("stream_error") => {
            let message = codex_event_text(payload, &["message", "details"])
                .unwrap_or_else(|| "codex stream error".to_string());
            flush_pending_agent_message(events, parser, RuntimeMessageLane::Reasoning);
            events.push(RuntimeEvent::Error {
                code: Some("runtime.error".to_string()),
                text: format!("codex: {message}"),
            });
        }
        Some("agent_message_delta") | Some("agent_message_content_delta") => {
            if let Some(text) = codex_event_text(payload, &["delta", "text", "content"]) {
                push_agent_message(events, parser, payload, text);
            }
        }
        Some("agent_message") => {
            if let Some(text) = codex_event_text(payload, &["message", "text", "content"]) {
                push_agent_message(events, parser, payload, text);
            }
        }
        Some("agent_reasoning_delta")
        | Some("agent_reasoning_raw_content_delta")
        | Some("reasoning_content_delta") => {
            if let Some(text) = codex_event_text(payload, &["delta", "text", "content"]) {
                push_message_delta(events, parser, RuntimeMessageLane::Reasoning, text);
            }
        }
        Some("agent_reasoning") | Some("agent_reasoning_raw_content") => {
            if let Some(text) = codex_event_text(
                payload,
                &["text", "content", "reasoning_text", "raw_content"],
            ) {
                push_message_delta(events, parser, RuntimeMessageLane::Reasoning, text);
            }
        }
        Some("agent_reasoning_section_break")
        | Some("session_configured")
        | Some("token_count") => {}
        Some("exec_command_begin") => {
            flush_pending_agent_message(events, parser, RuntimeMessageLane::Reasoning);
            let command = codex_event_text(payload, &["parsed_cmd", "command"])
                .unwrap_or_else(|| "command".to_string());
            events.push(RuntimeEvent::Status {
                code: None,
                text: format!("codex command '{}'", truncate_status_detail(&command)),
            });
        }
        Some("exec_command_end") => {
            flush_pending_agent_message(events, parser, RuntimeMessageLane::Reasoning);
            let exit_code = payload
                .get("exit_code")
                .and_then(Value::as_i64)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            events.push(RuntimeEvent::Status {
                code: None,
                text: format!("codex command completed (exit {exit_code})"),
            });
        }
        Some("web_search_begin") => {
            flush_pending_agent_message(events, parser, RuntimeMessageLane::Reasoning);
            events.push(RuntimeEvent::Status {
                code: None,
                text: "codex web search".to_string(),
            });
        }
        Some("web_search_end") => {
            flush_pending_agent_message(events, parser, RuntimeMessageLane::Reasoning);
            events.push(RuntimeEvent::Status {
                code: None,
                text: describe_codex_web_search(payload),
            });
        }
        Some("patch_apply_begin") => {
            flush_pending_agent_message(events, parser, RuntimeMessageLane::Reasoning);
            events.push(RuntimeEvent::Status {
                code: None,
                text: "codex patch apply".to_string(),
            });
        }
        Some("patch_apply_end") => {
            flush_pending_agent_message(events, parser, RuntimeMessageLane::Reasoning);
            let status = payload
                .get("success")
                .and_then(Value::as_bool)
                .map(|value| if value { "succeeded" } else { "failed" })
                .unwrap_or("completed");
            events.push(RuntimeEvent::Status {
                code: None,
                text: format!("codex patch apply ({status})"),
            });
        }
        Some("item.started")
        | Some("item.updated")
        | Some("item.completed")
        | Some("item_started")
        | Some("item_updated")
        | Some("item_completed") => {
            if let Some(item) = payload.get("item") {
                parse_codex_item(events, item, parser);
            }
        }
        Some(_) | None => {}
    }
}

fn push_agent_message(
    events: &mut Vec<RuntimeEvent>,
    parser: &mut CodexOutputParser,
    payload: &Value,
    text: String,
) {
    match codex_agent_message_lane(payload) {
        Some(lane) => {
            parser.discard_pending_duplicate(&text);
            push_message_delta(events, parser, lane, text);
        }
        None => parser.push_pending_unphased_agent_message(text),
    }
}

fn push_message_delta(
    events: &mut Vec<RuntimeEvent>,
    parser: &mut CodexOutputParser,
    lane: RuntimeMessageLane,
    text: String,
) {
    if matches!(lane, RuntimeMessageLane::Answer) {
        parser.emitted_answer = true;
    }
    events.push(RuntimeEvent::MessageDelta { lane, text });
}

fn flush_pending_agent_message(
    events: &mut Vec<RuntimeEvent>,
    parser: &mut CodexOutputParser,
    lane: RuntimeMessageLane,
) {
    if let Some(text) = parser.pending_unphased_agent_message.take() {
        push_message_delta(events, parser, lane, text);
    }
}

impl CodexOutputParser {
    fn push_pending_unphased_agent_message(&mut self, text: String) {
        let Some(pending) = &mut self.pending_unphased_agent_message else {
            self.pending_unphased_agent_message = Some(text);
            return;
        };

        if text == *pending || pending.ends_with(&text) {
            return;
        }
        if text.starts_with(pending.as_str()) {
            *pending = text;
            return;
        }
        pending.push_str(&text);
    }

    fn discard_pending_duplicate(&mut self, text: &str) {
        let Some(pending) = self.pending_unphased_agent_message.as_deref() else {
            return;
        };
        if pending == text || text.starts_with(pending) || pending.ends_with(text) {
            self.pending_unphased_agent_message = None;
        }
    }
}

impl CodexThreadState {
    fn persist_thread_id(&self, thread_id: &str) -> Result<()> {
        let root = self
            .sessions
            .read()
            .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?
            .get(&self.runtime_session_id)
            .and_then(|session| session.runtime_state_root.clone());

        if let Some(root) = root.as_deref() {
            save_thread_id(root, thread_id)?;
        }

        self.sessions
            .write()
            .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?
            .entry(self.runtime_session_id.clone())
            .and_modify(|session| {
                session.thread_id = Some(thread_id.to_string());
                session.last_launch_resumed_session = false;
            });
        Ok(())
    }
}

fn codex_event_payload(json: &Value) -> &Value {
    json.get("msg").unwrap_or(json)
}

fn codex_event_type(json: &Value) -> Option<&str> {
    json.get("msg")
        .and_then(|value| value.get("type"))
        .and_then(Value::as_str)
        .or_else(|| json.get("type").and_then(Value::as_str))
}

fn codex_event_text(payload: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(value) = payload.get(*key) {
            if let Some(text) = collect_codex_text(value, 0) {
                return Some(text);
            }
        }
    }
    None
}

fn extract_codex_thread_id(payload: &Value) -> Option<&str> {
    payload
        .get("thread_id")
        .and_then(Value::as_str)
        .or_else(|| {
            payload
                .pointer("/payload/thread_id")
                .and_then(Value::as_str)
        })
        .filter(|thread_id| !thread_id.trim().is_empty())
}

fn collect_codex_text(value: &Value, depth: usize) -> Option<String> {
    if depth > 6 {
        return None;
    }

    match value {
        Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
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

fn parse_codex_item(events: &mut Vec<RuntimeEvent>, item: &Value, parser: &mut CodexOutputParser) {
    match item.get("type").and_then(Value::as_str) {
        Some("agent_message") => {
            if let Some(text) = item.get("text").and_then(Value::as_str) {
                push_agent_message(events, parser, item, text.to_string());
            }
        }
        Some("error") => {
            let message = item
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("codex item error");
            events.push(RuntimeEvent::Error {
                code: Some("runtime.error".to_string()),
                text: format!("codex: {message}"),
            });
        }
        Some("command_execution") => {
            flush_pending_agent_message(events, parser, RuntimeMessageLane::Reasoning);
            let command = item
                .get("command")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let status = item
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            events.push(RuntimeEvent::Status {
                code: None,
                text: format!("codex command '{command}' ({status})"),
            });
        }
        Some("file_change") => {
            flush_pending_agent_message(events, parser, RuntimeMessageLane::Reasoning);
            let status = item
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            events.push(RuntimeEvent::Status {
                code: None,
                text: format!("codex file_change ({status})"),
            });
        }
        Some("web_search") => {
            flush_pending_agent_message(events, parser, RuntimeMessageLane::Reasoning);
            events.push(RuntimeEvent::Status {
                code: None,
                text: describe_codex_web_search(item),
            });
        }
        Some("reasoning") => {
            if let Some(text) = item.get("text").and_then(Value::as_str) {
                events.push(RuntimeEvent::MessageDelta {
                    lane: RuntimeMessageLane::Reasoning,
                    text: text.to_string(),
                });
            } else {
                events.push(RuntimeEvent::Status {
                    code: None,
                    text: "codex reasoning update".to_string(),
                });
            }
        }
        Some(other) => {
            events.push(RuntimeEvent::Status {
                code: None,
                text: describe_codex_item(other, item),
            });
        }
        None => {
            events.push(RuntimeEvent::Status {
                code: None,
                text: "codex item missing type".to_string(),
            });
        }
    }
}

// Codex's final answer authority is either an explicit final_answer phase or
// a completion boundary after the last unphased assistant message.
fn codex_agent_message_lane(payload: &Value) -> Option<RuntimeMessageLane> {
    match payload.get("phase").and_then(Value::as_str) {
        Some("final_answer") => Some(RuntimeMessageLane::Answer),
        Some("commentary") => Some(RuntimeMessageLane::Reasoning),
        Some(_) => Some(RuntimeMessageLane::Reasoning),
        None => None,
    }
}

fn describe_codex_web_search(item: &Value) -> String {
    let query = codex_item_string(item, &["query"])
        .or_else(|| item.pointer("/input/query").and_then(Value::as_str))
        .or_else(|| item.pointer("/action/query").and_then(Value::as_str))
        .or_else(|| codex_item_array_first(item, &["queries"]))
        .or_else(|| item.pointer("/input/queries/0").and_then(Value::as_str))
        .or_else(|| item.pointer("/action/queries/0").and_then(Value::as_str));
    let status = codex_item_string(item, &["status", "state"])
        .or_else(|| item.pointer("/action/type").and_then(Value::as_str))
        .map(normalize_web_search_status);

    match (query, status) {
        (Some(query), Some(status)) => {
            format!(
                "codex web search '{}' ({})",
                truncate_status_detail(query),
                status
            )
        }
        (Some(query), None) => {
            format!("codex web search '{}'", truncate_status_detail(query))
        }
        (None, Some(status)) => format!("codex web search ({status})"),
        (None, None) => "codex web search".to_string(),
    }
}

fn normalize_web_search_status(raw: &str) -> &str {
    match raw {
        "other" => "starting",
        value => value,
    }
}

fn describe_codex_item(item_type: &str, item: &Value) -> String {
    if let Some(summary) = codex_item_summary(item) {
        return format!("codex item {item_type} ({summary})");
    }
    format!("codex item: {item_type}")
}

fn codex_item_summary(item: &Value) -> Option<String> {
    let mut parts = Vec::new();

    if let Some(status) = codex_item_string(item, &["status", "state"]) {
        parts.push(status.to_string());
    }
    if let Some(query) = codex_item_string(item, &["query", "location", "title", "path"]) {
        parts.push(truncate_status_detail(query));
    }

    if parts.is_empty() {
        None
    } else {
        Some(parts.join(", "))
    }
}

fn codex_item_string<'a>(item: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .filter_map(|key| item.get(*key))
        .filter_map(Value::as_str)
        .find(|value| !value.trim().is_empty())
}

fn codex_item_array_first<'a>(item: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .filter_map(|key| item.get(*key))
        .filter_map(Value::as_array)
        .find_map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .find(|value| !value.trim().is_empty())
        })
}

fn truncate_status_detail(text: &str) -> String {
    const MAX_LEN: usize = 80;
    let trimmed = text.trim();
    if trimmed.chars().count() <= MAX_LEN {
        return trimmed.to_string();
    }
    let shortened: String = trimmed.chars().take(MAX_LEN - 1).collect();
    format!("{shortened}…")
}

#[cfg(test)]
mod tests {
    use crate::kernel::runtime::{
        ExecutionOutput, RuntimeAdapter, RuntimeEvent, RuntimeMessageLane,
        RuntimeSessionStartInput, RuntimeTurnInput, RuntimeTurnMode,
    };

    use super::{
        build_codex_exec_args, parse_codex_stdout, CodexRuntimeAdapter, CodexRuntimeConfig,
        CODEX_THREAD_ID_STATE_FILE,
    };
    use uuid::Uuid;

    #[tokio::test]
    async fn codex_adapter_builds_program_spec_for_registered_session() {
        let adapter = CodexRuntimeAdapter::new(CodexRuntimeConfig {
            executable: "codex".to_string(),
            model: Some("gpt-5-codex".to_string()),
        });
        assert_eq!(adapter.turn_mode(), RuntimeTurnMode::ProgramBacked);

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

        let program = adapter
            .build_turn_program(&RuntimeTurnInput {
                runtime_session_id: handle.runtime_session_id.clone(),
                prompt: "hello".to_string(),
                fresh_prompt: None,
                runtime_skill_ids: Vec::new(),
            })
            .expect("program");

        assert_eq!(program.executable, "codex");
        assert_eq!(
            program.args,
            vec![
                "exec".to_string(),
                "--dangerously-bypass-approvals-and-sandbox".to_string(),
                "--json".to_string(),
                "--model".to_string(),
                "gpt-5-codex".to_string(),
            ]
        );
        assert!(program.environment.is_empty());
        assert_eq!(program.stdin, "hello");
        assert_eq!(
            program.auth,
            Some(crate::kernel::runtime::RuntimeAuthKind::Codex)
        );

        adapter.close(&handle).await.expect("close");
    }

    #[test]
    fn codex_adapter_formats_nonzero_exit_from_stderr() {
        let adapter = CodexRuntimeAdapter::new(CodexRuntimeConfig {
            executable: "codex".to_string(),
            ..CodexRuntimeConfig::default()
        });
        let message = adapter.format_program_exit_error(
            &ExecutionOutput {
                stderr: b"stub failure\n".to_vec(),
                exit_code: Some(7),
                ..ExecutionOutput::default()
            },
            None,
        );

        assert!(
            message.contains("exited with code 7"),
            "exit code should be included in error"
        );
        assert!(
            message.contains("stub failure"),
            "stderr output should be surfaced in error"
        );
    }

    #[test]
    fn codex_exec_args_only_include_protocol_fields() {
        let args = build_codex_exec_args(Some("gpt-5-codex"), None);

        assert_eq!(
            args,
            vec![
                "exec".to_string(),
                "--dangerously-bypass-approvals-and-sandbox".to_string(),
                "--json".to_string(),
                "--model".to_string(),
                "gpt-5-codex".to_string(),
            ]
        );
    }

    #[test]
    fn codex_resume_args_use_saved_thread_id_and_read_prompt_from_stdin() {
        let args = build_codex_exec_args(Some("gpt-5-codex"), Some("thread-123"));

        assert_eq!(
            args,
            vec![
                "exec".to_string(),
                "resume".to_string(),
                "--dangerously-bypass-approvals-and-sandbox".to_string(),
                "--json".to_string(),
                "--model".to_string(),
                "gpt-5-codex".to_string(),
                "thread-123".to_string(),
                "-".to_string(),
            ]
        );
    }

    #[test]
    fn codex_args_omit_model_when_runtime_model_is_unset() {
        assert_eq!(
            build_codex_exec_args(None, None),
            vec![
                "exec".to_string(),
                "--dangerously-bypass-approvals-and-sandbox".to_string(),
                "--json".to_string(),
            ]
        );
    }

    #[test]
    fn codex_stdout_falls_back_to_plain_text_when_json_is_invalid() {
        let events = parse_codex_stdout(b"plain line\n");
        assert_eq!(events.len(), 1, "expected one parsed event");
        assert!(matches!(
            &events[0],
            RuntimeEvent::MessageDelta { lane: RuntimeMessageLane::Answer, text } if text == "plain line"
        ));
    }

    #[test]
    fn codex_web_search_status_includes_query_when_available() {
        let events = parse_codex_stdout(
            br#"{"type":"item.completed","item":{"type":"web_search","query":"weather in Sajiloni","status":"completed"}}"#,
        );
        assert_eq!(events.len(), 1, "expected one parsed event");
        assert!(matches!(
            &events[0],
            RuntimeEvent::Status { code: None, text }
                if text == "codex web search 'weather in Sajiloni' (completed)"
        ));
    }

    #[test]
    fn codex_web_search_started_and_completed_shapes_are_both_described() {
        let events = parse_codex_stdout(
            br#"{"type":"item.started","item":{"id":"item_2","type":"web_search","query":"","action":{"type":"other"}}}
{"type":"item.completed","item":{"id":"item_2","type":"web_search","query":"weather: Nairobi, Kenya","action":{"type":"search","query":"weather: Nairobi, Kenya","queries":["weather: Nairobi, Kenya"]}}}"#,
        );
        assert_eq!(events.len(), 2, "expected two parsed events");
        assert!(matches!(
            &events[0],
            RuntimeEvent::Status { code: None, text }
                if text == "codex web search (starting)"
        ));
        assert!(matches!(
            &events[1],
            RuntimeEvent::Status { code: None, text }
                if text == "codex web search 'weather: Nairobi, Kenya' (search)"
        ));
    }

    #[test]
    fn codex_stdout_parses_nested_msg_events_and_final_answer() {
        let events = parse_codex_stdout(
            br#"{"cwd":"/workspace","model":"gpt-5"}
{"id":"0","msg":{"type":"task_started"}}
{"id":"1","msg":{"type":"agent_reasoning_delta","delta":"Thinking..."}}
{"id":"2","msg":{"type":"task_complete","last_agent_message":"Done."}}"#,
        );

        assert_eq!(
            events.len(),
            5,
            "expected started, reasoning, answer, status, done"
        );
        assert!(matches!(
            &events[0],
            RuntimeEvent::Status { code: None, text } if text == "codex turn started"
        ));
        assert!(matches!(
            &events[1],
            RuntimeEvent::MessageDelta { lane: RuntimeMessageLane::Reasoning, text } if text == "Thinking..."
        ));
        assert!(matches!(
            &events[2],
            RuntimeEvent::MessageDelta { lane: RuntimeMessageLane::Answer, text } if text == "Done."
        ));
        assert!(matches!(
            &events[3],
            RuntimeEvent::Status { code: None, text } if text == "codex turn completed"
        ));
        assert!(matches!(&events[4], RuntimeEvent::Done));
    }

    #[test]
    fn codex_stdout_routes_unphased_messages_by_turn_boundary() {
        let events = parse_codex_stdout(
            br##"{"type":"event_msg","payload":{"type":"turn.started"}}
{"type":"event_msg","payload":{"type":"item.completed","item":{"type":"agent_message","text":"I am checking the files."}}}
{"type":"event_msg","payload":{"type":"exec_command_begin","parsed_cmd":"sed -n '1,80p' AGENTS.md"}}
{"type":"event_msg","payload":{"type":"exec_command_end","exit_code":0}}
{"type":"event_msg","payload":{"type":"item.completed","item":{"type":"agent_message","text":"# Result\n\n- Read `AGENTS.md`."}}}
{"type":"event_msg","payload":{"type":"turn.completed"}}"##,
        );

        assert!(matches!(
            &events[1],
            RuntimeEvent::MessageDelta { lane: RuntimeMessageLane::Reasoning, text }
                if text == "I am checking the files."
        ));
        assert!(matches!(
            &events[4],
            RuntimeEvent::MessageDelta { lane: RuntimeMessageLane::Answer, text }
                if text == "# Result\n\n- Read `AGENTS.md`."
        ));
    }

    #[test]
    fn codex_stdout_ignores_non_event_json_objects() {
        let events = parse_codex_stdout(br#"{"cwd":"/workspace","model":"gpt-5"}"#);
        assert!(
            events.is_empty(),
            "non-event JSON should not surface as status spam"
        );
    }

    #[tokio::test]
    async fn first_successful_turn_saves_codex_thread_id() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        let adapter = CodexRuntimeAdapter::new(CodexRuntimeConfig::default());
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
        assert!(!handle.resumes_existing_session);

        let turn_input = RuntimeTurnInput {
            runtime_session_id: handle.runtime_session_id.clone(),
            prompt: "hello".to_string(),
            fresh_prompt: None,
            runtime_skill_ids: Vec::new(),
        };
        let mut parser = adapter
            .program_output_parser(&turn_input)
            .expect("program output parser");
        let events = parser.parse_line(r#"{"type":"thread.started","thread_id":"thread-new"}"#);
        assert!(matches!(
            &events[0],
            RuntimeEvent::Status { code: None, text } if text == "codex thread started: thread-new"
        ));
        assert_eq!(
            std::fs::read_to_string(runtime_state_root.join(CODEX_THREAD_ID_STATE_FILE))
                .expect("read saved thread id"),
            "thread-new\n"
        );

        let program = adapter.build_turn_program(&turn_input).expect("program");
        assert_eq!(
            program.args,
            vec![
                "exec".to_string(),
                "resume".to_string(),
                "--dangerously-bypass-approvals-and-sandbox".to_string(),
                "--json".to_string(),
                "thread-new".to_string(),
                "-".to_string(),
            ]
        );

        adapter.close(&handle).await.expect("close");
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

        let program = adapter
            .build_turn_program(&RuntimeTurnInput {
                runtime_session_id: handle.runtime_session_id.clone(),
                prompt: "hello".to_string(),
                fresh_prompt: None,
                runtime_skill_ids: Vec::new(),
            })
            .expect("program");
        assert_eq!(
            program.args,
            vec![
                "exec".to_string(),
                "--dangerously-bypass-approvals-and-sandbox".to_string(),
                "--json".to_string(),
            ]
        );

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

        let program_a = adapter
            .build_turn_program(&RuntimeTurnInput {
                runtime_session_id: handle_a.runtime_session_id.clone(),
                prompt: "hello".to_string(),
                fresh_prompt: None,
                runtime_skill_ids: Vec::new(),
            })
            .expect("program a");
        let program_b = adapter
            .build_turn_program(&RuntimeTurnInput {
                runtime_session_id: handle_b.runtime_session_id.clone(),
                prompt: "hello".to_string(),
                fresh_prompt: None,
                runtime_skill_ids: Vec::new(),
            })
            .expect("program b");

        assert!(program_a.args.contains(&"thread-a".to_string()));
        assert!(program_b.args.contains(&"thread-b".to_string()));
        assert!(!program_a.args.contains(&"thread-b".to_string()));
        assert!(!program_b.args.contains(&"thread-a".to_string()));

        adapter.close(&handle_a).await.expect("close a");
        adapter.close(&handle_b).await.expect("close b");
    }
}
