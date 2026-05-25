use std::sync::RwLock;
use std::{
    collections::{HashMap, HashSet},
    path::Path,
};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use crate::kernel::runtime::{
    ExecutionOutput, RuntimeAdapter, RuntimeAdapterInfo, RuntimeCapabilityResult, RuntimeEvent,
    RuntimeEventSender, RuntimeMessageLane, RuntimeProgramSpec, RuntimeSessionHandle,
    RuntimeSessionStartInput, RuntimeTerminalTranscriptInput,
    RuntimeTerminalTranscriptProgramExecutor, RuntimeTerminalTurn, RuntimeTerminalTurnStatus,
    RuntimeTurnInput, RuntimeTurnMode,
};

const OPENCODE_RUNTIME_CONFIG_DIR: &str = "/runtime";
const OPENCODE_TRANSCRIPT_SESSION_LIMIT: usize = 200;

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
    sessions: RwLock<HashMap<String, OpenCodeSessionState>>,
}

#[derive(Debug, Clone, Copy)]
struct OpenCodeSessionState {
    resumes_existing_session: bool,
}

impl OpenCodeRuntimeAdapter {
    pub fn new(config: OpenCodeRuntimeConfig) -> Self {
        Self {
            config,
            sessions: RwLock::new(HashMap::new()),
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
        let resumes_existing_session = input
            .runtime_state_root
            .as_deref()
            .map(runtime_session_is_ready)
            .transpose()?
            .unwrap_or(false);
        self.sessions
            .write()
            .map_err(|_| anyhow!("opencode runtime session state lock poisoned"))?
            .insert(
                runtime_session_id.clone(),
                OpenCodeSessionState {
                    resumes_existing_session,
                },
            );

        Ok(RuntimeSessionHandle {
            runtime_session_id,
            resumes_existing_session,
        })
    }

    fn build_turn_program(&self, input: &RuntimeTurnInput) -> Result<RuntimeProgramSpec> {
        let session = get_runtime_session(&self.sessions, &input.runtime_session_id)?;

        Ok(RuntimeProgramSpec {
            executable: self.config.executable.clone(),
            args: build_opencode_run_args(
                &self.config,
                &input.prompt,
                session.resumes_existing_session,
            ),
            environment: Vec::new(),
            stdin: String::new(),
            auth: None,
        })
    }

    fn parse_program_output_line(&self, line: &str) -> Vec<RuntimeEvent> {
        parse_opencode_output_line(line)
    }

    fn build_terminal_program(&self) -> Result<RuntimeProgramSpec> {
        Ok(build_opencode_terminal_program(&self.config))
    }

    async fn export_terminal_transcript_with_executor(
        &self,
        _input: RuntimeTerminalTranscriptInput,
        executor: &mut dyn RuntimeTerminalTranscriptProgramExecutor,
    ) -> Result<Option<Vec<RuntimeTerminalTurn>>> {
        export_opencode_terminal_transcript_with_cli(&self.config, executor)
            .await
            .map(Some)
    }

    fn format_program_exit_error(
        &self,
        output: &ExecutionOutput,
        observed_error_text: Option<&str>,
    ) -> String {
        let code = output.exit_code.unwrap_or(1);
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let observed_detail = observed_error_text
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(str::to_string);
        let stderr_detail = if stderr.is_empty() {
            None
        } else {
            Some(stderr)
        };
        let detail = extract_opencode_error_detail(&output.stdout)
            .or(observed_detail)
            .or(stderr_detail);

        if let Some(detail) = detail {
            format!("opencode run exited with code {code}: {detail}")
        } else {
            format!("opencode run exited with code {code}")
        }
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

fn build_opencode_run_args(
    config: &OpenCodeRuntimeConfig,
    prompt: &str,
    resumes_existing_session: bool,
) -> Vec<String> {
    let mut args = vec!["run".to_string()];

    if resumes_existing_session {
        args.push("--continue".to_string());
    }

    args.extend([
        "--format".to_string(),
        "json".to_string(),
        "--thinking".to_string(),
    ]);

    if let Some(model) = &config.model {
        args.push("--model".to_string());
        args.push(model.to_string());
    }
    if let Some(agent) = &config.agent {
        args.push("--agent".to_string());
        args.push(agent.to_string());
    }
    args.push(prompt.to_string());

    args
}

fn build_opencode_terminal_program(config: &OpenCodeRuntimeConfig) -> RuntimeProgramSpec {
    let mut args = Vec::new();
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
        environment: opencode_runtime_environment(),
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
            "--max-count".to_string(),
            OPENCODE_TRANSCRIPT_SESSION_LIMIT.to_string(),
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
    vec![(
        "OPENCODE_CONFIG_DIR".to_string(),
        OPENCODE_RUNTIME_CONFIG_DIR.to_string(),
    )]
}

fn opencode_transcript_export_environment() -> Vec<(String, String)> {
    let mut environment = opencode_runtime_environment();
    environment.push(("OPENCODE_PURE".to_string(), "1".to_string()));
    environment
}

async fn export_opencode_terminal_transcript_with_cli(
    config: &OpenCodeRuntimeConfig,
    executor: &mut dyn RuntimeTerminalTranscriptProgramExecutor,
) -> Result<Vec<RuntimeTerminalTurn>> {
    let list_output = executor
        .execute(build_opencode_session_list_program(config))
        .await
        .context("failed to list OpenCode sessions through the OpenCode CLI")?;
    if !list_output.success() {
        return Err(opencode_program_error("session list", &list_output));
    }

    let session_ids = parse_opencode_session_list(&list_output.stdout)?;
    let mut turns = Vec::new();
    for session_id in session_ids {
        let export_output = executor
            .execute(build_opencode_export_program(config, &session_id))
            .await
            .with_context(|| {
                format!("failed to export OpenCode session {session_id} through the OpenCode CLI")
            })?;
        if !export_output.success() {
            return Err(opencode_program_error(
                &format!("export {session_id}"),
                &export_output,
            ));
        }
        turns.extend(parse_opencode_export(&session_id, &export_output.stdout)?);
    }

    turns.sort_by(|left, right| {
        left.started_at
            .cmp(&right.started_at)
            .then_with(|| left.source_id.cmp(&right.source_id))
    });
    Ok(turns)
}

fn opencode_program_error(action: &str, output: &ExecutionOutput) -> anyhow::Error {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if stderr.is_empty() {
        anyhow!(
            "OpenCode CLI {action} exited with code {:?}",
            output.exit_code
        )
    } else {
        anyhow!(
            "OpenCode CLI {action} exited with code {:?}: {stderr}",
            output.exit_code
        )
    }
}

fn parse_opencode_session_list(stdout: &[u8]) -> Result<Vec<String>> {
    let raw = String::from_utf8_lossy(stdout);
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }

    let sessions = serde_json::from_slice::<Vec<OpenCodeSessionListItem>>(stdout)
        .context("failed to parse OpenCode session list JSON")?;
    let mut seen = HashSet::new();
    let mut ids = sessions
        .into_iter()
        .filter(|session| !session.id.trim().is_empty())
        .filter_map(|session| {
            seen.insert(session.id.clone())
                .then_some((session.updated, session.id))
        })
        .collect::<Vec<_>>();
    ids.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));

    Ok(ids.into_iter().map(|(_, id)| id).collect())
}

fn parse_opencode_export(
    requested_session_id: &str,
    stdout: &[u8],
) -> Result<Vec<RuntimeTerminalTurn>> {
    let export = serde_json::from_slice::<OpenCodeExport>(stdout).with_context(|| {
        format!("failed to parse OpenCode export JSON for session {requested_session_id}")
    })?;
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

        let display_user_text = opencode_message_export_text(user);
        let assistant_text = opencode_message_export_text(assistant);
        if display_user_text.trim().is_empty() || assistant_text.trim().is_empty() {
            continue;
        }

        let session_id = assistant
            .info
            .session_id
            .as_deref()
            .filter(|id| !id.trim().is_empty())
            .unwrap_or(exported_session_id);
        let (error_code, error_text) = opencode_assistant_error(&assistant.info);

        turns.push(RuntimeTerminalTurn {
            source_id: format!(
                "opencode-export:{}:{}:{}",
                session_id, user.info.id, assistant.info.id
            ),
            prompt_user_text: display_user_text.clone(),
            display_user_text,
            assistant_text,
            status: opencode_assistant_status(&assistant.info),
            error_code,
            error_text,
            started_at: opencode_message_time(user, false)?,
            finished_at: opencode_message_time(assistant, true)?,
        });
    }

    Ok(turns)
}

#[derive(Debug, Deserialize)]
struct OpenCodeSessionListItem {
    id: String,
    #[serde(default)]
    updated: i64,
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

fn opencode_assistant_status(info: &OpenCodeExportMessageInfo) -> RuntimeTerminalTurnStatus {
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

    if info.finish.is_some() || info.time.completed.is_some() {
        RuntimeTerminalTurnStatus::Completed
    } else {
        RuntimeTerminalTurnStatus::Interrupted
    }
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
        .copied()
        .ok_or_else(|| anyhow!("runtime session '{runtime_session_id}' not found"))
}

fn runtime_session_is_ready(root: &Path) -> Result<bool> {
    Ok(root
        .join(crate::home::RUNTIME_SESSION_READY_MARKER)
        .is_file())
}

#[cfg(test)]
fn parse_opencode_stdout(stdout: &[u8]) -> Vec<RuntimeEvent> {
    let output = String::from_utf8_lossy(stdout);
    let mut events = Vec::new();

    for line in output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        events.extend(parse_opencode_output_line(line));
    }

    events
}

fn parse_opencode_output_line(line: &str) -> Vec<RuntimeEvent> {
    let line = line.trim();
    if line.is_empty() {
        return Vec::new();
    }

    let mut events = Vec::new();
    if let Ok(json) = serde_json::from_str::<Value>(line) {
        parse_opencode_json_event(&mut events, &json);
    } else {
        events.push(RuntimeEvent::MessageDelta {
            lane: RuntimeMessageLane::Answer,
            text: line.to_string(),
        });
    }
    events
}

fn parse_opencode_json_event(events: &mut Vec<RuntimeEvent>, json: &Value) {
    if let Some(message) = extract_error_message(json) {
        events.push(RuntimeEvent::Error {
            code: Some("runtime.error".to_string()),
            text: format!("opencode: {message}"),
        });
        return;
    }

    if let Some((lane, text)) = extract_text_delta(json) {
        events.push(RuntimeEvent::MessageDelta { lane, text });
        return;
    }

    if let Some(text) = describe_opencode_status(json) {
        events.push(RuntimeEvent::Status { code: None, text });
    }
}

fn extract_opencode_error_detail(stdout: &[u8]) -> Option<String> {
    let output = String::from_utf8_lossy(stdout);
    for line in output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let Ok(json) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if let Some(message) = extract_error_message(&json) {
            return Some(message);
        }
    }
    None
}

fn extract_error_message(json: &Value) -> Option<String> {
    let event_type = json.get("type").and_then(Value::as_str).unwrap_or_default();
    if !event_type.contains("error") {
        return None;
    }

    for pointer in [
        "/error/data/message",
        "/error/message",
        "/data/message",
        "/message",
        "/error",
    ] {
        if let Some(value) = json.pointer(pointer) {
            match value {
                Value::String(message) if !message.trim().is_empty() => {
                    return Some(message.to_string())
                }
                Value::Object(object) => {
                    if let Some(message) = object.get("message").and_then(Value::as_str) {
                        if !message.trim().is_empty() {
                            return Some(message.to_string());
                        }
                    }
                }
                _ => {}
            }
        }
    }

    Some("opencode error event".to_string())
}

fn extract_text_delta(json: &Value) -> Option<(RuntimeMessageLane, String)> {
    let lane = if is_reasoning_event(json) {
        RuntimeMessageLane::Reasoning
    } else {
        RuntimeMessageLane::Answer
    };

    for pointer in [
        "/text",
        "/part/text",
        "/data/text",
        "/message/text",
        "/delta/text",
    ] {
        if let Some(text) = json.pointer(pointer).and_then(Value::as_str) {
            if !text.trim().is_empty() {
                return Some((lane, text.to_string()));
            }
        }
    }

    for pointer in [
        "/content",
        "/part/content",
        "/data/content",
        "/message/content",
        "/parts",
        "/message/parts",
    ] {
        if let Some(value) = json.pointer(pointer) {
            let texts = collect_texts(value);
            if !texts.is_empty() {
                return Some((lane, texts.join("\n")));
            }
        }
    }

    None
}

fn is_reasoning_event(json: &Value) -> bool {
    for pointer in ["/type", "/part/type", "/message/type", "/delta/type"] {
        if let Some(event_type) = json.pointer(pointer).and_then(Value::as_str) {
            if event_type == "reasoning" || event_type.contains("reasoning") {
                return true;
            }
        }
    }
    false
}

fn describe_opencode_status(json: &Value) -> Option<String> {
    let event_type = json.get("type").and_then(Value::as_str)?;
    if matches!(
        event_type,
        "text"
            | "reasoning"
            | "response.output_text.delta"
            | "response.reasoning.delta"
            | "response.completed"
    ) {
        return None;
    }

    Some(format!("opencode event: {event_type}"))
}

fn collect_texts(value: &Value) -> Vec<String> {
    match value {
        Value::String(text) => {
            if text.trim().is_empty() {
                Vec::new()
            } else {
                vec![text.to_string()]
            }
        }
        Value::Array(items) => items.iter().flat_map(collect_texts).collect(),
        Value::Object(object) => {
            let mut out = Vec::new();

            if let Some(text) = object.get("text").and_then(Value::as_str) {
                if !text.trim().is_empty() {
                    out.push(text.to_string());
                }
            }
            if let Some(content) = object.get("content") {
                out.extend(collect_texts(content));
            }
            if let Some(parts) = object.get("parts") {
                out.extend(collect_texts(parts));
            }
            if let Some(delta) = object.get("delta") {
                out.extend(collect_texts(delta));
            }

            out
        }
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use crate::kernel::runtime::{
        ExecutionOutput, RuntimeAdapter, RuntimeEvent, RuntimeMessageLane, RuntimeProgramSpec,
        RuntimeSessionStartInput, RuntimeTerminalTranscriptInput,
        RuntimeTerminalTranscriptProgramExecutor, RuntimeTurnInput, RuntimeTurnMode,
    };

    use super::{parse_opencode_stdout, OpenCodeRuntimeAdapter, OpenCodeRuntimeConfig};
    use serde_json::json;
    use uuid::Uuid;

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

    #[tokio::test]
    async fn opencode_adapter_builds_program_spec_for_registered_session() {
        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig {
            executable: "opencode".to_string(),
            model: Some("gpt-5".to_string()),
            agent: Some("builder".to_string()),
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
                runtime_session_id: handle.runtime_session_id,
                prompt: "hello".to_string(),
                fresh_prompt: None,
                runtime_skill_ids: Vec::new(),
            })
            .expect("program");

        assert_eq!(program.executable, "opencode");
        assert_eq!(
            program.args,
            vec![
                "run".to_string(),
                "--format".to_string(),
                "json".to_string(),
                "--thinking".to_string(),
                "--model".to_string(),
                "gpt-5".to_string(),
                "--agent".to_string(),
                "builder".to_string(),
                "hello".to_string(),
            ]
        );
        assert!(program.environment.is_empty());
        assert!(program.stdin.is_empty());
    }

    #[test]
    fn opencode_adapter_builds_terminal_program_spec() {
        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig {
            executable: "opencode".to_string(),
            model: Some("gpt-5".to_string()),
            agent: Some("builder".to_string()),
        });

        let program = adapter.build_terminal_program().expect("program");

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
        assert_eq!(
            program.environment,
            vec![("OPENCODE_CONFIG_DIR".to_string(), "/runtime".to_string())]
        );
        assert!(program.stdin.is_empty());
        assert!(program.auth.is_none());
    }

    #[tokio::test]
    async fn opencode_terminal_transcript_export_uses_native_cli_export() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        let mut executor = FakeTranscriptExecutor {
            outputs: VecDeque::from([
                ExecutionOutput {
                    stdout: json!([
                        {
                            "id": "ses_opencode_session",
                            "title": "native test",
                            "updated": 3000,
                            "created": 1000,
                            "projectId": "proj",
                            "directory": "/workspace"
                        }
                    ])
                    .to_string()
                    .into_bytes(),
                    exit_code: Some(0),
                    ..ExecutionOutput::default()
                },
                ExecutionOutput {
                    stdout: json!({
                        "info": {
                            "id": "ses_opencode_session",
                            "title": "native test",
                            "time": { "created": 1000, "updated": 3000 },
                            "directory": "/workspace"
                        },
                        "messages": [
                            {
                                "info": {
                                    "id": "msg_user",
                                    "sessionID": "ses_opencode_session",
                                    "role": "user",
                                    "time": { "created": 1000 },
                                    "agent": "build",
                                    "model": { "providerID": "openai", "modelID": "gpt-5" }
                                },
                                "parts": [
                                    {
                                        "id": "part_user",
                                        "messageID": "msg_user",
                                        "sessionID": "ses_opencode_session",
                                        "type": "text",
                                        "text": "hello opencode"
                                    }
                                ]
                            },
                            {
                                "info": {
                                    "id": "msg_assistant",
                                    "sessionID": "ses_opencode_session",
                                    "role": "assistant",
                                    "time": { "created": 2000, "completed": 3000 },
                                    "parentID": "msg_user",
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
                                    },
                                    "finish": "stop"
                                },
                                "parts": [
                                    {
                                        "id": "part_assistant",
                                        "messageID": "msg_assistant",
                                        "sessionID": "ses_opencode_session",
                                        "type": "text",
                                        "text": "hello from native export"
                                    }
                                ]
                            }
                        ]
                    })
                    .to_string()
                    .into_bytes(),
                    exit_code: Some(0),
                    ..ExecutionOutput::default()
                },
            ]),
            programs: Vec::new(),
        };

        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig::default());
        let turns = adapter
            .export_terminal_transcript_with_executor(
                RuntimeTerminalTranscriptInput {
                    session_id: Uuid::new_v4(),
                    runtime_state_root,
                    exit_code: Some(0),
                },
                &mut executor,
            )
            .await
            .expect("export transcript")
            .expect("cli export path handled");

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

        assert_eq!(executor.programs.len(), 2);
        assert_eq!(
            executor.programs[0].args,
            vec![
                "session".to_string(),
                "list".to_string(),
                "--format".to_string(),
                "json".to_string(),
                "--max-count".to_string(),
                "200".to_string(),
            ]
        );
        assert_eq!(
            executor.programs[1].args,
            vec!["export".to_string(), "ses_opencode_session".to_string()]
        );
        assert_eq!(
            executor.programs[0].environment,
            vec![
                ("OPENCODE_CONFIG_DIR".to_string(), "/runtime".to_string()),
                ("OPENCODE_PURE".to_string(), "1".to_string()),
            ]
        );
        assert_eq!(
            executor.programs[1].environment,
            vec![
                ("OPENCODE_CONFIG_DIR".to_string(), "/runtime".to_string()),
                ("OPENCODE_PURE".to_string(), "1".to_string()),
            ]
        );
    }

    #[test]
    fn opencode_adapter_formats_structured_error_message() {
        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig {
            executable: "opencode".to_string(),
            ..OpenCodeRuntimeConfig::default()
        });
        let message = adapter.format_program_exit_error(
            &ExecutionOutput {
                stdout: br#"{"type":"error","error":{"name":"UnknownError","data":{"message":"provider unavailable"}}}"#
                    .to_vec(),
                exit_code: Some(7),
                ..ExecutionOutput::default()
            },
            None,
        );

        assert!(
            message.contains("provider unavailable"),
            "structured error detail should be surfaced"
        );
    }

    #[test]
    fn opencode_stdout_falls_back_to_plain_text_when_json_is_invalid() {
        let events = parse_opencode_stdout(b"plain line\n");
        assert_eq!(events.len(), 1, "expected one parsed event");
        assert!(matches!(
            &events[0],
            RuntimeEvent::MessageDelta { lane: RuntimeMessageLane::Answer, text } if text == "plain line"
        ));
    }

    #[test]
    fn opencode_stdout_parses_json_text_delta() {
        let events =
            parse_opencode_stdout(br#"{"type":"response.output_text.delta","text":"hello"}"#);

        assert_eq!(events.len(), 1, "expected one answer delta");
        assert!(matches!(
            &events[0],
            RuntimeEvent::MessageDelta { lane: RuntimeMessageLane::Answer, text } if text == "hello"
        ));
    }

    #[test]
    fn opencode_stdout_parses_reasoning_lane_without_status_spam() {
        let events = parse_opencode_stdout(br#"{"type":"reasoning","text":"planning next step"}"#);

        assert_eq!(events.len(), 1, "expected one reasoning delta");
        assert!(matches!(
            &events[0],
            RuntimeEvent::MessageDelta { lane: RuntimeMessageLane::Reasoning, text } if text == "planning next step"
        ));
    }

    #[test]
    fn opencode_continue_args_resume_last_session() {
        let args = super::build_opencode_run_args(
            &OpenCodeRuntimeConfig {
                executable: "opencode".to_string(),
                model: Some("gpt-5".to_string()),
                agent: Some("builder".to_string()),
            },
            "hello",
            true,
        );

        assert_eq!(
            args,
            vec![
                "run".to_string(),
                "--continue".to_string(),
                "--format".to_string(),
                "json".to_string(),
                "--thinking".to_string(),
                "--model".to_string(),
                "gpt-5".to_string(),
                "--agent".to_string(),
                "builder".to_string(),
                "hello".to_string(),
            ]
        );
    }
}
