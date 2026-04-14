use std::sync::RwLock;
use std::{collections::HashMap, path::Path};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;

use crate::kernel::runtime::{
    ExecutionOutput, RuntimeAdapter, RuntimeAdapterInfo, RuntimeAuthKind, RuntimeCapabilityResult,
    RuntimeEvent, RuntimeEventSender, RuntimeMessageLane, RuntimeProgramSpec, RuntimeSessionHandle,
    RuntimeSessionStartInput, RuntimeTurnInput, RuntimeTurnMode,
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
    sessions: RwLock<HashMap<String, CodexSessionState>>,
}

#[derive(Debug, Clone, Copy)]
struct CodexSessionState {
    resumes_existing_session: bool,
}

impl CodexRuntimeAdapter {
    pub fn new(config: CodexRuntimeConfig) -> Self {
        Self {
            config,
            sessions: RwLock::new(HashMap::new()),
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
        let resumes_existing_session = input
            .runtime_state_root
            .as_deref()
            .map(runtime_session_is_ready)
            .transpose()?
            .unwrap_or(false);
        self.sessions
            .write()
            .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?
            .insert(
                runtime_session_id.clone(),
                CodexSessionState {
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
            args: build_codex_exec_args(&self.config, session.resumes_existing_session),
            environment: Vec::new(),
            stdin: input.prompt.clone(),
            auth: Some(RuntimeAuthKind::Codex),
        })
    }

    fn parse_program_output_line(&self, line: &str) -> Vec<RuntimeEvent> {
        parse_codex_output_line(line)
    }

    fn format_program_exit_error(
        &self,
        output: &ExecutionOutput,
        _observed_error_text: Option<&str>,
    ) -> String {
        let code = output.exit_code.unwrap_or(1);
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if stderr.is_empty() {
            format!("codex exec exited with code {}", code)
        } else {
            format!("codex exec exited with code {}: {}", code, stderr)
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
                "codex adapter does not support runtime-side capability request resolution"
            ));
        }
        let _ = events.send(RuntimeEvent::Done);
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

fn build_codex_exec_args(
    config: &CodexRuntimeConfig,
    resumes_existing_session: bool,
) -> Vec<String> {
    let mut args = if resumes_existing_session {
        vec![
            "exec".to_string(),
            "resume".to_string(),
            "--last".to_string(),
            "--json".to_string(),
            "-".to_string(),
        ]
    } else {
        vec!["exec".to_string(), "--json".to_string()]
    };

    if let Some(model) = &config.model {
        args.push("--model".to_string());
        args.push(model.to_string());
    }

    args
}

fn get_runtime_session(
    sessions: &RwLock<HashMap<String, CodexSessionState>>,
    runtime_session_id: &str,
) -> Result<CodexSessionState> {
    sessions
        .read()
        .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?
        .get(runtime_session_id)
        .copied()
        .ok_or_else(|| anyhow!("runtime session '{}' not found", runtime_session_id))
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

    for line in output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        events.extend(parse_codex_output_line(line));
    }

    events
}

fn parse_codex_output_line(line: &str) -> Vec<RuntimeEvent> {
    let line = line.trim();
    if line.is_empty() {
        return Vec::new();
    }

    let mut events = Vec::new();
    if let Ok(json) = serde_json::from_str::<Value>(line) {
        parse_codex_json_event(&mut events, &json);
    } else {
        events.push(RuntimeEvent::MessageDelta {
            lane: RuntimeMessageLane::Answer,
            text: line.to_string(),
        });
    }
    events
}

fn parse_codex_json_event(events: &mut Vec<RuntimeEvent>, json: &Value) {
    let event_type = json.get("type").and_then(Value::as_str);
    match event_type {
        Some("thread.started") => {
            if let Some(thread_id) = json.get("thread_id").and_then(Value::as_str) {
                events.push(RuntimeEvent::Status {
                    code: None,
                    text: format!("codex thread started: {}", thread_id),
                });
            }
        }
        Some("turn.started") => {
            events.push(RuntimeEvent::Status {
                code: None,
                text: "codex turn started".to_string(),
            });
        }
        Some("turn.completed") => {
            events.push(RuntimeEvent::Status {
                code: None,
                text: "codex turn completed".to_string(),
            });
        }
        Some("turn.failed") => {
            let message = json
                .pointer("/error/message")
                .and_then(Value::as_str)
                .unwrap_or("codex turn failed");
            events.push(RuntimeEvent::Error {
                code: Some("runtime.error".to_string()),
                text: format!("codex: {}", message),
            });
        }
        Some("error") => {
            let message = json
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("codex stream error");
            events.push(RuntimeEvent::Error {
                code: Some("runtime.error".to_string()),
                text: format!("codex: {}", message),
            });
        }
        Some("item.started") | Some("item.updated") | Some("item.completed") => {
            if let Some(item) = json.get("item") {
                parse_codex_item(events, item);
            }
        }
        Some(other) => {
            events.push(RuntimeEvent::Status {
                code: None,
                text: format!("codex event: {}", other),
            });
        }
        None => {
            events.push(RuntimeEvent::Status {
                code: None,
                text: "codex event missing type".to_string(),
            });
        }
    }
}

fn parse_codex_item(events: &mut Vec<RuntimeEvent>, item: &Value) {
    match item.get("type").and_then(Value::as_str) {
        Some("agent_message") => {
            if let Some(text) = item.get("text").and_then(Value::as_str) {
                events.push(RuntimeEvent::MessageDelta {
                    lane: RuntimeMessageLane::Answer,
                    text: text.to_string(),
                });
            }
        }
        Some("error") => {
            let message = item
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("codex item error");
            events.push(RuntimeEvent::Error {
                code: Some("runtime.error".to_string()),
                text: format!("codex: {}", message),
            });
        }
        Some("command_execution") => {
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
                text: format!("codex command '{}' ({})", command, status),
            });
        }
        Some("file_change") => {
            let status = item
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            events.push(RuntimeEvent::Status {
                code: None,
                text: format!("codex file_change ({})", status),
            });
        }
        Some("web_search") => {
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
        (None, Some(status)) => format!("codex web search ({})", status),
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
        return format!("codex item {} ({})", item_type, summary);
    }
    format!("codex item: {}", item_type)
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
    format!("{}…", shortened)
}

#[cfg(test)]
mod tests {
    use crate::kernel::runtime::{
        ExecutionOutput, RuntimeAdapter, RuntimeEvent, RuntimeMessageLane,
        RuntimeSessionStartInput, RuntimeTurnInput, RuntimeTurnMode,
    };

    use super::{
        build_codex_exec_args, parse_codex_stdout, CodexRuntimeAdapter, CodexRuntimeConfig,
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
                selected_skills: Vec::new(),
                runtime_state_root: None,
            })
            .await
            .expect("start");

        let program = adapter
            .build_turn_program(&RuntimeTurnInput {
                runtime_session_id: handle.runtime_session_id.clone(),
                prompt: "hello".to_string(),
                selected_skills: Vec::new(),
            })
            .expect("program");

        assert_eq!(program.executable, "codex");
        assert_eq!(
            program.args,
            vec![
                "exec".to_string(),
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
        let args = build_codex_exec_args(
            &CodexRuntimeConfig {
                executable: "codex".to_string(),
                model: Some("gpt-5-codex".to_string()),
            },
            false,
        );

        assert_eq!(
            args,
            vec![
                "exec".to_string(),
                "--json".to_string(),
                "--model".to_string(),
                "gpt-5-codex".to_string(),
            ]
        );
    }

    #[test]
    fn codex_resume_args_request_last_session_and_read_prompt_from_stdin() {
        let args = build_codex_exec_args(
            &CodexRuntimeConfig {
                executable: "codex".to_string(),
                model: Some("gpt-5-codex".to_string()),
            },
            true,
        );

        assert_eq!(
            args,
            vec![
                "exec".to_string(),
                "resume".to_string(),
                "--last".to_string(),
                "--json".to_string(),
                "-".to_string(),
                "--model".to_string(),
                "gpt-5-codex".to_string(),
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
}
