use std::sync::RwLock;
use std::{collections::HashMap, path::Path};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;

use crate::kernel::runtime::{
    ExecutionOutput, RuntimeAdapter, RuntimeAdapterInfo, RuntimeCapabilityResult, RuntimeEvent,
    RuntimeEventSender, RuntimeMessageLane, RuntimeProgramSpec, RuntimeSessionHandle,
    RuntimeSessionStartInput, RuntimeTurnInput, RuntimeTurnMode,
};

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
    use crate::kernel::runtime::{
        ExecutionOutput, RuntimeAdapter, RuntimeEvent, RuntimeMessageLane,
        RuntimeSessionStartInput, RuntimeTurnInput, RuntimeTurnMode,
    };

    use super::{parse_opencode_stdout, OpenCodeRuntimeAdapter, OpenCodeRuntimeConfig};
    use uuid::Uuid;

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
                selected_skills: Vec::new(),
                runtime_state_root: None,
            })
            .await
            .expect("start");

        let program = adapter
            .build_turn_program(&RuntimeTurnInput {
                runtime_session_id: handle.runtime_session_id,
                prompt: "hello".to_string(),
                selected_skills: Vec::new(),
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
