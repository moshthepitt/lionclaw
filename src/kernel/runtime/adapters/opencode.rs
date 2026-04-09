use std::collections::HashSet;
use std::sync::RwLock;

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
    pub format: String,
    pub model: Option<String>,
    pub agent: Option<String>,
}

impl Default for OpenCodeRuntimeConfig {
    fn default() -> Self {
        Self {
            executable: "opencode".to_string(),
            format: "json".to_string(),
            model: None,
            agent: None,
        }
    }
}

#[derive(Debug)]
pub struct OpenCodeRuntimeAdapter {
    config: OpenCodeRuntimeConfig,
    sessions: RwLock<HashSet<String>>,
}

impl OpenCodeRuntimeAdapter {
    pub fn new(config: OpenCodeRuntimeConfig) -> Self {
        Self {
            config,
            sessions: RwLock::new(HashSet::new()),
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
        let _ = input;
        self.sessions
            .write()
            .map_err(|_| anyhow!("opencode runtime session state lock poisoned"))?
            .insert(runtime_session_id.clone());

        Ok(RuntimeSessionHandle { runtime_session_id })
    }

    fn build_turn_program(&self, input: &RuntimeTurnInput) -> Result<RuntimeProgramSpec> {
        ensure_runtime_session_exists(&self.sessions, &input.runtime_session_id)?;

        Ok(RuntimeProgramSpec {
            executable: self.config.executable.clone(),
            args: build_opencode_run_args(&self.config, &input.prompt),
            environment: Vec::new(),
            stdin: String::new(),
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
            format!("opencode run exited with code {}: {}", code, detail)
        } else {
            format!("opencode run exited with code {}", code)
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
        let _ = events.send(RuntimeEvent::Done);
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

fn build_opencode_run_args(config: &OpenCodeRuntimeConfig, prompt: &str) -> Vec<String> {
    let mut args = vec![
        "run".to_string(),
        "--format".to_string(),
        config.format.clone(),
    ];

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

fn ensure_runtime_session_exists(
    sessions: &RwLock<HashSet<String>>,
    runtime_session_id: &str,
) -> Result<()> {
    if sessions
        .read()
        .map_err(|_| anyhow!("opencode runtime session state lock poisoned"))?
        .contains(runtime_session_id)
    {
        Ok(())
    } else {
        Err(anyhow!(
            "runtime session '{}' not found",
            runtime_session_id
        ))
    }
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
    let event_type = json.get("type").and_then(Value::as_str);

    if let Some(event_type) = event_type {
        events.push(RuntimeEvent::Status {
            code: None,
            text: format!("opencode event: {}", event_type),
        });
    }

    if let Some(message) = extract_error_message(json) {
        events.push(RuntimeEvent::Error {
            code: Some("runtime.error".to_string()),
            text: format!("opencode: {}", message),
        });
        return;
    }

    if let Some(text) = extract_text_delta(json) {
        events.push(RuntimeEvent::MessageDelta {
            lane: RuntimeMessageLane::Answer,
            text,
        });
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

fn extract_text_delta(json: &Value) -> Option<String> {
    for pointer in [
        "/text",
        "/part/text",
        "/data/text",
        "/message/text",
        "/delta/text",
    ] {
        if let Some(text) = json.pointer(pointer).and_then(Value::as_str) {
            if !text.trim().is_empty() {
                return Some(text.to_string());
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
                return Some(texts.join("\n"));
            }
        }
    }

    None
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
            format: "json".to_string(),
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

        assert_eq!(program.executable, "opencode");
        assert_eq!(
            program.args,
            vec![
                "run".to_string(),
                "--format".to_string(),
                "json".to_string(),
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
}
