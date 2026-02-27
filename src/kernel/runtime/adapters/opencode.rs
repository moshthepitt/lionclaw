use std::collections::HashMap;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::kernel::runtime::{
    RuntimeAdapter, RuntimeAdapterInfo, RuntimeCapabilityResult, RuntimeEvent,
    RuntimeSessionHandle, RuntimeSessionStartInput, RuntimeTurnInput, RuntimeTurnOutput,
};

use super::subprocess::{run_non_interactive, SubprocessInvocation};

#[derive(Debug, Clone)]
pub struct OpenCodeRuntimeConfig {
    pub executable: String,
    pub format: String,
    pub model: Option<String>,
    pub agent: Option<String>,
    pub xdg_data_home: Option<String>,
    pub continue_last_session: bool,
}

impl Default for OpenCodeRuntimeConfig {
    fn default() -> Self {
        Self {
            executable: "opencode".to_string(),
            format: "json".to_string(),
            model: None,
            agent: None,
            xdg_data_home: None,
            continue_last_session: false,
        }
    }
}

impl OpenCodeRuntimeConfig {
    pub fn from_env() -> Self {
        let default = Self::default();
        Self {
            executable: std::env::var("LIONCLAW_OPENCODE_BIN")
                .ok()
                .filter(|raw| !raw.trim().is_empty())
                .unwrap_or(default.executable),
            format: std::env::var("LIONCLAW_OPENCODE_FORMAT")
                .ok()
                .filter(|raw| !raw.trim().is_empty())
                .unwrap_or(default.format),
            model: std::env::var("LIONCLAW_OPENCODE_MODEL")
                .ok()
                .filter(|raw| !raw.trim().is_empty()),
            agent: std::env::var("LIONCLAW_OPENCODE_AGENT")
                .ok()
                .filter(|raw| !raw.trim().is_empty()),
            xdg_data_home: std::env::var("LIONCLAW_OPENCODE_XDG_DATA_HOME")
                .ok()
                .filter(|raw| !raw.trim().is_empty()),
            continue_last_session: env_flag("LIONCLAW_OPENCODE_CONTINUE_LAST_SESSION")
                .unwrap_or(default.continue_last_session),
        }
    }
}

#[derive(Debug, Clone)]
struct OpenCodeSessionState {
    working_dir: Option<String>,
}

#[derive(Debug)]
pub struct OpenCodeRuntimeAdapter {
    config: OpenCodeRuntimeConfig,
    sessions: RwLock<HashMap<String, OpenCodeSessionState>>,
}

impl OpenCodeRuntimeAdapter {
    pub fn from_env() -> Self {
        Self::new(OpenCodeRuntimeConfig::from_env())
    }

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

    async fn session_start(&self, input: RuntimeSessionStartInput) -> Result<RuntimeSessionHandle> {
        let runtime_session_id = format!("opencode-{}", Uuid::new_v4());
        let state = OpenCodeSessionState {
            working_dir: input.working_dir,
        };
        self.sessions
            .write()
            .await
            .insert(runtime_session_id.clone(), state);

        Ok(RuntimeSessionHandle { runtime_session_id })
    }

    async fn turn(&self, input: RuntimeTurnInput) -> Result<RuntimeTurnOutput> {
        let session = self
            .sessions
            .read()
            .await
            .get(&input.runtime_session_id)
            .cloned()
            .ok_or_else(|| anyhow!("runtime session '{}' not found", input.runtime_session_id))?;

        let invocation = SubprocessInvocation {
            executable: self.config.executable.clone(),
            args: build_opencode_run_args(
                &self.config,
                session.working_dir.as_deref(),
                &input.prompt,
            ),
            working_dir: None,
            environment: build_opencode_environment(&self.config),
            input: String::new(),
        };

        let output = run_non_interactive(&invocation).await?;
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if !output.success() {
            let code = output.exit_code.unwrap_or(1);
            let detail = extract_opencode_error_detail(&output.stdout).or(if stderr.is_empty() {
                None
            } else {
                Some(stderr)
            });

            return if let Some(detail) = detail {
                Err(anyhow!(
                    "opencode run exited with code {}: {}",
                    code,
                    detail
                ))
            } else {
                Err(anyhow!("opencode run exited with code {}", code))
            };
        }

        let mut events = parse_opencode_stdout(&output.stdout);
        if !events
            .iter()
            .any(|event| matches!(event, RuntimeEvent::Done))
        {
            events.push(RuntimeEvent::Done);
        }

        Ok(RuntimeTurnOutput {
            events,
            capability_requests: Vec::new(),
        })
    }

    async fn resolve_capability_requests(
        &self,
        _handle: &RuntimeSessionHandle,
        results: Vec<RuntimeCapabilityResult>,
    ) -> Result<Vec<RuntimeEvent>> {
        if !results.is_empty() {
            return Err(anyhow!(
                "opencode adapter does not support runtime-side capability request resolution"
            ));
        }
        Ok(vec![RuntimeEvent::Done])
    }

    async fn cancel(&self, _handle: &RuntimeSessionHandle, _reason: Option<String>) -> Result<()> {
        Ok(())
    }

    async fn close(&self, handle: &RuntimeSessionHandle) -> Result<()> {
        self.sessions
            .write()
            .await
            .remove(&handle.runtime_session_id);
        Ok(())
    }
}

fn build_opencode_run_args(
    config: &OpenCodeRuntimeConfig,
    session_working_dir: Option<&str>,
    prompt: &str,
) -> Vec<String> {
    let mut args = vec![
        "run".to_string(),
        "--format".to_string(),
        config.format.clone(),
    ];

    if config.continue_last_session {
        args.push("--continue".to_string());
    }
    if let Some(model) = &config.model {
        args.push("--model".to_string());
        args.push(model.to_string());
    }
    if let Some(agent) = &config.agent {
        args.push("--agent".to_string());
        args.push(agent.to_string());
    }
    if let Some(working_dir) = session_working_dir {
        args.push("--dir".to_string());
        args.push(working_dir.to_string());
    }
    args.push(prompt.to_string());

    args
}

fn build_opencode_environment(config: &OpenCodeRuntimeConfig) -> Vec<(String, String)> {
    let mut env = Vec::new();
    if let Some(xdg_data_home) = &config.xdg_data_home {
        env.push(("XDG_DATA_HOME".to_string(), xdg_data_home.to_string()));
    }
    env
}

fn env_flag(name: &str) -> Option<bool> {
    let value = std::env::var(name).ok()?;
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn parse_opencode_stdout(stdout: &[u8]) -> Vec<RuntimeEvent> {
    let output = String::from_utf8_lossy(stdout);
    let mut events = Vec::new();

    for line in output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        if let Ok(json) = serde_json::from_str::<Value>(line) {
            parse_opencode_json_event(&mut events, &json);
        } else {
            events.push(RuntimeEvent::TextDelta(line.to_string()));
        }
    }

    events
}

fn parse_opencode_json_event(events: &mut Vec<RuntimeEvent>, json: &Value) {
    let event_type = json.get("type").and_then(Value::as_str);

    if let Some(event_type) = event_type {
        events.push(RuntimeEvent::Status(format!(
            "opencode event: {}",
            event_type
        )));
    }

    if let Some(message) = extract_error_message(json) {
        events.push(RuntimeEvent::Error(format!("opencode: {}", message)));
        return;
    }

    if let Some(text) = extract_text_delta(json) {
        events.push(RuntimeEvent::TextDelta(text));
    }
}

fn extract_opencode_error_detail(stdout: &[u8]) -> Option<String> {
    let output = String::from_utf8_lossy(stdout);
    for line in output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let json = serde_json::from_str::<Value>(line).ok()?;
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
        RuntimeAdapter, RuntimeEvent, RuntimeSessionStartInput, RuntimeTurnInput,
    };

    use super::{parse_opencode_stdout, OpenCodeRuntimeAdapter, OpenCodeRuntimeConfig};
    use tempfile::tempdir;
    use uuid::Uuid;

    #[cfg(unix)]
    #[tokio::test]
    async fn opencode_adapter_translates_text_events() {
        let sandbox = tempdir().expect("temp dir");
        let stub = sandbox.path().join("opencode-stub.sh");
        write_script(
            &stub,
            r#"#!/usr/bin/env bash
echo '{"type":"session.started","sessionID":"ses_123"}'
echo '{"type":"step_start","part":{"type":"step-start"}}'
echo '{"type":"text","part":{"type":"text","text":"hello from opencode"}}'
echo '{"type":"step_finish","part":{"type":"step-finish"}}'
"#,
        );

        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig {
            executable: stub.to_string_lossy().to_string(),
            ..OpenCodeRuntimeConfig::default()
        });

        let handle = adapter
            .session_start(RuntimeSessionStartInput {
                session_id: Uuid::new_v4(),
                working_dir: None,
                selected_skills: Vec::new(),
            })
            .await
            .expect("start");

        let output = adapter
            .turn(RuntimeTurnInput {
                runtime_session_id: handle.runtime_session_id.clone(),
                prompt: "hello".to_string(),
                selected_skills: Vec::new(),
            })
            .await
            .expect("turn");

        assert!(
            output.events.iter().any(|event| matches!(
                event,
                RuntimeEvent::TextDelta(text) if text == "hello from opencode"
            )),
            "opencode message content should map to text delta"
        );
        assert!(
            output
                .events
                .iter()
                .any(|event| matches!(event, RuntimeEvent::Done)),
            "turn output should include terminal done event"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn opencode_adapter_surfaces_structured_error_message() {
        let sandbox = tempdir().expect("temp dir");
        let stub = sandbox.path().join("opencode-stub-fail.sh");
        write_script(
            &stub,
            r#"#!/usr/bin/env bash
echo '{"type":"error","error":{"name":"UnknownError","data":{"message":"provider unavailable"}}}'
exit 7
"#,
        );

        let adapter = OpenCodeRuntimeAdapter::new(OpenCodeRuntimeConfig {
            executable: stub.to_string_lossy().to_string(),
            ..OpenCodeRuntimeConfig::default()
        });
        let handle = adapter
            .session_start(RuntimeSessionStartInput {
                session_id: Uuid::new_v4(),
                working_dir: None,
                selected_skills: Vec::new(),
            })
            .await
            .expect("start");

        let err = adapter
            .turn(RuntimeTurnInput {
                runtime_session_id: handle.runtime_session_id,
                prompt: "hello".to_string(),
                selected_skills: Vec::new(),
            })
            .await
            .expect_err("turn should fail");

        let message = err.to_string();
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
            RuntimeEvent::TextDelta(text) if text == "plain line"
        ));
    }

    #[cfg(unix)]
    fn write_script(path: &std::path::Path, content: &str) {
        use std::{fs, os::unix::fs::PermissionsExt};

        fs::write(path, content).expect("write script");
        let permissions = fs::Permissions::from_mode(0o755);
        fs::set_permissions(path, permissions).expect("chmod");
    }
}
