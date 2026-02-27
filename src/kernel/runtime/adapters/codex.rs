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
pub struct CodexRuntimeConfig {
    pub executable: String,
    pub model: Option<String>,
    pub sandbox_mode: String,
    pub skip_git_repo_check: bool,
    pub ephemeral: bool,
}

impl Default for CodexRuntimeConfig {
    fn default() -> Self {
        Self {
            executable: "codex".to_string(),
            model: None,
            sandbox_mode: "read-only".to_string(),
            skip_git_repo_check: true,
            ephemeral: true,
        }
    }
}

impl CodexRuntimeConfig {
    pub fn from_env() -> Self {
        let default = Self::default();
        Self {
            executable: std::env::var("LIONCLAW_CODEX_BIN")
                .ok()
                .filter(|raw| !raw.trim().is_empty())
                .unwrap_or(default.executable),
            model: std::env::var("LIONCLAW_CODEX_MODEL")
                .ok()
                .filter(|raw| !raw.trim().is_empty()),
            sandbox_mode: std::env::var("LIONCLAW_CODEX_SANDBOX")
                .ok()
                .filter(|raw| !raw.trim().is_empty())
                .unwrap_or(default.sandbox_mode),
            skip_git_repo_check: env_flag("LIONCLAW_CODEX_SKIP_GIT_REPO_CHECK")
                .unwrap_or(default.skip_git_repo_check),
            ephemeral: env_flag("LIONCLAW_CODEX_EPHEMERAL").unwrap_or(default.ephemeral),
        }
    }
}

#[derive(Debug, Clone)]
struct CodexSessionState {
    working_dir: Option<String>,
    environment: Vec<(String, String)>,
}

#[derive(Debug)]
pub struct CodexRuntimeAdapter {
    config: CodexRuntimeConfig,
    sessions: RwLock<HashMap<String, CodexSessionState>>,
}

impl CodexRuntimeAdapter {
    pub fn from_env() -> Self {
        Self::new(CodexRuntimeConfig::from_env())
    }

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

    async fn session_start(&self, input: RuntimeSessionStartInput) -> Result<RuntimeSessionHandle> {
        let runtime_session_id = format!("codex-{}", Uuid::new_v4());
        let state = CodexSessionState {
            working_dir: input.working_dir,
            environment: input.environment,
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
            args: build_codex_exec_args(&self.config, session.working_dir.as_deref()),
            working_dir: None,
            environment: session.environment.clone(),
            input: input.prompt,
        };

        let output = run_non_interactive(&invocation).await?;
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if !output.success() {
            let code = output.exit_code.unwrap_or(1);
            if stderr.is_empty() {
                return Err(anyhow!("codex exec exited with code {}", code));
            }
            return Err(anyhow!("codex exec exited with code {}: {}", code, stderr));
        }

        let mut events = parse_codex_stdout(&output.stdout);
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
                "codex adapter does not support runtime-side capability request resolution"
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

fn build_codex_exec_args(
    config: &CodexRuntimeConfig,
    session_working_dir: Option<&str>,
) -> Vec<String> {
    let mut args = vec!["exec".to_string(), "--json".to_string()];

    if let Some(model) = &config.model {
        args.push("--model".to_string());
        args.push(model.to_string());
    }
    if !config.sandbox_mode.trim().is_empty() {
        args.push("--sandbox".to_string());
        args.push(config.sandbox_mode.clone());
    }
    if config.skip_git_repo_check {
        args.push("--skip-git-repo-check".to_string());
    }
    if config.ephemeral {
        args.push("--ephemeral".to_string());
    }
    if let Some(working_dir) = session_working_dir {
        args.push("--cd".to_string());
        args.push(working_dir.to_string());
    }

    args
}

fn env_flag(name: &str) -> Option<bool> {
    let value = std::env::var(name).ok()?;
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn parse_codex_stdout(stdout: &[u8]) -> Vec<RuntimeEvent> {
    let output = String::from_utf8_lossy(stdout);
    let mut events = Vec::new();

    for line in output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        if let Ok(json) = serde_json::from_str::<Value>(line) {
            parse_codex_json_event(&mut events, &json);
        } else {
            events.push(RuntimeEvent::TextDelta(line.to_string()));
        }
    }

    events
}

fn parse_codex_json_event(events: &mut Vec<RuntimeEvent>, json: &Value) {
    let event_type = json.get("type").and_then(Value::as_str);
    match event_type {
        Some("thread.started") => {
            if let Some(thread_id) = json.get("thread_id").and_then(Value::as_str) {
                events.push(RuntimeEvent::Status(format!(
                    "codex thread started: {}",
                    thread_id
                )));
            }
        }
        Some("turn.started") => {
            events.push(RuntimeEvent::Status("codex turn started".to_string()));
        }
        Some("turn.completed") => {
            events.push(RuntimeEvent::Status("codex turn completed".to_string()));
        }
        Some("turn.failed") => {
            let message = json
                .pointer("/error/message")
                .and_then(Value::as_str)
                .unwrap_or("codex turn failed");
            events.push(RuntimeEvent::Error(format!("codex: {}", message)));
        }
        Some("error") => {
            let message = json
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("codex stream error");
            events.push(RuntimeEvent::Error(format!("codex: {}", message)));
        }
        Some("item.started") | Some("item.updated") | Some("item.completed") => {
            if let Some(item) = json.get("item") {
                parse_codex_item(events, item);
            }
        }
        Some(other) => {
            events.push(RuntimeEvent::Status(format!("codex event: {}", other)));
        }
        None => {
            events.push(RuntimeEvent::Status("codex event missing type".to_string()));
        }
    }
}

fn parse_codex_item(events: &mut Vec<RuntimeEvent>, item: &Value) {
    match item.get("type").and_then(Value::as_str) {
        Some("agent_message") => {
            if let Some(text) = item.get("text").and_then(Value::as_str) {
                events.push(RuntimeEvent::TextDelta(text.to_string()));
            }
        }
        Some("error") => {
            let message = item
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("codex item error");
            events.push(RuntimeEvent::Error(format!("codex: {}", message)));
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
            events.push(RuntimeEvent::Status(format!(
                "codex command '{}' ({})",
                command, status
            )));
        }
        Some("file_change") => {
            let status = item
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            events.push(RuntimeEvent::Status(format!(
                "codex file_change ({})",
                status
            )));
        }
        Some("reasoning") => {
            events.push(RuntimeEvent::Status("codex reasoning update".to_string()));
        }
        Some(other) => {
            events.push(RuntimeEvent::Status(format!("codex item: {}", other)));
        }
        None => {
            events.push(RuntimeEvent::Status("codex item missing type".to_string()));
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::kernel::runtime::{
        RuntimeAdapter, RuntimeEvent, RuntimeSessionStartInput, RuntimeTurnInput,
    };

    use super::{parse_codex_stdout, CodexRuntimeAdapter, CodexRuntimeConfig};
    use tempfile::tempdir;
    use uuid::Uuid;

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_adapter_translates_json_agent_message() {
        let sandbox = tempdir().expect("temp dir");
        let codex_stub = sandbox.path().join("codex-stub.sh");
        write_script(
            &codex_stub,
            r#"#!/usr/bin/env bash
cat >/dev/null
echo '{"type":"turn.started"}'
echo '{"type":"item.completed","item":{"id":"msg-1","type":"agent_message","text":"hello from codex"}}'
echo '{"type":"turn.completed","usage":{"input_tokens":1,"cached_input_tokens":0,"output_tokens":2}}'
"#,
        );

        let adapter = CodexRuntimeAdapter::new(CodexRuntimeConfig {
            executable: codex_stub.to_string_lossy().to_string(),
            ..CodexRuntimeConfig::default()
        });

        let handle = adapter
            .session_start(RuntimeSessionStartInput {
                session_id: Uuid::new_v4(),
                working_dir: None,
                environment: Vec::new(),
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
            output.capability_requests.is_empty(),
            "codex adapter should not emit kernel capability requests yet"
        );
        assert!(
            output.events.iter().any(|event| matches!(
                event,
                RuntimeEvent::TextDelta(text) if text == "hello from codex"
            )),
            "agent message should be translated into text delta"
        );
        assert!(
            output
                .events
                .iter()
                .any(|event| matches!(event, RuntimeEvent::Done)),
            "turn output should include terminal done event"
        );

        adapter.close(&handle).await.expect("close");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_adapter_surfaces_nonzero_exit() {
        let sandbox = tempdir().expect("temp dir");
        let codex_stub = sandbox.path().join("codex-stub-fail.sh");
        write_script(
            &codex_stub,
            r#"#!/usr/bin/env bash
cat >/dev/null
echo "stub failure" 1>&2
exit 7
"#,
        );

        let adapter = CodexRuntimeAdapter::new(CodexRuntimeConfig {
            executable: codex_stub.to_string_lossy().to_string(),
            ..CodexRuntimeConfig::default()
        });
        let handle = adapter
            .session_start(RuntimeSessionStartInput {
                session_id: Uuid::new_v4(),
                working_dir: None,
                environment: Vec::new(),
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
            message.contains("exited with code 7"),
            "exit code should be included in error"
        );
        assert!(
            message.contains("stub failure"),
            "stderr output should be surfaced in error"
        );
    }

    #[test]
    fn codex_stdout_falls_back_to_plain_text_when_json_is_invalid() {
        let events = parse_codex_stdout(b"plain line\n");
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
