use std::{collections::HashMap, process::Stdio, sync::Arc};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::RwLock;
use tokio::{io::AsyncWriteExt, process::Command};
use uuid::Uuid;

use super::policy::Capability;

#[derive(Debug, Clone)]
pub struct RuntimeAdapterInfo {
    pub id: String,
    pub version: String,
    pub healthy: bool,
}

#[derive(Debug, Clone)]
pub struct RuntimeSessionStartInput {
    pub session_id: Uuid,
    pub working_dir: Option<String>,
    pub selected_skills: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RuntimeSessionHandle {
    pub runtime_session_id: String,
}

#[derive(Debug, Clone)]
pub struct RuntimeTurnInput {
    pub runtime_session_id: String,
    pub prompt: String,
    pub selected_skills: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RuntimeCapabilityRequest {
    pub request_id: String,
    pub skill_id: String,
    pub capability: Capability,
    pub scope: Option<String>,
    pub payload: Value,
}

#[derive(Debug, Clone)]
pub struct RuntimeCapabilityResult {
    pub request_id: String,
    pub allowed: bool,
    pub reason: Option<String>,
    pub output: Value,
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeTurnOutput {
    pub events: Vec<RuntimeEvent>,
    pub capability_requests: Vec<RuntimeCapabilityRequest>,
}

#[derive(Debug, Clone)]
pub enum RuntimeEvent {
    TextDelta(String),
    Status(String),
    Done,
    Error(String),
}

#[async_trait]
pub trait RuntimeAdapter: Send + Sync {
    async fn info(&self) -> RuntimeAdapterInfo;
    async fn session_start(&self, input: RuntimeSessionStartInput) -> Result<RuntimeSessionHandle>;
    async fn turn(&self, input: RuntimeTurnInput) -> Result<RuntimeTurnOutput>;
    async fn resolve_capability_requests(
        &self,
        handle: &RuntimeSessionHandle,
        results: Vec<RuntimeCapabilityResult>,
    ) -> Result<Vec<RuntimeEvent>>;
    async fn cancel(&self, handle: &RuntimeSessionHandle, reason: Option<String>) -> Result<()>;
    async fn close(&self, handle: &RuntimeSessionHandle) -> Result<()>;
}

#[derive(Default)]
pub struct RuntimeRegistry {
    adapters: RwLock<HashMap<String, Arc<dyn RuntimeAdapter>>>,
}

impl RuntimeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(&self, id: impl Into<String>, adapter: Arc<dyn RuntimeAdapter>) {
        self.adapters.write().await.insert(id.into(), adapter);
    }

    pub async fn get(&self, id: &str) -> Option<Arc<dyn RuntimeAdapter>> {
        self.adapters.read().await.get(id).cloned()
    }

    pub async fn list(&self) -> Vec<String> {
        self.adapters.read().await.keys().cloned().collect()
    }
}

pub struct MockRuntimeAdapter;

#[async_trait]
impl RuntimeAdapter for MockRuntimeAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "mock".to_string(),
            version: "0.1".to_string(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        _input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("mock-{}", Uuid::new_v4()),
        })
    }

    async fn turn(&self, input: RuntimeTurnInput) -> Result<RuntimeTurnOutput> {
        let mut events = Vec::new();
        events.push(RuntimeEvent::Status(
            "mock runtime started turn".to_string(),
        ));

        let skill_context = if input.selected_skills.is_empty() {
            "no skill context selected".to_string()
        } else {
            format!("selected skills: {}", input.selected_skills.join(", "))
        };

        events.push(RuntimeEvent::TextDelta(format!(
            "[mock] {} | prompt: {}",
            skill_context, input.prompt
        )));

        let mut capability_requests = Vec::new();
        if let Some(skill_id) = input.selected_skills.first() {
            for (index, capability) in parse_capability_markers(&input.prompt)
                .into_iter()
                .enumerate()
            {
                capability_requests.push(RuntimeCapabilityRequest {
                    request_id: format!("req-{}", index + 1),
                    skill_id: skill_id.clone(),
                    capability,
                    scope: None,
                    payload: Value::Null,
                });
            }
        }

        if capability_requests.is_empty() {
            events.push(RuntimeEvent::Done);
        } else {
            events.push(RuntimeEvent::Status(format!(
                "mock runtime requested {} capability checks",
                capability_requests.len()
            )));
        }

        Ok(RuntimeTurnOutput {
            events,
            capability_requests,
        })
    }

    async fn resolve_capability_requests(
        &self,
        _handle: &RuntimeSessionHandle,
        results: Vec<RuntimeCapabilityResult>,
    ) -> Result<Vec<RuntimeEvent>> {
        let mut events = Vec::with_capacity(results.len() + 1);
        for result in results {
            let verdict = if result.allowed { "granted" } else { "denied" };
            events.push(RuntimeEvent::Status(format!(
                "capability:{}:{}",
                result.request_id, verdict
            )));
            if let Some(reason) = result.reason {
                events.push(RuntimeEvent::Status(format!(
                    "capability:{}:reason:{}",
                    result.request_id, reason
                )));
            }
        }
        events.push(RuntimeEvent::Done);
        Ok(events)
    }

    async fn cancel(&self, _handle: &RuntimeSessionHandle, _reason: Option<String>) -> Result<()> {
        Ok(())
    }

    async fn close(&self, _handle: &RuntimeSessionHandle) -> Result<()> {
        Ok(())
    }
}

fn parse_capability_markers(prompt: &str) -> Vec<Capability> {
    let mut requested = Vec::new();
    for (marker, capability) in [
        ("[cap:fs.read]", Capability::FsRead),
        ("[cap:fs.write]", Capability::FsWrite),
        ("[cap:net.egress]", Capability::NetEgress),
        ("[cap:secret.request]", Capability::SecretRequest),
        ("[cap:channel.send]", Capability::ChannelSend),
        ("[cap:scheduler.run]", Capability::SchedulerRun),
    ] {
        if prompt.contains(marker) {
            requested.push(capability);
        }
    }
    requested
}

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

        let mut command = Command::new(&self.config.executable);
        command.arg("exec").arg("--json");

        if let Some(model) = &self.config.model {
            command.arg("--model").arg(model);
        }
        if !self.config.sandbox_mode.trim().is_empty() {
            command.arg("--sandbox").arg(&self.config.sandbox_mode);
        }
        if self.config.skip_git_repo_check {
            command.arg("--skip-git-repo-check");
        }
        if self.config.ephemeral {
            command.arg("--ephemeral");
        }
        if let Some(working_dir) = session.working_dir.as_deref() {
            command.arg("--cd").arg(working_dir);
        }

        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = command.spawn().with_context(|| {
            format!(
                "failed to spawn Codex executable '{}'",
                self.config.executable
            )
        })?;

        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(input.prompt.as_bytes())
                .await
                .context("failed to write prompt to codex stdin")?;
            stdin
                .shutdown()
                .await
                .context("failed to close codex stdin")?;
        }

        let output = child
            .wait_with_output()
            .await
            .context("failed to wait for codex process")?;

        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if !output.status.success() {
            let code = output.status.code().unwrap_or(1);
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
    use super::{
        parse_codex_stdout, CodexRuntimeAdapter, CodexRuntimeConfig, RuntimeAdapter, RuntimeEvent,
        RuntimeSessionStartInput, RuntimeTurnInput,
    };
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
