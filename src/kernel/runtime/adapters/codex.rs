use std::collections::HashMap;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::kernel::runtime::{
    RuntimeAdapter, RuntimeAdapterInfo, RuntimeCapabilityResult, RuntimeEvent, RuntimeEventSender,
    RuntimeMessageLane, RuntimeSessionHandle, RuntimeSessionStartInput, RuntimeTurnInput,
    RuntimeTurnResult,
};

use super::subprocess::{run_non_interactive_streaming, SubprocessInvocation};

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
            sandbox_mode: default.sandbox_mode,
            skip_git_repo_check: default.skip_git_repo_check,
            ephemeral: default.ephemeral,
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

    async fn turn(
        &self,
        input: RuntimeTurnInput,
        events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
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

        let mut saw_done = false;
        let output = run_non_interactive_streaming(&invocation, |line| {
            for event in parse_codex_output_line(line) {
                if matches!(event, RuntimeEvent::Done) {
                    saw_done = true;
                }
                let _ = events.send(event);
            }
            Ok(())
        })
        .await?;
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if !output.success() {
            let code = output.exit_code.unwrap_or(1);
            if stderr.is_empty() {
                return Err(anyhow!("codex exec exited with code {}", code));
            }
            return Err(anyhow!("codex exec exited with code {}: {}", code, stderr));
        }

        if !saw_done {
            let _ = events.send(RuntimeEvent::Done);
        }

        Ok(RuntimeTurnResult {
            capability_requests: Vec::new(),
        })
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
        RuntimeAdapter, RuntimeEvent, RuntimeMessageLane, RuntimeSessionStartInput,
        RuntimeTurnInput,
    };

    use super::{parse_codex_stdout, CodexRuntimeAdapter, CodexRuntimeConfig};
    use tempfile::tempdir;
    use tokio::sync::mpsc;
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
echo '{"type":"item.completed","item":{"id":"reason-1","type":"reasoning","text":"thinking through the request"}}'
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

        let (tx, rx) = mpsc::unbounded_channel();
        let output = adapter
            .turn(
                RuntimeTurnInput {
                    runtime_session_id: handle.runtime_session_id.clone(),
                    prompt: "hello".to_string(),
                    selected_skills: Vec::new(),
                },
                tx,
            )
            .await
            .expect("turn");
        let events = collect_events(rx).await;

        assert!(
            output.capability_requests.is_empty(),
            "codex adapter should not emit kernel capability requests yet"
        );
        assert!(
            events.iter().any(|event| matches!(
                event,
                RuntimeEvent::MessageDelta { lane: RuntimeMessageLane::Reasoning, text } if text == "thinking through the request"
            )),
            "reasoning item text should be translated into reasoning delta"
        );
        assert!(
            events.iter().any(|event| matches!(
                event,
                RuntimeEvent::MessageDelta { lane: RuntimeMessageLane::Answer, text } if text == "hello from codex"
            )),
            "agent message should be translated into text delta"
        );
        assert!(
            events
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
            .turn(
                RuntimeTurnInput {
                    runtime_session_id: handle.runtime_session_id,
                    prompt: "hello".to_string(),
                    selected_skills: Vec::new(),
                },
                mpsc::unbounded_channel().0,
            )
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

    async fn collect_events(mut rx: mpsc::UnboundedReceiver<RuntimeEvent>) -> Vec<RuntimeEvent> {
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        events
    }

    #[cfg(unix)]
    fn write_script(path: &std::path::Path, content: &str) {
        use std::{fs, io::Write, os::unix::fs::PermissionsExt};

        let temp_path = path.with_extension("tmp");
        let mut file = fs::File::create(&temp_path).expect("create temp script");
        file.write_all(content.as_bytes())
            .expect("write temp script");
        file.sync_all().expect("sync temp script");
        drop(file);
        let permissions = fs::Permissions::from_mode(0o755);
        fs::set_permissions(&temp_path, permissions).expect("chmod temp script");
        fs::rename(&temp_path, path).expect("install script");
    }
}
