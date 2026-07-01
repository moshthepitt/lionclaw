use lionclaw_runtime_api::{
    RuntimeEvent, RuntimeFileChange, RuntimeFileChangeStatus, RuntimeMessageLane,
};
use serde_json::Value;

use super::protocol::extract_app_server_item_id;

const FILE_CHANGE_PATH_EVENT_LIMIT: usize = 50;

pub(crate) fn app_server_item_type(params: &Value) -> Option<&str> {
    let item = params.get("item").unwrap_or(params);
    item.get("type").and_then(Value::as_str)
}

pub(crate) fn app_server_event_payload<'a>(
    method: Option<&'a str>,
    message: &'a Value,
) -> Option<(&'a str, &'a Value)> {
    let message_type = message.get("type").and_then(Value::as_str);
    if matches!(method, Some("event_msg" | "event"))
        || matches!(message_type, Some("event_msg" | "event"))
    {
        let payload = message.get("payload").unwrap_or(message);
        return payload
            .get("type")
            .and_then(Value::as_str)
            .map(|kind| (kind, payload));
    }
    if method == Some("image_generation_end") || message_type == Some("image_generation_end") {
        return Some(("image_generation_end", message));
    }
    None
}

pub(crate) fn agent_message_phase_lane(params: &Value) -> Option<RuntimeMessageLane> {
    let phase = app_server_item(params)
        .get("phase")
        .and_then(Value::as_str)
        .or_else(|| params.get("phase").and_then(Value::as_str))?;
    match phase.trim().to_ascii_lowercase().as_str() {
        "answer" | "final" | "final_answer" | "final-answer" | "message" => {
            Some(RuntimeMessageLane::Answer)
        }
        "analysis" | "commentary" | "intermediate" | "plan" | "prelude" | "progress"
        | "reasoning" | "thinking" => Some(RuntimeMessageLane::Reasoning),
        _ => None,
    }
}

pub(crate) fn app_server_text(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .filter_map(|key| value.get(*key))
        .find_map(|value| collect_codex_text(value, 0))
}

pub(crate) fn app_server_error_text(value: &Value) -> String {
    app_server_text(value, &["message", "text", "details", "error"])
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| "codex app-server error".to_string())
}

pub(crate) fn completed_turn_error_text(params: &Value) -> Option<String> {
    let turn = params.get("turn").unwrap_or(params);
    let status = turn
        .get("status")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|status| !status.is_empty());
    let error = turn
        .get("error")
        .or_else(|| params.get("error"))
        .filter(|error| !error.is_null());
    if status.is_none_or(|status| status.eq_ignore_ascii_case("completed")) && error.is_none() {
        return None;
    }

    let message = error
        .map(app_server_error_text)
        .or_else(|| {
            app_server_text(turn, &["message", "statusMessage", "details", "error"])
                .map(|text| text.trim().to_string())
                .filter(|text| !text.is_empty())
        })
        .or_else(|| status.map(turn_status_error_text))
        .unwrap_or_else(|| "codex turn failed".to_string());
    Some(message)
}

fn turn_status_error_text(status: &str) -> String {
    if status.eq_ignore_ascii_case("failed") {
        "codex turn failed".to_string()
    } else if status.eq_ignore_ascii_case("interrupted") {
        "codex turn interrupted".to_string()
    } else {
        format!("codex turn ended with status '{status}'")
    }
}

pub(crate) fn error_notification_will_retry(params: &Value) -> bool {
    params
        .get("willRetry")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) fn describe_app_server_item(params: &Value) -> Option<String> {
    describe_app_server_status_item(params)
}

pub(crate) fn app_server_item_events(params: &Value) -> Vec<RuntimeEvent> {
    match app_server_item_type(params) {
        Some("fileChange") => codex_file_change_item(params)
            .map(|change| RuntimeEvent::FileChange { change })
            .into_iter()
            .collect(),
        Some(_) => describe_app_server_status_item(params)
            .map(|text| RuntimeEvent::Status { code: None, text })
            .into_iter()
            .collect(),
        None => Vec::new(),
    }
}

fn describe_app_server_status_item(params: &Value) -> Option<String> {
    let item = app_server_item(params);
    match app_server_item_type(params)? {
        "agentMessage" | "reasoning" | "userMessage" | "plan" | "fileChange" => None,
        "commandExecution" => describe_command_execution_item(item),
        "webSearch" => describe_web_search_item(item),
        "imageView" => describe_image_view_item(item),
        "collabToolCall" => describe_collab_tool_call_item(item),
        "enteredReviewMode" => Some("codex review started".to_string()),
        "exitedReviewMode" => Some("codex review completed".to_string()),
        other => Some(format!("codex activity: {other}")),
    }
}

fn app_server_item(params: &Value) -> &Value {
    params.get("item").unwrap_or(params)
}

fn describe_command_execution_item(item: &Value) -> Option<String> {
    let command = command_execution_display(item)?;
    let status = item
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("completed");
    let verb = command_execution_verb(status, &command);
    let mut text = format!("codex {verb}: {command}");
    if let Some(cwd) = item.get("cwd").and_then(Value::as_str) {
        if !cwd.trim().is_empty() {
            text.push_str(&format!("  cwd {}", compact_path(cwd)));
        }
    }
    if let Some(exit_code) = item.get("exitCode").and_then(Value::as_i64) {
        text.push_str(&format!("  exit {exit_code}"));
    }
    if let Some(duration_ms) = item.get("durationMs").and_then(Value::as_u64) {
        text.push_str(&format!("  {}", format_millis(duration_ms)));
    }
    Some(text)
}

fn command_execution_verb(status: &str, command: &str) -> &'static str {
    match status {
        "inProgress" => "running",
        "failed" => "failed",
        "declined" => "declined",
        "completed" => {
            let first = command.split_whitespace().next().unwrap_or("");
            match first.trim_matches('"') {
                "rg" | "grep" => "searched",
                "cat" | "sed" | "head" | "tail" | "nl" => "read",
                "ls" | "find" | "git" => "inspected",
                _ => "ran",
            }
        }
        _ => "command",
    }
}

fn command_execution_display(item: &Value) -> Option<String> {
    item.get("command")
        .and_then(command_value_display)
        .or_else(|| command_actions_display(item.get("commandActions")?))
}

fn command_value_display(value: &Value) -> Option<String> {
    match value {
        Value::String(command) => non_empty(command),
        Value::Array(parts) => {
            let parts = parts
                .iter()
                .filter_map(Value::as_str)
                .map(shell_word_display)
                .collect::<Vec<_>>();
            (!parts.is_empty()).then(|| parts.join(" "))
        }
        _ => None,
    }
}

fn command_actions_display(value: &Value) -> Option<String> {
    let actions = value.as_array()?;
    let mut rendered = Vec::new();
    for action in actions {
        if let Some(command) = action
            .get("command")
            .or_else(|| action.get("cmd"))
            .and_then(command_value_display)
        {
            rendered.push(command);
        } else if let Some(action_type) = action.get("type").and_then(Value::as_str) {
            rendered.push(action_type.to_string());
        }
    }
    (!rendered.is_empty()).then(|| rendered.join(" && "))
}

fn codex_file_change_item(params: &Value) -> Option<RuntimeFileChange> {
    let item = app_server_item(params);
    let changes = item.get("changes").and_then(Value::as_array)?;
    let status = item
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("completed");
    let status = match status {
        "inProgress" => RuntimeFileChangeStatus::Editing,
        "completed" => RuntimeFileChangeStatus::Edited,
        "failed" => RuntimeFileChangeStatus::Failed,
        "declined" => RuntimeFileChangeStatus::Declined,
        _ => RuntimeFileChangeStatus::Changed,
    };
    let paths = changes
        .iter()
        .filter_map(|change| change.get("path").and_then(Value::as_str))
        .filter(|path| !path.trim().is_empty())
        .map(compact_path)
        .take(FILE_CHANGE_PATH_EVENT_LIMIT)
        .collect::<Vec<_>>();
    Some(RuntimeFileChange {
        runtime: "codex".to_string(),
        operation_id: extract_app_server_item_id(params),
        status,
        paths,
        total_count: changes.len(),
    })
}

fn describe_web_search_item(item: &Value) -> Option<String> {
    let action = item
        .pointer("/action/type")
        .and_then(Value::as_str)
        .unwrap_or("search");
    let (verb, target) = match action {
        "open_page" => ("opened", web_search_open_target(item)?),
        "find_in_page" => ("found", web_search_find_target(item)?),
        _ => web_search_query_target(item)
            .map(|target| ("searched", target))
            .or_else(|| web_search_open_target(item).map(|target| ("opened", target)))?,
    };
    Some(format!("codex {verb}: {target}"))
}

fn web_search_query_target(item: &Value) -> Option<String> {
    first_non_empty_json_string(
        item,
        &[
            "/query",
            "/queries",
            "/action/query",
            "/action/queries",
            "/action/searchQuery",
        ],
    )
}

fn web_search_open_target(item: &Value) -> Option<String> {
    first_non_empty_json_string(
        item,
        &[
            "/url",
            "/urls",
            "/refId",
            "/action/url",
            "/action/urls",
            "/action/refId",
        ],
    )
}

fn web_search_find_target(item: &Value) -> Option<String> {
    first_non_empty_json_string(
        item,
        &[
            "/pattern",
            "/query",
            "/action/pattern",
            "/action/query",
            "/action/refId",
        ],
    )
}

fn describe_image_view_item(item: &Value) -> Option<String> {
    item.get("path")
        .and_then(Value::as_str)
        .map(|path| format!("codex viewed: {}", compact_path(path)))
}

fn describe_collab_tool_call_item(item: &Value) -> Option<String> {
    let tool = item.get("tool").and_then(Value::as_str)?;
    let status = item
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("completed");
    Some(format!("codex {status}: {tool}"))
}

fn non_empty(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn first_non_empty_json_string(value: &Value, pointers: &[&str]) -> Option<String> {
    pointers
        .iter()
        .filter_map(|pointer| value.pointer(pointer))
        .find_map(non_empty_json_string)
}

fn non_empty_json_string(value: &Value) -> Option<String> {
    value.as_str().and_then(non_empty).or_else(|| {
        value
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .find_map(non_empty)
    })
}

fn shell_word_display(value: &str) -> String {
    if value.is_empty()
        || value
            .chars()
            .any(|ch| ch.is_whitespace() || matches!(ch, '\'' | '"' | '\\' | '$' | '`'))
    {
        format!("{value:?}")
    } else {
        value.to_string()
    }
}

fn compact_path(path: &str) -> String {
    let path = path.trim();
    path.strip_prefix("/workspace/")
        .or_else(|| path.strip_prefix("./"))
        .unwrap_or(path)
        .to_string()
}

fn format_millis(duration_ms: u64) -> String {
    if duration_ms < 1_000 {
        format!("{duration_ms}ms")
    } else {
        format!("{:.1}s", duration_ms as f64 / 1_000.0)
    }
}

fn collect_codex_text(value: &Value, depth: usize) -> Option<String> {
    if depth > 6 {
        return None;
    }

    match value {
        Value::String(text) if text.is_empty() => None,
        Value::String(text) => Some(text.clone()),
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
