use anyhow::Result;
use lionclaw_runtime_api::{NetworkMode, TypedFailure};
use serde_json::{json, Value};

use super::event_mapping::app_server_error_text;

pub(crate) fn app_server_request_message(id: u64, method: &str, params: Value) -> Value {
    json!({
        "id": id,
        "method": method,
        "params": params,
    })
}

pub(crate) fn app_server_notification_message(method: &str, params: Value) -> Value {
    json!({
        "method": method,
        "params": params,
    })
}

pub(crate) fn app_server_error_response(id: Value, code: i64, message: String) -> Value {
    json!({
        "id": id,
        "error": {
            "code": code,
            "message": message,
        },
    })
}

pub(crate) fn response_id(message: &Value) -> Option<u64> {
    message.get("id").and_then(Value::as_u64)
}

pub(crate) fn parse_app_server_response(message: Value, method: &str) -> Result<Value> {
    if let Some(error) = message.get("error") {
        let code = error
            .get("code")
            .and_then(|code| {
                code.as_str()
                    .map(str::to_string)
                    .or_else(|| code.as_i64().map(|code| code.to_string()))
            })
            .unwrap_or_else(|| "codex.app_server".to_string());
        return Err(TypedFailure::permanent(
            code,
            format!(
                "codex app-server {method} failed: {}",
                app_server_error_text(error)
            ),
        )
        .into());
    }
    Ok(message.get("result").cloned().unwrap_or(Value::Null))
}

pub(crate) fn thread_start_params(model: Option<&str>) -> Value {
    let mut params = json!({
        "approvalPolicy": "never",
    });
    insert_optional_model(&mut params, model);
    params
}

pub(crate) fn thread_resume_params(thread_id: &str, model: Option<&str>) -> Value {
    let mut params = json!({
        "threadId": thread_id,
        "approvalPolicy": "never",
    });
    insert_optional_model(&mut params, model);
    params
}

pub(crate) fn turn_start_params(
    thread_id: &str,
    prompt: &str,
    model: Option<&str>,
    network_mode: NetworkMode,
) -> Value {
    let mut params = json!({
        "threadId": thread_id,
        "input": [
            {
                "type": "text",
                "text": prompt,
            }
        ],
        "approvalPolicy": "never",
        "sandboxPolicy": {
            "type": "externalSandbox",
            "networkAccess": match network_mode {
                NetworkMode::On => "enabled",
                NetworkMode::None => "restricted",
            },
        },
    });
    insert_optional_model(&mut params, model);
    params
}

fn insert_optional_model(params: &mut Value, model: Option<&str>) {
    let Some(model) = model else {
        return;
    };
    let Some(params) = params.as_object_mut() else {
        return;
    };
    params.insert("model".to_string(), json!(model));
}

pub(crate) fn extract_app_server_thread_id(value: &Value) -> Option<String> {
    value
        .pointer("/thread/id")
        .and_then(Value::as_str)
        .or_else(|| value.pointer("/turn/threadId").and_then(Value::as_str))
        .or_else(|| value.pointer("/turn/thread/id").and_then(Value::as_str))
        .or_else(|| value.pointer("/item/threadId").and_then(Value::as_str))
        .or_else(|| value.get("threadId").and_then(Value::as_str))
        .or_else(|| value.get("thread_id").and_then(Value::as_str))
        .filter(|thread_id| !thread_id.trim().is_empty())
        .map(|thread_id| thread_id.trim().to_string())
}

pub(crate) fn extract_app_server_turn_id(value: &Value) -> Option<String> {
    value
        .pointer("/turn/id")
        .and_then(Value::as_str)
        .or_else(|| value.pointer("/item/turnId").and_then(Value::as_str))
        .or_else(|| value.get("turnId").and_then(Value::as_str))
        .or_else(|| value.get("turn_id").and_then(Value::as_str))
        .filter(|turn_id| !turn_id.trim().is_empty())
        .map(|turn_id| turn_id.trim().to_string())
}

pub(crate) fn extract_app_server_model(value: &Value) -> Option<String> {
    value
        .pointer("/turn/model")
        .and_then(Value::as_str)
        .or_else(|| value.pointer("/thread/model").and_then(Value::as_str))
        .or_else(|| value.get("model").and_then(Value::as_str))
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .map(str::to_string)
}

pub(crate) fn extract_app_server_item_id(value: &Value) -> Option<String> {
    value
        .pointer("/item/id")
        .and_then(Value::as_str)
        .or_else(|| value.get("itemId").and_then(Value::as_str))
        .or_else(|| value.get("item_id").and_then(Value::as_str))
        .or_else(|| value.get("id").and_then(Value::as_str))
        .filter(|item_id| !item_id.trim().is_empty())
        .map(|item_id| item_id.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_error_preserves_scalar_code_without_json_quoting() {
        let error = parse_app_server_response(
            json!({"error": {"code": "capacity", "message": "busy"}}),
            "turn/start",
        )
        .unwrap_err();
        let failure = error.downcast_ref::<TypedFailure>().unwrap();
        assert_eq!(failure.evidence().code.as_deref(), Some("capacity"));
    }
}
