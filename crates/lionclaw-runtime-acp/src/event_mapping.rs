use serde_json::Value;

use lionclaw_runtime_api::{RawTurnPayload, RuntimeEvent, RuntimeMessageLane, TurnEvent};

use crate::driver::ACP_PROTOCOL_NAME;
use crate::protocol::AcpMessage;

pub(crate) fn acp_turn_events(message: &AcpMessage) -> Vec<TurnEvent> {
    if message.value.get("method").and_then(Value::as_str) != Some("session/update") {
        return Vec::new();
    }
    let Some(update) = message
        .value
        .pointer("/params/update")
        .or_else(|| message.value.get("params"))
    else {
        return Vec::new();
    };
    let Some(session_update) = update.get("sessionUpdate").and_then(Value::as_str) else {
        return Vec::new();
    };

    let event = match session_update {
        "agent_message_chunk" => {
            acp_content_text(update.get("content")).map(|text| RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text,
            })
        }
        "agent_thought_chunk" => {
            acp_content_text(update.get("content")).map(|text| RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Reasoning,
                text,
            })
        }
        "tool_call" | "tool_call_update" => {
            acp_tool_status(update).map(|text| RuntimeEvent::Status { code: None, text })
        }
        _ => None,
    };

    event
        .map(|event| {
            vec![TurnEvent::with_raw(
                event,
                RawTurnPayload {
                    driver: ACP_PROTOCOL_NAME.to_string(),
                    payload: message.raw.clone(),
                },
            )]
        })
        .unwrap_or_default()
}

fn acp_content_text(value: Option<&Value>) -> Option<String> {
    let text = match value {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| acp_content_text(Some(item)))
            .collect::<Vec<_>>()
            .join(""),
        Some(Value::Object(object)) => object
            .get("text")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| acp_content_text(object.get("content")))
            .or_else(|| acp_content_text(object.get("parts")))
            .unwrap_or_default(),
        _ => String::new(),
    };
    (!text.is_empty()).then_some(text)
}

fn acp_tool_status(update: &Value) -> Option<String> {
    let status = update
        .get("status")
        .and_then(Value::as_str)
        .filter(|status| !status.trim().is_empty())
        .unwrap_or("updated");
    let title = update
        .get("title")
        .or_else(|| update.get("kind"))
        .and_then(Value::as_str)
        .filter(|title| !title.trim().is_empty())
        .unwrap_or("tool");
    Some(format!("acp tool {status}: {title}"))
}
