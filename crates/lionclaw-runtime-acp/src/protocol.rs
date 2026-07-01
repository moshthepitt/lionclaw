use anyhow::{anyhow, Result};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AcpOpenedSession {
    pub(crate) session_id: String,
    pub(crate) resumed_existing: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct AcpSessionCapabilities {
    load_session: bool,
    resume_session: bool,
}

impl AcpSessionCapabilities {
    pub(crate) fn from_initialize_result(result: &Value) -> Self {
        let agent_capabilities = result.get("agentCapabilities");
        Self {
            load_session: agent_capabilities
                .and_then(|capabilities| capabilities.get("loadSession"))
                .and_then(Value::as_bool)
                .unwrap_or(false),
            resume_session: agent_capabilities
                .and_then(|capabilities| capabilities.pointer("/sessionCapabilities/resume"))
                .is_some_and(acp_capability_object_enabled),
        }
    }

    pub(crate) fn reopen_method(self) -> Option<&'static str> {
        if self.load_session {
            Some("session/load")
        } else if self.resume_session {
            Some("session/resume")
        } else {
            None
        }
    }
}

fn acp_capability_object_enabled(value: &Value) -> bool {
    value.as_object().is_some() || value.as_bool() == Some(true)
}

#[derive(Debug, Clone)]
pub(crate) struct AcpMessage {
    pub(crate) raw: String,
    pub(crate) value: Value,
}

#[derive(Debug, Clone)]
pub(crate) struct AcpResponse {
    pub(crate) raw: String,
    pub(crate) result: Value,
}

pub(crate) fn acp_response_id(message: &Value) -> Option<u64> {
    if message.get("result").is_none() && message.get("error").is_none() {
        return None;
    }
    message.get("id").and_then(Value::as_u64)
}

pub(crate) fn acp_is_server_request(message: &Value) -> bool {
    message.get("method").and_then(Value::as_str).is_some()
        && message.get("id").is_some()
        && message.get("result").is_none()
        && message.get("error").is_none()
}

pub(crate) fn parse_acp_response(message: AcpMessage, method: &str) -> Result<AcpResponse> {
    if let Some(error) = message.value.get("error") {
        return Err(anyhow!("ACP {method} failed: {}", acp_error_text(error)));
    }
    Ok(AcpResponse {
        raw: message.raw,
        result: message.value.get("result").cloned().unwrap_or(Value::Null),
    })
}

fn acp_error_text(error: &Value) -> String {
    let code = error.get("code").and_then(Value::as_i64);
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("unknown error");
    match code {
        Some(code) => format!("{code}: {message}"),
        None => message.to_string(),
    }
}
