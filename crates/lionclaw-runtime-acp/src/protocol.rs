use anyhow::Result;
use lionclaw_runtime_api::TypedFailure;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AcpOpenedSession {
    pub(crate) session_id: String,
    pub(crate) resumed_existing: bool,
    pub(crate) selections: AcpSessionSelections,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct AcpSessionSelections {
    pub(crate) models: Option<AcpSelectionSet>,
    pub(crate) modes: Option<AcpSelectionSet>,
    pub(crate) config_options: Vec<AcpConfigOption>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct AcpSelectionSet {
    pub(crate) current: Option<String>,
    pub(crate) values: Vec<AcpSelectionValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AcpSelectionValue {
    pub(crate) id: String,
    pub(crate) name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AcpConfigOption {
    pub(crate) id: String,
    pub(crate) current: Option<String>,
    pub(crate) values: Vec<String>,
}

impl AcpSessionSelections {
    pub(crate) fn from_session_result(result: &Value) -> Self {
        Self {
            models: selection_set(
                result.get("models"),
                "currentModelId",
                "availableModels",
                "modelId",
            ),
            modes: selection_set(result.get("modes"), "currentModeId", "availableModes", "id"),
            config_options: result
                .get("configOptions")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|option| {
                    Some(AcpConfigOption {
                        id: option.get("id")?.as_str()?.to_string(),
                        current: option
                            .get("currentValue")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        values: option
                            .get("options")
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                            .filter_map(|value| {
                                value
                                    .get("value")
                                    .and_then(Value::as_str)
                                    .map(str::to_string)
                            })
                            .collect(),
                    })
                })
                .collect(),
        }
    }
}

fn selection_set(
    value: Option<&Value>,
    current_key: &str,
    available_key: &str,
    id_key: &str,
) -> Option<AcpSelectionSet> {
    let value = value?;
    let values = value
        .get(available_key)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            Some(AcpSelectionValue {
                id: entry.get(id_key)?.as_str()?.to_string(),
                name: entry
                    .get("name")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            })
        })
        .collect();
    Some(AcpSelectionSet {
        current: value
            .get(current_key)
            .and_then(Value::as_str)
            .map(str::to_string),
        values,
    })
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
        return Err(acp_typed_failure(method, error).into());
    }
    Ok(AcpResponse {
        raw: message.raw,
        result: message.value.get("result").cloned().unwrap_or(Value::Null),
    })
}

pub(crate) fn acp_typed_failure(method: &str, error: &Value) -> TypedFailure {
    let code = error
        .get("code")
        .and_then(Value::as_i64)
        .map_or_else(|| "acp.error".to_string(), |code| code.to_string());
    let detail = format!("ACP {method} failed: {}", acp_error_text(error));
    let data = error.get("data").unwrap_or(&Value::Null);
    if data.get("retryable").and_then(Value::as_bool) == Some(true) {
        TypedFailure::transient(
            code,
            detail,
            data.get("retryAfterMs").and_then(Value::as_u64),
        )
    } else {
        TypedFailure::permanent(code, detail)
    }
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

#[cfg(test)]
mod failure_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn retryability_uses_structured_json_rpc_evidence_not_prose() {
        let prose = "rate limited, retry later";
        assert!(matches!(
            acp_typed_failure("session/prompt", &json!({"code": -32000, "message": prose})),
            TypedFailure::PermanentRuntime { .. }
        ));
        assert!(matches!(
            acp_typed_failure(
                "session/prompt",
                &json!({"code": -32000, "message": prose, "data": {"retryable": true, "retryAfterMs": 25}})
            ),
            TypedFailure::TransientRuntime {
                retry_after_ms: Some(25),
                ..
            }
        ));
    }
}
