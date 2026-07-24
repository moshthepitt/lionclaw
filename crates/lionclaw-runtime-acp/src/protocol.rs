use anyhow::anyhow;
use lionclaw_runtime_api::{
    AppliedRuntimeConfiguration, RuntimeConfigurationConfirmation, RuntimeUsage, RuntimeUsageCost,
    RuntimeUsageCostScope, RuntimeUsageDetails, TypedFailure,
};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AcpOpenedSession {
    pub(crate) session_id: String,
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

    pub(crate) fn current_model(&self) -> Option<&str> {
        self.models
            .as_ref()
            .and_then(|models| models.current.as_deref())
            .or_else(|| self.config_option_current("model"))
            .filter(|value| !value.trim().is_empty())
    }

    pub(crate) fn current_mode(&self) -> Option<&str> {
        self.modes
            .as_ref()
            .and_then(|modes| modes.current.as_deref())
            .or_else(|| self.config_option_current("mode"))
            .filter(|value| !value.trim().is_empty())
    }

    pub(crate) fn observed_configuration(&self) -> AppliedRuntimeConfiguration {
        let mut configuration = AppliedRuntimeConfiguration::default();
        if let Some(model) = self.current_model() {
            configuration.applied_model = Some(model.to_string());
            configuration.model_confirmation = Some(RuntimeConfigurationConfirmation::Observed);
        }
        if let Some(mode) = self.current_mode() {
            configuration.applied_mode = Some(mode.to_string());
            configuration.mode_confirmation = Some(RuntimeConfigurationConfirmation::Observed);
        }
        configuration
    }

    fn config_option_current(&self, id: &str) -> Option<&str> {
        self.config_options
            .iter()
            .find(|option| option.id == id)
            .and_then(|option| option.current.as_deref())
    }
}

pub(crate) fn acp_session_update(message: &Value) -> Option<&Value> {
    if message.get("method").and_then(Value::as_str) != Some("session/update") {
        return None;
    }
    message
        .pointer("/params/update")
        .or_else(|| message.get("params"))
}

pub(crate) fn acp_update_observed_configuration(update: &Value) -> AppliedRuntimeConfiguration {
    match update.get("sessionUpdate").and_then(Value::as_str) {
        Some("current_mode_update") => {
            let mut configuration = AppliedRuntimeConfiguration::default();
            if let Some(mode) = current_mode_update_value(update) {
                configuration.applied_mode = Some(mode.to_string());
                configuration.mode_confirmation = Some(RuntimeConfigurationConfirmation::Observed);
            }
            configuration
        }
        Some("config_option_update") => {
            AcpSessionSelections::from_session_result(update).observed_configuration()
        }
        _ => AppliedRuntimeConfiguration::default(),
    }
}

pub(crate) fn acp_update_usage(update: &Value) -> RuntimeUsage {
    if update.get("sessionUpdate").and_then(Value::as_str) != Some("usage_update") {
        return RuntimeUsage::NotReported;
    }
    RuntimeUsage::from_details(RuntimeUsageDetails {
        context_used_tokens: u64_field(update, "used"),
        context_window_tokens: u64_field(update, "size"),
        cost: usage_cost(update.get("cost"), RuntimeUsageCostScope::SessionCumulative),
        ..Default::default()
    })
}

pub(crate) fn acp_prompt_usage(result: &Value) -> RuntimeUsage {
    let Some(usage) = result.get("usage") else {
        return RuntimeUsage::NotReported;
    };
    RuntimeUsage::from_details(RuntimeUsageDetails {
        input_tokens: u64_field(usage, "inputTokens"),
        output_tokens: u64_field(usage, "outputTokens"),
        total_tokens: u64_field(usage, "totalTokens"),
        reasoning_tokens: u64_field(usage, "reasoningTokens")
            .or_else(|| u64_field(usage, "thoughtTokens")),
        cached_input_tokens: u64_field(usage, "cachedInputTokens")
            .or_else(|| u64_field(usage, "cacheReadInputTokens")),
        cost: usage_cost(usage.get("cost"), RuntimeUsageCostScope::Turn),
        ..Default::default()
    })
}

fn current_mode_update_value(update: &Value) -> Option<&str> {
    text_field(update, "currentModeId").or_else(|| text_field(update, "modeId"))
}

fn text_field<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
}

fn u64_field(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(Value::as_u64)
}

fn usage_cost(value: Option<&Value>, scope: RuntimeUsageCostScope) -> Option<RuntimeUsageCost> {
    let value = value?;
    let amount = match value.get("amount")? {
        Value::Number(amount) => amount.to_string(),
        Value::String(amount) if !amount.trim().is_empty() => amount.clone(),
        _ => return None,
    };
    let currency = text_field(value, "currency")?.to_string();
    Some(RuntimeUsageCost {
        amount,
        currency,
        scope,
    })
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
        if self.resume_session {
            Some("session/resume")
        } else if self.load_session {
            Some("session/load")
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
    pub(crate) value: Value,
}

#[derive(Debug, Clone)]
pub(crate) struct AcpResponse {
    pub(crate) result: Value,
}

pub(crate) enum AcpProviderRejection {
    Permanent(TypedFailure),
    Retryable(TypedFailure),
}

impl AcpProviderRejection {
    pub(crate) fn into_typed_failure(self) -> TypedFailure {
        match self {
            Self::Permanent(failure) | Self::Retryable(failure) => failure,
        }
    }
}

pub(crate) enum AcpResponseOutcome {
    Success(AcpResponse),
    Rejected(AcpProviderRejection),
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

pub(crate) fn parse_acp_response(
    message: AcpMessage,
    method: &str,
) -> anyhow::Result<AcpResponseOutcome> {
    let Some(response) = message.value.as_object() else {
        return Err(anyhow!("ACP {method} returned a non-object response"));
    };
    if response.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err(anyhow!(
            "ACP {method} returned a response without JSON-RPC 2.0"
        ));
    }
    match (response.get("result"), response.get("error")) {
        (Some(result), None) => Ok(AcpResponseOutcome::Success(AcpResponse {
            result: result.clone(),
        })),
        (None, Some(error))
            if error.get("code").and_then(Value::as_i64).is_some()
                && error.get("message").and_then(Value::as_str).is_some() =>
        {
            Ok(AcpResponseOutcome::Rejected(acp_provider_rejection(
                method, error,
            )))
        }
        (None, Some(_)) => Err(anyhow!(
            "ACP {method} returned a malformed JSON-RPC error response"
        )),
        (Some(_), Some(_)) => Err(anyhow!(
            "ACP {method} returned both result and error in one response"
        )),
        (None, None) => Err(anyhow!(
            "ACP {method} returned neither result nor error in its response"
        )),
    }
}

fn acp_provider_rejection(method: &str, error: &Value) -> AcpProviderRejection {
    let code = error
        .get("code")
        .and_then(Value::as_i64)
        .map_or_else(|| "acp.error".to_string(), |code| code.to_string());
    let detail = format!("ACP {method} failed: {}", acp_error_text(error));
    let data = error.get("data").unwrap_or(&Value::Null);
    if data.get("retryable").and_then(Value::as_bool) == Some(true) {
        AcpProviderRejection::Retryable(TypedFailure::transient(
            code,
            detail,
            data.get("retryAfterMs").and_then(Value::as_u64),
        ))
    } else {
        AcpProviderRejection::Permanent(TypedFailure::permanent(code, detail))
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
            acp_provider_rejection("session/prompt", &json!({"code": -32000, "message": prose})),
            AcpProviderRejection::Permanent(TypedFailure::PermanentRuntime { .. })
        ));
        assert!(matches!(
            acp_provider_rejection(
                "session/prompt",
                &json!({"code": -32000, "message": prose, "data": {"retryable": true, "retryAfterMs": 25}})
            ),
            AcpProviderRejection::Retryable(TypedFailure::TransientRuntime {
                retry_after_ms: Some(25),
                ..
            })
        ));
    }

    #[test]
    fn provider_metadata_without_a_retry_bit_remains_permanent() {
        let error = json!({
            "code": -32603,
            "message": "Internal error",
            "data": {"service": "session", "errorName": "APIError"}
        });
        assert!(matches!(
            acp_provider_rejection("session/prompt", &error),
            AcpProviderRejection::Permanent(TypedFailure::PermanentRuntime { .. })
        ));
    }
}
