use serde::{Deserialize, Serialize};

use crate::prelude::*;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppliedRuntimeConfiguration {
    pub requested_model: Option<String>,
    pub applied_model: Option<String>,
    pub model_confirmation: Option<RuntimeConfigurationConfirmation>,
    pub requested_mode: Option<String>,
    pub applied_mode: Option<String>,
    pub mode_confirmation: Option<RuntimeConfigurationConfirmation>,
}

impl AppliedRuntimeConfiguration {
    pub fn is_empty(&self) -> bool {
        self.requested_model.is_none()
            && self.applied_model.is_none()
            && self.model_confirmation.is_none()
            && self.requested_mode.is_none()
            && self.applied_mode.is_none()
            && self.mode_confirmation.is_none()
    }

    pub fn merge_observed(&mut self, observed: &Self) {
        if observed.applied_model.is_some() || observed.model_confirmation.is_some() {
            self.applied_model.clone_from(&observed.applied_model);
            self.model_confirmation = observed.model_confirmation;
        }
        if observed.applied_mode.is_some() || observed.mode_confirmation.is_some() {
            self.applied_mode.clone_from(&observed.applied_mode);
            self.mode_confirmation = observed.mode_confirmation;
        }
    }

    pub fn projected(mut self) -> Self {
        self.requested_model = self.requested_model.map(|value| bounded_text(&value));
        self.applied_model = self.applied_model.map(|value| bounded_text(&value));
        self.requested_mode = self.requested_mode.map(|value| bounded_text(&value));
        self.applied_mode = self.applied_mode.map(|value| bounded_text(&value));
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeConfigurationConfirmation {
    /// A first-class protocol setter completed successfully, but the protocol
    /// provides no generic read-current-selection operation.
    Acknowledged,
    /// The runtime returned the selected value as its current configuration.
    Observed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeUsageCostScope {
    Turn,
    SessionCumulative,
}

impl RuntimeUsageCostScope {
    pub const fn slug(self) -> &'static str {
        match self {
            Self::Turn => "turn",
            Self::SessionCumulative => "session_cumulative",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeUsageCost {
    pub amount: String,
    pub currency: String,
    pub scope: RuntimeUsageCostScope,
}

impl RuntimeUsageCost {
    pub fn projected(mut self) -> Self {
        self.amount = bounded_text(&self.amount);
        self.currency = bounded_text(&self.currency);
        self
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeUsageDetails {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_used_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<RuntimeUsageCost>,
}

impl RuntimeUsageDetails {
    pub fn is_empty(&self) -> bool {
        self.input_tokens.is_none()
            && self.output_tokens.is_none()
            && self.total_tokens.is_none()
            && self.reasoning_tokens.is_none()
            && self.cached_input_tokens.is_none()
            && self.context_used_tokens.is_none()
            && self.context_window_tokens.is_none()
            && self.cost.is_none()
    }

    pub fn merge_observed(&mut self, other: Self) {
        if other.input_tokens.is_some() {
            self.input_tokens = other.input_tokens;
        }
        if other.output_tokens.is_some() {
            self.output_tokens = other.output_tokens;
        }
        if other.total_tokens.is_some() {
            self.total_tokens = other.total_tokens;
        }
        if other.reasoning_tokens.is_some() {
            self.reasoning_tokens = other.reasoning_tokens;
        }
        if other.cached_input_tokens.is_some() {
            self.cached_input_tokens = other.cached_input_tokens;
        }
        if other.context_used_tokens.is_some() {
            self.context_used_tokens = other.context_used_tokens;
        }
        if other.context_window_tokens.is_some() {
            self.context_window_tokens = other.context_window_tokens;
        }
        if other.cost.is_some() {
            self.cost = other.cost;
        }
    }

    pub fn projected(mut self) -> Self {
        self.cost = self.cost.map(RuntimeUsageCost::projected);
        self
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum RuntimeUsage {
    #[default]
    NotReported,
    Reported {
        usage: RuntimeUsageDetails,
    },
}

impl RuntimeUsage {
    pub fn from_details(details: RuntimeUsageDetails) -> Self {
        if details.is_empty() {
            Self::NotReported
        } else {
            Self::Reported { usage: details }
        }
    }

    pub fn is_reported(&self) -> bool {
        matches!(self, Self::Reported { .. })
    }

    pub fn details(&self) -> Option<&RuntimeUsageDetails> {
        match self {
            Self::Reported { usage } => Some(usage),
            Self::NotReported => None,
        }
    }

    pub fn merge_observed(&mut self, other: Self) {
        let Self::Reported { usage: other } = other else {
            return;
        };
        match self {
            Self::NotReported => *self = Self::from_details(other),
            Self::Reported { usage } => usage.merge_observed(other),
        }
    }

    pub fn projected(self) -> Self {
        match self {
            Self::NotReported => Self::NotReported,
            Self::Reported { usage } => Self::from_details(usage.projected()),
        }
    }
}

pub const FAILURE_TEXT_LIMIT: usize = 8 * 1024;
const TRUNCATION_MARKER: &str = "\n...[truncated]";

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypedFailureEvidence {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    pub detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stderr: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub final_response: String,
    #[serde(default)]
    pub configuration: AppliedRuntimeConfiguration,
    #[serde(default)]
    pub runtime_usage: RuntimeUsage,
}

impl TypedFailureEvidence {
    pub fn new(code: Option<String>, detail: impl Into<String>) -> Self {
        Self {
            code: code.map(|code| bounded_text(&code)),
            detail: bounded_text(&detail.into()),
            ..Self::default()
        }
    }

    pub fn project(mut self) -> Self {
        self.code = self.code.map(|code| bounded_text(&code));
        self.detail = bounded_text(&self.detail);
        self.stderr = bounded_text(&self.stderr);
        self.final_response = bounded_text(&self.final_response);
        self.stop_reason = self.stop_reason.map(|reason| bounded_text(&reason));
        self.configuration = self.configuration.projected();
        self.runtime_usage = self.runtime_usage.projected();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum TypedFailure {
    #[error("invalid output: {evidence:?}")]
    InvalidOutput { evidence: Box<TypedFailureEvidence> },
    #[error("transient runtime failure: {evidence:?}")]
    TransientRuntime {
        evidence: Box<TypedFailureEvidence>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry_after_ms: Option<u64>,
        /// Engine-resolved wall-clock eligibility for the next automatic
        /// attempt. Adapters leave this unset because policy owns backoff.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        next_eligible_at_ms: Option<i64>,
    },
    #[error("permanent runtime failure: {evidence:?}")]
    PermanentRuntime { evidence: Box<TypedFailureEvidence> },
    #[error("deadline exhausted: {evidence:?}")]
    DeadlineExhausted { evidence: Box<TypedFailureEvidence> },
    #[error("execution interrupted: {evidence:?}")]
    Interrupted { evidence: Box<TypedFailureEvidence> },
    #[error("execution stopped: {evidence:?}")]
    OperatorStopped { evidence: Box<TypedFailureEvidence> },
    #[error("execution aborted: {evidence:?}")]
    OperatorAborted { evidence: Box<TypedFailureEvidence> },
}

impl TypedFailure {
    pub fn invalid(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::InvalidOutput {
            evidence: Box::new(TypedFailureEvidence::new(Some(code.into()), detail)),
        }
    }

    pub fn transient(
        code: impl Into<String>,
        detail: impl Into<String>,
        retry_after_ms: Option<u64>,
    ) -> Self {
        Self::TransientRuntime {
            evidence: Box::new(TypedFailureEvidence::new(Some(code.into()), detail)),
            retry_after_ms,
            next_eligible_at_ms: None,
        }
    }

    pub fn permanent(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::PermanentRuntime {
            evidence: Box::new(TypedFailureEvidence::new(Some(code.into()), detail)),
        }
    }

    pub fn evidence(&self) -> &TypedFailureEvidence {
        match self {
            Self::InvalidOutput { evidence }
            | Self::TransientRuntime { evidence, .. }
            | Self::PermanentRuntime { evidence }
            | Self::DeadlineExhausted { evidence }
            | Self::Interrupted { evidence }
            | Self::OperatorStopped { evidence }
            | Self::OperatorAborted { evidence } => evidence,
        }
    }

    pub fn evidence_mut(&mut self) -> &mut TypedFailureEvidence {
        match self {
            Self::InvalidOutput { evidence }
            | Self::TransientRuntime { evidence, .. }
            | Self::PermanentRuntime { evidence }
            | Self::DeadlineExhausted { evidence }
            | Self::Interrupted { evidence }
            | Self::OperatorStopped { evidence }
            | Self::OperatorAborted { evidence } => evidence,
        }
    }

    pub fn is_transient(&self) -> bool {
        matches!(self, Self::TransientRuntime { .. })
    }

    pub fn is_invalid_output(&self) -> bool {
        matches!(self, Self::InvalidOutput { .. })
    }

    pub fn automatically_retryable(&self) -> bool {
        self.is_transient() || self.is_invalid_output()
    }

    pub fn detail(&self) -> &str {
        &self.evidence().detail
    }

    pub fn retry_after_ms(&self) -> Option<u64> {
        match self {
            Self::TransientRuntime { retry_after_ms, .. } => *retry_after_ms,
            _ => None,
        }
    }

    pub fn next_eligible_at_ms(&self) -> Option<i64> {
        match self {
            Self::TransientRuntime {
                next_eligible_at_ms,
                ..
            } => *next_eligible_at_ms,
            _ => None,
        }
    }

    pub fn set_next_eligible_at_ms(&mut self, value: i64) {
        if let Self::TransientRuntime {
            next_eligible_at_ms,
            ..
        } = self
        {
            *next_eligible_at_ms = Some(value);
        }
    }

    pub fn category(&self) -> &'static str {
        match self {
            Self::InvalidOutput { .. } => "invalid_output",
            Self::TransientRuntime { .. } => "transient_runtime",
            Self::PermanentRuntime { .. } => "permanent_runtime",
            Self::DeadlineExhausted { .. } => "deadline_exhausted",
            Self::Interrupted { .. } => "interrupted",
            Self::OperatorStopped { .. } => "operator_stopped",
            Self::OperatorAborted { .. } => "operator_aborted",
        }
    }

    pub fn projected(mut self) -> Self {
        *self.evidence_mut() = self.evidence().clone().project();
        self
    }
}

pub fn bounded_text(input: &str) -> String {
    if input.len() <= FAILURE_TEXT_LIMIT {
        return input.to_string();
    }
    let keep = FAILURE_TEXT_LIMIT.saturating_sub(TRUNCATION_MARKER.len());
    let mut cut = keep;
    while !input.is_char_boundary(cut) {
        cut = cut.saturating_sub(1);
    }
    format!("{}{}", &input[..cut], TRUNCATION_MARKER)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn taxonomy_round_trips_and_unknown_fields_fail_closed() {
        let values = [
            TypedFailure::invalid("handoff.schema", "bad"),
            TypedFailure::transient("-32000", "busy", Some(25)),
            TypedFailure::permanent("auth", "denied"),
            TypedFailure::DeadlineExhausted {
                evidence: Box::new(TypedFailureEvidence::new(None, "late")),
            },
            TypedFailure::Interrupted {
                evidence: Box::new(TypedFailureEvidence::new(None, "dead")),
            },
            TypedFailure::OperatorStopped {
                evidence: Box::new(TypedFailureEvidence::new(None, "stop")),
            },
            TypedFailure::OperatorAborted {
                evidence: Box::new(TypedFailureEvidence::new(None, "abort")),
            },
        ];
        for value in values {
            let json = serde_json::to_value(&value).unwrap();
            assert_eq!(serde_json::from_value::<TypedFailure>(json).unwrap(), value);
        }
        assert!(serde_json::from_str::<TypedFailure>(
            r#"{"type":"permanent_runtime","evidence":{"detail":"x","unknown":1}}"#
        )
        .is_err());
    }

    #[test]
    fn evidence_projection_is_utf8_safe_and_bounded() {
        let projected = bounded_text(&("a".repeat(FAILURE_TEXT_LIMIT) + "é-tail"));
        assert!(projected.len() <= FAILURE_TEXT_LIMIT);
        assert!(projected.ends_with(TRUNCATION_MARKER));
    }

    #[test]
    fn evidence_projection_bounds_every_provider_controlled_string() {
        let oversized = "x".repeat(FAILURE_TEXT_LIMIT + 1);
        let evidence = TypedFailureEvidence {
            code: Some(oversized.clone()),
            detail: oversized.clone(),
            stop_reason: Some(oversized.clone()),
            stderr: oversized.clone(),
            final_response: oversized.clone(),
            configuration: AppliedRuntimeConfiguration {
                requested_model: Some(oversized.clone()),
                applied_model: Some(oversized.clone()),
                requested_mode: Some(oversized.clone()),
                applied_mode: Some(oversized),
                ..Default::default()
            },
            ..Default::default()
        }
        .project();

        let fields = [
            evidence.code.as_deref().unwrap(),
            &evidence.detail,
            evidence.stop_reason.as_deref().unwrap(),
            &evidence.stderr,
            &evidence.final_response,
            evidence.configuration.requested_model.as_deref().unwrap(),
            evidence.configuration.applied_model.as_deref().unwrap(),
            evidence.configuration.requested_mode.as_deref().unwrap(),
            evidence.configuration.applied_mode.as_deref().unwrap(),
        ];
        assert!(fields.iter().all(|field| field.len() <= FAILURE_TEXT_LIMIT));
    }
}
