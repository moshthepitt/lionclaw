use serde::{Deserialize, Serialize};

use crate::AppliedRuntimeConfiguration;

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
}

impl TypedFailureEvidence {
    pub fn new(code: Option<String>, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: bounded_text(&detail.into()),
            ..Self::default()
        }
    }

    pub fn project(mut self) -> Self {
        self.detail = bounded_text(&self.detail);
        self.stderr = bounded_text(&self.stderr);
        self.final_response = bounded_text(&self.final_response);
        self.stop_reason = self.stop_reason.map(|reason| bounded_text(&reason));
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
}
