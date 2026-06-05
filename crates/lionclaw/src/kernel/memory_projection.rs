use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::contracts::{SessionHistoryPolicy, TrustTier};

pub(crate) const NOOP_MEMORY_PROJECTOR_ID: &str = "noop_memory_projector";
pub(crate) const MEMORY_PROJECTION_MAX_ITEMS: usize = 16;

#[async_trait::async_trait]
pub(crate) trait MemoryProjector: Send + Sync {
    fn projector_id(&self) -> &str;

    async fn project(
        &self,
        request: MemoryProjectionRequest,
    ) -> Result<MemoryProjection, MemoryProjectionError>;
}

#[derive(Debug, Default)]
pub(crate) struct NoopMemoryProjector;

#[async_trait::async_trait]
impl MemoryProjector for NoopMemoryProjector {
    fn projector_id(&self) -> &str {
        NOOP_MEMORY_PROJECTOR_ID
    }

    async fn project(
        &self,
        _request: MemoryProjectionRequest,
    ) -> Result<MemoryProjection, MemoryProjectionError> {
        Ok(MemoryProjection {
            projector_id: self.projector_id().to_string(),
            items: Vec::new(),
        })
    }
}

#[allow(
    dead_code,
    reason = "projector implementations consume request metadata; the production noop projector intentionally ignores it"
)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MemoryProjectionRequest {
    pub session_id: Uuid,
    pub runtime_id: String,
    pub trust_tier: TrustTier,
    pub history_policy: SessionHistoryPolicy,
    pub max_items: usize,
    pub max_bytes: usize,
    pub sources: Vec<MemorySourceRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum MemorySourceRef {
    SessionTurnRange {
        before_sequence_no: Option<u64>,
        limit: usize,
        sequence_nos: Vec<u64>,
    },
    CompactionSummary {
        through_sequence_no: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MemoryProjection {
    pub projector_id: String,
    pub items: Vec<MemoryCandidate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MemoryCandidate {
    pub kind: MemoryCandidateKind,
    pub text: String,
    pub provenance: Vec<MemoryProvenance>,
}

#[allow(
    dead_code,
    reason = "candidate taxonomy is the stable memory boundary; the production noop projector emits no candidates yet"
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MemoryCandidateKind {
    StableFact,
    Preference,
    ActiveThread,
    Decision,
    Correction,
    Reminder,
    Other,
}

impl MemoryCandidateKind {
    pub(crate) fn title(self) -> &'static str {
        match self {
            Self::StableFact => "Stable fact",
            Self::Preference => "Preference",
            Self::ActiveThread => "Active thread",
            Self::Decision => "Decision",
            Self::Correction => "Correction",
            Self::Reminder => "Reminder",
            Self::Other => "Other",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MemoryProvenance {
    pub source: MemoryProvenanceSource,
    pub sequence_no: Option<u64>,
    pub event_id: Option<String>,
}

#[allow(
    dead_code,
    reason = "provenance source variants are part of the projector contract before useful projectors exist"
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MemoryProvenanceSource {
    SessionTurn,
    CompactionSummary,
}

#[derive(Debug, Clone)]
pub(crate) struct MemoryProjectionError {
    kind: MemoryProjectionErrorKind,
    audit_reason: &'static str,
    message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemoryProjectionErrorKind {
    ProjectorFailed,
    InvalidOutput,
}

impl MemoryProjectionError {
    #[allow(
        dead_code,
        reason = "future and test projectors can fail; the production noop projector never does"
    )]
    pub(crate) fn failed(message: impl Into<String>) -> Self {
        Self {
            kind: MemoryProjectionErrorKind::ProjectorFailed,
            audit_reason: "projector_failed",
            message: message.into(),
        }
    }

    pub(crate) fn invalid_output(audit_reason: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind: MemoryProjectionErrorKind::InvalidOutput,
            audit_reason,
            message: message.into(),
        }
    }

    pub(crate) fn audit_status(&self) -> &'static str {
        match self.kind {
            MemoryProjectionErrorKind::ProjectorFailed => "projector_failed",
            MemoryProjectionErrorKind::InvalidOutput => "projector_invalid_output",
        }
    }

    pub(crate) fn audit_reason(&self) -> &'static str {
        self.audit_reason
    }

    pub(crate) fn kind(&self) -> MemoryProjectionErrorKind {
        self.kind
    }
}

impl fmt::Display for MemoryProjectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for MemoryProjectionError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidMemoryProjection {
    pub projector_id: String,
    pub item_count: usize,
    pub projected_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemoryProjectionInvalidReason {
    ProjectorIdEmpty,
    ProjectorIdMismatch,
    TooManyItems,
    TooManyBytes,
    EmptyText,
    MissingProvenance,
    UnsupportedProvenance,
}

impl MemoryProjectionInvalidReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ProjectorIdEmpty => "projector_id_empty",
            Self::ProjectorIdMismatch => "projector_id_mismatch",
            Self::TooManyItems => "too_many_items",
            Self::TooManyBytes => "too_many_bytes",
            Self::EmptyText => "empty_text",
            Self::MissingProvenance => "missing_provenance",
            Self::UnsupportedProvenance => "unsupported_provenance",
        }
    }
}

pub(crate) fn validate_memory_projection(
    request: &MemoryProjectionRequest,
    expected_projector_id: &str,
    projection: &MemoryProjection,
) -> Result<ValidMemoryProjection, MemoryProjectionInvalidReason> {
    if projection.projector_id.trim().is_empty() {
        return Err(MemoryProjectionInvalidReason::ProjectorIdEmpty);
    }
    if projection.projector_id != expected_projector_id {
        return Err(MemoryProjectionInvalidReason::ProjectorIdMismatch);
    }
    if projection.items.len() > request.max_items {
        return Err(MemoryProjectionInvalidReason::TooManyItems);
    }

    let mut projected_bytes = 0usize;
    for item in &projection.items {
        if item.text.trim().is_empty() {
            return Err(MemoryProjectionInvalidReason::EmptyText);
        }
        if item.provenance.is_empty() {
            return Err(MemoryProjectionInvalidReason::MissingProvenance);
        }
        if !item
            .provenance
            .iter()
            .all(|provenance| provenance_supported(provenance, &request.sources))
        {
            return Err(MemoryProjectionInvalidReason::UnsupportedProvenance);
        }
        let item_bytes = item.text.len();
        if item_bytes > request.max_bytes.saturating_sub(projected_bytes) {
            return Err(MemoryProjectionInvalidReason::TooManyBytes);
        }
        projected_bytes += item_bytes;
    }

    Ok(ValidMemoryProjection {
        projector_id: projection.projector_id.clone(),
        item_count: projection.items.len(),
        projected_bytes,
    })
}

fn provenance_supported(provenance: &MemoryProvenance, sources: &[MemorySourceRef]) -> bool {
    if provenance
        .event_id
        .as_ref()
        .is_some_and(|event_id| event_id.trim().is_empty())
    {
        return false;
    }
    match provenance.source {
        MemoryProvenanceSource::SessionTurn => {
            let Some(sequence_no) = provenance.sequence_no else {
                return false;
            };
            sources.iter().any(|source| match source {
                MemorySourceRef::SessionTurnRange {
                    limit,
                    sequence_nos,
                    ..
                } => {
                    *limit > 0
                        && sequence_nos.len() <= *limit
                        && sequence_nos.contains(&sequence_no)
                }
                MemorySourceRef::CompactionSummary { .. } => false,
            })
        }
        MemoryProvenanceSource::CompactionSummary => {
            let Some(sequence_no) = provenance.sequence_no else {
                return false;
            };
            sources.iter().any(|source| match source {
                MemorySourceRef::CompactionSummary {
                    through_sequence_no,
                } => *through_sequence_no == sequence_no,
                MemorySourceRef::SessionTurnRange { .. } => false,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_rejects_empty_projector_id() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items("", vec![memory_candidate("remember this")]);

        let err = validate_memory_projection(&request, "test", &projection)
            .expect_err("empty projector id is invalid");
        assert_eq!(err.as_str(), "projector_id_empty");
    }

    #[test]
    fn validation_rejects_projector_id_mismatch() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items("other", vec![memory_candidate("remember this")]);

        let err = validate_memory_projection(&request, "test", &projection)
            .expect_err("projector id mismatch is invalid");
        assert_eq!(err.as_str(), "projector_id_mismatch");
    }

    #[test]
    fn validation_rejects_too_many_items() {
        let request = MemoryProjectionRequest {
            max_items: 1,
            ..request_with_session_turn_source()
        };
        let projection = projection_with_items(
            "test",
            vec![memory_candidate("first"), memory_candidate("second")],
        );

        let err = validate_memory_projection(&request, "test", &projection)
            .expect_err("too many items are invalid");
        assert_eq!(err.as_str(), "too_many_items");
    }

    #[test]
    fn validation_rejects_too_many_bytes() {
        let request = MemoryProjectionRequest {
            max_bytes: 5,
            ..request_with_session_turn_source()
        };
        let projection = projection_with_items("test", vec![memory_candidate("too large")]);

        let err = validate_memory_projection(&request, "test", &projection)
            .expect_err("over-budget memory text is invalid");
        assert_eq!(err.as_str(), "too_many_bytes");
    }

    #[test]
    fn validation_rejects_empty_candidate_text() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items("test", vec![memory_candidate(" ")]);

        let err = validate_memory_projection(&request, "test", &projection)
            .expect_err("empty memory text is invalid");
        assert_eq!(err.as_str(), "empty_text");
    }

    #[test]
    fn validation_rejects_missing_provenance() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items(
            "test",
            vec![MemoryCandidate {
                provenance: Vec::new(),
                ..memory_candidate("remember this")
            }],
        );

        let err = validate_memory_projection(&request, "test", &projection)
            .expect_err("missing provenance is invalid");
        assert_eq!(err.as_str(), "missing_provenance");
    }

    #[test]
    fn validation_rejects_unsupported_provenance() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items(
            "test",
            vec![MemoryCandidate {
                provenance: vec![compaction_provenance(3)],
                ..memory_candidate("remember this")
            }],
        );

        let err = validate_memory_projection(&request, "test", &projection)
            .expect_err("unsupported provenance is invalid");
        assert_eq!(err.as_str(), "unsupported_provenance");
    }

    #[test]
    fn validation_rejects_session_turn_at_or_after_before_sequence() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items(
            "test",
            vec![MemoryCandidate {
                provenance: vec![session_turn_provenance(10)],
                ..memory_candidate("remember this")
            }],
        );

        let err = validate_memory_projection(&request, "test", &projection)
            .expect_err("session turn outside the source range is invalid");
        assert_eq!(err.as_str(), "unsupported_provenance");
    }

    #[test]
    fn validation_rejects_session_turn_not_selected_by_source_range() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items(
            "test",
            vec![MemoryCandidate {
                provenance: vec![session_turn_provenance(5)],
                ..memory_candidate("remember this")
            }],
        );

        let err = validate_memory_projection(&request, "test", &projection)
            .expect_err("session turn outside the selected source records is invalid");
        assert_eq!(err.as_str(), "unsupported_provenance");
    }

    #[test]
    fn validation_rejects_missing_provenance_sequence() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items(
            "test",
            vec![MemoryCandidate {
                provenance: vec![MemoryProvenance {
                    source: MemoryProvenanceSource::SessionTurn,
                    sequence_no: None,
                    event_id: None,
                }],
                ..memory_candidate("remember this")
            }],
        );

        let err = validate_memory_projection(&request, "test", &projection)
            .expect_err("missing provenance sequence is invalid");
        assert_eq!(err.as_str(), "unsupported_provenance");
    }

    #[test]
    fn validation_rejects_blank_provenance_event_id() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items(
            "test",
            vec![MemoryCandidate {
                provenance: vec![MemoryProvenance {
                    event_id: Some(" ".to_string()),
                    ..session_turn_provenance(1)
                }],
                ..memory_candidate("remember this")
            }],
        );

        let err = validate_memory_projection(&request, "test", &projection)
            .expect_err("blank provenance event id is invalid");
        assert_eq!(err.as_str(), "unsupported_provenance");
    }

    #[test]
    fn validation_accepts_supported_session_turn_provenance() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items("test", vec![memory_candidate("remember this")]);

        let valid = validate_memory_projection(&request, "test", &projection)
            .expect("supported session turn provenance is valid");
        assert_eq!(valid.projector_id, "test");
        assert_eq!(valid.item_count, 1);
        assert_eq!(valid.projected_bytes, "remember this".len());
    }

    #[test]
    fn validation_accepts_supported_compaction_provenance() {
        let request = MemoryProjectionRequest {
            sources: vec![MemorySourceRef::CompactionSummary {
                through_sequence_no: 7,
            }],
            ..request_with_session_turn_source()
        };
        let projection = projection_with_items(
            "test",
            vec![MemoryCandidate {
                provenance: vec![compaction_provenance(7)],
                ..memory_candidate("remember this")
            }],
        );

        let valid = validate_memory_projection(&request, "test", &projection)
            .expect("supported compaction provenance is valid");
        assert_eq!(valid.projector_id, "test");
        assert_eq!(valid.item_count, 1);
        assert_eq!(valid.projected_bytes, "remember this".len());
    }

    #[test]
    fn request_serializes_as_jsonl_protocol_shape() {
        let request = request_with_session_turn_source();

        let encoded = serde_json::to_value(&request).expect("serialize request");

        assert_eq!(encoded["runtime_id"], "mock");
        assert_eq!(encoded["trust_tier"], "main");
        assert_eq!(encoded["history_policy"], "interactive");
        assert_eq!(encoded["sources"][0]["kind"], "session_turn_range");
        assert_eq!(
            encoded["sources"][0]["sequence_nos"],
            serde_json::json!([6, 7, 8, 9])
        );
    }

    #[test]
    fn response_deserializes_jsonl_protocol_shape() {
        let decoded: MemoryProjection = serde_json::from_str(
            r#"{"projector_id":"memory-core","items":[{"kind":"stable_fact","text":"User prefers concise summaries.","provenance":[{"source":"session_turn","sequence_no":7,"event_id":null}]}],"ignored":true}"#,
        )
        .expect("decode response");

        assert_eq!(decoded.projector_id, "memory-core");
        assert_eq!(decoded.items[0].kind, MemoryCandidateKind::StableFact);
        assert_eq!(
            decoded.items[0].provenance[0].source,
            MemoryProvenanceSource::SessionTurn
        );
    }

    fn projection_with_items(
        projector_id: impl Into<String>,
        items: Vec<MemoryCandidate>,
    ) -> MemoryProjection {
        MemoryProjection {
            projector_id: projector_id.into(),
            items,
        }
    }

    fn memory_candidate(text: impl Into<String>) -> MemoryCandidate {
        MemoryCandidate {
            kind: MemoryCandidateKind::StableFact,
            text: text.into(),
            provenance: vec![session_turn_provenance(7)],
        }
    }

    fn request_with_session_turn_source() -> MemoryProjectionRequest {
        MemoryProjectionRequest {
            session_id: Uuid::new_v4(),
            runtime_id: "mock".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: SessionHistoryPolicy::Interactive,
            max_items: MEMORY_PROJECTION_MAX_ITEMS,
            max_bytes: 1024,
            sources: vec![MemorySourceRef::SessionTurnRange {
                before_sequence_no: Some(10),
                limit: 4,
                sequence_nos: vec![6, 7, 8, 9],
            }],
        }
    }

    fn session_turn_provenance(sequence_no: u64) -> MemoryProvenance {
        MemoryProvenance {
            source: MemoryProvenanceSource::SessionTurn,
            sequence_no: Some(sequence_no),
            event_id: None,
        }
    }

    fn compaction_provenance(sequence_no: u64) -> MemoryProvenance {
        MemoryProvenance {
            source: MemoryProvenanceSource::CompactionSummary,
            sequence_no: Some(sequence_no),
            event_id: None,
        }
    }
}
