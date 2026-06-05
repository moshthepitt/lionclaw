use std::{error::Error, fmt};

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
#[derive(Debug, Clone)]
pub(crate) struct MemoryProjectionRequest {
    pub session_id: Uuid,
    pub runtime_id: String,
    pub trust_tier: TrustTier,
    pub history_policy: SessionHistoryPolicy,
    pub max_items: usize,
    pub max_bytes: usize,
    pub sources: Vec<MemorySourceRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MemorySourceRef {
    SessionTurnRange {
        before_sequence_no: Option<u64>,
        limit: usize,
    },
    CompactionSummary {
        through_sequence_no: u64,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct MemoryProjection {
    pub projector_id: String,
    pub items: Vec<MemoryCandidate>,
}

#[derive(Debug, Clone)]
pub(crate) struct MemoryCandidate {
    pub kind: MemoryCandidateKind,
    pub text: String,
    pub provenance: Vec<MemoryProvenance>,
}

#[allow(
    dead_code,
    reason = "candidate taxonomy is the stable memory boundary; the production noop projector emits no candidates yet"
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone)]
pub(crate) struct MemoryProvenance {
    pub source: MemoryProvenanceSource,
    pub sequence_no: Option<u64>,
    pub event_id: Option<String>,
}

#[allow(
    dead_code,
    reason = "provenance source variants are part of the projector contract before useful projectors exist"
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemoryProvenanceSource {
    SessionTurn,
    CompactionSummary,
}

#[derive(Debug, Clone)]
pub(crate) struct MemoryProjectionError {
    message: String,
}

impl MemoryProjectionError {
    #[allow(
        dead_code,
        reason = "future and test projectors can fail; the production noop projector never does"
    )]
    pub(crate) fn failed(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
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
        projected_bytes = projected_bytes.saturating_add(item.text.len());
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
                    before_sequence_no,
                    limit,
                } => {
                    *limit > 0
                        && before_sequence_no
                            .map(|before_sequence_no| sequence_no < before_sequence_no)
                            .unwrap_or(true)
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
    fn validation_rejects_empty_candidate_text() {
        let request = request_with_session_turn_source();
        let projection = MemoryProjection {
            projector_id: "test".to_string(),
            items: vec![MemoryCandidate {
                kind: MemoryCandidateKind::StableFact,
                text: " ".to_string(),
                provenance: vec![session_turn_provenance(1)],
            }],
        };

        let err = validate_memory_projection(&request, "test", &projection)
            .expect_err("empty memory text is invalid");
        assert_eq!(err.as_str(), "empty_text");
    }

    #[test]
    fn validation_rejects_missing_provenance() {
        let request = request_with_session_turn_source();
        let projection = MemoryProjection {
            projector_id: "test".to_string(),
            items: vec![MemoryCandidate {
                kind: MemoryCandidateKind::StableFact,
                text: "remember this".to_string(),
                provenance: Vec::new(),
            }],
        };

        let err = validate_memory_projection(&request, "test", &projection)
            .expect_err("missing provenance is invalid");
        assert_eq!(err.as_str(), "missing_provenance");
    }

    #[test]
    fn validation_rejects_unsupported_provenance() {
        let request = request_with_session_turn_source();
        let projection = MemoryProjection {
            projector_id: "test".to_string(),
            items: vec![MemoryCandidate {
                kind: MemoryCandidateKind::StableFact,
                text: "remember this".to_string(),
                provenance: vec![MemoryProvenance {
                    source: MemoryProvenanceSource::CompactionSummary,
                    sequence_no: Some(3),
                    event_id: None,
                }],
            }],
        };

        let err = validate_memory_projection(&request, "test", &projection)
            .expect_err("unsupported provenance is invalid");
        assert_eq!(err.as_str(), "unsupported_provenance");
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
}
