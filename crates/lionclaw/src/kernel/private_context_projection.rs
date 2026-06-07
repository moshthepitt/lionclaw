use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::contracts::{SessionHistoryPolicy, TrustTier};

pub(crate) const NOOP_PRIVATE_CONTEXT_PROJECTOR_ID: &str = "noop_private_context_projector";
pub(crate) const PRIVATE_CONTEXT_PROJECTION_MAX_ITEMS: usize = 16;

#[async_trait::async_trait]
pub(crate) trait PrivateContextProjector: Send + Sync {
    fn projector_id(&self) -> &str;

    async fn project(
        &self,
        request: PrivateContextProjectionRequest,
    ) -> Result<PrivateContextProjection, PrivateContextProjectionError>;
}

#[derive(Debug, Default)]
pub(crate) struct NoopPrivateContextProjector;

#[async_trait::async_trait]
impl PrivateContextProjector for NoopPrivateContextProjector {
    fn projector_id(&self) -> &str {
        NOOP_PRIVATE_CONTEXT_PROJECTOR_ID
    }

    async fn project(
        &self,
        request: PrivateContextProjectionRequest,
    ) -> Result<PrivateContextProjection, PrivateContextProjectionError> {
        Ok(PrivateContextProjection {
            request_id: request.request_id,
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
pub(crate) struct PrivateContextProjectionRequest {
    pub request_id: Uuid,
    pub session_id: Uuid,
    pub project_scope: String,
    pub runtime_id: String,
    pub trust_tier: TrustTier,
    pub history_policy: SessionHistoryPolicy,
    pub requested_classes: Vec<PrivateContextClassBudget>,
    pub current_input: Option<PrivateContextCurrentInput>,
    pub sources: Vec<PrivateContextSourceRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PrivateContextClassBudget {
    pub class: ProjectedContextClass,
    pub max_items: usize,
    pub max_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PrivateContextCurrentInput {
    pub text: String,
    pub included_bytes: usize,
    pub original_bytes: usize,
    pub was_capped: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum PrivateContextSourceRef {
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
pub(crate) struct PrivateContextProjection {
    pub request_id: Uuid,
    pub projector_id: String,
    pub items: Vec<ProjectedContextItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProjectedContextItem {
    pub class: ProjectedContextClass,
    pub kind: ProjectedContextItemKind,
    pub text: String,
    pub provenance: Vec<ProjectedContextProvenance>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProjectedContextClass {
    AssistantProfile,
    UserProfile,
    Memory,
}

impl ProjectedContextClass {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::AssistantProfile => "assistant_profile",
            Self::UserProfile => "user_profile",
            Self::Memory => "memory",
        }
    }
}

#[allow(
    dead_code,
    reason = "projected item taxonomy is the stable private context boundary; the production noop projector emits no items yet"
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProjectedContextItemKind {
    StableFact,
    Preference,
    ActiveThread,
    Decision,
    Correction,
    Reminder,
    Other,
}

impl ProjectedContextItemKind {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProjectedContextProvenance {
    pub source: ProjectedContextProvenanceSource,
    pub sequence_no: Option<u64>,
    pub event_id: Option<String>,
}

#[allow(
    dead_code,
    reason = "provenance source variants are part of the projector contract before useful projectors exist"
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProjectedContextProvenanceSource {
    SessionTurn,
    CompactionSummary,
}

#[derive(Debug, Clone)]
pub(crate) struct PrivateContextProjectionError {
    kind: PrivateContextProjectionErrorKind,
    audit_reason: &'static str,
    message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrivateContextProjectionErrorKind {
    ProjectorFailed,
    InvalidOutput,
    Timeout,
}

impl PrivateContextProjectionError {
    #[allow(
        dead_code,
        reason = "future and test projectors can fail; the production noop projector never does"
    )]
    pub(crate) fn failed(message: impl Into<String>) -> Self {
        Self {
            kind: PrivateContextProjectionErrorKind::ProjectorFailed,
            audit_reason: "projector_failed",
            message: message.into(),
        }
    }

    pub(crate) fn timeout(message: impl Into<String>) -> Self {
        Self {
            kind: PrivateContextProjectionErrorKind::Timeout,
            audit_reason: "projector_timeout",
            message: message.into(),
        }
    }

    pub(crate) fn invalid_output(audit_reason: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind: PrivateContextProjectionErrorKind::InvalidOutput,
            audit_reason,
            message: message.into(),
        }
    }

    pub(crate) fn audit_status(&self) -> &'static str {
        match self.kind {
            PrivateContextProjectionErrorKind::ProjectorFailed => "projector_failed",
            PrivateContextProjectionErrorKind::InvalidOutput => "projector_invalid_output",
            PrivateContextProjectionErrorKind::Timeout => "projector_timeout",
        }
    }

    pub(crate) fn audit_reason(&self) -> &'static str {
        self.audit_reason
    }

    pub(crate) fn kind(&self) -> PrivateContextProjectionErrorKind {
        self.kind
    }
}

impl fmt::Display for PrivateContextProjectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for PrivateContextProjectionError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidPrivateContextProjection {
    pub projector_id: String,
    pub items: Vec<ProjectedContextItem>,
    pub classes: Vec<ValidPrivateContextClassProjection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidPrivateContextClassProjection {
    pub class: ProjectedContextClass,
    pub requested_max_items: usize,
    pub requested_max_bytes: usize,
    pub projected_item_count: usize,
    pub accepted_item_count: usize,
    pub projected_bytes: usize,
    pub accepted_bytes: usize,
    pub was_capped: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrivateContextProjectionInvalidReason {
    RequestIdMismatch,
    ProjectorIdEmpty,
    ProjectorIdMismatch,
    UnrequestedClass,
    EmptyText,
    MissingProvenance,
    UnsupportedProvenance,
}

impl PrivateContextProjectionInvalidReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::RequestIdMismatch => "request_id_mismatch",
            Self::ProjectorIdEmpty => "projector_id_empty",
            Self::ProjectorIdMismatch => "projector_id_mismatch",
            Self::UnrequestedClass => "unrequested_class",
            Self::EmptyText => "empty_text",
            Self::MissingProvenance => "missing_provenance",
            Self::UnsupportedProvenance => "unsupported_provenance",
        }
    }
}

pub(crate) fn validate_private_context_projection(
    request: &PrivateContextProjectionRequest,
    expected_projector_id: &str,
    projection: &PrivateContextProjection,
) -> Result<ValidPrivateContextProjection, PrivateContextProjectionInvalidReason> {
    if projection.request_id != request.request_id {
        return Err(PrivateContextProjectionInvalidReason::RequestIdMismatch);
    }
    if projection.projector_id.trim().is_empty() {
        return Err(PrivateContextProjectionInvalidReason::ProjectorIdEmpty);
    }
    if projection.projector_id != expected_projector_id {
        return Err(PrivateContextProjectionInvalidReason::ProjectorIdMismatch);
    }
    let mut classes = request
        .requested_classes
        .iter()
        .map(|budget| ValidPrivateContextClassProjection {
            class: budget.class,
            requested_max_items: budget.max_items,
            requested_max_bytes: budget.max_bytes,
            projected_item_count: 0,
            accepted_item_count: 0,
            projected_bytes: 0,
            accepted_bytes: 0,
            was_capped: false,
        })
        .collect::<Vec<_>>();
    let mut accepted_items = Vec::new();
    for item in &projection.items {
        let Some(class) = classes.iter_mut().find(|class| class.class == item.class) else {
            return Err(PrivateContextProjectionInvalidReason::UnrequestedClass);
        };
        if item.text.trim().is_empty() {
            return Err(PrivateContextProjectionInvalidReason::EmptyText);
        }
        if item.provenance.is_empty() {
            return Err(PrivateContextProjectionInvalidReason::MissingProvenance);
        }
        if !item
            .provenance
            .iter()
            .all(|provenance| provenance_supported(provenance, &request.sources))
        {
            return Err(PrivateContextProjectionInvalidReason::UnsupportedProvenance);
        }
        let item_bytes = item.text.len();
        class.projected_item_count = class.projected_item_count.saturating_add(1);
        class.projected_bytes = class.projected_bytes.saturating_add(item_bytes);

        if class.accepted_item_count >= class.requested_max_items {
            class.was_capped = true;
            continue;
        }
        let remaining_bytes = class
            .requested_max_bytes
            .saturating_sub(class.accepted_bytes);
        if remaining_bytes == 0 {
            class.was_capped = true;
            continue;
        }

        let mut accepted = item.clone();
        if accepted.text.len() > remaining_bytes {
            accepted.text = cap_utf8(&accepted.text, remaining_bytes);
            class.was_capped = true;
        }
        if accepted.text.trim().is_empty() {
            class.was_capped = true;
            continue;
        }
        class.accepted_item_count = class.accepted_item_count.saturating_add(1);
        class.accepted_bytes = class.accepted_bytes.saturating_add(accepted.text.len());
        accepted_items.push(accepted);
    }

    Ok(ValidPrivateContextProjection {
        projector_id: projection.projector_id.clone(),
        items: accepted_items,
        classes,
    })
}

fn cap_utf8(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_string();
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_string()
}

fn provenance_supported(
    provenance: &ProjectedContextProvenance,
    sources: &[PrivateContextSourceRef],
) -> bool {
    if provenance
        .event_id
        .as_ref()
        .is_some_and(|event_id| event_id.trim().is_empty())
    {
        return false;
    }
    match provenance.source {
        ProjectedContextProvenanceSource::SessionTurn => {
            let Some(sequence_no) = provenance.sequence_no else {
                return false;
            };
            sources.iter().any(|source| match source {
                PrivateContextSourceRef::SessionTurnRange {
                    limit,
                    sequence_nos,
                    ..
                } => {
                    *limit > 0
                        && sequence_nos.len() <= *limit
                        && sequence_nos.contains(&sequence_no)
                }
                PrivateContextSourceRef::CompactionSummary { .. } => false,
            })
        }
        ProjectedContextProvenanceSource::CompactionSummary => {
            let Some(sequence_no) = provenance.sequence_no else {
                return false;
            };
            sources.iter().any(|source| match source {
                PrivateContextSourceRef::CompactionSummary {
                    through_sequence_no,
                } => *through_sequence_no == sequence_no,
                PrivateContextSourceRef::SessionTurnRange { .. } => false,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_rejects_request_id_mismatch() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items(
            Uuid::new_v4(),
            "test",
            vec![projected_context_item("remember this")],
        );

        let err = validate_private_context_projection(&request, "test", &projection)
            .expect_err("request id mismatch is invalid");
        assert_eq!(err.as_str(), "request_id_mismatch");
    }

    #[test]
    fn validation_rejects_empty_projector_id() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items(
            request.request_id,
            "",
            vec![projected_context_item("remember this")],
        );

        let err = validate_private_context_projection(&request, "test", &projection)
            .expect_err("empty projector id is invalid");
        assert_eq!(err.as_str(), "projector_id_empty");
    }

    #[test]
    fn validation_rejects_projector_id_mismatch() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items(
            request.request_id,
            "other",
            vec![projected_context_item("remember this")],
        );

        let err = validate_private_context_projection(&request, "test", &projection)
            .expect_err("projector id mismatch is invalid");
        assert_eq!(err.as_str(), "projector_id_mismatch");
    }

    #[test]
    fn validation_caps_items_by_class_budget() {
        let request = PrivateContextProjectionRequest {
            requested_classes: vec![PrivateContextClassBudget {
                class: ProjectedContextClass::Memory,
                max_items: 1,
                max_bytes: 1024,
            }],
            ..request_with_session_turn_source()
        };
        let projection = projection_with_items(
            request.request_id,
            "test",
            vec![
                projected_context_item("first"),
                projected_context_item("second"),
            ],
        );

        let valid = validate_private_context_projection(&request, "test", &projection)
            .expect("too many items are capped");
        assert_eq!(valid.items.len(), 1);
        assert_eq!(valid.items[0].text, "first");
        assert_eq!(valid.classes[0].projected_item_count, 2);
        assert_eq!(valid.classes[0].accepted_item_count, 1);
        assert!(valid.classes[0].was_capped);
    }

    #[test]
    fn validation_caps_item_text_by_class_byte_budget() {
        let request = PrivateContextProjectionRequest {
            requested_classes: vec![PrivateContextClassBudget {
                class: ProjectedContextClass::Memory,
                max_items: PRIVATE_CONTEXT_PROJECTION_MAX_ITEMS,
                max_bytes: 5,
            }],
            ..request_with_session_turn_source()
        };
        let projection = projection_with_items(
            request.request_id,
            "test",
            vec![projected_context_item("too large")],
        );

        let valid = validate_private_context_projection(&request, "test", &projection)
            .expect("over-budget text is capped");
        assert_eq!(valid.items.len(), 1);
        assert_eq!(valid.items[0].text, "too l");
        assert_eq!(valid.classes[0].projected_bytes, "too large".len());
        assert_eq!(valid.classes[0].accepted_bytes, 5);
        assert!(valid.classes[0].was_capped);
    }

    #[test]
    fn validation_rejects_unrequested_class() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items(
            request.request_id,
            "test",
            vec![ProjectedContextItem {
                class: ProjectedContextClass::UserProfile,
                ..projected_context_item("remember this")
            }],
        );

        let err = validate_private_context_projection(&request, "test", &projection)
            .expect_err("unrequested class is invalid");
        assert_eq!(err.as_str(), "unrequested_class");
    }

    #[test]
    fn validation_rejects_empty_candidate_text() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items(
            request.request_id,
            "test",
            vec![projected_context_item(" ")],
        );

        let err = validate_private_context_projection(&request, "test", &projection)
            .expect_err("empty memory text is invalid");
        assert_eq!(err.as_str(), "empty_text");
    }

    #[test]
    fn validation_rejects_missing_provenance() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items(
            request.request_id,
            "test",
            vec![ProjectedContextItem {
                provenance: Vec::new(),
                ..projected_context_item("remember this")
            }],
        );

        let err = validate_private_context_projection(&request, "test", &projection)
            .expect_err("missing provenance is invalid");
        assert_eq!(err.as_str(), "missing_provenance");
    }

    #[test]
    fn validation_rejects_unsupported_provenance() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items(
            request.request_id,
            "test",
            vec![ProjectedContextItem {
                provenance: vec![compaction_provenance(3)],
                ..projected_context_item("remember this")
            }],
        );

        let err = validate_private_context_projection(&request, "test", &projection)
            .expect_err("unsupported provenance is invalid");
        assert_eq!(err.as_str(), "unsupported_provenance");
    }

    #[test]
    fn validation_rejects_session_turn_at_or_after_before_sequence() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items(
            request.request_id,
            "test",
            vec![ProjectedContextItem {
                provenance: vec![session_turn_provenance(10)],
                ..projected_context_item("remember this")
            }],
        );

        let err = validate_private_context_projection(&request, "test", &projection)
            .expect_err("session turn outside the source range is invalid");
        assert_eq!(err.as_str(), "unsupported_provenance");
    }

    #[test]
    fn validation_rejects_session_turn_not_selected_by_source_range() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items(
            request.request_id,
            "test",
            vec![ProjectedContextItem {
                provenance: vec![session_turn_provenance(5)],
                ..projected_context_item("remember this")
            }],
        );

        let err = validate_private_context_projection(&request, "test", &projection)
            .expect_err("session turn outside the selected source records is invalid");
        assert_eq!(err.as_str(), "unsupported_provenance");
    }

    #[test]
    fn validation_rejects_missing_provenance_sequence() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items(
            request.request_id,
            "test",
            vec![ProjectedContextItem {
                provenance: vec![ProjectedContextProvenance {
                    source: ProjectedContextProvenanceSource::SessionTurn,
                    sequence_no: None,
                    event_id: None,
                }],
                ..projected_context_item("remember this")
            }],
        );

        let err = validate_private_context_projection(&request, "test", &projection)
            .expect_err("missing provenance sequence is invalid");
        assert_eq!(err.as_str(), "unsupported_provenance");
    }

    #[test]
    fn validation_rejects_blank_provenance_event_id() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items(
            request.request_id,
            "test",
            vec![ProjectedContextItem {
                provenance: vec![ProjectedContextProvenance {
                    event_id: Some(" ".to_string()),
                    ..session_turn_provenance(1)
                }],
                ..projected_context_item("remember this")
            }],
        );

        let err = validate_private_context_projection(&request, "test", &projection)
            .expect_err("blank provenance event id is invalid");
        assert_eq!(err.as_str(), "unsupported_provenance");
    }

    #[test]
    fn validation_accepts_supported_session_turn_provenance() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items(
            request.request_id,
            "test",
            vec![projected_context_item("remember this")],
        );

        let valid = validate_private_context_projection(&request, "test", &projection)
            .expect("supported session turn provenance is valid");
        assert_eq!(valid.projector_id, "test");
        assert_eq!(valid.items.len(), 1);
        assert_eq!(valid.classes[0].accepted_item_count, 1);
        assert_eq!(valid.classes[0].projected_bytes, "remember this".len());
    }

    #[test]
    fn validation_accepts_supported_compaction_provenance() {
        let request = PrivateContextProjectionRequest {
            sources: vec![PrivateContextSourceRef::CompactionSummary {
                through_sequence_no: 7,
            }],
            ..request_with_session_turn_source()
        };
        let projection = projection_with_items(
            request.request_id,
            "test",
            vec![ProjectedContextItem {
                provenance: vec![compaction_provenance(7)],
                ..projected_context_item("remember this")
            }],
        );

        let valid = validate_private_context_projection(&request, "test", &projection)
            .expect("supported compaction provenance is valid");
        assert_eq!(valid.projector_id, "test");
        assert_eq!(valid.items.len(), 1);
        assert_eq!(valid.classes[0].accepted_item_count, 1);
        assert_eq!(valid.classes[0].projected_bytes, "remember this".len());
    }

    #[test]
    fn request_serializes_as_jsonl_protocol_shape() {
        let request = request_with_session_turn_source();

        let encoded = serde_json::to_value(&request).expect("serialize request");

        assert_eq!(encoded["request_id"], request.request_id.to_string());
        assert_eq!(encoded["runtime_id"], "mock");
        assert_eq!(encoded["project_scope"], "project:test");
        assert_eq!(encoded["trust_tier"], "main");
        assert_eq!(encoded["history_policy"], "interactive");
        assert_eq!(encoded["requested_classes"][0]["class"], "memory");
        assert_eq!(encoded["sources"][0]["kind"], "session_turn_range");
        assert_eq!(
            encoded["sources"][0]["sequence_nos"],
            serde_json::json!([6, 7, 8, 9])
        );
    }

    #[test]
    fn response_deserializes_jsonl_protocol_shape() {
        let decoded: PrivateContextProjection = serde_json::from_str(
            r#"{"request_id":"11111111-1111-1111-1111-111111111111","projector_id":"private-context-core","items":[{"class":"memory","kind":"stable_fact","text":"User prefers concise summaries.","provenance":[{"source":"session_turn","sequence_no":7,"event_id":null}]}],"ignored":true}"#,
        )
        .expect("decode response");

        assert_eq!(
            decoded.request_id,
            Uuid::parse_str("11111111-1111-1111-1111-111111111111").expect("uuid")
        );
        assert_eq!(decoded.projector_id, "private-context-core");
        assert_eq!(decoded.items[0].kind, ProjectedContextItemKind::StableFact);
        assert_eq!(
            decoded.items[0].provenance[0].source,
            ProjectedContextProvenanceSource::SessionTurn
        );
    }

    fn projection_with_items(
        request_id: Uuid,
        projector_id: impl Into<String>,
        items: Vec<ProjectedContextItem>,
    ) -> PrivateContextProjection {
        PrivateContextProjection {
            request_id,
            projector_id: projector_id.into(),
            items,
        }
    }

    fn projected_context_item(text: impl Into<String>) -> ProjectedContextItem {
        ProjectedContextItem {
            class: ProjectedContextClass::Memory,
            kind: ProjectedContextItemKind::StableFact,
            text: text.into(),
            provenance: vec![session_turn_provenance(7)],
        }
    }

    fn request_with_session_turn_source() -> PrivateContextProjectionRequest {
        PrivateContextProjectionRequest {
            request_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            project_scope: "project:test".to_string(),
            runtime_id: "mock".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: SessionHistoryPolicy::Interactive,
            requested_classes: vec![PrivateContextClassBudget {
                class: ProjectedContextClass::Memory,
                max_items: PRIVATE_CONTEXT_PROJECTION_MAX_ITEMS,
                max_bytes: 1024,
            }],
            current_input: None,
            sources: vec![PrivateContextSourceRef::SessionTurnRange {
                before_sequence_no: Some(10),
                limit: 4,
                sequence_nos: vec![6, 7, 8, 9],
            }],
        }
    }

    fn session_turn_provenance(sequence_no: u64) -> ProjectedContextProvenance {
        ProjectedContextProvenance {
            source: ProjectedContextProvenanceSource::SessionTurn,
            sequence_no: Some(sequence_no),
            event_id: None,
        }
    }

    fn compaction_provenance(sequence_no: u64) -> ProjectedContextProvenance {
        ProjectedContextProvenance {
            source: ProjectedContextProvenanceSource::CompactionSummary,
            sequence_no: Some(sequence_no),
            event_id: None,
        }
    }
}
