use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    contracts::{SessionHistoryPolicy, TrustTier},
    kernel::prompt_context::PromptContextMode,
};

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
    pub runtime_id: String,
    pub trust_tier: TrustTier,
    pub history_policy: SessionHistoryPolicy,
    pub surface: PromptContextMode,
    pub project_scope: Option<String>,
    pub current_input: Option<String>,
    pub budgets: Vec<ProjectedContextBudget>,
    pub sources: Vec<PrivateContextSourceRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProjectedContextBudget {
    pub class: ProjectedContextClass,
    pub max_items: usize,
    pub max_bytes: usize,
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

    pub(crate) const ALL: [Self; 3] = [Self::AssistantProfile, Self::UserProfile, Self::Memory];

    fn sort_index(self) -> usize {
        match self {
            Self::AssistantProfile => 0,
            Self::UserProfile => 1,
            Self::Memory => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProjectedContextProvenance {
    pub source: ProjectedContextProvenanceSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence_no: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projector_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
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
    ProjectorRecord,
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
    pub dropped_item_count: usize,
    pub projected_bytes: usize,
    pub accepted_bytes: usize,
    pub was_capped: bool,
    pub byte_budget_capped: bool,
    pub item_count_capped: bool,
    pub status: &'static str,
    pub reason: Option<PrivateContextProjectionInvalidReason>,
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
    InvalidProjectorRecord,
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
            Self::InvalidProjectorRecord => "invalid_projector_record",
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
        .budgets
        .iter()
        .map(class_projection_from_budget)
        .collect::<Vec<_>>();
    let mut accepted_items = Vec::new();

    for class in ProjectedContextClass::ALL {
        let projected_items = projection
            .items
            .iter()
            .filter(|item| item.class == class)
            .collect::<Vec<_>>();
        if projected_items.is_empty() {
            continue;
        }

        let Some(summary) = classes.iter_mut().find(|summary| summary.class == class) else {
            classes.push(unrequested_class_projection(class, &projected_items));
            continue;
        };

        validate_requested_class_projection(
            summary,
            &projected_items,
            expected_projector_id,
            &request.sources,
            &mut accepted_items,
        );
    }
    classes.sort_by_key(|class| class.class.sort_index());

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

fn class_projection_from_budget(
    budget: &ProjectedContextBudget,
) -> ValidPrivateContextClassProjection {
    ValidPrivateContextClassProjection {
        class: budget.class,
        requested_max_items: budget.max_items,
        requested_max_bytes: budget.max_bytes,
        projected_item_count: 0,
        accepted_item_count: 0,
        dropped_item_count: 0,
        projected_bytes: 0,
        accepted_bytes: 0,
        was_capped: false,
        byte_budget_capped: false,
        item_count_capped: false,
        status: "projector_returned_no_items",
        reason: None,
    }
}

fn unrequested_class_projection(
    class: ProjectedContextClass,
    items: &[&ProjectedContextItem],
) -> ValidPrivateContextClassProjection {
    ValidPrivateContextClassProjection {
        class,
        requested_max_items: 0,
        requested_max_bytes: 0,
        projected_item_count: items.len(),
        accepted_item_count: 0,
        dropped_item_count: items.len(),
        projected_bytes: items
            .iter()
            .fold(0usize, |total, item| total.saturating_add(item.text.len())),
        accepted_bytes: 0,
        was_capped: false,
        byte_budget_capped: false,
        item_count_capped: false,
        status: "omitted",
        reason: Some(PrivateContextProjectionInvalidReason::UnrequestedClass),
    }
}

fn validate_requested_class_projection(
    summary: &mut ValidPrivateContextClassProjection,
    items: &[&ProjectedContextItem],
    expected_projector_id: &str,
    sources: &[PrivateContextSourceRef],
    accepted_items: &mut Vec<ProjectedContextItem>,
) {
    summary.projected_item_count = items.len();
    summary.projected_bytes = items
        .iter()
        .fold(0usize, |total, item| total.saturating_add(item.text.len()));

    if let Some(reason) = items
        .iter()
        .find_map(|item| semantic_invalid_reason(item, expected_projector_id, sources))
    {
        summary.status = "omitted";
        summary.reason = Some(reason);
        summary.dropped_item_count = items.len();
        return;
    }

    for item in items {
        if summary.accepted_item_count >= summary.requested_max_items {
            summary.item_count_capped = true;
            summary.was_capped = true;
            summary.dropped_item_count = summary.dropped_item_count.saturating_add(1);
            continue;
        }

        let remaining_bytes = summary
            .requested_max_bytes
            .saturating_sub(summary.accepted_bytes);
        if remaining_bytes == 0 {
            summary.byte_budget_capped = true;
            summary.was_capped = true;
            summary.dropped_item_count = summary.dropped_item_count.saturating_add(1);
            continue;
        }

        let mut accepted = (*item).clone();
        if accepted.text.len() > remaining_bytes {
            accepted.text = cap_utf8(&accepted.text, remaining_bytes);
            summary.byte_budget_capped = true;
            summary.was_capped = true;
        }
        if accepted.text.trim().is_empty() {
            summary.byte_budget_capped = true;
            summary.was_capped = true;
            summary.dropped_item_count = summary.dropped_item_count.saturating_add(1);
            continue;
        }

        summary.accepted_item_count = summary.accepted_item_count.saturating_add(1);
        summary.accepted_bytes = summary.accepted_bytes.saturating_add(accepted.text.len());
        accepted_items.push(accepted);
    }

    if summary.accepted_item_count > 0 {
        summary.status = "included";
    } else if summary.projected_item_count > 0 && summary.byte_budget_capped {
        summary.status = "byte_budget_capped";
    }
}

fn semantic_invalid_reason(
    item: &ProjectedContextItem,
    expected_projector_id: &str,
    sources: &[PrivateContextSourceRef],
) -> Option<PrivateContextProjectionInvalidReason> {
    if item.text.trim().is_empty() {
        return Some(PrivateContextProjectionInvalidReason::EmptyText);
    }
    if item.provenance.is_empty() {
        return Some(PrivateContextProjectionInvalidReason::MissingProvenance);
    }
    for provenance in &item.provenance {
        if !provenance_supported(provenance, expected_projector_id, sources) {
            return Some(match provenance.source {
                ProjectedContextProvenanceSource::ProjectorRecord => {
                    PrivateContextProjectionInvalidReason::InvalidProjectorRecord
                }
                ProjectedContextProvenanceSource::SessionTurn
                | ProjectedContextProvenanceSource::CompactionSummary => {
                    PrivateContextProjectionInvalidReason::UnsupportedProvenance
                }
            });
        }
    }
    None
}

fn provenance_supported(
    provenance: &ProjectedContextProvenance,
    expected_projector_id: &str,
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
        ProjectedContextProvenanceSource::ProjectorRecord => {
            provenance
                .projector_id
                .as_deref()
                .is_some_and(|projector_id| projector_id == expected_projector_id)
                && provenance
                    .record_id
                    .as_deref()
                    .is_some_and(valid_projector_record_handle)
                && provenance
                    .revision
                    .as_deref()
                    .is_none_or(valid_projector_record_handle)
        }
    }
}

fn valid_projector_record_handle(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| {
            byte.is_ascii_graphic() && !byte.is_ascii_whitespace() && byte != b'/' && byte != b'\\'
        })
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
            budgets: vec![ProjectedContextBudget {
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
        assert!(valid.classes[0].item_count_capped);
        assert_eq!(valid.classes[0].dropped_item_count, 1);
    }

    #[test]
    fn validation_caps_item_text_by_class_byte_budget() {
        let request = PrivateContextProjectionRequest {
            budgets: vec![ProjectedContextBudget {
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
        assert!(valid.classes[0].byte_budget_capped);
    }

    #[test]
    fn validation_omits_unrequested_class_without_failing_projection() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items(
            request.request_id,
            "test",
            vec![ProjectedContextItem {
                class: ProjectedContextClass::UserProfile,
                ..projected_context_item("remember this")
            }],
        );

        let valid = validate_private_context_projection(&request, "test", &projection)
            .expect("unrequested class is class-scoped");
        assert!(valid.items.is_empty());
        let user_profile = valid
            .classes
            .iter()
            .find(|class| class.class == ProjectedContextClass::UserProfile)
            .expect("user profile audit");
        assert_eq!(user_profile.status, "omitted");
        assert_eq!(
            user_profile.reason.map(|reason| reason.as_str()),
            Some("unrequested_class")
        );
    }

    #[test]
    fn validation_omits_only_semantically_invalid_class() {
        let request = request_with_all_class_budgets();
        let projection = projection_with_items(
            request.request_id,
            "test",
            vec![
                ProjectedContextItem {
                    class: ProjectedContextClass::AssistantProfile,
                    ..projected_context_item("assistant ok")
                },
                ProjectedContextItem {
                    class: ProjectedContextClass::UserProfile,
                    ..projected_context_item(" ")
                },
                ProjectedContextItem {
                    class: ProjectedContextClass::Memory,
                    ..projected_context_item("memory ok")
                },
            ],
        );

        let valid = validate_private_context_projection(&request, "test", &projection)
            .expect("semantic failure is class-scoped");
        assert_eq!(valid.items.len(), 2);
        assert!(valid.items.iter().any(|item| {
            item.class == ProjectedContextClass::AssistantProfile && item.text == "assistant ok"
        }));
        assert!(valid.items.iter().any(|item| {
            item.class == ProjectedContextClass::Memory && item.text == "memory ok"
        }));
        let user_profile = valid
            .classes
            .iter()
            .find(|class| class.class == ProjectedContextClass::UserProfile)
            .expect("user profile audit");
        assert_eq!(user_profile.status, "omitted");
        assert_eq!(
            user_profile.reason.map(|reason| reason.as_str()),
            Some("empty_text")
        );
    }

    #[test]
    fn validation_omits_class_with_missing_provenance() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items(
            request.request_id,
            "test",
            vec![ProjectedContextItem {
                provenance: Vec::new(),
                ..projected_context_item("remember this")
            }],
        );

        let valid = validate_private_context_projection(&request, "test", &projection)
            .expect("missing provenance is class-scoped");
        assert!(valid.items.is_empty());
        assert_eq!(
            valid.classes[0].reason.map(|reason| reason.as_str()),
            Some("missing_provenance")
        );
    }

    #[test]
    fn validation_omits_class_with_unsupported_provenance() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items(
            request.request_id,
            "test",
            vec![ProjectedContextItem {
                provenance: vec![compaction_provenance(3)],
                ..projected_context_item("remember this")
            }],
        );

        let valid = validate_private_context_projection(&request, "test", &projection)
            .expect("unsupported provenance is class-scoped");
        assert!(valid.items.is_empty());
        assert_eq!(
            valid.classes[0].reason.map(|reason| reason.as_str()),
            Some("unsupported_provenance")
        );
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

        let valid = validate_private_context_projection(&request, "test", &projection)
            .expect("session turn outside source range is class-scoped");
        assert!(valid.items.is_empty());
        assert_eq!(
            valid.classes[0].reason.map(|reason| reason.as_str()),
            Some("unsupported_provenance")
        );
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

        let valid = validate_private_context_projection(&request, "test", &projection)
            .expect("session turn outside selected records is class-scoped");
        assert!(valid.items.is_empty());
        assert_eq!(
            valid.classes[0].reason.map(|reason| reason.as_str()),
            Some("unsupported_provenance")
        );
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
                    projector_id: None,
                    record_id: None,
                    revision: None,
                }],
                ..projected_context_item("remember this")
            }],
        );

        let valid = validate_private_context_projection(&request, "test", &projection)
            .expect("missing provenance sequence is class-scoped");
        assert!(valid.items.is_empty());
        assert_eq!(
            valid.classes[0].reason.map(|reason| reason.as_str()),
            Some("unsupported_provenance")
        );
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

        let valid = validate_private_context_projection(&request, "test", &projection)
            .expect("blank provenance event id is class-scoped");
        assert!(valid.items.is_empty());
        assert_eq!(
            valid.classes[0].reason.map(|reason| reason.as_str()),
            Some("unsupported_provenance")
        );
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
    fn validation_accepts_supported_projector_record_provenance() {
        let request = request_with_session_turn_source();
        let projection = projection_with_items(
            request.request_id,
            "test",
            vec![ProjectedContextItem {
                provenance: vec![projector_record_provenance(
                    "test",
                    "record-1",
                    Some("rev_A"),
                )],
                ..projected_context_item("remember this")
            }],
        );

        let valid = validate_private_context_projection(&request, "test", &projection)
            .expect("supported projector record provenance is valid");
        assert_eq!(valid.items.len(), 1);
        assert_eq!(valid.classes[0].accepted_item_count, 1);
    }

    #[test]
    fn validation_omits_invalid_projector_record_provenance() {
        let oversized_revision = "x".repeat(129);
        let cases = [
            ("other", "record-1", Some("rev-1")),
            ("test", "", Some("rev-1")),
            ("test", "bad/record", Some("rev-1")),
            ("test", "record 1", Some("rev-1")),
            ("test", "record-1", Some("")),
            ("test", "record-1", Some("rev 1")),
            ("test", "record-1", Some(oversized_revision.as_str())),
        ];

        for (projector_id, record_id, revision) in cases {
            let request = request_with_session_turn_source();
            let projection = projection_with_items(
                request.request_id,
                "test",
                vec![ProjectedContextItem {
                    provenance: vec![projector_record_provenance(
                        projector_id,
                        record_id,
                        revision,
                    )],
                    ..projected_context_item("remember this")
                }],
            );

            let valid = validate_private_context_projection(&request, "test", &projection)
                .expect("invalid projector record is class-scoped");
            assert!(valid.items.is_empty(), "{projector_id:?} {record_id:?}");
            assert_eq!(
                valid.classes[0].reason.map(|reason| reason.as_str()),
                Some("invalid_projector_record")
            );
        }
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
        assert_eq!(encoded["surface"], "program_primary");
        assert_eq!(encoded["budgets"][0]["class"], "memory");
        assert_eq!(encoded["sources"][0]["kind"], "session_turn_range");
        assert_eq!(
            encoded["sources"][0]["sequence_nos"],
            serde_json::json!([6, 7, 8, 9])
        );
    }

    #[test]
    fn response_deserializes_jsonl_protocol_shape() {
        let decoded: PrivateContextProjection = serde_json::from_str(
            r#"{"request_id":"11111111-1111-1111-1111-111111111111","projector_id":"private-context-core","items":[{"class":"memory","text":"User prefers concise summaries.","provenance":[{"source":"session_turn","sequence_no":7}]}],"ignored":true}"#,
        )
        .expect("decode response");

        assert_eq!(
            decoded.request_id,
            Uuid::parse_str("11111111-1111-1111-1111-111111111111").expect("uuid")
        );
        assert_eq!(decoded.projector_id, "private-context-core");
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
            text: text.into(),
            provenance: vec![session_turn_provenance(7)],
        }
    }

    fn request_with_session_turn_source() -> PrivateContextProjectionRequest {
        PrivateContextProjectionRequest {
            request_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            runtime_id: "mock".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: SessionHistoryPolicy::Interactive,
            surface: PromptContextMode::ProgramPrimary,
            project_scope: Some("project:test".to_string()),
            current_input: None,
            budgets: vec![ProjectedContextBudget {
                class: ProjectedContextClass::Memory,
                max_items: PRIVATE_CONTEXT_PROJECTION_MAX_ITEMS,
                max_bytes: 1024,
            }],
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
            projector_id: None,
            record_id: None,
            revision: None,
        }
    }

    fn compaction_provenance(sequence_no: u64) -> ProjectedContextProvenance {
        ProjectedContextProvenance {
            source: ProjectedContextProvenanceSource::CompactionSummary,
            sequence_no: Some(sequence_no),
            event_id: None,
            projector_id: None,
            record_id: None,
            revision: None,
        }
    }

    fn projector_record_provenance(
        projector_id: &str,
        record_id: &str,
        revision: Option<&str>,
    ) -> ProjectedContextProvenance {
        ProjectedContextProvenance {
            source: ProjectedContextProvenanceSource::ProjectorRecord,
            sequence_no: None,
            event_id: None,
            projector_id: Some(projector_id.to_string()),
            record_id: Some(record_id.to_string()),
            revision: revision.map(str::to_string),
        }
    }

    fn request_with_all_class_budgets() -> PrivateContextProjectionRequest {
        PrivateContextProjectionRequest {
            budgets: ProjectedContextClass::ALL
                .into_iter()
                .map(|class| ProjectedContextBudget {
                    class,
                    max_items: PRIVATE_CONTEXT_PROJECTION_MAX_ITEMS,
                    max_bytes: 1024,
                })
                .collect(),
            ..request_with_session_turn_source()
        }
    }
}
