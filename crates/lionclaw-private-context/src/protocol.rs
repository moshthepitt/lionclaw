use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TrustTier {
    Main,
    Untrusted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SessionHistoryPolicy {
    Interactive,
    Conservative,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PromptContextMode {
    ProgramPrimary,
    ProgramResumePrimary,
    ProgramFresh,
    AttachedNativeTui,
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PrivateContextRecordRequest {
    pub session_id: Uuid,
    pub turn_id: Uuid,
    pub sequence_no: u64,
    pub runtime_id: String,
    pub trust_tier: TrustTier,
    pub history_policy: SessionHistoryPolicy,
    pub surface: PrivateContextRecordSurface,
    pub project_scope: Option<String>,
    pub transcript: PrivateContextRecordTranscript,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum PrivateContextRecordSurface {
    #[serde(rename = "program_turn")]
    Program,
    #[serde(rename = "channel_turn")]
    Channel,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct PrivateContextRecordTranscript {
    pub user: Option<PrivateContextRecordText>,
    pub assistant: Option<PrivateContextRecordText>,
}

impl PrivateContextRecordTranscript {
    pub(crate) fn user_text(&self) -> &str {
        self.user.as_ref().map_or("", |text| text.text.as_str())
    }

    pub(crate) fn assistant_text(&self) -> &str {
        self.assistant
            .as_ref()
            .map_or("", |text| text.text.as_str())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.user_text().trim().is_empty() && self.assistant_text().trim().is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PrivateContextRecordText {
    pub text: String,
    pub included_bytes: usize,
    pub original_bytes: usize,
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
    pub(crate) const ALL: [Self; 3] = [Self::AssistantProfile, Self::UserProfile, Self::Memory];

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::AssistantProfile => "assistant_profile",
            Self::UserProfile => "user_profile",
            Self::Memory => "memory",
        }
    }

    pub(crate) fn title(self) -> &'static str {
        match self {
            Self::AssistantProfile => "AssistantProfile",
            Self::UserProfile => "UserProfile",
            Self::Memory => "Memory",
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProjectedContextProvenanceSource {
    SessionTurn,
    CompactionSummary,
    ProjectorRecord,
}
