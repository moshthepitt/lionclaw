use serde_json::{json, Value};

use crate::{
    contracts::{SessionHistoryPolicy, TrustTier},
    workspace::AGENTS_FILE,
};

pub(crate) const PROMPT_CONTEXT_POLICY_VERSION: u32 = 2;
pub(crate) const ACTIVE_CONTEXT_FILE: &str = "continuity/ACTIVE.md";

const GENERATED_KERNEL_POLICY: &str = "kernel_policy";
const GENERATED_SAFE_WORKSPACE_RULES: &str = "safe_workspace_rules";
const GENERATED_RUNTIME_SESSION_NOTE: &str = "runtime_session_note";
const GENERATED_NATIVE_TUI_SESSION_NOTE: &str = "native_tui_session_note";
const GENERATED_DRAFT_OUTPUTS_NOTE: &str = "draft_outputs_note";
const GENERATED_RUNTIME_SECRETS_NOTE: &str = "runtime_secrets_note";

const MAIN_INTERACTIVE_TRANSCRIPT_TAIL: usize = 12;
const MAIN_CONSERVATIVE_TRANSCRIPT_TAIL: usize = 6;
const UNTRUSTED_INTERACTIVE_TRANSCRIPT_TAIL: usize = 4;
const UNTRUSTED_CONSERVATIVE_TRANSCRIPT_TAIL: usize = 2;

const MAIN_INTERACTIVE_TRANSCRIPT_BUDGET: usize = 12_288;
const MAIN_CONSERVATIVE_TRANSCRIPT_BUDGET: usize = 6_144;
const UNTRUSTED_INTERACTIVE_TRANSCRIPT_BUDGET: usize = 4_096;
const UNTRUSTED_CONSERVATIVE_TRANSCRIPT_BUDGET: usize = 2_048;
const AGENTS_MAIN_BUDGET: usize = 4096;
const MEMORY_MAIN_INTERACTIVE_BUDGET: usize = 4096;
const MEMORY_MAIN_CONSERVATIVE_BUDGET: usize = 2048;
const ACTIVE_MAIN_BUDGET: usize = 2048;
const ACTIVE_UNTRUSTED_INTERACTIVE_BUDGET: usize = 1024;
const ACTIVE_UNTRUSTED_CONSERVATIVE_BUDGET: usize = 512;
const HANDOFF_MAIN_INTERACTIVE_BUDGET: usize = 4096;
const HANDOFF_MAIN_CONSERVATIVE_BUDGET: usize = 3072;
const HANDOFF_UNTRUSTED_INTERACTIVE_BUDGET: usize = 1024;
const HANDOFF_UNTRUSTED_CONSERVATIVE_BUDGET: usize = 512;
const GENERATED_NOTE_BUDGET: usize = 4096;
const CURRENT_INPUT_BUDGET: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PromptContextMode {
    ProgramPrimary,
    ProgramResumePrimary,
    ProgramFresh,
    AttachedNativeTui,
}

impl PromptContextMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ProgramPrimary => "program_primary",
            Self::ProgramResumePrimary => "program_resume_primary",
            Self::ProgramFresh => "program_fresh",
            Self::AttachedNativeTui => "attached_native_tui",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContextClass {
    Kernel,
    WorkspaceRules,
    Memory,
    ActiveContinuity,
    SessionHandoff,
    Transcript,
    RuntimeNote,
    CurrentInput,
}

impl ContextClass {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Kernel => "kernel",
            Self::WorkspaceRules => "workspace_rules",
            Self::Memory => "memory",
            Self::ActiveContinuity => "active_continuity",
            Self::SessionHandoff => "session_handoff",
            Self::Transcript => "transcript",
            Self::RuntimeNote => "runtime_note",
            Self::CurrentInput => "current_input",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContextItemId {
    KernelPolicy,
    WorkspaceRules,
    SafeWorkspaceRules,
    MemoryContext,
    ActiveContinuity,
    SessionHandoff,
    RecentTranscript,
    RuntimeSessionNote,
    NativeTuiSessionNote,
    DraftOutputsNote,
    RuntimeSecretsNote,
    UserInput,
}

impl ContextItemId {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::KernelPolicy => "kernel_policy",
            Self::WorkspaceRules => "workspace_rules",
            Self::SafeWorkspaceRules => "safe_workspace_rules",
            Self::MemoryContext => "memory_context",
            Self::ActiveContinuity => "active_continuity",
            Self::SessionHandoff => "session_handoff",
            Self::RecentTranscript => "recent_transcript",
            Self::RuntimeSessionNote => "runtime_session_note",
            Self::NativeTuiSessionNote => "native_tui_session_note",
            Self::DraftOutputsNote => "draft_outputs_note",
            Self::RuntimeSecretsNote => "runtime_secrets_note",
            Self::UserInput => "user_input",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContextSource {
    Generated(&'static str),
    WorkspaceFile(&'static str),
    ContinuityFile(&'static str),
    MemoryProjection,
    CompactionSummary,
    TranscriptTail,
    CurrentUserInput,
}

impl ContextSource {
    pub(crate) fn as_str(self) -> String {
        match self {
            Self::Generated(name) => format!("generated:{name}"),
            Self::WorkspaceFile(file_name) => format!("workspace_file:{file_name}"),
            Self::ContinuityFile(file_name) => format!("continuity_file:{file_name}"),
            Self::MemoryProjection => "memory_projection".to_string(),
            Self::CompactionSummary => "compaction_summary".to_string(),
            Self::TranscriptTail => "transcript_tail".to_string(),
            Self::CurrentUserInput => "current_user_input".to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContextItemSpec {
    pub id: ContextItemId,
    pub title: &'static str,
    pub class: ContextClass,
    pub source: ContextSource,
    pub required: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct PromptContextPolicy {
    pub trust_tier: TrustTier,
    pub history_policy: SessionHistoryPolicy,
    pub mode: PromptContextMode,
    pub runtime_id: String,
    pub policy_version: u32,
    pub transcript_tail_limit: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct PromptContextBuild {
    pub sections: Vec<String>,
    pub audit: PromptContextAudit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExclusionDecision {
    pub reason: &'static str,
}

impl PromptContextPolicy {
    pub(crate) fn new(
        trust_tier: TrustTier,
        history_policy: SessionHistoryPolicy,
        mode: PromptContextMode,
        runtime_id: impl Into<String>,
    ) -> Self {
        let transcript_tail_limit = transcript_tail_limit(&trust_tier, history_policy);
        Self {
            trust_tier,
            history_policy,
            mode,
            runtime_id: runtime_id.into(),
            policy_version: PROMPT_CONTEXT_POLICY_VERSION,
            transcript_tail_limit,
        }
    }

    pub(crate) fn allows(&self, item: &ContextItemSpec) -> Result<(), ExclusionDecision> {
        if item.class == ContextClass::Transcript
            && self.mode == PromptContextMode::ProgramResumePrimary
        {
            return Err(ExclusionDecision {
                reason: "resumed_runtime_session",
            });
        }

        match &self.trust_tier {
            TrustTier::Main => {
                if item.id == ContextItemId::SafeWorkspaceRules {
                    return Err(ExclusionDecision {
                        reason: "trust_tier_main",
                    });
                }
                if item.class == ContextClass::WorkspaceRules
                    && !matches!(item.source, ContextSource::WorkspaceFile(_))
                {
                    return Err(ExclusionDecision {
                        reason: "trust_tier_main",
                    });
                }
                Ok(())
            }
            TrustTier::Untrusted => match item.class {
                ContextClass::RuntimeNote => match item.id {
                    ContextItemId::RuntimeSessionNote
                    | ContextItemId::NativeTuiSessionNote
                    | ContextItemId::DraftOutputsNote => Ok(()),
                    _ => Err(ExclusionDecision {
                        reason: "trust_tier_untrusted",
                    }),
                },
                ContextClass::Kernel
                | ContextClass::ActiveContinuity
                | ContextClass::SessionHandoff
                | ContextClass::Transcript
                | ContextClass::CurrentInput => Ok(()),
                ContextClass::WorkspaceRules if item.id == ContextItemId::SafeWorkspaceRules => {
                    Ok(())
                }
                _ => Err(ExclusionDecision {
                    reason: "trust_tier_untrusted",
                }),
            },
        }
    }

    pub(crate) fn max_bytes(&self, item: &ContextItemSpec) -> usize {
        match (&self.trust_tier, self.history_policy, item.id, item.class) {
            (_, _, _, ContextClass::Kernel | ContextClass::RuntimeNote) => GENERATED_NOTE_BUDGET,
            (_, _, _, ContextClass::CurrentInput) => CURRENT_INPUT_BUDGET,
            (TrustTier::Main, SessionHistoryPolicy::Interactive, _, ContextClass::Transcript) => {
                MAIN_INTERACTIVE_TRANSCRIPT_BUDGET
            }
            (TrustTier::Main, SessionHistoryPolicy::Conservative, _, ContextClass::Transcript) => {
                MAIN_CONSERVATIVE_TRANSCRIPT_BUDGET
            }
            (
                TrustTier::Untrusted,
                SessionHistoryPolicy::Interactive,
                _,
                ContextClass::Transcript,
            ) => UNTRUSTED_INTERACTIVE_TRANSCRIPT_BUDGET,
            (
                TrustTier::Untrusted,
                SessionHistoryPolicy::Conservative,
                _,
                ContextClass::Transcript,
            ) => UNTRUSTED_CONSERVATIVE_TRANSCRIPT_BUDGET,
            (TrustTier::Main, _, ContextItemId::WorkspaceRules, _) => AGENTS_MAIN_BUDGET,
            (
                TrustTier::Main,
                SessionHistoryPolicy::Interactive,
                ContextItemId::MemoryContext,
                _,
            ) => MEMORY_MAIN_INTERACTIVE_BUDGET,
            (
                TrustTier::Main,
                SessionHistoryPolicy::Conservative,
                ContextItemId::MemoryContext,
                _,
            ) => MEMORY_MAIN_CONSERVATIVE_BUDGET,
            (TrustTier::Main, _, ContextItemId::ActiveContinuity, _) => ACTIVE_MAIN_BUDGET,
            (
                TrustTier::Main,
                SessionHistoryPolicy::Interactive,
                ContextItemId::SessionHandoff,
                _,
            ) => HANDOFF_MAIN_INTERACTIVE_BUDGET,
            (
                TrustTier::Main,
                SessionHistoryPolicy::Conservative,
                ContextItemId::SessionHandoff,
                _,
            ) => HANDOFF_MAIN_CONSERVATIVE_BUDGET,
            (TrustTier::Untrusted, _, ContextItemId::SafeWorkspaceRules, _) => {
                GENERATED_NOTE_BUDGET
            }
            (
                TrustTier::Untrusted,
                SessionHistoryPolicy::Interactive,
                ContextItemId::ActiveContinuity,
                _,
            ) => ACTIVE_UNTRUSTED_INTERACTIVE_BUDGET,
            (
                TrustTier::Untrusted,
                SessionHistoryPolicy::Conservative,
                ContextItemId::ActiveContinuity,
                _,
            ) => ACTIVE_UNTRUSTED_CONSERVATIVE_BUDGET,
            (
                TrustTier::Untrusted,
                SessionHistoryPolicy::Interactive,
                ContextItemId::SessionHandoff,
                _,
            ) => HANDOFF_UNTRUSTED_INTERACTIVE_BUDGET,
            (
                TrustTier::Untrusted,
                SessionHistoryPolicy::Conservative,
                ContextItemId::SessionHandoff,
                _,
            ) => HANDOFF_UNTRUSTED_CONSERVATIVE_BUDGET,
            _ => 0,
        }
    }
}

fn transcript_tail_limit(trust_tier: &TrustTier, history_policy: SessionHistoryPolicy) -> usize {
    match (trust_tier, history_policy) {
        (TrustTier::Main, SessionHistoryPolicy::Interactive) => MAIN_INTERACTIVE_TRANSCRIPT_TAIL,
        (TrustTier::Main, SessionHistoryPolicy::Conservative) => MAIN_CONSERVATIVE_TRANSCRIPT_TAIL,
        (TrustTier::Untrusted, SessionHistoryPolicy::Interactive) => {
            UNTRUSTED_INTERACTIVE_TRANSCRIPT_TAIL
        }
        (TrustTier::Untrusted, SessionHistoryPolicy::Conservative) => {
            UNTRUSTED_CONSERVATIVE_TRANSCRIPT_TAIL
        }
    }
}

pub(crate) fn context_item_specs(mode: PromptContextMode) -> Vec<ContextItemSpec> {
    let mut items = vec![
        ContextItemSpec {
            id: ContextItemId::KernelPolicy,
            title: "LionClaw",
            class: ContextClass::Kernel,
            source: ContextSource::Generated(GENERATED_KERNEL_POLICY),
            required: true,
        },
        ContextItemSpec {
            id: ContextItemId::WorkspaceRules,
            title: AGENTS_FILE,
            class: ContextClass::WorkspaceRules,
            source: ContextSource::WorkspaceFile(AGENTS_FILE),
            required: false,
        },
        ContextItemSpec {
            id: ContextItemId::SafeWorkspaceRules,
            title: "Workspace Rules",
            class: ContextClass::WorkspaceRules,
            source: ContextSource::Generated(GENERATED_SAFE_WORKSPACE_RULES),
            required: true,
        },
        ContextItemSpec {
            id: ContextItemId::MemoryContext,
            title: "Memory",
            class: ContextClass::Memory,
            source: ContextSource::MemoryProjection,
            required: false,
        },
        ContextItemSpec {
            id: ContextItemId::ActiveContinuity,
            title: ACTIVE_CONTEXT_FILE,
            class: ContextClass::ActiveContinuity,
            source: ContextSource::ContinuityFile(ACTIVE_CONTEXT_FILE),
            required: false,
        },
        ContextItemSpec {
            id: ContextItemId::SessionHandoff,
            title: "Session Handoff",
            class: ContextClass::SessionHandoff,
            source: ContextSource::CompactionSummary,
            required: false,
        },
    ];

    match mode {
        PromptContextMode::ProgramPrimary | PromptContextMode::ProgramFresh => {
            extend_runtime_operational_notes(&mut items);
            items.push(transcript_item());
            items.push(current_input_item());
        }
        PromptContextMode::ProgramResumePrimary => {
            items.push(runtime_session_note_item());
            extend_runtime_operational_notes(&mut items);
            items.push(transcript_item());
            items.push(current_input_item());
        }
        PromptContextMode::AttachedNativeTui => {
            items.push(native_tui_session_note_item());
            extend_runtime_operational_notes(&mut items);
            items.push(transcript_item());
        }
    }

    items
}

fn extend_runtime_operational_notes(items: &mut Vec<ContextItemSpec>) {
    items.push(draft_outputs_note_item());
    items.push(runtime_secrets_note_item());
}

fn runtime_session_note_item() -> ContextItemSpec {
    ContextItemSpec {
        id: ContextItemId::RuntimeSessionNote,
        title: "Runtime Session",
        class: ContextClass::RuntimeNote,
        source: ContextSource::Generated(GENERATED_RUNTIME_SESSION_NOTE),
        required: true,
    }
}

fn native_tui_session_note_item() -> ContextItemSpec {
    ContextItemSpec {
        id: ContextItemId::NativeTuiSessionNote,
        title: "Native Runtime TUI Session",
        class: ContextClass::RuntimeNote,
        source: ContextSource::Generated(GENERATED_NATIVE_TUI_SESSION_NOTE),
        required: true,
    }
}

fn draft_outputs_note_item() -> ContextItemSpec {
    ContextItemSpec {
        id: ContextItemId::DraftOutputsNote,
        title: "Draft Outputs",
        class: ContextClass::RuntimeNote,
        source: ContextSource::Generated(GENERATED_DRAFT_OUTPUTS_NOTE),
        required: false,
    }
}

fn runtime_secrets_note_item() -> ContextItemSpec {
    ContextItemSpec {
        id: ContextItemId::RuntimeSecretsNote,
        title: "Runtime Secrets",
        class: ContextClass::RuntimeNote,
        source: ContextSource::Generated(GENERATED_RUNTIME_SECRETS_NOTE),
        required: false,
    }
}

fn transcript_item() -> ContextItemSpec {
    ContextItemSpec {
        id: ContextItemId::RecentTranscript,
        title: "Recent Transcript",
        class: ContextClass::Transcript,
        source: ContextSource::TranscriptTail,
        required: false,
    }
}

fn current_input_item() -> ContextItemSpec {
    ContextItemSpec {
        id: ContextItemId::UserInput,
        title: "User Input",
        class: ContextClass::CurrentInput,
        source: ContextSource::CurrentUserInput,
        required: true,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CappedSection {
    pub content: String,
    pub included_bytes: usize,
    pub original_bytes: usize,
    pub was_capped: bool,
}

pub(crate) fn cap_utf8_at_line_boundary(content: &str, max_bytes: usize) -> CappedSection {
    let original_bytes = content.len();
    if original_bytes <= max_bytes {
        return CappedSection {
            content: content.to_string(),
            included_bytes: original_bytes,
            original_bytes,
            was_capped: false,
        };
    }
    if max_bytes == 0 {
        return CappedSection {
            content: String::new(),
            included_bytes: 0,
            original_bytes,
            was_capped: original_bytes > 0,
        };
    }

    let mut limit = max_bytes.min(content.len());
    while limit > 0 && !content.is_char_boundary(limit) {
        limit -= 1;
    }

    let capped_at = content
        .char_indices()
        .take_while(|(idx, _)| *idx <= limit)
        .filter_map(|(idx, ch)| (ch == '\n').then_some(idx))
        .last()
        .unwrap_or(limit);
    let capped = content[..capped_at].to_string();
    CappedSection {
        included_bytes: capped.len(),
        content: capped,
        original_bytes,
        was_capped: true,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PromptContextIncluded {
    pub id: ContextItemId,
    pub class: ContextClass,
    pub source: ContextSource,
    pub included_bytes: usize,
    pub original_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PromptContextExcluded {
    pub id: ContextItemId,
    pub class: ContextClass,
    pub source: ContextSource,
    pub reason: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PromptContextCap {
    pub id: ContextItemId,
    pub class: ContextClass,
    pub source: ContextSource,
    pub included_bytes: usize,
    pub original_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PromptContextMemoryProjectionAudit {
    pub status: &'static str,
    pub projector_id: Option<String>,
    pub item_count: usize,
    pub source_count: usize,
    pub requested_max_items: usize,
    pub requested_max_bytes: usize,
    pub projected_bytes: usize,
    pub included_bytes: usize,
    pub original_bytes: usize,
    pub was_capped: bool,
    pub reason: Option<&'static str>,
}

impl PromptContextMemoryProjectionAudit {
    pub(crate) fn new(
        status: &'static str,
        projector_id: Option<String>,
        source_count: usize,
        requested_max_items: usize,
        requested_max_bytes: usize,
    ) -> Self {
        Self {
            status,
            projector_id,
            item_count: 0,
            source_count,
            requested_max_items,
            requested_max_bytes,
            projected_bytes: 0,
            included_bytes: 0,
            original_bytes: 0,
            was_capped: false,
            reason: None,
        }
    }

    pub(crate) fn with_counts(mut self, item_count: usize, projected_bytes: usize) -> Self {
        self.item_count = item_count;
        self.projected_bytes = projected_bytes;
        self
    }

    pub(crate) fn with_rendered_bytes(
        mut self,
        included_bytes: usize,
        original_bytes: usize,
        was_capped: bool,
    ) -> Self {
        self.included_bytes = included_bytes;
        self.original_bytes = original_bytes;
        self.was_capped = was_capped;
        self
    }

    pub(crate) fn with_reason(mut self, reason: &'static str) -> Self {
        self.reason = Some(reason);
        self
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PromptContextAudit {
    pub runtime_id: String,
    pub trust_tier: TrustTier,
    pub history_policy: SessionHistoryPolicy,
    pub policy_version: u32,
    pub mode: PromptContextMode,
    pub included: Vec<PromptContextIncluded>,
    pub excluded: Vec<PromptContextExcluded>,
    pub capped: Vec<PromptContextCap>,
    pub memory_projection: Option<PromptContextMemoryProjectionAudit>,
    pub total_included_bytes: usize,
}

impl PromptContextAudit {
    pub(crate) fn new(policy: &PromptContextPolicy) -> Self {
        Self {
            runtime_id: policy.runtime_id.clone(),
            trust_tier: policy.trust_tier.clone(),
            history_policy: policy.history_policy,
            policy_version: policy.policy_version,
            mode: policy.mode,
            included: Vec::new(),
            excluded: Vec::new(),
            capped: Vec::new(),
            memory_projection: None,
            total_included_bytes: 0,
        }
    }

    pub(crate) fn include(&mut self, item: &ContextItemSpec, capped: &CappedSection) {
        self.total_included_bytes = self
            .total_included_bytes
            .saturating_add(capped.included_bytes);
        self.included.push(PromptContextIncluded {
            id: item.id,
            class: item.class,
            source: item.source,
            included_bytes: capped.included_bytes,
            original_bytes: capped.original_bytes,
        });
        if capped.was_capped {
            self.capped.push(PromptContextCap {
                id: item.id,
                class: item.class,
                source: item.source,
                included_bytes: capped.included_bytes,
                original_bytes: capped.original_bytes,
            });
        }
    }

    pub(crate) fn exclude(&mut self, item: &ContextItemSpec, reason: &'static str) {
        self.excluded.push(PromptContextExcluded {
            id: item.id,
            class: item.class,
            source: item.source,
            reason,
        });
    }

    pub(crate) fn record_memory_projection(
        &mut self,
        projection: PromptContextMemoryProjectionAudit,
    ) {
        self.memory_projection = Some(projection);
    }

    pub(crate) fn to_details_json(&self) -> Value {
        json!({
            "runtime_id": self.runtime_id,
            "trust_tier": self.trust_tier.as_str(),
            "history_policy": self.history_policy.as_str(),
            "policy_version": self.policy_version,
            "mode": self.mode.as_str(),
            "included": self.included.iter().map(included_json).collect::<Vec<_>>(),
            "excluded": self.excluded.iter().map(excluded_json).collect::<Vec<_>>(),
            "capped": self.capped.iter().map(cap_json).collect::<Vec<_>>(),
            "memory_projection": self.memory_projection.as_ref().map(memory_projection_json),
            "total_included_bytes": self.total_included_bytes,
        })
    }
}

fn included_json(item: &PromptContextIncluded) -> Value {
    json!({
        "id": item.id.as_str(),
        "class": item.class.as_str(),
        "source": item.source.as_str(),
        "included_bytes": item.included_bytes,
        "original_bytes": item.original_bytes,
    })
}

fn excluded_json(item: &PromptContextExcluded) -> Value {
    json!({
        "id": item.id.as_str(),
        "class": item.class.as_str(),
        "source": item.source.as_str(),
        "reason": item.reason,
    })
}

fn cap_json(item: &PromptContextCap) -> Value {
    json!({
        "id": item.id.as_str(),
        "class": item.class.as_str(),
        "source": item.source.as_str(),
        "included_bytes": item.included_bytes,
        "original_bytes": item.original_bytes,
    })
}

fn memory_projection_json(item: &PromptContextMemoryProjectionAudit) -> Value {
    json!({
        "status": item.status,
        "projector_id": item.projector_id,
        "item_count": item.item_count,
        "source_count": item.source_count,
        "requested_max_items": item.requested_max_items,
        "requested_max_bytes": item.requested_max_bytes,
        "projected_bytes": item.projected_bytes,
        "included_bytes": item.included_bytes,
        "original_bytes": item.original_bytes,
        "was_capped": item.was_capped,
        "cap_status": if item.was_capped { "capped" } else { "uncapped" },
        "reason": item.reason,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cap_keeps_content_within_budget() {
        let capped = cap_utf8_at_line_boundary("abc", 3);
        assert_eq!(capped.content, "abc");
        assert!(!capped.was_capped);
        assert_eq!(capped.included_bytes, 3);
        assert_eq!(capped.original_bytes, 3);
    }

    #[test]
    fn cap_uses_newline_boundary_at_budget() {
        let capped = cap_utf8_at_line_boundary("first\nsecond\nthird", 12);
        assert_eq!(capped.content, "first\nsecond");
        assert_eq!(capped.included_bytes, 12);
        assert!(capped.was_capped);
    }

    #[test]
    fn cap_uses_previous_newline_boundary() {
        let capped = cap_utf8_at_line_boundary("first\nsecond\nthird", 11);
        assert_eq!(capped.content, "first");
        assert!(capped.was_capped);
    }

    #[test]
    fn cap_uses_utf8_boundary_without_newline() {
        let capped = cap_utf8_at_line_boundary("abéfg", 4);
        assert_eq!(capped.content, "abé");
        assert!(capped.was_capped);
    }

    #[test]
    fn cap_uses_previous_utf8_boundary_when_budget_splits_character() {
        let capped = cap_utf8_at_line_boundary("abéfg", 3);
        assert_eq!(capped.content, "ab");
        assert!(capped.was_capped);
    }

    #[test]
    fn cap_zero_budget_returns_empty_content() {
        let capped = cap_utf8_at_line_boundary("abc", 0);
        assert_eq!(capped.content, "");
        assert!(capped.was_capped);
        assert_eq!(capped.included_bytes, 0);
    }

    #[test]
    fn context_item_specs_are_unique_and_consistent() {
        for mode in [
            PromptContextMode::ProgramPrimary,
            PromptContextMode::ProgramResumePrimary,
            PromptContextMode::ProgramFresh,
            PromptContextMode::AttachedNativeTui,
        ] {
            let mut ids = Vec::new();
            for item in context_item_specs(mode) {
                assert!(
                    !ids.contains(&item.id),
                    "duplicate context item id '{}' in {}",
                    item.id.as_str(),
                    mode.as_str()
                );
                ids.push(item.id);
                assert_context_item_shape(item);
            }
        }
    }

    #[test]
    fn context_item_specs_keep_program_user_input_terminal() {
        for mode in [
            PromptContextMode::ProgramPrimary,
            PromptContextMode::ProgramResumePrimary,
            PromptContextMode::ProgramFresh,
        ] {
            let items = context_item_specs(mode);
            assert_eq!(
                items.last().map(|item| item.id),
                Some(ContextItemId::UserInput),
                "program context mode {} should end with current user input",
                mode.as_str()
            );
            assert_item_before(
                &items,
                ContextItemId::DraftOutputsNote,
                ContextItemId::UserInput,
            );
            assert_item_before(
                &items,
                ContextItemId::RuntimeSecretsNote,
                ContextItemId::UserInput,
            );
        }
    }

    #[test]
    fn context_item_specs_keep_attached_tui_transcript_after_runtime_notes() {
        let items = context_item_specs(PromptContextMode::AttachedNativeTui);
        assert_eq!(
            items.last().map(|item| item.id),
            Some(ContextItemId::RecentTranscript)
        );
        assert_item_before(
            &items,
            ContextItemId::NativeTuiSessionNote,
            ContextItemId::RecentTranscript,
        );
        assert_item_before(
            &items,
            ContextItemId::DraftOutputsNote,
            ContextItemId::RecentTranscript,
        );
        assert_item_before(
            &items,
            ContextItemId::RuntimeSecretsNote,
            ContextItemId::RecentTranscript,
        );
    }

    #[test]
    fn context_item_specs_exclude_legacy_private_file_slots() {
        let items = context_item_specs(PromptContextMode::ProgramPrimary);
        let item_names = items
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>();
        assert!(!item_names.contains(&"identity"));
        assert!(!item_names.contains(&"style_profile"));
        assert!(!item_names.contains(&"user_context"));
    }

    #[test]
    fn main_interactive_policy_allows_memory_projection() {
        let policy = PromptContextPolicy::new(
            TrustTier::Main,
            SessionHistoryPolicy::Interactive,
            PromptContextMode::ProgramPrimary,
            "codex",
        );
        assert_eq!(policy.transcript_tail_limit, 12);
        let memory = context_item_specs(PromptContextMode::ProgramPrimary)
            .into_iter()
            .find(|item| item.id == ContextItemId::MemoryContext)
            .expect("memory context item");
        assert!(policy.allows(&memory).is_ok());
        assert_eq!(policy.max_bytes(&memory), MEMORY_MAIN_INTERACTIVE_BUDGET);
    }

    #[test]
    fn conservative_policy_is_tighter_than_interactive() {
        let interactive = PromptContextPolicy::new(
            TrustTier::Main,
            SessionHistoryPolicy::Interactive,
            PromptContextMode::ProgramPrimary,
            "codex",
        );
        let conservative = PromptContextPolicy::new(
            TrustTier::Main,
            SessionHistoryPolicy::Conservative,
            PromptContextMode::ProgramPrimary,
            "codex",
        );
        let memory = context_item_specs(PromptContextMode::ProgramPrimary)
            .into_iter()
            .find(|item| item.id == ContextItemId::MemoryContext)
            .expect("memory context item");
        assert!(conservative.transcript_tail_limit < interactive.transcript_tail_limit);
        assert!(conservative.max_bytes(&memory) < interactive.max_bytes(&memory));
    }

    #[test]
    fn untrusted_policy_excludes_private_context() {
        let policy = PromptContextPolicy::new(
            TrustTier::Untrusted,
            SessionHistoryPolicy::Interactive,
            PromptContextMode::ProgramPrimary,
            "codex",
        );
        let items = context_item_specs(PromptContextMode::ProgramPrimary);
        for id in [ContextItemId::WorkspaceRules, ContextItemId::MemoryContext] {
            let item = items
                .iter()
                .find(|item| item.id == id)
                .unwrap_or_else(|| panic!("missing {}", id.as_str()));
            assert!(
                policy.allows(item).is_err(),
                "{} should be excluded",
                id.as_str()
            );
        }
        let safe = items
            .iter()
            .find(|item| item.id == ContextItemId::SafeWorkspaceRules)
            .expect("safe workspace rules");
        assert!(policy.allows(safe).is_ok());
        assert_eq!(policy.transcript_tail_limit, 4);
    }

    #[test]
    fn untrusted_conservative_policy_tightens_allowed_context() {
        let interactive = PromptContextPolicy::new(
            TrustTier::Untrusted,
            SessionHistoryPolicy::Interactive,
            PromptContextMode::ProgramPrimary,
            "codex",
        );
        let conservative = PromptContextPolicy::new(
            TrustTier::Untrusted,
            SessionHistoryPolicy::Conservative,
            PromptContextMode::ProgramPrimary,
            "codex",
        );
        let items = context_item_specs(PromptContextMode::ProgramPrimary);
        let active = items
            .iter()
            .find(|item| item.id == ContextItemId::ActiveContinuity)
            .expect("active continuity");
        let handoff = items
            .iter()
            .find(|item| item.id == ContextItemId::SessionHandoff)
            .expect("session handoff");
        let memory = items
            .iter()
            .find(|item| item.id == ContextItemId::MemoryContext)
            .expect("memory context");

        assert_eq!(interactive.transcript_tail_limit, 4);
        assert_eq!(conservative.transcript_tail_limit, 2);
        assert!(conservative.max_bytes(active) < interactive.max_bytes(active));
        assert!(conservative.max_bytes(handoff) < interactive.max_bytes(handoff));
        assert!(conservative.allows(memory).is_err());
    }

    #[test]
    fn untrusted_policy_excludes_runtime_secret_guidance() {
        let policy = PromptContextPolicy::new(
            TrustTier::Untrusted,
            SessionHistoryPolicy::Interactive,
            PromptContextMode::ProgramPrimary,
            "codex",
        );
        let item = context_item_specs(PromptContextMode::ProgramPrimary)
            .into_iter()
            .find(|item| item.id == ContextItemId::RuntimeSecretsNote)
            .expect("runtime secrets note");

        let err = policy
            .allows(&item)
            .expect_err("untrusted runtime excludes secret guidance");
        assert_eq!(err.reason, "trust_tier_untrusted");
    }

    #[test]
    fn untrusted_policy_allows_only_safe_runtime_notes() {
        for (mode, expected_runtime_note) in [
            (
                PromptContextMode::ProgramResumePrimary,
                ContextItemId::RuntimeSessionNote,
            ),
            (
                PromptContextMode::AttachedNativeTui,
                ContextItemId::NativeTuiSessionNote,
            ),
            (
                PromptContextMode::ProgramPrimary,
                ContextItemId::DraftOutputsNote,
            ),
        ] {
            let policy = PromptContextPolicy::new(
                TrustTier::Untrusted,
                SessionHistoryPolicy::Interactive,
                mode,
                "codex",
            );
            let item = context_item_specs(mode)
                .into_iter()
                .find(|item| item.id == expected_runtime_note)
                .unwrap_or_else(|| panic!("missing {}", expected_runtime_note.as_str()));

            assert!(
                policy.allows(&item).is_ok(),
                "{} should be allowed for untrusted {}",
                expected_runtime_note.as_str(),
                mode.as_str()
            );
        }
    }

    #[test]
    fn every_allowed_item_has_positive_budget() {
        for trust_tier in [TrustTier::Main, TrustTier::Untrusted] {
            for history_policy in [
                SessionHistoryPolicy::Interactive,
                SessionHistoryPolicy::Conservative,
            ] {
                for mode in [
                    PromptContextMode::ProgramPrimary,
                    PromptContextMode::ProgramResumePrimary,
                    PromptContextMode::ProgramFresh,
                    PromptContextMode::AttachedNativeTui,
                ] {
                    let policy =
                        PromptContextPolicy::new(trust_tier.clone(), history_policy, mode, "mock");
                    for item in context_item_specs(mode) {
                        if policy.allows(&item).is_ok() {
                            assert!(
                                policy.max_bytes(&item) > 0,
                                "{} allowed in {} without a budget",
                                item.id.as_str(),
                                mode.as_str()
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn current_input_budget_is_finite() {
        let policy = PromptContextPolicy::new(
            TrustTier::Main,
            SessionHistoryPolicy::Interactive,
            PromptContextMode::ProgramPrimary,
            "codex",
        );
        let current_input = context_item_specs(PromptContextMode::ProgramPrimary)
            .into_iter()
            .find(|item| item.id == ContextItemId::UserInput)
            .expect("current input item");

        assert_eq!(policy.max_bytes(&current_input), CURRENT_INPUT_BUDGET);
        assert!(policy.max_bytes(&current_input) < usize::MAX);
    }

    #[test]
    fn resume_primary_excludes_transcript() {
        let policy = PromptContextPolicy::new(
            TrustTier::Main,
            SessionHistoryPolicy::Interactive,
            PromptContextMode::ProgramResumePrimary,
            "codex",
        );
        let transcript = context_item_specs(PromptContextMode::ProgramResumePrimary)
            .into_iter()
            .find(|item| item.id == ContextItemId::RecentTranscript)
            .expect("transcript item");
        let err = policy
            .allows(&transcript)
            .expect_err("resume primary excludes transcript");
        assert_eq!(err.reason, "resumed_runtime_session");
    }

    #[test]
    fn audit_json_uses_names_without_content() {
        let policy = PromptContextPolicy::new(
            TrustTier::Main,
            SessionHistoryPolicy::Interactive,
            PromptContextMode::ProgramPrimary,
            "codex",
        );
        let mut audit = PromptContextAudit::new(&policy);
        let item = context_item_specs(PromptContextMode::ProgramPrimary)
            .into_iter()
            .find(|item| item.id == ContextItemId::MemoryContext)
            .expect("memory context");
        audit.include(
            &item,
            &CappedSection {
                content: "SECRET_MEMORY_FACT_SHOULD_NOT_APPEAR".to_string(),
                included_bytes: 32,
                original_bytes: 64,
                was_capped: true,
            },
        );
        let details = audit.to_details_json();
        assert_eq!(details["policy_version"], PROMPT_CONTEXT_POLICY_VERSION);
        let raw = serde_json::to_string(&details).expect("serialize audit");
        assert!(raw.contains("\"memory_context\""));
        assert!(raw.contains("\"memory\""));
        assert!(!raw.contains("SECRET_MEMORY_FACT_SHOULD_NOT_APPEAR"));
    }

    fn assert_item_before(items: &[ContextItemSpec], earlier: ContextItemId, later: ContextItemId) {
        let earlier_index = items
            .iter()
            .position(|item| item.id == earlier)
            .unwrap_or_else(|| panic!("missing {}", earlier.as_str()));
        let later_index = items
            .iter()
            .position(|item| item.id == later)
            .unwrap_or_else(|| panic!("missing {}", later.as_str()));
        assert!(
            earlier_index < later_index,
            "{} should appear before {}",
            earlier.as_str(),
            later.as_str()
        );
    }

    fn assert_context_item_shape(item: ContextItemSpec) {
        match item.id {
            ContextItemId::KernelPolicy => {
                assert_eq!(item.class, ContextClass::Kernel);
                assert_eq!(
                    item.source,
                    ContextSource::Generated(GENERATED_KERNEL_POLICY)
                );
                assert!(item.required);
            }
            ContextItemId::WorkspaceRules => {
                assert_eq!(item.class, ContextClass::WorkspaceRules);
                assert_eq!(item.source, ContextSource::WorkspaceFile(AGENTS_FILE));
                assert!(!item.required);
            }
            ContextItemId::SafeWorkspaceRules => {
                assert_eq!(item.class, ContextClass::WorkspaceRules);
                assert_eq!(
                    item.source,
                    ContextSource::Generated(GENERATED_SAFE_WORKSPACE_RULES)
                );
                assert!(item.required);
            }
            ContextItemId::MemoryContext => {
                assert_eq!(item.class, ContextClass::Memory);
                assert_eq!(item.source, ContextSource::MemoryProjection);
                assert!(!item.required);
            }
            ContextItemId::ActiveContinuity => {
                assert_eq!(item.class, ContextClass::ActiveContinuity);
                assert_eq!(
                    item.source,
                    ContextSource::ContinuityFile(ACTIVE_CONTEXT_FILE)
                );
                assert!(!item.required);
            }
            ContextItemId::SessionHandoff => {
                assert_eq!(item.class, ContextClass::SessionHandoff);
                assert_eq!(item.source, ContextSource::CompactionSummary);
                assert!(!item.required);
            }
            ContextItemId::RecentTranscript => {
                assert_eq!(item.class, ContextClass::Transcript);
                assert_eq!(item.source, ContextSource::TranscriptTail);
                assert!(!item.required);
            }
            ContextItemId::RuntimeSessionNote => {
                assert_eq!(item.class, ContextClass::RuntimeNote);
                assert_eq!(
                    item.source,
                    ContextSource::Generated(GENERATED_RUNTIME_SESSION_NOTE)
                );
                assert!(item.required);
            }
            ContextItemId::NativeTuiSessionNote => {
                assert_eq!(item.class, ContextClass::RuntimeNote);
                assert_eq!(
                    item.source,
                    ContextSource::Generated(GENERATED_NATIVE_TUI_SESSION_NOTE)
                );
                assert!(item.required);
            }
            ContextItemId::DraftOutputsNote => {
                assert_eq!(item.class, ContextClass::RuntimeNote);
                assert_eq!(
                    item.source,
                    ContextSource::Generated(GENERATED_DRAFT_OUTPUTS_NOTE)
                );
                assert!(!item.required);
            }
            ContextItemId::RuntimeSecretsNote => {
                assert_eq!(item.class, ContextClass::RuntimeNote);
                assert_eq!(
                    item.source,
                    ContextSource::Generated(GENERATED_RUNTIME_SECRETS_NOTE)
                );
                assert!(!item.required);
            }
            ContextItemId::UserInput => {
                assert_eq!(item.class, ContextClass::CurrentInput);
                assert_eq!(item.source, ContextSource::CurrentUserInput);
                assert!(item.required);
            }
        }
    }
}
