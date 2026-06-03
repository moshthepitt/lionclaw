use serde_json::{json, Value};

use crate::{
    contracts::{SessionHistoryPolicy, TrustTier},
    workspace::{AGENTS_FILE, IDENTITY_FILE, SOUL_FILE, USER_FILE},
};

use super::continuity::MEMORY_FILE;

pub(crate) const PROMPT_CONTEXT_POLICY_VERSION: u32 = 1;

const MAIN_INTERACTIVE_TRANSCRIPT_TAIL: usize = 12;
const MAIN_CONSERVATIVE_TRANSCRIPT_TAIL: usize = 6;
const UNTRUSTED_INTERACTIVE_TRANSCRIPT_TAIL: usize = 4;
const UNTRUSTED_CONSERVATIVE_TRANSCRIPT_TAIL: usize = 2;

const MAIN_INTERACTIVE_TRANSCRIPT_BUDGET: usize = 12_288;
const MAIN_CONSERVATIVE_TRANSCRIPT_BUDGET: usize = 6_144;
const UNTRUSTED_INTERACTIVE_TRANSCRIPT_BUDGET: usize = 4_096;
const UNTRUSTED_CONSERVATIVE_TRANSCRIPT_BUDGET: usize = 2_048;
const AGENTS_MAIN_BUDGET: usize = 4096;
const IDENTITY_MAIN_BUDGET: usize = 2048;
const SOUL_MAIN_INTERACTIVE_BUDGET: usize = 1024;
const SOUL_MAIN_CONSERVATIVE_BUDGET: usize = 512;
const USER_MAIN_INTERACTIVE_BUDGET: usize = 2048;
const USER_MAIN_CONSERVATIVE_BUDGET: usize = 1024;
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
const CURRENT_INPUT_BUDGET: usize = usize::MAX;
const ACTIVE_CONTEXT_FILE: &str = "continuity/ACTIVE.md";

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
    OperatorPrivate,
    UserPrivate,
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
            Self::OperatorPrivate => "operator_private",
            Self::UserPrivate => "user_private",
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
    Identity,
    StyleProfile,
    UserContext,
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
            Self::Identity => "identity",
            Self::StyleProfile => "style_profile",
            Self::UserContext => "user_context",
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
pub(crate) enum GeneratedContextSource {
    KernelPolicy,
    SafeWorkspaceRules,
    RuntimeSessionNote,
    NativeTuiSessionNote,
    DraftOutputsNote,
    RuntimeSecretsNote,
}

impl GeneratedContextSource {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::KernelPolicy => "kernel_policy",
            Self::SafeWorkspaceRules => "safe_workspace_rules",
            Self::RuntimeSessionNote => "runtime_session_note",
            Self::NativeTuiSessionNote => "native_tui_session_note",
            Self::DraftOutputsNote => "draft_outputs_note",
            Self::RuntimeSecretsNote => "runtime_secrets_note",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkspaceContextFile {
    Agents,
    Identity,
    Soul,
    User,
}

impl WorkspaceContextFile {
    pub(crate) fn file_name(self) -> &'static str {
        match self {
            Self::Agents => AGENTS_FILE,
            Self::Identity => IDENTITY_FILE,
            Self::Soul => SOUL_FILE,
            Self::User => USER_FILE,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContinuityContextFile {
    Memory,
    Active,
}

impl ContinuityContextFile {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Memory => MEMORY_FILE,
            Self::Active => ACTIVE_CONTEXT_FILE,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContextSource {
    Generated(GeneratedContextSource),
    WorkspaceFile(WorkspaceContextFile),
    ContinuityFile(ContinuityContextFile),
    CompactionSummary,
    TranscriptTail,
    CurrentUserInput,
}

impl ContextSource {
    pub(crate) fn as_str(self) -> String {
        match self {
            Self::Generated(source) => format!("generated:{}", source.as_str()),
            Self::WorkspaceFile(file) => format!("workspace_file:{}", file.file_name()),
            Self::ContinuityFile(file) => format!("continuity_file:{}", file.as_str()),
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
                ContextClass::RuntimeNote if item.id == ContextItemId::RuntimeSecretsNote => {
                    Err(ExclusionDecision {
                        reason: "trust_tier_untrusted",
                    })
                }
                ContextClass::Kernel
                | ContextClass::ActiveContinuity
                | ContextClass::SessionHandoff
                | ContextClass::Transcript
                | ContextClass::RuntimeNote
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
            (TrustTier::Main, _, ContextItemId::Identity, _) => IDENTITY_MAIN_BUDGET,
            (
                TrustTier::Main,
                SessionHistoryPolicy::Interactive,
                ContextItemId::StyleProfile,
                _,
            ) => SOUL_MAIN_INTERACTIVE_BUDGET,
            (
                TrustTier::Main,
                SessionHistoryPolicy::Conservative,
                ContextItemId::StyleProfile,
                _,
            ) => SOUL_MAIN_CONSERVATIVE_BUDGET,
            (TrustTier::Main, SessionHistoryPolicy::Interactive, ContextItemId::UserContext, _) => {
                USER_MAIN_INTERACTIVE_BUDGET
            }
            (
                TrustTier::Main,
                SessionHistoryPolicy::Conservative,
                ContextItemId::UserContext,
                _,
            ) => USER_MAIN_CONSERVATIVE_BUDGET,
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
            source: ContextSource::Generated(GeneratedContextSource::KernelPolicy),
            required: true,
        },
        ContextItemSpec {
            id: ContextItemId::WorkspaceRules,
            title: AGENTS_FILE,
            class: ContextClass::WorkspaceRules,
            source: ContextSource::WorkspaceFile(WorkspaceContextFile::Agents),
            required: false,
        },
        ContextItemSpec {
            id: ContextItemId::SafeWorkspaceRules,
            title: "Workspace Rules",
            class: ContextClass::WorkspaceRules,
            source: ContextSource::Generated(GeneratedContextSource::SafeWorkspaceRules),
            required: true,
        },
        ContextItemSpec {
            id: ContextItemId::Identity,
            title: IDENTITY_FILE,
            class: ContextClass::OperatorPrivate,
            source: ContextSource::WorkspaceFile(WorkspaceContextFile::Identity),
            required: false,
        },
        ContextItemSpec {
            id: ContextItemId::StyleProfile,
            title: SOUL_FILE,
            class: ContextClass::OperatorPrivate,
            source: ContextSource::WorkspaceFile(WorkspaceContextFile::Soul),
            required: false,
        },
        ContextItemSpec {
            id: ContextItemId::UserContext,
            title: USER_FILE,
            class: ContextClass::UserPrivate,
            source: ContextSource::WorkspaceFile(WorkspaceContextFile::User),
            required: false,
        },
        ContextItemSpec {
            id: ContextItemId::MemoryContext,
            title: MEMORY_FILE,
            class: ContextClass::Memory,
            source: ContextSource::ContinuityFile(ContinuityContextFile::Memory),
            required: false,
        },
        ContextItemSpec {
            id: ContextItemId::ActiveContinuity,
            title: ACTIVE_CONTEXT_FILE,
            class: ContextClass::ActiveContinuity,
            source: ContextSource::ContinuityFile(ContinuityContextFile::Active),
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
            items.push(transcript_item());
            items.push(current_input_item());
        }
        PromptContextMode::ProgramResumePrimary => {
            items.push(ContextItemSpec {
                id: ContextItemId::RuntimeSessionNote,
                title: "Runtime Session",
                class: ContextClass::RuntimeNote,
                source: ContextSource::Generated(GeneratedContextSource::RuntimeSessionNote),
                required: true,
            });
            items.push(transcript_item());
            items.push(current_input_item());
        }
        PromptContextMode::AttachedNativeTui => {
            items.push(ContextItemSpec {
                id: ContextItemId::NativeTuiSessionNote,
                title: "Native Runtime TUI Session",
                class: ContextClass::RuntimeNote,
                source: ContextSource::Generated(GeneratedContextSource::NativeTuiSessionNote),
                required: true,
            });
            items.push(transcript_item());
        }
    }

    items.push(ContextItemSpec {
        id: ContextItemId::DraftOutputsNote,
        title: "Draft Outputs",
        class: ContextClass::RuntimeNote,
        source: ContextSource::Generated(GeneratedContextSource::DraftOutputsNote),
        required: false,
    });
    items.push(ContextItemSpec {
        id: ContextItemId::RuntimeSecretsNote,
        title: "Runtime Secrets",
        class: ContextClass::RuntimeNote,
        source: ContextSource::Generated(GeneratedContextSource::RuntimeSecretsNote),
        required: false,
    });

    items
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

    let capped_at = content[..limit]
        .rfind('\n')
        .filter(|idx| *idx > 0)
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
    fn cap_uses_newline_boundary() {
        let capped = cap_utf8_at_line_boundary("first\nsecond\nthird", 12);
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
    fn main_interactive_policy_allows_private_context() {
        let policy = PromptContextPolicy::new(
            TrustTier::Main,
            SessionHistoryPolicy::Interactive,
            PromptContextMode::ProgramPrimary,
            "codex",
        );
        assert_eq!(policy.transcript_tail_limit, 12);
        let user = context_item_specs(PromptContextMode::ProgramPrimary)
            .into_iter()
            .find(|item| item.id == ContextItemId::UserContext)
            .expect("user context item");
        assert!(policy.allows(&user).is_ok());
        assert_eq!(policy.max_bytes(&user), USER_MAIN_INTERACTIVE_BUDGET);
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
        for id in [
            ContextItemId::WorkspaceRules,
            ContextItemId::Identity,
            ContextItemId::StyleProfile,
            ContextItemId::UserContext,
            ContextItemId::MemoryContext,
        ] {
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
        let private = items
            .iter()
            .find(|item| item.id == ContextItemId::UserContext)
            .expect("user context");

        assert_eq!(interactive.transcript_tail_limit, 4);
        assert_eq!(conservative.transcript_tail_limit, 2);
        assert!(conservative.max_bytes(active) < interactive.max_bytes(active));
        assert!(conservative.max_bytes(handoff) < interactive.max_bytes(handoff));
        assert!(conservative.allows(private).is_err());
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
            .find(|item| item.id == ContextItemId::UserContext)
            .expect("user context");
        audit.include(
            &item,
            &CappedSection {
                content: "SECRET_USER_FACT_SHOULD_NOT_APPEAR".to_string(),
                included_bytes: 32,
                original_bytes: 64,
                was_capped: true,
            },
        );
        let details = audit.to_details_json();
        let raw = serde_json::to_string(&details).expect("serialize audit");
        assert!(raw.contains("\"user_context\""));
        assert!(raw.contains("\"user_private\""));
        assert!(!raw.contains("SECRET_USER_FACT_SHOULD_NOT_APPEAR"));
    }
}
