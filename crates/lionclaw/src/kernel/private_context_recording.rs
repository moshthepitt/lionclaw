use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::contracts::{SessionHistoryPolicy, TrustTier};

pub(crate) const PRIVATE_CONTEXT_RECORD_MAX_USER_TEXT_BYTES: usize = 16 * 1024;
pub(crate) const PRIVATE_CONTEXT_RECORD_MAX_ASSISTANT_TEXT_BYTES: usize = 32 * 1024;

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
    #[serde(rename = "attached_native_tui_turn")]
    AttachedNativeTui,
    #[serde(rename = "channel_turn")]
    Channel,
}

impl PrivateContextRecordSurface {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Program => "program_turn",
            Self::AttachedNativeTui => "attached_native_tui_turn",
            Self::Channel => "channel_turn",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct PrivateContextRecordTranscript {
    pub user: Option<PrivateContextRecordText>,
    pub assistant: Option<PrivateContextRecordText>,
}

impl PrivateContextRecordTranscript {
    pub(crate) fn from_committed_text(user_text: &str, assistant_text: &str) -> Self {
        Self {
            user: private_context_record_text(
                user_text,
                PRIVATE_CONTEXT_RECORD_MAX_USER_TEXT_BYTES,
            ),
            assistant: private_context_record_text(
                assistant_text,
                PRIVATE_CONTEXT_RECORD_MAX_ASSISTANT_TEXT_BYTES,
            ),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.user.is_none() && self.assistant.is_none()
    }

    pub(crate) fn included_bytes(&self) -> PrivateContextRecordByteCounts {
        PrivateContextRecordByteCounts {
            user_included_bytes: self
                .user
                .as_ref()
                .map(|text| text.included_bytes)
                .unwrap_or(0),
            user_original_bytes: self
                .user
                .as_ref()
                .map(|text| text.original_bytes)
                .unwrap_or(0),
            assistant_included_bytes: self
                .assistant
                .as_ref()
                .map(|text| text.included_bytes)
                .unwrap_or(0),
            assistant_original_bytes: self
                .assistant
                .as_ref()
                .map(|text| text.original_bytes)
                .unwrap_or(0),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PrivateContextRecordText {
    pub text: String,
    pub included_bytes: usize,
    pub original_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PrivateContextRecordByteCounts {
    pub user_included_bytes: usize,
    pub user_original_bytes: usize,
    pub assistant_included_bytes: usize,
    pub assistant_original_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PrivateContextRecordOutcome {
    pub status: PrivateContextRecordStatus,
    pub reason: Option<&'static str>,
    pub exit_status: Option<i32>,
    pub elapsed_ms: u64,
}

impl PrivateContextRecordOutcome {
    pub(crate) fn completed(elapsed_ms: u64) -> Self {
        Self {
            status: PrivateContextRecordStatus::Completed,
            reason: None,
            exit_status: Some(0),
            elapsed_ms,
        }
    }

    pub(crate) fn failed(reason: &'static str, exit_status: Option<i32>, elapsed_ms: u64) -> Self {
        Self {
            status: PrivateContextRecordStatus::Failed,
            reason: Some(reason),
            exit_status,
            elapsed_ms,
        }
    }

    pub(crate) fn invalid_output(
        reason: &'static str,
        exit_status: Option<i32>,
        elapsed_ms: u64,
    ) -> Self {
        Self {
            status: PrivateContextRecordStatus::InvalidOutput,
            reason: Some(reason),
            exit_status,
            elapsed_ms,
        }
    }

    pub(crate) fn timed_out(elapsed_ms: u64) -> Self {
        Self {
            status: PrivateContextRecordStatus::TimedOut,
            reason: Some("timed_out"),
            exit_status: None,
            elapsed_ms,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrivateContextRecordStatus {
    Completed,
    Failed,
    TimedOut,
    InvalidOutput,
}

impl PrivateContextRecordStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::InvalidOutput => "invalid_output",
        }
    }
}

fn private_context_record_text(text: &str, max_bytes: usize) -> Option<PrivateContextRecordText> {
    if text.trim().is_empty() {
        return None;
    }
    let capped = cap_utf8(text, max_bytes);
    Some(PrivateContextRecordText {
        included_bytes: capped.len(),
        original_bytes: text.len(),
        text: capped,
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

#[cfg(test)]
mod tests {
    use super::{
        PrivateContextRecordTranscript, PRIVATE_CONTEXT_RECORD_MAX_ASSISTANT_TEXT_BYTES,
        PRIVATE_CONTEXT_RECORD_MAX_USER_TEXT_BYTES,
    };

    #[test]
    fn transcript_omits_whitespace_only_roles() {
        let transcript = PrivateContextRecordTranscript::from_committed_text(" \n", "answer");

        assert!(transcript.user.is_none());
        assert_eq!(transcript.assistant.expect("assistant").text, "answer");
    }

    #[test]
    fn transcript_caps_utf8_text_and_reports_byte_counts() {
        let user = "a".repeat(PRIVATE_CONTEXT_RECORD_MAX_USER_TEXT_BYTES + 8);
        let assistant = format!(
            "{}é",
            "b".repeat(PRIVATE_CONTEXT_RECORD_MAX_ASSISTANT_TEXT_BYTES - 1)
        );

        let transcript = PrivateContextRecordTranscript::from_committed_text(&user, &assistant);
        let counts = transcript.included_bytes();

        assert_eq!(
            counts.user_included_bytes,
            PRIVATE_CONTEXT_RECORD_MAX_USER_TEXT_BYTES
        );
        assert_eq!(
            counts.user_original_bytes,
            PRIVATE_CONTEXT_RECORD_MAX_USER_TEXT_BYTES + 8
        );
        assert_eq!(
            counts.assistant_included_bytes,
            PRIVATE_CONTEXT_RECORD_MAX_ASSISTANT_TEXT_BYTES - 1
        );
        assert_eq!(
            counts.assistant_original_bytes,
            PRIVATE_CONTEXT_RECORD_MAX_ASSISTANT_TEXT_BYTES + 1
        );
    }
}
