mod codex;
mod mock;
mod opencode;
mod state_file;

use chrono::{DateTime, TimeZone, Utc};

use super::RuntimeTerminalTranscriptState;

pub use codex::{CodexRuntimeAdapter, CodexRuntimeConfig};
pub use mock::MockRuntimeAdapter;
pub use opencode::{OpenCodeRuntimeAdapter, OpenCodeRuntimeConfig};

#[derive(Debug, Clone, PartialEq, Eq)]
struct TerminalTranscriptCandidate {
    id: String,
    updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy)]
enum TerminalTranscriptTimestampPrecision {
    Seconds,
    Milliseconds,
}

impl TerminalTranscriptCandidate {
    fn new(id: impl Into<String>, updated_at: Option<DateTime<Utc>>) -> Option<Self> {
        let id = id.into();
        let id = id.trim();
        if id.is_empty() {
            return None;
        }
        Some(Self {
            id: id.to_string(),
            updated_at,
        })
    }
}

#[derive(Debug, Default)]
struct TerminalTranscriptTarget {
    id: Option<String>,
    reconciled: bool,
    resumable: bool,
}

impl TerminalTranscriptTarget {
    fn is_empty(&self) -> bool {
        self.id.is_none()
    }

    fn unreconciled_id(&self) -> Option<&str> {
        if self.reconciled() {
            return None;
        }
        self.id.as_deref()
    }

    fn choose_if_empty(&mut self, id: &str) -> bool {
        if self.id.is_some() {
            return false;
        }
        self.id = Some(id.to_string());
        true
    }

    fn record_reconciliation(&mut self, id: &str, reconciled: bool, resumable: bool) {
        if self.id.as_deref() == Some(id) {
            self.reconciled = reconciled;
            self.resumable = reconciled && resumable;
        }
    }

    fn resumable(&self) -> bool {
        self.reconciled && self.resumable
    }

    fn reconciled(&self) -> bool {
        self.id.is_none() || self.reconciled
    }

    fn transcript_state(
        &self,
        source_selection_reconciled: bool,
    ) -> RuntimeTerminalTranscriptState {
        RuntimeTerminalTranscriptState::new(
            source_selection_reconciled && self.reconciled(),
            self.resumable(),
        )
    }
}

fn choose_terminal_transcript_target(
    linked_id: Option<&str>,
    latest: Option<&TerminalTranscriptCandidate>,
    launch_started_at: Option<DateTime<Utc>>,
) -> Option<String> {
    if let Some(linked_id) = linked_id.and_then(|id| {
        let id = id.trim();
        if id.is_empty() {
            None
        } else {
            Some(id)
        }
    }) {
        if let (Some(latest), Some(launch_started_at)) = (latest, launch_started_at) {
            if latest.id != linked_id
                && latest
                    .updated_at
                    .is_some_and(|updated_at| updated_at >= launch_started_at)
            {
                return Some(latest.id.clone());
            }
        }
        return Some(linked_id.to_string());
    }

    latest.map(|candidate| candidate.id.clone())
}

fn normalize_terminal_transcript_launch_started_at(
    launch_started_at: Option<DateTime<Utc>>,
    precision: TerminalTranscriptTimestampPrecision,
) -> Option<DateTime<Utc>> {
    let started_at = launch_started_at?;
    match precision {
        TerminalTranscriptTimestampPrecision::Seconds => {
            DateTime::<Utc>::from_timestamp(started_at.timestamp(), 0)
        }
        TerminalTranscriptTimestampPrecision::Milliseconds => Utc
            .timestamp_millis_opt(started_at.timestamp_millis())
            .single(),
    }
}
