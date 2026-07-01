use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeMessageLane {
    Answer,
    Reasoning,
}

impl RuntimeMessageLane {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Answer => "answer",
            Self::Reasoning => "reasoning",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeArtifact {
    pub artifact_id: String,
    pub path: PathBuf,
    pub filename: Option<String>,
    pub mime_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeFileChangeStatus {
    Editing,
    Edited,
    Failed,
    Declined,
    Changed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeFileChange {
    pub runtime: String,
    pub operation_id: Option<String>,
    pub status: RuntimeFileChangeStatus,
    pub paths: Vec<String>,
    pub total_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeEvent {
    MessageDelta {
        lane: RuntimeMessageLane,
        text: String,
    },
    MessageBoundary {
        lane: RuntimeMessageLane,
    },
    Status {
        code: Option<String>,
        text: String,
    },
    Artifact {
        artifact: RuntimeArtifact,
    },
    FileChange {
        change: RuntimeFileChange,
    },
    Done,
    Error {
        code: Option<String>,
        text: String,
    },
}

/// Raw, driver-specific payload retained alongside a canonical event for
/// debugging. Retention is debug-only: it is never parsed back into canonical
/// text or replayed into a prompt. Only the paired [`RuntimeEvent`] is canonical.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawTurnPayload {
    /// Driver/protocol that produced the payload, e.g. `"driver-protocol"`.
    pub driver: String,
    /// The payload exactly as the driver emitted it (e.g. one JSON-RPC line).
    pub payload: String,
}

/// One record in a runtime turn's canonical journal.
///
/// A protocol driver translates each harness message into journal records.
/// `event` is the canonical, public output LionClaw persists, replays, and
/// shows operators. `raw`, when present, retains the originating driver payload
/// for debugging only and is excluded from every canonical projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnEvent {
    pub event: RuntimeEvent,
    pub raw: Option<RawTurnPayload>,
}

impl TurnEvent {
    /// A record for a kernel-synthesized event with no retained raw payload.
    pub fn canonical(event: RuntimeEvent) -> Self {
        Self { event, raw: None }
    }

    /// A record retaining the raw driver payload the event was derived from.
    pub fn with_raw(event: RuntimeEvent, raw: RawTurnPayload) -> Self {
        Self {
            event,
            raw: Some(raw),
        }
    }
}

/// Project a canonical journal to its public [`RuntimeEvent`] stream, dropping
/// every retained raw payload. This is the only sanctioned way to derive
/// operator-visible output from a journal: raw retention never contributes.
pub fn canonical_events(journal: &[TurnEvent]) -> impl Iterator<Item = &RuntimeEvent> {
    journal.iter().map(|record| &record.event)
}

pub type RuntimeTurnJournalSender = mpsc::UnboundedSender<TurnEvent>;
pub type RuntimeEventSender = mpsc::UnboundedSender<RuntimeEvent>;

pub fn append_streamed_text_delta(existing: &mut String, delta: &str) {
    existing.push_str(delta);
}

pub fn append_streamed_text_boundary(existing: &mut String) {
    if existing.trim().is_empty() || existing.ends_with("\n\n") {
        return;
    }
    if existing.ends_with('\n') {
        existing.push('\n');
    } else {
        existing.push_str("\n\n");
    }
}
