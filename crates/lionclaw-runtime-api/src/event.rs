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
    Configuration {
        configuration: crate::AppliedRuntimeConfiguration,
    },
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
        Self {
            event: bounded_runtime_event(event),
            raw: None,
        }
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

pub const RUNTIME_TURN_JOURNAL_CAPACITY: usize = 64;
const RUNTIME_EVENT_ITEM_LIMIT: usize = 64;
pub type RuntimeTurnJournalSender = mpsc::Sender<TurnEvent>;
pub type RuntimeEventSender = mpsc::UnboundedSender<RuntimeEvent>;

fn bounded_runtime_event(event: RuntimeEvent) -> RuntimeEvent {
    let bounded_optional =
        |value: Option<String>| value.map(|value| crate::failure::bounded_text(&value));
    match event {
        RuntimeEvent::Configuration { mut configuration } => {
            configuration.requested_model = bounded_optional(configuration.requested_model);
            configuration.applied_model = bounded_optional(configuration.applied_model);
            configuration.requested_mode = bounded_optional(configuration.requested_mode);
            configuration.applied_mode = bounded_optional(configuration.applied_mode);
            RuntimeEvent::Configuration { configuration }
        }
        RuntimeEvent::MessageDelta { lane, text } => RuntimeEvent::MessageDelta {
            lane,
            text: crate::failure::bounded_text(&text),
        },
        RuntimeEvent::MessageBoundary { lane } => RuntimeEvent::MessageBoundary { lane },
        RuntimeEvent::Status { code, text } => RuntimeEvent::Status {
            code: bounded_optional(code),
            text: crate::failure::bounded_text(&text),
        },
        RuntimeEvent::Artifact { mut artifact } => {
            artifact.artifact_id = crate::failure::bounded_text(&artifact.artifact_id);
            artifact.path = PathBuf::from(crate::failure::bounded_text(
                &artifact.path.to_string_lossy(),
            ));
            artifact.filename = bounded_optional(artifact.filename);
            artifact.mime_type = bounded_optional(artifact.mime_type);
            RuntimeEvent::Artifact { artifact }
        }
        RuntimeEvent::FileChange { mut change } => {
            change.runtime = crate::failure::bounded_text(&change.runtime);
            change.operation_id = bounded_optional(change.operation_id);
            change.paths.truncate(RUNTIME_EVENT_ITEM_LIMIT);
            for path in &mut change.paths {
                *path = crate::failure::bounded_text(path);
            }
            RuntimeEvent::FileChange { change }
        }
        RuntimeEvent::Done => RuntimeEvent::Done,
        RuntimeEvent::Error { code, text } => RuntimeEvent::Error {
            code: bounded_optional(code),
            text: crate::failure::bounded_text(&text),
        },
    }
}

pub fn append_streamed_text_delta(existing: &mut String, delta: &str) {
    let remaining = crate::failure::FAILURE_TEXT_LIMIT.saturating_sub(existing.len());
    let mut end = remaining.min(delta.len());
    while !delta.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    existing.push_str(&delta[..end]);
}

pub fn append_streamed_text_boundary(existing: &mut String) {
    if existing.trim().is_empty() || existing.ends_with("\n\n") {
        return;
    }
    if existing.ends_with('\n') {
        append_streamed_text_delta(existing, "\n");
    } else {
        append_streamed_text_delta(existing, "\n\n");
    }
}

pub fn observe_final_response(existing: &mut String, event: &RuntimeEvent) {
    match event {
        RuntimeEvent::MessageDelta {
            lane: RuntimeMessageLane::Answer,
            text,
        } => append_streamed_text_delta(existing, text),
        RuntimeEvent::MessageBoundary {
            lane: RuntimeMessageLane::Answer,
        } => append_streamed_text_boundary(existing),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn runtime_journal_ingress_is_bounded_and_lossless() {
        let (journal, mut receiver) = mpsc::channel(RUNTIME_TURN_JOURNAL_CAPACITY);
        for _ in 0..RUNTIME_TURN_JOURNAL_CAPACITY {
            journal
                .try_send(TurnEvent::canonical(RuntimeEvent::Done))
                .expect("journal has its declared capacity");
        }
        assert!(matches!(
            journal.try_send(TurnEvent::canonical(RuntimeEvent::Done)),
            Err(mpsc::error::TrySendError::Full(_))
        ));
        let blocked = tokio::spawn({
            let journal = journal.clone();
            async move {
                journal
                    .send(TurnEvent::canonical(RuntimeEvent::Done))
                    .await
                    .unwrap()
            }
        });
        tokio::task::yield_now().await;
        assert!(
            !blocked.is_finished(),
            "a full evidence journal backpressures instead of dropping"
        );
        receiver.recv().await.unwrap();
        blocked.await.unwrap();
        let mut retained = 0;
        while receiver.try_recv().is_ok() {
            retained += 1;
        }
        assert_eq!(retained, RUNTIME_TURN_JOURNAL_CAPACITY);
    }

    #[test]
    fn final_response_assembly_never_exceeds_its_utf8_byte_bound() {
        let mut response = String::new();
        append_streamed_text_delta(
            &mut response,
            &"é".repeat(crate::failure::FAILURE_TEXT_LIMIT),
        );
        append_streamed_text_boundary(&mut response);
        assert!(response.len() <= crate::failure::FAILURE_TEXT_LIMIT);
        assert!(response.is_char_boundary(response.len()));
    }

    #[test]
    fn canonical_journal_records_bound_provider_controlled_content() {
        let event = TurnEvent::canonical(RuntimeEvent::FileChange {
            change: RuntimeFileChange {
                runtime: "r".repeat(crate::failure::FAILURE_TEXT_LIMIT * 2),
                operation_id: Some("o".repeat(crate::failure::FAILURE_TEXT_LIMIT * 2)),
                status: RuntimeFileChangeStatus::Editing,
                paths: (0..RUNTIME_EVENT_ITEM_LIMIT * 2)
                    .map(|_| "p".repeat(crate::failure::FAILURE_TEXT_LIMIT * 2))
                    .collect(),
                total_count: RUNTIME_EVENT_ITEM_LIMIT * 2,
            },
        });
        let RuntimeEvent::FileChange { change } = event.event else {
            panic!("expected file-change event");
        };
        assert!(change.runtime.len() <= crate::failure::FAILURE_TEXT_LIMIT);
        assert!(change.operation_id.unwrap().len() <= crate::failure::FAILURE_TEXT_LIMIT);
        assert_eq!(change.paths.len(), RUNTIME_EVENT_ITEM_LIMIT);
        assert!(change
            .paths
            .iter()
            .all(|path| path.len() <= crate::failure::FAILURE_TEXT_LIMIT));
        assert_eq!(change.total_count, RUNTIME_EVENT_ITEM_LIMIT * 2);
    }
}
