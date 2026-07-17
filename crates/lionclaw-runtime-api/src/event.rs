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

/// One record in a runtime turn's canonical journal.
///
/// A protocol driver translates each harness message into this bounded public
/// form. The field is private so adapter implementations cannot bypass the
/// canonical ingress constructor with an unbounded provider payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnEvent {
    event: RuntimeEvent,
}

impl TurnEvent {
    pub fn canonical(event: RuntimeEvent) -> Self {
        Self {
            event: bounded_runtime_event(event),
        }
    }

    pub fn event(&self) -> &RuntimeEvent {
        &self.event
    }

    pub fn into_event(self) -> RuntimeEvent {
        self.event
    }
}

/// Project a canonical journal to its public [`RuntimeEvent`] stream.
pub fn canonical_events(journal: &[TurnEvent]) -> impl Iterator<Item = &RuntimeEvent> {
    journal.iter().map(|record| &record.event)
}

pub const RUNTIME_TURN_JOURNAL_CAPACITY: usize = 64;
const RUNTIME_EVENT_ITEM_LIMIT: usize = 64;
pub type RuntimeTurnJournalSender = mpsc::Sender<TurnEvent>;
pub type RuntimeEventSender = mpsc::UnboundedSender<RuntimeEvent>;

fn bounded_runtime_event(event: RuntimeEvent) -> RuntimeEvent {
    let bounded_optional = |value: Option<String>| value.map(|value| crate::bounded_text(&value));
    match event {
        RuntimeEvent::Configuration { configuration } => RuntimeEvent::Configuration {
            configuration: configuration.projected(),
        },
        RuntimeEvent::MessageDelta { lane, text } => RuntimeEvent::MessageDelta {
            lane,
            text: crate::bounded_text(&text),
        },
        RuntimeEvent::MessageBoundary { lane } => RuntimeEvent::MessageBoundary { lane },
        RuntimeEvent::Status { code, text } => RuntimeEvent::Status {
            code: bounded_optional(code),
            text: crate::bounded_text(&text),
        },
        RuntimeEvent::Artifact { mut artifact } => {
            artifact.artifact_id = crate::bounded_text(&artifact.artifact_id);
            artifact.path = PathBuf::from(crate::bounded_text(&artifact.path.to_string_lossy()));
            artifact.filename = bounded_optional(artifact.filename);
            artifact.mime_type = bounded_optional(artifact.mime_type);
            RuntimeEvent::Artifact { artifact }
        }
        RuntimeEvent::FileChange { mut change } => {
            change.runtime = crate::bounded_text(&change.runtime);
            change.operation_id = bounded_optional(change.operation_id);
            change.paths.truncate(RUNTIME_EVENT_ITEM_LIMIT);
            for path in &mut change.paths {
                *path = crate::bounded_text(path);
            }
            RuntimeEvent::FileChange { change }
        }
        RuntimeEvent::Done => RuntimeEvent::Done,
        RuntimeEvent::Error { code, text } => RuntimeEvent::Error {
            code: bounded_optional(code),
            text: crate::bounded_text(&text),
        },
    }
}

pub fn append_streamed_text_delta(existing: &mut String, delta: &str) {
    let remaining = crate::FAILURE_TEXT_LIMIT.saturating_sub(existing.len());
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
        append_streamed_text_delta(&mut response, &"é".repeat(crate::FAILURE_TEXT_LIMIT));
        append_streamed_text_boundary(&mut response);
        assert!(response.len() <= crate::FAILURE_TEXT_LIMIT);
        assert!(response.is_char_boundary(response.len()));
    }

    #[test]
    fn canonical_journal_records_bound_provider_controlled_content() {
        let event = TurnEvent::canonical(RuntimeEvent::FileChange {
            change: RuntimeFileChange {
                runtime: "r".repeat(crate::FAILURE_TEXT_LIMIT * 2),
                operation_id: Some("o".repeat(crate::FAILURE_TEXT_LIMIT * 2)),
                status: RuntimeFileChangeStatus::Editing,
                paths: (0..RUNTIME_EVENT_ITEM_LIMIT * 2)
                    .map(|_| "p".repeat(crate::FAILURE_TEXT_LIMIT * 2))
                    .collect(),
                total_count: RUNTIME_EVENT_ITEM_LIMIT * 2,
            },
        });
        let RuntimeEvent::FileChange { change } = event.event else {
            panic!("expected file-change event");
        };
        assert!(change.runtime.len() <= crate::FAILURE_TEXT_LIMIT);
        assert!(change.operation_id.unwrap().len() <= crate::FAILURE_TEXT_LIMIT);
        assert_eq!(change.paths.len(), RUNTIME_EVENT_ITEM_LIMIT);
        assert!(change
            .paths
            .iter()
            .all(|path| path.len() <= crate::FAILURE_TEXT_LIMIT));
        assert_eq!(change.total_count, RUNTIME_EVENT_ITEM_LIMIT * 2);
    }
}
