use lionclaw_runtime_api::{RuntimeEvent, RuntimeEventSender, RuntimeTurnJournalSender, TurnEvent};

#[derive(Clone, Copy)]
pub(crate) enum CodexAppServerEventSink<'a> {
    Runtime(&'a RuntimeEventSender),
    Journal(&'a RuntimeTurnJournalSender),
}

impl<'a> CodexAppServerEventSink<'a> {
    pub(crate) fn runtime(events: &'a RuntimeEventSender) -> Self {
        Self::Runtime(events)
    }

    pub(crate) fn journal(journal: &'a RuntimeTurnJournalSender) -> Self {
        Self::Journal(journal)
    }

    pub(crate) async fn send(self, event: RuntimeEvent) {
        match self {
            Self::Runtime(events) => {
                drop(events.send(event));
            }
            Self::Journal(journal) => {
                drop(journal.send(TurnEvent::canonical(event)).await);
            }
        }
    }
}

impl<'a> From<&'a RuntimeEventSender> for CodexAppServerEventSink<'a> {
    fn from(events: &'a RuntimeEventSender) -> Self {
        Self::runtime(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lionclaw_runtime_api::{RuntimeMessageLane, RUNTIME_TURN_JOURNAL_CAPACITY};

    #[tokio::test]
    async fn production_journal_sink_backpressures_a_provider_flood_without_loss() {
        let (journal, mut receiver) = tokio::sync::mpsc::channel(RUNTIME_TURN_JOURNAL_CAPACITY);
        let expected = RUNTIME_TURN_JOURNAL_CAPACITY * 4;
        let drain = tokio::spawn(async move {
            let mut received = 0;
            while receiver.recv().await.is_some() {
                received += 1;
            }
            received
        });
        let sink = CodexAppServerEventSink::journal(&journal);
        for sequence in 0..expected {
            sink.send(RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text: sequence.to_string(),
            })
            .await;
        }
        drop(journal);
        assert_eq!(drain.await.unwrap(), expected);
    }
}
