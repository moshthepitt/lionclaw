use lionclaw_runtime_api::{
    RawTurnPayload, RuntimeEvent, RuntimeEventSender, RuntimeTurnJournalSender, TurnEvent,
};

const CODEX_APP_SERVER_DRIVER: &str = "codex-app-server";

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

    pub(crate) fn send(self, event: RuntimeEvent, raw_payload: &str) {
        match self {
            Self::Runtime(events) => {
                drop(events.send(event));
            }
            Self::Journal(journal) => {
                drop(journal.send(TurnEvent::with_raw(
                    event,
                    RawTurnPayload {
                        driver: CODEX_APP_SERVER_DRIVER.to_string(),
                        payload: raw_payload.to_string(),
                    },
                )));
            }
        }
    }
}

impl<'a> From<&'a RuntimeEventSender> for CodexAppServerEventSink<'a> {
    fn from(events: &'a RuntimeEventSender) -> Self {
        Self::runtime(events)
    }
}
