use tokio::sync::watch;

const DEFAULT_CANCEL_REASON: &str = "turn cancellation requested";

#[derive(Debug, Clone)]
pub struct TurnCancellation {
    tx: watch::Sender<Option<String>>,
}

impl Default for TurnCancellation {
    fn default() -> Self {
        let (tx, _rx) = watch::channel(None);
        Self { tx }
    }
}

impl TurnCancellation {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn request(&self, reason: impl Into<String>) -> bool {
        let reason = normalize_cancel_reason(reason.into());
        if self.reason().is_some() {
            return false;
        }
        self.tx.send_if_modified(|current| {
            if current.is_some() {
                false
            } else {
                *current = Some(reason);
                true
            }
        })
    }

    pub fn reason(&self) -> Option<String> {
        self.tx.borrow().clone()
    }

    pub async fn cancelled(&self) -> String {
        if let Some(reason) = self.reason() {
            return reason;
        }

        let mut rx = self.tx.subscribe();
        loop {
            if rx.changed().await.is_err() {
                return DEFAULT_CANCEL_REASON.to_string();
            }
            if let Some(reason) = rx.borrow().clone() {
                return reason;
            }
        }
    }
}

fn normalize_cancel_reason(reason: String) -> String {
    let reason = reason.trim();
    if reason.is_empty() {
        DEFAULT_CANCEL_REASON.to_string()
    } else {
        reason.chars().take(512).collect()
    }
}
