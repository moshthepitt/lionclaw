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
        let reason = normalize_cancel_reason(reason);
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
        let mut rx = self.tx.subscribe();
        loop {
            let reason = rx.borrow().clone();
            if let Some(reason) = reason {
                return reason;
            }
            if rx.changed().await.is_err() {
                return DEFAULT_CANCEL_REASON.to_string();
            }
        }
    }
}

pub(crate) fn normalize_cancel_reason(reason: impl Into<String>) -> String {
    let reason = reason.into();
    let reason = reason.trim();
    if reason.is_empty() {
        DEFAULT_CANCEL_REASON.to_string()
    } else {
        reason.chars().take(512).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancelled_returns_already_requested_reason() {
        let cancellation = TurnCancellation::new();
        assert!(cancellation.request("operator stop"));

        let reason = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            cancellation.cancelled(),
        )
        .await
        .expect("already requested cancellation should not wait");

        assert_eq!(reason, "operator stop");
    }

    #[tokio::test]
    async fn cancelled_waits_for_future_request() {
        let cancellation = TurnCancellation::new();
        let waiter = cancellation.clone();
        let task = tokio::spawn(async move { waiter.cancelled().await });

        tokio::task::yield_now().await;
        assert!(cancellation.request("operator stop"));

        let reason = tokio::time::timeout(std::time::Duration::from_millis(100), task)
            .await
            .expect("future cancellation request should wake waiter")
            .expect("waiter task should complete");

        assert_eq!(reason, "operator stop");
    }
}
