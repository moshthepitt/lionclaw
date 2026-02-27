use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct AuditEvent {
    pub event_id: Uuid,
    pub event_type: String,
    pub session_id: Option<Uuid>,
    pub actor: Option<String>,
    pub details: Value,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Default)]
pub struct AuditLog {
    events: RwLock<Vec<AuditEvent>>,
}

impl AuditLog {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn append(
        &self,
        event_type: impl Into<String>,
        session_id: Option<Uuid>,
        actor: Option<String>,
        details: Value,
    ) {
        let event = AuditEvent {
            event_id: Uuid::new_v4(),
            event_type: event_type.into(),
            session_id,
            actor,
            details,
            timestamp: Utc::now(),
        };
        self.events.write().await.push(event);
    }

    pub async fn query(
        &self,
        session_id: Option<Uuid>,
        event_type: Option<String>,
        since: Option<DateTime<Utc>>,
        limit: Option<usize>,
    ) -> Vec<AuditEvent> {
        let limit = limit.unwrap_or(100).min(1000);
        let wanted_type = event_type.map(|v| v.to_lowercase());

        let events = self.events.read().await;
        let mut filtered = events
            .iter()
            .filter(|event| {
                if let Some(id) = session_id {
                    if event.session_id != Some(id) {
                        return false;
                    }
                }
                if let Some(ref et) = wanted_type {
                    if event.event_type.to_lowercase() != *et {
                        return false;
                    }
                }
                if let Some(ts) = since {
                    if event.timestamp < ts {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect::<Vec<_>>();

        filtered.sort_by_key(|event| event.timestamp);
        filtered.into_iter().rev().take(limit).collect()
    }
}
