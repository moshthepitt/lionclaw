use std::collections::HashMap;

use chrono::{DateTime, Utc};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::contracts::TrustTier;

#[derive(Debug, Clone)]
pub struct Session {
    pub session_id: Uuid,
    pub channel_id: String,
    pub peer_id: String,
    pub trust_tier: TrustTier,
    pub created_at: DateTime<Utc>,
    pub last_turn_at: Option<DateTime<Utc>>,
    pub turn_count: u64,
}

#[derive(Debug, Default)]
pub struct SessionStore {
    sessions: RwLock<HashMap<Uuid, Session>>,
}

impl SessionStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn open(
        &self,
        channel_id: String,
        peer_id: String,
        trust_tier: TrustTier,
    ) -> Session {
        let session = Session {
            session_id: Uuid::new_v4(),
            channel_id,
            peer_id,
            trust_tier,
            created_at: Utc::now(),
            last_turn_at: None,
            turn_count: 0,
        };

        self.sessions
            .write()
            .await
            .insert(session.session_id, session.clone());

        session
    }

    pub async fn get(&self, session_id: Uuid) -> Option<Session> {
        self.sessions.read().await.get(&session_id).cloned()
    }

    pub async fn record_turn(&self, session_id: Uuid) -> Option<Session> {
        let mut sessions = self.sessions.write().await;
        let session = sessions.get_mut(&session_id)?;
        session.turn_count += 1;
        session.last_turn_at = Some(Utc::now());
        Some(session.clone())
    }
}
