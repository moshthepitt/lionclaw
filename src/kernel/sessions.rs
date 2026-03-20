use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use sqlx::{sqlite::SqliteRow, Row, SqlitePool};
use uuid::Uuid;

use crate::{
    contracts::{SessionHistoryPolicy, TrustTier},
    kernel::db::{ms_to_datetime, now_ms},
};

#[derive(Debug, Clone)]
pub struct Session {
    pub session_id: Uuid,
    pub channel_id: String,
    pub peer_id: String,
    pub trust_tier: TrustTier,
    pub history_policy: SessionHistoryPolicy,
    pub created_at: DateTime<Utc>,
    pub last_turn_at: Option<DateTime<Utc>>,
    pub turn_count: u64,
}

#[derive(Debug, Clone)]
pub struct SessionStore {
    pool: SqlitePool,
}

impl SessionStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn open(
        &self,
        channel_id: String,
        peer_id: String,
        trust_tier: TrustTier,
        history_policy: SessionHistoryPolicy,
    ) -> Result<Session> {
        let session_id = Uuid::new_v4();
        let created_at_ms = now_ms();

        sqlx::query(
            "INSERT INTO sessions \
             (session_id, channel_id, peer_id, trust_tier, history_policy, created_at_ms, last_turn_at_ms, turn_count) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, 0)",
        )
        .bind(session_id.to_string())
        .bind(&channel_id)
        .bind(&peer_id)
        .bind(trust_tier.as_str())
        .bind(history_policy.as_str())
        .bind(created_at_ms)
        .execute(&self.pool)
        .await
        .context("failed to insert session")?;

        self.get(session_id)
            .await?
            .ok_or_else(|| anyhow!("session disappeared immediately after insert"))
    }

    pub async fn get(&self, session_id: Uuid) -> Result<Option<Session>> {
        let row = sqlx::query(
            "SELECT session_id, channel_id, peer_id, trust_tier, history_policy, created_at_ms, last_turn_at_ms, turn_count \
             FROM sessions \
             WHERE session_id = ?1",
        )
        .bind(session_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .context("failed to query session")?;

        row.map(map_session_row).transpose()
    }

    pub async fn find_latest_by_channel_peer(
        &self,
        channel_id: &str,
        peer_id: &str,
    ) -> Result<Option<Session>> {
        let row = sqlx::query(
            "SELECT session_id, channel_id, peer_id, trust_tier, history_policy, created_at_ms, last_turn_at_ms, turn_count \
             FROM sessions \
             WHERE channel_id = ?1 AND peer_id = ?2 \
             ORDER BY created_at_ms DESC \
             LIMIT 1",
        )
        .bind(channel_id)
        .bind(peer_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to query latest session by channel and peer")?;

        row.map(map_session_row).transpose()
    }

    pub async fn record_turn(&self, session_id: Uuid) -> Result<Option<Session>> {
        let now = now_ms();
        let updated = sqlx::query(
            "UPDATE sessions \
             SET turn_count = turn_count + 1, last_turn_at_ms = ?2 \
             WHERE session_id = ?1",
        )
        .bind(session_id.to_string())
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to update session turn count")?;

        if updated.rows_affected() == 0 {
            return Ok(None);
        }

        self.get(session_id).await
    }
}

fn map_session_row(row: SqliteRow) -> Result<Session> {
    let session_id_raw: String = row.get("session_id");
    let trust_tier_raw: String = row.get("trust_tier");
    let history_policy_raw: String = row.get("history_policy");
    let created_at_ms: i64 = row.get("created_at_ms");
    let last_turn_at_ms: Option<i64> = row.get("last_turn_at_ms");
    let turn_count_raw: i64 = row.get("turn_count");

    let session_id = Uuid::parse_str(&session_id_raw)
        .with_context(|| format!("invalid uuid '{}'", session_id_raw))?;
    let trust_tier = TrustTier::from_str(&trust_tier_raw)
        .map_err(|err| anyhow!("invalid trust tier: {}", err))?;
    let history_policy = SessionHistoryPolicy::from_str(&history_policy_raw)
        .map_err(|err| anyhow!("invalid history policy: {}", err))?;
    let created_at = ms_to_datetime(created_at_ms)
        .ok_or_else(|| anyhow!("invalid created_at_ms '{}'", created_at_ms))?;
    let last_turn_at = last_turn_at_ms
        .map(|value| {
            ms_to_datetime(value).ok_or_else(|| anyhow!("invalid last_turn_at_ms '{}'", value))
        })
        .transpose()?;
    let turn_count = u64::try_from(turn_count_raw)
        .with_context(|| format!("invalid turn_count '{}'", turn_count_raw))?;

    Ok(Session {
        session_id,
        channel_id: row.get("channel_id"),
        peer_id: row.get("peer_id"),
        trust_tier,
        history_policy,
        created_at,
        last_turn_at,
        turn_count,
    })
}
