use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{sqlite::SqliteRow, Row, SqlitePool};
use uuid::Uuid;

use crate::{
    contracts::TrustTier,
    kernel::db::{ms_to_datetime, now_ms},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelPeerStatus {
    Pending,
    Approved,
    Blocked,
}

impl ChannelPeerStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approved => "approved",
            Self::Blocked => "blocked",
        }
    }
}

impl FromStr for ChannelPeerStatus {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "approved" => Ok(Self::Approved),
            "blocked" => Ok(Self::Blocked),
            other => Err(format!("invalid channel peer status '{}'", other)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChannelBindingRecord {
    pub channel_id: String,
    pub skill_id: String,
    pub enabled: bool,
    pub config: Value,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ChannelPeerRecord {
    pub channel_id: String,
    pub peer_id: String,
    pub status: ChannelPeerStatus,
    pub trust_tier: TrustTier,
    pub pairing_code: String,
    pub first_seen: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ChannelStreamEventRecord {
    pub sequence: i64,
    pub channel_id: String,
    pub peer_id: String,
    pub session_id: Uuid,
    pub turn_id: Uuid,
    pub kind: ChannelStreamEventKind,
    pub lane: Option<StreamMessageLane>,
    pub text: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy)]
pub struct ChannelStreamEventInsert<'a> {
    pub channel_id: &'a str,
    pub peer_id: &'a str,
    pub session_id: Uuid,
    pub turn_id: Uuid,
    pub kind: ChannelStreamEventKind,
    pub lane: Option<StreamMessageLane>,
    pub text: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelStreamEventKind {
    MessageDelta,
    Status,
    Error,
    Done,
}

impl ChannelStreamEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MessageDelta => "message_delta",
            Self::Status => "status",
            Self::Error => "error",
            Self::Done => "done",
        }
    }
}

impl FromStr for ChannelStreamEventKind {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "message_delta" => Ok(Self::MessageDelta),
            "status" => Ok(Self::Status),
            "error" => Ok(Self::Error),
            "done" => Ok(Self::Done),
            other => Err(format!("invalid channel stream event kind '{}'", other)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamMessageLane {
    Answer,
    Reasoning,
}

impl StreamMessageLane {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Answer => "answer",
            Self::Reasoning => "reasoning",
        }
    }
}

impl FromStr for StreamMessageLane {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "answer" => Ok(Self::Answer),
            "reasoning" => Ok(Self::Reasoning),
            other => Err(format!("invalid stream message lane '{}'", other)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChannelHealthRecord {
    pub channel_id: String,
    pub pending_peer_count: u64,
    pub approved_peer_count: u64,
    pub blocked_peer_count: u64,
    pub latest_inbound_at: Option<DateTime<Utc>>,
    pub latest_outbound_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageDirection {
    Inbound,
    Outbound,
}

impl MessageDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inbound => "inbound",
            Self::Outbound => "outbound",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChannelStateStore {
    pool: SqlitePool,
}

impl ChannelStateStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn upsert_binding(
        &self,
        channel_id: &str,
        skill_id: &str,
        enabled: bool,
        config: Value,
    ) -> Result<ChannelBindingRecord> {
        let config_json =
            serde_json::to_string(&config).context("failed to encode channel binding config")?;
        let now = now_ms();

        sqlx::query(
            "INSERT INTO channel_bindings (channel_id, skill_id, enabled, config_json, updated_at_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT(channel_id) DO UPDATE SET \
                 skill_id = excluded.skill_id, \
                 enabled = excluded.enabled, \
                 config_json = excluded.config_json, \
                 updated_at_ms = excluded.updated_at_ms",
        )
        .bind(channel_id)
        .bind(skill_id)
        .bind(if enabled { 1 } else { 0 })
        .bind(config_json)
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to upsert channel binding")?;

        self.get_binding(channel_id)
            .await?
            .ok_or_else(|| anyhow!("channel binding disappeared after upsert"))
    }

    pub async fn get_binding(&self, channel_id: &str) -> Result<Option<ChannelBindingRecord>> {
        let row = sqlx::query(
            "SELECT channel_id, skill_id, enabled, config_json, updated_at_ms \
             FROM channel_bindings WHERE channel_id = ?1",
        )
        .bind(channel_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to query channel binding")?;

        row.map(map_binding_row).transpose()
    }

    pub async fn list_bindings(&self) -> Result<Vec<ChannelBindingRecord>> {
        let rows = sqlx::query(
            "SELECT channel_id, skill_id, enabled, config_json, updated_at_ms \
             FROM channel_bindings ORDER BY updated_at_ms DESC",
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to list channel bindings")?;

        rows.into_iter().map(map_binding_row).collect()
    }

    pub async fn get_peer(
        &self,
        channel_id: &str,
        peer_id: &str,
    ) -> Result<Option<ChannelPeerRecord>> {
        let row = sqlx::query(
            "SELECT channel_id, peer_id, status, trust_tier, pairing_code, first_seen_ms, updated_at_ms \
             FROM channel_peers WHERE channel_id = ?1 AND peer_id = ?2",
        )
        .bind(channel_id)
        .bind(peer_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to query channel peer")?;

        row.map(map_peer_row).transpose()
    }

    pub async fn upsert_pending_peer(
        &self,
        channel_id: &str,
        peer_id: &str,
        pairing_code: &str,
    ) -> Result<ChannelPeerRecord> {
        let now = now_ms();

        sqlx::query(
            "INSERT INTO channel_peers \
             (channel_id, peer_id, status, trust_tier, pairing_code, first_seen_ms, updated_at_ms) \
             VALUES (?1, ?2, 'pending', 'untrusted', ?3, ?4, ?4) \
             ON CONFLICT(channel_id, peer_id) DO UPDATE SET \
                 status = CASE \
                    WHEN channel_peers.status = 'approved' THEN channel_peers.status \
                    ELSE 'pending' \
                 END, \
                 pairing_code = CASE \
                    WHEN channel_peers.status = 'approved' THEN channel_peers.pairing_code \
                    ELSE excluded.pairing_code \
                 END, \
                 updated_at_ms = excluded.updated_at_ms",
        )
        .bind(channel_id)
        .bind(peer_id)
        .bind(pairing_code)
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to upsert pending channel peer")?;

        self.get_peer(channel_id, peer_id)
            .await?
            .ok_or_else(|| anyhow!("channel peer disappeared after pending upsert"))
    }

    pub async fn approve_peer(
        &self,
        channel_id: &str,
        peer_id: &str,
        trust_tier: TrustTier,
    ) -> Result<Option<ChannelPeerRecord>> {
        let now = now_ms();
        let changed = sqlx::query(
            "UPDATE channel_peers \
             SET status = 'approved', trust_tier = ?3, updated_at_ms = ?4 \
             WHERE channel_id = ?1 AND peer_id = ?2",
        )
        .bind(channel_id)
        .bind(peer_id)
        .bind(trust_tier.as_str())
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to approve channel peer")?;

        if changed.rows_affected() == 0 {
            return Ok(None);
        }

        self.get_peer(channel_id, peer_id).await
    }

    pub async fn block_peer(
        &self,
        channel_id: &str,
        peer_id: &str,
    ) -> Result<Option<ChannelPeerRecord>> {
        let now = now_ms();
        let changed = sqlx::query(
            "UPDATE channel_peers \
             SET status = 'blocked', updated_at_ms = ?3 \
             WHERE channel_id = ?1 AND peer_id = ?2",
        )
        .bind(channel_id)
        .bind(peer_id)
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to block channel peer")?;

        if changed.rows_affected() == 0 {
            return Ok(None);
        }

        self.get_peer(channel_id, peer_id).await
    }

    pub async fn list_peers(&self, channel_id: Option<&str>) -> Result<Vec<ChannelPeerRecord>> {
        let rows = sqlx::query(
            "SELECT channel_id, peer_id, status, trust_tier, pairing_code, first_seen_ms, updated_at_ms \
             FROM channel_peers \
             WHERE (?1 IS NULL OR channel_id = ?1) \
             ORDER BY updated_at_ms DESC",
        )
        .bind(channel_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list channel peers")?;

        rows.into_iter().map(map_peer_row).collect()
    }

    pub async fn get_offset(&self, channel_id: &str) -> Result<i64> {
        let row = sqlx::query("SELECT offset FROM channel_offsets WHERE channel_id = ?1")
            .bind(channel_id)
            .fetch_optional(&self.pool)
            .await
            .context("failed to query channel offset")?;

        Ok(row.map(|value| value.get::<i64, _>("offset")).unwrap_or(0))
    }

    pub async fn set_offset(&self, channel_id: &str, offset: i64) -> Result<()> {
        let now = now_ms();
        sqlx::query(
            "INSERT INTO channel_offsets (channel_id, offset, updated_at_ms) \
             VALUES (?1, ?2, ?3) \
             ON CONFLICT(channel_id) DO UPDATE SET \
                 offset = excluded.offset, \
                 updated_at_ms = excluded.updated_at_ms",
        )
        .bind(channel_id)
        .bind(offset)
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to set channel offset")?;
        Ok(())
    }

    pub async fn insert_inbound_message(
        &self,
        channel_id: &str,
        peer_id: &str,
        external_message_id: Option<String>,
        update_id: Option<i64>,
        content: &str,
    ) -> Result<bool> {
        let message_id = Uuid::new_v4();
        let now = now_ms();

        let result = sqlx::query(
            "INSERT INTO channel_messages \
             (message_id, channel_id, peer_id, direction, external_message_id, update_id, content, created_at_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )
        .bind(message_id.to_string())
        .bind(channel_id)
        .bind(peer_id)
        .bind(MessageDirection::Inbound.as_str())
        .bind(external_message_id)
        .bind(update_id)
        .bind(content)
        .bind(now)
        .execute(&self.pool)
        .await;

        match result {
            Ok(done) => Ok(done.rows_affected() > 0),
            Err(err) => {
                // Duplicate inbound update IDs are expected when channel workers retry.
                if let sqlx::Error::Database(db_err) = &err {
                    if db_err.is_unique_violation() {
                        return Ok(false);
                    }
                }
                Err(err).context("failed to insert channel message")
            }
        }
    }

    pub async fn insert_outbound_message(
        &self,
        channel_id: &str,
        peer_id: &str,
        content: &str,
    ) -> Result<Uuid> {
        let message_id = Uuid::new_v4();
        let now = now_ms();

        sqlx::query(
            "INSERT INTO channel_messages \
             (message_id, channel_id, peer_id, direction, external_message_id, update_id, content, created_at_ms) \
             VALUES (?1, ?2, ?3, 'outbound', NULL, NULL, ?4, ?5)",
        )
        .bind(message_id.to_string())
        .bind(channel_id)
        .bind(peer_id)
        .bind(content)
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to queue outbound channel message")?;

        Ok(message_id)
    }

    pub async fn append_stream_event(
        &self,
        insert: ChannelStreamEventInsert<'_>,
    ) -> Result<ChannelStreamEventRecord> {
        let now = now_ms();

        let done = sqlx::query(
            "INSERT INTO channel_stream_events \
             (channel_id, peer_id, session_id, turn_id, kind, lane, text, created_at_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )
        .bind(insert.channel_id)
        .bind(insert.peer_id)
        .bind(insert.session_id.to_string())
        .bind(insert.turn_id.to_string())
        .bind(insert.kind.as_str())
        .bind(insert.lane.map(StreamMessageLane::as_str))
        .bind(insert.text)
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to append channel stream event")?;

        let sequence = done.last_insert_rowid();
        let row = sqlx::query(
            "SELECT sequence, channel_id, peer_id, session_id, turn_id, kind, lane, text, created_at_ms \
             FROM channel_stream_events WHERE sequence = ?1",
        )
        .bind(sequence)
        .fetch_one(&self.pool)
        .await
        .context("failed to reload appended channel stream event")?;

        map_stream_event_row(row)
    }

    pub async fn current_stream_head(&self, channel_id: &str) -> Result<i64> {
        let row = sqlx::query(
            "SELECT COALESCE(MAX(sequence), 0) AS sequence \
             FROM channel_stream_events WHERE channel_id = ?1",
        )
        .bind(channel_id)
        .fetch_one(&self.pool)
        .await
        .context("failed to query current channel stream head")?;

        Ok(row.get::<i64, _>("sequence"))
    }

    pub async fn get_stream_consumer_cursor(
        &self,
        channel_id: &str,
        consumer_id: &str,
    ) -> Result<Option<i64>> {
        let row = sqlx::query(
            "SELECT last_acked_sequence FROM channel_stream_consumers \
             WHERE channel_id = ?1 AND consumer_id = ?2",
        )
        .bind(channel_id)
        .bind(consumer_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to query channel stream consumer")?;

        Ok(row.map(|value| value.get::<i64, _>("last_acked_sequence")))
    }

    pub async fn create_stream_consumer(
        &self,
        channel_id: &str,
        consumer_id: &str,
        last_acked_sequence: i64,
    ) -> Result<()> {
        let now = now_ms();

        sqlx::query(
            "INSERT INTO channel_stream_consumers \
             (channel_id, consumer_id, last_acked_sequence, updated_at_ms) \
             VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(channel_id)
        .bind(consumer_id)
        .bind(last_acked_sequence)
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to create channel stream consumer")?;

        Ok(())
    }

    pub async fn list_stream_events_after(
        &self,
        channel_id: &str,
        after_sequence: i64,
        limit: usize,
    ) -> Result<Vec<ChannelStreamEventRecord>> {
        let query_limit = i64::try_from(limit).context("invalid channel stream limit")?;
        let rows = sqlx::query(
            "SELECT sequence, channel_id, peer_id, session_id, turn_id, kind, lane, text, created_at_ms \
             FROM channel_stream_events \
             WHERE channel_id = ?1 AND sequence > ?2 \
             ORDER BY sequence ASC \
             LIMIT ?3",
        )
        .bind(channel_id)
        .bind(after_sequence)
        .bind(query_limit)
        .fetch_all(&self.pool)
        .await
        .context("failed to list channel stream events")?;

        rows.into_iter().map(map_stream_event_row).collect()
    }

    pub async fn ack_stream_consumer(
        &self,
        channel_id: &str,
        consumer_id: &str,
        through_sequence: i64,
    ) -> Result<bool> {
        let now = now_ms();
        let changed = sqlx::query(
            "UPDATE channel_stream_consumers \
             SET last_acked_sequence = CASE \
                    WHEN last_acked_sequence < ?3 THEN ?3 \
                    ELSE last_acked_sequence \
                 END, \
                 updated_at_ms = ?4 \
             WHERE channel_id = ?1 AND consumer_id = ?2",
        )
        .bind(channel_id)
        .bind(consumer_id)
        .bind(through_sequence)
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to acknowledge channel stream consumer")?;

        Ok(changed.rows_affected() > 0)
    }

    pub async fn channel_health(&self, channel_id: &str) -> Result<ChannelHealthRecord> {
        let peer_counts = sqlx::query(
            "SELECT status, COUNT(*) AS count \
             FROM channel_peers \
             WHERE channel_id = ?1 \
             GROUP BY status",
        )
        .bind(channel_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to query channel peer counts")?;

        let latest_row = sqlx::query(
            "SELECT \
                MAX(CASE WHEN direction = 'inbound' THEN created_at_ms END) AS latest_inbound_at_ms, \
                MAX(CASE WHEN direction = 'outbound' THEN created_at_ms END) AS latest_outbound_at_ms \
             FROM channel_messages \
             WHERE channel_id = ?1",
        )
        .bind(channel_id)
        .fetch_one(&self.pool)
        .await
        .context("failed to query channel message activity")?;

        let mut pending_peer_count = 0_u64;
        let mut approved_peer_count = 0_u64;
        let mut blocked_peer_count = 0_u64;

        for row in peer_counts {
            let status: String = row.get("status");
            let count_raw: i64 = row.get("count");
            let count = u64::try_from(count_raw)
                .with_context(|| format!("invalid channel peer count '{}'", count_raw))?;
            match status.as_str() {
                "pending" => pending_peer_count = count,
                "approved" => approved_peer_count = count,
                "blocked" => blocked_peer_count = count,
                _ => {}
            }
        }

        let latest_inbound_at = latest_row
            .get::<Option<i64>, _>("latest_inbound_at_ms")
            .map(|value| {
                ms_to_datetime(value)
                    .ok_or_else(|| anyhow!("invalid latest_inbound_at_ms '{}'", value))
            })
            .transpose()?;
        let latest_outbound_at = latest_row
            .get::<Option<i64>, _>("latest_outbound_at_ms")
            .map(|value| {
                ms_to_datetime(value)
                    .ok_or_else(|| anyhow!("invalid latest_outbound_at_ms '{}'", value))
            })
            .transpose()?;

        Ok(ChannelHealthRecord {
            channel_id: channel_id.to_string(),
            pending_peer_count,
            approved_peer_count,
            blocked_peer_count,
            latest_inbound_at,
            latest_outbound_at,
        })
    }
}

fn map_binding_row(row: SqliteRow) -> Result<ChannelBindingRecord> {
    let updated_at_ms: i64 = row.get("updated_at_ms");
    let updated_at = ms_to_datetime(updated_at_ms)
        .ok_or_else(|| anyhow!("invalid updated_at_ms '{}'", updated_at_ms))?;
    let config_json: String = row.get("config_json");
    let config = serde_json::from_str(&config_json)
        .with_context(|| format!("invalid config_json '{}'", config_json))?;

    Ok(ChannelBindingRecord {
        channel_id: row.get("channel_id"),
        skill_id: row.get("skill_id"),
        enabled: row.get::<i64, _>("enabled") != 0,
        config,
        updated_at,
    })
}

fn map_peer_row(row: SqliteRow) -> Result<ChannelPeerRecord> {
    let status_raw: String = row.get("status");
    let trust_tier_raw: String = row.get("trust_tier");
    let first_seen_ms: i64 = row.get("first_seen_ms");
    let updated_at_ms: i64 = row.get("updated_at_ms");

    let status = ChannelPeerStatus::from_str(&status_raw)
        .map_err(|err| anyhow!("invalid channel peer status: {}", err))?;
    let trust_tier = TrustTier::from_str(&trust_tier_raw)
        .map_err(|err| anyhow!("invalid trust tier: {}", err))?;
    let first_seen = ms_to_datetime(first_seen_ms)
        .ok_or_else(|| anyhow!("invalid first_seen_ms '{}'", first_seen_ms))?;
    let updated_at = ms_to_datetime(updated_at_ms)
        .ok_or_else(|| anyhow!("invalid updated_at_ms '{}'", updated_at_ms))?;

    Ok(ChannelPeerRecord {
        channel_id: row.get("channel_id"),
        peer_id: row.get("peer_id"),
        status,
        trust_tier,
        pairing_code: row.get("pairing_code"),
        first_seen,
        updated_at,
    })
}

fn map_stream_event_row(row: SqliteRow) -> Result<ChannelStreamEventRecord> {
    let created_at_ms: i64 = row.get("created_at_ms");
    let created_at = ms_to_datetime(created_at_ms)
        .ok_or_else(|| anyhow!("invalid created_at_ms '{}'", created_at_ms))?;
    let session_id_raw: String = row.get("session_id");
    let session_id = Uuid::parse_str(&session_id_raw)
        .map_err(|err| anyhow!("invalid session_id '{}': {}", session_id_raw, err))?;
    let turn_id_raw: String = row.get("turn_id");
    let turn_id = Uuid::parse_str(&turn_id_raw)
        .map_err(|err| anyhow!("invalid turn_id '{}': {}", turn_id_raw, err))?;
    let kind_raw: String = row.get("kind");
    let kind = ChannelStreamEventKind::from_str(&kind_raw)
        .map_err(|err| anyhow!("invalid channel stream event kind: {}", err))?;
    let lane = row
        .get::<Option<String>, _>("lane")
        .map(|raw| {
            StreamMessageLane::from_str(&raw)
                .map_err(|err| anyhow!("invalid stream message lane: {}", err))
        })
        .transpose()?;

    Ok(ChannelStreamEventRecord {
        sequence: row.get("sequence"),
        channel_id: row.get("channel_id"),
        peer_id: row.get("peer_id"),
        session_id,
        turn_id,
        kind,
        lane,
        text: row.get("text"),
        created_at,
    })
}
