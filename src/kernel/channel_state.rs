use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{sqlite::SqliteRow, Row, Sqlite, SqlitePool, Transaction};
use uuid::Uuid;

use crate::{
    contracts::{
        ChannelAttachmentDescriptor, ChannelPairingStatus, ChannelRoutingProfile, ChannelTrigger,
        TrustTier,
    },
    kernel::db::{datetime_to_ms, ms_to_datetime, now_ms},
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
            other => Err(format!("invalid channel peer status '{other}'")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelGrantStatus {
    Approved,
    Blocked,
    Revoked,
}

impl ChannelGrantStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Approved => "approved",
            Self::Blocked => "blocked",
            Self::Revoked => "revoked",
        }
    }
}

impl FromStr for ChannelGrantStatus {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "approved" => Ok(Self::Approved),
            "blocked" => Ok(Self::Blocked),
            "revoked" => Ok(Self::Revoked),
            other => Err(format!("invalid channel grant status '{other}'")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChannelBindingRecord {
    pub channel_id: String,
    pub skill_alias: String,
    pub launch_mode: String,
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
pub struct ChannelPairingRequestRecord {
    pub pairing_id: Uuid,
    pub channel_id: String,
    pub code_hash: String,
    pub claim_policy: String,
    pub sender_ref: Option<String>,
    pub conversation_ref: Option<String>,
    pub thread_ref: Option<String>,
    pub requested_profile: ChannelRoutingProfile,
    pub status: ChannelPairingStatus,
    pub label: Option<String>,
    pub max_claims: i64,
    pub claim_count: i64,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ChannelGrantRecord {
    pub grant_id: Uuid,
    pub channel_id: String,
    pub sender_ref: Option<String>,
    pub conversation_ref: Option<String>,
    pub thread_ref: Option<String>,
    pub routing_profile: ChannelRoutingProfile,
    pub trust_tier: TrustTier,
    pub status: ChannelGrantStatus,
    pub label: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct ChannelInboundEventRecord {
    pub event_id: String,
    pub channel_id: String,
    pub sender_ref: String,
    pub conversation_ref: String,
    pub thread_ref: Option<String>,
    pub message_ref: Option<String>,
    pub trigger: ChannelTrigger,
    pub attachments: Vec<ChannelAttachmentDescriptor>,
    pub reply_to_ref: Option<String>,
    pub provider_metadata: Value,
    pub received_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct NewChannelInboundEvent<'a> {
    pub event_id: &'a str,
    pub channel_id: &'a str,
    pub sender_ref: &'a str,
    pub conversation_ref: &'a str,
    pub thread_ref: Option<&'a str>,
    pub message_ref: Option<&'a str>,
    pub trigger: ChannelTrigger,
    pub attachments: &'a [ChannelAttachmentDescriptor],
    pub reply_to_ref: Option<&'a str>,
    pub provider_metadata: &'a Value,
    pub received_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub(crate) struct ChannelGrantUpsert<'a> {
    pub channel_id: &'a str,
    pub sender_ref: Option<&'a str>,
    pub conversation_ref: Option<&'a str>,
    pub thread_ref: Option<&'a str>,
    pub routing_profile: ChannelRoutingProfile,
    pub trust_tier: TrustTier,
    pub status: ChannelGrantStatus,
    pub label: Option<&'a str>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct NewChannelTurn<'a> {
    pub turn_id: Uuid,
    pub channel_id: &'a str,
    pub session_key: &'a str,
    pub session_id: Uuid,
    pub inbound_event_id: &'a str,
    pub runtime_id: &'a str,
    pub status: ChannelTurnStatus,
}

#[derive(Debug, Clone)]
pub struct ChannelStreamEventRecord {
    pub sequence: i64,
    pub channel_id: String,
    pub peer_id: String,
    pub session_id: Option<Uuid>,
    pub turn_id: Option<Uuid>,
    pub kind: ChannelStreamEventKind,
    pub lane: Option<StreamMessageLane>,
    pub code: Option<String>,
    pub text: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy)]
pub struct ChannelStreamEventInsert<'a> {
    pub channel_id: &'a str,
    pub peer_id: &'a str,
    pub session_id: Option<Uuid>,
    pub turn_id: Option<Uuid>,
    pub kind: ChannelStreamEventKind,
    pub lane: Option<StreamMessageLane>,
    pub code: Option<&'a str>,
    pub text: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelStreamEventKind {
    MessageDelta,
    Status,
    Error,
    TurnCompleted,
    Done,
}

impl ChannelStreamEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MessageDelta => "message_delta",
            Self::Status => "status",
            Self::Error => "error",
            Self::TurnCompleted => "turn_completed",
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
            "turn_completed" => Ok(Self::TurnCompleted),
            "done" => Ok(Self::Done),
            other => Err(format!("invalid channel stream event kind '{other}'")),
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
            other => Err(format!("invalid stream message lane '{other}'")),
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
pub enum ChannelTurnStatus {
    WaitingForAttachments,
    Pending,
    Running,
    Completed,
    Failed,
}

impl ChannelTurnStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WaitingForAttachments => "waiting_for_attachments",
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

impl FromStr for ChannelTurnStatus {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "waiting_for_attachments" => Ok(Self::WaitingForAttachments),
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            other => Err(format!("invalid channel turn status '{other}'")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChannelTurnRecord {
    pub turn_id: Uuid,
    pub channel_id: String,
    pub session_key: String,
    pub session_id: Uuid,
    pub inbound_event_id: String,
    pub runtime_id: String,
    pub status: ChannelTurnStatus,
    pub last_error: Option<String>,
    pub answer_checkpoint_sequence: Option<i64>,
    pub queued_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct ChannelStateStore {
    pool: SqlitePool,
}

impl ChannelStateStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub(crate) async fn insert_inbound_event(
        &self,
        event: NewChannelInboundEvent<'_>,
    ) -> Result<Option<ChannelInboundEventRecord>> {
        let attachments_json = serde_json::to_string(event.attachments)
            .context("failed to encode attachment descriptors")?;
        let provider_metadata_json = serde_json::to_string(event.provider_metadata)
            .context("failed to encode provider metadata")?;
        let received_at_ms = datetime_to_ms(event.received_at);
        let created_at_ms = now_ms();

        let result = sqlx::query(
            "INSERT INTO channel_inbound_events \
             (event_id, channel_id, sender_ref, conversation_ref, thread_ref, message_ref, trigger, attachments_json, reply_to_ref, provider_metadata_json, received_at_ms, created_at_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        )
        .bind(event.event_id)
        .bind(event.channel_id)
        .bind(event.sender_ref)
        .bind(event.conversation_ref)
        .bind(event.thread_ref)
        .bind(event.message_ref)
        .bind(event.trigger.as_str())
        .bind(attachments_json)
        .bind(event.reply_to_ref)
        .bind(provider_metadata_json)
        .bind(received_at_ms)
        .bind(created_at_ms)
        .execute(&self.pool)
        .await;

        match result {
            Ok(done) => {
                if done.rows_affected() == 0 {
                    return Ok(None);
                }
                self.get_inbound_event(event.channel_id, event.event_id)
                    .await
            }
            Err(err) => {
                if let sqlx::Error::Database(db_err) = &err {
                    if db_err.is_unique_violation() {
                        return Ok(None);
                    }
                }
                Err(err).context("failed to insert channel inbound event")
            }
        }
    }

    pub async fn get_inbound_event(
        &self,
        channel_id: &str,
        event_id: &str,
    ) -> Result<Option<ChannelInboundEventRecord>> {
        let row = sqlx::query(
            "SELECT event_id, channel_id, sender_ref, conversation_ref, thread_ref, message_ref, trigger, attachments_json, reply_to_ref, provider_metadata_json, received_at_ms, created_at_ms \
             FROM channel_inbound_events WHERE channel_id = ?1 AND event_id = ?2",
        )
        .bind(channel_id)
        .bind(event_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to query channel inbound event")?;

        row.map(map_inbound_event_row).transpose()
    }

    pub async fn find_blocking_grant(
        &self,
        channel_id: &str,
        sender_ref: &str,
        conversation_ref: &str,
        thread_ref: Option<&str>,
    ) -> Result<Option<ChannelGrantRecord>> {
        if let Some(thread_ref) = thread_ref {
            if let Some(grant) = self
                .get_grant_by_scope(
                    channel_id,
                    Some(sender_ref),
                    Some(conversation_ref),
                    Some(thread_ref),
                    ChannelRoutingProfile::Thread,
                    ChannelGrantStatus::Blocked,
                )
                .await?
            {
                return Ok(Some(grant));
            }
        }

        if let Some(grant) = self
            .get_grant_by_scope(
                channel_id,
                Some(sender_ref),
                Some(conversation_ref),
                None,
                ChannelRoutingProfile::Conversation,
                ChannelGrantStatus::Blocked,
            )
            .await?
        {
            return Ok(Some(grant));
        }

        self.get_grant_by_scope(
            channel_id,
            Some(sender_ref),
            None,
            None,
            ChannelRoutingProfile::Direct,
            ChannelGrantStatus::Blocked,
        )
        .await
    }

    pub async fn find_approved_grant(
        &self,
        channel_id: &str,
        sender_ref: &str,
        conversation_ref: &str,
        thread_ref: Option<&str>,
        trigger: ChannelTrigger,
    ) -> Result<Option<ChannelGrantRecord>> {
        if let Some(thread_ref) = thread_ref {
            if let Some(grant) = self
                .get_grant_by_scope(
                    channel_id,
                    Some(sender_ref),
                    Some(conversation_ref),
                    Some(thread_ref),
                    ChannelRoutingProfile::Thread,
                    ChannelGrantStatus::Approved,
                )
                .await?
            {
                return Ok(Some(grant));
            }
        }

        if let Some(grant) = self
            .get_grant_by_scope(
                channel_id,
                Some(sender_ref),
                Some(conversation_ref),
                None,
                ChannelRoutingProfile::Conversation,
                ChannelGrantStatus::Approved,
            )
            .await?
        {
            return Ok(Some(grant));
        }

        if trigger == ChannelTrigger::Dm {
            return self
                .get_grant_by_scope(
                    channel_id,
                    Some(sender_ref),
                    None,
                    None,
                    ChannelRoutingProfile::Direct,
                    ChannelGrantStatus::Approved,
                )
                .await;
        }

        Ok(None)
    }

    pub async fn get_grant_by_scope(
        &self,
        channel_id: &str,
        sender_ref: Option<&str>,
        conversation_ref: Option<&str>,
        thread_ref: Option<&str>,
        routing_profile: ChannelRoutingProfile,
        status: ChannelGrantStatus,
    ) -> Result<Option<ChannelGrantRecord>> {
        let row = sqlx::query(
            "SELECT grant_id, channel_id, sender_ref, conversation_ref, thread_ref, routing_profile, trust_tier, status, label, created_at_ms, updated_at_ms, revoked_at_ms \
             FROM channel_grants \
             WHERE channel_id = ?1 \
               AND COALESCE(sender_ref, '') = COALESCE(?2, '') \
               AND COALESCE(conversation_ref, '') = COALESCE(?3, '') \
               AND COALESCE(thread_ref, '') = COALESCE(?4, '') \
               AND routing_profile = ?5 \
               AND status = ?6",
        )
        .bind(channel_id)
        .bind(sender_ref)
        .bind(conversation_ref)
        .bind(thread_ref)
        .bind(routing_profile.as_str())
        .bind(status.as_str())
        .fetch_optional(&self.pool)
        .await
        .context("failed to query channel grant by scope")?;

        row.map(map_grant_row).transpose()
    }

    pub async fn find_thread_session_grant(
        &self,
        channel_id: &str,
        conversation_ref: &str,
        thread_ref: &str,
        status: ChannelGrantStatus,
    ) -> Result<Option<ChannelGrantRecord>> {
        let row = sqlx::query(
            "SELECT grant_id, channel_id, sender_ref, conversation_ref, thread_ref, routing_profile, trust_tier, status, label, created_at_ms, updated_at_ms, revoked_at_ms \
             FROM channel_grants \
             WHERE channel_id = ?1 \
               AND conversation_ref = ?2 \
               AND thread_ref = ?3 \
               AND routing_profile = 'thread' \
               AND status = ?4 \
             ORDER BY updated_at_ms DESC \
             LIMIT 1",
        )
        .bind(channel_id)
        .bind(conversation_ref)
        .bind(thread_ref)
        .bind(status.as_str())
        .fetch_optional(&self.pool)
        .await
        .context("failed to query channel thread grant")?;

        row.map(map_grant_row).transpose()
    }

    pub async fn list_pairing_requests(
        &self,
        channel_id: Option<&str>,
        status: Option<ChannelPairingStatus>,
    ) -> Result<Vec<ChannelPairingRequestRecord>> {
        let rows = sqlx::query(
            "SELECT pairing_id, channel_id, code_hash, claim_policy, sender_ref, conversation_ref, thread_ref, requested_profile, status, label, max_claims, claim_count, created_at_ms, expires_at_ms, claimed_at_ms, updated_at_ms \
             FROM channel_pairing_requests \
             WHERE (?1 IS NULL OR channel_id = ?1) \
               AND (?2 IS NULL OR status = ?2) \
             ORDER BY updated_at_ms DESC",
        )
        .bind(channel_id)
        .bind(status.map(ChannelPairingStatus::as_str))
        .fetch_all(&self.pool)
        .await
        .context("failed to list channel pairing requests")?;

        rows.into_iter().map(map_pairing_row).collect()
    }

    pub async fn create_or_refresh_operator_pairing(
        &self,
        channel_id: &str,
        sender_ref: Option<&str>,
        conversation_ref: Option<&str>,
        thread_ref: Option<&str>,
        requested_profile: ChannelRoutingProfile,
        code_hash: &str,
    ) -> Result<(ChannelPairingRequestRecord, bool)> {
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .context("failed to start pairing request transaction")?;
        let existing = self
            .find_pending_operator_pairing_in_tx(
                &mut tx,
                channel_id,
                sender_ref,
                conversation_ref,
                thread_ref,
                requested_profile,
            )
            .await?;
        if let Some(existing) = existing {
            let now = now_ms();
            sqlx::query(
                "UPDATE channel_pairing_requests \
                 SET updated_at_ms = ?2 \
                 WHERE pairing_id = ?1 AND status = 'pending'",
            )
            .bind(existing.pairing_id.to_string())
            .bind(now)
            .execute(&mut *tx)
            .await
            .context("failed to refresh channel pairing request")?;
            let refreshed = self
                .get_pairing_request_by_id_in_tx(&mut tx, existing.pairing_id)
                .await?
                .ok_or_else(|| anyhow!("channel pairing disappeared after refresh"))?;
            tx.commit()
                .await
                .context("failed to commit pairing refresh")?;
            return Ok((refreshed, false));
        }

        let now = now_ms();
        let pairing_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO channel_pairing_requests \
             (pairing_id, channel_id, code_hash, claim_policy, sender_ref, conversation_ref, thread_ref, requested_profile, status, label, max_claims, claim_count, created_at_ms, expires_at_ms, claimed_at_ms, updated_at_ms) \
             VALUES (?1, ?2, ?3, 'operator_approval', ?4, ?5, ?6, ?7, 'pending', NULL, 1, 0, ?8, NULL, NULL, ?8)",
        )
        .bind(pairing_id.to_string())
        .bind(channel_id)
        .bind(code_hash)
        .bind(sender_ref)
        .bind(conversation_ref)
        .bind(thread_ref)
        .bind(requested_profile.as_str())
        .bind(now)
        .execute(&mut *tx)
        .await
        .context("failed to create channel pairing request")?;

        let created = self
            .get_pairing_request_by_id_in_tx(&mut tx, pairing_id)
            .await?
            .ok_or_else(|| anyhow!("channel pairing disappeared after insert"))?;
        tx.commit()
            .await
            .context("failed to commit pairing insert")?;
        Ok((created, true))
    }

    pub(crate) async fn get_pairing_request_by_id_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        pairing_id: Uuid,
    ) -> Result<Option<ChannelPairingRequestRecord>> {
        let row = sqlx::query(
            "SELECT pairing_id, channel_id, code_hash, claim_policy, sender_ref, conversation_ref, thread_ref, requested_profile, status, label, max_claims, claim_count, created_at_ms, expires_at_ms, claimed_at_ms, updated_at_ms \
             FROM channel_pairing_requests WHERE pairing_id = ?1",
        )
        .bind(pairing_id.to_string())
        .fetch_optional(&mut **tx)
        .await
        .context("failed to query channel pairing request by id")?;

        row.map(map_pairing_row).transpose()
    }

    pub(crate) async fn get_pairing_request_by_code_hash_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        channel_id: &str,
        code_hash: &str,
    ) -> Result<Option<ChannelPairingRequestRecord>> {
        let row = sqlx::query(
            "SELECT pairing_id, channel_id, code_hash, claim_policy, sender_ref, conversation_ref, thread_ref, requested_profile, status, label, max_claims, claim_count, created_at_ms, expires_at_ms, claimed_at_ms, updated_at_ms \
             FROM channel_pairing_requests \
             WHERE channel_id = ?1 AND code_hash = ?2",
        )
        .bind(channel_id)
        .bind(code_hash)
        .fetch_optional(&mut **tx)
        .await
        .context("failed to query channel pairing request by code hash")?;

        row.map(map_pairing_row).transpose()
    }

    pub(crate) async fn insert_or_update_grant_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        grant: ChannelGrantUpsert<'_>,
    ) -> Result<ChannelGrantRecord> {
        let now = now_ms();
        if let Some(existing) = self
            .get_grant_by_scope_in_tx(
                tx,
                grant.channel_id,
                grant.sender_ref,
                grant.conversation_ref,
                grant.thread_ref,
                grant.routing_profile,
            )
            .await?
        {
            let revoked_at = if grant.status == ChannelGrantStatus::Revoked {
                Some(now)
            } else {
                None
            };
            sqlx::query(
                "UPDATE channel_grants \
                 SET trust_tier = ?2, status = ?3, label = COALESCE(?4, label), updated_at_ms = ?5, revoked_at_ms = ?6 \
                 WHERE grant_id = ?1",
            )
            .bind(existing.grant_id.to_string())
            .bind(grant.trust_tier.as_str())
            .bind(grant.status.as_str())
            .bind(grant.label)
            .bind(now)
            .bind(revoked_at)
            .execute(&mut **tx)
            .await
            .context("failed to update channel grant")?;
            return self
                .get_grant_in_tx(tx, existing.grant_id)
                .await?
                .ok_or_else(|| anyhow!("channel grant disappeared after update"));
        }

        let grant_id = Uuid::new_v4();
        let revoked_at = if grant.status == ChannelGrantStatus::Revoked {
            Some(now)
        } else {
            None
        };
        sqlx::query(
            "INSERT INTO channel_grants \
             (grant_id, channel_id, sender_ref, conversation_ref, thread_ref, routing_profile, trust_tier, status, label, created_at_ms, updated_at_ms, revoked_at_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10, ?11)",
        )
        .bind(grant_id.to_string())
        .bind(grant.channel_id)
        .bind(grant.sender_ref)
        .bind(grant.conversation_ref)
        .bind(grant.thread_ref)
        .bind(grant.routing_profile.as_str())
        .bind(grant.trust_tier.as_str())
        .bind(grant.status.as_str())
        .bind(grant.label)
        .bind(now)
        .bind(revoked_at)
        .execute(&mut **tx)
        .await
        .context("failed to insert channel grant")?;

        self.get_grant_in_tx(tx, grant_id)
            .await?
            .ok_or_else(|| anyhow!("channel grant disappeared after insert"))
    }

    pub(crate) async fn mark_pairing_status_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        pairing_id: Uuid,
        status: ChannelPairingStatus,
        label: Option<&str>,
    ) -> Result<Option<ChannelPairingRequestRecord>> {
        let now = now_ms();
        let claimed_at = if status == ChannelPairingStatus::Approved {
            Some(now)
        } else {
            None
        };
        let changed = sqlx::query(
            "UPDATE channel_pairing_requests \
             SET status = ?2, label = COALESCE(?3, label), claimed_at_ms = COALESCE(?4, claimed_at_ms), updated_at_ms = ?5 \
             WHERE pairing_id = ?1",
        )
        .bind(pairing_id.to_string())
        .bind(status.as_str())
        .bind(label)
        .bind(claimed_at)
        .bind(now)
        .execute(&mut **tx)
        .await
        .context("failed to update channel pairing status")?;

        if changed.rows_affected() == 0 {
            return Ok(None);
        }
        self.get_pairing_request_by_id_in_tx(tx, pairing_id).await
    }

    pub async fn revoke_grant(
        &self,
        channel_id: &str,
        grant_id: Uuid,
    ) -> Result<Option<ChannelGrantRecord>> {
        let now = now_ms();
        let changed = sqlx::query(
            "UPDATE channel_grants \
             SET status = 'revoked', revoked_at_ms = ?3, updated_at_ms = ?3 \
             WHERE channel_id = ?1 AND grant_id = ?2 AND status != 'revoked'",
        )
        .bind(channel_id)
        .bind(grant_id.to_string())
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to revoke channel grant")?;

        if changed.rows_affected() == 0 {
            return Ok(None);
        }
        self.get_grant(grant_id).await
    }

    pub async fn get_grant(&self, grant_id: Uuid) -> Result<Option<ChannelGrantRecord>> {
        let row = sqlx::query(
            "SELECT grant_id, channel_id, sender_ref, conversation_ref, thread_ref, routing_profile, trust_tier, status, label, created_at_ms, updated_at_ms, revoked_at_ms \
             FROM channel_grants WHERE grant_id = ?1",
        )
        .bind(grant_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .context("failed to query channel grant")?;

        row.map(map_grant_row).transpose()
    }

    async fn get_grant_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        grant_id: Uuid,
    ) -> Result<Option<ChannelGrantRecord>> {
        let row = sqlx::query(
            "SELECT grant_id, channel_id, sender_ref, conversation_ref, thread_ref, routing_profile, trust_tier, status, label, created_at_ms, updated_at_ms, revoked_at_ms \
             FROM channel_grants WHERE grant_id = ?1",
        )
        .bind(grant_id.to_string())
        .fetch_optional(&mut **tx)
        .await
        .context("failed to query channel grant")?;

        row.map(map_grant_row).transpose()
    }

    async fn get_grant_by_scope_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        channel_id: &str,
        sender_ref: Option<&str>,
        conversation_ref: Option<&str>,
        thread_ref: Option<&str>,
        routing_profile: ChannelRoutingProfile,
    ) -> Result<Option<ChannelGrantRecord>> {
        let row = sqlx::query(
            "SELECT grant_id, channel_id, sender_ref, conversation_ref, thread_ref, routing_profile, trust_tier, status, label, created_at_ms, updated_at_ms, revoked_at_ms \
             FROM channel_grants \
             WHERE channel_id = ?1 \
               AND COALESCE(sender_ref, '') = COALESCE(?2, '') \
               AND COALESCE(conversation_ref, '') = COALESCE(?3, '') \
               AND COALESCE(thread_ref, '') = COALESCE(?4, '') \
               AND routing_profile = ?5",
        )
        .bind(channel_id)
        .bind(sender_ref)
        .bind(conversation_ref)
        .bind(thread_ref)
        .bind(routing_profile.as_str())
        .fetch_optional(&mut **tx)
        .await
        .context("failed to query channel grant by scope")?;

        row.map(map_grant_row).transpose()
    }

    async fn find_pending_operator_pairing_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        channel_id: &str,
        sender_ref: Option<&str>,
        conversation_ref: Option<&str>,
        thread_ref: Option<&str>,
        requested_profile: ChannelRoutingProfile,
    ) -> Result<Option<ChannelPairingRequestRecord>> {
        let row = sqlx::query(
            "SELECT pairing_id, channel_id, code_hash, claim_policy, sender_ref, conversation_ref, thread_ref, requested_profile, status, label, max_claims, claim_count, created_at_ms, expires_at_ms, claimed_at_ms, updated_at_ms \
             FROM channel_pairing_requests \
             WHERE channel_id = ?1 \
               AND claim_policy = 'operator_approval' \
               AND COALESCE(sender_ref, '') = COALESCE(?2, '') \
               AND COALESCE(conversation_ref, '') = COALESCE(?3, '') \
               AND COALESCE(thread_ref, '') = COALESCE(?4, '') \
               AND requested_profile = ?5 \
               AND status = 'pending' \
             ORDER BY updated_at_ms DESC \
             LIMIT 1",
        )
        .bind(channel_id)
        .bind(sender_ref)
        .bind(conversation_ref)
        .bind(thread_ref)
        .bind(requested_profile.as_str())
        .fetch_optional(&mut **tx)
        .await
        .context("failed to query pending operator pairing")?;

        row.map(map_pairing_row).transpose()
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
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to start approve channel peer transaction")?;
        let approved = self
            .approve_peer_in_tx(&mut tx, channel_id, peer_id, trust_tier)
            .await?;
        tx.commit()
            .await
            .context("failed to commit approve channel peer transaction")?;
        Ok(approved)
    }

    pub(crate) async fn approve_peer_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
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
        .execute(&mut **tx)
        .await
        .context("failed to approve channel peer")?;

        if changed.rows_affected() == 0 {
            return Ok(None);
        }

        self.get_peer_in_tx(tx, channel_id, peer_id).await
    }

    pub async fn block_peer(
        &self,
        channel_id: &str,
        peer_id: &str,
    ) -> Result<Option<ChannelPeerRecord>> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to start block channel peer transaction")?;
        let blocked = self.block_peer_in_tx(&mut tx, channel_id, peer_id).await?;
        tx.commit()
            .await
            .context("failed to commit block channel peer transaction")?;
        Ok(blocked)
    }

    pub(crate) async fn block_peer_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
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
        .execute(&mut **tx)
        .await
        .context("failed to block channel peer")?;

        if changed.rows_affected() == 0 {
            return Ok(None);
        }

        self.get_peer_in_tx(tx, channel_id, peer_id).await
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

    pub async fn list_pending_peers(&self, limit: usize) -> Result<Vec<ChannelPeerRecord>> {
        let rows = sqlx::query(
            "SELECT channel_id, peer_id, status, trust_tier, pairing_code, first_seen_ms, updated_at_ms \
             FROM channel_peers \
             WHERE status = 'pending' \
             ORDER BY updated_at_ms DESC, channel_id ASC, peer_id ASC \
             LIMIT ?1",
        )
        .bind(i64::try_from(limit).context("pending peer limit is too large")?)
        .fetch_all(&self.pool)
        .await
        .context("failed to list pending channel peers")?;

        rows.into_iter().map(map_peer_row).collect()
    }

    pub(crate) async fn get_peer_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        channel_id: &str,
        peer_id: &str,
    ) -> Result<Option<ChannelPeerRecord>> {
        let row = sqlx::query(
            "SELECT channel_id, peer_id, status, trust_tier, pairing_code, first_seen_ms, updated_at_ms \
             FROM channel_peers WHERE channel_id = ?1 AND peer_id = ?2",
        )
        .bind(channel_id)
        .bind(peer_id)
        .fetch_optional(&mut **tx)
        .await
        .context("failed to query channel peer")?;

        row.map(map_peer_row).transpose()
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

    pub async fn insert_outbound_message(&self, channel_id: &str, peer_id: &str) -> Result<Uuid> {
        let message_id = Uuid::new_v4();
        let now = now_ms();

        sqlx::query(
            "INSERT INTO channel_messages \
             (message_id, channel_id, peer_id, direction, external_message_id, update_id, created_at_ms) \
             VALUES (?1, ?2, ?3, 'outbound', NULL, NULL, ?4)",
        )
        .bind(message_id.to_string())
        .bind(channel_id)
        .bind(peer_id)
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
             (channel_id, peer_id, session_id, turn_id, kind, lane, code, text, created_at_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )
        .bind(insert.channel_id)
        .bind(insert.peer_id)
        .bind(insert.session_id.map(|value| value.to_string()))
        .bind(insert.turn_id.map(|value| value.to_string()))
        .bind(insert.kind.as_str())
        .bind(insert.lane.map(StreamMessageLane::as_str))
        .bind(insert.code)
        .bind(insert.text)
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to append channel stream event")?;

        let sequence = done.last_insert_rowid();
        let row = sqlx::query(
            "SELECT sequence, channel_id, peer_id, session_id, turn_id, kind, lane, code, text, created_at_ms \
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

    pub async fn first_answer_stream_sequence_for_turn(
        &self,
        channel_id: &str,
        turn_id: Uuid,
    ) -> Result<Option<i64>> {
        let row = sqlx::query(
            "SELECT MIN(sequence) AS sequence \
             FROM channel_stream_events \
             WHERE channel_id = ?1 AND turn_id = ?2 \
               AND kind = 'message_delta' AND lane = 'answer'",
        )
        .bind(channel_id)
        .bind(turn_id.to_string())
        .fetch_one(&self.pool)
        .await
        .context("failed to query first answer channel stream event for turn")?;

        Ok(row.get::<Option<i64>, _>("sequence"))
    }

    pub async fn first_stream_sequence_for_turn(
        &self,
        channel_id: &str,
        turn_id: Uuid,
    ) -> Result<Option<i64>> {
        let row = sqlx::query(
            "SELECT MIN(sequence) AS sequence \
             FROM channel_stream_events \
             WHERE channel_id = ?1 AND turn_id = ?2",
        )
        .bind(channel_id)
        .bind(turn_id.to_string())
        .fetch_one(&self.pool)
        .await
        .context("failed to query first channel stream event for turn")?;

        Ok(row.get::<Option<i64>, _>("sequence"))
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
            "SELECT sequence, channel_id, peer_id, session_id, turn_id, kind, lane, code, text, created_at_ms \
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

    pub(crate) async fn enqueue_turn_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        turn: NewChannelTurn<'_>,
    ) -> Result<()> {
        let queued_at_ms = now_ms();
        sqlx::query(
            "INSERT INTO channel_turns \
             (turn_id, channel_id, session_key, session_id, inbound_event_id, runtime_id, status, last_error, answer_checkpoint_sequence, queued_at_ms, started_at_ms, finished_at_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL, ?8, NULL, NULL)",
        )
        .bind(turn.turn_id.to_string())
        .bind(turn.channel_id)
        .bind(turn.session_key)
        .bind(turn.session_id.to_string())
        .bind(turn.inbound_event_id)
        .bind(turn.runtime_id)
        .bind(turn.status.as_str())
        .bind(queued_at_ms)
        .execute(&mut **tx)
        .await
        .context("failed to enqueue channel turn in transaction")?;

        Ok(())
    }

    pub async fn get_turn(&self, turn_id: Uuid) -> Result<Option<ChannelTurnRecord>> {
        let row = sqlx::query(
            "SELECT turn_id, channel_id, session_key, session_id, inbound_event_id, runtime_id, status, last_error, answer_checkpoint_sequence, queued_at_ms, started_at_ms, finished_at_ms \
             FROM channel_turns WHERE turn_id = ?1",
        )
        .bind(turn_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .context("failed to query channel turn")?;

        row.map(map_turn_row).transpose()
    }

    pub async fn update_answer_checkpoint_sequence(
        &self,
        turn_id: Uuid,
        sequence: i64,
    ) -> Result<bool> {
        let changed = sqlx::query(
            "UPDATE channel_turns \
             SET answer_checkpoint_sequence = ?2 \
             WHERE turn_id = ?1 AND status = 'running'",
        )
        .bind(turn_id.to_string())
        .bind(sequence)
        .execute(&self.pool)
        .await
        .context("failed to update channel turn answer checkpoint sequence")?;

        Ok(changed.rows_affected() > 0)
    }

    pub async fn claim_next_pending_turn(
        &self,
        channel_id: &str,
        session_key: &str,
    ) -> Result<Option<ChannelTurnRecord>> {
        let row = sqlx::query(
            "SELECT turn_id \
             FROM channel_turns \
             WHERE channel_id = ?1 AND session_key = ?2 AND status = 'pending' \
             ORDER BY queued_at_ms ASC \
             LIMIT 1",
        )
        .bind(channel_id)
        .bind(session_key)
        .fetch_optional(&self.pool)
        .await
        .context("failed to select pending channel turn")?;

        let Some(row) = row else {
            return Ok(None);
        };

        let turn_id_raw: String = row.get("turn_id");
        let started_at_ms = now_ms();
        let changed = sqlx::query(
            "UPDATE channel_turns \
             SET status = 'running', started_at_ms = ?2, last_error = NULL, answer_checkpoint_sequence = NULL \
             WHERE turn_id = ?1 AND status = 'pending'",
        )
        .bind(&turn_id_raw)
        .bind(started_at_ms)
        .execute(&self.pool)
        .await
        .context("failed to claim pending channel turn")?;

        if changed.rows_affected() == 0 {
            return Ok(None);
        }

        let turn_id = Uuid::parse_str(&turn_id_raw)
            .with_context(|| format!("invalid claimed turn id '{turn_id_raw}'"))?;
        self.get_turn(turn_id).await
    }

    pub async fn complete_turn(&self, turn_id: Uuid) -> Result<bool> {
        let finished_at_ms = now_ms();
        let changed = sqlx::query(
            "UPDATE channel_turns \
             SET status = 'completed', finished_at_ms = ?2 \
             WHERE turn_id = ?1 AND status = 'running'",
        )
        .bind(turn_id.to_string())
        .bind(finished_at_ms)
        .execute(&self.pool)
        .await
        .context("failed to complete channel turn")?;

        Ok(changed.rows_affected() > 0)
    }

    pub async fn fail_turn(&self, turn_id: Uuid, last_error: &str) -> Result<bool> {
        let finished_at_ms = now_ms();
        let changed = sqlx::query(
            "UPDATE channel_turns \
             SET status = 'failed', last_error = ?2, finished_at_ms = ?3 \
             WHERE turn_id = ?1 AND status IN ('waiting_for_attachments', 'pending', 'running')",
        )
        .bind(turn_id.to_string())
        .bind(last_error)
        .bind(finished_at_ms)
        .execute(&self.pool)
        .await
        .context("failed to fail channel turn")?;

        Ok(changed.rows_affected() > 0)
    }

    pub async fn fail_running_turns(&self, last_error: &str) -> Result<u64> {
        let finished_at_ms = now_ms();
        let changed = sqlx::query(
            "UPDATE channel_turns \
             SET status = 'failed', last_error = ?1, finished_at_ms = ?2 \
             WHERE status = 'running'",
        )
        .bind(last_error)
        .bind(finished_at_ms)
        .execute(&self.pool)
        .await
        .context("failed to fail stale running channel turns")?;

        Ok(changed.rows_affected())
    }

    pub async fn has_pending_turns(&self, channel_id: &str, session_key: &str) -> Result<bool> {
        let row = sqlx::query(
            "SELECT EXISTS( \
                SELECT 1 FROM channel_turns \
                WHERE channel_id = ?1 AND session_key = ?2 AND status = 'pending' \
             ) AS has_pending",
        )
        .bind(channel_id)
        .bind(session_key)
        .fetch_one(&self.pool)
        .await
        .context("failed to query pending channel turns")?;

        Ok(row.get::<i64, _>("has_pending") != 0)
    }

    pub async fn channel_health(&self, channel_id: &str) -> Result<ChannelHealthRecord> {
        let pairing_counts = sqlx::query(
            "SELECT status, COUNT(*) AS count \
             FROM channel_pairing_requests \
             WHERE channel_id = ?1 \
             GROUP BY status",
        )
        .bind(channel_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to query channel pairing counts")?;

        let grant_counts = sqlx::query(
            "SELECT status, COUNT(*) AS count \
             FROM channel_grants \
             WHERE channel_id = ?1 AND status IN ('approved', 'blocked') \
             GROUP BY status",
        )
        .bind(channel_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to query channel grant counts")?;

        let latest_inbound_row = sqlx::query(
            "SELECT MAX(created_at_ms) AS latest_inbound_at_ms \
             FROM channel_inbound_events \
             WHERE channel_id = ?1",
        )
        .bind(channel_id)
        .fetch_one(&self.pool)
        .await
        .context("failed to query channel inbound activity")?;

        let latest_outbound_row = sqlx::query(
            "SELECT MAX(created_at_ms) AS latest_outbound_at_ms \
             FROM channel_messages \
             WHERE channel_id = ?1 AND direction = 'outbound'",
        )
        .bind(channel_id)
        .fetch_one(&self.pool)
        .await
        .context("failed to query channel outbound activity")?;

        let mut pending_peer_count = 0_u64;
        let mut approved_peer_count = 0_u64;
        let mut blocked_peer_count = 0_u64;

        for row in pairing_counts {
            let status: String = row.get("status");
            let count_raw: i64 = row.get("count");
            let count = u64::try_from(count_raw)
                .with_context(|| format!("invalid channel pairing count '{count_raw}'"))?;
            if status.as_str() == "pending" {
                pending_peer_count = count;
            }
        }

        for row in grant_counts {
            let status: String = row.get("status");
            let count_raw: i64 = row.get("count");
            let count = u64::try_from(count_raw)
                .with_context(|| format!("invalid channel grant count '{count_raw}'"))?;
            match status.as_str() {
                "approved" => approved_peer_count = count,
                "blocked" => blocked_peer_count = blocked_peer_count.saturating_add(count),
                _ => {}
            }
        }

        let latest_inbound_at = latest_inbound_row
            .get::<Option<i64>, _>("latest_inbound_at_ms")
            .map(|value| {
                ms_to_datetime(value)
                    .ok_or_else(|| anyhow!("invalid latest_inbound_at_ms '{value}'"))
            })
            .transpose()?;
        let latest_outbound_at = latest_outbound_row
            .get::<Option<i64>, _>("latest_outbound_at_ms")
            .map(|value| {
                ms_to_datetime(value)
                    .ok_or_else(|| anyhow!("invalid latest_outbound_at_ms '{value}'"))
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

fn map_peer_row(row: SqliteRow) -> Result<ChannelPeerRecord> {
    let status_raw: String = row.get("status");
    let trust_tier_raw: String = row.get("trust_tier");
    let first_seen_ms: i64 = row.get("first_seen_ms");
    let updated_at_ms: i64 = row.get("updated_at_ms");

    let status = ChannelPeerStatus::from_str(&status_raw)
        .map_err(|err| anyhow!("invalid channel peer status: {err}"))?;
    let trust_tier =
        TrustTier::from_str(&trust_tier_raw).map_err(|err| anyhow!("invalid trust tier: {err}"))?;
    let first_seen = ms_to_datetime(first_seen_ms)
        .ok_or_else(|| anyhow!("invalid first_seen_ms '{first_seen_ms}'"))?;
    let updated_at = ms_to_datetime(updated_at_ms)
        .ok_or_else(|| anyhow!("invalid updated_at_ms '{updated_at_ms}'"))?;

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

fn map_pairing_row(row: SqliteRow) -> Result<ChannelPairingRequestRecord> {
    let pairing_id_raw: String = row.get("pairing_id");
    let requested_profile_raw: String = row.get("requested_profile");
    let status_raw: String = row.get("status");
    let created_at_ms: i64 = row.get("created_at_ms");
    let updated_at_ms: i64 = row.get("updated_at_ms");
    let pairing_id = Uuid::parse_str(&pairing_id_raw)
        .with_context(|| format!("invalid pairing_id '{pairing_id_raw}'"))?;
    let requested_profile = ChannelRoutingProfile::from_str(&requested_profile_raw)
        .map_err(|err| anyhow!("invalid channel routing profile: {err}"))?;
    let status = ChannelPairingStatus::from_str(&status_raw)
        .map_err(|err| anyhow!("invalid channel pairing status: {err}"))?;
    let created_at = ms_to_datetime(created_at_ms)
        .ok_or_else(|| anyhow!("invalid created_at_ms '{created_at_ms}'"))?;
    let updated_at = ms_to_datetime(updated_at_ms)
        .ok_or_else(|| anyhow!("invalid updated_at_ms '{updated_at_ms}'"))?;
    let expires_at = optional_datetime(row.get("expires_at_ms"), "expires_at_ms")?;
    let claimed_at = optional_datetime(row.get("claimed_at_ms"), "claimed_at_ms")?;

    Ok(ChannelPairingRequestRecord {
        pairing_id,
        channel_id: row.get("channel_id"),
        code_hash: row.get("code_hash"),
        claim_policy: row.get("claim_policy"),
        sender_ref: row.get("sender_ref"),
        conversation_ref: row.get("conversation_ref"),
        thread_ref: row.get("thread_ref"),
        requested_profile,
        status,
        label: row.get("label"),
        max_claims: row.get("max_claims"),
        claim_count: row.get("claim_count"),
        created_at,
        expires_at,
        claimed_at,
        updated_at,
    })
}

fn map_grant_row(row: SqliteRow) -> Result<ChannelGrantRecord> {
    let grant_id_raw: String = row.get("grant_id");
    let routing_profile_raw: String = row.get("routing_profile");
    let trust_tier_raw: String = row.get("trust_tier");
    let status_raw: String = row.get("status");
    let created_at_ms: i64 = row.get("created_at_ms");
    let updated_at_ms: i64 = row.get("updated_at_ms");
    let grant_id = Uuid::parse_str(&grant_id_raw)
        .with_context(|| format!("invalid grant_id '{grant_id_raw}'"))?;
    let routing_profile = ChannelRoutingProfile::from_str(&routing_profile_raw)
        .map_err(|err| anyhow!("invalid channel routing profile: {err}"))?;
    let trust_tier =
        TrustTier::from_str(&trust_tier_raw).map_err(|err| anyhow!("invalid trust tier: {err}"))?;
    let status = ChannelGrantStatus::from_str(&status_raw)
        .map_err(|err| anyhow!("invalid channel grant status: {err}"))?;
    let created_at = ms_to_datetime(created_at_ms)
        .ok_or_else(|| anyhow!("invalid created_at_ms '{created_at_ms}'"))?;
    let updated_at = ms_to_datetime(updated_at_ms)
        .ok_or_else(|| anyhow!("invalid updated_at_ms '{updated_at_ms}'"))?;
    let revoked_at = optional_datetime(row.get("revoked_at_ms"), "revoked_at_ms")?;

    Ok(ChannelGrantRecord {
        grant_id,
        channel_id: row.get("channel_id"),
        sender_ref: row.get("sender_ref"),
        conversation_ref: row.get("conversation_ref"),
        thread_ref: row.get("thread_ref"),
        routing_profile,
        trust_tier,
        status,
        label: row.get("label"),
        created_at,
        updated_at,
        revoked_at,
    })
}

fn map_inbound_event_row(row: SqliteRow) -> Result<ChannelInboundEventRecord> {
    let trigger_raw: String = row.get("trigger");
    let attachments_raw: String = row.get("attachments_json");
    let provider_metadata_raw: String = row.get("provider_metadata_json");
    let received_at_ms: i64 = row.get("received_at_ms");
    let created_at_ms: i64 = row.get("created_at_ms");
    let trigger = ChannelTrigger::from_str(&trigger_raw)
        .map_err(|err| anyhow!("invalid channel trigger: {err}"))?;
    let attachments = serde_json::from_str(&attachments_raw)
        .with_context(|| format!("invalid channel attachments '{attachments_raw}'"))?;
    let provider_metadata = serde_json::from_str(&provider_metadata_raw)
        .with_context(|| format!("invalid provider metadata '{provider_metadata_raw}'"))?;
    let received_at = ms_to_datetime(received_at_ms)
        .ok_or_else(|| anyhow!("invalid received_at_ms '{received_at_ms}'"))?;
    let created_at = ms_to_datetime(created_at_ms)
        .ok_or_else(|| anyhow!("invalid created_at_ms '{created_at_ms}'"))?;

    Ok(ChannelInboundEventRecord {
        event_id: row.get("event_id"),
        channel_id: row.get("channel_id"),
        sender_ref: row.get("sender_ref"),
        conversation_ref: row.get("conversation_ref"),
        thread_ref: row.get("thread_ref"),
        message_ref: row.get("message_ref"),
        trigger,
        attachments,
        reply_to_ref: row.get("reply_to_ref"),
        provider_metadata,
        received_at,
        created_at,
    })
}

fn map_stream_event_row(row: SqliteRow) -> Result<ChannelStreamEventRecord> {
    let created_at_ms: i64 = row.get("created_at_ms");
    let created_at = ms_to_datetime(created_at_ms)
        .ok_or_else(|| anyhow!("invalid created_at_ms '{created_at_ms}'"))?;
    let session_id = optional_uuid(row.get("session_id"), "session_id")?;
    let turn_id = optional_uuid(row.get("turn_id"), "turn_id")?;
    let kind_raw: String = row.get("kind");
    let kind = ChannelStreamEventKind::from_str(&kind_raw)
        .map_err(|err| anyhow!("invalid channel stream event kind: {err}"))?;
    let lane = row
        .get::<Option<String>, _>("lane")
        .map(|raw| {
            StreamMessageLane::from_str(&raw)
                .map_err(|err| anyhow!("invalid stream message lane: {err}"))
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
        code: row.get("code"),
        text: row.get("text"),
        created_at,
    })
}

fn optional_uuid(raw: Option<String>, column: &str) -> Result<Option<Uuid>> {
    raw.map(|value| {
        Uuid::parse_str(&value).map_err(|err| anyhow!("invalid {column} '{value}': {err}"))
    })
    .transpose()
}

fn optional_datetime(raw: Option<i64>, column: &str) -> Result<Option<DateTime<Utc>>> {
    raw.map(|value| ms_to_datetime(value).ok_or_else(|| anyhow!("invalid {column} '{value}'")))
        .transpose()
}

fn map_turn_row(row: SqliteRow) -> Result<ChannelTurnRecord> {
    let turn_id_raw: String = row.get("turn_id");
    let session_id_raw: String = row.get("session_id");
    let status_raw: String = row.get("status");
    let queued_at_ms: i64 = row.get("queued_at_ms");

    let turn_id = Uuid::parse_str(&turn_id_raw)
        .with_context(|| format!("invalid turn_id '{turn_id_raw}'"))?;
    let session_id = Uuid::parse_str(&session_id_raw)
        .with_context(|| format!("invalid session_id '{session_id_raw}'"))?;
    let status = ChannelTurnStatus::from_str(&status_raw)
        .map_err(|err| anyhow!("invalid channel turn status: {err}"))?;
    let queued_at = ms_to_datetime(queued_at_ms)
        .ok_or_else(|| anyhow!("invalid queued_at_ms '{queued_at_ms}'"))?;
    let started_at = row
        .get::<Option<i64>, _>("started_at_ms")
        .map(|value| {
            ms_to_datetime(value).ok_or_else(|| anyhow!("invalid started_at_ms '{value}'"))
        })
        .transpose()?;
    let finished_at = row
        .get::<Option<i64>, _>("finished_at_ms")
        .map(|value| {
            ms_to_datetime(value).ok_or_else(|| anyhow!("invalid finished_at_ms '{value}'"))
        })
        .transpose()?;

    Ok(ChannelTurnRecord {
        turn_id,
        channel_id: row.get("channel_id"),
        session_key: row.get("session_key"),
        session_id,
        inbound_event_id: row.get("inbound_event_id"),
        runtime_id: row.get("runtime_id"),
        status,
        last_error: row.get("last_error"),
        answer_checkpoint_sequence: row.get("answer_checkpoint_sequence"),
        queued_at,
        started_at,
        finished_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::db::Db;

    async fn new_store() -> ChannelStateStore {
        let db = Db::connect_memory().await.expect("connect memory db");
        ChannelStateStore::new(db.pool())
    }

    #[tokio::test]
    async fn list_pending_peers_returns_newest_pending_only() {
        let store = new_store().await;
        store
            .upsert_pending_peer("terminal", "alice", "code-a")
            .await
            .expect("seed alice");
        store
            .upsert_pending_peer("terminal", "bob", "code-b")
            .await
            .expect("seed bob");
        store
            .upsert_pending_peer("matrix", "carol", "code-c")
            .await
            .expect("seed carol");
        store
            .approve_peer("terminal", "alice", TrustTier::Main)
            .await
            .expect("approve alice");

        sqlx::query(
            "UPDATE channel_peers \
             SET updated_at_ms = CASE peer_id \
                WHEN 'alice' THEN 1000 \
                WHEN 'bob' THEN 3000 \
                WHEN 'carol' THEN 2000 \
                ELSE updated_at_ms \
             END",
        )
        .execute(store.pool())
        .await
        .expect("set peer timestamps");

        let peers = store
            .list_pending_peers(2)
            .await
            .expect("list pending peers");
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].peer_id, "bob");
        assert_eq!(peers[1].peer_id, "carol");
        assert!(peers
            .iter()
            .all(|peer| peer.status == ChannelPeerStatus::Pending));
    }
}
