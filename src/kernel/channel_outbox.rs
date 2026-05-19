use std::{str::FromStr, time::Duration};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{sqlite::SqliteRow, Row, Sqlite, SqlitePool, Transaction};
use uuid::Uuid;

use crate::kernel::db::{ms_to_datetime, now_ms};

pub(crate) const DEFAULT_CHANNEL_OUTBOX_LEASE_MS: u64 = 120_000;
pub(crate) const MAX_CHANNEL_OUTBOX_LEASE_MS: u64 = 15 * 60 * 1000;
pub(crate) const DEFAULT_CHANNEL_OUTBOX_PULL_LIMIT: usize = 10;
pub(crate) const MAX_CHANNEL_OUTBOX_PULL_LIMIT: usize = 100;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelDeliveryContent {
    pub text: String,
    #[serde(default = "default_channel_delivery_format_hint")]
    pub format_hint: String,
    #[serde(default)]
    pub attachments: Vec<ChannelDeliveryAttachment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelDeliveryAttachment {
    pub attachment_id: String,
    pub path: String,
    #[serde(default)]
    pub filename: Option<String>,
    #[serde(default)]
    pub mime_type: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelOutboxDeliveryStatus {
    Pending,
    Leased,
    Delivered,
    Failed,
}

impl ChannelOutboxDeliveryStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Leased => "leased",
            Self::Delivered => "delivered",
            Self::Failed => "failed",
        }
    }
}

impl FromStr for ChannelOutboxDeliveryStatus {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "leased" => Ok(Self::Leased),
            "delivered" => Ok(Self::Delivered),
            "failed" => Ok(Self::Failed),
            other => Err(format!("invalid channel outbox delivery status '{other}'")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelOutboxAttemptStatus {
    Leased,
    Delivered,
    RetryableFailed,
    TerminalFailed,
    StaleRejected,
}

impl ChannelOutboxAttemptStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Leased => "leased",
            Self::Delivered => "delivered",
            Self::RetryableFailed => "retryable_failed",
            Self::TerminalFailed => "terminal_failed",
            Self::StaleRejected => "stale_rejected",
        }
    }
}

impl FromStr for ChannelOutboxAttemptStatus {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "leased" => Ok(Self::Leased),
            "delivered" => Ok(Self::Delivered),
            "retryable_failed" => Ok(Self::RetryableFailed),
            "terminal_failed" => Ok(Self::TerminalFailed),
            "stale_rejected" => Ok(Self::StaleRejected),
            other => Err(format!("invalid channel outbox attempt status '{other}'")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelOutboxReportOutcome {
    Delivered,
    RetryableFailed,
    TerminalFailed,
}

#[derive(Debug, Clone)]
pub struct ChannelDeliveryRoute<'a> {
    pub channel_id: &'a str,
    pub conversation_ref: &'a str,
    pub thread_ref: Option<&'a str>,
    pub reply_to_ref: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct NewChannelDelivery<'a> {
    pub route: ChannelDeliveryRoute<'a>,
    pub session_id: Option<Uuid>,
    pub turn_id: Option<Uuid>,
    pub source_kind: Option<&'a str>,
    pub source_id: Option<&'a str>,
    pub content: ChannelDeliveryContent,
}

#[derive(Debug, Clone)]
pub struct ChannelOutboxPull {
    pub channel_id: String,
    pub worker_id: String,
    pub conversation_ref: Option<String>,
    pub thread_ref: Option<String>,
    pub limit: usize,
    pub lease_ms: u64,
}

#[derive(Debug, Clone)]
pub struct ChannelOutboxReport {
    pub delivery_id: Uuid,
    pub attempt_id: Uuid,
    pub channel_id: String,
    pub worker_id: String,
    pub outcome: ChannelOutboxReportOutcome,
    pub provider_receipt: Option<Value>,
    pub error_code: Option<String>,
    pub error_text: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ChannelDeliveryRecord {
    pub delivery_id: Uuid,
    pub channel_id: String,
    pub conversation_ref: String,
    pub thread_ref: Option<String>,
    pub reply_to_ref: Option<String>,
    pub session_id: Option<Uuid>,
    pub turn_id: Option<Uuid>,
    pub source_kind: Option<String>,
    pub source_id: Option<String>,
    pub status: ChannelOutboxDeliveryStatus,
    pub content: ChannelDeliveryContent,
    pub provider_receipt: Option<Value>,
    pub attempt_count: u32,
    pub next_attempt_at: DateTime<Utc>,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub current_attempt_id: Option<Uuid>,
    pub last_error_code: Option<String>,
    pub last_error_text: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub delivered_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct ChannelDeliveryLease {
    pub delivery: ChannelDeliveryRecord,
    pub attempt_id: Uuid,
    pub lease_expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ChannelOutboxReportResult {
    pub accepted: bool,
    pub delivery: ChannelDeliveryRecord,
    pub attempt_status: ChannelOutboxAttemptStatus,
    pub next_attempt_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct ChannelOutboxStore {
    pool: SqlitePool,
}

impl ChannelOutboxStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn enqueue_delivery(
        &self,
        input: NewChannelDelivery<'_>,
    ) -> Result<ChannelDeliveryRecord> {
        let delivery_id = Uuid::new_v4();
        let now = now_ms();
        let content_json = serde_json::to_string(&input.content)
            .context("failed to encode channel outbox content")?;

        sqlx::query(
            "INSERT INTO channel_outbox_messages \
             (delivery_id, channel_id, conversation_ref, thread_ref, reply_to_ref, session_id, turn_id, \
              source_kind, source_id, status, content_json, provider_receipt_json, attempt_count, \
              next_attempt_at_ms, lease_owner, lease_expires_at_ms, current_attempt_id, last_error_code, \
              last_error_text, created_at_ms, updated_at_ms, delivered_at_ms, failed_at_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'pending', ?10, NULL, 0, ?11, \
              NULL, NULL, NULL, NULL, NULL, ?11, ?11, NULL, NULL)",
        )
        .bind(delivery_id.to_string())
        .bind(input.route.channel_id)
        .bind(input.route.conversation_ref)
        .bind(input.route.thread_ref)
        .bind(input.route.reply_to_ref)
        .bind(input.session_id.map(|value| value.to_string()))
        .bind(input.turn_id.map(|value| value.to_string()))
        .bind(input.source_kind)
        .bind(input.source_id)
        .bind(content_json)
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to enqueue channel outbox delivery")?;

        self.get_delivery(delivery_id)
            .await?
            .ok_or_else(|| anyhow!("enqueued channel outbox delivery disappeared"))
    }

    pub async fn get_delivery(&self, delivery_id: Uuid) -> Result<Option<ChannelDeliveryRecord>> {
        let row = sqlx::query(
            "SELECT delivery_id, channel_id, conversation_ref, thread_ref, reply_to_ref, session_id, turn_id, \
             source_kind, source_id, status, content_json, provider_receipt_json, attempt_count, \
             next_attempt_at_ms, lease_owner, lease_expires_at_ms, current_attempt_id, last_error_code, \
             last_error_text, created_at_ms, updated_at_ms, delivered_at_ms, failed_at_ms \
             FROM channel_outbox_messages WHERE delivery_id = ?1",
        )
        .bind(delivery_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .context("failed to query channel outbox delivery")?;

        row.map(map_delivery_row).transpose()
    }

    pub async fn pull_due(&self, pull: ChannelOutboxPull) -> Result<Vec<ChannelDeliveryLease>> {
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .context("failed to start channel outbox pull transaction")?;
        let now = now_ms();
        let lease_ms = i64::try_from(pull.lease_ms).context("invalid channel outbox lease")?;
        let lease_expires_at_ms = now.saturating_add(lease_ms);
        let rows = sqlx::query(
            "SELECT delivery_id, current_attempt_id \
             FROM channel_outbox_messages \
             WHERE channel_id = ?1 \
               AND (?4 IS NULL OR conversation_ref = ?4) \
               AND (?5 IS NULL OR thread_ref = ?5) \
               AND ( \
                 (status = 'pending' AND next_attempt_at_ms <= ?2) OR \
                 (status = 'leased' AND lease_expires_at_ms IS NOT NULL AND lease_expires_at_ms <= ?2) \
               ) \
             ORDER BY created_at_ms ASC, delivery_id ASC \
             LIMIT ?3",
        )
        .bind(&pull.channel_id)
        .bind(now)
        .bind(i64::try_from(pull.limit).context("invalid channel outbox pull limit")?)
        .bind(pull.conversation_ref.as_deref())
        .bind(pull.thread_ref.as_deref())
        .fetch_all(&mut *tx)
        .await
        .context("failed to select due channel outbox deliveries")?;

        let mut leases = Vec::with_capacity(rows.len());
        for row in rows {
            let delivery_id_raw: String = row.get("delivery_id");
            let prior_attempt_id: Option<String> = row.get("current_attempt_id");
            let delivery_id = Uuid::parse_str(&delivery_id_raw).with_context(|| {
                format!("invalid channel outbox delivery id '{delivery_id_raw}'")
            })?;
            if let Some(prior_attempt_id) = prior_attempt_id {
                sqlx::query(
                    "UPDATE channel_outbox_attempts \
                     SET status = 'stale_rejected', error_code = 'lease_expired', \
                         error_text = 'lease expired before report', finished_at_ms = ?2 \
                     WHERE attempt_id = ?1 AND status = 'leased'",
                )
                .bind(prior_attempt_id)
                .bind(now)
                .execute(&mut *tx)
                .await
                .context("failed to close expired channel outbox attempt")?;
            }

            let attempt_id = Uuid::new_v4();
            sqlx::query(
                "UPDATE channel_outbox_messages \
                 SET status = 'leased', attempt_count = attempt_count + 1, lease_owner = ?2, \
                     lease_expires_at_ms = ?3, current_attempt_id = ?4, updated_at_ms = ?5 \
                 WHERE delivery_id = ?1 \
                   AND channel_id = ?6 \
                   AND ( \
                     (status = 'pending' AND next_attempt_at_ms <= ?5) OR \
                     (status = 'leased' AND lease_expires_at_ms IS NOT NULL AND lease_expires_at_ms <= ?5) \
                   )",
            )
            .bind(delivery_id.to_string())
            .bind(&pull.worker_id)
            .bind(lease_expires_at_ms)
            .bind(attempt_id.to_string())
            .bind(now)
            .bind(&pull.channel_id)
            .execute(&mut *tx)
            .await
            .context("failed to lease channel outbox delivery")?;

            sqlx::query(
                "INSERT INTO channel_outbox_attempts \
                 (attempt_id, delivery_id, channel_id, worker_id, status, provider_receipt_json, \
                  error_code, error_text, started_at_ms, finished_at_ms) \
                 VALUES (?1, ?2, ?3, ?4, 'leased', NULL, NULL, NULL, ?5, NULL)",
            )
            .bind(attempt_id.to_string())
            .bind(delivery_id.to_string())
            .bind(&pull.channel_id)
            .bind(&pull.worker_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .context("failed to insert channel outbox attempt")?;

            let delivery = self
                .get_delivery_in_tx(&mut tx, delivery_id)
                .await?
                .ok_or_else(|| anyhow!("leased channel outbox delivery disappeared"))?;
            let lease_expires_at = delivery
                .lease_expires_at
                .ok_or_else(|| anyhow!("leased channel outbox delivery missing lease expiry"))?;
            leases.push(ChannelDeliveryLease {
                delivery,
                attempt_id,
                lease_expires_at,
            });
        }

        tx.commit()
            .await
            .context("failed to commit channel outbox pull")?;
        Ok(leases)
    }

    pub async fn report(&self, report: ChannelOutboxReport) -> Result<ChannelOutboxReportResult> {
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .context("failed to start channel outbox report transaction")?;
        let now = now_ms();
        let delivery = self
            .get_delivery_in_tx(&mut tx, report.delivery_id)
            .await?
            .ok_or_else(|| anyhow!("channel outbox delivery not found"))?;
        let attempt = self
            .get_attempt_in_tx(&mut tx, report.attempt_id)
            .await?
            .ok_or_else(|| anyhow!("channel outbox attempt not found"))?;

        if attempt.delivery_id != report.delivery_id {
            return Err(anyhow!(
                "channel outbox attempt does not belong to delivery"
            ));
        }
        if delivery.channel_id != report.channel_id || attempt.channel_id != report.channel_id {
            return Err(anyhow!("channel outbox channel mismatch"));
        }
        if attempt.worker_id != report.worker_id {
            return Err(anyhow!("channel outbox worker mismatch"));
        }

        let is_current = delivery.status == ChannelOutboxDeliveryStatus::Leased
            && delivery.current_attempt_id == Some(report.attempt_id)
            && delivery.lease_owner.as_deref() == Some(report.worker_id.as_str());

        if !is_current {
            let attempt_status = if attempt.status == ChannelOutboxAttemptStatus::Leased {
                self.finish_attempt(
                    &mut tx,
                    ChannelOutboxAttemptFinish {
                        attempt_id: report.attempt_id,
                        status: ChannelOutboxAttemptStatus::StaleRejected,
                        provider_receipt: report.provider_receipt.as_ref(),
                        error_code: report.error_code.as_deref().or(Some("stale_report")),
                        error_text: report
                            .error_text
                            .as_deref()
                            .or(Some("stale outbox report rejected")),
                        finished_at_ms: now,
                    },
                )
                .await?;
                ChannelOutboxAttemptStatus::StaleRejected
            } else {
                attempt.status
            };
            let delivery = self
                .get_delivery_in_tx(&mut tx, report.delivery_id)
                .await?
                .ok_or_else(|| anyhow!("channel outbox delivery disappeared after stale report"))?;
            tx.commit()
                .await
                .context("failed to commit stale channel outbox report")?;
            return Ok(ChannelOutboxReportResult {
                accepted: false,
                delivery,
                attempt_status,
                next_attempt_at: None,
            });
        }

        let receipt_json = encode_optional_json(report.provider_receipt.as_ref())?;
        match report.outcome {
            ChannelOutboxReportOutcome::Delivered => {
                sqlx::query(
                    "UPDATE channel_outbox_messages \
                     SET status = 'delivered', provider_receipt_json = ?2, lease_owner = NULL, \
                         lease_expires_at_ms = NULL, current_attempt_id = NULL, last_error_code = NULL, \
                         last_error_text = NULL, updated_at_ms = ?3, delivered_at_ms = ?3 \
                     WHERE delivery_id = ?1",
                )
                .bind(report.delivery_id.to_string())
                .bind(receipt_json.as_deref())
                .bind(now)
                .execute(&mut *tx)
                .await
                .context("failed to mark channel outbox delivery delivered")?;
                self.finish_attempt(
                    &mut tx,
                    ChannelOutboxAttemptFinish {
                        attempt_id: report.attempt_id,
                        status: ChannelOutboxAttemptStatus::Delivered,
                        provider_receipt: report.provider_receipt.as_ref(),
                        error_code: None,
                        error_text: None,
                        finished_at_ms: now,
                    },
                )
                .await?;
            }
            ChannelOutboxReportOutcome::RetryableFailed => {
                let next_attempt_at_ms = now.saturating_add(retry_delay_ms(delivery.attempt_count));
                sqlx::query(
                    "UPDATE channel_outbox_messages \
                     SET status = 'pending', next_attempt_at_ms = ?2, lease_owner = NULL, \
                         lease_expires_at_ms = NULL, current_attempt_id = NULL, last_error_code = ?3, \
                         last_error_text = ?4, updated_at_ms = ?5 \
                     WHERE delivery_id = ?1",
                )
                .bind(report.delivery_id.to_string())
                .bind(next_attempt_at_ms)
                .bind(report.error_code.as_deref())
                .bind(report.error_text.as_deref())
                .bind(now)
                .execute(&mut *tx)
                .await
                .context("failed to reschedule channel outbox delivery")?;
                self.finish_attempt(
                    &mut tx,
                    ChannelOutboxAttemptFinish {
                        attempt_id: report.attempt_id,
                        status: ChannelOutboxAttemptStatus::RetryableFailed,
                        provider_receipt: report.provider_receipt.as_ref(),
                        error_code: report.error_code.as_deref(),
                        error_text: report.error_text.as_deref(),
                        finished_at_ms: now,
                    },
                )
                .await?;
            }
            ChannelOutboxReportOutcome::TerminalFailed => {
                sqlx::query(
                    "UPDATE channel_outbox_messages \
                     SET status = 'failed', lease_owner = NULL, lease_expires_at_ms = NULL, \
                         current_attempt_id = NULL, last_error_code = ?2, last_error_text = ?3, \
                         updated_at_ms = ?4, failed_at_ms = ?4 \
                     WHERE delivery_id = ?1",
                )
                .bind(report.delivery_id.to_string())
                .bind(report.error_code.as_deref())
                .bind(report.error_text.as_deref())
                .bind(now)
                .execute(&mut *tx)
                .await
                .context("failed to mark channel outbox delivery failed")?;
                self.finish_attempt(
                    &mut tx,
                    ChannelOutboxAttemptFinish {
                        attempt_id: report.attempt_id,
                        status: ChannelOutboxAttemptStatus::TerminalFailed,
                        provider_receipt: report.provider_receipt.as_ref(),
                        error_code: report.error_code.as_deref(),
                        error_text: report.error_text.as_deref(),
                        finished_at_ms: now,
                    },
                )
                .await?;
            }
        }

        let delivery = self
            .get_delivery_in_tx(&mut tx, report.delivery_id)
            .await?
            .ok_or_else(|| anyhow!("channel outbox delivery disappeared after report"))?;
        let next_attempt_at = if delivery.status == ChannelOutboxDeliveryStatus::Pending {
            Some(delivery.next_attempt_at)
        } else {
            None
        };
        let attempt_status = match report.outcome {
            ChannelOutboxReportOutcome::Delivered => ChannelOutboxAttemptStatus::Delivered,
            ChannelOutboxReportOutcome::RetryableFailed => {
                ChannelOutboxAttemptStatus::RetryableFailed
            }
            ChannelOutboxReportOutcome::TerminalFailed => {
                ChannelOutboxAttemptStatus::TerminalFailed
            }
        };
        tx.commit()
            .await
            .context("failed to commit channel outbox report")?;

        Ok(ChannelOutboxReportResult {
            accepted: true,
            delivery,
            attempt_status,
            next_attempt_at,
        })
    }

    async fn get_delivery_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        delivery_id: Uuid,
    ) -> Result<Option<ChannelDeliveryRecord>> {
        let row = sqlx::query(
            "SELECT delivery_id, channel_id, conversation_ref, thread_ref, reply_to_ref, session_id, turn_id, \
             source_kind, source_id, status, content_json, provider_receipt_json, attempt_count, \
             next_attempt_at_ms, lease_owner, lease_expires_at_ms, current_attempt_id, last_error_code, \
             last_error_text, created_at_ms, updated_at_ms, delivered_at_ms, failed_at_ms \
             FROM channel_outbox_messages WHERE delivery_id = ?1",
        )
        .bind(delivery_id.to_string())
        .fetch_optional(&mut **tx)
        .await
        .context("failed to query channel outbox delivery in transaction")?;

        row.map(map_delivery_row).transpose()
    }

    async fn get_attempt_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        attempt_id: Uuid,
    ) -> Result<Option<ChannelOutboxAttemptRecord>> {
        let row = sqlx::query(
            "SELECT delivery_id, channel_id, worker_id, status \
             FROM channel_outbox_attempts WHERE attempt_id = ?1",
        )
        .bind(attempt_id.to_string())
        .fetch_optional(&mut **tx)
        .await
        .context("failed to query channel outbox attempt")?;

        row.map(map_attempt_row).transpose()
    }

    async fn finish_attempt(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        finish: ChannelOutboxAttemptFinish<'_>,
    ) -> Result<()> {
        let provider_receipt_json = encode_optional_json(finish.provider_receipt)?;
        sqlx::query(
            "UPDATE channel_outbox_attempts \
             SET status = ?2, provider_receipt_json = ?3, error_code = ?4, error_text = ?5, finished_at_ms = ?6 \
             WHERE attempt_id = ?1",
        )
        .bind(finish.attempt_id.to_string())
        .bind(finish.status.as_str())
        .bind(provider_receipt_json.as_deref())
        .bind(finish.error_code)
        .bind(finish.error_text)
        .bind(finish.finished_at_ms)
        .execute(&mut **tx)
        .await
        .context("failed to finish channel outbox attempt")?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ChannelOutboxAttemptRecord {
    delivery_id: Uuid,
    channel_id: String,
    worker_id: String,
    status: ChannelOutboxAttemptStatus,
}

struct ChannelOutboxAttemptFinish<'a> {
    attempt_id: Uuid,
    status: ChannelOutboxAttemptStatus,
    provider_receipt: Option<&'a Value>,
    error_code: Option<&'a str>,
    error_text: Option<&'a str>,
    finished_at_ms: i64,
}

fn retry_delay_ms(attempt_count: u32) -> i64 {
    let exponent = attempt_count.saturating_sub(1).min(10);
    let delay = Duration::from_secs(60).saturating_mul(1_u32 << exponent);
    i64::try_from(delay.min(Duration::from_secs(15 * 60)).as_millis()).unwrap_or(900_000)
}

fn default_channel_delivery_format_hint() -> String {
    "plain".to_string()
}

fn encode_optional_json(value: Option<&Value>) -> Result<Option<String>> {
    value
        .map(|raw| serde_json::to_string(raw).context("failed to encode channel outbox json"))
        .transpose()
}

fn parse_optional_json(raw: Option<String>, column: &str) -> Result<Option<Value>> {
    raw.map(|value| {
        serde_json::from_str(&value)
            .with_context(|| format!("invalid channel outbox {column} json '{value}'"))
    })
    .transpose()
}

fn optional_uuid(raw: Option<String>, column: &str) -> Result<Option<Uuid>> {
    raw.map(|value| {
        Uuid::parse_str(&value).map_err(|err| anyhow!("invalid {column} '{value}': {err}"))
    })
    .transpose()
}

fn required_datetime(raw: i64, column: &str) -> Result<DateTime<Utc>> {
    ms_to_datetime(raw).ok_or_else(|| anyhow!("invalid {column} '{raw}'"))
}

fn optional_datetime(raw: Option<i64>, column: &str) -> Result<Option<DateTime<Utc>>> {
    raw.map(|value| required_datetime(value, column))
        .transpose()
}

fn map_delivery_row(row: SqliteRow) -> Result<ChannelDeliveryRecord> {
    let delivery_id_raw: String = row.get("delivery_id");
    let status_raw: String = row.get("status");
    let content_raw: String = row.get("content_json");
    let attempt_count_raw: i64 = row.get("attempt_count");
    let next_attempt_at_ms: i64 = row.get("next_attempt_at_ms");
    let created_at_ms: i64 = row.get("created_at_ms");
    let updated_at_ms: i64 = row.get("updated_at_ms");
    let delivery_id = Uuid::parse_str(&delivery_id_raw)
        .with_context(|| format!("invalid delivery_id '{delivery_id_raw}'"))?;
    let status = ChannelOutboxDeliveryStatus::from_str(&status_raw)
        .map_err(|err| anyhow!("invalid channel outbox status: {err}"))?;
    let content = serde_json::from_str(&content_raw)
        .with_context(|| format!("invalid channel outbox content '{content_raw}'"))?;
    let attempt_count = u32::try_from(attempt_count_raw)
        .with_context(|| format!("invalid attempt_count '{attempt_count_raw}'"))?;

    Ok(ChannelDeliveryRecord {
        delivery_id,
        channel_id: row.get("channel_id"),
        conversation_ref: row.get("conversation_ref"),
        thread_ref: row.get("thread_ref"),
        reply_to_ref: row.get("reply_to_ref"),
        session_id: optional_uuid(row.get("session_id"), "session_id")?,
        turn_id: optional_uuid(row.get("turn_id"), "turn_id")?,
        source_kind: row.get("source_kind"),
        source_id: row.get("source_id"),
        status,
        content,
        provider_receipt: parse_optional_json(
            row.get("provider_receipt_json"),
            "provider_receipt",
        )?,
        attempt_count,
        next_attempt_at: required_datetime(next_attempt_at_ms, "next_attempt_at_ms")?,
        lease_owner: row.get("lease_owner"),
        lease_expires_at: optional_datetime(row.get("lease_expires_at_ms"), "lease_expires_at_ms")?,
        current_attempt_id: optional_uuid(row.get("current_attempt_id"), "current_attempt_id")?,
        last_error_code: row.get("last_error_code"),
        last_error_text: row.get("last_error_text"),
        created_at: required_datetime(created_at_ms, "created_at_ms")?,
        updated_at: required_datetime(updated_at_ms, "updated_at_ms")?,
        delivered_at: optional_datetime(row.get("delivered_at_ms"), "delivered_at_ms")?,
        failed_at: optional_datetime(row.get("failed_at_ms"), "failed_at_ms")?,
    })
}

fn map_attempt_row(row: SqliteRow) -> Result<ChannelOutboxAttemptRecord> {
    let delivery_id_raw: String = row.get("delivery_id");
    let status_raw: String = row.get("status");
    Ok(ChannelOutboxAttemptRecord {
        delivery_id: Uuid::parse_str(&delivery_id_raw)
            .with_context(|| format!("invalid attempt delivery_id '{delivery_id_raw}'"))?,
        channel_id: row.get("channel_id"),
        worker_id: row.get("worker_id"),
        status: ChannelOutboxAttemptStatus::from_str(&status_raw)
            .map_err(|err| anyhow!("invalid channel outbox attempt status: {err}"))?,
    })
}
