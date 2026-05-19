use std::{path::PathBuf, str::FromStr};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use sqlx::{sqlite::SqliteRow, Row, Sqlite, SqlitePool, Transaction};

use crate::{
    contracts::ChannelAttachmentDescriptor,
    kernel::db::{ms_to_datetime, now_ms},
};

pub const MAX_CHANNEL_ATTACHMENTS_PER_EVENT: usize = 10;
pub const MAX_CHANNEL_ATTACHMENT_BYTES: usize = 25 * 1024 * 1024;
pub const MAX_CHANNEL_EVENT_ATTACHMENT_BYTES: usize = 50 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelAttachmentRecordStatus {
    Declared,
    Staged,
    Rejected,
}

impl ChannelAttachmentRecordStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Declared => "declared",
            Self::Staged => "staged",
            Self::Rejected => "rejected",
        }
    }
}

impl FromStr for ChannelAttachmentRecordStatus {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "declared" => Ok(Self::Declared),
            "staged" => Ok(Self::Staged),
            "rejected" => Ok(Self::Rejected),
            other => Err(format!("invalid channel attachment status '{other}'")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelAttachmentBatchStatus {
    Waiting,
    Finalized,
}

impl ChannelAttachmentBatchStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Waiting => "waiting",
            Self::Finalized => "finalized",
        }
    }
}

impl FromStr for ChannelAttachmentBatchStatus {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "waiting" => Ok(Self::Waiting),
            "finalized" => Ok(Self::Finalized),
            other => Err(format!("invalid channel attachment batch status '{other}'")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChannelAttachmentRecord {
    pub channel_id: String,
    pub event_id: String,
    pub attachment_id: String,
    pub kind: String,
    pub filename: Option<String>,
    pub mime_type: Option<String>,
    pub caption: Option<String>,
    pub provider_file_ref: Option<String>,
    pub status: ChannelAttachmentRecordStatus,
    pub size_bytes: Option<i64>,
    pub sha256: Option<String>,
    pub storage_path: Option<PathBuf>,
    pub rejection_code: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ChannelAttachmentBatchRecord {
    pub channel_id: String,
    pub event_id: String,
    pub status: ChannelAttachmentBatchStatus,
    pub finalized_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy)]
pub struct DeclareAttachmentRejection<'a> {
    pub attachment_id: &'a str,
    pub reason_code: &'a str,
}

#[derive(Debug, Clone, Copy)]
pub struct StageAttachmentUpdate<'a> {
    pub channel_id: &'a str,
    pub event_id: &'a str,
    pub attachment_id: &'a str,
    pub filename: Option<&'a str>,
    pub mime_type: Option<&'a str>,
    pub caption: Option<&'a str>,
    pub size_bytes: i64,
    pub sha256: &'a str,
    pub storage_path: &'a str,
}

#[derive(Debug, Clone, Copy)]
pub struct RejectAttachmentUpdate<'a> {
    pub channel_id: &'a str,
    pub event_id: &'a str,
    pub attachment_id: &'a str,
    pub rejection_code: &'a str,
}

#[derive(Debug, Clone)]
pub struct ChannelAttachmentStore {
    pool: SqlitePool,
}

impl ChannelAttachmentStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn declare_batch_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        channel_id: &str,
        event_id: &str,
        attachments: &[ChannelAttachmentDescriptor],
        initial_rejections: &[DeclareAttachmentRejection<'_>],
    ) -> Result<()> {
        if attachments.is_empty() {
            return Ok(());
        }

        let now = now_ms();
        for attachment in attachments {
            let rejection_code = initial_rejections
                .iter()
                .find(|rejection| rejection.attachment_id == attachment.attachment_id)
                .map(|rejection| rejection.reason_code);
            let status = if rejection_code.is_some() {
                "rejected"
            } else {
                "declared"
            };
            sqlx::query(
                "INSERT INTO channel_attachments \
                 (channel_id, event_id, attachment_id, kind, filename, mime_type, caption, provider_file_ref, status, size_bytes, sha256, storage_path, rejection_code, created_at_ms, updated_at_ms) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, NULL, NULL, ?11, ?12, ?12)",
            )
            .bind(channel_id)
            .bind(event_id)
            .bind(&attachment.attachment_id)
            .bind(&attachment.kind)
            .bind(attachment.filename.as_deref())
            .bind(attachment.mime_type.as_deref())
            .bind(attachment.caption.as_deref())
            .bind(&attachment.provider_file_ref)
            .bind(status)
            .bind(attachment.size_bytes)
            .bind(rejection_code)
            .bind(now)
            .execute(&mut **tx)
            .await
            .context("failed to declare channel attachment")?;
        }

        sqlx::query(
            "INSERT INTO channel_attachment_batches \
             (channel_id, event_id, status, finalized_at_ms, created_at_ms, updated_at_ms) \
             VALUES (?1, ?2, 'waiting', NULL, ?3, ?3)",
        )
        .bind(channel_id)
        .bind(event_id)
        .bind(now)
        .execute(&mut **tx)
        .await
        .context("failed to create channel attachment batch")?;

        Ok(())
    }

    pub(crate) async fn get_batch_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        channel_id: &str,
        event_id: &str,
    ) -> Result<Option<ChannelAttachmentBatchRecord>> {
        let row = sqlx::query(
            "SELECT channel_id, event_id, status, finalized_at_ms, created_at_ms, updated_at_ms \
             FROM channel_attachment_batches \
             WHERE channel_id = ?1 AND event_id = ?2",
        )
        .bind(channel_id)
        .bind(event_id)
        .fetch_optional(&mut **tx)
        .await
        .context("failed to query channel attachment batch in transaction")?;

        row.map(map_batch_row).transpose()
    }

    pub(crate) async fn get_attachment_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        channel_id: &str,
        event_id: &str,
        attachment_id: &str,
    ) -> Result<Option<ChannelAttachmentRecord>> {
        let row = sqlx::query(
            "SELECT channel_id, event_id, attachment_id, kind, filename, mime_type, caption, provider_file_ref, status, size_bytes, sha256, storage_path, rejection_code, created_at_ms, updated_at_ms \
             FROM channel_attachments \
             WHERE channel_id = ?1 AND event_id = ?2 AND attachment_id = ?3",
        )
        .bind(channel_id)
        .bind(event_id)
        .bind(attachment_id)
        .fetch_optional(&mut **tx)
        .await
        .context("failed to query channel attachment in transaction")?;

        row.map(map_attachment_row).transpose()
    }

    pub async fn list_event_attachments(
        &self,
        channel_id: &str,
        event_id: &str,
    ) -> Result<Vec<ChannelAttachmentRecord>> {
        let rows = sqlx::query(
            "SELECT channel_id, event_id, attachment_id, kind, filename, mime_type, caption, provider_file_ref, status, size_bytes, sha256, storage_path, rejection_code, created_at_ms, updated_at_ms \
             FROM channel_attachments \
             WHERE channel_id = ?1 AND event_id = ?2 \
             ORDER BY created_at_ms ASC, attachment_id ASC",
        )
        .bind(channel_id)
        .bind(event_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list channel attachments")?;

        rows.into_iter().map(map_attachment_row).collect()
    }

    pub(crate) async fn staged_size_for_event_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        channel_id: &str,
        event_id: &str,
    ) -> Result<i64> {
        let size = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT SUM(size_bytes) FROM channel_attachments \
             WHERE channel_id = ?1 AND event_id = ?2 AND status = 'staged'",
        )
        .bind(channel_id)
        .bind(event_id)
        .fetch_one(&mut **tx)
        .await
        .context("failed to query staged channel attachment bytes in transaction")?;

        Ok(size.unwrap_or(0))
    }

    pub(crate) async fn stage_attachment_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        update: StageAttachmentUpdate<'_>,
    ) -> Result<bool> {
        let now = now_ms();
        let changed = sqlx::query(
            "UPDATE channel_attachments \
             SET status = 'staged', filename = ?4, mime_type = ?5, caption = ?6, size_bytes = ?7, sha256 = ?8, storage_path = ?9, rejection_code = NULL, updated_at_ms = ?10 \
             WHERE channel_id = ?1 AND event_id = ?2 AND attachment_id = ?3 AND status = 'declared'",
        )
        .bind(update.channel_id)
        .bind(update.event_id)
        .bind(update.attachment_id)
        .bind(update.filename)
        .bind(update.mime_type)
        .bind(update.caption)
        .bind(update.size_bytes)
        .bind(update.sha256)
        .bind(update.storage_path)
        .bind(now)
        .execute(&mut **tx)
        .await
        .context("failed to mark channel attachment staged in transaction")?;

        Ok(changed.rows_affected() > 0)
    }

    pub(crate) async fn reject_attachment_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        update: RejectAttachmentUpdate<'_>,
    ) -> Result<bool> {
        let now = now_ms();
        let changed = sqlx::query(
            "UPDATE channel_attachments \
             SET status = 'rejected', rejection_code = ?4, updated_at_ms = ?5 \
             WHERE channel_id = ?1 AND event_id = ?2 AND attachment_id = ?3 AND status = 'declared'",
        )
        .bind(update.channel_id)
        .bind(update.event_id)
        .bind(update.attachment_id)
        .bind(update.rejection_code)
        .bind(now)
        .execute(&mut **tx)
        .await
        .context("failed to reject channel attachment in transaction")?;

        Ok(changed.rows_affected() > 0)
    }

    pub(crate) async fn reject_unstaged_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        channel_id: &str,
        event_id: &str,
        rejection_code: &str,
    ) -> Result<u64> {
        let now = now_ms();
        let changed = sqlx::query(
            "UPDATE channel_attachments \
             SET status = 'rejected', rejection_code = ?3, updated_at_ms = ?4 \
             WHERE channel_id = ?1 AND event_id = ?2 AND status = 'declared'",
        )
        .bind(channel_id)
        .bind(event_id)
        .bind(rejection_code)
        .bind(now)
        .execute(&mut **tx)
        .await
        .context("failed to reject unstaged channel attachments")?;

        Ok(changed.rows_affected())
    }

    pub(crate) async fn list_event_attachments_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        channel_id: &str,
        event_id: &str,
    ) -> Result<Vec<ChannelAttachmentRecord>> {
        let rows = sqlx::query(
            "SELECT channel_id, event_id, attachment_id, kind, filename, mime_type, caption, provider_file_ref, status, size_bytes, sha256, storage_path, rejection_code, created_at_ms, updated_at_ms \
             FROM channel_attachments \
             WHERE channel_id = ?1 AND event_id = ?2 \
             ORDER BY created_at_ms ASC, attachment_id ASC",
        )
        .bind(channel_id)
        .bind(event_id)
        .fetch_all(&mut **tx)
        .await
        .context("failed to list channel attachments in transaction")?;

        rows.into_iter().map(map_attachment_row).collect()
    }

    pub(crate) async fn finalize_batch_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        channel_id: &str,
        event_id: &str,
    ) -> Result<bool> {
        let now = now_ms();
        let changed = sqlx::query(
            "UPDATE channel_attachment_batches \
             SET status = 'finalized', finalized_at_ms = ?3, updated_at_ms = ?3 \
             WHERE channel_id = ?1 AND event_id = ?2 AND status = 'waiting'",
        )
        .bind(channel_id)
        .bind(event_id)
        .bind(now)
        .execute(&mut **tx)
        .await
        .context("failed to finalize channel attachment batch")?;

        Ok(changed.rows_affected() > 0)
    }

    pub async fn stale_waiting_batches(&self, older_than_ms: i64) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query(
            "SELECT channel_id, event_id \
             FROM channel_attachment_batches \
             WHERE status = 'waiting' AND created_at_ms < ?1 \
             ORDER BY created_at_ms ASC",
        )
        .bind(older_than_ms)
        .fetch_all(&self.pool)
        .await
        .context("failed to query stale waiting channel attachment batches")?;

        Ok(rows
            .into_iter()
            .map(|row| (row.get("channel_id"), row.get("event_id")))
            .collect())
    }
}

fn map_attachment_row(row: SqliteRow) -> Result<ChannelAttachmentRecord> {
    let status_raw: String = row.get("status");
    let status = ChannelAttachmentRecordStatus::from_str(&status_raw)
        .map_err(|err| anyhow!("invalid channel attachment status: {err}"))?;
    let created_at_ms: i64 = row.get("created_at_ms");
    let updated_at_ms: i64 = row.get("updated_at_ms");
    let storage_path = row
        .get::<Option<String>, _>("storage_path")
        .map(PathBuf::from);

    Ok(ChannelAttachmentRecord {
        channel_id: row.get("channel_id"),
        event_id: row.get("event_id"),
        attachment_id: row.get("attachment_id"),
        kind: row.get("kind"),
        filename: row.get("filename"),
        mime_type: row.get("mime_type"),
        caption: row.get("caption"),
        provider_file_ref: row.get("provider_file_ref"),
        status,
        size_bytes: row.get("size_bytes"),
        sha256: row.get("sha256"),
        storage_path,
        rejection_code: row.get("rejection_code"),
        created_at: ms_to_datetime(created_at_ms)
            .ok_or_else(|| anyhow!("invalid channel attachment created_at_ms '{created_at_ms}'"))?,
        updated_at: ms_to_datetime(updated_at_ms)
            .ok_or_else(|| anyhow!("invalid channel attachment updated_at_ms '{updated_at_ms}'"))?,
    })
}

fn map_batch_row(row: SqliteRow) -> Result<ChannelAttachmentBatchRecord> {
    let status_raw: String = row.get("status");
    let status = ChannelAttachmentBatchStatus::from_str(&status_raw)
        .map_err(|err| anyhow!("invalid channel attachment batch status: {err}"))?;
    let finalized_at_ms: Option<i64> = row.get("finalized_at_ms");
    let created_at_ms: i64 = row.get("created_at_ms");
    let updated_at_ms: i64 = row.get("updated_at_ms");

    Ok(ChannelAttachmentBatchRecord {
        channel_id: row.get("channel_id"),
        event_id: row.get("event_id"),
        status,
        finalized_at: finalized_at_ms
            .map(|value| {
                ms_to_datetime(value).ok_or_else(|| {
                    anyhow!("invalid channel attachment batch finalized_at_ms '{value}'")
                })
            })
            .transpose()?,
        created_at: ms_to_datetime(created_at_ms).ok_or_else(|| {
            anyhow!("invalid channel attachment batch created_at_ms '{created_at_ms}'")
        })?,
        updated_at: ms_to_datetime(updated_at_ms).ok_or_else(|| {
            anyhow!("invalid channel attachment batch updated_at_ms '{updated_at_ms}'")
        })?,
    })
}
