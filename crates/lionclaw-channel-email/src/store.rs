use std::{
    fs,
    path::{Component, Path, PathBuf},
};

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Row, SqlitePool,
};

use crate::{
    mailbox::{CandidateHeader, MalformedCandidateHeader},
    mime::{EmailAddress, HeaderFacts},
};

const LAST_DIGEST_AT_KEY: &str = "last_digest_at";
const LAST_DIGEST_ATTEMPT_AT_KEY: &str = "last_digest_sent_at";
const LAST_HELD_DIGEST_ROWID_KEY: &str = "last_held_digest_rowid";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MailStatus {
    Held,
    Suppressed,
    Admitted,
}

impl MailStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Held => "held",
            Self::Suppressed => "suppressed",
            Self::Admitted => "admitted",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeldItem {
    #[serde(skip)]
    pub(crate) digest_rowid: i64,
    pub held_id: String,
    pub event_id: String,
    pub sender_ref: String,
    pub conversation_ref: String,
    pub thread_ref: String,
    pub message_ref: String,
    pub sender_address: String,
    pub sender_name: Option<String>,
    pub subject: String,
    pub snippet: String,
    pub received_at: Option<String>,
    pub attachment_count: i64,
    pub classification_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ThreadContext {
    pub sender_address: String,
    pub subject: String,
    pub provider_message_id: Option<String>,
    pub references: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredReceipt {
    pub delivery_id: String,
    pub message_id: String,
    pub recipient: String,
    pub receipt: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingGrantRevocation {
    pub grant_id: String,
    pub channel_id: String,
    pub held_id: String,
}

#[derive(Debug, Clone)]
pub struct EmailStore {
    pool: SqlitePool,
}

impl EmailStore {
    pub async fn open(state_dir: &Path) -> Result<Self> {
        ensure_state_dir(state_dir)?;
        let db_path = state_dir.join("channel-email.sqlite3");
        ensure_state_db_path(&db_path)?;
        let options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .with_context(|| format!("failed to open {}", db_path.display()))?;
        let store = Self { pool };
        store.migrate().await?;
        ensure_state_db_path(&db_path)?;
        set_private_file_permissions(&db_path)?;
        Ok(store)
    }

    async fn migrate(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS mail_items (
                event_id TEXT PRIMARY KEY NOT NULL,
                held_id TEXT UNIQUE,
                status TEXT NOT NULL,
                uid_validity INTEGER NOT NULL,
                uid INTEGER NOT NULL,
                sender_ref TEXT NOT NULL,
                sender_address TEXT NOT NULL,
                sender_name TEXT,
                conversation_ref TEXT NOT NULL,
                thread_ref TEXT NOT NULL,
                message_ref TEXT NOT NULL,
                subject TEXT NOT NULL,
                snippet TEXT NOT NULL,
                received_at TEXT,
                attachment_count INTEGER NOT NULL DEFAULT 0,
                rfc822_size INTEGER,
                classification_reason TEXT,
                provider_message_id TEXT,
                in_reply_to TEXT,
                references_json TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;
        self.ensure_mail_items_rfc822_size_column().await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS outbox_receipts (
                delivery_id TEXT PRIMARY KEY NOT NULL,
                message_id TEXT NOT NULL,
                recipient TEXT NOT NULL,
                receipt_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS worker_state (
                key TEXT PRIMARY KEY NOT NULL,
                value TEXT NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS pending_grant_revocations (
                grant_id TEXT PRIMARY KEY NOT NULL,
                channel_id TEXT NOT NULL,
                held_id TEXT NOT NULL,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn ensure_mail_items_rfc822_size_column(&self) -> Result<()> {
        let rows = sqlx::query("PRAGMA table_info(mail_items)")
            .fetch_all(&self.pool)
            .await?;
        let has_column = rows.iter().any(|row| {
            row.try_get::<String, _>("name")
                .map(|name| name == "rfc822_size")
                .unwrap_or(false)
        });
        if !has_column {
            sqlx::query("ALTER TABLE mail_items ADD COLUMN rfc822_size INTEGER")
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    pub async fn mail_status(&self, event_id: &str) -> Result<Option<MailStatus>> {
        let Some(row) = sqlx::query("SELECT status FROM mail_items WHERE event_id = ?")
            .bind(event_id)
            .fetch_optional(&self.pool)
            .await?
        else {
            return Ok(None);
        };
        let status: String = row.try_get("status")?;
        Ok(Some(parse_mail_status(&status, event_id)?))
    }

    pub async fn mail_status_by_message_ref_for_sender(
        &self,
        message_ref: &str,
        sender_ref: &str,
        except_event_id: &str,
    ) -> Result<Option<MailStatus>> {
        let Some(row) = sqlx::query(
            r#"
            SELECT event_id, status
            FROM mail_items
            WHERE message_ref = ? AND sender_ref = ? AND event_id != ?
            ORDER BY created_at ASC, event_id ASC
            LIMIT 1
            "#,
        )
        .bind(message_ref)
        .bind(sender_ref)
        .bind(except_event_id)
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(None);
        };
        let event_id: String = row.try_get("event_id")?;
        let status: String = row.try_get("status")?;
        Ok(Some(parse_mail_status(&status, &event_id)?))
    }

    pub async fn held_candidates(&self, limit: i64) -> Result<Vec<CandidateHeader>> {
        let rows = sqlx::query(
            r#"
            SELECT event_id, uid_validity, uid, sender_ref, sender_address, sender_name,
                   conversation_ref, thread_ref, message_ref, subject, received_at,
                   attachment_count, rfc822_size, provider_message_id, in_reply_to,
                   references_json
            FROM mail_items
            WHERE status = 'held'
            ORDER BY updated_at ASC, event_id ASC
            LIMIT ?
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(held_candidate_from_row).collect()
    }

    pub async fn record_held(
        &self,
        candidate: &CandidateHeader,
        held_id: &str,
        snippet: &str,
        reason: &str,
    ) -> Result<()> {
        self.upsert_mail(
            candidate,
            Some(held_id),
            MailStatus::Held,
            snippet,
            Some(reason),
        )
        .await
    }

    pub async fn record_suppressed(&self, candidate: &CandidateHeader, reason: &str) -> Result<()> {
        self.upsert_mail(candidate, None, MailStatus::Suppressed, "", Some(reason))
            .await
    }

    pub async fn record_malformed_suppressed(
        &self,
        candidate: &MalformedCandidateHeader,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            INSERT INTO mail_items (
                event_id, held_id, status, uid_validity, uid, sender_ref, sender_address,
                sender_name, conversation_ref, thread_ref, message_ref, subject, snippet,
                received_at, attachment_count, rfc822_size, classification_reason,
                provider_message_id, in_reply_to, references_json, created_at, updated_at
            )
            VALUES (?, NULL, 'suppressed', ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, NULL, ?, ?, ?, NULL, NULL, '[]', ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                held_id=NULL,
                status=excluded.status,
                sender_ref=excluded.sender_ref,
                sender_address=excluded.sender_address,
                sender_name=NULL,
                conversation_ref=excluded.conversation_ref,
                thread_ref=excluded.thread_ref,
                message_ref=excluded.message_ref,
                subject=excluded.subject,
                snippet=excluded.snippet,
                received_at=NULL,
                attachment_count=excluded.attachment_count,
                rfc822_size=COALESCE(excluded.rfc822_size, mail_items.rfc822_size),
                classification_reason=excluded.classification_reason,
                provider_message_id=NULL,
                in_reply_to=NULL,
                references_json='[]',
                updated_at=excluded.updated_at
            "#,
        )
        .bind(&candidate.event_id)
        .bind(i64::from(candidate.uid_validity))
        .bind(i64::from(candidate.uid))
        .bind(&candidate.sender_ref)
        .bind("(unknown)")
        .bind(&candidate.conversation_ref)
        .bind(&candidate.thread_ref)
        .bind(&candidate.message_ref)
        .bind(&candidate.subject)
        .bind(&candidate.snippet)
        .bind(candidate.attachment_count as i64)
        .bind(candidate.rfc822_size.map(i64::from))
        .bind(&candidate.reason)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn record_admitted(&self, candidate: &CandidateHeader, snippet: &str) -> Result<()> {
        self.upsert_mail(candidate, None, MailStatus::Admitted, snippet, None)
            .await
    }

    async fn upsert_mail(
        &self,
        candidate: &CandidateHeader,
        held_id: Option<&str>,
        status: MailStatus,
        snippet: &str,
        reason: Option<&str>,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let received_at = candidate.facts.received_at.map(|value| value.to_rfc3339());
        let references_json = serde_json::to_string(&candidate.facts.references)?;
        sqlx::query(
            r#"
            INSERT INTO mail_items (
                event_id, held_id, status, uid_validity, uid, sender_ref, sender_address,
                sender_name, conversation_ref, thread_ref, message_ref, subject, snippet,
                received_at, attachment_count, rfc822_size, classification_reason,
                provider_message_id, in_reply_to, references_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                held_id=excluded.held_id,
                status=excluded.status,
                snippet=excluded.snippet,
                rfc822_size=COALESCE(excluded.rfc822_size, mail_items.rfc822_size),
                classification_reason=excluded.classification_reason,
                updated_at=excluded.updated_at
            "#,
        )
        .bind(&candidate.event_id)
        .bind(held_id)
        .bind(status.as_str())
        .bind(i64::from(candidate.uid_validity))
        .bind(i64::from(candidate.uid))
        .bind(&candidate.sender_ref)
        .bind(&candidate.facts.sender.address)
        .bind(&candidate.facts.sender.display_name)
        .bind(&candidate.conversation_ref)
        .bind(&candidate.thread_ref)
        .bind(&candidate.message_ref)
        .bind(&candidate.facts.subject)
        .bind(snippet)
        .bind(received_at)
        .bind(candidate.attachment_count as i64)
        .bind(candidate.rfc822_size.map(i64::from))
        .bind(reason)
        .bind(&candidate.facts.message_id)
        .bind(&candidate.facts.in_reply_to)
        .bind(references_json)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn held_since_last_digest(&self, limit: i64) -> Result<Vec<HeldItem>> {
        let after_rowid = self
            .state_value(LAST_HELD_DIGEST_ROWID_KEY)
            .await?
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        let rows = sqlx::query(
            r#"
            SELECT rowid AS digest_rowid,
                   held_id, event_id, sender_ref, conversation_ref, thread_ref, message_ref,
                   sender_address, sender_name, subject, snippet, received_at,
                   attachment_count, classification_reason
            FROM mail_items
            WHERE status = 'held' AND rowid > ?
            ORDER BY rowid ASC
            LIMIT ?
            "#,
        )
        .bind(after_rowid)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| -> Result<HeldItem> {
                let digest_rowid: i64 = row.try_get("digest_rowid")?;
                let held_id: Option<String> = row.try_get("held_id")?;
                let held_id = held_id
                    .ok_or_else(|| anyhow!("held mail row {digest_rowid} is missing held_id"))?;
                Ok(HeldItem {
                    digest_rowid,
                    held_id,
                    event_id: row.try_get("event_id")?,
                    sender_ref: row.try_get("sender_ref")?,
                    conversation_ref: row.try_get("conversation_ref")?,
                    thread_ref: row.try_get("thread_ref")?,
                    message_ref: row.try_get("message_ref")?,
                    sender_address: row.try_get("sender_address")?,
                    sender_name: row.try_get("sender_name")?,
                    subject: row.try_get("subject")?,
                    snippet: row.try_get("snippet")?,
                    received_at: row.try_get("received_at")?,
                    attachment_count: row.try_get("attachment_count")?,
                    classification_reason: row.try_get("classification_reason")?,
                })
            })
            .collect()
    }

    pub async fn suppressed_count_since_last_digest(&self) -> Result<i64> {
        let since = self
            .state_value(LAST_DIGEST_AT_KEY)
            .await?
            .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string());
        let row = sqlx::query(
            "SELECT COUNT(*) AS count FROM mail_items WHERE status = 'suppressed' AND created_at > ?",
        )
        .bind(since)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.try_get("count")?)
    }

    pub async fn mark_digest_sent(&self, held: &[HeldItem]) -> Result<()> {
        if let Some(item) = held.last() {
            self.set_state_value(LAST_HELD_DIGEST_ROWID_KEY, &item.digest_rowid.to_string())
                .await?;
        }
        self.set_state_value(LAST_DIGEST_AT_KEY, &Utc::now().to_rfc3339())
            .await?;
        Ok(())
    }

    pub async fn due_for_digest(&self, interval: std::time::Duration) -> Result<bool> {
        let Some(last) = self.state_value(LAST_DIGEST_ATTEMPT_AT_KEY).await? else {
            return Ok(true);
        };
        let Ok(last) = DateTime::parse_from_rfc3339(&last) else {
            return Ok(true);
        };
        Ok(Utc::now().signed_duration_since(last.with_timezone(&Utc))
            >= chrono::Duration::from_std(interval).unwrap_or_else(|_| chrono::Duration::hours(1)))
    }

    pub async fn mark_digest_attempt_now(&self) -> Result<()> {
        self.set_state_value(LAST_DIGEST_ATTEMPT_AT_KEY, &Utc::now().to_rfc3339())
            .await
    }

    pub async fn thread_context(
        &self,
        reply_to_ref: Option<&str>,
        thread_ref: Option<&str>,
    ) -> Result<Option<ThreadContext>> {
        let row = if let Some(reply_to_ref) = reply_to_ref {
            sqlx::query(
                r#"
                SELECT sender_address, subject, provider_message_id, references_json
                FROM mail_items
                WHERE message_ref = ? AND status = 'admitted'
                ORDER BY created_at DESC
                LIMIT 1
                "#,
            )
            .bind(reply_to_ref)
            .fetch_optional(&self.pool)
            .await?
        } else if let Some(thread_ref) = thread_ref {
            sqlx::query(
                r#"
                SELECT sender_address, subject, provider_message_id, references_json
                FROM mail_items
                WHERE thread_ref = ? AND status = 'admitted'
                ORDER BY created_at DESC
                LIMIT 1
                "#,
            )
            .bind(thread_ref)
            .fetch_optional(&self.pool)
            .await?
        } else {
            None
        };

        let Some(row) = row else {
            return Ok(None);
        };
        let references_json: String = row.try_get("references_json")?;
        let references = serde_json::from_str(&references_json)
            .context("failed to decode admitted email references")?;
        Ok(Some(ThreadContext {
            sender_address: row.try_get("sender_address")?,
            subject: row.try_get("subject")?,
            provider_message_id: row.try_get("provider_message_id")?,
            references,
        }))
    }

    pub async fn receipt(&self, delivery_id: &str) -> Result<Option<StoredReceipt>> {
        let Some(row) = sqlx::query(
            "SELECT delivery_id, message_id, recipient, receipt_json FROM outbox_receipts WHERE delivery_id = ?",
        )
        .bind(delivery_id)
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(None);
        };
        let receipt_json: String = row.try_get("receipt_json")?;
        Ok(Some(StoredReceipt {
            delivery_id: row.try_get("delivery_id")?,
            message_id: row.try_get("message_id")?,
            recipient: row.try_get("recipient")?,
            receipt: serde_json::from_str(&receipt_json)?,
        }))
    }

    pub async fn record_receipt(
        &self,
        delivery_id: &str,
        message_id: &str,
        recipient: &str,
        receipt: &Value,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO outbox_receipts (delivery_id, message_id, recipient, receipt_json, created_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(delivery_id) DO NOTHING
            "#,
        )
        .bind(delivery_id)
        .bind(message_id)
        .bind(recipient)
        .bind(serde_json::to_string(receipt)?)
        .bind(Utc::now().to_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn record_pending_grant_revocation(
        &self,
        grant_id: &str,
        channel_id: &str,
        held_id: &str,
        last_error: &str,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            INSERT INTO pending_grant_revocations (
                grant_id, channel_id, held_id, last_error, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(grant_id) DO UPDATE SET
                last_error=excluded.last_error,
                updated_at=excluded.updated_at
            "#,
        )
        .bind(grant_id)
        .bind(channel_id)
        .bind(held_id)
        .bind(last_error)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn pending_grant_revocations(&self) -> Result<Vec<PendingGrantRevocation>> {
        let rows = sqlx::query(
            r#"
            SELECT grant_id, channel_id, held_id
            FROM pending_grant_revocations
            ORDER BY created_at ASC
            LIMIT 25
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                Ok(PendingGrantRevocation {
                    grant_id: row.try_get("grant_id")?,
                    channel_id: row.try_get("channel_id")?,
                    held_id: row.try_get("held_id")?,
                })
            })
            .collect::<std::result::Result<Vec<_>, sqlx::Error>>()?)
    }

    pub async fn clear_pending_grant_revocation(&self, grant_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM pending_grant_revocations WHERE grant_id = ?")
            .bind(grant_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn state_value(&self, key: &str) -> Result<Option<String>> {
        let row = sqlx::query("SELECT value FROM worker_state WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|row| row.get("value")))
    }

    async fn set_state_value(&self, key: &str, value: &str) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO worker_state (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            "#,
        )
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

pub fn held_id_for(event_id: &str) -> String {
    format!("hld_{}", crate::protocol::short_hash(event_id))
}

fn parse_mail_status(status: &str, event_id: &str) -> Result<MailStatus> {
    match status {
        "held" => Ok(MailStatus::Held),
        "suppressed" => Ok(MailStatus::Suppressed),
        "admitted" => Ok(MailStatus::Admitted),
        other => bail!("unknown email mail status '{other}' for event {event_id}"),
    }
}

fn ensure_state_dir(state_dir: &Path) -> Result<()> {
    if state_dir.as_os_str().is_empty() {
        bail!("email state dir is required");
    }

    let mut current = PathBuf::new();
    for component in state_dir.components() {
        match component {
            Component::CurDir => continue,
            Component::ParentDir => bail!("email state dir must not contain '..'"),
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                current.push(component.as_os_str());
            }
        }

        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    bail!(
                        "email state dir {} must not be a symlink",
                        current.display()
                    );
                }
                if !metadata.is_dir() {
                    bail!("email state path {} is not a directory", current.display());
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current)
                    .with_context(|| format!("failed to create {}", current.display()))?;
                set_private_dir_permissions(&current)?;
            }
            Err(err) => {
                return Err(err).with_context(|| format!("failed to stat {}", current.display()));
            }
        }
    }
    set_private_dir_permissions(state_dir)?;
    Ok(())
}

fn ensure_state_db_path(db_path: &Path) -> Result<()> {
    match fs::symlink_metadata(db_path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                bail!(
                    "email state database {} must not be a symlink",
                    db_path.display()
                );
            }
            if !metadata.is_file() {
                bail!(
                    "email state database {} is not a regular file",
                    db_path.display()
                );
            }
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("failed to stat {}", db_path.display())),
    }
}

#[cfg(unix)]
fn set_private_dir_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("failed to chmod {}", path.display()))
}

#[cfg(not(unix))]
fn set_private_dir_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("failed to chmod {}", path.display()))
}

#[cfg(not(unix))]
fn set_private_file_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

fn held_candidate_from_row(row: sqlx::sqlite::SqliteRow) -> Result<CandidateHeader> {
    let event_id: String = row.try_get("event_id")?;
    let uid_validity = u32::try_from(row.try_get::<i64, _>("uid_validity")?)
        .with_context(|| format!("held mail {event_id} has invalid uid_validity"))?;
    let uid = u32::try_from(row.try_get::<i64, _>("uid")?)
        .with_context(|| format!("held mail {event_id} has invalid uid"))?;
    let received_at: Option<String> = row.try_get("received_at")?;
    let received_at = received_at
        .map(|value| -> Result<DateTime<Utc>> {
            Ok(DateTime::parse_from_rfc3339(&value)
                .with_context(|| format!("held mail {event_id} has invalid received_at"))?
                .with_timezone(&Utc))
        })
        .transpose()?;
    let references_json: String = row.try_get("references_json")?;
    let references = serde_json::from_str(&references_json)
        .with_context(|| format!("held mail {event_id} has invalid references"))?;
    let rfc822_size = row
        .try_get::<Option<i64>, _>("rfc822_size")?
        .map(|value| {
            u32::try_from(value).with_context(|| format!("held mail {event_id} has invalid size"))
        })
        .transpose()?;
    Ok(CandidateHeader {
        uid_validity,
        uid,
        event_id,
        sender_ref: row.try_get("sender_ref")?,
        conversation_ref: row.try_get("conversation_ref")?,
        thread_ref: row.try_get("thread_ref")?,
        message_ref: row.try_get("message_ref")?,
        attachment_count: usize::try_from(row.try_get::<i64, _>("attachment_count")?)
            .context("held mail attachment_count is invalid")?,
        rfc822_size,
        facts: HeaderFacts {
            sender: EmailAddress {
                address: row.try_get("sender_address")?,
                display_name: row.try_get("sender_name")?,
            },
            to: Vec::new(),
            subject: row.try_get("subject")?,
            message_id: row.try_get("provider_message_id")?,
            in_reply_to: row.try_get("in_reply_to")?,
            references,
            received_at,
            raw_headers: Vec::new(),
        },
    })
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;
    use crate::{
        mailbox::{CandidateHeader, MalformedCandidateHeader},
        mime::parse_headers_for_test,
        protocol::{conversation_ref, message_ref, sender_ref, thread_ref},
    };

    #[tokio::test]
    async fn held_digest_cursor_keeps_batches_after_the_limit() {
        let temp_dir = tempdir().expect("temp dir");
        let store = EmailStore::open(temp_dir.path()).await.expect("store");
        for index in 1..=25 {
            let candidate = candidate(index);
            record_held_for_digest(&store, &candidate).await;
        }

        let first = store.held_since_last_digest(20).await.expect("first batch");
        assert_eq!(first.len(), 20);
        store.mark_digest_sent(&first).await.expect("mark first");

        let second = store
            .held_since_last_digest(20)
            .await
            .expect("second batch");
        assert_eq!(second.len(), 5);
        assert_eq!(second[0].event_id, "email:imap:assistant:7:21");
    }

    #[tokio::test]
    async fn held_digest_fails_closed_on_malformed_held_rows() {
        let temp_dir = tempdir().expect("temp dir");
        let store = EmailStore::open(temp_dir.path()).await.expect("store");
        let malformed = candidate(1);
        let valid = candidate(2);
        record_held_for_digest(&store, &malformed).await;
        sqlx::query("UPDATE mail_items SET held_id = NULL WHERE event_id = ?")
            .bind(&malformed.event_id)
            .execute(&store.pool)
            .await
            .expect("malform held row");
        record_held_for_digest(&store, &valid).await;

        let err = store
            .held_since_last_digest(20)
            .await
            .expect_err("malformed held rows fail digest decoding");
        assert!(
            err.to_string().contains("missing held_id"),
            "unexpected error: {err:#}"
        );
        assert_eq!(
            store.state_value(LAST_HELD_DIGEST_ROWID_KEY).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn held_candidates_preserve_rfc822_size_fact() {
        let temp_dir = tempdir().expect("temp dir");
        let store = EmailStore::open(temp_dir.path()).await.expect("store");
        let mut candidate = candidate(1);
        candidate.rfc822_size = Some(1025);
        store
            .record_held(
                &candidate,
                &held_id_for(&candidate.event_id),
                "not downloaded",
                "approval_required",
            )
            .await
            .expect("record held");

        let held = store.held_candidates(10).await.expect("held candidates");

        assert_eq!(held.len(), 1);
        assert_eq!(held[0].rfc822_size, Some(1025));
    }

    #[tokio::test]
    async fn store_open_adds_rfc822_size_to_existing_mail_items_table() {
        let temp_dir = tempdir().expect("temp dir");
        let db_path = temp_dir.path().join("channel-email.sqlite3");
        let options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .expect("legacy store");
        sqlx::query(
            r#"
            CREATE TABLE mail_items (
                event_id TEXT PRIMARY KEY NOT NULL,
                held_id TEXT UNIQUE,
                status TEXT NOT NULL,
                uid_validity INTEGER NOT NULL,
                uid INTEGER NOT NULL,
                sender_ref TEXT NOT NULL,
                sender_address TEXT NOT NULL,
                sender_name TEXT,
                conversation_ref TEXT NOT NULL,
                thread_ref TEXT NOT NULL,
                message_ref TEXT NOT NULL,
                subject TEXT NOT NULL,
                snippet TEXT NOT NULL,
                received_at TEXT,
                attachment_count INTEGER NOT NULL DEFAULT 0,
                classification_reason TEXT,
                provider_message_id TEXT,
                in_reply_to TEXT,
                references_json TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            "#,
        )
        .execute(&pool)
        .await
        .expect("create legacy mail_items");
        pool.close().await;

        let store = EmailStore::open(temp_dir.path()).await.expect("open store");
        let columns = sqlx::query("PRAGMA table_info(mail_items)")
            .fetch_all(&store.pool)
            .await
            .expect("columns");

        assert!(columns.iter().any(|row| {
            row.try_get::<String, _>("name")
                .map(|name| name == "rfc822_size")
                .unwrap_or(false)
        }));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn store_open_rejects_symlinked_state_dir_ancestor() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempdir().expect("temp dir");
        let outside = temp_dir.path().join("outside-state");
        fs::create_dir(&outside).expect("outside state");
        let link = temp_dir.path().join("state-link");
        symlink(&outside, &link).expect("state symlink");

        let err = EmailStore::open(&link.join("email"))
            .await
            .expect_err("symlinked state ancestor should fail");

        assert!(err.to_string().contains("must not be a symlink"));
        assert!(!outside.join("email/channel-email.sqlite3").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn store_open_rejects_symlinked_database_file() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempdir().expect("temp dir");
        let state_dir = temp_dir.path().join("state");
        fs::create_dir(&state_dir).expect("state dir");
        let outside_db = temp_dir.path().join("outside.sqlite3");
        symlink(&outside_db, state_dir.join("channel-email.sqlite3")).expect("db symlink");

        let err = EmailStore::open(&state_dir)
            .await
            .expect_err("symlinked state db should fail");

        assert!(err.to_string().contains("must not be a symlink"));
        assert!(!outside_db.exists());
    }

    #[tokio::test]
    async fn thread_context_fails_closed_on_malformed_references() {
        let temp_dir = tempdir().expect("temp dir");
        let store = EmailStore::open(temp_dir.path()).await.expect("store");
        let candidate = candidate(1);
        store
            .record_admitted(&candidate, "downloaded")
            .await
            .expect("record admitted");
        sqlx::query("UPDATE mail_items SET references_json = ? WHERE event_id = ?")
            .bind("{not json")
            .bind(&candidate.event_id)
            .execute(&store.pool)
            .await
            .expect("malform references");

        let err = store
            .thread_context(None, Some(&candidate.thread_ref))
            .await
            .expect_err("malformed references fail thread lookup");
        assert!(
            err.to_string()
                .contains("failed to decode admitted email references"),
            "unexpected error: {err:#}"
        );
    }

    #[tokio::test]
    async fn mail_status_fails_closed_on_unknown_status() {
        let temp_dir = tempdir().expect("temp dir");
        let store = EmailStore::open(temp_dir.path()).await.expect("store");
        let candidate = candidate(1);
        store
            .record_admitted(&candidate, "downloaded")
            .await
            .expect("record admitted");
        sqlx::query("UPDATE mail_items SET status = ? WHERE event_id = ?")
            .bind("mystery")
            .bind(&candidate.event_id)
            .execute(&store.pool)
            .await
            .expect("malform status");

        let err = store
            .mail_status(&candidate.event_id)
            .await
            .expect_err("unknown status must fail closed");
        assert!(
            err.to_string().contains("unknown email mail status"),
            "unexpected error: {err:#}"
        );
    }

    #[tokio::test]
    async fn message_ref_status_lookup_finds_prior_provider_message_from_same_sender() {
        let temp_dir = tempdir().expect("temp dir");
        let store = EmailStore::open(temp_dir.path()).await.expect("store");
        let original = candidate(1);
        let mut same_sender_copy = candidate(2);
        same_sender_copy
            .message_ref
            .clone_from(&original.message_ref);
        let mut different_sender_copy = candidate_from_sender(3, "bob@example.com");
        different_sender_copy
            .message_ref
            .clone_from(&original.message_ref);
        store
            .record_admitted(&original, "downloaded")
            .await
            .expect("record original");

        assert_eq!(
            store
                .mail_status_by_message_ref_for_sender(
                    &same_sender_copy.message_ref,
                    &same_sender_copy.sender_ref,
                    &same_sender_copy.event_id,
                )
                .await
                .expect("lookup same-sender copy"),
            Some(MailStatus::Admitted)
        );
        assert_eq!(
            store
                .mail_status_by_message_ref_for_sender(
                    &different_sender_copy.message_ref,
                    &different_sender_copy.sender_ref,
                    &different_sender_copy.event_id,
                )
                .await
                .expect("lookup different-sender copy"),
            None
        );
        assert_eq!(
            store
                .mail_status_by_message_ref_for_sender(
                    &original.message_ref,
                    &original.sender_ref,
                    &original.event_id,
                )
                .await
                .expect("lookup self excluded"),
            None
        );
    }

    #[tokio::test]
    async fn malformed_candidates_are_persisted_as_suppressed_mail() {
        let temp_dir = tempdir().expect("temp dir");
        let store = EmailStore::open(temp_dir.path()).await.expect("store");
        let malformed = malformed_candidate(9);

        store
            .record_malformed_suppressed(&malformed)
            .await
            .expect("record malformed");

        assert_eq!(
            store
                .mail_status(&malformed.event_id)
                .await
                .expect("status"),
            Some(MailStatus::Suppressed)
        );
        let row = sqlx::query(
            "SELECT sender_ref, sender_address, classification_reason FROM mail_items WHERE event_id = ?",
        )
        .bind(&malformed.event_id)
        .fetch_one(&store.pool)
        .await
        .expect("mail item");
        assert_eq!(
            row.try_get::<String, _>("sender_ref").expect("sender_ref"),
            malformed.sender_ref
        );
        assert_eq!(
            row.try_get::<String, _>("sender_address")
                .expect("sender_address"),
            "(unknown)"
        );
        assert_eq!(
            row.try_get::<String, _>("classification_reason")
                .expect("classification_reason"),
            malformed.reason
        );
    }

    async fn record_held_for_digest(store: &EmailStore, candidate: &CandidateHeader) {
        store
            .record_held(
                candidate,
                &held_id_for(&candidate.event_id),
                "not downloaded",
                "approval_required",
            )
            .await
            .expect("record held");
    }

    fn candidate(index: u32) -> CandidateHeader {
        candidate_from_sender(index, "alice@example.com")
    }

    fn candidate_from_sender(index: u32, sender: &str) -> CandidateHeader {
        let message_id = format!("m{index}@example.com");
        let headers = format!(
            "From: {sender}\r\nSubject: Held {index}\r\nMessage-ID: <{message_id}>\r\n\r\n"
        );
        let facts = parse_headers_for_test(&headers);
        CandidateHeader {
            uid_validity: 7,
            uid: index,
            event_id: format!("email:imap:assistant:7:{index}"),
            sender_ref: sender_ref(&facts.sender.address),
            conversation_ref: conversation_ref("assistant"),
            thread_ref: thread_ref(&message_id),
            message_ref: message_ref(&message_id),
            attachment_count: 0,
            rfc822_size: Some(headers.len() as u32),
            facts,
        }
    }

    fn malformed_candidate(index: u32) -> MalformedCandidateHeader {
        let event_id = format!("email:imap:assistant:7:{index}");
        MalformedCandidateHeader {
            uid_validity: 7,
            uid: index,
            event_id: event_id.clone(),
            sender_ref: format!("email:malformed:{index}"),
            conversation_ref: conversation_ref("assistant"),
            thread_ref: thread_ref(&format!("malformed:{event_id}")),
            message_ref: message_ref(&format!("imap:7:{index}:malformed")),
            subject: "(malformed headers)".to_string(),
            snippet: "Header facts could not be parsed; body was not downloaded.".to_string(),
            attachment_count: 0,
            rfc822_size: Some(128),
            reason: "malformed_headers".to_string(),
        }
    }
}
