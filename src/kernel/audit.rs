use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{Row, SqlitePool};
use uuid::Uuid;

use crate::kernel::db::{datetime_to_ms, ms_to_datetime, now_ms};

#[derive(Debug, Clone)]
pub struct AuditEvent {
    pub event_id: Uuid,
    pub event_type: String,
    pub session_id: Option<Uuid>,
    pub actor: Option<String>,
    pub details: Value,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct AuditLog {
    pool: SqlitePool,
}

impl AuditLog {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn append(
        &self,
        event_type: impl Into<String>,
        session_id: Option<Uuid>,
        actor: Option<String>,
        details: Value,
    ) -> Result<()> {
        let event_id = Uuid::new_v4();
        let event_type = event_type.into();
        let details_json =
            serde_json::to_string(&details).context("failed to encode audit details")?;
        let timestamp_ms = now_ms();

        sqlx::query(
            "INSERT INTO audit_events \
             (event_id, event_type, session_id, actor, details_json, timestamp_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .bind(event_id.to_string())
        .bind(&event_type)
        .bind(session_id.map(|value| value.to_string()))
        .bind(actor)
        .bind(details_json)
        .bind(timestamp_ms)
        .execute(&self.pool)
        .await
        .context("failed to append audit event")?;

        Ok(())
    }

    pub async fn query(
        &self,
        session_id: Option<Uuid>,
        event_type: Option<String>,
        since: Option<DateTime<Utc>>,
        limit: Option<usize>,
    ) -> Result<Vec<AuditEvent>> {
        let limit = limit.unwrap_or(100).min(1000) as i64;
        let session_id_raw = session_id.map(|value| value.to_string());
        let since_ms = since.map(datetime_to_ms);

        let rows = sqlx::query(
            "SELECT event_id, event_type, session_id, actor, details_json, timestamp_ms \
             FROM audit_events \
             WHERE (?1 IS NULL OR session_id = ?1) \
               AND (?2 IS NULL OR lower(event_type) = lower(?2)) \
               AND (?3 IS NULL OR timestamp_ms >= ?3) \
             ORDER BY timestamp_ms DESC \
             LIMIT ?4",
        )
        .bind(session_id_raw)
        .bind(event_type)
        .bind(since_ms)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .context("failed to query audit events")?;

        rows.into_iter()
            .map(|row| {
                let event_id_raw: String = row.get("event_id");
                let session_id_raw: Option<String> = row.get("session_id");
                let details_raw: String = row.get("details_json");
                let timestamp_ms: i64 = row.get("timestamp_ms");

                let event_id = Uuid::parse_str(&event_id_raw)
                    .with_context(|| format!("invalid event id '{}'", event_id_raw))?;
                let session_id = session_id_raw
                    .map(|value| {
                        Uuid::parse_str(&value)
                            .with_context(|| format!("invalid session id '{}'", value))
                    })
                    .transpose()?;
                let details = serde_json::from_str(&details_raw)
                    .with_context(|| format!("invalid audit details '{}'", details_raw))?;
                let timestamp = ms_to_datetime(timestamp_ms)
                    .ok_or_else(|| anyhow!("invalid timestamp_ms '{}'", timestamp_ms))?;

                Ok(AuditEvent {
                    event_id,
                    event_type: row.get("event_type"),
                    session_id,
                    actor: row.get("actor"),
                    details,
                    timestamp,
                })
            })
            .collect()
    }
}
