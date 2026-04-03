use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use sqlx::{sqlite::SqliteRow, Row, SqlitePool};
use uuid::Uuid;

use crate::kernel::db::{ms_to_datetime, now_ms};

#[derive(Debug, Clone)]
pub struct SessionCompactionRecord {
    pub compaction_id: Uuid,
    pub session_id: Uuid,
    pub start_sequence_no: u64,
    pub through_sequence_no: u64,
    pub summary_text: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct SessionCompactionStore {
    pool: SqlitePool,
}

impl SessionCompactionStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn latest_through_sequence(&self, session_id: Uuid) -> Result<u64> {
        let raw = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(through_sequence_no) \
             FROM session_compactions \
             WHERE session_id = ?1",
        )
        .bind(session_id.to_string())
        .fetch_one(&self.pool)
        .await
        .context("failed to query latest session compaction")?
        .unwrap_or(0);
        u64::try_from(raw).context("invalid latest through_sequence_no")
    }

    pub async fn latest(&self, session_id: Uuid) -> Result<Option<SessionCompactionRecord>> {
        let row = sqlx::query(
            "SELECT compaction_id, session_id, start_sequence_no, through_sequence_no, summary_text, created_at_ms \
             FROM session_compactions \
             WHERE session_id = ?1 \
             ORDER BY through_sequence_no DESC, compaction_id DESC \
             LIMIT 1",
        )
        .bind(session_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .context("failed to query latest session compaction record")?;

        row.map(map_compaction_row).transpose()
    }

    pub async fn insert(
        &self,
        session_id: Uuid,
        start_sequence_no: u64,
        through_sequence_no: u64,
        summary_text: String,
    ) -> Result<SessionCompactionRecord> {
        let compaction_id = Uuid::new_v4();
        let created_at_ms = now_ms();
        sqlx::query(
            "INSERT INTO session_compactions \
             (compaction_id, session_id, start_sequence_no, through_sequence_no, summary_text, created_at_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .bind(compaction_id.to_string())
        .bind(session_id.to_string())
        .bind(i64::try_from(start_sequence_no).context("start_sequence_no is too large")?)
        .bind(i64::try_from(through_sequence_no).context("through_sequence_no is too large")?)
        .bind(summary_text)
        .bind(created_at_ms)
        .execute(&self.pool)
        .await
        .context("failed to insert session compaction")?;

        self.get(compaction_id)
            .await?
            .ok_or_else(|| anyhow!("session compaction disappeared immediately after insert"))
    }

    pub async fn get(&self, compaction_id: Uuid) -> Result<Option<SessionCompactionRecord>> {
        let row = sqlx::query(
            "SELECT compaction_id, session_id, start_sequence_no, through_sequence_no, summary_text, created_at_ms \
             FROM session_compactions \
             WHERE compaction_id = ?1",
        )
        .bind(compaction_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .context("failed to query session compaction")?;

        row.map(map_compaction_row).transpose()
    }

    pub async fn list_recent(
        &self,
        session_id: Uuid,
        limit: usize,
    ) -> Result<Vec<SessionCompactionRecord>> {
        let rows = sqlx::query(
            "SELECT compaction_id, session_id, start_sequence_no, through_sequence_no, summary_text, created_at_ms \
             FROM session_compactions \
             WHERE session_id = ?1 \
             ORDER BY through_sequence_no DESC, compaction_id DESC \
             LIMIT ?2",
        )
        .bind(session_id.to_string())
        .bind(i64::try_from(limit).context("session compaction limit is too large")?)
        .fetch_all(&self.pool)
        .await
        .context("failed to list session compactions")?;

        rows.into_iter().map(map_compaction_row).collect()
    }
}

fn map_compaction_row(row: SqliteRow) -> Result<SessionCompactionRecord> {
    let compaction_id_raw: String = row.get("compaction_id");
    let session_id_raw: String = row.get("session_id");
    let start_sequence_no_raw: i64 = row.get("start_sequence_no");
    let through_sequence_no_raw: i64 = row.get("through_sequence_no");
    let created_at_ms: i64 = row.get("created_at_ms");

    Ok(SessionCompactionRecord {
        compaction_id: Uuid::parse_str(&compaction_id_raw)
            .with_context(|| format!("invalid compaction_id '{}'", compaction_id_raw))?,
        session_id: Uuid::parse_str(&session_id_raw)
            .with_context(|| format!("invalid session_id '{}'", session_id_raw))?,
        start_sequence_no: u64::try_from(start_sequence_no_raw)
            .with_context(|| format!("invalid start_sequence_no '{}'", start_sequence_no_raw))?,
        through_sequence_no: u64::try_from(through_sequence_no_raw).with_context(|| {
            format!("invalid through_sequence_no '{}'", through_sequence_no_raw)
        })?,
        summary_text: row.get("summary_text"),
        created_at: ms_to_datetime(created_at_ms)
            .ok_or_else(|| anyhow!("invalid created_at_ms '{}'", created_at_ms))?,
    })
}
