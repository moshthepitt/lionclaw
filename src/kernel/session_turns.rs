use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use sqlx::{sqlite::SqliteRow, Row, SqlitePool};
use uuid::Uuid;

use crate::{
    contracts::{SessionTurnKind, SessionTurnStatus, SessionTurnView},
    kernel::db::{ms_to_datetime, now_ms},
};

#[derive(Debug, Clone)]
pub struct SessionTurnRecord {
    pub turn_id: Uuid,
    pub session_id: Uuid,
    pub sequence_no: u64,
    pub kind: SessionTurnKind,
    pub status: SessionTurnStatus,
    pub display_user_text: String,
    pub prompt_user_text: String,
    pub assistant_text: String,
    pub error_code: Option<String>,
    pub error_text: Option<String>,
    pub runtime_id: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct SessionTurnStore {
    pool: SqlitePool,
}

#[derive(Debug, Clone)]
pub struct NewSessionTurn {
    pub turn_id: Uuid,
    pub session_id: Uuid,
    pub kind: SessionTurnKind,
    pub display_user_text: String,
    pub prompt_user_text: String,
    pub runtime_id: String,
}

#[derive(Debug, Clone)]
pub struct SessionTurnCompletion {
    pub status: SessionTurnStatus,
    pub assistant_text: String,
    pub error_code: Option<String>,
    pub error_text: Option<String>,
}

impl SessionTurnStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn begin_turn(&self, turn: NewSessionTurn) -> Result<SessionTurnRecord> {
        let started_at_ms = now_ms();
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to start session turn transaction")?;

        let next_sequence_no: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(sequence_no), 0) + 1 \
             FROM session_turns \
             WHERE session_id = ?1",
        )
        .bind(turn.session_id.to_string())
        .fetch_one(&mut *tx)
        .await
        .context("failed to allocate next session turn sequence number")?;

        sqlx::query(
            "INSERT INTO session_turns \
             (turn_id, session_id, sequence_no, kind, status, display_user_text, prompt_user_text, assistant_text, error_code, error_text, runtime_id, started_at_ms, finished_at_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, '', NULL, NULL, ?8, ?9, NULL)",
        )
        .bind(turn.turn_id.to_string())
        .bind(turn.session_id.to_string())
        .bind(next_sequence_no)
        .bind(turn.kind.as_str())
        .bind(SessionTurnStatus::Running.as_str())
        .bind(&turn.display_user_text)
        .bind(&turn.prompt_user_text)
        .bind(&turn.runtime_id)
        .bind(started_at_ms)
        .execute(&mut *tx)
        .await
        .context("failed to insert session turn")?;

        tx.commit()
            .await
            .context("failed to commit session turn transaction")?;

        self.get(turn.turn_id)
            .await?
            .ok_or_else(|| anyhow!("session turn disappeared immediately after insert"))
    }

    pub async fn complete_turn(
        &self,
        turn_id: Uuid,
        completion: SessionTurnCompletion,
    ) -> Result<Option<SessionTurnRecord>> {
        let finished_at_ms = now_ms();
        let updated = sqlx::query(
            "UPDATE session_turns \
             SET status = ?2, assistant_text = ?3, error_code = ?4, error_text = ?5, finished_at_ms = ?6 \
             WHERE turn_id = ?1",
        )
        .bind(turn_id.to_string())
        .bind(completion.status.as_str())
        .bind(completion.assistant_text)
        .bind(completion.error_code)
        .bind(completion.error_text)
        .bind(finished_at_ms)
        .execute(&self.pool)
        .await
        .context("failed to finalize session turn")?;

        if updated.rows_affected() == 0 {
            return Ok(None);
        }

        self.get(turn_id).await
    }

    pub async fn get(&self, turn_id: Uuid) -> Result<Option<SessionTurnRecord>> {
        let row = sqlx::query(
            "SELECT turn_id, session_id, sequence_no, kind, status, display_user_text, prompt_user_text, assistant_text, error_code, error_text, runtime_id, started_at_ms, finished_at_ms \
             FROM session_turns \
             WHERE turn_id = ?1",
        )
        .bind(turn_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .context("failed to query session turn")?;

        row.map(map_session_turn_row).transpose()
    }

    pub async fn latest(&self, session_id: Uuid) -> Result<Option<SessionTurnRecord>> {
        let row = sqlx::query(
            "SELECT turn_id, session_id, sequence_no, kind, status, display_user_text, prompt_user_text, assistant_text, error_code, error_text, runtime_id, started_at_ms, finished_at_ms \
             FROM session_turns \
             WHERE session_id = ?1 \
             ORDER BY sequence_no DESC \
             LIMIT 1",
        )
        .bind(session_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .context("failed to query latest session turn")?;

        row.map(map_session_turn_row).transpose()
    }

    pub async fn list_recent(
        &self,
        session_id: Uuid,
        limit: usize,
    ) -> Result<Vec<SessionTurnRecord>> {
        let limit = i64::try_from(limit).context("session history limit is too large")?;
        let rows = sqlx::query(
            "SELECT turn_id, session_id, sequence_no, kind, status, display_user_text, prompt_user_text, assistant_text, error_code, error_text, runtime_id, started_at_ms, finished_at_ms \
             FROM session_turns \
             WHERE session_id = ?1 \
             ORDER BY sequence_no DESC \
             LIMIT ?2",
        )
        .bind(session_id.to_string())
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .context("failed to query session turn history")?;

        let mut turns = rows
            .into_iter()
            .map(map_session_turn_row)
            .collect::<Result<Vec<_>>>()?;
        turns.reverse();
        Ok(turns)
    }
}

impl From<SessionTurnRecord> for SessionTurnView {
    fn from(value: SessionTurnRecord) -> Self {
        Self {
            turn_id: value.turn_id,
            kind: value.kind,
            status: value.status,
            display_user_text: value.display_user_text,
            prompt_user_text: value.prompt_user_text,
            assistant_text: value.assistant_text,
            error_code: value.error_code,
            error_text: value.error_text,
            runtime_id: value.runtime_id,
            started_at: value.started_at,
            finished_at: value.finished_at,
        }
    }
}

fn map_session_turn_row(row: SqliteRow) -> Result<SessionTurnRecord> {
    let turn_id_raw: String = row.get("turn_id");
    let session_id_raw: String = row.get("session_id");
    let sequence_no_raw: i64 = row.get("sequence_no");
    let kind_raw: String = row.get("kind");
    let status_raw: String = row.get("status");
    let started_at_ms: i64 = row.get("started_at_ms");
    let finished_at_ms: Option<i64> = row.get("finished_at_ms");

    let turn_id = Uuid::parse_str(&turn_id_raw)
        .with_context(|| format!("invalid turn_id '{}'", turn_id_raw))?;
    let session_id = Uuid::parse_str(&session_id_raw)
        .with_context(|| format!("invalid session_id '{}'", session_id_raw))?;
    let sequence_no = u64::try_from(sequence_no_raw)
        .with_context(|| format!("invalid sequence_no '{}'", sequence_no_raw))?;
    let kind = SessionTurnKind::from_str(&kind_raw)
        .map_err(|err| anyhow!("invalid session turn kind: {}", err))?;
    let status = SessionTurnStatus::from_str(&status_raw)
        .map_err(|err| anyhow!("invalid session turn status: {}", err))?;
    let started_at = ms_to_datetime(started_at_ms)
        .ok_or_else(|| anyhow!("invalid started_at_ms '{}'", started_at_ms))?;
    let finished_at = finished_at_ms
        .map(|value| {
            ms_to_datetime(value).ok_or_else(|| anyhow!("invalid finished_at_ms '{}'", value))
        })
        .transpose()?;

    Ok(SessionTurnRecord {
        turn_id,
        session_id,
        sequence_no,
        kind,
        status,
        display_user_text: row.get("display_user_text"),
        prompt_user_text: row.get("prompt_user_text"),
        assistant_text: row.get("assistant_text"),
        error_code: row.get("error_code"),
        error_text: row.get("error_text"),
        runtime_id: row.get("runtime_id"),
        started_at,
        finished_at,
    })
}
