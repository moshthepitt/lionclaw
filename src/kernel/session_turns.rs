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

#[derive(Debug, Clone)]
pub struct InterruptedSessionTurn {
    pub turn_id: Uuid,
    pub session_id: Uuid,
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

    pub async fn checkpoint_assistant_text(
        &self,
        turn_id: Uuid,
        assistant_text: &str,
    ) -> Result<Option<SessionTurnRecord>> {
        let updated = sqlx::query(
            "UPDATE session_turns \
             SET assistant_text = ?2 \
             WHERE turn_id = ?1 AND status = ?3",
        )
        .bind(turn_id.to_string())
        .bind(assistant_text)
        .bind(SessionTurnStatus::Running.as_str())
        .execute(&self.pool)
        .await
        .context("failed to checkpoint session turn assistant text")?;

        if updated.rows_affected() == 0 {
            return Ok(None);
        }

        self.get(turn_id).await
    }

    pub async fn interrupt_running_turns(
        &self,
        reason: &str,
    ) -> Result<Vec<InterruptedSessionTurn>> {
        let finished_at_ms = now_ms();
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to start interrupted session turn transaction")?;

        let rows = sqlx::query(
            "SELECT turn_id, session_id \
             FROM session_turns \
             WHERE status = ?1 \
             ORDER BY started_at_ms ASC, turn_id ASC",
        )
        .bind(SessionTurnStatus::Running.as_str())
        .fetch_all(&mut *tx)
        .await
        .context("failed to query running session turns")?;

        sqlx::query(
            "UPDATE session_turns \
             SET status = ?1, error_code = ?2, error_text = ?3, finished_at_ms = ?4 \
             WHERE status = ?5",
        )
        .bind(SessionTurnStatus::Interrupted.as_str())
        .bind("runtime.interrupted")
        .bind(reason)
        .bind(finished_at_ms)
        .bind(SessionTurnStatus::Running.as_str())
        .execute(&mut *tx)
        .await
        .context("failed to interrupt running session turns")?;

        tx.commit()
            .await
            .context("failed to commit interrupted session turn transaction")?;

        rows.into_iter()
            .map(|row| {
                let turn_id_raw: String = row.get("turn_id");
                let session_id_raw: String = row.get("session_id");
                let turn_id = Uuid::parse_str(&turn_id_raw)
                    .with_context(|| format!("invalid interrupted turn_id '{turn_id_raw}'"))?;
                let session_id = Uuid::parse_str(&session_id_raw).with_context(|| {
                    format!("invalid interrupted session_id '{session_id_raw}'")
                })?;
                Ok(InterruptedSessionTurn {
                    turn_id,
                    session_id,
                })
            })
            .collect()
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

    pub async fn list_sequence_range(
        &self,
        session_id: Uuid,
        start_after_sequence_no: u64,
        through_sequence_no: u64,
    ) -> Result<Vec<SessionTurnRecord>> {
        let rows = sqlx::query(
            "SELECT turn_id, session_id, sequence_no, kind, status, display_user_text, prompt_user_text, assistant_text, error_code, error_text, runtime_id, started_at_ms, finished_at_ms \
             FROM session_turns \
             WHERE session_id = ?1 AND sequence_no > ?2 AND sequence_no <= ?3 \
             ORDER BY sequence_no ASC",
        )
        .bind(session_id.to_string())
        .bind(i64::try_from(start_after_sequence_no).context("start_after_sequence_no is too large")?)
        .bind(i64::try_from(through_sequence_no).context("through_sequence_no is too large")?)
        .fetch_all(&self.pool)
        .await
        .context("failed to query session turn sequence range")?;

        rows.into_iter().map(map_session_turn_row).collect()
    }

    pub async fn list_recent_failures(&self, limit: usize) -> Result<Vec<SessionTurnRecord>> {
        let rows = sqlx::query(
            "SELECT turn_id, session_id, sequence_no, kind, status, display_user_text, prompt_user_text, assistant_text, error_code, error_text, runtime_id, started_at_ms, finished_at_ms \
             FROM session_turns \
             WHERE status IN ('failed', 'timed_out', 'cancelled', 'interrupted') \
             ORDER BY started_at_ms DESC, turn_id DESC \
             LIMIT ?1",
        )
        .bind(i64::try_from(limit).context("session failure limit is too large")?)
        .fetch_all(&self.pool)
        .await
        .context("failed to query recent session failures")?;

        rows.into_iter().map(map_session_turn_row).collect()
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
        .with_context(|| format!("invalid turn_id '{turn_id_raw}'"))?;
    let session_id = Uuid::parse_str(&session_id_raw)
        .with_context(|| format!("invalid session_id '{session_id_raw}'"))?;
    let sequence_no = u64::try_from(sequence_no_raw)
        .with_context(|| format!("invalid sequence_no '{sequence_no_raw}'"))?;
    let kind = SessionTurnKind::from_str(&kind_raw)
        .map_err(|err| anyhow!("invalid session turn kind: {err}"))?;
    let status = SessionTurnStatus::from_str(&status_raw)
        .map_err(|err| anyhow!("invalid session turn status: {err}"))?;
    let started_at = ms_to_datetime(started_at_ms)
        .ok_or_else(|| anyhow!("invalid started_at_ms '{started_at_ms}'"))?;
    let finished_at = finished_at_ms
        .map(|value| {
            ms_to_datetime(value).ok_or_else(|| anyhow!("invalid finished_at_ms '{value}'"))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{SessionHistoryPolicy, TrustTier};
    use crate::home::runtime_project_partition_key;
    use crate::kernel::{db::Db, sessions::SessionStore};

    async fn new_store_with_session() -> (SessionTurnStore, Uuid) {
        let db = Db::connect_memory().await.expect("connect memory db");
        let pool = db.pool();
        let sessions = SessionStore::new(pool.clone());
        let turns = SessionTurnStore::new(pool);
        let session = sessions
            .open(
                "local-cli".to_string(),
                "failure-peer".to_string(),
                runtime_project_partition_key(None),
                TrustTier::Main,
                SessionHistoryPolicy::Interactive,
            )
            .await
            .expect("open session");
        (turns, session.session_id)
    }

    #[tokio::test]
    async fn list_recent_failures_returns_newest_failure_states_only() {
        let (store, session_id) = new_store_with_session().await;
        let failed = store
            .begin_turn(NewSessionTurn {
                turn_id: Uuid::new_v4(),
                session_id,
                kind: SessionTurnKind::Normal,
                display_user_text: "failed".to_string(),
                prompt_user_text: "failed".to_string(),
                runtime_id: "mock".to_string(),
            })
            .await
            .expect("begin failed turn");
        store
            .complete_turn(
                failed.turn_id,
                SessionTurnCompletion {
                    status: SessionTurnStatus::Failed,
                    assistant_text: String::new(),
                    error_code: Some("runtime.failed".to_string()),
                    error_text: Some("failed".to_string()),
                },
            )
            .await
            .expect("complete failed turn");

        let completed = store
            .begin_turn(NewSessionTurn {
                turn_id: Uuid::new_v4(),
                session_id,
                kind: SessionTurnKind::Normal,
                display_user_text: "completed".to_string(),
                prompt_user_text: "completed".to_string(),
                runtime_id: "mock".to_string(),
            })
            .await
            .expect("begin completed turn");
        store
            .complete_turn(
                completed.turn_id,
                SessionTurnCompletion {
                    status: SessionTurnStatus::Completed,
                    assistant_text: "ok".to_string(),
                    error_code: None,
                    error_text: None,
                },
            )
            .await
            .expect("complete completed turn");

        let interrupted = store
            .begin_turn(NewSessionTurn {
                turn_id: Uuid::new_v4(),
                session_id,
                kind: SessionTurnKind::Normal,
                display_user_text: "interrupted".to_string(),
                prompt_user_text: "interrupted".to_string(),
                runtime_id: "mock".to_string(),
            })
            .await
            .expect("begin interrupted turn");
        store
            .complete_turn(
                interrupted.turn_id,
                SessionTurnCompletion {
                    status: SessionTurnStatus::Interrupted,
                    assistant_text: String::new(),
                    error_code: Some("runtime.interrupted".to_string()),
                    error_text: Some("interrupted".to_string()),
                },
            )
            .await
            .expect("complete interrupted turn");

        sqlx::query(
            "UPDATE session_turns \
             SET started_at_ms = CASE turn_id \
                WHEN ?1 THEN 1000 \
                WHEN ?2 THEN 2000 \
                WHEN ?3 THEN 3000 \
                ELSE started_at_ms \
             END",
        )
        .bind(failed.turn_id.to_string())
        .bind(completed.turn_id.to_string())
        .bind(interrupted.turn_id.to_string())
        .execute(&store.pool)
        .await
        .expect("set turn timestamps");

        let failures = store
            .list_recent_failures(5)
            .await
            .expect("list recent failures");
        assert_eq!(failures.len(), 2);
        assert_eq!(failures[0].turn_id, interrupted.turn_id);
        assert_eq!(failures[1].turn_id, failed.turn_id);
        assert!(failures.iter().all(|turn| {
            matches!(
                turn.status,
                SessionTurnStatus::Failed
                    | SessionTurnStatus::TimedOut
                    | SessionTurnStatus::Cancelled
                    | SessionTurnStatus::Interrupted
            )
        }));
    }
}
