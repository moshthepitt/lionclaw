use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use lionclaw_runtime_api::{RawTurnPayload, RuntimeEvent, TurnEvent};
use sqlx::{sqlite::SqliteRow, Row, Sqlite, SqlitePool, Transaction};
use uuid::Uuid;

use crate::{
    contracts::{SessionTurnKind, SessionTurnStatus, SessionTurnView},
    kernel::db::{datetime_to_ms, ms_to_datetime, now_ms},
    kernel::jobs::SchedulerJobRunStatus,
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
    pub attachment_source_turn_id: Option<Uuid>,
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
    pub attachment_source_turn_id: Option<Uuid>,
    pub runtime_id: String,
}

#[derive(Debug, Clone)]
pub struct ImportedSessionTurn {
    pub turn_id: Uuid,
    pub session_id: Uuid,
    pub kind: SessionTurnKind,
    pub status: SessionTurnStatus,
    pub display_user_text: String,
    pub prompt_user_text: String,
    pub assistant_text: String,
    pub error_code: Option<String>,
    pub error_text: Option<String>,
    pub attachment_source_turn_id: Option<Uuid>,
    pub runtime_id: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
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

        self.begin_turn_with_started_at(turn, started_at_ms).await
    }

    pub async fn begin_turn_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        turn: NewSessionTurn,
    ) -> Result<SessionTurnRecord> {
        self.begin_turn_with_status_in_tx(tx, turn, SessionTurnStatus::Running)
            .await
    }

    pub async fn begin_turn_with_status_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        turn: NewSessionTurn,
        status: SessionTurnStatus,
    ) -> Result<SessionTurnRecord> {
        let started_at_ms = now_ms();

        sqlx::query(
            "INSERT INTO session_turns \
             (turn_id, session_id, sequence_no, kind, status, display_user_text, prompt_user_text, assistant_text, error_code, error_text, attachment_source_turn_id, runtime_id, started_at_ms, finished_at_ms) \
             VALUES (?1, ?2, (SELECT COALESCE(MAX(sequence_no), 0) + 1 FROM session_turns WHERE session_id = ?2), ?3, ?4, ?5, ?6, '', NULL, NULL, ?7, ?8, ?9, NULL)",
        )
        .bind(turn.turn_id.to_string())
        .bind(turn.session_id.to_string())
        .bind(turn.kind.as_str())
        .bind(status.as_str())
        .bind(&turn.display_user_text)
        .bind(&turn.prompt_user_text)
        .bind(turn.attachment_source_turn_id.map(|turn_id| turn_id.to_string()))
        .bind(&turn.runtime_id)
        .bind(started_at_ms)
        .execute(&mut **tx)
        .await
        .with_context(|| {
            format!(
                "failed to insert session turn {} for session {}",
                turn.turn_id, turn.session_id
            )
        })?;

        self.get_in_tx(tx, turn.turn_id).await?.ok_or_else(|| {
            anyhow!("session turn disappeared immediately after transactional insert")
        })
    }

    async fn begin_turn_with_started_at(
        &self,
        turn: NewSessionTurn,
        started_at_ms: i64,
    ) -> Result<SessionTurnRecord> {
        sqlx::query(
            "INSERT INTO session_turns \
             (turn_id, session_id, sequence_no, kind, status, display_user_text, prompt_user_text, assistant_text, error_code, error_text, attachment_source_turn_id, runtime_id, started_at_ms, finished_at_ms) \
             VALUES (?1, ?2, (SELECT COALESCE(MAX(sequence_no), 0) + 1 FROM session_turns WHERE session_id = ?2), ?3, ?4, ?5, ?6, '', NULL, NULL, ?7, ?8, ?9, NULL)",
        )
        .bind(turn.turn_id.to_string())
        .bind(turn.session_id.to_string())
        .bind(turn.kind.as_str())
        .bind(SessionTurnStatus::Running.as_str())
        .bind(&turn.display_user_text)
        .bind(&turn.prompt_user_text)
        .bind(turn.attachment_source_turn_id.map(|turn_id| turn_id.to_string()))
        .bind(&turn.runtime_id)
        .bind(started_at_ms)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "failed to insert session turn {} for session {}",
                turn.turn_id, turn.session_id
            )
        })?;

        self.get(turn.turn_id)
            .await?
            .ok_or_else(|| anyhow!("session turn disappeared immediately after insert"))
    }

    pub async fn complete_turn(
        &self,
        turn_id: Uuid,
        completion: SessionTurnCompletion,
    ) -> Result<Option<SessionTurnRecord>> {
        self.complete_turn_with_required_status(turn_id, completion, None)
            .await
    }

    pub async fn complete_running_turn(
        &self,
        turn_id: Uuid,
        completion: SessionTurnCompletion,
    ) -> Result<Option<SessionTurnRecord>> {
        self.complete_turn_with_required_status(
            turn_id,
            completion,
            Some(SessionTurnStatus::Running),
        )
        .await
    }

    pub async fn complete_running_turn_for_current_scheduler_run(
        &self,
        turn_id: Uuid,
        run_id: Uuid,
        session_id: Uuid,
        completion: SessionTurnCompletion,
    ) -> Result<Option<SessionTurnRecord>> {
        let finished_at_ms = now_ms();
        let updated = sqlx::query(
            "UPDATE session_turns \
             SET status = ?2, assistant_text = ?3, error_code = ?4, error_text = ?5, finished_at_ms = ?6 \
             WHERE turn_id = ?1 \
               AND status = ?7 \
               AND session_id = ?8 \
               AND EXISTS ( \
                 SELECT 1 \
                 FROM scheduler_job_runs \
                 JOIN scheduler_jobs ON scheduler_jobs.job_id = scheduler_job_runs.job_id \
                 JOIN sessions ON sessions.session_id = scheduler_job_runs.session_id \
                 WHERE scheduler_job_runs.run_id = ?9 \
                   AND scheduler_job_runs.status = ?10 \
                   AND scheduler_job_runs.session_id = ?8 \
                   AND scheduler_job_runs.turn_id = ?1 \
                   AND scheduler_jobs.running_run_id = scheduler_job_runs.run_id \
                   AND sessions.channel_id = 'scheduler' \
                   AND sessions.peer_id = 'job:' || scheduler_job_runs.job_id \
               )",
        )
        .bind(turn_id.to_string())
        .bind(completion.status.as_str())
        .bind(completion.assistant_text)
        .bind(completion.error_code)
        .bind(completion.error_text)
        .bind(finished_at_ms)
        .bind(SessionTurnStatus::Running.as_str())
        .bind(session_id.to_string())
        .bind(run_id.to_string())
        .bind(SchedulerJobRunStatus::Running.as_str())
        .execute(&self.pool)
        .await
        .context("failed to finalize scheduler-owned session turn")?;

        if updated.rows_affected() == 0 {
            return Ok(None);
        }

        self.get(turn_id).await
    }

    async fn complete_turn_with_required_status(
        &self,
        turn_id: Uuid,
        completion: SessionTurnCompletion,
        required_status: Option<SessionTurnStatus>,
    ) -> Result<Option<SessionTurnRecord>> {
        let finished_at_ms = now_ms();
        let updated = sqlx::query(
            "UPDATE session_turns \
             SET status = ?2, assistant_text = ?3, error_code = ?4, error_text = ?5, finished_at_ms = ?6 \
             WHERE turn_id = ?1 AND (?7 IS NULL OR status = ?7)",
        )
        .bind(turn_id.to_string())
        .bind(completion.status.as_str())
        .bind(completion.assistant_text)
        .bind(completion.error_code)
        .bind(completion.error_text)
        .bind(finished_at_ms)
        .bind(required_status.map(|status| status.as_str()))
        .execute(&self.pool)
        .await
        .context("failed to finalize session turn")?;

        if updated.rows_affected() == 0 {
            return Ok(None);
        }

        self.get(turn_id).await
    }

    pub async fn insert_imported_turn_if_absent(
        &self,
        turn: ImportedSessionTurn,
    ) -> Result<Option<SessionTurnRecord>> {
        let started_at_ms = datetime_to_ms(turn.started_at);
        let finished_at_ms = turn.finished_at.map(datetime_to_ms);
        let result = sqlx::query(
            "INSERT INTO session_turns \
             (turn_id, session_id, sequence_no, kind, status, display_user_text, prompt_user_text, assistant_text, error_code, error_text, attachment_source_turn_id, runtime_id, started_at_ms, finished_at_ms) \
             VALUES (?1, ?2, (SELECT COALESCE(MAX(sequence_no), 0) + 1 FROM session_turns WHERE session_id = ?2), ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13) \
             ON CONFLICT(turn_id) DO NOTHING",
        )
        .bind(turn.turn_id.to_string())
        .bind(turn.session_id.to_string())
        .bind(turn.kind.as_str())
        .bind(turn.status.as_str())
        .bind(&turn.display_user_text)
        .bind(&turn.prompt_user_text)
        .bind(&turn.assistant_text)
        .bind(turn.error_code)
        .bind(turn.error_text)
        .bind(turn.attachment_source_turn_id.map(|turn_id| turn_id.to_string()))
        .bind(&turn.runtime_id)
        .bind(started_at_ms)
        .bind(finished_at_ms)
        .execute(&self.pool)
        .await
        .context("failed to insert imported session turn")?;

        if result.rows_affected() == 0 {
            return Ok(None);
        }

        self.get(turn.turn_id).await
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

    pub async fn append_journal_event(&self, turn_id: Uuid, record: &TurnEvent) -> Result<u64> {
        let event_json = serde_json::to_string(&record.event)
            .context("failed to encode session turn journal event")?;
        let raw_driver = record.raw.as_ref().map(|raw| raw.driver.as_str());
        let raw_payload = record.raw.as_ref().map(|raw| raw.payload.as_str());
        let created_at_ms = now_ms();

        let row = sqlx::query(
            "INSERT INTO session_turn_journal_events \
             (turn_id, sequence_no, event_json, raw_driver, raw_payload, created_at_ms) \
             VALUES (?1, (SELECT COALESCE(MAX(sequence_no), 0) + 1 FROM session_turn_journal_events WHERE turn_id = ?1), ?2, ?3, ?4, ?5) \
             RETURNING sequence_no",
        )
        .bind(turn_id.to_string())
        .bind(event_json)
        .bind(raw_driver)
        .bind(raw_payload)
        .bind(created_at_ms)
        .fetch_one(&self.pool)
        .await
        .with_context(|| format!("failed to append journal event for session turn {turn_id}"))?;
        let sequence_no_raw: i64 = row.get("sequence_no");
        u64::try_from(sequence_no_raw)
            .with_context(|| format!("invalid journal sequence_no '{sequence_no_raw}'"))
    }

    pub async fn list_journal_events(&self, turn_id: Uuid) -> Result<Vec<TurnEvent>> {
        let rows = sqlx::query(
            "SELECT event_json, raw_driver, raw_payload \
             FROM session_turn_journal_events \
             WHERE turn_id = ?1 \
             ORDER BY sequence_no ASC",
        )
        .bind(turn_id.to_string())
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("failed to query journal events for session turn {turn_id}"))?;

        rows.into_iter().map(map_session_turn_journal_row).collect()
    }

    pub async fn update_running_turn_input(
        &self,
        turn_id: Uuid,
        kind: SessionTurnKind,
        display_user_text: &str,
        prompt_user_text: &str,
        attachment_source_turn_id: Option<Uuid>,
    ) -> Result<Option<SessionTurnRecord>> {
        let updated = sqlx::query(
            "UPDATE session_turns \
             SET kind = ?2, display_user_text = ?3, prompt_user_text = ?4, attachment_source_turn_id = ?5 \
             WHERE turn_id = ?1 AND status = ?6",
        )
        .bind(turn_id.to_string())
        .bind(kind.as_str())
        .bind(display_user_text)
        .bind(prompt_user_text)
        .bind(attachment_source_turn_id.map(|turn_id| turn_id.to_string()))
        .bind(SessionTurnStatus::Running.as_str())
        .execute(&self.pool)
        .await
        .context("failed to update running session turn input")?;

        if updated.rows_affected() == 0 {
            return Ok(None);
        }

        self.get(turn_id).await
    }

    pub async fn interrupt_running_turns_without_pending_channel_turns_excluding_scheduler(
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
               AND NOT EXISTS ( \
                 SELECT 1 FROM channel_turns \
                 WHERE channel_turns.turn_id = session_turns.turn_id \
                   AND channel_turns.status = 'pending' \
               ) \
               AND NOT EXISTS ( \
                 SELECT 1 FROM sessions \
                 WHERE sessions.session_id = session_turns.session_id \
                   AND sessions.channel_id = 'scheduler' \
               ) \
             ORDER BY started_at_ms ASC, turn_id ASC",
        )
        .bind(SessionTurnStatus::Running.as_str())
        .fetch_all(&mut *tx)
        .await
        .context("failed to query running session turns")?;

        sqlx::query(
            "UPDATE session_turns \
             SET status = ?1, error_code = ?2, error_text = ?3, finished_at_ms = ?4 \
             WHERE status = ?5 \
               AND NOT EXISTS ( \
                 SELECT 1 FROM channel_turns \
                 WHERE channel_turns.turn_id = session_turns.turn_id \
                   AND channel_turns.status = 'pending' \
               ) \
               AND NOT EXISTS ( \
                 SELECT 1 FROM sessions \
                 WHERE sessions.session_id = session_turns.session_id \
                   AND sessions.channel_id = 'scheduler' \
               )",
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

    pub async fn interrupt_running_scheduler_turns_for_jobs_started_before(
        &self,
        job_ids: &[Uuid],
        started_before_ms: i64,
        reason: &str,
    ) -> Result<Vec<InterruptedSessionTurn>> {
        self.interrupt_running_scheduler_turns_for_jobs_inner(
            job_ids,
            Some(started_before_ms),
            reason,
        )
        .await
    }

    pub async fn interrupt_running_scheduler_turns_for_jobs(
        &self,
        job_ids: &[Uuid],
        reason: &str,
    ) -> Result<Vec<InterruptedSessionTurn>> {
        self.interrupt_running_scheduler_turns_for_jobs_inner(job_ids, None, reason)
            .await
    }

    async fn interrupt_running_scheduler_turns_for_jobs_inner(
        &self,
        job_ids: &[Uuid],
        started_before_ms: Option<i64>,
        reason: &str,
    ) -> Result<Vec<InterruptedSessionTurn>> {
        if job_ids.is_empty() {
            return Ok(Vec::new());
        }

        let finished_at_ms = now_ms();
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to start interrupted scheduler turn transaction")?;
        let mut interrupted = Vec::new();

        for job_id in job_ids {
            let peer_id = format!("job:{job_id}");
            let rows = sqlx::query(
                "SELECT session_turns.turn_id, session_turns.session_id \
                 FROM session_turns \
                 JOIN sessions ON sessions.session_id = session_turns.session_id \
                 WHERE session_turns.status = ?1 \
                   AND sessions.channel_id = 'scheduler' \
                   AND sessions.peer_id = ?2 \
                   AND (?3 IS NULL OR session_turns.started_at_ms < ?3) \
                 ORDER BY session_turns.started_at_ms ASC, session_turns.turn_id ASC",
            )
            .bind(SessionTurnStatus::Running.as_str())
            .bind(&peer_id)
            .bind(started_before_ms)
            .fetch_all(&mut *tx)
            .await
            .context("failed to query running scheduler session turns")?;

            sqlx::query(
                "UPDATE session_turns \
                 SET status = ?1, error_code = ?2, error_text = ?3, finished_at_ms = ?4 \
                 WHERE status = ?5 \
                   AND (?6 IS NULL OR started_at_ms < ?6) \
                   AND session_id IN ( \
                     SELECT session_id FROM sessions \
                     WHERE channel_id = 'scheduler' AND peer_id = ?7 \
                   )",
            )
            .bind(SessionTurnStatus::Interrupted.as_str())
            .bind("runtime.interrupted")
            .bind(reason)
            .bind(finished_at_ms)
            .bind(SessionTurnStatus::Running.as_str())
            .bind(started_before_ms)
            .bind(&peer_id)
            .execute(&mut *tx)
            .await
            .context("failed to interrupt running scheduler session turns")?;

            for row in rows {
                let turn_id_raw: String = row.get("turn_id");
                let session_id_raw: String = row.get("session_id");
                let turn_id = Uuid::parse_str(&turn_id_raw)
                    .with_context(|| format!("invalid interrupted turn_id '{turn_id_raw}'"))?;
                let session_id = Uuid::parse_str(&session_id_raw).with_context(|| {
                    format!("invalid interrupted session_id '{session_id_raw}'")
                })?;
                interrupted.push(InterruptedSessionTurn {
                    turn_id,
                    session_id,
                });
            }
        }

        tx.commit()
            .await
            .context("failed to commit interrupted scheduler turn transaction")?;

        Ok(interrupted)
    }

    pub async fn interrupt_running_scheduler_turns_by_id(
        &self,
        turn_ids: &[Uuid],
        reason: &str,
    ) -> Result<Vec<InterruptedSessionTurn>> {
        if turn_ids.is_empty() {
            return Ok(Vec::new());
        }

        let finished_at_ms = now_ms();
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to start interrupted scheduler turn transaction")?;
        let mut interrupted = Vec::new();

        for turn_id in turn_ids {
            let row = sqlx::query(
                "SELECT session_turns.turn_id, session_turns.session_id \
                 FROM session_turns \
                 JOIN sessions ON sessions.session_id = session_turns.session_id \
                 WHERE session_turns.turn_id = ?1 \
                   AND session_turns.status = ?2 \
                   AND sessions.channel_id = 'scheduler'",
            )
            .bind(turn_id.to_string())
            .bind(SessionTurnStatus::Running.as_str())
            .fetch_optional(&mut *tx)
            .await
            .context("failed to query running scheduler session turn")?;

            let Some(row) = row else {
                continue;
            };

            let updated = sqlx::query(
                "UPDATE session_turns \
                 SET status = ?1, error_code = ?2, error_text = ?3, finished_at_ms = ?4 \
                 WHERE turn_id = ?5 \
                   AND status = ?6 \
                   AND session_id IN ( \
                     SELECT session_id FROM sessions WHERE channel_id = 'scheduler' \
                   )",
            )
            .bind(SessionTurnStatus::Interrupted.as_str())
            .bind("runtime.interrupted")
            .bind(reason)
            .bind(finished_at_ms)
            .bind(turn_id.to_string())
            .bind(SessionTurnStatus::Running.as_str())
            .execute(&mut *tx)
            .await
            .context("failed to interrupt running scheduler session turn")?;
            if updated.rows_affected() == 0 {
                continue;
            }

            let turn_id_raw: String = row.get("turn_id");
            let session_id_raw: String = row.get("session_id");
            let turn_id = Uuid::parse_str(&turn_id_raw)
                .with_context(|| format!("invalid interrupted turn_id '{turn_id_raw}'"))?;
            let session_id = Uuid::parse_str(&session_id_raw)
                .with_context(|| format!("invalid interrupted session_id '{session_id_raw}'"))?;
            interrupted.push(InterruptedSessionTurn {
                turn_id,
                session_id,
            });
        }

        tx.commit()
            .await
            .context("failed to commit interrupted scheduler turn transaction")?;

        Ok(interrupted)
    }

    pub async fn get(&self, turn_id: Uuid) -> Result<Option<SessionTurnRecord>> {
        let row = sqlx::query(
            "SELECT turn_id, session_id, sequence_no, kind, status, display_user_text, prompt_user_text, assistant_text, error_code, error_text, attachment_source_turn_id, runtime_id, started_at_ms, finished_at_ms \
             FROM session_turns \
             WHERE turn_id = ?1",
        )
        .bind(turn_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .context("failed to query session turn")?;

        row.map(map_session_turn_row).transpose()
    }

    pub(crate) async fn get_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        turn_id: Uuid,
    ) -> Result<Option<SessionTurnRecord>> {
        let row = sqlx::query(
            "SELECT turn_id, session_id, sequence_no, kind, status, display_user_text, prompt_user_text, assistant_text, error_code, error_text, attachment_source_turn_id, runtime_id, started_at_ms, finished_at_ms \
             FROM session_turns \
             WHERE turn_id = ?1",
        )
        .bind(turn_id.to_string())
        .fetch_optional(&mut **tx)
        .await
        .context("failed to query session turn in transaction")?;

        row.map(map_session_turn_row).transpose()
    }

    pub(crate) async fn mark_waiting_for_attachments_ready_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        turn_id: Uuid,
    ) -> Result<Option<SessionTurnRecord>> {
        let changed = sqlx::query(
            "UPDATE session_turns \
             SET status = ?2, error_code = NULL, error_text = NULL, finished_at_ms = NULL \
             WHERE turn_id = ?1 AND status = ?3",
        )
        .bind(turn_id.to_string())
        .bind(SessionTurnStatus::Running.as_str())
        .bind(SessionTurnStatus::WaitingForAttachments.as_str())
        .execute(&mut **tx)
        .await
        .context("failed to mark attachment-waiting session turn ready")?;

        if changed.rows_affected() == 0 {
            return Ok(None);
        }

        self.get_in_tx(tx, turn_id).await
    }

    pub async fn latest(&self, session_id: Uuid) -> Result<Option<SessionTurnRecord>> {
        let row = sqlx::query(
            "SELECT turn_id, session_id, sequence_no, kind, status, display_user_text, prompt_user_text, assistant_text, error_code, error_text, attachment_source_turn_id, runtime_id, started_at_ms, finished_at_ms \
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
            "SELECT turn_id, session_id, sequence_no, kind, status, display_user_text, prompt_user_text, assistant_text, error_code, error_text, attachment_source_turn_id, runtime_id, started_at_ms, finished_at_ms \
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

    pub async fn list_recent_before_sequence(
        &self,
        session_id: Uuid,
        before_sequence_no: u64,
        limit: usize,
    ) -> Result<Vec<SessionTurnRecord>> {
        let limit = i64::try_from(limit).context("session history limit is too large")?;
        let before_sequence_no =
            i64::try_from(before_sequence_no).context("before_sequence_no is too large")?;
        let rows = sqlx::query(
            "SELECT turn_id, session_id, sequence_no, kind, status, display_user_text, prompt_user_text, assistant_text, error_code, error_text, attachment_source_turn_id, runtime_id, started_at_ms, finished_at_ms \
             FROM session_turns \
             WHERE session_id = ?1 AND sequence_no < ?2 \
             ORDER BY sequence_no DESC \
             LIMIT ?3",
        )
        .bind(session_id.to_string())
        .bind(before_sequence_no)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .context("failed to query bounded session turn history")?;

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
            "SELECT turn_id, session_id, sequence_no, kind, status, display_user_text, prompt_user_text, assistant_text, error_code, error_text, attachment_source_turn_id, runtime_id, started_at_ms, finished_at_ms \
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
            "SELECT turn_id, session_id, sequence_no, kind, status, display_user_text, prompt_user_text, assistant_text, error_code, error_text, attachment_source_turn_id, runtime_id, started_at_ms, finished_at_ms \
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
    let attachment_source_turn_id_raw: Option<String> = row.get("attachment_source_turn_id");

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
    let attachment_source_turn_id = attachment_source_turn_id_raw
        .map(|value| {
            Uuid::parse_str(&value)
                .with_context(|| format!("invalid attachment_source_turn_id '{value}'"))
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
        attachment_source_turn_id,
        runtime_id: row.get("runtime_id"),
        started_at,
        finished_at,
    })
}

fn map_session_turn_journal_row(row: SqliteRow) -> Result<TurnEvent> {
    let event_json: String = row.get("event_json");
    let raw_driver: Option<String> = row.get("raw_driver");
    let raw_payload: Option<String> = row.get("raw_payload");
    let event = serde_json::from_str::<RuntimeEvent>(&event_json)
        .context("failed to decode session turn journal event")?;
    let raw = match (raw_driver, raw_payload) {
        (Some(driver), Some(payload)) => Some(RawTurnPayload { driver, payload }),
        (None, None) => None,
        _ => return Err(anyhow!("session turn journal raw payload is incomplete")),
    };
    Ok(TurnEvent { event, raw })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{SessionHistoryPolicy, TrustTier};
    use crate::home::runtime_project_partition_key;
    use crate::kernel::jobs::{JobSchedule, JobStore, NewSchedulerJob, SchedulerJobTriggerKind};
    use crate::kernel::{db::Db, sessions::SessionStore};
    use chrono::Duration as ChronoDuration;
    use lionclaw_runtime_api::{canonical_events, RuntimeMessageLane};

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

    async fn new_store_with_scheduler_run() -> (SessionTurnStore, JobStore, Uuid, Uuid, Uuid) {
        let db = Db::connect_memory().await.expect("connect memory db");
        let pool = db.pool();
        let sessions = SessionStore::new(pool.clone());
        let turns = SessionTurnStore::new(pool.clone());
        let jobs = JobStore::new(pool);
        let job = jobs
            .create_job(NewSchedulerJob {
                name: "scheduler-turn".to_string(),
                runtime_id: "mock".to_string(),
                schedule: JobSchedule::Once {
                    run_at: Utc::now() - ChronoDuration::minutes(1),
                },
                prompt_text: "prompt".to_string(),
                delivery: None,
                retry_attempts: 0,
            })
            .await
            .expect("create scheduler job");
        let session = sessions
            .open(
                "scheduler".to_string(),
                format!("job:{}", job.job_id),
                runtime_project_partition_key(None),
                TrustTier::Main,
                SessionHistoryPolicy::Conservative,
            )
            .await
            .expect("open scheduler session");
        let turn = turns
            .begin_turn(NewSessionTurn {
                turn_id: Uuid::new_v4(),
                session_id: session.session_id,
                kind: SessionTurnKind::Normal,
                display_user_text: "job".to_string(),
                prompt_user_text: "prompt".to_string(),
                attachment_source_turn_id: None,
                runtime_id: "mock".to_string(),
            })
            .await
            .expect("begin scheduler turn");
        let claimed = jobs
            .claim_due_jobs(Utc::now(), 1, SchedulerJobTriggerKind::Schedule)
            .await
            .expect("claim scheduler job")
            .pop()
            .expect("claimed scheduler job");
        assert_eq!(claimed.job.job_id, job.job_id);
        assert!(jobs
            .attach_run_turn(claimed.run.run_id, session.session_id, turn.turn_id)
            .await
            .expect("attach scheduler run turn"));

        (
            turns,
            jobs,
            session.session_id,
            turn.turn_id,
            claimed.run.run_id,
        )
    }

    #[tokio::test]
    async fn restart_turn_recovery_excludes_scheduler_sessions() {
        let db = Db::connect_memory().await.expect("connect memory db");
        let pool = db.pool();
        let sessions = SessionStore::new(pool.clone());
        let store = SessionTurnStore::new(pool);
        let normal_session = sessions
            .open(
                "local-cli".to_string(),
                "restart-peer".to_string(),
                runtime_project_partition_key(None),
                TrustTier::Main,
                SessionHistoryPolicy::Interactive,
            )
            .await
            .expect("open normal session");
        let scheduler_session = sessions
            .open(
                "scheduler".to_string(),
                format!("job:{}", Uuid::new_v4()),
                runtime_project_partition_key(None),
                TrustTier::Main,
                SessionHistoryPolicy::Conservative,
            )
            .await
            .expect("open scheduler session");

        let normal_turn = store
            .begin_turn_with_started_at(
                NewSessionTurn {
                    turn_id: Uuid::new_v4(),
                    session_id: normal_session.session_id,
                    kind: SessionTurnKind::Normal,
                    display_user_text: "normal".to_string(),
                    prompt_user_text: "normal".to_string(),
                    attachment_source_turn_id: None,
                    runtime_id: "mock".to_string(),
                },
                100,
            )
            .await
            .expect("begin normal turn");
        let scheduler_turn = store
            .begin_turn_with_started_at(
                NewSessionTurn {
                    turn_id: Uuid::new_v4(),
                    session_id: scheduler_session.session_id,
                    kind: SessionTurnKind::Normal,
                    display_user_text: "scheduler".to_string(),
                    prompt_user_text: "scheduler".to_string(),
                    attachment_source_turn_id: None,
                    runtime_id: "mock".to_string(),
                },
                100,
            )
            .await
            .expect("begin scheduler turn");

        let interrupted = store
            .interrupt_running_turns_without_pending_channel_turns_excluding_scheduler(
                "kernel restart",
            )
            .await
            .expect("interrupt restart turns");

        assert_eq!(interrupted.len(), 1);
        assert_eq!(interrupted[0].turn_id, normal_turn.turn_id);
        assert_eq!(
            store
                .get(normal_turn.turn_id)
                .await
                .expect("load normal turn")
                .expect("normal turn")
                .status,
            SessionTurnStatus::Interrupted
        );
        assert_eq!(
            store
                .get(scheduler_turn.turn_id)
                .await
                .expect("load scheduler turn")
                .expect("scheduler turn")
                .status,
            SessionTurnStatus::Running
        );
    }

    #[tokio::test]
    async fn session_turn_journal_persists_ordered_canonical_events_with_raw_retention() {
        let (store, session_id) = new_store_with_session().await;
        let turn = store
            .begin_turn(NewSessionTurn {
                turn_id: Uuid::new_v4(),
                session_id,
                kind: SessionTurnKind::Normal,
                display_user_text: "hello".to_string(),
                prompt_user_text: "hello".to_string(),
                attachment_source_turn_id: None,
                runtime_id: "codex".to_string(),
            })
            .await
            .expect("begin turn");

        let first = TurnEvent::with_raw(
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text: "hi".to_string(),
            },
            RawTurnPayload {
                driver: "codex-app-server".to_string(),
                payload: "{\"method\":\"item/agentMessage/delta\"}".to_string(),
            },
        );
        let second = TurnEvent::canonical(RuntimeEvent::Done);

        assert_eq!(
            store
                .append_journal_event(turn.turn_id, &first)
                .await
                .expect("append first"),
            1
        );
        assert_eq!(
            store
                .append_journal_event(turn.turn_id, &second)
                .await
                .expect("append second"),
            2
        );

        let journal = store
            .list_journal_events(turn.turn_id)
            .await
            .expect("list journal");

        assert_eq!(journal, vec![first, second]);
        assert_eq!(
            canonical_events(&journal).cloned().collect::<Vec<_>>(),
            vec![
                RuntimeEvent::MessageDelta {
                    lane: RuntimeMessageLane::Answer,
                    text: "hi".to_string(),
                },
                RuntimeEvent::Done,
            ]
        );
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
                attachment_source_turn_id: None,
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
                attachment_source_turn_id: None,
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
                attachment_source_turn_id: None,
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

    #[tokio::test]
    async fn complete_running_turn_does_not_overwrite_terminal_turn() {
        let (store, session_id) = new_store_with_session().await;
        let turn = store
            .begin_turn(NewSessionTurn {
                turn_id: Uuid::new_v4(),
                session_id,
                kind: SessionTurnKind::Normal,
                display_user_text: "recover".to_string(),
                prompt_user_text: "recover".to_string(),
                attachment_source_turn_id: None,
                runtime_id: "mock".to_string(),
            })
            .await
            .expect("begin turn");

        store
            .complete_turn(
                turn.turn_id,
                SessionTurnCompletion {
                    status: SessionTurnStatus::Interrupted,
                    assistant_text: String::new(),
                    error_code: Some("runtime.interrupted".to_string()),
                    error_text: Some("recovered".to_string()),
                },
            )
            .await
            .expect("interrupt turn")
            .expect("turn interrupted");

        let stale_completion = store
            .complete_running_turn(
                turn.turn_id,
                SessionTurnCompletion {
                    status: SessionTurnStatus::Completed,
                    assistant_text: "stale output".to_string(),
                    error_code: None,
                    error_text: None,
                },
            )
            .await
            .expect("attempt stale completion");
        assert!(
            stale_completion.is_none(),
            "terminal turn must not be overwritten by running-only completion"
        );

        let stored = store
            .get(turn.turn_id)
            .await
            .expect("load turn")
            .expect("turn exists");
        assert_eq!(stored.status, SessionTurnStatus::Interrupted);
        assert_eq!(stored.assistant_text, "");
        assert_eq!(stored.error_text.as_deref(), Some("recovered"));
    }

    #[tokio::test]
    async fn complete_running_turn_for_current_scheduler_run_completes_owned_turn() {
        let (store, _jobs, session_id, turn_id, run_id) = new_store_with_scheduler_run().await;

        let completed = store
            .complete_running_turn_for_current_scheduler_run(
                turn_id,
                run_id,
                session_id,
                SessionTurnCompletion {
                    status: SessionTurnStatus::Completed,
                    assistant_text: "done".to_string(),
                    error_code: None,
                    error_text: None,
                },
            )
            .await
            .expect("complete owned scheduler turn")
            .expect("owned scheduler turn completed");

        assert_eq!(completed.status, SessionTurnStatus::Completed);
        assert_eq!(completed.assistant_text, "done");
    }

    #[tokio::test]
    async fn complete_running_turn_for_current_scheduler_run_rejects_stale_owner() {
        let (store, jobs, session_id, turn_id, run_id) = new_store_with_scheduler_run().await;
        jobs.interrupt_run(run_id, "recovered")
            .await
            .expect("interrupt scheduler run")
            .expect("scheduler run");

        let stale_completion = store
            .complete_running_turn_for_current_scheduler_run(
                turn_id,
                run_id,
                session_id,
                SessionTurnCompletion {
                    status: SessionTurnStatus::Completed,
                    assistant_text: "stale output".to_string(),
                    error_code: None,
                    error_text: None,
                },
            )
            .await
            .expect("attempt stale scheduler completion");
        assert!(
            stale_completion.is_none(),
            "stale scheduler owner must not complete the turn"
        );

        let stored = store.get(turn_id).await.expect("load turn").expect("turn");
        assert_eq!(stored.status, SessionTurnStatus::Running);
        assert_eq!(stored.assistant_text, "");
    }

    #[tokio::test]
    async fn complete_running_turn_for_current_scheduler_run_requires_job_bound_session() {
        let (store, _jobs, session_id, turn_id, run_id) = new_store_with_scheduler_run().await;
        sqlx::query("UPDATE sessions SET peer_id = ?2 WHERE session_id = ?1")
            .bind(session_id.to_string())
            .bind(format!("job:{}", Uuid::new_v4()))
            .execute(&store.pool)
            .await
            .expect("move scheduler session peer");

        let mismatched_completion = store
            .complete_running_turn_for_current_scheduler_run(
                turn_id,
                run_id,
                session_id,
                SessionTurnCompletion {
                    status: SessionTurnStatus::Completed,
                    assistant_text: "wrong peer".to_string(),
                    error_code: None,
                    error_text: None,
                },
            )
            .await
            .expect("attempt mismatched scheduler completion");
        assert!(
            mismatched_completion.is_none(),
            "scheduler completion must require the job-bound scheduler session"
        );

        let stored = store.get(turn_id).await.expect("load turn").expect("turn");
        assert_eq!(stored.status, SessionTurnStatus::Running);
        assert_eq!(stored.assistant_text, "");
    }

    #[tokio::test]
    async fn scheduler_turn_recovery_ignores_turns_started_after_cutoff() {
        let db = Db::connect_memory().await.expect("connect memory db");
        let pool = db.pool();
        let sessions = SessionStore::new(pool.clone());
        let store = SessionTurnStore::new(pool);
        let job_id = Uuid::new_v4();
        let session = sessions
            .open(
                "scheduler".to_string(),
                format!("job:{job_id}"),
                runtime_project_partition_key(None),
                TrustTier::Main,
                SessionHistoryPolicy::Conservative,
            )
            .await
            .expect("open scheduler session");

        let stale_turn = store
            .begin_turn_with_started_at(
                NewSessionTurn {
                    turn_id: Uuid::new_v4(),
                    session_id: session.session_id,
                    kind: SessionTurnKind::Normal,
                    display_user_text: "stale".to_string(),
                    prompt_user_text: "stale".to_string(),
                    attachment_source_turn_id: None,
                    runtime_id: "mock".to_string(),
                },
                100,
            )
            .await
            .expect("begin stale scheduler turn");
        let fresh_turn = store
            .begin_turn_with_started_at(
                NewSessionTurn {
                    turn_id: Uuid::new_v4(),
                    session_id: session.session_id,
                    kind: SessionTurnKind::Normal,
                    display_user_text: "fresh".to_string(),
                    prompt_user_text: "fresh".to_string(),
                    attachment_source_turn_id: None,
                    runtime_id: "mock".to_string(),
                },
                300,
            )
            .await
            .expect("begin fresh scheduler turn");

        let interrupted = store
            .interrupt_running_scheduler_turns_for_jobs_started_before(
                &[job_id],
                200,
                "recover stale scheduler turn",
            )
            .await
            .expect("interrupt stale scheduler turns");

        assert_eq!(interrupted.len(), 1);
        assert_eq!(interrupted[0].turn_id, stale_turn.turn_id);
        assert_eq!(
            store
                .get(stale_turn.turn_id)
                .await
                .expect("load stale turn")
                .expect("stale turn")
                .status,
            SessionTurnStatus::Interrupted
        );
        assert_eq!(
            store
                .get(fresh_turn.turn_id)
                .await
                .expect("load fresh turn")
                .expect("fresh turn")
                .status,
            SessionTurnStatus::Running
        );
    }

    #[tokio::test]
    async fn scheduler_turn_recovery_by_id_interrupts_only_scheduler_turns() {
        let db = Db::connect_memory().await.expect("connect memory db");
        let pool = db.pool();
        let sessions = SessionStore::new(pool.clone());
        let store = SessionTurnStore::new(pool);
        let scheduler_session = sessions
            .open(
                "scheduler".to_string(),
                format!("job:{}", Uuid::new_v4()),
                runtime_project_partition_key(None),
                TrustTier::Main,
                SessionHistoryPolicy::Conservative,
            )
            .await
            .expect("open scheduler session");
        let normal_session = sessions
            .open(
                "local-cli".to_string(),
                "normal-peer".to_string(),
                runtime_project_partition_key(None),
                TrustTier::Main,
                SessionHistoryPolicy::Interactive,
            )
            .await
            .expect("open normal session");

        let scheduler_turn = store
            .begin_turn_with_started_at(
                NewSessionTurn {
                    turn_id: Uuid::new_v4(),
                    session_id: scheduler_session.session_id,
                    kind: SessionTurnKind::Normal,
                    display_user_text: "scheduler".to_string(),
                    prompt_user_text: "scheduler".to_string(),
                    attachment_source_turn_id: None,
                    runtime_id: "mock".to_string(),
                },
                300,
            )
            .await
            .expect("begin scheduler turn");
        let normal_turn = store
            .begin_turn_with_started_at(
                NewSessionTurn {
                    turn_id: Uuid::new_v4(),
                    session_id: normal_session.session_id,
                    kind: SessionTurnKind::Normal,
                    display_user_text: "normal".to_string(),
                    prompt_user_text: "normal".to_string(),
                    attachment_source_turn_id: None,
                    runtime_id: "mock".to_string(),
                },
                300,
            )
            .await
            .expect("begin normal turn");

        let interrupted = store
            .interrupt_running_scheduler_turns_by_id(
                &[scheduler_turn.turn_id, normal_turn.turn_id],
                "recover scheduler turn",
            )
            .await
            .expect("interrupt scheduler turn by id");

        assert_eq!(interrupted.len(), 1);
        assert_eq!(interrupted[0].turn_id, scheduler_turn.turn_id);
        assert_eq!(
            store
                .get(scheduler_turn.turn_id)
                .await
                .expect("load scheduler turn")
                .expect("scheduler turn")
                .status,
            SessionTurnStatus::Interrupted
        );
        assert_eq!(
            store
                .get(normal_turn.turn_id)
                .await
                .expect("load normal turn")
                .expect("normal turn")
                .status,
            SessionTurnStatus::Running
        );
    }

    #[tokio::test]
    async fn imported_turn_insert_is_idempotent() {
        let (store, session_id) = new_store_with_session().await;
        let turn_id = Uuid::new_v4();
        let started_at = DateTime::parse_from_rfc3339("2026-05-25T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let finished_at = DateTime::parse_from_rfc3339("2026-05-25T10:00:01Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let inserted = store
            .insert_imported_turn_if_absent(ImportedSessionTurn {
                turn_id,
                session_id,
                kind: SessionTurnKind::Normal,
                status: SessionTurnStatus::Completed,
                display_user_text: "hello".to_string(),
                prompt_user_text: "hello".to_string(),
                assistant_text: "answer".to_string(),
                error_code: None,
                error_text: None,
                attachment_source_turn_id: None,
                runtime_id: "codex".to_string(),
                started_at,
                finished_at: Some(finished_at),
            })
            .await
            .expect("insert imported")
            .expect("inserted");

        let duplicate = store
            .insert_imported_turn_if_absent(ImportedSessionTurn {
                turn_id,
                session_id,
                kind: SessionTurnKind::Normal,
                status: SessionTurnStatus::Completed,
                display_user_text: "hello".to_string(),
                prompt_user_text: "hello".to_string(),
                assistant_text: "answer".to_string(),
                error_code: None,
                error_text: None,
                attachment_source_turn_id: None,
                runtime_id: "codex".to_string(),
                started_at,
                finished_at: Some(finished_at),
            })
            .await
            .expect("duplicate insert");

        assert!(duplicate.is_none());
        assert_eq!(inserted.sequence_no, 1);
        assert_eq!(
            store
                .list_recent(session_id, 10)
                .await
                .expect("turns")
                .len(),
            1
        );
    }
}
