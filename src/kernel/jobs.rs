use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use cron::Schedule;
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqliteRow, Row, SqlitePool};
use uuid::Uuid;

use crate::kernel::db::{ms_to_datetime, now_ms};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum JobSchedule {
    Once { run_at: DateTime<Utc> },
    Interval { every_ms: u64, anchor_ms: i64 },
    Cron { expr: String, timezone: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDeliveryTarget {
    pub channel_id: String,
    pub peer_id: String,
}

#[derive(Debug, Clone)]
pub struct SchedulerJobRecord {
    pub job_id: Uuid,
    pub name: String,
    pub enabled: bool,
    pub runtime_id: String,
    pub schedule: JobSchedule,
    pub prompt_text: String,
    pub skill_ids: Vec<String>,
    pub delivery: Option<JobDeliveryTarget>,
    pub retry_attempts: u32,
    pub next_run_at: Option<DateTime<Utc>>,
    pub running_run_id: Option<Uuid>,
    pub last_run_at: Option<DateTime<Utc>>,
    pub last_status: Option<String>,
    pub last_error: Option<String>,
    pub consecutive_failures: u32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerJobTriggerKind {
    Schedule,
    Manual,
    Retry,
    Recovery,
}

impl SchedulerJobTriggerKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Schedule => "schedule",
            Self::Manual => "manual",
            Self::Retry => "retry",
            Self::Recovery => "recovery",
        }
    }
}

impl FromStr for SchedulerJobTriggerKind {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "schedule" => Ok(Self::Schedule),
            "manual" => Ok(Self::Manual),
            "retry" => Ok(Self::Retry),
            "recovery" => Ok(Self::Recovery),
            other => Err(format!("invalid scheduler job trigger kind '{}'", other)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerJobRunStatus {
    Running,
    Completed,
    Failed,
    DeadLetter,
    Interrupted,
}

impl SchedulerJobRunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::DeadLetter => "dead_letter",
            Self::Interrupted => "interrupted",
        }
    }
}

impl FromStr for SchedulerJobRunStatus {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "dead_letter" => Ok(Self::DeadLetter),
            "interrupted" => Ok(Self::Interrupted),
            other => Err(format!("invalid scheduler job run status '{}'", other)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerJobDeliveryStatus {
    Pending,
    Delivered,
    Failed,
    NotRequested,
}

impl SchedulerJobDeliveryStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Delivered => "delivered",
            Self::Failed => "failed",
            Self::NotRequested => "not_requested",
        }
    }
}

impl FromStr for SchedulerJobDeliveryStatus {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "delivered" => Ok(Self::Delivered),
            "failed" => Ok(Self::Failed),
            "not_requested" => Ok(Self::NotRequested),
            other => Err(format!("invalid scheduler job delivery status '{}'", other)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SchedulerJobRunRecord {
    pub run_id: Uuid,
    pub job_id: Uuid,
    pub attempt_no: u32,
    pub trigger_kind: SchedulerJobTriggerKind,
    pub scheduled_for: Option<DateTime<Utc>>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub status: SchedulerJobRunStatus,
    pub session_id: Option<Uuid>,
    pub turn_id: Option<Uuid>,
    pub delivery_status: Option<SchedulerJobDeliveryStatus>,
    pub error_text: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NewSchedulerJob {
    pub name: String,
    pub runtime_id: String,
    pub schedule: JobSchedule,
    pub prompt_text: String,
    pub skill_ids: Vec<String>,
    pub delivery: Option<JobDeliveryTarget>,
    pub retry_attempts: u32,
}

#[derive(Debug, Clone)]
pub struct ClaimedSchedulerJob {
    pub job: SchedulerJobRecord,
    pub run: SchedulerJobRunRecord,
}

#[derive(Debug, Clone)]
pub struct JobStore {
    pool: SqlitePool,
}

impl JobStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn create_job(&self, input: NewSchedulerJob) -> Result<SchedulerJobRecord> {
        let now = now_ms();
        let job_id = Uuid::new_v4();
        let next_run_at = compute_initial_next_run(&input.schedule, Utc::now())?;
        let schedule_kind = schedule_kind(&input.schedule);
        let schedule_json =
            serde_json::to_string(&input.schedule).context("failed to encode job schedule")?;
        let skill_ids_json =
            serde_json::to_string(&input.skill_ids).context("failed to encode job skills")?;
        let delivery_json = input
            .delivery
            .as_ref()
            .map(|value| serde_json::to_string(value))
            .transpose()
            .context("failed to encode job delivery target")?;

        sqlx::query(
            "INSERT INTO scheduler_jobs \
             (job_id, name, enabled, runtime_id, schedule_kind, schedule_json, prompt_text, \
              skill_ids_json, delivery_json, retry_attempts, next_run_at_ms, running_run_id, \
              last_run_at_ms, last_status, last_error, consecutive_failures, created_at_ms, updated_at_ms) \
             VALUES (?1, ?2, 1, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, NULL, NULL, NULL, NULL, 0, ?11, ?11)",
        )
        .bind(job_id.to_string())
        .bind(&input.name)
        .bind(&input.runtime_id)
        .bind(schedule_kind)
        .bind(schedule_json)
        .bind(&input.prompt_text)
        .bind(skill_ids_json)
        .bind(delivery_json)
        .bind(i64::from(input.retry_attempts))
        .bind(next_run_at.map(|value| value.timestamp_millis()))
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to insert scheduler job")?;

        self.get_job(job_id)
            .await?
            .ok_or_else(|| anyhow!("scheduler job disappeared after insert"))
    }

    pub async fn list_jobs(&self) -> Result<Vec<SchedulerJobRecord>> {
        let rows = sqlx::query(
            "SELECT job_id, name, enabled, runtime_id, schedule_kind, schedule_json, prompt_text, \
             skill_ids_json, delivery_json, retry_attempts, next_run_at_ms, running_run_id, last_run_at_ms, \
             last_status, last_error, consecutive_failures, created_at_ms, updated_at_ms \
             FROM scheduler_jobs \
             ORDER BY created_at_ms DESC, job_id DESC",
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to list scheduler jobs")?;

        rows.into_iter().map(map_job_row).collect()
    }

    pub async fn get_job(&self, job_id: Uuid) -> Result<Option<SchedulerJobRecord>> {
        let row = sqlx::query(
            "SELECT job_id, name, enabled, runtime_id, schedule_kind, schedule_json, prompt_text, \
             skill_ids_json, delivery_json, retry_attempts, next_run_at_ms, running_run_id, last_run_at_ms, \
             last_status, last_error, consecutive_failures, created_at_ms, updated_at_ms \
             FROM scheduler_jobs \
             WHERE job_id = ?1",
        )
        .bind(job_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .context("failed to query scheduler job")?;

        row.map(map_job_row).transpose()
    }

    pub async fn delete_job(&self, job_id: Uuid) -> Result<bool> {
        let result =
            sqlx::query("DELETE FROM scheduler_jobs WHERE job_id = ?1 AND running_run_id IS NULL")
                .bind(job_id.to_string())
                .execute(&self.pool)
                .await
                .context("failed to delete scheduler job")?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn pause_job(&self, job_id: Uuid) -> Result<Option<SchedulerJobRecord>> {
        let now = now_ms();
        let updated = sqlx::query(
            "UPDATE scheduler_jobs \
             SET enabled = 0, updated_at_ms = ?2 \
             WHERE job_id = ?1",
        )
        .bind(job_id.to_string())
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to pause scheduler job")?;
        if updated.rows_affected() == 0 {
            return Ok(None);
        }
        self.get_job(job_id).await
    }

    pub async fn resume_job(&self, job_id: Uuid) -> Result<Option<SchedulerJobRecord>> {
        let Some(job) = self.get_job(job_id).await? else {
            return Ok(None);
        };
        let next_run_at = compute_initial_next_run(&job.schedule, Utc::now())?;
        let now = now_ms();
        let updated = sqlx::query(
            "UPDATE scheduler_jobs \
             SET enabled = 1, next_run_at_ms = ?2, updated_at_ms = ?3 \
             WHERE job_id = ?1 AND running_run_id IS NULL",
        )
        .bind(job_id.to_string())
        .bind(next_run_at.map(|value| value.timestamp_millis()))
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to resume scheduler job")?;
        if updated.rows_affected() == 0 {
            return Ok(None);
        }
        self.get_job(job_id).await
    }

    pub async fn list_runs(
        &self,
        job_id: Uuid,
        limit: usize,
    ) -> Result<Vec<SchedulerJobRunRecord>> {
        let limit = i64::try_from(limit).context("scheduler job runs limit is too large")?;
        let rows = sqlx::query(
            "SELECT run_id, job_id, attempt_no, trigger_kind, scheduled_for_ms, started_at_ms, finished_at_ms, \
             status, session_id, turn_id, delivery_status, error_text \
             FROM scheduler_job_runs \
             WHERE job_id = ?1 \
             ORDER BY started_at_ms DESC, run_id DESC \
             LIMIT ?2",
        )
        .bind(job_id.to_string())
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .context("failed to list scheduler job runs")?;
        rows.into_iter().map(map_run_row).collect()
    }

    pub async fn try_acquire_tick_lease(&self, owner: &str, ttl: Duration) -> Result<bool> {
        let now = now_ms();
        let lease_expires = now + i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX);
        let result = sqlx::query(
            "UPDATE scheduler_state \
             SET lease_owner = ?2, lease_expires_at_ms = ?3, last_tick_started_at_ms = ?4 \
             WHERE state_id = 1 \
               AND (lease_owner IS NULL OR lease_expires_at_ms IS NULL OR lease_expires_at_ms < ?1 OR lease_owner = ?2)",
        )
        .bind(now)
        .bind(owner)
        .bind(lease_expires)
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to acquire scheduler tick lease")?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn release_tick_lease(&self, owner: &str) -> Result<()> {
        let now = now_ms();
        sqlx::query(
            "UPDATE scheduler_state \
             SET lease_owner = NULL, lease_expires_at_ms = NULL, last_tick_finished_at_ms = ?2 \
             WHERE state_id = 1 AND lease_owner = ?1",
        )
        .bind(owner)
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to release scheduler tick lease")?;
        Ok(())
    }

    pub async fn claim_due_jobs(
        &self,
        now: DateTime<Utc>,
        limit: usize,
        trigger_kind: SchedulerJobTriggerKind,
    ) -> Result<Vec<ClaimedSchedulerJob>> {
        let limit = i64::try_from(limit).context("claim limit is too large")?;
        let rows = sqlx::query(
            "SELECT job_id, name, enabled, runtime_id, schedule_kind, schedule_json, prompt_text, \
             skill_ids_json, delivery_json, retry_attempts, next_run_at_ms, running_run_id, last_run_at_ms, \
             last_status, last_error, consecutive_failures, created_at_ms, updated_at_ms \
             FROM scheduler_jobs \
             WHERE enabled = 1 AND running_run_id IS NULL AND next_run_at_ms IS NOT NULL AND next_run_at_ms <= ?1 \
             ORDER BY next_run_at_ms ASC, created_at_ms ASC, job_id ASC \
             LIMIT ?2",
        )
        .bind(now.timestamp_millis())
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .context("failed to query due scheduler jobs")?;

        let mut claimed = Vec::new();
        for row in rows {
            let job = map_job_row(row)?;
            if let Some(claim) = self
                .claim_job_run(job.job_id, trigger_kind, job.next_run_at, 1, true, now)
                .await?
            {
                claimed.push(claim);
            }
        }

        Ok(claimed)
    }

    pub async fn claim_manual_run(
        &self,
        job_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<Option<ClaimedSchedulerJob>> {
        self.claim_job_run(
            job_id,
            SchedulerJobTriggerKind::Manual,
            Some(now),
            1,
            false,
            now,
        )
        .await
    }

    async fn claim_job_run(
        &self,
        job_id: Uuid,
        trigger_kind: SchedulerJobTriggerKind,
        scheduled_for: Option<DateTime<Utc>>,
        attempt_no: u32,
        advance_schedule: bool,
        now: DateTime<Utc>,
    ) -> Result<Option<ClaimedSchedulerJob>> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to start claim scheduler job transaction")?;

        let row = sqlx::query(
            "SELECT job_id, name, enabled, runtime_id, schedule_kind, schedule_json, prompt_text, \
             skill_ids_json, delivery_json, retry_attempts, next_run_at_ms, running_run_id, last_run_at_ms, \
             last_status, last_error, consecutive_failures, created_at_ms, updated_at_ms \
             FROM scheduler_jobs \
             WHERE job_id = ?1",
        )
        .bind(job_id.to_string())
        .fetch_optional(&mut *tx)
        .await
        .context("failed to load scheduler job for claim")?;

        let Some(row) = row else {
            tx.commit()
                .await
                .context("failed to finish empty claim transaction")?;
            return Ok(None);
        };
        let job = map_job_row(row)?;
        if !job.enabled || job.running_run_id.is_some() {
            tx.commit()
                .await
                .context("failed to finish skipped claim transaction")?;
            return Ok(None);
        }

        let run_id = Uuid::new_v4();
        let started_at_ms = now.timestamp_millis();
        let next_run_at = if advance_schedule && is_recurring_schedule(&job.schedule) {
            advance_schedule_after_claim(&job.schedule, scheduled_for.or(job.next_run_at), now)?
        } else {
            job.next_run_at
        };

        let updated = sqlx::query(
            "UPDATE scheduler_jobs \
             SET next_run_at_ms = ?2, running_run_id = ?3, updated_at_ms = ?4 \
             WHERE job_id = ?1 AND running_run_id IS NULL",
        )
        .bind(job_id.to_string())
        .bind(next_run_at.map(|value| value.timestamp_millis()))
        .bind(run_id.to_string())
        .bind(started_at_ms)
        .execute(&mut *tx)
        .await
        .context("failed to mark scheduler job running")?;
        if updated.rows_affected() == 0 {
            tx.commit()
                .await
                .context("failed to finish lost-race claim transaction")?;
            return Ok(None);
        }

        sqlx::query(
            "INSERT INTO scheduler_job_runs \
             (run_id, job_id, attempt_no, trigger_kind, scheduled_for_ms, started_at_ms, finished_at_ms, \
              status, session_id, turn_id, delivery_status, error_text) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, NULL, NULL, ?8, NULL)",
        )
        .bind(run_id.to_string())
        .bind(job_id.to_string())
        .bind(i64::from(attempt_no))
        .bind(trigger_kind.as_str())
        .bind(scheduled_for.map(|value| value.timestamp_millis()))
        .bind(started_at_ms)
        .bind(SchedulerJobRunStatus::Running.as_str())
        .bind(
            job.delivery
                .as_ref()
                .map(|_| SchedulerJobDeliveryStatus::Pending.as_str())
                .unwrap_or(SchedulerJobDeliveryStatus::NotRequested.as_str()),
        )
        .execute(&mut *tx)
        .await
        .context("failed to insert scheduler job run")?;

        let run = sqlx::query(
            "SELECT run_id, job_id, attempt_no, trigger_kind, scheduled_for_ms, started_at_ms, finished_at_ms, \
             status, session_id, turn_id, delivery_status, error_text \
             FROM scheduler_job_runs WHERE run_id = ?1",
        )
        .bind(run_id.to_string())
        .fetch_one(&mut *tx)
        .await
        .context("failed to load claimed scheduler job run")?;
        let updated_job_row = sqlx::query(
            "SELECT job_id, name, enabled, runtime_id, schedule_kind, schedule_json, prompt_text, \
             skill_ids_json, delivery_json, retry_attempts, next_run_at_ms, running_run_id, last_run_at_ms, \
             last_status, last_error, consecutive_failures, created_at_ms, updated_at_ms \
             FROM scheduler_jobs \
             WHERE job_id = ?1",
        )
        .bind(job_id.to_string())
        .fetch_one(&mut *tx)
        .await
        .context("failed to load claimed scheduler job")?;

        tx.commit()
            .await
            .context("failed to commit scheduler job claim")?;

        Ok(Some(ClaimedSchedulerJob {
            job: map_job_row(updated_job_row)?,
            run: map_run_row(run)?,
        }))
    }

    pub async fn begin_retry_run(
        &self,
        current_run_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<Option<SchedulerJobRunRecord>> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to start retry run transaction")?;

        let current_row = sqlx::query(
            "SELECT run_id, job_id, attempt_no, trigger_kind, scheduled_for_ms, started_at_ms, finished_at_ms, \
             status, session_id, turn_id, delivery_status, error_text \
             FROM scheduler_job_runs WHERE run_id = ?1",
        )
        .bind(current_run_id.to_string())
        .fetch_optional(&mut *tx)
        .await
        .context("failed to query current scheduler run")?;
        let Some(current_row) = current_row else {
            tx.commit()
                .await
                .context("failed to finish empty retry transaction")?;
            return Ok(None);
        };
        let current = map_run_row(current_row)?;
        let job_row = sqlx::query(
            "SELECT job_id, name, enabled, runtime_id, schedule_kind, schedule_json, prompt_text, \
             skill_ids_json, delivery_json, retry_attempts, next_run_at_ms, running_run_id, last_run_at_ms, \
             last_status, last_error, consecutive_failures, created_at_ms, updated_at_ms \
             FROM scheduler_jobs WHERE job_id = ?1",
        )
        .bind(current.job_id.to_string())
        .fetch_one(&mut *tx)
        .await
        .context("failed to load scheduler job for retry")?;
        let job = map_job_row(job_row)?;
        if current.status != SchedulerJobRunStatus::Running {
            tx.commit()
                .await
                .context("failed to finish skipped retry transaction")?;
            return Ok(None);
        }

        let retry_run_id = Uuid::new_v4();
        let started_at_ms = now.timestamp_millis();
        sqlx::query(
            "UPDATE scheduler_job_runs \
             SET finished_at_ms = ?2, status = ?3, error_text = ?4 \
             WHERE run_id = ?1",
        )
        .bind(current_run_id.to_string())
        .bind(started_at_ms)
        .bind(SchedulerJobRunStatus::Failed.as_str())
        .bind("retrying after failed attempt")
        .execute(&mut *tx)
        .await
        .context("failed to mark current run failed before retry")?;

        sqlx::query(
            "UPDATE scheduler_jobs SET running_run_id = ?2, updated_at_ms = ?3 WHERE job_id = ?1",
        )
        .bind(current.job_id.to_string())
        .bind(retry_run_id.to_string())
        .bind(started_at_ms)
        .execute(&mut *tx)
        .await
        .context("failed to switch running scheduler job run to retry")?;

        sqlx::query(
            "INSERT INTO scheduler_job_runs \
             (run_id, job_id, attempt_no, trigger_kind, scheduled_for_ms, started_at_ms, finished_at_ms, \
              status, session_id, turn_id, delivery_status, error_text) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, NULL, NULL, ?8, NULL)",
        )
        .bind(retry_run_id.to_string())
        .bind(current.job_id.to_string())
        .bind(i64::from(current.attempt_no + 1))
        .bind(SchedulerJobTriggerKind::Retry.as_str())
        .bind(current.scheduled_for.map(|value| value.timestamp_millis()))
        .bind(started_at_ms)
        .bind(SchedulerJobRunStatus::Running.as_str())
        .bind(
            job.delivery
                .as_ref()
                .map(|_| SchedulerJobDeliveryStatus::Pending.as_str())
                .unwrap_or(SchedulerJobDeliveryStatus::NotRequested.as_str()),
        )
        .execute(&mut *tx)
        .await
        .context("failed to insert retry scheduler job run")?;

        let retry_row = sqlx::query(
            "SELECT run_id, job_id, attempt_no, trigger_kind, scheduled_for_ms, started_at_ms, finished_at_ms, \
             status, session_id, turn_id, delivery_status, error_text \
             FROM scheduler_job_runs WHERE run_id = ?1",
        )
        .bind(retry_run_id.to_string())
        .fetch_one(&mut *tx)
        .await
        .context("failed to load retry scheduler job run")?;

        tx.commit()
            .await
            .context("failed to commit retry scheduler job run")?;

        Ok(Some(map_run_row(retry_row)?))
    }

    pub async fn complete_run_success(
        &self,
        run_id: Uuid,
        session_id: Uuid,
        turn_id: Uuid,
        delivery_status: SchedulerJobDeliveryStatus,
    ) -> Result<Option<SchedulerJobRecord>> {
        let finished_at_ms = now_ms();
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to start complete scheduler run transaction")?;

        let run_row = sqlx::query(
            "SELECT run_id, job_id, attempt_no, trigger_kind, scheduled_for_ms, started_at_ms, finished_at_ms, \
             status, session_id, turn_id, delivery_status, error_text \
             FROM scheduler_job_runs WHERE run_id = ?1",
        )
        .bind(run_id.to_string())
        .fetch_optional(&mut *tx)
        .await
        .context("failed to query scheduler run for completion")?;
        let Some(run_row) = run_row else {
            tx.commit()
                .await
                .context("failed to finish empty complete transaction")?;
            return Ok(None);
        };
        let run = map_run_row(run_row)?;
        let job_row = sqlx::query(
            "SELECT job_id, name, enabled, runtime_id, schedule_kind, schedule_json, prompt_text, \
             skill_ids_json, delivery_json, retry_attempts, next_run_at_ms, running_run_id, last_run_at_ms, \
             last_status, last_error, consecutive_failures, created_at_ms, updated_at_ms \
             FROM scheduler_jobs WHERE job_id = ?1",
        )
        .bind(run.job_id.to_string())
        .fetch_one(&mut *tx)
        .await
        .context("failed to load scheduler job for success")?;
        let job = map_job_row(job_row)?;
        let disable_one_shot = matches!(job.schedule, JobSchedule::Once { .. })
            && run.trigger_kind != SchedulerJobTriggerKind::Manual;

        sqlx::query(
            "UPDATE scheduler_job_runs \
             SET finished_at_ms = ?2, status = ?3, session_id = ?4, turn_id = ?5, delivery_status = ?6, error_text = NULL \
             WHERE run_id = ?1",
        )
        .bind(run_id.to_string())
        .bind(finished_at_ms)
        .bind(SchedulerJobRunStatus::Completed.as_str())
        .bind(session_id.to_string())
        .bind(turn_id.to_string())
        .bind(delivery_status.as_str())
        .execute(&mut *tx)
        .await
        .context("failed to finalize successful scheduler job run")?;

        sqlx::query(
            "UPDATE scheduler_jobs \
             SET enabled = CASE WHEN ?2 THEN 0 ELSE enabled END, \
                 next_run_at_ms = CASE WHEN ?2 THEN NULL ELSE next_run_at_ms END, \
                 running_run_id = NULL, \
                 last_run_at_ms = ?3, \
                 last_status = ?4, \
                 last_error = NULL, \
                 consecutive_failures = 0, \
                 updated_at_ms = ?3 \
             WHERE job_id = ?1",
        )
        .bind(run.job_id.to_string())
        .bind(if disable_one_shot { 1 } else { 0 })
        .bind(finished_at_ms)
        .bind(SchedulerJobRunStatus::Completed.as_str())
        .execute(&mut *tx)
        .await
        .context("failed to finalize scheduler job state after success")?;

        let updated_job_row = sqlx::query(
            "SELECT job_id, name, enabled, runtime_id, schedule_kind, schedule_json, prompt_text, \
             skill_ids_json, delivery_json, retry_attempts, next_run_at_ms, running_run_id, last_run_at_ms, \
             last_status, last_error, consecutive_failures, created_at_ms, updated_at_ms \
             FROM scheduler_jobs WHERE job_id = ?1",
        )
        .bind(run.job_id.to_string())
        .fetch_one(&mut *tx)
        .await
        .context("failed to reload scheduler job after success")?;

        tx.commit()
            .await
            .context("failed to commit successful scheduler job completion")?;

        Ok(Some(map_job_row(updated_job_row)?))
    }

    pub async fn complete_run_failure(
        &self,
        run_id: Uuid,
        session_id: Option<Uuid>,
        turn_id: Option<Uuid>,
        error_text: &str,
        final_status: SchedulerJobRunStatus,
        delivery_status: SchedulerJobDeliveryStatus,
    ) -> Result<Option<SchedulerJobRecord>> {
        let finished_at_ms = now_ms();
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to start failed scheduler run transaction")?;

        let run_row = sqlx::query(
            "SELECT run_id, job_id, attempt_no, trigger_kind, scheduled_for_ms, started_at_ms, finished_at_ms, \
             status, session_id, turn_id, delivery_status, error_text \
             FROM scheduler_job_runs WHERE run_id = ?1",
        )
        .bind(run_id.to_string())
        .fetch_optional(&mut *tx)
        .await
        .context("failed to query scheduler run for failure")?;
        let Some(run_row) = run_row else {
            tx.commit()
                .await
                .context("failed to finish empty failed transaction")?;
            return Ok(None);
        };
        let run = map_run_row(run_row)?;
        let job_row = sqlx::query(
            "SELECT job_id, name, enabled, runtime_id, schedule_kind, schedule_json, prompt_text, \
             skill_ids_json, delivery_json, retry_attempts, next_run_at_ms, running_run_id, last_run_at_ms, \
             last_status, last_error, consecutive_failures, created_at_ms, updated_at_ms \
             FROM scheduler_jobs WHERE job_id = ?1",
        )
        .bind(run.job_id.to_string())
        .fetch_one(&mut *tx)
        .await
        .context("failed to load scheduler job for failure")?;
        let job = map_job_row(job_row)?;
        let disable_one_shot = matches!(job.schedule, JobSchedule::Once { .. })
            && run.trigger_kind != SchedulerJobTriggerKind::Manual
            && final_status == SchedulerJobRunStatus::DeadLetter;

        sqlx::query(
            "UPDATE scheduler_job_runs \
             SET finished_at_ms = ?2, status = ?3, session_id = ?4, turn_id = ?5, delivery_status = ?6, error_text = ?7 \
             WHERE run_id = ?1",
        )
        .bind(run_id.to_string())
        .bind(finished_at_ms)
        .bind(final_status.as_str())
        .bind(session_id.map(|value| value.to_string()))
        .bind(turn_id.map(|value| value.to_string()))
        .bind(delivery_status.as_str())
        .bind(error_text)
        .execute(&mut *tx)
        .await
        .context("failed to finalize failed scheduler job run")?;

        sqlx::query(
            "UPDATE scheduler_jobs \
             SET enabled = CASE WHEN ?2 THEN 0 ELSE enabled END, \
                 next_run_at_ms = CASE WHEN ?2 THEN NULL ELSE next_run_at_ms END, \
                 running_run_id = NULL, \
                 last_run_at_ms = ?3, \
                 last_status = ?4, \
                 last_error = ?5, \
                 consecutive_failures = consecutive_failures + 1, \
                 updated_at_ms = ?3 \
             WHERE job_id = ?1",
        )
        .bind(run.job_id.to_string())
        .bind(if disable_one_shot { 1 } else { 0 })
        .bind(finished_at_ms)
        .bind(final_status.as_str())
        .bind(error_text)
        .execute(&mut *tx)
        .await
        .context("failed to finalize scheduler job state after failure")?;

        let updated_job_row = sqlx::query(
            "SELECT job_id, name, enabled, runtime_id, schedule_kind, schedule_json, prompt_text, \
             skill_ids_json, delivery_json, retry_attempts, next_run_at_ms, running_run_id, last_run_at_ms, \
             last_status, last_error, consecutive_failures, created_at_ms, updated_at_ms \
             FROM scheduler_jobs WHERE job_id = ?1",
        )
        .bind(run.job_id.to_string())
        .fetch_one(&mut *tx)
        .await
        .context("failed to reload scheduler job after failure")?;

        tx.commit()
            .await
            .context("failed to commit failed scheduler job completion")?;

        Ok(Some(map_job_row(updated_job_row)?))
    }

    pub async fn interrupt_running_runs(
        &self,
        error_text: &str,
    ) -> Result<Vec<SchedulerJobRunRecord>> {
        let finished_at_ms = now_ms();
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to start interrupt scheduler runs transaction")?;

        let rows = sqlx::query(
            "SELECT run_id, job_id, attempt_no, trigger_kind, scheduled_for_ms, started_at_ms, finished_at_ms, \
             status, session_id, turn_id, delivery_status, error_text \
             FROM scheduler_job_runs \
             WHERE status = ?1 \
             ORDER BY started_at_ms ASC, run_id ASC",
        )
        .bind(SchedulerJobRunStatus::Running.as_str())
        .fetch_all(&mut *tx)
        .await
        .context("failed to query running scheduler job runs")?;

        sqlx::query(
            "UPDATE scheduler_job_runs \
             SET finished_at_ms = ?2, status = ?3, error_text = ?4 \
             WHERE status = ?1",
        )
        .bind(SchedulerJobRunStatus::Running.as_str())
        .bind(finished_at_ms)
        .bind(SchedulerJobRunStatus::Interrupted.as_str())
        .bind(error_text)
        .execute(&mut *tx)
        .await
        .context("failed to interrupt scheduler job runs")?;

        sqlx::query(
            "UPDATE scheduler_jobs \
             SET running_run_id = NULL, last_status = ?1, last_error = ?2, updated_at_ms = ?3 \
             WHERE running_run_id IS NOT NULL",
        )
        .bind(SchedulerJobRunStatus::Interrupted.as_str())
        .bind(error_text)
        .bind(finished_at_ms)
        .execute(&mut *tx)
        .await
        .context("failed to clear running scheduler job state")?;

        tx.commit()
            .await
            .context("failed to commit interrupted scheduler job runs")?;

        rows.into_iter().map(map_run_row).collect()
    }
}

pub fn compute_initial_next_run(
    schedule: &JobSchedule,
    now: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>> {
    match schedule {
        JobSchedule::Once { run_at } => Ok(Some(if *run_at > now { *run_at } else { now })),
        JobSchedule::Interval {
            every_ms,
            anchor_ms,
        } => {
            let anchor = ms_to_datetime(*anchor_ms)
                .ok_or_else(|| anyhow!("invalid interval anchor '{}'", anchor_ms))?;
            Ok(Some(compute_interval_next(*every_ms, anchor, now)?))
        }
        JobSchedule::Cron { expr, timezone } => Ok(Some(compute_cron_next(expr, timezone, now)?)),
    }
}

pub fn advance_schedule_after_claim(
    schedule: &JobSchedule,
    scheduled_for: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>> {
    match schedule {
        JobSchedule::Once { .. } => Ok(scheduled_for),
        JobSchedule::Interval {
            every_ms,
            anchor_ms,
        } => {
            let reference = scheduled_for
                .or_else(|| ms_to_datetime(*anchor_ms))
                .ok_or_else(|| {
                    anyhow!("interval schedule is missing a valid scheduled time or anchor")
                })?;
            Ok(Some(compute_interval_next(*every_ms, reference, now)?))
        }
        JobSchedule::Cron { expr, timezone } => {
            let reference = scheduled_for.unwrap_or(now);
            Ok(Some(compute_cron_next_after(
                expr, timezone, reference, now,
            )?))
        }
    }
}

pub fn is_recurring_schedule(schedule: &JobSchedule) -> bool {
    !matches!(schedule, JobSchedule::Once { .. })
}

fn schedule_kind(schedule: &JobSchedule) -> &'static str {
    match schedule {
        JobSchedule::Once { .. } => "once",
        JobSchedule::Interval { .. } => "interval",
        JobSchedule::Cron { .. } => "cron",
    }
}

fn compute_interval_next(
    every_ms: u64,
    reference: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<DateTime<Utc>> {
    if every_ms == 0 {
        return Err(anyhow!(
            "interval schedule every_ms must be greater than zero"
        ));
    }
    let every_ms = i64::try_from(every_ms).context("interval schedule every_ms is too large")?;
    let mut next_ms = reference.timestamp_millis() + every_ms;
    let now_ms = now.timestamp_millis();
    while next_ms <= now_ms {
        next_ms += every_ms;
    }
    ms_to_datetime(next_ms).ok_or_else(|| anyhow!("computed invalid next interval timestamp"))
}

fn compute_cron_next(expr: &str, timezone: &str, now: DateTime<Utc>) -> Result<DateTime<Utc>> {
    compute_cron_next_after(expr, timezone, now, now)
}

fn compute_cron_next_after(
    expr: &str,
    timezone: &str,
    reference: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<DateTime<Utc>> {
    let tz = timezone
        .parse::<Tz>()
        .with_context(|| format!("invalid cron timezone '{}'", timezone))?;
    let schedule = Schedule::from_str(&normalize_cron_expression(expr)?)
        .with_context(|| format!("invalid cron expression '{}'", expr))?;
    let mut base = reference.with_timezone(&tz);
    loop {
        let Some(next) = schedule.after(&base).next() else {
            return Err(anyhow!(
                "cron expression '{}' did not yield a next occurrence",
                expr
            ));
        };
        let next_utc = next.with_timezone(&Utc);
        if next_utc > now {
            return Ok(next_utc);
        }
        base = next;
    }
}

pub fn normalize_cron_expression(expr: &str) -> Result<String> {
    let trimmed = expr.trim();
    let field_count = trimmed.split_whitespace().count();
    match field_count {
        5 => Ok(format!("0 {}", trimmed)),
        6 | 7 => Ok(trimmed.to_string()),
        _ => Err(anyhow!("invalid cron expression '{}'", expr)),
    }
}

fn map_job_row(row: SqliteRow) -> Result<SchedulerJobRecord> {
    let job_id_raw: String = row.get("job_id");
    let schedule_json: String = row.get("schedule_json");
    let skill_ids_json: String = row.get("skill_ids_json");
    let delivery_json: Option<String> = row.get("delivery_json");
    let retry_attempts_raw: i64 = row.get("retry_attempts");
    let next_run_at_ms: Option<i64> = row.get("next_run_at_ms");
    let running_run_id_raw: Option<String> = row.get("running_run_id");
    let last_run_at_ms: Option<i64> = row.get("last_run_at_ms");
    let consecutive_failures_raw: i64 = row.get("consecutive_failures");
    let created_at_ms: i64 = row.get("created_at_ms");
    let updated_at_ms: i64 = row.get("updated_at_ms");

    let job_id = Uuid::parse_str(&job_id_raw)
        .with_context(|| format!("invalid scheduler job id '{}'", job_id_raw))?;
    let schedule: JobSchedule =
        serde_json::from_str(&schedule_json).context("failed to decode scheduler job schedule")?;
    let skill_ids: Vec<String> =
        serde_json::from_str(&skill_ids_json).context("failed to decode scheduler job skills")?;
    let delivery = delivery_json
        .as_deref()
        .map(|value| serde_json::from_str(value).context("failed to decode scheduler job delivery"))
        .transpose()?;
    let running_run_id = running_run_id_raw
        .as_deref()
        .map(|value| {
            Uuid::parse_str(value)
                .with_context(|| format!("invalid scheduler running run id '{}'", value))
        })
        .transpose()?;

    Ok(SchedulerJobRecord {
        job_id,
        name: row.get("name"),
        enabled: row.get::<i64, _>("enabled") != 0,
        runtime_id: row.get("runtime_id"),
        schedule,
        prompt_text: row.get("prompt_text"),
        skill_ids,
        delivery,
        retry_attempts: u32::try_from(retry_attempts_raw)
            .with_context(|| format!("invalid retry_attempts '{}'", retry_attempts_raw))?,
        next_run_at: next_run_at_ms
            .map(|value| {
                ms_to_datetime(value).ok_or_else(|| anyhow!("invalid next_run_at_ms '{}'", value))
            })
            .transpose()?,
        running_run_id,
        last_run_at: last_run_at_ms
            .map(|value| {
                ms_to_datetime(value).ok_or_else(|| anyhow!("invalid last_run_at_ms '{}'", value))
            })
            .transpose()?,
        last_status: row.get("last_status"),
        last_error: row.get("last_error"),
        consecutive_failures: u32::try_from(consecutive_failures_raw).with_context(|| {
            format!(
                "invalid scheduler job consecutive_failures '{}'",
                consecutive_failures_raw
            )
        })?,
        created_at: ms_to_datetime(created_at_ms)
            .ok_or_else(|| anyhow!("invalid created_at_ms '{}'", created_at_ms))?,
        updated_at: ms_to_datetime(updated_at_ms)
            .ok_or_else(|| anyhow!("invalid updated_at_ms '{}'", updated_at_ms))?,
    })
}

fn map_run_row(row: SqliteRow) -> Result<SchedulerJobRunRecord> {
    let run_id_raw: String = row.get("run_id");
    let job_id_raw: String = row.get("job_id");
    let attempt_no_raw: i64 = row.get("attempt_no");
    let trigger_kind_raw: String = row.get("trigger_kind");
    let scheduled_for_ms: Option<i64> = row.get("scheduled_for_ms");
    let started_at_ms: i64 = row.get("started_at_ms");
    let finished_at_ms: Option<i64> = row.get("finished_at_ms");
    let status_raw: String = row.get("status");
    let session_id_raw: Option<String> = row.get("session_id");
    let turn_id_raw: Option<String> = row.get("turn_id");
    let delivery_status_raw: Option<String> = row.get("delivery_status");

    Ok(SchedulerJobRunRecord {
        run_id: Uuid::parse_str(&run_id_raw)
            .with_context(|| format!("invalid scheduler run id '{}'", run_id_raw))?,
        job_id: Uuid::parse_str(&job_id_raw)
            .with_context(|| format!("invalid scheduler job id '{}'", job_id_raw))?,
        attempt_no: u32::try_from(attempt_no_raw)
            .with_context(|| format!("invalid scheduler attempt_no '{}'", attempt_no_raw))?,
        trigger_kind: SchedulerJobTriggerKind::from_str(&trigger_kind_raw)
            .map_err(|err| anyhow!(err))?,
        scheduled_for: scheduled_for_ms
            .map(|value| {
                ms_to_datetime(value).ok_or_else(|| anyhow!("invalid scheduled_for_ms '{}'", value))
            })
            .transpose()?,
        started_at: ms_to_datetime(started_at_ms)
            .ok_or_else(|| anyhow!("invalid started_at_ms '{}'", started_at_ms))?,
        finished_at: finished_at_ms
            .map(|value| {
                ms_to_datetime(value).ok_or_else(|| anyhow!("invalid finished_at_ms '{}'", value))
            })
            .transpose()?,
        status: SchedulerJobRunStatus::from_str(&status_raw).map_err(|err| anyhow!(err))?,
        session_id: session_id_raw
            .as_deref()
            .map(|value| {
                Uuid::parse_str(value)
                    .with_context(|| format!("invalid scheduler run session_id '{}'", value))
            })
            .transpose()?,
        turn_id: turn_id_raw
            .as_deref()
            .map(|value| {
                Uuid::parse_str(value)
                    .with_context(|| format!("invalid scheduler run turn_id '{}'", value))
            })
            .transpose()?,
        delivery_status: delivery_status_raw
            .as_deref()
            .map(|value| SchedulerJobDeliveryStatus::from_str(value).map_err(anyhow::Error::msg))
            .transpose()?,
        error_text: row.get("error_text"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::db::Db;
    use chrono::{Duration as ChronoDuration, TimeZone};

    fn dt(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
            .single()
            .expect("valid datetime")
    }

    async fn new_store() -> JobStore {
        let db = Db::connect_memory().await.expect("connect memory db");
        JobStore::new(db.pool())
    }

    #[test]
    fn interval_schedule_anchoring_prevents_drift() {
        let anchor = dt(2026, 3, 31, 9, 0, 0);
        let now = anchor + ChronoDuration::minutes(95);
        let next = compute_interval_next(
            u64::try_from(ChronoDuration::minutes(30).num_milliseconds()).unwrap(),
            anchor,
            now,
        )
        .expect("interval next run");

        assert_eq!(next, anchor + ChronoDuration::minutes(120));
    }

    #[test]
    fn cron_schedule_computes_future_occurrence() {
        let now = dt(2026, 3, 31, 9, 7, 0);
        let next = compute_initial_next_run(
            &JobSchedule::Cron {
                expr: "*/5 * * * *".to_string(),
                timezone: "UTC".to_string(),
            },
            now,
        )
        .expect("cron next run")
        .expect("cron next run present");

        assert!(next > now);
    }

    #[tokio::test]
    async fn recurring_claim_advances_before_run_and_interrupt_does_not_refire_same_slot() {
        let store = new_store().await;
        let every_ms = u64::try_from(ChronoDuration::minutes(30).num_milliseconds()).unwrap();
        let anchor = Utc::now() - ChronoDuration::hours(2);
        let job = store
            .create_job(NewSchedulerJob {
                name: "recurring".to_string(),
                runtime_id: "mock".to_string(),
                schedule: JobSchedule::Interval {
                    every_ms,
                    anchor_ms: anchor.timestamp_millis(),
                },
                prompt_text: "prompt".to_string(),
                skill_ids: Vec::new(),
                delivery: None,
                retry_attempts: 1,
            })
            .await
            .expect("create job");

        let due_at = Utc::now() - ChronoDuration::seconds(1);
        sqlx::query("UPDATE scheduler_jobs SET next_run_at_ms = ?2 WHERE job_id = ?1")
            .bind(job.job_id.to_string())
            .bind(due_at.timestamp_millis())
            .execute(store.pool())
            .await
            .expect("set due schedule");

        let claimed = store
            .claim_due_jobs(Utc::now(), 10, SchedulerJobTriggerKind::Schedule)
            .await
            .expect("claim due job");
        assert_eq!(claimed.len(), 1);
        assert!(claimed[0]
            .job
            .next_run_at
            .is_some_and(|next_run_at| next_run_at > due_at));

        store
            .interrupt_running_runs("simulated restart")
            .await
            .expect("interrupt running jobs");

        let reclaims = store
            .claim_due_jobs(Utc::now(), 10, SchedulerJobTriggerKind::Recovery)
            .await
            .expect("reclaim after interrupt");
        assert!(reclaims.is_empty());
    }

    #[tokio::test]
    async fn interrupted_one_shot_run_is_claimable_again() {
        let store = new_store().await;
        let job = store
            .create_job(NewSchedulerJob {
                name: "once".to_string(),
                runtime_id: "mock".to_string(),
                schedule: JobSchedule::Once {
                    run_at: Utc::now() - ChronoDuration::minutes(1),
                },
                prompt_text: "prompt".to_string(),
                skill_ids: Vec::new(),
                delivery: None,
                retry_attempts: 1,
            })
            .await
            .expect("create one-shot job");

        let first_claim = store
            .claim_due_jobs(Utc::now(), 10, SchedulerJobTriggerKind::Schedule)
            .await
            .expect("claim one-shot job");
        assert_eq!(first_claim.len(), 1);
        assert_eq!(first_claim[0].job.job_id, job.job_id);

        store
            .interrupt_running_runs("simulated restart")
            .await
            .expect("interrupt running jobs");

        let reclaimed = store
            .claim_due_jobs(
                Utc::now() + ChronoDuration::seconds(1),
                10,
                SchedulerJobTriggerKind::Recovery,
            )
            .await
            .expect("reclaim interrupted one-shot job");
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].job.job_id, job.job_id);
    }

    #[tokio::test]
    async fn retry_run_preserves_not_requested_delivery_status() {
        let store = new_store().await;
        let job = store
            .create_job(NewSchedulerJob {
                name: "retry".to_string(),
                runtime_id: "mock".to_string(),
                schedule: JobSchedule::Once {
                    run_at: Utc::now() - ChronoDuration::minutes(1),
                },
                prompt_text: "prompt".to_string(),
                skill_ids: Vec::new(),
                delivery: None,
                retry_attempts: 1,
            })
            .await
            .expect("create retry job");

        let first_claim = store
            .claim_due_jobs(Utc::now(), 10, SchedulerJobTriggerKind::Schedule)
            .await
            .expect("claim job");
        let retry = store
            .begin_retry_run(
                first_claim[0].run.run_id,
                Utc::now() + ChronoDuration::seconds(1),
            )
            .await
            .expect("begin retry")
            .expect("retry run");

        assert_eq!(
            retry.delivery_status,
            Some(SchedulerJobDeliveryStatus::NotRequested)
        );

        let runs = store.list_runs(job.job_id, 2).await.expect("list job runs");
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0].run_id, retry.run_id);
        assert_eq!(runs[0].status, SchedulerJobRunStatus::Running);
        assert_eq!(runs[1].status, SchedulerJobRunStatus::Failed);
    }

    #[tokio::test]
    async fn tick_lease_blocks_a_second_owner_until_release() {
        let store = new_store().await;

        assert!(store
            .try_acquire_tick_lease("owner-a", Duration::from_secs(60))
            .await
            .expect("acquire first lease"));
        assert!(!store
            .try_acquire_tick_lease("owner-b", Duration::from_secs(60))
            .await
            .expect("second owner blocked"));

        store
            .release_tick_lease("owner-a")
            .await
            .expect("release first lease");

        assert!(store
            .try_acquire_tick_lease("owner-b", Duration::from_secs(60))
            .await
            .expect("second owner acquires released lease"));
    }
}
