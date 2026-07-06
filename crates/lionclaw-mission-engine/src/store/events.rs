//! Event log append/load and the derived effect ledger.
//!
//! Append discipline (kernel `session_turns` + `audit` patterns): one
//! `BEGIN IMMEDIATE` transaction takes the write lock up front, verifies the
//! expected head (structural optimistic concurrency via the
//! `(mission_id, sequence_no)` primary key), inserts the events, and keeps
//! the effect ledger in lockstep — a `…Requested` event enqueues its effect
//! row and an outcome event settles it, atomically with the log.
//!
//! Effect leasing (kernel `channel_outbox` pattern): `pull_due` CAS-leases
//! queued rows, re-checking eligibility in the UPDATE's WHERE clause so
//! concurrent pullers can't double-lease.
//!
//! Payload externalization (>100KB → blob) happens where payloads are
//! constructed (engine/runners); events reaching append are ref-carrying.

use serde::{Deserialize, Serialize};

use crate::model::{
    EventEnvelope, IdemClass, InflightEffect, MissionEvent, MissionId, VersionStamps,
    SCHEMA_VERSION,
};

use super::MissionStore;

#[derive(Debug, Clone)]
pub struct NewEvent {
    pub stamps: VersionStamps,
    pub event: MissionEvent,
}

impl NewEvent {
    /// Engine-authored event with default stamps.
    pub fn new(event: MissionEvent) -> Self {
        Self {
            stamps: VersionStamps {
                schema_version: SCHEMA_VERSION,
                engine_version: env!("CARGO_PKG_VERSION").to_string(),
                ..Default::default()
            },
            event,
        }
    }

    pub fn with_model_id(mut self, model_id: Option<String>) -> Self {
        self.stamps.model_id = model_id;
        self
    }

    pub fn with_prompt_hash(mut self, prompt_hash: impl Into<String>) -> Self {
        self.stamps.prompt_hash = Some(prompt_hash.into());
        self
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AppendError {
    #[error("append conflict: expected head {expected}, log is at {actual}")]
    Conflict { expected: u64, actual: u64 },
    #[error("duplicate idempotency key '{key}'")]
    Duplicate { key: String },
    #[error(transparent)]
    Store(#[from] anyhow::Error),
}

#[derive(Debug, Clone)]
pub struct MissionSummary {
    pub mission_id: MissionId,
    pub workspace_dir: String,
    pub objective: String,
    pub created_at_ms: i64,
}

/// Persisted payload document: stamps + event, in one JSON column.
#[derive(Serialize, Deserialize)]
struct PayloadDoc {
    stamps: VersionStamps,
    event: MissionEvent,
}

impl MissionStore {
    /// Register a mission and append its `MissionCreated` event atomically.
    pub async fn create_mission(
        &self,
        mission_id: &MissionId,
        workspace_dir: &str,
        objective: &str,
        created: NewEvent,
        now_ms: i64,
    ) -> Result<(), AppendError> {
        let mut tx = self
            .pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(anyhow::Error::from)?;
        sqlx::query(
            "INSERT INTO missions (mission_id, workspace_dir, objective, created_at_ms)
             VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(mission_id.as_str())
        .bind(workspace_dir)
        .bind(objective)
        .bind(now_ms)
        .execute(&mut *tx)
        .await
        .map_err(|err| map_sqlx(err, "mission already exists"))?;
        insert_event(&mut tx, mission_id, 1, &created, now_ms).await?;
        tx.commit().await.map_err(anyhow::Error::from)?;
        Ok(())
    }

    /// Append events after `expected_head`, keeping the effect ledger in
    /// lockstep. Returns the new head.
    pub async fn append(
        &self,
        mission_id: &MissionId,
        expected_head: u64,
        events: &[NewEvent],
        now_ms: i64,
    ) -> Result<u64, AppendError> {
        let mut tx = self
            .pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(anyhow::Error::from)?;
        let actual: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(sequence_no), 0) FROM mission_events WHERE mission_id = ?1",
        )
        .bind(mission_id.as_str())
        .fetch_one(&mut *tx)
        .await
        .map_err(anyhow::Error::from)?;
        let actual = actual as u64;
        if actual != expected_head {
            return Err(AppendError::Conflict {
                expected: expected_head,
                actual,
            });
        }
        let mut seq = expected_head;
        for event in events {
            seq += 1;
            insert_event(&mut tx, mission_id, seq, event, now_ms).await?;
        }
        tx.commit().await.map_err(anyhow::Error::from)?;
        Ok(seq)
    }

    /// Load the full event stream in order.
    pub async fn load(&self, mission_id: &MissionId) -> anyhow::Result<Vec<EventEnvelope>> {
        let rows: Vec<(i64, i64, String)> = sqlx::query_as(
            "SELECT sequence_no, recorded_at_ms, payload_json
             FROM mission_events WHERE mission_id = ?1 ORDER BY sequence_no",
        )
        .bind(mission_id.as_str())
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|(sequence_no, recorded_at_ms, payload_json)| {
                // Unknown event types are a hard error: an engine older than
                // the log must refuse loudly, never skip silently.
                let doc: PayloadDoc = serde_json::from_str(&payload_json).map_err(|err| {
                    anyhow::anyhow!(
                        "cannot decode event {sequence_no} of mission {mission_id}: {err} \
                         (engine older than the log?)"
                    )
                })?;
                Ok(EventEnvelope {
                    mission_id: mission_id.clone(),
                    sequence_no: sequence_no as u64,
                    recorded_at_ms,
                    stamps: doc.stamps,
                    event: doc.event,
                })
            })
            .collect()
    }

    /// Load events after a given sequence number (snapshot tail).
    pub async fn load_after(
        &self,
        mission_id: &MissionId,
        after_seq: u64,
    ) -> anyhow::Result<Vec<EventEnvelope>> {
        let rows: Vec<(i64, i64, String)> = sqlx::query_as(
            "SELECT sequence_no, recorded_at_ms, payload_json
             FROM mission_events WHERE mission_id = ?1 AND sequence_no > ?2
             ORDER BY sequence_no",
        )
        .bind(mission_id.as_str())
        .bind(after_seq as i64)
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|(sequence_no, recorded_at_ms, payload_json)| {
                let doc: PayloadDoc = serde_json::from_str(&payload_json).map_err(|err| {
                    anyhow::anyhow!("cannot decode event {sequence_no}: {err}")
                })?;
                Ok(EventEnvelope {
                    mission_id: mission_id.clone(),
                    sequence_no: sequence_no as u64,
                    recorded_at_ms,
                    stamps: doc.stamps,
                    event: doc.event,
                })
            })
            .collect()
    }

    /// Rebuild the queued effect ledger from a folded state's inflight set
    /// (cursor rebuild). Inflight effects whose outcome is already recorded
    /// are, by definition, absent from `inflight`, so this reseeds exactly
    /// the still-owed effects as `queued`.
    pub async fn reseed_effects(
        &self,
        state: &crate::model::MissionState,
        now_ms: i64,
    ) -> anyhow::Result<()> {
        let mut tx = self.pool().begin_with("BEGIN IMMEDIATE").await?;
        for (key, effect) in &state.inflight {
            let request_json = serde_json::to_string(effect)?;
            let source_seq = inflight_source_seq(effect);
            sqlx::query(
                "INSERT INTO mission_effects
                     (effect_id, mission_id, source_seq, kind, request_json, status,
                      created_at_ms, updated_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, 'queued', ?6, ?6)
                 ON CONFLICT(effect_id) DO NOTHING",
            )
            .bind(key)
            .bind(state.mission_id.as_str())
            .bind(source_seq as i64)
            .bind(effect.kind_str())
            .bind(&request_json)
            .bind(now_ms)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn list_missions(&self) -> anyhow::Result<Vec<MissionSummary>> {
        let rows: Vec<(String, String, String, i64)> = sqlx::query_as(
            "SELECT mission_id, workspace_dir, objective, created_at_ms
             FROM missions ORDER BY created_at_ms, mission_id",
        )
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|(id, workspace_dir, objective, created_at_ms)| {
                Ok(MissionSummary {
                    mission_id: MissionId::parse(id)?,
                    workspace_dir,
                    objective,
                    created_at_ms,
                })
            })
            .collect()
    }

    /// Lease due effects for execution (CAS in the WHERE clause; the ledger
    /// is derived state and rebuildable from the log).
    pub async fn pull_due(
        &self,
        mission_id: &MissionId,
        worker_id: &str,
        limit: u32,
        lease_ms: i64,
        now_ms: i64,
    ) -> anyhow::Result<Vec<EffectLease>> {
        let mut tx = self.pool().begin_with("BEGIN IMMEDIATE").await?;
        // Due = queued, or leased with an expired lease (a crashed worker's
        // claim is reclaimable). The CAS UPDATE below re-checks this so a
        // concurrent puller can never double-lease.
        let rows: Vec<(String, String, i64)> = sqlx::query_as(
            "SELECT effect_id, request_json, attempt_count FROM mission_effects
             WHERE mission_id = ?1
               AND (status = 'queued'
                    OR (status = 'leased' AND lease_expires_at_ms <= ?3))
             ORDER BY source_seq LIMIT ?2",
        )
        .bind(mission_id.as_str())
        .bind(limit as i64)
        .bind(now_ms)
        .fetch_all(&mut *tx)
        .await?;
        let mut leases = Vec::with_capacity(rows.len());
        for (effect_id, request_json, attempt_count) in rows {
            let attempt_id = format!("{effect_id}-a{}", attempt_count + 1);
            let claimed = sqlx::query(
                "UPDATE mission_effects
                 SET status = 'leased', attempt_count = attempt_count + 1,
                     lease_owner = ?2, lease_expires_at_ms = ?3,
                     current_attempt_id = ?4, updated_at_ms = ?5
                 WHERE effect_id = ?1
                   AND (status = 'queued'
                        OR (status = 'leased' AND lease_expires_at_ms <= ?5))",
            )
            .bind(&effect_id)
            .bind(worker_id)
            .bind(now_ms + lease_ms)
            .bind(&attempt_id)
            .bind(now_ms)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if claimed == 0 {
                continue;
            }
            sqlx::query(
                "INSERT INTO mission_effect_attempts
                     (attempt_id, effect_id, worker_id, status, started_at_ms)
                 VALUES (?1, ?2, ?3, 'leased', ?4)",
            )
            .bind(&attempt_id)
            .bind(&effect_id)
            .bind(worker_id)
            .bind(now_ms)
            .execute(&mut *tx)
            .await?;
            let request: InflightEffect = serde_json::from_str(&request_json)
                .map_err(|err| anyhow::anyhow!("corrupt effect '{effect_id}': {err}"))?;
            leases.push(EffectLease {
                effect_id,
                attempt_id,
                request,
            });
        }
        tx.commit().await?;
        Ok(leases)
    }

    /// Current ledger status of an effect (`queued`/`leased`/`done`/`failed`),
    /// or `None` if the row is missing.
    pub async fn effect_status(&self, effect_id: &str) -> anyhow::Result<Option<String>> {
        Ok(sqlx::query_scalar(
            "SELECT status FROM mission_effects WHERE effect_id = ?1",
        )
        .bind(effect_id)
        .fetch_optional(self.pool())
        .await?)
    }

    /// Reset an effect to `queued` (reconcile path for safely re-runnable
    /// effects — engine-run oracles, never LLM role runs).
    pub async fn requeue_effect(&self, effect_id: &str, now_ms: i64) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE mission_effects
             SET status = 'queued', lease_owner = NULL, lease_expires_at_ms = NULL,
                 current_attempt_id = NULL, updated_at_ms = ?2
             WHERE effect_id = ?1 AND status IN ('queued', 'leased')",
        )
        .bind(effect_id)
        .bind(now_ms)
        .execute(self.pool())
        .await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct EffectLease {
    pub effect_id: String,
    pub attempt_id: String,
    pub request: InflightEffect,
}

async fn insert_event(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    mission_id: &MissionId,
    sequence_no: u64,
    event: &NewEvent,
    now_ms: i64,
) -> Result<(), AppendError> {
    let payload = serde_json::to_string(&PayloadDoc {
        stamps: event.stamps.clone(),
        event: event.event.clone(),
    })
    .map_err(anyhow::Error::from)?;
    let idem = event.event.idempotency();
    let (idem_key, idem_class) = match &idem {
        Some((IdemClass::Request, key)) => (Some(*key), Some("request")),
        Some((IdemClass::Outcome, key)) => (Some(*key), Some("outcome")),
        None => (None, None),
    };
    sqlx::query(
        "INSERT INTO mission_events
             (mission_id, sequence_no, recorded_at_ms, event_type, schema_version,
              payload_json, idempotency_key, idem_class)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
    )
    .bind(mission_id.as_str())
    .bind(sequence_no as i64)
    .bind(now_ms)
    .bind(event.event.event_type())
    .bind(event.stamps.schema_version)
    .bind(&payload)
    .bind(idem_key)
    .bind(idem_class)
    .execute(&mut **tx)
    .await
    .map_err(|err| match &idem {
        Some((_, key)) if is_unique_violation(&err) => AppendError::Duplicate {
            key: (*key).to_string(),
        },
        _ => AppendError::Store(err.into()),
    })?;

    // Keep the effect ledger in lockstep, in the same transaction.
    if let Some((key, effect)) = InflightEffect::from_request(&event.event, sequence_no) {
        let request_json = serde_json::to_string(&effect).map_err(anyhow::Error::from)?;
        sqlx::query(
            "INSERT INTO mission_effects
                 (effect_id, mission_id, source_seq, kind, request_json, status,
                  created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, 'queued', ?6, ?6)",
        )
        .bind(&key)
        .bind(mission_id.as_str())
        .bind(sequence_no as i64)
        .bind(effect.kind_str())
        .bind(&request_json)
        .bind(now_ms)
        .execute(&mut **tx)
        .await
        .map_err(anyhow::Error::from)?;
    } else if let Some(succeeded) = event.event.outcome_succeeded() {
        let (_, key) = idem.expect("outcome events carry an idempotency key");
        let status = if succeeded { "done" } else { "failed" };
        sqlx::query(
            "UPDATE mission_effect_attempts
             SET status = ?2, finished_at_ms = ?3
             WHERE attempt_id = (SELECT current_attempt_id FROM mission_effects WHERE effect_id = ?1)",
        )
        .bind(key)
        .bind(if succeeded { "completed" } else { "failed" })
        .bind(now_ms)
        .execute(&mut **tx)
        .await
        .map_err(anyhow::Error::from)?;
        sqlx::query(
            "UPDATE mission_effects
             SET status = ?2, lease_owner = NULL, lease_expires_at_ms = NULL,
                 updated_at_ms = ?3
             WHERE effect_id = ?1",
        )
        .bind(key)
        .bind(status)
        .bind(now_ms)
        .execute(&mut **tx)
        .await
        .map_err(anyhow::Error::from)?;
    }
    Ok(())
}

fn inflight_source_seq(effect: &InflightEffect) -> u64 {
    match effect {
        InflightEffect::RoleRun { requested_seq, .. }
        | InflightEffect::OracleRun { requested_seq, .. }
        | InflightEffect::TerminalReview { requested_seq, .. } => *requested_seq,
    }
}

fn is_unique_violation(err: &sqlx::Error) -> bool {
    err.as_database_error()
        .is_some_and(|db| db.is_unique_violation())
}

fn map_sqlx(err: sqlx::Error, unique_detail: &str) -> AppendError {
    if is_unique_violation(&err) {
        AppendError::Duplicate {
            key: unique_detail.to_string(),
        }
    } else {
        AppendError::Store(err.into())
    }
}
