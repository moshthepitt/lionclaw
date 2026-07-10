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
    #[error("{0}")]
    AlreadyExists(String),
    #[error(transparent)]
    Store(#[from] anyhow::Error),
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
        self.publish(mission_id, 1, std::slice::from_ref(&created), now_ms);
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
        self.publish(mission_id, expected_head + 1, events, now_ms);
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
        decode_rows(mission_id, rows)
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
        decode_rows(mission_id, rows)
    }

    /// Rebuild the effect ledger from a folded state's inflight set (cursor
    /// rebuild). Inflight effects whose outcome is already recorded are, by
    /// definition, absent from `inflight`.
    ///
    /// Reproducible effects (oracles) reseed as `queued` — a re-run is safe.
    /// A role run reseeds as an **expired lease**: the
    /// "an attempt was started" fact is otherwise ledger-only, and losing it
    /// would let a rebuild re-invoke the LLM. An expired lease makes reconcile
    /// synthesize failure instead (never re-run a possibly-already-run LLM).
    pub async fn reseed_effects(
        &self,
        state: &crate::model::MissionState,
        now_ms: i64,
    ) -> anyhow::Result<()> {
        let mut tx = self.pool().begin_with("BEGIN IMMEDIATE").await?;
        for (key, effect) in &state.inflight {
            let request_json = serde_json::to_string(effect)?;
            let source_seq = inflight_source_seq(effect);
            let is_role_run = matches!(effect, InflightEffect::RoleRun { .. });
            if is_role_run {
                sqlx::query(
                    "INSERT INTO mission_effects
                         (effect_id, mission_id, source_seq, kind, request_json, status,
                          lease_owner, lease_expires_at_ms, created_at_ms, updated_at_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5, 'leased', 'rebuild-orphan', 0, ?6, ?6)
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
            } else {
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
        }
        tx.commit().await?;
        Ok(())
    }

    /// Mission ids in creation order.
    pub async fn list_missions(&self) -> anyhow::Result<Vec<MissionId>> {
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT mission_id FROM missions ORDER BY created_at_ms, mission_id")
                .fetch_all(self.pool())
                .await?;
        rows.into_iter()
            .map(|(id,)| Ok(MissionId::parse(id)?))
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
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT effect_id, request_json FROM mission_effects
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
        for (effect_id, request_json) in rows {
            let claimed = sqlx::query(
                "UPDATE mission_effects
                 SET status = 'leased',
                     lease_owner = ?2, lease_expires_at_ms = ?3,
                     updated_at_ms = ?4
                 WHERE effect_id = ?1
                   AND (status = 'queued'
                        OR (status = 'leased' AND lease_expires_at_ms <= ?4))",
            )
            .bind(&effect_id)
            .bind(worker_id)
            .bind(now_ms + lease_ms)
            .bind(now_ms)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if claimed == 0 {
                continue;
            }
            let request: InflightEffect = serde_json::from_str(&request_json)
                .map_err(|err| anyhow::anyhow!("corrupt effect '{effect_id}': {err}"))?;
            leases.push(EffectLease { effect_id, request });
        }
        tx.commit().await?;
        Ok(leases)
    }

    /// Current ledger status of an effect (`queued`/`leased`/`done`/`failed`),
    /// plus its lease expiry (if leased). `None` if the row is missing.
    pub async fn effect_status(&self, effect_id: &str) -> anyhow::Result<Option<EffectStatus>> {
        let row: Option<(String, Option<i64>)> = sqlx::query_as(
            "SELECT status, lease_expires_at_ms FROM mission_effects WHERE effect_id = ?1",
        )
        .bind(effect_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(row.map(|(status, lease_expires_at_ms)| EffectStatus {
            status,
            lease_expires_at_ms,
        }))
    }

    /// Reset an effect to `queued` (reconcile path for safely re-runnable
    /// effects — engine-run oracles, never LLM role runs). Refuses to steal a
    /// live (unexpired) lease held by a concurrent driver.
    pub async fn requeue_effect(&self, effect_id: &str, now_ms: i64) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE mission_effects
             SET status = 'queued', lease_owner = NULL, lease_expires_at_ms = NULL,
                 updated_at_ms = ?2
             WHERE effect_id = ?1
               AND (status = 'queued'
                    OR (status = 'leased' AND lease_expires_at_ms <= ?2))",
        )
        .bind(effect_id)
        .bind(now_ms)
        .execute(self.pool())
        .await?;
        Ok(())
    }
}

/// An effect's ledger status and lease expiry.
#[derive(Debug, Clone)]
pub struct EffectStatus {
    pub status: String,
    pub lease_expires_at_ms: Option<i64>,
}

impl EffectStatus {
    /// Whether a concurrent driver still holds a live claim on this effect
    /// (leased with a lease that has not yet expired).
    pub fn is_live_lease(&self, now_ms: i64) -> bool {
        self.status == "leased" && self.lease_expires_at_ms.is_some_and(|exp| exp > now_ms)
    }
}

#[derive(Debug, Clone)]
pub struct EffectLease {
    pub effect_id: String,
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
             (mission_id, sequence_no, recorded_at_ms, schema_version,
              payload_json, idempotency_key, idem_class)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )
    .bind(mission_id.as_str())
    .bind(sequence_no as i64)
    .bind(now_ms)
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

/// Decode `(sequence_no, recorded_at_ms, payload_json)` rows into envelopes.
/// Shared by `load` and `load_after` so the tail loader gets the same rich
/// "engine older than the log?" diagnostic on an undecodable event.
fn decode_rows(
    mission_id: &MissionId,
    rows: Vec<(i64, i64, String)>,
) -> anyhow::Result<Vec<EventEnvelope>> {
    rows.into_iter()
        .map(|(sequence_no, recorded_at_ms, payload_json)| {
            // Unknown event types are a hard error: an engine older than the log
            // must refuse loudly, never skip silently.
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

fn is_unique_violation(err: &sqlx::Error) -> bool {
    err.as_database_error()
        .is_some_and(|db| db.is_unique_violation())
}

/// Map a `missions`-table insert error: a PK collision (not an idempotency key)
/// becomes `AlreadyExists` with `detail` as its message.
fn map_sqlx(err: sqlx::Error, detail: &str) -> AppendError {
    if is_unique_violation(&err) {
        AppendError::AlreadyExists(detail.to_string())
    } else {
        AppendError::Store(err.into())
    }
}

#[cfg(test)]
mod sink_tests {
    use std::sync::{Arc, Mutex};

    use crate::model::{
        DecisionAction, EventEnvelope, MissionConfig, MissionEvent, MissionTypeRef, StopBar,
    };
    use crate::ports::EventSink;

    use super::*;

    #[derive(Default)]
    struct Recorder(Mutex<Vec<u64>>);
    impl EventSink for Recorder {
        fn emit(&self, event: &EventEnvelope) {
            self.0.lock().unwrap().push(event.sequence_no);
        }
    }

    fn created() -> NewEvent {
        NewEvent::new(MissionEvent::MissionCreated {
            objective: "o".into(),
            mission_type: MissionTypeRef {
                name: "t".into(),
                digest: "d".into(),
            },
            runtime: "codex".into(),
            image_id: "img".into(),
            workspace_dir: "/w".into(),
            base_sha: "base".into(),
            config: MissionConfig {
                ratification_gate: false,
                stop: StopBar::Reviewed,
                planning: Default::default(),
                terminal_review: None,
            },
        })
    }

    fn decision() -> NewEvent {
        NewEvent::new(MissionEvent::DecisionRecorded {
            attention_id: "x".into(),
            action: DecisionAction::Continue,
            justification: String::new(),
            actor: "t".into(),
        })
    }

    // The sink sees exactly the committed sequence numbers, in order — and a
    // rolled-back append (stale head) publishes nothing, so a consumer never
    // sees a phantom event.
    #[tokio::test]
    async fn sink_fires_after_commit_never_on_a_rolled_back_append() {
        let dir = tempfile::tempdir().unwrap();
        let rec = Arc::new(Recorder::default());
        let store = MissionStore::open(dir.path())
            .await
            .unwrap()
            .with_sink(rec.clone());
        let id = MissionId::parse("mabc123def456").unwrap();

        store
            .create_mission(&id, "/w", "o", created(), 1)
            .await
            .unwrap();
        assert_eq!(*rec.0.lock().unwrap(), vec![1]);

        let head = store
            .append(&id, 1, &[decision(), decision()], 2)
            .await
            .unwrap();
        assert_eq!(head, 3);
        assert_eq!(*rec.0.lock().unwrap(), vec![1, 2, 3]);

        // Stale expected-head ⇒ Conflict ⇒ transaction never commits.
        assert!(store.append(&id, 1, &[decision()], 3).await.is_err());
        assert_eq!(
            *rec.0.lock().unwrap(),
            vec![1, 2, 3],
            "a rolled-back append must not publish"
        );
    }
}
