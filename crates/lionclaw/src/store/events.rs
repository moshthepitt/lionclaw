//! Event log append/load.
//!
//! Append discipline (kernel `session_turns` + `audit` patterns): one
//! `BEGIN IMMEDIATE` transaction takes the write lock up front, verifies the
//! expected head (structural optimistic concurrency via the
//! `(mission_id, sequence_no)` primary key), and inserts the events.
//!
//! Payload externalization (>100KB → blob) happens where payloads are
//! constructed (engine/runners); events reaching append are ref-carrying.

use serde::{Deserialize, Serialize};

use crate::model::{
    EffectEventClass, EventEnvelope, MissionEvent, MissionId, VersionStamps, SCHEMA_VERSION,
};

use super::MissionStore;

#[derive(Debug, Clone)]
pub struct NewEvent {
    pub stamps: VersionStamps,
    pub event: MissionEvent,
    pub(crate) settlement_evidence: Option<lionclaw_runtime_api::TypedFailureEvidence>,
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
            settlement_evidence: None,
        }
    }

    pub(crate) fn with_settlement_evidence(
        mut self,
        evidence: lionclaw_runtime_api::TypedFailureEvidence,
    ) -> Self {
        self.settlement_evidence = Some(evidence.project());
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
    #[error("duplicate effect id '{effect_id}'")]
    Duplicate { effect_id: String },
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

    /// Append events after `expected_head`. Returns the new head.
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
        let rows: Vec<(i64, i64, i64, String)> = sqlx::query_as(
            "SELECT sequence_no, recorded_at_ms, schema_version, payload_json
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
        let rows: Vec<(i64, i64, i64, String)> = sqlx::query_as(
            "SELECT sequence_no, recorded_at_ms, schema_version, payload_json
             FROM mission_events WHERE mission_id = ?1 AND sequence_no > ?2
             ORDER BY sequence_no",
        )
        .bind(mission_id.as_str())
        .bind(after_seq as i64)
        .fetch_all(self.pool())
        .await?;
        decode_rows(mission_id, rows)
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
    let effect_identity = event.event.effect_identity();
    let (effect_id, effect_class) = match &effect_identity {
        Some((EffectEventClass::Request, effect_id)) => (Some(*effect_id), Some("request")),
        Some((EffectEventClass::Outcome, effect_id)) => (Some(*effect_id), Some("outcome")),
        None => (None, None),
    };
    sqlx::query(
        "INSERT INTO mission_events
             (mission_id, sequence_no, recorded_at_ms, schema_version,
              payload_json, effect_id, effect_class)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )
    .bind(mission_id.as_str())
    .bind(sequence_no as i64)
    .bind(now_ms)
    .bind(event.stamps.schema_version)
    .bind(&payload)
    .bind(effect_id)
    .bind(effect_class)
    .execute(&mut **tx)
    .await
    .map_err(|err| match &effect_identity {
        Some((_, effect_id)) if is_unique_violation(&err) => AppendError::Duplicate {
            effect_id: (*effect_id).to_string(),
        },
        _ => AppendError::Store(err.into()),
    })?;

    Ok(())
}

/// Decode persisted rows into envelopes.
/// Shared by `load` and `load_after` so the tail loader gets the same rich
/// "engine older than the log?" diagnostic on an undecodable event.
fn decode_rows(
    mission_id: &MissionId,
    rows: Vec<(i64, i64, i64, String)>,
) -> anyhow::Result<Vec<EventEnvelope>> {
    rows.into_iter()
        .map(|(sequence_no, recorded_at_ms, stored_schema, payload_json)| {
            // Unknown event types are a hard error: an engine older than the log
            // must refuse loudly, never skip silently.
            let doc: PayloadDoc = serde_json::from_str(&payload_json).map_err(|err| {
                anyhow::anyhow!(
                    "cannot decode event {sequence_no} of mission {mission_id}: {err} \
                     (engine older than the log?)"
                )
            })?;
            if stored_schema != i64::from(doc.stamps.schema_version) {
                anyhow::bail!(
                    "event {sequence_no} of mission {mission_id} has conflicting schema stamps"
                );
            }
            if doc.stamps.schema_version != SCHEMA_VERSION {
                anyhow::bail!(
                    "event {sequence_no} of mission {mission_id} uses unsupported schema version {} (this engine requires {})",
                    doc.stamps.schema_version,
                    SCHEMA_VERSION,
                );
            }
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

/// Map a `missions`-table insert error: a PK collision (not an effect ID)
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
                // Verified: a reviewed-bar config without a terminal review
                // is a shape production refuses (create_mission + loader).
                stop: StopBar::Verified,
                planning: Default::default(),
                recovery: Default::default(),
                execution: Default::default(),
                terminal_review: None,
            },
        })
    }

    fn decision() -> NewEvent {
        NewEvent::new(MissionEvent::DecisionRecorded {
            attention_id: "x".into(),
            action: DecisionAction::Accept,
            justification: String::new(),
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
