//! Persisted fold snapshots: a discard-and-rebuildable cursor, never a
//! source of truth. Exactly one live snapshot per mission (superseded rows
//! are pruned in the same transaction — the session-compactions pattern),
//! guarded by `upto_sequence_no` monotonicity so a stale writer is a no-op.
//! A snapshot whose `reducer_version` differs from the running engine is
//! ignored and the state is refolded from sequence zero.

use anyhow::Result;

use crate::model::{fold, MissionId, MissionState, REDUCER_VERSION};

use super::MissionStore;

impl MissionStore {
    /// Persist a fold snapshot at `state.head`. Monotonic: an older
    /// `upto_sequence_no` is rejected without effect.
    pub async fn save_snapshot(&self, state: &MissionState, now_ms: i64) -> Result<()> {
        let state_json = serde_json::to_string(state)?;
        let mut tx = self.pool().begin_with("BEGIN IMMEDIATE").await?;
        let current: Option<(i64, i64)> = sqlx::query_as(
            "SELECT upto_sequence_no, reducer_version FROM mission_snapshots WHERE mission_id = ?1",
        )
        .bind(state.mission_id.as_str())
        .fetch_optional(&mut *tx)
        .await?;
        // Monotonic in head, but always replace a stale-reducer snapshot even
        // at equal head — otherwise a terminal/parked mission keeps refolding
        // from zero forever after a REDUCER_VERSION bump.
        if let Some((upto, reducer)) = current {
            if upto as u64 >= state.head && reducer as u32 == REDUCER_VERSION {
                tx.rollback().await?;
                return Ok(());
            }
        }
        sqlx::query(
            "INSERT INTO mission_snapshots
                 (mission_id, upto_sequence_no, reducer_version, state_json, created_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(mission_id) DO UPDATE SET
                 upto_sequence_no = excluded.upto_sequence_no,
                 reducer_version = excluded.reducer_version,
                 state_json = excluded.state_json,
                 created_at_ms = excluded.created_at_ms",
        )
        .bind(state.mission_id.as_str())
        .bind(state.head as i64)
        .bind(REDUCER_VERSION)
        .bind(&state_json)
        .bind(now_ms)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// The live snapshot cursor's `(upto_sequence_no, reducer_version)`, or
    /// `None` if no snapshot has been persisted. Lets callers confirm a
    /// snapshot was actually written rather than silently full-refolding.
    pub async fn snapshot_meta(&self, mission_id: &MissionId) -> Result<Option<(u64, u32)>> {
        let row: Option<(i64, i64)> = sqlx::query_as(
            "SELECT upto_sequence_no, reducer_version FROM mission_snapshots WHERE mission_id = ?1",
        )
        .bind(mission_id.as_str())
        .fetch_optional(self.pool())
        .await?;
        Ok(row.map(|(upto, reducer)| (upto as u64, reducer as u32)))
    }

    /// Load a mission's state, using the snapshot as a starting fold when it
    /// matches the current reducer, then applying the tail. Falls back to a
    /// full refold on a reducer-version mismatch or a missing snapshot.
    pub async fn load_state_snapshotted(
        &self,
        mission_id: &MissionId,
    ) -> Result<Option<MissionState>> {
        let row: Option<(i64, i64, String)> = sqlx::query_as(
            "SELECT upto_sequence_no, reducer_version, state_json
             FROM mission_snapshots WHERE mission_id = ?1",
        )
        .bind(mission_id.as_str())
        .fetch_optional(self.pool())
        .await?;

        let base = match row {
            Some((upto, reducer, json)) if reducer as u32 == REDUCER_VERSION => {
                serde_json::from_str::<MissionState>(&json)
                    .ok()
                    .map(|s| (upto as u64, s))
            }
            _ => None,
        };

        match base {
            Some((upto, mut state)) => {
                for envelope in self.load_after(mission_id, upto).await? {
                    crate::model::apply(&mut state, &envelope);
                }
                Ok(Some(state))
            }
            None => Ok(fold(self.load(mission_id).await?)),
        }
    }

    /// Drop all derived cursors for a mission (snapshot + effect ledger) and
    /// rebuild them from the log alone — the litmus that state is a pure fold
    /// (delete cursors, rebuild, assert equality).
    pub async fn rebuild_cursors(
        &self,
        mission_id: &MissionId,
        now_ms: i64,
    ) -> Result<MissionState> {
        {
            let mut tx = self.pool().begin_with("BEGIN IMMEDIATE").await?;
            sqlx::query("DELETE FROM mission_snapshots WHERE mission_id = ?1")
                .bind(mission_id.as_str())
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM mission_effects WHERE mission_id = ?1")
                .bind(mission_id.as_str())
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
        }
        let state = fold(self.load(mission_id).await?)
            .ok_or_else(|| anyhow::anyhow!("mission {mission_id} has no creation event"))?;
        // Re-derive queued effect rows from the fold's inflight set.
        self.reseed_effects(&state, now_ms).await?;
        self.save_snapshot(&state, now_ms).await?;
        Ok(state)
    }
}
