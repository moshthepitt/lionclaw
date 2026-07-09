//! SQLite-backed mission store: append-only event log (source of truth),
//! derived effect ledger, and the content-addressed blob store.
//!
//! One database per target workspace at `<workspace>/.lionclaw/mission.db`.
//! Connection discipline copied from the kernel's proven config: WAL,
//! `synchronous=NORMAL`, 5s busy timeout, foreign keys, own embedded
//! migrator (a fresh file, so no `_sqlx_migrations` interference).

mod blobs;
mod events;
mod snapshots;

pub use blobs::{BlobStore, BLOB_INLINE_MAX};
pub use events::{AppendError, EffectLease, EffectStatus, MissionSummary, NewEvent};

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteSynchronous,
};

use crate::model::MissionId;
use crate::ports::EventSink;

#[derive(Clone)]
pub struct MissionStore {
    pool: SqlitePool,
    blobs: BlobStore,
    lionclaw_dir: PathBuf,
    /// Fired after each append commits (see [`EventSink`]). Set once at
    /// construction, before any clone, so every clone shares it via the `Arc`.
    sink: Option<Arc<dyn EventSink>>,
}

impl MissionStore {
    /// Open (creating if needed) the mission store for a target workspace.
    pub async fn open(workspace_root: &Path) -> Result<Self> {
        let lionclaw_dir = workspace_root.join(".lionclaw");
        fs::create_dir_all(&lionclaw_dir)
            .with_context(|| format!("failed to create '{}'", lionclaw_dir.display()))?;
        let options = SqliteConnectOptions::new()
            .filename(lionclaw_dir.join("mission.db"))
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await
            .context("failed to open mission.db")?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .context("failed to run mission store migrations")?;
        Ok(Self {
            pool,
            blobs: BlobStore::new(lionclaw_dir.join("blobs")),
            lionclaw_dir,
            sink: None,
        })
    }

    /// Attach a live event sink. Call once at construction (before the store is
    /// cloned into an engine); every clone then shares this sink.
    pub fn with_sink(mut self, sink: Arc<dyn EventSink>) -> Self {
        self.sink = Some(sink);
        self
    }

    /// Fire the sink over a run of just-committed events. Called only after
    /// `tx.commit()`; a no-op when no sink is attached.
    pub(crate) fn publish(
        &self,
        mission_id: &MissionId,
        first_seq: u64,
        events: &[NewEvent],
        now_ms: i64,
    ) {
        let Some(sink) = &self.sink else { return };
        for (offset, event) in events.iter().enumerate() {
            sink.emit(&crate::model::EventEnvelope {
                mission_id: mission_id.clone(),
                sequence_no: first_seq + offset as u64,
                recorded_at_ms: now_ms,
                stamps: event.stamps.clone(),
                event: event.event.clone(),
            });
        }
    }

    pub fn blobs(&self) -> &BlobStore {
        &self.blobs
    }

    /// Mission-state root (`<workspace>/.lionclaw`): attempt dirs and
    /// worktrees live under here.
    pub fn lionclaw_dir(&self) -> &Path {
        &self.lionclaw_dir
    }

    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}
