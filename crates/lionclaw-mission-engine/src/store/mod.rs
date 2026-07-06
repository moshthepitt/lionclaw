//! SQLite-backed mission store: append-only event log (source of truth),
//! derived effect ledger, and the content-addressed blob store.
//!
//! One database per target workspace at `<workspace>/.lionclaw/mission.db`.
//! Connection discipline copied from the kernel's proven config: WAL,
//! `synchronous=NORMAL`, 5s busy timeout, foreign keys, own embedded
//! migrator (a fresh file, so no `_sqlx_migrations` interference).

mod blobs;
mod events;

pub use blobs::{BlobStore, BLOB_INLINE_MAX};
pub use events::{AppendError, EffectLease, MissionSummary, NewEvent};

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteSynchronous,
};

#[derive(Clone)]
pub struct MissionStore {
    pool: SqlitePool,
    blobs: BlobStore,
    lionclaw_dir: PathBuf,
}

impl MissionStore {
    /// Open (creating if needed) the mission store for a target workspace.
    pub async fn open(workspace_root: &Path) -> Result<Self> {
        let lionclaw_dir = workspace_root.join(".lionclaw");
        fs::create_dir_all(&lionclaw_dir).with_context(|| {
            format!("failed to create '{}'", lionclaw_dir.display())
        })?;
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
        })
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
