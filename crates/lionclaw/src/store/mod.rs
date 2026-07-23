//! SQLite-backed mission store: append-only event log (source of truth) and
//! the content-addressed blob store.
//!
//! One database per target workspace at `<workspace>/.lionclaw/mission.db`.
//! Connection discipline copied from the kernel's proven config: WAL,
//! `synchronous=NORMAL`, 5s busy timeout, foreign keys, own embedded
//! migrator (a fresh file, so no `_sqlx_migrations` interference).

mod blobs;
mod events;
mod snapshots;

pub use blobs::{BlobReadError, BlobStore, BLOB_INLINE_MAX};
pub use events::{AppendError, NewEvent};

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use rustix::fs::{chmodat, fchmod, mkdirat, open, openat, AtFlags, Mode, OFlags};
use rustix::io::Errno;
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
        let lionclaw_dir = prepare_state_root(workspace_root)?;
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

    /// Mission-state root (`<workspace>/.lionclaw`): durable conversation
    /// resources and disposable effect resources live under here.
    pub fn lionclaw_dir(&self) -> &Path {
        &self.lionclaw_dir
    }

    pub(crate) fn mission_type_dir(&self, mission_id: &MissionId) -> PathBuf {
        self.mission_dir(mission_id).join("mission-type")
    }

    pub(crate) fn mission_skills_dir(&self, mission_id: &MissionId) -> PathBuf {
        self.mission_dir(mission_id).join("skills")
    }

    pub(crate) fn mission_dirs(&self, mission_id: &MissionId) -> crate::resources::MissionDirs {
        crate::resources::MissionDirs::new(&self.lionclaw_dir, mission_id)
    }

    pub(crate) fn mission_dir(&self, mission_id: &MissionId) -> PathBuf {
        self.mission_dirs(mission_id).root().to_path_buf()
    }

    pub(crate) fn driver_lock_path(&self, mission_id: &MissionId) -> PathBuf {
        self.mission_dir(mission_id).join("driver.lock")
    }

    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

fn prepare_state_root(workspace_root: &Path) -> Result<PathBuf> {
    let workspace = open(
        workspace_root,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .with_context(|| {
        format!(
            "workspace root '{}' must be a real directory",
            workspace_root.display()
        )
    })?;
    let created = match mkdirat(&workspace, ".lionclaw", Mode::from_raw_mode(0o700)) {
        Ok(()) => true,
        Err(Errno::EXIST) => false,
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed to create LionClaw state directory '{}'",
                    workspace_root.join(".lionclaw").display()
                )
            })
        }
    };
    if created {
        chmodat(
            &workspace,
            ".lionclaw",
            Mode::from_raw_mode(0o700),
            AtFlags::empty(),
        )
        .with_context(|| {
            format!(
                "failed to protect LionClaw state directory '{}'",
                workspace_root.join(".lionclaw").display()
            )
        })?;
    }
    let state_dir = openat(
        &workspace,
        ".lionclaw",
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .with_context(|| {
        format!(
            "LionClaw state directory '{}' must be a real directory",
            workspace_root.join(".lionclaw").display()
        )
    })?;
    fchmod(&state_dir, Mode::from_raw_mode(0o700)).with_context(|| {
        format!(
            "failed to protect LionClaw state directory '{}'",
            workspace_root.join(".lionclaw").display()
        )
    })?;
    Ok(workspace_root.join(".lionclaw"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{symlink, PermissionsExt};

    #[tokio::test]
    async fn state_root_is_private_and_repairs_existing_permissions() {
        let workspace = tempfile::tempdir().unwrap();
        let state = workspace.path().join(".lionclaw");
        std::fs::create_dir(&state).unwrap();
        std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o755)).unwrap();

        let store = MissionStore::open(workspace.path()).await.unwrap();

        assert_eq!(store.lionclaw_dir(), state);
        assert_eq!(
            std::fs::metadata(store.lionclaw_dir())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
    }

    #[tokio::test]
    async fn state_root_rejects_a_symlink() {
        let workspace = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        symlink(outside.path(), workspace.path().join(".lionclaw")).unwrap();

        let error = match MissionStore::open(workspace.path()).await {
            Ok(_) => panic!("symlinked state root must fail closed"),
            Err(error) => error,
        };

        assert!(format!("{error:#}").contains("must be a real directory"));
        assert!(!outside.path().join("mission.db").exists());
    }
}
