use std::{
    cmp::Ordering,
    fs,
    path::{Component, Path, PathBuf},
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{
    sqlite::{
        SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
    },
    Row, Sqlite, SqlitePool, Transaction,
};
use uuid::Uuid;

use crate::{
    protocol::{
        PrivateContextProjection, PrivateContextProjectionRequest, PrivateContextRecordRequest,
        ProjectedContextBudget, ProjectedContextClass, ProjectedContextItem,
        ProjectedContextProvenance, ProjectedContextProvenanceSource, TrustTier,
    },
    validation::{
        audit_safe_visible_handle, cap_utf8, profile_slot_is_supported, projection_scopes,
        required_profile_slots, validate_body, validate_limit, validate_profile_slot,
        validate_query, validate_record_id, validate_scope, validate_scope_id, validate_tags,
        validate_text_chars, validate_title, MAX_MEMORY_BODY_BYTES, MAX_PROFILE_BODY_BYTES,
    },
};

const DB_FILE: &str = "lionclaw-private-context.sqlite3";
const SOURCE_KIND_EXPLICIT_OPERATOR: &str = "explicit_operator";
const SOURCE_KIND_EXPLICIT_TRANSCRIPT: &str = "explicit_transcript";
const ACTOR_CONTEXT_CLI: &str = "context_cli";
const ACTOR_RECORDER: &str = "recorder";
const META_SCHEMA_VERSION: &str = "schema_version";
const SCHEMA_VERSION: &str = "2";
const MAX_RECORDED_USER_TEXT_BYTES: usize = 16 * 1024;
const MAX_RECORDED_ASSISTANT_TEXT_BYTES: usize = 32 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct PrivateContextStore {
    pool: SqlitePool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ContextItem {
    pub id: String,
    pub class: ProjectedContextClass,
    pub scope: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slot: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub body: String,
    pub tags: Vec<String>,
    pub priority: i64,
    pub pinned: bool,
    pub created_at: String,
    pub updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<String>,
    pub revision: i64,
    pub source_kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ContextItemRevision {
    pub revision_id: i64,
    pub item_id: String,
    pub revision: i64,
    pub operation: String,
    pub class: ProjectedContextClass,
    pub scope: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slot: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub body: String,
    pub tags: Vec<String>,
    pub priority: i64,
    pub pinned: bool,
    pub created_at: String,
    pub updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<String>,
    pub source_kind: String,
    pub recorded_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct OperationLogEntry {
    pub operation_id: i64,
    pub operation: String,
    pub item_id: String,
    pub class: ProjectedContextClass,
    pub scope: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slot: Option<String>,
    pub revision: i64,
    pub source_kind: String,
    pub actor_surface: String,
    pub recorded_at: String,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct MemoryPatch {
    pub title: Option<Option<String>>,
    pub body: Option<String>,
    pub tags: Option<Vec<String>>,
    pub priority: Option<i64>,
    pub pinned: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RecordedTurn {
    pub private_context_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub sequence_no: u64,
    pub runtime_id: String,
    pub scope: String,
    pub user_text: String,
    pub assistant_text: String,
    pub recorded_at: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WriteSource {
    source_kind: SourceKind,
    actor_surface: ActorSurface,
}

impl WriteSource {
    fn context_cli() -> Self {
        Self {
            source_kind: SourceKind::ExplicitOperator,
            actor_surface: ActorSurface::ContextCli,
        }
    }

    fn recorder() -> Self {
        Self {
            source_kind: SourceKind::ExplicitTranscript,
            actor_surface: ActorSurface::Recorder,
        }
    }

    fn source_kind(self) -> &'static str {
        self.source_kind.as_str()
    }

    fn actor_surface(self) -> &'static str {
        self.actor_surface.as_str()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceKind {
    ExplicitOperator,
    ExplicitTranscript,
}

impl SourceKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::ExplicitOperator => SOURCE_KIND_EXPLICIT_OPERATOR,
            Self::ExplicitTranscript => SOURCE_KIND_EXPLICIT_TRANSCRIPT,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActorSurface {
    ContextCli,
    Recorder,
}

impl ActorSurface {
    fn as_str(self) -> &'static str {
        match self {
            Self::ContextCli => ACTOR_CONTEXT_CLI,
            Self::Recorder => ACTOR_RECORDER,
        }
    }
}

impl MemoryPatch {
    pub(crate) fn is_empty(&self) -> bool {
        self.title.is_none()
            && self.body.is_none()
            && self.tags.is_none()
            && self.priority.is_none()
            && self.pinned.is_none()
    }
}

impl PrivateContextStore {
    pub(crate) async fn open_from_env() -> Result<Self> {
        let state_dir = std::env::var_os("LIONCLAW_SKILL_STATE_DIR")
            .ok_or_else(|| anyhow!("LIONCLAW_SKILL_STATE_DIR is required"))?;
        Self::open(Path::new(&state_dir)).await
    }

    pub(crate) async fn open(state_dir: &Path) -> Result<Self> {
        ensure_state_dir(state_dir)?;
        let db_path = state_dir.join(DB_FILE);
        ensure_state_db_path(&db_path)?;
        let options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5))
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .with_context(|| format!("failed to open {}", db_path.display()))?;
        let store = Self { pool };
        store.migrate().await?;
        ensure_state_db_path(&db_path)?;
        set_private_file_permissions(&db_path)?;
        Ok(store)
    }

    async fn migrate(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY NOT NULL,
                value TEXT NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        self.migrate_source_kind_constraints().await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS context_items (
                id TEXT PRIMARY KEY NOT NULL,
                class TEXT NOT NULL CHECK (class IN ('assistant_profile', 'user_profile', 'memory')),
                scope TEXT NOT NULL,
                slot TEXT,
                title TEXT,
                body TEXT NOT NULL,
                tags TEXT NOT NULL DEFAULT '[]',
                priority INTEGER NOT NULL DEFAULT 0,
                pinned INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                deleted_at TEXT,
                revision INTEGER NOT NULL,
                source_kind TEXT NOT NULL CHECK (source_kind IN ('explicit_operator', 'explicit_transcript')),
                CHECK (
                    (
                        class IN ('assistant_profile', 'user_profile')
                        AND slot IS NOT NULL
                        AND title IS NULL
                        AND tags = '[]'
                        AND priority = 0
                        AND pinned = 0
                    )
                    OR (
                        class = 'memory'
                        AND slot IS NULL
                    )
                )
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS context_items_active_profile_slot_idx
            ON context_items (class, scope, slot)
            WHERE deleted_at IS NULL AND class IN ('assistant_profile', 'user_profile')
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS context_items_class_scope_updated_idx
            ON context_items (class, scope, deleted_at, updated_at, id)
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS context_item_revisions (
                revision_id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id TEXT NOT NULL,
                revision INTEGER NOT NULL,
                operation TEXT NOT NULL,
                class TEXT NOT NULL,
                scope TEXT NOT NULL,
                slot TEXT,
                title TEXT,
                body TEXT NOT NULL,
                tags TEXT NOT NULL DEFAULT '[]',
                priority INTEGER NOT NULL DEFAULT 0,
                pinned INTEGER NOT NULL DEFAULT 0,
                source_kind TEXT NOT NULL CHECK (source_kind IN ('explicit_operator', 'explicit_transcript')),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                deleted_at TEXT,
                recorded_at TEXT NOT NULL,
                UNIQUE(item_id, revision)
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS context_item_revisions_item_idx
            ON context_item_revisions (item_id, revision)
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS operation_log (
                operation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation TEXT NOT NULL,
                item_id TEXT NOT NULL,
                class TEXT NOT NULL,
                scope TEXT NOT NULL,
                slot TEXT,
                revision INTEGER NOT NULL,
                source_kind TEXT NOT NULL CHECK (source_kind IN ('explicit_operator', 'explicit_transcript')),
                actor_surface TEXT NOT NULL,
                recorded_at TEXT NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS operation_log_item_idx
            ON operation_log (item_id, revision)
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE VIRTUAL TABLE IF NOT EXISTS context_item_fts
            USING fts5(item_id UNINDEXED, title, body, tags)
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS recorded_turns (
                private_context_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                turn_id TEXT NOT NULL,
                sequence_no INTEGER NOT NULL,
                runtime_id TEXT NOT NULL,
                scope TEXT NOT NULL,
                user_text TEXT NOT NULL,
                assistant_text TEXT NOT NULL,
                recorded_at TEXT NOT NULL,
                PRIMARY KEY(private_context_id, session_id, turn_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS recorded_turns_scope_recorded_idx
            ON recorded_turns (private_context_id, scope, recorded_at, sequence_no, turn_id)
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE VIRTUAL TABLE IF NOT EXISTS recorded_turns_fts
            USING fts5(
                private_context_id UNINDEXED,
                session_id UNINDEXED,
                turn_id UNINDEXED,
                user_text,
                assistant_text
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO meta (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            "#,
        )
        .bind(META_SCHEMA_VERSION)
        .bind(SCHEMA_VERSION)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn migrate_source_kind_constraints(&self) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        rebuild_source_kind_table_if_needed(
            &mut tx,
            "context_items",
            r#"
            CREATE TABLE context_items (
                id TEXT PRIMARY KEY NOT NULL,
                class TEXT NOT NULL CHECK (class IN ('assistant_profile', 'user_profile', 'memory')),
                scope TEXT NOT NULL,
                slot TEXT,
                title TEXT,
                body TEXT NOT NULL,
                tags TEXT NOT NULL DEFAULT '[]',
                priority INTEGER NOT NULL DEFAULT 0,
                pinned INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                deleted_at TEXT,
                revision INTEGER NOT NULL,
                source_kind TEXT NOT NULL CHECK (source_kind IN ('explicit_operator', 'explicit_transcript')),
                CHECK (
                    (
                        class IN ('assistant_profile', 'user_profile')
                        AND slot IS NOT NULL
                        AND title IS NULL
                        AND tags = '[]'
                        AND priority = 0
                        AND pinned = 0
                    )
                    OR (
                        class = 'memory'
                        AND slot IS NULL
                    )
                )
            )
            "#,
            "id, class, scope, slot, title, body, tags, priority, pinned, created_at, updated_at, deleted_at, revision, source_kind",
        )
        .await?;
        rebuild_source_kind_table_if_needed(
            &mut tx,
            "context_item_revisions",
            r#"
            CREATE TABLE context_item_revisions (
                revision_id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id TEXT NOT NULL,
                revision INTEGER NOT NULL,
                operation TEXT NOT NULL,
                class TEXT NOT NULL,
                scope TEXT NOT NULL,
                slot TEXT,
                title TEXT,
                body TEXT NOT NULL,
                tags TEXT NOT NULL DEFAULT '[]',
                priority INTEGER NOT NULL DEFAULT 0,
                pinned INTEGER NOT NULL DEFAULT 0,
                source_kind TEXT NOT NULL CHECK (source_kind IN ('explicit_operator', 'explicit_transcript')),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                deleted_at TEXT,
                recorded_at TEXT NOT NULL,
                UNIQUE(item_id, revision)
            )
            "#,
            "revision_id, item_id, revision, operation, class, scope, slot, title, body, tags, priority, pinned, source_kind, created_at, updated_at, deleted_at, recorded_at",
        )
        .await?;
        rebuild_source_kind_table_if_needed(
            &mut tx,
            "operation_log",
            r#"
            CREATE TABLE operation_log (
                operation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation TEXT NOT NULL,
                item_id TEXT NOT NULL,
                class TEXT NOT NULL,
                scope TEXT NOT NULL,
                slot TEXT,
                revision INTEGER NOT NULL,
                source_kind TEXT NOT NULL CHECK (source_kind IN ('explicit_operator', 'explicit_transcript')),
                actor_surface TEXT NOT NULL,
                recorded_at TEXT NOT NULL
            )
            "#,
            "operation_id, operation, item_id, class, scope, slot, revision, source_kind, actor_surface, recorded_at",
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn set_profile(
        &self,
        class: ProjectedContextClass,
        scope: &str,
        slot: &str,
        body: &str,
    ) -> Result<ContextItem> {
        let now = now_timestamp();
        let mut tx = self.pool.begin().await?;
        let item = set_profile_in_tx(
            &mut tx,
            class,
            scope,
            slot,
            body,
            WriteSource::context_cli(),
            &now,
        )
        .await?;
        tx.commit().await?;
        Ok(item)
    }

    pub(crate) async fn list_profiles(
        &self,
        class: ProjectedContextClass,
        scope: Option<&str>,
    ) -> Result<Vec<ContextItem>> {
        validate_profile_projection_class(class)?;
        let rows = if let Some(scope) = scope {
            let scope = validate_scope(scope)?;
            sqlx::query(
                r#"
                SELECT *
                FROM context_items
                WHERE class = ? AND scope = ? AND deleted_at IS NULL
                ORDER BY slot ASC, id ASC
                "#,
            )
            .bind(class.as_str())
            .bind(scope)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                r#"
                SELECT *
                FROM context_items
                WHERE class = ? AND deleted_at IS NULL
                ORDER BY scope ASC, slot ASC, id ASC
                "#,
            )
            .bind(class.as_str())
            .fetch_all(&self.pool)
            .await?
        };
        let mut items = rows_to_items(rows)?;
        items.retain(|item| supported_profile_item_slot(class, item));
        sort_profiles(class, &mut items, &projection_scopes(None));
        Ok(items)
    }

    pub(crate) async fn show_profile(
        &self,
        class: ProjectedContextClass,
        scope: &str,
        slot: &str,
    ) -> Result<Option<ContextItem>> {
        let slot = validate_profile_slot(class, slot)?;
        let scope = validate_scope(scope)?;
        sqlx::query(
            r#"
            SELECT *
            FROM context_items
            WHERE class = ? AND scope = ? AND slot = ? AND deleted_at IS NULL
            "#,
        )
        .bind(class.as_str())
        .bind(scope)
        .bind(slot)
        .fetch_optional(&self.pool)
        .await?
        .map(row_to_item)
        .transpose()
    }

    pub(crate) async fn delete_profile(
        &self,
        class: ProjectedContextClass,
        scope: &str,
        slot: &str,
    ) -> Result<Option<ContextItem>> {
        let slot = validate_profile_slot(class, slot)?;
        let scope = validate_scope(scope)?;
        let now = now_timestamp();
        let mut tx = self.pool.begin().await?;
        let existing = active_profile_row(&mut tx, class, &scope, &slot).await?;
        let Some(existing) = existing else {
            tx.commit().await?;
            return Ok(None);
        };
        let mut item = row_to_item(existing)?;
        item.updated_at = now.clone();
        item.deleted_at = Some(now.clone());
        item.revision = item.revision.saturating_add(1);
        item.source_kind = WriteSource::context_cli().source_kind().to_string();
        sqlx::query(
            r#"
            UPDATE context_items
            SET updated_at = ?, deleted_at = ?, revision = ?, source_kind = ?
            WHERE id = ?
            "#,
        )
        .bind(&item.updated_at)
        .bind(&item.deleted_at)
        .bind(item.revision)
        .bind(&item.source_kind)
        .bind(&item.id)
        .execute(&mut *tx)
        .await?;
        record_revision(&mut tx, &item, "profile_delete", &now).await?;
        record_operation(
            &mut tx,
            &item,
            "profile_delete",
            &now,
            WriteSource::context_cli(),
        )
        .await?;
        tx.commit().await?;
        Ok(Some(item))
    }

    pub(crate) async fn profile_history(
        &self,
        class: ProjectedContextClass,
        scope: &str,
        slot: &str,
    ) -> Result<Vec<ContextItemRevision>> {
        let slot = validate_profile_slot(class, slot)?;
        let scope = validate_scope(scope)?;
        let rows = sqlx::query(
            r#"
            SELECT *
            FROM context_item_revisions
            WHERE class = ? AND scope = ? AND slot = ?
            ORDER BY revision ASC, revision_id ASC
            "#,
        )
        .bind(class.as_str())
        .bind(scope)
        .bind(slot)
        .fetch_all(&self.pool)
        .await?;
        rows_to_revisions(rows)
    }

    pub(crate) async fn remember_memory(
        &self,
        scope: &str,
        title: Option<&str>,
        body: &str,
        tags: &[String],
        priority: i64,
        pinned: bool,
    ) -> Result<ContextItem> {
        let now = now_timestamp();
        let mut tx = self.pool.begin().await?;
        let item = remember_memory_in_tx(
            &mut tx,
            scope,
            title,
            body,
            tags,
            priority,
            pinned,
            WriteSource::context_cli(),
            &now,
        )
        .await?;
        tx.commit().await?;
        Ok(item)
    }

    pub(crate) async fn list_memory(
        &self,
        scope: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<ContextItem>> {
        let limit = i64::try_from(validate_limit(limit)?)
            .context("validated list limit did not fit i64")?;
        let rows = if let Some(scope) = scope {
            let scope = validate_scope(scope)?;
            sqlx::query(
                r#"
                SELECT *
                FROM context_items
                WHERE class = 'memory' AND scope = ? AND deleted_at IS NULL
                ORDER BY updated_at DESC, id ASC
                LIMIT ?
                "#,
            )
            .bind(scope)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                r#"
                SELECT *
                FROM context_items
                WHERE class = 'memory' AND deleted_at IS NULL
                ORDER BY updated_at DESC, id ASC
                LIMIT ?
                "#,
            )
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        };
        rows_to_items(rows)
    }

    pub(crate) async fn search_memory(
        &self,
        query: &str,
        scope: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<ContextItem>> {
        let query = validate_query(query)?;
        let limit = i64::try_from(validate_limit(limit)?)
            .context("validated list limit did not fit i64")?;
        let Some(fts_query) = fts_query_from_text(&query) else {
            return Ok(Vec::new());
        };
        let rows = if let Some(scope) = scope {
            let scope = validate_scope(scope)?;
            sqlx::query(
                r#"
                SELECT ci.*
                FROM context_item_fts
                JOIN context_items ci ON ci.id = context_item_fts.item_id
                WHERE context_item_fts MATCH ?
                    AND ci.class = 'memory'
                    AND ci.scope = ?
                    AND ci.deleted_at IS NULL
                ORDER BY ci.pinned DESC, ci.priority DESC, bm25(context_item_fts) ASC, ci.updated_at DESC, ci.id ASC
                LIMIT ?
                "#,
            )
            .bind(fts_query)
            .bind(scope)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                r#"
                SELECT ci.*
                FROM context_item_fts
                JOIN context_items ci ON ci.id = context_item_fts.item_id
                WHERE context_item_fts MATCH ?
                    AND ci.class = 'memory'
                    AND ci.deleted_at IS NULL
                ORDER BY ci.pinned DESC, ci.priority DESC, bm25(context_item_fts) ASC, ci.updated_at DESC, ci.id ASC
                LIMIT ?
                "#,
            )
            .bind(fts_query)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        };
        rows_to_items(rows)
    }

    pub(crate) async fn show_memory(&self, id: &str) -> Result<Option<ContextItem>> {
        let id = validate_record_id(id)?;
        sqlx::query(
            r#"
            SELECT *
            FROM context_items
            WHERE id = ? AND class = 'memory' AND deleted_at IS NULL
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        .map(row_to_item)
        .transpose()
    }

    pub(crate) async fn update_memory(
        &self,
        id: &str,
        patch: MemoryPatch,
    ) -> Result<Option<ContextItem>> {
        if patch.is_empty() {
            bail!("memory update requires at least one field change");
        }
        let id = validate_record_id(id)?;
        let title = patch
            .title
            .as_ref()
            .map(|value| value.as_deref())
            .map(validate_title)
            .transpose()?;
        let body = patch
            .body
            .as_deref()
            .map(|body| validate_body("memory", body, MAX_MEMORY_BODY_BYTES))
            .transpose()?;
        let tags = patch
            .tags
            .as_ref()
            .map(|tags| validate_tags(tags))
            .transpose()?;
        let now = now_timestamp();
        let mut tx = self.pool.begin().await?;
        let Some(row) = active_memory_row(&mut tx, &id).await? else {
            tx.commit().await?;
            return Ok(None);
        };
        let mut item = row_to_item(row)?;
        if let Some(title) = title {
            item.title = title;
        }
        if let Some(body) = body {
            item.body = body;
        }
        if let Some(tags) = tags {
            item.tags = tags;
        }
        if let Some(priority) = patch.priority {
            item.priority = priority;
        }
        if let Some(pinned) = patch.pinned {
            item.pinned = pinned;
        }
        item.updated_at = now.clone();
        item.revision = item.revision.saturating_add(1);
        item.source_kind = WriteSource::context_cli().source_kind().to_string();
        update_item(&mut tx, &item).await?;
        refresh_fts(&mut tx, &item).await?;
        record_revision(&mut tx, &item, "memory_update", &now).await?;
        record_operation(
            &mut tx,
            &item,
            "memory_update",
            &now,
            WriteSource::context_cli(),
        )
        .await?;
        tx.commit().await?;
        Ok(Some(item))
    }

    pub(crate) async fn delete_memory(&self, id: &str) -> Result<Option<ContextItem>> {
        let id = validate_record_id(id)?;
        let now = now_timestamp();
        let mut tx = self.pool.begin().await?;
        let Some(row) = active_memory_row(&mut tx, &id).await? else {
            tx.commit().await?;
            return Ok(None);
        };
        let mut item = row_to_item(row)?;
        item.updated_at = now.clone();
        item.deleted_at = Some(now.clone());
        item.revision = item.revision.saturating_add(1);
        item.source_kind = WriteSource::context_cli().source_kind().to_string();
        update_item(&mut tx, &item).await?;
        refresh_fts(&mut tx, &item).await?;
        record_revision(&mut tx, &item, "memory_delete", &now).await?;
        record_operation(
            &mut tx,
            &item,
            "memory_delete",
            &now,
            WriteSource::context_cli(),
        )
        .await?;
        tx.commit().await?;
        Ok(Some(item))
    }

    pub(crate) async fn memory_history(&self, id: &str) -> Result<Vec<ContextItemRevision>> {
        let id = validate_record_id(id)?;
        let rows = sqlx::query(
            r#"
            SELECT *
            FROM context_item_revisions
            WHERE item_id = ? AND class = 'memory'
            ORDER BY revision ASC, revision_id ASC
            "#,
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await?;
        rows_to_revisions(rows)
    }

    pub(crate) async fn operation_log(
        &self,
        item_id: Option<&str>,
    ) -> Result<Vec<OperationLogEntry>> {
        let rows = if let Some(item_id) = item_id {
            let item_id = validate_record_id(item_id)?;
            sqlx::query(
                r#"
                SELECT *
                FROM operation_log
                WHERE item_id = ?
                ORDER BY operation_id ASC
                "#,
            )
            .bind(item_id)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                r#"
                SELECT *
                FROM operation_log
                ORDER BY operation_id ASC
                "#,
            )
            .fetch_all(&self.pool)
            .await?
        };
        rows_to_operations(rows)
    }

    pub(crate) async fn record_turn(
        &self,
        private_context_id: &str,
        request: &PrivateContextRecordRequest,
    ) -> Result<bool> {
        let private_context_id = validate_private_context_id(private_context_id)?;
        if request.transcript.is_empty() {
            return Ok(false);
        }
        let turn = recorded_turn_from_request(&private_context_id, request)?;
        let directives = explicit_directives(&turn.user_text);
        let now = turn.recorded_at.clone();
        let mut tx = self.pool.begin().await?;
        if recorded_turn_exists(&mut tx, &turn).await? {
            tx.commit().await?;
            return Ok(false);
        }
        insert_recorded_turn(&mut tx, &turn).await?;
        refresh_recorded_turn_fts(&mut tx, &turn).await?;
        for directive in directives {
            match directive {
                ExplicitDirective::Memory(body) => {
                    remember_memory_in_tx(
                        &mut tx,
                        &turn.scope,
                        None,
                        &body,
                        &[],
                        0,
                        false,
                        WriteSource::recorder(),
                        &now,
                    )
                    .await?;
                }
                ExplicitDirective::Profile { class, slot, body } => {
                    set_profile_in_tx(
                        &mut tx,
                        class,
                        &turn.scope,
                        slot,
                        &body,
                        WriteSource::recorder(),
                        &now,
                    )
                    .await?;
                }
            }
        }
        tx.commit().await?;
        Ok(true)
    }

    pub(crate) async fn project(
        &self,
        projector_id: &str,
        request: &PrivateContextProjectionRequest,
    ) -> Result<PrivateContextProjection> {
        if request.trust_tier == TrustTier::Untrusted {
            return Ok(PrivateContextProjection {
                request_id: request.request_id,
                projector_id: projector_id.to_string(),
                items: Vec::new(),
            });
        }

        let mut items = Vec::new();
        for class in ProjectedContextClass::ALL {
            let Some(budget) = budget_for_class(request, class) else {
                continue;
            };
            let class_items = match class {
                ProjectedContextClass::AssistantProfile | ProjectedContextClass::UserProfile => {
                    self.project_profile_items(
                        projector_id,
                        request.project_scope.as_deref(),
                        budget,
                    )
                    .await?
                }
                ProjectedContextClass::Memory => {
                    self.project_memory_items(projector_id, request, budget)
                        .await?
                }
            };
            items.extend(class_items);
        }

        Ok(PrivateContextProjection {
            request_id: request.request_id,
            projector_id: projector_id.to_string(),
            items,
        })
    }

    async fn project_profile_items(
        &self,
        projector_id: &str,
        project_scope: Option<&str>,
        budget: &ProjectedContextBudget,
    ) -> Result<Vec<ProjectedContextItem>> {
        let scopes = projection_scopes(project_scope);
        let rows = query_items_for_scopes(&self.pool, budget.class, &scopes).await?;
        let mut items = rows_to_items(rows)?;
        items.retain(|item| supported_profile_item_slot(budget.class, item));
        sort_profiles(budget.class, &mut items, &scopes);
        Ok(project_items(
            projector_id,
            budget,
            items,
            profile_projection_text,
        ))
    }

    async fn project_memory_items(
        &self,
        projector_id: &str,
        request: &PrivateContextProjectionRequest,
        budget: &ProjectedContextBudget,
    ) -> Result<Vec<ProjectedContextItem>> {
        let Some(current_input) = request.current_input.as_deref() else {
            return Ok(Vec::new());
        };
        let Some(fts_query) = fts_query_from_text(current_input) else {
            return Ok(Vec::new());
        };
        let mut scopes = projection_scopes(request.project_scope.as_deref());
        scopes.reverse();
        let limit = budget.max_items.saturating_mul(4).max(1);
        let explicit =
            query_explicit_memory_projection(&self.pool, &fts_query, &scopes, limit).await?;
        let recorded =
            query_recorded_turn_projection(&self.pool, projector_id, &fts_query, &scopes, limit)
                .await?;
        let mut matches = memory_projection_matches(explicit, recorded);
        sort_memory_projection_matches(&mut matches, &scopes);
        Ok(project_memory_matches(projector_id, budget, matches))
    }
}

#[derive(Debug, Clone)]
struct ExplicitMemoryMatch {
    item: ContextItem,
    fts_rank: f64,
}

#[derive(Debug, Clone)]
struct RecordedTurnMatch {
    turn: RecordedTurn,
    fts_rank: f64,
}

#[derive(Debug, Clone)]
enum MemoryProjectionMatch {
    Explicit(ExplicitMemoryMatch),
    Recorded(RecordedTurnMatch),
}

async fn active_profile_row(
    tx: &mut Transaction<'_, Sqlite>,
    class: ProjectedContextClass,
    scope: &str,
    slot: &str,
) -> Result<Option<SqliteRow>> {
    sqlx::query(
        r#"
        SELECT *
        FROM context_items
        WHERE class = ? AND scope = ? AND slot = ? AND deleted_at IS NULL
        "#,
    )
    .bind(class.as_str())
    .bind(scope)
    .bind(slot)
    .fetch_optional(&mut **tx)
    .await
    .map_err(Into::into)
}

async fn active_memory_row(
    tx: &mut Transaction<'_, Sqlite>,
    id: &str,
) -> Result<Option<SqliteRow>> {
    sqlx::query(
        r#"
        SELECT *
        FROM context_items
        WHERE id = ? AND class = 'memory' AND deleted_at IS NULL
        "#,
    )
    .bind(id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(Into::into)
}

async fn set_profile_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    class: ProjectedContextClass,
    scope: &str,
    slot: &str,
    body: &str,
    source: WriteSource,
    now: &str,
) -> Result<ContextItem> {
    let slot = validate_profile_slot(class, slot)?;
    let scope = validate_scope(scope)?;
    let body = validate_body("profile", body, MAX_PROFILE_BODY_BYTES)?;
    let existing = active_profile_row(tx, class, &scope, &slot).await?;
    let item = if let Some(existing) = existing {
        let mut item = row_to_item(existing)?;
        item.body = body;
        item.updated_at = now.to_string();
        item.revision = item.revision.saturating_add(1);
        item.source_kind = source.source_kind().to_string();
        sqlx::query(
            r#"
            UPDATE context_items
            SET body = ?, updated_at = ?, revision = ?, source_kind = ?
            WHERE id = ?
            "#,
        )
        .bind(&item.body)
        .bind(&item.updated_at)
        .bind(item.revision)
        .bind(&item.source_kind)
        .bind(&item.id)
        .execute(&mut **tx)
        .await?;
        item
    } else {
        let item = ContextItem {
            id: new_record_id(),
            class,
            scope,
            slot: Some(slot),
            title: None,
            body,
            tags: Vec::new(),
            priority: 0,
            pinned: false,
            created_at: now.to_string(),
            updated_at: now.to_string(),
            deleted_at: None,
            revision: 1,
            source_kind: source.source_kind().to_string(),
        };
        insert_item(tx, &item).await?;
        item
    };
    record_revision(tx, &item, "profile_set", now).await?;
    record_operation(tx, &item, "profile_set", now, source).await?;
    Ok(item)
}

async fn remember_memory_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    scope: &str,
    title: Option<&str>,
    body: &str,
    tags: &[String],
    priority: i64,
    pinned: bool,
    source: WriteSource,
    now: &str,
) -> Result<ContextItem> {
    let scope = validate_scope(scope)?;
    let title = validate_title(title)?;
    let body = validate_body("memory", body, MAX_MEMORY_BODY_BYTES)?;
    let tags = validate_tags(tags)?;
    let item = ContextItem {
        id: new_record_id(),
        class: ProjectedContextClass::Memory,
        scope,
        slot: None,
        title,
        body,
        tags,
        priority,
        pinned,
        created_at: now.to_string(),
        updated_at: now.to_string(),
        deleted_at: None,
        revision: 1,
        source_kind: source.source_kind().to_string(),
    };
    insert_item(tx, &item).await?;
    refresh_fts(tx, &item).await?;
    record_revision(tx, &item, "memory_remember", now).await?;
    record_operation(tx, &item, "memory_remember", now, source).await?;
    Ok(item)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExplicitDirective {
    Memory(String),
    Profile {
        class: ProjectedContextClass,
        slot: &'static str,
        body: String,
    },
}

fn explicit_directives(user_text: &str) -> Vec<ExplicitDirective> {
    user_text.lines().filter_map(explicit_directive).collect()
}

fn explicit_directive(raw_line: &str) -> Option<ExplicitDirective> {
    let mut line = raw_line.trim();
    line = line
        .strip_prefix("- ")
        .or_else(|| line.strip_prefix("* "))
        .unwrap_or(line)
        .trim_start();
    if let Some(body) = directive_body(line, "remember:") {
        return Some(ExplicitDirective::Memory(body));
    }
    if let Some(body) = directive_body(line, "style:") {
        return Some(ExplicitDirective::Profile {
            class: ProjectedContextClass::AssistantProfile,
            slot: "style",
            body,
        });
    }
    if let Some(body) = directive_body(line, "preferences:") {
        return Some(ExplicitDirective::Profile {
            class: ProjectedContextClass::UserProfile,
            slot: "preferences",
            body,
        });
    }
    None
}

fn directive_body(line: &str, prefix: &str) -> Option<String> {
    let head = line.get(..prefix.len())?;
    if !head.eq_ignore_ascii_case(prefix) {
        return None;
    }
    let body = line.get(prefix.len()..)?.trim();
    (!body.is_empty()).then(|| body.to_string())
}

fn recorded_turn_from_request(
    private_context_id: &str,
    request: &PrivateContextRecordRequest,
) -> Result<RecordedTurn> {
    let scope = record_scope(request.project_scope.as_deref())?;
    let runtime_id = validate_runtime_id(&request.runtime_id)?;
    let user_text = validate_recorded_text(
        "recorded user text",
        request.transcript.user_text(),
        MAX_RECORDED_USER_TEXT_BYTES,
    )?;
    let assistant_text = validate_recorded_text(
        "recorded assistant text",
        request.transcript.assistant_text(),
        MAX_RECORDED_ASSISTANT_TEXT_BYTES,
    )?;
    Ok(RecordedTurn {
        private_context_id: private_context_id.to_string(),
        session_id: request.session_id.to_string(),
        turn_id: request.turn_id.to_string(),
        sequence_no: request.sequence_no,
        runtime_id,
        scope,
        user_text,
        assistant_text,
        recorded_at: now_timestamp(),
    })
}

fn record_scope(project_scope: Option<&str>) -> Result<String> {
    match project_scope {
        Some(scope) => {
            let scope = scope.trim();
            validate_scope_id(scope)?;
            Ok(format!("project:{scope}"))
        }
        None => Ok("global".to_string()),
    }
}

fn validate_private_context_id(private_context_id: &str) -> Result<String> {
    if !audit_safe_visible_handle(private_context_id) {
        bail!(
            "private context id must be 1..128 visible ASCII bytes with no whitespace or path separators"
        );
    }
    Ok(private_context_id.to_string())
}

fn validate_runtime_id(runtime_id: &str) -> Result<String> {
    if !audit_safe_visible_handle(runtime_id) {
        bail!(
            "runtime id must be 1..128 visible ASCII bytes with no whitespace or path separators"
        );
    }
    Ok(runtime_id.to_string())
}

fn validate_recorded_text(label: &str, text: &str, max_bytes: usize) -> Result<String> {
    validate_text_chars(label, text)?;
    Ok(cap_utf8_preserving(text, max_bytes))
}

fn cap_utf8_preserving(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_string();
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_string()
}

async fn recorded_turn_exists(
    tx: &mut Transaction<'_, Sqlite>,
    turn: &RecordedTurn,
) -> Result<bool> {
    let exists = sqlx::query(
        r#"
        SELECT 1
        FROM recorded_turns
        WHERE private_context_id = ? AND session_id = ? AND turn_id = ?
        "#,
    )
    .bind(&turn.private_context_id)
    .bind(&turn.session_id)
    .bind(&turn.turn_id)
    .fetch_optional(&mut **tx)
    .await?
    .is_some();
    Ok(exists)
}

async fn insert_recorded_turn(tx: &mut Transaction<'_, Sqlite>, turn: &RecordedTurn) -> Result<()> {
    let sequence_no =
        i64::try_from(turn.sequence_no).context("recorded turn sequence number exceeds i64")?;
    sqlx::query(
        r#"
        INSERT INTO recorded_turns (
            private_context_id, session_id, turn_id, sequence_no, runtime_id,
            scope, user_text, assistant_text, recorded_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(&turn.private_context_id)
    .bind(&turn.session_id)
    .bind(&turn.turn_id)
    .bind(sequence_no)
    .bind(&turn.runtime_id)
    .bind(&turn.scope)
    .bind(&turn.user_text)
    .bind(&turn.assistant_text)
    .bind(&turn.recorded_at)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn refresh_recorded_turn_fts(
    tx: &mut Transaction<'_, Sqlite>,
    turn: &RecordedTurn,
) -> Result<()> {
    sqlx::query(
        r#"
        DELETE FROM recorded_turns_fts
        WHERE private_context_id = ? AND session_id = ? AND turn_id = ?
        "#,
    )
    .bind(&turn.private_context_id)
    .bind(&turn.session_id)
    .bind(&turn.turn_id)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO recorded_turns_fts (
            private_context_id, session_id, turn_id, user_text, assistant_text
        )
        VALUES (?, ?, ?, ?, ?)
        "#,
    )
    .bind(&turn.private_context_id)
    .bind(&turn.session_id)
    .bind(&turn.turn_id)
    .bind(&turn.user_text)
    .bind(&turn.assistant_text)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_item(tx: &mut Transaction<'_, Sqlite>, item: &ContextItem) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO context_items (
            id, class, scope, slot, title, body, tags, priority, pinned,
            created_at, updated_at, deleted_at, revision, source_kind
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(&item.id)
    .bind(item.class.as_str())
    .bind(&item.scope)
    .bind(&item.slot)
    .bind(&item.title)
    .bind(&item.body)
    .bind(tags_json(&item.tags)?)
    .bind(item.priority)
    .bind(bool_to_i64(item.pinned))
    .bind(&item.created_at)
    .bind(&item.updated_at)
    .bind(&item.deleted_at)
    .bind(item.revision)
    .bind(&item.source_kind)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn update_item(tx: &mut Transaction<'_, Sqlite>, item: &ContextItem) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE context_items
        SET title = ?,
            body = ?,
            tags = ?,
            priority = ?,
            pinned = ?,
            updated_at = ?,
            deleted_at = ?,
            revision = ?,
            source_kind = ?
        WHERE id = ?
        "#,
    )
    .bind(&item.title)
    .bind(&item.body)
    .bind(tags_json(&item.tags)?)
    .bind(item.priority)
    .bind(bool_to_i64(item.pinned))
    .bind(&item.updated_at)
    .bind(&item.deleted_at)
    .bind(item.revision)
    .bind(&item.source_kind)
    .bind(&item.id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn record_revision(
    tx: &mut Transaction<'_, Sqlite>,
    item: &ContextItem,
    operation: &str,
    recorded_at: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO context_item_revisions (
            item_id, revision, operation, class, scope, slot, title, body, tags,
            priority, pinned, source_kind, created_at, updated_at, deleted_at, recorded_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(&item.id)
    .bind(item.revision)
    .bind(operation)
    .bind(item.class.as_str())
    .bind(&item.scope)
    .bind(&item.slot)
    .bind(&item.title)
    .bind(&item.body)
    .bind(tags_json(&item.tags)?)
    .bind(item.priority)
    .bind(bool_to_i64(item.pinned))
    .bind(&item.source_kind)
    .bind(&item.created_at)
    .bind(&item.updated_at)
    .bind(&item.deleted_at)
    .bind(recorded_at)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn record_operation(
    tx: &mut Transaction<'_, Sqlite>,
    item: &ContextItem,
    operation: &str,
    recorded_at: &str,
    source: WriteSource,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO operation_log (
            operation, item_id, class, scope, slot, revision,
            source_kind, actor_surface, recorded_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(operation)
    .bind(&item.id)
    .bind(item.class.as_str())
    .bind(&item.scope)
    .bind(&item.slot)
    .bind(item.revision)
    .bind(&item.source_kind)
    .bind(source.actor_surface())
    .bind(recorded_at)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn refresh_fts(tx: &mut Transaction<'_, Sqlite>, item: &ContextItem) -> Result<()> {
    sqlx::query("DELETE FROM context_item_fts WHERE item_id = ?")
        .bind(&item.id)
        .execute(&mut **tx)
        .await?;
    if item.class == ProjectedContextClass::Memory && item.deleted_at.is_none() {
        sqlx::query(
            r#"
            INSERT INTO context_item_fts (item_id, title, body, tags)
            VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(&item.id)
        .bind(item.title.as_deref().unwrap_or(""))
        .bind(&item.body)
        .bind(item.tags.join(" "))
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

async fn query_items_for_scopes(
    pool: &SqlitePool,
    class: ProjectedContextClass,
    scopes: &[String],
) -> Result<Vec<SqliteRow>> {
    validate_profile_projection_class(class)?;
    let first = required_scope(scopes, 0)?;
    if let Some(second) = scopes.get(1) {
        sqlx::query(
            r#"
            SELECT *
            FROM context_items
            WHERE class = ? AND deleted_at IS NULL AND (scope = ? OR scope = ?)
            "#,
        )
        .bind(class.as_str())
        .bind(first)
        .bind(second)
        .fetch_all(pool)
        .await
        .map_err(Into::into)
    } else {
        sqlx::query(
            r#"
            SELECT *
            FROM context_items
            WHERE class = ? AND deleted_at IS NULL AND scope = ?
            "#,
        )
        .bind(class.as_str())
        .bind(first)
        .fetch_all(pool)
        .await
        .map_err(Into::into)
    }
}

async fn query_explicit_memory_projection(
    pool: &SqlitePool,
    fts_query: &str,
    scopes: &[String],
    max_items: usize,
) -> Result<Vec<ExplicitMemoryMatch>> {
    let limit = i64::try_from(max_items.max(1)).unwrap_or(i64::MAX);
    let first = required_scope(scopes, 0)?;
    let rows: Vec<SqliteRow> = if let Some(second) = scopes.get(1) {
        sqlx::query(
            r#"
            SELECT ci.*, bm25(context_item_fts) AS fts_rank
            FROM context_item_fts
            JOIN context_items ci ON ci.id = context_item_fts.item_id
            WHERE context_item_fts MATCH ?
                AND ci.class = 'memory'
                AND ci.deleted_at IS NULL
                AND (ci.scope = ? OR ci.scope = ?)
            ORDER BY CASE ci.scope WHEN ? THEN 0 ELSE 1 END,
                ci.pinned DESC,
                ci.priority DESC,
                bm25(context_item_fts) ASC,
                ci.updated_at DESC,
                ci.id ASC
            LIMIT ?
            "#,
        )
        .bind(fts_query)
        .bind(first)
        .bind(second)
        .bind(first)
        .bind(limit)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query(
            r#"
            SELECT ci.*, bm25(context_item_fts) AS fts_rank
            FROM context_item_fts
            JOIN context_items ci ON ci.id = context_item_fts.item_id
            WHERE context_item_fts MATCH ?
                AND ci.class = 'memory'
                AND ci.deleted_at IS NULL
                AND ci.scope = ?
            ORDER BY ci.pinned DESC,
                ci.priority DESC,
                bm25(context_item_fts) ASC,
                ci.updated_at DESC,
                ci.id ASC
            LIMIT ?
            "#,
        )
        .bind(fts_query)
        .bind(first)
        .bind(limit)
        .fetch_all(pool)
        .await?
    };
    rows.into_iter().map(row_to_explicit_memory_match).collect()
}

async fn query_recorded_turn_projection(
    pool: &SqlitePool,
    private_context_id: &str,
    fts_query: &str,
    scopes: &[String],
    max_items: usize,
) -> Result<Vec<RecordedTurnMatch>> {
    let private_context_id = validate_private_context_id(private_context_id)?;
    let limit = i64::try_from(max_items.max(1)).unwrap_or(i64::MAX);
    let first = required_scope(scopes, 0)?;
    let rows: Vec<SqliteRow> = if let Some(second) = scopes.get(1) {
        sqlx::query(
            r#"
            SELECT rt.*, bm25(recorded_turns_fts) AS fts_rank
            FROM recorded_turns_fts
            JOIN recorded_turns rt
                ON rt.private_context_id = recorded_turns_fts.private_context_id
                AND rt.session_id = recorded_turns_fts.session_id
                AND rt.turn_id = recorded_turns_fts.turn_id
            WHERE recorded_turns_fts MATCH ?
                AND rt.private_context_id = ?
                AND (rt.scope = ? OR rt.scope = ?)
            ORDER BY CASE rt.scope WHEN ? THEN 0 ELSE 1 END,
                bm25(recorded_turns_fts) ASC,
                rt.recorded_at DESC,
                rt.session_id ASC,
                rt.turn_id ASC
            LIMIT ?
            "#,
        )
        .bind(fts_query)
        .bind(&private_context_id)
        .bind(first)
        .bind(second)
        .bind(first)
        .bind(limit)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query(
            r#"
            SELECT rt.*, bm25(recorded_turns_fts) AS fts_rank
            FROM recorded_turns_fts
            JOIN recorded_turns rt
                ON rt.private_context_id = recorded_turns_fts.private_context_id
                AND rt.session_id = recorded_turns_fts.session_id
                AND rt.turn_id = recorded_turns_fts.turn_id
            WHERE recorded_turns_fts MATCH ?
                AND rt.private_context_id = ?
                AND rt.scope = ?
            ORDER BY bm25(recorded_turns_fts) ASC,
                rt.recorded_at DESC,
                rt.session_id ASC,
                rt.turn_id ASC
            LIMIT ?
            "#,
        )
        .bind(fts_query)
        .bind(&private_context_id)
        .bind(first)
        .bind(limit)
        .fetch_all(pool)
        .await?
    };
    rows.into_iter().map(row_to_recorded_turn_match).collect()
}

fn required_scope(scopes: &[String], index: usize) -> Result<&str> {
    scopes
        .get(index)
        .map(String::as_str)
        .ok_or_else(|| anyhow!("projection scope list was unexpectedly empty"))
}

async fn rebuild_source_kind_table_if_needed(
    tx: &mut Transaction<'_, Sqlite>,
    table: &str,
    create_sql: &str,
    columns: &str,
) -> Result<()> {
    let Some(sql) = table_create_sql(tx, table).await? else {
        return Ok(());
    };
    if sql.contains("source_kind IN ('explicit_operator', 'explicit_transcript')") {
        return Ok(());
    }
    if !sql.contains("source_kind = 'explicit_operator'") {
        bail!("unsupported {table} source_kind constraint");
    }
    let replacement = format!("{table}__source_kind_migration");
    sqlx::query(&format!("ALTER TABLE {table} RENAME TO {replacement}"))
        .execute(&mut **tx)
        .await?;
    drop_rebuilt_table_indexes(tx, table).await?;
    sqlx::query(create_sql).execute(&mut **tx).await?;
    sqlx::query(&format!(
        "INSERT INTO {table} ({columns}) SELECT {columns} FROM {replacement}"
    ))
    .execute(&mut **tx)
    .await?;
    sqlx::query(&format!("DROP TABLE {replacement}"))
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn drop_rebuilt_table_indexes(tx: &mut Transaction<'_, Sqlite>, table: &str) -> Result<()> {
    let indexes: &[&str] = match table {
        "context_items" => &[
            "context_items_active_profile_slot_idx",
            "context_items_class_scope_updated_idx",
        ],
        "context_item_revisions" => &["context_item_revisions_item_idx"],
        "operation_log" => &["operation_log_item_idx"],
        _ => &[],
    };
    for index in indexes {
        sqlx::query(&format!("DROP INDEX IF EXISTS {index}"))
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

async fn table_create_sql(tx: &mut Transaction<'_, Sqlite>, table: &str) -> Result<Option<String>> {
    sqlx::query("SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?")
        .bind(table)
        .fetch_optional(&mut **tx)
        .await?
        .map(|row| row.try_get("sql").map_err(Into::into))
        .transpose()
}

fn rows_to_items(rows: Vec<SqliteRow>) -> Result<Vec<ContextItem>> {
    rows.into_iter().map(row_to_item).collect()
}

fn rows_to_revisions(rows: Vec<SqliteRow>) -> Result<Vec<ContextItemRevision>> {
    rows.into_iter().map(row_to_revision).collect()
}

fn rows_to_operations(rows: Vec<SqliteRow>) -> Result<Vec<OperationLogEntry>> {
    rows.into_iter().map(row_to_operation).collect()
}

fn row_to_explicit_memory_match(row: SqliteRow) -> Result<ExplicitMemoryMatch> {
    let fts_rank = row.try_get("fts_rank")?;
    Ok(ExplicitMemoryMatch {
        item: row_to_item(row)?,
        fts_rank,
    })
}

fn row_to_recorded_turn_match(row: SqliteRow) -> Result<RecordedTurnMatch> {
    let fts_rank = row.try_get("fts_rank")?;
    Ok(RecordedTurnMatch {
        turn: row_to_recorded_turn(row)?,
        fts_rank,
    })
}

fn row_to_item(row: SqliteRow) -> Result<ContextItem> {
    let class = class_from_str(row.try_get::<String, _>("class")?.as_str())?;
    let tags = parse_tags(row.try_get::<String, _>("tags")?)?;
    Ok(ContextItem {
        id: row.try_get("id")?,
        class,
        scope: row.try_get("scope")?,
        slot: row.try_get("slot")?,
        title: row.try_get("title")?,
        body: row.try_get("body")?,
        tags,
        priority: row.try_get("priority")?,
        pinned: row.try_get::<i64, _>("pinned")? != 0,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        deleted_at: row.try_get("deleted_at")?,
        revision: row.try_get("revision")?,
        source_kind: row.try_get("source_kind")?,
    })
}

fn row_to_recorded_turn(row: SqliteRow) -> Result<RecordedTurn> {
    let sequence_no = u64::try_from(row.try_get::<i64, _>("sequence_no")?)
        .context("recorded turn sequence number is negative")?;
    Ok(RecordedTurn {
        private_context_id: row.try_get("private_context_id")?,
        session_id: row.try_get("session_id")?,
        turn_id: row.try_get("turn_id")?,
        sequence_no,
        runtime_id: row.try_get("runtime_id")?,
        scope: row.try_get("scope")?,
        user_text: row.try_get("user_text")?,
        assistant_text: row.try_get("assistant_text")?,
        recorded_at: row.try_get("recorded_at")?,
    })
}

fn row_to_revision(row: SqliteRow) -> Result<ContextItemRevision> {
    let class = class_from_str(row.try_get::<String, _>("class")?.as_str())?;
    let tags = parse_tags(row.try_get::<String, _>("tags")?)?;
    Ok(ContextItemRevision {
        revision_id: row.try_get("revision_id")?,
        item_id: row.try_get("item_id")?,
        revision: row.try_get("revision")?,
        operation: row.try_get("operation")?,
        class,
        scope: row.try_get("scope")?,
        slot: row.try_get("slot")?,
        title: row.try_get("title")?,
        body: row.try_get("body")?,
        tags,
        priority: row.try_get("priority")?,
        pinned: row.try_get::<i64, _>("pinned")? != 0,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        deleted_at: row.try_get("deleted_at")?,
        source_kind: row.try_get("source_kind")?,
        recorded_at: row.try_get("recorded_at")?,
    })
}

fn row_to_operation(row: SqliteRow) -> Result<OperationLogEntry> {
    let class = class_from_str(row.try_get::<String, _>("class")?.as_str())?;
    Ok(OperationLogEntry {
        operation_id: row.try_get("operation_id")?,
        operation: row.try_get("operation")?,
        item_id: row.try_get("item_id")?,
        class,
        scope: row.try_get("scope")?,
        slot: row.try_get("slot")?,
        revision: row.try_get("revision")?,
        source_kind: row.try_get("source_kind")?,
        actor_surface: row.try_get("actor_surface")?,
        recorded_at: row.try_get("recorded_at")?,
    })
}

fn class_from_str(value: &str) -> Result<ProjectedContextClass> {
    match value {
        "assistant_profile" => Ok(ProjectedContextClass::AssistantProfile),
        "user_profile" => Ok(ProjectedContextClass::UserProfile),
        "memory" => Ok(ProjectedContextClass::Memory),
        other => bail!("unknown context item class '{other}'"),
    }
}

fn validate_profile_projection_class(class: ProjectedContextClass) -> Result<()> {
    match class {
        ProjectedContextClass::AssistantProfile | ProjectedContextClass::UserProfile => Ok(()),
        ProjectedContextClass::Memory => bail!("memory is not a profile class"),
    }
}

fn sort_profiles(class: ProjectedContextClass, items: &mut [ContextItem], scopes: &[String]) {
    let slots = required_profile_slots(class);
    items.sort_by(|left, right| {
        let left_slot = left
            .slot
            .as_deref()
            .and_then(|slot| slots.iter().position(|candidate| *candidate == slot))
            .unwrap_or(usize::MAX);
        let right_slot = right
            .slot
            .as_deref()
            .and_then(|slot| slots.iter().position(|candidate| *candidate == slot))
            .unwrap_or(usize::MAX);
        let left_scope = scopes
            .iter()
            .position(|scope| scope == &left.scope)
            .unwrap_or(usize::MAX);
        let right_scope = scopes
            .iter()
            .position(|scope| scope == &right.scope)
            .unwrap_or(usize::MAX);
        (left_slot, left_scope, left.id.as_str()).cmp(&(right_slot, right_scope, right.id.as_str()))
    });
}

fn supported_profile_item_slot(class: ProjectedContextClass, item: &ContextItem) -> bool {
    item.slot
        .as_deref()
        .is_some_and(|slot| profile_slot_is_supported(class, slot))
}

fn budget_for_class(
    request: &PrivateContextProjectionRequest,
    class: ProjectedContextClass,
) -> Option<&ProjectedContextBudget> {
    request.budgets.iter().find(|budget| budget.class == class)
}

fn project_items(
    projector_id: &str,
    budget: &ProjectedContextBudget,
    items: Vec<ContextItem>,
    render: fn(&ContextItem) -> String,
) -> Vec<ProjectedContextItem> {
    let mut projected = Vec::new();
    let mut accepted_bytes = 0usize;
    for item in items {
        if projected.len() >= budget.max_items {
            break;
        }
        let remaining = budget.max_bytes.saturating_sub(accepted_bytes);
        if remaining == 0 {
            break;
        }
        let text = cap_utf8(&render(&item), remaining);
        if text.trim().is_empty() {
            continue;
        }
        accepted_bytes = accepted_bytes.saturating_add(text.len());
        projected.push(ProjectedContextItem {
            class: budget.class,
            text,
            provenance: vec![ProjectedContextProvenance {
                source: ProjectedContextProvenanceSource::ProjectorRecord,
                sequence_no: None,
                event_id: None,
                projector_id: Some(projector_id.to_string()),
                record_id: Some(item.id),
                revision: Some(item.revision.to_string()),
            }],
        });
    }
    projected
}

fn memory_projection_matches(
    explicit: Vec<ExplicitMemoryMatch>,
    recorded: Vec<RecordedTurnMatch>,
) -> Vec<MemoryProjectionMatch> {
    explicit
        .into_iter()
        .map(MemoryProjectionMatch::Explicit)
        .chain(recorded.into_iter().map(MemoryProjectionMatch::Recorded))
        .collect()
}

fn sort_memory_projection_matches(items: &mut [MemoryProjectionMatch], scopes: &[String]) {
    items.sort_by(|left, right| {
        memory_scope_rank(left, scopes)
            .cmp(&memory_scope_rank(right, scopes))
            .then_with(|| memory_kind_rank(left).cmp(&memory_kind_rank(right)))
            .then_with(|| memory_pinned_rank(left).cmp(&memory_pinned_rank(right)))
            .then_with(|| memory_priority_cmp(left, right))
            .then_with(|| memory_fts_rank(left).total_cmp(&memory_fts_rank(right)))
            .then_with(|| memory_recency(right).cmp(memory_recency(left)))
            .then_with(|| memory_stable_id(left).cmp(&memory_stable_id(right)))
    });
}

fn project_memory_matches(
    projector_id: &str,
    budget: &ProjectedContextBudget,
    items: Vec<MemoryProjectionMatch>,
) -> Vec<ProjectedContextItem> {
    let mut projected = Vec::new();
    let mut accepted_bytes = 0usize;
    for item in items {
        if projected.len() >= budget.max_items {
            break;
        }
        let remaining = budget.max_bytes.saturating_sub(accepted_bytes);
        if remaining == 0 {
            break;
        }
        let text = cap_utf8(&memory_match_text(&item), remaining);
        if text.trim().is_empty() {
            continue;
        }
        accepted_bytes = accepted_bytes.saturating_add(text.len());
        projected.push(ProjectedContextItem {
            class: ProjectedContextClass::Memory,
            text,
            provenance: vec![memory_match_provenance(projector_id, &item)],
        });
    }
    projected
}

fn memory_scope_rank(item: &MemoryProjectionMatch, scopes: &[String]) -> usize {
    scopes
        .iter()
        .position(|scope| scope == memory_scope(item))
        .unwrap_or(usize::MAX)
}

fn memory_scope(item: &MemoryProjectionMatch) -> &str {
    match item {
        MemoryProjectionMatch::Explicit(item) => &item.item.scope,
        MemoryProjectionMatch::Recorded(item) => &item.turn.scope,
    }
}

fn memory_kind_rank(item: &MemoryProjectionMatch) -> usize {
    match item {
        MemoryProjectionMatch::Explicit(_) => 0,
        MemoryProjectionMatch::Recorded(_) => 1,
    }
}

fn memory_pinned_rank(item: &MemoryProjectionMatch) -> bool {
    match item {
        MemoryProjectionMatch::Explicit(item) => !item.item.pinned,
        MemoryProjectionMatch::Recorded(_) => true,
    }
}

fn memory_priority_cmp(left: &MemoryProjectionMatch, right: &MemoryProjectionMatch) -> Ordering {
    match (left, right) {
        (MemoryProjectionMatch::Explicit(left), MemoryProjectionMatch::Explicit(right)) => {
            right.item.priority.cmp(&left.item.priority)
        }
        _ => Ordering::Equal,
    }
}

fn memory_fts_rank(item: &MemoryProjectionMatch) -> f64 {
    match item {
        MemoryProjectionMatch::Explicit(item) => item.fts_rank,
        MemoryProjectionMatch::Recorded(item) => item.fts_rank,
    }
}

fn memory_recency(item: &MemoryProjectionMatch) -> &str {
    match item {
        MemoryProjectionMatch::Explicit(item) => &item.item.updated_at,
        MemoryProjectionMatch::Recorded(item) => &item.turn.recorded_at,
    }
}

fn memory_stable_id(item: &MemoryProjectionMatch) -> String {
    match item {
        MemoryProjectionMatch::Explicit(item) => item.item.id.clone(),
        MemoryProjectionMatch::Recorded(item) => {
            format!("{}:{}", item.turn.session_id, item.turn.turn_id)
        }
    }
}

fn memory_match_text(item: &MemoryProjectionMatch) -> String {
    match item {
        MemoryProjectionMatch::Explicit(item) => memory_projection_text(&item.item),
        MemoryProjectionMatch::Recorded(item) => recorded_turn_projection_text(&item.turn),
    }
}

fn memory_match_provenance(
    projector_id: &str,
    item: &MemoryProjectionMatch,
) -> ProjectedContextProvenance {
    match item {
        MemoryProjectionMatch::Explicit(item) => ProjectedContextProvenance {
            source: ProjectedContextProvenanceSource::ProjectorRecord,
            sequence_no: None,
            event_id: None,
            projector_id: Some(projector_id.to_string()),
            record_id: Some(item.item.id.clone()),
            revision: Some(item.item.revision.to_string()),
        },
        MemoryProjectionMatch::Recorded(item) => ProjectedContextProvenance {
            source: ProjectedContextProvenanceSource::SessionTurn,
            sequence_no: Some(item.turn.sequence_no),
            event_id: Some(format!("{}:{}", item.turn.session_id, item.turn.turn_id)),
            projector_id: None,
            record_id: None,
            revision: None,
        },
    }
}

fn profile_projection_text(item: &ContextItem) -> String {
    let scope = scope_label(&item.scope);
    let slot = item.slot.as_deref().unwrap_or("profile");
    format!("{scope} {slot}: {}", item.body)
}

fn memory_projection_text(item: &ContextItem) -> String {
    match item.title.as_deref() {
        Some(title) => format!("{title}\n{}", item.body),
        None => item.body.clone(),
    }
}

fn recorded_turn_projection_text(turn: &RecordedTurn) -> String {
    format!(
        "Turn {}\nUser: {}\nAssistant: {}",
        turn.sequence_no, turn.user_text, turn.assistant_text
    )
}

fn scope_label(scope: &str) -> &'static str {
    if scope == "global" {
        "global"
    } else {
        "project"
    }
}

fn fts_query_from_text(raw: &str) -> Option<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            current.push(ch.to_ascii_lowercase());
        } else {
            push_fts_token(&mut tokens, &mut current);
        }
    }
    push_fts_token(&mut tokens, &mut current);
    tokens.dedup();
    if tokens.is_empty() {
        return None;
    }
    Some(tokens.join(" OR "))
}

fn push_fts_token(tokens: &mut Vec<String>, current: &mut String) {
    if current.len() >= 2 && !is_stop_word(current) && !tokens.contains(current) {
        tokens.push(format!("{current}*"));
    }
    current.clear();
}

fn is_stop_word(value: &str) -> bool {
    matches!(
        value,
        "a" | "an"
            | "and"
            | "are"
            | "as"
            | "at"
            | "be"
            | "but"
            | "by"
            | "for"
            | "from"
            | "how"
            | "i"
            | "in"
            | "is"
            | "it"
            | "me"
            | "my"
            | "of"
            | "on"
            | "or"
            | "our"
            | "the"
            | "this"
            | "to"
            | "we"
            | "what"
            | "with"
            | "you"
            | "your"
    )
}

fn parse_tags(raw: String) -> Result<Vec<String>> {
    serde_json::from_str(&raw).with_context(|| "failed to decode memory tags")
}

fn tags_json(tags: &[String]) -> Result<String> {
    serde_json::to_string(tags).with_context(|| "failed to encode memory tags")
}

fn bool_to_i64(value: bool) -> i64 {
    i64::from(value)
}

fn new_record_id() -> String {
    format!("pcx_{}", Uuid::new_v4().simple())
}

fn now_timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn ensure_state_dir(state_dir: &Path) -> Result<()> {
    if state_dir.as_os_str().is_empty() {
        bail!("state directory path is required");
    }
    reject_parent_dir_components(state_dir, "state directory")?;
    ensure_no_existing_symlink_components(state_dir)?;
    fs::create_dir_all(state_dir)
        .with_context(|| format!("failed to create state directory {}", state_dir.display()))?;
    ensure_no_symlink_components(state_dir)?;
    let metadata = fs::metadata(state_dir)
        .with_context(|| format!("failed to stat state directory {}", state_dir.display()))?;
    if !metadata.is_dir() {
        bail!("state directory {} is not a directory", state_dir.display());
    }
    set_private_dir_permissions(state_dir)?;
    Ok(())
}

fn ensure_state_db_path(db_path: &Path) -> Result<()> {
    if let Ok(metadata) = fs::symlink_metadata(db_path) {
        if metadata.file_type().is_symlink() {
            bail!("state database {} must not be a symlink", db_path.display());
        }
        if !metadata.is_file() {
            bail!("state database {} is not a file", db_path.display());
        }
    }
    Ok(())
}

fn reject_parent_dir_components(path: &Path, label: &str) -> Result<()> {
    for component in path.components() {
        if component == Component::ParentDir {
            bail!("{label} path must not contain parent traversal");
        }
    }
    Ok(())
}

fn ensure_no_existing_symlink_components(path: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        if matches!(component, Component::Prefix(_) | Component::RootDir) {
            continue;
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                bail!(
                    "state directory component {} must not be a symlink",
                    current.display()
                );
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => break,
            Err(err) => {
                return Err(err).with_context(|| format!("failed to stat {}", current.display()))
            }
        }
    }
    Ok(())
}

fn ensure_no_symlink_components(path: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        if matches!(component, Component::Prefix(_) | Component::RootDir) {
            continue;
        }
        let metadata = fs::symlink_metadata(&current)
            .with_context(|| format!("failed to stat {}", current.display()))?;
        if metadata.file_type().is_symlink() {
            bail!(
                "state directory component {} must not be a symlink",
                current.display()
            );
        }
    }
    Ok(())
}

#[cfg(unix)]
fn set_private_dir_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = fs::metadata(path)
        .with_context(|| format!("failed to stat {}", path.display()))?
        .permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(path, permissions)
        .with_context(|| format!("failed to chmod {}", path.display()))
}

#[cfg(not(unix))]
fn set_private_dir_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = fs::metadata(path)
        .with_context(|| format!("failed to stat {}", path.display()))?
        .permissions();
    permissions.set_mode(0o600);
    fs::set_permissions(path, permissions)
        .with_context(|| format!("failed to chmod {}", path.display()))
}

#[cfg(not(unix))]
fn set_private_file_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{
        PrivateContextRecordSurface, PrivateContextRecordText, PrivateContextRecordTranscript,
    };
    use tempfile::tempdir;

    fn request_for(
        classes: &[ProjectedContextClass],
        current_input: Option<&str>,
        project_scope: Option<&str>,
    ) -> PrivateContextProjectionRequest {
        PrivateContextProjectionRequest {
            request_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            runtime_id: "codex".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: crate::protocol::SessionHistoryPolicy::Interactive,
            surface: crate::protocol::PromptContextMode::ProgramPrimary,
            project_scope: project_scope.map(str::to_string),
            current_input: current_input.map(str::to_string),
            budgets: classes
                .iter()
                .copied()
                .map(|class| ProjectedContextBudget {
                    class,
                    max_items: 16,
                    max_bytes: 1024,
                })
                .collect(),
            sources: Vec::new(),
        }
    }

    fn record_request_for(
        sequence_no: u64,
        user_text: &str,
        assistant_text: &str,
        project_scope: Option<&str>,
    ) -> PrivateContextRecordRequest {
        PrivateContextRecordRequest {
            session_id: Uuid::new_v4(),
            turn_id: Uuid::new_v4(),
            sequence_no,
            runtime_id: "codex".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: crate::protocol::SessionHistoryPolicy::Interactive,
            surface: PrivateContextRecordSurface::Program,
            project_scope: project_scope.map(str::to_string),
            transcript: PrivateContextRecordTranscript {
                user: (!user_text.is_empty()).then(|| record_text(user_text)),
                assistant: (!assistant_text.is_empty()).then(|| record_text(assistant_text)),
            },
        }
    }

    fn record_text(text: &str) -> PrivateContextRecordText {
        PrivateContextRecordText {
            text: text.to_string(),
            included_bytes: text.len(),
            original_bytes: text.len(),
        }
    }

    async fn count_rows(store: &PrivateContextStore, table: &str) -> i64 {
        sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(&store.pool)
            .await
            .expect("count rows")
    }

    async fn create_legacy_context_items_schema(state_dir: &Path) {
        fs::create_dir_all(state_dir).expect("state dir");
        let db_path = state_dir.join(DB_FILE);
        let options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .connect_with(options)
            .await
            .expect("legacy db");
        sqlx::query(
            r#"
            CREATE TABLE context_items (
                id TEXT PRIMARY KEY NOT NULL,
                class TEXT NOT NULL CHECK (class IN ('assistant_profile', 'user_profile', 'memory')),
                scope TEXT NOT NULL,
                slot TEXT,
                title TEXT,
                body TEXT NOT NULL,
                tags TEXT NOT NULL DEFAULT '[]',
                priority INTEGER NOT NULL DEFAULT 0,
                pinned INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                deleted_at TEXT,
                revision INTEGER NOT NULL,
                source_kind TEXT NOT NULL CHECK (source_kind = 'explicit_operator'),
                CHECK (
                    (
                        class IN ('assistant_profile', 'user_profile')
                        AND slot IS NOT NULL
                        AND title IS NULL
                        AND tags = '[]'
                        AND priority = 0
                        AND pinned = 0
                    )
                    OR (
                        class = 'memory'
                        AND slot IS NULL
                    )
                )
            )
            "#,
        )
        .execute(&pool)
        .await
        .expect("legacy context_items");
        sqlx::query(
            r#"
            CREATE UNIQUE INDEX context_items_active_profile_slot_idx
            ON context_items (class, scope, slot)
            WHERE deleted_at IS NULL AND class IN ('assistant_profile', 'user_profile')
            "#,
        )
        .execute(&pool)
        .await
        .expect("legacy profile index");
        sqlx::query(
            r#"
            CREATE INDEX context_items_class_scope_updated_idx
            ON context_items (class, scope, deleted_at, updated_at, id)
            "#,
        )
        .execute(&pool)
        .await
        .expect("legacy class index");
        let now = now_timestamp();
        sqlx::query(
            r#"
            INSERT INTO context_items (
                id, class, scope, slot, title, body, tags, priority, pinned,
                created_at, updated_at, deleted_at, revision, source_kind
            )
            VALUES (
                'pcx_legacy', 'memory', 'global', NULL, NULL, 'legacy body',
                '[]', 0, 0, ?, ?, NULL, 1, 'explicit_operator'
            )
            "#,
        )
        .bind(&now)
        .bind(&now)
        .execute(&pool)
        .await
        .expect("legacy item");
        pool.close().await;
    }

    #[tokio::test]
    async fn recorder_inserts_turn_and_is_idempotent() {
        let temp_dir = tempdir().expect("temp dir");
        let store = PrivateContextStore::open(temp_dir.path())
            .await
            .expect("store");
        let request = record_request_for(
            7,
            "We discussed sqlite migrations.",
            "Use a table rebuild for changed checks.",
            Some("alpha"),
        );

        assert!(store
            .record_turn("lionclaw-private-context", &request)
            .await
            .expect("record turn"));
        assert!(!store
            .record_turn("lionclaw-private-context", &request)
            .await
            .expect("record duplicate"));

        assert_eq!(count_rows(&store, "recorded_turns").await, 1);
        assert_eq!(count_rows(&store, "recorded_turns_fts").await, 1);
        let turn = sqlx::query("SELECT * FROM recorded_turns")
            .fetch_one(&store.pool)
            .await
            .expect("recorded turn");
        assert_eq!(
            turn.try_get::<String, _>("private_context_id").expect("id"),
            "lionclaw-private-context"
        );
        assert_eq!(
            turn.try_get::<String, _>("scope").expect("scope"),
            "project:alpha"
        );
        assert_eq!(
            turn.try_get::<String, _>("user_text").expect("user"),
            "We discussed sqlite migrations."
        );
    }

    #[tokio::test]
    async fn source_kind_migration_recreates_indexes_on_rebuilt_table() {
        let temp_dir = tempdir().expect("temp dir");
        create_legacy_context_items_schema(temp_dir.path()).await;

        let store = PrivateContextStore::open(temp_dir.path())
            .await
            .expect("migrated store");
        let index_count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM sqlite_schema
            WHERE type = 'index'
                AND tbl_name = 'context_items'
                AND name IN (
                    'context_items_active_profile_slot_idx',
                    'context_items_class_scope_updated_idx'
                )
            "#,
        )
        .fetch_one(&store.pool)
        .await
        .expect("index count");
        assert_eq!(index_count, 2);
        assert_eq!(
            store
                .list_memory(Some("global"), None)
                .await
                .expect("legacy memory")
                .len(),
            1
        );

        store
            .record_turn(
                "lionclaw-private-context",
                &record_request_for(17, "remember: migrated transcript memory", "ok", None),
            )
            .await
            .expect("record explicit transcript after migration");
    }

    #[tokio::test]
    async fn recorder_directives_create_durable_items_with_shared_metadata() {
        let temp_dir = tempdir().expect("temp dir");
        let store = PrivateContextStore::open(temp_dir.path())
            .await
            .expect("store");
        let request = record_request_for(
            8,
            "remember: Use sqlx migrations carefully.\n- STYLE: Be exact.\n* preferences: Prefer terse status.",
            "remember: assistant text must not write memory.",
            None,
        );

        store
            .record_turn("lionclaw-private-context", &request)
            .await
            .expect("record turn");

        let memories = store
            .list_memory(Some("global"), None)
            .await
            .expect("memories");
        assert_eq!(memories.len(), 1);
        assert_eq!(memories[0].body, "Use sqlx migrations carefully.");
        assert_eq!(memories[0].source_kind, SOURCE_KIND_EXPLICIT_TRANSCRIPT);
        let assistant = store
            .show_profile(ProjectedContextClass::AssistantProfile, "global", "style")
            .await
            .expect("assistant profile")
            .expect("style profile");
        assert_eq!(assistant.body, "Be exact.");
        assert_eq!(assistant.source_kind, SOURCE_KIND_EXPLICIT_TRANSCRIPT);
        let user = store
            .show_profile(ProjectedContextClass::UserProfile, "global", "preferences")
            .await
            .expect("user profile")
            .expect("preferences profile");
        assert_eq!(user.body, "Prefer terse status.");

        let operations = store.operation_log(None).await.expect("operations");
        assert_eq!(operations.len(), 3);
        assert!(operations.iter().all(|operation| {
            operation.source_kind == SOURCE_KIND_EXPLICIT_TRANSCRIPT
                && operation.actor_surface == ACTOR_RECORDER
        }));
        let history = store
            .memory_history(&memories[0].id)
            .await
            .expect("history");
        assert_eq!(history[0].source_kind, SOURCE_KIND_EXPLICIT_TRANSCRIPT);
    }

    #[tokio::test]
    async fn recorder_ignores_retired_directive_prefixes() {
        let temp_dir = tempdir().expect("temp dir");
        let store = PrivateContextStore::open(temp_dir.path())
            .await
            .expect("store");
        let request = record_request_for(
            18,
            "remember that alpha uses sqlite WAL\nassistant style: Be terse.\nassistant workflow: First workflow.\nassistant default: Explain tradeoffs.\nuser preferences: Prefer terse status.\nuser standing requests: Pause before broad changes.",
            "ok",
            None,
        );

        store
            .record_turn("lionclaw-private-context", &request)
            .await
            .expect("record turn");

        assert_eq!(count_rows(&store, "recorded_turns").await, 1);
        assert_eq!(count_rows(&store, "context_items").await, 0);
        assert_eq!(count_rows(&store, "operation_log").await, 0);
    }

    #[tokio::test]
    async fn recorder_profile_directives_are_slot_upserts() {
        let temp_dir = tempdir().expect("temp dir");
        let store = PrivateContextStore::open(temp_dir.path())
            .await
            .expect("store");
        store
            .record_turn(
                "lionclaw-private-context",
                &record_request_for(19, "style: First style.", "ok", None),
            )
            .await
            .expect("first record");
        store
            .record_turn(
                "lionclaw-private-context",
                &record_request_for(20, "style: Second style.", "ok", None),
            )
            .await
            .expect("second record");

        let profiles = store
            .list_profiles(ProjectedContextClass::AssistantProfile, Some("global"))
            .await
            .expect("profiles");
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].body, "Second style.");
        assert_eq!(profiles[0].revision, 2);

        let history = store
            .profile_history(ProjectedContextClass::AssistantProfile, "global", "style")
            .await
            .expect("history");
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].body, "First style.");
        assert_eq!(history[1].body, "Second style.");
    }

    #[tokio::test]
    async fn recorder_ignores_non_explicit_and_substring_directives() {
        let temp_dir = tempdir().expect("temp dir");
        let store = PrivateContextStore::open(temp_dir.path())
            .await
            .expect("store");
        let request = record_request_for(
            9,
            "Please remember: this is not line-start grammar.\nI like deterministic tests.\nstyle maybe terse.",
            "style: assistant text is ignored.",
            None,
        );

        store
            .record_turn("lionclaw-private-context", &request)
            .await
            .expect("record turn");

        assert_eq!(count_rows(&store, "recorded_turns").await, 1);
        assert_eq!(count_rows(&store, "context_items").await, 0);
        assert_eq!(count_rows(&store, "operation_log").await, 0);
    }

    #[tokio::test]
    async fn assistant_text_alone_cannot_create_durable_items() {
        let temp_dir = tempdir().expect("temp dir");
        let store = PrivateContextStore::open(temp_dir.path())
            .await
            .expect("store");
        let request = record_request_for(
            21,
            "",
            "remember: assistant directives are not durable writes.",
            None,
        );

        store
            .record_turn("lionclaw-private-context", &request)
            .await
            .expect("record turn");

        assert_eq!(count_rows(&store, "recorded_turns").await, 1);
        assert_eq!(count_rows(&store, "context_items").await, 0);
        assert_eq!(count_rows(&store, "operation_log").await, 0);
    }

    #[tokio::test]
    async fn recorder_ignores_directives_outside_recorded_user_text_cap() {
        let temp_dir = tempdir().expect("temp dir");
        let store = PrivateContextStore::open(temp_dir.path())
            .await
            .expect("store");
        let user_text = format!(
            "{}\nremember: directives outside the recorded cap must not write.",
            "x".repeat(MAX_RECORDED_USER_TEXT_BYTES)
        );
        let request = record_request_for(22, &user_text, "ok", None);

        store
            .record_turn("lionclaw-private-context", &request)
            .await
            .expect("record turn");

        assert_eq!(count_rows(&store, "recorded_turns").await, 1);
        assert_eq!(count_rows(&store, "context_items").await, 0);
        assert_eq!(count_rows(&store, "operation_log").await, 0);
    }

    #[tokio::test]
    async fn recorder_rolls_back_turn_when_directive_write_fails() {
        let temp_dir = tempdir().expect("temp dir");
        let store = PrivateContextStore::open(temp_dir.path())
            .await
            .expect("store");
        let body = "x".repeat(MAX_MEMORY_BODY_BYTES + 1);
        let request = record_request_for(10, &format!("remember: {body}"), "ok", None);

        store
            .record_turn("lionclaw-private-context", &request)
            .await
            .expect_err("oversized directive should fail");

        assert_eq!(count_rows(&store, "recorded_turns").await, 0);
        assert_eq!(count_rows(&store, "recorded_turns_fts").await, 0);
        assert_eq!(count_rows(&store, "context_items").await, 0);
    }

    #[tokio::test]
    async fn recorded_turns_project_as_memory_with_session_turn_provenance() {
        let temp_dir = tempdir().expect("temp dir");
        let store = PrivateContextStore::open(temp_dir.path())
            .await
            .expect("store");
        let request = record_request_for(
            11,
            "The alpha project uses sqlite for private context.",
            "Confirmed.",
            Some("alpha"),
        );
        store
            .record_turn("lionclaw-private-context", &request)
            .await
            .expect("record turn");

        let projection = store
            .project(
                "lionclaw-private-context",
                &request_for(
                    &[ProjectedContextClass::Memory],
                    Some("sqlite"),
                    Some("alpha"),
                ),
            )
            .await
            .expect("projection");

        assert_eq!(projection.items.len(), 1);
        assert_eq!(projection.items[0].class, ProjectedContextClass::Memory);
        assert!(projection.items[0].text.contains("Turn 11"));
        assert!(projection.items[0]
            .text
            .contains("User: The alpha project uses sqlite for private context."));
        assert_eq!(
            projection.items[0].provenance[0].source,
            ProjectedContextProvenanceSource::SessionTurn
        );
        assert_eq!(projection.items[0].provenance[0].sequence_no, Some(11));
        assert!(projection.items[0].provenance[0].event_id.is_some());
    }

    #[tokio::test]
    async fn project_scoped_recorded_turns_do_not_leak() {
        let temp_dir = tempdir().expect("temp dir");
        let store = PrivateContextStore::open(temp_dir.path())
            .await
            .expect("store");
        store
            .record_turn(
                "lionclaw-private-context",
                &record_request_for(12, "Alpha sqlite detail.", "ok", Some("alpha")),
            )
            .await
            .expect("alpha turn");
        store
            .record_turn(
                "lionclaw-private-context",
                &record_request_for(13, "Beta sqlite detail.", "ok", Some("beta")),
            )
            .await
            .expect("beta turn");

        let projection = store
            .project(
                "lionclaw-private-context",
                &request_for(
                    &[ProjectedContextClass::Memory],
                    Some("sqlite"),
                    Some("alpha"),
                ),
            )
            .await
            .expect("projection");

        let joined = projection
            .items
            .iter()
            .map(|item| item.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(joined.contains("Alpha sqlite detail."));
        assert!(!joined.contains("Beta sqlite detail."));
    }

    #[tokio::test]
    async fn profiles_upsert_and_keep_history() {
        let temp_dir = tempdir().expect("temp dir");
        let store = PrivateContextStore::open(temp_dir.path())
            .await
            .expect("store");

        let first = store
            .set_profile(
                ProjectedContextClass::AssistantProfile,
                "global",
                "style",
                "Be concise.",
            )
            .await
            .expect("first profile");
        let second = store
            .set_profile(
                ProjectedContextClass::AssistantProfile,
                "global",
                "style",
                "Be precise.",
            )
            .await
            .expect("second profile");

        assert_eq!(first.id, second.id);
        assert_eq!(second.revision, 2);
        assert_eq!(
            store
                .list_profiles(ProjectedContextClass::AssistantProfile, Some("global"))
                .await
                .expect("profiles")
                .len(),
            1
        );
        let history = store
            .profile_history(ProjectedContextClass::AssistantProfile, "global", "style")
            .await
            .expect("history");
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].operation, "profile_set");
        assert_eq!(history[0].body, "Be concise.");
        assert_eq!(history[1].operation, "profile_set");
        assert_eq!(history[1].body, "Be precise.");
    }

    #[tokio::test]
    async fn unsupported_profile_slots_are_hidden_from_lists_and_projection() {
        let temp_dir = tempdir().expect("temp dir");
        let store = PrivateContextStore::open(temp_dir.path())
            .await
            .expect("store");
        store
            .set_profile(
                ProjectedContextClass::AssistantProfile,
                "global",
                "style",
                "Use current style.",
            )
            .await
            .expect("profile");
        let now = now_timestamp();
        sqlx::query(
            r#"
            INSERT INTO context_items (
                id, class, scope, slot, title, body, tags, priority, pinned,
                created_at, updated_at, deleted_at, revision, source_kind
            )
            VALUES (
                'pcx_unsupported_profile', 'assistant_profile', 'global', 'workflow',
                NULL, 'Retired workflow.', '[]', 0, 0, ?, ?, NULL, 1,
                'explicit_operator'
            )
            "#,
        )
        .bind(&now)
        .bind(&now)
        .execute(&store.pool)
        .await
        .expect("insert unsupported profile");

        let profiles = store
            .list_profiles(ProjectedContextClass::AssistantProfile, Some("global"))
            .await
            .expect("profiles");
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].slot.as_deref(), Some("style"));

        let projection = store
            .project(
                "lionclaw-private-context",
                &request_for(&[ProjectedContextClass::AssistantProfile], None, None),
            )
            .await
            .expect("projection");
        assert_eq!(projection.items.len(), 1);
        assert_eq!(projection.items[0].text, "global style: Use current style.");
    }

    #[tokio::test]
    async fn memory_search_requires_fts_match() {
        let temp_dir = tempdir().expect("temp dir");
        let store = PrivateContextStore::open(temp_dir.path())
            .await
            .expect("store");
        store
            .remember_memory(
                "global",
                Some("Rust"),
                "Use sqlx for async sqlite work.",
                &["rust".to_string()],
                0,
                false,
            )
            .await
            .expect("matching memory");
        store
            .remember_memory(
                "global",
                Some("Pinned unrelated"),
                "Projector startup facts.",
                &["projector".to_string()],
                100,
                true,
            )
            .await
            .expect("unrelated memory");

        let matches = store
            .search_memory("sqlite transaction", None, None)
            .await
            .expect("search");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].title.as_deref(), Some("Rust"));
        assert!(store
            .search_memory("?!?!?!", None, None)
            .await
            .expect("punctuation search")
            .is_empty());
    }

    #[tokio::test]
    async fn memory_history_includes_revision_bodies() {
        let temp_dir = tempdir().expect("temp dir");
        let store = PrivateContextStore::open(temp_dir.path())
            .await
            .expect("store");
        let item = store
            .remember_memory(
                "global",
                Some("First"),
                "First body.",
                &["initial".to_string()],
                0,
                false,
            )
            .await
            .expect("memory");
        store
            .update_memory(
                &item.id,
                MemoryPatch {
                    title: Some(Some("Second".to_string())),
                    body: Some("Second body.".to_string()),
                    tags: Some(vec!["updated".to_string()]),
                    priority: Some(5),
                    pinned: Some(true),
                },
            )
            .await
            .expect("update memory");

        let history = store.memory_history(&item.id).await.expect("history");
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].operation, "memory_remember");
        assert_eq!(history[0].title.as_deref(), Some("First"));
        assert_eq!(history[0].body, "First body.");
        assert_eq!(history[0].tags, vec!["initial".to_string()]);
        assert_eq!(history[1].operation, "memory_update");
        assert_eq!(history[1].title.as_deref(), Some("Second"));
        assert_eq!(history[1].body, "Second body.");
        assert_eq!(history[1].tags, vec!["updated".to_string()]);
        assert_eq!(history[1].priority, 5);
        assert!(history[1].pinned);
    }

    #[tokio::test]
    async fn projection_scopes_profiles_and_memory_differ_by_order() {
        let temp_dir = tempdir().expect("temp dir");
        let store = PrivateContextStore::open(temp_dir.path())
            .await
            .expect("store");
        store
            .set_profile(
                ProjectedContextClass::AssistantProfile,
                "global",
                "style",
                "Global assistant.",
            )
            .await
            .expect("global profile");
        store
            .set_profile(
                ProjectedContextClass::AssistantProfile,
                "project:alpha",
                "style",
                "Project assistant.",
            )
            .await
            .expect("project profile");
        store
            .remember_memory(
                "global",
                Some("Global memory"),
                "Use sqlite carefully.",
                &[],
                0,
                false,
            )
            .await
            .expect("global memory");
        store
            .remember_memory(
                "project:alpha",
                Some("Project memory"),
                "Use sqlite carefully in alpha.",
                &[],
                0,
                false,
            )
            .await
            .expect("project memory");

        let projection = store
            .project(
                "lionclaw-private-context",
                &request_for(
                    &[
                        ProjectedContextClass::AssistantProfile,
                        ProjectedContextClass::Memory,
                    ],
                    Some("sqlite"),
                    Some("alpha"),
                ),
            )
            .await
            .expect("projection");

        let profile_texts = projection
            .items
            .iter()
            .filter(|item| item.class == ProjectedContextClass::AssistantProfile)
            .map(|item| item.text.as_str())
            .collect::<Vec<_>>();
        assert_eq!(profile_texts[0], "global style: Global assistant.");
        assert_eq!(profile_texts[1], "project style: Project assistant.");

        let memory_texts = projection
            .items
            .iter()
            .filter(|item| item.class == ProjectedContextClass::Memory)
            .map(|item| item.text.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            memory_texts[0],
            "Project memory\nUse sqlite carefully in alpha."
        );
        assert_eq!(memory_texts[1], "Global memory\nUse sqlite carefully.");
    }

    #[tokio::test]
    async fn memory_projection_requires_current_input() {
        let temp_dir = tempdir().expect("temp dir");
        let store = PrivateContextStore::open(temp_dir.path())
            .await
            .expect("store");
        store
            .remember_memory("global", None, "Use sqlite carefully.", &[], 0, true)
            .await
            .expect("memory");

        let projection = store
            .project(
                "lionclaw-private-context",
                &request_for(&[ProjectedContextClass::Memory], None, None),
            )
            .await
            .expect("projection");

        assert!(projection.items.is_empty());
    }

    #[tokio::test]
    async fn operation_log_omits_body_text() {
        let temp_dir = tempdir().expect("temp dir");
        let store = PrivateContextStore::open(temp_dir.path())
            .await
            .expect("store");
        let item = store
            .remember_memory(
                "global",
                Some("Secret"),
                "Never audit this body.",
                &["secret-tag".to_string()],
                0,
                false,
            )
            .await
            .expect("memory");

        let log = store
            .operation_log(Some(&item.id))
            .await
            .expect("operation log");
        assert_eq!(log[0].source_kind, SOURCE_KIND_EXPLICIT_OPERATOR);
        assert_eq!(log[0].actor_surface, ACTOR_CONTEXT_CLI);
        let history = store.memory_history(&item.id).await.expect("history");
        assert_eq!(history[0].source_kind, SOURCE_KIND_EXPLICIT_OPERATOR);
        let encoded = serde_json::to_string(&log).expect("json");
        assert!(encoded.contains("memory_remember"));
        assert!(!encoded.contains("Never audit this body."));
        assert!(!encoded.contains("Secret"));
        assert!(!encoded.contains("secret-tag"));
    }

    #[tokio::test]
    async fn profile_projection_does_not_depend_on_fts_relevance() {
        let temp_dir = tempdir().expect("temp dir");
        let store = PrivateContextStore::open(temp_dir.path())
            .await
            .expect("store");
        store
            .set_profile(
                ProjectedContextClass::UserProfile,
                "global",
                "preferences",
                "Prefers direct answers.",
            )
            .await
            .expect("profile");

        let projection = store
            .project(
                "lionclaw-private-context",
                &request_for(
                    &[ProjectedContextClass::UserProfile],
                    Some("unrelated punctuation ?!?"),
                    None,
                ),
            )
            .await
            .expect("projection");

        assert_eq!(projection.items.len(), 1);
        assert_eq!(
            projection.items[0].class,
            ProjectedContextClass::UserProfile
        );
        assert!(projection.items[0].text.contains("Prefers direct answers."));
    }

    #[tokio::test]
    async fn memory_projection_requires_fts_relevance() {
        let temp_dir = tempdir().expect("temp dir");
        let store = PrivateContextStore::open(temp_dir.path())
            .await
            .expect("store");
        store
            .remember_memory("global", Some("Pinned"), "sqlite details.", &[], 100, true)
            .await
            .expect("pinned memory");

        let punctuation = store
            .project(
                "lionclaw-private-context",
                &request_for(&[ProjectedContextClass::Memory], Some("?!?!"), None),
            )
            .await
            .expect("punctuation projection");
        assert!(punctuation.items.is_empty());

        let unrelated = store
            .project(
                "lionclaw-private-context",
                &request_for(&[ProjectedContextClass::Memory], Some("banana"), None),
            )
            .await
            .expect("unrelated projection");
        assert!(unrelated.items.is_empty());
    }

    #[tokio::test]
    async fn deleted_memory_is_not_searched_or_projected() {
        let temp_dir = tempdir().expect("temp dir");
        let store = PrivateContextStore::open(temp_dir.path())
            .await
            .expect("store");
        let item = store
            .remember_memory("global", None, "sqlite deletion marker.", &[], 0, false)
            .await
            .expect("memory");
        store.delete_memory(&item.id).await.expect("delete memory");

        assert!(store
            .search_memory("sqlite deletion", None, None)
            .await
            .expect("search")
            .is_empty());
        let projection = store
            .project(
                "lionclaw-private-context",
                &request_for(
                    &[ProjectedContextClass::Memory],
                    Some("sqlite deletion"),
                    None,
                ),
            )
            .await
            .expect("projection");
        assert!(projection.items.is_empty());
    }

    #[tokio::test]
    async fn projection_respects_requested_classes_and_budgets() {
        let temp_dir = tempdir().expect("temp dir");
        let store = PrivateContextStore::open(temp_dir.path())
            .await
            .expect("store");
        store
            .set_profile(
                ProjectedContextClass::AssistantProfile,
                "global",
                "style",
                "Be concise.",
            )
            .await
            .expect("profile");
        store
            .remember_memory(
                "global",
                Some("SQLite"),
                "sqlite budget marker.",
                &[],
                0,
                false,
            )
            .await
            .expect("memory");

        let mut request = request_for(&[ProjectedContextClass::Memory], Some("sqlite"), None);
        request.budgets[0].max_items = 1;
        request.budgets[0].max_bytes = 10;
        let projection = store
            .project("lionclaw-private-context", &request)
            .await
            .expect("projection");

        assert_eq!(projection.items.len(), 1);
        assert_eq!(projection.items[0].class, ProjectedContextClass::Memory);
        assert!(projection.items[0].text.len() <= 10);
        assert!(!projection
            .items
            .iter()
            .any(|item| item.class == ProjectedContextClass::AssistantProfile));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn open_rejects_symlinked_state_dir_ancestor() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempdir().expect("temp dir");
        let outside = temp_dir.path().join("outside-state");
        fs::create_dir(&outside).expect("outside state");
        let link = temp_dir.path().join("state-link");
        symlink(&outside, &link).expect("state symlink");

        let err = PrivateContextStore::open(&link.join("private-context"))
            .await
            .expect_err("symlinked state ancestor should fail");

        assert!(err.to_string().contains("must not be a symlink"));
        assert!(!outside.join("private-context").join(DB_FILE).exists());
    }
}
