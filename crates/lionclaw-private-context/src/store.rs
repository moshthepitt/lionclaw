use std::{
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
        PrivateContextProjection, PrivateContextProjectionRequest, ProjectedContextBudget,
        ProjectedContextClass, ProjectedContextItem, ProjectedContextProvenance,
        ProjectedContextProvenanceSource, TrustTier,
    },
    validation::{
        cap_utf8, projection_scopes, required_profile_slots, validate_body, validate_limit,
        validate_profile_slot, validate_query, validate_record_id, validate_scope, validate_tags,
        validate_title, MAX_MEMORY_BODY_BYTES, MAX_PROFILE_BODY_BYTES,
    },
};

const DB_FILE: &str = "lionclaw-private-context.sqlite3";
const SOURCE_KIND_EXPLICIT_OPERATOR: &str = "explicit_operator";
const ACTOR_CONTEXT_CLI: &str = "context_cli";
const META_SCHEMA_VERSION: &str = "schema_version";
const SCHEMA_VERSION: &str = "1";

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
    pub tags: Vec<String>,
    pub priority: i64,
    pub pinned: bool,
    pub created_at: String,
    pub updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<String>,
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
                source_kind TEXT NOT NULL CHECK (source_kind = 'explicit_operator'),
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
                source_kind TEXT NOT NULL CHECK (source_kind = 'explicit_operator'),
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

    pub(crate) async fn set_profile(
        &self,
        class: ProjectedContextClass,
        scope: &str,
        slot: &str,
        body: &str,
    ) -> Result<ContextItem> {
        let slot = validate_profile_slot(class, slot)?;
        let scope = validate_scope(scope)?;
        let body = validate_body("profile", body, MAX_PROFILE_BODY_BYTES)?;
        let now = now_timestamp();
        let mut tx = self.pool.begin().await?;
        let existing = active_profile_row(&mut tx, class, &scope, &slot).await?;
        let item = if let Some(existing) = existing {
            let mut item = row_to_item(existing)?;
            item.body = body;
            item.updated_at = now.clone();
            item.revision = item.revision.saturating_add(1);
            sqlx::query(
                r#"
                UPDATE context_items
                SET body = ?, updated_at = ?, revision = ?
                WHERE id = ?
                "#,
            )
            .bind(&item.body)
            .bind(&item.updated_at)
            .bind(item.revision)
            .bind(&item.id)
            .execute(&mut *tx)
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
                created_at: now.clone(),
                updated_at: now.clone(),
                deleted_at: None,
                revision: 1,
                source_kind: SOURCE_KIND_EXPLICIT_OPERATOR.to_string(),
            };
            insert_item(&mut tx, &item).await?;
            item
        };
        record_revision(&mut tx, &item, "profile_set", &now).await?;
        record_operation(&mut tx, &item, "profile_set", &now).await?;
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
        sqlx::query(
            r#"
            UPDATE context_items
            SET updated_at = ?, deleted_at = ?, revision = ?
            WHERE id = ?
            "#,
        )
        .bind(&item.updated_at)
        .bind(&item.deleted_at)
        .bind(item.revision)
        .bind(&item.id)
        .execute(&mut *tx)
        .await?;
        record_revision(&mut tx, &item, "profile_delete", &now).await?;
        record_operation(&mut tx, &item, "profile_delete", &now).await?;
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
        let scope = validate_scope(scope)?;
        let title = validate_title(title)?;
        let body = validate_body("memory", body, MAX_MEMORY_BODY_BYTES)?;
        let tags = validate_tags(tags)?;
        let now = now_timestamp();
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
            created_at: now.clone(),
            updated_at: now.clone(),
            deleted_at: None,
            revision: 1,
            source_kind: SOURCE_KIND_EXPLICIT_OPERATOR.to_string(),
        };
        let mut tx = self.pool.begin().await?;
        insert_item(&mut tx, &item).await?;
        refresh_fts(&mut tx, &item).await?;
        record_revision(&mut tx, &item, "memory_remember", &now).await?;
        record_operation(&mut tx, &item, "memory_remember", &now).await?;
        tx.commit().await?;
        Ok(item)
    }

    pub(crate) async fn list_memory(
        &self,
        scope: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<ContextItem>> {
        let limit = i64::try_from(validate_limit(limit)?).expect("validated list limit fits i64");
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
        let limit = i64::try_from(validate_limit(limit)?).expect("validated list limit fits i64");
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
        update_item(&mut tx, &item).await?;
        refresh_fts(&mut tx, &item).await?;
        record_revision(&mut tx, &item, "memory_update", &now).await?;
        record_operation(&mut tx, &item, "memory_update", &now).await?;
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
        update_item(&mut tx, &item).await?;
        refresh_fts(&mut tx, &item).await?;
        record_revision(&mut tx, &item, "memory_delete", &now).await?;
        record_operation(&mut tx, &item, "memory_delete", &now).await?;
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
        let rows =
            query_memory_projection(&self.pool, &fts_query, &scopes, budget.max_items).await?;
        let items = rows_to_items(rows)?;
        Ok(project_items(
            projector_id,
            budget,
            items,
            memory_projection_text,
        ))
    }
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
            revision = ?
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
    .bind(ACTOR_CONTEXT_CLI)
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
    if scopes.len() == 2 {
        sqlx::query(
            r#"
            SELECT *
            FROM context_items
            WHERE class = ? AND deleted_at IS NULL AND (scope = ? OR scope = ?)
            "#,
        )
        .bind(class.as_str())
        .bind(&scopes[0])
        .bind(&scopes[1])
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
        .bind(&scopes[0])
        .fetch_all(pool)
        .await
        .map_err(Into::into)
    }
}

async fn query_memory_projection(
    pool: &SqlitePool,
    fts_query: &str,
    scopes: &[String],
    max_items: usize,
) -> Result<Vec<SqliteRow>> {
    let limit = i64::try_from(max_items.max(1)).unwrap_or(i64::MAX);
    if scopes.len() == 2 {
        sqlx::query(
            r#"
            SELECT ci.*
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
        .bind(&scopes[0])
        .bind(&scopes[1])
        .bind(&scopes[0])
        .bind(limit)
        .fetch_all(pool)
        .await
        .map_err(Into::into)
    } else {
        sqlx::query(
            r#"
            SELECT ci.*
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
        .bind(&scopes[0])
        .bind(limit)
        .fetch_all(pool)
        .await
        .map_err(Into::into)
    }
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
        tags,
        priority: row.try_get("priority")?,
        pinned: row.try_get::<i64, _>("pinned")? != 0,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        deleted_at: row.try_get("deleted_at")?,
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
        assert_eq!(history[1].operation, "profile_set");
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
    async fn projection_scopes_profiles_and_memory_differ_by_order() {
        let temp_dir = tempdir().expect("temp dir");
        let store = PrivateContextStore::open(temp_dir.path())
            .await
            .expect("store");
        store
            .set_profile(
                ProjectedContextClass::AssistantProfile,
                "global",
                "identity",
                "Global assistant.",
            )
            .await
            .expect("global profile");
        store
            .set_profile(
                ProjectedContextClass::AssistantProfile,
                "project:alpha",
                "identity",
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
        assert_eq!(profile_texts[0], "global identity: Global assistant.");
        assert_eq!(profile_texts[1], "project identity: Project assistant.");

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
                &[],
                0,
                false,
            )
            .await
            .expect("memory");

        let log = store
            .operation_log(Some(&item.id))
            .await
            .expect("operation log");
        let encoded = serde_json::to_string(&log).expect("json");
        assert!(encoded.contains("memory_remember"));
        assert!(!encoded.contains("Never audit this body."));
        assert!(!encoded.contains("Secret"));
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
