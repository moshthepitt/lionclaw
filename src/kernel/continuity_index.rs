use anyhow::{Context, Result};
use sqlx::{Row, SqlitePool};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContinuityIndexedDocument {
    pub relative_path: String,
    pub title: String,
    pub body: String,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContinuityIndexedMatch {
    pub relative_path: String,
    pub title: String,
    pub snippet: String,
}

#[derive(Debug, Clone)]
pub struct ContinuityIndexStore {
    pool: SqlitePool,
}

impl ContinuityIndexStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub fn can_search(&self, query: &str) -> bool {
        build_match_query(query).is_some()
    }

    pub async fn replace_all(&self, documents: &[ContinuityIndexedDocument]) -> Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to start continuity index rebuild transaction")?;
        sqlx::query("DELETE FROM continuity_documents")
            .execute(&mut *tx)
            .await
            .context("failed to clear continuity index")?;

        for document in documents {
            insert_document(&mut tx, document).await?;
        }

        tx.commit()
            .await
            .context("failed to commit continuity index rebuild transaction")?;
        Ok(())
    }

    pub async fn upsert(&self, document: &ContinuityIndexedDocument) -> Result<()> {
        sqlx::query(
            "INSERT INTO continuity_documents (relative_path, title, body, updated_at_ms) \
             VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT(relative_path) DO UPDATE SET \
               title = excluded.title, \
               body = excluded.body, \
               updated_at_ms = excluded.updated_at_ms",
        )
        .bind(&document.relative_path)
        .bind(&document.title)
        .bind(&document.body)
        .bind(document.updated_at_ms)
        .execute(&self.pool)
        .await
        .context("failed to upsert continuity index document")?;
        Ok(())
    }

    pub async fn remove(&self, relative_path: &str) -> Result<()> {
        sqlx::query("DELETE FROM continuity_documents WHERE relative_path = ?1")
            .bind(relative_path)
            .execute(&self.pool)
            .await
            .context("failed to remove continuity index document")?;
        Ok(())
    }

    pub async fn search(&self, query: &str, limit: usize) -> Result<Vec<ContinuityIndexedMatch>> {
        let Some(match_query) = build_match_query(query) else {
            return Ok(Vec::new());
        };

        let rows = sqlx::query(
            "SELECT d.relative_path, d.title, \
                    snippet(continuity_documents_fts, 1, '', '', ' … ', 16) AS snippet \
             FROM continuity_documents_fts \
             JOIN continuity_documents d ON d.rowid = continuity_documents_fts.rowid \
             WHERE continuity_documents_fts MATCH ?1 \
             ORDER BY bm25(continuity_documents_fts), d.updated_at_ms DESC \
             LIMIT ?2",
        )
        .bind(match_query)
        .bind(i64::try_from(limit).context("continuity search limit is too large")?)
        .fetch_all(&self.pool)
        .await
        .context("failed to search continuity index")?;

        Ok(rows
            .into_iter()
            .map(|row| ContinuityIndexedMatch {
                relative_path: row.get("relative_path"),
                title: row.get("title"),
                snippet: row.get::<String, _>("snippet"),
            })
            .collect())
    }
}

async fn insert_document(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    document: &ContinuityIndexedDocument,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO continuity_documents (relative_path, title, body, updated_at_ms) \
         VALUES (?1, ?2, ?3, ?4)",
    )
    .bind(&document.relative_path)
    .bind(&document.title)
    .bind(&document.body)
    .bind(document.updated_at_ms)
    .execute(&mut **tx)
    .await
    .context("failed to insert continuity index document")?;
    Ok(())
}

fn build_match_query(query: &str) -> Option<String> {
    if !query.is_ascii() {
        return None;
    }

    let tokens = query
        .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(|token| format!("{token}*"))
        .collect::<Vec<_>>();
    if tokens.is_empty() {
        None
    } else {
        Some(tokens.join(" AND "))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{build_match_query, ContinuityIndexStore, ContinuityIndexedDocument};
    use crate::kernel::db::Db;

    #[tokio::test]
    async fn search_matches_updated_documents() {
        let temp_dir = tempdir().expect("temp dir");
        let db = Db::connect_file(&temp_dir.path().join("lionclaw.db"))
            .await
            .expect("db connect");
        let store = ContinuityIndexStore::new(db.pool());

        store
            .replace_all(&[
                ContinuityIndexedDocument {
                    relative_path: "MEMORY.md".to_string(),
                    title: "Memory".to_string(),
                    body: "Prefers direct continuity reviews.".to_string(),
                    updated_at_ms: 10,
                },
                ContinuityIndexedDocument {
                    relative_path: "continuity/open-loops/review-audit.md".to_string(),
                    title: "Review Audit Trail".to_string(),
                    body: "Need to review continuity audit coverage.".to_string(),
                    updated_at_ms: 20,
                },
            ])
            .await
            .expect("replace docs");

        let matches = store.search("audit coverage", 10).await.expect("search");
        assert!(!matches.is_empty());
        assert!(matches
            .iter()
            .any(|item| item.relative_path == "continuity/open-loops/review-audit.md"));

        store
            .upsert(&ContinuityIndexedDocument {
                relative_path: "MEMORY.md".to_string(),
                title: "Memory".to_string(),
                body: "Prefers compact kernels.".to_string(),
                updated_at_ms: 30,
            })
            .await
            .expect("upsert");
        assert_eq!(
            store.search("direct", 10).await.expect("search old").len(),
            0
        );
        assert_eq!(
            store.search("compact", 10).await.expect("search new").len(),
            1
        );

        let hyphenated = store
            .search("open-loops", 10)
            .await
            .expect("search hyphenated token");
        assert!(!hyphenated.is_empty());
        assert!(hyphenated
            .iter()
            .any(|item| item.relative_path == "continuity/open-loops/review-audit.md"));

        assert!(build_match_query("café review").is_none());
    }
}
