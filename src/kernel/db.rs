use std::{path::Path, time::Duration};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
    SqlitePool,
};

#[derive(Clone)]
pub struct Db {
    pool: SqlitePool,
}

impl Db {
    pub async fn connect_file(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent().filter(|value| !value.as_os_str().is_empty()) {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed to create db directory {}", parent.display()))?;
        }

        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));

        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await
            .with_context(|| format!("failed to connect sqlite database {}", path.display()))?;

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .context("failed to run database migrations")?;

        Ok(Self { pool })
    }

    #[cfg(test)]
    pub async fn connect_memory() -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(":memory:")
            .create_if_missing(true)
            .foreign_keys(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .context("failed to connect in-memory sqlite database")?;

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .context("failed to run in-memory database migrations")?;

        Ok(Self { pool })
    }

    pub fn pool(&self) -> SqlitePool {
        self.pool.clone()
    }
}

pub fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

pub fn datetime_to_ms(value: DateTime<Utc>) -> i64 {
    value.timestamp_millis()
}

pub fn ms_to_datetime(value: i64) -> Option<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_millis(value)
}
