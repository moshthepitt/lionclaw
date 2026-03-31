use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use sqlx::{sqlite::SqliteRow, Row, SqlitePool};
use uuid::Uuid;

use crate::kernel::db::{ms_to_datetime, now_ms};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    Any,
    SkillUse,
    FsRead,
    FsWrite,
    NetEgress,
    SecretRequest,
    ChannelSend,
    SchedulerRun,
}

impl Capability {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Any => "*",
            Self::SkillUse => "skill.use",
            Self::FsRead => "fs.read",
            Self::FsWrite => "fs.write",
            Self::NetEgress => "net.egress",
            Self::SecretRequest => "secret.request",
            Self::ChannelSend => "channel.send",
            Self::SchedulerRun => "scheduler.run",
        }
    }
}

impl FromStr for Capability {
    type Err = String;

    fn from_str(raw: &str) -> std::result::Result<Self, Self::Err> {
        match raw.trim() {
            "*" => Ok(Self::Any),
            "skill.use" => Ok(Self::SkillUse),
            "fs.read" => Ok(Self::FsRead),
            "fs.write" => Ok(Self::FsWrite),
            "net.egress" => Ok(Self::NetEgress),
            "secret.request" => Ok(Self::SecretRequest),
            "channel.send" => Ok(Self::ChannelSend),
            "scheduler.run" => Ok(Self::SchedulerRun),
            other => Err(format!("unsupported capability '{}'", other)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    Any,
    Job(Uuid),
    Session(Uuid),
    Channel(String),
    Runtime(String),
    Exact(String),
}

impl Scope {
    pub fn as_str(&self) -> String {
        match self {
            Self::Any => "*".to_string(),
            Self::Job(id) => format!("job:{}", id),
            Self::Session(id) => format!("session:{}", id),
            Self::Channel(id) => format!("channel:{}", id),
            Self::Runtime(id) => format!("runtime:{}", id),
            Self::Exact(raw) => raw.clone(),
        }
    }
}

impl FromStr for Scope {
    type Err = String;

    fn from_str(raw: &str) -> std::result::Result<Self, Self::Err> {
        let value = raw.trim();
        if value.is_empty() {
            return Err("scope cannot be empty".to_string());
        }
        if value == "*" {
            return Ok(Self::Any);
        }
        if let Some(rest) = value.strip_prefix("job:") {
            return Uuid::parse_str(rest)
                .map(Self::Job)
                .map_err(|_| format!("invalid job scope '{}'", value));
        }
        if let Some(rest) = value.strip_prefix("session:") {
            return Uuid::parse_str(rest)
                .map(Self::Session)
                .map_err(|_| format!("invalid session scope '{}'", value));
        }
        if let Some(rest) = value.strip_prefix("channel:") {
            if rest.trim().is_empty() {
                return Err("channel scope cannot be empty".to_string());
            }
            return Ok(Self::Channel(rest.to_string()));
        }
        if let Some(rest) = value.strip_prefix("runtime:") {
            if rest.trim().is_empty() {
                return Err("runtime scope cannot be empty".to_string());
            }
            return Ok(Self::Runtime(rest.to_string()));
        }
        Ok(Self::Exact(value.to_string()))
    }
}

#[derive(Debug, Clone)]
pub struct Grant {
    pub grant_id: Uuid,
    pub skill_id: String,
    pub capability: Capability,
    pub scope: Scope,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct PolicyStore {
    pool: SqlitePool,
}

impl PolicyStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn grant(
        &self,
        skill_id: String,
        capability: Capability,
        scope: Scope,
        ttl_seconds: Option<i64>,
    ) -> Result<Grant> {
        let skill_exists = sqlx::query("SELECT 1 FROM skills WHERE skill_id = ?1 LIMIT 1")
            .bind(&skill_id)
            .fetch_optional(&self.pool)
            .await
            .context("failed to validate skill existence for grant")?;
        if skill_exists.is_none() {
            return Err(anyhow!("skill '{}' not found", skill_id));
        }

        let created_at_ms = now_ms();
        let expires_at_ms = ttl_seconds.map(|seconds| created_at_ms + (seconds * 1000));
        let capability_raw = capability.as_str();
        let scope_raw = scope.as_str();

        if expires_at_ms.is_none() {
            let existing = sqlx::query(
                "SELECT grant_id, skill_id, capability, scope, created_at_ms, expires_at_ms \
                 FROM policy_grants \
                 WHERE skill_id = ?1 AND capability = ?2 AND scope = ?3 AND expires_at_ms IS NULL \
                 ORDER BY created_at_ms DESC \
                 LIMIT 1",
            )
            .bind(&skill_id)
            .bind(capability_raw)
            .bind(&scope_raw)
            .fetch_optional(&self.pool)
            .await
            .context("failed to query existing non-expiring grant")?;

            if let Some(row) = existing {
                return map_grant_row(row);
            }
        }

        let grant_id = Uuid::new_v4();

        sqlx::query(
            "INSERT INTO policy_grants \
             (grant_id, skill_id, capability, scope, created_at_ms, expires_at_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .bind(grant_id.to_string())
        .bind(&skill_id)
        .bind(capability_raw)
        .bind(&scope_raw)
        .bind(created_at_ms)
        .bind(expires_at_ms)
        .execute(&self.pool)
        .await
        .context("failed to insert policy grant")?;

        let row = sqlx::query(
            "SELECT grant_id, skill_id, capability, scope, created_at_ms, expires_at_ms \
             FROM policy_grants \
             WHERE grant_id = ?1",
        )
        .bind(grant_id.to_string())
        .fetch_one(&self.pool)
        .await
        .context("failed to load inserted policy grant")?;

        map_grant_row(row)
    }

    pub async fn revoke(&self, grant_id: Uuid) -> Result<bool> {
        let deleted = sqlx::query("DELETE FROM policy_grants WHERE grant_id = ?1")
            .bind(grant_id.to_string())
            .execute(&self.pool)
            .await
            .context("failed to revoke policy grant")?;

        Ok(deleted.rows_affected() > 0)
    }

    pub async fn is_allowed(
        &self,
        skill_id: &str,
        capability: Capability,
        scope: &Scope,
    ) -> Result<bool> {
        let now = now_ms();
        let capability_raw = capability.as_str();
        let scope_raw = scope.as_str();

        let row = sqlx::query(
            "SELECT 1 \
             FROM policy_grants \
             WHERE skill_id = ?1 \
               AND (capability = ?2 OR capability = '*') \
               AND (scope = ?3 OR scope = '*') \
               AND (expires_at_ms IS NULL OR expires_at_ms >= ?4) \
             LIMIT 1",
        )
        .bind(skill_id)
        .bind(capability_raw)
        .bind(scope_raw)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .context("failed to evaluate policy")?;

        Ok(row.is_some())
    }
}

fn map_grant_row(row: SqliteRow) -> Result<Grant> {
    let grant_id_raw: String = row.get("grant_id");
    let capability_raw: String = row.get("capability");
    let scope_raw: String = row.get("scope");
    let created_at_ms: i64 = row.get("created_at_ms");
    let expires_at_ms: Option<i64> = row.get("expires_at_ms");

    let grant_id = Uuid::parse_str(&grant_id_raw)
        .with_context(|| format!("invalid grant id '{}'", grant_id_raw))?;
    let capability = Capability::from_str(&capability_raw)
        .map_err(|err| anyhow!("invalid capability: {}", err))?;
    let scope = Scope::from_str(&scope_raw).map_err(|err| anyhow!("invalid scope: {}", err))?;
    let created_at = ms_to_datetime(created_at_ms)
        .ok_or_else(|| anyhow!("invalid created_at_ms '{}'", created_at_ms))?;
    let expires_at = expires_at_ms
        .map(|value| {
            ms_to_datetime(value).ok_or_else(|| anyhow!("invalid expires_at_ms '{}'", value))
        })
        .transpose()?;

    Ok(Grant {
        grant_id,
        skill_id: row.get("skill_id"),
        capability,
        scope,
        created_at,
        expires_at,
    })
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::{Capability, PolicyStore, Scope};
    use crate::kernel::db::{now_ms, Db};

    #[tokio::test]
    async fn defaults_to_deny() {
        let db = Db::connect_memory().await.expect("db");
        let store = PolicyStore::new(db.pool());
        let scope = Scope::from_str("*").expect("scope");
        assert!(!store
            .is_allowed("skill-a", Capability::SkillUse, &scope)
            .await
            .expect("policy"));
    }

    #[tokio::test]
    async fn allows_after_grant() {
        let db = Db::connect_memory().await.expect("db");
        let store = PolicyStore::new(db.pool());
        let scope = Scope::from_str("*").expect("scope");
        seed_skill(db.pool(), "skill-a").await;

        store
            .grant(
                "skill-a".to_string(),
                Capability::SkillUse,
                scope.clone(),
                None,
            )
            .await
            .expect("grant");

        assert!(store
            .is_allowed("skill-a", Capability::SkillUse, &scope)
            .await
            .expect("policy"));
    }

    #[test]
    fn parses_scopes() {
        let id = uuid::Uuid::new_v4();
        let parsed = Scope::from_str(&format!("session:{}", id)).expect("scope");
        assert!(matches!(parsed, Scope::Session(value) if value == id));

        let parsed = Scope::from_str("channel:telegram").expect("scope");
        assert!(matches!(parsed, Scope::Channel(value) if value == "telegram"));
    }

    async fn seed_skill(pool: sqlx::SqlitePool, skill_id: &str) {
        sqlx::query(
            "INSERT INTO skills \
             (skill_id, name, description, source, reference, hash, enabled, installed_at_ms) \
             VALUES (?1, 'seed', 'seed', 'seed', '', 'seed-hash', 0, ?2)",
        )
        .bind(skill_id)
        .bind(now_ms())
        .execute(&pool)
        .await
        .expect("seed skill");
    }
}
