use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use sqlx::{sqlite::SqliteRow, Row, SqlitePool};
use thiserror::Error;

use crate::kernel::db::{ms_to_datetime, now_ms};

#[derive(Debug, Clone)]
pub struct SkillRecord {
    pub skill_id: String,
    pub alias: String,
    pub name: String,
    pub description: String,
    pub source: String,
    pub reference: Option<String>,
    pub hash: String,
    pub snapshot_path: Option<String>,
    pub skill_md: Option<String>,
    pub enabled: bool,
    pub installed_at: DateTime<Utc>,
}

#[derive(Debug)]
pub struct SkillInstallInput {
    pub source: String,
    pub alias: String,
    pub reference: Option<String>,
    pub hash: Option<String>,
    pub skill_md: Option<String>,
    pub snapshot_path: Option<String>,
}

#[derive(Debug, Error)]
#[error("{message}")]
pub struct SkillAliasValidationError {
    message: String,
}

impl SkillAliasValidationError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, Error)]
#[error("skill alias '{alias}' is already enabled by '{existing_skill_id}'")]
pub struct SkillAliasConflict {
    pub alias: String,
    pub existing_skill_id: String,
}

#[derive(Debug, Clone)]
pub struct SkillStore {
    pool: SqlitePool,
}

impl SkillStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn install(&self, input: SkillInstallInput) -> Result<SkillRecord> {
        validate_skill_alias(&input.alias)?;

        let (name, description) = input
            .skill_md
            .as_deref()
            .map(parse_skill_frontmatter)
            .unwrap_or_else(|| {
                let fallback = derive_name_from_source(&input.source);
                (fallback, "Installed skill".to_string())
            });

        let hash = input.hash.unwrap_or_else(|| {
            let mut hasher = Sha256::new();
            hasher.update(input.source.as_bytes());
            if let Some(ref reference) = input.reference {
                hasher.update(reference.as_bytes());
            }
            if let Some(ref skill_md) = input.skill_md {
                hasher.update(skill_md.as_bytes());
            }
            hex::encode(hasher.finalize())
        });

        let reference_key = input.reference.unwrap_or_default();

        if let Some(existing) = self
            .find_by_provenance(&input.source, &reference_key, &hash)
            .await?
        {
            if existing.alias == input.alias {
                return Ok(existing);
            }
            return self.set_alias(&existing.skill_id, &input.alias).await;
        }

        let skill_id = derive_skill_id(&name, &hash);
        let installed_at_ms = now_ms();
        let alias = input.alias;
        let snapshot_path = input.snapshot_path.unwrap_or_default();
        let skill_md = input.skill_md.unwrap_or_default();

        let insert_result = sqlx::query(
            "INSERT INTO skills \
             (skill_id, alias, name, description, source, reference, hash, snapshot_path, skill_md, enabled, installed_at_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10)",
        )
        .bind(&skill_id)
        .bind(&alias)
        .bind(&name)
        .bind(&description)
        .bind(&input.source)
        .bind(&reference_key)
        .bind(&hash)
        .bind(&snapshot_path)
        .bind(&skill_md)
        .bind(installed_at_ms)
        .execute(&self.pool)
        .await;

        if let Err(err) = insert_result {
            if let Some(existing) = self
                .find_by_provenance(&input.source, &reference_key, &hash)
                .await?
            {
                if existing.alias == alias {
                    return Ok(existing);
                }
                return self.set_alias(&existing.skill_id, &alias).await;
            }
            return Err(err).context("failed to insert skill");
        }

        self.get(&skill_id)
            .await?
            .ok_or_else(|| anyhow!("skill disappeared immediately after insert"))
    }

    pub async fn list(&self) -> Result<Vec<SkillRecord>> {
        let rows = sqlx::query(
            "SELECT skill_id, alias, name, description, source, reference, hash, snapshot_path, skill_md, enabled, installed_at_ms \
             FROM skills \
             ORDER BY installed_at_ms ASC",
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to list skills")?;

        rows.into_iter().map(map_skill_row).collect()
    }

    pub async fn set_enabled(&self, skill_id: &str, enabled: bool) -> Result<Option<SkillRecord>> {
        if enabled {
            if let Some(skill) = self.get(skill_id).await? {
                if let Some(existing) = self.find_enabled_by_alias(&skill.alias).await? {
                    if existing.skill_id != skill.skill_id {
                        return Err(SkillAliasConflict {
                            alias: skill.alias,
                            existing_skill_id: existing.skill_id,
                        }
                        .into());
                    }
                }
            }
        }

        let changed = sqlx::query("UPDATE skills SET enabled = ?2 WHERE skill_id = ?1")
            .bind(skill_id)
            .bind(if enabled { 1 } else { 0 })
            .execute(&self.pool)
            .await
            .context("failed to update skill enabled state")?;

        if changed.rows_affected() == 0 {
            return Ok(None);
        }

        self.get(skill_id).await
    }

    pub async fn get(&self, skill_id: &str) -> Result<Option<SkillRecord>> {
        let row = sqlx::query(
            "SELECT skill_id, alias, name, description, source, reference, hash, snapshot_path, skill_md, enabled, installed_at_ms \
             FROM skills \
             WHERE skill_id = ?1",
        )
        .bind(skill_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to query skill")?;

        row.map(map_skill_row).transpose()
    }

    pub async fn list_enabled(&self) -> Result<Vec<SkillRecord>> {
        let rows = sqlx::query(
            "SELECT skill_id, alias, name, description, source, reference, hash, snapshot_path, skill_md, enabled, installed_at_ms \
             FROM skills \
             WHERE enabled = 1 \
             ORDER BY installed_at_ms ASC",
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to list enabled skills")?;

        rows.into_iter().map(map_skill_row).collect()
    }

    async fn find_by_provenance(
        &self,
        source: &str,
        reference: &str,
        hash: &str,
    ) -> Result<Option<SkillRecord>> {
        let row = sqlx::query(
            "SELECT skill_id, alias, name, description, source, reference, hash, snapshot_path, skill_md, enabled, installed_at_ms \
             FROM skills \
             WHERE source = ?1 AND reference = ?2 AND hash = ?3",
        )
        .bind(source)
        .bind(reference)
        .bind(hash)
        .fetch_optional(&self.pool)
        .await
        .context("failed to query skill by provenance")?;

        row.map(map_skill_row).transpose()
    }

    async fn find_enabled_by_alias(&self, alias: &str) -> Result<Option<SkillRecord>> {
        let row = sqlx::query(
            "SELECT skill_id, alias, name, description, source, reference, hash, snapshot_path, skill_md, enabled, installed_at_ms \
             FROM skills \
             WHERE alias = ?1 AND enabled = 1",
        )
        .bind(alias)
        .fetch_optional(&self.pool)
        .await
        .context("failed to query enabled skill by alias")?;

        row.map(map_skill_row).transpose()
    }

    async fn set_alias(&self, skill_id: &str, alias: &str) -> Result<SkillRecord> {
        sqlx::query("UPDATE skills SET alias = ?2 WHERE skill_id = ?1")
            .bind(skill_id)
            .bind(alias)
            .execute(&self.pool)
            .await
            .context("failed to update skill alias")?;

        self.get(skill_id)
            .await?
            .ok_or_else(|| anyhow!("skill disappeared immediately after alias update"))
    }
}

fn map_skill_row(row: SqliteRow) -> Result<SkillRecord> {
    let installed_at_ms: i64 = row.get("installed_at_ms");
    let installed_at = ms_to_datetime(installed_at_ms)
        .ok_or_else(|| anyhow!("invalid installed_at_ms '{installed_at_ms}'"))?;

    let enabled_raw: i64 = row.get("enabled");
    let enabled = enabled_raw != 0;

    let reference_raw: String = row.get("reference");
    let reference = if reference_raw.is_empty() {
        None
    } else {
        Some(reference_raw)
    };

    let snapshot_path_raw: String = row.get("snapshot_path");
    let snapshot_path = if snapshot_path_raw.trim().is_empty() {
        None
    } else {
        Some(snapshot_path_raw)
    };

    let skill_md_raw: String = row.get("skill_md");
    let skill_md = if skill_md_raw.trim().is_empty() {
        None
    } else {
        Some(skill_md_raw)
    };

    Ok(SkillRecord {
        skill_id: row.get("skill_id"),
        alias: row.get("alias"),
        name: row.get("name"),
        description: row.get("description"),
        source: row.get("source"),
        reference,
        hash: row.get("hash"),
        snapshot_path,
        skill_md,
        enabled,
        installed_at,
    })
}

pub fn derive_name_from_source(source: &str) -> String {
    source
        .split('/')
        .next_back()
        .unwrap_or("skill")
        .trim()
        .trim_end_matches(".md")
        .to_string()
}

pub fn sanitize_skill_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
            out.push(ch.to_ascii_lowercase());
        } else if ch.is_whitespace() {
            out.push('-');
        }
    }
    if out.is_empty() {
        "skill".to_string()
    } else {
        out
    }
}

pub fn validate_skill_alias(alias: &str) -> Result<()> {
    let trimmed = alias.trim();
    if alias != trimmed {
        return Err(SkillAliasValidationError::new(format!(
            "skill alias '{alias}' has surrounding whitespace"
        ))
        .into());
    }
    let alias = trimmed;
    if alias.is_empty() {
        return Err(SkillAliasValidationError::new("skill alias is required").into());
    }
    if matches!(alias, "." | "..") {
        return Err(SkillAliasValidationError::new(format!(
            "skill alias '{alias}' is not path-safe"
        ))
        .into());
    }
    if alias
        .chars()
        .any(|ch| !(ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.')))
    {
        return Err(SkillAliasValidationError::new(format!(
            "skill alias '{alias}' may only contain ASCII letters, numbers, '.', '_' and '-'"
        ))
        .into());
    }
    Ok(())
}

pub fn derive_skill_id(name: &str, hash: &str) -> String {
    let short_hash = &hash[..12.min(hash.len())];
    format!("{}-{}", sanitize_skill_name(name), short_hash)
}

pub fn parse_skill_frontmatter(content: &str) -> (String, String) {
    let trimmed = content.trim_start();
    if !trimmed.starts_with("---") {
        return ("skill".to_string(), "Installed skill".to_string());
    }

    let mut lines = trimmed.lines();
    let _ = lines.next(); // opening ---

    let mut name = None;
    let mut description = None;

    for line in lines {
        let line = line.trim();
        if line == "---" {
            break;
        }
        if let Some(rest) = line.strip_prefix("name:") {
            name = Some(rest.trim().trim_matches('"').to_string());
        }
        if let Some(rest) = line.strip_prefix("description:") {
            description = Some(rest.trim().trim_matches('"').to_string());
        }
    }

    (
        name.unwrap_or_else(|| "skill".to_string()),
        description.unwrap_or_else(|| "Installed skill".to_string()),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        derive_skill_id, parse_skill_frontmatter, sanitize_skill_name, validate_skill_alias,
    };

    #[test]
    fn parses_name_and_description() {
        let input = r#"---
name: demo-skill
description: Demo skill description
---

body"#;

        let (name, description) = parse_skill_frontmatter(input);
        assert_eq!(name, "demo-skill");
        assert_eq!(description, "Demo skill description");
    }

    #[test]
    fn derives_stable_skill_id() {
        assert_eq!(
            derive_skill_id("Channel Telegram", "0123456789abcdef"),
            "channel-telegram-0123456789ab"
        );
        assert_eq!(sanitize_skill_name("Channel Telegram"), "channel-telegram");
    }

    #[test]
    fn validates_path_safe_skill_aliases() {
        validate_skill_alias("terminal").expect("valid alias");
        validate_skill_alias("channel.terminal_1").expect("valid alias");
        validate_skill_alias(" terminal").expect_err("leading whitespace");
        validate_skill_alias("../terminal").expect_err("path traversal");
        validate_skill_alias("telegram/channel").expect_err("slash");
    }
}
