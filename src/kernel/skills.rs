use std::collections::HashMap;

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct SkillRecord {
    pub skill_id: String,
    pub name: String,
    pub description: String,
    pub source: String,
    pub reference: Option<String>,
    pub hash: String,
    pub enabled: bool,
    pub installed_at: DateTime<Utc>,
}

#[derive(Debug)]
pub struct SkillInstallInput {
    pub source: String,
    pub reference: Option<String>,
    pub hash: Option<String>,
    pub skill_md: Option<String>,
}

#[derive(Debug, Default)]
pub struct SkillStore {
    by_id: RwLock<HashMap<String, SkillRecord>>,
}

impl SkillStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn install(&self, input: SkillInstallInput) -> SkillRecord {
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

        let short_hash = &hash[..12.min(hash.len())];
        let skill_id = format!("{}-{}", sanitize_skill_name(&name), short_hash);

        let record = SkillRecord {
            skill_id: skill_id.clone(),
            name,
            description,
            source: input.source,
            reference: input.reference,
            hash,
            enabled: false,
            installed_at: Utc::now(),
        };

        self.by_id.write().await.insert(skill_id, record.clone());
        record
    }

    pub async fn list(&self) -> Vec<SkillRecord> {
        let mut skills = self
            .by_id
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        skills.sort_by_key(|skill| skill.installed_at);
        skills
    }

    pub async fn set_enabled(&self, skill_id: &str, enabled: bool) -> Option<SkillRecord> {
        let mut skills = self.by_id.write().await;
        let skill = skills.get_mut(skill_id)?;
        skill.enabled = enabled;
        Some(skill.clone())
    }

    pub async fn get(&self, skill_id: &str) -> Option<SkillRecord> {
        self.by_id.read().await.get(skill_id).cloned()
    }

    pub async fn list_enabled(&self) -> Vec<SkillRecord> {
        self.by_id
            .read()
            .await
            .values()
            .filter(|skill| skill.enabled)
            .cloned()
            .collect()
    }
}

fn derive_name_from_source(source: &str) -> String {
    source
        .split('/')
        .next_back()
        .unwrap_or("skill")
        .trim()
        .trim_end_matches(".md")
        .to_string()
}

fn sanitize_skill_name(name: &str) -> String {
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

fn parse_skill_frontmatter(content: &str) -> (String, String) {
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
    use super::parse_skill_frontmatter;

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
}
