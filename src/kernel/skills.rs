use anyhow::Result;
use thiserror::Error;

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
    if alias.starts_with('.') {
        return Err(SkillAliasValidationError::new(format!(
            "skill alias '{alias}' must not start with '.'"
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
    let _ = lines.next();

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
        validate_skill_alias(".terminal").expect_err("leading dot");
        validate_skill_alias("../terminal").expect_err("path traversal");
        validate_skill_alias("telegram/channel").expect_err("slash");
    }
}
