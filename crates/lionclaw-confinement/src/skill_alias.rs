use anyhow::Result;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{message}")]
pub struct SkillAliasValidationError {
    message: String,
}

impl SkillAliasValidationError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
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
