//! Identifier newtypes with zenith-compatible validation.
//!
//! Validation rules ported from Zenith (Apache-2.0, Intelligent Internet)
//! `models.py` — `ASSERTION_ID_REGEX`, `TASK_ID_REGEX`, `SKILL_NAME_REGEX` —
//! hand-rolled as charset checks so the model stays regex-free.

use std::fmt;

use serde::{Deserialize, Serialize};

macro_rules! id_type {
    ($name:ident, $validate:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub fn new(raw: impl Into<String>) -> Result<Self, IdError> {
                let raw = raw.into();
                $validate(&raw)?;
                Ok(Self(raw))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdError;

            fn try_from(raw: String) -> Result<Self, IdError> {
                Self::new(raw)
            }
        }

        impl From<$name> for String {
            fn from(id: $name) -> String {
                id.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct IdError(String);

/// `^[A-Z][A-Z0-9-]+$` — uppercase-first, then uppercase/digit/hyphen, len >= 2.
fn validate_assertion_id(raw: &str) -> Result<(), IdError> {
    let mut chars = raw.chars();
    let first_ok = chars.next().is_some_and(|c| c.is_ascii_uppercase());
    let rest: Vec<char> = chars.collect();
    if !first_ok
        || rest.is_empty()
        || !rest
            .iter()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || *c == '-')
    {
        return Err(IdError(format!(
            "assertion id '{raw}' must match ^[A-Z][A-Z0-9-]+$"
        )));
    }
    Ok(())
}

fn validate_requirement_id(raw: &str) -> Result<(), IdError> {
    validate_assertion_id(raw).map_err(|_| {
        IdError(format!(
            "requirement id '{raw}' must match ^[A-Z][A-Z0-9-]+$"
        ))
    })
}

/// `^[A-Za-z][A-Za-z0-9_-]*$`.
fn validate_task_id(raw: &str) -> Result<(), IdError> {
    let mut chars = raw.chars();
    let first_ok = chars.next().is_some_and(|c| c.is_ascii_alphabetic());
    if !first_ok || !chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-') {
        return Err(IdError(format!(
            "task id '{raw}' must match ^[A-Za-z][A-Za-z0-9_-]*$"
        )));
    }
    Ok(())
}

/// `^[a-z][a-z0-9_-]*$` — role and oracle names (zenith skill names).
fn validate_component_name(raw: &str) -> Result<(), IdError> {
    let mut chars = raw.chars();
    let first_ok = chars.next().is_some_and(|c| c.is_ascii_lowercase());
    if !first_ok
        || !chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
    {
        return Err(IdError(format!(
            "name '{raw}' must match ^[a-z][a-z0-9_-]*$"
        )));
    }
    Ok(())
}

id_type!(AssertionId, validate_assertion_id, "Contract assertion id.");
id_type!(
    RequirementId,
    validate_requirement_id,
    "Objective requirement id."
);
id_type!(TaskId, validate_task_id, "Plan task id.");
id_type!(RoleName, validate_component_name, "Mission-type role name.");
id_type!(
    OracleName,
    validate_component_name,
    "Mission-type oracle name."
);
id_type!(
    InputName,
    validate_component_name,
    "Mission-type prepared input name."
);

/// Effect identity: the lowercase SHA-256 digest of a durable request.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct EffectId(String);

impl EffectId {
    pub fn parse(raw: impl Into<String>) -> Result<Self, IdError> {
        let raw = raw.into();
        if raw.len() != 64
            || !raw
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(IdError(format!(
                "effect id '{raw}' must be 64 lowercase hexadecimal characters"
            )));
        }
        Ok(Self(raw))
    }

    pub fn for_parts(parts: &[&str]) -> Self {
        use sha2::{Digest, Sha256};

        Self(lowercase_hex(&Sha256::digest(
            parts.join("\u{1f}").as_bytes(),
        )))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn resource_name(&self) -> String {
        format!("lionclaw-effect-{}", self.0)
    }
}

impl TryFrom<String> for EffectId {
    type Error = IdError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        Self::parse(raw)
    }
}

impl From<EffectId> for String {
    fn from(id: EffectId) -> Self {
        id.0
    }
}

impl fmt::Display for EffectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Mission id: `m` + 12 hex chars, derived from workspace, objective, and
/// creation time without RNG.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct MissionId(String);

impl TryFrom<String> for MissionId {
    type Error = IdError;

    fn try_from(raw: String) -> Result<Self, IdError> {
        Self::parse(raw)
    }
}

impl From<MissionId> for String {
    fn from(id: MissionId) -> String {
        id.0
    }
}

impl MissionId {
    pub fn parse(raw: impl Into<String>) -> Result<Self, IdError> {
        let raw = raw.into();
        let hex = raw.strip_prefix('m').unwrap_or("");
        if hex.len() != 12 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(IdError(format!(
                "mission id '{raw}' must be 'm' + 12 hex chars"
            )));
        }
        Ok(Self(raw))
    }

    pub fn from_digest_prefix(digest_hex: &str) -> Self {
        Self(format!("m{}", &digest_hex[..12]))
    }

    pub fn for_creation(workspace: &str, objective: &str, now_ms: i64) -> Self {
        use sha2::{Digest, Sha256};

        Self::from_digest_prefix(&lowercase_hex(&Sha256::digest(
            format!("{workspace}\u{1f}{objective}\u{1f}{now_ms}").as_bytes(),
        )))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MissionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The first 12 chars of a sha/digest, for compact human display.
fn lowercase_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

pub fn short_hex(hex: &str) -> String {
    hex.chars().take(12).collect()
}

/// The terminal reviewer's runner tag: its attempt-dir name and the task id
/// scripted reviewers (tests, self-test) route on. Never a ledger id — review
/// events carry no `task_id` — but it shares the attempt-dir namespace with
/// plan tasks, so plan validation reserves it (a crashed worker attempt named
/// like this could otherwise leave dirs the reviewer would silently reuse).
pub const TERMINAL_REVIEW_TASK_TAG: &str = "terminal-review";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assertion_id_rules() {
        assert!(AssertionId::new("TESTS-PASS").is_ok());
        assert!(AssertionId::new("A1").is_ok());
        assert!(AssertionId::new("A").is_err()); // len >= 2
        assert!(AssertionId::new("a-lower").is_err());
        assert!(AssertionId::new("1ST").is_err());
        assert!(AssertionId::new("HAS SPACE").is_err());
        assert!(AssertionId::new("").is_err());
    }

    #[test]
    fn task_id_rules() {
        assert!(TaskId::new("fix").is_ok());
        assert!(TaskId::new("Fix_bug-2").is_ok());
        assert!(TaskId::new("f").is_ok()); // single char allowed
        assert!(TaskId::new("2fix").is_err());
        assert!(TaskId::new("fix.bug").is_err());
        assert!(TaskId::new("").is_err());
    }

    #[test]
    fn component_name_rules() {
        assert!(RoleName::new("implementer").is_ok());
        assert!(OracleName::new("cargo-test").is_ok());
        assert!(RoleName::new("Implementer").is_err());
        assert!(OracleName::new("-x").is_err());
    }

    #[test]
    fn mission_id_roundtrip() {
        let id = MissionId::from_digest_prefix("abcdef0123456789");
        assert_eq!(id.as_str(), "mabcdef012345");
        assert!(MissionId::parse(id.as_str()).is_ok());
        assert!(MissionId::parse("nope").is_err());
    }

    #[test]
    fn effect_id_is_a_path_safe_sha256_digest() {
        let id = EffectId::for_parts(&["mission", "request"]);
        assert_eq!(id.as_str().len(), 64);
        assert!(EffectId::parse(id.as_str()).is_ok());
        assert!(EffectId::parse("ABC").is_err());
        assert!(EffectId::parse("../escape").is_err());
    }
}
