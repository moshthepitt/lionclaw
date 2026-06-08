use anyhow::{anyhow, bail, Result};

use crate::protocol::ProjectedContextClass;

pub(crate) const MAX_PROFILE_BODY_BYTES: usize = 8 * 1024;
pub(crate) const MAX_MEMORY_BODY_BYTES: usize = 8 * 1024;
pub(crate) const MAX_TITLE_BYTES: usize = 160;
pub(crate) const MAX_TAGS: usize = 16;
pub(crate) const MAX_TAG_BYTES: usize = 64;
pub(crate) const MAX_QUERY_BYTES: usize = 4 * 1024;
pub(crate) const DEFAULT_LIST_LIMIT: usize = 100;
pub(crate) const MAX_LIST_LIMIT: usize = 100;

pub(crate) const ASSISTANT_PROFILE_SLOTS: &[&str] =
    &["identity", "style", "boundaries", "workflow", "defaults"];
pub(crate) const USER_PROFILE_SLOTS: &[&str] = &[
    "identity",
    "preferences",
    "environment",
    "working_style",
    "standing_requests",
];

pub(crate) fn required_profile_slots(class: ProjectedContextClass) -> &'static [&'static str] {
    match class {
        ProjectedContextClass::AssistantProfile => ASSISTANT_PROFILE_SLOTS,
        ProjectedContextClass::UserProfile => USER_PROFILE_SLOTS,
        ProjectedContextClass::Memory => &[],
    }
}

pub(crate) fn validate_profile_class(class: ProjectedContextClass) -> Result<()> {
    match class {
        ProjectedContextClass::AssistantProfile | ProjectedContextClass::UserProfile => Ok(()),
        ProjectedContextClass::Memory => bail!("memory is not a profile class"),
    }
}

pub(crate) fn validate_profile_slot(class: ProjectedContextClass, slot: &str) -> Result<String> {
    validate_profile_class(class)?;
    let slot = slot.trim();
    if required_profile_slots(class).contains(&slot) {
        Ok(slot.to_string())
    } else {
        bail!(
            "{} slot '{}' is invalid; expected one of: {}",
            class.title(),
            slot,
            required_profile_slots(class).join(", ")
        );
    }
}

pub(crate) fn validate_scope(raw: &str) -> Result<String> {
    let scope = raw.trim();
    if scope == "global" {
        return Ok(scope.to_string());
    }
    let Some(scope_id) = scope.strip_prefix("project:") else {
        bail!("scope must be 'global' or 'project:<scope_id>'");
    };
    validate_scope_id(scope_id)?;
    Ok(scope.to_string())
}

pub(crate) fn projection_scopes(project_scope: Option<&str>) -> Vec<String> {
    let mut scopes = vec!["global".to_string()];
    if let Some(project_scope) = project_scope {
        if validate_scope_id(project_scope).is_ok() {
            scopes.push(format!("project:{project_scope}"));
        }
    }
    scopes
}

pub(crate) fn validate_scope_id(scope_id: &str) -> Result<()> {
    if !audit_safe_visible_handle(scope_id) {
        bail!(
            "project scope id must be 1..128 visible ASCII bytes with no whitespace or path separators"
        );
    }
    if looks_like_windows_absolute_path(scope_id) {
        bail!("project scope id must not look like a host absolute path");
    }
    Ok(())
}

pub(crate) fn validate_body(label: &str, raw: &str, max_bytes: usize) -> Result<String> {
    let body = raw.trim();
    if body.is_empty() {
        bail!("{label} body is required");
    }
    validate_text_chars(label, body)?;
    if body.len() > max_bytes {
        bail!("{label} body exceeds {max_bytes} bytes");
    }
    Ok(body.to_string())
}

pub(crate) fn validate_title(raw: Option<&str>) -> Result<Option<String>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let title = raw.trim();
    if title.is_empty() {
        return Ok(None);
    }
    validate_text_chars("memory title", title)?;
    if title.len() > MAX_TITLE_BYTES {
        bail!("memory title exceeds {MAX_TITLE_BYTES} bytes");
    }
    Ok(Some(title.to_string()))
}

pub(crate) fn validate_tags(raw: &[String]) -> Result<Vec<String>> {
    if raw.len() > MAX_TAGS {
        bail!("memory tags exceed maximum of {MAX_TAGS}");
    }
    let mut tags = Vec::new();
    for tag in raw {
        let tag = tag.trim();
        if tag.is_empty() {
            continue;
        }
        if tag.len() > MAX_TAG_BYTES {
            bail!("memory tag '{tag}' exceeds {MAX_TAG_BYTES} bytes");
        }
        if !tag
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && byte != b',' && byte != b'/' && byte != b'\\')
        {
            bail!("memory tag '{tag}' must be visible ASCII without comma or path separators");
        }
        if !tags.iter().any(|existing| existing == tag) {
            tags.push(tag.to_string());
        }
    }
    Ok(tags)
}

pub(crate) fn validate_query(raw: &str) -> Result<String> {
    let query = raw.trim();
    if query.is_empty() {
        bail!("memory search query is required");
    }
    validate_text_chars("memory search query", query)?;
    if query.len() > MAX_QUERY_BYTES {
        bail!("memory search query exceeds {MAX_QUERY_BYTES} bytes");
    }
    Ok(query.to_string())
}

pub(crate) fn validate_limit(limit: Option<usize>) -> Result<usize> {
    let limit = limit.unwrap_or(DEFAULT_LIST_LIMIT);
    if limit == 0 || limit > MAX_LIST_LIMIT {
        bail!("limit must be between 1 and {MAX_LIST_LIMIT}");
    }
    Ok(limit)
}

pub(crate) fn validate_record_id(id: &str) -> Result<String> {
    if !audit_safe_visible_handle(id) {
        return Err(anyhow!(
            "record id must be 1..128 visible ASCII bytes with no whitespace or path separators"
        ));
    }
    Ok(id.to_string())
}

pub(crate) fn audit_safe_visible_handle(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| {
            byte.is_ascii_graphic() && !byte.is_ascii_whitespace() && byte != b'/' && byte != b'\\'
        })
}

pub(crate) fn validate_text_chars(label: &str, value: &str) -> Result<()> {
    if value
        .chars()
        .any(|ch| ch.is_control() && !matches!(ch, '\n' | '\r' | '\t'))
    {
        bail!("{label} contains unsupported control characters");
    }
    Ok(())
}

fn looks_like_windows_absolute_path(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

pub(crate) fn cap_utf8(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_string();
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_global_and_project_scope() {
        assert_eq!(validate_scope("global").expect("global"), "global");
        assert_eq!(
            validate_scope("project:abc_123").expect("project"),
            "project:abc_123"
        );
    }

    #[test]
    fn rejects_unsafe_project_scope_ids() {
        for scope in [
            "project:",
            "project:has space",
            "project:../x",
            "project:/tmp/project",
            "project:C:",
            "project:line\nbreak",
        ] {
            assert!(
                validate_scope(scope).is_err(),
                "scope should be rejected: {scope}"
            );
        }
    }

    #[test]
    fn validates_profile_slots_by_class() {
        assert!(validate_profile_slot(ProjectedContextClass::AssistantProfile, "style").is_ok());
        assert!(
            validate_profile_slot(ProjectedContextClass::AssistantProfile, "preferences").is_err()
        );
        assert!(validate_profile_slot(ProjectedContextClass::UserProfile, "preferences").is_ok());
        assert!(validate_profile_slot(ProjectedContextClass::Memory, "identity").is_err());
    }

    #[test]
    fn validates_body_and_query_limits() {
        assert!(validate_body("profile", "hello", 8).is_ok());
        assert!(validate_body("profile", "    ", 8).is_err());
        assert!(validate_body("profile", "012345678", 8).is_err());
        assert!(validate_query("private context").is_ok());
        assert!(validate_query("").is_err());
    }

    #[test]
    fn deduplicates_and_validates_tags() {
        let tags = validate_tags(&["rust".to_string(), "rust".to_string(), "local".to_string()])
            .expect("tags");
        assert_eq!(tags, vec!["rust".to_string(), "local".to_string()]);
        assert!(validate_tags(&["bad/tag".to_string()]).is_err());
    }
}
