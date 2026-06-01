use anyhow::Error;

const OPERATOR_DIAGNOSTIC_MAX_CHARS: usize = 512;
const TRUNCATED_MARKER: &str = " [truncated]";
const SENSITIVE_DIAGNOSTIC_KEYS: &[&str] = &[
    "access_token",
    "refresh_token",
    "id_token",
    "client_secret",
    "password",
];
const SENSITIVE_HEADER_NAMES: &[&str] = &["authorization", "cookie", "set-cookie"];

pub(crate) fn render_operator_error(err: &Error) -> String {
    render_operator_diagnostic(&format!("{err:#}"), false)
        .unwrap_or_else(|| "unknown error".to_string())
}

pub(crate) fn render_operator_diagnostic(raw: &str, source_truncated: bool) -> Option<String> {
    let sanitized = sanitize_diagnostic_text(raw);
    if sanitized.is_empty() {
        return None;
    }
    let (bounded, output_truncated) =
        truncate_diagnostic_text(&sanitized, OPERATOR_DIAGNOSTIC_MAX_CHARS);
    if source_truncated || output_truncated {
        Some(append_truncated_marker(&bounded))
    } else {
        Some(bounded)
    }
}

fn sanitize_diagnostic_text(raw: &str) -> String {
    let header_redacted = raw
        .lines()
        .map(redact_sensitive_header_line)
        .collect::<Vec<_>>()
        .join("\n");
    let mut redacted = header_redacted;
    for key in SENSITIVE_DIAGNOSTIC_KEYS {
        redacted = redact_sensitive_key_values(&redacted, key);
    }
    redacted.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn redact_sensitive_header_line(line: &str) -> String {
    let trimmed = line.trim();
    for name in SENSITIVE_HEADER_NAMES {
        if starts_with_header_name(trimmed, name) {
            return "[redacted sensitive header]".to_string();
        }
    }
    trimmed.to_string()
}

fn starts_with_header_name(line: &str, name: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    let Some(rest) = lower.strip_prefix(name) else {
        return false;
    };
    rest.trim_start().starts_with(':')
}

fn redact_sensitive_key_values(text: &str, key: &str) -> String {
    let mut redacted = text.to_string();
    let replacement = "[redacted]";
    let mut search_start = 0;
    loop {
        let lower = redacted.to_ascii_lowercase();
        let Some(relative_key_start) = lower[search_start..].find(key) else {
            return redacted;
        };
        let after_key = search_start + relative_key_start + key.len();
        let Some((value_start, value_end)) = sensitive_value_span(&redacted, after_key) else {
            search_start = after_key;
            continue;
        };
        redacted.replace_range(value_start..value_end, replacement);
        search_start = value_start + replacement.len();
    }
}

fn sensitive_value_span(text: &str, mut index: usize) -> Option<(usize, usize)> {
    index = skip_diagnostic_spacing_or_quotes(text, index);
    let separator = text[index..].chars().next()?;
    if separator != '=' && separator != ':' {
        return None;
    }
    index += separator.len_utf8();
    index = skip_diagnostic_spacing_or_quotes(text, index);
    let start = index;
    while index < text.len() {
        let ch = text[index..].chars().next()?;
        if ch.is_whitespace() || matches!(ch, '&' | ',' | ';' | '"' | '\'') {
            break;
        }
        index += ch.len_utf8();
    }
    (start < index).then_some((start, index))
}

fn skip_diagnostic_spacing_or_quotes(text: &str, mut index: usize) -> usize {
    while index < text.len() {
        let Some(ch) = text[index..].chars().next() else {
            return index;
        };
        if !(ch.is_whitespace() || matches!(ch, '"' | '\'')) {
            return index;
        }
        index += ch.len_utf8();
    }
    index
}

fn truncate_diagnostic_text(text: &str, max_chars: usize) -> (String, bool) {
    let mut chars = text.chars();
    let bounded = chars.by_ref().take(max_chars).collect::<String>();
    (bounded, chars.next().is_some())
}

fn append_truncated_marker(text: &str) -> String {
    let max_prefix_chars = OPERATOR_DIAGNOSTIC_MAX_CHARS.saturating_sub(TRUNCATED_MARKER.len());
    let prefix = text.chars().take(max_prefix_chars).collect::<String>();
    format!("{prefix}{TRUNCATED_MARKER}")
}

#[cfg(test)]
mod tests {
    use anyhow::Context as _;

    use super::{render_operator_diagnostic, render_operator_error, OPERATOR_DIAGNOSTIC_MAX_CHARS};

    #[test]
    fn diagnostics_redact_spaced_secret_assignments() {
        let rendered = render_operator_diagnostic(
            "invalid_grant refresh_token : secret-refresh client_secret = secret-client",
            false,
        )
        .expect("diagnostic");

        assert!(rendered.contains("invalid_grant"));
        assert!(rendered.contains("[redacted]"));
        assert!(!rendered.contains("secret-refresh"));
        assert!(!rendered.contains("secret-client"));
    }

    #[test]
    fn diagnostics_redact_pretty_printed_json_secrets() {
        let rendered = render_operator_diagnostic(
            r#"{
  "error": "invalid_grant",
  "refresh_token": "secret-refresh",
  "client_secret": "secret-client"
}"#,
            false,
        )
        .expect("diagnostic");

        assert!(rendered.contains("invalid_grant"));
        assert!(rendered.contains("[redacted]"));
        assert!(!rendered.contains("secret-refresh"));
        assert!(!rendered.contains("secret-client"));
    }

    #[test]
    fn diagnostics_redact_sensitive_headers_and_truncate() {
        let rendered = render_operator_diagnostic(
            &format!("Authorization : Bearer secret-token\n{}", "x".repeat(600)),
            true,
        )
        .expect("diagnostic");

        assert!(rendered.contains("[redacted sensitive header]"));
        assert!(rendered.contains("[truncated]"));
        assert!(rendered.chars().count() <= OPERATOR_DIAGNOSTIC_MAX_CHARS);
        assert!(!rendered.contains("secret-token"));
    }

    #[test]
    fn operator_error_renders_sanitized_error_chain() {
        let err = Err::<(), _>(anyhow::anyhow!("refresh_token = secret-refresh"))
            .context("failed to obtain XOAUTH2 access token for SMTP")
            .expect_err("error chain");

        let rendered = render_operator_error(&err);

        assert!(rendered.contains("failed to obtain XOAUTH2 access token for SMTP"));
        assert!(rendered.contains("[redacted]"));
        assert!(!rendered.contains("secret-refresh"));
    }
}
