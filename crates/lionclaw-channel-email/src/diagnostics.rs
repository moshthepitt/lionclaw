use anyhow::Error;
use base64::{
    engine::general_purpose::{STANDARD, STANDARD_NO_PAD},
    Engine as _,
};

const OPERATOR_DIAGNOSTIC_MAX_CHARS: usize = 512;
const TRUNCATED_MARKER: &str = " [truncated]";
const SENSITIVE_DIAGNOSTIC_KEYS: &[&str] = &[
    "access_token",
    "access-token",
    "access token",
    "accesstoken",
    "refresh_token",
    "refresh-token",
    "refresh token",
    "refreshtoken",
    "id_token",
    "id-token",
    "id token",
    "idtoken",
    "client_secret",
    "client-secret",
    "client secret",
    "clientsecret",
    "authorization_code",
    "authorization-code",
    "authorization code",
    "authorizationcode",
    "auth_code",
    "auth-code",
    "auth code",
    "authcode",
    "code_verifier",
    "code-verifier",
    "code verifier",
    "codeverifier",
    "password",
];
const CONDITIONAL_SENSITIVE_DIAGNOSTIC_KEYS: &[&str] = &["code"];
const SENSITIVE_HEADER_NAMES: &[&str] = &[
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
];
const SENSITIVE_CREDENTIAL_SCHEMES: &[&str] = &["bearer", "basic"];

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
        redacted = redact_sensitive_key_values(&redacted, key, false, always_sensitive);
    }
    for key in CONDITIONAL_SENSITIVE_DIAGNOSTIC_KEYS {
        redacted = redact_sensitive_key_values(&redacted, key, true, looks_like_credential_value);
    }
    redacted = redact_sensitive_credential_schemes(&redacted);
    redacted.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn redact_sensitive_header_line(line: &str) -> String {
    let trimmed = line.trim();
    if contains_sensitive_header(trimmed) {
        return "[redacted sensitive header]".to_string();
    }
    trimmed.to_string()
}

fn contains_sensitive_header(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    SENSITIVE_HEADER_NAMES
        .iter()
        .any(|name| contains_header_name(&lower, name))
}

fn contains_header_name(line: &str, name: &str) -> bool {
    let mut search_start = 0;
    while let Some(relative_start) = line[search_start..].find(name) {
        let start = search_start + relative_start;
        let after_name = start + name.len();
        let starts_at_boundary = start == 0 || !is_header_name_byte(line.as_bytes()[start - 1]);
        if starts_at_boundary && header_separator_follows(&line[after_name..]) {
            return true;
        }
        search_start = after_name;
    }
    false
}

fn header_separator_follows(text: &str) -> bool {
    let text = text.trim_start();
    if let Some(rest) = text.strip_prefix('"').or_else(|| text.strip_prefix('\'')) {
        return header_value_separator_follows(rest);
    }
    header_value_separator_follows(text)
}

fn header_value_separator_follows(text: &str) -> bool {
    matches!(text.trim_start().chars().next(), Some(':' | '='))
}

fn is_header_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'-'
}

fn redact_sensitive_key_values(
    text: &str,
    key: &str,
    require_boundary: bool,
    should_redact: fn(&str) -> bool,
) -> String {
    let mut redacted = text.to_string();
    let replacement = "[redacted]";
    let mut search_start = 0;
    loop {
        let lower = redacted.to_ascii_lowercase();
        let Some(relative_key_start) = lower[search_start..].find(key) else {
            return redacted;
        };
        let key_start = search_start + relative_key_start;
        let after_key = key_start + key.len();
        if require_boundary && !sensitive_key_has_boundaries(&lower, key_start, after_key) {
            search_start = after_key;
            continue;
        }
        let Some((value_start, value_end)) = sensitive_value_span(&redacted, after_key) else {
            search_start = after_key;
            continue;
        };
        if !should_redact(&redacted[value_start..value_end]) {
            search_start = value_end;
            continue;
        }
        redacted.replace_range(value_start..value_end, replacement);
        search_start = value_start + replacement.len();
    }
}

fn sensitive_key_has_boundaries(text: &str, start: usize, after_key: usize) -> bool {
    let starts_at_boundary = start == 0 || !is_sensitive_key_byte(text.as_bytes()[start - 1]);
    let ends_at_boundary = text[after_key..]
        .chars()
        .next()
        .is_none_or(|ch| !is_sensitive_key_char(ch));
    starts_at_boundary && ends_at_boundary
}

fn is_sensitive_key_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')
}

fn is_sensitive_key_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-')
}

fn always_sensitive(_value: &str) -> bool {
    true
}

fn looks_like_credential_value(value: &str) -> bool {
    value.chars().count() >= 16 && value.chars().any(|ch| ch.is_ascii_alphanumeric())
}

fn redact_sensitive_credential_schemes(text: &str) -> String {
    let mut redacted = text.to_string();
    let replacement = "[redacted]";
    let mut search_start = 0;
    loop {
        let lower = redacted.to_ascii_lowercase();
        let Some((scheme_start, scheme)) = next_credential_scheme(&lower, search_start) else {
            return redacted;
        };
        let after_scheme = scheme_start + scheme.len();
        let value_start = skip_diagnostic_spacing(&redacted, after_scheme);
        let Some((value_start, value_end)) = credential_scheme_value_span(&redacted, value_start)
        else {
            search_start = after_scheme;
            continue;
        };
        if credential_scheme_value_is_sensitive(scheme, &redacted[value_start..value_end]) {
            redacted.replace_range(value_start..value_end, replacement);
            search_start = value_start + replacement.len();
        } else {
            search_start = after_scheme;
        }
    }
}

fn next_credential_scheme(text: &str, search_start: usize) -> Option<(usize, &'static str)> {
    SENSITIVE_CREDENTIAL_SCHEMES
        .iter()
        .filter_map(|scheme| next_credential_scheme_for(text, search_start, scheme))
        .min_by_key(|(start, _)| *start)
}

fn next_credential_scheme_for(
    text: &str,
    mut search_start: usize,
    scheme: &'static str,
) -> Option<(usize, &'static str)> {
    while let Some(relative) = text[search_start..].find(scheme) {
        let start = search_start + relative;
        let after_scheme = start + scheme.len();
        if credential_scheme_has_boundaries(text, start, after_scheme) {
            return Some((start, scheme));
        }
        search_start = after_scheme;
    }
    None
}

fn credential_scheme_has_boundaries(text: &str, start: usize, after_scheme: usize) -> bool {
    let starts_at_boundary = start == 0 || !text.as_bytes()[start - 1].is_ascii_alphanumeric();
    let followed_by_spacing = text[after_scheme..]
        .chars()
        .next()
        .is_some_and(char::is_whitespace);
    starts_at_boundary && followed_by_spacing
}

fn credential_scheme_value_span(text: &str, mut index: usize) -> Option<(usize, usize)> {
    if let Some(quote) = text[index..]
        .chars()
        .next()
        .filter(|ch| matches!(ch, '"' | '\''))
    {
        return quoted_sensitive_value_span(text, index, quote);
    }

    let start = index;
    while index < text.len() {
        let ch = text[index..].chars().next()?;
        if ch.is_whitespace() || matches!(ch, ',' | ';' | '"' | '\'') {
            break;
        }
        index += ch.len_utf8();
    }
    (start < index).then_some((start, index))
}

fn credential_scheme_value_is_sensitive(scheme: &str, value: &str) -> bool {
    match scheme {
        "basic" => looks_like_basic_credential(value),
        "bearer" => value.chars().count() >= 8,
        _ => false,
    }
}

fn looks_like_basic_credential(value: &str) -> bool {
    decode_base64(value).is_some_and(|decoded| decoded.contains(&b':'))
}

fn decode_base64(value: &str) -> Option<Vec<u8>> {
    STANDARD
        .decode(value)
        .or_else(|_| STANDARD_NO_PAD.decode(value))
        .ok()
}

fn sensitive_value_span(text: &str, mut index: usize) -> Option<(usize, usize)> {
    index = skip_diagnostic_spacing_or_quotes(text, index);
    let separator = text[index..].chars().next()?;
    if separator != '=' && separator != ':' {
        return None;
    }
    index += separator.len_utf8();
    index = skip_diagnostic_spacing(text, index);
    if let Some(quote) = text[index..]
        .chars()
        .next()
        .filter(|ch| matches!(ch, '"' | '\''))
    {
        return quoted_sensitive_value_span(text, index, quote);
    }
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

fn quoted_sensitive_value_span(
    text: &str,
    mut index: usize,
    quote: char,
) -> Option<(usize, usize)> {
    index += quote.len_utf8();
    let start = index;
    let mut escaped = false;
    while index < text.len() {
        let ch = text[index..].chars().next()?;
        if escaped {
            escaped = false;
            index += ch.len_utf8();
            continue;
        }
        if ch == '\\' {
            escaped = true;
            index += ch.len_utf8();
            continue;
        }
        if ch == quote {
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

fn skip_diagnostic_spacing(text: &str, mut index: usize) -> usize {
    while index < text.len() {
        let Some(ch) = text[index..].chars().next() else {
            return index;
        };
        if !ch.is_whitespace() {
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
            "invalid_grant refresh_token : secret-refresh client_secret = secret-client code_verifier=secret-verifier",
            false,
        )
        .expect("diagnostic");

        assert!(rendered.contains("invalid_grant"));
        assert!(rendered.contains("[redacted]"));
        assert!(!rendered.contains("secret-refresh"));
        assert!(!rendered.contains("secret-client"));
        assert!(!rendered.contains("secret-verifier"));
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
    fn diagnostics_redact_quoted_sensitive_values_with_spaces() {
        let rendered = render_operator_diagnostic(
            r#"password: "secret password with spaces" client_secret='client secret with spaces'"#,
            false,
        )
        .expect("diagnostic");

        assert_eq!(
            rendered,
            r#"password: "[redacted]" client_secret='[redacted]'"#
        );
        assert!(!rendered.contains("secret password"));
        assert!(!rendered.contains("client secret"));
    }

    #[test]
    fn diagnostics_redact_common_oauth_secret_key_spellings() {
        let rendered = render_operator_diagnostic(
            "refreshToken=secret-refresh client-secret: secret-client access token = secret-access codeVerifier: secret-verifier",
            false,
        )
        .expect("diagnostic");

        assert!(rendered.contains("[redacted]"));
        assert!(!rendered.contains("secret-refresh"));
        assert!(!rendered.contains("secret-client"));
        assert!(!rendered.contains("secret-access"));
        assert!(!rendered.contains("secret-verifier"));
    }

    #[test]
    fn diagnostics_redact_oauth_authorization_code_echoes() {
        let rendered = render_operator_diagnostic(
            "invalid_grant code=4/0AbCdEfGhIjKlMnOpQrStUvWxYz authorization_code=secret-auth-code code_verifier=secret-verifier",
            false,
        )
        .expect("diagnostic");

        assert!(rendered.contains("invalid_grant"));
        assert!(rendered.contains("[redacted]"));
        assert!(!rendered.contains("0AbCdEfGhIjKlMnOpQrStUvWxYz"));
        assert!(!rendered.contains("secret-auth-code"));
        assert!(!rendered.contains("secret-verifier"));
    }

    #[test]
    fn diagnostics_keep_plain_error_code_context() {
        let rendered =
            render_operator_diagnostic("provider error code: invalid_grant status_code=400", false)
                .expect("diagnostic");

        assert_eq!(
            rendered,
            "provider error code: invalid_grant status_code=400"
        );
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
    fn diagnostics_redact_prefixed_sensitive_headers() {
        let rendered = render_operator_diagnostic(
            "> Authorization: Bearer secret-token\nDEBUG Proxy-Authorization : Basic secret-proxy",
            false,
        )
        .expect("diagnostic");

        assert_eq!(
            rendered,
            "[redacted sensitive header] [redacted sensitive header]"
        );
        assert!(!rendered.contains("secret-token"));
        assert!(!rendered.contains("secret-proxy"));
    }

    #[test]
    fn diagnostics_redact_quoted_sensitive_header_names() {
        let rendered = render_operator_diagnostic(
            r#"{"Authorization": "Bearer secret-token", "error": "invalid_grant"}
'Set-Cookie' : "session=secret-cookie"
DEBUG "Proxy-Authorization" : "Basic secret-proxy""#,
            false,
        )
        .expect("diagnostic");

        assert_eq!(
            rendered,
            "[redacted sensitive header] [redacted sensitive header] [redacted sensitive header]"
        );
        assert!(!rendered.contains("secret-token"));
        assert!(!rendered.contains("secret-cookie"));
        assert!(!rendered.contains("secret-proxy"));
    }

    #[test]
    fn diagnostics_redact_sensitive_header_assignments() {
        let rendered = render_operator_diagnostic(
            "Authorization=Bearer secret-token\nCookie = session=secret-cookie\nProxy-Authorization = Basic secret-proxy",
            false,
        )
        .expect("diagnostic");

        assert_eq!(
            rendered,
            "[redacted sensitive header] [redacted sensitive header] [redacted sensitive header]"
        );
        assert!(!rendered.contains("secret-token"));
        assert!(!rendered.contains("secret-cookie"));
        assert!(!rendered.contains("secret-proxy"));
    }

    #[test]
    fn diagnostics_redact_standalone_credential_scheme_values() {
        let rendered = render_operator_diagnostic(
            "invalid_grant xbearer ignored Bearer lowercaseopaque revoked Basic dXNlcjpwYXNz",
            false,
        )
        .expect("diagnostic");

        assert_eq!(
            rendered,
            "invalid_grant xbearer ignored Bearer [redacted] revoked Basic [redacted]"
        );
        assert!(!rendered.contains("lowercaseopaque"));
        assert!(!rendered.contains("dXNlcjpwYXNz"));
    }

    #[test]
    fn diagnostics_redact_short_basic_credentials_without_redacting_plain_words() {
        let rendered =
            render_operator_diagnostic("Basic dTpw failed; Basic auth still enabled", false)
                .expect("diagnostic");

        assert_eq!(
            rendered,
            "Basic [redacted] failed; Basic auth still enabled"
        );
        assert!(!rendered.contains("dTpw"));
    }

    #[test]
    fn diagnostics_keep_plain_credential_scheme_words() {
        let rendered = render_operator_diagnostic(
            "OAuth2 token endpoint omitted token_type; email XOAUTH2 requires Bearer access tokens",
            false,
        )
        .expect("diagnostic");

        assert_eq!(
            rendered,
            "OAuth2 token endpoint omitted token_type; email XOAUTH2 requires Bearer access tokens"
        );
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
