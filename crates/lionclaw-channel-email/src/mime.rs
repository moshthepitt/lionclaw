#[cfg(test)]
use anyhow::Context;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use mail_parser::{
    decoders::html::html_to_text, HeaderValue, Message, MessageParser, MimeHeaders, PartType,
};
use serde::{Deserialize, Serialize};

use crate::protocol::{normalize_address, sanitize_header_text, sanitize_subject};

const MAX_MESSAGE_ID_CHARS: usize = 256;
const MAX_REFERENCE_IDS: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmailAddress {
    pub address: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeaderFacts {
    pub sender: EmailAddress,
    pub to: Vec<EmailAddress>,
    pub subject: String,
    pub message_id: Option<String>,
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
    pub received_at: Option<DateTime<Utc>>,
    pub raw_headers: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct ParsedEmail {
    pub facts: HeaderFacts,
    pub text: String,
    pub snippet: String,
    pub attachments: Vec<ParsedAttachment>,
}

#[derive(Debug, Clone)]
pub struct ParsedAttachment {
    pub filename: Option<String>,
    pub mime_type: Option<String>,
    pub content: Vec<u8>,
}

pub fn parse_header_facts(raw_headers: &[u8]) -> Result<HeaderFacts> {
    let headers = MessageParser::default()
        .parse_headers(raw_headers)
        .ok_or_else(|| anyhow!("email headers could not be parsed"))?;
    let sender = headers
        .from()
        .and_then(|from| from.first())
        .and_then(|addr| {
            let address = normalize_address(addr.address.as_deref()?)?;
            Some(EmailAddress {
                address,
                display_name: addr.name.as_deref().and_then(sanitize_header_text),
            })
        })
        .or_else(|| fallback_sender(raw_headers))
        .ok_or_else(|| anyhow!("email sender address could not be parsed"))?;

    let to = headers
        .to()
        .map(|addresses| {
            addresses
                .iter()
                .filter_map(|addr| {
                    let address = normalize_address(addr.address.as_deref()?)?;
                    Some(EmailAddress {
                        address,
                        display_name: addr.name.as_deref().and_then(sanitize_header_text),
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let received_at = headers
        .date()
        .and_then(|date| chrono::DateTime::parse_from_rfc3339(&date.to_rfc3339()).ok())
        .map(|date| date.with_timezone(&Utc));

    Ok(HeaderFacts {
        sender,
        to,
        subject: sanitize_subject(headers.subject()),
        message_id: headers.message_id().and_then(clean_message_id),
        in_reply_to: first_id_header(Some(headers.in_reply_to())),
        references: bounded_reference_ids(Some(headers.references())),
        received_at,
        raw_headers: parse_raw_headers(raw_headers),
    })
}

pub fn parse_full_message(raw: &[u8]) -> Result<ParsedEmail> {
    let message = MessageParser::default()
        .parse(raw)
        .ok_or_else(|| anyhow!("email MIME could not be parsed"))?;
    let facts = parse_header_facts(raw)?;
    let body = message_body_text(&message);
    let stripped = strip_quoted_history(&body);
    let text = if stripped.trim().is_empty() {
        body
    } else {
        stripped
    };
    let snippet = snippet(&text, 512);
    let attachments = message
        .attachments()
        .filter_map(|part| {
            let content = match &part.body {
                PartType::Binary(content) | PartType::InlineBinary(content) => {
                    content.as_ref().to_vec()
                }
                PartType::Message(message) => message.raw_message.as_ref().to_vec(),
                PartType::Text(text) => text.as_bytes().to_vec(),
                PartType::Html(html) => html.as_bytes().to_vec(),
                PartType::Multipart(_) => return None,
            };
            let mime_type = part.content_type().map(|content_type| {
                let subtype = content_type.c_subtype.as_deref().unwrap_or("octet-stream");
                format!("{}/{}", content_type.c_type, subtype)
            });
            Some(ParsedAttachment {
                filename: part.attachment_name().and_then(sanitize_header_text),
                mime_type,
                content,
            })
        })
        .collect();

    Ok(ParsedEmail {
        facts,
        text,
        snippet,
        attachments,
    })
}

fn message_body_text(message: &Message<'_>) -> String {
    let plain = message.body_text(0).unwrap_or_default().into_owned();
    if !plain.trim().is_empty() {
        return plain;
    }

    message
        .body_html(0)
        .map(|html| html_to_text(html.as_ref()))
        .unwrap_or(plain)
}

pub fn header_value<'a>(facts: &'a HeaderFacts, name: &str) -> Option<&'a str> {
    facts
        .raw_headers
        .iter()
        .rev()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

pub fn header_values<'a>(facts: &'a HeaderFacts, name: &str) -> Vec<&'a str> {
    facts
        .raw_headers
        .iter()
        .filter(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
        .collect()
}

pub fn authentication_results_authenticates_sender(
    facts: &HeaderFacts,
    trusted_authserv_id: &str,
) -> bool {
    let Some(from_domain) = address_domain(&facts.sender.address) else {
        return false;
    };
    header_values(facts, "Authentication-Results")
        .into_iter()
        .find(|value| authserv_id_matches(value, trusted_authserv_id))
        .is_some_and(|value| authentication_results_aligns_from_domain(value, &from_domain))
}

pub fn snippet(raw: &str, max_chars: usize) -> String {
    let collapsed = raw
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string();
    if collapsed.chars().count() <= max_chars {
        collapsed
    } else {
        let mut clipped = collapsed.chars().take(max_chars).collect::<String>();
        clipped.push_str("...");
        clipped
    }
}

pub fn strip_quoted_history(raw: &str) -> String {
    let mut kept = Vec::new();
    for line in raw.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with('>')
            || trimmed.eq_ignore_ascii_case("-----original message-----")
            || (trimmed.starts_with("On ") && trimmed.ends_with("wrote:"))
        {
            break;
        }
        kept.push(line);
    }
    kept.join("\n").trim().to_string()
}

fn first_id_header(value: Option<&HeaderValue<'_>>) -> Option<String> {
    id_header_values(value).into_iter().next()
}

fn id_header_values(value: Option<&HeaderValue<'_>>) -> Vec<String> {
    match value {
        Some(HeaderValue::Text(text)) => clean_message_id(text).into_iter().collect(),
        Some(HeaderValue::TextList(values)) => values
            .iter()
            .filter_map(|value| clean_message_id(value))
            .collect(),
        _ => Vec::new(),
    }
}

fn bounded_reference_ids(value: Option<&HeaderValue<'_>>) -> Vec<String> {
    id_header_values(value)
        .into_iter()
        .take(MAX_REFERENCE_IDS)
        .collect()
}

fn clean_message_id(raw: &str) -> Option<String> {
    if raw.chars().any(char::is_control) {
        return None;
    }
    let value = raw.trim().trim_matches('<').trim_matches('>').to_string();
    let valid = !value.trim().is_empty()
        && value.chars().count() <= MAX_MESSAGE_ID_CHARS
        && value
            .chars()
            .all(|ch| !ch.is_whitespace() && !ch.is_control() && !matches!(ch, '<' | '>'));
    valid.then_some(value)
}

fn fallback_sender(raw_headers: &[u8]) -> Option<EmailAddress> {
    let from = parse_raw_headers(raw_headers)
        .into_iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("from"))?
        .1;
    let candidate = from
        .split('<')
        .nth(1)
        .and_then(|value| value.split('>').next())
        .unwrap_or(from.as_str());
    let address = normalize_address(candidate)?;
    Some(EmailAddress {
        address,
        display_name: None,
    })
}

fn authserv_id_matches(value: &str, expected: &str) -> bool {
    value
        .split(';')
        .next()
        .and_then(|prefix| prefix.split_whitespace().next())
        .is_some_and(|authserv_id| authserv_id.eq_ignore_ascii_case(expected))
}

fn authentication_results_aligns_from_domain(value: &str, from_domain: &str) -> bool {
    value
        .split(';')
        .skip(1)
        .any(|part| auth_result_part_aligns_from_domain(part, from_domain))
}

fn auth_result_part_aligns_from_domain(part: &str, from_domain: &str) -> bool {
    if auth_result_is_pass(part, "dmarc") {
        return auth_result_domain_property_matches(part, "header.from", from_domain);
    }
    if auth_result_is_pass(part, "dkim") {
        return auth_result_domain_property_matches(part, "header.d", from_domain)
            || auth_result_domain_property_matches(part, "header.i", from_domain);
    }
    if auth_result_is_pass(part, "spf") {
        return auth_result_domain_property_matches(part, "smtp.mailfrom", from_domain);
    }
    false
}

fn auth_result_is_pass(part: &str, method: &str) -> bool {
    part.split_whitespace()
        .next()
        .and_then(|token| token.split_once('='))
        .is_some_and(|(candidate, result)| {
            candidate.eq_ignore_ascii_case(method) && result.eq_ignore_ascii_case("pass")
        })
}

fn auth_result_domain_property_matches(part: &str, property: &str, from_domain: &str) -> bool {
    part.split_whitespace().any(|token| {
        token
            .trim_matches(|ch| matches!(ch, ',' | ';'))
            .split_once('=')
            .and_then(|(name, value)| {
                name.eq_ignore_ascii_case(property)
                    .then(|| normalize_auth_result_domain(value))
            })
            .flatten()
            .is_some_and(|domain| domain == from_domain)
    })
}

fn address_domain(address: &str) -> Option<String> {
    address
        .split_once('@')
        .map(|(_, domain)| normalize_domain(domain))
        .filter(|domain| !domain.is_empty())
}

fn normalize_auth_result_domain(value: &str) -> Option<String> {
    let value =
        value.trim_matches(|ch| matches!(ch, '<' | '>' | '"' | '\'' | '(' | ')' | '[' | ']'));
    let domain = value
        .split_once('@')
        .map(|(_, domain)| domain)
        .unwrap_or(value);
    let domain = normalize_domain(domain);
    (!domain.is_empty()).then_some(domain)
}

fn normalize_domain(value: &str) -> String {
    value.trim().trim_end_matches('.').to_ascii_lowercase()
}

pub fn parse_raw_headers(raw: &[u8]) -> Vec<(String, String)> {
    let text = String::from_utf8_lossy(raw);
    let mut out: Vec<(String, String)> = Vec::new();
    for line in text.lines() {
        if line.trim_end_matches('\r').is_empty() {
            break;
        }
        if line.starts_with(' ') || line.starts_with('\t') {
            if let Some((_, value)) = out.last_mut() {
                value.push(' ');
                value.push_str(line.trim());
            }
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim();
        if name.is_empty() {
            continue;
        }
        out.push((name.to_string(), value.trim().to_string()));
    }
    out
}

pub fn attachment_summary(attachments: &[ParsedAttachment]) -> Vec<String> {
    attachments
        .iter()
        .map(|attachment| {
            let filename = attachment.filename.as_deref().unwrap_or("(unnamed)");
            let mime_type = attachment
                .mime_type
                .as_deref()
                .unwrap_or("application/octet-stream");
            format!(
                "{filename} ({mime_type}, {} bytes)",
                attachment.content.len()
            )
        })
        .collect()
}

pub fn require_nonempty_body(parsed: &ParsedEmail) -> Result<()> {
    if parsed.text.trim().is_empty() && parsed.attachments.is_empty() {
        anyhow::bail!("email contains no usable text or attachments");
    }
    Ok(())
}

#[cfg(test)]
pub fn parse_headers_for_test(raw: &str) -> HeaderFacts {
    parse_header_facts(raw.as_bytes())
        .context("parse headers")
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sender_subject_and_ids() {
        let facts = parse_headers_for_test(
            "From: Alice <Alice@Example.COM>\r\nSubject: Build failed\r\nMessage-ID: <m1@example.com>\r\n\r\n",
        );
        assert_eq!(facts.sender.address, "alice@example.com");
        assert_eq!(facts.sender.display_name.as_deref(), Some("Alice"));
        assert_eq!(facts.subject, "Build failed");
        assert_eq!(facts.message_id.as_deref(), Some("m1@example.com"));
    }

    #[test]
    fn sanitizes_untrusted_header_display_text() {
        assert_eq!(
            sanitize_header_text(" Alice\r\n  Admission: fake ").as_deref(),
            Some("Alice Admission: fake")
        );
    }

    #[test]
    fn strips_quoted_reply_history() {
        assert_eq!(
            strip_quoted_history("Please fix this.\n\n> old text\n> older"),
            "Please fix this."
        );
    }

    #[test]
    fn parses_html_only_body_as_text() {
        let parsed = parse_full_message(
            b"From: Alice <alice@example.com>\r\nSubject: Hello\r\nContent-Type: text/html; charset=utf-8\r\n\r\n<p>Hello<br>world &amp; team</p>",
        )
        .expect("parse html email");

        assert!(parsed.text.contains("Hello"));
        assert!(parsed.text.contains("world & team"));
        require_nonempty_body(&parsed).expect("html body should count as usable body");
    }

    #[test]
    fn raw_header_parser_stops_at_message_body() {
        let headers = parse_raw_headers(
            b"From: Alice <alice@example.com>\r\nSubject: Hello\r\n\r\nFrom: Mallory <mallory@example.com>\r\n Auto-Submitted: auto-replied\r\n",
        );

        assert_eq!(
            headers,
            vec![
                ("From".to_string(), "Alice <alice@example.com>".to_string()),
                ("Subject".to_string(), "Hello".to_string())
            ]
        );
    }

    #[test]
    fn fallback_sender_does_not_trust_body_from_lines() {
        let err = parse_header_facts(
            b"Subject: Missing sender\r\n\r\nFrom: Mallory <mallory@example.com>\r\n",
        )
        .expect_err("body From line must not become sender identity");

        assert!(
            err.to_string().contains("sender address"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn bounds_message_ids_and_references_from_untrusted_headers() {
        let long_id = format!("{}@example.com", "x".repeat(MAX_MESSAGE_ID_CHARS + 1));
        let references = (0..(MAX_REFERENCE_IDS + 4))
            .map(|index| format!("<m{index}@example.com>"))
            .collect::<Vec<_>>()
            .join(" ");
        let facts = parse_header_facts(
            format!(
                "From: Alice <alice@example.com>\r\nMessage-ID: <{long_id}>\r\nReferences: {references}\r\n\r\n"
            )
            .as_bytes(),
        )
        .expect("parse headers");

        assert!(facts.message_id.is_none());
        assert_eq!(facts.references.len(), MAX_REFERENCE_IDS);
        assert_eq!(facts.references[0], "m0@example.com");
        assert_eq!(
            facts.references[MAX_REFERENCE_IDS - 1],
            format!("m{}@example.com", MAX_REFERENCE_IDS - 1)
        );
    }

    #[test]
    fn authentication_results_requires_trusted_aligned_sender_domain() {
        let facts = parse_headers_for_test(
            "Authentication-Results: mx.example.com; dmarc=pass header.from=example.com; dkim=pass header.d=example.com\r\nFrom: Alice <alice@example.com>\r\nSubject: Hello\r\n\r\n",
        );

        assert!(authentication_results_authenticates_sender(
            &facts,
            "mx.example.com"
        ));
        assert!(!authentication_results_authenticates_sender(
            &facts,
            "other.example.com"
        ));

        let spoof = parse_headers_for_test(
            "Authentication-Results: mx.example.com; dmarc=pass header.from=evil.example\r\nFrom: Alice <alice@example.com>\r\nSubject: Hello\r\n\r\n",
        );
        assert!(!authentication_results_authenticates_sender(
            &spoof,
            "mx.example.com"
        ));
    }

    #[test]
    fn authentication_results_uses_first_trusted_provider_result_only() {
        let facts = parse_headers_for_test(
            "Authentication-Results: mx.example.com; dmarc=fail header.from=example.com\r\nAuthentication-Results: mx.example.com; dmarc=pass header.from=example.com\r\nFrom: Alice <alice@example.com>\r\nSubject: Hello\r\n\r\n",
        );

        assert!(!authentication_results_authenticates_sender(
            &facts,
            "mx.example.com"
        ));
    }

    #[test]
    fn authentication_results_accepts_aligned_dkim_or_spf_fallbacks() {
        let dkim = parse_headers_for_test(
            "Authentication-Results: mx.example.com; dkim=pass header.i=@example.com\r\nFrom: Alice <alice@example.com>\r\nSubject: Hello\r\n\r\n",
        );
        let spf = parse_headers_for_test(
            "Authentication-Results: mx.example.com; spf=pass smtp.mailfrom=alice@example.com\r\nFrom: Alice <alice@example.com>\r\nSubject: Hello\r\n\r\n",
        );

        assert!(authentication_results_authenticates_sender(
            &dkim,
            "mx.example.com"
        ));
        assert!(authentication_results_authenticates_sender(
            &spf,
            "mx.example.com"
        ));
    }

    #[test]
    fn ignores_malformed_message_ids_with_spaces_or_controls() {
        let facts = parse_header_facts(
            b"From: Alice <alice@example.com>\r\nMessage-ID: <bad id@example.com>\r\nIn-Reply-To: <root\x1b@example.com>\r\n\r\n",
        )
        .expect("parse headers");

        assert!(facts.message_id.is_none());
        assert!(facts.in_reply_to.is_none());
    }
}
