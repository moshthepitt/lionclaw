#[cfg(test)]
use anyhow::Context;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use mail_parser::{
    decoders::html::html_to_text, HeaderValue, Message, MessageParser, MimeHeaders, PartType,
};
use serde::{Deserialize, Serialize};

use crate::protocol::{normalize_address, sanitize_subject};

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
                display_name: addr.name.as_deref().map(str::to_string),
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
                        display_name: addr.name.as_deref().map(str::to_string),
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
        message_id: headers.message_id().map(clean_message_id),
        in_reply_to: first_id_header(Some(headers.in_reply_to())),
        references: id_header_values(Some(headers.references())),
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
                filename: part.attachment_name().map(str::to_string),
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
        Some(HeaderValue::Text(text)) => vec![clean_message_id(text)],
        Some(HeaderValue::TextList(values)) => {
            values.iter().map(|value| clean_message_id(value)).collect()
        }
        _ => Vec::new(),
    }
}

fn clean_message_id(raw: &str) -> String {
    raw.trim()
        .trim_matches('<')
        .trim_matches('>')
        .replace(['\r', '\n'], "")
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

pub fn parse_raw_headers(raw: &[u8]) -> Vec<(String, String)> {
    let text = String::from_utf8_lossy(raw);
    let mut out: Vec<(String, String)> = Vec::new();
    for line in text.lines() {
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
        assert_eq!(facts.subject, "Build failed");
        assert_eq!(facts.message_id.as_deref(), Some("m1@example.com"));
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
}
