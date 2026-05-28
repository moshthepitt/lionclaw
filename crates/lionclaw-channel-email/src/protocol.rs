use sha2::{Digest, Sha256};

pub const CHANNEL_ID: &str = "email";
pub const INBOUND_TRIGGER: &str = "command";
pub const INBOUND_SESSION_BINDING: &str = "thread_actor";
const MAX_HEADER_TEXT_CHARS: usize = 512;

pub fn normalize_address(raw: &str) -> Option<String> {
    let addr = raw.trim().trim_matches('<').trim_matches('>').trim();
    let (local, domain) = addr.split_once('@')?;
    let local = local.trim();
    let domain = domain.trim().to_ascii_lowercase();
    if local.is_empty()
        || domain.is_empty()
        || local
            .chars()
            .any(|ch| ch.is_whitespace() || matches!(ch, '<' | '>' | '@'))
        || domain
            .chars()
            .any(|ch| ch.is_whitespace() || matches!(ch, '<' | '>' | '@'))
    {
        return None;
    }
    Some(format!("{}@{}", local.to_ascii_lowercase(), domain))
}

pub fn sender_ref(address: &str) -> String {
    format!("email:addr:{address}")
}

pub fn conversation_ref(mailbox_id: &str) -> String {
    format!("email:mailbox:{mailbox_id}")
}

pub fn thread_ref(root_message_ref: &str) -> String {
    format!("email:thread:{}", short_hash(root_message_ref))
}

pub fn message_ref(provider_ref: &str) -> String {
    format!("email:message:{}", short_hash(provider_ref))
}

pub fn event_id(mailbox_id: &str, uid_validity: u32, uid: u32) -> String {
    format!("email:imap:{mailbox_id}:{uid_validity}:{uid}")
}

pub fn provider_file_ref(
    mailbox_id: &str,
    uid_validity: u32,
    uid: u32,
    part_index: usize,
) -> String {
    format!("email:imap:{mailbox_id}:{uid_validity}:{uid}:part:{part_index}")
}

pub fn mailbox_id_for(address_or_label: &str) -> String {
    normalize_address(address_or_label)
        .unwrap_or_else(|| address_or_label.trim().to_ascii_lowercase())
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

pub fn stable_message_root(
    message_id: Option<&str>,
    in_reply_to: Option<&str>,
    references: &[String],
    fallback: &str,
) -> String {
    references
        .iter()
        .map(String::as_str)
        .find(|value| !value.trim().is_empty())
        .or_else(|| in_reply_to.filter(|value| !value.trim().is_empty()))
        .or_else(|| message_id.filter(|value| !value.trim().is_empty()))
        .unwrap_or(fallback)
        .to_string()
}

pub fn generated_message_id(delivery_id: &str, from_address: &str) -> String {
    let domain = from_address
        .split_once('@')
        .map(|(_, domain)| domain)
        .filter(|domain| !domain.trim().is_empty())
        .unwrap_or("lionclaw.local");
    format!("lc.{}@{}", short_hash(delivery_id), domain)
}

pub fn short_hash(raw: &str) -> String {
    let digest = Sha256::digest(raw.as_bytes());
    hex::encode(&digest[..16])
}

pub fn non_empty_text(raw: impl Into<String>) -> Option<String> {
    let value = raw.into();
    (!value.trim().is_empty()).then_some(value)
}

pub fn held_body_not_downloaded_text() -> &'static str {
    "Body not downloaded because the sender is not approved."
}

pub fn sanitize_header_text(raw: &str) -> Option<String> {
    let mut value = String::new();
    let mut kept_chars = 0;
    let mut pending_space = false;
    let mut truncated = false;

    for ch in raw.chars() {
        if ch.is_whitespace() {
            if !value.is_empty() {
                pending_space = true;
            }
            continue;
        }

        if pending_space && !value.is_empty() {
            if kept_chars + 1 >= MAX_HEADER_TEXT_CHARS {
                truncated = true;
                break;
            }
            value.push(' ');
            kept_chars += 1;
            pending_space = false;
        }

        if kept_chars == MAX_HEADER_TEXT_CHARS {
            truncated = true;
            break;
        }
        value.push(ch);
        kept_chars += 1;
    }

    if truncated {
        value.push_str("...");
    }
    (!value.is_empty()).then_some(value)
}

pub fn sanitize_subject(raw: Option<&str>) -> String {
    raw.and_then(sanitize_header_text)
        .unwrap_or_else(|| "(no subject)".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_exact_sender_addresses() {
        assert_eq!(
            normalize_address(" Alice@Example.COM ").as_deref(),
            Some("alice@example.com")
        );
        assert!(normalize_address("no-at").is_none());
        assert!(normalize_address("Alice <alice@example.com>").is_none());
        assert!(normalize_address("alice@bad domain").is_none());
    }

    #[test]
    fn sanitizes_header_text_to_one_line() {
        assert_eq!(
            sanitize_header_text(" Alice\r\n  Build\tfailed ").as_deref(),
            Some("Alice Build failed")
        );
        assert_eq!(sanitize_subject(Some("\r\n\t")), "(no subject)");
    }

    #[test]
    fn sanitizes_header_text_to_bounded_display_text() {
        let raw = "x".repeat(MAX_HEADER_TEXT_CHARS + 1);

        assert_eq!(
            sanitize_header_text(&raw)
                .expect("sanitized")
                .chars()
                .count(),
            MAX_HEADER_TEXT_CHARS + 3
        );
    }

    #[test]
    fn builds_stable_refs() {
        assert_eq!(
            sender_ref("alice@example.com"),
            "email:addr:alice@example.com"
        );
        assert_eq!(
            conversation_ref("assistant@example.com"),
            "email:mailbox:assistant@example.com"
        );
        assert!(thread_ref("<root@example.com>").starts_with("email:thread:"));
        assert_eq!(
            stable_message_root(None, None, &[], "imap:7:42"),
            "imap:7:42"
        );
        assert_eq!(
            stable_message_root(Some("m1@example.com"), None, &[], "imap:7:42"),
            "m1@example.com"
        );
        assert_eq!(
            stable_message_root(None, Some("root@example.com"), &[], "imap:7:42"),
            "root@example.com"
        );
    }
}
