use sha2::{Digest, Sha256};

pub const CHANNEL_ID: &str = "email";
pub const INBOUND_TRIGGER: &str = "command";
pub const INBOUND_SESSION_BINDING: &str = "thread_actor";

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
) -> String {
    references
        .first()
        .map(String::as_str)
        .or(in_reply_to)
        .or(message_id)
        .unwrap_or("missing-message-id")
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

pub fn sanitize_subject(raw: Option<&str>) -> String {
    let subject = raw.unwrap_or("(no subject)").trim();
    if subject.is_empty() {
        "(no subject)".to_string()
    } else {
        subject.replace(['\r', '\n'], " ")
    }
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
    }
}
