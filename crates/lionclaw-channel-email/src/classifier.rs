use serde::{Deserialize, Serialize};

use crate::mime::{header_value, header_values, HeaderFacts};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MailClassification {
    Candidate,
    Suppressed { reason: String },
}

pub fn classify_headers(facts: &HeaderFacts, own_address: &str) -> MailClassification {
    if facts.sender.address.eq_ignore_ascii_case(own_address) {
        return suppressed("self_sender");
    }

    if local_part(&facts.sender.address).is_some_and(|local| {
        matches!(
            local,
            "noreply" | "no-reply" | "mailer-daemon" | "postmaster"
        )
    }) {
        return suppressed("automated_sender");
    }

    if let Some(value) = header_value(facts, "Auto-Submitted") {
        if !value.eq_ignore_ascii_case("no") {
            return suppressed("auto_submitted");
        }
    }

    if let Some(value) = header_value(facts, "Precedence") {
        if matches!(
            value.to_ascii_lowercase().as_str(),
            "bulk" | "list" | "junk"
        ) {
            return suppressed("precedence_bulk");
        }
    }

    if header_value(facts, "List-Id").is_some()
        || header_value(facts, "List-Unsubscribe").is_some()
        || header_value(facts, "List-Post").is_some()
    {
        return suppressed("mailing_list");
    }

    if header_value(facts, "X-Auto-Response-Suppress").is_some() {
        return suppressed("auto_response_suppress");
    }

    if header_values(facts, "Return-Path")
        .iter()
        .any(|value| value.trim() == "<>")
    {
        return suppressed("bounce_return_path");
    }

    MailClassification::Candidate
}

fn suppressed(reason: &str) -> MailClassification {
    MailClassification::Suppressed {
        reason: reason.to_string(),
    }
}

fn local_part(address: &str) -> Option<&str> {
    address.split_once('@').map(|(local, _)| local)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mime::parse_headers_for_test;

    #[test]
    fn suppresses_auto_submitted_mail() {
        let facts = parse_headers_for_test(
            "From: Robot <robot@example.com>\r\nAuto-Submitted: auto-replied\r\nSubject: Re\r\n\r\n",
        );
        assert_eq!(
            classify_headers(&facts, "assistant@example.com"),
            MailClassification::Suppressed {
                reason: "auto_submitted".to_string()
            }
        );
    }

    #[test]
    fn leaves_normal_mail_as_candidate() {
        let facts = parse_headers_for_test(
            "From: Alice <alice@example.com>\r\nSubject: Build failed\r\n\r\n",
        );
        assert_eq!(
            classify_headers(&facts, "assistant@example.com"),
            MailClassification::Candidate
        );
    }
}
