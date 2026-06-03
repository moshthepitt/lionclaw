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

    if local_part(&facts.sender.address).is_some_and(is_automated_local_part) {
        return suppressed("automated_sender");
    }

    let auto_submitted = header_values(facts, "Auto-Submitted");
    if !auto_submitted.is_empty()
        && auto_submitted
            .iter()
            .any(|value| !value.eq_ignore_ascii_case("no"))
    {
        return suppressed("auto_submitted");
    }

    if header_values(facts, "Precedence").iter().any(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "bulk" | "list" | "junk"
        )
    }) {
        return suppressed("precedence_bulk");
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

fn is_automated_local_part(local: &str) -> bool {
    let compact = local
        .chars()
        .filter(|ch| !matches!(ch, '-' | '_' | '.'))
        .collect::<String>();
    matches!(
        compact.as_str(),
        "noreply" | "donotreply" | "mailerdaemon" | "postmaster"
    )
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
    fn suppresses_common_no_reply_sender_variants() {
        for sender in [
            "noreply@example.com",
            "no-reply@example.com",
            "no_reply@example.com",
            "do-not-reply@example.com",
            "mailer.daemon@example.com",
            "postmaster@example.com",
        ] {
            let facts =
                parse_headers_for_test(&format!("From: {sender}\r\nSubject: Automated\r\n\r\n"));

            assert_eq!(
                classify_headers(&facts, "assistant@example.com"),
                MailClassification::Suppressed {
                    reason: "automated_sender".to_string()
                },
                "{sender} should be suppressed"
            );
        }
    }

    #[test]
    fn suppresses_if_any_duplicate_auto_submitted_header_is_automated() {
        let facts = parse_headers_for_test(
            "From: Robot <robot@example.com>\r\nAuto-Submitted: auto-replied\r\nAuto-Submitted: no\r\nSubject: Re\r\n\r\n",
        );

        assert_eq!(
            classify_headers(&facts, "assistant@example.com"),
            MailClassification::Suppressed {
                reason: "auto_submitted".to_string()
            }
        );
    }

    #[test]
    fn suppresses_if_any_duplicate_precedence_header_is_bulk() {
        let facts = parse_headers_for_test(
            "From: List <list@example.com>\r\nPrecedence: normal\r\nPrecedence: bulk\r\nSubject: News\r\n\r\n",
        );

        assert_eq!(
            classify_headers(&facts, "assistant@example.com"),
            MailClassification::Suppressed {
                reason: "precedence_bulk".to_string()
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
