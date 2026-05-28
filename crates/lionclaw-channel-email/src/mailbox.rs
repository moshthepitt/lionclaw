use std::{error::Error, fmt, num::NonZeroU32};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use imap_client::{
    client::tokio::Client as ImapClient,
    imap_next::imap_types::{
        body::{Body, BodyStructure, SinglePartExtensionData, SpecificFields},
        core::{AString, IString, NString, Vec1},
        fetch::{MacroOrMessageDataItemNames, MessageDataItem, MessageDataItemName, Section},
        flag::{Flag, StoreType},
        search::SearchKey,
        sequence::SequenceSet,
    },
};
use mail_send::{mail_builder::MessageBuilder, SmtpClientBuilder};
use serde_json::{json, Value};
use tracing::warn;

use crate::{
    config::{ImapTlsMode, MailboxConfig},
    mime::{parse_header_facts, HeaderFacts},
    protocol::{
        conversation_ref, event_id, generated_message_id, message_ref, provider_file_ref,
        sender_ref, stable_message_root, thread_ref,
    },
};

#[derive(Debug, Clone)]
pub struct CandidateHeader {
    pub uid_validity: u32,
    pub uid: u32,
    pub event_id: String,
    pub sender_ref: String,
    pub conversation_ref: String,
    pub thread_ref: String,
    pub message_ref: String,
    pub attachment_count: usize,
    pub rfc822_size: Option<u32>,
    pub facts: HeaderFacts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StaleMailboxCandidate {
    pub expected_uid_validity: u32,
    pub actual_uid_validity: u32,
}

impl fmt::Display for StaleMailboxCandidate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "IMAP UIDVALIDITY changed from {} to {}",
            self.expected_uid_validity, self.actual_uid_validity
        )
    }
}

impl Error for StaleMailboxCandidate {}

pub fn is_stale_mailbox_candidate(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.downcast_ref::<StaleMailboxCandidate>().is_some())
}

#[derive(Debug, Clone)]
pub struct FetchedMessage {
    pub raw: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct OutboundAttachment {
    pub filename: String,
    pub mime_type: String,
    pub content: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct OutboundEmail {
    pub delivery_id: String,
    pub to: String,
    pub subject: String,
    pub text: String,
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
    pub attachments: Vec<OutboundAttachment>,
}

#[async_trait]
pub trait MailboxEngine: Send {
    async fn list_candidate_headers(&mut self) -> Result<Vec<CandidateHeader>>;
    async fn fetch_full_message_after_authorize(
        &mut self,
        candidate: &CandidateHeader,
    ) -> Result<FetchedMessage>;
    async fn record_seen_or_processed(&mut self, candidate: &CandidateHeader) -> Result<()>;
    async fn send_threaded_reply(&mut self, email: OutboundEmail) -> Result<Value>;
}

pub trait MailboxFactory: Clone + Send + Sync + 'static {
    fn open(&self, config: MailboxConfig) -> Box<dyn MailboxEngine>;
}

#[derive(Debug, Clone, Copy)]
pub struct RealMailboxFactory;

impl MailboxFactory for RealMailboxFactory {
    fn open(&self, config: MailboxConfig) -> Box<dyn MailboxEngine> {
        Box::new(RealMailboxEngine { config })
    }
}

#[derive(Debug, Clone)]
pub struct RealMailboxEngine {
    config: MailboxConfig,
}

impl RealMailboxEngine {
    async fn imap(&self) -> Result<ImapClient> {
        let mut client = match self.config.imap_tls {
            ImapTlsMode::Implicit => {
                ImapClient::rustls(
                    self.config.imap_host.clone(),
                    self.config.imap_port,
                    false,
                    None,
                )
                .await
            }
            ImapTlsMode::StartTls => {
                ImapClient::rustls(
                    self.config.imap_host.clone(),
                    self.config.imap_port,
                    true,
                    None,
                )
                .await
            }
            ImapTlsMode::Insecure => {
                ImapClient::insecure(self.config.imap_host.clone(), self.config.imap_port).await
            }
        }
        .context("failed to connect to IMAP server")?;
        client
            .authenticate_plain(
                self.config.imap_username.clone(),
                self.config.imap_password.clone(),
            )
            .await
            .context("failed to authenticate to IMAP server")?;
        if let Err(err) = client.id(Some(Self::imap_id_params())).await {
            warn!(error = %err, "IMAP ID command failed");
        }
        Ok(client)
    }

    fn imap_id_params() -> Vec<(IString<'static>, NString<'static>)> {
        [
            ("name", "lionclaw-channel-email"),
            ("version", env!("CARGO_PKG_VERSION")),
            ("vendor", "LionClaw"),
        ]
        .into_iter()
        .map(|(name, value)| {
            (
                IString::try_from(name).expect("valid IMAP ID field"),
                NString::try_from(value).expect("valid IMAP ID value"),
            )
        })
        .collect()
    }

    async fn selected_imap(&self, read_only: bool) -> Result<(ImapClient, u32)> {
        let mut client = self.imap().await?;
        let selected = if read_only {
            client
                .examine(self.config.imap_mailbox.as_str())
                .await
                .context("failed to examine IMAP mailbox")?
        } else {
            client
                .select(self.config.imap_mailbox.as_str())
                .await
                .context("failed to select IMAP mailbox")?
        };
        let uid_validity = selected
            .uid_validity
            .map(NonZeroU32::get)
            .ok_or_else(|| anyhow!("IMAP server did not return UIDVALIDITY"))?;
        Ok((client, uid_validity))
    }

    async fn selected_candidate_imap(
        &self,
        read_only: bool,
        candidate: &CandidateHeader,
    ) -> Result<ImapClient> {
        let (client, uid_validity) = self.selected_imap(read_only).await?;
        if uid_validity != candidate.uid_validity {
            return Err(StaleMailboxCandidate {
                expected_uid_validity: candidate.uid_validity,
                actual_uid_validity: uid_validity,
            }
            .into());
        }
        Ok(client)
    }

    fn header_fetch_items() -> MacroOrMessageDataItemNames<'static> {
        let fields = [
            "From",
            "To",
            "Subject",
            "Date",
            "Message-ID",
            "In-Reply-To",
            "References",
            "Auto-Submitted",
            "Precedence",
            "List-Id",
            "List-Unsubscribe",
            "List-Post",
            "Return-Path",
            "X-Auto-Response-Suppress",
            "Content-Type",
        ]
        .into_iter()
        .map(|field| AString::try_from(field).expect("valid IMAP header field"))
        .collect::<Vec<_>>();
        MacroOrMessageDataItemNames::MessageDataItemNames(vec![
            MessageDataItemName::BodyExt {
                section: Some(Section::HeaderFields(
                    None,
                    Vec1::try_from(fields).expect("nonempty header list"),
                )),
                partial: None,
                peek: true,
            },
            MessageDataItemName::BodyStructure,
            MessageDataItemName::Rfc822Size,
        ])
    }

    fn full_fetch_items(max_message_bytes: usize) -> MacroOrMessageDataItemNames<'static> {
        let fetch_limit = u32::try_from(max_message_bytes.saturating_add(1)).unwrap_or(u32::MAX);
        let fetch_limit = NonZeroU32::new(fetch_limit).unwrap_or(NonZeroU32::MIN);
        MacroOrMessageDataItemNames::MessageDataItemNames(vec![MessageDataItemName::BodyExt {
            section: None,
            partial: Some((0, fetch_limit)),
            peek: true,
        }])
    }

    fn candidate_from_headers(
        &self,
        uid_validity: u32,
        uid: NonZeroU32,
        raw_headers: &[u8],
        attachment_count: usize,
        rfc822_size: Option<u32>,
    ) -> Result<CandidateHeader> {
        let facts = parse_header_facts(raw_headers)?;
        let sender = sender_ref(&facts.sender.address);
        let conversation = conversation_ref(&self.config.mailbox_id);
        let provider = facts
            .message_id
            .clone()
            .unwrap_or_else(|| format!("imap:{}:{}", uid_validity, uid));
        let root = stable_message_root(
            facts.message_id.as_deref(),
            facts.in_reply_to.as_deref(),
            &facts.references,
            &provider,
        );
        let thread = thread_ref(&root);
        let message = message_ref(&provider);
        Ok(CandidateHeader {
            uid_validity,
            uid: uid.get(),
            event_id: event_id(&self.config.mailbox_id, uid_validity, uid.get()),
            sender_ref: sender,
            conversation_ref: conversation,
            thread_ref: thread,
            message_ref: message,
            attachment_count,
            rfc822_size,
            facts,
        })
    }

    fn push_candidate_from_headers(
        &self,
        candidates: &mut Vec<CandidateHeader>,
        uid_validity: u32,
        uid: NonZeroU32,
        raw_headers: &[u8],
        attachment_count: usize,
        rfc822_size: Option<u32>,
    ) -> bool {
        match self.candidate_from_headers(
            uid_validity,
            uid,
            raw_headers,
            attachment_count,
            rfc822_size,
        ) {
            Ok(candidate) => {
                candidates.push(candidate);
                true
            }
            Err(err) => {
                warn!(
                    uid_validity,
                    uid = uid.get(),
                    error = %err,
                    "skipping email candidate with malformed headers"
                );
                false
            }
        }
    }

    async fn mark_malformed_candidates_processed(
        &self,
        client: &mut ImapClient,
        uids: Vec<NonZeroU32>,
    ) {
        if uids.is_empty() {
            return;
        }
        let count = uids.len();
        match SequenceSet::try_from(uids) {
            Ok(sequence_set) => {
                if let Err(err) = client
                    .uid_silent_store(sequence_set, StoreType::Add, [Flag::Seen])
                    .await
                {
                    warn!(
                        count,
                        error = %err,
                        "failed to mark malformed email candidates processed"
                    );
                }
            }
            Err(err) => warn!(
                count,
                error = %err,
                "failed to build malformed email candidate UID set"
            ),
        }
    }
}

#[async_trait]
impl MailboxEngine for RealMailboxEngine {
    async fn list_candidate_headers(&mut self) -> Result<Vec<CandidateHeader>> {
        let (mut client, uid_validity) = self.selected_imap(false).await?;
        let mut uids = client
            .uid_search([SearchKey::Unseen])
            .await
            .context("failed to search IMAP mailbox")?;
        uids.sort();
        uids.reverse();
        uids.truncate(self.config.fetch_limit);
        if uids.is_empty() {
            return Ok(Vec::new());
        }
        let sequence_set = SequenceSet::try_from(uids.clone())?;
        let fetched = client
            .uid_fetch(sequence_set, Self::header_fetch_items())
            .await
            .context("failed to fetch IMAP message headers")?;
        let mut candidates = Vec::new();
        let mut malformed_uids = Vec::new();
        for uid in uids {
            let Some(items) = fetched.get(&uid) else {
                continue;
            };
            let Some(raw_headers) = body_data(items.as_ref()) else {
                warn!(
                    uid_validity,
                    uid = uid.get(),
                    "skipping email candidate with missing fetched headers"
                );
                malformed_uids.push(uid);
                continue;
            };
            let attachment_count = bodystructure_attachment_count(items.as_ref());
            let rfc822_size = rfc822_size(items.as_ref());
            let added = self.push_candidate_from_headers(
                &mut candidates,
                uid_validity,
                uid,
                &raw_headers,
                attachment_count,
                rfc822_size,
            );
            if !added {
                malformed_uids.push(uid);
            }
        }
        self.mark_malformed_candidates_processed(&mut client, malformed_uids)
            .await;
        Ok(candidates)
    }

    async fn fetch_full_message_after_authorize(
        &mut self,
        candidate: &CandidateHeader,
    ) -> Result<FetchedMessage> {
        let mut client = self.selected_candidate_imap(true, candidate).await?;
        let uid = NonZeroU32::new(candidate.uid).ok_or_else(|| anyhow!("invalid uid"))?;
        let fetched = client
            .uid_fetch(
                SequenceSet::try_from(vec![uid])?,
                Self::full_fetch_items(self.config.max_message_bytes),
            )
            .await
            .context("failed to fetch full IMAP message")?;
        let raw = fetched
            .get(&uid)
            .and_then(|items| body_data(items.as_ref()))
            .ok_or_else(|| anyhow!("IMAP full message fetch returned no body"))?;
        Ok(FetchedMessage { raw })
    }

    async fn record_seen_or_processed(&mut self, candidate: &CandidateHeader) -> Result<()> {
        let mut client = self.selected_candidate_imap(false, candidate).await?;
        let uid = NonZeroU32::new(candidate.uid).ok_or_else(|| anyhow!("invalid uid"))?;
        client
            .uid_silent_store(
                SequenceSet::try_from(vec![uid])?,
                StoreType::Add,
                [Flag::Seen],
            )
            .await
            .context("failed to mark IMAP message seen")?;
        Ok(())
    }

    async fn send_threaded_reply(&mut self, email: OutboundEmail) -> Result<Value> {
        let message_id = generated_message_id(&email.delivery_id, &self.config.address);
        let from_name = self.config.from_name.as_deref().unwrap_or("LionClaw");
        let mut message = MessageBuilder::new()
            .from((from_name, self.config.address.as_str()))
            .to(email.to.as_str())
            .subject(email.subject.as_str())
            .message_id(message_id.as_str())
            .text_body(email.text.as_str());
        if let Some(in_reply_to) = &email.in_reply_to {
            message = message.in_reply_to(in_reply_to.as_str());
        }
        if !email.references.is_empty() {
            message = message.references(email.references.clone());
        }
        for attachment in &email.attachments {
            message = message.attachment(
                attachment.mime_type.as_str(),
                attachment.filename.as_str(),
                attachment.content.as_slice(),
            );
        }

        let mut client =
            SmtpClientBuilder::new(self.config.smtp_host.as_str(), self.config.smtp_port)
                .map_err(|err| anyhow!("failed to build SMTP client: {err}"))?
                .implicit_tls(self.config.smtp_implicit_tls)
                .credentials((
                    self.config.smtp_username.as_str(),
                    self.config.smtp_password.as_str(),
                ))
                .connect()
                .await
                .context("failed to connect to SMTP server")?;
        client
            .send(message)
            .await
            .map_err(|err| anyhow!("failed to send SMTP message: {err}"))?;
        Ok(json!({
            "provider": "smtp",
            "message_id": message_id,
            "recipient": email.to,
        }))
    }
}

pub fn attachment_provider_ref(
    mailbox_id: &str,
    uid_validity: u32,
    uid: u32,
    part_index: usize,
) -> String {
    provider_file_ref(mailbox_id, uid_validity, uid, part_index)
}

fn body_data(items: &[MessageDataItem<'_>]) -> Option<Vec<u8>> {
    items.iter().find_map(|item| match item {
        MessageDataItem::BodyExt { data, .. } => data.0.as_ref().map(|data| data.as_ref().to_vec()),
        _ => None,
    })
}

fn bodystructure_attachment_count(items: &[MessageDataItem<'_>]) -> usize {
    items
        .iter()
        .find_map(|item| match item {
            MessageDataItem::BodyStructure(body) => Some(count_attachments(body)),
            _ => None,
        })
        .unwrap_or(0)
}

fn rfc822_size(items: &[MessageDataItem<'_>]) -> Option<u32> {
    items.iter().find_map(|item| match item {
        MessageDataItem::Rfc822Size(size) => Some(*size),
        _ => None,
    })
}

fn count_attachments(body: &BodyStructure<'_>) -> usize {
    match body {
        BodyStructure::Single {
            body,
            extension_data,
        } => usize::from(is_attachment_like_body(body, extension_data.as_ref())),
        BodyStructure::Multi { bodies, .. } => bodies.as_ref().iter().map(count_attachments).sum(),
    }
}

fn is_attachment_like_body(
    body: &Body<'_>,
    extension_data: Option<&SinglePartExtensionData<'_>>,
) -> bool {
    if has_parameter_name(&body.basic.parameter_list, "name") {
        return true;
    }
    if let Some(disposition) = extension_data
        .and_then(|data| data.tail.as_ref())
        .and_then(|tail| tail.disposition.as_ref())
    {
        if i_string_eq(&disposition.0, "attachment")
            || disposition
                .1
                .iter()
                .any(|(name, _)| i_string_eq(name, "filename"))
        {
            return true;
        }
    }

    match &body.specific {
        SpecificFields::Basic { r#type, .. } => !i_string_eq(r#type, "text"),
        SpecificFields::Message { .. } => true,
        SpecificFields::Text { .. } => false,
    }
}

fn has_parameter_name(
    params: &[(
        imap_client::imap_next::imap_types::core::IString<'_>,
        imap_client::imap_next::imap_types::core::IString<'_>,
    )],
    name: &str,
) -> bool {
    params.iter().any(|(param, _)| i_string_eq(param, name))
}

fn i_string_eq(
    value: &imap_client::imap_next::imap_types::core::IString<'_>,
    expected: &str,
) -> bool {
    value.as_ref().eq_ignore_ascii_case(expected.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ImapTlsMode;

    #[test]
    fn malformed_candidate_headers_do_not_discard_well_formed_candidates() {
        let engine = RealMailboxEngine {
            config: test_mailbox_config(),
        };
        let mut candidates = Vec::new();

        let malformed_added = engine.push_candidate_from_headers(
            &mut candidates,
            7,
            NonZeroU32::new(41).expect("nonzero uid"),
            b"Subject: Missing sender\r\n\r\n",
            0,
            Some(128),
        );
        let well_formed_added = engine.push_candidate_from_headers(
            &mut candidates,
            7,
            NonZeroU32::new(42).expect("nonzero uid"),
            b"From: Alice <alice@example.com>\r\nSubject: Build failed\r\nMessage-ID: <m1@example.com>\r\n\r\n",
            0,
            Some(256),
        );

        assert!(!malformed_added);
        assert!(well_formed_added);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].uid, 42);
        assert_eq!(candidates[0].sender_ref, "email:addr:alice@example.com");
    }

    #[test]
    fn missing_message_ids_do_not_share_thread_identity() {
        let engine = RealMailboxEngine {
            config: test_mailbox_config(),
        };
        let first = engine
            .candidate_from_headers(
                7,
                NonZeroU32::new(41).expect("nonzero uid"),
                b"From: Alice <alice@example.com>\r\nSubject: First\r\n\r\n",
                0,
                Some(128),
            )
            .expect("first candidate");
        let second = engine
            .candidate_from_headers(
                7,
                NonZeroU32::new(42).expect("nonzero uid"),
                b"From: Alice <alice@example.com>\r\nSubject: Second\r\nMessage-ID: <>\r\n\r\n",
                0,
                Some(128),
            )
            .expect("second candidate");

        assert_ne!(first.thread_ref, second.thread_ref);
        assert_ne!(first.message_ref, second.message_ref);
        assert!(first.facts.message_id.is_none());
        assert!(second.facts.message_id.is_none());
    }

    #[test]
    fn stale_mailbox_candidate_error_is_detected_through_context() {
        let error: anyhow::Error = StaleMailboxCandidate {
            expected_uid_validity: 7,
            actual_uid_validity: 8,
        }
        .into();
        let error = error.context("failed to fetch authorized email body");

        assert!(is_stale_mailbox_candidate(&error));
    }

    fn test_mailbox_config() -> MailboxConfig {
        MailboxConfig {
            mailbox_id: "assistant-example-com".to_string(),
            address: "assistant@example.com".to_string(),
            imap_host: "imap.example.com".to_string(),
            imap_port: 993,
            imap_tls: ImapTlsMode::Implicit,
            imap_username: "assistant@example.com".to_string(),
            imap_password: "secret".to_string(),
            imap_mailbox: "INBOX".to_string(),
            smtp_host: "smtp.example.com".to_string(),
            smtp_port: 587,
            smtp_implicit_tls: false,
            smtp_username: "assistant@example.com".to_string(),
            smtp_password: "secret".to_string(),
            from_name: Some("LionClaw".to_string()),
            fetch_limit: 25,
            max_message_bytes: crate::config::DEFAULT_MAX_MESSAGE_BYTES,
        }
    }
}
