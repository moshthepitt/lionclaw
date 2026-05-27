use std::num::NonZeroU32;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use imap_client::{
    client::tokio::Client as ImapClient,
    imap_next::imap_types::{
        body::{Body, BodyStructure, SinglePartExtensionData, SpecificFields},
        core::{AString, Vec1},
        fetch::{MacroOrMessageDataItemNames, MessageDataItem, MessageDataItemName, Section},
        flag::{Flag, StoreType},
        search::SearchKey,
        sequence::SequenceSet,
    },
};
use mail_send::{mail_builder::MessageBuilder, SmtpClientBuilder};
use serde_json::{json, Value};

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
    pub facts: HeaderFacts,
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
        Ok(client)
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

    fn full_fetch_items() -> MacroOrMessageDataItemNames<'static> {
        MacroOrMessageDataItemNames::MessageDataItemNames(vec![MessageDataItemName::BodyExt {
            section: None,
            partial: None,
            peek: true,
        }])
    }

    fn candidate_from_headers(
        &self,
        uid_validity: u32,
        uid: NonZeroU32,
        raw_headers: &[u8],
        attachment_count: usize,
    ) -> Result<CandidateHeader> {
        let facts = parse_header_facts(raw_headers)?;
        let sender = sender_ref(&facts.sender.address);
        let conversation = conversation_ref(&self.config.mailbox_id);
        let root = stable_message_root(
            facts.message_id.as_deref(),
            facts.in_reply_to.as_deref(),
            &facts.references,
        );
        let thread = thread_ref(&root);
        let provider = facts
            .message_id
            .clone()
            .unwrap_or_else(|| format!("imap:{}:{}", uid_validity, uid));
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
            facts,
        })
    }
}

#[async_trait]
impl MailboxEngine for RealMailboxEngine {
    async fn list_candidate_headers(&mut self) -> Result<Vec<CandidateHeader>> {
        let (mut client, uid_validity) = self.selected_imap(true).await?;
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
        for uid in uids {
            let Some(items) = fetched.get(&uid) else {
                continue;
            };
            let Some(raw_headers) = body_data(items.as_ref()) else {
                continue;
            };
            let attachment_count = bodystructure_attachment_count(items.as_ref());
            candidates.push(self.candidate_from_headers(
                uid_validity,
                uid,
                &raw_headers,
                attachment_count,
            )?);
        }
        Ok(candidates)
    }

    async fn fetch_full_message_after_authorize(
        &mut self,
        candidate: &CandidateHeader,
    ) -> Result<FetchedMessage> {
        let (mut client, _) = self.selected_imap(true).await?;
        let uid = NonZeroU32::new(candidate.uid).ok_or_else(|| anyhow!("invalid uid"))?;
        let fetched = client
            .uid_fetch(SequenceSet::try_from(vec![uid])?, Self::full_fetch_items())
            .await
            .context("failed to fetch full IMAP message")?;
        let raw = fetched
            .get(&uid)
            .and_then(|items| body_data(items.as_ref()))
            .ok_or_else(|| anyhow!("IMAP full message fetch returned no body"))?;
        Ok(FetchedMessage { raw })
    }

    async fn record_seen_or_processed(&mut self, candidate: &CandidateHeader) -> Result<()> {
        if !self.config.mark_seen_after_admission {
            return Ok(());
        }
        let (mut client, _) = self.selected_imap(false).await?;
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
