use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Value};
use tokio::io::AsyncReadExt;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

pub use crate::mailbox::RealMailboxFactory;

use crate::{
    api::{
        ChannelAttachmentDescriptor, ChannelAttachmentMissingReport, ChannelInboundRequest,
        ChannelOutboxAttachment, ChannelOutboxDelivery, ChannelOutboxReportInput,
        DaemonInfoResponse, LionClawApi,
    },
    classifier::{classify_headers, MailClassification},
    config::{validate_max_message_bytes, WorkerConfig},
    mailbox::{
        attachment_provider_ref, is_stale_mailbox_candidate, CandidateHeader, MailboxEngine,
        MailboxFactory, MalformedCandidateHeader, OutboundAttachment, OutboundEmail,
    },
    mime::{
        attachment_summary, parse_full_message, require_nonempty_body, ParsedAttachment,
        ParsedEmail,
    },
    protocol::{
        conversation_ref, generated_message_id, held_body_not_downloaded_text, message_ref,
        non_empty_text, sanitize_header_text, short_hash, CHANNEL_ID, INBOUND_SESSION_BINDING,
        INBOUND_TRIGGER,
    },
    store::{held_id_for, EmailStore, MailStatus, ThreadContext},
};

const ATTACHMENT_KIND: &str = "document";
const DEFAULT_DIGEST_LIMIT: i64 = 20;
const STALE_MAILBOX_UID_REASON: &str = "stale_mailbox_uid";

#[derive(Debug)]
pub struct EmailWorker<F>
where
    F: MailboxFactory,
{
    config: WorkerConfig,
    api: LionClawApi,
    mailbox_factory: F,
}

#[derive(Debug)]
struct PreparedInboundAttachment {
    descriptor: ChannelAttachmentDescriptor,
    content: Vec<u8>,
}

#[derive(Debug)]
struct OutboundAttachmentError {
    code: &'static str,
    source: anyhow::Error,
}

impl OutboundAttachmentError {
    fn new(code: &'static str, source: anyhow::Error) -> Self {
        Self { code, source }
    }
}

#[derive(Debug)]
enum DeliveryResult {
    Delivered { receipt: Value },
    RetryableFailed { code: &'static str, text: String },
    TerminalFailed { code: &'static str, text: String },
}

impl<F> EmailWorker<F>
where
    F: MailboxFactory,
{
    pub fn new(config: WorkerConfig, mailbox_factory: F) -> Result<Self> {
        validate_max_message_bytes(config.mailbox.max_message_bytes)?;
        let api = LionClawApi::new(config.base_url.clone())?;
        Ok(Self {
            config,
            api,
            mailbox_factory,
        })
    }

    pub async fn run(self) -> Result<()> {
        loop {
            if let Err(err) = self.tick().await {
                error!(error = %err, "email worker tick failed");
            }
            if self.config.once {
                return Ok(());
            }
            sleep(self.config.poll_interval).await;
        }
    }

    async fn tick(&self) -> Result<()> {
        let daemon = self.api.daemon_info().await?;
        validate_local_daemon(&daemon, &self.config.home)?;
        let worker_id = self.worker_id(&daemon);
        let store = EmailStore::open(&self.config.state_dir).await?;
        let mut mailbox = self.mailbox_factory.open(self.config.mailbox.clone());

        self.report_ready(&worker_id, &daemon).await;
        self.process_pending_release_revocations(&store).await?;
        self.process_held_releases(&store, mailbox.as_mut(), &worker_id)
            .await?;
        self.process_inbound(&store, mailbox.as_mut(), &worker_id)
            .await?;
        self.process_outbox(&store, mailbox.as_mut(), &worker_id)
            .await?;
        self.process_digest(&store, mailbox.as_mut()).await?;

        Ok(())
    }

    async fn process_inbound(
        &self,
        store: &EmailStore,
        mailbox: &mut dyn MailboxEngine,
        worker_id: &str,
    ) -> Result<()> {
        let batch = mailbox.list_candidate_headers().await?;
        if batch.is_empty() {
            debug!("email inbox has no unread candidate headers");
            return Ok(());
        }

        info!(
            candidates = batch.candidates.len(),
            malformed = batch.malformed.len(),
            "processing email candidate headers"
        );
        for malformed in batch.malformed {
            if let Err(err) = process_malformed_candidate(store, mailbox, &malformed).await {
                warn!(
                    event_id = %malformed.event_id,
                    sender_ref = %malformed.sender_ref,
                    error = %err,
                    "failed to process malformed email candidate"
                );
            }
        }

        for candidate in batch.candidates {
            let status = store.mail_status(&candidate.event_id).await?;
            match status {
                Some(MailStatus::Admitted | MailStatus::Suppressed) => {
                    if let Err(err) = mailbox.record_seen_or_processed(&candidate).await {
                        warn!(
                            event_id = %candidate.event_id,
                            sender_ref = %candidate.sender_ref,
                            error = %err,
                            "failed to mark terminal email candidate processed"
                        );
                    }
                    continue;
                }
                Some(MailStatus::Held) | None => {}
            }
            if status.is_none()
                && store
                    .mail_status_by_message_ref(&candidate.message_ref, &candidate.event_id)
                    .await?
                    .is_some()
            {
                suppress_candidate(store, mailbox, &candidate, "duplicate_message_ref").await?;
                continue;
            }
            if let Err(err) = self
                .process_candidate(
                    store,
                    mailbox,
                    &candidate,
                    worker_id,
                    matches!(status, Some(MailStatus::Held)),
                )
                .await
            {
                warn!(
                    event_id = %candidate.event_id,
                    sender_ref = %candidate.sender_ref,
                    error = %err,
                    "failed to process email candidate"
                );
            }
        }

        Ok(())
    }

    async fn process_held_releases(
        &self,
        store: &EmailStore,
        mailbox: &mut dyn MailboxEngine,
        worker_id: &str,
    ) -> Result<()> {
        let held_limit = i64::try_from(self.config.mailbox.fetch_limit).unwrap_or(i64::MAX);
        let held = store.held_candidates(held_limit).await?;
        if held.is_empty() {
            return Ok(());
        }

        debug!(count = held.len(), "checking held email candidates");
        for candidate in held {
            if let Err(err) = self
                .process_candidate(store, mailbox, &candidate, worker_id, true)
                .await
            {
                warn!(
                    event_id = %candidate.event_id,
                    sender_ref = %candidate.sender_ref,
                    error = %err,
                    "failed to process held email candidate"
                );
            }
        }

        Ok(())
    }

    async fn process_candidate(
        &self,
        store: &EmailStore,
        mailbox: &mut dyn MailboxEngine,
        candidate: &CandidateHeader,
        worker_id: &str,
        previously_held: bool,
    ) -> Result<()> {
        let held_id = held_id_for(&candidate.event_id);
        match classify_headers(&candidate.facts, &self.config.mailbox.address) {
            MailClassification::Candidate => {}
            MailClassification::Suppressed { reason } => {
                suppress_candidate(store, mailbox, candidate, &reason).await?;
                return Ok(());
            }
        }

        let authorization = self
            .api
            .authorize(
                &self.config.channel_id,
                &candidate.sender_ref,
                &candidate.conversation_ref,
                Some(&candidate.thread_ref),
                INBOUND_TRIGGER,
                INBOUND_SESSION_BINDING,
            )
            .await
            .context("failed to authorize email sender")?;

        if !authorization.authorized {
            if authorization.reason_code == "blocked_grant" {
                suppress_candidate(
                    store,
                    mailbox,
                    candidate,
                    authorization.reason_code.as_str(),
                )
                .await?;
            } else {
                store
                    .record_held(
                        candidate,
                        &held_id,
                        held_body_not_downloaded_text(),
                        authorization.reason_code.as_str(),
                    )
                    .await?;
                mark_held_candidate_processed(store, mailbox, candidate).await?;
            }
            return Ok(());
        }

        if authorization
            .one_shot_release_held_id()
            .is_some_and(|released_held_id| !previously_held || released_held_id != held_id)
        {
            store
                .record_held(
                    candidate,
                    &held_id,
                    held_body_not_downloaded_text(),
                    "release_grant_mismatch",
                )
                .await?;
            mark_held_candidate_processed(store, mailbox, candidate).await?;
            return Ok(());
        }

        let consumed_release_grant_id = self
            .record_consumed_one_shot_release_grant_if_needed(
                store,
                &authorization,
                held_id.as_str(),
                previously_held,
            )
            .await?;
        if candidate_message_size_exceeds_limit(candidate, self.config.mailbox.max_message_bytes) {
            self.suppress_and_revoke_release_grant(
                store,
                mailbox,
                candidate,
                "message_too_large",
                consumed_release_grant_id.as_deref(),
                held_id.as_str(),
            )
            .await?;
            return Ok(());
        }
        let fetched = match mailbox.fetch_full_message_after_authorize(candidate).await {
            Ok(fetched) => fetched,
            Err(err) if is_stale_mailbox_candidate(&err) => {
                self.suppress_and_revoke_release_grant(
                    store,
                    mailbox,
                    candidate,
                    STALE_MAILBOX_UID_REASON,
                    consumed_release_grant_id.as_deref(),
                    held_id.as_str(),
                )
                .await?;
                return Ok(());
            }
            Err(err) => return Err(err.context("failed to fetch authorized email body")),
        };
        if fetched.raw.len() > self.config.mailbox.max_message_bytes {
            self.suppress_and_revoke_release_grant(
                store,
                mailbox,
                candidate,
                "message_too_large",
                consumed_release_grant_id.as_deref(),
                held_id.as_str(),
            )
            .await?;
            return Ok(());
        }
        let parsed = match parse_full_message(&fetched.raw) {
            Ok(parsed) => parsed,
            Err(err) => {
                self.suppress_and_revoke_release_grant(
                    store,
                    mailbox,
                    candidate,
                    "malformed_message",
                    consumed_release_grant_id.as_deref(),
                    held_id.as_str(),
                )
                .await?;
                return Err(err.context("failed to parse authorized email"));
            }
        };
        if let Err(err) = require_nonempty_body(&parsed) {
            self.suppress_and_revoke_release_grant(
                store,
                mailbox,
                candidate,
                "empty_message",
                consumed_release_grant_id.as_deref(),
                held_id.as_str(),
            )
            .await?;
            return Err(err);
        }

        let prepared_attachments =
            prepare_inbound_attachments(&self.config.mailbox.mailbox_id, candidate, &parsed)?;
        let inbound = self.build_inbound(candidate, &parsed, &prepared_attachments, &authorization);
        let response = self
            .api
            .inbound(&inbound)
            .await
            .context("failed to submit email inbound event")?;

        match response.outcome.as_str() {
            "queued" | "duplicate" => {}
            "waiting_for_attachments" => {
                stage_and_finalize_attachments(
                    &self.api,
                    &self.config.channel_id,
                    &candidate.event_id,
                    worker_id,
                    &prepared_attachments,
                )
                .await?;
            }
            "blocked" => {
                self.suppress_and_revoke_release_grant(
                    store,
                    mailbox,
                    candidate,
                    "blocked",
                    consumed_release_grant_id.as_deref(),
                    held_id.as_str(),
                )
                .await?;
                return Ok(());
            }
            "pending_approval" | "trigger_ignored" => {
                let reason = response
                    .reason_code
                    .as_deref()
                    .unwrap_or(response.outcome.as_str());
                store
                    .record_held(candidate, &held_id, held_body_not_downloaded_text(), reason)
                    .await?;
                mark_held_candidate_processed(store, mailbox, candidate).await?;
                self.revoke_consumed_one_shot_release_grant_if_needed(
                    store,
                    consumed_release_grant_id.as_deref(),
                    held_id.as_str(),
                )
                .await?;
                return Ok(());
            }
            other => bail!("unexpected inbound outcome '{other}'"),
        }

        store.record_admitted(candidate, &parsed.snippet).await?;
        if let Err(err) = mailbox.record_seen_or_processed(candidate).await {
            if is_stale_mailbox_candidate(&err) {
                warn!(
                    event_id = %candidate.event_id,
                    sender_ref = %candidate.sender_ref,
                    error = %err,
                    "admitted email candidate became stale before it could be marked processed"
                );
            } else {
                return Err(err);
            }
        }
        self.revoke_consumed_one_shot_release_grant_if_needed(
            store,
            consumed_release_grant_id.as_deref(),
            held_id.as_str(),
        )
        .await?;
        Ok(())
    }

    async fn suppress_and_revoke_release_grant(
        &self,
        store: &EmailStore,
        mailbox: &mut dyn MailboxEngine,
        candidate: &CandidateHeader,
        reason: &str,
        grant_id: Option<&str>,
        held_id: &str,
    ) -> Result<()> {
        suppress_candidate(store, mailbox, candidate, reason).await?;
        self.revoke_consumed_one_shot_release_grant_if_needed(store, grant_id, held_id)
            .await
    }

    fn build_inbound(
        &self,
        candidate: &CandidateHeader,
        parsed: &ParsedEmail,
        attachments: &[PreparedInboundAttachment],
        authorization: &crate::api::ChannelActorAuthorizeResponse,
    ) -> ChannelInboundRequest {
        let descriptors = attachments
            .iter()
            .map(|attachment| attachment.descriptor.clone())
            .collect::<Vec<_>>();
        ChannelInboundRequest {
            channel_id: self.config.channel_id.clone(),
            event_id: candidate.event_id.clone(),
            sender_ref: candidate.sender_ref.clone(),
            conversation_ref: candidate.conversation_ref.clone(),
            thread_ref: Some(candidate.thread_ref.clone()),
            message_ref: Some(candidate.message_ref.clone()),
            text: non_empty_text(inbound_envelope_text(
                &self.config.mailbox.address,
                candidate,
                parsed,
            )),
            attachments: descriptors,
            reply_to_ref: parsed.facts.in_reply_to.as_deref().map(message_ref),
            trigger: INBOUND_TRIGGER.to_string(),
            session_binding: INBOUND_SESSION_BINDING.to_string(),
            received_at: parsed.facts.received_at,
            provider_metadata: json!({
                "provider": "imap",
                "mailbox_id": self.config.mailbox.mailbox_id,
                "mailbox_address": self.config.mailbox.address,
                "uid_validity": candidate.uid_validity,
                "uid": candidate.uid,
                "sender_address": parsed.facts.sender.address,
                "subject": parsed.facts.subject,
                "message_id": parsed.facts.message_id,
                "in_reply_to": parsed.facts.in_reply_to,
                "rfc822_size": candidate.rfc822_size,
                "authorization_reason_code": authorization.reason_code,
            }),
        }
    }

    async fn process_outbox(
        &self,
        store: &EmailStore,
        mailbox: &mut dyn MailboxEngine,
        worker_id: &str,
    ) -> Result<()> {
        let deliveries = self
            .api
            .pull_outbox(
                &self.config.channel_id,
                worker_id,
                self.config.pull_limit,
                self.config.lease_ms,
            )
            .await?;
        if deliveries.is_empty() {
            debug!("email outbox is empty");
            return Ok(());
        }

        info!(
            count = deliveries.len(),
            "processing email outbox deliveries"
        );
        for delivery in deliveries {
            let result = self.deliver_outbox(store, mailbox, &delivery).await;
            self.report_result(&delivery, worker_id, result).await?;
        }

        Ok(())
    }

    async fn deliver_outbox(
        &self,
        store: &EmailStore,
        mailbox: &mut dyn MailboxEngine,
        delivery: &ChannelOutboxDelivery,
    ) -> DeliveryResult {
        match self.deliver_outbox_inner(store, mailbox, delivery).await {
            Ok(receipt) => DeliveryResult::Delivered { receipt },
            Err(err) => err,
        }
    }

    async fn deliver_outbox_inner(
        &self,
        store: &EmailStore,
        mailbox: &mut dyn MailboxEngine,
        delivery: &ChannelOutboxDelivery,
    ) -> Result<Value, DeliveryResult> {
        if delivery.conversation_ref != conversation_ref(&self.config.mailbox.mailbox_id) {
            return Err(terminal(
                "unknown_mailbox",
                anyhow!("delivery conversation_ref does not target this email mailbox"),
            ));
        }

        if let Some(stored) = store
            .receipt(&delivery.delivery_id)
            .await
            .map_err(|err| retryable("receipt_lookup_failed", err))?
        {
            return Ok(stored.receipt);
        }

        if delivery.content.text.trim().is_empty() && delivery.content.attachments.is_empty() {
            return Err(terminal(
                "empty_delivery",
                anyhow!("email delivery contains no text or attachments"),
            ));
        }

        let context = store
            .thread_context(
                delivery.reply_to_ref.as_deref(),
                delivery.thread_ref.as_deref(),
            )
            .await
            .map_err(|err| retryable("thread_lookup_failed", err))?
            .ok_or_else(|| {
                terminal(
                    "unknown_email_thread",
                    anyhow!("no admitted email thread matches delivery"),
                )
            })?;
        let attachments = prepare_outbound_attachments(
            &delivery.content.attachments,
            self.config.mailbox.max_message_bytes,
            delivery.content.text.len(),
        )
        .await
        .map_err(|err| terminal(err.code, err.source))?;
        let references = reply_references(&context);
        let outbound = OutboundEmail {
            delivery_id: delivery.delivery_id.clone(),
            to: context.sender_address.clone(),
            subject: reply_subject(&context.subject),
            text: delivery.content.text.clone(),
            in_reply_to: context.provider_message_id.clone(),
            references,
            attachments,
        };
        let mut receipt = mailbox
            .send_threaded_reply(outbound)
            .await
            .map_err(|err| retryable("smtp_send_failed", err))?;
        if let Some(object) = receipt.as_object_mut() {
            object.insert("delivery_id".to_string(), json!(delivery.delivery_id));
            object.insert("attempt_id".to_string(), json!(delivery.attempt_id));
            object.insert("session_id".to_string(), json!(delivery.session_id));
            object.insert("turn_id".to_string(), json!(delivery.turn_id));
        }
        let message_id = receipt
            .get("message_id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| {
                generated_message_id(&delivery.delivery_id, &self.config.mailbox.address)
            });
        if let Err(err) = store
            .record_receipt(
                &delivery.delivery_id,
                &message_id,
                &context.sender_address,
                &receipt,
            )
            .await
        {
            warn!(
                delivery_id = %delivery.delivery_id,
                message_id = %message_id,
                error = %err,
                "failed to cache email delivery receipt after provider accepted message"
            );
            if let Some(object) = receipt.as_object_mut() {
                object.insert("local_receipt_recorded".to_string(), json!(false));
                object.insert(
                    "local_receipt_record_error_code".to_string(),
                    json!("receipt_record_failed"),
                );
            }
        }
        Ok(receipt)
    }

    async fn process_digest(
        &self,
        store: &EmailStore,
        mailbox: &mut dyn MailboxEngine,
    ) -> Result<()> {
        let Some(admin_to) = self.config.digest.admin_to.as_deref() else {
            return Ok(());
        };
        if !store.due_for_digest(self.config.digest.interval).await? {
            return Ok(());
        }
        store.mark_digest_attempt_now().await?;

        let held = store.held_since_last_digest(DEFAULT_DIGEST_LIMIT).await?;
        let suppressed_count = store.suppressed_count_since_last_digest().await?;
        if held.is_empty() && suppressed_count == 0 {
            store.mark_digest_sent(&held).await?;
            return Ok(());
        }

        let text = digest_text(&self.config.mailbox.address, &held, suppressed_count);
        let delivery_id = format!(
            "digest:{}",
            short_hash(&format!(
                "{}:{}",
                self.config.mailbox.mailbox_id,
                chrono::Utc::now()
            ))
        );
        mailbox
            .send_threaded_reply(OutboundEmail {
                delivery_id,
                to: admin_to.to_string(),
                subject: format!("LionClaw email digest for {}", self.config.mailbox.address),
                text,
                in_reply_to: None,
                references: Vec::new(),
                attachments: Vec::new(),
            })
            .await
            .context("failed to send held-mail digest")?;
        store.mark_digest_sent(&held).await?;
        Ok(())
    }

    async fn process_pending_release_revocations(&self, store: &EmailStore) -> Result<()> {
        for pending in store.pending_grant_revocations().await? {
            self.revoke_one_shot_release_grant(
                store,
                &pending.channel_id,
                &pending.grant_id,
                &pending.held_id,
            )
            .await?;
        }
        Ok(())
    }

    async fn record_consumed_one_shot_release_grant_if_needed(
        &self,
        store: &EmailStore,
        authorization: &crate::api::ChannelActorAuthorizeResponse,
        held_id: &str,
        previously_held: bool,
    ) -> Result<Option<String>> {
        let Some(grant_id) = one_shot_release_grant_id(authorization, held_id, previously_held)
        else {
            return Ok(None);
        };
        store
            .record_pending_grant_revocation(
                grant_id,
                &self.config.channel_id,
                held_id,
                "release_attempt_started",
            )
            .await?;
        Ok(Some(grant_id.to_string()))
    }

    async fn revoke_consumed_one_shot_release_grant_if_needed(
        &self,
        store: &EmailStore,
        grant_id: Option<&str>,
        held_id: &str,
    ) -> Result<()> {
        let Some(grant_id) = grant_id else {
            return Ok(());
        };
        self.revoke_one_shot_release_grant(store, &self.config.channel_id, grant_id, held_id)
            .await
    }

    async fn revoke_one_shot_release_grant(
        &self,
        store: &EmailStore,
        channel_id: &str,
        grant_id: &str,
        held_id: &str,
    ) -> Result<()> {
        match self
            .api
            .revoke_channel_grant(channel_id, grant_id, "email_one_shot_release_consumed")
            .await
        {
            Ok(()) => store.clear_pending_grant_revocation(grant_id).await?,
            Err(err) => {
                warn!(
                    grant_id,
                    held_id,
                    error = %err,
                    "failed to revoke consumed one-shot email release grant"
                );
                store
                    .record_pending_grant_revocation(
                        grant_id,
                        channel_id,
                        held_id,
                        &err.to_string(),
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn report_result(
        &self,
        delivery: &ChannelOutboxDelivery,
        worker_id: &str,
        result: DeliveryResult,
    ) -> Result<()> {
        match result {
            DeliveryResult::Delivered { receipt } => {
                self.api
                    .report_outbox(ChannelOutboxReportInput {
                        delivery,
                        channel_id: &self.config.channel_id,
                        worker_id,
                        outcome: "delivered",
                        provider_receipt: Some(receipt),
                        error_code: None,
                        error_text: None,
                    })
                    .await
            }
            DeliveryResult::RetryableFailed { code, text } => {
                warn!(delivery_id = %delivery.delivery_id, code, error = %text, "email delivery retryable failure");
                self.api
                    .report_outbox(ChannelOutboxReportInput {
                        delivery,
                        channel_id: &self.config.channel_id,
                        worker_id,
                        outcome: "retryable_failed",
                        provider_receipt: None,
                        error_code: Some(code),
                        error_text: Some(&text),
                    })
                    .await
            }
            DeliveryResult::TerminalFailed { code, text } => {
                warn!(delivery_id = %delivery.delivery_id, code, error = %text, "email delivery terminal failure");
                self.api
                    .report_outbox(ChannelOutboxReportInput {
                        delivery,
                        channel_id: &self.config.channel_id,
                        worker_id,
                        outcome: "terminal_failed",
                        provider_receipt: None,
                        error_code: Some(code),
                        error_text: Some(&text),
                    })
                    .await
            }
        }
    }

    async fn report_ready(&self, worker_id: &str, daemon: &DaemonInfoResponse) {
        let _ = self
            .api
            .report_health(
                &self.config.channel_id,
                worker_id,
                "ok",
                "email.ready",
                "email worker is running",
                json!({
                    "mailbox_id": self.config.mailbox.mailbox_id,
                    "address": self.config.mailbox.address,
                    "home": self.config.home.display().to_string(),
                    "daemon_bind_addr": daemon.bind_addr,
                }),
            )
            .await
            .inspect_err(|err| warn!(error = %err, "failed to report email health"));
    }

    fn worker_id(&self, daemon: &DaemonInfoResponse) -> String {
        if self.config.worker_id == format!("{CHANNEL_ID}:worker") {
            format!(
                "{CHANNEL_ID}:worker:{}:{}",
                daemon.home_id, self.config.mailbox.mailbox_id
            )
        } else {
            self.config.worker_id.clone()
        }
    }
}

async fn stage_and_finalize_attachments(
    api: &LionClawApi,
    channel_id: &str,
    event_id: &str,
    worker_id: &str,
    attachments: &[PreparedInboundAttachment],
) -> Result<()> {
    let mut missing = Vec::new();
    for attachment in attachments {
        let staged = api
            .stage_attachment(
                channel_id,
                event_id,
                &attachment.descriptor,
                attachment.content.clone(),
            )
            .await
            .context("failed to stage email attachment")?;
        match staged.status.as_str() {
            "staged" | "duplicate" => {}
            "rejected" => missing.push(ChannelAttachmentMissingReport {
                attachment_id: attachment.descriptor.attachment_id.clone(),
                reason_code: staged
                    .reason_code
                    .unwrap_or_else(|| "attachment_rejected".to_string()),
                reason_text: None,
            }),
            other => missing.push(ChannelAttachmentMissingReport {
                attachment_id: attachment.descriptor.attachment_id.clone(),
                reason_code: "unexpected_stage_status".to_string(),
                reason_text: Some(other.to_string()),
            }),
        }
    }
    let finalized = api
        .finalize_attachments(channel_id, event_id, worker_id, &missing)
        .await
        .context("failed to finalize email attachments")?;
    match finalized.outcome.as_str() {
        "queued" | "already_finalized" => Ok(()),
        "not_ready" => bail!("email attachments were not ready after staging"),
        other => bail!("unexpected attachment finalize outcome '{other}'"),
    }
}

async fn suppress_candidate(
    store: &EmailStore,
    mailbox: &mut dyn MailboxEngine,
    candidate: &CandidateHeader,
    reason: &str,
) -> Result<()> {
    store.record_suppressed(candidate, reason).await?;
    match mailbox.record_seen_or_processed(candidate).await {
        Ok(()) => Ok(()),
        Err(err) if is_stale_mailbox_candidate(&err) => {
            warn!(
                event_id = %candidate.event_id,
                sender_ref = %candidate.sender_ref,
                error = %err,
                "suppressed email candidate became stale before it could be marked processed"
            );
            Ok(())
        }
        Err(err) => Err(err),
    }
}

async fn process_malformed_candidate(
    store: &EmailStore,
    mailbox: &mut dyn MailboxEngine,
    candidate: &MalformedCandidateHeader,
) -> Result<()> {
    match store.mail_status(&candidate.event_id).await? {
        Some(MailStatus::Admitted | MailStatus::Suppressed) => {}
        Some(MailStatus::Held) | None => store.record_malformed_suppressed(candidate).await?,
    }

    match mailbox.record_malformed_seen_or_processed(candidate).await {
        Ok(()) => Ok(()),
        Err(err) if is_stale_mailbox_candidate(&err) => {
            warn!(
                event_id = %candidate.event_id,
                sender_ref = %candidate.sender_ref,
                error = %err,
                "malformed email candidate became stale before it could be marked processed"
            );
            Ok(())
        }
        Err(err) => Err(err),
    }
}

async fn mark_held_candidate_processed(
    store: &EmailStore,
    mailbox: &mut dyn MailboxEngine,
    candidate: &CandidateHeader,
) -> Result<()> {
    match mailbox.record_seen_or_processed(candidate).await {
        Ok(()) => Ok(()),
        Err(err) if is_stale_mailbox_candidate(&err) => {
            warn!(
                event_id = %candidate.event_id,
                sender_ref = %candidate.sender_ref,
                error = %err,
                "held email candidate became stale; suppressing held record"
            );
            store
                .record_suppressed(candidate, STALE_MAILBOX_UID_REASON)
                .await
        }
        Err(err) => {
            warn!(
                event_id = %candidate.event_id,
                sender_ref = %candidate.sender_ref,
                error = %err,
                "failed to mark held email candidate processed"
            );
            Ok(())
        }
    }
}

fn candidate_message_size_exceeds_limit(
    candidate: &CandidateHeader,
    max_message_bytes: usize,
) -> bool {
    candidate
        .rfc822_size
        .is_some_and(|size| u64::from(size) > max_message_bytes as u64)
}

fn one_shot_release_grant_id<'a>(
    authorization: &'a crate::api::ChannelActorAuthorizeResponse,
    held_id: &str,
    previously_held: bool,
) -> Option<&'a str> {
    if !previously_held || authorization.one_shot_release_held_id() != Some(held_id) {
        return None;
    }
    authorization.grant_id.as_deref()
}

fn prepare_inbound_attachments(
    mailbox_id: &str,
    candidate: &CandidateHeader,
    parsed: &ParsedEmail,
) -> Result<Vec<PreparedInboundAttachment>> {
    parsed
        .attachments
        .iter()
        .enumerate()
        .map(|(index, attachment)| {
            prepare_inbound_attachment(mailbox_id, candidate, index, attachment)
        })
        .collect()
}

fn prepare_inbound_attachment(
    mailbox_id: &str,
    candidate: &CandidateHeader,
    index: usize,
    attachment: &ParsedAttachment,
) -> Result<PreparedInboundAttachment> {
    let part_index = index + 1;
    let size_bytes = i64::try_from(attachment.content.len()).context("attachment too large")?;
    Ok(PreparedInboundAttachment {
        descriptor: ChannelAttachmentDescriptor {
            attachment_id: format!("email-att-{part_index}"),
            kind: ATTACHMENT_KIND.to_string(),
            mime_type: attachment.mime_type.clone(),
            filename: attachment.filename.clone(),
            size_bytes: Some(size_bytes),
            provider_file_ref: attachment_provider_ref(
                mailbox_id,
                candidate.uid_validity,
                candidate.uid,
                part_index,
            ),
            caption: attachment.filename.clone(),
        },
        content: attachment.content.clone(),
    })
}

async fn prepare_outbound_attachments(
    attachments: &[ChannelOutboxAttachment],
    max_message_bytes: usize,
    initial_bytes: usize,
) -> std::result::Result<Vec<OutboundAttachment>, OutboundAttachmentError> {
    let max_message_bytes = u64::try_from(max_message_bytes).unwrap_or(u64::MAX);
    let mut total_bytes = u64::try_from(initial_bytes).unwrap_or(u64::MAX);
    if total_bytes > max_message_bytes {
        return Err(OutboundAttachmentError::new(
            "message_too_large",
            anyhow!(
                "email delivery body exceeds EMAIL_MAX_MESSAGE_BYTES ({max_message_bytes} bytes)"
            ),
        ));
    }
    let mut prepared = Vec::with_capacity(attachments.len());
    for attachment in attachments {
        let path = PathBuf::from(&attachment.path);
        let metadata = fs::symlink_metadata(&path).map_err(|err| {
            OutboundAttachmentError::new(
                "attachment_unreadable",
                anyhow!("failed to stat attachment {}: {err}", path.display()),
            )
        })?;
        if metadata.file_type().is_symlink() {
            return Err(OutboundAttachmentError::new(
                "attachment_unsafe_path",
                anyhow!("attachment {} must not be a symlink", path.display()),
            ));
        }
        if !metadata.is_file() {
            return Err(OutboundAttachmentError::new(
                "attachment_unreadable",
                anyhow!("attachment {} is not a regular file", path.display()),
            ));
        }
        let attachment_bytes = metadata.len();
        let remaining = max_message_bytes.saturating_sub(total_bytes);
        if attachment_bytes > remaining {
            return Err(OutboundAttachmentError::new(
                "message_too_large",
                anyhow!(
                    "attachment {} would exceed EMAIL_MAX_MESSAGE_BYTES ({max_message_bytes} bytes)",
                    path.display()
                ),
            ));
        }
        let file = tokio::fs::File::open(&path).await.map_err(|err| {
            OutboundAttachmentError::new(
                "attachment_unreadable",
                anyhow!("failed to open attachment {}: {err}", path.display()),
            )
        })?;
        let opened_metadata = file.metadata().await.map_err(|err| {
            OutboundAttachmentError::new(
                "attachment_unreadable",
                anyhow!(
                    "failed to inspect opened attachment {}: {err}",
                    path.display()
                ),
            )
        })?;
        ensure_opened_same_regular_file(&path, &metadata, &opened_metadata)?;
        let mut content = Vec::new();
        file.take(remaining.saturating_add(1))
            .read_to_end(&mut content)
            .await
            .map_err(|err| {
                OutboundAttachmentError::new(
                    "attachment_unreadable",
                    anyhow!("failed to read attachment {}: {err}", path.display()),
                )
            })?;
        let content_len = u64::try_from(content.len()).unwrap_or(u64::MAX);
        if content_len > remaining {
            return Err(OutboundAttachmentError::new(
                "message_too_large",
                anyhow!(
                    "attachment {} grew while reading and exceeded EMAIL_MAX_MESSAGE_BYTES ({max_message_bytes} bytes)",
                    path.display()
                ),
            ));
        }
        total_bytes = total_bytes.saturating_add(content_len);
        prepared.push(OutboundAttachment {
            filename: attachment
                .filename
                .as_deref()
                .and_then(sanitize_header_text)
                .or_else(|| filename_from_path(&path).and_then(|name| sanitize_header_text(&name)))
                .unwrap_or_else(|| attachment.attachment_id.clone()),
            mime_type: attachment
                .mime_type
                .clone()
                .unwrap_or_else(|| "application/octet-stream".to_string()),
            content,
        });
    }
    Ok(prepared)
}

fn ensure_opened_same_regular_file(
    path: &Path,
    expected: &fs::Metadata,
    opened: &fs::Metadata,
) -> std::result::Result<(), OutboundAttachmentError> {
    if !opened.is_file() {
        return Err(OutboundAttachmentError::new(
            "attachment_unreadable",
            anyhow!("attachment {} is not a regular file", path.display()),
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        if expected.dev() != opened.dev() || expected.ino() != opened.ino() {
            return Err(OutboundAttachmentError::new(
                "attachment_unsafe_path",
                anyhow!("attachment {} changed while being opened", path.display()),
            ));
        }
    }

    Ok(())
}

fn filename_from_path(path: &Path) -> Option<String> {
    path.file_name()
        .and_then(|value| value.to_str())
        .map(str::to_string)
}

fn inbound_envelope_text(
    mailbox_address: &str,
    candidate: &CandidateHeader,
    parsed: &ParsedEmail,
) -> String {
    let mut lines = vec![
        "Email received for LionClaw.".to_string(),
        String::new(),
        "Admission: allowed".to_string(),
        format!("From: {}", display_address(&parsed.facts.sender)),
        format!("To: {mailbox_address}"),
        format!("Subject: {}", parsed.facts.subject),
        format!(
            "Received: {}",
            parsed
                .facts
                .received_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "unknown".to_string())
        ),
        format!("Thread: {}", candidate.thread_ref),
        String::new(),
        "Latest message (untrusted external input):".to_string(),
        parsed.text.clone(),
    ];
    let summary = attachment_summary(&parsed.attachments);
    if !summary.is_empty() {
        lines.push(String::new());
        lines.push("Attachments:".to_string());
        lines.extend(summary.into_iter().map(|item| format!("- {item}")));
    }
    lines.join("\n")
}

fn display_address(address: &crate::mime::EmailAddress) -> String {
    address.display_name.as_ref().map_or_else(
        || address.address.clone(),
        |name| format!("{name} <{}>", address.address),
    )
}

fn reply_subject(subject: &str) -> String {
    if subject.trim_start().to_ascii_lowercase().starts_with("re:") {
        subject.to_string()
    } else {
        format!("Re: {subject}")
    }
}

fn reply_references(context: &ThreadContext) -> Vec<String> {
    let mut references = context.references.clone();
    if let Some(provider_message_id) = &context.provider_message_id {
        if !references
            .iter()
            .any(|existing| existing == provider_message_id)
        {
            references.push(provider_message_id.clone());
        }
    }
    references
}

fn digest_text(
    mailbox_address: &str,
    held: &[crate::store::HeldItem],
    suppressed_count: i64,
) -> String {
    let mut lines = vec![
        format!("LionClaw email digest for {mailbox_address}"),
        String::new(),
        format!("Held messages: {}", held.len()),
        format!("Suppressed automated messages: {suppressed_count}"),
    ];
    if !held.is_empty() {
        lines.push(String::new());
        lines.push(
            "Held messages were not downloaded because the sender is not approved.".to_string(),
        );
        for item in held {
            lines.push(String::new());
            lines.push(format!("Held ID: {}", item.held_id));
            lines.push(format!("From: {}", item.sender_address));
            lines.push(format!("Subject: {}", item.subject));
            if let Some(received_at) = &item.received_at {
                lines.push(format!("Received: {received_at}"));
            }
            lines.push(format!("Snippet: {}", item.snippet));
            lines.push(format!("Attachments: {}", item.attachment_count));
            lines.push(format!("Sender ref: {}", item.sender_ref));
            lines.push(format!("Conversation ref: {}", item.conversation_ref));
            lines.push(format!("Thread ref: {}", item.thread_ref));
            lines.push(format!("Message ref: {}", item.message_ref));
            if let Some(reason) = &item.classification_reason {
                lines.push(format!("Reason: {reason}"));
            }
            lines.push(format!(
                "Actions: allow {}, block {}, or release {} once with a direct sender grant labeled email-release:{}.",
                item.sender_address, item.sender_address, item.held_id, item.held_id
            ));
        }
    }
    lines.join("\n")
}

fn validate_local_daemon(info: &DaemonInfoResponse, expected_home: &Path) -> Result<()> {
    if info.daemon != "lionclawd" {
        bail!("local endpoint is not lionclawd: {}", info.daemon);
    }
    if info.status != "ok" {
        bail!("local daemon is not ok: {}", info.status);
    }
    if info.home_root.trim().is_empty() {
        bail!("local daemon did not report home root");
    }
    let expected_home = fs::canonicalize(expected_home).with_context(|| {
        format!(
            "failed to resolve configured LionClaw home {}",
            expected_home.display()
        )
    })?;
    let reported_home = fs::canonicalize(&info.home_root).with_context(|| {
        format!(
            "failed to resolve reported LionClaw home {}",
            info.home_root
        )
    })?;
    if reported_home != expected_home {
        bail!(
            "local daemon home root mismatch: expected {}, got {}",
            expected_home.display(),
            reported_home.display()
        );
    }
    Ok(())
}

fn retryable(code: &'static str, err: anyhow::Error) -> DeliveryResult {
    DeliveryResult::RetryableFailed {
        code,
        text: err.to_string(),
    }
}

fn terminal(code: &'static str, err: anyhow::Error) -> DeliveryResult {
    DeliveryResult::TerminalFailed {
        code,
        text: err.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    };

    use async_trait::async_trait;
    use axum::{
        extract::State,
        http::StatusCode,
        response::IntoResponse,
        routing::{get, post},
        Json, Router,
    };
    use tempfile::tempdir;
    use tokio::net::TcpListener;

    use super::*;
    use crate::{
        config::{DigestConfig, ImapTlsMode, MailboxConfig, DEFAULT_MAX_MESSAGE_BYTES},
        mailbox::{CandidateHeaderBatch, FetchedMessage, MailboxFactory, StaleMailboxCandidate},
        mime::parse_headers_for_test,
        protocol::{conversation_ref, message_ref, sender_ref, thread_ref},
    };

    #[test]
    fn reply_subject_keeps_existing_re_prefix() {
        assert_eq!(reply_subject("Build failed"), "Re: Build failed");
        assert_eq!(reply_subject("Re: Build failed"), "Re: Build failed");
    }

    #[tokio::test]
    async fn outbound_attachment_filename_is_line_safe() {
        let temp_dir = tempdir().expect("temp dir");
        let path = temp_dir.path().join("artifact.txt");
        std::fs::write(&path, "hello").expect("write attachment");

        let attachments = vec![ChannelOutboxAttachment {
            attachment_id: "att-1".to_string(),
            path: path.display().to_string(),
            filename: Some("report\r\nInjected: yes.txt".to_string()),
            mime_type: Some("text/plain".to_string()),
        }];

        let prepared = prepare_outbound_attachments(&attachments, DEFAULT_MAX_MESSAGE_BYTES, 0)
            .await
            .expect("prepare attachments");

        assert_eq!(prepared[0].filename, "report Injected: yes.txt");
    }

    #[tokio::test]
    async fn oversized_outbound_attachment_is_rejected_before_smtp() {
        let temp_dir = tempdir().expect("temp dir");
        let path = temp_dir.path().join("too-large.bin");
        std::fs::write(&path, [0_u8; 17]).expect("write attachment");

        let attachments = vec![ChannelOutboxAttachment {
            attachment_id: "att-1".to_string(),
            path: path.display().to_string(),
            filename: Some("too-large.bin".to_string()),
            mime_type: Some("application/octet-stream".to_string()),
        }];

        let err = prepare_outbound_attachments(&attachments, 16, 0)
            .await
            .expect_err("oversized attachment should fail before read");

        assert_eq!(err.code, "message_too_large");
        assert!(err.source.to_string().contains("EMAIL_MAX_MESSAGE_BYTES"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinked_outbound_attachment_is_rejected_before_reading() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempdir().expect("temp dir");
        let target = temp_dir.path().join("target.bin");
        std::fs::write(&target, "secret").expect("write target");
        let link = temp_dir.path().join("link.bin");
        symlink(&target, &link).expect("attachment symlink");

        let attachments = vec![ChannelOutboxAttachment {
            attachment_id: "att-1".to_string(),
            path: link.display().to_string(),
            filename: Some("link.bin".to_string()),
            mime_type: Some("application/octet-stream".to_string()),
        }];

        let err = prepare_outbound_attachments(&attachments, DEFAULT_MAX_MESSAGE_BYTES, 0)
            .await
            .expect_err("symlinked attachment should fail before read");

        assert_eq!(err.code, "attachment_unsafe_path");
    }

    #[cfg(unix)]
    #[test]
    fn changed_outbound_attachment_path_is_rejected_after_opening() {
        let temp_dir = tempdir().expect("temp dir");
        let expected_path = temp_dir.path().join("expected.bin");
        let opened_path = temp_dir.path().join("opened.bin");
        std::fs::write(&expected_path, "expected").expect("write expected");
        std::fs::write(&opened_path, "opened").expect("write opened");
        let expected = std::fs::symlink_metadata(&expected_path).expect("expected metadata");
        let opened = std::fs::metadata(&opened_path).expect("opened metadata");

        let err = ensure_opened_same_regular_file(&expected_path, &expected, &opened)
            .expect_err("changed attachment path should fail");

        assert_eq!(err.code, "attachment_unsafe_path");
    }

    #[tokio::test]
    async fn outbound_text_and_attachments_share_message_size_budget() {
        let temp_dir = tempdir().expect("temp dir");
        let path = temp_dir.path().join("part.bin");
        std::fs::write(&path, [0_u8; 8]).expect("write attachment");

        let attachments = vec![ChannelOutboxAttachment {
            attachment_id: "att-1".to_string(),
            path: path.display().to_string(),
            filename: Some("part.bin".to_string()),
            mime_type: Some("application/octet-stream".to_string()),
        }];

        let err = prepare_outbound_attachments(&attachments, 16, 9)
            .await
            .expect_err("text and attachment total should be capped");

        assert_eq!(err.code, "message_too_large");
    }

    #[test]
    fn inbound_envelope_wraps_untrusted_body() {
        let facts = parse_headers_for_test(
            "From: Alice <alice@example.com>\r\nSubject: Build failed\r\nMessage-ID: <m1@example.com>\r\n\r\n",
        );
        let candidate = CandidateHeader {
            uid_validity: 1,
            uid: 2,
            event_id: "email:imap:box:1:2".to_string(),
            sender_ref: "email:addr:alice@example.com".to_string(),
            conversation_ref: "email:mailbox:box".to_string(),
            thread_ref: "email:thread:root".to_string(),
            message_ref: "email:message:m1".to_string(),
            attachment_count: 0,
            rfc822_size: Some(128),
            facts: facts.clone(),
        };
        let parsed = ParsedEmail {
            facts,
            text: "Please check this.".to_string(),
            snippet: "Please check this.".to_string(),
            attachments: Vec::new(),
        };

        let text = inbound_envelope_text("assistant@example.com", &candidate, &parsed);

        assert!(text.contains("Admission: allowed"));
        assert!(text.contains("Latest message (untrusted external input):"));
        assert!(text.contains("Please check this."));
    }

    #[tokio::test]
    async fn unauthorized_sender_is_held_without_full_fetch() {
        let fixture = EmailFixture::new(false).await;

        EmailWorker::new(fixture.config(), fixture.mailbox.clone())
            .expect("worker")
            .tick()
            .await
            .expect("tick");

        assert_eq!(fixture.mailbox.full_fetches(), 0);
        assert_eq!(fixture.mailbox.seen(), 1);
        assert_eq!(fixture.api.inbound_requests.lock().unwrap().len(), 0);
        let store = EmailStore::open(&fixture.state_dir).await.expect("store");
        assert_eq!(
            store
                .mail_status("email:imap:assistant-example-com:7:42")
                .await
                .expect("status"),
            Some(MailStatus::Held)
        );
        let held = store
            .held_since_last_digest(10)
            .await
            .expect("held digest rows");
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].snippet, held_body_not_downloaded_text());
    }

    #[tokio::test]
    async fn authorized_sender_is_fetched_and_queued() {
        let fixture = EmailFixture::new(true).await;

        EmailWorker::new(fixture.config(), fixture.mailbox.clone())
            .expect("worker")
            .tick()
            .await
            .expect("tick");

        assert_eq!(fixture.mailbox.full_fetches(), 1);
        {
            let inbound = fixture.api.inbound_requests.lock().unwrap();
            assert_eq!(inbound.len(), 1);
            assert_eq!(inbound[0]["sender_ref"], "email:addr:alice@example.com");
            assert_eq!(inbound[0]["thread_ref"], thread_ref("root@example.com"));
            assert_eq!(inbound[0]["reply_to_ref"], message_ref("root@example.com"));
            assert_eq!(
                inbound[0]["session_binding"],
                crate::protocol::INBOUND_SESSION_BINDING
            );
            assert!(inbound[0]["text"]
                .as_str()
                .expect("inbound text")
                .contains("Please check this."));
            let metadata = inbound[0]["provider_metadata"]
                .as_object()
                .expect("provider metadata");
            assert_eq!(
                metadata
                    .get("authorization_reason_code")
                    .and_then(|value| value.as_str()),
                Some("approved")
            );
            assert!(!metadata.contains_key("grant_id"));
            assert!(!metadata.contains_key("references"));
        }
        {
            let authorize = fixture.api.authorize_requests.lock().unwrap();
            assert_eq!(authorize.len(), 1);
            assert_eq!(authorize[0]["thread_ref"], thread_ref("root@example.com"));
            assert_eq!(
                authorize[0]["session_binding"],
                crate::protocol::INBOUND_SESSION_BINDING
            );
            assert_eq!(authorize[0]["trigger"], crate::protocol::INBOUND_TRIGGER);
        }
        let store = EmailStore::open(&fixture.state_dir).await.expect("store");
        assert_eq!(
            store
                .mail_status("email:imap:assistant-example-com:7:42")
                .await
                .expect("status"),
            Some(MailStatus::Admitted)
        );
    }

    #[tokio::test]
    async fn authorized_oversized_message_is_suppressed_without_full_fetch() {
        let fixture = EmailFixture::with_candidate(
            true,
            candidate_with_rfc822_size(Some(1025)),
            full_message(),
        )
        .await;
        let mut config = fixture.config();
        config.mailbox.max_message_bytes = 1024;

        EmailWorker::new(config, fixture.mailbox.clone())
            .expect("worker")
            .tick()
            .await
            .expect("tick");

        assert_eq!(fixture.mailbox.full_fetches(), 0);
        assert_eq!(fixture.mailbox.seen(), 1);
        assert_eq!(fixture.api.inbound_requests.lock().unwrap().len(), 0);
        let store = EmailStore::open(&fixture.state_dir).await.expect("store");
        assert_eq!(
            store
                .mail_status("email:imap:assistant-example-com:7:42")
                .await
                .expect("status"),
            Some(MailStatus::Suppressed)
        );
    }

    #[tokio::test]
    async fn authorized_unknown_size_message_is_suppressed_after_bounded_fetch() {
        let fixture = EmailFixture::with_candidate(
            true,
            candidate_with_rfc822_size(None),
            oversized_message(2048),
        )
        .await;
        let mut config = fixture.config();
        config.mailbox.max_message_bytes = 1024;

        EmailWorker::new(config, fixture.mailbox.clone())
            .expect("worker")
            .tick()
            .await
            .expect("tick");

        assert_eq!(fixture.mailbox.full_fetches(), 1);
        assert_eq!(fixture.mailbox.seen(), 1);
        assert_eq!(fixture.api.inbound_requests.lock().unwrap().len(), 0);
        let store = EmailStore::open(&fixture.state_dir).await.expect("store");
        assert_eq!(
            store
                .mail_status("email:imap:assistant-example-com:7:42")
                .await
                .expect("status"),
            Some(MailStatus::Suppressed)
        );
    }

    #[tokio::test]
    async fn suppressed_automated_mail_is_marked_processed_without_full_fetch() {
        let candidate = candidate_from_headers(
            "From: Robot <robot@example.com>\r\n\
             To: Assistant <assistant@example.com>\r\n\
             Subject: Automated reply\r\n\
             Message-ID: <auto@example.com>\r\n\
             Auto-Submitted: auto-replied\r\n\
             \r\n",
        );
        let event_id = candidate.event_id.clone();
        let fixture = EmailFixture::with_candidate(false, candidate, full_message()).await;

        EmailWorker::new(fixture.config(), fixture.mailbox.clone())
            .expect("worker")
            .tick()
            .await
            .expect("tick");

        assert_eq!(fixture.mailbox.full_fetches(), 0);
        assert_eq!(fixture.mailbox.seen(), 1);
        assert_eq!(fixture.api.authorize_requests.lock().unwrap().len(), 0);
        assert_eq!(fixture.api.inbound_requests.lock().unwrap().len(), 0);
        let store = EmailStore::open(&fixture.state_dir).await.expect("store");
        assert_eq!(
            store.mail_status(&event_id).await.expect("status"),
            Some(MailStatus::Suppressed)
        );
    }

    #[tokio::test]
    async fn copied_provider_message_is_suppressed_without_requeueing() {
        let original = candidate();
        let duplicate = candidate_with_uid(43);
        let duplicate_event_id = duplicate.event_id.clone();
        let fixture = EmailFixture::with_candidate(true, duplicate, full_message()).await;
        let store = EmailStore::open(&fixture.state_dir).await.expect("store");
        store
            .record_admitted(&original, "downloaded")
            .await
            .expect("record original");

        EmailWorker::new(fixture.config(), fixture.mailbox.clone())
            .expect("worker")
            .tick()
            .await
            .expect("tick");

        assert_eq!(fixture.mailbox.full_fetches(), 0);
        assert_eq!(fixture.mailbox.seen(), 1);
        assert_eq!(fixture.api.authorize_requests.lock().unwrap().len(), 0);
        assert_eq!(fixture.api.inbound_requests.lock().unwrap().len(), 0);
        assert_eq!(
            store
                .mail_status(&duplicate_event_id)
                .await
                .expect("duplicate status"),
            Some(MailStatus::Suppressed)
        );
    }

    #[tokio::test]
    async fn malformed_header_candidate_is_persisted_before_marking_seen() {
        let malformed = malformed_candidate(44);
        let event_id = malformed.event_id.clone();
        let fixture = EmailFixture::new(true).await;
        fixture.mailbox.set_candidates(Vec::new());
        fixture.mailbox.set_malformed_candidates(vec![malformed]);

        EmailWorker::new(fixture.config(), fixture.mailbox.clone())
            .expect("worker")
            .tick()
            .await
            .expect("tick");

        assert_eq!(fixture.mailbox.full_fetches(), 0);
        assert_eq!(fixture.mailbox.seen(), 1);
        assert_eq!(fixture.api.authorize_requests.lock().unwrap().len(), 0);
        assert_eq!(fixture.api.inbound_requests.lock().unwrap().len(), 0);
        let store = EmailStore::open(&fixture.state_dir).await.expect("store");
        assert_eq!(
            store.mail_status(&event_id).await.expect("status"),
            Some(MailStatus::Suppressed)
        );
    }

    #[tokio::test]
    async fn terminal_admitted_mail_is_marked_processed_without_requeueing() {
        let fixture = EmailFixture::new(true).await;

        let worker = EmailWorker::new(fixture.config(), fixture.mailbox.clone()).expect("worker");
        worker.tick().await.expect("first tick");
        worker.tick().await.expect("second tick");

        assert_eq!(fixture.mailbox.full_fetches(), 1);
        assert_eq!(fixture.mailbox.seen(), 2);
        assert_eq!(fixture.api.authorize_requests.lock().unwrap().len(), 1);
        assert_eq!(fixture.api.inbound_requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn local_daemon_home_mismatch_fails_before_mailbox_access() {
        let fixture = EmailFixture::new(true).await;
        let foreign_home = fixture.root.path().join("foreign-home");
        std::fs::create_dir_all(&foreign_home).expect("foreign home");
        fixture.api.set_home_root(&foreign_home);

        let err = EmailWorker::new(fixture.config(), fixture.mailbox.clone())
            .expect("worker")
            .tick()
            .await
            .expect_err("foreign daemon home should fail");

        assert!(
            err.to_string().contains("home root mismatch"),
            "unexpected error: {err:#}"
        );
        assert_eq!(fixture.mailbox.full_fetches(), 0);
        assert_eq!(fixture.api.authorize_requests.lock().unwrap().len(), 0);
        assert_eq!(fixture.api.inbound_requests.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn malformed_authorized_mail_is_suppressed_and_marked_processed() {
        let fixture = EmailFixture::with_candidate(
            true,
            candidate(),
            b"Subject: Broken message\r\n\r\nNo usable sender header.".to_vec(),
        )
        .await;

        EmailWorker::new(fixture.config(), fixture.mailbox.clone())
            .expect("worker")
            .tick()
            .await
            .expect("tick");

        assert_eq!(fixture.mailbox.full_fetches(), 1);
        assert_eq!(fixture.mailbox.seen(), 1);
        assert_eq!(fixture.api.inbound_requests.lock().unwrap().len(), 0);
        let store = EmailStore::open(&fixture.state_dir).await.expect("store");
        assert_eq!(
            store
                .mail_status("email:imap:assistant-example-com:7:42")
                .await
                .expect("status"),
            Some(MailStatus::Suppressed)
        );
    }

    #[tokio::test]
    async fn held_sender_is_fetched_after_later_authorization() {
        let fixture = EmailFixture::new(false).await;

        let worker = EmailWorker::new(fixture.config(), fixture.mailbox.clone()).expect("worker");
        worker.tick().await.expect("first tick");
        assert_eq!(fixture.mailbox.full_fetches(), 0);
        assert_eq!(fixture.mailbox.seen(), 1);

        fixture.mailbox.set_candidates(Vec::new());
        fixture.api.set_authorized(true);
        worker.tick().await.expect("second tick");

        assert_eq!(fixture.mailbox.full_fetches(), 1);
        assert_eq!(fixture.mailbox.seen(), 2);
        assert_eq!(fixture.api.inbound_requests.lock().unwrap().len(), 1);
        let store = EmailStore::open(&fixture.state_dir).await.expect("store");
        assert_eq!(
            store
                .mail_status("email:imap:assistant-example-com:7:42")
                .await
                .expect("status"),
            Some(MailStatus::Admitted)
        );
    }

    #[tokio::test]
    async fn stale_held_uidvalidity_is_suppressed_without_fetching_or_marking_seen() {
        let fixture = EmailFixture::new(false).await;
        let store = EmailStore::open(&fixture.state_dir).await.expect("store");
        let candidate = candidate();
        store
            .record_held(
                &candidate,
                &held_id_for(&candidate.event_id),
                held_body_not_downloaded_text(),
                "approval_required",
            )
            .await
            .expect("record held");
        fixture.mailbox.set_candidates(Vec::new());
        fixture.mailbox.set_stale_uid_validity(true);

        let worker = EmailWorker::new(fixture.config(), fixture.mailbox.clone()).expect("worker");
        worker.tick().await.expect("tick");

        assert_eq!(fixture.mailbox.full_fetches(), 0);
        assert_eq!(fixture.mailbox.seen(), 0);
        assert_eq!(
            store
                .mail_status(&candidate.event_id)
                .await
                .expect("status"),
            Some(MailStatus::Suppressed)
        );
    }

    #[tokio::test]
    async fn stale_released_uidvalidity_is_suppressed_and_revokes_release_grant() {
        let fixture = EmailFixture::new(false).await;
        let store = EmailStore::open(&fixture.state_dir).await.expect("store");
        let candidate = candidate();
        let held_id = held_id_for(&candidate.event_id);
        store
            .record_held(
                &candidate,
                &held_id,
                held_body_not_downloaded_text(),
                "approval_required",
            )
            .await
            .expect("record held");
        fixture.mailbox.set_candidates(Vec::new());
        fixture.mailbox.set_stale_uid_validity(true);
        fixture.api.set_one_shot_release_authorized(&held_id);

        let worker = EmailWorker::new(fixture.config(), fixture.mailbox.clone()).expect("worker");
        worker.tick().await.expect("tick");

        assert_eq!(fixture.mailbox.full_fetches(), 0);
        assert_eq!(fixture.mailbox.seen(), 0);
        assert_eq!(fixture.api.inbound_requests.lock().unwrap().len(), 0);
        assert_eq!(
            store
                .mail_status(&candidate.event_id)
                .await
                .expect("status"),
            Some(MailStatus::Suppressed)
        );
        let revoked = fixture.api.revoked_grants.lock().unwrap();
        assert_eq!(revoked.len(), 1);
        assert_eq!(
            revoked[0]["grant_id"],
            "00000000-0000-0000-0000-000000000086"
        );
    }

    #[tokio::test]
    async fn receipt_cache_failure_after_smtp_acceptance_does_not_retry_delivery() {
        let fixture = EmailFixture::new(true).await;
        let store = EmailStore::open(&fixture.state_dir).await.expect("store");
        let candidate = candidate();
        store
            .record_admitted(&candidate, "downloaded")
            .await
            .expect("record admitted context");
        install_failing_receipt_insert_trigger(&fixture.state_dir).await;

        let config = fixture.config();
        let worker = EmailWorker::new(config.clone(), fixture.mailbox.clone()).expect("worker");
        let mut mailbox = fixture.mailbox.open(config.mailbox.clone());
        let result = worker
            .deliver_outbox(
                &store,
                mailbox.as_mut(),
                &outbox_delivery_for_thread(candidate.thread_ref.clone()),
            )
            .await;

        let DeliveryResult::Delivered { receipt } = result else {
            panic!("post-SMTP receipt cache failure must not request retry");
        };
        assert_eq!(fixture.mailbox.sent().len(), 1);
        assert_eq!(receipt["local_receipt_recorded"], false);
        assert_eq!(
            receipt["local_receipt_record_error_code"],
            "receipt_record_failed"
        );
    }

    #[tokio::test]
    async fn one_shot_release_grant_is_revoked_after_admission() {
        let fixture = EmailFixture::new(false).await;

        let worker = EmailWorker::new(fixture.config(), fixture.mailbox.clone()).expect("worker");
        worker.tick().await.expect("first tick");

        let held_id = held_id_for("email:imap:assistant-example-com:7:42");
        fixture.api.set_one_shot_release_authorized(&held_id);
        worker.tick().await.expect("release tick");

        assert_eq!(fixture.mailbox.full_fetches(), 1);
        let revoked = fixture.api.revoked_grants.lock().unwrap();
        assert_eq!(revoked.len(), 1);
        assert_eq!(
            revoked[0]["grant_id"],
            "00000000-0000-0000-0000-000000000086"
        );
        assert_eq!(revoked[0]["reason"], "email_one_shot_release_consumed");
    }

    #[tokio::test]
    async fn mismatched_one_shot_release_grant_does_not_admit_other_held_mail() {
        let fixture = EmailFixture::new(false).await;

        let worker = EmailWorker::new(fixture.config(), fixture.mailbox.clone()).expect("worker");
        worker.tick().await.expect("first tick");

        fixture.api.set_one_shot_release_authorized("hld_different");
        worker.tick().await.expect("mismatched release tick");

        assert_eq!(fixture.mailbox.full_fetches(), 0);
        assert_eq!(fixture.api.inbound_requests.lock().unwrap().len(), 0);
        assert!(fixture.api.revoked_grants.lock().unwrap().is_empty());

        let store = EmailStore::open(&fixture.state_dir).await.expect("store");
        assert_eq!(
            store
                .mail_status("email:imap:assistant-example-com:7:42")
                .await
                .expect("status"),
            Some(MailStatus::Held)
        );
    }

    #[tokio::test]
    async fn one_shot_release_revocation_survives_failed_release_processing() {
        let fixture = EmailFixture::with_candidate(
            false,
            candidate(),
            b"Subject: Broken message\r\n\r\nNo usable sender header.".to_vec(),
        )
        .await;

        let worker = EmailWorker::new(fixture.config(), fixture.mailbox.clone()).expect("worker");
        worker.tick().await.expect("first tick");

        let held_id = held_id_for("email:imap:assistant-example-com:7:42");
        fixture.api.set_one_shot_release_authorized(&held_id);
        fixture.api.set_revoke_fails(true);
        worker.tick().await.expect("failed release tick");

        let store = EmailStore::open(&fixture.state_dir).await.expect("store");
        let pending = store
            .pending_grant_revocations()
            .await
            .expect("pending revocations");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].grant_id, "00000000-0000-0000-0000-000000000086");

        fixture.api.set_revoke_fails(false);
        fixture.api.set_revoke_missing(true);
        worker.tick().await.expect("retry absent revocation tick");

        assert!(store
            .pending_grant_revocations()
            .await
            .expect("pending revocations")
            .is_empty());
        let revoked = fixture.api.revoked_grants.lock().unwrap();
        assert_eq!(revoked.len(), 2);
        assert_eq!(
            revoked[1]["grant_id"],
            "00000000-0000-0000-0000-000000000086"
        );
    }

    #[tokio::test]
    async fn known_oversized_held_release_is_suppressed_without_fetching() {
        let fixture = EmailFixture::with_candidate(
            false,
            candidate_with_rfc822_size(Some(1025)),
            oversized_message(2048),
        )
        .await;
        let mut config = fixture.config();
        config.mailbox.max_message_bytes = 1024;

        let worker = EmailWorker::new(config, fixture.mailbox.clone()).expect("worker");
        worker.tick().await.expect("first tick");
        assert_eq!(fixture.mailbox.full_fetches(), 0);

        let held_id = held_id_for("email:imap:assistant-example-com:7:42");
        fixture.mailbox.set_candidates(Vec::new());
        fixture.api.set_one_shot_release_authorized(&held_id);
        worker.tick().await.expect("release tick");

        assert_eq!(fixture.mailbox.full_fetches(), 0);
        assert_eq!(fixture.mailbox.seen(), 2);
        let store = EmailStore::open(&fixture.state_dir).await.expect("store");
        assert_eq!(
            store
                .mail_status("email:imap:assistant-example-com:7:42")
                .await
                .expect("status"),
            Some(MailStatus::Suppressed)
        );
        let revoked = fixture.api.revoked_grants.lock().unwrap();
        assert_eq!(revoked.len(), 1);
        assert_eq!(
            revoked[0]["grant_id"],
            "00000000-0000-0000-0000-000000000086"
        );
    }

    struct EmailFixture {
        root: tempfile::TempDir,
        state_dir: PathBuf,
        api_url: String,
        api: Arc<ApiState>,
        mailbox: FakeMailboxFactory,
    }

    impl EmailFixture {
        async fn new(authorized: bool) -> Self {
            Self::with_candidate(authorized, candidate(), full_message()).await
        }

        async fn with_candidate(
            authorized: bool,
            candidate: CandidateHeader,
            raw: Vec<u8>,
        ) -> Self {
            let root = tempdir().expect("temp dir");
            let home = root.path().join("home");
            std::fs::create_dir_all(&home).expect("home dir");
            let state_dir = root.path().join("state");
            let api = Arc::new(ApiState::default());
            api.set_home_root(&home);
            api.set_authorized(authorized);
            let api_url = spawn_api(api.clone()).await;
            let mailbox = FakeMailboxFactory::new(candidate, raw);
            Self {
                root,
                state_dir,
                api_url,
                api,
                mailbox,
            }
        }

        fn config(&self) -> WorkerConfig {
            WorkerConfig {
                home: self.root.path().join("home"),
                state_dir: self.state_dir.clone(),
                base_url: self.api_url.clone(),
                channel_id: CHANNEL_ID.to_string(),
                worker_id: "email:worker".to_string(),
                once: true,
                poll_interval: std::time::Duration::from_millis(10),
                pull_limit: 10,
                lease_ms: 60_000,
                mailbox: MailboxConfig {
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
                    max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
                },
                digest: DigestConfig {
                    interval: std::time::Duration::from_secs(3600),
                    admin_to: None,
                },
            }
        }
    }

    #[derive(Default)]
    struct ApiState {
        home_root: Mutex<String>,
        authorized: AtomicBool,
        fail_revoke: AtomicBool,
        missing_revoke: AtomicBool,
        grant: Mutex<Option<AuthGrant>>,
        authorize_requests: Mutex<Vec<Value>>,
        inbound_requests: Mutex<Vec<Value>>,
        revoked_grants: Mutex<Vec<Value>>,
    }

    #[derive(Clone)]
    struct AuthGrant {
        grant_id: String,
        routing_profile: String,
        label: String,
    }

    impl ApiState {
        fn set_home_root(&self, home: &Path) {
            let home = home.canonicalize().expect("canonical home");
            *self.home_root.lock().unwrap() = home.display().to_string();
        }

        fn set_authorized(&self, authorized: bool) {
            self.authorized.store(authorized, Ordering::SeqCst);
            if !authorized {
                *self.grant.lock().unwrap() = None;
            }
        }

        fn set_revoke_fails(&self, fail: bool) {
            self.fail_revoke.store(fail, Ordering::SeqCst);
        }

        fn set_revoke_missing(&self, missing: bool) {
            self.missing_revoke.store(missing, Ordering::SeqCst);
        }

        fn set_one_shot_release_authorized(&self, held_id: &str) {
            self.authorized.store(true, Ordering::SeqCst);
            *self.grant.lock().unwrap() = Some(AuthGrant {
                grant_id: "00000000-0000-0000-0000-000000000086".to_string(),
                routing_profile: "direct".to_string(),
                label: format!("email-release:{held_id}"),
            });
        }
    }

    async fn spawn_api(state: Arc<ApiState>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let app = Router::new()
            .route("/v0/daemon/info", get(daemon_info))
            .route("/v0/channels/health/report", post(ok))
            .route("/v0/channels/authorize", post(authorize))
            .route("/v0/channels/inbound", post(inbound))
            .route("/v0/channels/grants/revoke", post(revoke_grant))
            .route("/v0/channels/outbox/pull", post(outbox_pull))
            .route("/v0/channels/outbox/report", post(ok))
            .with_state(state);
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server");
        });
        format!("http://{addr}")
    }

    async fn daemon_info(State(state): State<Arc<ApiState>>) -> Json<Value> {
        let home_root = state.home_root.lock().unwrap().clone();
        Json(json!({
            "daemon": "lionclawd",
            "status": "ok",
            "home_id": "home-test",
            "home_root": home_root,
            "bind_addr": "127.0.0.1:0"
        }))
    }

    async fn ok() -> Json<Value> {
        Json(json!({ "ok": true }))
    }

    async fn authorize(State(state): State<Arc<ApiState>>, Json(body): Json<Value>) -> Json<Value> {
        state.authorize_requests.lock().unwrap().push(body);
        let authorized = state.authorized.load(Ordering::SeqCst);
        let grant = state.grant.lock().unwrap().clone();
        Json(json!({
            "authorized": authorized,
            "reason_code": if authorized { "approved" } else { "approval_required" },
            "grant_id": grant.as_ref().map(|grant| grant.grant_id.as_str()),
            "grant_routing_profile": grant.as_ref().map(|grant| grant.routing_profile.as_str()),
            "grant_label": grant.as_ref().map(|grant| grant.label.as_str()),
        }))
    }

    async fn inbound(State(state): State<Arc<ApiState>>, Json(body): Json<Value>) -> Json<Value> {
        state.inbound_requests.lock().unwrap().push(body);
        Json(json!({ "outcome": "queued" }))
    }

    async fn revoke_grant(
        State(state): State<Arc<ApiState>>,
        Json(body): Json<Value>,
    ) -> impl IntoResponse {
        state.revoked_grants.lock().unwrap().push(body);
        if state.missing_revoke.load(Ordering::SeqCst) {
            (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "channel grant not found" })),
            )
        } else if state.fail_revoke.load(Ordering::SeqCst) {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "revoke failed" })),
            )
        } else {
            (StatusCode::OK, Json(json!({ "revoked": true })))
        }
    }

    async fn outbox_pull() -> Json<Value> {
        Json(json!({ "deliveries": [] }))
    }

    #[derive(Clone)]
    struct FakeMailboxFactory {
        state: Arc<FakeMailboxState>,
    }

    impl FakeMailboxFactory {
        fn new(candidate: CandidateHeader, raw: Vec<u8>) -> Self {
            Self {
                state: Arc::new(FakeMailboxState {
                    candidate,
                    override_candidates: Mutex::new(None),
                    override_malformed: Mutex::new(None),
                    raw,
                    stale_uid_validity: AtomicBool::new(false),
                    full_fetches: AtomicUsize::new(0),
                    seen: AtomicUsize::new(0),
                    sent: Mutex::new(Vec::new()),
                }),
            }
        }

        fn full_fetches(&self) -> usize {
            self.state.full_fetches.load(Ordering::SeqCst)
        }

        fn seen(&self) -> usize {
            self.state.seen.load(Ordering::SeqCst)
        }

        fn set_candidates(&self, candidates: Vec<CandidateHeader>) {
            *self.state.override_candidates.lock().unwrap() = Some(candidates);
        }

        fn set_malformed_candidates(&self, candidates: Vec<MalformedCandidateHeader>) {
            *self.state.override_malformed.lock().unwrap() = Some(candidates);
        }

        fn set_stale_uid_validity(&self, stale: bool) {
            self.state.stale_uid_validity.store(stale, Ordering::SeqCst);
        }

        fn sent(&self) -> Vec<OutboundEmail> {
            self.state.sent.lock().unwrap().clone()
        }
    }

    struct FakeMailboxState {
        candidate: CandidateHeader,
        override_candidates: Mutex<Option<Vec<CandidateHeader>>>,
        override_malformed: Mutex<Option<Vec<MalformedCandidateHeader>>>,
        raw: Vec<u8>,
        stale_uid_validity: AtomicBool,
        full_fetches: AtomicUsize,
        seen: AtomicUsize,
        sent: Mutex<Vec<OutboundEmail>>,
    }

    impl MailboxFactory for FakeMailboxFactory {
        fn open(&self, _config: MailboxConfig) -> Box<dyn MailboxEngine> {
            Box::new(FakeMailbox {
                state: self.state.clone(),
            })
        }
    }

    struct FakeMailbox {
        state: Arc<FakeMailboxState>,
    }

    #[async_trait]
    impl MailboxEngine for FakeMailbox {
        async fn list_candidate_headers(&mut self) -> Result<CandidateHeaderBatch> {
            let candidates = self
                .state
                .override_candidates
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| vec![self.state.candidate.clone()]);
            let malformed = self
                .state
                .override_malformed
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_default();
            Ok(CandidateHeaderBatch {
                candidates,
                malformed,
            })
        }

        async fn fetch_full_message_after_authorize(
            &mut self,
            candidate: &CandidateHeader,
        ) -> Result<FetchedMessage> {
            if self.state.stale_uid_validity.load(Ordering::SeqCst) {
                return Err(stale_uid_validity_error(candidate.uid_validity));
            }
            self.state.full_fetches.fetch_add(1, Ordering::SeqCst);
            Ok(FetchedMessage {
                raw: self.state.raw.clone(),
            })
        }

        async fn record_seen_or_processed(&mut self, candidate: &CandidateHeader) -> Result<()> {
            if self.state.stale_uid_validity.load(Ordering::SeqCst) {
                return Err(stale_uid_validity_error(candidate.uid_validity));
            }
            self.state.seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn record_malformed_seen_or_processed(
            &mut self,
            candidate: &MalformedCandidateHeader,
        ) -> Result<()> {
            if self.state.stale_uid_validity.load(Ordering::SeqCst) {
                return Err(stale_uid_validity_error(candidate.uid_validity));
            }
            self.state.seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn send_threaded_reply(&mut self, email: OutboundEmail) -> Result<Value> {
            self.state.sent.lock().unwrap().push(email);
            Ok(json!({
                "provider": "smtp",
                "message_id": "lc.test@example.com",
                "recipient": "alice@example.com"
            }))
        }
    }

    fn candidate() -> CandidateHeader {
        candidate_from_headers(
            "From: Alice <alice@example.com>\r\nTo: Assistant <assistant@example.com>\r\nSubject: Build failed\r\nMessage-ID: <m1@example.com>\r\nIn-Reply-To: <root@example.com>\r\nReferences: <root@example.com>\r\n\r\n",
        )
    }

    fn candidate_with_uid(uid: u32) -> CandidateHeader {
        let mut candidate = candidate();
        candidate.uid = uid;
        candidate.event_id = format!("email:imap:assistant-example-com:7:{uid}");
        candidate
    }

    fn candidate_from_headers(raw_headers: &str) -> CandidateHeader {
        let facts = parse_headers_for_test(raw_headers);
        let provider_message_id = facts.message_id.as_deref().unwrap_or("missing-message-id");
        let root_message_id = facts
            .references
            .first()
            .or(facts.in_reply_to.as_ref())
            .or(facts.message_id.as_ref())
            .map(String::as_str)
            .unwrap_or(provider_message_id);
        CandidateHeader {
            uid_validity: 7,
            uid: 42,
            event_id: "email:imap:assistant-example-com:7:42".to_string(),
            sender_ref: sender_ref(&facts.sender.address),
            conversation_ref: conversation_ref("assistant-example-com"),
            thread_ref: thread_ref(root_message_id),
            message_ref: message_ref(provider_message_id),
            attachment_count: 0,
            rfc822_size: Some(raw_headers.len() as u32),
            facts,
        }
    }

    fn candidate_with_rfc822_size(rfc822_size: Option<u32>) -> CandidateHeader {
        let mut candidate = candidate();
        candidate.rfc822_size = rfc822_size;
        candidate
    }

    fn malformed_candidate(uid: u32) -> MalformedCandidateHeader {
        let event_id = format!("email:imap:assistant-example-com:7:{uid}");
        MalformedCandidateHeader {
            uid_validity: 7,
            uid,
            event_id: event_id.clone(),
            sender_ref: format!("email:malformed:{uid}"),
            conversation_ref: conversation_ref("assistant-example-com"),
            thread_ref: thread_ref(&format!("malformed:{event_id}")),
            message_ref: message_ref(&format!("imap:7:{uid}:malformed")),
            subject: "(malformed headers)".to_string(),
            snippet: "Header facts could not be parsed; body was not downloaded.".to_string(),
            attachment_count: 0,
            rfc822_size: Some(128),
            reason: "malformed_headers".to_string(),
        }
    }

    fn stale_uid_validity_error(uid_validity: u32) -> anyhow::Error {
        StaleMailboxCandidate {
            expected_uid_validity: uid_validity,
            actual_uid_validity: uid_validity + 1,
        }
        .into()
    }

    fn full_message() -> Vec<u8> {
        b"From: Alice <alice@example.com>\r\nTo: Assistant <assistant@example.com>\r\nSubject: Build failed\r\nMessage-ID: <m1@example.com>\r\nIn-Reply-To: <root@example.com>\r\nReferences: <root@example.com>\r\n\r\nPlease check this.".to_vec()
    }

    fn oversized_message(size: usize) -> Vec<u8> {
        let mut message = full_message();
        message.resize(size, b'x');
        message
    }

    fn outbox_delivery_for_thread(thread_ref: String) -> ChannelOutboxDelivery {
        ChannelOutboxDelivery {
            delivery_id: "delivery-1".to_string(),
            attempt_id: "attempt-1".to_string(),
            conversation_ref: conversation_ref("assistant-example-com"),
            thread_ref: Some(thread_ref),
            reply_to_ref: None,
            session_id: Some("session-1".to_string()),
            turn_id: Some("turn-1".to_string()),
            content: crate::api::ChannelOutboxContent {
                text: "Accepted by SMTP".to_string(),
                attachments: Vec::new(),
            },
        }
    }

    async fn install_failing_receipt_insert_trigger(state_dir: &Path) {
        let db_path = state_dir.join("channel-email.sqlite3");
        let options = sqlx::sqlite::SqliteConnectOptions::new().filename(db_path);
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .expect("open test db");
        sqlx::query(
            r#"
            CREATE TRIGGER fail_outbox_receipt_insert
            BEFORE INSERT ON outbox_receipts
            BEGIN
                SELECT RAISE(ABORT, 'receipt write failed');
            END
            "#,
        )
        .execute(&pool)
        .await
        .expect("install failing receipt trigger");
        pool.close().await;
    }
}
