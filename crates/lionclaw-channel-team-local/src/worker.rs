use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Value};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::{
    api::{
        ChannelAttachmentDescriptor, ChannelInboundRequest, ChannelOutboxAttachment,
        ChannelOutboxDelivery, ChannelOutboxReportInput, DaemonInfoResponse, LionClawApi,
    },
    config::WorkerConfig,
    discovery::{ProjectDiscovery, ProjectMember},
    protocol::{
        inbound_event_id, inbound_message_ref, parse_delivery_route, peer_conversation_ref,
        provider_file_ref, sender_ref, CHANNEL_ID,
    },
};

const INBOUND_TRIGGER: &str = "dm";
const INBOUND_SESSION_BINDING: &str = "grant";
const ATTACHMENT_KIND: &str = "document";
const HEALTH_REPORT_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub struct TeamLocalWorker {
    config: WorkerConfig,
    local_api: LionClawApi,
}

#[derive(Debug)]
struct PreparedAttachment {
    descriptor: ChannelAttachmentDescriptor,
    content: Vec<u8>,
}

#[derive(Debug)]
enum DeliveryResult {
    Delivered { receipt: Value },
    RetryableFailed { code: &'static str, text: String },
    TerminalFailed { code: &'static str, text: String },
}

impl TeamLocalWorker {
    pub fn new(config: WorkerConfig) -> Result<Self> {
        let local_api = LionClawApi::new(config.base_url.clone())?;
        Ok(Self { config, local_api })
    }

    pub async fn run(self) -> Result<()> {
        let mut last_health_report = None;
        loop {
            match self.tick(&mut last_health_report).await {
                Ok(()) => {}
                Err(err) => error!(error = %err, "team-local worker tick failed"),
            }
            if self.config.once {
                return Ok(());
            }
            sleep(self.config.poll_interval).await;
        }
    }

    async fn tick(&self, last_health_report: &mut Option<Instant>) -> Result<()> {
        let discovery = ProjectDiscovery::discover(&self.config.home)?;
        let local_member = discovery.self_member()?;
        let local_info = self.local_api.daemon_info().await?;
        validate_local_daemon(local_member, &local_info)?;
        let worker_id = self.worker_id(&local_member.home_id);

        debug!(
            instance = %discovery.self_instance,
            project = %discovery.project_root.display(),
            "team-local worker is ready"
        );
        if health_report_due(*last_health_report) {
            self.report_health(&discovery, &worker_id).await;
            *last_health_report = Some(Instant::now());
        }

        let deliveries = self
            .local_api
            .pull_outbox(
                &self.config.channel_id,
                &worker_id,
                self.config.pull_limit,
                self.config.lease_ms,
            )
            .await?;
        if deliveries.is_empty() {
            debug!("team-local outbox is empty");
            return Ok(());
        }

        info!(
            deliveries = deliveries.len(),
            instance = %discovery.self_instance,
            project = %discovery.project_root.display(),
            "pulled team-local deliveries"
        );

        for delivery in deliveries {
            let result = self
                .deliver(&local_info, &discovery, &delivery, &worker_id)
                .await;
            self.report_result(&delivery, &worker_id, result).await?;
        }

        Ok(())
    }

    async fn report_health(&self, discovery: &ProjectDiscovery, worker_id: &str) {
        if let Err(err) = self
            .local_api
            .report_health(
                &self.config.channel_id,
                worker_id,
                "ok",
                "team_local.ready",
                "team-local worker is running",
                json!({
                    "project_root": discovery.project_root.display().to_string(),
                    "instance": discovery.self_instance.as_str(),
                }),
            )
            .await
        {
            warn!(error = %err, "failed to report team-local health");
        }
    }

    async fn deliver(
        &self,
        local_info: &DaemonInfoResponse,
        discovery: &ProjectDiscovery,
        delivery: &ChannelOutboxDelivery,
        worker_id: &str,
    ) -> DeliveryResult {
        match self
            .deliver_inner(local_info, discovery, delivery, worker_id)
            .await
        {
            Ok(receipt) => DeliveryResult::Delivered { receipt },
            Err(err) => err,
        }
    }

    async fn deliver_inner(
        &self,
        local_info: &DaemonInfoResponse,
        discovery: &ProjectDiscovery,
        delivery: &ChannelOutboxDelivery,
        worker_id: &str,
    ) -> Result<Value, DeliveryResult> {
        if let Some(result) = unsupported_reply_ref(delivery) {
            return Err(result);
        }

        let route = parse_delivery_route(&delivery.conversation_ref)
            .map_err(|err| terminal("invalid_route", err))?;
        let target = discovery.member_for_route(&route).ok_or_else(|| {
            terminal(
                "unknown_recipient",
                anyhow!("no project instance matches route"),
            )
        })?;
        if target.home_id == local_info.home_id {
            return Err(terminal(
                "self_delivery",
                anyhow!("team-local delivery targets the sender instance"),
            ));
        }

        let target_api =
            LionClawApi::new(target.base_url.clone()).map_err(|err| retryable("client", err))?;
        let target_info = target_api
            .daemon_info()
            .await
            .map_err(|err| retryable("target_unreachable", err))?;
        verify_target_daemon(target, &target_info)
            .map_err(|err| terminal("target_verification_failed", err))?;

        let inbound_sender_ref = sender_ref(&local_info.home_id);
        let inbound_conversation_ref = peer_conversation_ref(&local_info.home_id);
        let authorization = target_api
            .authorize(
                CHANNEL_ID,
                &inbound_sender_ref,
                &inbound_conversation_ref,
                delivery.thread_ref.as_deref(),
            )
            .await
            .map_err(|err| retryable("authorize_failed", err))?;
        if !authorization.authorized {
            let code = match authorization.reason_code.as_str() {
                "blocked_grant" => "blocked",
                "approval_required" | "actor_approval_required" => "unauthorized",
                "trigger_insufficient" => "misconfigured_authorization",
                _ => "unauthorized",
            };
            return Err(DeliveryResult::TerminalFailed {
                code,
                text: authorization.reason_code,
            });
        }

        let event_id = inbound_event_id(&local_info.home_id, &delivery.delivery_id);
        let prepared_attachments =
            prepare_attachments(&delivery.delivery_id, &delivery.content.attachments)
                .await
                .map_err(|err| terminal("attachment_unreadable", err))?;
        let descriptors = prepared_attachments
            .iter()
            .map(|attachment| attachment.descriptor.clone())
            .collect::<Vec<_>>();
        let inbound = ChannelInboundRequest {
            channel_id: CHANNEL_ID.to_string(),
            event_id: event_id.clone(),
            sender_ref: inbound_sender_ref,
            conversation_ref: inbound_conversation_ref,
            thread_ref: delivery.thread_ref.clone(),
            message_ref: Some(inbound_message_ref(&delivery.delivery_id)),
            text: non_empty_text(&delivery.content.text),
            attachments: descriptors,
            reply_to_ref: delivery.reply_to_ref.clone(),
            trigger: INBOUND_TRIGGER.to_string(),
            session_binding: INBOUND_SESSION_BINDING.to_string(),
            received_at: None,
            provider_metadata: json!({
                "provider": CHANNEL_ID,
                "sender_home_id": local_info.home_id,
                "sender_bind_addr": local_info.bind_addr,
                "sender_delivery_id": delivery.delivery_id,
                "sender_attempt_id": delivery.attempt_id,
                "sender_session_id": delivery.session_id,
                "sender_turn_id": delivery.turn_id,
                "sender_thread_ref": delivery.thread_ref,
                "sender_reply_to_ref": delivery.reply_to_ref,
                "target_instance": target.name,
                "target_home_id": target.home_id,
            }),
        };
        let inbound_response = target_api
            .inbound(&inbound)
            .await
            .map_err(|err| retryable("inbound_failed", err))?;
        match inbound_response.outcome.as_str() {
            "queued" | "duplicate" => {}
            "waiting_for_attachments" => {
                stage_and_finalize_attachments(
                    &target_api,
                    &event_id,
                    worker_id,
                    &prepared_attachments,
                )
                .await?;
            }
            "blocked" => {
                return Err(DeliveryResult::TerminalFailed {
                    code: "blocked",
                    text: inbound_response
                        .reason_code
                        .unwrap_or_else(|| "blocked".to_string()),
                });
            }
            "pending_approval" | "trigger_ignored" => {
                return Err(DeliveryResult::TerminalFailed {
                    code: "not_accepted",
                    text: inbound_response
                        .reason_code
                        .unwrap_or(inbound_response.outcome),
                });
            }
            other => {
                return Err(retryable(
                    "unexpected_inbound_outcome",
                    anyhow!("target returned inbound outcome '{other}'"),
                ));
            }
        }

        Ok(json!({
            "provider": CHANNEL_ID,
            "target_instance": target.name,
            "target_home_id": target.home_id,
            "target_event_id": event_id,
            "target_inbound_outcome": inbound_response.outcome,
        }))
    }

    async fn report_result(
        &self,
        delivery: &ChannelOutboxDelivery,
        worker_id: &str,
        result: DeliveryResult,
    ) -> Result<()> {
        match result {
            DeliveryResult::Delivered { receipt } => {
                self.local_api
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
                warn!(delivery_id = %delivery.delivery_id, code, error = %text, "team-local delivery retryable failure");
                self.local_api
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
                warn!(delivery_id = %delivery.delivery_id, code, error = %text, "team-local delivery terminal failure");
                self.local_api
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

    fn worker_id(&self, local_home_id: &str) -> String {
        if self.config.worker_id == format!("{CHANNEL_ID}:worker") {
            format!("{CHANNEL_ID}:worker:{local_home_id}")
        } else {
            self.config.worker_id.clone()
        }
    }
}

async fn stage_and_finalize_attachments(
    target_api: &LionClawApi,
    event_id: &str,
    worker_id: &str,
    attachments: &[PreparedAttachment],
) -> Result<(), DeliveryResult> {
    let missing = Vec::new();
    for attachment in attachments {
        let staged = target_api
            .stage_attachment(
                CHANNEL_ID,
                event_id,
                &attachment.descriptor,
                attachment.content.clone(),
            )
            .await
            .map_err(|err| retryable("attachment_stage_failed", err))?;
        if staged.status == "rejected" {
            debug!(
                attachment_id = %attachment.descriptor.attachment_id,
                reason = ?staged.reason_code,
                "target rejected team-local attachment during stage"
            );
        }
    }
    let finalized = target_api
        .finalize_attachments(CHANNEL_ID, event_id, worker_id, &missing)
        .await
        .map_err(|err| retryable("attachment_finalize_failed", err))?;
    match finalized.outcome.as_str() {
        "queued" | "already_finalized" => Ok(()),
        "not_ready" => Err(retryable(
            "attachment_finalize_not_ready",
            anyhow!("target attachments were not ready after staging"),
        )),
        other => Err(retryable(
            "unexpected_attachment_finalize_outcome",
            anyhow!("target returned attachment finalize outcome '{other}'"),
        )),
    }
}

async fn prepare_attachments(
    delivery_id: &str,
    attachments: &[ChannelOutboxAttachment],
) -> Result<Vec<PreparedAttachment>> {
    let mut prepared = Vec::with_capacity(attachments.len());
    for attachment in attachments {
        let path = PathBuf::from(&attachment.path);
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("failed to stat attachment {}", path.display()))?;
        if metadata.file_type().is_symlink() {
            bail!("attachment {} must not be a symlink", path.display());
        }
        if !metadata.is_file() {
            bail!("attachment {} is not a regular file", path.display());
        }
        let size_bytes = i64::try_from(metadata.len())
            .with_context(|| format!("attachment {} is too large", path.display()))?;
        let content = tokio::fs::read(&path)
            .await
            .with_context(|| format!("failed to read attachment {}", path.display()))?;
        prepared.push(PreparedAttachment {
            descriptor: ChannelAttachmentDescriptor {
                attachment_id: attachment.attachment_id.clone(),
                kind: ATTACHMENT_KIND.to_string(),
                mime_type: attachment.mime_type.clone(),
                filename: attachment.filename.clone().or_else(|| {
                    Path::new(&attachment.path)
                        .file_name()
                        .and_then(|value| value.to_str())
                        .map(str::to_string)
                }),
                size_bytes: Some(size_bytes),
                provider_file_ref: provider_file_ref(
                    delivery_id,
                    attachment.attachment_id.as_str(),
                ),
                caption: None,
            },
            content,
        });
    }
    Ok(prepared)
}

fn validate_local_daemon(local: &ProjectMember, info: &DaemonInfoResponse) -> Result<()> {
    verify_member_daemon("local", local, info)
}

fn verify_target_daemon(target: &ProjectMember, target_info: &DaemonInfoResponse) -> Result<()> {
    verify_member_daemon("target", target, target_info)
}

fn verify_member_daemon(
    label: &str,
    member: &ProjectMember,
    info: &DaemonInfoResponse,
) -> Result<()> {
    if info.daemon != "lionclawd" {
        bail!("{label} endpoint is not lionclawd: {}", info.daemon);
    }
    if info.status != "ok" {
        bail!("{label} daemon is not ok: {}", info.status);
    }
    if info.home_id != member.home_id {
        bail!(
            "{label} home id mismatch: expected {}, got {}",
            member.home_id,
            info.home_id
        );
    }
    let reported_home = PathBuf::from(&info.home_root);
    match fs::canonicalize(&reported_home) {
        Ok(reported_home) if reported_home == member.home => {}
        Ok(reported_home) => bail!(
            "{label} home root mismatch: expected {}, got {}",
            member.home.display(),
            reported_home.display()
        ),
        Err(err) => bail!("failed to resolve reported {label} home root: {err}"),
    }
    Ok(())
}

fn non_empty_text(text: &str) -> Option<String> {
    (!text.trim().is_empty()).then(|| text.to_string())
}

fn unsupported_reply_ref(delivery: &ChannelOutboxDelivery) -> Option<DeliveryResult> {
    delivery
        .reply_to_ref
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    Some(terminal(
        "unsupported_reply_ref",
        anyhow!("team-local does not support reply refs; send an addressed message to the instance instead"),
    ))
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

fn health_report_due(last_report: Option<Instant>) -> bool {
    match last_report {
        Some(instant) => instant.elapsed() >= HEALTH_REPORT_INTERVAL,
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::SocketAddr,
        path::Path,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    };

    use axum::{
        body::Bytes,
        extract::State,
        routing::{get, post},
        Json, Router,
    };
    use serde_json::{json, Value};
    use tempfile::tempdir;
    use tokio::net::TcpListener;

    use super::{health_report_due, non_empty_text, verify_target_daemon, TeamLocalWorker};
    use crate::{
        api::DaemonInfoResponse,
        config::WorkerConfig,
        discovery::ProjectMember,
        protocol::{peer_conversation_ref, sender_ref, CHANNEL_ID},
    };

    #[test]
    fn non_empty_text_preserves_payload_whitespace() {
        assert_eq!(non_empty_text("  hello  "), Some("  hello  ".to_string()));
        assert_eq!(non_empty_text(" \n\t "), None);
    }

    #[test]
    fn health_report_due_on_startup_and_after_interval() {
        assert!(health_report_due(None));
        assert!(!health_report_due(Some(Instant::now())));
        assert!(health_report_due(Some(
            Instant::now() - Duration::from_secs(61)
        )));
    }

    #[test]
    fn target_daemon_must_report_ok_status() {
        let temp_dir = tempdir().expect("temp dir");
        let target_home = temp_dir.path().join("reviewer");
        std::fs::create_dir_all(&target_home).expect("target home");
        let target_home = std::fs::canonicalize(&target_home).expect("canonical target");
        let target = ProjectMember {
            name: "reviewer".to_string(),
            home: target_home.clone(),
            home_id: "home-reviewer".to_string(),
            base_url: "http://127.0.0.1:9002".to_string(),
        };
        let target_info = DaemonInfoResponse {
            daemon: "lionclawd".to_string(),
            status: "starting".to_string(),
            home_id: target.home_id.clone(),
            home_root: target_home.display().to_string(),
            bind_addr: "127.0.0.1:9002".to_string(),
        };

        let err = verify_target_daemon(&target, &target_info)
            .expect_err("unhealthy target should fail verification");

        assert!(err.to_string().contains("target daemon is not ok"));
    }

    #[tokio::test]
    async fn delivery_posts_authorized_inbound_and_reports_delivered() {
        let fixture = TeamLocalFixture::new().await;
        let target_state = Arc::new(TargetState {
            info: fixture.target_info(),
            inbound_outcomes: vec!["queued"],
            ..TargetState::default()
        });
        let target_url = spawn_target_server(target_state.clone()).await;
        fixture.write_target_bind(&target_url);
        let local_state = Arc::new(LocalState::with_delivery(
            fixture.local_info(),
            outbox_delivery(&fixture.target_home_id, None),
        ));
        let local_url = spawn_local_server(local_state.clone()).await;

        let mut last_health_report = None;
        TeamLocalWorker::new(fixture.worker_config(local_url))
            .expect("worker")
            .tick(&mut last_health_report)
            .await
            .expect("tick");

        let authorize = only_request(&target_state.authorize_requests).await;
        assert_eq!(authorize["channel_id"], CHANNEL_ID);
        assert_eq!(authorize["sender_ref"], sender_ref(&fixture.local_home_id));
        assert_eq!(
            authorize["conversation_ref"],
            peer_conversation_ref(&fixture.local_home_id)
        );
        assert!(authorize.get("thread_ref").is_none());
        let inbound = only_request(&target_state.inbound_requests).await;
        assert_eq!(inbound["channel_id"], CHANNEL_ID);
        assert_eq!(inbound["sender_ref"], sender_ref(&fixture.local_home_id));
        assert_eq!(
            inbound["conversation_ref"],
            peer_conversation_ref(&fixture.local_home_id)
        );
        assert_eq!(inbound["event_id"], "team-local:home-main:delivery-1");
        assert_eq!(inbound["text"], "  hello reviewer  ");
        assert_eq!(inbound["trigger"], "dm");
        assert_eq!(inbound["session_binding"], "grant");
        let report = only_request(&local_state.reports).await;
        assert_eq!(report["outcome"], "delivered");
        assert_eq!(
            report["provider_receipt"]["target_inbound_outcome"],
            "queued"
        );
    }

    #[tokio::test]
    async fn delivery_preserves_thread_ref() {
        let fixture = TeamLocalFixture::new().await;
        let target_state = Arc::new(TargetState {
            info: fixture.target_info(),
            inbound_outcomes: vec!["queued"],
            ..TargetState::default()
        });
        let target_url = spawn_target_server(target_state.clone()).await;
        fixture.write_target_bind(&target_url);
        let local_state = Arc::new(LocalState::with_delivery(
            fixture.local_info(),
            outbox_delivery_with_refs(&fixture.target_home_id, None, Some("thread-alpha"), None),
        ));
        let local_url = spawn_local_server(local_state.clone()).await;

        let mut last_health_report = None;
        TeamLocalWorker::new(fixture.worker_config(local_url))
            .expect("worker")
            .tick(&mut last_health_report)
            .await
            .expect("tick");

        let authorize = only_request(&target_state.authorize_requests).await;
        assert_eq!(authorize["thread_ref"], "thread-alpha");
        let inbound = only_request(&target_state.inbound_requests).await;
        assert_eq!(inbound["thread_ref"], "thread-alpha");
        assert_eq!(
            inbound["provider_metadata"]["sender_thread_ref"],
            "thread-alpha"
        );
        assert!(inbound["reply_to_ref"].is_null());
        assert!(inbound["provider_metadata"]["sender_reply_to_ref"].is_null());
    }

    #[tokio::test]
    async fn reply_ref_delivery_is_terminal_failed_without_target_inbound() {
        let fixture = TeamLocalFixture::new().await;
        let target_state = Arc::new(TargetState {
            info: fixture.target_info(),
            inbound_outcomes: vec!["queued"],
            ..TargetState::default()
        });
        let _target_url = spawn_target_server(target_state.clone()).await;
        let local_state = Arc::new(LocalState::with_delivery(
            fixture.local_info(),
            outbox_delivery_with_refs(&fixture.target_home_id, None, None, Some("source-message")),
        ));
        let local_url = spawn_local_server(local_state.clone()).await;

        let mut last_health_report = None;
        TeamLocalWorker::new(fixture.worker_config(local_url))
            .expect("worker")
            .tick(&mut last_health_report)
            .await
            .expect("tick");

        assert!(target_state.authorize_requests.lock().await.is_empty());
        assert!(target_state.inbound_requests.lock().await.is_empty());
        let report = only_request(&local_state.reports).await;
        assert_eq!(report["outcome"], "terminal_failed");
        assert!(report["provider_receipt"].is_null());
        assert_eq!(report["error_code"], "unsupported_reply_ref");
        assert_eq!(
            report["error_text"],
            "team-local does not support reply refs; send an addressed message to the instance instead"
        );
    }

    #[tokio::test]
    async fn idle_worker_reports_health_after_local_verification() {
        let fixture = TeamLocalFixture::new().await;
        let local_state = Arc::new(LocalState::empty(fixture.local_info()));
        let local_url = spawn_local_server(local_state.clone()).await;

        let mut last_health_report = None;
        TeamLocalWorker::new(fixture.worker_config(local_url))
            .expect("worker")
            .tick(&mut last_health_report)
            .await
            .expect("tick");
        assert!(last_health_report.is_some());

        assert_eq!(local_state.outbox_pulls.load(Ordering::SeqCst), 1);
        let health = only_request(&local_state.health_reports).await;
        assert_eq!(health["channel_id"], CHANNEL_ID);
        assert_eq!(health["reporter_id"], "team-local:worker:home-main");
        assert_eq!(health["status"], "ok");
        assert_eq!(health["checks"][0]["details"]["instance"], "main");
        assert!(local_state.reports.lock().await.is_empty());
    }

    #[tokio::test]
    async fn idle_worker_skips_health_between_report_intervals() {
        let fixture = TeamLocalFixture::new().await;
        let local_state = Arc::new(LocalState::empty(fixture.local_info()));
        let local_url = spawn_local_server(local_state.clone()).await;

        let mut last_health_report = Some(Instant::now());
        TeamLocalWorker::new(fixture.worker_config(local_url))
            .expect("worker")
            .tick(&mut last_health_report)
            .await
            .expect("tick");

        assert_eq!(local_state.outbox_pulls.load(Ordering::SeqCst), 1);
        assert!(local_state.health_reports.lock().await.is_empty());
        assert!(local_state.reports.lock().await.is_empty());
    }

    #[tokio::test]
    async fn local_daemon_identity_mismatch_fails_before_outbox_pull() {
        let fixture = TeamLocalFixture::new().await;
        let mut local_info = fixture.local_info();
        local_info.home_id = "wrong-home".to_string();
        let local_state = Arc::new(LocalState::with_delivery(
            local_info,
            outbox_delivery(&fixture.target_home_id, None),
        ));
        let local_url = spawn_local_server(local_state.clone()).await;

        let mut last_health_report = None;
        let err = TeamLocalWorker::new(fixture.worker_config(local_url))
            .expect("worker")
            .tick(&mut last_health_report)
            .await
            .expect_err("wrong local daemon identity should fail");

        assert!(err.to_string().contains("local home id mismatch"));
        assert!(last_health_report.is_none());
        assert_eq!(local_state.outbox_pulls.load(Ordering::SeqCst), 0);
        assert!(local_state.health_reports.lock().await.is_empty());
        assert!(local_state.reports.lock().await.is_empty());
    }

    #[tokio::test]
    async fn duplicate_recipient_inbound_is_reported_delivered() {
        let fixture = TeamLocalFixture::new().await;
        let target_state = Arc::new(TargetState {
            info: fixture.target_info(),
            inbound_outcomes: vec!["duplicate"],
            ..TargetState::default()
        });
        let target_url = spawn_target_server(target_state).await;
        fixture.write_target_bind(&target_url);
        let local_state = Arc::new(LocalState::with_delivery(
            fixture.local_info(),
            outbox_delivery(&fixture.target_home_id, None),
        ));
        let local_url = spawn_local_server(local_state.clone()).await;

        let mut last_health_report = None;
        TeamLocalWorker::new(fixture.worker_config(local_url))
            .expect("worker")
            .tick(&mut last_health_report)
            .await
            .expect("tick");

        let report = only_request(&local_state.reports).await;
        assert_eq!(report["outcome"], "delivered");
        assert_eq!(
            report["provider_receipt"]["target_inbound_outcome"],
            "duplicate"
        );
    }

    #[tokio::test]
    async fn attachment_delivery_uses_stage_and_finalize_apis() {
        let fixture = TeamLocalFixture::new().await;
        let attachment_path = fixture.project_root.join("note.txt");
        std::fs::write(&attachment_path, b"attachment bytes").expect("attachment");
        let target_state = Arc::new(TargetState {
            info: fixture.target_info(),
            inbound_outcomes: vec!["waiting_for_attachments"],
            ..TargetState::default()
        });
        let target_url = spawn_target_server(target_state.clone()).await;
        fixture.write_target_bind(&target_url);
        let local_state = Arc::new(LocalState::with_delivery(
            fixture.local_info(),
            outbox_delivery(&fixture.target_home_id, Some(&attachment_path)),
        ));
        let local_url = spawn_local_server(local_state.clone()).await;

        let mut last_health_report = None;
        TeamLocalWorker::new(fixture.worker_config(local_url))
            .expect("worker")
            .tick(&mut last_health_report)
            .await
            .expect("tick");

        let stage = only_request(&target_state.stage_requests).await;
        assert!(stage["body"]
            .as_str()
            .expect("stage body")
            .contains("attachment bytes"));
        let finalize = only_request(&target_state.finalize_requests).await;
        assert_eq!(finalize["channel_id"], CHANNEL_ID);
        assert_eq!(finalize["event_id"], "team-local:home-main:delivery-1");
        assert_eq!(finalize["missing"], json!([]));
        let report = only_request(&local_state.reports).await;
        assert_eq!(report["outcome"], "delivered");
        assert_eq!(
            report["provider_receipt"]["target_inbound_outcome"],
            "waiting_for_attachments"
        );
    }

    struct TeamLocalFixture {
        project_root: std::path::PathBuf,
        local_home: std::path::PathBuf,
        target_home: std::path::PathBuf,
        local_home_id: String,
        target_home_id: String,
        _temp_dir: tempfile::TempDir,
    }

    impl TeamLocalFixture {
        async fn new() -> Self {
            let temp_dir = tempdir().expect("temp dir");
            let project_root = temp_dir.path().join("project");
            let local_home = project_root.join(".lionclaw/instances/main");
            let target_home = project_root.join(".lionclaw/instances/reviewer");
            std::fs::create_dir_all(local_home.join("config")).expect("local config");
            std::fs::create_dir_all(target_home.join("config")).expect("target config");
            std::fs::write(project_root.join(".lionclaw/project.toml"), "version = 1\n")
                .expect("project");
            std::fs::write(local_home.join("config/home-id"), "home-main\n").expect("local id");
            std::fs::write(target_home.join("config/home-id"), "home-reviewer\n")
                .expect("target id");
            Self {
                project_root,
                local_home,
                target_home,
                local_home_id: "home-main".to_string(),
                target_home_id: "home-reviewer".to_string(),
                _temp_dir: temp_dir,
            }
        }

        fn write_target_bind(&self, base_url: &str) {
            std::fs::write(
                self.target_home.join("config/lionclaw.toml"),
                format!("[daemon]\nbind = {base_url:?}\n"),
            )
            .expect("target config");
        }

        fn local_info(&self) -> DaemonInfoResponse {
            DaemonInfoResponse {
                daemon: "lionclawd".to_string(),
                status: "ok".to_string(),
                home_id: self.local_home_id.clone(),
                home_root: self.local_home.display().to_string(),
                bind_addr: "127.0.0.1:9001".to_string(),
            }
        }

        fn target_info(&self) -> DaemonInfoResponse {
            DaemonInfoResponse {
                daemon: "lionclawd".to_string(),
                status: "ok".to_string(),
                home_id: self.target_home_id.clone(),
                home_root: self.target_home.display().to_string(),
                bind_addr: "127.0.0.1:9002".to_string(),
            }
        }

        fn worker_config(&self, base_url: String) -> WorkerConfig {
            WorkerConfig {
                home: self.local_home.clone(),
                base_url,
                channel_id: CHANNEL_ID.to_string(),
                worker_id: format!("{CHANNEL_ID}:worker"),
                once: true,
                poll_interval: std::time::Duration::from_secs(1),
                pull_limit: 10,
                lease_ms: 120_000,
            }
        }
    }

    #[derive(Debug)]
    struct LocalState {
        info: DaemonInfoResponse,
        deliveries: Vec<Value>,
        outbox_pulls: AtomicUsize,
        reports: tokio::sync::Mutex<Vec<Value>>,
        health_reports: tokio::sync::Mutex<Vec<Value>>,
    }

    impl LocalState {
        fn with_delivery(info: DaemonInfoResponse, delivery: Value) -> Self {
            Self::with_deliveries(info, vec![delivery])
        }

        fn empty(info: DaemonInfoResponse) -> Self {
            Self::with_deliveries(info, Vec::new())
        }

        fn with_deliveries(info: DaemonInfoResponse, deliveries: Vec<Value>) -> Self {
            Self {
                info,
                deliveries,
                outbox_pulls: AtomicUsize::new(0),
                reports: tokio::sync::Mutex::default(),
                health_reports: tokio::sync::Mutex::default(),
            }
        }
    }

    #[derive(Debug, Default)]
    struct TargetState {
        info: DaemonInfoResponse,
        inbound_outcomes: Vec<&'static str>,
        inbound_count: AtomicUsize,
        authorize_requests: tokio::sync::Mutex<Vec<Value>>,
        inbound_requests: tokio::sync::Mutex<Vec<Value>>,
        stage_requests: tokio::sync::Mutex<Vec<Value>>,
        finalize_requests: tokio::sync::Mutex<Vec<Value>>,
    }

    impl Default for DaemonInfoResponse {
        fn default() -> Self {
            Self {
                daemon: "lionclawd".to_string(),
                status: "ok".to_string(),
                home_id: String::new(),
                home_root: String::new(),
                bind_addr: String::new(),
            }
        }
    }

    async fn spawn_local_server(state: Arc<LocalState>) -> String {
        let router = Router::new()
            .route("/v0/daemon/info", get(local_info))
            .route("/v0/channels/outbox/pull", post(local_outbox_pull))
            .route("/v0/channels/outbox/report", post(local_outbox_report))
            .route("/v0/channels/health/report", post(local_health_report))
            .with_state(state);
        spawn_server(router).await
    }

    async fn spawn_target_server(state: Arc<TargetState>) -> String {
        let router = Router::new()
            .route("/v0/daemon/info", get(target_info))
            .route("/v0/channels/authorize", post(target_authorize))
            .route("/v0/channels/inbound", post(target_inbound))
            .route("/v0/channels/attachments/stage", post(target_stage))
            .route("/v0/channels/attachments/finalize", post(target_finalize))
            .with_state(state);
        spawn_server(router).await
    }

    async fn spawn_server(router: Router) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr: SocketAddr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, router).await.expect("serve");
        });
        format!("http://{addr}")
    }

    async fn local_info(State(state): State<Arc<LocalState>>) -> Json<Value> {
        Json(daemon_info_json(&state.info))
    }

    async fn local_outbox_pull(State(state): State<Arc<LocalState>>) -> Json<Value> {
        state.outbox_pulls.fetch_add(1, Ordering::SeqCst);
        Json(json!({ "deliveries": state.deliveries.clone() }))
    }

    async fn local_outbox_report(
        State(state): State<Arc<LocalState>>,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        state.reports.lock().await.push(body);
        Json(json!({ "accepted": true }))
    }

    async fn local_health_report(
        State(state): State<Arc<LocalState>>,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        state.health_reports.lock().await.push(body);
        Json(json!({ "accepted": true }))
    }

    async fn target_info(State(state): State<Arc<TargetState>>) -> Json<Value> {
        Json(daemon_info_json(&state.info))
    }

    async fn target_authorize(
        State(state): State<Arc<TargetState>>,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        state.authorize_requests.lock().await.push(body);
        Json(json!({
            "authorized": true,
            "reason_code": "approved",
            "grant_id": null,
            "session_key": "channel:team-local:direct:home-main"
        }))
    }

    async fn target_inbound(
        State(state): State<Arc<TargetState>>,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        state.inbound_requests.lock().await.push(body);
        let index = state.inbound_count.fetch_add(1, Ordering::SeqCst);
        let outcome = state
            .inbound_outcomes
            .get(index)
            .copied()
            .or_else(|| state.inbound_outcomes.last().copied())
            .unwrap_or("queued");
        Json(json!({ "outcome": outcome }))
    }

    async fn target_stage(State(state): State<Arc<TargetState>>, body: Bytes) -> Json<Value> {
        state.stage_requests.lock().await.push(json!({
            "body": String::from_utf8_lossy(&body).to_string()
        }));
        Json(json!({
            "channel_id": CHANNEL_ID,
            "event_id": "team-local:home-main:delivery-1",
            "attachment_id": "att-1",
            "status": "staged",
            "size_bytes": body.len(),
            "sha256": "test-sha256",
            "runtime_path": "/lionclaw/channel-attachments/att-1/note.txt",
            "reason_code": null
        }))
    }

    async fn target_finalize(
        State(state): State<Arc<TargetState>>,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        state.finalize_requests.lock().await.push(body);
        Json(json!({
            "channel_id": CHANNEL_ID,
            "event_id": "team-local:home-main:delivery-1",
            "outcome": "queued",
            "session_id": null,
            "turn_id": null
        }))
    }

    fn daemon_info_json(info: &DaemonInfoResponse) -> Value {
        json!({
            "daemon": info.daemon,
            "status": info.status,
            "home_id": info.home_id,
            "home_root": info.home_root,
            "bind_addr": info.bind_addr
        })
    }

    fn outbox_delivery(target_home_id: &str, attachment_path: Option<&Path>) -> Value {
        outbox_delivery_with_refs(target_home_id, attachment_path, None, None)
    }

    fn outbox_delivery_with_refs(
        target_home_id: &str,
        attachment_path: Option<&Path>,
        thread_ref: Option<&str>,
        reply_to_ref: Option<&str>,
    ) -> Value {
        let attachments = attachment_path.map_or_else(Vec::new, |path| {
            vec![json!({
                "attachment_id": "att-1",
                "path": path,
                "filename": "note.txt",
                "mime_type": "text/plain"
            })]
        });
        json!({
            "delivery_id": "delivery-1",
            "attempt_id": "attempt-1",
            "conversation_ref": peer_conversation_ref(target_home_id),
            "thread_ref": thread_ref,
            "reply_to_ref": reply_to_ref,
            "session_id": null,
            "turn_id": null,
            "content": {
                "text": "  hello reviewer  ",
                "attachments": attachments
            }
        })
    }

    async fn only_request(requests: &tokio::sync::Mutex<Vec<Value>>) -> Value {
        let requests = requests.lock().await;
        assert_eq!(requests.len(), 1);
        requests[0].clone()
    }
}
