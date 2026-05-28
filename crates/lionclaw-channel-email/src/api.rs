use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use reqwest::{
    multipart::{Form, Part},
    StatusCode,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct LionClawApi {
    client: reqwest::Client,
    base_url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DaemonInfoResponse {
    pub daemon: String,
    pub status: String,
    pub home_id: String,
    pub home_root: String,
    pub bind_addr: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChannelActorAuthorizeResponse {
    pub authorized: bool,
    pub reason_code: String,
    #[serde(default)]
    pub grant_id: Option<String>,
    #[serde(default)]
    pub grant_label: Option<String>,
}

impl ChannelActorAuthorizeResponse {
    pub fn one_shot_release_held_id(&self) -> Option<&str> {
        self.grant_label
            .as_deref()
            .and_then(|label| label.strip_prefix("email-release:"))
            .filter(|held_id| !held_id.trim().is_empty())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChannelInboundResponse {
    pub outcome: String,
    #[serde(default)]
    pub reason_code: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChannelAttachmentStageResponse {
    pub status: String,
    #[serde(default)]
    pub reason_code: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChannelAttachmentFinalizeResponse {
    pub outcome: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChannelOutboxPullResponse {
    pub deliveries: Vec<ChannelOutboxDelivery>,
}

#[derive(Debug)]
pub struct ChannelOutboxReportInput<'a> {
    pub delivery: &'a ChannelOutboxDelivery,
    pub channel_id: &'a str,
    pub worker_id: &'a str,
    pub outcome: &'a str,
    pub provider_receipt: Option<Value>,
    pub error_code: Option<&'a str>,
    pub error_text: Option<&'a str>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChannelOutboxDelivery {
    pub delivery_id: String,
    pub attempt_id: String,
    pub conversation_ref: String,
    #[serde(default)]
    pub thread_ref: Option<String>,
    #[serde(default)]
    pub reply_to_ref: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub turn_id: Option<String>,
    pub content: ChannelOutboxContent,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChannelOutboxContent {
    pub text: String,
    #[serde(default)]
    pub attachments: Vec<ChannelOutboxAttachment>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChannelOutboxAttachment {
    pub attachment_id: String,
    pub path: String,
    #[serde(default)]
    pub filename: Option<String>,
    #[serde(default)]
    pub mime_type: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChannelAttachmentDescriptor {
    pub attachment_id: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<i64>,
    pub provider_file_ref: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caption: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChannelInboundRequest {
    pub channel_id: String,
    pub event_id: String,
    pub sender_ref: String,
    pub conversation_ref: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    pub attachments: Vec<ChannelAttachmentDescriptor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_to_ref: Option<String>,
    pub trigger: String,
    pub session_binding: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_at: Option<DateTime<Utc>>,
    pub provider_metadata: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChannelAttachmentMissingReport {
    pub attachment_id: String,
    pub reason_code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason_text: Option<String>,
}

impl LionClawApi {
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .context("failed to build HTTP client")?;
        Ok(Self {
            client,
            base_url: base_url.into().trim_end_matches('/').to_string(),
        })
    }

    pub async fn daemon_info(&self) -> Result<DaemonInfoResponse> {
        self.get_json("/v0/daemon/info").await
    }

    pub async fn authorize(
        &self,
        channel_id: &str,
        sender_ref: &str,
        conversation_ref: &str,
        thread_ref: Option<&str>,
        trigger: &str,
        session_binding: &str,
    ) -> Result<ChannelActorAuthorizeResponse> {
        self.post_json(
            "/v0/channels/authorize",
            &serde_json::json!({
                "channel_id": channel_id,
                "sender_ref": sender_ref,
                "conversation_ref": conversation_ref,
                "thread_ref": thread_ref,
                "trigger": trigger,
                "session_binding": session_binding,
            }),
        )
        .await
    }

    pub async fn inbound(&self, request: &ChannelInboundRequest) -> Result<ChannelInboundResponse> {
        self.post_json("/v0/channels/inbound", request).await
    }

    pub async fn stage_attachment(
        &self,
        channel_id: &str,
        event_id: &str,
        descriptor: &ChannelAttachmentDescriptor,
        content: Vec<u8>,
    ) -> Result<ChannelAttachmentStageResponse> {
        let mut form = Form::new()
            .text("channel_id", channel_id.to_string())
            .text("event_id", event_id.to_string())
            .text("attachment_id", descriptor.attachment_id.clone())
            .text("kind", descriptor.kind.clone());
        if let Some(filename) = &descriptor.filename {
            form = form.text("filename", filename.clone());
        }
        if let Some(mime_type) = &descriptor.mime_type {
            form = form.text("mime_type", mime_type.clone());
        }
        if let Some(caption) = &descriptor.caption {
            form = form.text("caption", caption.clone());
        }

        let filename = descriptor
            .filename
            .clone()
            .unwrap_or_else(|| descriptor.attachment_id.clone());
        let mut part = Part::bytes(content).file_name(filename);
        if let Some(mime_type) = &descriptor.mime_type {
            part = part
                .mime_str(mime_type)
                .with_context(|| format!("invalid attachment mime type '{mime_type}'"))?;
        }
        let response = self
            .client
            .post(self.url("/v0/channels/attachments/stage"))
            .multipart(form.part("file", part))
            .send()
            .await
            .context("failed to stage attachment")?;
        response_json(response).await
    }

    pub async fn finalize_attachments(
        &self,
        channel_id: &str,
        event_id: &str,
        worker_id: &str,
        missing: &[ChannelAttachmentMissingReport],
    ) -> Result<ChannelAttachmentFinalizeResponse> {
        self.post_json(
            "/v0/channels/attachments/finalize",
            &serde_json::json!({
                "channel_id": channel_id,
                "event_id": event_id,
                "worker_id": worker_id,
                "missing": missing,
            }),
        )
        .await
    }

    pub async fn pull_outbox(
        &self,
        channel_id: &str,
        worker_id: &str,
        limit: usize,
        lease_ms: u64,
    ) -> Result<Vec<ChannelOutboxDelivery>> {
        let response: ChannelOutboxPullResponse = self
            .post_json(
                "/v0/channels/outbox/pull",
                &serde_json::json!({
                    "channel_id": channel_id,
                    "worker_id": worker_id,
                    "limit": limit,
                    "lease_ms": lease_ms,
                }),
            )
            .await?;
        Ok(response.deliveries)
    }

    pub async fn report_outbox(&self, report: ChannelOutboxReportInput<'_>) -> Result<()> {
        let _: Value = self
            .post_json(
                "/v0/channels/outbox/report",
                &serde_json::json!({
                    "channel_id": report.channel_id,
                    "worker_id": report.worker_id,
                    "delivery_id": report.delivery.delivery_id,
                    "attempt_id": report.delivery.attempt_id,
                    "outcome": report.outcome,
                    "provider_receipt": report.provider_receipt,
                    "error_code": report.error_code,
                    "error_text": report.error_text,
                }),
            )
            .await?;
        Ok(())
    }

    pub async fn consume_channel_grant(
        &self,
        channel_id: &str,
        grant_id: &str,
        expected_label: &str,
        reason: &str,
    ) -> Result<()> {
        let _: Value = self
            .post_json(
                "/v0/channels/grants/consume",
                &serde_json::json!({
                    "channel_id": channel_id,
                    "grant_id": grant_id,
                    "expected_label": expected_label,
                    "reason": reason,
                }),
            )
            .await?;
        Ok(())
    }

    pub async fn report_health(
        &self,
        channel_id: &str,
        worker_id: &str,
        status: &str,
        code: &str,
        message: &str,
        details: Value,
    ) -> Result<()> {
        let _: Value = self
            .post_json(
                "/v0/channels/health/report",
                &serde_json::json!({
                    "channel_id": channel_id,
                    "reporter_id": worker_id,
                    "status": status,
                    "checks": [{
                        "code": code,
                        "status": status,
                        "message": message,
                        "details": details,
                    }],
                    "observed_at": Utc::now(),
                }),
            )
            .await?;
        Ok(())
    }

    async fn get_json<T>(&self, path: &str) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let response = self
            .client
            .get(self.url(path))
            .send()
            .await
            .with_context(|| format!("GET {path} failed"))?;
        response_json(response).await
    }

    async fn post_json<T, B>(&self, path: &str, body: &B) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
        B: Serialize + ?Sized,
    {
        let response = self
            .client
            .post(self.url(path))
            .json(body)
            .send()
            .await
            .with_context(|| format!("POST {path} failed"))?;
        response_json(response).await
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }
}

async fn response_json<T>(response: reqwest::Response) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let (status, text) = response_text(response).await?;
    if !status.is_success() {
        anyhow::bail!("HTTP {status}: {text}");
    }
    serde_json::from_str(&text).with_context(|| format!("failed to decode response body: {text}"))
}

async fn response_text(response: reqwest::Response) -> Result<(StatusCode, String)> {
    let status = response.status();
    let text = response
        .text()
        .await
        .context("failed to read response body")?;
    Ok((status, text))
}
