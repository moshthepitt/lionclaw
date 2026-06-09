use std::{
    collections::{BTreeMap, BTreeSet},
    io::{IsTerminal, Read, Write},
    os::unix::net::UnixStream,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use serde::Serialize;

use crate::{
    config::{SendAttachmentConfig, SendConfig},
    inventory::{ProjectInventory, RecipientRoute, RouteError},
};

const SOCKET_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Serialize)]
pub struct SendSummary {
    pub ok: bool,
    pub deliveries: Vec<SendDelivery>,
}

#[derive(Debug, Serialize)]
pub struct SendDelivery {
    pub recipient: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<SendError>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SendError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Serialize)]
struct ChannelSendRequest<'a> {
    idempotency_key: &'a str,
    channel_id: &'a str,
    conversation_ref: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    thread_ref: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_to_ref: Option<&'a str>,
    content: ChannelSendContent<'a>,
}

#[derive(Debug, Serialize)]
struct ChannelSendContent<'a> {
    text: &'a str,
    format_hint: &'a str,
    attachments: Vec<ChannelSendAttachment<'a>>,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct ChannelSendAttachment<'a> {
    path: &'a str,
}

struct SendRequestInput<'a> {
    route: &'a RecipientRoute,
    message: &'a str,
    format_hint: &'a str,
    attachments: &'a [ChannelSendAttachment<'a>],
    reply_to_ref: Option<&'a str>,
    idempotency_key: String,
}

pub fn run(mut config: SendConfig) -> Result<SendSummary> {
    let message = match config.message.take() {
        Some(message) => message,
        None if !config.attachments.is_empty() && std::io::stdin().is_terminal() => String::new(),
        None => {
            let mut message = String::new();
            std::io::stdin()
                .read_to_string(&mut message)
                .context("failed to read team-local message from stdin")?;
            message
        }
    };
    send_message(config, message)
}

fn send_message(config: SendConfig, message: String) -> Result<SendSummary> {
    if message.trim().is_empty() && config.attachments.is_empty() {
        bail!("team-local send message or attachment is required");
    }
    if config
        .reply_to_ref
        .as_deref()
        .map(str::trim)
        .is_some_and(|value| !value.is_empty())
    {
        bail!("team-local does not support reply refs; send an addressed message to the instance instead");
    }

    let recipients = unique_recipients(&config.recipients)?;
    let attachments = validate_attachments(&config.attachments)?;
    let inventory = ProjectInventory::load(&config.instances_file)?;
    let idempotency_base = match config.idempotency_key {
        Some(key) => key,
        None => generated_idempotency_key()?,
    };
    let recipient_count = recipients.len();
    let plan = match inventory.plan(&config.self_instance, &recipients) {
        Ok(plan) => plan,
        Err(errors) => {
            return Ok(SendSummary {
                ok: false,
                deliveries: unresolved_deliveries(&recipients, errors),
            });
        }
    };

    let deliveries = plan
        .into_iter()
        .map(|item| {
            send_to_route(
                &config.channel_send_socket,
                &item.recipient,
                SendRequestInput {
                    route: &item.route,
                    message: message.as_str(),
                    format_hint: config.format_hint.as_str(),
                    attachments: attachments.as_slice(),
                    reply_to_ref: config.reply_to_ref.as_deref(),
                    idempotency_key: idempotency_key_for(
                        &idempotency_base,
                        &item.recipient,
                        recipient_count,
                    ),
                },
            )
        })
        .collect::<Vec<_>>();

    Ok(SendSummary {
        ok: deliveries.iter().all(|delivery| delivery.ok),
        deliveries,
    })
}

fn unique_recipients(recipients: &[String]) -> Result<Vec<String>> {
    let mut seen = BTreeSet::new();
    let mut unique = Vec::with_capacity(recipients.len());
    for recipient in recipients {
        let recipient = recipient.trim();
        if recipient.is_empty() {
            bail!("team-local recipient cannot be empty");
        }
        if !seen.insert(recipient.to_string()) {
            bail!("duplicate team-local recipient '{recipient}'");
        }
        unique.push(recipient.to_string());
    }
    Ok(unique)
}

fn validate_attachments(
    attachments: &[SendAttachmentConfig],
) -> Result<Vec<ChannelSendAttachment<'_>>> {
    attachments
        .iter()
        .map(|attachment| {
            let path = attachment.path.trim();
            if path.is_empty() {
                bail!("team-local attachment path cannot be empty");
            }
            Ok(ChannelSendAttachment { path })
        })
        .collect()
}

fn idempotency_key_for(base: &str, recipient: &str, recipient_count: usize) -> String {
    if recipient_count == 1 {
        base.to_string()
    } else {
        format!("{base}:{recipient}")
    }
}

fn unresolved_deliveries(
    recipients: &[String],
    errors: Vec<(String, RouteError)>,
) -> Vec<SendDelivery> {
    let mut errors = errors.into_iter().collect::<BTreeMap<_, _>>();
    recipients
        .iter()
        .map(|recipient| {
            let error = errors.remove(recipient).map(Into::into).unwrap_or_else(|| {
                send_error(
                    "not_sent",
                    "team-local did not send any messages because at least one recipient could not be resolved",
                )
            });
            SendDelivery::failed(recipient.clone(), error)
        })
        .collect()
}

fn generated_idempotency_key() -> Result<String> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?;
    Ok(format!(
        "team-local-send-{}-{}",
        std::process::id(),
        elapsed.as_nanos()
    ))
}

fn send_to_route(socket: &Path, recipient: &str, request: SendRequestInput<'_>) -> SendDelivery {
    match send_request(socket, request) {
        Ok(response) => response.into_delivery(recipient.to_string()),
        Err(err) => SendDelivery::failed(
            recipient.to_string(),
            SendError {
                code: "send_failed".to_string(),
                message: err.to_string(),
            },
        ),
    }
}

fn send_request(socket: &Path, request: SendRequestInput<'_>) -> Result<ChannelSendResponse> {
    let mut stream = UnixStream::connect(socket)
        .with_context(|| format!("failed to connect to {}", socket.display()))?;
    stream
        .set_read_timeout(Some(SOCKET_TIMEOUT))
        .context("failed to set channel-send read timeout")?;
    stream
        .set_write_timeout(Some(SOCKET_TIMEOUT))
        .context("failed to set channel-send write timeout")?;

    let wire_request = ChannelSendRequest {
        idempotency_key: request.idempotency_key.as_str(),
        channel_id: &request.route.channel_id,
        conversation_ref: &request.route.conversation_ref,
        thread_ref: request.route.thread_ref.as_deref(),
        reply_to_ref: request.reply_to_ref,
        content: ChannelSendContent {
            text: request.message,
            format_hint: request.format_hint,
            attachments: request.attachments.to_vec(),
        },
    };
    serde_json::to_writer(&mut stream, &wire_request)
        .context("failed to encode channel-send request")?;
    stream
        .write_all(b"\n")
        .context("failed to write channel-send request")?;
    stream
        .shutdown(std::net::Shutdown::Write)
        .context("failed to finish channel-send request")?;

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .context("failed to read channel-send response")?;
    ChannelSendResponse::parse(&response)
}

fn send_error(code: impl Into<String>, message: impl Into<String>) -> SendError {
    SendError {
        code: code.into(),
        message: message.into(),
    }
}

impl From<RouteError> for SendError {
    fn from(error: RouteError) -> Self {
        Self {
            code: error.code,
            message: error.message,
        }
    }
}

#[derive(Debug)]
enum ChannelSendResponse {
    Accepted {
        delivery_id: Option<String>,
        status: Option<String>,
    },
    Rejected(SendError),
}

impl ChannelSendResponse {
    fn parse(raw: &str) -> Result<Self> {
        let value = serde_json::from_str::<serde_json::Value>(raw.trim())
            .with_context(|| format!("failed to parse channel-send response: {raw}"))?;
        if value.get("ok").and_then(serde_json::Value::as_bool) == Some(true) {
            return Ok(Self::Accepted {
                delivery_id: value
                    .get("delivery_id")
                    .and_then(serde_json::Value::as_str)
                    .map(ToString::to_string),
                status: value
                    .get("status")
                    .and_then(serde_json::Value::as_str)
                    .map(ToString::to_string),
            });
        }
        let error = value.get("error").unwrap_or(&serde_json::Value::Null);
        Ok(Self::Rejected(SendError {
            code: error
                .get("code")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("send_rejected")
                .to_string(),
            message: error
                .get("message")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("channel-send rejected the request")
                .to_string(),
        }))
    }

    fn into_delivery(self, recipient: String) -> SendDelivery {
        match self {
            Self::Accepted {
                delivery_id,
                status,
            } => SendDelivery {
                recipient,
                ok: true,
                delivery_id,
                status,
                error: None,
            },
            Self::Rejected(error) => SendDelivery::failed(recipient, error),
        }
    }
}

impl SendDelivery {
    fn failed(recipient: String, error: SendError) -> Self {
        Self {
            recipient,
            ok: false,
            delivery_id: None,
            status: None,
            error: Some(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{BufRead, BufReader, Write},
        os::unix::net::UnixListener,
        path::{Path, PathBuf},
        thread,
    };

    use serde_json::json;
    use tempfile::tempdir;

    use crate::config::{SendAttachmentConfig, SendConfig};

    use super::send_message;

    #[test]
    fn sends_same_message_to_multiple_recipients() {
        let temp_dir = tempdir().expect("temp dir");
        let socket = temp_dir.path().join("channel-send.sock");
        let listener = UnixListener::bind(&socket).expect("listener");
        let server = thread::spawn(move || {
            let mut requests = Vec::new();
            for index in 0..2 {
                let (stream, _) = listener.accept().expect("accept");
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                reader.read_line(&mut line).expect("request");
                let request: serde_json::Value = serde_json::from_str(&line).expect("json");
                requests.push(request);
                let mut stream = reader.into_inner();
                writeln!(
                    stream,
                    "{}",
                    json!({
                        "ok": true,
                        "delivery_id": format!("delivery-{index}"),
                        "status": "queued"
                    })
                )
                .expect("response");
            }
            requests
        });

        let inventory = temp_dir.path().join("instances.json");
        std::fs::write(
            &inventory,
            json!({
                "schema_version": 2,
                "default_instance": "main",
                "instances": [
                    { "name": "main" },
                    {
                        "name": "reviewer",
                        "channel_send": {
                            "status": "configured",
                            "channel_id": "team-local",
                            "conversation_ref": "team-local:peer:home-reviewer",
                            "thread_ref": null
                        }
                    },
                    {
                        "name": "qa",
                        "channel_send": {
                            "status": "configured",
                            "channel_id": "team-local",
                            "conversation_ref": "team-local:peer:home-qa"
                        }
                    }
                ]
            })
            .to_string(),
        )
        .expect("inventory");

        let summary = send_message(
            SendConfig {
                self_instance: "main".to_string(),
                instances_file: inventory,
                channel_send_socket: socket,
                recipients: vec!["reviewer".to_string(), "qa".to_string()],
                message: None,
                format_hint: "markdown".to_string(),
                attachments: Vec::new(),
                reply_to_ref: None,
                idempotency_key: Some("turn-1".to_string()),
            },
            " Please check this.\n".to_string(),
        )
        .expect("send");

        assert!(summary.ok);
        assert_eq!(summary.deliveries.len(), 2);
        let requests = server.join().expect("server");
        assert_eq!(requests[0]["idempotency_key"], "turn-1:reviewer");
        assert_eq!(requests[0]["channel_id"], "team-local");
        assert_eq!(
            requests[0]["conversation_ref"],
            "team-local:peer:home-reviewer"
        );
        assert_eq!(requests[0]["content"]["text"], " Please check this.\n");
        assert_eq!(requests[1]["idempotency_key"], "turn-1:qa");
        assert_eq!(requests[1]["conversation_ref"], "team-local:peer:home-qa");
    }

    #[test]
    fn sends_format_and_runtime_attachments() {
        let temp_dir = tempdir().expect("temp dir");
        let socket = temp_dir.path().join("channel-send.sock");
        let listener = UnixListener::bind(&socket).expect("listener");
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept");
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line).expect("request");
            let request: serde_json::Value = serde_json::from_str(&line).expect("json");
            let mut stream = reader.into_inner();
            writeln!(
                stream,
                "{}",
                json!({
                    "ok": true,
                    "delivery_id": "delivery-1",
                    "status": "queued"
                })
            )
            .expect("response");
            request
        });

        let inventory = write_single_recipient_inventory(temp_dir.path());

        let summary = send_message(
            SendConfig {
                self_instance: "main".to_string(),
                instances_file: inventory,
                channel_send_socket: socket,
                recipients: vec!["reviewer".to_string()],
                message: None,
                format_hint: "html".to_string(),
                attachments: vec![SendAttachmentConfig {
                    path: "/runtime/output/report.html".to_string(),
                }],
                reply_to_ref: None,
                idempotency_key: Some("turn-1".to_string()),
            },
            String::new(),
        )
        .expect("send");

        assert!(summary.ok);
        let request = server.join().expect("server");
        assert!(request.get("reply_to_ref").is_none());
        assert_eq!(request["content"]["text"], "");
        assert_eq!(request["content"]["format_hint"], "html");
        assert_eq!(
            request["content"]["attachments"][0]["path"],
            "/runtime/output/report.html"
        );
    }

    #[test]
    fn rejects_reply_refs() {
        let temp_dir = tempdir().expect("temp dir");
        let inventory = write_single_recipient_inventory(temp_dir.path());

        let err = send_message(
            SendConfig {
                self_instance: "main".to_string(),
                instances_file: inventory,
                channel_send_socket: temp_dir.path().join("missing.sock"),
                recipients: vec!["reviewer".to_string()],
                message: None,
                format_hint: "markdown".to_string(),
                attachments: Vec::new(),
                reply_to_ref: Some("source-message".to_string()),
                idempotency_key: Some("turn-1".to_string()),
            },
            "reply".to_string(),
        )
        .expect_err("team-local is address-only");

        assert!(err.to_string().contains("does not support reply refs"));
    }

    fn write_single_recipient_inventory(root: &Path) -> PathBuf {
        let inventory = root.join("instances.json");
        std::fs::write(
            &inventory,
            json!({
                "schema_version": 2,
                "default_instance": "main",
                "instances": [
                    { "name": "main" },
                    {
                        "name": "reviewer",
                        "channel_send": {
                            "status": "configured",
                            "channel_id": "team-local",
                            "conversation_ref": "team-local:peer:home-reviewer"
                        }
                    }
                ]
            })
            .to_string(),
        )
        .expect("inventory");
        inventory
    }

    #[test]
    fn reports_inventory_route_errors_without_connecting_to_socket() {
        let temp_dir = tempdir().expect("temp dir");
        let inventory = temp_dir.path().join("instances.json");
        std::fs::write(
            &inventory,
            json!({
                "schema_version": 2,
                "default_instance": "main",
                "instances": [
                    { "name": "main" },
                    {
                        "name": "reviewer",
                        "channel_send": { "status": "channel_missing" }
                    },
                    {
                        "name": "qa",
                        "channel_send": {
                            "status": "configured",
                            "channel_id": "team-local",
                            "conversation_ref": "team-local:peer:home-qa"
                        }
                    }
                ]
            })
            .to_string(),
        )
        .expect("inventory");

        let summary = send_message(
            SendConfig {
                self_instance: "main".to_string(),
                instances_file: inventory,
                channel_send_socket: temp_dir.path().join("missing.sock"),
                recipients: vec![
                    "qa".to_string(),
                    "main".to_string(),
                    "reviewer".to_string(),
                    "missing".to_string(),
                ],
                message: None,
                format_hint: "markdown".to_string(),
                attachments: Vec::new(),
                reply_to_ref: None,
                idempotency_key: Some("turn-1".to_string()),
            },
            "Please check this.".to_string(),
        )
        .expect("send");

        assert!(!summary.ok);
        assert_eq!(summary.deliveries.len(), 4);
        assert_eq!(
            summary.deliveries[0].error.as_ref().expect("error").code,
            "not_sent"
        );
        assert_eq!(
            summary.deliveries[1].error.as_ref().expect("error").code,
            "self_recipient"
        );
        assert_eq!(
            summary.deliveries[2].error.as_ref().expect("error").code,
            "channel_missing"
        );
        assert_eq!(
            summary.deliveries[3].error.as_ref().expect("error").code,
            "unknown_recipient"
        );
    }
}
