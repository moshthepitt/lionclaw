use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{Read, Write},
    os::unix::net::UnixStream,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::{config::SendConfig, protocol::CHANNEL_ID};

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

#[derive(Debug, Deserialize)]
struct ProjectInventory {
    instances: Vec<ProjectInventoryEntry>,
}

#[derive(Debug, Deserialize)]
struct ProjectInventoryEntry {
    name: String,
    #[serde(default)]
    channel_send: Option<ProjectInventoryChannelSend>,
}

#[derive(Debug, Deserialize)]
struct ProjectInventoryChannelSend {
    status: String,
    #[serde(default)]
    channel_id: Option<String>,
    #[serde(default)]
    conversation_ref: Option<String>,
    #[serde(default)]
    thread_ref: Option<Option<String>>,
}

#[derive(Debug, Clone)]
struct RecipientRoute {
    channel_id: String,
    conversation_ref: String,
    thread_ref: Option<String>,
}

#[derive(Debug, Clone)]
struct RecipientPlan {
    recipient: String,
    route: RecipientRoute,
}

#[derive(Debug, Serialize)]
struct ChannelSendRequest<'a> {
    idempotency_key: String,
    channel_id: &'a str,
    conversation_ref: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    thread_ref: Option<&'a str>,
    content: ChannelSendContent<'a>,
}

#[derive(Debug, Serialize)]
struct ChannelSendContent<'a> {
    text: &'a str,
    format_hint: &'static str,
    attachments: Vec<ChannelSendAttachment>,
}

#[derive(Debug, Serialize)]
struct ChannelSendAttachment {}

pub fn run(mut config: SendConfig) -> Result<SendSummary> {
    let message = match config.message.take() {
        Some(message) => message,
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
    if message.trim().is_empty() {
        bail!("team-local send message cannot be empty");
    }

    let recipients = unique_recipients(&config.recipients)?;
    let inventory = ProjectInventory::load(&config.instances_file)?;
    let idempotency_base = match config.idempotency_key {
        Some(key) => key,
        None => generated_idempotency_key()?,
    };
    let recipient_count = recipients.len();
    let plan = match inventory.plan(&config.self_instance, &recipients) {
        Ok(plan) => plan,
        Err(deliveries) => {
            return Ok(SendSummary {
                ok: false,
                deliveries,
            });
        }
    };

    let deliveries = plan
        .into_iter()
        .map(|item| {
            send_to_route(
                &config.channel_send_socket,
                &item.recipient,
                &item.route,
                message.as_str(),
                idempotency_key_for(&idempotency_base, &item.recipient, recipient_count),
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

fn idempotency_key_for(base: &str, recipient: &str, recipient_count: usize) -> String {
    if recipient_count == 1 {
        base.to_string()
    } else {
        format!("{base}:{recipient}")
    }
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

fn send_to_route(
    socket: &Path,
    recipient: &str,
    route: &RecipientRoute,
    message: &str,
    idempotency_key: String,
) -> SendDelivery {
    match send_request(socket, route, message, idempotency_key) {
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

fn send_request(
    socket: &Path,
    route: &RecipientRoute,
    message: &str,
    idempotency_key: String,
) -> Result<ChannelSendResponse> {
    let mut stream = UnixStream::connect(socket)
        .with_context(|| format!("failed to connect to {}", socket.display()))?;
    stream
        .set_read_timeout(Some(SOCKET_TIMEOUT))
        .context("failed to set channel-send read timeout")?;
    stream
        .set_write_timeout(Some(SOCKET_TIMEOUT))
        .context("failed to set channel-send write timeout")?;

    let request = ChannelSendRequest {
        idempotency_key,
        channel_id: &route.channel_id,
        conversation_ref: &route.conversation_ref,
        thread_ref: route.thread_ref.as_deref(),
        content: ChannelSendContent {
            text: message,
            format_hint: "markdown",
            attachments: Vec::new(),
        },
    };
    serde_json::to_writer(&mut stream, &request)
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

impl ProjectInventory {
    fn load(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        serde_json::from_str(&content)
            .with_context(|| format!("failed to parse {}", path.display()))
    }

    fn route_for(
        &self,
        self_instance: &str,
        recipient: &str,
    ) -> std::result::Result<RecipientRoute, SendError> {
        if recipient == self_instance {
            return Err(send_error(
                "self_recipient",
                "team-local cannot send a message to the selected instance itself",
            ));
        }
        let Some(entry) = self.instances.iter().find(|entry| entry.name == recipient) else {
            return Err(send_error(
                "unknown_recipient",
                format!("project instance '{recipient}' is not in instances.json"),
            ));
        };
        let Some(channel_send) = &entry.channel_send else {
            return Err(send_error(
                "recipient_unconfigured",
                format!("project instance '{recipient}' has no channel_send route"),
            ));
        };
        if channel_send.status != "configured" {
            return Err(send_error(
                channel_send.status.as_str(),
                format!(
                    "project instance '{recipient}' channel_send status is '{}'",
                    channel_send.status
                ),
            ));
        }
        let channel_id =
            required_route_field(recipient, "channel_id", channel_send.channel_id.as_deref())?;
        if channel_id != CHANNEL_ID {
            return Err(send_error(
                "wrong_channel",
                format!("project instance '{recipient}' uses channel '{channel_id}'"),
            ));
        }
        let conversation_ref = required_route_field(
            recipient,
            "conversation_ref",
            channel_send.conversation_ref.as_deref(),
        )?;
        Ok(RecipientRoute {
            channel_id: channel_id.to_string(),
            conversation_ref: conversation_ref.to_string(),
            thread_ref: channel_send.thread_ref.clone().flatten(),
        })
    }

    fn plan(
        &self,
        self_instance: &str,
        recipients: &[String],
    ) -> std::result::Result<Vec<RecipientPlan>, Vec<SendDelivery>> {
        let mut plans = Vec::with_capacity(recipients.len());
        let mut errors = BTreeMap::new();
        for recipient in recipients {
            match self.route_for(self_instance, recipient) {
                Ok(route) => plans.push(RecipientPlan {
                    recipient: recipient.clone(),
                    route,
                }),
                Err(error) => {
                    errors.insert(recipient.clone(), error);
                }
            }
        }
        if errors.is_empty() {
            Ok(plans)
        } else {
            Err(recipients
                .iter()
                .map(|recipient| {
                    let error = errors.remove(recipient).unwrap_or_else(|| {
                        send_error(
                            "not_sent",
                            "team-local did not send any messages because at least one recipient could not be resolved",
                        )
                    });
                    SendDelivery::failed(recipient.clone(), error)
                })
                .collect())
        }
    }
}

fn required_route_field<'a>(
    recipient: &str,
    field: &str,
    value: Option<&'a str>,
) -> std::result::Result<&'a str, SendError> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            send_error(
                "recipient_misconfigured",
                format!("project instance '{recipient}' route is missing {field}"),
            )
        })
}

fn send_error(code: impl Into<String>, message: impl Into<String>) -> SendError {
    SendError {
        code: code.into(),
        message: message.into(),
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
        thread,
    };

    use serde_json::json;
    use tempfile::tempdir;

    use super::{send_message, SendConfig};

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
