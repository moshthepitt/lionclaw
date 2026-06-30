use std::path::{Path, PathBuf};

use super::policy::Capability;
use anyhow::{anyhow, Context, Result};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};

const FS_READ_MAX_BYTES: u64 = 64 * 1024;
pub const CHANNEL_SEND_ROUTE_NOT_PROJECTED: &str =
    "channel.send route is not projected for this turn";

#[derive(Debug, Clone)]
pub struct CapabilityExecutionContext<'a> {
    pub channel_send_route: Option<CapabilityChannelSendRoute<'a>>,
    pub projected_channel_send_routes: Vec<CapabilityChannelSendRoute<'a>>,
    pub allow_channel_send_route_selection: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct CapabilityChannelSendRoute<'a> {
    pub channel_id: &'a str,
    pub conversation_ref: &'a str,
    pub thread_ref: Option<&'a str>,
    pub reply_to_ref: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct CapabilityBroker {
    filesystem: FilesystemBroker,
    network: NetworkBroker,
    secrets: SecretBroker,
    scheduler: SchedulerBroker,
}

impl Default for CapabilityBroker {
    fn default() -> Self {
        Self::new(default_workspace_root())
    }
}

impl CapabilityBroker {
    pub fn new(workspace_root: PathBuf) -> Self {
        Self {
            filesystem: FilesystemBroker::new(workspace_root),
            network: NetworkBroker,
            secrets: SecretBroker,
            scheduler: SchedulerBroker,
        }
    }

    pub async fn execute(
        &self,
        context: &CapabilityExecutionContext<'_>,
        capability: Capability,
        payload: &Value,
    ) -> Result<Value> {
        match capability {
            Capability::FsRead => self.filesystem.read(payload).await,
            Capability::FsWrite => self.filesystem.write(payload).await,
            Capability::NetEgress => self.network.egress(payload).await,
            Capability::SecretRequest => self.secrets.request(payload).await,
            Capability::ChannelSend => build_channel_send_intent(context, payload).await,
            Capability::SchedulerRun => self.scheduler.run(payload).await,
            Capability::Any => Err(anyhow!(
                "capability '{}' is not broker-executable",
                capability.as_str()
            )),
        }
    }
}

#[derive(Debug, Clone)]
struct FilesystemBroker {
    workspace_root: PathBuf,
}

impl FilesystemBroker {
    fn new(workspace_root: PathBuf) -> Self {
        Self { workspace_root }
    }

    async fn read(&self, payload: &Value) -> Result<Value> {
        let request: FsReadRequest = parse_payload(payload, Capability::FsRead)?;
        let resolved = self.resolve_existing_path(&request.path)?;

        let metadata = tokio::fs::metadata(&resolved)
            .await
            .with_context(|| format!("failed to read metadata for '{}'", resolved.display()))?;
        if metadata.len() > FS_READ_MAX_BYTES {
            return Err(anyhow!(
                "fs.read rejects files larger than {FS_READ_MAX_BYTES} bytes"
            ));
        }

        let content = tokio::fs::read_to_string(&resolved)
            .await
            .with_context(|| format!("failed to read file '{}'", resolved.display()))?;

        Ok(json!({
            "path": resolved.to_string_lossy(),
            "bytes": content.len(),
            "content": content,
        }))
    }

    async fn write(&self, payload: &Value) -> Result<Value> {
        let request: FsWriteRequest = parse_payload(payload, Capability::FsWrite)?;
        let resolved = self.resolve_write_path(&request.path)?;

        tokio::fs::write(&resolved, request.content.as_bytes())
            .await
            .with_context(|| format!("failed to write file '{}'", resolved.display()))?;

        Ok(json!({
            "path": resolved.to_string_lossy(),
            "bytes_written": request.content.len(),
        }))
    }

    fn resolve_existing_path(&self, raw_path: &str) -> Result<PathBuf> {
        let requested = self.resolve_requested_path(raw_path)?;
        let canonical = requested
            .canonicalize()
            .with_context(|| format!("path '{}' does not exist", requested.display()))?;
        self.ensure_within_workspace(&canonical)?;
        Ok(canonical)
    }

    fn resolve_write_path(&self, raw_path: &str) -> Result<PathBuf> {
        let requested = self.resolve_requested_path(raw_path)?;
        let parent = requested
            .parent()
            .ok_or_else(|| anyhow!("path '{raw_path}' must include a parent directory"))?;
        let canonical_parent = parent
            .canonicalize()
            .with_context(|| format!("parent directory '{}' does not exist", parent.display()))?;
        self.ensure_within_workspace(&canonical_parent)?;

        let file_name = requested
            .file_name()
            .ok_or_else(|| anyhow!("path '{raw_path}' must include a file name"))?;

        Ok(canonical_parent.join(file_name))
    }

    fn resolve_requested_path(&self, raw_path: &str) -> Result<PathBuf> {
        let trimmed = raw_path.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("path cannot be empty"));
        }

        let requested = PathBuf::from(trimmed);
        if requested.is_absolute() {
            Ok(requested)
        } else {
            Ok(self.workspace_root.join(requested))
        }
    }

    fn ensure_within_workspace(&self, path: &Path) -> Result<()> {
        if path == self.workspace_root || path.starts_with(&self.workspace_root) {
            return Ok(());
        }
        Err(anyhow!(
            "path '{}' is outside workspace root '{}'",
            path.display(),
            self.workspace_root.display()
        ))
    }
}

#[derive(Debug, Clone, Copy)]
struct NetworkBroker;

impl NetworkBroker {
    async fn egress(&self, payload: &Value) -> Result<Value> {
        let request: NetEgressRequest = parse_payload(payload, Capability::NetEgress)?;
        let method = request
            .method
            .as_deref()
            .map(str::to_uppercase)
            .unwrap_or_else(|| "GET".to_string());

        Err(anyhow!(
            "net.egress broker is not configured (method='{}', url='{}')",
            method,
            request.url
        ))
    }
}

#[derive(Debug, Clone, Copy)]
struct SecretBroker;

impl SecretBroker {
    async fn request(&self, payload: &Value) -> Result<Value> {
        let request: SecretRequest = parse_payload(payload, Capability::SecretRequest)?;

        Err(anyhow!(
            "secret broker is not configured for '{}'",
            request.name
        ))
    }
}

#[derive(Debug, Clone, Copy)]
struct SchedulerBroker;

impl SchedulerBroker {
    async fn run(&self, payload: &Value) -> Result<Value> {
        let request: SchedulerRunRequest = parse_payload(payload, Capability::SchedulerRun)?;

        Err(anyhow!(
            "scheduler broker is not configured for '{}'",
            request.job
        ))
    }
}

#[derive(Debug, Deserialize)]
struct FsReadRequest {
    path: String,
}

#[derive(Debug, Deserialize)]
struct FsWriteRequest {
    path: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct NetEgressRequest {
    url: String,
    #[serde(default)]
    method: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SecretRequest {
    name: String,
}

#[derive(Debug, Deserialize)]
struct ChannelSendRequest {
    #[serde(default)]
    channel_id: Option<String>,
    #[serde(default)]
    conversation_ref: Option<String>,
    #[serde(default)]
    thread_ref: Option<String>,
    #[serde(default)]
    reply_to_ref: Option<String>,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    attachment_count: usize,
}

impl ChannelSendRequest {
    fn route_override_fields(&self) -> Vec<&'static str> {
        let mut fields = Vec::new();
        if self.channel_id.is_some() {
            fields.push("channel_id");
        }
        if self.conversation_ref.is_some() {
            fields.push("conversation_ref");
        }
        if self.thread_ref.is_some() {
            fields.push("thread_ref");
        }
        if self.reply_to_ref.is_some() {
            fields.push("reply_to_ref");
        }
        fields
    }
}

#[derive(Debug, Deserialize)]
struct SchedulerRunRequest {
    job: String,
}

async fn build_channel_send_intent(
    context: &CapabilityExecutionContext<'_>,
    payload: &Value,
) -> Result<Value> {
    let request: ChannelSendRequest = parse_payload(payload, Capability::ChannelSend)?;
    let override_fields = request.route_override_fields();
    let route = resolve_channel_send_route(context, &request, &override_fields)?;
    if !context.allow_channel_send_route_selection && !override_fields.is_empty() {
        return Err(anyhow!(
            "channel.send route is derived from the current channel session; remove {}",
            override_fields.join(", ")
        ));
    }

    let content = request.content.unwrap_or_default();
    let attachment_count = if context.allow_channel_send_route_selection {
        request.attachment_count
    } else {
        0
    };
    if content.trim().is_empty() && attachment_count == 0 {
        return Err(anyhow!("channel.send content cannot be empty"));
    }

    let channel_id = route.channel_id.trim();
    if channel_id.is_empty() {
        return Err(anyhow!("channel.send channel_id cannot be empty"));
    }

    let conversation_ref = route.conversation_ref.trim();
    if conversation_ref.is_empty() {
        return Err(anyhow!("channel.send conversation_ref cannot be empty"));
    }

    let mut intent = serde_json::Map::from_iter([
        ("channel_id".to_string(), json!(channel_id)),
        ("conversation_ref".to_string(), json!(conversation_ref)),
        ("content".to_string(), json!(content)),
    ]);
    if let Some(thread_ref) = route
        .thread_ref
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        intent.insert("thread_ref".to_string(), json!(thread_ref));
    }
    let reply_to_ref = if context.allow_channel_send_route_selection {
        request
            .reply_to_ref
            .as_deref()
            .and_then(non_empty_trimmed)
            .or_else(|| route.reply_to_ref.and_then(non_empty_trimmed))
    } else {
        route.reply_to_ref.and_then(non_empty_trimmed)
    };
    if let Some(reply_to_ref) = reply_to_ref {
        intent.insert("reply_to_ref".to_string(), json!(reply_to_ref));
    }
    Ok(Value::Object(intent))
}

fn resolve_channel_send_route<'a>(
    context: &CapabilityExecutionContext<'a>,
    request: &ChannelSendRequest,
    override_fields: &[&str],
) -> Result<CapabilityChannelSendRoute<'a>> {
    if override_fields.is_empty() {
        return context
            .channel_send_route
            .ok_or_else(|| anyhow!("channel.send is only available in channel sessions"));
    }

    if !context.allow_channel_send_route_selection {
        return context
            .channel_send_route
            .ok_or_else(|| anyhow!("channel.send is only available in channel sessions"));
    }

    if request
        .channel_id
        .as_deref()
        .and_then(non_empty_trimmed)
        .is_none()
        || request
            .conversation_ref
            .as_deref()
            .and_then(non_empty_trimmed)
            .is_none()
    {
        return Err(anyhow!(
            "channel.send route selection requires channel_id and conversation_ref"
        ));
    }

    context
        .channel_send_route
        .into_iter()
        .chain(context.projected_channel_send_routes.iter().copied())
        .find(|route| channel_send_route_matches(*route, request))
        .ok_or_else(|| anyhow!(CHANNEL_SEND_ROUTE_NOT_PROJECTED))
}

fn channel_send_route_matches(
    route: CapabilityChannelSendRoute<'_>,
    request: &ChannelSendRequest,
) -> bool {
    request.channel_id.as_deref().and_then(non_empty_trimmed) == non_empty_trimmed(route.channel_id)
        && request
            .conversation_ref
            .as_deref()
            .and_then(non_empty_trimmed)
            == non_empty_trimmed(route.conversation_ref)
        && request.thread_ref.as_deref().and_then(non_empty_trimmed)
            == route.thread_ref.and_then(non_empty_trimmed)
}

fn non_empty_trimmed(value: &str) -> Option<&str> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn parse_payload<T>(payload: &Value, capability: Capability) -> Result<T>
where
    T: DeserializeOwned,
{
    serde_json::from_value(payload.clone())
        .with_context(|| format!("invalid payload for capability '{}'", capability.as_str()))
}

fn default_workspace_root() -> PathBuf {
    match std::env::current_dir() {
        Ok(path) => path.canonicalize().unwrap_or(path),
        Err(_) => PathBuf::from("."),
    }
}
