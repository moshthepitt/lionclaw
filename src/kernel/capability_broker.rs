use std::path::{Path, PathBuf};

use super::policy::Capability;
use anyhow::{anyhow, Context, Result};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};

const FS_READ_MAX_BYTES: u64 = 64 * 1024;

#[derive(Debug, Clone)]
pub struct CapabilityExecutionContext<'a> {
    pub session_channel_id: &'a str,
    pub session_peer_id: &'a str,
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
            Capability::Any | Capability::SkillUse => Err(anyhow!(
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
                "fs.read rejects files larger than {} bytes",
                FS_READ_MAX_BYTES
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
            .ok_or_else(|| anyhow!("path '{}' must include a parent directory", raw_path))?;
        let canonical_parent = parent
            .canonicalize()
            .with_context(|| format!("parent directory '{}' does not exist", parent.display()))?;
        self.ensure_within_workspace(&canonical_parent)?;

        let file_name = requested
            .file_name()
            .ok_or_else(|| anyhow!("path '{}' must include a file name", raw_path))?;

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
    content: String,
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
    if request.content.trim().is_empty() {
        return Err(anyhow!("channel.send content cannot be empty"));
    }

    let channel_id = request
        .channel_id
        .as_deref()
        .unwrap_or(context.session_channel_id)
        .trim()
        .to_string();
    if channel_id.is_empty() {
        return Err(anyhow!("channel.send channel_id cannot be empty"));
    }

    let conversation_ref = request
        .conversation_ref
        .as_deref()
        .unwrap_or(context.session_peer_id)
        .trim()
        .to_string();
    if conversation_ref.is_empty() {
        return Err(anyhow!("channel.send conversation_ref cannot be empty"));
    }

    Ok(json!({
        "channel_id": channel_id,
        "conversation_ref": conversation_ref,
        "content": request.content,
    }))
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
