use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::str::FromStr;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrustTier {
    Main,
    Untrusted,
}

impl TrustTier {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Main => "main",
            Self::Untrusted => "untrusted",
        }
    }
}

impl FromStr for TrustTier {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "main" => Ok(Self::Main),
            "untrusted" => Ok(Self::Untrusted),
            other => Err(format!("invalid trust tier '{}'", other)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionOpenRequest {
    pub channel_id: String,
    pub peer_id: String,
    pub trust_tier: TrustTier,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionOpenResponse {
    pub session_id: Uuid,
    pub channel_id: String,
    pub peer_id: String,
    pub trust_tier: TrustTier,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionTurnRequest {
    pub session_id: Uuid,
    pub user_text: String,
    pub runtime_id: Option<String>,
    #[serde(default)]
    pub runtime_working_dir: Option<String>,
    #[serde(default)]
    pub runtime_timeout_ms: Option<u64>,
    #[serde(default)]
    pub runtime_env_passthrough: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeEventDto {
    pub kind: String,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionTurnResponse {
    pub session_id: Uuid,
    pub assistant_text: String,
    pub selected_skills: Vec<String>,
    pub runtime_id: String,
    pub runtime_events: Vec<RuntimeEventDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillInstallRequest {
    pub source: String,
    pub reference: Option<String>,
    pub hash: Option<String>,
    pub skill_md: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillInstallResponse {
    pub skill_id: String,
    pub name: String,
    pub hash: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillView {
    pub skill_id: String,
    pub name: String,
    pub description: String,
    pub source: String,
    pub reference: Option<String>,
    pub hash: String,
    pub enabled: bool,
    pub installed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillListResponse {
    pub skills: Vec<SkillView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillToggleRequest {
    pub skill_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillToggleResponse {
    pub skill_id: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyGrantRequest {
    pub skill_id: String,
    pub capability: String,
    pub scope: String,
    pub ttl_seconds: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyGrantResponse {
    pub grant_id: Uuid,
    pub skill_id: String,
    pub capability: String,
    pub scope: String,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyRevokeRequest {
    pub grant_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyRevokeResponse {
    pub grant_id: Uuid,
    pub revoked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEventView {
    pub event_id: Uuid,
    pub event_type: String,
    pub session_id: Option<Uuid>,
    pub actor: Option<String>,
    pub details: Value,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditQueryResponse {
    pub events: Vec<AuditEventView>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuditQueryParams {
    pub session_id: Option<Uuid>,
    pub event_type: Option<String>,
    pub since: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelBindRequest {
    pub channel_id: String,
    pub skill_id: String,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub config: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelBindingView {
    pub channel_id: String,
    pub skill_id: String,
    pub enabled: bool,
    pub config: Value,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelBindResponse {
    pub binding: ChannelBindingView,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelListResponse {
    pub bindings: Vec<ChannelBindingView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelPeerView {
    pub channel_id: String,
    pub peer_id: String,
    pub status: String,
    pub trust_tier: TrustTier,
    pub pairing_code: Option<String>,
    pub first_seen: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelPeerListResponse {
    pub peers: Vec<ChannelPeerView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelPeerApproveRequest {
    pub channel_id: String,
    pub peer_id: String,
    pub pairing_code: String,
    #[serde(default)]
    pub trust_tier: Option<TrustTier>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelPeerBlockRequest {
    pub channel_id: String,
    pub peer_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelPeerResponse {
    pub peer: ChannelPeerView,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChannelPeerListParams {
    pub channel_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelInboundRequest {
    pub channel_id: String,
    pub peer_id: String,
    pub text: String,
    #[serde(default)]
    pub update_id: Option<i64>,
    #[serde(default)]
    pub external_message_id: Option<String>,
    #[serde(default)]
    pub runtime_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelInboundResponse {
    pub accepted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelOutboxPullRequest {
    pub channel_id: String,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelOutboxMessageView {
    pub message_id: Uuid,
    pub channel_id: String,
    pub peer_id: String,
    pub content: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelOutboxPullResponse {
    pub messages: Vec<ChannelOutboxMessageView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelOutboxAckRequest {
    pub message_id: Uuid,
    pub external_message_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelOutboxAckResponse {
    pub message_id: Uuid,
    pub acknowledged: bool,
}
