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
    #[serde(default)]
    pub history_policy: Option<SessionHistoryPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonInfoResponse {
    pub service: String,
    pub status: String,
    pub home_id: String,
    pub home_root: String,
    pub bind_addr: String,
    pub project_scope: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionOpenResponse {
    pub session_id: Uuid,
    pub channel_id: String,
    pub peer_id: String,
    pub trust_tier: TrustTier,
    pub history_policy: SessionHistoryPolicy,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionHistoryPolicy {
    Interactive,
    #[default]
    Conservative,
}

impl SessionHistoryPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::Conservative => "conservative",
        }
    }
}

impl FromStr for SessionHistoryPolicy {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "interactive" => Ok(Self::Interactive),
            "conservative" => Ok(Self::Conservative),
            other => Err(format!("invalid history policy '{}'", other)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionTurnKind {
    Normal,
    Retry,
    Continue,
}

impl SessionTurnKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Retry => "retry",
            Self::Continue => "continue",
        }
    }
}

impl FromStr for SessionTurnKind {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "normal" => Ok(Self::Normal),
            "retry" => Ok(Self::Retry),
            "continue" => Ok(Self::Continue),
            other => Err(format!("invalid session turn kind '{}'", other)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionTurnStatus {
    Running,
    Completed,
    Failed,
    TimedOut,
    Cancelled,
    Interrupted,
}

impl SessionTurnStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
            Self::Interrupted => "interrupted",
        }
    }
}

impl FromStr for SessionTurnStatus {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "timed_out" => Ok(Self::TimedOut),
            "cancelled" => Ok(Self::Cancelled),
            "interrupted" => Ok(Self::Interrupted),
            other => Err(format!("invalid session turn status '{}'", other)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionTurnView {
    pub turn_id: Uuid,
    pub kind: SessionTurnKind,
    pub status: SessionTurnStatus,
    pub display_user_text: String,
    pub prompt_user_text: String,
    pub assistant_text: String,
    pub error_code: Option<String>,
    pub error_text: Option<String>,
    pub runtime_id: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionHistoryRequest {
    pub session_id: Uuid,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionHistoryResponse {
    pub turns: Vec<SessionTurnView>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SessionLatestQuery {
    pub channel_id: String,
    pub peer_id: String,
    #[serde(default)]
    pub history_policy: Option<SessionHistoryPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionLatestResponse {
    #[serde(default)]
    pub session: Option<SessionOpenResponse>,
    pub turns: Vec<SessionTurnView>,
    #[serde(default)]
    pub resume_after_sequence: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionActionKind {
    ContinueLastPartial,
    RetryLastTurn,
    ResetSession,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionActionRequest {
    pub session_id: Uuid,
    pub action: SessionActionKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionActionResponse {
    pub session_id: Uuid,
    #[serde(default)]
    pub turn_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamLaneDto {
    Answer,
    Reasoning,
}

impl StreamLaneDto {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Answer => "answer",
            Self::Reasoning => "reasoning",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamEventKindDto {
    MessageDelta,
    Status,
    Error,
    Done,
}

impl StreamEventKindDto {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::MessageDelta => "message_delta",
            Self::Status => "status",
            Self::Error => "error",
            Self::Done => "done",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEventDto {
    pub kind: StreamEventKindDto,
    #[serde(default)]
    pub lane: Option<StreamLaneDto>,
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionTurnResponse {
    pub session_id: Uuid,
    pub turn_id: Uuid,
    pub assistant_text: String,
    pub selected_skills: Vec<String>,
    pub runtime_id: String,
    pub stream_events: Vec<StreamEventDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillInstallRequest {
    pub source: String,
    pub reference: Option<String>,
    pub hash: Option<String>,
    pub skill_md: Option<String>,
    #[serde(default)]
    pub snapshot_path: Option<String>,
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
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum JobScheduleDto {
    Once { run_at: DateTime<Utc> },
    Interval { every_ms: u64, anchor_ms: i64 },
    Cron { expr: String, timezone: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDeliveryTargetDto {
    pub channel_id: String,
    pub peer_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerJobRunStatusDto {
    Running,
    Completed,
    Failed,
    DeadLetter,
    Interrupted,
}

impl SchedulerJobRunStatusDto {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::DeadLetter => "dead_letter",
            Self::Interrupted => "interrupted",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerJobDeliveryStatusDto {
    Pending,
    Delivered,
    Failed,
    NotRequested,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerJobTriggerKindDto {
    Schedule,
    Manual,
    Retry,
    Recovery,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobCreateRequest {
    pub name: String,
    pub runtime_id: String,
    pub schedule: JobScheduleDto,
    pub prompt_text: String,
    #[serde(default)]
    pub skill_ids: Vec<String>,
    #[serde(default)]
    pub allow_capabilities: Vec<String>,
    #[serde(default)]
    pub delivery: Option<JobDeliveryTargetDto>,
    #[serde(default)]
    pub retry_attempts: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRefRequest {
    pub job_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRunsRequest {
    pub job_id: Uuid,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobView {
    pub job_id: Uuid,
    pub name: String,
    pub enabled: bool,
    pub runtime_id: String,
    pub schedule: JobScheduleDto,
    pub prompt_text: String,
    pub skill_ids: Vec<String>,
    #[serde(default)]
    pub delivery: Option<JobDeliveryTargetDto>,
    pub retry_attempts: u32,
    #[serde(default)]
    pub next_run_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub running_run_id: Option<Uuid>,
    #[serde(default)]
    pub last_run_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_status: Option<SchedulerJobRunStatusDto>,
    #[serde(default)]
    pub last_error: Option<String>,
    pub consecutive_failures: u32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobCreateResponse {
    pub job: JobView,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobGetResponse {
    pub job: JobView,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobListResponse {
    pub jobs: Vec<JobView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobToggleResponse {
    pub job: JobView,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRemoveResponse {
    pub job_id: Uuid,
    pub removed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRunView {
    pub run_id: Uuid,
    pub job_id: Uuid,
    pub attempt_no: u32,
    pub trigger_kind: SchedulerJobTriggerKindDto,
    #[serde(default)]
    pub scheduled_for: Option<DateTime<Utc>>,
    pub started_at: DateTime<Utc>,
    #[serde(default)]
    pub finished_at: Option<DateTime<Utc>>,
    pub status: SchedulerJobRunStatusDto,
    #[serde(default)]
    pub session_id: Option<Uuid>,
    #[serde(default)]
    pub turn_id: Option<Uuid>,
    #[serde(default)]
    pub delivery_status: Option<SchedulerJobDeliveryStatusDto>,
    #[serde(default)]
    pub error_text: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRunsResponse {
    pub runs: Vec<JobRunView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobManualRunResponse {
    pub job: JobView,
    pub run: JobRunView,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobTickResponse {
    pub claimed_runs: usize,
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
pub struct ContinuityArtifactView {
    pub title: String,
    pub relative_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuityOpenLoopView {
    pub title: String,
    pub relative_path: String,
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub next_step: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuityMemoryProposalView {
    pub title: String,
    pub relative_path: String,
    #[serde(default)]
    pub rationale: Option<String>,
    #[serde(default)]
    pub entries: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuitySearchMatchView {
    pub title: String,
    pub relative_path: String,
    pub snippet: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuityStatusResponse {
    pub memory_path: String,
    pub active_path: String,
    #[serde(default)]
    pub latest_daily_path: Option<String>,
    pub open_loops: Vec<ContinuityOpenLoopView>,
    pub recent_artifacts: Vec<ContinuityArtifactView>,
    pub memory_proposals: Vec<ContinuityMemoryProposalView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuitySearchRequest {
    pub query: String,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuitySearchResponse {
    pub matches: Vec<ContinuitySearchMatchView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuityPathRequest {
    pub relative_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuityDraftListRequest {
    #[serde(default)]
    pub runtime_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuityDraftActionRequest {
    #[serde(default)]
    pub runtime_id: Option<String>,
    pub relative_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuityDraftView {
    pub relative_path: String,
    pub size_bytes: u64,
    pub media_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuityDraftListResponse {
    pub runtime_id: String,
    pub drafts: Vec<ContinuityDraftView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuityDraftPromoteResponse {
    pub runtime_id: String,
    pub draft_path: String,
    pub artifact_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuityDraftDiscardResponse {
    pub runtime_id: String,
    pub draft_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuityGetResponse {
    pub relative_path: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuityProposalListResponse {
    pub proposals: Vec<ContinuityMemoryProposalView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuityProposalActionResponse {
    pub archived_path: String,
    #[serde(default)]
    pub memory_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuityOpenLoopListResponse {
    pub loops: Vec<ContinuityOpenLoopView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuityOpenLoopActionResponse {
    pub archived_path: String,
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
    pub session_id: Option<Uuid>,
    #[serde(default)]
    pub update_id: Option<i64>,
    #[serde(default)]
    pub external_message_id: Option<String>,
    #[serde(default)]
    pub runtime_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelInboundResponse {
    pub outcome: ChannelInboundOutcome,
    #[serde(default)]
    pub turn_id: Option<Uuid>,
    #[serde(default)]
    pub session_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelInboundOutcome {
    Queued,
    Duplicate,
    PairingPending,
    PeerBlocked,
}

impl ChannelInboundOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Duplicate => "duplicate",
            Self::PairingPending => "pairing_pending",
            Self::PeerBlocked => "peer_blocked",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelStreamStartMode {
    Resume,
    Tail,
}

impl ChannelStreamStartMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Resume => "resume",
            Self::Tail => "tail",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelStreamPullRequest {
    pub channel_id: String,
    pub consumer_id: String,
    #[serde(default)]
    pub start_mode: Option<ChannelStreamStartMode>,
    #[serde(default)]
    pub start_after_sequence: Option<i64>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub wait_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelStreamEventView {
    pub sequence: i64,
    pub channel_id: String,
    pub peer_id: String,
    pub session_id: Uuid,
    pub turn_id: Uuid,
    pub kind: StreamEventKindDto,
    #[serde(default)]
    pub lane: Option<StreamLaneDto>,
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelStreamPullResponse {
    pub events: Vec<ChannelStreamEventView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelStreamAckRequest {
    pub channel_id: String,
    pub consumer_id: String,
    pub through_sequence: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelStreamAckResponse {
    pub channel_id: String,
    pub consumer_id: String,
    pub through_sequence: i64,
    pub acknowledged: bool,
}
