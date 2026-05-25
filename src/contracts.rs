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
            other => Err(format!("invalid trust tier '{other}'")),
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
    pub daemon: String,
    pub status: String,
    pub home_id: String,
    pub home_root: String,
    pub bind_addr: String,
    pub project_scope: String,
    pub daemon_fingerprint: String,
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
            other => Err(format!("invalid history policy '{other}'")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionTurnKind {
    Normal,
    Retry,
    Continue,
    RuntimeControl,
}

impl SessionTurnKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Retry => "retry",
            Self::Continue => "continue",
            Self::RuntimeControl => "runtime_control",
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
            "runtime_control" => Ok(Self::RuntimeControl),
            other => Err(format!("invalid session turn kind '{other}'")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionTurnStatus {
    Running,
    WaitingForAttachments,
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
            Self::WaitingForAttachments => "waiting_for_attachments",
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
            "waiting_for_attachments" => Ok(Self::WaitingForAttachments),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "timed_out" => Ok(Self::TimedOut),
            "cancelled" => Ok(Self::Cancelled),
            "interrupted" => Ok(Self::Interrupted),
            other => Err(format!("invalid session turn status '{other}'")),
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

impl SessionActionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ContinueLastPartial => "continue_last_partial",
            Self::RetryLastTurn => "retry_last_turn",
            Self::ResetSession => "reset_session",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum SessionActionRequest {
    ContinueLastPartial {
        session_id: Uuid,
    },
    RetryLastTurn {
        session_id: Uuid,
    },
    ResetSession {
        session_id: Uuid,
    },
    CancelActiveTurn {
        session_id: Uuid,
        channel_id: String,
        session_key: String,
        #[serde(default)]
        expected_turn_id: Option<Uuid>,
        #[serde(default)]
        reason: Option<String>,
    },
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
    MessageBoundary,
    FileChange,
    Status,
    Error,
    TurnCompleted,
    Done,
}

impl StreamEventKindDto {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::MessageDelta => "message_delta",
            Self::MessageBoundary => "message_boundary",
            Self::FileChange => "file_change",
            Self::Status => "status",
            Self::Error => "error",
            Self::TurnCompleted => "turn_completed",
            Self::Done => "done",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamFileChangeStatusDto {
    Editing,
    Edited,
    Failed,
    Declined,
    Changed,
}

impl StreamFileChangeStatusDto {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Editing => "editing",
            Self::Edited => "edited",
            Self::Failed => "failed",
            Self::Declined => "declined",
            Self::Changed => "changed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamFileChangeDto {
    pub runtime: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    pub status: StreamFileChangeStatusDto,
    #[serde(default)]
    pub paths: Vec<String>,
    pub total_count: usize,
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
    #[serde(default)]
    pub file_change: Option<StreamFileChangeDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionTurnResponse {
    pub session_id: Uuid,
    pub turn_id: Uuid,
    pub status: SessionTurnStatus,
    pub assistant_text: String,
    #[serde(default)]
    pub error_code: Option<String>,
    #[serde(default)]
    pub error_text: Option<String>,
    pub runtime_skill_ids: Vec<String>,
    pub runtime_id: String,
    pub stream_events: Vec<StreamEventDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyGrantRequest {
    pub skill_alias: String,
    pub capability: String,
    pub scope: String,
    pub ttl_seconds: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyGrantResponse {
    pub grant_id: Uuid,
    pub skill_alias: String,
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
    pub conversation_ref: String,
    #[serde(default)]
    pub thread_ref: Option<String>,
    #[serde(default)]
    pub reply_to_ref: Option<String>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelRoutingProfile {
    Direct,
    Conversation,
    Thread,
    Outbound,
}

impl ChannelRoutingProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Conversation => "conversation",
            Self::Thread => "thread",
            Self::Outbound => "outbound",
        }
    }
}

impl FromStr for ChannelRoutingProfile {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "direct" => Ok(Self::Direct),
            "conversation" => Ok(Self::Conversation),
            "thread" => Ok(Self::Thread),
            "outbound" => Ok(Self::Outbound),
            other => Err(format!("invalid channel routing profile '{other}'")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelSessionBinding {
    #[default]
    Grant,
    Actor,
    Conversation,
    Thread,
    ConversationActor,
    ThreadActor,
}

impl ChannelSessionBinding {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Grant => "grant",
            Self::Actor => "actor",
            Self::Conversation => "conversation",
            Self::Thread => "thread",
            Self::ConversationActor => "conversation_actor",
            Self::ThreadActor => "thread_actor",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelTrigger {
    Dm,
    Command,
    Mention,
    ReplyToBot,
    ThreadContinuation,
    None,
}

impl ChannelTrigger {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Dm => "dm",
            Self::Command => "command",
            Self::Mention => "mention",
            Self::ReplyToBot => "reply_to_bot",
            Self::ThreadContinuation => "thread_continuation",
            Self::None => "none",
        }
    }
}

impl FromStr for ChannelTrigger {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "dm" => Ok(Self::Dm),
            "command" => Ok(Self::Command),
            "mention" => Ok(Self::Mention),
            "reply_to_bot" => Ok(Self::ReplyToBot),
            "thread_continuation" => Ok(Self::ThreadContinuation),
            "none" => Ok(Self::None),
            other => Err(format!("invalid channel trigger '{other}'")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelPairingStatus {
    Pending,
    Approved,
    Blocked,
    Expired,
}

impl ChannelPairingStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approved => "approved",
            Self::Blocked => "blocked",
            Self::Expired => "expired",
        }
    }
}

impl FromStr for ChannelPairingStatus {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "pending" => Ok(Self::Pending),
            "approved" => Ok(Self::Approved),
            "blocked" => Ok(Self::Blocked),
            "expired" => Ok(Self::Expired),
            other => Err(format!("invalid channel pairing status '{other}'")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelBindingView {
    pub channel_id: String,
    pub skill_alias: String,
    pub launch_mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelListResponse {
    pub bindings: Vec<ChannelBindingView>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelHealthStatus {
    Ok,
    Warning,
    Error,
}

impl ChannelHealthStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Warning => "warning",
            Self::Error => "error",
        }
    }
}

impl FromStr for ChannelHealthStatus {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "ok" => Ok(Self::Ok),
            "warning" => Ok(Self::Warning),
            "error" => Ok(Self::Error),
            other => Err(format!("invalid channel health status '{other}'")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelHealthCheck {
    pub code: String,
    pub status: ChannelHealthStatus,
    pub message: String,
    #[serde(default)]
    pub details: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelHealthReportRequest {
    pub channel_id: String,
    pub reporter_id: String,
    pub status: ChannelHealthStatus,
    #[serde(default)]
    pub checks: Vec<ChannelHealthCheck>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelHealthReportResponse {
    pub accepted: bool,
    pub channel_id: String,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelPairingView {
    pub pairing_id: Uuid,
    pub channel_id: String,
    #[serde(default)]
    pub sender_ref: Option<String>,
    #[serde(default)]
    pub conversation_ref: Option<String>,
    #[serde(default)]
    pub thread_ref: Option<String>,
    pub requested_profile: ChannelRoutingProfile,
    pub status: ChannelPairingStatus,
    #[serde(default)]
    pub label: Option<String>,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelPairingListResponse {
    pub pairings: Vec<ChannelPairingView>,
    #[serde(default)]
    pub grants: Vec<ChannelGrantView>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChannelPairingListParams {
    pub channel_id: Option<String>,
    #[serde(default)]
    pub status: Option<ChannelPairingStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelActorAuthorizeRequest {
    pub channel_id: String,
    pub sender_ref: String,
    pub conversation_ref: String,
    #[serde(default)]
    pub thread_ref: Option<String>,
    pub trigger: ChannelTrigger,
    #[serde(default)]
    pub session_binding: ChannelSessionBinding,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelActorAuthorizeResponse {
    pub authorized: bool,
    pub reason_code: String,
    #[serde(default)]
    pub grant_id: Option<Uuid>,
    #[serde(default)]
    pub session_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelPairingInviteRequest {
    pub channel_id: String,
    pub requested_profile: ChannelRoutingProfile,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub conversation_ref: Option<String>,
    #[serde(default)]
    pub thread_ref: Option<String>,
    #[serde(default)]
    pub expires_in_ms: Option<u64>,
    #[serde(default)]
    pub max_claims: Option<u32>,
    #[serde(default)]
    pub operator_actor: Option<ChannelOperatorActor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelPairingInviteResponse {
    pub pairing_id: Uuid,
    pub channel_id: String,
    pub token: String,
    pub requested_profile: ChannelRoutingProfile,
    pub expires_at: DateTime<Utc>,
    pub max_claims: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelOperatorActor {
    pub sender_ref: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelPairingClaimRequest {
    pub channel_id: String,
    pub token: String,
    pub sender_ref: String,
    pub conversation_ref: String,
    #[serde(default)]
    pub thread_ref: Option<String>,
    #[serde(default)]
    pub provider_metadata: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelPairingClaimOutcome {
    Approved,
    Expired,
    AlreadyClaimed,
    InvalidToken,
    ScopeMismatch,
}

impl ChannelPairingClaimOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Approved => "approved",
            Self::Expired => "expired",
            Self::AlreadyClaimed => "already_claimed",
            Self::InvalidToken => "invalid_token",
            Self::ScopeMismatch => "scope_mismatch",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelPairingClaimResponse {
    pub outcome: ChannelPairingClaimOutcome,
    #[serde(default)]
    pub grant_id: Option<Uuid>,
    #[serde(default)]
    pub reason_code: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelPairingApproveRequest {
    pub channel_id: String,
    #[serde(default)]
    pub pairing_id: Option<Uuid>,
    #[serde(default)]
    pub pairing_code: Option<String>,
    #[serde(default)]
    pub routing_profile: Option<ChannelRoutingProfile>,
    #[serde(default)]
    pub trust_tier: Option<TrustTier>,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelPairingBlockRequest {
    pub channel_id: String,
    #[serde(default)]
    pub pairing_id: Option<Uuid>,
    #[serde(default)]
    pub sender_ref: Option<String>,
    #[serde(default)]
    pub conversation_ref: Option<String>,
    #[serde(default)]
    pub thread_ref: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelPairingBlockResponse {
    #[serde(default)]
    pub grant: Option<ChannelGrantView>,
    pub blocked_pairing_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelGrantRevokeRequest {
    pub channel_id: String,
    pub grant_id: Uuid,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelGrantView {
    pub grant_id: Uuid,
    pub channel_id: String,
    #[serde(default)]
    pub sender_ref: Option<String>,
    #[serde(default)]
    pub conversation_ref: Option<String>,
    #[serde(default)]
    pub thread_ref: Option<String>,
    pub routing_profile: ChannelRoutingProfile,
    pub trust_tier: TrustTier,
    pub status: String,
    #[serde(default)]
    pub label: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelGrantResponse {
    pub grant: ChannelGrantView,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelGrantRevokeResponse {
    pub grant_id: Uuid,
    pub revoked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelAttachmentDescriptor {
    pub attachment_id: String,
    pub kind: String,
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub filename: Option<String>,
    #[serde(default)]
    pub size_bytes: Option<i64>,
    pub provider_file_ref: String,
    #[serde(default)]
    pub caption: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelAttachmentStatus {
    Staged,
    Rejected,
}

impl ChannelAttachmentStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Staged => "staged",
            Self::Rejected => "rejected",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelAttachmentStageResponse {
    pub channel_id: String,
    pub event_id: String,
    pub attachment_id: String,
    pub status: ChannelAttachmentStatus,
    pub size_bytes: i64,
    pub sha256: String,
    #[serde(default)]
    pub runtime_path: Option<String>,
    #[serde(default)]
    pub reason_code: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelAttachmentMissingReport {
    pub attachment_id: String,
    pub reason_code: String,
    #[serde(default)]
    pub reason_text: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelAttachmentFinalizeRequest {
    pub channel_id: String,
    pub event_id: String,
    pub worker_id: String,
    #[serde(default)]
    pub missing: Vec<ChannelAttachmentMissingReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelAttachmentFinalizeOutcome {
    Queued,
    AlreadyFinalized,
    NotReady,
}

impl ChannelAttachmentFinalizeOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::AlreadyFinalized => "already_finalized",
            Self::NotReady => "not_ready",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelAttachmentFinalizeResponse {
    pub channel_id: String,
    pub event_id: String,
    pub outcome: ChannelAttachmentFinalizeOutcome,
    #[serde(default)]
    pub session_id: Option<Uuid>,
    #[serde(default)]
    pub turn_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelInboundRequest {
    pub channel_id: String,
    pub event_id: String,
    pub sender_ref: String,
    pub conversation_ref: String,
    #[serde(default)]
    pub thread_ref: Option<String>,
    #[serde(default)]
    pub message_ref: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub attachments: Vec<ChannelAttachmentDescriptor>,
    #[serde(default)]
    pub reply_to_ref: Option<String>,
    pub trigger: ChannelTrigger,
    #[serde(default)]
    pub session_binding: ChannelSessionBinding,
    #[serde(default)]
    pub received_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub provider_metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelInboundResponse {
    pub outcome: ChannelInboundOutcome,
    #[serde(default)]
    pub reason_code: Option<String>,
    #[serde(default)]
    pub pairing_id: Option<Uuid>,
    #[serde(default)]
    pub pairing_code: Option<String>,
    #[serde(default)]
    pub turn_id: Option<Uuid>,
    #[serde(default)]
    pub session_id: Option<Uuid>,
    #[serde(default)]
    pub session_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelInboundOutcome {
    Queued,
    WaitingForAttachments,
    Duplicate,
    PendingApproval,
    Blocked,
    TriggerIgnored,
}

impl ChannelInboundOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::WaitingForAttachments => "waiting_for_attachments",
            Self::Duplicate => "duplicate",
            Self::PendingApproval => "pending_approval",
            Self::Blocked => "blocked",
            Self::TriggerIgnored => "trigger_ignored",
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
    #[serde(default)]
    pub session_id: Option<Uuid>,
    #[serde(default)]
    pub turn_id: Option<Uuid>,
    pub kind: StreamEventKindDto,
    #[serde(default)]
    pub lane: Option<StreamLaneDto>,
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub file_change: Option<StreamFileChangeDto>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelOutboxContentDto {
    pub text: String,
    #[serde(default = "default_channel_outbox_format_hint")]
    pub format_hint: String,
    #[serde(default)]
    pub attachments: Vec<ChannelOutboxAttachmentDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelOutboxAttachmentDto {
    pub attachment_id: String,
    pub path: String,
    #[serde(default)]
    pub filename: Option<String>,
    #[serde(default)]
    pub mime_type: Option<String>,
}

fn default_channel_outbox_format_hint() -> String {
    "plain".to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelOutboxDeliveryStatusDto {
    Pending,
    Leased,
    Delivered,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelOutboxAttemptStatusDto {
    Leased,
    Delivered,
    RetryableFailed,
    TerminalFailed,
    StaleRejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelOutboxReportOutcomeDto {
    Delivered,
    RetryableFailed,
    TerminalFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelOutboxPullRequest {
    pub channel_id: String,
    pub worker_id: String,
    #[serde(default)]
    pub conversation_ref: Option<String>,
    #[serde(default)]
    pub thread_ref: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub lease_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelOutboxDeliveryView {
    pub delivery_id: Uuid,
    pub attempt_id: Uuid,
    pub channel_id: String,
    pub conversation_ref: String,
    #[serde(default)]
    pub thread_ref: Option<String>,
    #[serde(default)]
    pub reply_to_ref: Option<String>,
    #[serde(default)]
    pub session_id: Option<Uuid>,
    #[serde(default)]
    pub turn_id: Option<Uuid>,
    pub content: ChannelOutboxContentDto,
    pub attempt_count: u32,
    pub lease_expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelOutboxPullResponse {
    pub deliveries: Vec<ChannelOutboxDeliveryView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelOutboxReportRequest {
    pub delivery_id: Uuid,
    pub attempt_id: Uuid,
    pub channel_id: String,
    pub worker_id: String,
    pub outcome: ChannelOutboxReportOutcomeDto,
    #[serde(default)]
    pub provider_receipt: Option<serde_json::Value>,
    #[serde(default)]
    pub error_code: Option<String>,
    #[serde(default)]
    pub error_text: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelOutboxReportResponse {
    pub delivery_id: Uuid,
    pub attempt_id: Uuid,
    pub accepted: bool,
    pub status: ChannelOutboxDeliveryStatusDto,
    pub attempt_status: ChannelOutboxAttemptStatusDto,
    #[serde(default)]
    pub next_attempt_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::ChannelOutboxContentDto;

    #[test]
    fn channel_outbox_content_defaults_to_plain_text() {
        let content: ChannelOutboxContentDto =
            serde_json::from_str(r#"{"text":"hello"}"#).expect("decode outbox content");

        assert_eq!(content.format_hint, "plain");
    }
}
