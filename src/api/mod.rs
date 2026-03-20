use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde_json::json;

use crate::{
    contracts::{
        AuditQueryParams, AuditQueryResponse, ChannelBindRequest, ChannelBindResponse,
        ChannelInboundRequest, ChannelInboundResponse, ChannelListResponse,
        ChannelPeerApproveRequest, ChannelPeerBlockRequest, ChannelPeerListParams,
        ChannelPeerListResponse, ChannelPeerResponse, ChannelStreamAckRequest,
        ChannelStreamAckResponse, ChannelStreamPullRequest, ChannelStreamPullResponse,
        DaemonInfoResponse, PolicyGrantRequest, PolicyGrantResponse, PolicyRevokeRequest,
        PolicyRevokeResponse, SessionActionRequest, SessionActionResponse, SessionHistoryRequest,
        SessionHistoryResponse, SessionOpenRequest, SessionOpenResponse, SessionTurnRequest,
        SessionTurnResponse, SkillInstallRequest, SkillInstallResponse, SkillListResponse,
        SkillToggleRequest, SkillToggleResponse,
    },
    kernel::{Kernel, KernelError},
};

#[derive(Clone)]
pub struct ApiState {
    pub kernel: Arc<Kernel>,
    pub daemon_info: DaemonInfoResponse,
}

pub fn build_router(kernel: Arc<Kernel>, daemon_info: DaemonInfoResponse) -> Router {
    let state = ApiState {
        kernel,
        daemon_info,
    };

    Router::new()
        .route("/health", get(health))
        .route("/v0/daemon/info", get(daemon_info_endpoint))
        .route("/v0/sessions/open", post(open_session))
        .route("/v0/sessions/history", post(session_history))
        .route("/v0/sessions/action", post(session_action))
        .route("/v0/sessions/turn", post(turn_session))
        .route("/v0/skills/install", post(install_skill))
        .route("/v0/skills/list", get(list_skills))
        .route("/v0/skills/enable", post(enable_skill))
        .route("/v0/skills/disable", post(disable_skill))
        .route("/v0/channels/bind", post(bind_channel))
        .route("/v0/channels/list", get(list_channels))
        .route("/v0/channels/peers", get(list_channel_peers))
        .route("/v0/channels/peers/approve", post(approve_channel_peer))
        .route("/v0/channels/peers/block", post(block_channel_peer))
        .route("/v0/channels/inbound", post(channel_inbound))
        .route("/v0/channels/stream/pull", post(channel_stream_pull))
        .route("/v0/channels/stream/ack", post(channel_stream_ack))
        .route("/v0/policy/grant", post(grant_policy))
        .route("/v0/policy/revoke", post(revoke_policy))
        .route("/v0/audit/query", get(query_audit))
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    Json(json!({"status": "ok", "service": "lionclawd"}))
}

async fn daemon_info_endpoint(State(state): State<ApiState>) -> Json<DaemonInfoResponse> {
    Json(state.daemon_info)
}

async fn open_session(
    State(state): State<ApiState>,
    Json(req): Json<SessionOpenRequest>,
) -> Result<Json<SessionOpenResponse>, ApiError> {
    let opened = state.kernel.open_session(req).await?;
    Ok(Json(opened))
}

async fn turn_session(
    State(state): State<ApiState>,
    Json(req): Json<SessionTurnRequest>,
) -> Result<Json<SessionTurnResponse>, ApiError> {
    let result = state.kernel.turn_session(req).await?;
    Ok(Json(result))
}

async fn session_history(
    State(state): State<ApiState>,
    Json(req): Json<SessionHistoryRequest>,
) -> Result<Json<SessionHistoryResponse>, ApiError> {
    let result = state.kernel.session_history(req).await?;
    Ok(Json(result))
}

async fn session_action(
    State(state): State<ApiState>,
    Json(req): Json<SessionActionRequest>,
) -> Result<Json<SessionActionResponse>, ApiError> {
    let result = state.kernel.session_action(req).await?;
    Ok(Json(result))
}

async fn install_skill(
    State(state): State<ApiState>,
    Json(req): Json<SkillInstallRequest>,
) -> Result<Json<SkillInstallResponse>, ApiError> {
    let result = state.kernel.install_skill(req).await?;
    Ok(Json(result))
}

async fn list_skills(State(state): State<ApiState>) -> Result<Json<SkillListResponse>, ApiError> {
    let result = state.kernel.list_skills().await?;
    Ok(Json(result))
}

async fn enable_skill(
    State(state): State<ApiState>,
    Json(req): Json<SkillToggleRequest>,
) -> Result<Json<SkillToggleResponse>, ApiError> {
    let result = state.kernel.enable_skill(req.skill_id).await?;
    Ok(Json(result))
}

async fn disable_skill(
    State(state): State<ApiState>,
    Json(req): Json<SkillToggleRequest>,
) -> Result<Json<SkillToggleResponse>, ApiError> {
    let result = state.kernel.disable_skill(req.skill_id).await?;
    Ok(Json(result))
}

async fn bind_channel(
    State(state): State<ApiState>,
    Json(req): Json<ChannelBindRequest>,
) -> Result<Json<ChannelBindResponse>, ApiError> {
    let result = state.kernel.bind_channel(req).await?;
    Ok(Json(result))
}

async fn list_channels(
    State(state): State<ApiState>,
) -> Result<Json<ChannelListResponse>, ApiError> {
    let result = state.kernel.list_channels().await?;
    Ok(Json(result))
}

async fn list_channel_peers(
    State(state): State<ApiState>,
    Query(params): Query<ChannelPeerListParams>,
) -> Result<Json<ChannelPeerListResponse>, ApiError> {
    let result = state.kernel.list_channel_peers(params.channel_id).await?;
    Ok(Json(result))
}

async fn approve_channel_peer(
    State(state): State<ApiState>,
    Json(req): Json<ChannelPeerApproveRequest>,
) -> Result<Json<ChannelPeerResponse>, ApiError> {
    let result = state.kernel.approve_channel_peer(req).await?;
    Ok(Json(result))
}

async fn block_channel_peer(
    State(state): State<ApiState>,
    Json(req): Json<ChannelPeerBlockRequest>,
) -> Result<Json<ChannelPeerResponse>, ApiError> {
    let result = state.kernel.block_channel_peer(req).await?;
    Ok(Json(result))
}

async fn channel_inbound(
    State(state): State<ApiState>,
    Json(req): Json<ChannelInboundRequest>,
) -> Result<Json<ChannelInboundResponse>, ApiError> {
    let result = state.kernel.ingest_channel_inbound(req).await?;
    Ok(Json(result))
}

async fn channel_stream_pull(
    State(state): State<ApiState>,
    Json(req): Json<ChannelStreamPullRequest>,
) -> Result<Json<ChannelStreamPullResponse>, ApiError> {
    let result = state.kernel.pull_channel_stream(req).await?;
    Ok(Json(result))
}

async fn channel_stream_ack(
    State(state): State<ApiState>,
    Json(req): Json<ChannelStreamAckRequest>,
) -> Result<Json<ChannelStreamAckResponse>, ApiError> {
    let result = state.kernel.ack_channel_stream(req).await?;
    Ok(Json(result))
}

async fn grant_policy(
    State(state): State<ApiState>,
    Json(req): Json<PolicyGrantRequest>,
) -> Result<Json<PolicyGrantResponse>, ApiError> {
    let result = state.kernel.grant_policy(req).await?;
    Ok(Json(result))
}

async fn revoke_policy(
    State(state): State<ApiState>,
    Json(req): Json<PolicyRevokeRequest>,
) -> Result<Json<PolicyRevokeResponse>, ApiError> {
    let result = state.kernel.revoke_policy(req.grant_id).await?;
    Ok(Json(result))
}

async fn query_audit(
    State(state): State<ApiState>,
    Query(params): Query<AuditQueryParams>,
) -> Result<Json<AuditQueryResponse>, ApiError> {
    let since = if let Some(raw) = params.since {
        Some(
            DateTime::parse_from_rfc3339(&raw)
                .map_err(|_| ApiError::bad_request("invalid 'since' timestamp; expected RFC3339"))?
                .with_timezone(&Utc),
        )
    } else {
        None
    };

    let response = state
        .kernel
        .query_audit(params.session_id, params.event_type, since, params.limit)
        .await?;

    Ok(Json(response))
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }
}

impl From<KernelError> for ApiError {
    fn from(value: KernelError) -> Self {
        match value {
            KernelError::BadRequest(message) => Self {
                status: StatusCode::BAD_REQUEST,
                message,
            },
            KernelError::NotFound(message) => Self {
                status: StatusCode::NOT_FOUND,
                message,
            },
            KernelError::Conflict(message) => Self {
                status: StatusCode::CONFLICT,
                message,
            },
            KernelError::Runtime(message) => Self {
                status: StatusCode::BAD_GATEWAY,
                message,
            },
            KernelError::RuntimeTimeout(message) => Self {
                status: StatusCode::GATEWAY_TIMEOUT,
                message,
            },
            KernelError::Internal(message) => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message,
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (self.status, Json(json!({"error": self.message}))).into_response()
    }
}
