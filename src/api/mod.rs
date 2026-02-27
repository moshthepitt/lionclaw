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
        AuditQueryParams, AuditQueryResponse, PolicyGrantRequest, PolicyGrantResponse,
        PolicyRevokeRequest, PolicyRevokeResponse, SessionOpenRequest, SessionOpenResponse,
        SessionTurnRequest, SessionTurnResponse, SkillInstallRequest, SkillInstallResponse,
        SkillListResponse, SkillToggleRequest, SkillToggleResponse,
    },
    kernel::{Kernel, KernelError},
};

#[derive(Clone)]
pub struct ApiState {
    pub kernel: Arc<Kernel>,
}

pub fn build_router(kernel: Arc<Kernel>) -> Router {
    let state = ApiState { kernel };

    Router::new()
        .route("/health", get(health))
        .route("/v0/sessions/open", post(open_session))
        .route("/v0/sessions/turn", post(turn_session))
        .route("/v0/skills/install", post(install_skill))
        .route("/v0/skills/list", get(list_skills))
        .route("/v0/skills/enable", post(enable_skill))
        .route("/v0/skills/disable", post(disable_skill))
        .route("/v0/policy/grant", post(grant_policy))
        .route("/v0/policy/revoke", post(revoke_policy))
        .route("/v0/audit/query", get(query_audit))
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    Json(json!({"status": "ok", "service": "lionclawd"}))
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
