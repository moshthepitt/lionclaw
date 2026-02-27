use std::{path::Path, str::FromStr, sync::Arc};

use chrono::{DateTime, Utc};
use serde_json::json;

use crate::contracts::{
    AuditEventView, AuditQueryResponse, PolicyGrantRequest, PolicyGrantResponse,
    PolicyRevokeResponse, RuntimeEventDto, SessionOpenRequest, SessionOpenResponse,
    SessionTurnRequest, SessionTurnResponse, SkillInstallRequest, SkillInstallResponse,
    SkillListResponse, SkillToggleResponse, SkillView,
};

use super::{
    audit::AuditLog,
    channels::{ChannelRegistry, LocalCliChannel},
    db::Db,
    error::KernelError,
    policy::{Capability, PolicyStore, Scope},
    runtime::{
        MockRuntimeAdapter, RuntimeCapabilityRequest, RuntimeCapabilityResult, RuntimeEvent,
        RuntimeRegistry, RuntimeSessionStartInput, RuntimeTurnInput,
    },
    selector::SkillSelector,
    sessions::SessionStore,
    skills::{SkillInstallInput, SkillStore},
};

pub struct Kernel {
    sessions: SessionStore,
    skills: SkillStore,
    selector: SkillSelector,
    policy: PolicyStore,
    runtime: RuntimeRegistry,
    channels: ChannelRegistry,
    audit: AuditLog,
}

impl Kernel {
    pub async fn new(db_path: &Path) -> anyhow::Result<Self> {
        let db = Db::connect_file(db_path).await?;
        let pool = db.pool();
        let runtime = RuntimeRegistry::new();
        let channels = ChannelRegistry::new();

        let kernel = Self {
            sessions: SessionStore::new(pool.clone()),
            skills: SkillStore::new(pool.clone()),
            selector: SkillSelector::new(),
            policy: PolicyStore::new(pool.clone()),
            runtime,
            channels,
            audit: AuditLog::new(pool),
        };

        kernel.bootstrap().await;
        Ok(kernel)
    }

    async fn bootstrap(&self) {
        let runtime = Arc::new(MockRuntimeAdapter);
        let channel = Arc::new(LocalCliChannel);
        self.runtime.register("mock", runtime).await;
        self.channels.register(channel).await;
    }

    pub async fn open_session(
        &self,
        req: SessionOpenRequest,
    ) -> Result<SessionOpenResponse, KernelError> {
        if req.channel_id.trim().is_empty() || req.peer_id.trim().is_empty() {
            return Err(KernelError::BadRequest(
                "channel_id and peer_id are required".to_string(),
            ));
        }

        let session = self
            .sessions
            .open(
                req.channel_id.trim().to_string(),
                req.peer_id.trim().to_string(),
                req.trust_tier,
            )
            .await
            .map_err(internal)?;

        self.audit
            .append(
                "session.open",
                Some(session.session_id),
                Some("api".to_string()),
                json!({"channel_id": session.channel_id, "peer_id": session.peer_id}),
            )
            .await
            .map_err(internal)?;

        Ok(SessionOpenResponse {
            session_id: session.session_id,
            channel_id: session.channel_id,
            peer_id: session.peer_id,
            trust_tier: session.trust_tier,
            created_at: session.created_at,
        })
    }

    pub async fn turn_session(
        &self,
        req: SessionTurnRequest,
    ) -> Result<SessionTurnResponse, KernelError> {
        if req.user_text.trim().is_empty() {
            return Err(KernelError::BadRequest("user_text is required".to_string()));
        }

        let session = self
            .sessions
            .get(req.session_id)
            .await
            .map_err(internal)?
            .ok_or_else(|| KernelError::NotFound("session not found".to_string()))?;

        let enabled_skills = self.skills.list_enabled().await.map_err(internal)?;
        let selected_skill_ids = self.selector.select(&req.user_text, &enabled_skills);
        let session_scope = Scope::Session(req.session_id);
        let any_scope = Scope::Any;

        let mut allowed_skills = Vec::new();
        for skill_id in selected_skill_ids {
            let allowed_for_session = self
                .policy
                .is_allowed(&skill_id, Capability::SkillUse, &session_scope)
                .await
                .map_err(internal)?;
            let allowed_globally = self
                .policy
                .is_allowed(&skill_id, Capability::SkillUse, &any_scope)
                .await
                .map_err(internal)?;

            if allowed_for_session || allowed_globally {
                allowed_skills.push(skill_id);
            }
        }

        let runtime_id = req.runtime_id.unwrap_or_else(|| "mock".to_string());
        let adapter = self.runtime.get(&runtime_id).await.ok_or_else(|| {
            KernelError::NotFound(format!("runtime adapter '{}' not found", runtime_id))
        })?;

        let handle = adapter
            .session_start(RuntimeSessionStartInput {
                session_id: session.session_id,
                working_dir: None,
                selected_skills: allowed_skills.clone(),
            })
            .await
            .map_err(|err| KernelError::Runtime(err.to_string()))?;

        let turn_output = adapter
            .turn(RuntimeTurnInput {
                runtime_session_id: handle.runtime_session_id.clone(),
                prompt: req.user_text.clone(),
                selected_skills: allowed_skills.clone(),
            })
            .await
            .map_err(|err| KernelError::Runtime(err.to_string()))?;

        let mut runtime_events = turn_output.events;
        if !turn_output.capability_requests.is_empty() {
            let capability_results = self
                .evaluate_capability_requests(
                    session.session_id,
                    &allowed_skills,
                    turn_output.capability_requests,
                )
                .await?;
            let followup = adapter
                .resolve_capability_requests(&handle, capability_results)
                .await
                .map_err(|err| KernelError::Runtime(err.to_string()))?;
            runtime_events.extend(followup);
        }

        adapter
            .close(&handle)
            .await
            .map_err(|err| KernelError::Runtime(err.to_string()))?;

        let _ = self
            .sessions
            .record_turn(session.session_id)
            .await
            .map_err(internal)?;

        let mut assistant = String::new();
        let mut event_views = Vec::new();
        for event in runtime_events {
            match event {
                RuntimeEvent::TextDelta(text) => {
                    if !assistant.is_empty() {
                        assistant.push('\n');
                    }
                    assistant.push_str(&text);
                    event_views.push(RuntimeEventDto {
                        kind: "text_delta".to_string(),
                        text,
                    });
                }
                RuntimeEvent::Status(text) => {
                    event_views.push(RuntimeEventDto {
                        kind: "status".to_string(),
                        text,
                    });
                }
                RuntimeEvent::Done => {
                    event_views.push(RuntimeEventDto {
                        kind: "done".to_string(),
                        text: "done".to_string(),
                    });
                }
                RuntimeEvent::Error(text) => {
                    event_views.push(RuntimeEventDto {
                        kind: "error".to_string(),
                        text,
                    });
                }
            }
        }

        self.audit
            .append(
                "session.turn",
                Some(session.session_id),
                Some("api".to_string()),
                json!({
                    "runtime_id": runtime_id,
                    "selected_skills": allowed_skills,
                    "prompt_len": req.user_text.len(),
                }),
            )
            .await
            .map_err(internal)?;

        Ok(SessionTurnResponse {
            session_id: session.session_id,
            assistant_text: assistant,
            selected_skills: allowed_skills,
            runtime_id,
            runtime_events: event_views,
        })
    }

    pub async fn install_skill(
        &self,
        req: SkillInstallRequest,
    ) -> Result<SkillInstallResponse, KernelError> {
        if req.source.trim().is_empty() {
            return Err(KernelError::BadRequest("source is required".to_string()));
        }

        let installed = self
            .skills
            .install(SkillInstallInput {
                source: req.source,
                reference: req.reference,
                hash: req.hash,
                skill_md: req.skill_md,
            })
            .await
            .map_err(internal)?;

        self.audit
            .append(
                "skill.install",
                None,
                Some("api".to_string()),
                json!({"skill_id": installed.skill_id, "name": installed.name, "hash": installed.hash}),
            )
            .await
            .map_err(internal)?;

        Ok(SkillInstallResponse {
            skill_id: installed.skill_id,
            name: installed.name,
            hash: installed.hash,
            enabled: installed.enabled,
        })
    }

    pub async fn list_skills(&self) -> Result<SkillListResponse, KernelError> {
        let skills = self
            .skills
            .list()
            .await
            .map_err(internal)?
            .into_iter()
            .map(to_skill_view)
            .collect::<Vec<_>>();

        Ok(SkillListResponse { skills })
    }

    pub async fn enable_skill(&self, skill_id: String) -> Result<SkillToggleResponse, KernelError> {
        let updated = self
            .skills
            .set_enabled(&skill_id, true)
            .await
            .map_err(internal)?
            .ok_or_else(|| KernelError::NotFound("skill not found".to_string()))?;

        self.audit
            .append(
                "skill.enable",
                None,
                Some("api".to_string()),
                json!({"skill_id": updated.skill_id}),
            )
            .await
            .map_err(internal)?;

        Ok(SkillToggleResponse {
            skill_id: updated.skill_id,
            enabled: updated.enabled,
        })
    }

    pub async fn disable_skill(
        &self,
        skill_id: String,
    ) -> Result<SkillToggleResponse, KernelError> {
        let updated = self
            .skills
            .set_enabled(&skill_id, false)
            .await
            .map_err(internal)?
            .ok_or_else(|| KernelError::NotFound("skill not found".to_string()))?;

        self.audit
            .append(
                "skill.disable",
                None,
                Some("api".to_string()),
                json!({"skill_id": updated.skill_id}),
            )
            .await
            .map_err(internal)?;

        Ok(SkillToggleResponse {
            skill_id: updated.skill_id,
            enabled: updated.enabled,
        })
    }

    pub async fn grant_policy(
        &self,
        req: PolicyGrantRequest,
    ) -> Result<PolicyGrantResponse, KernelError> {
        if self
            .skills
            .get(&req.skill_id)
            .await
            .map_err(internal)?
            .is_none()
        {
            return Err(KernelError::NotFound("skill not found".to_string()));
        }

        let capability = Capability::from_str(&req.capability)
            .map_err(|err| KernelError::BadRequest(format!("invalid capability: {}", err)))?;
        let scope = Scope::from_str(&req.scope)
            .map_err(|err| KernelError::BadRequest(format!("invalid scope: {}", err)))?;

        if let Some(ttl) = req.ttl_seconds {
            if ttl <= 0 {
                return Err(KernelError::BadRequest(
                    "ttl_seconds must be greater than zero".to_string(),
                ));
            }
        }

        let grant = self
            .policy
            .grant(req.skill_id, capability, scope, req.ttl_seconds)
            .await
            .map_err(internal)?;

        self.audit
            .append(
                "policy.grant",
                None,
                Some("api".to_string()),
                json!({
                    "grant_id": grant.grant_id,
                    "skill_id": grant.skill_id,
                    "capability": grant.capability.as_str(),
                    "scope": grant.scope.as_str(),
                    "created_at": grant.created_at,
                }),
            )
            .await
            .map_err(internal)?;

        Ok(PolicyGrantResponse {
            grant_id: grant.grant_id,
            skill_id: grant.skill_id,
            capability: grant.capability.as_str().to_string(),
            scope: grant.scope.as_str(),
            expires_at: grant.expires_at,
        })
    }

    pub async fn revoke_policy(
        &self,
        grant_id: uuid::Uuid,
    ) -> Result<PolicyRevokeResponse, KernelError> {
        let revoked = self.policy.revoke(grant_id).await.map_err(internal)?;

        self.audit
            .append(
                "policy.revoke",
                None,
                Some("api".to_string()),
                json!({"grant_id": grant_id, "revoked": revoked}),
            )
            .await
            .map_err(internal)?;

        Ok(PolicyRevokeResponse { grant_id, revoked })
    }

    pub async fn query_audit(
        &self,
        session_id: Option<uuid::Uuid>,
        event_type: Option<String>,
        since: Option<DateTime<Utc>>,
        limit: Option<usize>,
    ) -> Result<AuditQueryResponse, KernelError> {
        let events = self
            .audit
            .query(session_id, event_type, since, limit)
            .await
            .map_err(internal)?
            .into_iter()
            .map(|event| AuditEventView {
                event_id: event.event_id,
                event_type: event.event_type,
                session_id: event.session_id,
                actor: event.actor,
                details: event.details,
                timestamp: event.timestamp,
            })
            .collect::<Vec<_>>();

        Ok(AuditQueryResponse { events })
    }
}

fn to_skill_view(skill: super::skills::SkillRecord) -> SkillView {
    SkillView {
        skill_id: skill.skill_id,
        name: skill.name,
        description: skill.description,
        source: skill.source,
        reference: skill.reference,
        hash: skill.hash,
        enabled: skill.enabled,
        installed_at: skill.installed_at,
    }
}

fn internal(err: anyhow::Error) -> KernelError {
    KernelError::Internal(err.to_string())
}

impl Kernel {
    async fn evaluate_capability_requests(
        &self,
        session_id: uuid::Uuid,
        allowed_skill_ids: &[String],
        requests: Vec<RuntimeCapabilityRequest>,
    ) -> Result<Vec<RuntimeCapabilityResult>, KernelError> {
        let session_scope = Scope::Session(session_id);
        let any_scope = Scope::Any;

        let mut results = Vec::with_capacity(requests.len());
        for request in requests {
            let request_id = request.request_id.clone();
            let skill_id = request.skill_id.clone();
            let requested_scope_raw = request
                .scope
                .clone()
                .unwrap_or_else(|| session_scope.as_str());

            let mut decision_scope = session_scope.clone();
            let mut allowed = false;
            let mut reason = None;

            let skill_is_selected = allowed_skill_ids.iter().any(|value| value == &skill_id);
            if !skill_is_selected {
                reason = Some("skill is not selected for this turn".to_string());
            } else {
                match request.scope.as_deref() {
                    Some(raw_scope) => match Scope::from_str(raw_scope) {
                        Ok(parsed) => {
                            decision_scope = parsed;
                        }
                        Err(err) => {
                            reason = Some(format!("invalid scope from runtime: {}", err));
                        }
                    },
                    None => {
                        decision_scope = session_scope.clone();
                    }
                }
            }

            if reason.is_none() {
                let allowed_for_scope = self
                    .policy
                    .is_allowed(&skill_id, request.capability, &decision_scope)
                    .await
                    .map_err(internal)?;
                let allowed_globally = self
                    .policy
                    .is_allowed(&skill_id, request.capability, &any_scope)
                    .await
                    .map_err(internal)?;

                allowed = allowed_for_scope || allowed_globally;
                if !allowed {
                    reason = Some("policy denied capability request".to_string());
                }
            }

            self.audit
                .append(
                    "capability.request",
                    Some(session_id),
                    Some("kernel".to_string()),
                    json!({
                        "request_id": request_id,
                        "skill_id": skill_id,
                        "capability": request.capability.as_str(),
                        "requested_scope": requested_scope_raw,
                        "effective_scope": decision_scope.as_str(),
                        "allowed": allowed,
                        "reason": reason.clone(),
                        "payload": request.payload,
                    }),
                )
                .await
                .map_err(internal)?;

            results.push(RuntimeCapabilityResult {
                request_id,
                allowed,
                reason,
                output: serde_json::Value::Null,
            });
        }

        Ok(results)
    }
}
