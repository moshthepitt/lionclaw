use std::sync::Arc;

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
    error::KernelError,
    policy::PolicyStore,
    runtime::{
        MockRuntimeAdapter, RuntimeEvent, RuntimeRegistry, RuntimeSessionStartInput,
        RuntimeTurnInput,
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
    pub async fn new() -> Self {
        let runtime = RuntimeRegistry::new();
        let channels = ChannelRegistry::new();

        let kernel = Self {
            sessions: SessionStore::new(),
            skills: SkillStore::new(),
            selector: SkillSelector::new(),
            policy: PolicyStore::new(),
            runtime,
            channels,
            audit: AuditLog::new(),
        };

        kernel.bootstrap().await;
        kernel
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
            .await;

        self.audit
            .append(
                "session.open",
                Some(session.session_id),
                Some("api".to_string()),
                json!({"channel_id": session.channel_id, "peer_id": session.peer_id}),
            )
            .await;

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
            .ok_or_else(|| KernelError::NotFound("session not found".to_string()))?;

        let enabled_skills = self.skills.list_enabled().await;
        let selected_skill_ids = self.selector.select(&req.user_text, &enabled_skills);
        let scope = format!("session:{}", req.session_id);

        let mut allowed_skills = Vec::new();
        for skill_id in selected_skill_ids {
            if self.policy.is_allowed(&skill_id, "skill.use", &scope).await
                || self.policy.is_allowed(&skill_id, "skill.use", "*").await
            {
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

        let runtime_events = adapter
            .turn(RuntimeTurnInput {
                runtime_session_id: handle.runtime_session_id.clone(),
                prompt: req.user_text.clone(),
                selected_skills: allowed_skills.clone(),
            })
            .await
            .map_err(|err| KernelError::Runtime(err.to_string()))?;

        adapter
            .close(&handle)
            .await
            .map_err(|err| KernelError::Runtime(err.to_string()))?;

        let _ = self.sessions.record_turn(session.session_id).await;

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
            .await;

        Ok(SessionTurnResponse {
            session_id: session.session_id,
            assistant_text: assistant,
            selected_skills: self.selector.select(&req.user_text, &enabled_skills),
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
            .await;

        self.audit
            .append(
                "skill.install",
                None,
                Some("api".to_string()),
                json!({"skill_id": installed.skill_id, "name": installed.name, "hash": installed.hash}),
            )
            .await;

        Ok(SkillInstallResponse {
            skill_id: installed.skill_id,
            name: installed.name,
            hash: installed.hash,
            enabled: installed.enabled,
        })
    }

    pub async fn list_skills(&self) -> SkillListResponse {
        let skills = self
            .skills
            .list()
            .await
            .into_iter()
            .map(to_skill_view)
            .collect::<Vec<_>>();

        SkillListResponse { skills }
    }

    pub async fn enable_skill(&self, skill_id: String) -> Result<SkillToggleResponse, KernelError> {
        let updated = self
            .skills
            .set_enabled(&skill_id, true)
            .await
            .ok_or_else(|| KernelError::NotFound("skill not found".to_string()))?;

        self.audit
            .append(
                "skill.enable",
                None,
                Some("api".to_string()),
                json!({"skill_id": updated.skill_id}),
            )
            .await;

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
            .ok_or_else(|| KernelError::NotFound("skill not found".to_string()))?;

        self.audit
            .append(
                "skill.disable",
                None,
                Some("api".to_string()),
                json!({"skill_id": updated.skill_id}),
            )
            .await;

        Ok(SkillToggleResponse {
            skill_id: updated.skill_id,
            enabled: updated.enabled,
        })
    }

    pub async fn grant_policy(
        &self,
        req: PolicyGrantRequest,
    ) -> Result<PolicyGrantResponse, KernelError> {
        if self.skills.get(&req.skill_id).await.is_none() {
            return Err(KernelError::NotFound("skill not found".to_string()));
        }

        if req.capability.trim().is_empty() || req.scope.trim().is_empty() {
            return Err(KernelError::BadRequest(
                "capability and scope are required".to_string(),
            ));
        }

        let grant = self
            .policy
            .grant(req.skill_id, req.capability, req.scope, req.ttl_seconds)
            .await;

        self.audit
            .append(
                "policy.grant",
                None,
                Some("api".to_string()),
                json!({
                    "grant_id": grant.grant_id,
                    "skill_id": grant.skill_id,
                    "capability": grant.capability,
                    "scope": grant.scope,
                    "created_at": grant.created_at,
                }),
            )
            .await;

        Ok(PolicyGrantResponse {
            grant_id: grant.grant_id,
            skill_id: grant.skill_id,
            capability: grant.capability,
            scope: grant.scope,
            expires_at: grant.expires_at,
        })
    }

    pub async fn revoke_policy(
        &self,
        grant_id: uuid::Uuid,
    ) -> Result<PolicyRevokeResponse, KernelError> {
        let revoked = self.policy.revoke(grant_id).await;

        if !revoked {
            return Err(KernelError::NotFound("grant not found".to_string()));
        }

        self.audit
            .append(
                "policy.revoke",
                None,
                Some("api".to_string()),
                json!({"grant_id": grant_id}),
            )
            .await;

        Ok(PolicyRevokeResponse { grant_id, revoked })
    }

    pub async fn query_audit(
        &self,
        session_id: Option<uuid::Uuid>,
        event_type: Option<String>,
        since: Option<DateTime<Utc>>,
        limit: Option<usize>,
    ) -> AuditQueryResponse {
        let events = self
            .audit
            .query(session_id, event_type, since, limit)
            .await
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

        AuditQueryResponse { events }
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
