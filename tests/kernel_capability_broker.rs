use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use lionclaw::{
    contracts::{
        ChannelBindRequest, ChannelPeerApproveRequest, JobCreateRequest, PolicyGrantRequest,
        SessionOpenRequest, SessionTurnRequest, SkillInstallRequest, StreamEventKindDto, TrustTier,
    },
    kernel::{
        policy::Capability,
        runtime::{
            RuntimeAdapter, RuntimeAdapterInfo, RuntimeCapabilityRequest, RuntimeCapabilityResult,
            RuntimeEvent, RuntimeEventSender, RuntimeSessionHandle, RuntimeSessionStartInput,
            RuntimeTurnInput, RuntimeTurnResult,
        },
        InboundChannelText, Kernel,
    },
};
use serde_json::{json, Value};
use tempfile::TempDir;
use uuid::Uuid;

#[tokio::test]
async fn fs_read_capability_executes_through_kernel_broker() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");
    let read_target = env.read_target();
    env.write_read_target(&read_target, "lionclaw broker fs read test content");

    kernel
        .register_runtime_adapter(
            "single-capability",
            Arc::new(SingleCapabilityRuntimeAdapter::new(
                Capability::FsRead,
                json!({"path": read_target.to_string_lossy().to_string()}),
            )),
        )
        .await;

    let (session_id, skill_id) = prepare_session_with_skill(
        &kernel,
        "peer-cap-broker-fs",
        "broker-fs-read",
        "Capability broker file read skill",
    )
    .await;
    grant_capability(&kernel, &skill_id, "fs.read").await;

    let response = kernel
        .turn_session(SessionTurnRequest {
            session_id,
            user_text: "please run broker-fs-read now".to_string(),
            runtime_id: Some("single-capability".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn should succeed");

    assert!(
        response.stream_events.iter().any(|event| {
            event.kind == StreamEventKindDto::Status
                && event
                    .text
                    .as_deref()
                    .is_some_and(|text| text.contains("capability:req-1:granted"))
        }),
        "capability should be granted after broker execution"
    );
    assert!(
        response.stream_events.iter().any(|event| {
            event.kind == StreamEventKindDto::Status
                && event
                    .text
                    .as_deref()
                    .is_some_and(|text| text.contains("lionclaw broker fs read test content"))
        }),
        "runtime should receive fs.read output content"
    );

    let details = latest_capability_result(&kernel, session_id).await;
    assert_eq!(details["allowed"].as_bool(), Some(true));
    assert_eq!(
        details["output_summary"]["bytes"].as_u64(),
        Some("lionclaw broker fs read test content".len() as u64)
    );
}

#[tokio::test]
async fn invalid_capability_payload_is_denied_by_broker() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");
    kernel
        .register_runtime_adapter(
            "single-capability",
            Arc::new(SingleCapabilityRuntimeAdapter::new(
                Capability::FsRead,
                Value::Null,
            )),
        )
        .await;

    let (session_id, skill_id) = prepare_session_with_skill(
        &kernel,
        "peer-cap-broker-invalid",
        "broker-invalid-payload",
        "Capability broker invalid payload skill",
    )
    .await;
    grant_capability(&kernel, &skill_id, "fs.read").await;

    let response = kernel
        .turn_session(SessionTurnRequest {
            session_id,
            user_text: "run broker-invalid-payload now".to_string(),
            runtime_id: Some("single-capability".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn should complete with denied capability result");

    assert!(
        response.stream_events.iter().any(|event| {
            event.kind == StreamEventKindDto::Status
                && event
                    .text
                    .as_deref()
                    .is_some_and(|text| text.contains("capability:req-1:denied"))
        }),
        "invalid payload should be denied"
    );
    assert!(
        response.stream_events.iter().any(|event| {
            event.kind == StreamEventKindDto::Status
                && event
                    .text
                    .as_deref()
                    .is_some_and(|text| text.contains("broker execution failed"))
        }),
        "denied event should include broker failure reason"
    );

    let details = latest_capability_result(&kernel, session_id).await;
    assert_eq!(details["allowed"].as_bool(), Some(false));
    assert!(
        details["reason"]
            .as_str()
            .expect("reason present")
            .contains("broker execution failed"),
        "audit should capture broker deny reason"
    );
}

#[tokio::test]
async fn runtime_cannot_override_kernel_selected_scope() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");
    let (session_id, skill_id) = prepare_session_with_skill(
        &kernel,
        "peer-cap-broker-scope",
        "broker-scope-guard",
        "Capability broker scope guard skill",
    )
    .await;
    let created = kernel
        .create_job(JobCreateRequest {
            name: "scope guard".to_string(),
            runtime_id: "mock".to_string(),
            schedule: lionclaw::contracts::JobScheduleDto::Once {
                run_at: Utc::now() + ChronoDuration::minutes(10),
            },
            prompt_text: "scheduled scope guard".to_string(),
            skill_ids: vec![skill_id.clone()],
            allow_capabilities: vec!["fs.read".to_string()],
            delivery: None,
            retry_attempts: Some(0),
        })
        .await
        .expect("create scoped job");

    kernel
        .register_runtime_adapter(
            "single-capability",
            Arc::new(SingleCapabilityRuntimeAdapter::with_scope(
                Capability::FsRead,
                format!("job:{}", created.job.job_id),
                json!({"path": "README.md"}),
            )),
        )
        .await;

    let response = kernel
        .turn_session(SessionTurnRequest {
            session_id,
            user_text: "attempt a scoped capability override".to_string(),
            runtime_id: Some("single-capability".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn should complete with denied capability result");

    assert!(
        response.stream_events.iter().any(|event| {
            event.kind == StreamEventKindDto::Status
                && event
                    .text
                    .as_deref()
                    .is_some_and(|text| text.contains("capability:req-1:denied"))
        }),
        "scope override should be denied"
    );

    let details = latest_capability_result(&kernel, session_id).await;
    assert_eq!(details["allowed"].as_bool(), Some(false));
    assert_eq!(
        details["reason"].as_str(),
        Some("runtime cannot override policy scope")
    );
}

#[tokio::test]
async fn channel_send_capability_uses_session_channel_defaults() {
    let env = TestEnv::new();
    let kernel = Kernel::new(&env.db_path()).await.expect("kernel init");
    kernel
        .register_runtime_adapter(
            "single-capability",
            Arc::new(SingleCapabilityRuntimeAdapter::new(
                Capability::ChannelSend,
                json!({"content": "hello from capability broker"}),
            )),
        )
        .await;

    let peer_id = "peer-cap-broker-channel";
    let (session_id, skill_id) = prepare_session_with_skill(
        &kernel,
        peer_id,
        "broker-channel-send",
        "Capability broker channel send skill",
    )
    .await;
    kernel
        .bind_channel(ChannelBindRequest {
            channel_id: "local-cli".to_string(),
            skill_id: skill_id.clone(),
            enabled: Some(true),
            config: None,
        })
        .await
        .expect("bind local-cli channel to skill");
    let _ = kernel
        .process_inbound_channel_text(InboundChannelText {
            channel_id: "local-cli".to_string(),
            peer_id: peer_id.to_string(),
            text: "seed pairing".to_string(),
            session_id: None,
            runtime_id: Some("single-capability".to_string()),
            update_id: Some(9101),
            external_message_id: Some("cap-broker-9101".to_string()),
        })
        .await
        .expect("seed pairing state");
    let peers = kernel
        .list_channel_peers(Some("local-cli".to_string()))
        .await
        .expect("list peers");
    let pairing_code = peers
        .peers
        .iter()
        .find(|peer| peer.peer_id == peer_id)
        .and_then(|peer| peer.pairing_code.clone())
        .expect("pending peer pairing code");
    kernel
        .approve_channel_peer(ChannelPeerApproveRequest {
            channel_id: "local-cli".to_string(),
            peer_id: peer_id.to_string(),
            pairing_code,
            trust_tier: Some(TrustTier::Main),
        })
        .await
        .expect("approve peer");
    grant_capability(&kernel, &skill_id, "channel.send").await;

    let response = kernel
        .turn_session(SessionTurnRequest {
            session_id,
            user_text: "run broker-channel-send now".to_string(),
            runtime_id: Some("single-capability".to_string()),
            runtime_working_dir: None,
            runtime_timeout_ms: None,
            runtime_env_passthrough: None,
        })
        .await
        .expect("turn should succeed");

    assert!(
        response.stream_events.iter().any(|event| {
            event.kind == StreamEventKindDto::Status
                && event
                    .text
                    .as_deref()
                    .is_some_and(|text| text.contains("capability:req-1:granted"))
        }),
        "channel send should be granted after broker execution"
    );

    let details = latest_capability_result(&kernel, session_id).await;
    assert_eq!(details["allowed"].as_bool(), Some(true));
    assert_eq!(
        details["output_summary"]["channel_id"].as_str(),
        Some("local-cli"),
        "channel broker should default to the session channel"
    );
    assert_eq!(
        details["output_summary"]["conversation_ref"].as_str(),
        Some(peer_id),
        "channel broker should default to session peer id"
    );
    let message_ids = details["output_summary"]["message_ids"]
        .as_array()
        .expect("message ids");
    assert!(
        !message_ids.is_empty(),
        "queued outbound message ids must exist"
    );
    assert!(
        message_ids.iter().all(|entry| entry.as_str().is_some()),
        "queued message ids should serialize as strings"
    );
}

struct SingleCapabilityRuntimeAdapter {
    capability: Capability,
    scope: Option<String>,
    payload: Value,
}

impl SingleCapabilityRuntimeAdapter {
    fn new(capability: Capability, payload: Value) -> Self {
        Self {
            capability,
            scope: None,
            payload,
        }
    }

    fn with_scope(capability: Capability, scope: String, payload: Value) -> Self {
        Self {
            capability,
            scope: Some(scope),
            payload,
        }
    }
}

#[async_trait]
impl RuntimeAdapter for SingleCapabilityRuntimeAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "single-capability".to_string(),
            version: "0.1".to_string(),
            healthy: true,
        }
    }

    async fn session_start(
        &self,
        _input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("single-capability-{}", Uuid::new_v4()),
            resumes_existing_session: false,
        })
    }

    async fn turn(
        &self,
        input: RuntimeTurnInput,
        events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
        let _ = events.send(RuntimeEvent::Status {
            code: None,
            text: "single capability runtime started turn".to_string(),
        });
        let mut capability_requests = Vec::new();

        if let Some(skill_id) = input.selected_skills.first() {
            capability_requests.push(RuntimeCapabilityRequest {
                request_id: "req-1".to_string(),
                skill_id: skill_id.clone(),
                capability: self.capability,
                scope: self.scope.clone(),
                payload: self.payload.clone(),
            });
        } else {
            let _ = events.send(RuntimeEvent::Status {
                code: None,
                text: "single capability runtime had no selected skill".to_string(),
            });
            let _ = events.send(RuntimeEvent::Done);
        }

        Ok(RuntimeTurnResult {
            capability_requests,
        })
    }

    async fn resolve_capability_requests(
        &self,
        _handle: &RuntimeSessionHandle,
        results: Vec<RuntimeCapabilityResult>,
        events: RuntimeEventSender,
    ) -> Result<()> {
        for result in results {
            let verdict = if result.allowed { "granted" } else { "denied" };
            let _ = events.send(RuntimeEvent::Status {
                code: None,
                text: format!("capability:{}:{}", result.request_id, verdict),
            });
            let _ = events.send(RuntimeEvent::Status {
                code: None,
                text: format!("capability:{}:output:{}", result.request_id, result.output),
            });
            if let Some(reason) = result.reason {
                let _ = events.send(RuntimeEvent::Status {
                    code: None,
                    text: format!("capability:{}:reason:{}", result.request_id, reason),
                });
            }
        }
        let _ = events.send(RuntimeEvent::Done);
        Ok(())
    }

    async fn cancel(&self, _handle: &RuntimeSessionHandle, _reason: Option<String>) -> Result<()> {
        Ok(())
    }

    async fn close(&self, _handle: &RuntimeSessionHandle) -> Result<()> {
        Ok(())
    }
}

async fn prepare_session_with_skill(
    kernel: &Kernel,
    peer_id: &str,
    skill_name: &str,
    skill_description: &str,
) -> (Uuid, String) {
    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "local-cli".to_string(),
            peer_id: peer_id.to_string(),
            trust_tier: TrustTier::Main,
            history_policy: None,
        })
        .await
        .expect("open session");

    let skill = kernel
        .install_skill(SkillInstallRequest {
            source: format!("local/{skill_name}"),
            reference: Some("main".to_string()),
            hash: Some(format!("{skill_name}-hash")),
            skill_md: Some(format!(
                "---\nname: {skill_name}\ndescription: {skill_description}\n---"
            )),
            snapshot_path: None,
        })
        .await
        .expect("install skill");

    kernel
        .enable_skill(skill.skill_id.clone())
        .await
        .expect("enable skill");
    grant_capability(kernel, &skill.skill_id, "skill.use").await;

    (session.session_id, skill.skill_id)
}

async fn grant_capability(kernel: &Kernel, skill_id: &str, capability: &str) {
    kernel
        .grant_policy(PolicyGrantRequest {
            skill_id: skill_id.to_string(),
            capability: capability.to_string(),
            scope: "*".to_string(),
            ttl_seconds: None,
        })
        .await
        .expect("grant capability");
}

async fn latest_capability_result(kernel: &Kernel, session_id: Uuid) -> Value {
    let audit = kernel
        .query_audit(
            Some(session_id),
            Some("capability.result".to_string()),
            None,
            Some(1),
        )
        .await
        .expect("query capability results");

    audit
        .events
        .first()
        .expect("capability result event")
        .details
        .clone()
}

struct TestEnv {
    temp_dir: TempDir,
}

impl TestEnv {
    fn new() -> Self {
        Self {
            temp_dir: tempfile::tempdir().expect("create temp dir"),
        }
    }

    fn db_path(&self) -> PathBuf {
        self.temp_dir.path().join("lionclaw.db")
    }

    fn read_target(&self) -> PathBuf {
        std::env::current_dir()
            .expect("current dir")
            .join("target")
            .join("lionclaw-capability-tests")
            .join(format!("{}.txt", Uuid::new_v4()))
    }

    fn write_read_target(&self, path: &Path, content: &str) {
        let parent = path.parent().expect("parent");
        std::fs::create_dir_all(parent).expect("create parent");
        std::fs::write(path, content).expect("write file");
    }
}
