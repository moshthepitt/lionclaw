use anyhow::Result;
use async_trait::async_trait;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::kernel::{
    policy::Capability,
    runtime::{
        HiddenTurnSupport, RuntimeAdapter, RuntimeAdapterInfo, RuntimeCapabilityRequest,
        RuntimeCapabilityResult, RuntimeEvent, RuntimeEventSender, RuntimeMessageLane,
        RuntimeSessionHandle, RuntimeSessionStartInput, RuntimeTurnInput, RuntimeTurnResult,
    },
};

pub struct MockRuntimeAdapter;

#[async_trait]
impl RuntimeAdapter for MockRuntimeAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "mock".to_string(),
            version: "0.1".to_string(),
            healthy: true,
        }
    }

    fn hidden_turn_support(&self) -> HiddenTurnSupport {
        HiddenTurnSupport::SideEffectFree
    }

    async fn session_start(
        &self,
        _input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("mock-{}", Uuid::new_v4()),
        })
    }

    async fn turn(
        &self,
        input: RuntimeTurnInput,
        events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
        let _ = events.send(RuntimeEvent::Status {
            code: None,
            text: "mock runtime started turn".to_string(),
        });

        let skill_context = if input.selected_skills.is_empty() {
            "no skill context selected".to_string()
        } else {
            format!("selected skills: {}", input.selected_skills.join(", "))
        };

        let _ = events.send(RuntimeEvent::MessageDelta {
            lane: RuntimeMessageLane::Answer,
            text: format!("[mock] {} | prompt: {}", skill_context, input.prompt),
        });

        let mut capability_requests = Vec::new();
        if let Some(skill_id) = input.selected_skills.first() {
            for (index, (capability, payload)) in parse_capability_markers(&input.prompt)
                .into_iter()
                .enumerate()
            {
                capability_requests.push(RuntimeCapabilityRequest {
                    request_id: format!("req-{}", index + 1),
                    skill_id: skill_id.clone(),
                    capability,
                    scope: None,
                    payload,
                });
            }
        }

        if capability_requests.is_empty() {
            let _ = events.send(RuntimeEvent::Done);
        } else {
            let _ = events.send(RuntimeEvent::Status {
                code: None,
                text: format!(
                    "mock runtime requested {} capability checks",
                    capability_requests.len()
                ),
            });
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

fn parse_capability_markers(prompt: &str) -> Vec<(Capability, Value)> {
    let mut requested = Vec::new();
    for (marker, capability, payload) in [
        (
            "[cap:fs.read]",
            Capability::FsRead,
            json!({"path": "README.md"}),
        ),
        (
            "[cap:fs.write]",
            Capability::FsWrite,
            json!({"path": "target/lionclaw-mock-write.txt", "content": "mock runtime write"}),
        ),
        (
            "[cap:net.egress]",
            Capability::NetEgress,
            json!({"method": "GET", "url": "https://example.invalid"}),
        ),
        (
            "[cap:secret.request]",
            Capability::SecretRequest,
            json!({"name": "example-secret"}),
        ),
        (
            "[cap:channel.send]",
            Capability::ChannelSend,
            json!({"content": "mock runtime channel send"}),
        ),
        (
            "[cap:scheduler.run]",
            Capability::SchedulerRun,
            json!({"job": "mock-job"}),
        ),
    ] {
        if prompt.contains(marker) {
            requested.push((capability, payload));
        }
    }
    requested
}
