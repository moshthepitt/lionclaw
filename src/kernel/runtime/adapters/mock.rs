use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;

use crate::kernel::{
    policy::Capability,
    runtime::{
        RuntimeAdapter, RuntimeAdapterInfo, RuntimeCapabilityRequest, RuntimeCapabilityResult,
        RuntimeEvent, RuntimeSessionHandle, RuntimeSessionStartInput, RuntimeTurnInput,
        RuntimeTurnOutput,
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

    async fn session_start(
        &self,
        _input: RuntimeSessionStartInput,
    ) -> Result<RuntimeSessionHandle> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("mock-{}", Uuid::new_v4()),
        })
    }

    async fn turn(&self, input: RuntimeTurnInput) -> Result<RuntimeTurnOutput> {
        let mut events = Vec::new();
        events.push(RuntimeEvent::Status(
            "mock runtime started turn".to_string(),
        ));

        let skill_context = if input.selected_skills.is_empty() {
            "no skill context selected".to_string()
        } else {
            format!("selected skills: {}", input.selected_skills.join(", "))
        };

        events.push(RuntimeEvent::TextDelta(format!(
            "[mock] {} | prompt: {}",
            skill_context, input.prompt
        )));

        let mut capability_requests = Vec::new();
        if let Some(skill_id) = input.selected_skills.first() {
            for (index, capability) in parse_capability_markers(&input.prompt)
                .into_iter()
                .enumerate()
            {
                capability_requests.push(RuntimeCapabilityRequest {
                    request_id: format!("req-{}", index + 1),
                    skill_id: skill_id.clone(),
                    capability,
                    scope: None,
                    payload: Value::Null,
                });
            }
        }

        if capability_requests.is_empty() {
            events.push(RuntimeEvent::Done);
        } else {
            events.push(RuntimeEvent::Status(format!(
                "mock runtime requested {} capability checks",
                capability_requests.len()
            )));
        }

        Ok(RuntimeTurnOutput {
            events,
            capability_requests,
        })
    }

    async fn resolve_capability_requests(
        &self,
        _handle: &RuntimeSessionHandle,
        results: Vec<RuntimeCapabilityResult>,
    ) -> Result<Vec<RuntimeEvent>> {
        let mut events = Vec::with_capacity(results.len() + 1);
        for result in results {
            let verdict = if result.allowed { "granted" } else { "denied" };
            events.push(RuntimeEvent::Status(format!(
                "capability:{}:{}",
                result.request_id, verdict
            )));
            if let Some(reason) = result.reason {
                events.push(RuntimeEvent::Status(format!(
                    "capability:{}:reason:{}",
                    result.request_id, reason
                )));
            }
        }
        events.push(RuntimeEvent::Done);
        Ok(events)
    }

    async fn cancel(&self, _handle: &RuntimeSessionHandle, _reason: Option<String>) -> Result<()> {
        Ok(())
    }

    async fn close(&self, _handle: &RuntimeSessionHandle) -> Result<()> {
        Ok(())
    }
}

fn parse_capability_markers(prompt: &str) -> Vec<Capability> {
    let mut requested = Vec::new();
    for (marker, capability) in [
        ("[cap:fs.read]", Capability::FsRead),
        ("[cap:fs.write]", Capability::FsWrite),
        ("[cap:net.egress]", Capability::NetEgress),
        ("[cap:secret.request]", Capability::SecretRequest),
        ("[cap:channel.send]", Capability::ChannelSend),
        ("[cap:scheduler.run]", Capability::SchedulerRun),
    ] {
        if prompt.contains(marker) {
            requested.push(capability);
        }
    }
    requested
}
