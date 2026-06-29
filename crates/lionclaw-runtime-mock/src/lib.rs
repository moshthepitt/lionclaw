#![cfg_attr(
    not(test),
    warn(
        clippy::allow_attributes_without_reason,
        clippy::clone_on_ref_ptr,
        clippy::expect_used,
        clippy::future_not_send,
        clippy::get_unwrap,
        clippy::indexing_slicing,
        clippy::large_futures,
        clippy::large_stack_arrays,
        clippy::large_types_passed_by_value,
        clippy::let_underscore_must_use,
        clippy::mutex_atomic,
        clippy::mutex_integer,
        clippy::panic,
        clippy::panic_in_result_fn,
        clippy::pathbuf_init_then_push,
        clippy::rc_buffer,
        clippy::rc_mutex,
        clippy::redundant_clone,
        clippy::same_name_method,
        clippy::significant_drop_in_scrutinee,
        clippy::significant_drop_tightening,
        clippy::uninlined_format_args,
        clippy::unused_result_ok,
        clippy::unwrap_in_result,
        clippy::unwrap_used,
        reason = "production code follows LionClaw's strict Clippy profile; tests keep fail-fast ergonomics"
    )
)]

use anyhow::Result;
use async_trait::async_trait;
use serde_json::{json, Value};
use uuid::Uuid;

use lionclaw_runtime_api::{
    Capability, HiddenTurnSupport, RuntimeAdapter, RuntimeAdapterInfo, RuntimeCapabilityRequest,
    RuntimeCapabilityResult, RuntimeControlExecution, RuntimeControlOutcome, RuntimeEvent,
    RuntimeEventSender, RuntimeMessageLane, RuntimeSessionHandle, RuntimeSessionStartInput,
    RuntimeTurnInput, RuntimeTurnJournalSender, RuntimeTurnResult, TurnEvent,
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
            resumes_existing_session: false,
        })
    }

    async fn turn(
        &self,
        input: RuntimeTurnInput,
        journal: RuntimeTurnJournalSender,
    ) -> Result<RuntimeTurnResult> {
        drop(journal.send(TurnEvent::canonical(RuntimeEvent::Status {
            code: None,
            text: "mock runtime started turn".to_string(),
        })));

        let skill_context = if input.runtime_skill_ids.is_empty() {
            "no runtime skills available".to_string()
        } else {
            format!("runtime skill ids: {}", input.runtime_skill_ids.join(", "))
        };

        drop(
            journal.send(TurnEvent::canonical(RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text: format!("[mock] {} | prompt: {}", skill_context, input.prompt),
            })),
        );

        let mut capability_requests = Vec::new();
        if let Some(skill_id) = input.runtime_skill_ids.first() {
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
            drop(journal.send(TurnEvent::canonical(RuntimeEvent::Done)));
        } else {
            drop(journal.send(TurnEvent::canonical(RuntimeEvent::Status {
                code: None,
                text: format!(
                    "mock runtime requested {} capability checks",
                    capability_requests.len()
                ),
            })));
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
            drop(events.send(RuntimeEvent::Status {
                code: None,
                text: format!("capability:{}:{}", result.request_id, verdict),
            }));
            if let Some(reason) = result.reason {
                drop(events.send(RuntimeEvent::Status {
                    code: None,
                    text: format!("capability:{}:reason:{}", result.request_id, reason),
                }));
            }
        }
        drop(events.send(RuntimeEvent::Done));
        Ok(())
    }

    async fn runtime_control(
        &self,
        execution: RuntimeControlExecution,
        events: RuntimeEventSender,
    ) -> Result<RuntimeControlOutcome> {
        let command = execution.input.command_name.as_str();
        match command {
            "handled" => {
                drop(events.send(RuntimeEvent::Status {
                    code: Some("mock.control".to_string()),
                    text: "mock runtime saw handled control".to_string(),
                }));
                Ok(RuntimeControlOutcome::Handled {
                    message: "mock runtime handled control".to_string(),
                })
            }
            "failed" => Ok(RuntimeControlOutcome::Failed {
                code: Some("mock.control_failed".to_string()),
                message: "mock runtime control failed".to_string(),
            }),
            "interactive" => Ok(RuntimeControlOutcome::InteractiveOnly {
                message: "mock runtime control is interactive-only".to_string(),
            }),
            _ => Ok(RuntimeControlOutcome::Unsupported {
                message: format!("mock runtime does not support '/{command}'"),
            }),
        }
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
