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
use uuid::Uuid;

use lionclaw_runtime_api::{
    HiddenTurnSupport, RuntimeAdapter, RuntimeAdapterInfo, RuntimeCapabilityResult,
    RuntimeControlExecution, RuntimeControlOutcome, RuntimeEvent, RuntimeEventSender,
    RuntimeMessageLane, RuntimeSessionHandle, RuntimeSessionStartInput, RuntimeTurnInput,
    RuntimeTurnJournalSender, RuntimeTurnResult, TurnEvent,
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

        drop(
            journal.send(TurnEvent::canonical(RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text: format!("[mock] prompt: {}", input.prompt),
            })),
        );

        drop(journal.send(TurnEvent::canonical(RuntimeEvent::Done)));

        Ok(RuntimeTurnResult {
            capability_requests: Vec::new(),
            configuration: Default::default(),
            final_response: String::new(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use lionclaw_runtime_api::RuntimeSessionReady;

    /// The mock never derives capability requests from skill IDs — the dead
    /// speculative-activation path is gone. A prompt packed with capability
    /// markers produces zero requests.
    #[tokio::test]
    async fn mock_turn_never_activates_capabilities_from_skill_ids() {
        let adapter = MockRuntimeAdapter;
        let handle = adapter
            .session_start(RuntimeSessionStartInput {
                session_id: uuid::Uuid::nil(),
                working_dir: None,
                environment: Vec::new(),
                runtime_state_root: None,
                runtime_session_ready: RuntimeSessionReady::not_ready(),
            })
            .await
            .expect("session_start");

        let (journal_tx, mut journal_rx) =
            tokio::sync::mpsc::unbounded_channel::<lionclaw_runtime_api::TurnEvent>();
        let result = adapter
            .turn(
                RuntimeTurnInput {
                    runtime_session_id: handle.runtime_session_id.clone(),
                    prompt: "[cap:fs.read] [cap:net.egress] [cap:secret.request]".to_string(),
                    fresh_prompt: None,
                },
                journal_tx,
            )
            .await
            .expect("turn");

        assert!(
            result.capability_requests.is_empty(),
            "no speculative capability activation from skill IDs"
        );
        // Drain events to satisfy the journal sender.
        while journal_rx.try_recv().is_ok() {}
        adapter.close(&handle).await.expect("close");
    }
}
