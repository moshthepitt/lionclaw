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
    RuntimeAdapter, RuntimeAdapterInfo, RuntimeEvent, RuntimeMessageLane, RuntimeSessionHandle,
    RuntimeSessionStartInput, RuntimeTurnJournalSender, TurnEvent, TurnExecution, TurnResult,
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

    fn session_start(&self, _input: RuntimeSessionStartInput) -> Result<RuntimeSessionHandle> {
        Ok(RuntimeSessionHandle {
            runtime_session_id: format!("mock-{}", Uuid::new_v4()),
        })
    }

    async fn turn(
        &self,
        execution: TurnExecution,
        journal: RuntimeTurnJournalSender,
    ) -> Result<TurnResult> {
        let input = execution.input;
        let final_response = format!("[mock] prompt: {}", input.prompt);
        drop(
            journal
                .send(TurnEvent::canonical(RuntimeEvent::Status {
                    code: None,
                    text: "mock runtime started turn".to_string(),
                }))
                .await,
        );

        drop(
            journal
                .send(TurnEvent::canonical(RuntimeEvent::MessageDelta {
                    lane: RuntimeMessageLane::Answer,
                    text: final_response.clone(),
                }))
                .await,
        );

        drop(journal.send(TurnEvent::canonical(RuntimeEvent::Done)).await);

        Ok(TurnResult {
            configuration: Default::default(),
            final_response,
        })
    }

    async fn cancel(
        &self,
        _handle: &RuntimeSessionHandle,
        _reason: Option<String>,
    ) -> Result<lionclaw_runtime_api::RuntimeCancellation> {
        Ok(lionclaw_runtime_api::RuntimeCancellation::NoActiveTurn)
    }

    fn close(&self, _handle: &RuntimeSessionHandle) -> Result<()> {
        Ok(())
    }
}
