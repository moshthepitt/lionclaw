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

mod adapter;
mod auth;
mod context;
mod driver;
mod event;
mod program;
mod state;
mod turn;

pub use adapter::{
    RuntimeAdapter, RuntimeAdapterInfo, RuntimeCancellation, RuntimeResume, RuntimeResumeMode,
    RuntimeSessionHandle, RuntimeSessionStartInput, RuntimeTerminalProgramInput, TurnExecution,
    TurnInput,
};
pub use auth::{
    RuntimeAuthContext, RuntimeAuthKind, RuntimeAuthPreparation, RuntimeAuthProvider,
    RuntimeAuthRegistry,
};
pub use context::{
    safe_relative_path, RuntimeExecutionContext, RuntimeMcpServerSpec,
    RuntimeNativeHomeArtifactDir, RuntimePathProjection, RuntimePathProjectionKind,
};
pub use driver::{RuntimeDriverConfig, RuntimeDriverProvider, RuntimeTerminalConfig};
pub use event::{
    append_streamed_text_boundary, append_streamed_text_delta, canonical_events,
    observe_final_response, RuntimeArtifact, RuntimeEvent, RuntimeEventSender, RuntimeFileChange,
    RuntimeFileChangeStatus, RuntimeMessageLane, RuntimeTurnJournalSender, TurnEvent,
    RUNTIME_TURN_JOURNAL_CAPACITY,
};
pub use lionclaw_model::{
    bounded_failure_text as bounded_text, AppliedRuntimeConfiguration,
    RuntimeConfigurationConfirmation, TypedFailure, TypedFailureEvidence, FAILURE_TEXT_LIMIT,
};
pub use program::{
    ExecutionOutput, NetworkMode, RuntimeProgramExecutor, RuntimeProgramSession,
    RuntimeProgramSpec, RuntimeProgramStdoutLine, RuntimeProgramStdoutLineError,
    RuntimeProgramStdoutSender, RUNTIME_PROGRAM_STDOUT_LINE_LIMIT,
};
pub use state::{
    clear_state_value, load_ready_state_value, load_state_value,
    runtime_session_ready_marker_exists, save_state_value, RuntimeSessionReady,
    RUNTIME_SESSION_READY_MARKER,
};
pub use turn::TurnResult;
