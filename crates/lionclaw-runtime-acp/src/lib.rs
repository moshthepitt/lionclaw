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
mod client;
mod driver;
mod event_mapping;
mod policy;
mod program;
mod protocol;
mod state;

pub use adapter::AcpRuntimeAdapter;
pub use driver::{
    AcpRuntimeConfig, AcpRuntimeDriver, ACP_DEFAULT_WORKING_DIR, ACP_PROTOCOL_NAME,
    ACP_SESSION_ID_STATE_FILE,
};

#[cfg(test)]
pub(crate) use event_mapping::acp_turn_events;
#[cfg(test)]
pub(crate) use policy::acp_permission_denial;
#[cfg(test)]
pub(crate) use protocol::AcpMessage;

#[cfg(test)]
mod tests;
