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
mod app_server;
mod control;
mod driver;
mod host_auth;
mod program;
mod state;

pub use adapter::CodexRuntimeAdapter;
pub use driver::{
    codex_runtime_auth_kind, CodexRuntimeConfig, CodexRuntimeDriver, CODEX_DEFAULT_EXECUTABLE,
    CODEX_RUNTIME_AUTH_KIND, CODEX_RUNTIME_DRIVER, CODEX_SKILL_PROJECTION_ROOT,
};
pub use host_auth::{
    codex_home_identity, ensure_codex_host_auth_ready, prepare_codex_runtime_auth,
    sync_codex_home_into_runtime_home, CodexRuntimeAuthProvider,
};

#[cfg(test)]
pub(crate) use app_server::{
    app_server_error_text, app_server_item_events, app_server_text,
    codex_default_generated_image_path, codex_generated_image_path, completed_turn_error_text,
    describe_app_server_item, extract_app_server_turn_id, finish_app_server_session, response_id,
    turn_start_params, AppServerMessage, AppServerTransport, CodexAppServerClient,
};
#[cfg(test)]
pub(crate) use control::{
    describe_model_list_response, invalid_thread_control_arguments, model_list_include_hidden,
};
#[cfg(test)]
pub(crate) use program::{
    build_codex_app_server_program, build_codex_terminal_program, codex_mcp_server_override_args,
};
#[cfg(test)]
pub(crate) use state::{save_thread_id, CodexThreadState, CODEX_THREAD_ID_STATE_FILE};

#[cfg(test)]
mod tests;
