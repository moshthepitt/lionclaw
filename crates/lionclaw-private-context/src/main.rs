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

mod cli;
mod context;
mod projector;
mod protocol;
mod recorder;
mod store;
mod validation;

use anyhow::Result;
use clap::Parser;
use std::process::ExitCode;

#[tokio::main]
async fn main() -> Result<ExitCode> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "lionclaw_private_context=info".into()),
        )
        .init();

    cli::Cli::parse().run().await
}
