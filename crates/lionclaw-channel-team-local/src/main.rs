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

mod api;
mod config;
mod discovery;
mod inventory;
mod protocol;
mod worker;

use anyhow::Result;
use std::process::ExitCode;

#[tokio::main]
async fn main() -> Result<ExitCode> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "lionclaw_channel_team_local=info".into()),
        )
        .init();

    match config::Command::from_env_and_args()? {
        config::Command::Worker(config) => worker::TeamLocalWorker::new(config)?
            .run()
            .await
            .map(|()| ExitCode::SUCCESS),
        config::Command::List(config) => {
            let inventory = inventory::ProjectInventory::load(&config.instances_file)?;
            let summary = inventory.list_summary(&config.self_instance);
            println!("{}", serde_json::to_string_pretty(&summary)?);
            Ok(ExitCode::SUCCESS)
        }
        config::Command::Resolve(config) => {
            let inventory = inventory::ProjectInventory::load(&config.inventory.instances_file)?;
            let summary =
                inventory.resolve_summary(&config.inventory.self_instance, &config.recipient);
            let ok = summary.ok;
            println!("{}", serde_json::to_string_pretty(&summary)?);
            if ok {
                Ok(ExitCode::SUCCESS)
            } else {
                Ok(ExitCode::FAILURE)
            }
        }
        config::Command::Help => Ok(ExitCode::SUCCESS),
    }
}
