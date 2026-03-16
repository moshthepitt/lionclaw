use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::net::TcpListener;
use tracing::info;

use crate::{
    api::build_router,
    config::Config,
    kernel::{Kernel, KernelOptions},
};

pub async fn run(config: Config) -> anyhow::Result<()> {
    config.home.ensure_base_dirs().await?;

    let kernel = Arc::new(
        Kernel::new_with_options(
            &config.db_path,
            KernelOptions {
                runtime_turn_timeout: Duration::from_millis(config.runtime_turn_timeout_ms),
                default_runtime_id: config.default_runtime_id.clone(),
                workspace_root: Some(config.workspace_root.clone()),
                ..KernelOptions::default()
            },
        )
        .await?,
    );
    let app = build_router(kernel);

    let listener = TcpListener::bind(config.bind_addr)
        .await
        .with_context(|| format!("failed to bind {}", config.bind_addr))?;

    info!(bind_addr = %config.bind_addr, "lionclawd starting");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server exited with error")?;

    info!("lionclawd stopped");
    Ok(())
}

pub fn init_tracing() {
    let filter =
        std::env::var("RUST_LOG").unwrap_or_else(|_| "lionclaw=info,tower_http=info".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
