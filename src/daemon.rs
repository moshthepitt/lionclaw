use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::net::TcpListener;
use tracing::info;

use crate::{
    api::build_router,
    config::Config,
    contracts::DaemonInfoResponse,
    kernel::{Kernel, KernelOptions},
    operator::{config::OperatorConfig, runtime::register_configured_runtimes},
};

pub async fn run(config: Config) -> anyhow::Result<()> {
    config.home.ensure_base_dirs().await?;
    let home_id = config.home.ensure_home_id().await?;
    let operator_config = OperatorConfig::load(&config.home).await?;
    let workspace_root = if std::env::var_os("LIONCLAW_WORKSPACE").is_some()
        || std::env::var_os("LIONCLAW_WORKSPACE_ROOT").is_some()
    {
        config.workspace_root.clone()
    } else {
        operator_config.workspace_root(&config.home)
    };
    let default_runtime_id = config
        .default_runtime_id
        .clone()
        .or_else(|| operator_config.defaults.runtime.clone());

    let kernel = Arc::new(
        Kernel::new_with_options(
            &config.db_path,
            KernelOptions {
                runtime_turn_idle_timeout: Duration::from_millis(
                    config.runtime_turn_idle_timeout_ms,
                ),
                runtime_turn_hard_timeout: Duration::from_millis(
                    config.runtime_turn_hard_timeout_ms,
                ),
                default_runtime_id,
                workspace_root: Some(workspace_root),
                ..KernelOptions::default()
            },
        )
        .await?,
    );
    register_configured_runtimes(&kernel, &operator_config).await?;
    let app = build_router(
        kernel,
        DaemonInfoResponse {
            service: "lionclawd".to_string(),
            status: "ok".to_string(),
            home_id,
            home_root: config.home.root().display().to_string(),
            bind_addr: config.bind_addr.clone(),
        },
    );

    let listener = TcpListener::bind(&config.bind_addr)
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
