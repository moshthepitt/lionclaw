use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::net::TcpListener;
use tracing::info;

use crate::{
    api::build_router,
    config::{resolve_project_workspace_root, Config},
    contracts::DaemonInfoResponse,
    home::runtime_project_partition_key,
    kernel::{Kernel, KernelOptions, RuntimeExecutionPolicy},
    operator::{
        config::{daemon_compat_fingerprint, OperatorConfig},
        runtime::{configured_runtime_execution_profiles, register_configured_runtimes},
    },
};

pub async fn run(config: Config) -> anyhow::Result<()> {
    config.home.ensure_base_dirs().await?;
    let home_id = config.home.ensure_home_id().await?;
    let operator_config = OperatorConfig::load(&config.home).await?;
    let workspace_root = if std::env::var_os("LIONCLAW_WORKSPACE").is_some() {
        config.workspace_root.clone()
    } else {
        operator_config.workspace_root(&config.home)
    };
    let project_workspace_root = config
        .project_workspace_root
        .clone()
        .or_else(|| resolve_project_workspace_root().ok())
        .unwrap_or_else(|| workspace_root.clone());
    let project_scope = runtime_project_partition_key(Some(project_workspace_root.as_path()));
    let config_fingerprint = daemon_compat_fingerprint(&operator_config);
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
                runtime_execution_policy: RuntimeExecutionPolicy::for_working_dir_root(
                    project_workspace_root.clone(),
                ),
                default_runtime_id,
                default_preset_name: operator_config.defaults.preset.clone(),
                execution_presets: operator_config.presets.clone(),
                runtime_execution_profiles: configured_runtime_execution_profiles(&operator_config),
                runtime_secrets_home: Some(config.home.clone()),
                workspace_root: Some(workspace_root),
                project_workspace_root: Some(project_workspace_root),
                runtime_root: Some(config.home.runtime_dir()),
                workspace_name: Some(operator_config.daemon.workspace.clone()),
                ..KernelOptions::default()
            },
        )
        .await?,
    );
    register_configured_runtimes(&kernel, &operator_config).await?;
    let scheduler_kernel = kernel.clone();
    tokio::spawn(async move {
        scheduler_kernel.run_scheduler_loop().await;
    });
    let app = build_router(
        kernel,
        DaemonInfoResponse {
            service: "lionclawd".to_string(),
            status: "ok".to_string(),
            home_id,
            home_root: config.home.root().display().to_string(),
            bind_addr: config.bind_addr.clone(),
            project_scope,
            config_fingerprint,
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
