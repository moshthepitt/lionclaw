use std::sync::Arc;

use anyhow::Context;
use lionclaw::{api::build_router, config::Config, kernel::Kernel};
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config = Config::from_env();
    let kernel = Arc::new(Kernel::new(&config.db_path).await?);
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

fn init_tracing() {
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
