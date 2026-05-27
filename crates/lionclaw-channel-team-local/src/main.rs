mod api;
mod config;
mod discovery;
mod protocol;
mod worker;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "lionclaw_channel_team_local=info".into()),
        )
        .init();

    match config::WorkerCommand::from_env_and_args()? {
        config::WorkerCommand::Run(config) => worker::TeamLocalWorker::new(config)?.run().await,
        config::WorkerCommand::Help => Ok(()),
    }
}
