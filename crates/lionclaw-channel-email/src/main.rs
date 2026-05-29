mod api;
mod auth;
mod classifier;
mod config;
mod mailbox;
mod mime;
mod oauth2;
mod protocol;
mod store;
mod worker;

use anyhow::Result;

use crate::{
    config::WorkerCommand,
    worker::{EmailWorker, RealMailboxFactory},
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "lionclaw_channel_email=info".into()),
        )
        .init();

    match WorkerCommand::from_env_and_args()? {
        WorkerCommand::Run(config) => EmailWorker::new(*config, RealMailboxFactory)?.run().await?,
        WorkerCommand::Oauth2(command) => oauth2::run(command).await?,
        WorkerCommand::Help => {}
    }

    Ok(())
}
