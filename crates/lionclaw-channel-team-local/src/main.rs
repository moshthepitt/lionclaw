mod api;
mod config;
mod discovery;
mod inventory;
mod protocol;
mod send;
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

    match config::Command::from_env_and_args()? {
        config::Command::Worker(config) => worker::TeamLocalWorker::new(config)?.run().await,
        config::Command::List(config) => {
            let inventory = inventory::ProjectInventory::load(&config.instances_file)?;
            let summary = inventory.list_summary(&config.self_instance);
            println!("{}", serde_json::to_string_pretty(&summary)?);
            Ok(())
        }
        config::Command::Resolve(config) => {
            let inventory = inventory::ProjectInventory::load(&config.inventory.instances_file)?;
            let summary =
                inventory.resolve_summary(&config.inventory.self_instance, &config.recipient);
            let ok = summary.ok;
            println!("{}", serde_json::to_string_pretty(&summary)?);
            if ok {
                Ok(())
            } else {
                std::process::exit(1);
            }
        }
        config::Command::Send(config) => {
            let summary = send::run(config)?;
            let ok = summary.ok;
            println!("{}", serde_json::to_string_pretty(&summary)?);
            if ok {
                Ok(())
            } else {
                std::process::exit(1);
            }
        }
        config::Command::Help => Ok(()),
    }
}
