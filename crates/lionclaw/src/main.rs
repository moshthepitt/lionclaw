use clap::Parser;
use lionclaw::cli::{run, Cli};

#[tokio::main]
async fn main() -> anyhow::Result<std::process::ExitCode> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();
    run(Cli::parse()).await
}
