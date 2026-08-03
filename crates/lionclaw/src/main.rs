use std::io::Write;

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
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            let exit_code = std::process::ExitCode::from(error.exit_code() as u8);
            let rendered = error.to_string();
            let result = if error.use_stderr() {
                std::io::stderr().write_all(rendered.as_bytes())
            } else {
                std::io::stdout().write_all(rendered.as_bytes())
            };
            match result {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => {}
                Err(err) => return Err(err.into()),
            }
            return Ok(exit_code);
        }
    };
    run(cli).await
}
