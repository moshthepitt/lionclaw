use std::process::ExitCode;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "lionclaw-private-context")]
#[command(about = "LionClaw private context projector, recorder, and operator command")]
pub(crate) struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Projector,
    Recorder,
    Context {
        #[command(subcommand)]
        command: crate::context::ContextCommand,
    },
}

impl Cli {
    pub(crate) async fn run(self) -> Result<ExitCode> {
        match self.command {
            Command::Projector => crate::projector::run().await,
            Command::Recorder => crate::recorder::run().await,
            Command::Context { command } => crate::context::run(command).await,
        }
    }
}
