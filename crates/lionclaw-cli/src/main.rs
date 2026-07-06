use clap::Parser;

#[derive(Parser)]
#[command(name = "lionclaw", about = "LionClaw mission engine")]
enum Command {
    /// Mission engine commands (placeholder; filled in as the engine lands).
    Mission,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _command = Command::parse();
    Ok(())
}
