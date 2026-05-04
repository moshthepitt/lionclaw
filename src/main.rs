#[tokio::main]
async fn main() -> anyhow::Result<std::process::ExitCode> {
    lionclaw::operator::cli::run().await
}
