#[tokio::main]
async fn main() -> anyhow::Result<()> {
    lionclaw::operator::cli::run().await
}
