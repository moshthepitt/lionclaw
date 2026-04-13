#[tokio::main]
async fn main() -> anyhow::Result<()> {
    lionclaw::daemon::init_tracing();
    lionclaw::daemon::run(lionclaw::config::Config::from_env()?).await
}
