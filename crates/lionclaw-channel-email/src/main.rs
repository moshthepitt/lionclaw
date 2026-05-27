use anyhow::Result;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "lionclaw_channel_email=info".into()),
        )
        .init();
    tracing::info!("email channel worker is not implemented yet");
    Ok(())
}
