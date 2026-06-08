use std::process::ExitCode;

use anyhow::{anyhow, bail, Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::{
    protocol::PrivateContextProjectionRequest, store::PrivateContextStore,
    validation::audit_safe_visible_handle,
};

pub(crate) async fn run() -> Result<ExitCode> {
    let projector_id = projector_id_from_env()?;
    let store = PrivateContextStore::open_from_env().await?;
    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();
    let stdout = tokio::io::stdout();
    let mut stdout = tokio::io::BufWriter::new(stdout);

    while let Some(line) = lines
        .next_line()
        .await
        .context("failed to read private context projector request")?
    {
        if line.trim().is_empty() {
            continue;
        }
        let request = serde_json::from_str::<PrivateContextProjectionRequest>(&line)
            .with_context(|| "failed to decode private context projection request")?;
        let projection = store.project(&projector_id, &request).await?;
        let response = serde_json::to_string(&projection)
            .with_context(|| "failed to encode private context projection response")?;
        stdout.write_all(response.as_bytes()).await?;
        stdout.write_all(b"\n").await?;
        stdout.flush().await?;
    }

    Ok(ExitCode::SUCCESS)
}

fn projector_id_from_env() -> Result<String> {
    let projector_id = std::env::var("LIONCLAW_PRIVATE_CONTEXT_PROJECTOR_ID")
        .map_err(|_| anyhow!("LIONCLAW_PRIVATE_CONTEXT_PROJECTOR_ID is required"))?;
    if !audit_safe_visible_handle(&projector_id) {
        bail!(
            "LIONCLAW_PRIVATE_CONTEXT_PROJECTOR_ID must be 1..128 visible ASCII bytes with no whitespace or path separators"
        );
    }
    Ok(projector_id)
}
