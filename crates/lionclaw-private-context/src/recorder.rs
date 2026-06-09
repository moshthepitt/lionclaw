use std::process::ExitCode;

use anyhow::{anyhow, bail, Context, Result};
use tokio::io::AsyncReadExt;

use crate::{
    protocol::PrivateContextRecordRequest, store::PrivateContextStore,
    validation::audit_safe_visible_handle,
};

pub(crate) async fn run() -> Result<ExitCode> {
    let private_context_id = private_context_id_from_env()?;
    let store = PrivateContextStore::open_from_env().await?;
    let request = read_request().await?;
    store
        .record_turn(&private_context_id, &request)
        .await
        .context("failed to record private context turn")?;
    Ok(ExitCode::SUCCESS)
}

async fn read_request() -> Result<PrivateContextRecordRequest> {
    let mut input = String::new();
    tokio::io::stdin()
        .read_to_string(&mut input)
        .await
        .context("failed to read private context recorder request")?;
    if input.trim().is_empty() {
        bail!("private context recorder request is required");
    }
    serde_json::from_str(&input).context("failed to decode private context recorder request")
}

fn private_context_id_from_env() -> Result<String> {
    let private_context_id = std::env::var("LIONCLAW_PRIVATE_CONTEXT_ID")
        .map_err(|_| anyhow!("LIONCLAW_PRIVATE_CONTEXT_ID is required"))?;
    if !audit_safe_visible_handle(&private_context_id) {
        bail!(
            "LIONCLAW_PRIVATE_CONTEXT_ID must be 1..128 visible ASCII bytes with no whitespace or path separators"
        );
    }
    Ok(private_context_id)
}
