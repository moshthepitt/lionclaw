use std::path::Path;

use anyhow::{Context, Result};

use crate::kernel::continuity::ContinuityLayout;

pub const IDENTITY_FILE: &str = "IDENTITY.md";
pub const SOUL_FILE: &str = "SOUL.md";
pub const AGENTS_FILE: &str = "AGENTS.md";
pub const USER_FILE: &str = "USER.md";
pub const GENERATED_AGENTS_FILE: &str = "AGENTS.generated.md";

const IDENTITY_TEMPLATE: &str = "# Identity\n\nLionClaw is your local secure-first agent kernel.\n";
const SOUL_TEMPLATE: &str =
    "# Soul\n\nLionClaw is calm, precise, security-first, and execution-oriented.\n";
const AGENTS_TEMPLATE: &str =
    "# Agents\n\nFollow kernel policy, use installed skills, and preserve auditability.\n";
const USER_TEMPLATE: &str =
    "# User\n\nCapture operator-specific goals, preferences, and environment notes here.\n";

pub async fn bootstrap_workspace(workspace_root: &Path) -> Result<()> {
    tokio::fs::create_dir_all(workspace_root)
        .await
        .with_context(|| format!("failed to create {}", workspace_root.display()))?;

    ensure_file(workspace_root.join(IDENTITY_FILE), IDENTITY_TEMPLATE).await?;
    ensure_file(workspace_root.join(SOUL_FILE), SOUL_TEMPLATE).await?;
    ensure_file(workspace_root.join(AGENTS_FILE), AGENTS_TEMPLATE).await?;
    ensure_file(workspace_root.join(USER_FILE), USER_TEMPLATE).await?;
    ContinuityLayout::new(workspace_root)
        .ensure_base_layout()
        .await?;

    Ok(())
}

pub async fn read_workspace_sections(workspace_root: &Path) -> Result<Vec<(String, String)>> {
    let mut sections = Vec::new();
    for file_name in [IDENTITY_FILE, SOUL_FILE, AGENTS_FILE, USER_FILE] {
        let path = workspace_root.join(file_name);
        if !tokio::fs::try_exists(&path)
            .await
            .with_context(|| format!("failed to stat {}", path.display()))?
        {
            continue;
        }
        let content = tokio::fs::read_to_string(&path)
            .await
            .with_context(|| format!("failed to read {}", path.display()))?;
        sections.push((file_name.to_string(), content));
    }

    for (label, content) in ContinuityLayout::new(workspace_root)
        .read_prompt_sections()
        .await?
    {
        sections.push((label, content));
    }

    Ok(sections)
}

async fn ensure_file(path: std::path::PathBuf, content: &str) -> Result<()> {
    match tokio::fs::try_exists(&path).await {
        Ok(true) => Ok(()),
        Ok(false) => tokio::fs::write(&path, content)
            .await
            .with_context(|| format!("failed to write {}", path.display())),
        Err(err) => Err(err).with_context(|| format!("failed to stat {}", path.display())),
    }
}
