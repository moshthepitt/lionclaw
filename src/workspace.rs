use std::path::Path;

use anyhow::Result;

use crate::kernel::{continuity::ContinuityLayout, continuity_fs::ContinuityFs};

pub const IDENTITY_FILE: &str = "IDENTITY.md";
pub const SOUL_FILE: &str = "SOUL.md";
pub const AGENTS_FILE: &str = "AGENTS.md";
pub const USER_FILE: &str = "USER.md";
pub const GENERATED_AGENTS_FILE: &str = "AGENTS.generated.md";

const IDENTITY_TEMPLATE: &str = "# Identity\n\nLionClaw is your local secure-first agent kernel.\n";
const SOUL_TEMPLATE: &str =
    "# Soul\n\nLionClaw is calm, precise, security-first, and execution-oriented.\n";
const AGENTS_TEMPLATE: &str = "# Agents\n\nFollow kernel policy, use installed skills, and preserve auditability.\nInstalled LionClaw skills are mounted under /lionclaw/skills and may also appear in the runtime's native skill directory.\nWrite generated files intended for review or keeping to LIONCLAW_DRAFTS_DIR.\nIf this preset mounts runtime secrets, look under /run/secrets for the LionClaw-provided dotenv file whose name starts with lionclaw-runtime-secrets-, read it, and do not print its contents.\n";
const USER_TEMPLATE: &str =
    "# User\n\nCapture operator-specific goals, preferences, and environment notes here.\n";

pub async fn bootstrap_workspace(workspace_root: &Path) -> Result<()> {
    let fs = ContinuityFs::bootstrap(workspace_root)?;
    fs.ensure_file(Path::new(IDENTITY_FILE), IDENTITY_TEMPLATE)?;
    fs.ensure_file(Path::new(SOUL_FILE), SOUL_TEMPLATE)?;
    fs.ensure_file(Path::new(AGENTS_FILE), AGENTS_TEMPLATE)?;
    fs.ensure_file(Path::new(USER_FILE), USER_TEMPLATE)?;
    ContinuityLayout::new(workspace_root)
        .ensure_base_layout()
        .await?;

    Ok(())
}

pub async fn read_workspace_sections(workspace_root: &Path) -> Result<Vec<(String, String)>> {
    let fs = ContinuityFs::open_existing(workspace_root)?;
    let mut sections = Vec::new();
    for file_name in [IDENTITY_FILE, SOUL_FILE, AGENTS_FILE, USER_FILE] {
        if let Some(content) = fs.read_to_string_if_exists(Path::new(file_name))? {
            sections.push((file_name.to_string(), content));
        }
    }

    for (label, content) in ContinuityLayout::new(workspace_root)
        .read_prompt_sections()
        .await?
    {
        sections.push((label, content));
    }

    Ok(sections)
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;

    use tempfile::tempdir;

    use super::{bootstrap_workspace, read_workspace_sections, AGENTS_FILE, SOUL_FILE};

    #[tokio::test]
    async fn bootstrap_workspace_rejects_symlinked_root() {
        let temp_dir = tempdir().expect("temp dir");
        let outside = temp_dir.path().join("outside");
        std::fs::create_dir_all(&outside).expect("create outside");
        let workspace = temp_dir.path().join("workspace");
        symlink(&outside, &workspace).expect("symlink workspace");

        let err = bootstrap_workspace(&workspace)
            .await
            .expect_err("bootstrap should fail");
        assert!(err.to_string().contains("failed to open"));
    }

    #[tokio::test]
    async fn read_workspace_sections_rejects_symlinked_identity_file() {
        let temp_dir = tempdir().expect("temp dir");
        let workspace = temp_dir.path().join("workspace");
        bootstrap_workspace(&workspace)
            .await
            .expect("bootstrap workspace");

        let outside = temp_dir.path().join("outside.md");
        std::fs::write(&outside, "# Outside\n").expect("write outside file");
        std::fs::remove_file(workspace.join(SOUL_FILE)).expect("remove soul file");
        symlink(&outside, workspace.join(SOUL_FILE)).expect("symlink soul file");

        let err = read_workspace_sections(&workspace)
            .await
            .expect_err("read workspace sections should fail");
        let message = err.to_string();
        assert!(message.contains("failed to open") || message.contains("not a regular file"));
    }

    #[tokio::test]
    async fn bootstrap_workspace_writes_runtime_secret_guidance_to_agents() {
        let temp_dir = tempdir().expect("temp dir");
        let workspace = temp_dir.path().join("workspace");
        bootstrap_workspace(&workspace)
            .await
            .expect("bootstrap workspace");

        let agents = std::fs::read_to_string(workspace.join(AGENTS_FILE)).expect("read agents");
        assert!(agents.contains("LIONCLAW_DRAFTS_DIR"));
        assert!(agents.contains("/run/secrets"));
        assert!(agents.contains("lionclaw-runtime-secrets-"));
        assert!(agents.contains("do not print its contents"));
    }
}
