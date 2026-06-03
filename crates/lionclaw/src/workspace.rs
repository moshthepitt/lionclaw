use std::path::Path;

use anyhow::{bail, Result};

use crate::kernel::{
    continuity::{ContinuityLayout, MEMORY_FILE},
    continuity_fs::ContinuityFs,
};

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

#[deprecated(note = "runtime prompt construction must use policy-selected targeted reads")]
pub async fn read_workspace_sections(workspace_root: &Path) -> Result<Vec<(String, String)>> {
    let mut sections = Vec::new();
    for file_name in [IDENTITY_FILE, SOUL_FILE, AGENTS_FILE, USER_FILE] {
        if let Some(content) = read_workspace_section(workspace_root, file_name).await? {
            sections.push((file_name.to_string(), content));
        }
    }

    let continuity = ContinuityLayout::new(workspace_root);
    if let Some(content) = continuity.read_memory_prompt_section().await? {
        sections.push((MEMORY_FILE.to_string(), content));
    }
    if let Some(content) = continuity.read_active_prompt_section().await? {
        sections.push(("continuity/ACTIVE.md".to_string(), content));
    }

    Ok(sections)
}

pub async fn read_workspace_section(
    workspace_root: &Path,
    file_name: &str,
) -> Result<Option<String>> {
    if ![IDENTITY_FILE, SOUL_FILE, AGENTS_FILE, USER_FILE].contains(&file_name) {
        bail!("workspace prompt section '{file_name}' is not allowlisted");
    }
    let fs = ContinuityFs::open_existing(workspace_root)?;
    fs.read_to_string_if_exists(Path::new(file_name))
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;

    use tempfile::tempdir;

    use super::{bootstrap_workspace, read_workspace_section, AGENTS_FILE, SOUL_FILE};

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

    #[allow(deprecated)]
    #[tokio::test]
    async fn compatibility_workspace_sections_reject_symlinked_files() {
        let temp_dir = tempdir().expect("temp dir");
        let workspace = temp_dir.path().join("workspace");
        bootstrap_workspace(&workspace)
            .await
            .expect("bootstrap workspace");

        let outside = temp_dir.path().join("outside.md");
        std::fs::write(&outside, "# Outside\n").expect("write outside file");
        std::fs::remove_file(workspace.join(SOUL_FILE)).expect("remove soul file");
        symlink(&outside, workspace.join(SOUL_FILE)).expect("symlink soul file");

        let err = super::read_workspace_sections(&workspace)
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

    #[tokio::test]
    async fn read_workspace_section_reads_allowlisted_file() {
        let temp_dir = tempdir().expect("temp dir");
        let workspace = temp_dir.path().join("workspace");
        bootstrap_workspace(&workspace)
            .await
            .expect("bootstrap workspace");

        let agents = read_workspace_section(&workspace, AGENTS_FILE)
            .await
            .expect("read agents")
            .expect("agents exists");
        assert!(agents.contains("Follow kernel policy"));
    }

    #[tokio::test]
    async fn read_workspace_section_rejects_unallowlisted_file() {
        let temp_dir = tempdir().expect("temp dir");
        let workspace = temp_dir.path().join("workspace");
        bootstrap_workspace(&workspace)
            .await
            .expect("bootstrap workspace");

        let err = read_workspace_section(&workspace, "../USER.md")
            .await
            .expect_err("path traversal should fail");
        assert!(err.to_string().contains("not allowlisted"));
    }

    #[tokio::test]
    async fn read_workspace_section_rejects_symlinked_file() {
        let temp_dir = tempdir().expect("temp dir");
        let workspace = temp_dir.path().join("workspace");
        bootstrap_workspace(&workspace)
            .await
            .expect("bootstrap workspace");

        let outside = temp_dir.path().join("outside.md");
        std::fs::write(&outside, "# Outside\n").expect("write outside file");
        std::fs::remove_file(workspace.join(SOUL_FILE)).expect("remove soul file");
        symlink(&outside, workspace.join(SOUL_FILE)).expect("symlink soul file");

        let err = read_workspace_section(&workspace, SOUL_FILE)
            .await
            .expect_err("read workspace section should fail");
        let message = err.to_string();
        assert!(message.contains("failed to open") || message.contains("not a regular file"));
    }
}
