use std::path::Path;

use anyhow::{bail, Result};

use crate::kernel::{continuity::ContinuityLayout, continuity_fs::ContinuityFs};

pub const AGENTS_FILE: &str = "AGENTS.md";
pub const GENERATED_AGENTS_FILE: &str = "AGENTS.generated.md";

const AGENTS_TEMPLATE: &str =
    "# Agents\n\nFollow kernel policy, use installed skills, and preserve auditability.\n";

pub async fn bootstrap_workspace(workspace_root: &Path) -> Result<()> {
    let fs = ContinuityFs::bootstrap(workspace_root)?;
    fs.ensure_file(Path::new(AGENTS_FILE), AGENTS_TEMPLATE)?;
    ContinuityLayout::new(workspace_root)
        .ensure_base_layout()
        .await?;

    Ok(())
}

pub async fn read_workspace_section(
    workspace_root: &Path,
    file_name: &str,
) -> Result<Option<String>> {
    if file_name != AGENTS_FILE {
        bail!("workspace prompt section '{file_name}' is not allowlisted");
    }
    let fs = ContinuityFs::open_existing(workspace_root)?;
    fs.read_to_string_if_exists(Path::new(file_name))
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;

    use tempfile::tempdir;

    use super::{bootstrap_workspace, read_workspace_section, AGENTS_FILE};

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
    async fn bootstrap_workspace_writes_workspace_rules_only() {
        let temp_dir = tempdir().expect("temp dir");
        let workspace = temp_dir.path().join("workspace");
        bootstrap_workspace(&workspace)
            .await
            .expect("bootstrap workspace");

        let agents = std::fs::read_to_string(workspace.join(AGENTS_FILE)).expect("read agents");
        assert!(agents.contains("Follow kernel policy"));
        assert!(!agents.contains("LIONCLAW_DRAFTS_DIR"));
        assert!(!agents.contains("/run/secrets"));
        assert!(!workspace.join("IDENTITY.md").exists());
        assert!(!workspace.join("SOUL.md").exists());
        assert!(!workspace.join("USER.md").exists());
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
        std::fs::remove_file(workspace.join(AGENTS_FILE)).expect("remove agents file");
        symlink(&outside, workspace.join(AGENTS_FILE)).expect("symlink agents file");

        let err = read_workspace_section(&workspace, AGENTS_FILE)
            .await
            .expect_err("read workspace section should fail");
        let message = err.to_string();
        assert!(message.contains("failed to open") || message.contains("not a regular file"));
    }
}
