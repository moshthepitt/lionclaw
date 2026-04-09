use std::{io::ErrorKind, path::PathBuf};

use anyhow::{anyhow, Context, Result};

use crate::home::LionClawHome;

pub async fn resolve_runtime_secrets_file(home: &LionClawHome) -> Result<Option<PathBuf>> {
    let path = home.runtime_secrets_env_path();
    let metadata = match tokio::fs::symlink_metadata(&path).await {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(err).with_context(|| format!("failed to stat {}", path.display()));
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(anyhow!(
            "runtime secrets file '{}' cannot be a symlink",
            path.display()
        ));
    }
    if !metadata.file_type().is_file() {
        return Err(anyhow!(
            "runtime secrets path '{}' must be a regular file",
            path.display()
        ));
    }
    harden_runtime_secret_permissions(&path).await?;
    let canonical = tokio::fs::canonicalize(&path)
        .await
        .with_context(|| format!("failed to canonicalize {}", path.display()))?;
    Ok(Some(canonical))
}

#[cfg(unix)]
async fn harden_runtime_secret_permissions(path: &std::path::Path) -> Result<()> {
    use std::{fs::Permissions, os::unix::fs::PermissionsExt};

    let config_dir = path.parent().ok_or_else(|| {
        anyhow!(
            "runtime secrets file '{}' does not have a parent directory",
            path.display()
        )
    })?;
    let config_mode = tokio::fs::metadata(config_dir)
        .await
        .with_context(|| format!("failed to read metadata for {}", config_dir.display()))?
        .permissions()
        .mode();
    if config_mode & 0o077 != 0 {
        tokio::fs::set_permissions(config_dir, Permissions::from_mode(0o700))
            .await
            .with_context(|| format!("failed to chmod {}", config_dir.display()))?;
    }

    let file_mode = tokio::fs::metadata(path)
        .await
        .with_context(|| format!("failed to read metadata for {}", path.display()))?
        .permissions()
        .mode();
    if file_mode & 0o077 != 0 {
        tokio::fs::set_permissions(path, Permissions::from_mode(0o600))
            .await
            .with_context(|| format!("failed to chmod {}", path.display()))?;
    }

    Ok(())
}

#[cfg(not(unix))]
async fn harden_runtime_secret_permissions(_path: &std::path::Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::resolve_runtime_secrets_file;
    use crate::home::LionClawHome;

    #[tokio::test]
    async fn missing_runtime_secrets_file_returns_none() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        home.ensure_base_dirs().await.expect("base dirs");

        let secrets = resolve_runtime_secrets_file(&home)
            .await
            .expect("resolve secrets");
        assert!(secrets.is_none());
    }

    #[tokio::test]
    async fn runtime_secrets_file_resolves_path() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        home.ensure_base_dirs().await.expect("base dirs");
        tokio::fs::write(home.runtime_secrets_env_path(), "GITHUB_TOKEN=ghp_test\n")
            .await
            .expect("write runtime secrets");

        let secrets = resolve_runtime_secrets_file(&home)
            .await
            .expect("resolve secrets");
        assert_eq!(secrets, Some(home.runtime_secrets_env_path()));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn runtime_secrets_file_is_hardened_to_owner_only_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        home.ensure_base_dirs().await.expect("base dirs");
        std::fs::set_permissions(home.config_dir(), std::fs::Permissions::from_mode(0o755))
            .expect("chmod config dir");
        tokio::fs::write(home.runtime_secrets_env_path(), "GITHUB_TOKEN=ghp_test\n")
            .await
            .expect("write runtime secrets");
        std::fs::set_permissions(
            home.runtime_secrets_env_path(),
            std::fs::Permissions::from_mode(0o644),
        )
        .expect("chmod env file");

        let resolved = resolve_runtime_secrets_file(&home)
            .await
            .expect("resolve secrets");
        assert_eq!(resolved, Some(home.runtime_secrets_env_path()));

        let config_mode = std::fs::metadata(home.config_dir())
            .expect("config metadata")
            .permissions()
            .mode()
            & 0o777;
        let file_mode = std::fs::metadata(home.runtime_secrets_env_path())
            .expect("file metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(config_mode, 0o700);
        assert_eq!(file_mode, 0o600);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn runtime_secrets_file_rejects_symlinks() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        home.ensure_base_dirs().await.expect("base dirs");

        let real = temp_dir.path().join("real.env");
        std::fs::write(&real, "GITHUB_TOKEN=ghp_test\n").expect("write real env");
        std::os::unix::fs::symlink(&real, home.runtime_secrets_env_path()).expect("symlink env");

        let err = resolve_runtime_secrets_file(&home)
            .await
            .expect_err("symlink should fail");
        assert!(err.to_string().contains("cannot be a symlink"));
    }
}
