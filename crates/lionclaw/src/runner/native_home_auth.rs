use std::io::ErrorKind;
use std::path::{Component, Path};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use lionclaw_runtime_api::{RuntimeAuthContext, RuntimeAuthPreparation, RuntimeAuthProvider};

use crate::config::NativeHomeAuthConfig;

pub(super) const NATIVE_HOME_AUTH_KIND: &str = "native-home";

#[derive(Debug, Clone)]
pub(super) struct NativeHomeAuthProvider {
    config: NativeHomeAuthConfig,
}

impl NativeHomeAuthProvider {
    pub(super) fn new(config: NativeHomeAuthConfig) -> Self {
        Self { config }
    }

    async fn validate_source(&self) -> Result<()> {
        validate_source_root(&self.config.source).await?;
        for path in &self.config.required_files {
            validate_source_file(&self.config.source, path, true).await?;
        }
        for path in &self.config.optional_files {
            validate_source_file(&self.config.source, path, false).await?;
        }
        Ok(())
    }

    async fn project(&self, runtime_home: &Path) -> Result<()> {
        validate_source_root(&self.config.source).await?;
        let target_root = runtime_home.join(&self.config.target);
        create_private_dir(&target_root).await?;

        for (path, required) in self
            .config
            .required_files
            .iter()
            .map(|path| (path, true))
            .chain(self.config.optional_files.iter().map(|path| (path, false)))
        {
            if !validate_source_file(&self.config.source, path, required).await? {
                continue;
            }
            let source = self.config.source.join(path);
            let target = target_root.join(path);
            if let Some(parent) = target.parent() {
                create_private_dir(parent).await?;
            }
            tokio::fs::copy(&source, &target).await.with_context(|| {
                format!(
                    "failed to project native-home file '{}' to '{}'",
                    source.display(),
                    target.display()
                )
            })?;
            set_private_file_permissions(&target).await?;
        }
        Ok(())
    }
}

async fn validate_source_root(source: &Path) -> Result<()> {
    let metadata = tokio::fs::symlink_metadata(source)
        .await
        .with_context(|| format!("failed to inspect native home '{}'", source.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!(
            "native home '{}' must be a directory and not a symlink",
            source.display()
        );
    }
    Ok(())
}

#[async_trait]
impl RuntimeAuthProvider for NativeHomeAuthProvider {
    fn kind(&self) -> &'static str {
        NATIVE_HOME_AUTH_KIND
    }

    async fn validate(&self, _context: &RuntimeAuthContext) -> Result<()> {
        self.validate_source().await
    }

    async fn prepare(&self, input: RuntimeAuthPreparation<'_>) -> Result<Vec<(String, String)>> {
        let runtime_home = input.runtime_home_root.ok_or_else(|| {
            anyhow::anyhow!(
                "runtime '{}' has no writable runtime home for native-home auth",
                input.runtime_id
            )
        })?;
        self.project(runtime_home).await?;
        Ok(Vec::new())
    }
}

async fn validate_source_file(root: &Path, relative: &Path, required: bool) -> Result<bool> {
    let mut current = root.to_path_buf();
    let components = relative.components().collect::<Vec<_>>();
    for (index, component) in components.iter().enumerate() {
        let Component::Normal(name) = component else {
            bail!(
                "native-home file '{}' must be a clean relative path",
                relative.display()
            );
        };
        current.push(name);
        let metadata = match tokio::fs::symlink_metadata(&current).await {
            Ok(metadata) => metadata,
            Err(err) if !required && err.kind() == ErrorKind::NotFound => return Ok(false),
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to inspect native-home file '{}'", current.display())
                })
            }
        };
        if metadata.file_type().is_symlink() {
            bail!(
                "native-home file path '{}' contains symlink '{}'",
                relative.display(),
                current.display()
            );
        }
        let is_leaf = index + 1 == components.len();
        if (is_leaf && !metadata.is_file()) || (!is_leaf && !metadata.is_dir()) {
            bail!(
                "native-home file path '{}' has invalid component '{}'",
                relative.display(),
                current.display()
            );
        }
    }
    Ok(true)
}

async fn create_private_dir(path: &Path) -> Result<()> {
    tokio::fs::create_dir_all(path).await.with_context(|| {
        format!(
            "failed to create native-home directory '{}'",
            path.display()
        )
    })?;
    set_private_dir_permissions(path).await
}

#[cfg(unix)]
async fn set_private_dir_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
        .await
        .with_context(|| {
            format!(
                "failed to protect native-home directory '{}'",
                path.display()
            )
        })
}

#[cfg(not(unix))]
async fn set_private_dir_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
async fn set_private_file_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .await
        .with_context(|| format!("failed to protect native-home file '{}'", path.display()))
}

#[cfg(not(unix))]
async fn set_private_file_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lionclaw_runtime_api::{NetworkMode, RuntimeAuthContext, RuntimeAuthPreparation};
    use std::path::PathBuf;

    fn config(source: PathBuf) -> NativeHomeAuthConfig {
        NativeHomeAuthConfig {
            source,
            target: PathBuf::from(".example"),
            required_files: vec![PathBuf::from("config.toml")],
            optional_files: vec![PathBuf::from("auth/session.json")],
        }
    }

    #[tokio::test]
    async fn projects_only_declared_files_into_the_ephemeral_home() {
        let source = tempfile::tempdir().expect("source");
        let runtime = tempfile::tempdir().expect("runtime");
        tokio::fs::create_dir_all(source.path().join("auth"))
            .await
            .unwrap();
        tokio::fs::write(source.path().join("config.toml"), b"model = 'x'\n")
            .await
            .unwrap();
        tokio::fs::write(source.path().join("auth/session.json"), b"secret")
            .await
            .unwrap();
        tokio::fs::write(source.path().join("unlisted.txt"), b"private")
            .await
            .unwrap();
        let provider = NativeHomeAuthProvider::new(config(source.path().to_path_buf()));
        let context = RuntimeAuthContext::default();

        let environment = provider
            .prepare(RuntimeAuthPreparation {
                runtime_id: "example",
                network_mode: NetworkMode::On,
                runtime_home_root: Some(runtime.path()),
                host_context: &context,
            })
            .await
            .expect("project native home");

        assert!(environment.is_empty());
        assert_eq!(
            tokio::fs::read_to_string(runtime.path().join(".example/config.toml"))
                .await
                .unwrap(),
            "model = 'x'\n"
        );
        assert_eq!(
            tokio::fs::read(runtime.path().join(".example/auth/session.json"))
                .await
                .unwrap(),
            b"secret"
        );
        assert!(!runtime.path().join(".example/unlisted.txt").exists());
    }

    #[tokio::test]
    async fn rejects_missing_required_files_and_source_symlinks() {
        let source = tempfile::tempdir().expect("source");
        let runtime = tempfile::tempdir().expect("runtime");
        let provider = NativeHomeAuthProvider::new(config(source.path().to_path_buf()));
        let context = RuntimeAuthContext::default();
        let err = provider
            .prepare(RuntimeAuthPreparation {
                runtime_id: "example",
                network_mode: NetworkMode::On,
                runtime_home_root: Some(runtime.path()),
                host_context: &context,
            })
            .await
            .expect_err("required config is missing");
        assert!(err.to_string().contains("config.toml"), "got {err:#}");

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            tokio::fs::write(source.path().join("real.toml"), b"x")
                .await
                .unwrap();
            symlink("real.toml", source.path().join("config.toml")).unwrap();
            let err = provider
                .validate(&context)
                .await
                .expect_err("symlinked auth input");
            assert!(err.to_string().contains("contains symlink"), "got {err:#}");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_symlinked_directories_inside_the_source() {
        use std::os::unix::fs::symlink;

        let source = tempfile::tempdir().expect("source");
        let outside = tempfile::tempdir().expect("outside");
        tokio::fs::write(source.path().join("config.toml"), b"config")
            .await
            .unwrap();
        tokio::fs::write(outside.path().join("session.json"), b"secret")
            .await
            .unwrap();
        symlink(outside.path(), source.path().join("auth")).unwrap();
        let provider = NativeHomeAuthProvider::new(config(source.path().to_path_buf()));

        let err = provider
            .validate(&RuntimeAuthContext::default())
            .await
            .expect_err("intermediate symlink must not escape the source");

        assert!(err.to_string().contains("contains symlink"), "got {err:#}");
    }
}
