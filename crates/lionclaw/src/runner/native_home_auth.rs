use std::{
    ffi::OsStr,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use lionclaw_durable_fs::RootedDirectory;
use lionclaw_runtime_api::{
    RuntimeAuthIdentity, RuntimeAuthKind, RuntimeAuthMaterialization, RuntimeAuthPreparation,
    RuntimeAuthProjection, RuntimeAuthProvider, RuntimeCredentialProjection,
    MAX_RUNTIME_CREDENTIAL_AGGREGATE_BYTES, MAX_RUNTIME_CREDENTIAL_BYTES,
};
use sha2::{Digest, Sha256};

use crate::{
    config::NativeHomeAuthConfig,
    credential_file::{open_regular_file_beneath, read_open_file_bounded},
};

pub(crate) const NATIVE_HOME_AUTH_KIND: &str = "native-home";

#[derive(Debug, Clone)]
pub(crate) struct NativeHomeAuthProvider {
    config: NativeHomeAuthConfig,
}

impl NativeHomeAuthProvider {
    pub(crate) fn new(config: NativeHomeAuthConfig) -> Self {
        Self { config }
    }

    async fn materialize(&self, staging_root: &Path) -> Result<RuntimeAuthMaterialization> {
        let config = self.config.clone();
        let staging_root = staging_root.to_path_buf();
        tokio::task::spawn_blocking(move || materialize_native_home(&config, &staging_root))
            .await
            .context("failed to join native-home credential materialization task")?
    }
}

#[async_trait]
impl RuntimeAuthProvider for NativeHomeAuthProvider {
    fn kind(&self) -> &'static str {
        NATIVE_HOME_AUTH_KIND
    }

    async fn prepare(
        &self,
        input: RuntimeAuthPreparation<'_>,
    ) -> Result<RuntimeAuthMaterialization> {
        let staging_root = input.auth_staging_root.ok_or_else(|| {
            anyhow!(
                "runtime '{}' has no effect-owned auth staging root for native-home auth",
                input.runtime_id
            )
        })?;
        self.materialize(staging_root).await
    }
}

fn materialize_native_home(
    config: &NativeHomeAuthConfig,
    staging_root: &Path,
) -> Result<RuntimeAuthMaterialization> {
    let staged_files = RootedDirectory::new(staging_root, staging_root)?;
    let declarations = canonical_declarations(config);
    let result = (|| {
        let mut digest = Sha256::new();
        digest_field(
            &mut digest,
            b"domain",
            b"lionclaw-native-home-auth-identity-v1",
        );
        digest_field(&mut digest, b"source", config.source.as_os_str().as_bytes());
        let mut aggregate_bytes = 0_usize;
        let mut credentials = Vec::new();

        for (index, (relative, required)) in declarations.iter().enumerate() {
            digest_field(
                &mut digest,
                b"declaration",
                if *required { b"required" } else { b"optional" },
            );
            digest_field(&mut digest, b"path", relative.as_os_str().as_bytes());
            let staged_name = format!("native-home-credential-{index:04}");
            let Some(source) = open_regular_file_beneath(
                &config.source,
                relative,
                *required,
                "native-home credential",
            )?
            else {
                digest_field(&mut digest, b"presence", b"absent");
                staged_files.remove_file(
                    OsStr::new(&staged_name),
                    "stale staged native-home credential",
                )?;
                continue;
            };
            let contents = read_open_file_bounded(
                source,
                &config.source.join(relative),
                MAX_RUNTIME_CREDENTIAL_BYTES,
                "native-home credential",
            )?;
            aggregate_bytes = aggregate_bytes
                .checked_add(contents.len())
                .ok_or_else(|| anyhow!("native-home credential aggregate size overflow"))?;
            if aggregate_bytes > MAX_RUNTIME_CREDENTIAL_AGGREGATE_BYTES {
                bail!(
                    "native-home credentials exceed the {} byte aggregate limit",
                    MAX_RUNTIME_CREDENTIAL_AGGREGATE_BYTES
                );
            }

            digest_field(&mut digest, b"presence", b"present");
            digest_field(&mut digest, b"content", &contents);
            staged_files.write_private_atomic(
                OsStr::new(&staged_name),
                &contents,
                MAX_RUNTIME_CREDENTIAL_BYTES,
                "staged native-home credential",
            )?;
            credentials.push(
                RuntimeCredentialProjection::new(&staged_name, config.target.join(relative))
                    .map_err(anyhow::Error::msg)?,
            );
        }

        let identity = RuntimeAuthIdentity::new(format!(
            "native-home-auth:v1:{}",
            hex::encode(digest.finalize())
        ))
        .map_err(anyhow::Error::msg)?;
        Ok(RuntimeAuthMaterialization::new(
            RuntimeAuthKind::from_static(NATIVE_HOME_AUTH_KIND),
            identity,
            RuntimeAuthProjection::new(Vec::new(), credentials),
        ))
    })();

    match result {
        Ok(materialization) => Ok(materialization),
        Err(error) => {
            if let Err(cleanup_error) = clear_staged_credentials(&staged_files, declarations.len())
            {
                return Err(error.context(format!(
                    "failed to clear incomplete native-home credentials: {cleanup_error:#}"
                )));
            }
            Err(error)
        }
    }
}

fn canonical_declarations(config: &NativeHomeAuthConfig) -> Vec<(PathBuf, bool)> {
    let mut required = config.required_files.clone();
    required.sort_by(|left, right| {
        left.as_os_str()
            .as_bytes()
            .cmp(right.as_os_str().as_bytes())
    });
    let mut optional = config.optional_files.clone();
    optional.sort_by(|left, right| {
        left.as_os_str()
            .as_bytes()
            .cmp(right.as_os_str().as_bytes())
    });
    required
        .into_iter()
        .map(|path| (path, true))
        .chain(optional.into_iter().map(|path| (path, false)))
        .collect()
}

fn clear_staged_credentials(staged_files: &RootedDirectory, count: usize) -> Result<()> {
    for index in 0..count {
        staged_files.remove_file(
            OsStr::new(&format!("native-home-credential-{index:04}")),
            "incomplete staged native-home credential",
        )?;
    }
    Ok(())
}

fn digest_field(digest: &mut Sha256, label: &[u8], value: &[u8]) {
    digest.update(label.len().to_be_bytes());
    digest.update(label);
    digest.update(value.len().to_be_bytes());
    digest.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use lionclaw_runtime_api::{NetworkGrant, RuntimeAuthContext, RuntimeAuthPreparation};
    use std::path::PathBuf;

    fn config(source: PathBuf) -> NativeHomeAuthConfig {
        NativeHomeAuthConfig {
            source,
            target: PathBuf::from(".example"),
            required_files: vec![PathBuf::from("config.toml")],
            optional_files: vec![PathBuf::from("auth/session.json")],
        }
    }

    fn model_network() -> NetworkGrant {
        NetworkGrant::allow_single("api.example.com", 443).expect("model network grant")
    }

    async fn materialize(
        provider: &NativeHomeAuthProvider,
        staging: &Path,
    ) -> RuntimeAuthMaterialization {
        let context = RuntimeAuthContext::default();
        let network = model_network();
        provider
            .prepare(RuntimeAuthPreparation {
                runtime_id: "example",
                network: &network,
                auth_staging_root: Some(staging),
                host_context: &context,
            })
            .await
            .expect("materialize native-home auth")
    }

    #[tokio::test]
    async fn identity_tracks_credential_rotation_at_the_same_source_path() {
        let source = tempfile::tempdir().expect("source");
        let staging = tempfile::tempdir().expect("staging");
        std::fs::write(source.path().join("config.toml"), b"token-a").unwrap();
        let provider = NativeHomeAuthProvider::new(config(source.path().to_path_buf()));
        let first = materialize(&provider, staging.path())
            .await
            .identity()
            .as_str()
            .to_string();

        std::fs::write(source.path().join("config.toml"), b"token-b").unwrap();
        let second = materialize(&provider, staging.path())
            .await
            .identity()
            .as_str()
            .to_string();

        assert_ne!(first, second);
        assert_eq!(
            std::fs::read(staging.path().join("native-home-credential-0000")).unwrap(),
            b"token-b"
        );
    }

    #[tokio::test]
    async fn identity_is_stable_for_unchanged_credential_bytes() {
        let source = tempfile::tempdir().expect("source");
        let staging = tempfile::tempdir().expect("staging");
        std::fs::write(source.path().join("config.toml"), b"stable-token").unwrap();
        let provider = NativeHomeAuthProvider::new(config(source.path().to_path_buf()));

        assert_eq!(
            materialize(&provider, staging.path()).await.identity(),
            materialize(&provider, staging.path()).await.identity()
        );
    }

    #[tokio::test]
    async fn declaration_order_does_not_change_identity_or_projection() {
        let source = tempfile::tempdir().expect("source");
        let first_staging = tempfile::tempdir().expect("first staging");
        let second_staging = tempfile::tempdir().expect("second staging");
        std::fs::create_dir(source.path().join("auth")).unwrap();
        for (path, contents) in [
            ("a.toml", b"required-a".as_slice()),
            ("z.toml", b"required-z".as_slice()),
            ("auth/a.json", b"optional-a".as_slice()),
            ("auth/z.json", b"optional-z".as_slice()),
        ] {
            std::fs::write(source.path().join(path), contents).unwrap();
        }
        let config = |reverse| {
            let mut required_files = vec![PathBuf::from("a.toml"), PathBuf::from("z.toml")];
            let mut optional_files =
                vec![PathBuf::from("auth/a.json"), PathBuf::from("auth/z.json")];
            if reverse {
                required_files.reverse();
                optional_files.reverse();
            }
            NativeHomeAuthConfig {
                source: source.path().to_path_buf(),
                target: PathBuf::from(".example"),
                required_files,
                optional_files,
            }
        };
        let first = materialize(
            &NativeHomeAuthProvider::new(config(false)),
            first_staging.path(),
        )
        .await;
        let second = materialize(
            &NativeHomeAuthProvider::new(config(true)),
            second_staging.path(),
        )
        .await;

        assert_eq!(first.identity(), second.identity());
        assert_eq!(first.projection(), second.projection());
        assert_eq!(
            first
                .projection()
                .credentials()
                .iter()
                .map(|credential| credential.native_home_target())
                .collect::<Vec<_>>(),
            vec![
                Path::new(".example/a.toml"),
                Path::new(".example/z.toml"),
                Path::new(".example/auth/a.json"),
                Path::new(".example/auth/z.json"),
            ]
        );
    }

    #[tokio::test]
    async fn identity_tracks_optional_credential_presence() {
        let source = tempfile::tempdir().expect("source");
        let staging = tempfile::tempdir().expect("staging");
        std::fs::write(source.path().join("config.toml"), b"config").unwrap();
        let provider = NativeHomeAuthProvider::new(config(source.path().to_path_buf()));
        let absent = materialize(&provider, staging.path())
            .await
            .identity()
            .clone();

        std::fs::create_dir(source.path().join("auth")).unwrap();
        std::fs::write(source.path().join("auth/session.json"), b"session").unwrap();
        let present = materialize(&provider, staging.path())
            .await
            .identity()
            .clone();

        assert_ne!(absent, present);
    }

    #[tokio::test]
    async fn identity_does_not_retain_credential_text() {
        let source = tempfile::tempdir().expect("source");
        let staging = tempfile::tempdir().expect("staging");
        let credential = "credential-text-that-must-not-appear";
        std::fs::write(source.path().join("config.toml"), credential).unwrap();
        let provider = NativeHomeAuthProvider::new(config(source.path().to_path_buf()));

        let identity = materialize(&provider, staging.path())
            .await
            .identity()
            .as_str()
            .to_string();

        assert!(!identity.contains(credential));
        assert_eq!(
            identity.len(),
            "native-home-auth:v1:".len() + Sha256::output_size() * 2
        );
    }

    #[tokio::test]
    async fn stages_only_declared_files_and_returns_exact_native_home_targets() {
        use std::os::unix::fs::PermissionsExt;

        let source = tempfile::tempdir().expect("source");
        let staging = tempfile::tempdir().expect("staging");
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
        let network = model_network();

        let materialization = provider
            .prepare(RuntimeAuthPreparation {
                runtime_id: "example",
                network: &network,
                auth_staging_root: Some(staging.path()),
                host_context: &context,
            })
            .await
            .expect("stage native-home auth");
        let projection = materialization.projection();

        assert_eq!(materialization.kind().as_str(), NATIVE_HOME_AUTH_KIND);
        assert!(projection.environment().is_empty());
        assert_eq!(
            projection
                .credentials()
                .iter()
                .map(|credential| (
                    credential.staged_source().to_path_buf(),
                    credential.native_home_target().to_path_buf()
                ))
                .collect::<Vec<_>>(),
            vec![
                (
                    PathBuf::from("native-home-credential-0000"),
                    PathBuf::from(".example/config.toml"),
                ),
                (
                    PathBuf::from("native-home-credential-0001"),
                    PathBuf::from(".example/auth/session.json"),
                ),
            ]
        );
        assert_eq!(
            tokio::fs::read_to_string(staging.path().join("native-home-credential-0000"))
                .await
                .unwrap(),
            "model = 'x'\n"
        );
        assert_eq!(
            tokio::fs::read(staging.path().join("native-home-credential-0001"))
                .await
                .unwrap(),
            b"secret"
        );
        assert!(!staging.path().join("unlisted.txt").exists());
        for credential in projection.credentials() {
            assert_eq!(
                std::fs::metadata(staging.path().join(credential.staged_source()))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }

    #[tokio::test]
    async fn rejects_missing_required_files_and_source_symlinks() {
        let source = tempfile::tempdir().expect("source");
        let staging = tempfile::tempdir().expect("staging");
        let provider = NativeHomeAuthProvider::new(config(source.path().to_path_buf()));
        let context = RuntimeAuthContext::default();
        let network = model_network();
        let err = provider
            .prepare(RuntimeAuthPreparation {
                runtime_id: "example",
                network: &network,
                auth_staging_root: Some(staging.path()),
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
                .prepare(RuntimeAuthPreparation {
                    runtime_id: "example",
                    network: &network,
                    auth_staging_root: Some(staging.path()),
                    host_context: &context,
                })
                .await
                .expect_err("symlinked auth input");
            assert!(err.to_string().contains("contains symlink"), "got {err:#}");
        }
    }

    #[tokio::test]
    async fn rejects_oversized_credentials_before_staging() {
        let source = tempfile::tempdir().expect("source");
        let staging = tempfile::tempdir().expect("staging");
        let credential = std::fs::File::create(source.path().join("config.toml")).unwrap();
        credential
            .set_len((MAX_RUNTIME_CREDENTIAL_BYTES + 1) as u64)
            .unwrap();
        let provider = NativeHomeAuthProvider::new(config(source.path().to_path_buf()));
        let context = RuntimeAuthContext::default();
        let network = model_network();

        let error = provider
            .prepare(RuntimeAuthPreparation {
                runtime_id: "example",
                network: &network,
                auth_staging_root: Some(staging.path()),
                host_context: &context,
            })
            .await
            .expect_err("oversized credential must fail before publication");

        assert!(format!("{error:#}").contains("exceeds"));
        assert_eq!(std::fs::read_dir(staging.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn rejects_aggregate_overflow_and_removes_partial_staging() {
        let source = tempfile::tempdir().expect("source");
        let staging = tempfile::tempdir().expect("staging");
        let required_files = (0..5)
            .map(|index| PathBuf::from(format!("credential-{index}")))
            .collect::<Vec<_>>();
        for path in &required_files {
            std::fs::File::create(source.path().join(path))
                .unwrap()
                .set_len(MAX_RUNTIME_CREDENTIAL_BYTES as u64)
                .unwrap();
        }
        let provider = NativeHomeAuthProvider::new(NativeHomeAuthConfig {
            source: source.path().to_path_buf(),
            target: PathBuf::from(".example"),
            required_files,
            optional_files: Vec::new(),
        });
        let context = RuntimeAuthContext::default();
        let network = model_network();

        let error = provider
            .prepare(RuntimeAuthPreparation {
                runtime_id: "example",
                network: &network,
                auth_staging_root: Some(staging.path()),
                host_context: &context,
            })
            .await
            .expect_err("aggregate overflow must fail before publication");

        assert!(format!("{error:#}").contains("aggregate limit"));
        assert_eq!(std::fs::read_dir(staging.path()).unwrap().count(), 0);
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

        let context = RuntimeAuthContext::default();
        let network = model_network();
        let staging = tempfile::tempdir().expect("staging");
        let err = provider
            .prepare(RuntimeAuthPreparation {
                runtime_id: "example",
                network: &network,
                auth_staging_root: Some(staging.path()),
                host_context: &context,
            })
            .await
            .expect_err("intermediate symlink must not escape the source");

        assert!(err.to_string().contains("contains symlink"), "got {err:#}");
    }

    #[tokio::test]
    async fn removes_a_stale_optional_staged_credential_when_the_source_disappears() {
        let source = tempfile::tempdir().expect("source");
        let staging = tempfile::tempdir().expect("staging");
        tokio::fs::create_dir_all(source.path().join("auth"))
            .await
            .unwrap();
        tokio::fs::write(source.path().join("config.toml"), b"config")
            .await
            .unwrap();
        let optional = source.path().join("auth/session.json");
        tokio::fs::write(&optional, b"secret").await.unwrap();
        let provider = NativeHomeAuthProvider::new(config(source.path().to_path_buf()));
        let context = RuntimeAuthContext::default();
        let network = model_network();
        let input = || RuntimeAuthPreparation {
            runtime_id: "example",
            network: &network,
            auth_staging_root: Some(staging.path()),
            host_context: &context,
        };

        assert_eq!(
            provider
                .prepare(input())
                .await
                .unwrap()
                .projection()
                .credentials()
                .len(),
            2
        );
        tokio::fs::remove_file(optional).await.unwrap();
        assert_eq!(
            provider
                .prepare(input())
                .await
                .unwrap()
                .projection()
                .credentials()
                .len(),
            1
        );
        assert!(!staging.path().join("native-home-credential-0001").exists());
    }
}
