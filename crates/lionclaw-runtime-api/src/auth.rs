use std::{
    collections::BTreeMap,
    fmt,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::NetworkGrant;

pub const MAX_RUNTIME_CREDENTIAL_BYTES: usize = 1024 * 1024;
/// Provider-neutral headroom over the four-file built-in native-home profile.
pub const MAX_RUNTIME_CREDENTIAL_PROJECTIONS: usize = 16;
/// Allows every file in the largest built-in profile to reach the per-file cap.
pub const MAX_RUNTIME_CREDENTIAL_AGGREGATE_BYTES: usize = 4 * MAX_RUNTIME_CREDENTIAL_BYTES;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RuntimeAuthKind(String);

impl RuntimeAuthKind {
    pub fn new(kind: impl Into<String>) -> Result<Self, String> {
        let kind = kind.into().trim().to_string();
        if kind.is_empty() {
            return Err("runtime auth kind is required".to_string());
        }
        Ok(Self(kind))
    }

    pub fn from_static(kind: &'static str) -> Self {
        Self(kind.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RuntimeAuthKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RuntimeAuthIdentity(String);

impl RuntimeAuthIdentity {
    pub fn new(identity: impl Into<String>) -> Result<Self, String> {
        let identity = identity.into();
        if identity.is_empty() {
            return Err("runtime auth identity is required".to_string());
        }
        Ok(Self(identity))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for RuntimeAuthIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RuntimeAuthIdentity([REDACTED])")
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeAuthContext {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    home_overrides: BTreeMap<String, PathBuf>,
}

impl RuntimeAuthContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_home_override(mut self, kind: impl AsRef<str>, path: impl Into<PathBuf>) -> Self {
        self.insert_home_override(kind, path);
        self
    }

    pub fn insert_home_override(&mut self, kind: impl AsRef<str>, path: impl Into<PathBuf>) {
        let kind = kind.as_ref().trim();
        if !kind.is_empty() {
            self.home_overrides.insert(kind.to_string(), path.into());
        }
    }

    pub fn home_override(&self, kind: &str) -> Option<&Path> {
        self.home_overrides.get(kind).map(PathBuf::as_path)
    }

    pub fn home_overrides(&self) -> &BTreeMap<String, PathBuf> {
        &self.home_overrides
    }

    pub fn is_empty(&self) -> bool {
        self.home_overrides.is_empty()
    }
}

pub struct RuntimeAuthPreparation<'a> {
    pub runtime_id: &'a str,
    pub network: &'a NetworkGrant,
    pub auth_staging_root: Option<&'a Path>,
    pub host_context: &'a RuntimeAuthContext,
}

#[derive(Clone, PartialEq, Eq)]
pub struct RuntimeCredentialProjection {
    staged_source: PathBuf,
    native_home_target: PathBuf,
}

impl RuntimeCredentialProjection {
    pub fn new(
        staged_source: impl Into<PathBuf>,
        native_home_target: impl Into<PathBuf>,
    ) -> Result<Self, String> {
        Ok(Self {
            staged_source: normalized_relative_path(
                staged_source.into(),
                "staged credential source",
            )?,
            native_home_target: normalized_relative_path(
                native_home_target.into(),
                "native-home credential target",
            )?,
        })
    }

    pub fn staged_source(&self) -> &Path {
        &self.staged_source
    }

    pub fn native_home_target(&self) -> &Path {
        &self.native_home_target
    }
}

impl fmt::Debug for RuntimeCredentialProjection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeCredentialProjection")
            .field("staged_source", &self.staged_source)
            .field("native_home_target", &self.native_home_target)
            .finish()
    }
}

#[derive(Clone, Default, PartialEq, Eq)]
pub struct RuntimeAuthProjection {
    environment: Vec<(String, String)>,
    credentials: Vec<RuntimeCredentialProjection>,
}

impl RuntimeAuthProjection {
    pub fn new(
        environment: Vec<(String, String)>,
        credentials: Vec<RuntimeCredentialProjection>,
    ) -> Self {
        Self {
            environment,
            credentials,
        }
    }

    pub fn environment(&self) -> &[(String, String)] {
        &self.environment
    }

    pub fn credentials(&self) -> &[RuntimeCredentialProjection] {
        &self.credentials
    }
}

impl fmt::Debug for RuntimeAuthProjection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeAuthProjection")
            .field("environment_entries", &self.environment.len())
            .field("credentials", &self.credentials)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RuntimeAuthMaterialization {
    kind: RuntimeAuthKind,
    identity: RuntimeAuthIdentity,
    projection: RuntimeAuthProjection,
}

impl RuntimeAuthMaterialization {
    pub fn new(
        kind: RuntimeAuthKind,
        identity: RuntimeAuthIdentity,
        projection: RuntimeAuthProjection,
    ) -> Self {
        Self {
            kind,
            identity,
            projection,
        }
    }

    pub fn kind(&self) -> &RuntimeAuthKind {
        &self.kind
    }

    pub fn identity(&self) -> &RuntimeAuthIdentity {
        &self.identity
    }

    pub fn projection(&self) -> &RuntimeAuthProjection {
        &self.projection
    }
}

impl fmt::Debug for RuntimeAuthMaterialization {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeAuthMaterialization")
            .field("kind", &self.kind)
            .field("identity", &self.identity)
            .field("projection", &self.projection)
            .finish()
    }
}

fn normalized_relative_path(path: PathBuf, label: &str) -> Result<PathBuf, String> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        let Component::Normal(name) = component else {
            return Err(format!(
                "{label} '{}' must be a clean relative path",
                path.display()
            ));
        };
        normalized.push(name);
    }
    if normalized.as_os_str().is_empty() {
        return Err(format!("{label} is required"));
    }
    Ok(normalized)
}

#[async_trait]
pub trait RuntimeAuthProvider: Send + Sync {
    fn kind(&self) -> &'static str;

    async fn prepare(
        &self,
        input: RuntimeAuthPreparation<'_>,
    ) -> Result<RuntimeAuthMaterialization>;
}

#[derive(Clone, Default)]
pub struct RuntimeAuthRegistry {
    providers: Arc<BTreeMap<String, Arc<dyn RuntimeAuthProvider>>>,
}

impl RuntimeAuthRegistry {
    pub fn new(providers: impl IntoIterator<Item = Arc<dyn RuntimeAuthProvider>>) -> Self {
        let providers = providers
            .into_iter()
            .map(|provider| (provider.kind().to_string(), provider))
            .collect();
        Self {
            providers: Arc::new(providers),
        }
    }

    pub fn empty() -> Self {
        Self::default()
    }

    pub fn get_kind(&self, kind: &str) -> Option<Arc<dyn RuntimeAuthProvider>> {
        self.providers.get(kind).cloned()
    }
}

impl fmt::Debug for RuntimeAuthRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeAuthRegistry")
            .field("providers", &self.providers.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_projection_normalizes_clean_relative_paths() {
        let projection = RuntimeCredentialProjection::new("staged//auth.json", ".codex/auth.json/")
            .expect("credential projection");

        assert_eq!(projection.staged_source(), Path::new("staged/auth.json"));
        assert_eq!(
            projection.native_home_target(),
            Path::new(".codex/auth.json")
        );
    }

    #[test]
    fn credential_projection_rejects_empty_absolute_and_traversing_paths() {
        for (source, target) in [
            ("", ".codex/auth.json"),
            ("/host/auth.json", ".codex/auth.json"),
            ("auth.json", ""),
            ("auth.json", "/runtime/home/.codex/auth.json"),
            ("../auth.json", ".codex/auth.json"),
            ("auth.json", ".codex/../auth.json"),
        ] {
            assert!(
                RuntimeCredentialProjection::new(source, target).is_err(),
                "accepted source={source:?} target={target:?}"
            );
        }
    }

    #[test]
    fn auth_projection_debug_redacts_environment_values() {
        let projection = RuntimeAuthProjection::new(
            vec![("OPENAI_API_KEY".into(), "sk-secret".into())],
            vec![RuntimeCredentialProjection::new("auth", ".codex/auth.json").unwrap()],
        );

        let debug = format!("{projection:?}");
        assert!(!debug.contains("sk-secret"));
        assert!(debug.contains("environment_entries"));
        assert!(debug.contains(".codex/auth.json"));
    }

    #[test]
    fn materialization_debug_redacts_the_profile_identity() {
        let materialization = RuntimeAuthMaterialization::new(
            RuntimeAuthKind::from_static("test"),
            RuntimeAuthIdentity::new("principal-that-must-not-be-logged").unwrap(),
            RuntimeAuthProjection::default(),
        );

        let debug = format!("{materialization:?}");
        assert!(!debug.contains("principal-that-must-not-be-logged"));
        assert!(debug.contains("[REDACTED]"));
    }
}
