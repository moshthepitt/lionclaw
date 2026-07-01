use std::{
    collections::BTreeMap,
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::program::NetworkMode;

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
    pub network_mode: NetworkMode,
    pub runtime_home_root: Option<&'a Path>,
    pub host_context: &'a RuntimeAuthContext,
}

#[async_trait]
pub trait RuntimeAuthProvider: Send + Sync {
    fn kind(&self) -> &'static str;

    async fn validate(&self, context: &RuntimeAuthContext) -> Result<()>;

    async fn prepare(&self, input: RuntimeAuthPreparation<'_>) -> Result<Vec<(String, String)>>;

    fn host_home_override_env(&self) -> Option<&'static str> {
        None
    }

    fn identity(&self, _context: &RuntimeAuthContext) -> Result<Option<String>> {
        Ok(None)
    }

    fn guidance(&self) -> Option<&'static str> {
        None
    }
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

    pub fn get(&self, auth: &RuntimeAuthKind) -> Option<Arc<dyn RuntimeAuthProvider>> {
        self.get_kind(auth.as_str())
    }

    pub fn get_kind(&self, kind: &str) -> Option<Arc<dyn RuntimeAuthProvider>> {
        self.providers.get(kind).cloned()
    }

    pub fn providers(&self) -> impl Iterator<Item = Arc<dyn RuntimeAuthProvider>> + '_ {
        self.providers.values().cloned()
    }

    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }
}

impl fmt::Debug for RuntimeAuthRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeAuthRegistry")
            .field("providers", &self.providers.keys().collect::<Vec<_>>())
            .finish()
    }
}
