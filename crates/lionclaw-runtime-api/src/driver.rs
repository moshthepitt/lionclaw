use std::{collections::BTreeMap, fmt, sync::Arc};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::{adapter::RuntimeAdapter, auth::RuntimeAuthKind};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct RuntimeTerminalConfig {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resume_args: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_arg: Option<String>,
}

impl RuntimeTerminalConfig {
    pub fn is_empty(&self) -> bool {
        self.args.is_empty() && self.resume_args.is_empty() && self.message_arg.is_none()
    }

    pub fn validate(&self) -> Result<()> {
        if self
            .message_arg
            .as_ref()
            .is_some_and(|argument| argument.trim().is_empty())
        {
            anyhow::bail!("runtime terminal message argument must not be empty");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeDriverConfig {
    pub runtime_id: String,
    pub executable: String,
    pub args: Vec<String>,
    pub environment: Vec<(String, String)>,
    pub model: Option<String>,
    pub mode: Option<String>,
    pub auth: Option<RuntimeAuthKind>,
    pub terminal: RuntimeTerminalConfig,
}

pub trait RuntimeDriverProvider: Send + Sync {
    fn driver(&self) -> &'static str;

    fn validate_config(&self, _config: &RuntimeDriverConfig) -> Result<()> {
        Ok(())
    }

    fn create_adapter(&self, config: RuntimeDriverConfig) -> Arc<dyn RuntimeAdapter>;
}

/// Driver implementations keyed by their declared protocol identifier.
///
/// Runtime profiles select an entry from this registry; construction code
/// never needs to know which product a profile names.
#[derive(Clone, Default)]
pub struct RuntimeDriverRegistry {
    providers: Arc<BTreeMap<String, Arc<dyn RuntimeDriverProvider>>>,
}

impl RuntimeDriverRegistry {
    pub fn new(providers: impl IntoIterator<Item = Arc<dyn RuntimeDriverProvider>>) -> Self {
        let providers = providers
            .into_iter()
            .map(|provider| (provider.driver().to_string(), provider))
            .collect();
        Self {
            providers: Arc::new(providers),
        }
    }

    pub fn get(&self, driver: &str) -> Option<Arc<dyn RuntimeDriverProvider>> {
        self.providers.get(driver).cloned()
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.providers.keys().map(String::as_str)
    }
}

impl fmt::Debug for RuntimeDriverRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeDriverRegistry")
            .field("providers", &self.providers.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DeclaredDriver;

    impl RuntimeDriverProvider for DeclaredDriver {
        fn driver(&self) -> &'static str {
            "declared-protocol"
        }

        fn create_adapter(&self, _config: RuntimeDriverConfig) -> Arc<dyn RuntimeAdapter> {
            unreachable!("registry selection does not instantiate the adapter")
        }
    }

    #[test]
    fn profile_driver_is_selected_by_declared_registry_identity() {
        let registry =
            RuntimeDriverRegistry::new(
                [Arc::new(DeclaredDriver) as Arc<dyn RuntimeDriverProvider>],
            );

        assert!(registry.get("declared-protocol").is_some());
        assert!(registry.get("unregistered-product").is_none());
    }

    #[test]
    fn terminal_message_argument_must_be_explicit_and_nonempty() {
        RuntimeTerminalConfig {
            args: vec!["--tui".to_string()],
            resume_args: vec!["--continue".to_string()],
            message_arg: Some("--prompt".to_string()),
        }
        .validate()
        .unwrap();

        let error = RuntimeTerminalConfig {
            message_arg: Some(" ".to_string()),
            ..Default::default()
        }
        .validate()
        .unwrap_err();
        assert!(error.to_string().contains("must not be empty"));
    }
}
