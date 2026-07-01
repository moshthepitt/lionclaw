use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::{
    adapter::RuntimeAdapter,
    auth::{RuntimeAuthKind, RuntimeAuthProvider},
};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct RuntimeTerminalConfig {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
}

impl RuntimeTerminalConfig {
    pub fn is_empty(&self) -> bool {
        self.args.is_empty()
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

    fn auth_provider(&self) -> Option<Arc<dyn RuntimeAuthProvider>> {
        None
    }
}
