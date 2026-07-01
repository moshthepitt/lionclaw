use std::sync::Arc;

use anyhow::{bail, Result};
use lionclaw_runtime_api::{
    RuntimeAdapter, RuntimeAuthKind, RuntimeDriverConfig, RuntimeDriverProvider,
};

use crate::{adapter::CodexRuntimeAdapter, host_auth::CodexRuntimeAuthProvider};

pub const CODEX_RUNTIME_DRIVER: &str = "codex";
pub const CODEX_RUNTIME_AUTH_KIND: &str = CODEX_RUNTIME_DRIVER;
pub const CODEX_DEFAULT_EXECUTABLE: &str = "codex";
pub const CODEX_SKILL_PROJECTION_ROOT: &str = ".codex/skills";

pub fn codex_runtime_auth_kind() -> RuntimeAuthKind {
    RuntimeAuthKind::from_static(CODEX_RUNTIME_AUTH_KIND)
}

#[derive(Debug, Clone)]
pub struct CodexRuntimeConfig {
    pub executable: String,
    pub model: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct CodexRuntimeDriver;

impl RuntimeDriverProvider for CodexRuntimeDriver {
    fn driver(&self) -> &'static str {
        CODEX_RUNTIME_DRIVER
    }

    fn validate_config(&self, config: &RuntimeDriverConfig) -> Result<()> {
        if !config.args.is_empty() {
            bail!("driver '{CODEX_RUNTIME_DRIVER}' does not support runtime args");
        }
        if !config.environment.is_empty() {
            bail!("driver '{CODEX_RUNTIME_DRIVER}' does not support runtime environment");
        }
        if config.mode.is_some() {
            bail!("driver '{CODEX_RUNTIME_DRIVER}' does not support runtime mode");
        }
        if !config.terminal.is_empty() {
            bail!("driver '{CODEX_RUNTIME_DRIVER}' does not support runtime terminal profile");
        }
        let expected = codex_runtime_auth_kind();
        match &config.auth {
            Some(auth) if auth == &expected => {}
            Some(auth) => {
                bail!(
                    "driver '{CODEX_RUNTIME_DRIVER}' requires auth kind '{}', got '{}'",
                    expected.as_str(),
                    auth.as_str()
                );
            }
            None => {
                bail!(
                    "driver '{CODEX_RUNTIME_DRIVER}' requires auth kind '{}'",
                    expected.as_str()
                );
            }
        }
        Ok(())
    }

    fn create_adapter(&self, config: RuntimeDriverConfig) -> Arc<dyn RuntimeAdapter> {
        Arc::new(CodexRuntimeAdapter::new(CodexRuntimeConfig {
            executable: config.executable,
            model: config.model,
        }))
    }

    fn auth_provider(&self) -> Option<Arc<dyn lionclaw_runtime_api::RuntimeAuthProvider>> {
        Some(Arc::new(CodexRuntimeAuthProvider))
    }
}

impl Default for CodexRuntimeConfig {
    fn default() -> Self {
        Self {
            executable: CODEX_DEFAULT_EXECUTABLE.to_string(),
            model: None,
        }
    }
}
