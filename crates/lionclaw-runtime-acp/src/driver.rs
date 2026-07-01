use std::sync::Arc;

use lionclaw_runtime_api::{
    RuntimeAdapter, RuntimeAuthKind, RuntimeDriverConfig, RuntimeDriverProvider,
    RuntimeTerminalConfig,
};

use crate::adapter::AcpRuntimeAdapter;

pub const ACP_PROTOCOL_NAME: &str = "acp";
pub const ACP_DEFAULT_WORKING_DIR: &str = "/workspace";
pub const ACP_SESSION_ID_STATE_FILE: &str = ".lionclaw-acp-session-id";
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcpRuntimeConfig {
    pub runtime_id: String,
    pub executable: String,
    pub args: Vec<String>,
    pub environment: Vec<(String, String)>,
    pub model: Option<String>,
    pub mode: Option<String>,
    pub auth: Option<RuntimeAuthKind>,
    pub terminal: RuntimeTerminalConfig,
    pub session_id_state_file: String,
    pub default_working_dir: String,
}

impl AcpRuntimeConfig {
    pub(crate) fn normalized_runtime_id(&self) -> String {
        let runtime_id = self.runtime_id.trim();
        if runtime_id.is_empty() {
            "acp".to_string()
        } else {
            runtime_id.to_string()
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AcpRuntimeDriver;

impl RuntimeDriverProvider for AcpRuntimeDriver {
    fn driver(&self) -> &'static str {
        ACP_PROTOCOL_NAME
    }

    fn create_adapter(&self, config: RuntimeDriverConfig) -> Arc<dyn RuntimeAdapter> {
        Arc::new(AcpRuntimeAdapter::new(AcpRuntimeConfig {
            runtime_id: config.runtime_id,
            executable: config.executable,
            args: config.args,
            environment: config.environment,
            model: config.model,
            mode: config.mode,
            auth: config.auth,
            terminal: config.terminal,
            session_id_state_file: ACP_SESSION_ID_STATE_FILE.to_string(),
            default_working_dir: ACP_DEFAULT_WORKING_DIR.to_string(),
        }))
    }
}
