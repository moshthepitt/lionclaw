use std::fmt;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::auth::RuntimeAuthKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NetworkMode {
    None,
    On,
}

impl NetworkMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::On => "on",
        }
    }
}

/// Adapter-produced program invocation details, independent from how LionClaw
/// chooses to confine the process.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct RuntimeProgramSpec {
    pub executable: String,
    pub args: Vec<String>,
    pub environment: Vec<(String, String)>,
    pub stdin: String,
    pub auth: Option<RuntimeAuthKind>,
}

impl fmt::Debug for RuntimeProgramSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeProgramSpec")
            .field("executable", &self.executable)
            .field("args", &self.args)
            .field("environment_count", &self.environment.len())
            .field("stdin_len", &self.stdin.len())
            .field("auth", &self.auth)
            .finish()
    }
}

#[derive(Debug, Clone, Default)]
pub struct ExecutionOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: Option<i32>,
    pub exit_signal: Option<i32>,
}

impl ExecutionOutput {
    pub fn success(&self) -> bool {
        self.exit_code == Some(0) && self.exit_signal.is_none()
    }

    pub fn status_description(&self) -> String {
        if let Some(code) = self.exit_code {
            return format!("code {code}");
        }
        if let Some(signal) = self.exit_signal {
            return format!("signal {signal}");
        }
        "unknown status".to_string()
    }
}

#[async_trait]
pub trait RuntimeProgramSession: Send {
    async fn write_line(&mut self, line: &str) -> Result<()>;
    async fn read_line(&mut self) -> Result<Option<String>>;
    async fn shutdown(self: Box<Self>) -> Result<ExecutionOutput>;
}

pub type RuntimeProgramStdoutSender = mpsc::UnboundedSender<String>;

#[async_trait]
pub trait RuntimeProgramExecutor: Send {
    async fn execute_streaming(
        &mut self,
        program: RuntimeProgramSpec,
        stdout: RuntimeProgramStdoutSender,
    ) -> Result<ExecutionOutput>;

    async fn execute_captured(&mut self, program: RuntimeProgramSpec) -> Result<ExecutionOutput>;

    async fn spawn(
        &mut self,
        program: RuntimeProgramSpec,
    ) -> Result<Box<dyn RuntimeProgramSession>>;
}
