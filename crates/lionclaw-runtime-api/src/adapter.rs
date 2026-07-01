use std::path::PathBuf;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use uuid::Uuid;

use crate::{
    capability::{RuntimeCapabilityResult, RuntimeTurnResult},
    context::{RuntimeExecutionContext, RuntimeNativeHomeArtifactDir},
    event::{RuntimeEvent, RuntimeEventSender, RuntimeTurnJournalSender},
    program::{ExecutionOutput, RuntimeProgramExecutor, RuntimeProgramSpec},
    program_backed::{execute_program_backed_turn, RuntimeProgramOutputParser},
    state::RuntimeSessionReady,
};

#[derive(Debug, Clone)]
pub struct RuntimeAdapterInfo {
    pub id: String,
    pub version: String,
    pub healthy: bool,
}

#[derive(Debug, Clone)]
pub struct RuntimeSessionStartInput {
    pub session_id: Uuid,
    pub working_dir: Option<String>,
    pub environment: Vec<(String, String)>,
    pub runtime_skill_ids: Vec<String>,
    pub runtime_state_root: Option<PathBuf>,
    pub runtime_session_ready: RuntimeSessionReady,
}

#[derive(Debug, Clone)]
pub struct RuntimeSessionHandle {
    pub runtime_session_id: String,
    pub resumes_existing_session: bool,
}

#[derive(Debug, Clone)]
pub struct RuntimeTerminalProgramInput {
    pub session_id: Uuid,
    pub runtime_state_root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct RuntimeTurnInput {
    pub runtime_session_id: String,
    pub prompt: String,
    pub fresh_prompt: Option<String>,
    pub runtime_skill_ids: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeControlOrigin {
    SessionTurn,
    ChannelInbound,
}

impl RuntimeControlOrigin {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SessionTurn => "session_turn",
            Self::ChannelInbound => "channel_inbound",
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeControlInput {
    pub runtime_session_id: String,
    pub raw: String,
    pub command_name: String,
    pub arguments: String,
    pub origin: RuntimeControlOrigin,
    pub runtime_skill_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeControlOutcome {
    Handled {
        message: String,
    },
    Unsupported {
        message: String,
    },
    InteractiveOnly {
        message: String,
    },
    Failed {
        code: Option<String>,
        message: String,
    },
}

impl RuntimeControlOutcome {
    pub fn message(&self) -> &str {
        match self {
            Self::Handled { message }
            | Self::Unsupported { message }
            | Self::InteractiveOnly { message }
            | Self::Failed { message, .. } => message,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Handled { .. } => "handled",
            Self::Unsupported { .. } => "unsupported",
            Self::InteractiveOnly { .. } => "interactive_only",
            Self::Failed { .. } => "failed",
        }
    }

    pub fn failed_error_code(&self) -> Option<&str> {
        match self {
            Self::Failed { code, .. } => Some(code.as_deref().unwrap_or("runtime.control.failed")),
            Self::Handled { .. } | Self::Unsupported { .. } | Self::InteractiveOnly { .. } => None,
        }
    }
}

pub struct RuntimeControlExecution {
    pub input: RuntimeControlInput,
    pub context: RuntimeExecutionContext,
    pub executor: Box<dyn RuntimeProgramExecutor>,
}

pub struct RuntimeProgramTurnExecution {
    pub input: RuntimeTurnInput,
    pub context: RuntimeExecutionContext,
    pub executor: Box<dyn RuntimeProgramExecutor>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HiddenTurnSupport {
    Unsupported,
    SideEffectFree,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeTurnMode {
    Direct,
    ProgramBacked,
}

#[async_trait]
pub trait RuntimeAdapter: Send + Sync {
    async fn info(&self) -> RuntimeAdapterInfo;
    fn hidden_turn_support(&self) -> HiddenTurnSupport {
        HiddenTurnSupport::Unsupported
    }
    fn turn_mode(&self) -> RuntimeTurnMode {
        RuntimeTurnMode::Direct
    }
    fn native_home_artifact_dirs(&self) -> Result<Vec<RuntimeNativeHomeArtifactDir>> {
        Ok(Vec::new())
    }
    async fn session_start(&self, input: RuntimeSessionStartInput) -> Result<RuntimeSessionHandle>;
    async fn turn(
        &self,
        _input: RuntimeTurnInput,
        _journal: RuntimeTurnJournalSender,
    ) -> Result<RuntimeTurnResult> {
        Err(anyhow!("runtime does not implement direct turns"))
    }
    fn build_turn_program(
        &self,
        _input: &RuntimeTurnInput,
        _context: &RuntimeExecutionContext,
    ) -> Result<RuntimeProgramSpec> {
        Err(anyhow!("runtime does not support program-backed turns"))
    }
    fn build_terminal_program(
        &self,
        _input: RuntimeTerminalProgramInput,
    ) -> Result<RuntimeProgramSpec> {
        Err(anyhow!("runtime does not expose a native terminal UI"))
    }
    fn program_output_parser(
        &self,
        _input: &RuntimeTurnInput,
    ) -> Option<Box<dyn RuntimeProgramOutputParser>> {
        None
    }
    fn parse_program_output_line(&self, _line: &str) -> Vec<RuntimeEvent> {
        Vec::new()
    }
    fn format_program_exit_error(
        &self,
        output: &ExecutionOutput,
        observed_error_text: Option<&str>,
    ) -> String {
        if let Some(text) = observed_error_text.filter(|text| !text.trim().is_empty()) {
            format!(
                "runtime process exited with {}: {text}",
                output.status_description()
            )
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            if stderr.is_empty() {
                format!(
                    "runtime process exited with {}",
                    output.status_description()
                )
            } else {
                format!(
                    "runtime process exited with {}: {stderr}",
                    output.status_description()
                )
            }
        }
    }
    fn prepare_program_retry_after_failure(
        &self,
        _input: &RuntimeTurnInput,
        _output: &ExecutionOutput,
        _observed_error_text: Option<&str>,
        _journal: &RuntimeTurnJournalSender,
    ) -> Result<bool> {
        Ok(false)
    }
    async fn program_backed_turn(
        &self,
        execution: RuntimeProgramTurnExecution,
        journal: RuntimeTurnJournalSender,
    ) -> Result<RuntimeTurnResult> {
        execute_program_backed_turn(self, execution, journal).await
    }
    async fn runtime_control(
        &self,
        execution: RuntimeControlExecution,
        _events: RuntimeEventSender,
    ) -> Result<RuntimeControlOutcome> {
        Ok(RuntimeControlOutcome::Unsupported {
            message: format!(
                "runtime does not support native control command '/{}'",
                execution.input.command_name
            ),
        })
    }
    async fn resolve_capability_requests(
        &self,
        handle: &RuntimeSessionHandle,
        results: Vec<RuntimeCapabilityResult>,
        events: RuntimeEventSender,
    ) -> Result<()>;
    async fn cancel(&self, handle: &RuntimeSessionHandle, reason: Option<String>) -> Result<()>;
    async fn close(&self, handle: &RuntimeSessionHandle) -> Result<()>;
}
