use std::path::PathBuf;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use uuid::Uuid;

use crate::{
    context::{RuntimeExecutionContext, RuntimeNativeHomeArtifactDir},
    event::RuntimeTurnJournalSender,
    program::{RuntimeProgramExecutor, RuntimeProgramSpec},
    state::RuntimeSessionReady,
    turn::TurnResult,
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
    pub resume: RuntimeResume,
}

/// Profile-declared native conversation support and its mission-private state.
#[derive(Debug, Clone)]
pub enum RuntimeResume {
    Reconstruct,
    Native {
        state_root: PathBuf,
        ready: RuntimeSessionReady,
    },
}

#[derive(Debug, Clone)]
pub struct RuntimeSessionHandle {
    pub runtime_session_id: String,
    pub resume_mode: RuntimeResumeMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeResumeMode {
    Reconstructed,
    Resumed,
}

#[derive(Debug, Clone)]
pub struct RuntimeTerminalProgramInput {
    pub session_id: Uuid,
    pub runtime_state_root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct TurnInput {
    pub runtime_session_id: String,
    pub prompt: String,
    pub fresh_prompt: Option<String>,
}

pub struct TurnExecution {
    pub input: TurnInput,
    pub context: RuntimeExecutionContext,
    pub executor: Box<dyn RuntimeProgramExecutor>,
}

/// Outcome of a cancellation request at the adapter's structured protocol
/// boundary. Absence of an active turn is not an acknowledgement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeCancellation {
    NoActiveTurn,
    Acknowledged,
}

#[async_trait]
pub trait RuntimeAdapter: Send + Sync {
    async fn info(&self) -> RuntimeAdapterInfo;
    fn native_home_artifact_dirs(&self) -> Result<Vec<RuntimeNativeHomeArtifactDir>> {
        Ok(Vec::new())
    }
    async fn session_start(&self, input: RuntimeSessionStartInput) -> Result<RuntimeSessionHandle>;
    async fn turn(
        &self,
        execution: TurnExecution,
        journal: RuntimeTurnJournalSender,
    ) -> Result<TurnResult>;
    fn build_terminal_program(
        &self,
        _input: RuntimeTerminalProgramInput,
    ) -> Result<RuntimeProgramSpec> {
        Err(anyhow!("runtime does not expose a native terminal UI"))
    }
    async fn cancel(
        &self,
        handle: &RuntimeSessionHandle,
        reason: Option<String>,
    ) -> Result<RuntimeCancellation>;
    async fn close(&self, handle: &RuntimeSessionHandle) -> Result<()>;
}
