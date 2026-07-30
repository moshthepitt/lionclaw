use anyhow::{anyhow, Result};
use async_trait::async_trait;
use uuid::Uuid;

use crate::{
    context::RuntimeExecutionContext,
    event::RuntimeTurnJournalSender,
    program::{RuntimeProgramExecutor, RuntimeProgramSpec},
    state::{RuntimeSessionReady, RuntimeStateDir},
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
        state: RuntimeStateDir,
        ready: RuntimeSessionReady,
    },
}

#[derive(Debug, Clone)]
pub struct RuntimeSessionHandle {
    pub runtime_session_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeResumeMode {
    Reconstructed,
    Resumed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeNativeStateAvailability {
    Reopenable,
    Unavailable,
}

/// The adapter's authoritative observation after one native session attempt.
///
/// Intent belongs to [`RuntimeSessionHandle`]. This observation is recorded
/// only after the runtime has actually established or rejected native state,
/// so the runner never infers continuity from a requested start mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeNativeSessionObservation {
    Reconstructed {
        state: RuntimeNativeStateAvailability,
    },
    Resumed,
    ReopenFailed,
}

impl RuntimeNativeSessionObservation {
    pub(crate) const fn committable_mode(self) -> Option<RuntimeResumeMode> {
        match self {
            Self::Reconstructed {
                state: RuntimeNativeStateAvailability::Reopenable,
            } => Some(RuntimeResumeMode::Reconstructed),
            Self::Resumed => Some(RuntimeResumeMode::Resumed),
            Self::Reconstructed {
                state: RuntimeNativeStateAvailability::Unavailable,
            }
            | Self::ReopenFailed => None,
        }
    }

    pub const fn is_reopen_failure(self) -> bool {
        matches!(self, Self::ReopenFailed)
    }
}

/// Adapter-declared policy for a failed attempt to reopen native conversation
/// state. The runner, rather than a concrete runtime branch, owns the single
/// canonical reconstruction attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeNativeReopenRecovery {
    Unsupported,
    ForgetAndReconstruct,
}

#[derive(Debug, Clone)]
pub struct RuntimeTerminalProgramInput {
    pub session_id: Uuid,
    pub runtime_state: RuntimeStateDir,
    pub resume: bool,
    pub bootstrap_message: String,
}

#[derive(Debug, Clone)]
pub struct TurnInput {
    pub runtime_session_id: String,
    pub prompt: String,
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

/// Synchronous lifecycle hooks in this trait are bounded host-side state
/// operations. Implementations must not wait on child processes, protocol
/// responses, or network I/O from these hooks.
#[async_trait]
pub trait RuntimeAdapter: Send + Sync {
    async fn info(&self) -> RuntimeAdapterInfo;
    fn native_reopen_recovery(&self) -> RuntimeNativeReopenRecovery {
        RuntimeNativeReopenRecovery::Unsupported
    }
    /// Forget the host-owned identity rejected by the exact reopen attempt.
    fn forget_native_reopen(&self, _handle: &RuntimeSessionHandle) -> Result<()> {
        Err(anyhow!("runtime does not support native reopen recovery"))
    }
    fn native_session_observation(
        &self,
        _handle: &RuntimeSessionHandle,
    ) -> Result<Option<RuntimeNativeSessionObservation>> {
        Ok(None)
    }
    /// Register bounded host state for one runtime session attempt.
    fn session_start(&self, input: RuntimeSessionStartInput) -> Result<RuntimeSessionHandle>;
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
    /// Release bounded host state for a completed runtime session attempt.
    fn close(&self, handle: &RuntimeSessionHandle) -> Result<()>;
}
