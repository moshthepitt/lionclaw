use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::{anyhow, bail, Result};
use lionclaw_runtime_api::{
    clear_state_value, load_ready_state_value, save_state_value, RuntimeSessionReady,
    RuntimeStateDir,
};
use tokio::sync::{mpsc, oneshot};

pub(crate) const CODEX_THREAD_ID_STATE_FILE: &str = ".lionclaw-codex-thread-id";
pub(crate) const MAX_CODEX_PROTOCOL_ID_BYTES: usize = 1_024;

pub(crate) fn validate_protocol_id(id: &str) -> Result<()> {
    if id.is_empty() {
        bail!("codex app-server protocol identifier must not be empty");
    }
    if id.len() > MAX_CODEX_PROTOCOL_ID_BYTES {
        bail!(
            "codex app-server protocol identifier is {} bytes (maximum {MAX_CODEX_PROTOCOL_ID_BYTES})",
            id.len()
        );
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct CodexSessionState {
    pub(crate) runtime_state: Option<RuntimeStateDir>,
    pub(crate) thread_id: Option<String>,
    pub(crate) active_turn: Option<ActiveCodexTurn>,
    pub(crate) native_reopen_failed: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct ActiveCodexTurn {
    pub(crate) thread_id: String,
    pub(crate) turn_id: String,
    pub(crate) interrupt_tx: mpsc::UnboundedSender<CodexInterruptRequest>,
}

#[derive(Debug)]
pub(crate) struct CodexInterruptRequest {
    pub(crate) ack_tx: oneshot::Sender<Result<()>>,
}

#[derive(Clone)]
pub(crate) struct CodexThreadState {
    pub(crate) sessions: Arc<RwLock<HashMap<String, CodexSessionState>>>,
    pub(crate) runtime_session_id: String,
}

pub(crate) fn load_ready_saved_thread_id(
    state: &RuntimeStateDir,
    runtime_session_ready: RuntimeSessionReady,
) -> Result<Option<String>> {
    let thread_id = load_ready_state_value(
        state,
        CODEX_THREAD_ID_STATE_FILE,
        "codex thread",
        runtime_session_ready,
    )?;
    if let Some(thread_id) = thread_id.as_deref() {
        validate_protocol_id(thread_id)?;
    }
    Ok(thread_id)
}

pub(crate) fn save_thread_id(state: &RuntimeStateDir, thread_id: &str) -> Result<()> {
    validate_protocol_id(thread_id)?;
    save_state_value(state, CODEX_THREAD_ID_STATE_FILE, thread_id, "codex thread")
}

pub(crate) fn forget_thread_id(
    sessions: &RwLock<HashMap<String, CodexSessionState>>,
    runtime_session_id: &str,
) -> Result<()> {
    let runtime_state = sessions
        .read()
        .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?
        .get(runtime_session_id)
        .ok_or_else(|| anyhow!("runtime session '{runtime_session_id}' not found"))?
        .runtime_state
        .clone();
    if let Some(state) = runtime_state {
        clear_state_value(&state, CODEX_THREAD_ID_STATE_FILE, "codex thread")?;
    }
    let mut sessions = sessions
        .write()
        .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?;
    let session = sessions
        .get_mut(runtime_session_id)
        .ok_or_else(|| anyhow!("runtime session '{runtime_session_id}' not found"))?;
    session.thread_id = None;
    session.native_reopen_failed = false;
    drop(sessions);
    Ok(())
}

pub(crate) fn mark_native_reopen_failed(
    sessions: &RwLock<HashMap<String, CodexSessionState>>,
    runtime_session_id: &str,
) -> Result<()> {
    let mut sessions = sessions
        .write()
        .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?;
    sessions
        .get_mut(runtime_session_id)
        .ok_or_else(|| anyhow!("runtime session '{runtime_session_id}' not found"))?
        .native_reopen_failed = true;
    drop(sessions);
    Ok(())
}

impl CodexThreadState {
    pub(crate) fn set_active_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
        interrupt_tx: mpsc::UnboundedSender<CodexInterruptRequest>,
    ) -> Result<()> {
        validate_protocol_id(thread_id)?;
        validate_protocol_id(turn_id)?;
        let mut sessions = self
            .sessions
            .write()
            .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?;
        let session = sessions
            .get_mut(&self.runtime_session_id)
            .ok_or_else(|| anyhow!("runtime session '{}' not found", self.runtime_session_id))?;
        session.active_turn = Some(ActiveCodexTurn {
            thread_id: thread_id.to_string(),
            turn_id: turn_id.to_string(),
            interrupt_tx,
        });
        drop(sessions);
        Ok(())
    }

    pub(crate) fn clear_active_turn(&self, thread_id: &str, turn_id: &str) -> Result<()> {
        let mut sessions = self
            .sessions
            .write()
            .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?;
        let Some(session) = sessions.get_mut(&self.runtime_session_id) else {
            return Ok(());
        };
        if session
            .active_turn
            .as_ref()
            .is_some_and(|active| active.thread_id == thread_id && active.turn_id == turn_id)
        {
            session.active_turn = None;
        }
        drop(sessions);
        Ok(())
    }

    pub(crate) fn current_thread_id(&self) -> Result<Option<String>> {
        Ok(self
            .sessions
            .read()
            .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?
            .get(&self.runtime_session_id)
            .ok_or_else(|| anyhow!("runtime session '{}' not found", self.runtime_session_id))?
            .thread_id
            .clone())
    }

    pub(crate) fn persist_thread_id(&self, thread_id: &str) -> Result<()> {
        validate_protocol_id(thread_id)?;
        let root = self
            .sessions
            .read()
            .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?
            .get(&self.runtime_session_id)
            .ok_or_else(|| anyhow!("runtime session '{}' not found", self.runtime_session_id))?
            .runtime_state
            .clone();

        if let Some(state) = root.as_ref() {
            save_thread_id(state, thread_id)?;
        }

        {
            let mut sessions = self
                .sessions
                .write()
                .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?;
            let session = sessions.get_mut(&self.runtime_session_id).ok_or_else(|| {
                anyhow!("runtime session '{}' not found", self.runtime_session_id)
            })?;
            session.thread_id = Some(thread_id.to_string());
            drop(sessions);
        }
        Ok(())
    }
}
