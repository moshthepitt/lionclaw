use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::{anyhow, Result};
use lionclaw_runtime_api::{load_ready_state_value, save_state_value, RuntimeSessionReady};
use tokio::sync::{mpsc, oneshot};

pub(crate) const CODEX_THREAD_ID_STATE_FILE: &str = ".lionclaw-codex-thread-id";

#[derive(Debug, Clone)]
pub(crate) struct CodexSessionState {
    pub(crate) runtime_state_root: Option<PathBuf>,
    pub(crate) thread_id: Option<String>,
    pub(crate) active_turn: Option<ActiveCodexTurn>,
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
    root: &Path,
    runtime_session_ready: RuntimeSessionReady,
) -> Result<Option<String>> {
    load_ready_state_value(
        root,
        CODEX_THREAD_ID_STATE_FILE,
        "codex thread",
        runtime_session_ready,
    )
}

pub(crate) fn save_thread_id(root: &Path, thread_id: &str) -> Result<()> {
    save_state_value(root, CODEX_THREAD_ID_STATE_FILE, thread_id, "codex thread")
}

impl CodexThreadState {
    pub(crate) fn set_active_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
        interrupt_tx: mpsc::UnboundedSender<CodexInterruptRequest>,
    ) -> Result<()> {
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

    pub(crate) fn runtime_state_root(&self) -> Result<Option<PathBuf>> {
        Ok(self
            .sessions
            .read()
            .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?
            .get(&self.runtime_session_id)
            .ok_or_else(|| anyhow!("runtime session '{}' not found", self.runtime_session_id))?
            .runtime_state_root
            .clone())
    }

    pub(crate) fn persist_thread_id(&self, thread_id: &str) -> Result<()> {
        let root = self
            .sessions
            .read()
            .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?
            .get(&self.runtime_session_id)
            .ok_or_else(|| anyhow!("runtime session '{}' not found", self.runtime_session_id))?
            .runtime_state_root
            .clone();

        if let Some(root) = root.as_deref() {
            save_thread_id(root, thread_id)?;
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
