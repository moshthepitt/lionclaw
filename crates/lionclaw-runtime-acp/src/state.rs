use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};

use anyhow::{anyhow, Result};
use tokio::sync::{mpsc, oneshot, Notify};

use lionclaw_runtime_api::{
    clear_state_value, load_ready_state_value, save_state_value, RuntimeSessionReady,
    RuntimeStateDir,
};

use crate::driver::AcpRuntimeConfig;

#[derive(Debug, Clone)]
pub(crate) struct AcpSessionState {
    pub(crate) runtime_state: Option<RuntimeStateDir>,
    pub(crate) session_id: Option<String>,
    pub(crate) active_turn: Option<ActiveAcpTurn>,
}

#[derive(Debug, Clone)]
pub(crate) struct ActiveAcpTurn {
    pub(crate) session_id: String,
    pub(crate) cancel_tx: mpsc::UnboundedSender<AcpCancelRequest>,
    pub(crate) completion: Arc<AcpTurnCompletion>,
}

#[derive(Debug)]
pub(crate) struct AcpCancelRequest {
    pub(crate) sent: oneshot::Sender<Result<(), String>>,
}

#[derive(Debug, Default)]
pub(crate) struct AcpTurnCompletion {
    done: AtomicBool,
    notify: Notify,
}

impl AcpTurnCompletion {
    pub(crate) async fn wait(&self) {
        loop {
            if self.done.load(Ordering::Acquire) {
                return;
            }
            let notified = self.notify.notified();
            if self.done.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    fn mark_done(&self) {
        self.done.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
}

pub(crate) fn get_runtime_session(
    sessions: &RwLock<HashMap<String, AcpSessionState>>,
    runtime_session_id: &str,
) -> Result<AcpSessionState> {
    sessions
        .read()
        .map_err(|_| anyhow!("ACP runtime session state lock poisoned"))?
        .get(runtime_session_id)
        .cloned()
        .ok_or_else(|| anyhow!("unknown ACP runtime session '{runtime_session_id}'"))
}

pub(crate) fn remember_acp_session_id(
    config: &AcpRuntimeConfig,
    sessions: &RwLock<HashMap<String, AcpSessionState>>,
    runtime_session_id: &str,
    session_id: &str,
) -> Result<()> {
    let runtime_state =
        update_runtime_session_id(sessions, runtime_session_id, session_id.to_string())?;
    if let Some(state) = runtime_state.as_ref() {
        save_acp_session_id(config, state, session_id)?;
    }
    Ok(())
}

pub(crate) fn forget_acp_session_id(
    config: &AcpRuntimeConfig,
    sessions: &RwLock<HashMap<String, AcpSessionState>>,
    runtime_session_id: &str,
) -> Result<()> {
    let runtime_state = clear_runtime_session_id(sessions, runtime_session_id)?;
    if let Some(state) = runtime_state.as_ref() {
        clear_state_value(state, &config.session_id_state_file, "ACP session id")?;
    }
    Ok(())
}

fn update_runtime_session_id(
    sessions: &RwLock<HashMap<String, AcpSessionState>>,
    runtime_session_id: &str,
    session_id: String,
) -> Result<Option<RuntimeStateDir>> {
    let mut sessions = sessions
        .write()
        .map_err(|_| anyhow!("ACP runtime session state lock poisoned"))?;
    let state = sessions
        .get_mut(runtime_session_id)
        .ok_or_else(|| anyhow!("unknown ACP runtime session '{runtime_session_id}'"))?;
    state.session_id = Some(session_id);
    let runtime_state = state.runtime_state.clone();
    drop(sessions);
    Ok(runtime_state)
}

fn clear_runtime_session_id(
    sessions: &RwLock<HashMap<String, AcpSessionState>>,
    runtime_session_id: &str,
) -> Result<Option<RuntimeStateDir>> {
    let mut sessions = sessions
        .write()
        .map_err(|_| anyhow!("ACP runtime session state lock poisoned"))?;
    let state = sessions
        .get_mut(runtime_session_id)
        .ok_or_else(|| anyhow!("unknown ACP runtime session '{runtime_session_id}'"))?;
    state.session_id = None;
    let runtime_state = state.runtime_state.clone();
    drop(sessions);
    Ok(runtime_state)
}

pub(crate) fn register_active_acp_turn(
    sessions: &Arc<RwLock<HashMap<String, AcpSessionState>>>,
    runtime_session_id: &str,
    session_id: &str,
    cancel_tx: mpsc::UnboundedSender<AcpCancelRequest>,
) -> Result<ActiveAcpTurnRegistration> {
    let sessions_ref = Arc::clone(sessions);
    let completion = Arc::new(AcpTurnCompletion::default());
    {
        let mut sessions = sessions
            .write()
            .map_err(|_| anyhow!("ACP runtime session state lock poisoned"))?;
        let state = sessions
            .get_mut(runtime_session_id)
            .ok_or_else(|| anyhow!("unknown ACP runtime session '{runtime_session_id}'"))?;
        state.active_turn = Some(ActiveAcpTurn {
            session_id: session_id.to_string(),
            cancel_tx,
            completion: Arc::clone(&completion),
        });
        drop(sessions);
    }
    Ok(ActiveAcpTurnRegistration {
        sessions: sessions_ref,
        runtime_session_id: runtime_session_id.to_string(),
        completion,
    })
}

pub(crate) struct ActiveAcpTurnRegistration {
    sessions: Arc<RwLock<HashMap<String, AcpSessionState>>>,
    runtime_session_id: String,
    completion: Arc<AcpTurnCompletion>,
}

impl Drop for ActiveAcpTurnRegistration {
    fn drop(&mut self) {
        if let Ok(mut sessions) = self.sessions.write() {
            if let Some(state) = sessions.get_mut(&self.runtime_session_id) {
                if state
                    .active_turn
                    .as_ref()
                    .is_some_and(|turn| Arc::ptr_eq(&turn.completion, &self.completion))
                {
                    state.active_turn = None;
                }
            }
        }
        self.completion.mark_done();
    }
}

fn save_acp_session_id(
    config: &AcpRuntimeConfig,
    runtime_state: &RuntimeStateDir,
    session_id: &str,
) -> Result<()> {
    save_state_value(
        runtime_state,
        &config.session_id_state_file,
        session_id,
        "ACP session id",
    )
}

pub(crate) fn load_ready_acp_session_id(
    config: &AcpRuntimeConfig,
    runtime_state: &RuntimeStateDir,
    runtime_session_ready: RuntimeSessionReady,
) -> Result<Option<String>> {
    load_ready_state_value(
        runtime_state,
        &config.session_id_state_file,
        "ACP session id",
        runtime_session_ready,
    )
}

pub(crate) fn normalize_acp_session_id(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}
