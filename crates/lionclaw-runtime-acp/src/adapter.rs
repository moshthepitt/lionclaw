use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tracing::warn;
use uuid::Uuid;

use lionclaw_runtime_api::{
    RuntimeAdapter, RuntimeAdapterInfo, RuntimeCapabilityResult, RuntimeEvent, RuntimeEventSender,
    RuntimeMcpServerSpec, RuntimeProgramExecutor, RuntimeProgramSpec, RuntimeProgramTurnExecution,
    RuntimeSessionHandle, RuntimeSessionStartInput, RuntimeTerminalProgramInput, RuntimeTurnInput,
    RuntimeTurnJournalSender, RuntimeTurnMode, RuntimeTurnResult,
};

use crate::client::{finish_acp_session, AcpClient, AcpEnsureSession};
use crate::driver::AcpRuntimeConfig;
use crate::program::{build_acp_program, build_acp_terminal_program};
use crate::state::{
    get_runtime_session, load_ready_acp_session_id, register_active_acp_turn, AcpCancelRequest,
    AcpSessionState,
};

const ACP_CANCEL_ACK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub struct AcpRuntimeAdapter {
    config: AcpRuntimeConfig,
    sessions: Arc<RwLock<HashMap<String, AcpSessionState>>>,
}

impl AcpRuntimeAdapter {
    pub fn new(config: AcpRuntimeConfig) -> Self {
        Self {
            config,
            sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl RuntimeAdapter for AcpRuntimeAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: self.config.normalized_runtime_id(),
            version: "0.1".to_string(),
            healthy: !self.config.executable.trim().is_empty(),
        }
    }

    fn turn_mode(&self) -> RuntimeTurnMode {
        RuntimeTurnMode::ProgramBacked
    }

    fn build_terminal_program(
        &self,
        _input: RuntimeTerminalProgramInput,
    ) -> Result<RuntimeProgramSpec> {
        Ok(build_acp_terminal_program(&self.config))
    }

    async fn session_start(&self, input: RuntimeSessionStartInput) -> Result<RuntimeSessionHandle> {
        let runtime_id = self.config.normalized_runtime_id();
        let runtime_session_id = format!("{runtime_id}-{}", Uuid::new_v4());
        let session_id = match input.runtime_state_root.as_deref() {
            Some(root) => {
                load_ready_acp_session_id(&self.config, root, input.runtime_session_ready)?
            }
            None => None,
        };
        let resumes_existing_session = session_id.is_some();
        self.sessions
            .write()
            .map_err(|_| anyhow!("ACP runtime session state lock poisoned"))?
            .insert(
                runtime_session_id.clone(),
                AcpSessionState {
                    runtime_state_root: input.runtime_state_root,
                    session_id,
                    active_turn: None,
                },
            );

        Ok(RuntimeSessionHandle {
            runtime_session_id,
            resumes_existing_session,
        })
    }

    async fn program_backed_turn(
        &self,
        execution: RuntimeProgramTurnExecution,
        journal: RuntimeTurnJournalSender,
    ) -> Result<RuntimeTurnResult> {
        let RuntimeProgramTurnExecution {
            input,
            context,
            executor,
        } = execution;
        let mut driver = AcpTurnRunner {
            config: self.config.clone(),
            sessions: Arc::clone(&self.sessions),
            working_dir: context
                .working_dir
                .unwrap_or_else(|| self.config.default_working_dir.clone()),
            mcp_servers: context.mcp_servers,
            executor,
        };
        driver.run_turn(input, journal).await
    }

    async fn resolve_capability_requests(
        &self,
        _handle: &RuntimeSessionHandle,
        results: Vec<RuntimeCapabilityResult>,
        events: RuntimeEventSender,
    ) -> Result<()> {
        if !results.is_empty() {
            return Err(anyhow!(
                "ACP adapter does not support runtime-side capability request resolution"
            ));
        }
        drop(events.send(RuntimeEvent::Done));
        Ok(())
    }

    async fn cancel(&self, handle: &RuntimeSessionHandle, _reason: Option<String>) -> Result<()> {
        let active_turn = self
            .sessions
            .read()
            .map_err(|_| anyhow!("ACP runtime session state lock poisoned"))?
            .get(&handle.runtime_session_id)
            .and_then(|state| state.active_turn.clone());
        let Some(active_turn) = active_turn else {
            return Ok(());
        };

        let completion = Arc::clone(&active_turn.completion);
        let (sent, cancel_sent) = oneshot::channel();
        active_turn
            .cancel_tx
            .send(AcpCancelRequest { sent })
            .map_err(|_| {
                anyhow!(
                    "ACP turn '{}' is no longer accepting cancellation",
                    active_turn.session_id
                )
            })?;
        match timeout(ACP_CANCEL_ACK_TIMEOUT, cancel_sent).await {
            Ok(Ok(result)) => result.map_err(anyhow::Error::msg),
            Ok(Err(_)) => Err(anyhow!(
                "ACP turn cancellation send acknowledgement was dropped for '{}'",
                active_turn.session_id
            )),
            Err(_) => {
                warn!(
                    session_id = active_turn.session_id,
                    "timed out waiting for ACP session/cancel send acknowledgement"
                );
                return Ok(());
            }
        }?;

        match timeout(ACP_CANCEL_ACK_TIMEOUT, completion.wait()).await {
            Ok(()) => Ok(()),
            Err(_) => {
                warn!(
                    session_id = active_turn.session_id,
                    "timed out waiting for ACP cancelled turn to finish"
                );
                Ok(())
            }
        }
    }

    async fn close(&self, handle: &RuntimeSessionHandle) -> Result<()> {
        self.sessions
            .write()
            .map_err(|_| anyhow!("ACP runtime session state lock poisoned"))?
            .remove(&handle.runtime_session_id);
        Ok(())
    }
}

struct AcpTurnRunner {
    config: AcpRuntimeConfig,
    sessions: Arc<RwLock<HashMap<String, AcpSessionState>>>,
    working_dir: String,
    mcp_servers: Vec<RuntimeMcpServerSpec>,
    executor: Box<dyn RuntimeProgramExecutor>,
}

impl AcpTurnRunner {
    async fn run_turn(
        &mut self,
        input: RuntimeTurnInput,
        journal: RuntimeTurnJournalSender,
    ) -> Result<RuntimeTurnResult> {
        let runtime_session_id = input.runtime_session_id.clone();
        let session_state = get_runtime_session(&self.sessions, &runtime_session_id)?;
        let program = build_acp_program(&self.config);
        let session = self.executor.spawn(program).await?;
        let mut client = AcpClient::new(session);
        let mut active_turn = None;

        let result = async {
            let session_capabilities = client.initialize().await?;
            let opened_session = client
                .ensure_session(AcpEnsureSession {
                    config: &self.config,
                    sessions: &self.sessions,
                    runtime_session_id: &runtime_session_id,
                    session_state: &session_state,
                    session_capabilities,
                    working_dir: &self.working_dir,
                    mcp_servers: &self.mcp_servers,
                })
                .await?;
            client
                .configure_session(&self.config, &opened_session.session_id)
                .await?;
            let (cancel_tx, mut cancel_rx) = mpsc::unbounded_channel();
            active_turn = Some(register_active_acp_turn(
                &self.sessions,
                &runtime_session_id,
                &opened_session.session_id,
                cancel_tx,
            )?);
            let prompt = if opened_session.resumed_existing {
                &input.prompt
            } else {
                input.fresh_prompt.as_deref().unwrap_or(&input.prompt)
            };
            let prompt_result = client
                .prompt(&opened_session.session_id, prompt, &journal, &mut cancel_rx)
                .await;
            prompt_result?;
            Ok(RuntimeTurnResult::default())
        }
        .await;

        let result = finish_acp_session(client, result).await;
        drop(active_turn);
        result
    }
}
