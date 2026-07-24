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
    RuntimeAdapter, RuntimeAdapterInfo, RuntimeMcpServerSpec, RuntimeNativeReopenRecovery,
    RuntimeNativeSessionObservation, RuntimeProgramExecutor, RuntimeProgramSpec, RuntimeResume,
    RuntimeSessionHandle, RuntimeSessionStartInput, RuntimeTerminalProgramInput,
    RuntimeTurnJournalSender, TurnExecution, TurnInput, TurnResult, TypedFailure,
};

use crate::client::{finish_acp_session, AcpClient, AcpEnsureSession};
use crate::driver::AcpRuntimeConfig;
use crate::program::{build_acp_program, build_acp_terminal_program};
use crate::state::{
    clear_native_session_observation, forget_acp_session_id, get_runtime_session,
    load_ready_acp_session_id, register_active_acp_turn, AcpCancelRequest, AcpSessionState,
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

    fn build_terminal_program(
        &self,
        _input: RuntimeTerminalProgramInput,
    ) -> Result<RuntimeProgramSpec> {
        Ok(build_acp_terminal_program(&self.config))
    }

    fn native_reopen_recovery(&self) -> RuntimeNativeReopenRecovery {
        RuntimeNativeReopenRecovery::ForgetAndReconstruct
    }

    fn forget_native_reopen(&self, handle: &RuntimeSessionHandle) -> Result<()> {
        forget_acp_session_id(&self.config, &self.sessions, &handle.runtime_session_id)
    }

    fn native_session_observation(
        &self,
        handle: &RuntimeSessionHandle,
    ) -> Result<Option<RuntimeNativeSessionObservation>> {
        Ok(
            get_runtime_session(&self.sessions, &handle.runtime_session_id)?
                .native_session_observation,
        )
    }

    fn session_start(&self, input: RuntimeSessionStartInput) -> Result<RuntimeSessionHandle> {
        let runtime_id = self.config.normalized_runtime_id();
        let runtime_session_id = format!("{runtime_id}-{}", Uuid::new_v4());
        let (runtime_state, session_id) = match input.resume {
            RuntimeResume::Native { state, ready } => {
                let session_id = load_ready_acp_session_id(&self.config, &state, ready)?;
                (Some(state), session_id)
            }
            RuntimeResume::Reconstruct => (None, None),
        };
        self.sessions
            .write()
            .map_err(|_| anyhow!("ACP runtime session state lock poisoned"))?
            .insert(
                runtime_session_id.clone(),
                AcpSessionState {
                    runtime_state,
                    session_id,
                    active_turn: None,
                    native_session_observation: None,
                },
            );

        Ok(RuntimeSessionHandle { runtime_session_id })
    }

    async fn turn(
        &self,
        execution: TurnExecution,
        journal: RuntimeTurnJournalSender,
    ) -> Result<TurnResult> {
        let TurnExecution {
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

    async fn cancel(
        &self,
        handle: &RuntimeSessionHandle,
        _reason: Option<String>,
    ) -> Result<lionclaw_runtime_api::RuntimeCancellation> {
        let active_turn = self
            .sessions
            .read()
            .map_err(|_| anyhow!("ACP runtime session state lock poisoned"))?
            .get(&handle.runtime_session_id)
            .and_then(|state| state.active_turn.clone());
        let Some(active_turn) = active_turn else {
            return Ok(lionclaw_runtime_api::RuntimeCancellation::NoActiveTurn);
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
                return Err(anyhow!(
                    "timed out waiting for ACP session/cancel send acknowledgement for '{}'",
                    active_turn.session_id
                ));
            }
        }?;

        match timeout(ACP_CANCEL_ACK_TIMEOUT, completion.wait()).await {
            Ok(()) => Ok(lionclaw_runtime_api::RuntimeCancellation::Acknowledged),
            Err(_) => {
                warn!(
                    session_id = active_turn.session_id,
                    "timed out waiting for ACP cancelled turn to finish"
                );
                Err(anyhow!(
                    "timed out waiting for ACP cancelled turn to finish for '{}'",
                    active_turn.session_id
                ))
            }
        }
    }

    fn close(&self, handle: &RuntimeSessionHandle) -> Result<()> {
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
        input: TurnInput,
        journal: RuntimeTurnJournalSender,
    ) -> Result<TurnResult> {
        let runtime_session_id = input.runtime_session_id.clone();
        clear_native_session_observation(&self.sessions, &runtime_session_id)?;
        let session_state = get_runtime_session(&self.sessions, &runtime_session_id)?;
        let program = build_acp_program(&self.config);
        let session = self.executor.spawn(program).await?;
        let mut client = AcpClient::new(session);
        let mut active_turn = None;
        let mut applied_configuration = None;

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
            let mut configuration = client
                .configure_session(
                    &self.config,
                    &opened_session.session_id,
                    &opened_session.selections,
                )
                .await?
                .projected();
            applied_configuration = Some(configuration.clone());
            if !configuration.is_empty() {
                drop(
                    journal
                        .send(lionclaw_runtime_api::TurnEvent::canonical(
                            lionclaw_runtime_api::RuntimeEvent::Configuration {
                                configuration: configuration.clone(),
                            },
                        ))
                        .await,
                );
            }
            let (cancel_tx, mut cancel_rx) = mpsc::unbounded_channel();
            active_turn = Some(register_active_acp_turn(
                &self.sessions,
                &runtime_session_id,
                &opened_session.session_id,
                cancel_tx,
            )?);
            let prompt_result = client
                .prompt(
                    &opened_session.session_id,
                    &input.prompt,
                    &journal,
                    &mut cancel_rx,
                )
                .await;
            let final_response = prompt_result?;
            configuration.merge_observed(client.observed_configuration());
            Ok(TurnResult {
                configuration: configuration.projected(),
                runtime_usage: client.runtime_usage().clone(),
                final_response,
            }
            .projected())
        }
        .await;

        let failed_final_response = if let Ok(completed) = &result {
            completed.final_response.clone()
        } else {
            client.take_final_response()
        };
        let failed_configuration = applied_configuration.as_ref().map(|configuration| {
            let mut configuration = configuration.clone();
            configuration.merge_observed(client.observed_configuration());
            configuration.projected()
        });
        let failed_runtime_usage = client.runtime_usage().clone().projected();
        let result = finish_acp_session(client, result).await.map_err(|error| {
            let mut error = configured_failure(
                error,
                failed_configuration.as_ref(),
                &failed_runtime_usage,
                "acp.runtime",
            );
            if let Some(failure) = error.downcast_mut::<TypedFailure>() {
                failure.evidence_mut().final_response = failed_final_response;
            }
            error
        });
        drop(active_turn);
        result
    }
}

fn configured_failure(
    error: anyhow::Error,
    configuration: Option<&lionclaw_runtime_api::AppliedRuntimeConfiguration>,
    runtime_usage: &lionclaw_runtime_api::RuntimeUsage,
    code: &str,
) -> anyhow::Error {
    let mut failure = error
        .downcast_ref::<TypedFailure>()
        .cloned()
        .unwrap_or_else(|| TypedFailure::permanent(code, error.to_string()));
    if let Some(configuration) = configuration {
        failure.evidence_mut().configuration = configuration.clone();
    }
    failure
        .evidence_mut()
        .runtime_usage
        .merge_observed(runtime_usage.clone());
    anyhow::Error::new(failure.projected())
}
