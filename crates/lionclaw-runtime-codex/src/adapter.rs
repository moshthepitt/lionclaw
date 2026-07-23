use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use lionclaw_runtime_api::{
    RuntimeAdapter, RuntimeAdapterInfo, RuntimeExecutionContext, RuntimeMcpServerSpec,
    RuntimeNativeReopenRecovery, RuntimeNativeSessionObservation, RuntimeProgramExecutor,
    RuntimeProgramSpec, RuntimeResume, RuntimeSessionHandle, RuntimeSessionStartInput,
    RuntimeTerminalProgramInput, RuntimeTurnJournalSender, TurnExecution, TurnInput, TurnResult,
    TypedFailure,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};
use uuid::Uuid;

use crate::app_server::{
    extract_app_server_model, extract_app_server_thread_id, extract_app_server_turn_id,
    finish_app_server_session, thread_resume_params, thread_start_params, turn_start_params,
    AppServerRequestFailure, AppServerTransport, CodexAppServerClient, CodexAppServerEventSink,
    ExecutionSessionTransport,
};
use crate::driver::CodexRuntimeConfig;
use crate::program::{build_codex_app_server_program, build_codex_terminal_program};
use crate::state::{
    load_ready_saved_thread_id, validate_protocol_id, CodexInterruptRequest, CodexSessionState,
    CodexThreadState,
};

#[derive(Debug)]
pub struct CodexRuntimeAdapter {
    pub(crate) config: CodexRuntimeConfig,
    sessions: Arc<RwLock<HashMap<String, CodexSessionState>>>,
}

struct CodexAppServerTurnRunner<'a> {
    adapter: &'a CodexRuntimeAdapter,
    context: RuntimeExecutionContext,
    executor: Box<dyn RuntimeProgramExecutor>,
}

fn validate_app_server_model(requested: Option<&str>, applied: Option<&str>) -> Result<()> {
    match (requested, applied) {
        (None, _) => Ok(()),
        (Some(requested), Some(applied)) if requested == applied => Ok(()),
        (Some(requested), Some(applied)) => Err(anyhow!(
            "codex app-server applied model '{applied}', expected '{requested}'"
        )),
        (Some(_), None) => Err(anyhow!(
            "codex app-server did not report the applied model in turn/start"
        )),
    }
}

impl CodexAppServerTurnRunner<'_> {
    async fn run_turn(
        &mut self,
        input: TurnInput,
        journal: RuntimeTurnJournalSender,
    ) -> Result<TurnResult> {
        let network_mode = self.context.network_mode;
        let thread_state = self.adapter.thread_state_for(&input.runtime_session_id);
        let transport = self
            .adapter
            .start_app_server_transport(self.executor.as_mut(), &self.context.mcp_servers)
            .await?;
        let mut client =
            CodexAppServerClient::new_with_runtime_context(transport, self.context.clone());
        let sink = CodexAppServerEventSink::journal(&journal);
        let mut applied_configuration = None;

        let result = async {
            let saved_thread_id = self.adapter.current_thread_id(&input.runtime_session_id)?;
            client.initialize(sink, &thread_state).await?;
            let thread_id = self
                .adapter
                .ensure_app_server_thread(
                    &mut client,
                    saved_thread_id.as_deref(),
                    sink,
                    &thread_state,
                )
                .await?;
            let response = client
                .request(
                    "turn/start",
                    turn_start_params(
                        &thread_id,
                        &input.prompt,
                        self.adapter.config.model.as_deref(),
                        network_mode,
                    ),
                    sink,
                    &thread_state,
                )
                .await?;
            let turn_id = extract_app_server_turn_id(&response);
            if let Some(turn_id) = turn_id.as_deref() {
                validate_protocol_id(turn_id)?;
            }
            let applied_model = extract_app_server_model(&response);
            let configuration = lionclaw_runtime_api::AppliedRuntimeConfiguration {
                requested_model: self.adapter.config.model.clone(),
                applied_model: applied_model.clone(),
                model_confirmation: applied_model
                    .as_ref()
                    .map(|_| lionclaw_runtime_api::RuntimeConfigurationConfirmation::Observed),
                requested_mode: None,
                applied_mode: None,
                mode_confirmation: None,
            }
            .projected();
            applied_configuration = Some(configuration.clone());
            validate_app_server_model(
                self.adapter.config.model.as_deref(),
                applied_model.as_deref(),
            )?;
            drop(
                journal
                    .send(lionclaw_runtime_api::TurnEvent::canonical(
                        lionclaw_runtime_api::RuntimeEvent::Configuration { configuration },
                    ))
                    .await,
            );
            let (interrupt_tx, mut interrupt_rx) = mpsc::unbounded_channel();
            client
                .wait_for_turn_completed(
                    turn_id.as_deref(),
                    Some(&thread_id),
                    Some(interrupt_tx),
                    sink,
                    &thread_state,
                    Some(&mut interrupt_rx),
                )
                .await?;
            let final_response = client.take_final_response();
            Ok(TurnResult {
                configuration: lionclaw_runtime_api::AppliedRuntimeConfiguration {
                    requested_model: self.adapter.config.model.clone(),
                    model_confirmation: applied_model
                        .as_ref()
                        .map(|_| lionclaw_runtime_api::RuntimeConfigurationConfirmation::Observed),
                    applied_model,
                    requested_mode: None,
                    applied_mode: None,
                    mode_confirmation: None,
                },
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
        finish_app_server_session(client, result)
            .await
            .map_err(|error| {
                let mut failure = error
                    .downcast_ref::<TypedFailure>()
                    .cloned()
                    .unwrap_or_else(|| TypedFailure::permanent("codex.runtime", error.to_string()));
                if let Some(configuration) = applied_configuration {
                    failure.evidence_mut().configuration = configuration;
                }
                failure.evidence_mut().final_response = failed_final_response;
                anyhow::Error::new(failure.projected())
            })
    }
}

impl CodexRuntimeAdapter {
    pub fn new(config: CodexRuntimeConfig) -> Self {
        Self {
            config,
            sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn run_app_server_turn(
        &self,
        execution: TurnExecution,
        journal: RuntimeTurnJournalSender,
    ) -> Result<TurnResult> {
        let TurnExecution {
            input,
            context,
            executor,
        } = execution;
        crate::state::clear_native_session_observation(&self.sessions, &input.runtime_session_id)?;
        let mut driver = CodexAppServerTurnRunner {
            adapter: self,
            context,
            executor,
        };
        driver.run_turn(input, journal).await
    }

    pub(crate) async fn start_app_server_transport(
        &self,
        executor: &mut dyn RuntimeProgramExecutor,
        mcp_servers: &[RuntimeMcpServerSpec],
    ) -> Result<ExecutionSessionTransport> {
        let session = executor
            .spawn(build_codex_app_server_program(&self.config, mcp_servers))
            .await?;
        Ok(ExecutionSessionTransport::new(session))
    }

    pub(crate) async fn ensure_app_server_thread<'a, T>(
        &self,
        client: &mut CodexAppServerClient<T>,
        saved_thread_id: Option<&str>,
        sink: impl Into<CodexAppServerEventSink<'a>>,
        thread_state: &CodexThreadState,
    ) -> Result<String>
    where
        T: AppServerTransport + Send,
    {
        let sink = sink.into();
        if let Some(thread_id) = saved_thread_id {
            let thread_id = self
                .resume_app_server_thread(client, thread_id, sink, thread_state)
                .await?;
            preserve_observation(
                thread_state
                    .record_native_session_observation(RuntimeNativeSessionObservation::Resumed),
            );
            return Ok(thread_id);
        }

        let response = client
            .request(
                "thread/start",
                thread_start_params(self.config.model.as_deref()),
                sink,
                thread_state,
            )
            .await?;
        let thread_id = match extract_app_server_thread_id(&response) {
            Some(thread_id) => thread_id,
            None => thread_state.current_thread_id()?.ok_or_else(|| {
                anyhow!("codex app-server thread/start response missing thread id")
            })?,
        };
        let persisted = thread_state.persist_thread_id(&thread_id)?;
        preserve_observation(thread_state.record_native_session_observation(
            RuntimeNativeSessionObservation::Reconstructed {
                state: if persisted {
                    lionclaw_runtime_api::RuntimeNativeStateAvailability::Reopenable
                } else {
                    lionclaw_runtime_api::RuntimeNativeStateAvailability::Unavailable
                },
            },
        ));
        Ok(thread_id)
    }

    pub(crate) async fn resume_app_server_thread<'a, T>(
        &self,
        client: &mut CodexAppServerClient<T>,
        thread_id: &str,
        sink: impl Into<CodexAppServerEventSink<'a>>,
        thread_state: &CodexThreadState,
    ) -> Result<String>
    where
        T: AppServerTransport + Send,
    {
        let sink = sink.into();
        let response = match client
            .request_classified(
                "thread/resume",
                thread_resume_params(thread_id, self.config.model.as_deref()),
                sink,
                thread_state,
            )
            .await
        {
            Ok(response) => response,
            Err(AppServerRequestFailure::Rejected(failure)) => {
                if matches!(&failure, TypedFailure::PermanentRuntime { .. }) {
                    preserve_observation(crate::state::record_native_reopen_failure(
                        &self.sessions,
                        &thread_state.runtime_session_id,
                    ));
                }
                return Err(failure.into());
            }
            Err(error) => return Err(error.into_anyhow()),
        };
        if let Some(resolved_thread_id) = extract_app_server_thread_id(&response) {
            if resolved_thread_id != thread_id {
                return Err(anyhow!(
                    "codex app-server resumed thread '{resolved_thread_id}', expected '{thread_id}'"
                ));
            }
        }
        thread_state.persist_thread_id(thread_id)?;
        Ok(thread_id.to_string())
    }

    pub(crate) fn thread_state_for(&self, runtime_session_id: &str) -> CodexThreadState {
        CodexThreadState {
            sessions: Arc::clone(&self.sessions),
            runtime_session_id: runtime_session_id.to_string(),
        }
    }

    pub(crate) fn current_thread_id(&self, runtime_session_id: &str) -> Result<Option<String>> {
        Ok(self.session_state(runtime_session_id)?.thread_id)
    }

    pub(crate) fn session_state(&self, runtime_session_id: &str) -> Result<CodexSessionState> {
        self.sessions
            .read()
            .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?
            .get(runtime_session_id)
            .cloned()
            .ok_or_else(|| anyhow!("runtime session '{runtime_session_id}' not found"))
    }
}

fn preserve_observation(observation: Result<()>) {
    if let Err(error) = observation {
        tracing::warn!(
            error = %error,
            "failed to retain native session observation; the next turn will reconstruct"
        );
    }
}

#[async_trait]
impl RuntimeAdapter for CodexRuntimeAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "codex".to_string(),
            version: "0.1".to_string(),
            healthy: !self.config.executable.trim().is_empty(),
        }
    }

    fn native_reopen_recovery(&self) -> RuntimeNativeReopenRecovery {
        RuntimeNativeReopenRecovery::ForgetAndReconstruct
    }

    fn forget_native_reopen(&self, handle: &RuntimeSessionHandle) -> Result<()> {
        crate::state::forget_thread_id(&self.sessions, &handle.runtime_session_id)?;
        Ok(())
    }

    fn native_session_observation(
        &self,
        handle: &RuntimeSessionHandle,
    ) -> Result<Option<RuntimeNativeSessionObservation>> {
        Ok(self
            .session_state(&handle.runtime_session_id)?
            .native_session_observation)
    }

    fn session_start(&self, input: RuntimeSessionStartInput) -> Result<RuntimeSessionHandle> {
        let runtime_session_id = format!("codex-{}", Uuid::new_v4());
        let (runtime_state, thread_id) = match input.resume {
            RuntimeResume::Native { state, ready } => {
                let thread_id = load_ready_saved_thread_id(&state, ready)?;
                (Some(state), thread_id)
            }
            RuntimeResume::Reconstruct => (None, None),
        };
        self.sessions
            .write()
            .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?
            .insert(
                runtime_session_id.clone(),
                CodexSessionState {
                    runtime_state,
                    thread_id,
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
        self.run_app_server_turn(execution, journal).await
    }

    fn build_terminal_program(
        &self,
        _input: RuntimeTerminalProgramInput,
    ) -> Result<RuntimeProgramSpec> {
        Ok(build_codex_terminal_program(&self.config))
    }

    async fn cancel(
        &self,
        handle: &RuntimeSessionHandle,
        _reason: Option<String>,
    ) -> Result<lionclaw_runtime_api::RuntimeCancellation> {
        let active_turn = self
            .session_state(&handle.runtime_session_id)
            .ok()
            .and_then(|state| state.active_turn);
        let Some(active_turn) = active_turn else {
            return Ok(lionclaw_runtime_api::RuntimeCancellation::NoActiveTurn);
        };

        let (ack_tx, ack_rx) = oneshot::channel();
        active_turn
            .interrupt_tx
            .send(CodexInterruptRequest { ack_tx })
            .map_err(|_| {
                anyhow!(
                    "codex turn interrupt channel closed before turn/interrupt for {}",
                    active_turn.turn_id
                )
            })?;

        let result = timeout(Duration::from_secs(5), ack_rx)
            .await
            .map_err(|_| {
                anyhow!(
                    "timed out waiting for codex turn/interrupt acknowledgement for {}/{}",
                    active_turn.thread_id,
                    active_turn.turn_id
                )
            })?
            .map_err(|_| {
                anyhow!(
                    "codex turn interrupt task dropped acknowledgement for {}/{}",
                    active_turn.thread_id,
                    active_turn.turn_id
                )
            })?;
        result?;
        Ok(lionclaw_runtime_api::RuntimeCancellation::Acknowledged)
    }

    fn close(&self, handle: &RuntimeSessionHandle) -> Result<()> {
        self.sessions
            .write()
            .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?
            .remove(&handle.runtime_session_id);
        Ok(())
    }
}

#[cfg(test)]
mod configuration_tests {
    use super::*;

    #[test]
    fn app_server_model_evidence_must_match_the_codex_request() {
        validate_app_server_model(Some("gpt-5.5"), Some("gpt-5.5")).unwrap();
        assert!(validate_app_server_model(Some("gpt-5.5"), Some("fallback")).is_err());
        assert!(validate_app_server_model(Some("gpt-5.5"), None).is_err());
        validate_app_server_model(None, Some("runtime-default")).unwrap();
    }
}
