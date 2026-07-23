use std::collections::HashMap;
use std::sync::RwLock;

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tracing::warn;

use lionclaw_runtime_api::{
    AppliedRuntimeConfiguration, ExecutionOutput, RuntimeEvent, RuntimeMcpServerSpec,
    RuntimeProgramSession, RuntimeTurnJournalSender, TurnEvent, TypedFailure,
};

use crate::driver::AcpRuntimeConfig;
use crate::event_mapping::acp_turn_events;
use crate::policy::{acp_error_response, acp_permission_denial};
use crate::program::acp_mcp_servers;
use crate::protocol::{
    acp_is_server_request, acp_response_id, parse_acp_response, AcpMessage, AcpOpenedSession,
    AcpProviderRejection, AcpResponse, AcpResponseOutcome, AcpSelectionSet, AcpSessionCapabilities,
    AcpSessionSelections,
};
use crate::state::{
    forget_acp_session_id, normalize_acp_session_id, record_native_session_observation,
    remember_acp_session_id, AcpCancelRequest, AcpSessionState,
};

pub(crate) struct AcpClient {
    session: Option<Box<dyn RuntimeProgramSession>>,
    next_id: u64,
    final_response: String,
}

enum AcpRequestFailure {
    ProviderRejected(AcpProviderRejection),
    Other(anyhow::Error),
}

impl AcpRequestFailure {
    fn into_anyhow(self) -> anyhow::Error {
        match self {
            Self::ProviderRejected(rejection) => rejection.into_typed_failure().into(),
            Self::Other(error) => error,
        }
    }
}

fn classify_acp_response(
    message: AcpMessage,
    method: &str,
) -> std::result::Result<AcpResponse, AcpRequestFailure> {
    match parse_acp_response(message, method).map_err(AcpRequestFailure::Other)? {
        AcpResponseOutcome::Success(response) => Ok(response),
        AcpResponseOutcome::Rejected(rejection) => {
            Err(AcpRequestFailure::ProviderRejected(rejection))
        }
    }
}

struct AcpCancelWait<'a> {
    session_id: &'a str,
    cancel_rx: &'a mut mpsc::UnboundedReceiver<AcpCancelRequest>,
}

pub(crate) struct AcpEnsureSession<'a> {
    pub(crate) config: &'a AcpRuntimeConfig,
    pub(crate) sessions: &'a RwLock<HashMap<String, AcpSessionState>>,
    pub(crate) runtime_session_id: &'a str,
    pub(crate) session_state: &'a AcpSessionState,
    pub(crate) session_capabilities: AcpSessionCapabilities,
    pub(crate) working_dir: &'a str,
    pub(crate) mcp_servers: &'a [RuntimeMcpServerSpec],
}

impl AcpClient {
    pub(crate) fn new(session: Box<dyn RuntimeProgramSession>) -> Self {
        Self {
            session: Some(session),
            next_id: 1,
            final_response: String::new(),
        }
    }

    pub(crate) async fn initialize(&mut self) -> Result<AcpSessionCapabilities> {
        let response = self
            .request(
                "initialize",
                json!({
                    "protocolVersion": 1,
                    "clientCapabilities": {
                        "fs": {
                            "readTextFile": false,
                            "writeTextFile": false,
                        },
                        "terminal": false,
                    },
                }),
                None,
            )
            .await?;
        Ok(AcpSessionCapabilities::from_initialize_result(
            &response.result,
        ))
    }

    pub(crate) async fn ensure_session(
        &mut self,
        input: AcpEnsureSession<'_>,
    ) -> Result<AcpOpenedSession> {
        let mcp_servers = acp_mcp_servers(input.mcp_servers);
        if let Some(session_id) = input.session_state.session_id.as_deref() {
            if let Some(reopen_method) = input.session_capabilities.reopen_method() {
                let response = match self
                    .request_classified(
                        reopen_method,
                        json!({
                            "sessionId": session_id,
                            "cwd": input.working_dir,
                            "mcpServers": mcp_servers.clone(),
                        }),
                        None,
                    )
                    .await
                {
                    Ok(response) => response,
                    Err(AcpRequestFailure::ProviderRejected(AcpProviderRejection::Permanent(
                        failure,
                    ))) => {
                        record_native_session_observation(
                            input.sessions,
                            input.runtime_session_id,
                            lionclaw_runtime_api::RuntimeNativeSessionObservation::ReopenFailed,
                        )?;
                        return Err(failure.into());
                    }
                    Err(error) => return Err(error.into_anyhow()),
                };
                record_native_session_observation(
                    input.sessions,
                    input.runtime_session_id,
                    lionclaw_runtime_api::RuntimeNativeSessionObservation::Resumed,
                )?;
                return Ok(AcpOpenedSession {
                    session_id: session_id.to_string(),
                    selections: AcpSessionSelections::from_session_result(&response.result),
                });
            } else {
                forget_acp_session_id(input.config, input.sessions, input.runtime_session_id)?;
            }
        }

        let response = self
            .request(
                "session/new",
                json!({
                    "cwd": input.working_dir,
                    "mcpServers": mcp_servers,
                }),
                None,
            )
            .await?;
        let session_id = response
            .result
            .get("sessionId")
            .and_then(Value::as_str)
            .and_then(normalize_acp_session_id)
            .context("ACP session/new response is missing sessionId")?;
        if input.session_capabilities.reopen_method().is_some() {
            let persisted = remember_acp_session_id(
                input.config,
                input.sessions,
                input.runtime_session_id,
                &session_id,
            )?;
            record_native_session_observation(
                input.sessions,
                input.runtime_session_id,
                lionclaw_runtime_api::RuntimeNativeSessionObservation::Reconstructed {
                    state: if persisted {
                        lionclaw_runtime_api::RuntimeNativeStateAvailability::Reopenable
                    } else {
                        lionclaw_runtime_api::RuntimeNativeStateAvailability::Unavailable
                    },
                },
            )?;
        } else {
            forget_acp_session_id(input.config, input.sessions, input.runtime_session_id)?;
            record_native_session_observation(
                input.sessions,
                input.runtime_session_id,
                lionclaw_runtime_api::RuntimeNativeSessionObservation::Reconstructed {
                    state: lionclaw_runtime_api::RuntimeNativeStateAvailability::Unavailable,
                },
            )?;
        }

        Ok(AcpOpenedSession {
            session_id,
            selections: AcpSessionSelections::from_session_result(&response.result),
        })
    }

    pub(crate) async fn configure_session(
        &mut self,
        config: &AcpRuntimeConfig,
        session_id: &str,
        selections: &AcpSessionSelections,
    ) -> Result<AppliedRuntimeConfiguration> {
        let mut applied = AppliedRuntimeConfiguration {
            requested_model: config.model.clone(),
            requested_mode: config.mode.clone(),
            ..Default::default()
        };
        if let Some(model) = config.model.as_deref() {
            let (selected, confirmation) = self
                .apply_selection(
                    session_id,
                    "model",
                    model,
                    selections.models.as_ref(),
                    selections,
                )
                .await?;
            applied.applied_model = Some(selected);
            applied.model_confirmation = Some(confirmation);
        }

        if let Some(mode) = config.mode.as_deref() {
            let (selected, confirmation) = self
                .apply_selection(
                    session_id,
                    "mode",
                    mode,
                    selections.modes.as_ref(),
                    selections,
                )
                .await?;
            applied.applied_mode = Some(selected);
            applied.mode_confirmation = Some(confirmation);
        }

        Ok(applied)
    }

    async fn apply_selection(
        &mut self,
        session_id: &str,
        kind: &str,
        requested: &str,
        first_class: Option<&AcpSelectionSet>,
        selections: &AcpSessionSelections,
    ) -> Result<(
        String,
        lionclaw_runtime_api::RuntimeConfigurationConfirmation,
    )> {
        if let Some(first_class) = first_class {
            let matches = first_class
                .values
                .iter()
                .filter(|value| value.id == requested || value.name.as_deref() == Some(requested))
                .collect::<Vec<_>>();
            let selected = match matches.as_slice() {
                [selected] => selected.id.clone(),
                [] => {
                    return Err(anyhow!(
                        "ACP runtime does not advertise requested {kind} '{requested}'"
                    ))
                }
                _ => return Err(anyhow!("ACP requested {kind} '{requested}' is ambiguous")),
            };
            let (method, id_key) = match kind {
                "model" => ("session/set_model", "modelId"),
                "mode" => ("session/set_mode", "modeId"),
                _ => return Err(anyhow!("unsupported ACP selection kind '{kind}'")),
            };
            self.request(
                method,
                json!({"sessionId": session_id, (id_key): selected}),
                None,
            )
            .await?;
            return Ok((
                selected,
                lionclaw_runtime_api::RuntimeConfigurationConfirmation::Acknowledged,
            ));
        }

        let option = selections
            .config_options
            .iter()
            .find(|option| option.id == kind)
            .ok_or_else(|| anyhow!("ACP runtime cannot apply requested {kind} '{requested}'"))?;
        if !option.values.is_empty() && !option.values.iter().any(|value| value == requested) {
            return Err(anyhow!(
                "ACP runtime does not advertise requested {kind} '{requested}'"
            ));
        }
        let response = self
            .request(
                "session/set_config_option",
                json!({"sessionId": session_id, "configId": kind, "value": requested}),
                None,
            )
            .await?;
        let observed = AcpSessionSelections::from_session_result(&response.result)
            .config_options
            .into_iter()
            .find(|candidate| candidate.id == kind)
            .and_then(|candidate| candidate.current)
            .ok_or_else(|| anyhow!("ACP runtime did not confirm applied {kind} '{requested}'"))?;
        if observed != requested {
            return Err(anyhow!(
                "ACP runtime applied {kind} '{observed}' instead of requested '{requested}'"
            ));
        }
        Ok((
            observed,
            lionclaw_runtime_api::RuntimeConfigurationConfirmation::Observed,
        ))
    }

    pub(crate) async fn prompt(
        &mut self,
        session_id: &str,
        prompt: &str,
        journal: &RuntimeTurnJournalSender,
        cancel_rx: &mut mpsc::UnboundedReceiver<AcpCancelRequest>,
    ) -> Result<String> {
        self.final_response.clear();
        self.request_with_cancel(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{
                    "type": "text",
                    "text": prompt,
                }],
            }),
            Some(journal),
            session_id,
            cancel_rx,
        )
        .await?;
        drop(journal.send(TurnEvent::canonical(RuntimeEvent::Done)).await);
        Ok(self.take_final_response())
    }

    async fn request_with_cancel(
        &mut self,
        method: &str,
        params: Value,
        journal: Option<&RuntimeTurnJournalSender>,
        session_id: &str,
        cancel_rx: &mut mpsc::UnboundedReceiver<AcpCancelRequest>,
    ) -> Result<AcpResponse> {
        let id = self.next_request_id();
        self.send_request(id, method, params).await?;
        self.wait_for_response(
            id,
            method,
            journal,
            Some(AcpCancelWait {
                session_id,
                cancel_rx,
            }),
        )
        .await
        .map_err(AcpRequestFailure::into_anyhow)
    }

    async fn request(
        &mut self,
        method: &str,
        params: Value,
        journal: Option<&RuntimeTurnJournalSender>,
    ) -> Result<AcpResponse> {
        self.request_classified(method, params, journal)
            .await
            .map_err(AcpRequestFailure::into_anyhow)
    }

    async fn request_classified(
        &mut self,
        method: &str,
        params: Value,
        journal: Option<&RuntimeTurnJournalSender>,
    ) -> std::result::Result<AcpResponse, AcpRequestFailure> {
        let id = self.next_request_id();
        self.send_request(id, method, params)
            .await
            .map_err(AcpRequestFailure::Other)?;
        self.wait_for_response(id, method, journal, None).await
    }

    async fn wait_for_response(
        &mut self,
        id: u64,
        method: &str,
        journal: Option<&RuntimeTurnJournalSender>,
        mut cancel: Option<AcpCancelWait<'_>>,
    ) -> std::result::Result<AcpResponse, AcpRequestFailure> {
        if let Some(cancel) = cancel.as_mut() {
            loop {
                tokio::select! {
                    maybe_cancel = cancel.cancel_rx.recv() => {
                        match maybe_cancel {
                            Some(cancel_request) => {
                                self.cancel_session(cancel.session_id, cancel_request).await;
                            }
                            None => break,
                        }
                    }
                    maybe_message = self.recv() => {
                        let Some(message) = maybe_message.map_err(AcpRequestFailure::Other)? else {
                            return Err(AcpRequestFailure::Other(anyhow!(
                                "ACP process closed before responding to {method}"
                            )));
                        };
                        if acp_response_id(&message.value).is_some_and(|response_id| response_id == id) {
                            return classify_acp_response(message, method);
                        }
                        self.dispatch_message(message, journal)
                            .await
                            .map_err(AcpRequestFailure::Other)?;
                    }
                }
            }
        }

        loop {
            let Some(message) = self.recv().await.map_err(AcpRequestFailure::Other)? else {
                return Err(AcpRequestFailure::Other(anyhow!(
                    "ACP process closed before responding to {method}"
                )));
            };
            if acp_response_id(&message.value).is_some_and(|response_id| response_id == id) {
                return classify_acp_response(message, method);
            }
            self.dispatch_message(message, journal)
                .await
                .map_err(AcpRequestFailure::Other)?;
        }
    }

    async fn send_request(&mut self, id: u64, method: &str, params: Value) -> Result<()> {
        self.send(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }))
        .await
    }

    async fn send_notification(&mut self, method: &str, params: Value) -> Result<()> {
        self.send(&json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }))
        .await
    }

    async fn cancel_session(&mut self, session_id: &str, cancel: AcpCancelRequest) {
        let result = self
            .send_notification(
                "session/cancel",
                json!({
                    "sessionId": session_id,
                }),
            )
            .await
            .map_err(|err| err.to_string());
        drop(cancel.sent.send(result));
    }

    async fn dispatch_message(
        &mut self,
        message: AcpMessage,
        journal: Option<&RuntimeTurnJournalSender>,
    ) -> Result<()> {
        if acp_is_server_request(&message.value) {
            self.respond_to_server_request(&message.value).await?;
            return Ok(());
        }

        if let Some(journal) = journal {
            for record in acp_turn_events(&message) {
                lionclaw_runtime_api::observe_final_response(
                    &mut self.final_response,
                    record.event(),
                );
                drop(journal.send(record).await);
            }
        }
        Ok(())
    }

    pub(crate) fn take_final_response(&mut self) -> String {
        std::mem::take(&mut self.final_response)
            .trim_end()
            .to_string()
    }

    async fn respond_to_server_request(&mut self, request: &Value) -> Result<()> {
        let id = request
            .get("id")
            .cloned()
            .context("ACP server request is missing id")?;
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match method {
            "session/request_permission" => {
                self.send(&json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": acp_permission_denial(request.get("params")),
                }))
                .await
            }
            "fs/read_text_file" | "fs/write_text_file" => {
                self.send(&acp_error_response(
                    id,
                    -32000,
                    "LionClaw disables ACP filesystem access",
                ))
                .await
            }
            _ => {
                self.send(&acp_error_response(
                    id,
                    -32601,
                    &format!("LionClaw does not support ACP request '{method}'"),
                ))
                .await
            }
        }
    }

    async fn send(&mut self, message: &Value) -> Result<()> {
        let session = self
            .session
            .as_mut()
            .context("ACP session is already closed")?;
        session.write_line(&serde_json::to_string(message)?).await
    }

    async fn recv(&mut self) -> Result<Option<AcpMessage>> {
        let session = self
            .session
            .as_mut()
            .context("ACP session is already closed")?;
        loop {
            let Some(line) = session.read_line().await? else {
                return Ok(None);
            };
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let value = serde_json::from_str(trimmed)
                .with_context(|| format!("invalid ACP JSON-RPC line: {trimmed}"))?;
            return Ok(Some(AcpMessage { value }));
        }
    }

    pub(crate) async fn shutdown(mut self) -> Result<ExecutionOutput> {
        let Some(session) = self.session.take() else {
            return Ok(ExecutionOutput::default());
        };
        session.shutdown().await
    }

    fn next_request_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

pub(crate) async fn finish_acp_session<R>(client: AcpClient, result: Result<R>) -> Result<R> {
    let shutdown = client.shutdown().await.and_then(ensure_acp_exit_success);

    match (result, shutdown) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(err)) => Err(err),
        (Err(err), Ok(())) => Err(err),
        (Err(err), Err(shutdown_err)) => {
            warn!(
                error = %shutdown_err,
                "ACP shutdown failed after runtime error"
            );
            Err(err)
        }
    }
}

fn ensure_acp_exit_success(output: ExecutionOutput) -> Result<()> {
    if output.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let detail = if stderr.is_empty() {
        format!("ACP process exited with {}", output.status_description())
    } else {
        format!(
            "ACP process exited with {}: {stderr}",
            output.status_description()
        )
    };
    let mut failure = TypedFailure::permanent("acp.process_exit", detail);
    failure.evidence_mut().exit_code = output.exit_code;
    failure.evidence_mut().stop_reason =
        output.exit_signal.map(|signal| format!("signal {signal}"));
    failure.evidence_mut().stderr = stderr;
    Err(anyhow::Error::new(failure.projected()))
}

#[cfg(test)]
mod exit_tests {
    use super::*;

    #[test]
    fn nonzero_exit_preserves_structured_process_evidence() {
        let error = ensure_acp_exit_success(ExecutionOutput {
            stderr: b"fatal protocol error".to_vec(),
            exit_code: Some(17),
            ..Default::default()
        })
        .expect_err("nonzero exit must fail");
        let failure = error.downcast_ref::<TypedFailure>().expect("typed failure");
        assert!(matches!(failure, TypedFailure::PermanentRuntime { .. }));
        assert_eq!(failure.evidence().exit_code, Some(17));
        assert_eq!(failure.evidence().stderr, "fatal protocol error");
    }
}
