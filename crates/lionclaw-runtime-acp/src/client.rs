use std::collections::HashMap;
use std::sync::RwLock;

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tracing::warn;

use lionclaw_runtime_api::{
    ExecutionOutput, RawTurnPayload, RuntimeEvent, RuntimeMcpServerSpec, RuntimeProgramSession,
    RuntimeTurnJournalSender, TurnEvent,
};

use crate::driver::{AcpRuntimeConfig, ACP_PROTOCOL_NAME};
use crate::event_mapping::acp_turn_events;
use crate::policy::{acp_error_response, acp_permission_denial};
use crate::program::acp_mcp_servers;
use crate::protocol::{
    acp_is_server_request, acp_response_id, parse_acp_response, AcpMessage, AcpOpenedSession,
    AcpResponse, AcpSessionCapabilities,
};
use crate::state::{
    forget_acp_session_id, normalize_acp_session_id, remember_acp_session_id, AcpCancelRequest,
    AcpSessionState,
};

pub(crate) struct AcpClient {
    session: Option<Box<dyn RuntimeProgramSession>>,
    next_id: u64,
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
                self.request(
                    reopen_method,
                    json!({
                        "sessionId": session_id,
                        "cwd": input.working_dir,
                        "mcpServers": mcp_servers.clone(),
                    }),
                    None,
                )
                .await?;
                return Ok(AcpOpenedSession {
                    session_id: session_id.to_string(),
                    resumed_existing: true,
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
            remember_acp_session_id(
                input.config,
                input.sessions,
                input.runtime_session_id,
                &session_id,
            )?;
        } else {
            forget_acp_session_id(input.config, input.sessions, input.runtime_session_id)?;
        }

        Ok(AcpOpenedSession {
            session_id,
            resumed_existing: false,
        })
    }

    pub(crate) async fn configure_session(
        &mut self,
        config: &AcpRuntimeConfig,
        session_id: &str,
    ) -> Result<()> {
        if let Some(model) = config.model.as_deref() {
            self.request(
                "session/set_config_option",
                json!({
                    "sessionId": session_id,
                    "configId": "model",
                    "value": model,
                }),
                None,
            )
            .await?;
        }

        if let Some(mode) = config.mode.as_deref() {
            self.request(
                "session/set_config_option",
                json!({
                    "sessionId": session_id,
                    "configId": "mode",
                    "value": mode,
                }),
                None,
            )
            .await?;
        }

        Ok(())
    }

    pub(crate) async fn prompt(
        &mut self,
        session_id: &str,
        prompt: &str,
        journal: &RuntimeTurnJournalSender,
        cancel_rx: &mut mpsc::UnboundedReceiver<AcpCancelRequest>,
    ) -> Result<()> {
        let response = self
            .request_with_cancel(
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
        drop(journal.send(TurnEvent::with_raw(
            RuntimeEvent::Done,
            RawTurnPayload {
                driver: ACP_PROTOCOL_NAME.to_string(),
                payload: response.raw,
            },
        )));
        Ok(())
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
    }

    async fn request(
        &mut self,
        method: &str,
        params: Value,
        journal: Option<&RuntimeTurnJournalSender>,
    ) -> Result<AcpResponse> {
        let id = self.next_request_id();
        self.send_request(id, method, params).await?;
        self.wait_for_response(id, method, journal, None).await
    }

    async fn wait_for_response(
        &mut self,
        id: u64,
        method: &str,
        journal: Option<&RuntimeTurnJournalSender>,
        mut cancel: Option<AcpCancelWait<'_>>,
    ) -> Result<AcpResponse> {
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
                        let Some(message) = maybe_message? else {
                            return Err(anyhow!("ACP process closed before responding to {method}"));
                        };
                        if acp_response_id(&message.value).is_some_and(|response_id| response_id == id) {
                            return parse_acp_response(message, method);
                        }
                        self.dispatch_message(message, journal).await?;
                    }
                }
            }
        }

        loop {
            let Some(message) = self.recv().await? else {
                return Err(anyhow!("ACP process closed before responding to {method}"));
            };
            if acp_response_id(&message.value).is_some_and(|response_id| response_id == id) {
                return parse_acp_response(message, method);
            }
            self.dispatch_message(message, journal).await?;
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
                drop(journal.send(record));
            }
        }
        Ok(())
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
            return Ok(Some(AcpMessage {
                raw: trimmed.to_string(),
                value,
            }));
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
    if stderr.is_empty() {
        return Err(anyhow!(
            "ACP process exited with {}",
            output.status_description()
        ));
    }
    Err(anyhow!(
        "ACP process exited with {}: {stderr}",
        output.status_description()
    ))
}
