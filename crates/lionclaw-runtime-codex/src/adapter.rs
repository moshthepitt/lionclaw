use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use lionclaw_runtime_api::{
    RuntimeAdapter, RuntimeAdapterInfo, RuntimeCapabilityResult, RuntimeControlExecution,
    RuntimeControlOutcome, RuntimeEvent, RuntimeEventSender, RuntimeExecutionContext,
    RuntimeMcpServerSpec, RuntimeNativeHomeArtifactDir, RuntimeProgramExecutor, RuntimeProgramSpec,
    RuntimeProgramTurnExecution, RuntimeSessionHandle, RuntimeSessionStartInput,
    RuntimeTerminalProgramInput, RuntimeTurnInput, RuntimeTurnJournalSender, RuntimeTurnMode,
    RuntimeTurnResult,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};
use uuid::Uuid;

use crate::app_server::{
    extract_app_server_thread_id, extract_app_server_turn_id, finish_app_server_session,
    thread_resume_params, thread_start_params, turn_start_params, AppServerTransport,
    CodexAppServerClient, CodexAppServerEventSink, ExecutionSessionTransport,
    CODEX_GENERATED_IMAGES_NATIVE_HOME_DIR,
};
use crate::driver::CodexRuntimeConfig;
use crate::program::{build_codex_app_server_program, build_codex_terminal_program};
use crate::state::{
    load_ready_saved_thread_id, CodexInterruptRequest, CodexSessionState, CodexThreadState,
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

impl CodexAppServerTurnRunner<'_> {
    async fn run_turn(
        &mut self,
        input: RuntimeTurnInput,
        journal: RuntimeTurnJournalSender,
    ) -> Result<RuntimeTurnResult> {
        let network_mode = self.context.network_mode;
        let thread_state = self.adapter.thread_state_for(&input.runtime_session_id);
        let transport = self
            .adapter
            .start_app_server_transport(self.executor.as_mut(), &self.context.mcp_servers)
            .await?;
        let mut client =
            CodexAppServerClient::new_with_runtime_context(transport, self.context.clone());
        let sink = CodexAppServerEventSink::journal(&journal);

        let result = async {
            client.initialize(sink, &thread_state).await?;
            let thread_id = self
                .adapter
                .ensure_app_server_thread(
                    &mut client,
                    &input.runtime_session_id,
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
            Ok(RuntimeTurnResult::default())
        }
        .await;

        finish_app_server_session(client, result).await
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
        execution: RuntimeProgramTurnExecution,
        journal: RuntimeTurnJournalSender,
    ) -> Result<RuntimeTurnResult> {
        let RuntimeProgramTurnExecution {
            input,
            context,
            executor,
        } = execution;
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
        runtime_session_id: &str,
        sink: impl Into<CodexAppServerEventSink<'a>>,
        thread_state: &CodexThreadState,
    ) -> Result<String>
    where
        T: AppServerTransport + Send,
    {
        let sink = sink.into();
        if let Some(thread_id) = self.current_thread_id(runtime_session_id)? {
            return self
                .resume_app_server_thread(client, &thread_id, sink, thread_state)
                .await;
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
            None => self.current_thread_id(runtime_session_id)?.ok_or_else(|| {
                anyhow!("codex app-server thread/start response missing thread id")
            })?,
        };
        thread_state.persist_thread_id(&thread_id)?;
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
        let response = client
            .request(
                "thread/resume",
                thread_resume_params(thread_id, self.config.model.as_deref()),
                sink,
                thread_state,
            )
            .await?;
        let resolved_thread_id =
            extract_app_server_thread_id(&response).unwrap_or_else(|| thread_id.to_string());
        thread_state.persist_thread_id(&resolved_thread_id)?;
        Ok(resolved_thread_id)
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

#[async_trait]
impl RuntimeAdapter for CodexRuntimeAdapter {
    async fn info(&self) -> RuntimeAdapterInfo {
        RuntimeAdapterInfo {
            id: "codex".to_string(),
            version: "0.1".to_string(),
            healthy: !self.config.executable.trim().is_empty(),
        }
    }

    fn turn_mode(&self) -> RuntimeTurnMode {
        RuntimeTurnMode::ProgramBacked
    }

    fn native_home_artifact_dirs(&self) -> Result<Vec<RuntimeNativeHomeArtifactDir>> {
        Ok(vec![RuntimeNativeHomeArtifactDir::new(
            CODEX_GENERATED_IMAGES_NATIVE_HOME_DIR,
        )?])
    }

    async fn session_start(&self, input: RuntimeSessionStartInput) -> Result<RuntimeSessionHandle> {
        let runtime_session_id = format!("codex-{}", Uuid::new_v4());
        let thread_id = match input.runtime_state_root.as_deref() {
            Some(root) => load_ready_saved_thread_id(root, input.runtime_session_ready)?,
            None => None,
        };
        let resumes_existing_session = thread_id.is_some();
        self.sessions
            .write()
            .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?
            .insert(
                runtime_session_id.clone(),
                CodexSessionState {
                    runtime_state_root: input.runtime_state_root,
                    thread_id,
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
        self.run_app_server_turn(execution, journal).await
    }

    fn build_terminal_program(
        &self,
        _input: RuntimeTerminalProgramInput,
    ) -> Result<RuntimeProgramSpec> {
        Ok(build_codex_terminal_program(&self.config))
    }

    async fn runtime_control(
        &self,
        execution: RuntimeControlExecution,
        events: RuntimeEventSender,
    ) -> Result<RuntimeControlOutcome> {
        self.run_app_server_control(execution, events).await
    }

    async fn resolve_capability_requests(
        &self,
        _handle: &RuntimeSessionHandle,
        results: Vec<RuntimeCapabilityResult>,
        events: RuntimeEventSender,
    ) -> Result<()> {
        if !results.is_empty() {
            return Err(anyhow!(
                "codex adapter does not support runtime-side capability request resolution"
            ));
        }
        drop(events.send(RuntimeEvent::Done));
        Ok(())
    }

    async fn cancel(&self, handle: &RuntimeSessionHandle, _reason: Option<String>) -> Result<()> {
        let active_turn = self
            .session_state(&handle.runtime_session_id)
            .ok()
            .and_then(|state| state.active_turn);
        let Some(active_turn) = active_turn else {
            return Ok(());
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

        timeout(Duration::from_secs(5), ack_rx)
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
            })?
    }

    async fn close(&self, handle: &RuntimeSessionHandle) -> Result<()> {
        self.sessions
            .write()
            .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?
            .remove(&handle.runtime_session_id);
        Ok(())
    }
}
