#![cfg_attr(
    not(test),
    warn(
        clippy::allow_attributes_without_reason,
        clippy::clone_on_ref_ptr,
        clippy::expect_used,
        clippy::future_not_send,
        clippy::get_unwrap,
        clippy::indexing_slicing,
        clippy::large_futures,
        clippy::large_stack_arrays,
        clippy::large_types_passed_by_value,
        clippy::let_underscore_must_use,
        clippy::mutex_atomic,
        clippy::mutex_integer,
        clippy::panic,
        clippy::panic_in_result_fn,
        clippy::pathbuf_init_then_push,
        clippy::rc_buffer,
        clippy::rc_mutex,
        clippy::redundant_clone,
        clippy::same_name_method,
        clippy::significant_drop_in_scrutinee,
        clippy::significant_drop_tightening,
        clippy::uninlined_format_args,
        clippy::unused_result_ok,
        clippy::unwrap_in_result,
        clippy::unwrap_used,
        reason = "production code follows LionClaw's strict Clippy profile; tests keep fail-fast ergonomics"
    )
)]

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use tokio::{
    sync::{mpsc, oneshot},
    time::{timeout, timeout_at, Instant},
};
use tracing::warn;
use uuid::Uuid;

use lionclaw_runtime_api::{
    choose_terminal_transcript_target, load_ready_state_value, load_state_value,
    normalize_terminal_transcript_launch_started_at, safe_relative_path, save_state_value,
    ExecutionOutput, NetworkMode, RuntimeAdapter, RuntimeAdapterInfo, RuntimeArtifact,
    RuntimeAuthKind, RuntimeCapabilityResult, RuntimeControlExecution, RuntimeControlOutcome,
    RuntimeEvent, RuntimeEventSender, RuntimeExecutionContext, RuntimeFileChange,
    RuntimeFileChangeStatus, RuntimeMessageLane, RuntimeProgramExecutor, RuntimeProgramSession,
    RuntimeProgramSpec, RuntimeProgramTurnExecution, RuntimeSessionHandle, RuntimeSessionReady,
    RuntimeSessionStartInput, RuntimeTerminalProgramInput, RuntimeTerminalTranscript,
    RuntimeTerminalTranscriptInput, RuntimeTerminalTranscriptProgramExecutor,
    RuntimeTerminalTranscriptWarning, RuntimeTerminalTurn, RuntimeTerminalTurnStatus,
    RuntimeTurnMode, RuntimeTurnResult, TerminalTranscriptCandidate, TerminalTranscriptTarget,
    TerminalTranscriptTimestampPrecision,
};

const FILE_CHANGE_PATH_EVENT_LIMIT: usize = 50;
const CODEX_APP_SERVER_MAX_PAGE_LIMIT: u32 = 100;
const LIONCLAW_RUNTIME_CONTEXT_PATH: &str = "/runtime/AGENTS.generated.md";
const CODEX_GENERATED_IMAGES_RUNTIME_DIR: &str = "/runtime/home/.codex/generated_images";
const CODEX_RUNTIME_WORKSPACE_PATH: &str = "/workspace";
const CODEX_TRUSTED_LEVEL: &str = "trusted";

#[derive(Debug, Clone)]
pub struct CodexRuntimeConfig {
    pub executable: String,
    pub model: Option<String>,
}

impl Default for CodexRuntimeConfig {
    fn default() -> Self {
        Self {
            executable: "codex".to_string(),
            model: None,
        }
    }
}

#[derive(Debug)]
pub struct CodexRuntimeAdapter {
    config: CodexRuntimeConfig,
    sessions: Arc<RwLock<HashMap<String, CodexSessionState>>>,
}

const CODEX_THREAD_ID_STATE_FILE: &str = ".lionclaw-codex-thread-id";

#[derive(Debug, Clone)]
struct CodexSessionState {
    runtime_state_root: Option<PathBuf>,
    thread_id: Option<String>,
    active_turn: Option<ActiveCodexTurn>,
}

#[derive(Debug, Clone)]
struct ActiveCodexTurn {
    thread_id: String,
    turn_id: String,
    interrupt_tx: mpsc::UnboundedSender<CodexInterruptRequest>,
}

#[derive(Debug)]
struct CodexInterruptRequest {
    ack_tx: oneshot::Sender<Result<()>>,
}

struct CodexTranscriptSessionGuard<'a> {
    adapter: &'a CodexRuntimeAdapter,
    runtime_session_id: String,
}

impl Drop for CodexTranscriptSessionGuard<'_> {
    fn drop(&mut self) {
        self.adapter
            .remove_transcript_session(&self.runtime_session_id);
    }
}

#[derive(Clone)]
struct CodexThreadState {
    sessions: Arc<RwLock<HashMap<String, CodexSessionState>>>,
    runtime_session_id: String,
}

struct CodexTerminalTranscriptExportRequest<'a> {
    events: &'a RuntimeEventSender,
    thread_state: &'a CodexThreadState,
    runtime_state_root: &'a Path,
    resume_thread_id: Option<&'a str>,
    launch_started_at: Option<DateTime<Utc>>,
    deadline: Instant,
    hard_timeout: Duration,
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
        events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
        let RuntimeProgramTurnExecution {
            input,
            context,
            mut executor,
        } = execution;
        let network_mode = context.network_mode;
        let thread_state = self.thread_state_for(&input.runtime_session_id);
        let transport = self.start_app_server_transport(executor.as_mut()).await?;
        let mut client = CodexAppServerClient::new_with_runtime_context(transport, context);

        let result = async {
            client.initialize(&events, &thread_state).await?;
            let thread_id = self
                .ensure_app_server_thread(
                    &mut client,
                    &input.runtime_session_id,
                    &events,
                    &thread_state,
                )
                .await?;
            let response = client
                .request(
                    "turn/start",
                    turn_start_params(
                        &thread_id,
                        &input.prompt,
                        self.config.model.as_deref(),
                        network_mode,
                    ),
                    &events,
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
                    &events,
                    &thread_state,
                    Some(&mut interrupt_rx),
                )
                .await?;
            Ok(RuntimeTurnResult::default())
        }
        .await;

        finish_app_server_session(client, result).await
    }

    async fn run_app_server_control(
        &self,
        execution: RuntimeControlExecution,
        events: RuntimeEventSender,
    ) -> Result<RuntimeControlOutcome> {
        let command = execution.input.command_name.as_str();
        match command {
            "model" | "models" => self.run_model_list_control(execution, events).await,
            "permissions" | "permission" => Ok(RuntimeControlOutcome::InteractiveOnly {
                message:
                    "Codex permission changes must be made through LionClaw runtime policy, not a runtime slash command."
                        .to_string(),
            }),
            "rename" | "name" | "compact" => self.run_thread_control(execution, events).await,
            "review" => Ok(RuntimeControlOutcome::Unsupported {
                message: "Codex review control is not exposed through LionClaw yet.".to_string(),
            }),
            _ => Ok(RuntimeControlOutcome::Unsupported {
                message: format!("Codex does not support native control command '/{command}'"),
            }),
        }
    }

    async fn run_model_list_control(
        &self,
        execution: RuntimeControlExecution,
        events: RuntimeEventSender,
    ) -> Result<RuntimeControlOutcome> {
        let RuntimeControlExecution {
            input,
            mut executor,
            ..
        } = execution;
        let include_hidden = match model_list_include_hidden(&input.arguments) {
            Ok(include_hidden) => include_hidden,
            Err(outcome) => return Ok(outcome),
        };
        let thread_state = self.thread_state_for(&input.runtime_session_id);
        let transport = self.start_app_server_transport(executor.as_mut()).await?;
        let mut client = CodexAppServerClient::new(transport);
        let result = async {
            client.initialize(&events, &thread_state).await?;
            let response = client
                .request(
                    "model/list",
                    json!({
                        "includeHidden": include_hidden,
                    }),
                    &events,
                    &thread_state,
                )
                .await?;

            Ok(RuntimeControlOutcome::Handled {
                message: describe_model_list_response(&response),
            })
        }
        .await;

        finish_app_server_session(client, result).await
    }

    async fn run_thread_control(
        &self,
        execution: RuntimeControlExecution,
        events: RuntimeEventSender,
    ) -> Result<RuntimeControlOutcome> {
        let RuntimeControlExecution {
            input,
            mut executor,
            ..
        } = execution;
        let Some(saved_thread_id) = self.current_thread_id(&input.runtime_session_id)? else {
            return Ok(RuntimeControlOutcome::Failed {
                code: Some("runtime.control.thread_required".to_string()),
                message: format!(
                    "Codex control '/{}' needs an existing Codex thread; send a prompt first.",
                    input.command_name
                ),
            });
        };
        if let Some(outcome) =
            invalid_thread_control_arguments(&input.command_name, &input.arguments)
        {
            return Ok(outcome);
        }

        let thread_state = self.thread_state_for(&input.runtime_session_id);
        let transport = self.start_app_server_transport(executor.as_mut()).await?;
        let mut client = CodexAppServerClient::new(transport);

        let result = async {
            client.initialize(&events, &thread_state).await?;
            let thread_id = self
                .resume_app_server_thread(&mut client, &saved_thread_id, &events, &thread_state)
                .await?;

            let outcome = match input.command_name.as_str() {
                "rename" | "name" => {
                    let name = input.arguments.trim();
                    client
                        .request(
                            "thread/name/set",
                            json!({
                                "threadId": thread_id,
                                "name": name,
                            }),
                            &events,
                            &thread_state,
                        )
                        .await?;
                    RuntimeControlOutcome::Handled {
                        message: format!("Renamed Codex thread to '{name}'."),
                    }
                }
                "compact" => {
                    let (interrupt_tx, mut interrupt_rx) = mpsc::unbounded_channel();
                    client
                        .request(
                            "thread/compact/start",
                            json!({
                                "threadId": thread_id,
                            }),
                            &events,
                            &thread_state,
                        )
                        .await?;
                    client
                        .wait_for_context_compaction_completed(
                            &thread_id,
                            interrupt_tx,
                            &mut interrupt_rx,
                            &events,
                            &thread_state,
                        )
                        .await?;
                    RuntimeControlOutcome::Handled {
                        message: "Compacted Codex thread.".to_string(),
                    }
                }
                other => RuntimeControlOutcome::Unsupported {
                    message: format!("Codex does not support native control command '/{other}'"),
                },
            };

            Ok(outcome)
        }
        .await;

        finish_app_server_session(client, result).await
    }

    async fn start_app_server_transport(
        &self,
        executor: &mut dyn RuntimeProgramExecutor,
    ) -> Result<ExecutionSessionTransport> {
        let session = executor
            .spawn(build_codex_app_server_program(&self.config))
            .await?;
        Ok(ExecutionSessionTransport::new(session))
    }

    async fn ensure_app_server_thread<T>(
        &self,
        client: &mut CodexAppServerClient<T>,
        runtime_session_id: &str,
        events: &RuntimeEventSender,
        thread_state: &CodexThreadState,
    ) -> Result<String>
    where
        T: AppServerTransport + Send,
    {
        if let Some(thread_id) = self.current_thread_id(runtime_session_id)? {
            return self
                .resume_app_server_thread(client, &thread_id, events, thread_state)
                .await;
        }

        let response = client
            .request(
                "thread/start",
                thread_start_params(self.config.model.as_deref()),
                events,
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

    async fn resume_app_server_thread<T>(
        &self,
        client: &mut CodexAppServerClient<T>,
        thread_id: &str,
        events: &RuntimeEventSender,
        thread_state: &CodexThreadState,
    ) -> Result<String>
    where
        T: AppServerTransport + Send,
    {
        let response = client
            .request(
                "thread/resume",
                thread_resume_params(thread_id, self.config.model.as_deref()),
                events,
                thread_state,
            )
            .await?;
        let resolved_thread_id =
            extract_app_server_thread_id(&response).unwrap_or_else(|| thread_id.to_string());
        thread_state.persist_thread_id(&resolved_thread_id)?;
        Ok(resolved_thread_id)
    }

    fn thread_state_for(&self, runtime_session_id: &str) -> CodexThreadState {
        CodexThreadState {
            sessions: Arc::clone(&self.sessions),
            runtime_session_id: runtime_session_id.to_string(),
        }
    }

    fn current_thread_id(&self, runtime_session_id: &str) -> Result<Option<String>> {
        Ok(self.session_state(runtime_session_id)?.thread_id)
    }

    async fn export_terminal_transcript_from_app_server(
        &self,
        input: RuntimeTerminalTranscriptInput,
        executor: &mut dyn RuntimeTerminalTranscriptProgramExecutor,
        deadline: Instant,
        hard_timeout: Duration,
    ) -> Result<RuntimeTerminalTranscript> {
        let runtime_session_id = format!("codex-terminal-{}", input.session_id);
        let resume_thread_id = load_saved_thread_id(&input.runtime_state_root)?;
        let _session_guard =
            self.transcript_session_guard(&runtime_session_id, input.runtime_state_root.clone())?;
        let session = timeout_at(
            deadline,
            executor.spawn(build_codex_app_server_program(&self.config)),
        )
        .await
        .map_err(|_| codex_app_server_deadline_error(hard_timeout, "starting app-server"))?;
        let result = match session {
            Ok(session) => {
                let transport = ExecutionSessionTransport::new(session);
                let mut client = CodexAppServerClient::new(transport);
                let thread_state = self.thread_state_for(&runtime_session_id);
                let (events, _event_rx) = mpsc::unbounded_channel();
                let export = {
                    timeout_at(deadline, client.initialize(&events, &thread_state))
                        .await
                        .map_err(|_| {
                            codex_app_server_deadline_error(hard_timeout, "initializing app-server")
                        })??;
                    self.export_terminal_transcript_from_app_server_client(
                        &mut client,
                        CodexTerminalTranscriptExportRequest {
                            events: &events,
                            thread_state: &thread_state,
                            runtime_state_root: &input.runtime_state_root,
                            resume_thread_id: resume_thread_id.as_deref(),
                            launch_started_at: input.launch_started_at,
                            deadline,
                            hard_timeout,
                        },
                    )
                    .await
                };
                finish_app_server_transcript_session(client, export, deadline, hard_timeout).await
            }
            Err(err) => Err(err),
        };
        result
    }

    async fn export_terminal_transcript_from_app_server_client<T>(
        &self,
        client: &mut CodexAppServerClient<T>,
        request: CodexTerminalTranscriptExportRequest<'_>,
    ) -> Result<RuntimeTerminalTranscript>
    where
        T: AppServerTransport + Send,
    {
        let mut cursor = None;
        let mut seen_cursors = HashSet::new();
        let mut turns = Vec::new();
        let mut warnings = Vec::new();
        let mut target = TerminalTranscriptTarget::default();
        let mut seen_thread_ids = HashSet::new();
        let mut source_selection_reconciled = true;

        loop {
            let response_result = codex_app_server_request_until(
                client,
                "thread/list",
                codex_thread_list_params(cursor.as_deref()),
                request.events,
                request.thread_state,
                request.deadline,
            )
            .await;
            let response = match response_result {
                Ok(Some(response)) => response,
                Ok(None) => {
                    warnings.push(RuntimeTerminalTranscriptWarning::new(
                        "codex-app-server",
                        codex_app_server_deadline_warning(request.hard_timeout, "listing threads"),
                    ));
                    source_selection_reconciled = false;
                    break;
                }
                Err(err) if request.resume_thread_id.is_some() => {
                    warnings.push(RuntimeTerminalTranscriptWarning::new(
                        "codex-app-server",
                        format!("{err:#}"),
                    ));
                    source_selection_reconciled = false;
                    break;
                }
                Err(err) => {
                    return Err(err).context("failed to list Codex app-server threads");
                }
            };
            let listed_threads = match codex_app_server_threads(&response) {
                Ok(threads) => threads,
                Err(err) if request.resume_thread_id.is_some() => {
                    warnings.push(RuntimeTerminalTranscriptWarning::new(
                        "codex-app-server",
                        format!("{err:#}"),
                    ));
                    source_selection_reconciled = false;
                    break;
                }
                Err(err) => {
                    return Err(err).context("failed to list Codex app-server threads");
                }
            };
            let mut deadline_reached = false;
            for thread in listed_threads {
                if !seen_thread_ids.insert(thread.id.clone()) {
                    continue;
                }
                if target.is_empty() {
                    let latest = TerminalTranscriptCandidate::new(
                        thread.id.clone(),
                        codex_listed_thread_updated_at(&thread),
                    );
                    if let Some(thread_id) = choose_terminal_transcript_target(
                        request.resume_thread_id,
                        latest.as_ref(),
                        normalize_terminal_transcript_launch_started_at(
                            request.launch_started_at,
                            TerminalTranscriptTimestampPrecision::Seconds,
                        ),
                    ) {
                        if target.choose_if_empty(&thread_id) {
                            save_thread_id(request.runtime_state_root, &thread_id)?;
                        }
                    }
                }
                match codex_app_server_thread_terminal_transcript(
                    client,
                    request.events,
                    request.thread_state,
                    &thread,
                    request.deadline,
                )
                .await
                {
                    Ok(thread_transcript) => {
                        if codex_record_app_server_thread_transcript(
                            &mut target,
                            &thread,
                            thread_transcript,
                            &mut turns,
                        ) {
                            warnings.push(RuntimeTerminalTranscriptWarning::new(
                                format!("codex-app-server:{}", thread.id),
                                codex_app_server_deadline_warning(
                                    request.hard_timeout,
                                    "listing thread turns",
                                ),
                            ));
                            deadline_reached = true;
                        }
                        if deadline_reached {
                            break;
                        }
                    }
                    Err(err) => {
                        warnings.push(RuntimeTerminalTranscriptWarning::new(
                            format!("codex-app-server:{}", thread.id),
                            format!("{err:#}"),
                        ));
                        continue;
                    }
                }
            }
            if deadline_reached {
                break;
            }

            let next_cursor = response
                .get("nextCursor")
                .and_then(Value::as_str)
                .and_then(non_empty);
            let Some(next_cursor) = next_cursor else {
                break;
            };
            if !seen_cursors.insert(next_cursor.clone()) {
                bail!("codex app-server repeated thread/list cursor {next_cursor}");
            }
            cursor = Some(next_cursor);
        }

        let fallback_thread_id = target.unreconciled_id().map(str::to_string).or_else(|| {
            request
                .resume_thread_id
                .filter(|_| target.is_empty())
                .map(str::to_string)
        });

        if let Some(thread_id) = fallback_thread_id {
            if !seen_thread_ids.contains(&thread_id) {
                let thread = CodexListedThread {
                    id: thread_id,
                    started_at: None,
                    finished_at: None,
                };
                if target.choose_if_empty(&thread.id) {
                    save_thread_id(request.runtime_state_root, &thread.id)?;
                }
                match codex_app_server_thread_terminal_transcript(
                    client,
                    request.events,
                    request.thread_state,
                    &thread,
                    request.deadline,
                )
                .await
                {
                    Ok(thread_transcript) => {
                        if codex_record_app_server_thread_transcript(
                            &mut target,
                            &thread,
                            thread_transcript,
                            &mut turns,
                        ) {
                            warnings.push(RuntimeTerminalTranscriptWarning::new(
                                format!("codex-app-server:{}", thread.id),
                                codex_app_server_deadline_warning(
                                    request.hard_timeout,
                                    "listing thread turns",
                                ),
                            ));
                        }
                    }
                    Err(err) => warnings.push(RuntimeTerminalTranscriptWarning::new(
                        format!("codex-app-server:{}", thread.id),
                        format!("{err:#}"),
                    )),
                }
            }
        }

        turns.sort_by(|left, right| {
            left.started_at
                .cmp(&right.started_at)
                .then_with(|| left.source_id.cmp(&right.source_id))
        });
        Ok(RuntimeTerminalTranscript::new(
            turns,
            warnings,
            target.transcript_state(source_selection_reconciled),
        ))
    }

    fn transcript_session_guard(
        &self,
        runtime_session_id: &str,
        runtime_state_root: PathBuf,
    ) -> Result<CodexTranscriptSessionGuard<'_>> {
        self.sessions
            .write()
            .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?
            .insert(
                runtime_session_id.to_string(),
                CodexSessionState {
                    runtime_state_root: Some(runtime_state_root),
                    thread_id: None,
                    active_turn: None,
                },
            );
        Ok(CodexTranscriptSessionGuard {
            adapter: self,
            runtime_session_id: runtime_session_id.to_string(),
        })
    }

    fn remove_transcript_session(&self, runtime_session_id: &str) {
        if let Ok(mut sessions) = self.sessions.write() {
            sessions.remove(runtime_session_id);
        }
    }

    fn session_state(&self, runtime_session_id: &str) -> Result<CodexSessionState> {
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
        events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
        self.run_app_server_turn(execution, events).await
    }

    fn build_terminal_program(
        &self,
        input: RuntimeTerminalProgramInput,
    ) -> Result<RuntimeProgramSpec> {
        Ok(build_codex_terminal_program(
            &self.config,
            load_ready_saved_thread_id(&input.runtime_state_root, input.runtime_session_ready)?,
        ))
    }

    async fn export_terminal_transcript(
        &self,
        input: RuntimeTerminalTranscriptInput,
        executor: &mut dyn RuntimeTerminalTranscriptProgramExecutor,
    ) -> Result<RuntimeTerminalTranscript> {
        let hard_timeout = executor.hard_timeout();
        let deadline = Instant::now() + hard_timeout;
        self.export_terminal_transcript_from_app_server(input, executor, deadline, hard_timeout)
            .await
            .context("failed to export Codex native TUI transcript through app-server")
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

fn codex_record_app_server_thread_transcript(
    target: &mut TerminalTranscriptTarget,
    thread: &CodexListedThread,
    transcript: CodexThreadTerminalTranscript,
    turns: &mut Vec<RuntimeTerminalTurn>,
) -> bool {
    let deadline_reached = transcript.deadline_reached;
    target.record_reconciliation(&thread.id, !deadline_reached, transcript.resumable);
    turns.extend(transcript.turns);
    deadline_reached
}

fn build_codex_app_server_program(config: &CodexRuntimeConfig) -> RuntimeProgramSpec {
    let mut args = codex_runtime_config_override_args();
    args.push("app-server".to_string());

    RuntimeProgramSpec {
        executable: config.executable.clone(),
        args,
        environment: Vec::new(),
        stdin: String::new(),
        auth: Some(RuntimeAuthKind::Codex),
    }
}

fn build_codex_terminal_program(
    config: &CodexRuntimeConfig,
    thread_id: Option<String>,
) -> RuntimeProgramSpec {
    let mut args = vec![
        "--sandbox".to_string(),
        "danger-full-access".to_string(),
        "--ask-for-approval".to_string(),
        "never".to_string(),
    ];
    args.extend(codex_terminal_config_override_args());
    if let Some(model) = &config.model {
        args.push("--model".to_string());
        args.push(model.clone());
    }
    if let Some(thread_id) = thread_id {
        args.push("resume".to_string());
        args.push(thread_id);
    }

    RuntimeProgramSpec {
        executable: config.executable.clone(),
        args,
        environment: Vec::new(),
        stdin: String::new(),
        auth: Some(RuntimeAuthKind::Codex),
    }
}

fn codex_terminal_config_override_args() -> Vec<String> {
    let mut args = codex_runtime_config_override_args();
    args.push("-c".to_string());
    args.push(format!(
        "model_instructions_file=\"{LIONCLAW_RUNTIME_CONTEXT_PATH}\""
    ));
    args
}

fn codex_runtime_config_override_args() -> Vec<String> {
    vec![
        "-c".to_string(),
        "check_for_update_on_startup=false".to_string(),
        "-c".to_string(),
        format!(
            "projects.\"{CODEX_RUNTIME_WORKSPACE_PATH}\".trust_level=\"{CODEX_TRUSTED_LEVEL}\""
        ),
    ]
}

fn codex_thread_list_params(cursor: Option<&str>) -> Value {
    json!({
        "cursor": cursor,
        "limit": CODEX_APP_SERVER_MAX_PAGE_LIMIT,
        "sortKey": "updated_at",
        "sortDirection": "desc",
        "sourceKinds": ["cli"],
        "archived": false,
    })
}

fn codex_thread_turns_list_params(thread_id: &str, cursor: Option<&str>) -> Value {
    json!({
        "threadId": thread_id,
        "cursor": cursor,
        "limit": CODEX_APP_SERVER_MAX_PAGE_LIMIT,
        "sortDirection": "desc",
        "itemsView": "full",
    })
}

#[derive(Debug, Clone)]
struct CodexListedThread {
    id: String,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
}

fn codex_listed_thread_updated_at(thread: &CodexListedThread) -> Option<DateTime<Utc>> {
    thread
        .finished_at
        .as_ref()
        .or(thread.started_at.as_ref())
        .cloned()
}

#[derive(Debug, Clone)]
struct CodexThreadTerminalTranscript {
    turns: Vec<RuntimeTerminalTurn>,
    resumable: bool,
    deadline_reached: bool,
}

#[derive(Debug, Clone)]
struct CodexTerminalTurnPage {
    turns: Vec<RuntimeTerminalTurn>,
    newest_turn_resumable: Option<bool>,
}

fn codex_app_server_threads(response: &Value) -> Result<Vec<CodexListedThread>> {
    let threads = response
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("codex thread/list response missing data"))?;
    Ok(threads
        .iter()
        .filter_map(|thread| {
            let id = thread
                .get("id")
                .and_then(Value::as_str)
                .and_then(non_empty)?;
            Some(CodexListedThread {
                id,
                started_at: codex_app_server_timestamp(thread.get("createdAt")),
                finished_at: codex_app_server_timestamp(thread.get("updatedAt")),
            })
        })
        .collect())
}

async fn codex_app_server_thread_terminal_transcript<T>(
    client: &mut CodexAppServerClient<T>,
    events: &RuntimeEventSender,
    thread_state: &CodexThreadState,
    thread: &CodexListedThread,
    deadline: Instant,
) -> Result<CodexThreadTerminalTranscript>
where
    T: AppServerTransport + Send,
{
    let mut cursor = None;
    let mut seen_cursors = HashSet::new();
    let mut turns = Vec::new();
    let mut newest_turn_resumable = None;

    loop {
        let response = codex_app_server_request_until(
            client,
            "thread/turns/list",
            codex_thread_turns_list_params(&thread.id, cursor.as_deref()),
            events,
            thread_state,
            deadline,
        )
        .await;
        let response = match response {
            Ok(Some(response)) => response,
            Ok(None) => {
                return Ok(CodexThreadTerminalTranscript {
                    turns,
                    resumable: newest_turn_resumable.unwrap_or(false),
                    deadline_reached: true,
                });
            }
            Err(err) if codex_thread_turns_unmaterialized_error(&err) => {
                return Ok(CodexThreadTerminalTranscript {
                    turns,
                    resumable: false,
                    deadline_reached: false,
                });
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "failed to list Codex app-server turns for thread {}",
                        thread.id
                    )
                });
            }
        };
        let page = codex_app_server_terminal_turn_page(thread, &response)?;
        if newest_turn_resumable.is_none() {
            newest_turn_resumable = page.newest_turn_resumable;
        }
        turns.extend(page.turns);

        let next_cursor = response
            .get("nextCursor")
            .and_then(Value::as_str)
            .and_then(non_empty);
        let Some(next_cursor) = next_cursor else {
            break;
        };
        if !seen_cursors.insert(next_cursor.clone()) {
            bail!(
                "codex app-server repeated thread/turns/list cursor {next_cursor} for thread {}",
                thread.id
            );
        }
        cursor = Some(next_cursor);
    }

    Ok(CodexThreadTerminalTranscript {
        turns,
        resumable: newest_turn_resumable.unwrap_or(false),
        deadline_reached: false,
    })
}

async fn codex_app_server_request_until<T>(
    client: &mut CodexAppServerClient<T>,
    method: &str,
    params: Value,
    events: &RuntimeEventSender,
    thread_state: &CodexThreadState,
    deadline: Instant,
) -> Result<Option<Value>>
where
    T: AppServerTransport + Send,
{
    if Instant::now() >= deadline {
        return Ok(None);
    }
    match timeout_at(
        deadline,
        client.request(method, params, events, thread_state),
    )
    .await
    {
        Ok(response) => response.map(Some),
        Err(_) => Ok(None),
    }
}

fn codex_app_server_deadline_error(hard_timeout: Duration, action: &str) -> anyhow::Error {
    anyhow!(
        "timed out after {}s while {action} for Codex native TUI transcript through app-server",
        hard_timeout.as_secs_f32()
    )
}

fn codex_app_server_deadline_warning(hard_timeout: Duration, action: &str) -> String {
    format!(
        "timed out after {}s while {action} for Codex native TUI transcript through app-server; returning partial transcript",
        hard_timeout.as_secs_f32()
    )
}

fn codex_thread_turns_unmaterialized_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<CodexAppServerResponseError>()
            .is_some_and(|err| {
                err.method == "thread/turns/list"
                    && err
                        .message
                        .contains("thread/turns/list is unavailable before first user message")
            })
    })
}

fn codex_app_server_terminal_turn_page(
    thread: &CodexListedThread,
    response: &Value,
) -> Result<CodexTerminalTurnPage> {
    let page = response
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("codex thread/turns/list response missing data"))?;
    let mut turns = Vec::new();
    let mut newest_turn_resumable = None;
    for (index, turn) in page.iter().enumerate() {
        let terminal_turn =
            codex_app_server_terminal_turn(&thread.id, thread.started_at, thread.finished_at, turn);
        if index == 0 {
            newest_turn_resumable = Some(codex_app_server_turn_is_successful_for_resume(
                turn,
                terminal_turn.as_ref(),
            ));
        }
        if let Some(turn) = terminal_turn {
            turns.push(turn);
        }
    }
    Ok(CodexTerminalTurnPage {
        turns,
        newest_turn_resumable,
    })
}

fn codex_app_server_terminal_turn(
    thread_id: &str,
    thread_started_at: Option<DateTime<Utc>>,
    thread_finished_at: Option<DateTime<Utc>>,
    turn: &Value,
) -> Option<RuntimeTerminalTurn> {
    let turn_id = turn.get("id").and_then(Value::as_str).and_then(non_empty)?;
    let user_text = codex_app_server_turn_user_text(turn)?;
    let assistant_text = codex_app_server_turn_assistant_text(turn)?;
    let (started_at, finished_at) =
        codex_app_server_turn_times(turn, thread_started_at, thread_finished_at)?;
    let (status, error_code, error_text) = codex_app_server_turn_status(turn);

    Some(RuntimeTerminalTurn {
        source_id: format!("codex-app-server:{thread_id}:{turn_id}"),
        display_user_text: user_text.clone(),
        prompt_user_text: user_text,
        assistant_text,
        status,
        error_code,
        error_text,
        started_at,
        finished_at,
    })
}

fn codex_app_server_turn_times(
    turn: &Value,
    thread_started_at: Option<DateTime<Utc>>,
    thread_finished_at: Option<DateTime<Utc>>,
) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let turn_started_at = codex_app_server_timestamp(turn.get("startedAt"));
    let turn_finished_at = codex_app_server_timestamp(turn.get("completedAt"));
    let started_at = turn_started_at
        .or(thread_started_at)
        .or(turn_finished_at)
        .or(thread_finished_at)?;
    let finished_at = turn_finished_at
        .or(thread_finished_at)
        .unwrap_or(started_at);
    Some((started_at, finished_at))
}

fn codex_app_server_turn_user_text(turn: &Value) -> Option<String> {
    let parts = codex_app_server_turn_items(turn)
        .filter(|item| item.get("type").and_then(Value::as_str) == Some("userMessage"))
        .flat_map(|item| {
            item.get("content")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        })
        .filter_map(codex_app_server_user_input_text)
        .collect::<Vec<_>>();
    non_empty(&parts.join("\n\n"))
}

fn codex_app_server_turn_assistant_text(turn: &Value) -> Option<String> {
    let mut final_or_unknown = Vec::new();
    let mut commentary = Vec::new();
    for item in codex_app_server_turn_items(turn)
        .filter(|item| item.get("type").and_then(Value::as_str) == Some("agentMessage"))
    {
        let Some(text) = item.get("text").and_then(Value::as_str).and_then(non_empty) else {
            continue;
        };
        match item.get("phase").and_then(Value::as_str) {
            Some("commentary") => commentary.push(text),
            _ => final_or_unknown.push(text),
        }
    }
    if final_or_unknown.is_empty() {
        non_empty(&commentary.join("\n\n"))
    } else {
        non_empty(&final_or_unknown.join("\n\n"))
    }
}

fn codex_app_server_turn_items(turn: &Value) -> impl Iterator<Item = &Value> {
    turn.get("items")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
}

fn codex_app_server_user_input_text(input: &Value) -> Option<String> {
    match input.get("type").and_then(Value::as_str) {
        Some("text") => input
            .get("text")
            .and_then(Value::as_str)
            .and_then(non_empty),
        _ => None,
    }
}

fn codex_app_server_turn_status(
    turn: &Value,
) -> (RuntimeTerminalTurnStatus, Option<String>, Option<String>) {
    let status = codex_app_server_terminal_turn_status(turn.get("status").and_then(Value::as_str));
    let error_text = codex_app_server_turn_error_text(turn);
    let status = if status == RuntimeTerminalTurnStatus::Completed && error_text.is_some() {
        RuntimeTerminalTurnStatus::Failed
    } else {
        status
    };
    let error_code = error_text
        .as_ref()
        .map(|_| "runtime.codex.turn_failed".to_string());
    (status, error_code, error_text)
}

fn codex_app_server_turn_is_successful_for_resume(
    raw_turn: &Value,
    imported_turn: Option<&RuntimeTerminalTurn>,
) -> bool {
    codex_app_server_turn_has_completed_status(raw_turn)
        && codex_app_server_turn_error_text(raw_turn).is_none()
        && imported_turn.is_some_and(|turn| turn.status == RuntimeTerminalTurnStatus::Completed)
}

fn codex_app_server_turn_has_completed_status(turn: &Value) -> bool {
    turn.get("status")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|status| !status.is_empty())
        .is_some_and(|status| status.eq_ignore_ascii_case("completed"))
}

fn codex_app_server_turn_error_text(turn: &Value) -> Option<String> {
    turn.get("error")
        .filter(|error| !error.is_null())
        .map(app_server_error_text)
}

fn codex_app_server_terminal_turn_status(status: Option<&str>) -> RuntimeTerminalTurnStatus {
    let Some(status) = status.map(str::trim).filter(|status| !status.is_empty()) else {
        return RuntimeTerminalTurnStatus::Completed;
    };
    if status.eq_ignore_ascii_case("completed") {
        RuntimeTerminalTurnStatus::Completed
    } else if status.eq_ignore_ascii_case("failed") {
        RuntimeTerminalTurnStatus::Failed
    } else {
        RuntimeTerminalTurnStatus::Interrupted
    }
}

fn codex_app_server_timestamp(value: Option<&Value>) -> Option<DateTime<Utc>> {
    value
        .and_then(Value::as_i64)
        .and_then(|seconds| DateTime::<Utc>::from_timestamp(seconds, 0))
}

#[async_trait]
trait AppServerTransport {
    async fn send(&mut self, message: &Value) -> Result<()>;
    async fn recv(&mut self) -> Result<Option<Value>>;
    async fn shutdown(&mut self) -> Result<ExecutionOutput>;
}

struct ExecutionSessionTransport {
    session: Option<Box<dyn RuntimeProgramSession>>,
}

impl ExecutionSessionTransport {
    fn new(session: Box<dyn RuntimeProgramSession>) -> Self {
        Self {
            session: Some(session),
        }
    }
}

#[async_trait]
impl AppServerTransport for ExecutionSessionTransport {
    async fn send(&mut self, message: &Value) -> Result<()> {
        let session = self
            .session
            .as_mut()
            .context("codex app-server session is already closed")?;
        session.write_line(&serde_json::to_string(message)?).await
    }

    async fn recv(&mut self) -> Result<Option<Value>> {
        let session = self
            .session
            .as_mut()
            .context("codex app-server session is already closed")?;
        loop {
            let Some(line) = session.read_line().await? else {
                return Ok(None);
            };
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            return serde_json::from_str(trimmed)
                .with_context(|| format!("invalid codex app-server JSON-RPC line: {trimmed}"))
                .map(Some);
        }
    }

    async fn shutdown(&mut self) -> Result<ExecutionOutput> {
        let Some(session) = self.session.take() else {
            return Ok(ExecutionOutput::default());
        };
        session.shutdown().await
    }
}

struct CodexAppServerClient<T> {
    transport: T,
    runtime_context: Option<RuntimeExecutionContext>,
    next_id: u64,
    completed_turns: HashSet<String>,
    completed_turn_threads: HashSet<String>,
    unmatched_turn_completed: bool,
    failed_turns: HashMap<String, String>,
    unmatched_turn_failure: Option<String>,
    started_thread_turns: HashMap<String, String>,
    agent_message_lanes: HashMap<String, RuntimeMessageLane>,
    active_agent_message_item_id: Option<String>,
    active_answer_item_ids: HashSet<String>,
    last_answer_item_id: Option<String>,
    completed_context_compaction_threads: HashSet<String>,
    unmatched_context_compaction_completed: bool,
    emitted_artifact_ids: HashSet<String>,
}

impl<T> CodexAppServerClient<T>
where
    T: AppServerTransport + Send,
{
    fn new(transport: T) -> Self {
        Self::new_with_optional_runtime_context(transport, None)
    }

    fn new_with_runtime_context(transport: T, runtime_context: RuntimeExecutionContext) -> Self {
        Self::new_with_optional_runtime_context(transport, Some(runtime_context))
    }

    fn new_with_optional_runtime_context(
        transport: T,
        runtime_context: Option<RuntimeExecutionContext>,
    ) -> Self {
        Self {
            transport,
            runtime_context,
            next_id: 1,
            completed_turns: HashSet::new(),
            completed_turn_threads: HashSet::new(),
            unmatched_turn_completed: false,
            failed_turns: HashMap::new(),
            unmatched_turn_failure: None,
            started_thread_turns: HashMap::new(),
            agent_message_lanes: HashMap::new(),
            active_agent_message_item_id: None,
            active_answer_item_ids: HashSet::new(),
            last_answer_item_id: None,
            completed_context_compaction_threads: HashSet::new(),
            unmatched_context_compaction_completed: false,
            emitted_artifact_ids: HashSet::new(),
        }
    }

    async fn initialize(
        &mut self,
        events: &RuntimeEventSender,
        thread_state: &CodexThreadState,
    ) -> Result<()> {
        self.request(
            "initialize",
            json!({
                "clientInfo": {
                    "name": "lionclaw",
                    "title": "LionClaw",
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "capabilities": {
                    "experimentalApi": true,
                },
            }),
            events,
            thread_state,
        )
        .await?;
        self.notify("initialized", json!({})).await
    }

    async fn notify(&mut self, method: &str, params: Value) -> Result<()> {
        self.transport
            .send(&app_server_notification_message(method, params))
            .await
    }

    async fn request(
        &mut self,
        method: &str,
        params: Value,
        events: &RuntimeEventSender,
        thread_state: &CodexThreadState,
    ) -> Result<Value> {
        let id = self.next_request_id();
        self.transport
            .send(&app_server_request_message(id, method, params))
            .await?;

        loop {
            let Some(message) = self.transport.recv().await? else {
                bail!("codex app-server closed before responding to {method}");
            };
            if response_id(&message).is_some_and(|response_id| response_id == id) {
                return parse_app_server_response(message, method);
            }
            self.handle_message(message, events, thread_state).await?;
            if let Some(message) = self.turn_failure(None) {
                bail!("{message}");
            }
        }
    }

    async fn wait_for_turn_completed(
        &mut self,
        turn_id: Option<&str>,
        thread_id: Option<&str>,
        interrupt_tx: Option<mpsc::UnboundedSender<CodexInterruptRequest>>,
        events: &RuntimeEventSender,
        thread_state: &CodexThreadState,
        mut interrupt_rx: Option<&mut mpsc::UnboundedReceiver<CodexInterruptRequest>>,
    ) -> Result<()> {
        if let Some(message) = self.turn_failure(turn_id) {
            bail!("{message}");
        }
        let mut registered_turn_id = None;
        let result = async {
            self.register_active_wait_turn(
                thread_id,
                turn_id,
                interrupt_tx.as_ref(),
                &mut registered_turn_id,
                thread_state,
            )?;
            if self.wait_turn_completed(turn_id, thread_id) {
                return Ok(());
            }

            loop {
                if let Some(interrupt_rx) = interrupt_rx.as_mut() {
                    tokio::select! {
                        message = self.transport.recv() => {
                            let Some(message) = message? else {
                                bail!("codex app-server closed before turn completed");
                            };
                            self.handle_message(message, events, thread_state).await?;
                        }
                        interrupt = interrupt_rx.recv() => {
                            if let Some(interrupt) = interrupt {
                                self.handle_turn_interrupt(
                                    thread_id,
                                    registered_turn_id.as_deref().or(turn_id),
                                    interrupt,
                                    events,
                                    thread_state,
                                )
                                .await;
                            }
                        }
                    }
                } else {
                    let Some(message) = self.transport.recv().await? else {
                        bail!("codex app-server closed before turn completed");
                    };
                    self.handle_message(message, events, thread_state).await?;
                }
                self.register_active_wait_turn(
                    thread_id,
                    turn_id,
                    interrupt_tx.as_ref(),
                    &mut registered_turn_id,
                    thread_state,
                )?;
                if let Some(message) = self.turn_failure(turn_id) {
                    bail!("{message}");
                }
                if self.wait_turn_completed(turn_id, thread_id) {
                    return Ok(());
                }
            }
        }
        .await;

        if let (Some(thread_id), Some(turn_id)) = (thread_id, registered_turn_id.as_deref()) {
            thread_state.clear_active_turn(thread_id, turn_id)?;
        }
        result
    }

    async fn wait_for_context_compaction_completed(
        &mut self,
        thread_id: &str,
        interrupt_tx: mpsc::UnboundedSender<CodexInterruptRequest>,
        interrupt_rx: &mut mpsc::UnboundedReceiver<CodexInterruptRequest>,
        events: &RuntimeEventSender,
        thread_state: &CodexThreadState,
    ) -> Result<()> {
        if let Some(message) = self.turn_failure(None) {
            bail!("{message}");
        }
        if self.context_compaction_completed(thread_id) && self.thread_turn_completed(thread_id) {
            return Ok(());
        }

        let mut registered_turn_id = None;
        let result = async {
            self.register_active_wait_turn(
                Some(thread_id),
                None,
                Some(&interrupt_tx),
                &mut registered_turn_id,
                thread_state,
            )?;
            loop {
                tokio::select! {
                    message = self.transport.recv() => {
                        let Some(message) = message? else {
                            bail!("codex app-server closed before context compaction completed");
                        };
                        self.handle_message(message, events, thread_state).await?;
                    }
                    interrupt = interrupt_rx.recv() => {
                        if let Some(interrupt) = interrupt {
                            self.handle_turn_interrupt(
                                Some(thread_id),
                                registered_turn_id.as_deref(),
                                interrupt,
                                events,
                                thread_state,
                            )
                            .await;
                        }
                    }
                }
                self.register_active_wait_turn(
                    Some(thread_id),
                    None,
                    Some(&interrupt_tx),
                    &mut registered_turn_id,
                    thread_state,
                )?;
                if let Some(message) = self.turn_failure(None) {
                    bail!("{message}");
                }
                if self.context_compaction_completed(thread_id)
                    && self.thread_turn_completed(thread_id)
                {
                    return Ok(());
                }
            }
        }
        .await;

        if let Some(turn_id) = registered_turn_id.as_deref() {
            thread_state.clear_active_turn(thread_id, turn_id)?;
        }
        result
    }

    async fn handle_turn_interrupt(
        &mut self,
        thread_id: Option<&str>,
        turn_id: Option<&str>,
        interrupt: CodexInterruptRequest,
        events: &RuntimeEventSender,
        thread_state: &CodexThreadState,
    ) {
        let result = match (thread_id, turn_id) {
            (Some(thread_id), Some(turn_id)) => {
                self.interrupt_turn(thread_id, turn_id, events, thread_state)
                    .await
            }
            _ => Err(anyhow!(
                "codex turn/interrupt requested before app-server reported an active turn id"
            )),
        };
        drop(interrupt.ack_tx.send(result));
    }

    async fn interrupt_turn(
        &mut self,
        thread_id: &str,
        turn_id: &str,
        events: &RuntimeEventSender,
        thread_state: &CodexThreadState,
    ) -> Result<()> {
        let method = "turn/interrupt";
        let id = self.next_request_id();
        self.transport
            .send(&app_server_request_message(
                id,
                method,
                json!({
                    "threadId": thread_id,
                    "turnId": turn_id,
                }),
            ))
            .await?;

        loop {
            let Some(message) = self.transport.recv().await? else {
                bail!("codex app-server closed before responding to {method}");
            };
            if response_id(&message).is_some_and(|response_id| response_id == id) {
                parse_app_server_response(message, method)?;
                return Ok(());
            }
            self.handle_message(message, events, thread_state).await?;
        }
    }

    async fn shutdown(mut self) -> Result<ExecutionOutput> {
        self.transport.shutdown().await
    }

    fn next_request_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn turn_completed(&self, turn_id: Option<&str>) -> bool {
        match turn_id {
            Some(turn_id) => {
                self.unmatched_turn_completed || self.completed_turns.contains(turn_id)
            }
            None => self.unmatched_turn_completed || !self.completed_turns.is_empty(),
        }
    }

    fn wait_turn_completed(&self, turn_id: Option<&str>, thread_id: Option<&str>) -> bool {
        match (turn_id, thread_id) {
            (Some(turn_id), _) => self.turn_completed(Some(turn_id)),
            (None, Some(thread_id)) => self.thread_turn_completed(thread_id),
            (None, None) => self.turn_completed(None),
        }
    }

    fn thread_turn_completed(&self, thread_id: &str) -> bool {
        self.unmatched_turn_completed || self.completed_turn_threads.contains(thread_id)
    }

    fn register_active_wait_turn(
        &self,
        thread_id: Option<&str>,
        turn_id: Option<&str>,
        interrupt_tx: Option<&mpsc::UnboundedSender<CodexInterruptRequest>>,
        registered_turn_id: &mut Option<String>,
        thread_state: &CodexThreadState,
    ) -> Result<()> {
        let (Some(thread_id), Some(interrupt_tx)) = (thread_id, interrupt_tx) else {
            return Ok(());
        };
        if registered_turn_id.is_some() {
            return Ok(());
        }
        let turn_id = turn_id
            .map(str::to_string)
            .or_else(|| self.started_thread_turns.get(thread_id).cloned());
        let Some(turn_id) = turn_id else {
            return Ok(());
        };

        thread_state.set_active_turn(thread_id, &turn_id, interrupt_tx.clone())?;
        *registered_turn_id = Some(turn_id);
        Ok(())
    }

    fn turn_failure(&self, turn_id: Option<&str>) -> Option<&str> {
        match turn_id {
            Some(turn_id) => self
                .failed_turns
                .get(turn_id)
                .map(String::as_str)
                .or(self.unmatched_turn_failure.as_deref()),
            None => self
                .unmatched_turn_failure
                .as_deref()
                .or_else(|| self.failed_turns.values().next().map(String::as_str)),
        }
    }

    fn remember_turn_failure(&mut self, params: &Value, message: String) {
        if let Some(turn_id) = extract_app_server_turn_id(params) {
            self.failed_turns.insert(turn_id, message);
        } else {
            self.unmatched_turn_failure = Some(message);
        }
    }

    fn context_compaction_completed(&self, thread_id: &str) -> bool {
        self.unmatched_context_compaction_completed
            || self
                .completed_context_compaction_threads
                .contains(thread_id)
    }

    async fn handle_message(
        &mut self,
        message: Value,
        events: &RuntimeEventSender,
        thread_state: &CodexThreadState,
    ) -> Result<()> {
        if message.get("id").is_some() && message.get("method").is_some() {
            self.respond_to_server_request(&message).await?;
            return Ok(());
        }

        let Some(method) = message.get("method").and_then(Value::as_str) else {
            if let Some(artifact) =
                self.generated_artifact_for_message(None, &message, thread_state)?
            {
                drop(events.send(RuntimeEvent::Artifact { artifact }));
            }
            return Ok(());
        };
        let params = message.get("params").unwrap_or(&Value::Null);
        if let Some(artifact) =
            self.generated_artifact_for_message(Some(method), params, thread_state)?
        {
            drop(events.send(RuntimeEvent::Artifact { artifact }));
        }
        match method {
            "thread/started" => {
                if let Some(thread_id) = extract_app_server_thread_id(params) {
                    thread_state.persist_thread_id(&thread_id)?;
                    drop(events.send(RuntimeEvent::Status {
                        code: None,
                        text: format!("codex thread started: {thread_id}"),
                    }));
                }
            }
            "thread/name/updated" => {
                drop(events.send(RuntimeEvent::Status {
                    code: None,
                    text: "codex thread renamed".to_string(),
                }));
            }
            "turn/started" => {
                self.reset_turn_message_state();
                if let (Some(thread_id), Some(turn_id)) = (
                    extract_app_server_thread_id(params),
                    extract_app_server_turn_id(params),
                ) {
                    self.started_thread_turns.insert(thread_id, turn_id);
                }
                drop(events.send(RuntimeEvent::Status {
                    code: None,
                    text: "codex turn started".to_string(),
                }));
            }
            "turn/completed" => {
                self.reset_turn_message_state();
                if let Some(message) = completed_turn_error_text(params) {
                    self.remember_turn_failure(params, message);
                } else {
                    if let Some(thread_id) = extract_app_server_thread_id(params) {
                        self.completed_turn_threads.insert(thread_id);
                    }
                    if let Some(turn_id) = extract_app_server_turn_id(params) {
                        self.completed_turns.insert(turn_id);
                    } else {
                        self.unmatched_turn_completed = true;
                    }
                    drop(events.send(RuntimeEvent::Status {
                        code: None,
                        text: "codex turn completed".to_string(),
                    }));
                    drop(events.send(RuntimeEvent::Done));
                }
            }
            "item/agentMessage/delta" => {
                if let Some(text) = app_server_text(params, &["delta", "text", "content"]) {
                    let lane = self.agent_message_lane(params);
                    if lane == RuntimeMessageLane::Answer {
                        self.record_answer_delta_boundary(params, events);
                    }
                    drop(events.send(RuntimeEvent::MessageDelta { lane, text }));
                }
            }
            "item/agentReasoning/delta" | "item/agentReasoningRawContent/delta" => {
                if let Some(text) = app_server_text(params, &["delta", "text", "content"]) {
                    drop(events.send(RuntimeEvent::MessageDelta {
                        lane: RuntimeMessageLane::Reasoning,
                        text,
                    }));
                }
            }
            "item/started" | "item/completed" | "item/updated" => {
                for event in self.record_app_server_item(method, params) {
                    drop(events.send(event));
                }
            }
            "error" => {
                let message = app_server_error_text(params);
                if error_notification_will_retry(params) {
                    drop(events.send(RuntimeEvent::Status {
                        code: Some("runtime.retrying".to_string()),
                        text: message,
                    }));
                } else {
                    self.remember_turn_failure(params, message);
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn generated_artifact_for_message(
        &mut self,
        method: Option<&str>,
        message: &Value,
        thread_state: &CodexThreadState,
    ) -> Result<Option<RuntimeArtifact>> {
        let Some(payload) = codex_generated_image_payload(method, message) else {
            return Ok(None);
        };

        let Some(call_id) = codex_generated_image_call_id(payload) else {
            return Ok(None);
        };
        let thread_id = extract_app_server_thread_id(payload)
            .or_else(|| extract_app_server_thread_id(message))
            .or_else(|| thread_state.current_thread_id().ok().flatten());
        let Some(thread_id) = thread_id else {
            return Ok(None);
        };
        let Some(runtime_state_root) = thread_state.runtime_state_root()? else {
            return Ok(None);
        };

        let filename = format!("{call_id}.png");
        let path = if let Some(saved_path) = codex_generated_image_saved_path(payload) {
            let Some(path) = codex_generated_image_path(
                saved_path,
                &runtime_state_root,
                self.runtime_context.as_ref(),
            ) else {
                return Ok(None);
            };
            path
        } else {
            codex_default_generated_image_path(
                &thread_id,
                &filename,
                &runtime_state_root,
                self.runtime_context.as_ref(),
            )
        };
        let artifact_id = format!("codex:image:{thread_id}:{call_id}");
        if !self.emitted_artifact_ids.insert(artifact_id.clone()) {
            return Ok(None);
        }
        let filename = path
            .file_name()
            .and_then(|name| name.to_str())
            .map(str::to_string)
            .unwrap_or(filename);

        Ok(Some(RuntimeArtifact {
            artifact_id,
            path,
            filename: Some(filename),
            mime_type: Some("image/png".to_string()),
        }))
    }

    fn record_app_server_item(&mut self, method: &str, params: &Value) -> Vec<RuntimeEvent> {
        self.record_agent_message_item(method, params);
        self.record_answer_item_completion(method, params);
        match (method, app_server_item_type(params)) {
            ("item/started", Some("contextCompaction")) => {
                vec![RuntimeEvent::Status {
                    code: None,
                    text: "codex context compaction started".to_string(),
                }]
            }
            ("item/completed", Some("contextCompaction")) => {
                if let Some(thread_id) = extract_app_server_thread_id(params) {
                    self.completed_context_compaction_threads.insert(thread_id);
                } else {
                    self.unmatched_context_compaction_completed = true;
                }
                vec![RuntimeEvent::Status {
                    code: None,
                    text: "codex context compacted".to_string(),
                }]
            }
            (_, Some(_)) => app_server_item_events(params),
            (_, None) => Vec::new(),
        }
    }

    fn record_answer_delta_boundary(&mut self, params: &Value, events: &RuntimeEventSender) {
        let Some(item_id) = self.agent_message_item_id(params) else {
            return;
        };
        self.begin_answer_item(item_id, events);
    }

    fn record_answer_item_completion(&mut self, method: &str, params: &Value) {
        if method != "item/completed" || app_server_item_type(params) != Some("agentMessage") {
            return;
        }
        let Some(item_id) = extract_app_server_item_id(params) else {
            return;
        };
        if self
            .agent_message_lanes
            .get(&item_id)
            .copied()
            .unwrap_or(RuntimeMessageLane::Answer)
            == RuntimeMessageLane::Answer
        {
            self.active_answer_item_ids.remove(&item_id);
        }
        self.agent_message_lanes.remove(&item_id);
    }

    fn begin_answer_item(&mut self, item_id: String, events: &RuntimeEventSender) {
        if self.active_answer_item_ids.insert(item_id.clone())
            && self
                .last_answer_item_id
                .as_deref()
                .is_some_and(|last_item_id| last_item_id != item_id)
        {
            drop(events.send(RuntimeEvent::MessageBoundary {
                lane: RuntimeMessageLane::Answer,
            }));
        }
        self.last_answer_item_id = Some(item_id);
    }

    fn reset_turn_message_state(&mut self) {
        self.agent_message_lanes.clear();
        self.active_agent_message_item_id = None;
        self.active_answer_item_ids.clear();
        self.last_answer_item_id = None;
    }

    fn record_agent_message_item(&mut self, method: &str, params: &Value) {
        if app_server_item_type(params) != Some("agentMessage") {
            return;
        }
        let Some(item_id) = extract_app_server_item_id(params) else {
            return;
        };
        match method {
            "item/started" | "item/updated" => {
                self.active_agent_message_item_id = Some(item_id.clone());
                if let Some(lane) = agent_message_phase_lane(params) {
                    self.agent_message_lanes.insert(item_id, lane);
                }
            }
            "item/completed"
                if self.active_agent_message_item_id.as_deref() == Some(item_id.as_str()) =>
            {
                self.active_agent_message_item_id = None;
            }
            _ => {}
        }
    }

    fn agent_message_lane(&self, params: &Value) -> RuntimeMessageLane {
        self.agent_message_item_id(params)
            .and_then(|item_id| self.agent_message_lanes.get(&item_id).copied())
            .or_else(|| agent_message_phase_lane(params))
            .unwrap_or(RuntimeMessageLane::Answer)
    }

    fn agent_message_item_id(&self, params: &Value) -> Option<String> {
        extract_app_server_item_id(params).or_else(|| self.active_agent_message_item_id.clone())
    }

    async fn respond_to_server_request(&mut self, message: &Value) -> Result<()> {
        let id = message
            .get("id")
            .cloned()
            .context("codex app-server request missing id")?;
        let method = message.get("method").and_then(Value::as_str).unwrap_or("");
        let message = match method {
            "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
                format!(
                    "LionClaw does not grant runtime-internal approval callback '{method}'; runtime permissions are controlled by LionClaw policy"
                )
            }
            _ => format!("unsupported LionClaw app-server callback '{method}'"),
        };
        let response = app_server_error_response(id, -32601, message);
        self.transport.send(&response).await
    }
}

fn app_server_request_message(id: u64, method: &str, params: Value) -> Value {
    json!({
        "id": id,
        "method": method,
        "params": params,
    })
}

fn app_server_notification_message(method: &str, params: Value) -> Value {
    json!({
        "method": method,
        "params": params,
    })
}

fn app_server_error_response(id: Value, code: i64, message: String) -> Value {
    json!({
        "id": id,
        "error": {
            "code": code,
            "message": message,
        },
    })
}

fn response_id(message: &Value) -> Option<u64> {
    message.get("id").and_then(Value::as_u64)
}

fn parse_app_server_response(message: Value, method: &str) -> Result<Value> {
    if let Some(error) = message.get("error") {
        return Err(CodexAppServerResponseError {
            method: method.to_string(),
            code: error.get("code").and_then(Value::as_i64),
            message: app_server_error_text(error),
        }
        .into());
    }
    Ok(message.get("result").cloned().unwrap_or(Value::Null))
}

#[derive(Debug)]
struct CodexAppServerResponseError {
    method: String,
    code: Option<i64>,
    message: String,
}

impl std::fmt::Display for CodexAppServerResponseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.code {
            Some(code) => write!(
                formatter,
                "codex app-server {} failed with code {}: {}",
                self.method, code, self.message
            ),
            None => write!(
                formatter,
                "codex app-server {} failed: {}",
                self.method, self.message
            ),
        }
    }
}

impl std::error::Error for CodexAppServerResponseError {}

fn ensure_app_server_exit_success(output: ExecutionOutput) -> Result<()> {
    if output.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if stderr.is_empty() {
        bail!(
            "codex app-server exited with {}",
            output.status_description()
        );
    }
    bail!(
        "codex app-server exited with {}: {stderr}",
        output.status_description()
    );
}

async fn finish_app_server_session<T, R>(
    client: CodexAppServerClient<T>,
    result: Result<R>,
) -> Result<R>
where
    T: AppServerTransport + Send,
{
    let shutdown = client
        .shutdown()
        .await
        .and_then(ensure_app_server_exit_success);

    match (result, shutdown) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(err)) => Err(err),
        (Err(err), Ok(())) => Err(err),
        (Err(err), Err(shutdown_err)) => {
            warn!(
                error = %shutdown_err,
                "codex app-server shutdown failed after runtime error"
            );
            Err(err)
        }
    }
}

async fn finish_app_server_transcript_session<T>(
    client: CodexAppServerClient<T>,
    result: Result<RuntimeTerminalTranscript>,
    deadline: Instant,
    hard_timeout: Duration,
) -> Result<RuntimeTerminalTranscript>
where
    T: AppServerTransport + Send,
{
    let shutdown = timeout_at(deadline, client.shutdown()).await;
    match (result, shutdown) {
        (Ok(transcript), Ok(Ok(output))) => Ok(transcript_with_shutdown_result(
            transcript,
            ensure_app_server_exit_success(output),
        )),
        (Ok(mut transcript), Ok(Err(err))) => {
            transcript
                .warnings
                .push(codex_app_server_shutdown_warning(err));
            Ok(transcript)
        }
        (Ok(mut transcript), Err(_)) => {
            transcript
                .warnings
                .push(codex_app_server_shutdown_warning(anyhow!(
                    "timed out after {}s while shutting down Codex app-server",
                    hard_timeout.as_secs_f32()
                )));
            Ok(transcript)
        }
        (Err(err), Ok(Ok(output))) => {
            if let Err(shutdown_err) = ensure_app_server_exit_success(output) {
                warn!(
                    error = %shutdown_err,
                    "codex app-server shutdown failed after runtime error"
                );
            }
            Err(err)
        }
        (Err(err), Ok(Err(shutdown_err))) => {
            warn!(
                error = %shutdown_err,
                "codex app-server shutdown failed after runtime error"
            );
            Err(err)
        }
        (Err(err), Err(_)) => {
            warn!(
                timeout_secs = hard_timeout.as_secs_f32(),
                "codex app-server shutdown timed out after runtime error"
            );
            Err(err)
        }
    }
}

fn transcript_with_shutdown_result(
    mut transcript: RuntimeTerminalTranscript,
    shutdown: Result<()>,
) -> RuntimeTerminalTranscript {
    if let Err(err) = shutdown {
        transcript
            .warnings
            .push(codex_app_server_shutdown_warning(err));
    }
    transcript
}

fn codex_app_server_shutdown_warning(err: anyhow::Error) -> RuntimeTerminalTranscriptWarning {
    RuntimeTerminalTranscriptWarning::new(
        "codex-app-server",
        format!("Codex app-server shutdown after transcript export failed: {err:#}"),
    )
}

fn thread_start_params(model: Option<&str>) -> Value {
    let mut params = json!({
        "approvalPolicy": "never",
    });
    insert_optional_model(&mut params, model);
    params
}

fn thread_resume_params(thread_id: &str, model: Option<&str>) -> Value {
    let mut params = json!({
        "threadId": thread_id,
        "approvalPolicy": "never",
    });
    insert_optional_model(&mut params, model);
    params
}

fn turn_start_params(
    thread_id: &str,
    prompt: &str,
    model: Option<&str>,
    network_mode: NetworkMode,
) -> Value {
    let mut params = json!({
        "threadId": thread_id,
        "input": [
            {
                "type": "text",
                "text": prompt,
            }
        ],
        "approvalPolicy": "never",
        "sandboxPolicy": {
            "type": "externalSandbox",
            "networkAccess": match network_mode {
                NetworkMode::On => "enabled",
                NetworkMode::None => "restricted",
            },
        },
    });
    insert_optional_model(&mut params, model);
    params
}

fn insert_optional_model(params: &mut Value, model: Option<&str>) {
    let Some(model) = model else {
        return;
    };
    let Some(params) = params.as_object_mut() else {
        return;
    };
    params.insert("model".to_string(), json!(model));
}

fn invalid_thread_control_arguments(
    command_name: &str,
    arguments: &str,
) -> Option<RuntimeControlOutcome> {
    let arguments = arguments.trim();
    match command_name {
        "rename" | "name" if arguments.is_empty() => Some(invalid_arguments_outcome(
            "Codex rename requires a non-empty name.",
        )),
        "compact" if !arguments.is_empty() => Some(invalid_arguments_outcome(
            "Codex compact does not accept arguments.",
        )),
        _ => None,
    }
}

fn model_list_include_hidden(arguments: &str) -> std::result::Result<bool, RuntimeControlOutcome> {
    let mut include_hidden = false;
    for argument in arguments.split_whitespace() {
        match argument {
            "--hidden" | "--include-hidden" => include_hidden = true,
            argument if argument.starts_with('-') => {
                return Err(invalid_arguments_outcome(
                    "Codex model listing only accepts --hidden or --include-hidden.",
                ));
            }
            _ => {
                return Err(RuntimeControlOutcome::InteractiveOnly {
                    message:
                        "Codex model selection is interactive; configure the LionClaw runtime model instead."
                            .to_string(),
                });
            }
        }
    }
    Ok(include_hidden)
}

fn invalid_arguments_outcome(message: &str) -> RuntimeControlOutcome {
    RuntimeControlOutcome::Failed {
        code: Some("runtime.control.invalid_arguments".to_string()),
        message: message.to_string(),
    }
}

fn extract_app_server_thread_id(value: &Value) -> Option<String> {
    value
        .pointer("/thread/id")
        .and_then(Value::as_str)
        .or_else(|| value.pointer("/turn/threadId").and_then(Value::as_str))
        .or_else(|| value.pointer("/turn/thread/id").and_then(Value::as_str))
        .or_else(|| value.pointer("/item/threadId").and_then(Value::as_str))
        .or_else(|| value.get("threadId").and_then(Value::as_str))
        .or_else(|| value.get("thread_id").and_then(Value::as_str))
        .filter(|thread_id| !thread_id.trim().is_empty())
        .map(|thread_id| thread_id.trim().to_string())
}

fn extract_app_server_turn_id(value: &Value) -> Option<String> {
    value
        .pointer("/turn/id")
        .and_then(Value::as_str)
        .or_else(|| value.pointer("/item/turnId").and_then(Value::as_str))
        .or_else(|| value.get("turnId").and_then(Value::as_str))
        .or_else(|| value.get("turn_id").and_then(Value::as_str))
        .filter(|turn_id| !turn_id.trim().is_empty())
        .map(|turn_id| turn_id.trim().to_string())
}

fn extract_app_server_item_id(value: &Value) -> Option<String> {
    value
        .pointer("/item/id")
        .and_then(Value::as_str)
        .or_else(|| value.get("itemId").and_then(Value::as_str))
        .or_else(|| value.get("item_id").and_then(Value::as_str))
        .or_else(|| value.get("id").and_then(Value::as_str))
        .filter(|item_id| !item_id.trim().is_empty())
        .map(|item_id| item_id.trim().to_string())
}

fn app_server_item_type(params: &Value) -> Option<&str> {
    let item = params.get("item").unwrap_or(params);
    item.get("type").and_then(Value::as_str)
}

fn codex_generated_image_payload<'a>(
    method: Option<&'a str>,
    message: &'a Value,
) -> Option<&'a Value> {
    if let Some(("image_generation_end", payload)) = app_server_event_payload(method, message) {
        if codex_generated_image_is_publishable(method, payload) {
            return Some(payload);
        }
    }

    let item = message.get("item").unwrap_or(message);
    let item_type = item.get("type").and_then(Value::as_str);
    if matches!(method, Some("item/completed" | "item/updated"))
        && matches!(item_type, Some("imageGeneration" | "image_generation_call"))
        && codex_generated_image_is_publishable(method, item)
    {
        return Some(item);
    }

    if method.is_none() && message.get("type").and_then(Value::as_str) == Some("response_item") {
        let payload = message.get("payload")?;
        if payload.get("type").and_then(Value::as_str) == Some("image_generation_call")
            && codex_generated_image_is_publishable(method, payload)
        {
            return Some(payload);
        }
    }

    None
}

fn codex_generated_image_is_publishable(method: Option<&str>, payload: &Value) -> bool {
    if codex_generated_image_has_saved_path(payload) {
        return true;
    }

    match payload
        .get("status")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|status| !status.is_empty())
    {
        Some(status) => matches!(
            status.to_ascii_lowercase().as_str(),
            "completed" | "complete" | "succeeded" | "success" | "done"
        ),
        None => {
            method == Some("item/completed")
                || payload.get("type").and_then(Value::as_str) == Some("image_generation_end")
        }
    }
}

fn codex_generated_image_call_id(payload: &Value) -> Option<String> {
    payload
        .get("call_id")
        .or_else(|| payload.get("callId"))
        .or_else(|| payload.get("id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn codex_generated_image_has_saved_path(payload: &Value) -> bool {
    codex_generated_image_saved_path(payload).is_some()
}

fn codex_generated_image_saved_path(payload: &Value) -> Option<&str> {
    payload
        .get("saved_path")
        .or_else(|| payload.get("savedPath"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn codex_default_generated_image_path(
    thread_id: &str,
    filename: &str,
    runtime_state_root: &Path,
    runtime_context: Option<&RuntimeExecutionContext>,
) -> PathBuf {
    let runtime_path = PathBuf::from(CODEX_GENERATED_IMAGES_RUNTIME_DIR)
        .join(thread_id)
        .join(filename);
    if let Some(path) =
        runtime_context.and_then(|context| context.host_path_for_runtime_path(&runtime_path))
    {
        return path;
    }
    runtime_state_root
        .join("home")
        .join(".codex")
        .join("generated_images")
        .join(thread_id)
        .join(filename)
}

fn codex_generated_image_path(
    raw: &str,
    runtime_state_root: &Path,
    runtime_context: Option<&RuntimeExecutionContext>,
) -> Option<PathBuf> {
    let path = Path::new(raw);
    if path.is_absolute() {
        if let Some(host_path) =
            runtime_context.and_then(|context| context.host_path_for_runtime_path(path))
        {
            return Some(host_path);
        }
    }
    if let Ok(container_relative) = path.strip_prefix("/runtime") {
        return runtime_state_child_path(runtime_state_root, container_relative);
    }
    if path.is_absolute() {
        return None;
    }
    runtime_state_child_path(runtime_state_root, path)
}

fn runtime_state_child_path(runtime_state_root: &Path, relative_path: &Path) -> Option<PathBuf> {
    let safe_path = safe_relative_path(relative_path)?;
    if safe_path.as_os_str().is_empty() {
        return None;
    }
    Some(runtime_state_root.join(safe_path))
}

fn app_server_event_payload<'a>(
    method: Option<&'a str>,
    message: &'a Value,
) -> Option<(&'a str, &'a Value)> {
    let message_type = message.get("type").and_then(Value::as_str);
    if matches!(method, Some("event_msg" | "event"))
        || matches!(message_type, Some("event_msg" | "event"))
    {
        let payload = message.get("payload").unwrap_or(message);
        return payload
            .get("type")
            .and_then(Value::as_str)
            .map(|kind| (kind, payload));
    }
    if method == Some("image_generation_end") || message_type == Some("image_generation_end") {
        return Some(("image_generation_end", message));
    }
    None
}

fn agent_message_phase_lane(params: &Value) -> Option<RuntimeMessageLane> {
    let phase = app_server_item(params)
        .get("phase")
        .and_then(Value::as_str)
        .or_else(|| params.get("phase").and_then(Value::as_str))?;
    match phase.trim().to_ascii_lowercase().as_str() {
        "answer" | "final" | "final_answer" | "final-answer" | "message" => {
            Some(RuntimeMessageLane::Answer)
        }
        "analysis" | "commentary" | "intermediate" | "plan" | "prelude" | "progress"
        | "reasoning" | "thinking" => Some(RuntimeMessageLane::Reasoning),
        _ => None,
    }
}

fn app_server_text(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .filter_map(|key| value.get(*key))
        .find_map(|value| collect_codex_text(value, 0))
}

fn app_server_error_text(value: &Value) -> String {
    app_server_text(value, &["message", "text", "details", "error"])
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| "codex app-server error".to_string())
}

fn completed_turn_error_text(params: &Value) -> Option<String> {
    let turn = params.get("turn").unwrap_or(params);
    let status = turn
        .get("status")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|status| !status.is_empty());
    let error = turn
        .get("error")
        .or_else(|| params.get("error"))
        .filter(|error| !error.is_null());
    if status.is_none_or(|status| status.eq_ignore_ascii_case("completed")) && error.is_none() {
        return None;
    }

    let message = error
        .map(app_server_error_text)
        .or_else(|| {
            app_server_text(turn, &["message", "statusMessage", "details", "error"])
                .map(|text| text.trim().to_string())
                .filter(|text| !text.is_empty())
        })
        .or_else(|| status.map(turn_status_error_text))
        .unwrap_or_else(|| "codex turn failed".to_string());
    Some(message)
}

fn turn_status_error_text(status: &str) -> String {
    if status.eq_ignore_ascii_case("failed") {
        "codex turn failed".to_string()
    } else if status.eq_ignore_ascii_case("interrupted") {
        "codex turn interrupted".to_string()
    } else {
        format!("codex turn ended with status '{status}'")
    }
}

fn error_notification_will_retry(params: &Value) -> bool {
    params
        .get("willRetry")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

#[cfg(test)]
fn describe_app_server_item(params: &Value) -> Option<String> {
    describe_app_server_status_item(params)
}

fn app_server_item_events(params: &Value) -> Vec<RuntimeEvent> {
    match app_server_item_type(params) {
        Some("fileChange") => codex_file_change_item(params)
            .map(|change| RuntimeEvent::FileChange { change })
            .into_iter()
            .collect(),
        Some(_) => describe_app_server_status_item(params)
            .map(|text| RuntimeEvent::Status { code: None, text })
            .into_iter()
            .collect(),
        None => Vec::new(),
    }
}

fn describe_app_server_status_item(params: &Value) -> Option<String> {
    let item = app_server_item(params);
    match app_server_item_type(params)? {
        "agentMessage" | "reasoning" | "userMessage" | "plan" | "fileChange" => None,
        "commandExecution" => describe_command_execution_item(item),
        "webSearch" => describe_web_search_item(item),
        "imageView" => describe_image_view_item(item),
        "collabToolCall" => describe_collab_tool_call_item(item),
        "enteredReviewMode" => Some("codex review started".to_string()),
        "exitedReviewMode" => Some("codex review completed".to_string()),
        other => Some(format!("codex activity: {other}")),
    }
}

fn app_server_item(params: &Value) -> &Value {
    params.get("item").unwrap_or(params)
}

fn describe_command_execution_item(item: &Value) -> Option<String> {
    let command = command_execution_display(item)?;
    let status = item
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("completed");
    let verb = command_execution_verb(status, &command);
    let mut text = format!("codex {verb}: {command}");
    if let Some(cwd) = item.get("cwd").and_then(Value::as_str) {
        if !cwd.trim().is_empty() {
            text.push_str(&format!("  cwd {}", compact_path(cwd)));
        }
    }
    if let Some(exit_code) = item.get("exitCode").and_then(Value::as_i64) {
        text.push_str(&format!("  exit {exit_code}"));
    }
    if let Some(duration_ms) = item.get("durationMs").and_then(Value::as_u64) {
        text.push_str(&format!("  {}", format_millis(duration_ms)));
    }
    Some(text)
}

fn command_execution_verb(status: &str, command: &str) -> &'static str {
    match status {
        "inProgress" => "running",
        "failed" => "failed",
        "declined" => "declined",
        "completed" => {
            let first = command.split_whitespace().next().unwrap_or("");
            match first.trim_matches('"') {
                "rg" | "grep" => "searched",
                "cat" | "sed" | "head" | "tail" | "nl" => "read",
                "ls" | "find" | "git" => "inspected",
                _ => "ran",
            }
        }
        _ => "command",
    }
}

fn command_execution_display(item: &Value) -> Option<String> {
    item.get("command")
        .and_then(command_value_display)
        .or_else(|| command_actions_display(item.get("commandActions")?))
}

fn command_value_display(value: &Value) -> Option<String> {
    match value {
        Value::String(command) => non_empty(command),
        Value::Array(parts) => {
            let parts = parts
                .iter()
                .filter_map(Value::as_str)
                .map(shell_word_display)
                .collect::<Vec<_>>();
            (!parts.is_empty()).then(|| parts.join(" "))
        }
        _ => None,
    }
}

fn command_actions_display(value: &Value) -> Option<String> {
    let actions = value.as_array()?;
    let mut rendered = Vec::new();
    for action in actions {
        if let Some(command) = action
            .get("command")
            .or_else(|| action.get("cmd"))
            .and_then(command_value_display)
        {
            rendered.push(command);
        } else if let Some(action_type) = action.get("type").and_then(Value::as_str) {
            rendered.push(action_type.to_string());
        }
    }
    (!rendered.is_empty()).then(|| rendered.join(" && "))
}

fn codex_file_change_item(params: &Value) -> Option<RuntimeFileChange> {
    let item = app_server_item(params);
    let changes = item.get("changes").and_then(Value::as_array)?;
    let status = item
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("completed");
    let status = match status {
        "inProgress" => RuntimeFileChangeStatus::Editing,
        "completed" => RuntimeFileChangeStatus::Edited,
        "failed" => RuntimeFileChangeStatus::Failed,
        "declined" => RuntimeFileChangeStatus::Declined,
        _ => RuntimeFileChangeStatus::Changed,
    };
    let paths = changes
        .iter()
        .filter_map(|change| change.get("path").and_then(Value::as_str))
        .filter(|path| !path.trim().is_empty())
        .map(compact_path)
        .take(FILE_CHANGE_PATH_EVENT_LIMIT)
        .collect::<Vec<_>>();
    Some(RuntimeFileChange {
        runtime: "codex".to_string(),
        operation_id: extract_app_server_item_id(params),
        status,
        paths,
        total_count: changes.len(),
    })
}

fn describe_web_search_item(item: &Value) -> Option<String> {
    let action = item
        .pointer("/action/type")
        .and_then(Value::as_str)
        .unwrap_or("search");
    let (verb, target) = match action {
        "open_page" => ("opened", web_search_open_target(item)?),
        "find_in_page" => ("found", web_search_find_target(item)?),
        _ => web_search_query_target(item)
            .map(|target| ("searched", target))
            .or_else(|| web_search_open_target(item).map(|target| ("opened", target)))?,
    };
    Some(format!("codex {verb}: {target}"))
}

fn web_search_query_target(item: &Value) -> Option<String> {
    first_non_empty_json_string(
        item,
        &[
            "/query",
            "/queries",
            "/action/query",
            "/action/queries",
            "/action/searchQuery",
        ],
    )
}

fn web_search_open_target(item: &Value) -> Option<String> {
    first_non_empty_json_string(
        item,
        &[
            "/url",
            "/urls",
            "/refId",
            "/action/url",
            "/action/urls",
            "/action/refId",
        ],
    )
}

fn web_search_find_target(item: &Value) -> Option<String> {
    first_non_empty_json_string(
        item,
        &[
            "/pattern",
            "/query",
            "/action/pattern",
            "/action/query",
            "/action/refId",
        ],
    )
}

fn describe_image_view_item(item: &Value) -> Option<String> {
    item.get("path")
        .and_then(Value::as_str)
        .map(|path| format!("codex viewed: {}", compact_path(path)))
}

fn describe_collab_tool_call_item(item: &Value) -> Option<String> {
    let tool = item.get("tool").and_then(Value::as_str)?;
    let status = item
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("completed");
    Some(format!("codex {status}: {tool}"))
}

fn non_empty(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn first_non_empty_json_string(value: &Value, pointers: &[&str]) -> Option<String> {
    pointers
        .iter()
        .filter_map(|pointer| value.pointer(pointer))
        .find_map(non_empty_json_string)
}

fn non_empty_json_string(value: &Value) -> Option<String> {
    value.as_str().and_then(non_empty).or_else(|| {
        value
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .find_map(non_empty)
    })
}

fn shell_word_display(value: &str) -> String {
    if value.is_empty()
        || value
            .chars()
            .any(|ch| ch.is_whitespace() || matches!(ch, '\'' | '"' | '\\' | '$' | '`'))
    {
        format!("{value:?}")
    } else {
        value.to_string()
    }
}

fn compact_path(path: &str) -> String {
    let path = path.trim();
    path.strip_prefix("/workspace/")
        .or_else(|| path.strip_prefix("./"))
        .unwrap_or(path)
        .to_string()
}

fn format_millis(duration_ms: u64) -> String {
    if duration_ms < 1_000 {
        format!("{duration_ms}ms")
    } else {
        format!("{:.1}s", duration_ms as f64 / 1_000.0)
    }
}

fn describe_model_list_response(response: &Value) -> String {
    let models: Vec<String> = response
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(model_display_name)
        .take(20)
        .collect();

    if models.is_empty() {
        return "Codex returned no available models.".to_string();
    }

    format!("Available Codex models: {}.", models.join(", "))
}

fn model_display_name(value: &Value) -> Option<String> {
    value
        .get("displayName")
        .and_then(Value::as_str)
        .or_else(|| value.get("model").and_then(Value::as_str))
        .or_else(|| value.get("id").and_then(Value::as_str))
        .or_else(|| value.get("name").and_then(Value::as_str))
        .or_else(|| value.get("label").and_then(Value::as_str))
        .filter(|name| !name.trim().is_empty())
        .map(|name| name.trim().to_string())
}

fn load_saved_thread_id(root: &Path) -> Result<Option<String>> {
    load_state_value(root, CODEX_THREAD_ID_STATE_FILE, "codex thread")
}

fn load_ready_saved_thread_id(
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

fn save_thread_id(root: &Path, thread_id: &str) -> Result<()> {
    save_state_value(root, CODEX_THREAD_ID_STATE_FILE, thread_id, "codex thread")
}

impl CodexThreadState {
    fn set_active_turn(
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

    fn clear_active_turn(&self, thread_id: &str, turn_id: &str) -> Result<()> {
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

    fn current_thread_id(&self) -> Result<Option<String>> {
        Ok(self
            .sessions
            .read()
            .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?
            .get(&self.runtime_session_id)
            .ok_or_else(|| anyhow!("runtime session '{}' not found", self.runtime_session_id))?
            .thread_id
            .clone())
    }

    fn runtime_state_root(&self) -> Result<Option<PathBuf>> {
        Ok(self
            .sessions
            .read()
            .map_err(|_| anyhow!("codex runtime session state lock poisoned"))?
            .get(&self.runtime_session_id)
            .ok_or_else(|| anyhow!("runtime session '{}' not found", self.runtime_session_id))?
            .runtime_state_root
            .clone())
    }

    fn persist_thread_id(&self, thread_id: &str) -> Result<()> {
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

fn collect_codex_text(value: &Value, depth: usize) -> Option<String> {
    if depth > 6 {
        return None;
    }

    match value {
        Value::String(text) if text.is_empty() => None,
        Value::String(text) => Some(text.clone()),
        Value::Array(values) => {
            let parts: Vec<String> = values
                .iter()
                .filter_map(|value| collect_codex_text(value, depth + 1))
                .collect();
            if parts.is_empty() {
                None
            } else {
                Some(parts.join(""))
            }
        }
        Value::Object(map) => {
            for key in [
                "delta",
                "text",
                "content",
                "message",
                "last_agent_message",
                "reasoning_text",
                "raw_content",
                "summary_text",
            ] {
                if let Some(value) = map.get(key) {
                    if let Some(text) = collect_codex_text(value, depth + 1) {
                        return Some(text);
                    }
                }
            }
            None
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        future::pending,
        path::{Path, PathBuf},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    };

    use anyhow::Result;
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use lionclaw_runtime_api::{
        append_streamed_text_boundary, append_streamed_text_delta, ExecutionOutput, NetworkMode,
        RuntimeAdapter, RuntimeAuthKind, RuntimeControlExecution, RuntimeControlInput,
        RuntimeControlOrigin, RuntimeControlOutcome, RuntimeEvent, RuntimeEventSender,
        RuntimeExecutionContext, RuntimeFileChangeStatus, RuntimeMessageLane,
        RuntimePathProjection, RuntimeProgramExecutor, RuntimeProgramSession, RuntimeProgramSpec,
        RuntimeProgramStdoutSender, RuntimeSessionHandle, RuntimeSessionReady,
        RuntimeSessionStartInput, RuntimeTerminalProgramInput, RuntimeTerminalTurnStatus,
        RUNTIME_SESSION_READY_MARKER,
    };
    use serde_json::{json, Value};
    use tokio::time::Instant;
    use uuid::Uuid;

    use super::{
        build_codex_app_server_program, build_codex_terminal_program, load_saved_thread_id,
        save_thread_id, AppServerTransport, CodexAppServerClient, CodexRuntimeAdapter,
        CodexRuntimeConfig, CodexTerminalTranscriptExportRequest, CODEX_THREAD_ID_STATE_FILE,
    };

    fn runtime_not_ready() -> RuntimeSessionReady {
        RuntimeSessionReady::not_ready()
    }

    fn mark_runtime_ready(runtime_state_root: &Path) -> RuntimeSessionReady {
        std::fs::write(
            runtime_state_root.join(RUNTIME_SESSION_READY_MARKER),
            "ready\n",
        )
        .expect("write runtime ready marker");
        RuntimeSessionReady::from_runtime_state_root(runtime_state_root)
            .expect("runtime ready marker should be valid")
    }

    #[derive(Clone)]
    struct FakeAppServerTransport {
        incoming: Arc<Mutex<VecDeque<Value>>>,
        sent: Arc<Mutex<Vec<Value>>>,
        shutdowns: Arc<AtomicUsize>,
        output: ExecutionOutput,
        hang_when_empty: bool,
    }

    impl FakeAppServerTransport {
        fn new(incoming: Vec<Value>) -> Self {
            Self {
                incoming: Arc::new(Mutex::new(incoming.into())),
                sent: Arc::new(Mutex::new(Vec::new())),
                shutdowns: Arc::new(AtomicUsize::new(0)),
                output: ExecutionOutput {
                    exit_code: Some(0),
                    ..ExecutionOutput::default()
                },
                hang_when_empty: false,
            }
        }

        fn hanging_after(incoming: Vec<Value>) -> Self {
            Self {
                hang_when_empty: true,
                ..Self::new(incoming)
            }
        }
    }

    struct UnusedRuntimeProgramExecutor;

    #[async_trait]
    impl RuntimeProgramExecutor for UnusedRuntimeProgramExecutor {
        async fn execute_streaming(
            &mut self,
            _program: RuntimeProgramSpec,
            _stdout: RuntimeProgramStdoutSender,
        ) -> Result<ExecutionOutput> {
            anyhow::bail!("test did not expect streaming runtime execution")
        }

        async fn execute_captured(
            &mut self,
            _program: RuntimeProgramSpec,
        ) -> Result<ExecutionOutput> {
            anyhow::bail!("test did not expect captured runtime execution")
        }

        async fn spawn(
            &mut self,
            _program: RuntimeProgramSpec,
        ) -> Result<Box<dyn RuntimeProgramSession>> {
            anyhow::bail!("test did not expect interactive runtime execution")
        }
    }

    async fn start_codex_test_session(
        runtime_state_root: Option<PathBuf>,
    ) -> (
        CodexRuntimeAdapter,
        RuntimeSessionHandle,
        super::CodexThreadState,
    ) {
        start_codex_test_session_with_config(CodexRuntimeConfig::default(), runtime_state_root)
            .await
    }

    async fn start_codex_test_session_with_config(
        config: CodexRuntimeConfig,
        runtime_state_root: Option<PathBuf>,
    ) -> (
        CodexRuntimeAdapter,
        RuntimeSessionHandle,
        super::CodexThreadState,
    ) {
        let adapter = CodexRuntimeAdapter::new(config);
        let runtime_session_ready = runtime_not_ready();
        let handle = adapter
            .session_start(RuntimeSessionStartInput {
                session_id: Uuid::new_v4(),
                working_dir: None,
                environment: Vec::new(),
                runtime_skill_ids: Vec::new(),
                runtime_state_root,
                runtime_session_ready,
            })
            .await
            .expect("start");
        let thread_state = adapter.thread_state_for(&handle.runtime_session_id);
        (adapter, handle, thread_state)
    }

    fn omits_jsonrpc_header(message: &Value) -> bool {
        message.get("jsonrpc").is_none()
    }

    fn codex_protocol_fixture(name: &str) -> Value {
        let raw = match name {
            "compact_context_compaction_v2" => include_str!(
                "../tests/fixtures/codex_app_server/compact_context_compaction_v2.json"
            ),
            "turn_interrupt_v2" => {
                include_str!("../tests/fixtures/codex_app_server/turn_interrupt_v2.json")
            }
            other => panic!("unknown Codex protocol fixture: {other}"),
        };
        serde_json::from_str(raw).expect("Codex protocol fixture is valid JSON")
    }

    fn fixture_server_messages(fixture: &Value) -> Vec<Value> {
        fixture
            .get("server")
            .and_then(Value::as_array)
            .expect("fixture server messages")
            .clone()
    }

    fn fixture_client_request(fixture: &Value) -> (&str, Value) {
        let client = fixture.get("client").expect("fixture client request");
        let method = client
            .get("method")
            .and_then(Value::as_str)
            .expect("fixture client method");
        let params = client.get("params").cloned().unwrap_or(Value::Null);
        (method, params)
    }

    fn fixture_client_message(fixture: &Value) -> Value {
        fixture
            .get("client")
            .expect("fixture client request")
            .clone()
    }

    fn codex_completed_app_server_turn(
        turn_id: &str,
        user_text: &str,
        assistant_text: &str,
        started_at: i64,
        completed_at: i64,
    ) -> Value {
        json!({
            "id": turn_id,
            "itemsView": "full",
            "status": "completed",
            "startedAt": started_at,
            "completedAt": completed_at,
            "items": [
                {
                    "type": "userMessage",
                    "id": format!("{turn_id}_user"),
                    "content": [{"type": "text", "text": user_text}]
                },
                {
                    "type": "agentMessage",
                    "id": format!("{turn_id}_answer"),
                    "phase": "final_answer",
                    "text": assistant_text
                }
            ]
        })
    }

    fn codex_interrupted_app_server_turn(turn_id: &str, user_text: &str, started_at: i64) -> Value {
        json!({
            "id": turn_id,
            "itemsView": "full",
            "status": "inProgress",
            "startedAt": started_at,
            "completedAt": null,
            "items": [{
                "type": "userMessage",
                "id": format!("{turn_id}_user"),
                "content": [{"type": "text", "text": user_text}]
            }]
        })
    }

    fn codex_test_deadline() -> (Instant, Duration) {
        let hard_timeout = Duration::from_secs(30);
        (Instant::now() + hard_timeout, hard_timeout)
    }

    fn codex_test_timestamp(seconds: i64) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(seconds, 0).expect("valid test timestamp")
    }

    fn codex_transcript_export_request<'a>(
        events: &'a RuntimeEventSender,
        thread_state: &'a super::CodexThreadState,
        runtime_state_root: &'a Path,
        resume_thread_id: Option<&'a str>,
        deadline: Instant,
        hard_timeout: Duration,
    ) -> CodexTerminalTranscriptExportRequest<'a> {
        codex_transcript_export_request_after(
            events,
            thread_state,
            runtime_state_root,
            resume_thread_id,
            None,
            deadline,
            hard_timeout,
        )
    }

    fn codex_transcript_export_request_after<'a>(
        events: &'a RuntimeEventSender,
        thread_state: &'a super::CodexThreadState,
        runtime_state_root: &'a Path,
        resume_thread_id: Option<&'a str>,
        launch_started_at: Option<DateTime<Utc>>,
        deadline: Instant,
        hard_timeout: Duration,
    ) -> CodexTerminalTranscriptExportRequest<'a> {
        CodexTerminalTranscriptExportRequest {
            events,
            thread_state,
            runtime_state_root,
            resume_thread_id,
            launch_started_at,
            deadline,
            hard_timeout,
        }
    }

    #[async_trait]
    impl AppServerTransport for FakeAppServerTransport {
        async fn send(&mut self, message: &Value) -> Result<()> {
            self.sent.lock().expect("sent lock").push(message.clone());
            Ok(())
        }

        async fn recv(&mut self) -> Result<Option<Value>> {
            let next = self.incoming.lock().expect("incoming lock").pop_front();
            if next.is_some() || !self.hang_when_empty {
                return Ok(next);
            }
            pending::<()>().await;
            unreachable!("pending fake app-server transport returned")
        }

        async fn shutdown(&mut self) -> Result<ExecutionOutput> {
            self.shutdowns.fetch_add(1, Ordering::SeqCst);
            Ok(self.output.clone())
        }
    }

    fn answer_text_from_events(events: impl IntoIterator<Item = RuntimeEvent>) -> String {
        let mut text = String::new();
        for event in events {
            match event {
                RuntimeEvent::MessageDelta {
                    lane: RuntimeMessageLane::Answer,
                    text: delta,
                } => append_streamed_text_delta(&mut text, &delta),
                RuntimeEvent::MessageBoundary {
                    lane: RuntimeMessageLane::Answer,
                } => append_streamed_text_boundary(&mut text),
                _ => {}
            }
        }
        text
    }

    #[test]
    fn codex_terminal_program_uses_lionclaw_context_and_outer_boundary() {
        let program = build_codex_terminal_program(
            &CodexRuntimeConfig {
                executable: "codex".to_string(),
                model: Some("gpt-5.5".to_string()),
            },
            None,
        );

        assert_eq!(program.executable, "codex");
        assert_eq!(
            program.args,
            vec![
                "--sandbox".to_string(),
                "danger-full-access".to_string(),
                "--ask-for-approval".to_string(),
                "never".to_string(),
                "-c".to_string(),
                "check_for_update_on_startup=false".to_string(),
                "-c".to_string(),
                "projects.\"/workspace\".trust_level=\"trusted\"".to_string(),
                "-c".to_string(),
                "model_instructions_file=\"/runtime/AGENTS.generated.md\"".to_string(),
                "--model".to_string(),
                "gpt-5.5".to_string(),
            ]
        );
        assert_eq!(program.auth, Some(RuntimeAuthKind::Codex));
        assert!(program.environment.is_empty());
        assert!(program.stdin.is_empty());
    }

    #[test]
    fn codex_terminal_program_resumes_saved_thread_after_global_options() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        save_thread_id(&runtime_state_root, "thr_saved").expect("save thread");

        let adapter = CodexRuntimeAdapter::new(CodexRuntimeConfig {
            executable: "codex".to_string(),
            model: Some("gpt-5.5".to_string()),
        });
        let runtime_session_ready = mark_runtime_ready(&runtime_state_root);
        let program = adapter
            .build_terminal_program(RuntimeTerminalProgramInput {
                session_id: Uuid::new_v4(),
                runtime_state_root,
                runtime_session_ready,
            })
            .expect("terminal program");

        assert_eq!(
            program.args,
            vec![
                "--sandbox".to_string(),
                "danger-full-access".to_string(),
                "--ask-for-approval".to_string(),
                "never".to_string(),
                "-c".to_string(),
                "check_for_update_on_startup=false".to_string(),
                "-c".to_string(),
                "projects.\"/workspace\".trust_level=\"trusted\"".to_string(),
                "-c".to_string(),
                "model_instructions_file=\"/runtime/AGENTS.generated.md\"".to_string(),
                "--model".to_string(),
                "gpt-5.5".to_string(),
                "resume".to_string(),
                "thr_saved".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn codex_terminal_transcript_export_pages_app_server_thread_history() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let (adapter, _handle, thread_state) =
            start_codex_test_session(Some(temp_dir.path().to_path_buf())).await;
        let transport = FakeAppServerTransport::new(vec![
            json!({"id": 1, "result": {}}),
            json!({
                "id": 2,
                "result": {
                    "data": [{
                        "id": "thr_cli",
                        "createdAt": 1780000000,
                        "updatedAt": 1780000018
                    }],
                    "nextCursor": null,
                    "backwardsCursor": null
                }
            }),
            json!({
                "id": 3,
                "result": {
                    "data": [codex_completed_app_server_turn(
                        "turn_2",
                        "again",
                        "second answer",
                        1780000010,
                        1780000017,
                    )],
                    "nextCursor": "turns-page-2",
                    "backwardsCursor": "turns-page-1"
                }
            }),
            json!({
                "id": 4,
                "result": {
                    "data": [codex_completed_app_server_turn(
                        "turn_1",
                        "hello",
                        "answer",
                        1780000001,
                        1780000007,
                    )],
                    "nextCursor": null,
                    "backwardsCursor": "turns-page-2"
                }
            }),
        ]);
        let sent = transport.sent.clone();
        let mut client = CodexAppServerClient::new(transport);
        let (events, _events_rx) = tokio::sync::mpsc::unbounded_channel();

        client
            .initialize(&events, &thread_state)
            .await
            .expect("initialize");
        let (deadline, hard_timeout) = codex_test_deadline();
        let transcript = adapter
            .export_terminal_transcript_from_app_server_client(
                &mut client,
                codex_transcript_export_request(
                    &events,
                    &thread_state,
                    temp_dir.path(),
                    None,
                    deadline,
                    hard_timeout,
                ),
            )
            .await
            .expect("transcript");

        assert!(transcript.warnings.is_empty());
        assert!(transcript.state.is_reconciled());
        assert!(transcript.state.is_resumable());
        assert_eq!(
            load_saved_thread_id(temp_dir.path()).expect("saved thread"),
            Some("thr_cli".to_string())
        );
        let turns = transcript.turns;
        assert_eq!(turns.len(), 2);
        let turn = &turns[0];
        assert_eq!(turn.source_id, "codex-app-server:thr_cli:turn_1");
        assert_eq!(turn.display_user_text, "hello");
        assert_eq!(turn.prompt_user_text, "hello");
        assert_eq!(turn.assistant_text, "answer");
        assert_eq!(turn.status, RuntimeTerminalTurnStatus::Completed);
        assert_eq!(turns[1].source_id, "codex-app-server:thr_cli:turn_2");
        assert_eq!(turns[1].display_user_text, "again");
        assert_eq!(turns[1].assistant_text, "second answer");

        let sent = sent.lock().expect("sent lock").clone();
        assert_eq!(
            sent[2].get("method").and_then(Value::as_str),
            Some("thread/list")
        );
        assert_eq!(
            sent[2]
                .pointer("/params/sourceKinds/0")
                .and_then(Value::as_str),
            Some("cli")
        );
        assert_eq!(
            sent[2]
                .pointer("/params/sortDirection")
                .and_then(Value::as_str),
            Some("desc")
        );
        assert_eq!(
            sent[2].pointer("/params/sortKey").and_then(Value::as_str),
            Some("updated_at")
        );
        assert_eq!(
            sent[3].get("method").and_then(Value::as_str),
            Some("thread/turns/list")
        );
        assert_eq!(
            sent[3].pointer("/params/itemsView").and_then(Value::as_str),
            Some("full")
        );
        assert_eq!(
            sent[3]
                .pointer("/params/sortDirection")
                .and_then(Value::as_str),
            Some("desc")
        );
        assert_eq!(
            sent[4].pointer("/params/cursor").and_then(Value::as_str),
            Some("turns-page-2")
        );
    }

    #[test]
    fn codex_terminal_program_ignores_saved_thread_without_ready_marker() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        save_thread_id(&runtime_state_root, "thr_saved").expect("save thread");

        let adapter = CodexRuntimeAdapter::new(CodexRuntimeConfig::default());
        let program = adapter
            .build_terminal_program(RuntimeTerminalProgramInput {
                session_id: Uuid::new_v4(),
                runtime_state_root,
                runtime_session_ready: runtime_not_ready(),
            })
            .expect("terminal program");

        assert!(!program.args.iter().any(|arg| arg == "resume"));
        assert!(!program.args.iter().any(|arg| arg == "thr_saved"));
    }

    #[tokio::test]
    async fn codex_terminal_transcript_follows_newest_cli_thread() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let (adapter, _handle, thread_state) =
            start_codex_test_session(Some(temp_dir.path().to_path_buf())).await;
        save_thread_id(temp_dir.path(), "thr_saved").expect("save thread");
        let transport = FakeAppServerTransport::new(vec![
            json!({"id": 1, "result": {}}),
            json!({
                "id": 2,
                "result": {
                    "data": [
                        {
                            "id": "thr_other",
                            "createdAt": 1780000010,
                            "updatedAt": 1780000018
                        },
                        {
                            "id": "thr_saved",
                            "createdAt": 1780000001,
                            "updatedAt": 1780000007
                        }
                    ],
                    "nextCursor": null,
                    "backwardsCursor": null
                }
            }),
            json!({
                "id": 3,
                "result": {
                    "data": [codex_completed_app_server_turn(
                        "turn_other",
                        "newer",
                        "newer answer",
                        1780000010,
                        1780000017,
                    )],
                    "nextCursor": null,
                    "backwardsCursor": null
                }
            }),
            json!({
                "id": 4,
                "result": {
                    "data": [codex_completed_app_server_turn(
                        "turn_saved",
                        "hello",
                        "answer",
                        1780000001,
                        1780000007,
                    )],
                    "nextCursor": null,
                    "backwardsCursor": null
                }
            }),
        ]);
        let mut client = CodexAppServerClient::new(transport);
        let (events, _events_rx) = tokio::sync::mpsc::unbounded_channel();

        client
            .initialize(&events, &thread_state)
            .await
            .expect("initialize");
        let (deadline, hard_timeout) = codex_test_deadline();
        let transcript = adapter
            .export_terminal_transcript_from_app_server_client(
                &mut client,
                codex_transcript_export_request_after(
                    &events,
                    &thread_state,
                    temp_dir.path(),
                    Some("thr_saved"),
                    Some(codex_test_timestamp(1780000009)),
                    deadline,
                    hard_timeout,
                ),
            )
            .await
            .expect("transcript");

        assert!(transcript.state.is_reconciled());
        assert!(transcript.state.is_resumable());
        assert_eq!(
            load_saved_thread_id(temp_dir.path()).expect("saved thread"),
            Some("thr_other".to_string())
        );
        assert_eq!(transcript.turns.len(), 2);
        assert_eq!(
            transcript.turns[0].source_id,
            "codex-app-server:thr_saved:turn_saved"
        );
        assert_eq!(
            transcript.turns[1].source_id,
            "codex-app-server:thr_other:turn_other"
        );
    }

    #[tokio::test]
    async fn codex_terminal_transcript_keeps_saved_thread_when_latest_predates_launch() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let (adapter, _handle, thread_state) =
            start_codex_test_session(Some(temp_dir.path().to_path_buf())).await;
        save_thread_id(temp_dir.path(), "thr_saved").expect("save thread");
        let transport = FakeAppServerTransport::new(vec![
            json!({"id": 1, "result": {}}),
            json!({
                "id": 2,
                "result": {
                    "data": [{
                        "id": "thr_stale",
                        "createdAt": 1780000001,
                        "updatedAt": 1780000009
                    }],
                    "nextCursor": null,
                    "backwardsCursor": null
                }
            }),
            json!({
                "id": 3,
                "result": {
                    "data": [codex_completed_app_server_turn(
                        "turn_stale",
                        "stale",
                        "stale answer",
                        1780000002,
                        1780000008,
                    )],
                    "nextCursor": null,
                    "backwardsCursor": null
                }
            }),
            json!({
                "id": 4,
                "result": {
                    "data": [codex_completed_app_server_turn(
                        "turn_saved",
                        "hello",
                        "answer",
                        1780000001,
                        1780000007,
                    )],
                    "nextCursor": null,
                    "backwardsCursor": null
                }
            }),
        ]);
        let mut client = CodexAppServerClient::new(transport);
        let (events, _events_rx) = tokio::sync::mpsc::unbounded_channel();

        client
            .initialize(&events, &thread_state)
            .await
            .expect("initialize");
        let (deadline, hard_timeout) = codex_test_deadline();
        let transcript = adapter
            .export_terminal_transcript_from_app_server_client(
                &mut client,
                codex_transcript_export_request_after(
                    &events,
                    &thread_state,
                    temp_dir.path(),
                    Some("thr_saved"),
                    Some(codex_test_timestamp(1780000010)),
                    deadline,
                    hard_timeout,
                ),
            )
            .await
            .expect("transcript");

        assert!(transcript.state.is_reconciled());
        assert!(transcript.state.is_resumable());
        assert_eq!(
            load_saved_thread_id(temp_dir.path()).expect("saved thread"),
            Some("thr_saved".to_string())
        );
        assert_eq!(transcript.turns.len(), 2);
        assert_eq!(
            transcript.turns[0].source_id,
            "codex-app-server:thr_saved:turn_saved"
        );
        assert_eq!(
            transcript.turns[1].source_id,
            "codex-app-server:thr_stale:turn_stale"
        );
    }

    #[tokio::test]
    async fn codex_terminal_transcript_tracks_failed_current_thread_without_resumability() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let (adapter, _handle, thread_state) =
            start_codex_test_session(Some(temp_dir.path().to_path_buf())).await;
        save_thread_id(temp_dir.path(), "thr_good").expect("save thread");
        let transport = FakeAppServerTransport::new(vec![
            json!({"id": 1, "result": {}}),
            json!({
                "id": 2,
                "result": {
                    "data": [
                        {
                            "id": "thr_bad",
                            "createdAt": 1780000010,
                            "updatedAt": 1780000020
                        },
                        {
                            "id": "thr_good",
                            "createdAt": 1780000001,
                            "updatedAt": 1780000007
                        }
                    ],
                    "nextCursor": null,
                    "backwardsCursor": null
                }
            }),
            json!({
                "id": 3,
                "error": {
                    "code": -32000,
                    "message": "thread storage is corrupt"
                }
            }),
            json!({
                "id": 4,
                "result": {
                    "data": [codex_completed_app_server_turn(
                        "turn_good",
                        "hello",
                        "answer",
                        1780000001,
                        1780000007,
                    )],
                    "nextCursor": null,
                    "backwardsCursor": null
                }
            }),
        ]);
        let mut client = CodexAppServerClient::new(transport);
        let (events, _events_rx) = tokio::sync::mpsc::unbounded_channel();

        client
            .initialize(&events, &thread_state)
            .await
            .expect("initialize");
        let (deadline, hard_timeout) = codex_test_deadline();
        let transcript = adapter
            .export_terminal_transcript_from_app_server_client(
                &mut client,
                codex_transcript_export_request_after(
                    &events,
                    &thread_state,
                    temp_dir.path(),
                    Some("thr_good"),
                    Some(codex_test_timestamp(1780000009)),
                    deadline,
                    hard_timeout,
                ),
            )
            .await
            .expect("transcript");

        assert_eq!(transcript.turns.len(), 1);
        assert!(!transcript.state.is_reconciled());
        assert!(!transcript.state.is_resumable());
        assert_eq!(
            transcript.turns[0].source_id,
            "codex-app-server:thr_good:turn_good"
        );
        assert_eq!(
            load_saved_thread_id(temp_dir.path()).expect("saved thread"),
            Some("thr_bad".to_string())
        );
        assert_eq!(transcript.warnings.len(), 1);
        assert_eq!(transcript.warnings[0].source_id, "codex-app-server:thr_bad");
        assert!(
            transcript.warnings[0]
                .error
                .contains("thread storage is corrupt"),
            "unexpected warning: {}",
            transcript.warnings[0].error
        );
    }

    #[tokio::test]
    async fn codex_list_failure_fallback_imports_without_reconciling() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let (adapter, _handle, thread_state) =
            start_codex_test_session(Some(temp_dir.path().to_path_buf())).await;
        save_thread_id(temp_dir.path(), "thr_saved").expect("save thread");
        let transport = FakeAppServerTransport::new(vec![
            json!({"id": 1, "result": {}}),
            json!({
                "id": 2,
                "error": {
                    "code": -32000,
                    "message": "thread list unavailable"
                }
            }),
            json!({
                "id": 3,
                "result": {
                    "data": [codex_completed_app_server_turn(
                        "turn_saved",
                        "hello",
                        "answer",
                        1780000001,
                        1780000007,
                    )],
                    "nextCursor": null,
                    "backwardsCursor": null
                }
            }),
        ]);
        let mut client = CodexAppServerClient::new(transport);
        let (events, _events_rx) = tokio::sync::mpsc::unbounded_channel();

        client
            .initialize(&events, &thread_state)
            .await
            .expect("initialize");
        let (deadline, hard_timeout) = codex_test_deadline();
        let transcript = adapter
            .export_terminal_transcript_from_app_server_client(
                &mut client,
                codex_transcript_export_request(
                    &events,
                    &thread_state,
                    temp_dir.path(),
                    Some("thr_saved"),
                    deadline,
                    hard_timeout,
                ),
            )
            .await
            .expect("transcript");

        assert!(!transcript.state.is_reconciled());
        assert!(!transcript.state.is_resumable());
        assert_eq!(transcript.turns.len(), 1);
        assert_eq!(
            transcript.turns[0].source_id,
            "codex-app-server:thr_saved:turn_saved"
        );
        assert_eq!(
            load_saved_thread_id(temp_dir.path()).expect("saved thread"),
            Some("thr_saved".to_string())
        );
        assert_eq!(transcript.warnings.len(), 1);
        assert_eq!(transcript.warnings[0].source_id, "codex-app-server");
        assert!(
            transcript.warnings[0]
                .error
                .contains("thread list unavailable"),
            "unexpected warning: {}",
            transcript.warnings[0].error
        );
    }

    #[tokio::test]
    async fn codex_malformed_list_fallback_imports_without_reconciling() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let (adapter, _handle, thread_state) =
            start_codex_test_session(Some(temp_dir.path().to_path_buf())).await;
        save_thread_id(temp_dir.path(), "thr_saved").expect("save thread");
        let transport = FakeAppServerTransport::new(vec![
            json!({"id": 1, "result": {}}),
            json!({"id": 2, "result": {"unexpected": true}}),
            json!({
                "id": 3,
                "result": {
                    "data": [codex_completed_app_server_turn(
                        "turn_saved",
                        "hello",
                        "answer",
                        1780000001,
                        1780000007,
                    )],
                    "nextCursor": null,
                    "backwardsCursor": null
                }
            }),
        ]);
        let mut client = CodexAppServerClient::new(transport);
        let (events, _events_rx) = tokio::sync::mpsc::unbounded_channel();

        client
            .initialize(&events, &thread_state)
            .await
            .expect("initialize");
        let (deadline, hard_timeout) = codex_test_deadline();
        let transcript = adapter
            .export_terminal_transcript_from_app_server_client(
                &mut client,
                codex_transcript_export_request(
                    &events,
                    &thread_state,
                    temp_dir.path(),
                    Some("thr_saved"),
                    deadline,
                    hard_timeout,
                ),
            )
            .await
            .expect("transcript");

        assert!(!transcript.state.is_reconciled());
        assert!(!transcript.state.is_resumable());
        assert_eq!(transcript.turns.len(), 1);
        assert_eq!(
            transcript.turns[0].source_id,
            "codex-app-server:thr_saved:turn_saved"
        );
        assert_eq!(transcript.warnings.len(), 1);
        assert_eq!(transcript.warnings[0].source_id, "codex-app-server");
        assert!(
            transcript.warnings[0].error.contains("missing data"),
            "unexpected warning: {}",
            transcript.warnings[0].error
        );
    }

    #[tokio::test]
    async fn codex_terminal_transcript_treats_unmaterialized_thread_as_empty() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let (adapter, _handle, thread_state) =
            start_codex_test_session(Some(temp_dir.path().to_path_buf())).await;
        let transport = FakeAppServerTransport::new(vec![
            json!({"id": 1, "result": {}}),
            json!({
                "id": 2,
                "result": {
                    "data": [{
                        "id": "thr_empty",
                        "createdAt": 1780000000,
                        "updatedAt": 1780000001
                    }],
                    "nextCursor": null,
                    "backwardsCursor": null
                }
            }),
            json!({
                "id": 3,
                "error": {
                    "code": -32602,
                    "message": "thread thr_empty is not materialized yet; thread/turns/list is unavailable before first user message"
                }
            }),
        ]);
        let mut client = CodexAppServerClient::new(transport);
        let (events, _events_rx) = tokio::sync::mpsc::unbounded_channel();

        client
            .initialize(&events, &thread_state)
            .await
            .expect("initialize");
        let (deadline, hard_timeout) = codex_test_deadline();
        let transcript = adapter
            .export_terminal_transcript_from_app_server_client(
                &mut client,
                codex_transcript_export_request(
                    &events,
                    &thread_state,
                    temp_dir.path(),
                    None,
                    deadline,
                    hard_timeout,
                ),
            )
            .await
            .expect("transcript");

        assert!(transcript.turns.is_empty());
        assert!(transcript.warnings.is_empty());
        assert!(transcript.state.is_reconciled());
        assert!(!transcript.state.is_resumable());
        assert_eq!(
            load_saved_thread_id(temp_dir.path()).expect("saved thread"),
            Some("thr_empty".to_string())
        );
    }

    #[tokio::test]
    async fn codex_terminal_transcript_deduplicates_overlapping_thread_pages() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let (adapter, _handle, thread_state) =
            start_codex_test_session(Some(temp_dir.path().to_path_buf())).await;
        let transport = FakeAppServerTransport::new(vec![
            json!({"id": 1, "result": {}}),
            json!({
                "id": 2,
                "result": {
                    "data": [{"id": "thr_cli"}],
                    "nextCursor": "threads-page-2",
                    "backwardsCursor": null
                }
            }),
            json!({
                "id": 3,
                "result": {
                    "data": [codex_completed_app_server_turn(
                        "turn_done",
                        "hello",
                        "answer",
                        1780000001,
                        1780000007,
                    )],
                    "nextCursor": null,
                    "backwardsCursor": null
                }
            }),
            json!({
                "id": 4,
                "result": {
                    "data": [{"id": "thr_cli"}],
                    "nextCursor": null,
                    "backwardsCursor": null
                }
            }),
        ]);
        let sent = transport.sent.clone();
        let mut client = CodexAppServerClient::new(transport);
        let (events, _events_rx) = tokio::sync::mpsc::unbounded_channel();

        client
            .initialize(&events, &thread_state)
            .await
            .expect("initialize");
        let (deadline, hard_timeout) = codex_test_deadline();
        let transcript = adapter
            .export_terminal_transcript_from_app_server_client(
                &mut client,
                codex_transcript_export_request(
                    &events,
                    &thread_state,
                    temp_dir.path(),
                    None,
                    deadline,
                    hard_timeout,
                ),
            )
            .await
            .expect("transcript");

        assert!(transcript.state.is_reconciled());
        assert!(transcript.state.is_resumable());
        assert_eq!(transcript.turns.len(), 1);
        let sent = sent.lock().expect("sent lock");
        let turn_list_requests = sent
            .iter()
            .filter(|message| {
                message.get("method").and_then(Value::as_str) == Some("thread/turns/list")
            })
            .count();
        assert_eq!(turn_list_requests, 1);
    }

    #[tokio::test]
    async fn codex_terminal_transcript_resumability_uses_latest_raw_turn_status() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let (adapter, _handle, thread_state) =
            start_codex_test_session(Some(temp_dir.path().to_path_buf())).await;
        let transport = FakeAppServerTransport::new(vec![
            json!({"id": 1, "result": {}}),
            json!({
                "id": 2,
                "result": {
                    "data": [{"id": "thr_cli"}],
                    "nextCursor": null,
                    "backwardsCursor": null
                }
            }),
            json!({
                "id": 3,
                "result": {
                    "data": [
                        codex_interrupted_app_server_turn(
                            "turn_pending",
                            "unfinished prompt",
                            1780000010,
                        ),
                        codex_completed_app_server_turn(
                            "turn_done",
                            "hello",
                            "answer",
                            1780000001,
                            1780000007,
                        )
                    ],
                    "nextCursor": null,
                    "backwardsCursor": null
                }
            }),
        ]);
        let mut client = CodexAppServerClient::new(transport);
        let (events, _events_rx) = tokio::sync::mpsc::unbounded_channel();

        client
            .initialize(&events, &thread_state)
            .await
            .expect("initialize");
        let (deadline, hard_timeout) = codex_test_deadline();
        let transcript = adapter
            .export_terminal_transcript_from_app_server_client(
                &mut client,
                codex_transcript_export_request(
                    &events,
                    &thread_state,
                    temp_dir.path(),
                    Some("thr_cli"),
                    deadline,
                    hard_timeout,
                ),
            )
            .await
            .expect("transcript");

        assert_eq!(transcript.turns.len(), 1);
        assert_eq!(
            transcript.turns[0].source_id,
            "codex-app-server:thr_cli:turn_done"
        );
        assert!(transcript.state.is_reconciled());
        assert!(!transcript.state.is_resumable());
        assert!(transcript.warnings.is_empty());
    }

    #[tokio::test]
    async fn codex_terminal_transcript_resumability_requires_importable_latest_turn() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let (adapter, _handle, thread_state) =
            start_codex_test_session(Some(temp_dir.path().to_path_buf())).await;
        let mut latest = codex_completed_app_server_turn(
            "turn_missing_time",
            "new prompt",
            "new answer",
            1780000010,
            1780000017,
        );
        latest["startedAt"] = Value::Null;
        latest["completedAt"] = Value::Null;
        let transport = FakeAppServerTransport::new(vec![
            json!({"id": 1, "result": {}}),
            json!({
                "id": 2,
                "result": {
                    "data": [{"id": "thr_cli"}],
                    "nextCursor": null,
                    "backwardsCursor": null
                }
            }),
            json!({
                "id": 3,
                "result": {
                    "data": [
                        latest,
                        codex_completed_app_server_turn(
                            "turn_done",
                            "hello",
                            "answer",
                            1780000001,
                            1780000007,
                        )
                    ],
                    "nextCursor": null,
                    "backwardsCursor": null
                }
            }),
        ]);
        let mut client = CodexAppServerClient::new(transport);
        let (events, _events_rx) = tokio::sync::mpsc::unbounded_channel();

        client
            .initialize(&events, &thread_state)
            .await
            .expect("initialize");
        let (deadline, hard_timeout) = codex_test_deadline();
        let transcript = adapter
            .export_terminal_transcript_from_app_server_client(
                &mut client,
                codex_transcript_export_request(
                    &events,
                    &thread_state,
                    temp_dir.path(),
                    Some("thr_cli"),
                    deadline,
                    hard_timeout,
                ),
            )
            .await
            .expect("transcript");

        assert_eq!(transcript.turns.len(), 1);
        assert_eq!(
            transcript.turns[0].source_id,
            "codex-app-server:thr_cli:turn_done"
        );
        assert!(transcript.state.is_reconciled());
        assert!(!transcript.state.is_resumable());
        assert!(transcript.warnings.is_empty());
    }

    #[tokio::test]
    async fn codex_terminal_transcript_resumability_requires_explicit_completed_status() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let (adapter, _handle, thread_state) =
            start_codex_test_session(Some(temp_dir.path().to_path_buf())).await;
        let mut latest = codex_completed_app_server_turn(
            "turn_missing_status",
            "new prompt",
            "new answer",
            1780000010,
            1780000017,
        );
        latest
            .as_object_mut()
            .expect("turn object")
            .remove("status");
        let transport = FakeAppServerTransport::new(vec![
            json!({"id": 1, "result": {}}),
            json!({
                "id": 2,
                "result": {
                    "data": [{"id": "thr_cli"}],
                    "nextCursor": null,
                    "backwardsCursor": null
                }
            }),
            json!({
                "id": 3,
                "result": {
                    "data": [
                        latest,
                        codex_completed_app_server_turn(
                            "turn_done",
                            "hello",
                            "answer",
                            1780000001,
                            1780000007,
                        )
                    ],
                    "nextCursor": null,
                    "backwardsCursor": null
                }
            }),
        ]);
        let mut client = CodexAppServerClient::new(transport);
        let (events, _events_rx) = tokio::sync::mpsc::unbounded_channel();

        client
            .initialize(&events, &thread_state)
            .await
            .expect("initialize");
        let (deadline, hard_timeout) = codex_test_deadline();
        let transcript = adapter
            .export_terminal_transcript_from_app_server_client(
                &mut client,
                codex_transcript_export_request(
                    &events,
                    &thread_state,
                    temp_dir.path(),
                    Some("thr_cli"),
                    deadline,
                    hard_timeout,
                ),
            )
            .await
            .expect("transcript");

        assert_eq!(transcript.turns.len(), 2);
        assert_eq!(
            transcript.turns[1].source_id,
            "codex-app-server:thr_cli:turn_missing_status"
        );
        assert_eq!(
            transcript.turns[1].status,
            RuntimeTerminalTurnStatus::Completed
        );
        assert!(transcript.state.is_reconciled());
        assert!(!transcript.state.is_resumable());
        assert!(transcript.warnings.is_empty());
    }

    #[tokio::test]
    async fn codex_terminal_transcript_resumability_rejects_completed_turn_with_error() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let (adapter, _handle, thread_state) =
            start_codex_test_session(Some(temp_dir.path().to_path_buf())).await;
        let mut latest = codex_completed_app_server_turn(
            "turn_error",
            "new prompt",
            "new answer",
            1780000010,
            1780000017,
        );
        latest["error"] = json!({"message": "boom"});
        let transport = FakeAppServerTransport::new(vec![
            json!({"id": 1, "result": {}}),
            json!({
                "id": 2,
                "result": {
                    "data": [{"id": "thr_cli"}],
                    "nextCursor": null,
                    "backwardsCursor": null
                }
            }),
            json!({
                "id": 3,
                "result": {
                    "data": [
                        latest,
                        codex_completed_app_server_turn(
                            "turn_done",
                            "hello",
                            "answer",
                            1780000001,
                            1780000007,
                        )
                    ],
                    "nextCursor": null,
                    "backwardsCursor": null
                }
            }),
        ]);
        let mut client = CodexAppServerClient::new(transport);
        let (events, _events_rx) = tokio::sync::mpsc::unbounded_channel();

        client
            .initialize(&events, &thread_state)
            .await
            .expect("initialize");
        let (deadline, hard_timeout) = codex_test_deadline();
        let transcript = adapter
            .export_terminal_transcript_from_app_server_client(
                &mut client,
                codex_transcript_export_request(
                    &events,
                    &thread_state,
                    temp_dir.path(),
                    Some("thr_cli"),
                    deadline,
                    hard_timeout,
                ),
            )
            .await
            .expect("transcript");

        assert_eq!(transcript.turns.len(), 2);
        let latest = transcript
            .turns
            .iter()
            .find(|turn| turn.source_id == "codex-app-server:thr_cli:turn_error")
            .expect("latest turn imported");
        assert_eq!(latest.status, RuntimeTerminalTurnStatus::Failed);
        assert_eq!(
            latest.error_code,
            Some("runtime.codex.turn_failed".to_string())
        );
        assert_eq!(latest.error_text, Some("boom".to_string()));
        assert!(transcript.state.is_reconciled());
        assert!(!transcript.state.is_resumable());
        assert!(transcript.warnings.is_empty());
    }

    #[tokio::test]
    async fn codex_terminal_transcript_timeout_returns_partial_transcript() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let (adapter, _handle, thread_state) =
            start_codex_test_session(Some(temp_dir.path().to_path_buf())).await;
        let transport = FakeAppServerTransport::hanging_after(vec![
            json!({"id": 1, "result": {}}),
            json!({
                "id": 2,
                "result": {
                    "data": [{"id": "thr_cli"}],
                    "nextCursor": null,
                    "backwardsCursor": null
                }
            }),
            json!({
                "id": 3,
                "result": {
                    "data": [codex_completed_app_server_turn(
                        "turn_done",
                        "hello",
                        "answer",
                        1780000001,
                        1780000007,
                    )],
                    "nextCursor": "more-turns",
                    "backwardsCursor": null
                }
            }),
        ]);
        let mut client = CodexAppServerClient::new(transport);
        let (events, _events_rx) = tokio::sync::mpsc::unbounded_channel();

        client
            .initialize(&events, &thread_state)
            .await
            .expect("initialize");
        let hard_timeout = Duration::from_millis(100);
        let transcript = adapter
            .export_terminal_transcript_from_app_server_client(
                &mut client,
                codex_transcript_export_request(
                    &events,
                    &thread_state,
                    temp_dir.path(),
                    Some("thr_cli"),
                    Instant::now() + hard_timeout,
                    hard_timeout,
                ),
            )
            .await
            .expect("partial transcript");

        assert_eq!(transcript.turns.len(), 1);
        assert_eq!(
            transcript.turns[0].source_id,
            "codex-app-server:thr_cli:turn_done"
        );
        assert!(!transcript.state.is_reconciled());
        assert!(!transcript.state.is_resumable());
        assert_eq!(transcript.warnings.len(), 1);
        assert_eq!(transcript.warnings[0].source_id, "codex-app-server:thr_cli");
        assert!(
            transcript.warnings[0].error.contains("timed out"),
            "unexpected warning: {}",
            transcript.warnings[0].error
        );
    }

    #[test]
    fn codex_app_server_turns_use_thread_time_when_turn_time_is_missing() {
        let mut turn = codex_completed_app_server_turn("turn_done", "hello", "answer", 1, 2);
        turn["startedAt"] = Value::Null;
        turn["completedAt"] = Value::Null;

        let turn = super::codex_app_server_terminal_turn(
            "thr_cli",
            Some(codex_test_timestamp(1780000010)),
            Some(codex_test_timestamp(1780000020)),
            &turn,
        )
        .expect("turn");

        assert_eq!(turn.started_at, codex_test_timestamp(1780000010));
        assert_eq!(turn.finished_at, codex_test_timestamp(1780000020));
    }

    #[test]
    fn codex_app_server_turns_skip_turns_without_deterministic_time() {
        let mut turn = codex_completed_app_server_turn("turn_done", "hello", "answer", 1, 2);
        turn["startedAt"] = Value::Null;
        turn["completedAt"] = Value::Null;

        assert!(super::codex_app_server_terminal_turn("thr_cli", None, None, &turn).is_none());
    }

    #[test]
    fn codex_app_server_turn_status_preserves_terminal_state() {
        let (status, error_code, error_text) = super::codex_app_server_turn_status(&json!({
            "status": "completed"
        }));
        assert_eq!(status, RuntimeTerminalTurnStatus::Completed);
        assert_eq!(error_code, None);
        assert_eq!(error_text, None);

        let (status, error_code, error_text) = super::codex_app_server_turn_status(&json!({
            "status": "completed",
            "error": {"message": "quota exceeded"}
        }));
        assert_eq!(status, RuntimeTerminalTurnStatus::Failed);
        assert_eq!(error_code, Some("runtime.codex.turn_failed".to_string()));
        assert_eq!(error_text, Some("quota exceeded".to_string()));

        let (status, error_code, error_text) = super::codex_app_server_turn_status(&json!({
            "status": "failed",
            "error": {"message": "quota exceeded"}
        }));
        assert_eq!(status, RuntimeTerminalTurnStatus::Failed);
        assert_eq!(error_code, Some("runtime.codex.turn_failed".to_string()));
        assert_eq!(error_text, Some("quota exceeded".to_string()));

        let (status, error_code, error_text) = super::codex_app_server_turn_status(&json!({
            "status": "interrupted"
        }));
        assert_eq!(status, RuntimeTerminalTurnStatus::Interrupted);
        assert_eq!(error_code, None);
        assert_eq!(error_text, None);

        let (status, error_code, error_text) = super::codex_app_server_turn_status(&json!({
            "status": "interrupted",
            "error": {"message": "cancelled"}
        }));
        assert_eq!(status, RuntimeTerminalTurnStatus::Interrupted);
        assert_eq!(error_code, Some("runtime.codex.turn_failed".to_string()));
        assert_eq!(error_text, Some("cancelled".to_string()));

        let (status, error_code, error_text) = super::codex_app_server_turn_status(&json!({
            "status": "inProgress"
        }));
        assert_eq!(status, RuntimeTerminalTurnStatus::Interrupted);
        assert_eq!(error_code, None);
        assert_eq!(error_text, None);
    }

    #[test]
    fn codex_app_server_program_uses_default_stdio_transport() {
        let program = build_codex_app_server_program(&CodexRuntimeConfig {
            executable: "codex".to_string(),
            model: Some("gpt-5-codex".to_string()),
        });

        assert_eq!(program.executable, "codex");
        assert_eq!(
            program.args,
            vec![
                "-c".to_string(),
                "check_for_update_on_startup=false".to_string(),
                "-c".to_string(),
                "projects.\"/workspace\".trust_level=\"trusted\"".to_string(),
                "app-server".to_string(),
            ]
        );
        assert_eq!(program.stdin, "");
        assert_eq!(program.auth, Some(RuntimeAuthKind::Codex));
    }

    #[test]
    fn app_server_text_preserves_streamed_delta_whitespace() {
        let params = json!({
            "delta": {
                "content": [
                    {"text": "Hello"},
                    {"text": " world"},
                    {"text": "\n"},
                ],
            },
        });

        assert_eq!(
            super::app_server_text(&params, &["delta"]),
            Some("Hello world\n".to_string())
        );
        assert_eq!(
            super::app_server_error_text(&json!({"message": "  failed \n"})),
            "failed"
        );
    }

    #[test]
    fn app_server_item_descriptions_render_operator_activity() {
        assert_eq!(
            super::describe_app_server_item(&json!({
                "item": {
                    "type": "commandExecution",
                    "command": ["cargo", "test"],
                    "cwd": "/workspace/lionclaw",
                    "status": "completed",
                    "exitCode": 0,
                    "durationMs": 1234,
                }
            })),
            Some("codex ran: cargo test  cwd lionclaw  exit 0  1.2s".to_string())
        );
        assert_eq!(
            super::describe_app_server_item(&json!({
                "item": {
                    "type": "commandExecution",
                    "command": ["rg", "operator console"],
                    "status": "completed",
                }
            })),
            Some("codex searched: rg \"operator console\"".to_string())
        );
        let file_change_events = super::app_server_item_events(&json!({
            "item": {
                "id": "edit-1",
                "type": "fileChange",
                "status": "completed",
                "changes": [
                    {"path": "/workspace/src/operator/run_tui.rs", "kind": "update"},
                    {"path": "Cargo.toml", "kind": "update"}
                ],
            }
        }));
        assert!(matches!(
            file_change_events.as_slice(),
            [RuntimeEvent::FileChange { change }]
                if change.runtime == "codex"
                    && change.operation_id.as_deref() == Some("edit-1")
                    && change.status == RuntimeFileChangeStatus::Edited
                    && change.paths == ["src/operator/run_tui.rs", "Cargo.toml"]
                    && change.total_count == 2
        ));
        assert_eq!(
            super::describe_app_server_item(&json!({
                "item": {
                    "type": "webSearch",
                    "action": {"type": "search", "query": "ratatui scrollbar"}
                }
            })),
            Some("codex searched: ratatui scrollbar".to_string())
        );
        assert_eq!(
            super::describe_app_server_item(&json!({
                "item": {
                    "type": "webSearch",
                    "query": "",
                    "action": {
                        "type": "search",
                        "queries": ["official Ratatui scrollbar docs Scrollbar ratatui widgets"]
                    }
                }
            })),
            Some(
                "codex searched: official Ratatui scrollbar docs Scrollbar ratatui widgets"
                    .to_string()
            )
        );
        assert_eq!(
            super::describe_app_server_item(&json!({
                "item": {
                    "type": "webSearch",
                    "query": "",
                    "action": {"type": "search", "queries": ["", "  "]}
                }
            })),
            None
        );
        assert_eq!(
            super::describe_app_server_item(&json!({
                "item": {
                    "type": "webSearch",
                    "action": {
                        "type": "open_page",
                        "url": "https://docs.rs/ratatui/latest/ratatui/widgets/struct.Scrollbar.html"
                    }
                }
            })),
            Some(
                "codex opened: https://docs.rs/ratatui/latest/ratatui/widgets/struct.Scrollbar.html"
                    .to_string()
            )
        );
        assert_eq!(
            super::describe_app_server_item(&json!({
                "item": {"type": "agentMessage", "text": "hello"}
            })),
            None
        );
    }

    #[tokio::test]
    async fn app_server_agent_message_phases_choose_transcript_lane() {
        let (_adapter, _handle, thread_state) = start_codex_test_session(None).await;
        let mut client = CodexAppServerClient::new(FakeAppServerTransport::new(Vec::new()));
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

        for message in [
            json!({"method": "turn/started", "params": {"threadId": "thr_1", "turnId": "turn_1"}}),
            json!({
                "method": "item/started",
                "params": {
                    "item": {
                        "id": "msg_plan",
                        "type": "agentMessage",
                        "phase": "prelude"
                    }
                }
            }),
            json!({"method": "item/agentMessage/delta", "params": {"itemId": "msg_plan", "delta": "I'll inspect first."}}),
            json!({
                "method": "item/started",
                "params": {
                    "item": {
                        "id": "msg_final",
                        "type": "agentMessage",
                        "phase": "final_answer"
                    }
                }
            }),
            json!({"method": "item/agentMessage/delta", "params": {"itemId": "msg_final", "delta": "Final answer."}}),
        ] {
            client
                .handle_message(message, &event_tx, &thread_state)
                .await
                .expect("handle message");
        }

        let events: Vec<RuntimeEvent> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Reasoning,
                text
            } if text == "I'll inspect first."
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::MessageDelta {
                lane: RuntimeMessageLane::Answer,
                text
            } if text == "Final answer."
        )));
        assert!(!events
            .iter()
            .any(|event| matches!(event, RuntimeEvent::MessageBoundary { .. })));
    }

    #[tokio::test]
    async fn app_server_agent_message_items_emit_answer_boundaries() {
        let (_adapter, _handle, thread_state) = start_codex_test_session(None).await;
        let mut client = CodexAppServerClient::new(FakeAppServerTransport::new(Vec::new()));
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

        for message in [
            json!({"method": "turn/started", "params": {"threadId": "thr_1", "turnId": "turn_1"}}),
            json!({
                "method": "item/started",
                "params": {
                    "item": {
                        "id": "msg_intro",
                        "type": "agentMessage",
                        "phase": "final_answer"
                    }
                }
            }),
            json!({"method": "item/agentMessage/delta", "params": {"itemId": "msg_intro", "delta": "Intro."}}),
            json!({"method": "item/completed", "params": {"item": {"id": "msg_intro", "type": "agentMessage"}}}),
            json!({
                "method": "item/started",
                "params": {
                    "item": {
                        "id": "msg_body",
                        "type": "agentMessage",
                        "phase": "final_answer"
                    }
                }
            }),
            json!({"method": "item/agentMessage/delta", "params": {"itemId": "msg_body", "delta": "**Project**"}}),
        ] {
            client
                .handle_message(message, &event_tx, &thread_state)
                .await
                .expect("handle message");
        }

        let events: Vec<RuntimeEvent> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
        let message_events = events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    RuntimeEvent::MessageDelta { .. } | RuntimeEvent::MessageBoundary { .. }
                )
            })
            .cloned()
            .collect::<Vec<_>>();

        assert!(matches!(
            message_events.as_slice(),
            [
                RuntimeEvent::MessageDelta {
                    lane: RuntimeMessageLane::Answer,
                    text: intro
                },
                RuntimeEvent::MessageBoundary {
                    lane: RuntimeMessageLane::Answer
                },
                RuntimeEvent::MessageDelta {
                    lane: RuntimeMessageLane::Answer,
                    text: body
                }
            ] if intro == "Intro." && body == "**Project**"
        ));
    }

    #[test]
    fn model_list_control_arguments_only_allow_listing_flags() {
        assert_eq!(super::model_list_include_hidden(""), Ok(false));
        assert_eq!(super::model_list_include_hidden("--hidden"), Ok(true));
        assert_eq!(
            super::model_list_include_hidden("--include-hidden"),
            Ok(true)
        );

        let outcome = super::model_list_include_hidden("gpt-5-codex")
            .expect_err("model selection should not be faked as a list request");
        assert!(matches!(
            outcome,
            RuntimeControlOutcome::InteractiveOnly { message } if message.contains("model selection is interactive")
        ));

        let outcome = super::model_list_include_hidden("--unknown")
            .expect_err("unknown model list flags should be rejected");
        assert!(matches!(
            outcome,
            RuntimeControlOutcome::Failed { code, message }
                if code.as_deref() == Some("runtime.control.invalid_arguments")
                    && message.contains("--hidden")
        ));
    }

    #[test]
    fn thread_control_argument_validation_rejects_ambiguous_forms() {
        assert!(matches!(
            super::invalid_thread_control_arguments("rename", ""),
            Some(RuntimeControlOutcome::Failed { code, message })
                if code.as_deref() == Some("runtime.control.invalid_arguments")
                    && message.contains("non-empty name")
        ));
        assert!(matches!(
            super::invalid_thread_control_arguments("compact", "now"),
            Some(RuntimeControlOutcome::Failed { code, message })
                if code.as_deref() == Some("runtime.control.invalid_arguments")
                    && message.contains("does not accept arguments")
        ));
    }

    #[tokio::test]
    async fn review_control_is_unsupported_without_a_verified_native_mapping() {
        let adapter = CodexRuntimeAdapter::new(CodexRuntimeConfig::default());
        let (event_tx, _event_rx) = tokio::sync::mpsc::unbounded_channel();

        let outcome = adapter
            .run_app_server_control(
                RuntimeControlExecution {
                    input: RuntimeControlInput {
                        runtime_session_id: "codex-test".to_string(),
                        raw: "/review base main".to_string(),
                        command_name: "review".to_string(),
                        arguments: "base main".to_string(),
                        origin: RuntimeControlOrigin::SessionTurn,
                        runtime_skill_ids: Vec::new(),
                    },
                    context: RuntimeExecutionContext {
                        network_mode: NetworkMode::On,
                        environment: Vec::new(),
                        runtime_state_root: None,
                        runtime_path_projections: Vec::new(),
                    },
                    executor: Box::new(UnusedRuntimeProgramExecutor),
                },
                event_tx,
            )
            .await
            .expect("review outcome");

        assert!(matches!(
            outcome,
            RuntimeControlOutcome::Unsupported { message }
                if message.contains("not exposed through LionClaw")
        ));
    }

    #[test]
    fn model_list_description_uses_codex_app_server_display_fields() {
        let response = json!({
            "data": [
                {
                    "id": "model-id",
                    "model": "gpt-5-codex",
                    "displayName": "GPT-5 Codex"
                },
                {
                    "id": "fallback-id",
                    "model": "fallback-model"
                }
            ]
        });

        assert_eq!(
            super::describe_model_list_response(&response),
            "Available Codex models: GPT-5 Codex, fallback-model."
        );
    }

    #[test]
    fn completed_turn_error_text_only_flags_failed_turns() {
        assert_eq!(
            super::completed_turn_error_text(&json!({
                "turn": {
                    "id": "turn_1",
                    "status": "completed",
                    "error": null
                }
            })),
            None
        );
        assert_eq!(
            super::completed_turn_error_text(&json!({
                "turn": {
                    "id": "turn_1",
                    "status": "failed",
                    "error": {"message": "boom"}
                }
            })),
            Some("boom".to_string())
        );
        assert_eq!(
            super::completed_turn_error_text(&json!({
                "turn": {
                    "id": "turn_1",
                    "status": "interrupted",
                    "error": null
                }
            })),
            Some("codex turn interrupted".to_string())
        );
    }

    #[tokio::test]
    async fn codex_app_server_protocol_streams_turn_and_saves_thread_id() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        let (adapter, handle, thread_state) = start_codex_test_session_with_config(
            CodexRuntimeConfig {
                executable: "codex".to_string(),
                model: Some("gpt-5-codex".to_string()),
            },
            Some(runtime_state_root.clone()),
        )
        .await;
        let transport = FakeAppServerTransport::new(vec![
            json!({"id": 1, "result": {"serverInfo": {"name": "codex", "version": "test"}, "userAgent": "codex/test"}}),
            json!({"method": "thread/started", "params": {"threadId": "thr_1"}}),
            json!({"id": 2, "result": {}}),
            json!({"id": 3, "result": {"turn": {"id": "turn_1"}}}),
            json!({"method": "item/agentMessage/delta", "params": {"threadId": "thr_1", "turnId": "turn_1", "delta": "Hello"}}),
            json!({"method": "turn/completed", "params": {"threadId": "thr_1", "turnId": "turn_1"}}),
        ]);
        let sent = transport.sent.clone();
        let mut client = CodexAppServerClient::new(transport);
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

        client
            .initialize(&event_tx, &thread_state)
            .await
            .expect("initialize");
        let thread_id = adapter
            .ensure_app_server_thread(
                &mut client,
                &handle.runtime_session_id,
                &event_tx,
                &thread_state,
            )
            .await
            .expect("thread");
        assert_eq!(thread_id, "thr_1");
        let response = client
            .request(
                "turn/start",
                super::turn_start_params(
                    &thread_id,
                    "hello",
                    Some("gpt-5-codex"),
                    NetworkMode::None,
                ),
                &event_tx,
                &thread_state,
            )
            .await
            .expect("turn start");
        client
            .wait_for_turn_completed(
                super::extract_app_server_turn_id(&response).as_deref(),
                None,
                None,
                &event_tx,
                &thread_state,
                None,
            )
            .await
            .expect("turn completed");

        let events: Vec<RuntimeEvent> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::MessageDelta { lane: RuntimeMessageLane::Answer, text } if text == "Hello"
        )));
        assert_eq!(
            std::fs::read_to_string(runtime_state_root.join(CODEX_THREAD_ID_STATE_FILE))
                .expect("thread state"),
            "thr_1\n"
        );

        let sent = sent.lock().expect("sent lock").clone();
        assert!(sent.iter().all(omits_jsonrpc_header));
        assert_eq!(sent[0]["method"], "initialize");
        assert_eq!(sent[1]["method"], "initialized");
        assert_eq!(sent[2]["method"], "thread/start");
        assert_eq!(sent[3]["method"], "turn/start");
        assert_eq!(
            sent[3]["params"]["sandboxPolicy"]["type"],
            "externalSandbox"
        );
        assert_eq!(
            sent[3]["params"]["sandboxPolicy"]["networkAccess"],
            "restricted"
        );

        adapter.close(&handle).await.expect("close");
    }

    #[tokio::test]
    async fn codex_app_server_separates_distinct_agent_message_items() {
        let (adapter, handle, thread_state) = start_codex_test_session(None).await;
        let transport = FakeAppServerTransport::new(vec![
            json!({"id": 1, "result": {"turn": {"id": "turn_1"}}}),
            json!({"method": "item/agentMessage/delta", "params": {"threadId": "thr_1", "turnId": "turn_1", "itemId": "msg_1", "delta": "First."}}),
            json!({"method": "item/agentMessage/delta", "params": {"threadId": "thr_1", "turnId": "turn_1", "itemId": "msg_2", "delta": "Second."}}),
            json!({"method": "turn/completed", "params": {"threadId": "thr_1", "turnId": "turn_1"}}),
        ]);
        let mut client = CodexAppServerClient::new(transport);
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

        let response = client
            .request("turn/start", json!({}), &event_tx, &thread_state)
            .await
            .expect("turn start");
        client
            .wait_for_turn_completed(
                super::extract_app_server_turn_id(&response).as_deref(),
                Some("thr_1"),
                None,
                &event_tx,
                &thread_state,
                None,
            )
            .await
            .expect("turn completed");

        let events = std::iter::from_fn(|| event_rx.try_recv().ok());
        assert_eq!(answer_text_from_events(events), "First.\n\nSecond.");

        adapter.close(&handle).await.expect("close");
    }

    async fn assert_image_generation_event_emits_runtime_artifact(event_message: Value) {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        let generated_dir = runtime_state_root
            .join("home")
            .join(".codex")
            .join("generated_images")
            .join("thr_1");
        assert_image_generation_event_emits_runtime_artifact_at(
            event_message,
            runtime_state_root,
            generated_dir,
            None,
        )
        .await;
    }

    async fn assert_image_generation_event_emits_runtime_artifact_at(
        event_message: Value,
        runtime_state_root: PathBuf,
        generated_dir: PathBuf,
        runtime_context: Option<RuntimeExecutionContext>,
    ) {
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        std::fs::create_dir_all(&generated_dir).expect("create generated image dir");
        std::fs::write(generated_dir.join("ig_1.png"), b"png").expect("write generated image");
        let (adapter, handle, thread_state) =
            start_codex_test_session(Some(runtime_state_root.clone())).await;
        thread_state
            .persist_thread_id("thr_1")
            .expect("persist thread id");
        let transport = FakeAppServerTransport::new(vec![
            json!({"id": 1, "result": {"turn": {"id": "turn_1"}}}),
            event_message,
            json!({"method": "turn/completed", "params": {"threadId": "thr_1", "turnId": "turn_1"}}),
        ]);
        let mut client = match runtime_context {
            Some(runtime_context) => {
                CodexAppServerClient::new_with_runtime_context(transport, runtime_context)
            }
            None => CodexAppServerClient::new(transport),
        };
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

        let response = client
            .request("turn/start", json!({}), &event_tx, &thread_state)
            .await
            .expect("turn start");
        client
            .wait_for_turn_completed(
                super::extract_app_server_turn_id(&response).as_deref(),
                Some("thr_1"),
                None,
                &event_tx,
                &thread_state,
                None,
            )
            .await
            .expect("turn completed");

        let events: Vec<RuntimeEvent> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
        let artifact = events
            .iter()
            .find_map(|event| match event {
                RuntimeEvent::Artifact { artifact } => Some(artifact),
                _ => None,
            })
            .expect("image artifact event");
        assert_eq!(artifact.artifact_id, "codex:image:thr_1:ig_1");
        assert_eq!(artifact.filename.as_deref(), Some("ig_1.png"));
        assert_eq!(artifact.mime_type.as_deref(), Some("image/png"));
        assert_eq!(artifact.path, generated_dir.join("ig_1.png"));

        adapter.close(&handle).await.expect("close");
    }

    #[tokio::test]
    async fn codex_app_server_method_image_generation_event_emits_runtime_artifact() {
        assert_image_generation_event_emits_runtime_artifact(json!({
            "method": "event_msg",
            "params": {
                "payload": {
                    "type": "image_generation_end",
                    "call_id": "ig_1",
                    "status": "completed"
                }
            }
        }))
        .await;
    }

    #[tokio::test]
    async fn codex_app_server_top_level_image_generation_event_emits_runtime_artifact() {
        assert_image_generation_event_emits_runtime_artifact(json!({
            "type": "event_msg",
            "payload": {
                "type": "image_generation_end",
                "call_id": "ig_1",
                "status": "completed",
                "result": "base64 omitted"
            }
        }))
        .await;
    }

    #[tokio::test]
    async fn codex_app_server_image_generation_saved_path_maps_runtime_root() {
        assert_image_generation_event_emits_runtime_artifact(json!({
            "type": "event_msg",
            "payload": {
                "type": "image_generation_end",
                "call_id": "ig_1",
                "status": "generating",
                "saved_path": "/runtime/home/.codex/generated_images/thr_1/ig_1.png",
                "result": "base64 omitted"
            }
        }))
        .await;
    }

    #[tokio::test]
    async fn codex_app_server_image_generation_saved_path_maps_runtime_home_projection() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        let runtime_home_root = temp_dir.path().join("runtime-home");
        let generated_dir = runtime_home_root
            .join(".codex")
            .join("generated_images")
            .join("thr_1");
        let runtime_context = RuntimeExecutionContext {
            network_mode: NetworkMode::On,
            environment: Vec::new(),
            runtime_state_root: Some(runtime_state_root.clone()),
            runtime_path_projections: vec![
                RuntimePathProjection::directory("/runtime", runtime_state_root.clone())
                    .expect("runtime projection"),
                RuntimePathProjection::directory("/runtime/home", runtime_home_root)
                    .expect("runtime home projection"),
            ],
        };

        assert_image_generation_event_emits_runtime_artifact_at(
            json!({
                "type": "event_msg",
                "payload": {
                    "type": "image_generation_end",
                    "call_id": "ig_1",
                    "status": "generating",
                    "saved_path": "/runtime/home/.codex/generated_images/thr_1/ig_1.png",
                    "result": "base64 omitted"
                }
            }),
            runtime_state_root,
            generated_dir,
            Some(runtime_context),
        )
        .await;
    }

    #[tokio::test]
    async fn codex_app_server_image_generation_default_path_maps_runtime_home_projection() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        let runtime_home_root = temp_dir.path().join("runtime-home");
        let generated_dir = runtime_home_root
            .join(".codex")
            .join("generated_images")
            .join("thr_1");
        let runtime_context = RuntimeExecutionContext {
            network_mode: NetworkMode::On,
            environment: Vec::new(),
            runtime_state_root: Some(runtime_state_root.clone()),
            runtime_path_projections: vec![
                RuntimePathProjection::directory("/runtime", runtime_state_root.clone())
                    .expect("runtime projection"),
                RuntimePathProjection::directory("/runtime/home", runtime_home_root)
                    .expect("runtime home projection"),
            ],
        };

        assert_image_generation_event_emits_runtime_artifact_at(
            json!({
                "type": "event_msg",
                "payload": {
                    "type": "image_generation_end",
                    "call_id": "ig_1",
                    "status": "completed",
                    "result": "base64 omitted"
                }
            }),
            runtime_state_root,
            generated_dir,
            Some(runtime_context),
        )
        .await;
    }

    #[test]
    fn codex_generated_image_path_rejects_runtime_root_traversal() {
        let runtime_state_root = PathBuf::from("/host/runtime-state");

        assert_eq!(
            super::codex_generated_image_path(
                "/runtime/./home/.codex/generated_images/thr_1/ig_1.png",
                &runtime_state_root,
                None,
            ),
            Some(PathBuf::from(
                "/host/runtime-state/home/.codex/generated_images/thr_1/ig_1.png"
            ))
        );
        assert_eq!(
            super::codex_generated_image_path("/runtime/../outside.png", &runtime_state_root, None),
            None
        );
        assert_eq!(
            super::codex_generated_image_path("../outside.png", &runtime_state_root, None),
            None
        );
        assert_eq!(
            super::codex_generated_image_path("/tmp/outside.png", &runtime_state_root, None),
            None
        );
        assert_eq!(
            super::codex_generated_image_path("/runtime", &runtime_state_root, None),
            None
        );
        assert_eq!(
            super::codex_generated_image_path(".", &runtime_state_root, None),
            None
        );
    }

    #[test]
    fn codex_generated_image_path_uses_runtime_home_projection() {
        let runtime_state_root = PathBuf::from("/host/runtime-state");
        let runtime_home_root = PathBuf::from("/host/runtime-home");
        let context = RuntimeExecutionContext {
            network_mode: NetworkMode::On,
            environment: Vec::new(),
            runtime_state_root: Some(runtime_state_root.clone()),
            runtime_path_projections: vec![
                RuntimePathProjection::directory("/runtime", runtime_state_root.clone())
                    .expect("runtime projection"),
                RuntimePathProjection::directory("/runtime/home", runtime_home_root.clone())
                    .expect("runtime home projection"),
            ],
        };

        assert_eq!(
            super::codex_generated_image_path(
                "/runtime/home/.codex/generated_images/thr_1/ig_1.png",
                &runtime_state_root,
                Some(&context),
            ),
            Some(PathBuf::from(
                "/host/runtime-home/.codex/generated_images/thr_1/ig_1.png"
            ))
        );
    }

    #[test]
    fn codex_default_generated_image_path_uses_runtime_home_projection() {
        let runtime_state_root = PathBuf::from("/host/runtime-state");
        let runtime_home_root = PathBuf::from("/host/runtime-home");
        let context = RuntimeExecutionContext {
            network_mode: NetworkMode::On,
            environment: Vec::new(),
            runtime_state_root: Some(runtime_state_root.clone()),
            runtime_path_projections: vec![
                RuntimePathProjection::directory("/runtime", runtime_state_root.clone())
                    .expect("runtime projection"),
                RuntimePathProjection::directory("/runtime/home", runtime_home_root.clone())
                    .expect("runtime home projection"),
            ],
        };

        assert_eq!(
            super::codex_default_generated_image_path(
                "thr_1",
                "ig_1.png",
                &runtime_state_root,
                Some(&context),
            ),
            runtime_home_root
                .join(".codex")
                .join("generated_images")
                .join("thr_1")
                .join("ig_1.png")
        );
        assert_eq!(
            super::codex_default_generated_image_path(
                "thr_1",
                "ig_1.png",
                &runtime_state_root,
                None,
            ),
            runtime_state_root
                .join("home")
                .join(".codex")
                .join("generated_images")
                .join("thr_1")
                .join("ig_1.png")
        );
    }

    #[tokio::test]
    async fn codex_app_server_image_generation_unsafe_saved_path_does_not_use_default_artifact() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        let generated_dir = runtime_state_root
            .join("home")
            .join(".codex")
            .join("generated_images")
            .join("thr_1");
        std::fs::create_dir_all(&generated_dir).expect("create generated image dir");
        std::fs::write(generated_dir.join("ig_1.png"), b"png").expect("write generated image");
        let (adapter, handle, thread_state) =
            start_codex_test_session(Some(runtime_state_root)).await;
        thread_state
            .persist_thread_id("thr_1")
            .expect("persist thread id");
        let transport = FakeAppServerTransport::new(vec![
            json!({"id": 1, "result": {"turn": {"id": "turn_1"}}}),
            json!({
                "type": "event_msg",
                "payload": {
                    "type": "image_generation_end",
                    "call_id": "ig_1",
                    "status": "generating",
                    "saved_path": "/runtime/../outside.png"
                }
            }),
            json!({"method": "turn/completed", "params": {"threadId": "thr_1", "turnId": "turn_1"}}),
        ]);
        let mut client = CodexAppServerClient::new(transport);
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

        let response = client
            .request("turn/start", json!({}), &event_tx, &thread_state)
            .await
            .expect("turn start");
        client
            .wait_for_turn_completed(
                super::extract_app_server_turn_id(&response).as_deref(),
                Some("thr_1"),
                None,
                &event_tx,
                &thread_state,
                None,
            )
            .await
            .expect("turn completed");

        let artifacts = std::iter::from_fn(|| event_rx.try_recv().ok())
            .filter(|event| matches!(event, RuntimeEvent::Artifact { .. }))
            .count();
        assert_eq!(artifacts, 0);

        adapter.close(&handle).await.expect("close");
    }

    #[tokio::test]
    async fn codex_app_server_image_generation_item_emits_runtime_artifact() {
        assert_image_generation_event_emits_runtime_artifact(json!({
            "method": "item/completed",
            "params": {
                "threadId": "thr_1",
                "turnId": "turn_1",
                "item": {
                    "type": "imageGeneration",
                    "id": "ig_1",
                    "status": "completed"
                }
            }
        }))
        .await;
    }

    #[tokio::test]
    async fn codex_app_server_response_item_image_generation_emits_runtime_artifact() {
        assert_image_generation_event_emits_runtime_artifact(json!({
            "type": "response_item",
            "payload": {
                "type": "image_generation_call",
                "id": "ig_1",
                "status": "completed",
                "result": "base64 omitted"
            }
        }))
        .await;
    }

    #[tokio::test]
    async fn codex_app_server_image_generation_interim_update_does_not_dedupe_completed_path() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        let final_image = runtime_state_root
            .join("home")
            .join(".codex")
            .join("generated_images")
            .join("thr_1")
            .join("ig_1-final.png");
        std::fs::create_dir_all(final_image.parent().expect("final image parent"))
            .expect("create generated image dir");
        std::fs::write(&final_image, b"png").expect("write final generated image");
        let (adapter, handle, thread_state) =
            start_codex_test_session(Some(runtime_state_root)).await;
        thread_state
            .persist_thread_id("thr_1")
            .expect("persist thread id");
        let transport = FakeAppServerTransport::new(vec![
            json!({"id": 1, "result": {"turn": {"id": "turn_1"}}}),
            json!({
                "method": "item/updated",
                "params": {
                    "threadId": "thr_1",
                    "turnId": "turn_1",
                    "item": {
                        "type": "imageGeneration",
                        "id": "ig_1",
                        "status": "generating"
                    }
                }
            }),
            json!({
                "method": "item/updated",
                "params": {
                    "threadId": "thr_1",
                    "turnId": "turn_1",
                    "item": {
                        "type": "imageGeneration",
                        "id": "ig_1",
                        "status": "completed",
                        "savedPath": "/runtime/home/.codex/generated_images/thr_1/ig_1-final.png"
                    }
                }
            }),
            json!({"method": "turn/completed", "params": {"threadId": "thr_1", "turnId": "turn_1"}}),
        ]);
        let mut client = CodexAppServerClient::new(transport);
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

        let response = client
            .request("turn/start", json!({}), &event_tx, &thread_state)
            .await
            .expect("turn start");
        client
            .wait_for_turn_completed(
                super::extract_app_server_turn_id(&response).as_deref(),
                Some("thr_1"),
                None,
                &event_tx,
                &thread_state,
                None,
            )
            .await
            .expect("turn completed");

        let artifacts = std::iter::from_fn(|| event_rx.try_recv().ok())
            .filter_map(|event| match event {
                RuntimeEvent::Artifact { artifact } => Some(artifact),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].artifact_id, "codex:image:thr_1:ig_1");
        assert_eq!(artifacts[0].filename.as_deref(), Some("ig_1-final.png"));
        assert_eq!(artifacts[0].path, final_image);

        adapter.close(&handle).await.expect("close");
    }

    #[tokio::test]
    async fn codex_app_server_protocol_resumes_saved_thread_id() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        std::fs::write(
            runtime_state_root.join(CODEX_THREAD_ID_STATE_FILE),
            "thr_saved\n",
        )
        .expect("write thread id");

        let (adapter, handle, thread_state) =
            start_codex_ready_test_session(runtime_state_root).await;
        let transport = FakeAppServerTransport::new(vec![
            json!({"id": 1, "result": {"thread": {"id": "thr_saved"}}}),
        ]);
        let sent = transport.sent.clone();
        let mut client = CodexAppServerClient::new(transport);
        let (event_tx, _event_rx) = tokio::sync::mpsc::unbounded_channel();

        let thread_id = adapter
            .ensure_app_server_thread(
                &mut client,
                &handle.runtime_session_id,
                &event_tx,
                &thread_state,
            )
            .await
            .expect("resume thread");

        assert_eq!(thread_id, "thr_saved");
        let sent = sent.lock().expect("sent lock").clone();
        assert_eq!(sent[0]["method"], "thread/resume");
        assert_eq!(sent[0]["params"]["threadId"], "thr_saved");

        adapter.close(&handle).await.expect("close");
    }

    #[tokio::test]
    async fn codex_app_server_unmatched_terminal_notifications_complete_current_waiter() {
        let (adapter, handle, thread_state) = start_codex_test_session(None).await;
        let transport = FakeAppServerTransport::new(vec![
            json!({"id": 1, "result": {"turn": {"id": "turn_1"}}}),
            json!({
                "method": "turn/completed",
                "params": {
                    "threadId": "thr_1",
                    "turn": {"status": "completed"}
                }
            }),
        ]);
        let mut client = CodexAppServerClient::new(transport);
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

        let response = client
            .request("turn/start", json!({}), &event_tx, &thread_state)
            .await
            .expect("turn start");
        client
            .wait_for_turn_completed(
                super::extract_app_server_turn_id(&response).as_deref(),
                None,
                None,
                &event_tx,
                &thread_state,
                None,
            )
            .await
            .expect("id-less completion should complete the active turn");

        let events: Vec<RuntimeEvent> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
        assert!(events
            .iter()
            .any(|event| matches!(event, RuntimeEvent::Done)));

        adapter.close(&handle).await.expect("close");
    }

    #[tokio::test]
    async fn codex_app_server_compaction_uses_context_compaction_contract_fixture() {
        let fixture = codex_protocol_fixture("compact_context_compaction_v2");
        let (method, params) = fixture_client_request(&fixture);
        let (adapter, handle, thread_state) = start_codex_test_session(None).await;
        let transport = FakeAppServerTransport::new(fixture_server_messages(&fixture));
        let sent = transport.sent.clone();
        let mut client = CodexAppServerClient::new(transport);
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
        let (interrupt_tx, mut interrupt_rx) = tokio::sync::mpsc::unbounded_channel();

        client
            .request(method, params, &event_tx, &thread_state)
            .await
            .expect("compact start request");
        client
            .wait_for_context_compaction_completed(
                "thr_1",
                interrupt_tx,
                &mut interrupt_rx,
                &event_tx,
                &thread_state,
            )
            .await
            .expect("context compaction should complete through turn notifications");

        let sent = sent.lock().expect("sent lock").clone();
        assert_eq!(sent, vec![fixture_client_message(&fixture)]);

        let events: Vec<RuntimeEvent> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::Status { text, .. } if text == "codex context compacted"
        )));
        assert!(events
            .iter()
            .any(|event| matches!(event, RuntimeEvent::Done)));

        adapter.close(&handle).await.expect("close");
    }

    #[tokio::test]
    async fn codex_app_server_turn_interrupt_uses_contract_fixture() {
        let fixture = codex_protocol_fixture("turn_interrupt_v2");
        let (adapter, handle, thread_state) = start_codex_test_session(None).await;
        let transport = FakeAppServerTransport::new(fixture_server_messages(&fixture));
        let sent = transport.sent.clone();
        let mut client = CodexAppServerClient::new(transport);
        let (event_tx, _event_rx) = tokio::sync::mpsc::unbounded_channel();

        client
            .interrupt_turn("thr_1", "turn_1", &event_tx, &thread_state)
            .await
            .expect("turn interrupt should accept interrupted terminal notification");

        let sent = sent.lock().expect("sent lock").clone();
        assert_eq!(sent, vec![fixture_client_message(&fixture)]);

        adapter.close(&handle).await.expect("close");
    }

    #[tokio::test]
    async fn codex_cancel_interrupts_active_app_server_turn() {
        let (adapter, handle, thread_state) = start_codex_test_session(None).await;
        let (interrupt_tx, mut interrupt_rx) = tokio::sync::mpsc::unbounded_channel();
        thread_state
            .set_active_turn("thr_1", "turn_1", interrupt_tx)
            .expect("set active turn");

        let wait_task = tokio::spawn(async move {
            let request = interrupt_rx.recv().await.expect("interrupt request");
            request.ack_tx.send(Ok(())).expect("ack interrupt");
        });

        adapter
            .cancel(&handle, Some("test timeout".to_string()))
            .await
            .expect("cancel sends native Codex interrupt");
        wait_task.await.expect("wait task joins");

        adapter.close(&handle).await.expect("close");
    }

    #[tokio::test]
    async fn codex_turn_started_notification_registers_active_cancel_target() {
        let (adapter, handle, thread_state) = start_codex_test_session(None).await;
        let transport = FakeAppServerTransport::new(Vec::new());
        let mut client = CodexAppServerClient::new(transport);
        let (event_tx, _event_rx) = tokio::sync::mpsc::unbounded_channel();
        let (interrupt_tx, mut interrupt_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut registered_turn_id = None;

        client
            .handle_message(
                json!({
                    "method": "turn/started",
                    "params": {
                        "threadId": "thr_1",
                        "turnId": "turn_1"
                    }
                }),
                &event_tx,
                &thread_state,
            )
            .await
            .expect("turn started notification");
        client
            .register_active_wait_turn(
                Some("thr_1"),
                None,
                Some(&interrupt_tx),
                &mut registered_turn_id,
                &thread_state,
            )
            .expect("register active wait turn");

        assert_eq!(registered_turn_id.as_deref(), Some("turn_1"));
        let wait_task = tokio::spawn(async move {
            let request = interrupt_rx.recv().await.expect("interrupt request");
            request.ack_tx.send(Ok(())).expect("ack interrupt");
        });

        adapter
            .cancel(&handle, Some("test timeout".to_string()))
            .await
            .expect("cancel sends native Codex interrupt");
        wait_task.await.expect("wait task joins");

        adapter.close(&handle).await.expect("close");
    }

    #[tokio::test]
    async fn codex_app_server_failed_turn_completion_returns_error() {
        let (adapter, handle, thread_state) = start_codex_test_session(None).await;
        let transport = FakeAppServerTransport::new(vec![
            json!({"id": 1, "result": {"turn": {"id": "turn_1"}}}),
            json!({
                "method": "turn/completed",
                "params": {
                    "threadId": "thr_1",
                    "turn": {
                        "id": "turn_1",
                        "status": "failed",
                        "error": {"message": "boom from model"}
                    }
                }
            }),
        ]);
        let mut client = CodexAppServerClient::new(transport);
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

        let response = client
            .request("turn/start", json!({}), &event_tx, &thread_state)
            .await
            .expect("turn start");
        let err = client
            .wait_for_turn_completed(
                super::extract_app_server_turn_id(&response).as_deref(),
                None,
                None,
                &event_tx,
                &thread_state,
                None,
            )
            .await
            .expect_err("failed turn should be returned as an adapter error");

        assert!(err.to_string().contains("boom from model"));
        let events: Vec<RuntimeEvent> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
        assert!(!events
            .iter()
            .any(|event| matches!(event, RuntimeEvent::Done)));

        adapter.close(&handle).await.expect("close");
    }

    #[tokio::test]
    async fn codex_app_server_unmatched_terminal_error_returns_error() {
        let (adapter, handle, thread_state) = start_codex_test_session(None).await;
        let transport = FakeAppServerTransport::new(vec![
            json!({"id": 1, "result": {"turn": {"id": "turn_1"}}}),
            json!({
                "method": "error",
                "params": {
                    "threadId": "thr_1",
                    "error": {"message": "global app-server failure"},
                    "willRetry": false
                }
            }),
        ]);
        let mut client = CodexAppServerClient::new(transport);
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

        let response = client
            .request("turn/start", json!({}), &event_tx, &thread_state)
            .await
            .expect("turn start");
        let err = client
            .wait_for_turn_completed(
                super::extract_app_server_turn_id(&response).as_deref(),
                None,
                None,
                &event_tx,
                &thread_state,
                None,
            )
            .await
            .expect_err("unmatched terminal errors should fail a specific turn");

        assert!(err.to_string().contains("global app-server failure"));
        let events: Vec<RuntimeEvent> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
        assert!(!events
            .iter()
            .any(|event| matches!(event, RuntimeEvent::Done)));

        adapter.close(&handle).await.expect("close");
    }

    #[tokio::test]
    async fn codex_app_server_retryable_error_notification_does_not_fail_turn() {
        let (adapter, handle, thread_state) = start_codex_test_session(None).await;
        let transport = FakeAppServerTransport::new(vec![
            json!({"id": 1, "result": {"turn": {"id": "turn_1"}}}),
            json!({
                "method": "error",
                "params": {
                    "threadId": "thr_1",
                    "turnId": "turn_1",
                    "message": "temporary overload",
                    "willRetry": true
                }
            }),
            json!({
                "method": "turn/completed",
                "params": {
                    "threadId": "thr_1",
                    "turn": {
                        "id": "turn_1",
                        "status": "completed"
                    }
                }
            }),
        ]);
        let mut client = CodexAppServerClient::new(transport);
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

        let response = client
            .request("turn/start", json!({}), &event_tx, &thread_state)
            .await
            .expect("turn start");
        client
            .wait_for_turn_completed(
                super::extract_app_server_turn_id(&response).as_deref(),
                None,
                None,
                &event_tx,
                &thread_state,
                None,
            )
            .await
            .expect("retryable error should not fail a completed turn");

        let events: Vec<RuntimeEvent> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::Status { code: Some(code), text }
                if code == "runtime.retrying" && text == "temporary overload"
        )));
        assert!(!events
            .iter()
            .any(|event| matches!(event, RuntimeEvent::Error { .. })));
        assert!(events
            .iter()
            .any(|event| matches!(event, RuntimeEvent::Done)));

        adapter.close(&handle).await.expect("close");
    }

    #[tokio::test]
    async fn codex_app_server_protocol_rejects_internal_approval_callbacks() {
        let (adapter, handle, thread_state) = start_codex_test_session(None).await;
        let transport = FakeAppServerTransport::new(vec![
            json!({"id": 1, "result": {}}),
            json!({"id": "approval-1", "method": "item/commandExecution/requestApproval", "params": {}}),
            json!({"method": "turn/completed", "params": {"turnId": "turn_1"}}),
        ]);
        let sent = transport.sent.clone();
        let mut client = CodexAppServerClient::new(transport);
        let (event_tx, _event_rx) = tokio::sync::mpsc::unbounded_channel();

        let response = client
            .request("turn/start", json!({}), &event_tx, &thread_state)
            .await
            .expect("turn start");
        client
            .wait_for_turn_completed(
                super::extract_app_server_turn_id(&response).as_deref(),
                None,
                None,
                &event_tx,
                &thread_state,
                None,
            )
            .await
            .expect("turn completed");

        let sent = sent.lock().expect("sent lock").clone();
        assert!(sent.iter().any(|message| {
            message.get("id").and_then(Value::as_str) == Some("approval-1")
                && omits_jsonrpc_header(message)
                && message.pointer("/error/code").and_then(Value::as_i64) == Some(-32601)
                && message
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .is_some_and(|message| message.contains("LionClaw policy"))
        }));

        adapter.close(&handle).await.expect("close");
    }

    #[tokio::test]
    async fn finish_app_server_session_shuts_down_after_protocol_error() {
        let transport = FakeAppServerTransport::new(Vec::new());
        let shutdowns = transport.shutdowns.clone();
        let client = CodexAppServerClient::new(transport);

        let err = super::finish_app_server_session::<_, ()>(
            client,
            Err(anyhow::anyhow!("protocol failed")),
        )
        .await
        .expect_err("protocol error should be returned");

        assert!(err.to_string().contains("protocol failed"));
        assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn missing_or_invalid_thread_file_starts_fresh_codex_thread() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        std::fs::write(runtime_state_root.join(CODEX_THREAD_ID_STATE_FILE), "\n")
            .expect("write invalid thread id");

        let (adapter, handle, _) = start_codex_test_session(Some(runtime_state_root)).await;
        assert!(!handle.resumes_existing_session);

        adapter.close(&handle).await.expect("close");
    }

    #[tokio::test]
    async fn saved_thread_file_without_ready_marker_starts_fresh_codex_thread() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        std::fs::write(
            runtime_state_root.join(CODEX_THREAD_ID_STATE_FILE),
            "thread-old\n",
        )
        .expect("write thread id");

        let (adapter, handle, _) = start_codex_test_session(Some(runtime_state_root)).await;
        assert!(!handle.resumes_existing_session);

        adapter.close(&handle).await.expect("close");
    }

    #[tokio::test]
    async fn symlinked_thread_file_is_rejected() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path().join("runtime-state");
        std::fs::create_dir_all(&runtime_state_root).expect("create runtime state root");
        let target = temp_dir.path().join("thread-id-target");
        std::fs::write(&target, "thread-old\n").expect("write target");
        symlink(&target, runtime_state_root.join(CODEX_THREAD_ID_STATE_FILE))
            .expect("create symlink");

        let adapter = CodexRuntimeAdapter::new(CodexRuntimeConfig::default());
        let runtime_session_ready = mark_runtime_ready(&runtime_state_root);
        let err = adapter
            .session_start(RuntimeSessionStartInput {
                session_id: Uuid::new_v4(),
                working_dir: None,
                environment: Vec::new(),
                runtime_skill_ids: Vec::new(),
                runtime_state_root: Some(runtime_state_root),
                runtime_session_ready,
            })
            .await
            .expect_err("symlinked thread state should fail");
        assert!(err.to_string().contains("cannot be a symlink"));
    }

    #[tokio::test]
    async fn different_lionclaw_sessions_do_not_share_codex_thread_ids() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_a = temp_dir.path().join("runtime-a");
        let runtime_b = temp_dir.path().join("runtime-b");
        std::fs::create_dir_all(&runtime_a).expect("create runtime a");
        std::fs::create_dir_all(&runtime_b).expect("create runtime b");
        std::fs::write(runtime_a.join(CODEX_THREAD_ID_STATE_FILE), "thread-a\n")
            .expect("write thread a");
        std::fs::write(runtime_b.join(CODEX_THREAD_ID_STATE_FILE), "thread-b\n")
            .expect("write thread b");

        let adapter = CodexRuntimeAdapter::new(CodexRuntimeConfig::default());
        let runtime_a_ready = mark_runtime_ready(&runtime_a);
        let runtime_b_ready = mark_runtime_ready(&runtime_b);
        let handle_a = adapter
            .session_start(RuntimeSessionStartInput {
                session_id: Uuid::new_v4(),
                working_dir: None,
                environment: Vec::new(),
                runtime_skill_ids: Vec::new(),
                runtime_state_root: Some(runtime_a),
                runtime_session_ready: runtime_a_ready,
            })
            .await
            .expect("start a");
        let handle_b = adapter
            .session_start(RuntimeSessionStartInput {
                session_id: Uuid::new_v4(),
                working_dir: None,
                environment: Vec::new(),
                runtime_skill_ids: Vec::new(),
                runtime_state_root: Some(runtime_b),
                runtime_session_ready: runtime_b_ready,
            })
            .await
            .expect("start b");

        assert_eq!(
            adapter
                .current_thread_id(&handle_a.runtime_session_id)
                .expect("thread a"),
            Some("thread-a".to_string())
        );
        assert_eq!(
            adapter
                .current_thread_id(&handle_b.runtime_session_id)
                .expect("thread b"),
            Some("thread-b".to_string())
        );

        adapter.close(&handle_a).await.expect("close a");
        adapter.close(&handle_b).await.expect("close b");
    }

    async fn start_codex_ready_test_session(
        runtime_state_root: PathBuf,
    ) -> (
        CodexRuntimeAdapter,
        RuntimeSessionHandle,
        super::CodexThreadState,
    ) {
        let adapter = CodexRuntimeAdapter::new(CodexRuntimeConfig::default());
        let runtime_session_ready = mark_runtime_ready(&runtime_state_root);
        let handle = adapter
            .session_start(RuntimeSessionStartInput {
                session_id: Uuid::new_v4(),
                working_dir: None,
                environment: Vec::new(),
                runtime_skill_ids: Vec::new(),
                runtime_state_root: Some(runtime_state_root),
                runtime_session_ready,
            })
            .await
            .expect("start");
        let thread_state = adapter.thread_state_for(&handle.runtime_session_id);
        (adapter, handle, thread_state)
    }
}
