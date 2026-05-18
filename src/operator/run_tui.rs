use std::{
    io::{self, Stdout},
    path::PathBuf,
    time::Duration,
};

use anyhow::{anyhow, Result};
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout, Margin, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Wrap},
    Frame, Terminal,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use uuid::Uuid;

use crate::{
    contracts::{
        SessionActionKind, SessionActionRequest, SessionHistoryRequest, SessionTurnRequest,
        SessionTurnResponse, SessionTurnStatus, SessionTurnView, StreamEventDto,
        StreamEventKindDto, StreamLaneDto,
    },
    home::LionClawHome,
    kernel::{
        input_routing::{classify_input, ClassifiedInput, LionClawControlInput},
        Kernel,
    },
    operator::{
        config::OperatorConfig,
        reconcile::{open_runtime_kernel_for_work_root, render_runtime_cache_for_work_root},
        run::{
            kernel_to_anyhow, local_peer_id_for_project, partial_history_marker,
            resolve_repl_session, resolve_run_runtime_id,
        },
        runtime::validate_runtime_launch_prerequisites,
        target::{inspect_target_work_root, list_project_instance_statuses, TargetContext},
    },
    runtime_timeouts::RuntimeTurnTimeouts,
};

use crate::kernel::runtime::{execution::planner::resolve_execution_preset, ExecutionPlanPurpose};

const EVENT_POLL: Duration = Duration::from_millis(50);
const HISTORY_LIMIT: usize = 24;

pub(crate) struct RunConsoleInvocation<'a> {
    pub(crate) target: &'a TargetContext,
    pub(crate) requested_runtime: Option<String>,
    pub(crate) continue_last_session: bool,
    pub(crate) timeout_override: Option<RuntimeTurnTimeouts>,
}

pub(crate) struct RunConsoleOutcome {
    pub(crate) launch_blocked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InstanceSummary {
    pub(crate) name: Option<String>,
    pub(crate) is_default: bool,
    pub(crate) home: PathBuf,
    pub(crate) work_root: Option<PathBuf>,
    pub(crate) work_root_finding: Option<String>,
    pub(crate) shared_work_root_count: usize,
}

impl InstanceSummary {
    fn display_name(&self) -> &str {
        self.name.as_deref().unwrap_or("selected home")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LaunchBlocker {
    title: String,
    detail: String,
    suggestion: String,
}

impl LaunchBlocker {
    fn for_instance(instance_name: &str, detail: impl Into<String>) -> Self {
        Self {
            title: format!("Launch blocked for {instance_name}"),
            detail: detail.into(),
            suggestion: "Run lionclaw doctor for setup guidance. The run command will not repair configuration.".to_string(),
        }
    }

    pub(crate) fn standalone(detail: impl Into<String>) -> Self {
        Self {
            title: "Launch blocked".to_string(),
            detail: detail.into(),
            suggestion: "Run lionclaw doctor for setup guidance.".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BoundarySummary {
    workspace: String,
    network: String,
    secrets: String,
    timeout: String,
    preset: String,
}

#[derive(Clone)]
struct ReadyInstance {
    summary: InstanceSummary,
    runtime_id: String,
    runtime_kind: String,
    runtime_override: Option<String>,
    boundary: BoundarySummary,
    kernel: Kernel,
    session_id: Uuid,
}

enum SelectedInstanceState {
    Ready(Box<ReadyInstance>),
    Blocked {
        summary: InstanceSummary,
        blocker: LaunchBlocker,
    },
}

impl SelectedInstanceState {
    fn summary(&self) -> &InstanceSummary {
        match self {
            Self::Ready(ready) => &ready.summary,
            Self::Blocked { summary, .. } => summary,
        }
    }

    fn is_ready(&self) -> bool {
        matches!(self, Self::Ready(_))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Focus {
    Instances,
    Transcript,
    Composer,
    Inspectors,
}

impl Focus {
    fn next(self, project_mode: bool) -> Self {
        match (self, project_mode) {
            (Self::Instances, _) => Self::Transcript,
            (Self::Transcript, _) => Self::Composer,
            (Self::Composer, _) => Self::Inspectors,
            (Self::Inspectors, true) => Self::Instances,
            (Self::Inspectors, false) => Self::Transcript,
        }
    }

    fn previous(self, project_mode: bool) -> Self {
        match (self, project_mode) {
            (Self::Instances, _) => Self::Inspectors,
            (Self::Transcript, true) => Self::Instances,
            (Self::Transcript, false) => Self::Inspectors,
            (Self::Composer, _) => Self::Transcript,
            (Self::Inspectors, _) => Self::Composer,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Overlay {
    Help,
    Palette,
    ExitConfirm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TranscriptLineKind {
    User,
    Answer,
    Reasoning,
    Status,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TranscriptLine {
    kind: TranscriptLineKind,
    text: String,
}

impl TranscriptLine {
    fn new(kind: TranscriptLineKind, text: impl Into<String>) -> Self {
        Self {
            kind,
            text: text.into(),
        }
    }
}

pub(crate) struct ConsoleApp {
    project_root: Option<PathBuf>,
    instances: Vec<InstanceSummary>,
    selected_index: usize,
    selected: SelectedInstanceState,
    continue_last_session: bool,
    timeout_override: Option<RuntimeTurnTimeouts>,
    focus: Focus,
    overlay: Option<Overlay>,
    composer: String,
    transcript: Vec<TranscriptLine>,
    transcript_scroll: u16,
    status: String,
    active_turn: Option<JoinHandle<()>>,
    active_turn_cancel: Option<oneshot::Sender<String>>,
    saw_ready_instance: bool,
    should_quit: bool,
}

impl ConsoleApp {
    pub(crate) async fn load(invocation: RunConsoleInvocation<'_>) -> Result<Self> {
        let target = invocation.target;
        let (project_root, instances, selected_index) = resolve_console_instances(target)?;
        let selected_summary = instances
            .get(selected_index)
            .cloned()
            .ok_or_else(|| anyhow!("console has no selected instance"))?;
        let requested_runtime = invocation.requested_runtime;
        let selected = open_selected_instance(
            selected_summary,
            requested_runtime.clone(),
            invocation.continue_last_session,
            invocation.timeout_override,
        )
        .await;
        let saw_ready_instance = selected.is_ready();
        let mut app = Self {
            project_root,
            instances,
            selected_index,
            selected,
            continue_last_session: invocation.continue_last_session,
            timeout_override: invocation.timeout_override,
            focus: if target.project_root.is_some() {
                Focus::Instances
            } else {
                Focus::Composer
            },
            overlay: None,
            composer: String::new(),
            transcript: Vec::new(),
            transcript_scroll: 0,
            status: "idle".to_string(),
            active_turn: None,
            active_turn_cancel: None,
            saw_ready_instance,
            should_quit: false,
        };
        app.load_selected_history().await;
        Ok(app)
    }

    fn project_mode(&self) -> bool {
        self.project_root.is_some()
    }

    fn active(&self) -> bool {
        self.active_turn.is_some()
    }

    fn selected_name(&self) -> &str {
        self.selected.summary().display_name()
    }

    async fn switch_selected(&mut self, next_index: usize) {
        if self.active() {
            self.status = "finish the active turn before switching instances".to_string();
            return;
        }
        if next_index >= self.instances.len() || next_index == self.selected_index {
            return;
        }

        self.selected_index = next_index;
        let summary = self
            .instances
            .get(next_index)
            .cloned()
            .unwrap_or_else(|| self.selected.summary().clone());
        self.status = format!("selected {}", summary.display_name());
        self.selected = open_selected_instance(
            summary,
            None,
            self.continue_last_session,
            self.timeout_override,
        )
        .await;
        if self.selected.is_ready() {
            self.saw_ready_instance = true;
        }
        self.composer.clear();
        self.transcript.clear();
        self.transcript_scroll = 0;
        self.load_selected_history().await;
    }

    async fn load_selected_history(&mut self) {
        let SelectedInstanceState::Ready(ready) = &self.selected else {
            return;
        };

        match ready
            .kernel
            .session_history(SessionHistoryRequest {
                session_id: ready.session_id,
                limit: Some(HISTORY_LIMIT),
            })
            .await
        {
            Ok(history) => {
                for turn in history.turns {
                    push_history_turn(&mut self.transcript, &turn);
                }
                if !self.transcript.is_empty() {
                    self.status = format!(
                        "loaded {} recent transcript entr{}",
                        self.transcript.len(),
                        if self.transcript.len() == 1 {
                            "y"
                        } else {
                            "ies"
                        }
                    );
                }
            }
            Err(err) => {
                self.transcript.push(TranscriptLine::new(
                    TranscriptLineKind::Error,
                    format!("failed to load session history: {err}"),
                ));
            }
        }
    }

    fn submit_composer(&mut self, backend_tx: &mpsc::UnboundedSender<BackendEvent>) {
        if self.active() {
            self.status = "a turn is already active".to_string();
            return;
        }
        if !self.selected.is_ready() {
            self.status = "launch is blocked for the selected instance".to_string();
            return;
        }
        let text = std::mem::take(&mut self.composer);
        match classify_input(&text) {
            ClassifiedInput::Empty => {
                self.status = "composer is empty".to_string();
                self.composer = text;
            }
            ClassifiedInput::Prompt(prompt) => self.start_prompt_turn(prompt, backend_tx),
            ClassifiedInput::RuntimeControl(control) => {
                self.start_prompt_turn(control.raw, backend_tx);
            }
            ClassifiedInput::LionClawControl(control) => {
                self.handle_lionclaw_control(control, backend_tx);
            }
        }
    }

    fn start_prompt_turn(
        &mut self,
        prompt: String,
        backend_tx: &mpsc::UnboundedSender<BackendEvent>,
    ) {
        let Some(ready) = self.ready_instance() else {
            self.status = "launch is blocked for the selected instance".to_string();
            return;
        };
        let kernel = ready.kernel.clone();
        let session_id = ready.session_id;
        let runtime_id = ready.runtime_id.clone();
        let instance_name = ready.summary.display_name().to_string();

        self.transcript.push(TranscriptLine::new(
            TranscriptLineKind::User,
            prompt.clone(),
        ));
        self.status = format!("running turn on {instance_name}");
        let (handle, cancel_tx) = spawn_streamed_turn(
            kernel,
            session_id,
            runtime_id,
            StreamedSubmission::Prompt(prompt),
            backend_tx.clone(),
        );
        self.active_turn = Some(handle);
        self.active_turn_cancel = Some(cancel_tx);
    }

    fn handle_lionclaw_control(
        &mut self,
        control: LionClawControlInput,
        backend_tx: &mpsc::UnboundedSender<BackendEvent>,
    ) {
        match control.command_name.as_str() {
            "exit" | "quit" => {
                if self.active() {
                    self.status = "finish the active turn before exiting".to_string();
                } else {
                    self.should_quit = true;
                }
            }
            "continue" => self.start_session_action(
                "/lionclaw continue",
                SessionActionKind::ContinueLastPartial,
                backend_tx,
            ),
            "retry" => self.start_session_action(
                "/lionclaw retry",
                SessionActionKind::RetryLastTurn,
                backend_tx,
            ),
            "reset" => self.start_reset(backend_tx),
            "" => {
                self.transcript.push(TranscriptLine::new(
                    TranscriptLineKind::Error,
                    "missing LionClaw command",
                ));
            }
            other => {
                self.transcript.push(TranscriptLine::new(
                    TranscriptLineKind::Error,
                    format!("unknown LionClaw command: {other}"),
                ));
            }
        }
    }

    fn start_session_action(
        &mut self,
        label: &str,
        action: SessionActionKind,
        backend_tx: &mpsc::UnboundedSender<BackendEvent>,
    ) {
        let Some(ready) = self.ready_instance() else {
            self.status = "launch is blocked for the selected instance".to_string();
            return;
        };
        let kernel = ready.kernel.clone();
        let session_id = ready.session_id;
        let runtime_id = ready.runtime_id.clone();

        self.transcript
            .push(TranscriptLine::new(TranscriptLineKind::User, label));
        self.status = format!("running {label}");
        let (handle, cancel_tx) = spawn_streamed_turn(
            kernel,
            session_id,
            runtime_id,
            StreamedSubmission::Action(action),
            backend_tx.clone(),
        );
        self.active_turn = Some(handle);
        self.active_turn_cancel = Some(cancel_tx);
    }

    fn start_reset(&mut self, backend_tx: &mpsc::UnboundedSender<BackendEvent>) {
        let Some(ready) = self.ready_instance() else {
            self.status = "launch is blocked for the selected instance".to_string();
            return;
        };
        let kernel = ready.kernel.clone();
        let session_id = ready.session_id;

        self.transcript.push(TranscriptLine::new(
            TranscriptLineKind::User,
            "/lionclaw reset",
        ));
        self.status = "resetting session".to_string();
        let backend_tx = backend_tx.clone();
        self.active_turn = Some(tokio::spawn(async move {
            let result = kernel
                .session_action(SessionActionRequest {
                    session_id,
                    action: SessionActionKind::ResetSession,
                })
                .await
                .map(|response| response.session_id)
                .map_err(|err| err.to_string());
            let _ = backend_tx.send(BackendEvent::SessionReset(result));
        }));
    }

    fn ready_instance(&self) -> Option<&ReadyInstance> {
        match &self.selected {
            SelectedInstanceState::Ready(ready) => Some(ready),
            SelectedInstanceState::Blocked { .. } => None,
        }
    }

    fn apply_backend_event(&mut self, event: BackendEvent) {
        match event {
            BackendEvent::Stream(event) => {
                push_stream_event(&mut self.transcript, &event);
            }
            BackendEvent::TurnFinished(result) => {
                self.active_turn = None;
                self.active_turn_cancel = None;
                match result {
                    Ok(outcome) => {
                        if !outcome.answer_seen && !outcome.response.assistant_text.is_empty() {
                            self.transcript.push(TranscriptLine::new(
                                TranscriptLineKind::Answer,
                                outcome.response.assistant_text,
                            ));
                        }
                        self.status = "idle".to_string();
                    }
                    Err(message) => {
                        self.transcript
                            .push(TranscriptLine::new(TranscriptLineKind::Error, message));
                        self.status = "turn failed".to_string();
                    }
                }
            }
            BackendEvent::SessionReset(result) => {
                self.active_turn = None;
                self.active_turn_cancel = None;
                match result {
                    Ok(session_id) => {
                        if let SelectedInstanceState::Ready(ready) = &mut self.selected {
                            ready.session_id = session_id;
                        }
                        self.transcript.clear();
                        self.transcript.push(TranscriptLine::new(
                            TranscriptLineKind::Status,
                            "opened a fresh session",
                        ));
                        self.status = "idle".to_string();
                    }
                    Err(message) => {
                        self.transcript
                            .push(TranscriptLine::new(TranscriptLineKind::Error, message));
                        self.status = "reset failed".to_string();
                    }
                }
            }
        }
    }
}

fn resolve_console_instances(
    target: &TargetContext,
) -> Result<(Option<PathBuf>, Vec<InstanceSummary>, usize)> {
    if let Some(project_root) = target.project_root.as_ref() {
        let entries = list_project_instance_statuses(project_root)?;
        let instances = entries
            .into_iter()
            .map(|entry| InstanceSummary {
                name: Some(entry.name),
                is_default: entry.is_default,
                home: entry.home,
                work_root: entry.work_root,
                work_root_finding: entry.work_root_finding,
                shared_work_root_count: entry.shared_work_root_count,
            })
            .collect::<Vec<_>>();
        if instances.is_empty() {
            return Err(anyhow!(
                "project has no instances; run lionclaw instance create main"
            ));
        }

        let selected_name = target.instance_name.as_deref();
        let selected_index = instances
            .iter()
            .position(|entry| entry.name.as_deref() == selected_name)
            .or_else(|| instances.iter().position(|entry| entry.is_default))
            .unwrap_or(0);
        Ok((Some(project_root.clone()), instances, selected_index))
    } else {
        let inspection = inspect_target_work_root(target);
        let summary = InstanceSummary {
            name: target.instance_name.clone(),
            is_default: false,
            home: target.instance_home.root().to_path_buf(),
            work_root: inspection.work_root,
            work_root_finding: inspection.finding,
            shared_work_root_count: 0,
        };
        Ok((None, vec![summary], 0))
    }
}

async fn open_selected_instance(
    summary: InstanceSummary,
    requested_runtime: Option<String>,
    continue_last_session: bool,
    timeout_override: Option<RuntimeTurnTimeouts>,
) -> SelectedInstanceState {
    match try_open_selected_instance(
        summary.clone(),
        requested_runtime,
        continue_last_session,
        timeout_override,
    )
    .await
    {
        Ok(ready) => SelectedInstanceState::Ready(Box::new(ready)),
        Err(err) => SelectedInstanceState::Blocked {
            blocker: LaunchBlocker::for_instance(summary.display_name(), err.to_string()),
            summary,
        },
    }
}

async fn try_open_selected_instance(
    summary: InstanceSummary,
    requested_runtime: Option<String>,
    continue_last_session: bool,
    timeout_override: Option<RuntimeTurnTimeouts>,
) -> Result<ReadyInstance> {
    if let Some(finding) = summary.work_root_finding.as_deref() {
        return Err(anyhow!(finding.to_string()));
    }
    let work_root = summary
        .work_root
        .as_deref()
        .ok_or_else(|| anyhow!("instance does not have a resolved work root"))?;
    let home = LionClawHome::new(summary.home.clone());
    let config = OperatorConfig::load(&home).await?;
    let runtime_id = resolve_run_runtime_id(
        &config,
        requested_runtime.as_deref(),
        summary.display_name(),
    )?;
    validate_runtime_launch_prerequisites(&home, &config, &runtime_id).await?;
    render_runtime_cache_for_work_root(&home, &config, &runtime_id, work_root).await?;
    let effective_timeouts = timeout_override.unwrap_or_else(RuntimeTurnTimeouts::interactive);
    let kernel = open_runtime_kernel_for_work_root(
        &home,
        &config,
        Some(runtime_id.clone()),
        work_root,
        Some(effective_timeouts),
    )
    .await?;
    let peer_id = local_peer_id_for_project(work_root);
    let session_id = resolve_repl_session(&kernel, &peer_id, continue_last_session)
        .await
        .map_err(kernel_to_anyhow)?
        .session_id;
    let runtime_kind = config
        .runtime(&runtime_id)
        .map(|runtime| runtime.kind().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let boundary = resolve_boundary_summary(&config, effective_timeouts)?;

    Ok(ReadyInstance {
        summary,
        runtime_id,
        runtime_kind,
        runtime_override: requested_runtime,
        boundary,
        kernel,
        session_id,
    })
}

fn resolve_boundary_summary(
    config: &OperatorConfig,
    timeouts: RuntimeTurnTimeouts,
) -> Result<BoundarySummary> {
    let (preset_name, preset) = resolve_execution_preset(
        ExecutionPlanPurpose::Interactive,
        None,
        config.defaults.preset.as_deref(),
        &config.presets,
    )
    .map_err(|err| anyhow!(err))?;
    Ok(BoundarySummary {
        workspace: preset.workspace_access.as_str().to_string(),
        network: preset.network_mode.as_str().to_string(),
        secrets: if preset.mount_runtime_secrets {
            "on".to_string()
        } else {
            "off".to_string()
        },
        timeout: format!(
            "quiet {}, max {}",
            crate::runtime_timeouts::format_duration(timeouts.idle),
            crate::runtime_timeouts::format_duration(timeouts.hard)
        ),
        preset: preset_name,
    })
}

fn push_history_turn(transcript: &mut Vec<TranscriptLine>, turn: &SessionTurnView) {
    transcript.push(TranscriptLine::new(
        TranscriptLineKind::User,
        turn.display_user_text.clone(),
    ));
    if matches!(
        turn.status,
        SessionTurnStatus::TimedOut
            | SessionTurnStatus::Failed
            | SessionTurnStatus::Cancelled
            | SessionTurnStatus::Interrupted
    ) && !turn.assistant_text.trim().is_empty()
    {
        transcript.push(TranscriptLine::new(
            TranscriptLineKind::Status,
            partial_history_marker(turn.status),
        ));
    }
    if !turn.assistant_text.trim().is_empty() {
        transcript.push(TranscriptLine::new(
            TranscriptLineKind::Answer,
            turn.assistant_text.clone(),
        ));
    }
    if let Some(error_text) = turn.error_text.as_deref() {
        transcript.push(TranscriptLine::new(
            TranscriptLineKind::Error,
            error_text.to_string(),
        ));
    }
}

enum StreamedSubmission {
    Prompt(String),
    Action(SessionActionKind),
}

struct TurnOutcome {
    response: SessionTurnResponse,
    answer_seen: bool,
}

enum BackendEvent {
    Stream(StreamEventDto),
    TurnFinished(Result<TurnOutcome, String>),
    SessionReset(Result<Uuid, String>),
}

fn spawn_streamed_turn(
    kernel: Kernel,
    session_id: Uuid,
    runtime_id: String,
    submission: StreamedSubmission,
    backend_tx: mpsc::UnboundedSender<BackendEvent>,
) -> (JoinHandle<()>, oneshot::Sender<String>) {
    let (cancel_tx, cancel_rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        let (stream_tx, mut stream_rx) = mpsc::unbounded_channel();
        let mut answer_seen = false;
        match submission {
            StreamedSubmission::Prompt(prompt) => {
                let request = SessionTurnRequest {
                    session_id,
                    user_text: prompt,
                    runtime_id: Some(runtime_id),
                    runtime_working_dir: None,
                    runtime_timeout_ms: None,
                    runtime_env_passthrough: None,
                };
                let turn_future =
                    kernel.turn_session_streaming_cancellable(request, stream_tx, cancel_rx);
                tokio::pin!(turn_future);
                run_stream_future(
                    &mut turn_future,
                    &mut stream_rx,
                    &backend_tx,
                    &mut answer_seen,
                )
                .await;
            }
            StreamedSubmission::Action(action) => {
                let turn_future = kernel.run_session_action_streaming_cancellable(
                    session_id,
                    action,
                    Some(runtime_id),
                    stream_tx,
                    cancel_rx,
                );
                tokio::pin!(turn_future);
                run_stream_future(
                    &mut turn_future,
                    &mut stream_rx,
                    &backend_tx,
                    &mut answer_seen,
                )
                .await;
            }
        }
    });
    (handle, cancel_tx)
}

async fn run_stream_future<F>(
    turn_future: &mut std::pin::Pin<&mut F>,
    stream_rx: &mut mpsc::UnboundedReceiver<StreamEventDto>,
    backend_tx: &mpsc::UnboundedSender<BackendEvent>,
    answer_seen: &mut bool,
) where
    F: std::future::Future<Output = Result<SessionTurnResponse, crate::kernel::KernelError>>,
{
    let result = loop {
        tokio::select! {
            result = &mut *turn_future => {
                break result;
            }
            maybe_event = stream_rx.recv() => {
                let Some(event) = maybe_event else {
                    continue;
                };
                if is_answer_delta(&event) {
                    *answer_seen = true;
                }
                let _ = backend_tx.send(BackendEvent::Stream(event));
            }
        }
    };

    while let Ok(event) = stream_rx.try_recv() {
        if is_answer_delta(&event) {
            *answer_seen = true;
        }
        let _ = backend_tx.send(BackendEvent::Stream(event));
    }

    let result = result
        .map(|response| TurnOutcome {
            response,
            answer_seen: *answer_seen,
        })
        .map_err(|err| err.to_string());
    let _ = backend_tx.send(BackendEvent::TurnFinished(result));
}

fn is_answer_delta(event: &StreamEventDto) -> bool {
    matches!(
        (&event.kind, &event.lane),
        (
            StreamEventKindDto::MessageDelta,
            Some(StreamLaneDto::Answer)
        )
    ) && event.text.as_deref().is_some_and(|text| !text.is_empty())
}

pub(crate) fn push_stream_event(transcript: &mut Vec<TranscriptLine>, event: &StreamEventDto) {
    match (&event.kind, &event.lane, event.text.as_deref()) {
        (StreamEventKindDto::MessageDelta, Some(StreamLaneDto::Answer), Some(text)) => {
            append_transcript_delta(transcript, TranscriptLineKind::Answer, text);
        }
        (StreamEventKindDto::MessageDelta, Some(StreamLaneDto::Reasoning), Some(text)) => {
            append_transcript_delta(transcript, TranscriptLineKind::Reasoning, text);
        }
        (StreamEventKindDto::Status, _, Some(text)) => {
            transcript.push(TranscriptLine::new(TranscriptLineKind::Status, text));
        }
        (StreamEventKindDto::Error, _, Some(text)) => {
            transcript.push(TranscriptLine::new(TranscriptLineKind::Error, text));
        }
        (StreamEventKindDto::TurnCompleted, _, _)
        | (StreamEventKindDto::Done, _, _)
        | (_, _, None) => {}
        (_, _, Some(text)) => {
            transcript.push(TranscriptLine::new(TranscriptLineKind::Status, text));
        }
    }
}

fn append_transcript_delta(
    transcript: &mut Vec<TranscriptLine>,
    kind: TranscriptLineKind,
    text: &str,
) {
    if let Some(last) = transcript.last_mut() {
        if last.kind == kind {
            last.text.push_str(text);
            return;
        }
    }
    transcript.push(TranscriptLine::new(kind, text));
}

pub(crate) async fn run_console(invocation: RunConsoleInvocation<'_>) -> Result<RunConsoleOutcome> {
    let app = ConsoleApp::load(invocation).await?;
    run_console_app(app).await
}

pub(crate) async fn run_launch_blocker(blocker: LaunchBlocker) -> Result<()> {
    let summary = InstanceSummary {
        name: None,
        is_default: false,
        home: PathBuf::new(),
        work_root: None,
        work_root_finding: Some(blocker.detail.clone()),
        shared_work_root_count: 0,
    };
    let app = ConsoleApp {
        project_root: None,
        instances: vec![summary.clone()],
        selected_index: 0,
        selected: SelectedInstanceState::Blocked { summary, blocker },
        continue_last_session: false,
        timeout_override: None,
        focus: Focus::Composer,
        overlay: None,
        composer: String::new(),
        transcript: Vec::new(),
        transcript_scroll: 0,
        status: "launch blocked".to_string(),
        active_turn: None,
        active_turn_cancel: None,
        saw_ready_instance: false,
        should_quit: false,
    };
    run_console_app(app).await.map(|_| ())
}

async fn run_console_app(mut app: ConsoleApp) -> Result<RunConsoleOutcome> {
    let mut terminal = enter_terminal()?;
    let (backend_tx, mut backend_rx) = mpsc::unbounded_channel();
    let result = run_terminal_loop(&mut terminal, &mut app, &backend_tx, &mut backend_rx).await;
    let cleanup_result = leave_terminal(terminal);
    if let Some(handle) = app.active_turn.take() {
        handle.abort();
    }
    result?;
    cleanup_result?;
    Ok(RunConsoleOutcome {
        launch_blocked: !app.saw_ready_instance,
    })
}

type ConsoleTerminal = Terminal<CrosstermBackend<Stdout>>;

fn enter_terminal() -> Result<ConsoleTerminal> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    Terminal::new(backend).map_err(Into::into)
}

fn leave_terminal(mut terminal: ConsoleTerminal) -> Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

async fn run_terminal_loop(
    terminal: &mut ConsoleTerminal,
    app: &mut ConsoleApp,
    backend_tx: &mpsc::UnboundedSender<BackendEvent>,
    backend_rx: &mut mpsc::UnboundedReceiver<BackendEvent>,
) -> Result<()> {
    loop {
        while let Ok(event) = backend_rx.try_recv() {
            app.apply_backend_event(event);
        }
        terminal.draw(|frame| render_app(frame, app))?;
        if app.should_quit {
            break;
        }

        if event::poll(EVENT_POLL)? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    handle_key(app, key, backend_tx).await;
                }
            }
        }
    }

    Ok(())
}

async fn handle_key(
    app: &mut ConsoleApp,
    key: KeyEvent,
    backend_tx: &mpsc::UnboundedSender<BackendEvent>,
) {
    if app.overlay.is_some() {
        handle_overlay_key(app, key);
        return;
    }

    match (key.code, key.modifiers) {
        (KeyCode::Char('?'), KeyModifiers::NONE) => app.overlay = Some(Overlay::Help),
        (KeyCode::Char('p'), KeyModifiers::CONTROL) => app.overlay = Some(Overlay::Palette),
        (KeyCode::Tab, _) => app.focus = app.focus.next(app.project_mode()),
        (KeyCode::BackTab, _) => app.focus = app.focus.previous(app.project_mode()),
        (KeyCode::Esc, _) => {
            app.composer.clear();
            app.status = "composer cleared".to_string();
        }
        (KeyCode::Enter, KeyModifiers::ALT) | (KeyCode::Char('j'), KeyModifiers::CONTROL) => {
            app.composer.push('\n');
        }
        (KeyCode::Enter, _) => app.submit_composer(backend_tx),
        (KeyCode::Char('d'), KeyModifiers::CONTROL) | (KeyCode::F(10), _) => {
            if app.active() {
                app.status = "finish the active turn before exiting".to_string();
            } else {
                app.should_quit = true;
            }
        }
        (KeyCode::Char('c'), KeyModifiers::CONTROL) => {
            if app.active() {
                if let Some(cancel_tx) = app.active_turn_cancel.take() {
                    let _ = cancel_tx.send("turn interrupted from operator console".to_string());
                    app.status = "interrupt requested".to_string();
                } else {
                    app.status = "interrupt already requested".to_string();
                }
            } else {
                app.overlay = Some(Overlay::ExitConfirm);
            }
        }
        (KeyCode::Up, _) if app.focus == Focus::Instances => {
            let next = app.selected_index.saturating_sub(1);
            app.switch_selected(next).await;
        }
        (KeyCode::Down, _) if app.focus == Focus::Instances => {
            let next = app.selected_index.saturating_add(1);
            app.switch_selected(next).await;
        }
        (KeyCode::Up, _) | (KeyCode::PageUp, _) if app.focus == Focus::Transcript => {
            app.transcript_scroll = app.transcript_scroll.saturating_sub(1);
        }
        (KeyCode::Down, _) | (KeyCode::PageDown, _) if app.focus == Focus::Transcript => {
            app.transcript_scroll = app.transcript_scroll.saturating_add(1);
        }
        (KeyCode::Backspace, _) => {
            app.composer.pop();
        }
        (KeyCode::Char('u'), KeyModifiers::CONTROL) => app.composer.clear(),
        (KeyCode::Char(ch), KeyModifiers::NONE | KeyModifiers::SHIFT) => app.composer.push(ch),
        _ => {}
    }
}

fn handle_overlay_key(app: &mut ConsoleApp, key: KeyEvent) {
    match (app.overlay, key.code, key.modifiers) {
        (Some(Overlay::ExitConfirm), KeyCode::Char('y'), KeyModifiers::NONE) => {
            app.should_quit = true;
        }
        (_, KeyCode::Esc | KeyCode::Enter, _) => app.overlay = None,
        (_, KeyCode::Char('c'), KeyModifiers::CONTROL) => app.overlay = None,
        (_, _, _) => {}
    }
}

pub(crate) fn render_app(frame: &mut Frame<'_>, app: &ConsoleApp) {
    let area = frame.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),
            Constraint::Min(10),
            Constraint::Length(composer_height(app)),
            Constraint::Length(1),
        ])
        .split(area);

    render_header(frame, chunks[0], app);
    render_body(frame, chunks[1], app);
    render_composer(frame, chunks[2], app);
    render_footer(frame, chunks[3], app);

    if let Some(overlay) = app.overlay {
        render_overlay(frame, area, app, overlay);
    }
}

fn composer_height(app: &ConsoleApp) -> u16 {
    let line_count = app.composer.lines().count().max(1) as u16;
    line_count.saturating_add(2).clamp(3, 8)
}

fn render_header(frame: &mut Frame<'_>, area: Rect, app: &ConsoleApp) {
    let (runtime, boundary) = match &app.selected {
        SelectedInstanceState::Ready(ready) => {
            let runtime = if ready.runtime_override.is_some() {
                format!("{} ({}, override)", ready.runtime_id, ready.runtime_kind)
            } else {
                format!("{} ({})", ready.runtime_id, ready.runtime_kind)
            };
            let boundary = format!(
                "workspace {} | network {} | secrets {} | timeout {} | preset {}",
                ready.boundary.workspace,
                ready.boundary.network,
                ready.boundary.secrets,
                ready.boundary.timeout,
                ready.boundary.preset
            );
            (runtime, boundary)
        }
        SelectedInstanceState::Blocked { blocker, .. } => {
            ("blocked".to_string(), blocker.title.clone())
        }
    };
    let work_root = app
        .selected
        .summary()
        .work_root
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "-".to_string());
    let title = format!(" LionClaw Operator Console | {} ", app.selected_name());
    let lines = vec![
        Line::from(vec![
            Span::styled("runtime ", Style::default().fg(Color::Gray)),
            Span::raw(runtime),
        ]),
        Line::from(vec![
            Span::styled("work root ", Style::default().fg(Color::Gray)),
            Span::raw(work_root),
        ]),
        Line::from(boundary),
    ];
    let paragraph = Paragraph::new(lines)
        .block(Block::default().title(title).borders(Borders::ALL))
        .wrap(Wrap { trim: true });
    frame.render_widget(paragraph, area);
}

fn render_body(frame: &mut Frame<'_>, area: Rect, app: &ConsoleApp) {
    if app.project_mode() {
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Length(30), Constraint::Min(20)])
            .split(area);
        render_instances(frame, chunks[0], app);
        render_main_panes(frame, chunks[1], app);
    } else {
        render_main_panes(frame, area, app);
    }
}

fn render_instances(frame: &mut Frame<'_>, area: Rect, app: &ConsoleApp) {
    let items = app
        .instances
        .iter()
        .enumerate()
        .map(|(index, instance)| {
            let marker = if index == app.selected_index {
                ">"
            } else {
                " "
            };
            let default = if instance.is_default { "*" } else { " " };
            let blocked = if instance.work_root_finding.is_some() {
                " !"
            } else {
                ""
            };
            let shared = if instance.shared_work_root_count > 1 {
                format!(" [{}]", instance.shared_work_root_count)
            } else {
                String::new()
            };
            let line = Line::from(format!(
                "{marker}{default} {}{blocked}{shared}",
                instance.display_name()
            ));
            if index == app.selected_index {
                ListItem::new(line.style(Style::default().fg(Color::Yellow)))
            } else {
                ListItem::new(line)
            }
        })
        .collect::<Vec<_>>();
    let title = if app.focus == Focus::Instances {
        " Instances [focus] "
    } else {
        " Instances "
    };
    let list = List::new(items).block(Block::default().title(title).borders(Borders::ALL));
    frame.render_widget(list, area);
}

fn render_main_panes(frame: &mut Frame<'_>, area: Rect, app: &ConsoleApp) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(8), Constraint::Length(7)])
        .split(area);
    render_transcript(frame, chunks[0], app);
    render_inspectors(frame, chunks[1], app);
}

fn render_transcript(frame: &mut Frame<'_>, area: Rect, app: &ConsoleApp) {
    let title = if app.focus == Focus::Transcript {
        " Transcript [focus] "
    } else {
        " Transcript "
    };
    if let SelectedInstanceState::Blocked { blocker, .. } = &app.selected {
        let text = Text::from(vec![
            Line::styled(&blocker.title, Style::default().fg(Color::Red)),
            Line::raw(""),
            Line::raw(&blocker.detail),
            Line::raw(""),
            Line::raw(&blocker.suggestion),
        ]);
        let paragraph = Paragraph::new(text)
            .block(
                Block::default()
                    .title(" Launch Blocker ")
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: false });
        frame.render_widget(paragraph, area);
        return;
    }

    let lines = if app.transcript.is_empty() {
        vec![Line::styled(
            "No turns yet.",
            Style::default().fg(Color::DarkGray),
        )]
    } else {
        transcript_render_lines(&app.transcript)
    };
    let paragraph = Paragraph::new(lines)
        .block(Block::default().title(title).borders(Borders::ALL))
        .scroll((app.transcript_scroll, 0))
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, area);
}

pub(crate) fn transcript_render_lines(lines: &[TranscriptLine]) -> Vec<Line<'static>> {
    lines
        .iter()
        .flat_map(|line| {
            let (prefix, style) = match line.kind {
                TranscriptLineKind::User => (
                    "you> ",
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                TranscriptLineKind::Answer => ("lionclaw> ", Style::default().fg(Color::White)),
                TranscriptLineKind::Reasoning => ("thinking> ", Style::default().fg(Color::Blue)),
                TranscriptLineKind::Status => ("status> ", Style::default().fg(Color::Green)),
                TranscriptLineKind::Error => ("error> ", Style::default().fg(Color::Red)),
            };
            split_render_line(prefix, style, &line.text)
        })
        .collect()
}

fn split_render_line(prefix: &str, style: Style, text: &str) -> Vec<Line<'static>> {
    if text.is_empty() {
        return vec![Line::from(vec![Span::styled(prefix.to_string(), style)])];
    }
    text.lines()
        .map(|line| {
            Line::from(vec![
                Span::styled(prefix.to_string(), style),
                Span::raw(line.to_string()),
            ])
        })
        .collect()
}

fn render_inspectors(frame: &mut Frame<'_>, area: Rect, app: &ConsoleApp) {
    let title = if app.focus == Focus::Inspectors {
        " Inspectors [focus] "
    } else {
        " Inspectors "
    };
    let lines = match &app.selected {
        SelectedInstanceState::Ready(ready) => vec![
            Line::from(format!("session: {}", ready.session_id)),
            Line::from(format!(
                "runtime: {} kind={} override={}",
                ready.runtime_id,
                ready.runtime_kind,
                ready.runtime_override.as_deref().unwrap_or("-")
            )),
            Line::from(format!(
                "boundary: workspace={} network={} secrets={} preset={}",
                ready.boundary.workspace,
                ready.boundary.network,
                ready.boundary.secrets,
                ready.boundary.preset
            )),
            Line::from("drafts: runtime-private /drafts"),
            Line::from("audit: kernel-owned runtime plan events"),
        ],
        SelectedInstanceState::Blocked { blocker, .. } => vec![
            Line::from("session: -"),
            Line::from("runtime: blocked"),
            Line::from(format!("blocker: {}", blocker.detail)),
        ],
    };
    let paragraph = Paragraph::new(lines)
        .block(Block::default().title(title).borders(Borders::ALL))
        .wrap(Wrap { trim: true });
    frame.render_widget(paragraph, area);
}

fn render_composer(frame: &mut Frame<'_>, area: Rect, app: &ConsoleApp) {
    let title = if app.focus == Focus::Composer {
        " Composer [focus] "
    } else {
        " Composer "
    };
    let content = if app.composer.is_empty() {
        Text::from(Line::styled(
            "Enter submits. Alt+Enter inserts a newline.",
            Style::default().fg(Color::DarkGray),
        ))
    } else {
        Text::from(app.composer.clone())
    };
    let paragraph = Paragraph::new(content)
        .block(Block::default().title(title).borders(Borders::ALL))
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, area);
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, app: &ConsoleApp) {
    let active = if app.active() { "active" } else { "idle" };
    let text = format!(
        " {} | {} | ? help | Tab focus | Enter submit | Ctrl+D/F10 exit ",
        active, app.status
    );
    let paragraph = Paragraph::new(text)
        .alignment(Alignment::Left)
        .style(Style::default().fg(Color::Gray));
    frame.render_widget(paragraph, area);
}

fn render_overlay(frame: &mut Frame<'_>, area: Rect, app: &ConsoleApp, overlay: Overlay) {
    let popup = centered_rect(70, 55, area);
    frame.render_widget(Clear, popup);
    let (title, lines) = match overlay {
        Overlay::Help => (
            " Help ",
            vec![
                Line::from("? help"),
                Line::from("Ctrl+P command palette"),
                Line::from("Tab / Shift+Tab move focus"),
                Line::from("Enter submit prompt or /lionclaw command"),
                Line::from("Alt+Enter insert newline"),
                Line::from("Up / Down switch instances when instance panel has focus"),
                Line::from("Ctrl+D or F10 exit when idle"),
                Line::from("Esc closes this overlay"),
            ],
        ),
        Overlay::Palette => (
            " Command Palette ",
            vec![
                Line::from("/lionclaw continue"),
                Line::from("/lionclaw retry"),
                Line::from("/lionclaw reset"),
                Line::from("/lionclaw exit"),
                Line::from("Runtime slash commands pass through from the composer."),
            ],
        ),
        Overlay::ExitConfirm => (
            " Exit ",
            vec![
                Line::from("Press y to exit."),
                Line::from("Press Esc to return to the console."),
            ],
        ),
    };
    let paragraph = Paragraph::new(lines)
        .block(Block::default().title(title).borders(Borders::ALL))
        .wrap(Wrap { trim: true });
    frame.render_widget(paragraph, popup);
    let _ = app;
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1]);
    horizontal[1].inner(Margin {
        vertical: 0,
        horizontal: 1,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;

    #[test]
    fn stream_events_append_answer_status_and_errors() {
        let mut transcript = Vec::new();
        push_stream_event(
            &mut transcript,
            &StreamEventDto {
                kind: StreamEventKindDto::MessageDelta,
                lane: Some(StreamLaneDto::Answer),
                code: None,
                text: Some("hello".to_string()),
            },
        );
        push_stream_event(
            &mut transcript,
            &StreamEventDto {
                kind: StreamEventKindDto::MessageDelta,
                lane: Some(StreamLaneDto::Answer),
                code: None,
                text: Some(" world".to_string()),
            },
        );
        push_stream_event(
            &mut transcript,
            &StreamEventDto {
                kind: StreamEventKindDto::Status,
                lane: None,
                code: None,
                text: Some("done".to_string()),
            },
        );

        assert_eq!(
            transcript,
            vec![
                TranscriptLine::new(TranscriptLineKind::Answer, "hello world"),
                TranscriptLine::new(TranscriptLineKind::Status, "done"),
            ]
        );
    }

    #[test]
    fn transcript_rendering_preserves_routing_prefixes() {
        let lines = transcript_render_lines(&[
            TranscriptLine::new(TranscriptLineKind::User, "/compact"),
            TranscriptLine::new(TranscriptLineKind::Answer, "ok"),
            TranscriptLine::new(TranscriptLineKind::Error, "failed"),
        ]);
        let rendered = lines
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>();

        assert_eq!(
            rendered,
            vec!["you> /compact", "lionclaw> ok", "error> failed"]
        );
    }

    #[test]
    fn runtime_slash_commands_remain_passthrough_controls() {
        match classify_input("/compact now") {
            ClassifiedInput::RuntimeControl(control) => {
                assert_eq!(control.raw, "/compact now");
                assert_eq!(control.command_name, "compact");
            }
            other => panic!("expected runtime control, got {other:?}"),
        }

        match classify_input(" /compact now") {
            ClassifiedInput::Prompt(prompt) => assert_eq!(prompt, " /compact now"),
            other => panic!("expected prompt, got {other:?}"),
        }
    }

    #[test]
    fn blocked_instance_renders_launch_blocker() {
        let summary = InstanceSummary {
            name: Some("main".to_string()),
            is_default: true,
            home: PathBuf::from("/tmp/project/.lionclaw/instances/main"),
            work_root: None,
            work_root_finding: Some("missing runtime".to_string()),
            shared_work_root_count: 0,
        };
        let app = ConsoleApp {
            project_root: Some(PathBuf::from("/tmp/project")),
            instances: vec![summary.clone()],
            selected_index: 0,
            selected: SelectedInstanceState::Blocked {
                summary,
                blocker: LaunchBlocker::for_instance("main", "missing runtime"),
            },
            continue_last_session: false,
            timeout_override: None,
            focus: Focus::Transcript,
            overlay: None,
            composer: String::new(),
            transcript: Vec::new(),
            transcript_scroll: 0,
            status: "launch blocked".to_string(),
            active_turn: None,
            active_turn_cancel: None,
            saw_ready_instance: false,
            should_quit: false,
        };

        let rendered = render_to_text(&app, 100, 30);
        assert!(rendered.contains("Launch Blocker"));
        assert!(rendered.contains("Launch blocked for main"));
        assert!(rendered.contains("missing runtime"));
    }

    #[test]
    fn project_mode_lists_instances_and_selects_requested_instance() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let project = crate::operator::target::init_project(temp_dir.path()).expect("init");
        crate::operator::target::create_project_instance(
            &project.project_root,
            "reviewer",
            Some(project.project_root.as_path()),
            false,
        )
        .expect("create reviewer");
        let target = crate::operator::target::resolve_target(
            &crate::operator::target::TargetSelection {
                home: None,
                project: Some(project.project_root.clone()),
                instance: Some("reviewer".to_string()),
            },
            crate::operator::target::WorkRootRequirement::Optional,
        )
        .expect("resolve target");

        let (project_root, instances, selected_index) =
            resolve_console_instances(&target).expect("instances");

        assert_eq!(
            project_root.as_deref(),
            Some(project.project_root.as_path())
        );
        assert_eq!(instances.len(), 2);
        assert_eq!(
            instances
                .get(selected_index)
                .and_then(|instance| instance.name.as_deref()),
            Some("reviewer")
        );
        assert!(instances
            .iter()
            .any(|instance| instance.name.as_deref() == Some("main")));
    }

    #[test]
    fn instance_switching_is_blocked_while_turn_is_active() {
        let mut app = blocked_test_app();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        runtime.block_on(async {
            app.active_turn = Some(tokio::spawn(async {}));
            app.switch_selected(1).await;
        });

        assert_eq!(app.selected_index, 0);
        assert!(app.status.contains("finish the active turn"));
        if let Some(handle) = app.active_turn.take() {
            handle.abort();
        }
    }

    #[test]
    fn ctrl_c_requests_active_turn_interrupt() {
        let mut app = blocked_test_app();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let (backend_tx, _backend_rx) = mpsc::unbounded_channel();
        let (cancel_tx, cancel_rx) = oneshot::channel();

        runtime.block_on(async {
            app.active_turn = Some(tokio::spawn(std::future::pending()));
            app.active_turn_cancel = Some(cancel_tx);
            handle_key(
                &mut app,
                KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL),
                &backend_tx,
            )
            .await;
            let reason = cancel_rx.await.expect("cancel reason");
            assert_eq!(reason, "turn interrupted from operator console");
        });

        assert_eq!(app.status, "interrupt requested");
        assert!(app.active_turn_cancel.is_none());
        if let Some(handle) = app.active_turn.take() {
            handle.abort();
        }
    }

    fn blocked_test_app() -> ConsoleApp {
        let main = InstanceSummary {
            name: Some("main".to_string()),
            is_default: true,
            home: PathBuf::from("/tmp/main"),
            work_root: None,
            work_root_finding: Some("blocked".to_string()),
            shared_work_root_count: 0,
        };
        let reviewer = InstanceSummary {
            name: Some("reviewer".to_string()),
            is_default: false,
            home: PathBuf::from("/tmp/reviewer"),
            work_root: None,
            work_root_finding: Some("blocked".to_string()),
            shared_work_root_count: 0,
        };
        ConsoleApp {
            project_root: Some(PathBuf::from("/tmp/project")),
            instances: vec![main.clone(), reviewer],
            selected_index: 0,
            selected: SelectedInstanceState::Blocked {
                summary: main,
                blocker: LaunchBlocker::for_instance("main", "blocked"),
            },
            continue_last_session: false,
            timeout_override: None,
            focus: Focus::Instances,
            overlay: None,
            composer: String::new(),
            transcript: Vec::new(),
            transcript_scroll: 0,
            status: "idle".to_string(),
            active_turn: None,
            active_turn_cancel: None,
            saw_ready_instance: false,
            should_quit: false,
        }
    }

    fn render_to_text(app: &ConsoleApp, width: u16, height: u16) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).expect("terminal");
        terminal
            .draw(|frame| render_app(frame, app))
            .expect("render");
        let buffer = terminal.backend().buffer();
        let area = buffer.area;
        let mut text = String::new();
        for y in area.y..area.y + area.height {
            for x in area.x..area.x + area.width {
                text.push_str(buffer[(x, y)].symbol());
            }
            text.push('\n');
        }
        text
    }
}
