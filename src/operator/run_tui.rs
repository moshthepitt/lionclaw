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
    layout::{Constraint, Direction, Layout, Margin, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Clear, Paragraph, Wrap},
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
const PANEL_BORDER: Color = Color::Rgb(0, 205, 220);
const PANEL_MUTED: Color = Color::Rgb(128, 132, 142);
const PANEL_SELECTED: Color = Color::Rgb(0, 68, 72);
const PANEL_READY: Color = Color::Rgb(91, 255, 112);
const PANEL_WARN: Color = Color::Rgb(255, 198, 55);
const PANEL_ERROR: Color = Color::Rgb(255, 82, 82);

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
    pub(crate) default_runtime: Option<String>,
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
        let (project_root, instances, selected_index) = resolve_console_instances(target).await?;
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

    fn project_label(&self) -> String {
        self.project_root
            .as_ref()
            .and_then(|root| root.file_name())
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .map(ToString::to_string)
            .unwrap_or_else(|| "single".to_string())
    }

    fn runtime_label(&self) -> String {
        match &self.selected {
            SelectedInstanceState::Ready(ready) => {
                if ready.runtime_override.is_some() {
                    format!("{} override", ready.runtime_id)
                } else {
                    ready.runtime_id.clone()
                }
            }
            SelectedInstanceState::Blocked { summary, .. } => summary
                .default_runtime
                .clone()
                .unwrap_or_else(|| "blocked".to_string()),
        }
    }

    fn runtime_kind_label(&self) -> String {
        match &self.selected {
            SelectedInstanceState::Ready(ready) => ready.runtime_kind.clone(),
            SelectedInstanceState::Blocked { .. } => "-".to_string(),
        }
    }

    fn boundary_summary(&self) -> BoundarySummary {
        match &self.selected {
            SelectedInstanceState::Ready(ready) => ready.boundary.clone(),
            SelectedInstanceState::Blocked { .. } => BoundarySummary {
                workspace: "blocked".to_string(),
                network: "blocked".to_string(),
                secrets: "blocked".to_string(),
                timeout: "-".to_string(),
                preset: "-".to_string(),
            },
        }
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
            drop(backend_tx.send(BackendEvent::SessionReset(result)));
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

async fn resolve_console_instances(
    target: &TargetContext,
) -> Result<(Option<PathBuf>, Vec<InstanceSummary>, usize)> {
    if let Some(project_root) = target.project_root.as_ref() {
        let entries = list_project_instance_statuses(project_root)?;
        let mut instances = Vec::new();
        for entry in entries {
            let home = entry.home;
            let default_runtime = load_default_runtime(&home).await;
            instances.push(InstanceSummary {
                name: Some(entry.name),
                is_default: entry.is_default,
                home,
                work_root: entry.work_root,
                work_root_finding: entry.work_root_finding,
                shared_work_root_count: entry.shared_work_root_count,
                default_runtime,
            });
        }
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
            home: target.instance_home.root(),
            work_root: inspection.work_root,
            work_root_finding: inspection.finding,
            shared_work_root_count: 0,
            default_runtime: load_default_runtime(&target.instance_home.root()).await,
        };
        Ok((None, vec![summary], 0))
    }
}

async fn load_default_runtime(home: &std::path::Path) -> Option<String> {
    OperatorConfig::load(&LionClawHome::new(home.to_path_buf()))
        .await
        .ok()
        .and_then(|config| config.defaults.runtime)
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
            "staged".to_string()
        } else {
            "off".to_string()
        },
        timeout: if timeouts.idle == timeouts.hard {
            crate::runtime_timeouts::format_duration(timeouts.hard)
        } else {
            format!(
                "quiet {}, max {}",
                crate::runtime_timeouts::format_duration(timeouts.idle),
                crate::runtime_timeouts::format_duration(timeouts.hard)
            )
        },
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
                if backend_tx.send(BackendEvent::Stream(event)).is_err() {
                    return;
                }
            }
        }
    };

    while let Ok(event) = stream_rx.try_recv() {
        if is_answer_delta(&event) {
            *answer_seen = true;
        }
        if backend_tx.send(BackendEvent::Stream(event)).is_err() {
            return;
        }
    }

    let result = result
        .map(|response| TurnOutcome {
            response,
            answer_seen: *answer_seen,
        })
        .map_err(|err| err.to_string());
    drop(backend_tx.send(BackendEvent::TurnFinished(result)));
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
        default_runtime: None,
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
                    match cancel_tx.send("turn interrupted from operator console".to_string()) {
                        Ok(()) => app.status = "interrupt requested".to_string(),
                        Err(_) => app.status = "interrupt already completed".to_string(),
                    }
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
            Constraint::Length(3),
            Constraint::Length(1),
            Constraint::Min(10),
            Constraint::Length(1),
            Constraint::Length(6),
            Constraint::Length(1),
            Constraint::Length(3),
        ])
        .split(area);

    let [header_area, _, body_area, _, composer_area, _, footer_area] = chunks.as_ref() else {
        return;
    };

    render_header(frame, *header_area, app);
    render_body(frame, *body_area, app);
    render_composer(frame, *composer_area, app);
    render_footer(frame, *footer_area, app);

    if let Some(overlay) = app.overlay {
        render_overlay(frame, area, app, overlay);
    }
}

fn render_header(frame: &mut Frame<'_>, area: Rect, app: &ConsoleApp) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(PANEL_MUTED));
    frame.render_widget(block, area);
    if area.height < 3 || area.width < 4 {
        return;
    }

    let boundary = app.boundary_summary();
    let line = Line::from(vec![
        Span::styled(
            "LionClaw",
            Style::default()
                .fg(PANEL_BORDER)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("    project: ", Style::default().fg(PANEL_MUTED)),
        Span::styled(app.project_label(), Style::default().fg(Color::White)),
        Span::styled("    instance: ", Style::default().fg(PANEL_BORDER)),
        Span::styled(
            app.selected_name().to_string(),
            Style::default().fg(Color::White),
        ),
        Span::styled("    runtime: ", Style::default().fg(PANEL_BORDER)),
        Span::styled(app.runtime_label(), Style::default().fg(Color::White)),
        Span::styled("    net: ", Style::default().fg(PANEL_BORDER)),
        Span::styled(boundary.network, Style::default().fg(Color::White)),
        Span::styled("    secrets: ", Style::default().fg(PANEL_BORDER)),
        Span::styled(
            boundary.secrets,
            Style::default().fg(if app.selected.is_ready() {
                PANEL_WARN
            } else {
                PANEL_ERROR
            }),
        ),
        Span::styled("    /workspace: ", Style::default().fg(PANEL_BORDER)),
        Span::styled(boundary.workspace, Style::default().fg(Color::White)),
        Span::styled("    timeout: ", Style::default().fg(PANEL_BORDER)),
        Span::styled(boundary.timeout, Style::default().fg(Color::White)),
    ]);
    frame.render_widget(
        Paragraph::new(line),
        Rect {
            x: area.x.saturating_add(2),
            y: area.y.saturating_add(1),
            width: area.width.saturating_sub(4),
            height: 1,
        },
    );
}

fn render_body(frame: &mut Frame<'_>, area: Rect, app: &ConsoleApp) {
    if app.project_mode() {
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(44),
                Constraint::Length(1),
                Constraint::Min(40),
                Constraint::Length(1),
                Constraint::Length(44),
            ])
            .split(area);
        let [instances_area, _, transcript_area, _, boundary_area] = chunks.as_ref() else {
            render_transcript(frame, area, app);
            return;
        };
        render_instances(frame, *instances_area, app);
        render_transcript(frame, *transcript_area, app);
        render_boundary(frame, *boundary_area, app);
    } else {
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Min(40),
                Constraint::Length(1),
                Constraint::Length(44),
            ])
            .split(area);
        let [transcript_area, _, boundary_area] = chunks.as_ref() else {
            render_transcript(frame, area, app);
            return;
        };
        render_transcript(frame, *transcript_area, app);
        render_boundary(frame, *boundary_area, app);
    }
}

fn render_instances(frame: &mut Frame<'_>, area: Rect, app: &ConsoleApp) {
    let content = render_panel_shell(frame, area, "Instances", app.focus == Focus::Instances);
    if content.width < 8 || content.height < 2 {
        return;
    }

    let mut row_y = content.y.saturating_add(1);
    for (index, instance) in app.instances.iter().enumerate() {
        if row_y >= content.y.saturating_add(content.height.saturating_sub(7)) {
            break;
        }
        let selected = index == app.selected_index;
        let selected_blocked = selected && !app.selected.is_ready();
        let blocked = instance.work_root_finding.is_some() || selected_blocked;
        let icon = if blocked {
            "!"
        } else if selected && app.selected.is_ready() {
            "●"
        } else {
            "○"
        };
        let state = if blocked {
            "blocked"
        } else if selected && app.selected.is_ready() {
            "ready"
        } else {
            "idle"
        };
        let selected_runtime = app.runtime_label();
        let runtime = instance.default_runtime.as_deref().unwrap_or(if selected {
            selected_runtime.as_str()
        } else {
            "-"
        });
        let default_mark = if instance.is_default { " default" } else { "" };
        let shared = if instance.shared_work_root_count > 1 {
            format!(" [{}]", instance.shared_work_root_count)
        } else {
            String::new()
        };
        let row = format_instance_row(
            icon,
            instance.display_name(),
            runtime,
            &format!("{state}{default_mark}{shared}"),
            content.width as usize,
        );
        let style = instance_row_style(selected, state);
        frame.render_widget(
            Paragraph::new(Line::raw(row)).style(style),
            Rect {
                x: content.x,
                y: row_y,
                width: content.width,
                height: 1,
            },
        );
        row_y = row_y.saturating_add(2);
    }

    if content.height > 12 {
        let menu_top = content.y + content.height - 8;
        draw_horizontal_rule(frame, content.x, menu_top, content.width, PANEL_MUTED);
        render_menu_row(frame, content, menu_top + 1, "Sessions");
        render_menu_row(frame, content, menu_top + 2, "Drafts");
        render_menu_row(frame, content, menu_top + 3, "Audit");
        render_menu_row(frame, content, menu_top + 4, "Boundary");
        render_menu_row(frame, content, menu_top + 5, "Runtime");
    }
}

fn render_transcript(frame: &mut Frame<'_>, area: Rect, app: &ConsoleApp) {
    let content = render_panel_shell(frame, area, "Transcript", app.focus == Focus::Transcript);
    if let SelectedInstanceState::Blocked { blocker, .. } = &app.selected {
        let text = Text::from(vec![
            Line::styled(&blocker.title, Style::default().fg(PANEL_ERROR)),
            Line::raw(""),
            Line::raw(&blocker.detail),
            Line::raw(""),
            Line::raw(&blocker.suggestion),
        ]);
        frame.render_widget(
            Paragraph::new(text).wrap(Wrap { trim: false }),
            content.inner(Margin {
                vertical: 1,
                horizontal: 1,
            }),
        );
        return;
    }

    let lines = if app.transcript.is_empty() {
        vec![Line::styled(
            "No turns yet. Submit a prompt from the composer.",
            Style::default().fg(PANEL_MUTED),
        )]
    } else {
        transcript_render_lines(&app.transcript)
    };
    let activity_rows = 2;
    let transcript_area = Rect {
        x: content.x,
        y: content.y,
        width: content.width.saturating_sub(1),
        height: content.height.saturating_sub(activity_rows),
    };
    frame.render_widget(
        Paragraph::new(lines)
            .scroll((app.transcript_scroll, 0))
            .wrap(Wrap { trim: false }),
        transcript_area,
    );
    if content.height > activity_rows {
        let rule_y = content.y + content.height - activity_rows;
        draw_horizontal_rule(frame, content.x, rule_y, content.width, PANEL_MUTED);
        let activity = Line::from(vec![
            Span::styled("runtime activity", Style::default().fg(PANEL_BORDER)),
            Span::raw("  ▶  "),
            Span::styled(
                if app.active() { "streaming..." } else { "idle" },
                Style::default().fg(if app.active() {
                    PANEL_BORDER
                } else {
                    PANEL_MUTED
                }),
            ),
        ]);
        frame.render_widget(
            Paragraph::new(activity),
            Rect {
                x: content.x,
                y: rule_y.saturating_add(1),
                width: content.width,
                height: 1,
            },
        );
    }
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

fn render_boundary(frame: &mut Frame<'_>, area: Rect, app: &ConsoleApp) {
    let content = render_panel_shell(frame, area, "Boundary", app.focus == Focus::Inspectors);
    if content.width < 8 || content.height < 4 {
        return;
    }

    let boundary = app.boundary_summary();
    let mut lines = vec![
        section_line("▱", "Mounts"),
        Line::raw(""),
        kv_arrow_line("/workspace", "repo", &boundary.workspace),
        kv_arrow_line("/runtime", "session", "private"),
        kv_arrow_line("/drafts", "", "private"),
        kv_arrow_line("/lionclaw/skills", "", "ro"),
        Line::raw(""),
        section_line("◎", "Network"),
        Line::raw(""),
        Line::raw(format!("  {}", boundary.network)),
        Line::styled(
            format!("  runtime kind {}", app.runtime_kind_label()),
            Style::default().fg(PANEL_MUTED),
        ),
        Line::raw(""),
        section_line("⚿", "Secrets"),
        Line::raw(""),
        Line::from(vec![
            Span::raw("  runtime auth "),
            Span::styled(boundary.secrets.clone(), Style::default().fg(PANEL_WARN)),
        ]),
        Line::from(vec![
            Span::raw("  runtime-secrets.env "),
            Span::styled(
                if boundary.secrets == "staged" {
                    "not mounted"
                } else {
                    "off"
                },
                Style::default().fg(PANEL_WARN),
            ),
        ]),
        Line::raw(""),
        section_line("▣", "Audit"),
        Line::raw(""),
    ];
    if app.selected.is_ready() {
        lines.extend([
            check_line("runtime.plan.allow"),
            check_line("runtime.started"),
            check_line("session.turn.open"),
        ]);
    } else {
        lines.extend([
            Line::styled("  ! launch blocked", Style::default().fg(PANEL_ERROR)),
            Line::raw(format!("  {}", app.status)),
        ]);
    }

    frame.render_widget(
        Paragraph::new(lines).wrap(Wrap { trim: false }),
        content.inner(Margin {
            vertical: 1,
            horizontal: 1,
        }),
    );
}

fn render_composer(frame: &mut Frame<'_>, area: Rect, app: &ConsoleApp) {
    let border_style = if app.focus == Focus::Composer {
        Style::default().fg(PANEL_BORDER)
    } else {
        Style::default().fg(PANEL_MUTED)
    };
    frame.render_widget(
        Block::default()
            .borders(Borders::ALL)
            .border_style(border_style),
        area,
    );
    if area.width < 4 || area.height < 4 {
        return;
    }

    let prompt = if app.composer.is_empty() {
        Line::from(vec![
            Span::styled(">  ", Style::default().fg(PANEL_BORDER)),
            Span::styled(
                "Ask through the selected runtime...",
                Style::default().fg(PANEL_MUTED),
            ),
        ])
    } else {
        Line::from(vec![
            Span::styled(">  ", Style::default().fg(PANEL_BORDER)),
            Span::raw(app.composer.clone()),
        ])
    };
    frame.render_widget(
        Paragraph::new(prompt).wrap(Wrap { trim: false }),
        Rect {
            x: area.x.saturating_add(2),
            y: area.y.saturating_add(1),
            width: area.width.saturating_sub(4),
            height: 2,
        },
    );

    let rule_y = area.y + area.height - 3;
    draw_horizontal_rule(
        frame,
        area.x.saturating_add(1),
        rule_y,
        area.width.saturating_sub(2),
        PANEL_MUTED,
    );
    let mode = if app.active() { "running" } else { "normal" };
    let status = Line::from(vec![
        Span::styled(mode, Style::default().fg(PANEL_BORDER)),
        Span::styled("   |   instance: ", Style::default().fg(PANEL_MUTED)),
        Span::raw(app.selected_name().to_string()),
        Span::styled(
            "   |   runtime controls pass through",
            Style::default().fg(PANEL_MUTED),
        ),
    ]);
    frame.render_widget(
        Paragraph::new(status),
        Rect {
            x: area.x.saturating_add(2),
            y: rule_y.saturating_add(1),
            width: area.width.saturating_sub(4),
            height: 1,
        },
    );
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, app: &ConsoleApp) {
    frame.render_widget(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(PANEL_MUTED)),
        area,
    );
    if area.height < 3 || area.width < 4 {
        return;
    }
    let line = Line::from(vec![
        key_span("?"),
        Span::raw(" Help     |     "),
        key_span("Ctrl+P"),
        Span::raw(" Palette     |     "),
        key_span("Tab"),
        Span::raw(" Focus     |     "),
        key_span("Enter"),
        Span::raw(" Submit     |     "),
        key_span("Ctrl+C"),
        Span::raw(" Interrupt     |     "),
        key_span("F10"),
        Span::raw(" Exit"),
        Span::styled(
            format!("     {}", app.status),
            Style::default().fg(PANEL_MUTED),
        ),
    ]);
    frame.render_widget(
        Paragraph::new(line),
        Rect {
            x: area.x.saturating_add(2),
            y: area.y.saturating_add(1),
            width: area.width.saturating_sub(4),
            height: 1,
        },
    );
}

fn render_panel_shell(frame: &mut Frame<'_>, area: Rect, title: &str, focused: bool) -> Rect {
    let _ = focused;
    let border_style = Style::default().fg(PANEL_BORDER);
    frame.render_widget(
        Block::default()
            .borders(Borders::ALL)
            .border_style(border_style),
        area,
    );
    if area.width > 4 && area.height > 3 {
        frame.render_widget(
            Paragraph::new(Line::styled(
                title.to_string(),
                Style::default()
                    .fg(PANEL_BORDER)
                    .add_modifier(Modifier::BOLD),
            )),
            Rect {
                x: area.x.saturating_add(2),
                y: area.y.saturating_add(1),
                width: area.width.saturating_sub(4),
                height: 1,
            },
        );
        draw_horizontal_rule(
            frame,
            area.x.saturating_add(1),
            area.y.saturating_add(2),
            area.width.saturating_sub(2),
            PANEL_MUTED,
        );
    }
    Rect {
        x: area.x.saturating_add(1),
        y: area.y.saturating_add(3),
        width: area.width.saturating_sub(2),
        height: area.height.saturating_sub(4),
    }
}

fn draw_horizontal_rule(frame: &mut Frame<'_>, x: u16, y: u16, width: u16, color: Color) {
    if width == 0 {
        return;
    }
    frame.render_widget(
        Paragraph::new("─".repeat(width as usize)).style(Style::default().fg(color)),
        Rect {
            x,
            y,
            width,
            height: 1,
        },
    );
}

fn format_instance_row(icon: &str, name: &str, runtime: &str, state: &str, width: usize) -> String {
    let name_width = 11.min(width.saturating_sub(18)).max(4);
    let runtime_width = 10.min(width.saturating_sub(name_width + 8)).max(3);
    let state_width = width.saturating_sub(name_width + runtime_width + 6).max(3);
    let mut row = format!(
        " {icon} {:name_width$} {:runtime_width$} {:>state_width$}",
        truncate_to(name, name_width),
        truncate_to(runtime, runtime_width),
        truncate_to(state, state_width),
    );
    if row.chars().count() > width {
        row = truncate_to(&row, width);
    }
    while row.chars().count() < width {
        row.push(' ');
    }
    row
}

fn instance_row_style(selected: bool, state: &str) -> Style {
    let fg = match state {
        "ready" => PANEL_READY,
        "blocked" => PANEL_ERROR,
        _ => Color::White,
    };
    let style = Style::default().fg(fg);
    if selected {
        style.bg(PANEL_SELECTED)
    } else {
        style
    }
}

fn render_menu_row(frame: &mut Frame<'_>, content: Rect, y: u16, label: &str) {
    if y >= content.y + content.height {
        return;
    }
    let line = format!(
        "   {:<width$}>",
        label,
        width = content.width.saturating_sub(6) as usize
    );
    frame.render_widget(
        Paragraph::new(Line::styled(line, Style::default().fg(Color::White))),
        Rect {
            x: content.x,
            y,
            width: content.width,
            height: 1,
        },
    );
}

fn section_line(icon: &'static str, label: &'static str) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{icon}  "), Style::default().fg(PANEL_BORDER)),
        Span::styled(
            label,
            Style::default()
                .fg(PANEL_BORDER)
                .add_modifier(Modifier::BOLD),
        ),
    ])
}

fn kv_arrow_line(left: &'static str, middle: &'static str, right: &str) -> Line<'static> {
    let target = if middle.is_empty() {
        right.to_string()
    } else {
        format!("{middle} {right}")
    };
    Line::from(vec![
        Span::raw(format!("  {left:<18}")),
        Span::styled("->  ", Style::default().fg(PANEL_BORDER)),
        Span::raw(target),
    ])
}

fn check_line(label: &'static str) -> Line<'static> {
    Line::from(vec![
        Span::styled("  ✓ ", Style::default().fg(PANEL_READY)),
        Span::raw(label),
    ])
}

fn key_span(key: &'static str) -> Span<'static> {
    Span::styled(
        key,
        Style::default()
            .fg(PANEL_BORDER)
            .add_modifier(Modifier::BOLD),
    )
}

fn truncate_to(value: &str, width: usize) -> String {
    value.chars().take(width).collect()
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
    let [_, vertical_area, _] = vertical.as_ref() else {
        return area;
    };
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(*vertical_area);
    let [_, horizontal_area, _] = horizontal.as_ref() else {
        return area;
    };
    horizontal_area.inner(Margin {
        vertical: 0,
        horizontal: 1,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::KernelOptions;
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
            default_runtime: None,
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
        assert!(rendered.contains("Transcript"));
        assert!(rendered.contains("Launch blocked for main"));
        assert!(rendered.contains("missing runtime"));
    }

    #[tokio::test]
    async fn mockup_sized_layout_renders_ribbon_three_panes_composer_and_footer() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let kernel = Kernel::new_with_options(
            &temp_dir.path().join("lionclaw.db"),
            KernelOptions::default(),
        )
        .await
        .expect("kernel");
        let main = InstanceSummary {
            name: Some("main".to_string()),
            is_default: true,
            home: temp_dir.path().join("instances/main"),
            work_root: Some(temp_dir.path().join("repo")),
            work_root_finding: None,
            shared_work_root_count: 0,
            default_runtime: Some("codex".to_string()),
        };
        let reviewer = InstanceSummary {
            name: Some("reviewer".to_string()),
            is_default: false,
            home: temp_dir.path().join("instances/reviewer"),
            work_root: Some(temp_dir.path().join("repo")),
            work_root_finding: None,
            shared_work_root_count: 2,
            default_runtime: Some("opencode".to_string()),
        };
        let qa = InstanceSummary {
            name: Some("qa".to_string()),
            is_default: false,
            home: temp_dir.path().join("instances/qa"),
            work_root: None,
            work_root_finding: Some("missing runtime".to_string()),
            shared_work_root_count: 0,
            default_runtime: Some("mock".to_string()),
        };
        let app = ConsoleApp {
            project_root: Some(PathBuf::from("/workspace/lionclaw")),
            instances: vec![main.clone(), reviewer, qa],
            selected_index: 0,
            selected: SelectedInstanceState::Ready(Box::new(ReadyInstance {
                summary: main,
                runtime_id: "codex".to_string(),
                runtime_kind: "codex".to_string(),
                runtime_override: None,
                boundary: BoundarySummary {
                    workspace: "rw".to_string(),
                    network: "none".to_string(),
                    secrets: "staged".to_string(),
                    timeout: "2h".to_string(),
                    preset: "everyday".to_string(),
                },
                kernel,
                session_id: Uuid::new_v4(),
            })),
            continue_last_session: false,
            timeout_override: None,
            focus: Focus::Composer,
            overlay: None,
            composer: String::new(),
            transcript: vec![
                TranscriptLine::new(TranscriptLineKind::Status, "runtime started"),
                TranscriptLine::new(
                    TranscriptLineKind::User,
                    "Please review the changes in this branch.",
                ),
                TranscriptLine::new(TranscriptLineKind::Answer, "Summary\nLooks good overall."),
            ],
            transcript_scroll: 0,
            status: "idle".to_string(),
            active_turn: None,
            active_turn_cancel: None,
            saw_ready_instance: true,
            should_quit: false,
        };

        let rendered = render_to_text(&app, 160, 50);
        assert!(rendered.contains("LionClaw    project: lionclaw"));
        assert!(rendered.contains("instance: main"));
        assert!(rendered.contains("runtime: codex"));
        assert!(rendered.contains("net: none"));
        assert!(rendered.contains("Instances"));
        assert!(rendered.contains("Transcript"));
        assert!(rendered.contains("Boundary"));
        assert!(rendered.contains("Ask through the selected runtime"));
        assert!(rendered.contains("runtime controls pass through"));
        assert!(rendered.contains("Ctrl+P"));
        assert_eq!(rendered.lines().count(), 50);
    }

    #[tokio::test]
    async fn project_mode_lists_instances_and_selects_requested_instance() {
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
            resolve_console_instances(&target).await.expect("instances");

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
            default_runtime: Some("codex".to_string()),
        };
        let reviewer = InstanceSummary {
            name: Some("reviewer".to_string()),
            is_default: false,
            home: PathBuf::from("/tmp/reviewer"),
            work_root: None,
            work_root_finding: Some("blocked".to_string()),
            shared_work_root_count: 0,
            default_runtime: Some("opencode".to_string()),
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
