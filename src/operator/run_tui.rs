use std::{
    io::{self, Stdout},
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Result};
use crossterm::{
    event::{
        self, DisableBracketedPaste, EnableBracketedPaste, Event, KeyCode, KeyEvent, KeyEventKind,
        KeyModifiers,
    },
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Margin, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{
        Block, Borders, Clear, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState, Wrap,
    },
    Frame, Terminal,
};
use ratatui_textarea::{TextArea, WrapMode};
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

use crate::kernel::runtime::{
    append_streamed_text_boundary, append_streamed_text_delta,
    execution::planner::resolve_execution_preset, ExecutionPlanPurpose,
};

const EVENT_POLL: Duration = Duration::from_millis(50);
const HISTORY_LIMIT: usize = 24;
const PANEL_BORDER: Color = Color::Rgb(0, 205, 220);
const PANEL_MUTED: Color = Color::Rgb(128, 132, 142);
const PANEL_SELECTED: Color = Color::Rgb(0, 68, 72);
const PANEL_READY: Color = Color::Rgb(91, 255, 112);
const PANEL_WARN: Color = Color::Rgb(255, 198, 55);
const PANEL_ERROR: Color = Color::Rgb(255, 82, 82);
const ACTIVITY_ITEM_LIMIT: usize = 12;
const DEFAULT_TRANSCRIPT_PAGE_SCROLL: usize = 8;
const COMPOSER_MIN_HEIGHT: u16 = 6;
const COMPOSER_MAX_HEIGHT: u16 = 12;
const COMPOSER_CHROME_HEIGHT: u16 = 4;
const ACTIVITY_ERROR_MARKERS: &[&str] = &["error", "failed", "denied"];
const ACTIVITY_DONE_MARKERS: &[&str] = &[
    "completed",
    "started",
    "mounted",
    "granted",
    "compacted",
    " edited:",
];
const ACTIVITY_COMMAND_MARKERS: &[&str] = &[
    "command",
    "exec",
    " running:",
    " ran:",
    " searched:",
    " read:",
    " inspected:",
    " opened:",
    " found:",
];
const ACTIVITY_PROGRESS_MARKERS: &[&str] =
    &["progress", "checking", "reading", "research", " editing:"];

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

    pub(crate) fn no_project_instances(project_root: &std::path::Path) -> Self {
        Self {
            title: "No instances configured".to_string(),
            detail: format!(
                "Project {} has no configured instances.",
                project_root.display()
            ),
            suggestion: "Create one with: lionclaw instance create main. The run command will not create or repair configuration.".to_string(),
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

impl BoundarySummary {
    fn workspace_compact(&self) -> &str {
        match self.workspace.as_str() {
            "read-write" => "rw",
            "read-only" => "ro",
            other => other,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActivityStatus {
    Idle,
    Running,
    Complete,
    Failed,
}

impl ActivityStatus {
    fn label(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Running => "running",
            Self::Complete => "complete",
            Self::Failed => "failed",
        }
    }

    fn style(self) -> Style {
        let fg = match self {
            Self::Idle => PANEL_MUTED,
            Self::Running => PANEL_WARN,
            Self::Complete => PANEL_READY,
            Self::Failed => PANEL_ERROR,
        };
        Style::default().fg(fg).add_modifier(Modifier::BOLD)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActivityItemKind {
    Done,
    Command,
    Progress,
    Status,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActivityItem {
    kind: ActivityItemKind,
    text: String,
}

#[derive(Debug)]
struct ActivitySummary {
    status: ActivityStatus,
    event_count: usize,
    command_count: usize,
    progress_count: usize,
    started_at: Option<Instant>,
    ended_at: Option<Instant>,
    items: Vec<ActivityItem>,
    open_progress_item: Option<usize>,
    open_progress_text: String,
}

impl ActivitySummary {
    fn new() -> Self {
        Self {
            status: ActivityStatus::Idle,
            event_count: 0,
            command_count: 0,
            progress_count: 0,
            started_at: None,
            ended_at: None,
            items: Vec::new(),
            open_progress_item: None,
            open_progress_text: String::new(),
        }
    }

    fn start(&mut self) {
        self.status = ActivityStatus::Running;
        self.event_count = 0;
        self.command_count = 0;
        self.progress_count = 0;
        self.started_at = Some(Instant::now());
        self.ended_at = None;
        self.items.clear();
        self.open_progress_item = None;
        self.open_progress_text.clear();
    }

    fn complete(&mut self) {
        if self.status != ActivityStatus::Failed {
            self.status = ActivityStatus::Complete;
            self.ended_at.get_or_insert_with(Instant::now);
        }
    }

    fn fail(&mut self) {
        self.status = ActivityStatus::Failed;
        self.ended_at.get_or_insert_with(Instant::now);
    }

    fn is_empty(&self) -> bool {
        self.event_count == 0 && self.items.is_empty()
    }

    fn elapsed_label(&self) -> Option<String> {
        self.started_at.map(|started_at| {
            let elapsed = self.ended_at.map_or_else(
                || started_at.elapsed(),
                |ended_at| ended_at.duration_since(started_at),
            );
            format_elapsed(elapsed)
        })
    }

    fn record_stream_event(&mut self, event: &StreamEventDto) {
        if event.kind == StreamEventKindDto::MessageBoundary {
            if event.lane == Some(StreamLaneDto::Reasoning) {
                self.close_progress_item();
            }
            return;
        }
        self.event_count = self.event_count.saturating_add(1);
        match (&event.kind, &event.lane, event.text.as_deref()) {
            (StreamEventKindDto::MessageDelta, Some(StreamLaneDto::Reasoning), Some(text)) => {
                self.record_progress_delta(text);
            }
            (StreamEventKindDto::Status, _, Some(text)) => {
                self.close_progress_item();
                let kind = classify_activity_status(text);
                if kind == ActivityItemKind::Command {
                    self.command_count = self.command_count.saturating_add(1);
                }
                if kind == ActivityItemKind::Progress {
                    self.progress_count = self.progress_count.saturating_add(1);
                }
                self.push_item(kind, normalize_activity_text(text));
            }
            (StreamEventKindDto::Error, _, Some(text)) => {
                self.close_progress_item();
                self.fail();
                self.push_item(ActivityItemKind::Error, normalize_activity_text(text));
            }
            (StreamEventKindDto::TurnCompleted | StreamEventKindDto::Done, _, _) => {
                self.close_progress_item();
                self.complete();
            }
            _ => {}
        }
    }

    fn record_progress_delta(&mut self, delta: &str) {
        if delta.trim().is_empty() {
            return;
        }
        if self.open_progress_item.is_none() {
            self.progress_count = self.progress_count.saturating_add(1);
            self.open_progress_text.clear();
            self.open_progress_item = self.push_item(
                ActivityItemKind::Progress,
                summarize_activity_text("progress", ""),
            );
        }
        append_progress_delta_text(&mut self.open_progress_text, delta);
        self.refresh_open_progress_item();
    }

    fn refresh_open_progress_item(&mut self) {
        let Some(index) = self.open_progress_item else {
            return;
        };
        let Some(item) = self.items.get_mut(index) else {
            self.close_progress_item();
            return;
        };
        if item.kind != ActivityItemKind::Progress {
            self.close_progress_item();
            return;
        }
        item.text = summarize_activity_text("progress", &self.open_progress_text);
    }

    fn close_progress_item(&mut self) {
        self.open_progress_item = None;
        self.open_progress_text.clear();
    }

    fn push_item(&mut self, kind: ActivityItemKind, text: String) -> Option<usize> {
        if text.trim().is_empty() {
            return None;
        }
        self.items.push(ActivityItem { kind, text });
        let overflow = self.items.len().saturating_sub(ACTIVITY_ITEM_LIMIT);
        if overflow > 0 {
            self.items.drain(0..overflow);
            self.open_progress_item = self
                .open_progress_item
                .and_then(|index| index.checked_sub(overflow));
        }
        Some(self.items.len() - 1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InspectorMode {
    Instance,
    Activity,
}

impl InspectorMode {
    fn label(self) -> &'static str {
        match self {
            Self::Instance => "instance",
            Self::Activity => "activity",
        }
    }
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
    fn label(self) -> &'static str {
        match self {
            Self::Instances => "instances",
            Self::Transcript => "transcript",
            Self::Composer => "composer",
            Self::Inspectors => "inspector",
        }
    }

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

struct ConsoleComposer {
    input: TextArea<'static>,
}

impl ConsoleComposer {
    fn new() -> Self {
        Self::from_text("")
    }

    fn from_text(text: &str) -> Self {
        let mut input = TextArea::default();
        input.set_style(Style::default().fg(Color::White));
        input.set_cursor_line_style(Style::default());
        input.set_cursor_style(
            Style::default()
                .fg(PANEL_BORDER)
                .add_modifier(Modifier::REVERSED),
        );
        input.set_styled_placeholder(Line::from(vec![Span::styled(
            "Ask through the selected runtime...",
            Style::default().fg(PANEL_MUTED),
        )]));
        input.set_wrap_mode(WrapMode::WordOrGlyph);
        if !text.is_empty() {
            input.insert_str(text);
        }
        Self { input }
    }

    fn clear(&mut self) {
        self.input.clear();
    }

    fn handle_key(&mut self, key: KeyEvent) -> bool {
        self.input.input(key)
    }

    fn insert_str(&mut self, text: &str) -> bool {
        self.input.insert_str(text)
    }

    fn insert_newline(&mut self) {
        self.input.insert_newline();
    }

    fn text(&self) -> String {
        self.input.clone().into_lines().join("\n")
    }

    fn take_text(&mut self) -> String {
        let text = self.text();
        *self = Self::new();
        text
    }

    fn restore_text(&mut self, text: String) {
        *self = Self::from_text(&text);
    }

    fn line_count(&self) -> usize {
        self.input.clone().into_lines().len()
    }

    fn widget(&self) -> &TextArea<'static> {
        &self.input
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
    composer: ConsoleComposer,
    transcript: Vec<TranscriptLine>,
    transcript_scroll: usize,
    transcript_scroll_limit: usize,
    transcript_page_size: usize,
    activity: ActivitySummary,
    inspector_mode: InspectorMode,
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
            composer: ConsoleComposer::new(),
            transcript: Vec::new(),
            transcript_scroll: 0,
            transcript_scroll_limit: 0,
            transcript_page_size: DEFAULT_TRANSCRIPT_PAGE_SCROLL,
            activity: ActivitySummary::new(),
            inspector_mode: InspectorMode::Instance,
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

    fn scroll_transcript_up(&mut self, amount: usize) {
        self.transcript_scroll = self.transcript_scroll.saturating_sub(amount);
    }

    fn scroll_transcript_down(&mut self, amount: usize) {
        self.transcript_scroll = self
            .transcript_scroll
            .saturating_add(amount)
            .min(self.transcript_scroll_limit);
    }

    fn set_transcript_scroll_limit(&mut self, limit: usize) {
        self.transcript_scroll_limit = limit;
        self.transcript_scroll = self.transcript_scroll.min(limit);
    }

    fn set_transcript_viewport(&mut self, line_count: usize, viewport_height: u16) {
        self.transcript_page_size = usize::from(viewport_height.max(1));
        self.set_transcript_scroll_limit(transcript_scroll_limit(line_count, viewport_height));
    }

    fn composer_height(&self, terminal_height: u16) -> u16 {
        let content_height = self.composer.line_count().min(usize::from(u16::MAX)) as u16;
        let desired = content_height
            .saturating_add(COMPOSER_CHROME_HEIGHT)
            .clamp(COMPOSER_MIN_HEIGHT, COMPOSER_MAX_HEIGHT);
        let available = terminal_height
            .saturating_sub(16)
            .clamp(COMPOSER_MIN_HEIGHT, COMPOSER_MAX_HEIGHT);
        desired.min(available)
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

    fn context_label(&self) -> String {
        if self.project_mode() {
            format!("{}/{}", self.project_label(), self.selected_name())
        } else {
            self.selected_name().to_string()
        }
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
        self.transcript_scroll_limit = 0;
        self.transcript_page_size = DEFAULT_TRANSCRIPT_PAGE_SCROLL;
        self.activity = ActivitySummary::new();
        self.inspector_mode = InspectorMode::Instance;
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
        let text = self.composer.take_text();
        match classify_input(&text) {
            ClassifiedInput::Empty => {
                self.status = "composer is empty".to_string();
                self.composer.restore_text(text);
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
        self.activity.start();
        self.inspector_mode = InspectorMode::Activity;
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
        self.activity.start();
        self.inspector_mode = InspectorMode::Activity;
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
        self.activity.start();
        self.inspector_mode = InspectorMode::Activity;
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
                push_stream_event(&mut self.transcript, &mut self.activity, &event);
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
                        self.activity.complete();
                        self.status = "idle".to_string();
                    }
                    Err(message) => {
                        self.activity.fail();
                        self.activity.push_item(ActivityItemKind::Error, message);
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
                        self.activity.complete();
                        self.activity.push_item(
                            ActivityItemKind::Done,
                            "opened a fresh session".to_string(),
                        );
                        self.status = "idle".to_string();
                    }
                    Err(message) => {
                        self.activity.fail();
                        self.activity.push_item(ActivityItemKind::Error, message);
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
                "{}/{}",
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

fn push_stream_event(
    transcript: &mut Vec<TranscriptLine>,
    activity: &mut ActivitySummary,
    event: &StreamEventDto,
) {
    activity.record_stream_event(event);
    match (&event.kind, &event.lane, event.text.as_deref()) {
        (StreamEventKindDto::MessageDelta, Some(StreamLaneDto::Answer), Some(text)) => {
            append_transcript_delta(transcript, TranscriptLineKind::Answer, text);
        }
        (StreamEventKindDto::MessageBoundary, Some(StreamLaneDto::Answer), _) => {
            append_transcript_boundary(transcript, TranscriptLineKind::Answer);
        }
        (StreamEventKindDto::TurnCompleted, _, _)
        | (StreamEventKindDto::Done, _, _)
        | (_, _, None)
        | (StreamEventKindDto::MessageDelta, Some(StreamLaneDto::Reasoning), Some(_))
        | (StreamEventKindDto::MessageBoundary, _, _)
        | (StreamEventKindDto::Status, _, Some(_))
        | (StreamEventKindDto::Error, _, Some(_)) => {}
        (_, _, Some(_)) => {}
    }
}

fn append_transcript_delta(
    transcript: &mut Vec<TranscriptLine>,
    kind: TranscriptLineKind,
    text: &str,
) {
    if let Some(last) = transcript.last_mut() {
        if last.kind == kind {
            append_delta_text(&mut last.text, text);
            return;
        }
    }
    transcript.push(TranscriptLine::new(kind, text));
}

fn append_delta_text(existing: &mut String, delta: &str) {
    append_streamed_text_delta(existing, delta);
}

fn append_transcript_boundary(transcript: &mut [TranscriptLine], kind: TranscriptLineKind) {
    let Some(last) = transcript.last_mut() else {
        return;
    };
    if last.kind == kind {
        append_streamed_text_boundary(&mut last.text);
    }
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
        composer: ConsoleComposer::new(),
        transcript: Vec::new(),
        transcript_scroll: 0,
        transcript_scroll_limit: 0,
        transcript_page_size: DEFAULT_TRANSCRIPT_PAGE_SCROLL,
        activity: ActivitySummary::new(),
        inspector_mode: InspectorMode::Instance,
        status: "launch blocked".to_string(),
        active_turn: None,
        active_turn_cancel: None,
        saw_ready_instance: false,
        should_quit: false,
    };
    run_console_app(app).await.map(|_| ())
}

pub(crate) async fn run_project_launch_blocker(
    project_root: PathBuf,
    blocker: LaunchBlocker,
) -> Result<()> {
    let summary = InstanceSummary {
        name: Some("no instances".to_string()),
        is_default: false,
        home: PathBuf::new(),
        work_root: None,
        work_root_finding: Some(blocker.detail.clone()),
        shared_work_root_count: 0,
        default_runtime: None,
    };
    let app = ConsoleApp {
        project_root: Some(project_root),
        instances: vec![summary.clone()],
        selected_index: 0,
        selected: SelectedInstanceState::Blocked { summary, blocker },
        continue_last_session: false,
        timeout_override: None,
        focus: Focus::Instances,
        overlay: None,
        composer: ConsoleComposer::new(),
        transcript: Vec::new(),
        transcript_scroll: 0,
        transcript_scroll_limit: 0,
        transcript_page_size: DEFAULT_TRANSCRIPT_PAGE_SCROLL,
        activity: ActivitySummary::new(),
        inspector_mode: InspectorMode::Instance,
        status: "no instances configured".to_string(),
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
    execute!(stdout, EnterAlternateScreen, EnableBracketedPaste)?;
    let backend = CrosstermBackend::new(stdout);
    Terminal::new(backend).map_err(Into::into)
}

fn leave_terminal(mut terminal: ConsoleTerminal) -> Result<()> {
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        DisableBracketedPaste,
        LeaveAlternateScreen
    )?;
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
            handle_terminal_event(app, event::read()?, backend_tx).await;
        }
    }

    Ok(())
}

async fn handle_terminal_event(
    app: &mut ConsoleApp,
    event: Event,
    backend_tx: &mpsc::UnboundedSender<BackendEvent>,
) {
    match event {
        Event::Key(key) if key.kind == KeyEventKind::Press => {
            handle_key(app, key, backend_tx).await;
        }
        Event::Paste(text) => {
            app.focus = Focus::Composer;
            let char_count = text.chars().count();
            app.composer.insert_str(&text);
            app.status = format!("pasted {char_count} characters");
        }
        _ => {}
    }
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

    if is_composer_newline_key(key) {
        app.focus = Focus::Composer;
        app.composer.insert_newline();
        return;
    }

    match (key.code, key.modifiers) {
        (KeyCode::F(1), _) => app.overlay = Some(Overlay::Help),
        (KeyCode::Char('p'), KeyModifiers::CONTROL) => app.overlay = Some(Overlay::Palette),
        (KeyCode::Char('o'), KeyModifiers::CONTROL) => {
            app.inspector_mode = match app.inspector_mode {
                InspectorMode::Instance => InspectorMode::Activity,
                InspectorMode::Activity => InspectorMode::Instance,
            };
            app.focus = Focus::Inspectors;
            app.status = format!("inspector: {}", app.inspector_mode.label());
        }
        (KeyCode::Tab, _) => {
            app.focus = app.focus.next(app.project_mode());
            app.status = format!("focus: {}", app.focus.label());
        }
        (KeyCode::BackTab, _) => {
            app.focus = app.focus.previous(app.project_mode());
            app.status = format!("focus: {}", app.focus.label());
        }
        (KeyCode::Esc, _) => {
            app.composer.clear();
            app.status = "composer cleared".to_string();
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
        (KeyCode::Up, _) if app.focus == Focus::Transcript => {
            app.scroll_transcript_up(1);
        }
        (KeyCode::PageUp, _) if app.focus == Focus::Transcript => {
            app.scroll_transcript_up(app.transcript_page_size);
        }
        (KeyCode::Down, _) if app.focus == Focus::Transcript => {
            app.scroll_transcript_down(1);
        }
        (KeyCode::PageDown, _) if app.focus == Focus::Transcript => {
            app.scroll_transcript_down(app.transcript_page_size);
        }
        _ if app.focus == Focus::Composer => {
            app.composer.handle_key(key);
        }
        _ if key_starts_composer(key) => {
            app.focus = Focus::Composer;
            app.composer.handle_key(key);
        }
        _ => {}
    }
}

fn is_composer_newline_key(key: KeyEvent) -> bool {
    (key.code == KeyCode::Enter
        && (key.modifiers.contains(KeyModifiers::SHIFT)
            || key.modifiers.contains(KeyModifiers::ALT)))
        || (key.code == KeyCode::Char('j') && key.modifiers == KeyModifiers::CONTROL)
}

fn key_starts_composer(key: KeyEvent) -> bool {
    matches!(key.code, KeyCode::Char(_))
        && !key.modifiers.contains(KeyModifiers::CONTROL)
        && !key.modifiers.contains(KeyModifiers::ALT)
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

pub(crate) fn render_app(frame: &mut Frame<'_>, app: &mut ConsoleApp) {
    let area = frame.area();
    let composer_height = app.composer_height(area.height);
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(1),
            Constraint::Min(10),
            Constraint::Length(1),
            Constraint::Length(composer_height),
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
    let workspace = boundary.workspace_compact().to_string();
    let line = Line::from(vec![
        Span::styled(
            "LionClaw",
            Style::default()
                .fg(PANEL_BORDER)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("  |  ", Style::default().fg(PANEL_MUTED)),
        Span::styled(app.context_label(), Style::default().fg(Color::White)),
        Span::raw("    "),
        Span::styled(app.runtime_label(), Style::default().fg(Color::White)),
        Span::raw("    "),
        Span::styled("net:", Style::default().fg(PANEL_BORDER)),
        Span::styled(boundary.network, Style::default().fg(Color::White)),
        Span::raw("    "),
        Span::styled("secrets:", Style::default().fg(PANEL_BORDER)),
        Span::styled(
            boundary.secrets,
            Style::default().fg(if app.selected.is_ready() {
                PANEL_WARN
            } else {
                PANEL_ERROR
            }),
        ),
        Span::raw("    "),
        Span::styled(workspace, Style::default().fg(Color::White)),
        Span::raw("    "),
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

fn render_body(frame: &mut Frame<'_>, area: Rect, app: &mut ConsoleApp) {
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
        let [instances_area, _, transcript_area, _, inspector_area] = chunks.as_ref() else {
            render_transcript(frame, area, app);
            return;
        };
        render_instances(frame, *instances_area, app);
        render_transcript(frame, *transcript_area, app);
        render_inspector(frame, *inspector_area, app);
    } else {
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Min(40),
                Constraint::Length(1),
                Constraint::Length(44),
            ])
            .split(area);
        let [transcript_area, _, inspector_area] = chunks.as_ref() else {
            render_transcript(frame, area, app);
            return;
        };
        render_transcript(frame, *transcript_area, app);
        render_inspector(frame, *inspector_area, app);
    }
}

fn render_instances(frame: &mut Frame<'_>, area: Rect, app: &ConsoleApp) {
    let content = render_panel_shell(frame, area, "Project", app.focus == Focus::Instances);
    if content.width < 8 || content.height < 2 {
        return;
    }

    let mut row_y = content.y;
    render_section_heading(frame, content, row_y, "Instances");
    row_y = row_y.saturating_add(2);
    for (index, instance) in app.instances.iter().enumerate() {
        if row_y >= content.y.saturating_add(content.height) {
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
        } else if instance.work_root.is_some() {
            "ready"
        } else {
            "idle"
        };
        let default_mark = if instance.is_default { " default" } else { "" };
        let shared = if instance.shared_work_root_count > 1 {
            format!(" [{}]", instance.shared_work_root_count)
        } else {
            String::new()
        };
        let row = format_instance_row(
            icon,
            instance.display_name(),
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
        row_y = row_y.saturating_add(1);
    }

    if row_y.saturating_add(4) < content.y.saturating_add(content.height) {
        row_y = row_y.saturating_add(1);
        draw_horizontal_rule(frame, content.x, row_y, content.width, PANEL_MUTED);
        row_y = row_y.saturating_add(2);
        render_section_heading(frame, content, row_y, "Sessions");
        row_y = row_y.saturating_add(2);
        let session_state = if app.selected.is_ready() {
            if app.continue_last_session {
                "continued"
            } else {
                "current"
            }
        } else {
            "blocked"
        };
        render_project_object_row(frame, content, row_y, "current", session_state, false);
        row_y = row_y.saturating_add(2);
        if row_y.saturating_add(3) < content.y.saturating_add(content.height) {
            render_section_heading(frame, content, row_y, "Drafts");
            row_y = row_y.saturating_add(2);
            render_project_object_row(frame, content, row_y, "No drafts yet", "", true);
        }
    }
}

fn render_transcript(frame: &mut Frame<'_>, area: Rect, app: &mut ConsoleApp) {
    let content = render_panel_shell(frame, area, "Transcript", app.focus == Focus::Transcript);
    if let SelectedInstanceState::Blocked { blocker, .. } = &app.selected {
        let text = Text::from(launch_blocker_lines(blocker));
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
    let activity_rows = if app.activity.is_empty() { 0 } else { 3 };
    let transcript_area = Rect {
        x: content.x,
        y: content.y,
        width: content.width.saturating_sub(1),
        height: content.height.saturating_sub(activity_rows),
    };
    if transcript_area.width == 0 || transcript_area.height == 0 {
        app.set_transcript_viewport(0, transcript_area.height);
        return;
    }
    let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false });
    let rendered_line_count = paragraph.line_count(transcript_area.width);
    app.set_transcript_viewport(rendered_line_count, transcript_area.height);
    let scroll = app.transcript_scroll;
    frame.render_widget(
        paragraph.scroll((scroll.min(u16::MAX as usize) as u16, 0)),
        transcript_area,
    );
    let scrollbar_area = Rect {
        x: content.x + content.width.saturating_sub(1),
        y: transcript_area.y,
        width: 1.min(content.width),
        height: transcript_area.height,
    };
    render_transcript_scrollbar(frame, scrollbar_area, rendered_line_count, scroll);
    if activity_rows > 0 && content.height > activity_rows {
        let rule_y = content.y + content.height - activity_rows;
        draw_horizontal_rule(frame, content.x, rule_y, content.width, PANEL_MUTED);
        let mut spans = vec![
            Span::styled(
                "activity",
                Style::default()
                    .fg(PANEL_BORDER)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  ▸  "),
            Span::styled(app.activity.status.label(), app.activity.status.style()),
            Span::raw(format!("    {} events", app.activity.event_count)),
        ];
        if app.activity.command_count > 0 {
            spans.push(Span::raw(format!(
                "    {} commands",
                app.activity.command_count
            )));
        }
        if app.activity.progress_count > 0 {
            spans.push(Span::raw(format!(
                "    {} progress notes",
                app.activity.progress_count
            )));
        }
        if let Some(elapsed) = app.activity.elapsed_label() {
            spans.push(Span::styled(
                format!("    {elapsed}"),
                Style::default().fg(PANEL_BORDER),
            ));
        }
        let activity = Line::from(spans);
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

fn launch_blocker_lines(blocker: &LaunchBlocker) -> Vec<Line<'static>> {
    let mut lines = vec![
        Line::styled(blocker.title.clone(), Style::default().fg(PANEL_ERROR)),
        Line::raw(""),
    ];
    lines.extend(multiline_prefixed_lines(
        "",
        "",
        Style::default().fg(Color::White),
        &blocker.detail,
    ));
    lines.push(Line::raw(""));
    lines.extend(multiline_prefixed_lines(
        "",
        "",
        Style::default().fg(Color::White),
        &blocker.suggestion,
    ));
    lines
}

pub(crate) fn transcript_render_lines(lines: &[TranscriptLine]) -> Vec<Line<'static>> {
    let mut rendered = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if index > 0 {
            rendered.push(Line::raw(""));
        }
        let (role, style) = match line.kind {
            TranscriptLineKind::User => (
                "you",
                Style::default()
                    .fg(PANEL_BORDER)
                    .add_modifier(Modifier::BOLD),
            ),
            TranscriptLineKind::Answer => (
                "lionclaw",
                Style::default()
                    .fg(PANEL_BORDER)
                    .add_modifier(Modifier::BOLD),
            ),
            TranscriptLineKind::Status => ("note", Style::default().fg(PANEL_MUTED)),
            TranscriptLineKind::Error => ("error", Style::default().fg(PANEL_ERROR)),
        };
        rendered.push(Line::styled(role.to_string(), style));
        match line.kind {
            TranscriptLineKind::Answer => rendered.extend(transcript_markdown_lines(&line.text)),
            TranscriptLineKind::User | TranscriptLineKind::Status | TranscriptLineKind::Error => {
                rendered.extend(multiline_prefixed_lines(
                    "",
                    "",
                    Style::default().fg(Color::White),
                    &line.text,
                ));
            }
        }
    }
    rendered
}

#[derive(Clone, Copy)]
struct TranscriptMarkdownStyleSheet;

impl tui_markdown::StyleSheet for TranscriptMarkdownStyleSheet {
    fn heading(&self, _level: u8) -> Style {
        Style::default()
            .fg(PANEL_BORDER)
            .add_modifier(Modifier::BOLD)
    }

    fn code(&self) -> Style {
        Style::default().fg(PANEL_WARN)
    }

    fn link(&self) -> Style {
        Style::default()
            .fg(PANEL_BORDER)
            .add_modifier(Modifier::UNDERLINED)
    }

    fn blockquote(&self) -> Style {
        Style::default().fg(PANEL_MUTED)
    }

    fn heading_meta(&self) -> Style {
        Style::default().fg(PANEL_MUTED)
    }

    fn metadata_block(&self) -> Style {
        Style::default().fg(PANEL_MUTED)
    }
}

fn transcript_markdown_lines(text: &str) -> Vec<Line<'static>> {
    let input = markdown_with_preserved_line_breaks(text);
    let options = tui_markdown::Options::new(TranscriptMarkdownStyleSheet);
    let rendered = tui_markdown::from_str_with_options(&input, &options);
    let lines = rendered
        .lines
        .into_iter()
        .map(owned_transcript_line)
        .filter(|line| !is_rendered_code_fence_marker(line))
        .collect::<Vec<_>>();

    if lines.is_empty() {
        vec![Line::raw("")]
    } else {
        lines
    }
}

fn is_rendered_code_fence_marker(line: &Line<'_>) -> bool {
    let mut spans = line.spans.iter();
    let Some(span) = spans.next() else {
        return false;
    };
    if spans.next().is_some() {
        return false;
    }
    let text = span.content.trim();
    text == "```"
        || text
            .strip_prefix("```")
            .is_some_and(is_markdown_info_string)
}

fn is_markdown_info_string(value: &str) -> bool {
    !value.trim().is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '+' | '#'))
}

fn markdown_with_preserved_line_breaks(text: &str) -> String {
    let mut output = String::with_capacity(text.len() + text.matches('\n').count() * 2);
    let mut in_fenced_code = false;

    for segment in text.split_inclusive('\n') {
        let (line, had_newline) = segment
            .strip_suffix('\n')
            .map_or((segment, false), |line| (line, true));
        let fence_line = is_markdown_fence_line(line);
        if fence_line && !in_fenced_code {
            ensure_markdown_blank_line(&mut output);
        }
        let preserve_soft_break = !in_fenced_code && !fence_line && !line.trim().is_empty();

        output.push_str(line);
        if had_newline {
            if preserve_soft_break {
                output.push_str("  \n");
            } else {
                output.push('\n');
            }
        }
        if fence_line {
            in_fenced_code = !in_fenced_code;
        }
    }

    output
}

fn ensure_markdown_blank_line(output: &mut String) {
    if output.is_empty() || output.ends_with("\n\n") {
        return;
    }
    if output.ends_with('\n') {
        output.push('\n');
    } else {
        output.push_str("\n\n");
    }
}

fn is_markdown_fence_line(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with("```") || trimmed.starts_with("~~~")
}

fn owned_transcript_line(line: Line<'_>) -> Line<'static> {
    let spans = line
        .spans
        .into_iter()
        .map(|span| {
            let mut style = span.style;
            if style.fg.is_none() {
                style.fg = Some(Color::White);
            }
            Span::styled(span.content.into_owned(), style)
        })
        .collect();
    Line {
        style: line.style,
        alignment: line.alignment,
        spans,
    }
}

fn transcript_scroll_limit(line_count: usize, viewport_height: u16) -> usize {
    line_count
        .saturating_sub(viewport_height as usize)
        .min(u16::MAX as usize)
}

fn render_transcript_scrollbar(
    frame: &mut Frame<'_>,
    area: Rect,
    line_count: usize,
    scroll: usize,
) {
    if line_count <= area.height as usize || area.width == 0 || area.height == 0 {
        return;
    }
    let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
        .begin_symbol(Some("^"))
        .end_symbol(Some("v"))
        .thumb_style(Style::default().fg(PANEL_BORDER))
        .track_style(Style::default().fg(PANEL_MUTED));
    let mut state = ScrollbarState::new(line_count)
        .viewport_content_length(area.height as usize)
        .position(scroll);
    frame.render_stateful_widget(scrollbar, area, &mut state);
}

fn multiline_prefixed_lines(
    first_prefix: &str,
    continuation_prefix: &str,
    prefix_style: Style,
    text: &str,
) -> Vec<Line<'static>> {
    if text.is_empty() {
        return vec![Line::from(vec![Span::styled(
            first_prefix.to_string(),
            prefix_style,
        )])];
    }
    text.lines()
        .enumerate()
        .map(|(index, line)| {
            let prefix = if index == 0 {
                first_prefix
            } else {
                continuation_prefix
            };
            Line::from(vec![
                Span::styled(prefix.to_string(), prefix_style),
                Span::raw(line.to_string()),
            ])
        })
        .collect()
}

fn render_inspector(frame: &mut Frame<'_>, area: Rect, app: &ConsoleApp) {
    let title = match app.inspector_mode {
        InspectorMode::Instance => "Inspector  Instance",
        InspectorMode::Activity => "Inspector  Activity",
    };
    let content = render_panel_shell(frame, area, title, app.focus == Focus::Inspectors);
    if content.width < 8 || content.height < 4 {
        return;
    }

    match app.inspector_mode {
        InspectorMode::Instance => render_instance_inspector(frame, content, app),
        InspectorMode::Activity => render_activity_inspector(frame, content, app),
    }
}

fn render_instance_inspector(frame: &mut Frame<'_>, content: Rect, app: &ConsoleApp) {
    if let SelectedInstanceState::Blocked { blocker, .. } = &app.selected {
        let text = Text::from(vec![
            section_line("!", "Selected"),
            Line::raw(""),
            kv_line("instance", app.selected_name()),
            kv_line("state", "blocked"),
            kv_line("reason", &blocker.title),
            Line::raw(""),
            Line::styled(
                "Launch guidance is shown in the transcript pane.",
                Style::default().fg(PANEL_MUTED),
            ),
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

    let boundary = app.boundary_summary();
    let mut lines = vec![
        section_line("●", "Selected"),
        Line::raw(""),
        kv_line("instance", app.selected_name()),
        kv_line("runtime", &app.runtime_label()),
        kv_line("kind", &app.runtime_kind_label()),
        kv_line("preset", &boundary.preset),
        Line::raw(""),
        section_line("▱", "Boundary"),
        Line::raw(""),
        kv_arrow_line("/workspace", "repo", &boundary.workspace),
        kv_arrow_line("/runtime", "session", "private"),
        kv_arrow_line("/drafts", "", "private"),
        kv_arrow_line("/lionclaw/skills", "", "ro"),
        Line::raw(""),
        section_line("◎", "Network"),
        Line::raw(""),
        Line::raw(format!("  {}", boundary.network)),
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
    lines.extend([
        check_line("runtime.plan.allow"),
        check_line("runtime.started"),
        check_line("session.turn.open"),
    ]);

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

    frame.render_widget(
        Paragraph::new(Line::styled(">", Style::default().fg(PANEL_BORDER))),
        Rect {
            x: area.x.saturating_add(2),
            y: area.y.saturating_add(1),
            width: 1,
            height: 1,
        },
    );
    frame.render_widget(
        app.composer.widget(),
        Rect {
            x: area.x.saturating_add(5),
            y: area.y.saturating_add(1),
            width: area.width.saturating_sub(7),
            height: area.height.saturating_sub(4),
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
    let status = Line::from(vec![Span::styled(mode, Style::default().fg(PANEL_BORDER))]);
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

fn render_activity_inspector(frame: &mut Frame<'_>, content: Rect, app: &ConsoleApp) {
    let mut lines = Vec::new();
    if app.activity.is_empty() {
        lines.push(Line::styled(
            "No runtime activity for the current turn.",
            Style::default().fg(PANEL_MUTED),
        ));
    } else {
        lines.push(Line::from(vec![
            Span::raw("status "),
            Span::styled(app.activity.status.label(), app.activity.status.style()),
        ]));
        lines.push(Line::raw(format!("events {}", app.activity.event_count)));
        if app.activity.command_count > 0 {
            lines.push(Line::raw(format!(
                "commands {}",
                app.activity.command_count
            )));
        }
        if app.activity.progress_count > 0 {
            lines.push(Line::raw(format!(
                "progress notes {}",
                app.activity.progress_count
            )));
        }
        lines.push(Line::raw(""));
        for item in &app.activity.items {
            lines.extend(activity_item_lines(item));
        }
    }

    frame.render_widget(
        Paragraph::new(lines).wrap(Wrap { trim: false }),
        content.inner(Margin {
            vertical: 1,
            horizontal: 1,
        }),
    );
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, _app: &ConsoleApp) {
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
        key_span("F1"),
        Span::raw(" Help     "),
        key_span("Ctrl+P"),
        Span::raw(" Palette     "),
        key_span("Ctrl+O"),
        Span::raw(" Activity     "),
        key_span("Tab"),
        Span::raw(" Focus     "),
        key_span("Ctrl+C"),
        Span::raw(" Interrupt     "),
        key_span("F10"),
        Span::raw(" Exit"),
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
    let border_style = if focused {
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
    if area.width > 4 && area.height > 3 {
        frame.render_widget(
            Paragraph::new(Line::styled(
                if focused {
                    format!("▶ {title}")
                } else {
                    title.to_string()
                },
                Style::default().fg(PANEL_BORDER).add_modifier(if focused {
                    Modifier::BOLD
                } else {
                    Modifier::empty()
                }),
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

fn render_section_heading(frame: &mut Frame<'_>, content: Rect, y: u16, label: &str) {
    if y >= content.y + content.height {
        return;
    }
    frame.render_widget(
        Paragraph::new(Line::styled(
            label.to_string(),
            Style::default().fg(Color::White),
        )),
        Rect {
            x: content.x,
            y,
            width: content.width,
            height: 1,
        },
    );
}

fn render_project_object_row(
    frame: &mut Frame<'_>,
    content: Rect,
    y: u16,
    name: &str,
    detail: &str,
    muted: bool,
) {
    if y >= content.y + content.height {
        return;
    }
    let width = content.width as usize;
    let detail_width = 12.min(width.saturating_sub(10));
    let name_width = width.saturating_sub(detail_width + 2).max(1);
    let row = if detail.is_empty() {
        format!(" {}", truncate_to(name, width.saturating_sub(1)))
    } else {
        format!(
            " {:name_width$} {:>detail_width$}",
            truncate_to(name, name_width),
            truncate_to(detail, detail_width),
        )
    };
    frame.render_widget(
        Paragraph::new(Line::raw(row)).style(if muted {
            Style::default().fg(PANEL_MUTED)
        } else {
            Style::default().fg(Color::White)
        }),
        Rect {
            x: content.x,
            y,
            width: content.width,
            height: 1,
        },
    );
}

fn format_instance_row(icon: &str, name: &str, state: &str, width: usize) -> String {
    let name_width = 14.min(width.saturating_sub(14)).max(4);
    let state_width = width.saturating_sub(name_width + 5).max(3);
    let mut row = format!(
        " {icon} {:name_width$} {:>state_width$}",
        truncate_to(name, name_width),
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

fn kv_line(label: &'static str, value: &str) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("  {label:<10}"), Style::default().fg(PANEL_MUTED)),
        Span::raw(value.to_string()),
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

fn activity_item_lines(item: &ActivityItem) -> Vec<Line<'static>> {
    let (icon, style) = match item.kind {
        ActivityItemKind::Done => ("✓", Style::default().fg(PANEL_READY)),
        ActivityItemKind::Command => ("→", Style::default().fg(PANEL_BORDER)),
        ActivityItemKind::Progress => ("•", Style::default().fg(PANEL_BORDER)),
        ActivityItemKind::Status => ("→", Style::default().fg(PANEL_MUTED)),
        ActivityItemKind::Error => ("!", Style::default().fg(PANEL_ERROR)),
    };
    multiline_prefixed_lines(&format!("{icon}  "), "   ", style, &item.text)
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

fn classify_activity_status(text: &str) -> ActivityItemKind {
    let lower = text.to_ascii_lowercase();
    if contains_any(&lower, ACTIVITY_ERROR_MARKERS) {
        ActivityItemKind::Error
    } else if contains_any(&lower, ACTIVITY_DONE_MARKERS) {
        ActivityItemKind::Done
    } else if contains_any(&lower, ACTIVITY_COMMAND_MARKERS) {
        ActivityItemKind::Command
    } else if contains_any(&lower, ACTIVITY_PROGRESS_MARKERS) {
        ActivityItemKind::Progress
    } else {
        ActivityItemKind::Status
    }
}

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}

fn normalize_activity_text(text: &str) -> String {
    let trimmed = text.trim();
    trimmed
        .strip_prefix("codex item: ")
        .map(|item| format!("codex {item}"))
        .unwrap_or(trimmed.to_string())
}

fn summarize_activity_text(prefix: &str, text: &str) -> String {
    let normalized = normalize_activity_text(text);
    format!("{prefix}: {}", truncate_to(&normalized, 80))
}

fn append_progress_delta_text(existing: &mut String, delta: &str) {
    if needs_progress_word_separator(existing, delta) {
        existing.push(' ');
    }
    append_streamed_text_delta(existing, delta);
}

fn needs_progress_word_separator(existing: &str, delta: &str) -> bool {
    let Some(previous) = existing.chars().last() else {
        return false;
    };
    let Some(next) = delta.chars().next() else {
        return false;
    };
    previous.is_alphanumeric() && next.is_alphanumeric()
}

fn format_elapsed(duration: Duration) -> String {
    let seconds = duration.as_secs();
    format!("{:02}:{:02}", seconds / 60, seconds % 60)
}

fn render_overlay(frame: &mut Frame<'_>, area: Rect, app: &ConsoleApp, overlay: Overlay) {
    let popup = centered_rect(70, 55, area);
    frame.render_widget(Clear, popup);
    let (title, lines) = match overlay {
        Overlay::Help => (
            " Help ",
            vec![
                Line::from("F1 help"),
                Line::from("Ctrl+P command palette"),
                Line::from("Ctrl+O toggle activity inspector"),
                Line::from("Tab / Shift+Tab move focus"),
                Line::from("Enter submit prompt or /lionclaw command"),
                Line::from("Shift+Enter or Alt+Enter insert newline"),
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

    fn rendered_line_strings(lines: &[Line<'_>]) -> Vec<String> {
        lines
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect()
    }

    #[test]
    fn stream_events_append_answers_and_summarize_runtime_activity() {
        let mut transcript = Vec::new();
        let mut activity = ActivitySummary::new();
        activity.start();
        push_stream_event(
            &mut transcript,
            &mut activity,
            &StreamEventDto {
                kind: StreamEventKindDto::MessageDelta,
                lane: Some(StreamLaneDto::Answer),
                code: None,
                text: Some("hello".to_string()),
            },
        );
        push_stream_event(
            &mut transcript,
            &mut activity,
            &StreamEventDto {
                kind: StreamEventKindDto::MessageDelta,
                lane: Some(StreamLaneDto::Answer),
                code: None,
                text: Some(" world".to_string()),
            },
        );
        push_stream_event(
            &mut transcript,
            &mut activity,
            &StreamEventDto {
                kind: StreamEventKindDto::Status,
                lane: None,
                code: None,
                text: Some("codex ran: cargo test".to_string()),
            },
        );
        push_stream_event(
            &mut transcript,
            &mut activity,
            &StreamEventDto {
                kind: StreamEventKindDto::MessageDelta,
                lane: Some(StreamLaneDto::Reasoning),
                code: None,
                text: Some("checking project state".to_string()),
            },
        );

        assert_eq!(
            transcript,
            vec![TranscriptLine::new(
                TranscriptLineKind::Answer,
                "hello world"
            ),]
        );
        assert_eq!(activity.event_count, 4);
        assert_eq!(activity.command_count, 1);
        assert_eq!(activity.progress_count, 1);
        assert!(activity
            .items
            .iter()
            .any(|item| item.text == "codex ran: cargo test"));
    }

    #[test]
    fn answer_deltas_repair_missing_sentence_boundary_space() {
        let mut transcript = vec![TranscriptLine::new(
            TranscriptLineKind::Answer,
            "decisions.",
        )];

        append_transcript_delta(&mut transcript, TranscriptLineKind::Answer, "The markdown");

        assert_eq!(
            transcript,
            vec![TranscriptLine::new(
                TranscriptLineKind::Answer,
                "decisions. The markdown",
            )]
        );

        append_transcript_delta(&mut transcript, TranscriptLineKind::Answer, "\nstd.");
        append_transcript_delta(&mut transcript, TranscriptLineKind::Answer, "io::Result");
        assert!(transcript[0].text.ends_with("\nstd.io::Result"));

        append_transcript_delta(&mut transcript, TranscriptLineKind::Answer, ".");
        append_transcript_delta(&mut transcript, TranscriptLineKind::Answer, "**Report**");
        assert!(transcript[0].text.ends_with("std.io::Result. **Report**"));
    }

    #[test]
    fn answer_boundaries_preserve_streamed_message_blocks() {
        let mut transcript = Vec::new();
        let mut activity = ActivitySummary::new();
        activity.start();

        push_stream_event(
            &mut transcript,
            &mut activity,
            &StreamEventDto {
                kind: StreamEventKindDto::MessageDelta,
                lane: Some(StreamLaneDto::Answer),
                code: None,
                text: Some("I'll inspect the docs first.".to_string()),
            },
        );
        push_stream_event(
            &mut transcript,
            &mut activity,
            &StreamEventDto {
                kind: StreamEventKindDto::MessageBoundary,
                lane: Some(StreamLaneDto::Answer),
                code: None,
                text: None,
            },
        );
        push_stream_event(
            &mut transcript,
            &mut activity,
            &StreamEventDto {
                kind: StreamEventKindDto::MessageDelta,
                lane: Some(StreamLaneDto::Answer),
                code: None,
                text: Some("**Project**".to_string()),
            },
        );

        assert_eq!(
            transcript,
            vec![TranscriptLine::new(
                TranscriptLineKind::Answer,
                "I'll inspect the docs first.\n\n**Project**",
            )]
        );
        assert_eq!(activity.event_count, 2);
    }

    #[test]
    fn activity_classification_accepts_runtime_neutral_summaries() {
        let mut activity = ActivitySummary::new();
        activity.start();
        for text in [
            "opencode searched: README.md",
            "claude running: cargo test",
            "runtime progress: reading docs",
        ] {
            activity.record_stream_event(&StreamEventDto {
                kind: StreamEventKindDto::Status,
                lane: None,
                code: None,
                text: Some(text.to_string()),
            });
        }

        assert_eq!(activity.command_count, 2);
        assert_eq!(activity.progress_count, 1);
        assert!(activity
            .items
            .iter()
            .any(|item| item.text == "opencode searched: README.md"));
    }

    #[test]
    fn reasoning_deltas_coalesce_into_one_activity_progress_item() {
        let mut activity = ActivitySummary::new();
        activity.start();

        for text in ["those", "next", "for", "the", "project"] {
            activity.record_stream_event(&StreamEventDto {
                kind: StreamEventKindDto::MessageDelta,
                lane: Some(StreamLaneDto::Reasoning),
                code: None,
                text: Some(text.to_string()),
            });
        }

        assert_eq!(activity.event_count, 5);
        assert_eq!(activity.progress_count, 1);
        assert_eq!(activity.items.len(), 1);
        assert_eq!(
            activity.items[0],
            ActivityItem {
                kind: ActivityItemKind::Progress,
                text: "progress: those next for the project".to_string(),
            }
        );

        activity.record_stream_event(&StreamEventDto {
            kind: StreamEventKindDto::MessageBoundary,
            lane: Some(StreamLaneDto::Reasoning),
            code: None,
            text: None,
        });
        activity.record_stream_event(&StreamEventDto {
            kind: StreamEventKindDto::MessageDelta,
            lane: Some(StreamLaneDto::Reasoning),
            code: None,
            text: Some("then report".to_string()),
        });

        assert_eq!(activity.progress_count, 2);
        assert_eq!(activity.items.len(), 2);
        assert_eq!(activity.items[1].text, "progress: then report");
    }

    #[test]
    fn activity_elapsed_label_freezes_after_terminal_status() {
        let started_at = Instant::now() - Duration::from_secs(120);
        let ended_at = started_at + Duration::from_secs(23);
        let mut activity = ActivitySummary::new();
        activity.started_at = Some(started_at);
        activity.ended_at = Some(ended_at);
        activity.status = ActivityStatus::Failed;

        assert_eq!(activity.elapsed_label().as_deref(), Some("00:23"));

        activity.start();

        assert_eq!(activity.status, ActivityStatus::Running);
        assert!(activity.ended_at.is_none());
    }

    #[test]
    fn transcript_rendering_uses_message_blocks() {
        let lines = transcript_render_lines(&[
            TranscriptLine::new(TranscriptLineKind::User, "/compact"),
            TranscriptLine::new(TranscriptLineKind::Answer, "ok"),
            TranscriptLine::new(TranscriptLineKind::Error, "failed"),
        ]);
        let rendered = rendered_line_strings(&lines);

        assert_eq!(
            rendered,
            vec!["you", "/compact", "", "lionclaw", "ok", "", "error", "failed"]
        );
    }

    #[test]
    fn transcript_rendering_renders_markdown_without_collapsing_stream_lines() {
        let lines = transcript_render_lines(&[TranscriptLine::new(
            TranscriptLineKind::Answer,
            "opening line\n**Report**\nThis is **bold** text.",
        )]);
        let rendered = rendered_line_strings(&lines);

        assert_eq!(
            rendered,
            vec!["lionclaw", "opening line", "Report", "This is bold text."]
        );
        assert!(rendered.iter().all(|line| !line.contains("**")));
        assert!(lines[2]
            .spans
            .iter()
            .any(|span| span.style.add_modifier.contains(Modifier::BOLD)));
        assert!(lines[3]
            .spans
            .iter()
            .any(|span| span.style.add_modifier.contains(Modifier::BOLD)));
    }

    #[test]
    fn transcript_rendering_handles_markdown_code_fences() {
        let lines = transcript_render_lines(&[TranscriptLine::new(
            TranscriptLineKind::Answer,
            "Core product idea:\n```text\nRecord now -> transcribe later\n```\nNext step.",
        )]);
        let rendered = rendered_line_strings(&lines);

        assert!(rendered
            .iter()
            .any(|line| line.contains("Record now -> transcribe later")));
        assert!(rendered.iter().all(|line| !line.contains("```")));
        assert!(rendered.iter().all(|line| line.trim() != "text"));
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
        let mut app = ConsoleApp {
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
            composer: ConsoleComposer::new(),
            transcript: Vec::new(),
            transcript_scroll: 0,
            transcript_scroll_limit: 0,
            transcript_page_size: DEFAULT_TRANSCRIPT_PAGE_SCROLL,
            activity: ActivitySummary::new(),
            inspector_mode: InspectorMode::Instance,
            status: "launch blocked".to_string(),
            active_turn: None,
            active_turn_cancel: None,
            saw_ready_instance: false,
            should_quit: false,
        };

        let rendered = render_to_text(&mut app, 100, 30);
        assert!(rendered.contains("Transcript"));
        assert!(rendered.contains("Launch blocked for main"));
        assert!(rendered.contains("missing runtime"));
    }

    #[test]
    fn launch_blocker_rendering_preserves_embedded_newlines() {
        let mut app = blocked_test_app();
        let summary = app.selected.summary().clone();
        app.selected = SelectedInstanceState::Blocked {
            summary,
            blocker: LaunchBlocker::for_instance(
                "main",
                "no default runtime configured for instance \"main\"\nRun:\n  lionclaw configure --runtime codex",
            ),
        };
        app.focus = Focus::Transcript;

        let rendered = render_to_text(&mut app, 120, 30);

        assert!(rendered.contains("no default runtime configured"));
        assert!(rendered.contains("\"main\""));
        assert!(rendered.contains("Run:"));
        assert!(rendered.contains("lionclaw configure --runtime codex"));
        assert!(!rendered.contains("\"main\"Run:"));
    }

    #[test]
    fn activity_items_render_multiline_text_with_continuation_indent() {
        let lines = activity_item_lines(&ActivityItem {
            kind: ActivityItemKind::Command,
            text: "ran cargo test\nexit 0".to_string(),
        });
        let rendered = lines
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>();

        assert_eq!(rendered, vec!["→  ran cargo test", "   exit 0"]);
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
        let mut app = ConsoleApp {
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
            composer: ConsoleComposer::new(),
            transcript: vec![
                TranscriptLine::new(
                    TranscriptLineKind::User,
                    "Please review the changes in this branch.",
                ),
                TranscriptLine::new(TranscriptLineKind::Answer, "Summary\nLooks good overall."),
            ],
            transcript_scroll: 0,
            transcript_scroll_limit: 0,
            transcript_page_size: DEFAULT_TRANSCRIPT_PAGE_SCROLL,
            activity: ActivitySummary::new(),
            inspector_mode: InspectorMode::Instance,
            status: "idle".to_string(),
            active_turn: None,
            active_turn_cancel: None,
            saw_ready_instance: true,
            should_quit: false,
        };

        let rendered = render_to_text(&mut app, 160, 50);
        assert!(rendered.contains("LionClaw  |  lionclaw/main"));
        assert!(rendered.contains("codex"));
        assert!(rendered.contains("net:none"));
        assert!(rendered.contains("Project"));
        assert!(rendered.contains("Instances"));
        assert!(rendered.contains("Transcript"));
        assert!(rendered.contains("Inspector  Instance"));
        assert!(rendered.contains("Boundary"));
        assert!(rendered.contains("Ask through the selected runtime"));
        assert!(!rendered.contains("runtime controls pass through"));
        assert!(rendered.contains("Ctrl+O"));
        assert!(rendered.contains("Ctrl+P"));
        assert_eq!(rendered.lines().count(), 50);
    }

    #[tokio::test]
    async fn transcript_scroll_is_bounded_to_wrapped_content_and_renders_scrollbar() {
        let body = (0..40)
            .map(|index| format!("visible-line-{index:02}"))
            .collect::<Vec<_>>()
            .join("\n");
        let mut app =
            ready_test_app(vec![TranscriptLine::new(TranscriptLineKind::Answer, body)]).await;
        app.focus = Focus::Transcript;
        app.transcript_scroll = usize::MAX;

        let rendered = render_to_text(&mut app, 100, 24);

        assert!(app.transcript_scroll_limit > 0);
        assert_eq!(app.transcript_scroll, app.transcript_scroll_limit);
        assert!(rendered.contains("visible-line-39"));
        assert!(rendered.contains("^"));
        assert!(rendered.contains("v"));
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

    #[test]
    fn question_mark_is_printable_and_f1_opens_help() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let (backend_tx, _backend_rx) = mpsc::unbounded_channel();

        let mut composer_app = blocked_test_app();
        composer_app.focus = Focus::Composer;
        composer_app.composer = ConsoleComposer::from_text("Is this ok");
        runtime.block_on(async {
            handle_key(
                &mut composer_app,
                KeyEvent::new(KeyCode::Char('?'), KeyModifiers::NONE),
                &backend_tx,
            )
            .await;
        });
        assert_eq!(composer_app.composer.text(), "Is this ok?");
        assert!(composer_app.overlay.is_none());

        let mut nav_app = blocked_test_app();
        nav_app.focus = Focus::Instances;
        runtime.block_on(async {
            handle_key(
                &mut nav_app,
                KeyEvent::new(KeyCode::Char('?'), KeyModifiers::NONE),
                &backend_tx,
            )
            .await;
        });
        assert_eq!(nav_app.composer.text(), "?");
        assert_eq!(nav_app.focus, Focus::Composer);
        assert!(nav_app.overlay.is_none());

        runtime.block_on(async {
            handle_key(
                &mut nav_app,
                KeyEvent::new(KeyCode::F(1), KeyModifiers::NONE),
                &backend_tx,
            )
            .await;
        });
        assert_eq!(nav_app.overlay, Some(Overlay::Help));
    }

    #[test]
    fn composer_handles_multiline_editing_and_bracketed_paste() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let (backend_tx, _backend_rx) = mpsc::unbounded_channel();
        let mut app = blocked_test_app();

        runtime.block_on(async {
            handle_terminal_event(
                &mut app,
                Event::Paste("first line\nsecond line".to_string()),
                &backend_tx,
            )
            .await;
            handle_key(
                &mut app,
                KeyEvent::new(KeyCode::Enter, KeyModifiers::SHIFT),
                &backend_tx,
            )
            .await;
            handle_terminal_event(
                &mut app,
                Event::Paste("third line".to_string()),
                &backend_tx,
            )
            .await;
        });

        assert_eq!(app.composer.text(), "first line\nsecond line\nthird line");
        assert_eq!(app.focus, Focus::Composer);
        assert_eq!(app.status, "pasted 10 characters");
    }

    #[test]
    fn composer_height_expands_for_multiline_input() {
        let mut app = blocked_test_app();
        app.composer = ConsoleComposer::from_text("one\ntwo\nthree\nfour\nfive\nsix\nseven\neight");

        assert_eq!(app.composer_height(80), COMPOSER_MAX_HEIGHT);

        app.composer = ConsoleComposer::new();
        assert_eq!(app.composer_height(80), COMPOSER_MIN_HEIGHT);
    }

    #[test]
    fn ctrl_o_toggles_activity_inspector() {
        let mut app = blocked_test_app();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let (backend_tx, _backend_rx) = mpsc::unbounded_channel();

        runtime.block_on(async {
            handle_key(
                &mut app,
                KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL),
                &backend_tx,
            )
            .await;
        });

        assert_eq!(app.inspector_mode, InspectorMode::Activity);
        assert_eq!(app.focus, Focus::Inspectors);
        let rendered = render_to_text(&mut app, 100, 30);
        assert!(rendered.contains("Inspector  Activity"));
    }

    #[test]
    fn tab_moves_focus_and_render_marks_focused_pane() {
        let mut app = blocked_test_app();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let (backend_tx, _backend_rx) = mpsc::unbounded_channel();

        runtime.block_on(async {
            handle_key(
                &mut app,
                KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE),
                &backend_tx,
            )
            .await;
        });

        assert_eq!(app.focus, Focus::Transcript);
        assert_eq!(app.status, "focus: transcript");
        let rendered = render_to_text(&mut app, 100, 30);
        assert!(rendered.contains("▶ Transcript"));
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
            composer: ConsoleComposer::new(),
            transcript: Vec::new(),
            transcript_scroll: 0,
            transcript_scroll_limit: 0,
            transcript_page_size: DEFAULT_TRANSCRIPT_PAGE_SCROLL,
            activity: ActivitySummary::new(),
            inspector_mode: InspectorMode::Instance,
            status: "idle".to_string(),
            active_turn: None,
            active_turn_cancel: None,
            saw_ready_instance: false,
            should_quit: false,
        }
    }

    async fn ready_test_app(transcript: Vec<TranscriptLine>) -> ConsoleApp {
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
            default_runtime: Some("mock".to_string()),
        };
        ConsoleApp {
            project_root: None,
            instances: vec![main.clone()],
            selected_index: 0,
            selected: SelectedInstanceState::Ready(Box::new(ReadyInstance {
                summary: main,
                runtime_id: "mock".to_string(),
                runtime_kind: "mock".to_string(),
                runtime_override: None,
                boundary: BoundarySummary {
                    workspace: "rw".to_string(),
                    network: "none".to_string(),
                    secrets: "off".to_string(),
                    timeout: "2h".to_string(),
                    preset: "test".to_string(),
                },
                kernel,
                session_id: Uuid::new_v4(),
            })),
            continue_last_session: false,
            timeout_override: None,
            focus: Focus::Composer,
            overlay: None,
            composer: ConsoleComposer::new(),
            transcript,
            transcript_scroll: 0,
            transcript_scroll_limit: 0,
            transcript_page_size: DEFAULT_TRANSCRIPT_PAGE_SCROLL,
            activity: ActivitySummary::new(),
            inspector_mode: InspectorMode::Instance,
            status: "idle".to_string(),
            active_turn: None,
            active_turn_cancel: None,
            saw_ready_instance: true,
            should_quit: false,
        }
    }

    fn render_to_text(app: &mut ConsoleApp, width: u16, height: u16) -> String {
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
