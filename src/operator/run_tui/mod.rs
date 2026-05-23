use std::{
    io::{self, Stdout},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{anyhow, Result};
use crossterm::{
    event::{
        self, DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture,
        Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseEventKind,
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
        Block, Borders, Clear, List, ListItem, ListState, Paragraph, Scrollbar,
        ScrollbarOrientation, ScrollbarState, Wrap,
    },
    Frame, Terminal,
};
use ratatui_textarea::{TextArea, WrapMode};
use tokio::{sync::mpsc, task::JoinHandle};
use uuid::Uuid;

use crate::{
    contracts::{
        AuditEventView, SessionActionKind, SessionActionRequest, SessionHistoryRequest,
        SessionTurnRequest, SessionTurnResponse, SessionTurnStatus, SessionTurnView,
        StreamEventDto, StreamEventKindDto, StreamFileChangeDto, StreamFileChangeStatusDto,
        StreamLaneDto,
    },
    home::LionClawHome,
    kernel::{
        input_routing::{classify_input, ClassifiedInput, LionClawControlInput},
        Kernel, TurnCancellation,
    },
    operator::{
        config::OperatorConfig,
        reconcile::{open_runtime_kernel_for_work_root, render_runtime_cache_for_work_root},
        run::{
            kernel_to_anyhow, local_peer_id_for_project, partial_history_marker,
            resolve_repl_session, resolve_run_runtime_id,
        },
        runtime::validate_runtime_launch_prerequisites_for_work_root,
        target::{inspect_target_work_root, list_project_instance_statuses, TargetContext},
    },
    runtime_timeouts::RuntimeTurnTimeouts,
};

use crate::kernel::runtime::{
    append_streamed_text_boundary, append_streamed_text_delta,
    execution::planner::resolve_execution_preset, ExecutionPlanPurpose,
};

mod backend;
mod input;
mod render;

use backend::{
    load_audit_events, load_project_objects, open_selected_instance, push_history_turn,
    push_stream_event, resolve_console_instances, spawn_streamed_turn, BackendEvent,
    StreamedSubmission,
};
use input::handle_terminal_event;
use render::{render_app, vertical_scroll_limit};

#[cfg(test)]
use backend::append_transcript_delta;
#[cfg(test)]
use input::{global_command_for, handle_key};
#[cfg(test)]
use render::{
    activity_item_lines, boundary_display_rows, footer_hint_spans,
    scrollbar_position_for_pane_offset, transcript_render_lines, transcript_with_activity_lines,
};

const EVENT_POLL: Duration = Duration::from_millis(50);
const HISTORY_LIMIT: usize = 24;
const PANEL_BG: Color = Color::Reset;
const PANEL_INK: Color = Color::Reset;
const PANEL_TEXT: Color = Color::Reset;
const PANEL_BORDER: Color = Color::Rgb(216, 145, 53);
const PANEL_LINE: Color = Color::DarkGray;
const PANEL_MUTED: Color = Color::Gray;
const PANEL_SELECTED: Color = Color::Rgb(42, 42, 38);
const PANEL_READY: Color = Color::Rgb(142, 163, 122);
const PANEL_WARN: Color = Color::Rgb(216, 145, 53);
const PANEL_ERROR: Color = Color::Rgb(196, 85, 48);
const ACTIVITY_ITEM_HISTORY_LIMIT: usize = 200;
const FILE_CHANGE_HISTORY_LIMIT: usize = 100;
const DEFAULT_TRANSCRIPT_PAGE_SCROLL: usize = 8;
const DEFAULT_ACTIVITY_PAGE_SCROLL: usize = 8;
const RUN_PROMPT_MIN_HEIGHT: u16 = 4;
const RUN_PROMPT_MAX_HEIGHT: u16 = 8;
const RUN_PROMPT_CHROME_HEIGHT: u16 = 2;
const KV_LABEL_WIDTH: usize = 16;
const LOCAL_CLI_CHANNEL_ID: &str = "local-cli";
const PROJECT_SESSION_LIMIT: usize = 5;
const AUDIT_EVENT_LIMIT: usize = 8;
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct VerticalScroll {
    offset: usize,
    limit: usize,
    page_size: usize,
    follow_tail: bool,
}

impl VerticalScroll {
    fn top(page_size: usize) -> Self {
        Self {
            offset: 0,
            limit: 0,
            page_size,
            follow_tail: false,
        }
    }

    fn tail(page_size: usize) -> Self {
        Self {
            offset: 0,
            limit: 0,
            page_size,
            follow_tail: true,
        }
    }

    fn reset_top(&mut self) {
        self.offset = 0;
        self.limit = 0;
        self.follow_tail = false;
    }

    fn reset_tail(&mut self) {
        self.offset = 0;
        self.limit = 0;
        self.follow_tail = true;
    }

    fn scroll_up(&mut self, amount: usize) {
        self.offset = self.offset.saturating_sub(amount);
        self.follow_tail = false;
    }

    fn scroll_down(&mut self, amount: usize) {
        self.offset = self.offset.saturating_add(amount).min(self.limit);
        self.follow_tail = self.offset == self.limit;
    }

    fn scroll_to_top(&mut self) {
        self.offset = 0;
        self.follow_tail = false;
    }

    fn scroll_to_bottom(&mut self) {
        self.offset = self.limit;
        self.follow_tail = true;
    }

    fn set_viewport(&mut self, line_count: usize, viewport_height: u16) {
        self.page_size = usize::from(viewport_height.max(1));
        self.limit = vertical_scroll_limit(line_count, viewport_height);
        if self.follow_tail {
            self.offset = self.limit;
        } else {
            self.offset = self.offset.min(self.limit);
        }
    }
}

pub(crate) struct RunConsoleInvocation<'a> {
    pub(crate) target: &'a TargetContext,
    pub(crate) requested_runtime: Option<String>,
    pub(crate) continue_last_session: bool,
    pub(crate) timeout_override: Option<RuntimeTurnTimeouts>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ConsoleLaunchOptions {
    requested_runtime: Option<String>,
    continue_last_session: bool,
    timeout_override: Option<RuntimeTurnTimeouts>,
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
    turn_timeout: String,
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

    fn workspace_display(&self) -> &str {
        match self.workspace.as_str() {
            "rw" => "read-write",
            "ro" => "read-only",
            other => other,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BoundaryDisplayRow {
    label: &'static str,
    value: String,
}

impl BoundaryDisplayRow {
    fn new(label: &'static str, value: impl Into<String>) -> Self {
        Self {
            label,
            value: value.into(),
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
    FileChange,
    Progress,
    Status,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActivityItem {
    kind: ActivityItemKind,
    text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileChangeStatus {
    Editing,
    Edited,
    Failed,
    Declined,
    Unknown,
}

impl FileChangeStatus {
    fn label(self) -> &'static str {
        match self {
            Self::Editing => "editing",
            Self::Edited => "edited",
            Self::Failed => "failed",
            Self::Declined => "declined",
            Self::Unknown => "changed",
        }
    }

    fn from_stream(status: &StreamFileChangeStatusDto) -> Self {
        match status {
            StreamFileChangeStatusDto::Editing => Self::Editing,
            StreamFileChangeStatusDto::Edited => Self::Edited,
            StreamFileChangeStatusDto::Failed => Self::Failed,
            StreamFileChangeStatusDto::Declined => Self::Declined,
            StreamFileChangeStatusDto::Changed => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileChangeSummary {
    runtime: String,
    status: FileChangeStatus,
    paths: Vec<String>,
    total_count: usize,
}

impl FileChangeSummary {
    fn from_stream(change: &StreamFileChangeDto) -> Self {
        let paths = change
            .paths
            .iter()
            .filter(|path| !path.trim().is_empty())
            .cloned()
            .collect::<Vec<_>>();
        Self {
            runtime: change.runtime.clone(),
            status: FileChangeStatus::from_stream(&change.status),
            total_count: change.total_count.max(paths.len()),
            paths,
        }
    }

    fn path_count(&self) -> usize {
        self.total_count
    }

    fn hidden_count(&self) -> usize {
        self.total_count.saturating_sub(self.paths.len())
    }

    fn label(&self) -> String {
        if self.paths.is_empty() {
            format!(
                "{} {} {} file{}",
                self.runtime,
                self.status.label(),
                self.total_count,
                plural_s(self.total_count)
            )
        } else {
            let hidden_count = self.hidden_count();
            let suffix = if hidden_count > 0 {
                format!(" +{hidden_count}")
            } else {
                String::new()
            };
            format!(
                "{} {}: {}{}",
                self.runtime,
                self.status.label(),
                self.paths.join(", "),
                suffix
            )
        }
    }
}

#[derive(Debug)]
struct ActivitySummary {
    status: ActivityStatus,
    event_count: usize,
    command_count: usize,
    file_change_count: usize,
    progress_count: usize,
    started_at: Option<Instant>,
    ended_at: Option<Instant>,
    last_event_at: Option<Instant>,
    items: Vec<ActivityItem>,
    file_changes: Vec<FileChangeSummary>,
    open_progress_item: Option<usize>,
    open_progress_text: String,
}

impl ActivitySummary {
    fn new() -> Self {
        Self {
            status: ActivityStatus::Idle,
            event_count: 0,
            command_count: 0,
            file_change_count: 0,
            progress_count: 0,
            started_at: None,
            ended_at: None,
            last_event_at: None,
            items: Vec::new(),
            file_changes: Vec::new(),
            open_progress_item: None,
            open_progress_text: String::new(),
        }
    }

    fn start(&mut self) {
        self.status = ActivityStatus::Running;
        self.event_count = 0;
        self.command_count = 0;
        self.file_change_count = 0;
        self.progress_count = 0;
        self.started_at = Some(Instant::now());
        self.ended_at = None;
        self.last_event_at = None;
        self.items.clear();
        self.file_changes.clear();
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
        self.status == ActivityStatus::Idle
            && self.event_count == 0
            && self.items.is_empty()
            && self.file_changes.is_empty()
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
        self.last_event_at = Some(Instant::now());
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
            (StreamEventKindDto::FileChange, _, _) => {
                self.close_progress_item();
                if let Some(file_change) = event
                    .file_change
                    .as_ref()
                    .map(FileChangeSummary::from_stream)
                {
                    self.record_file_change(file_change);
                } else if let Some(text) = event.text.as_deref() {
                    self.push_item(ActivityItemKind::FileChange, normalize_activity_text(text));
                }
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
            (StreamEventKindDto::Error, _, Some(text))
                if is_non_failure_terminal_code(event.code.as_deref()) =>
            {
                self.close_progress_item();
                self.push_item(ActivityItemKind::Status, normalize_activity_text(text));
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

    fn record_file_change(&mut self, file_change: FileChangeSummary) {
        self.file_change_count = self
            .file_change_count
            .saturating_add(file_change.path_count());
        let text = file_change.label();
        self.file_changes.push(file_change);
        let overflow = self
            .file_changes
            .len()
            .saturating_sub(FILE_CHANGE_HISTORY_LIMIT);
        if overflow > 0 {
            self.file_changes.drain(0..overflow);
        }
        self.push_item(ActivityItemKind::FileChange, text);
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
        let overflow = self.items.len().saturating_sub(ACTIVITY_ITEM_HISTORY_LIMIT);
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
enum InspectorSubject {
    Selection,
    Runtime,
    Boundary,
    Activity,
    Audit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlPane {
    Project,
    Inspector(InspectorSubject),
    Files,
}

const CONTROL_PANES: &[ControlPane] = &[
    ControlPane::Project,
    ControlPane::Inspector(InspectorSubject::Selection),
    ControlPane::Inspector(InspectorSubject::Runtime),
    ControlPane::Inspector(InspectorSubject::Boundary),
    ControlPane::Inspector(InspectorSubject::Activity),
    ControlPane::Inspector(InspectorSubject::Audit),
    ControlPane::Files,
];

impl InspectorSubject {
    fn label(self, app: &ConsoleApp) -> &'static str {
        match self {
            Self::Selection => match app.project_cursor {
                ProjectSelection::Instance(_) => "Instance",
                ProjectSelection::Session(_) => "Session",
            },
            Self::Runtime => "Runtime",
            Self::Boundary => "Boundary",
            Self::Activity => "Activity",
            Self::Audit => "Audit",
        }
    }
}

impl ControlPane {
    fn label(self, app: &ConsoleApp) -> &'static str {
        match self {
            Self::Project => "Project",
            Self::Inspector(subject) => subject.label(app),
            Self::Files => "Files",
        }
    }
}

#[derive(Clone)]
struct ReadyInstance {
    summary: InstanceSummary,
    runtime_id: String,
    runtime_kind: String,
    runtime_executable: String,
    runtime_model: Option<String>,
    runtime_agent: Option<String>,
    runtime_override: Option<String>,
    boundary: BoundarySummary,
    kernel: Kernel,
    session_id: Uuid,
    peer_id: String,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectSessionItem {
    session_id: Uuid,
    turn_count: u64,
    current: bool,
}

impl ProjectSessionItem {
    fn label(&self) -> String {
        if self.current {
            "current".to_string()
        } else {
            short_session_id(self.session_id)
        }
    }

    fn detail(&self) -> String {
        turn_count_label(self.turn_count)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProjectObjectSection<T> {
    Ready(Vec<T>),
    Empty(String),
    Unavailable(String),
    Error(String),
}

impl<T> ProjectObjectSection<T> {
    fn ready(items: Vec<T>, empty_message: impl Into<String>) -> Self {
        if items.is_empty() {
            Self::Empty(empty_message.into())
        } else {
            Self::Ready(items)
        }
    }
}

impl ConsoleLaunchOptions {
    fn from_invocation(invocation: &RunConsoleInvocation<'_>) -> Self {
        Self {
            requested_runtime: invocation.requested_runtime.clone(),
            continue_last_session: invocation.continue_last_session,
            timeout_override: invocation.timeout_override,
        }
    }

    async fn open_instance(
        &self,
        project_root: Option<&Path>,
        summary: InstanceSummary,
    ) -> SelectedInstanceState {
        open_selected_instance(project_root, summary, self).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectObjects {
    sessions: ProjectObjectSection<ProjectSessionItem>,
}

impl Default for ProjectObjects {
    fn default() -> Self {
        Self {
            sessions: ProjectObjectSection::Empty("No sessions yet".to_string()),
        }
    }
}

impl ProjectObjects {
    fn unavailable(message: impl Into<String>) -> Self {
        Self {
            sessions: ProjectObjectSection::Unavailable(message.into()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectSelection {
    Instance(usize),
    Session(Uuid),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AuditEventItem {
    event_type: String,
    actor: Option<String>,
    session_id: Option<Uuid>,
    timestamp: String,
    summary: String,
}

impl AuditEventItem {
    fn from_view(event: AuditEventView) -> Self {
        let summary = audit_event_summary(&event);
        Self {
            event_type: event.event_type,
            actor: event.actor,
            session_id: event.session_id,
            timestamp: event.timestamp.format("%H:%M:%S UTC").to_string(),
            summary,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AuditTrail {
    Empty,
    Unavailable(String),
    Ready(Vec<AuditEventItem>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KeyHint {
    key: &'static str,
    label: &'static str,
    description: &'static str,
}

impl KeyHint {
    const fn new(key: &'static str, label: &'static str, description: &'static str) -> Self {
        Self {
            key,
            label,
            description,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GlobalCommand {
    Commands,
    FocusControls,
    ToggleMaximize,
    NextFocus,
    PreviousFocus,
    InterruptOrConfirmExit,
    Exit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KeyChord {
    code: KeyCode,
    modifiers: KeyModifiers,
}

impl KeyChord {
    const fn new(code: KeyCode, modifiers: KeyModifiers) -> Self {
        Self { code, modifiers }
    }

    fn matches(self, key: KeyEvent) -> bool {
        key.code == self.code && key.modifiers == self.modifiers
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GlobalKeyBinding {
    chord: KeyChord,
    hint: KeyHint,
    command: GlobalCommand,
    show_in_footer: bool,
}

impl GlobalKeyBinding {
    const fn new(
        chord: KeyChord,
        hint: KeyHint,
        command: GlobalCommand,
        show_in_footer: bool,
    ) -> Self {
        Self {
            chord,
            hint,
            command,
            show_in_footer,
        }
    }
}

const GLOBAL_KEY_BINDINGS: &[GlobalKeyBinding] = &[
    GlobalKeyBinding::new(
        KeyChord::new(KeyCode::Char('p'), KeyModifiers::CONTROL),
        KeyHint::new("Ctrl+P", "Commands", "open commands and help"),
        GlobalCommand::Commands,
        true,
    ),
    GlobalKeyBinding::new(
        KeyChord::new(KeyCode::Char('o'), KeyModifiers::CONTROL),
        KeyHint::new("Ctrl+O", "Controls", "focus the control panes"),
        GlobalCommand::FocusControls,
        true,
    ),
    GlobalKeyBinding::new(
        KeyChord::new(KeyCode::Char('x'), KeyModifiers::CONTROL),
        KeyHint::new("Ctrl+X", "Max", "maximize run or active control pane"),
        GlobalCommand::ToggleMaximize,
        false,
    ),
    GlobalKeyBinding::new(
        KeyChord::new(KeyCode::Tab, KeyModifiers::NONE),
        KeyHint::new("Tab", "Focus", "move between run and controls"),
        GlobalCommand::NextFocus,
        true,
    ),
    GlobalKeyBinding::new(
        KeyChord::new(KeyCode::BackTab, KeyModifiers::NONE),
        KeyHint::new("Shift+Tab", "Previous", "move to the previous surface"),
        GlobalCommand::PreviousFocus,
        false,
    ),
    GlobalKeyBinding::new(
        KeyChord::new(KeyCode::Char('c'), KeyModifiers::CONTROL),
        KeyHint::new(
            "Ctrl+C",
            "Interrupt",
            "interrupt an active turn; confirm exit when idle",
        ),
        GlobalCommand::InterruptOrConfirmExit,
        true,
    ),
    GlobalKeyBinding::new(
        KeyChord::new(KeyCode::Char('d'), KeyModifiers::CONTROL),
        KeyHint::new("Ctrl+D", "Exit", "exit when idle"),
        GlobalCommand::Exit,
        true,
    ),
];

const HELP_CONTEXT_KEY_HINTS: &[KeyHint] = &[
    KeyHint::new(
        "Enter",
        "Submit / Open",
        "submit the prompt or activate a project item",
    ),
    KeyHint::new("Shift+Enter", "Newline", "insert a prompt newline"),
    KeyHint::new("Alt+Enter", "Newline", "insert a prompt newline"),
    KeyHint::new(
        "Up / Down",
        "Move",
        "scroll the run transcript, edit the prompt, or move a project item",
    ),
    KeyHint::new(
        "PageUp / PageDown",
        "Page",
        "scroll Run or activity by a page",
    ),
    KeyHint::new(
        "Left / Right",
        "Controls",
        "cycle project, runtime, boundary, activity, audit, and files",
    ),
    KeyHint::new(
        "Esc",
        "Cancel",
        "close overlays, leave controls, or clear the prompt",
    ),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Focus {
    Run,
    Controls,
}

impl Focus {
    fn label(self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Controls => "controls",
        }
    }

    fn next(self) -> Self {
        match self {
            Self::Run => Self::Controls,
            Self::Controls => Self::Run,
        }
    }

    fn previous(self) -> Self {
        match self {
            Self::Run => Self::Controls,
            Self::Controls => Self::Run,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Surface {
    Run,
    Controls,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ViewMode {
    Normal,
    Maximized(Surface),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScrollTarget {
    Transcript,
    Activity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Overlay {
    Help,
    ExitConfirm,
    InstanceSwitchConfirm { target_index: usize },
    SessionSwitchConfirm { session_id: Uuid },
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
        input.set_style(Style::default().fg(PANEL_TEXT).bg(PANEL_BG));
        input.set_cursor_line_style(Style::default().bg(PANEL_BG));
        input.set_cursor_style(
            Style::default()
                .fg(PANEL_BORDER)
                .bg(PANEL_BG)
                .add_modifier(Modifier::REVERSED),
        );
        input.set_styled_placeholder(Line::from(vec![Span::styled(
            "Ask through the selected runtime...",
            Style::default().fg(PANEL_MUTED).bg(PANEL_BG),
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

    fn is_blank(&self) -> bool {
        self.text().trim().is_empty()
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
    project_objects: ProjectObjects,
    project_cursor: ProjectSelection,
    project_list_state: ListState,
    launch: ConsoleLaunchOptions,
    focus: Focus,
    control_pane: ControlPane,
    view_mode: ViewMode,
    overlay: Option<Overlay>,
    composer: ConsoleComposer,
    transcript: Vec<TranscriptLine>,
    transcript_scroll: VerticalScroll,
    active_turn_anchor: Option<usize>,
    activity: ActivitySummary,
    activity_scroll: VerticalScroll,
    audit: AuditTrail,
    inspector_subject: InspectorSubject,
    status: String,
    active_turn: Option<JoinHandle<()>>,
    active_turn_cancel: Option<TurnCancellation>,
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
        let launch = ConsoleLaunchOptions::from_invocation(&invocation);
        let selected = launch
            .open_instance(project_root.as_deref(), selected_summary)
            .await;
        let saw_ready_instance = selected.is_ready();
        let project_objects = load_project_objects(&selected).await;
        let audit = load_audit_events(&selected).await;
        let mut app = Self {
            project_root,
            instances,
            selected_index,
            selected,
            project_objects,
            project_cursor: ProjectSelection::Instance(selected_index),
            project_list_state: ListState::default(),
            launch,
            focus: Focus::Run,
            control_pane: ControlPane::Project,
            view_mode: ViewMode::Normal,
            overlay: None,
            composer: ConsoleComposer::new(),
            transcript: Vec::new(),
            transcript_scroll: VerticalScroll::top(DEFAULT_TRANSCRIPT_PAGE_SCROLL),
            active_turn_anchor: None,
            activity: ActivitySummary::new(),
            activity_scroll: VerticalScroll::tail(DEFAULT_ACTIVITY_PAGE_SCROLL),
            audit,
            inspector_subject: InspectorSubject::Selection,
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

    fn project_selections(&self) -> Vec<ProjectSelection> {
        let mut selections = (0..self.instances.len())
            .map(ProjectSelection::Instance)
            .collect::<Vec<_>>();
        if let ProjectObjectSection::Ready(sessions) = &self.project_objects.sessions {
            selections.extend(
                sessions
                    .iter()
                    .map(|session| ProjectSelection::Session(session.session_id)),
            );
        }
        selections
    }

    fn project_cursor_label(&self) -> String {
        match self.project_cursor {
            ProjectSelection::Instance(index) => self
                .instances
                .get(index)
                .map(|instance| format!("instance {}", instance.display_name()))
                .unwrap_or_else(|| "project item".to_string()),
            ProjectSelection::Session(session_id) => self
                .project_session(session_id)
                .map(|session| format!("session {}", session.label()))
                .unwrap_or_else(|| format!("session {}", short_session_id(session_id))),
        }
    }

    fn ensure_project_cursor(&mut self) {
        let selections = self.project_selections();
        if selections.contains(&self.project_cursor) {
            return;
        }
        let fallback_index = self
            .selected_index
            .min(self.instances.len().saturating_sub(1));
        self.project_cursor = ProjectSelection::Instance(fallback_index);
    }

    fn move_project_cursor(&mut self, delta: isize) {
        let selections = self.project_selections();
        if selections.is_empty() {
            return;
        }
        let current = selections
            .iter()
            .position(|selection| *selection == self.project_cursor)
            .unwrap_or_else(|| {
                selections
                    .iter()
                    .position(|selection| {
                        *selection == ProjectSelection::Instance(self.selected_index)
                    })
                    .unwrap_or(0)
            });
        let next = if delta < 0 {
            current.saturating_sub((-delta) as usize)
        } else {
            current
                .saturating_add(delta as usize)
                .min(selections.len() - 1)
        };
        let Some(selection) = selections.get(next).copied() else {
            return;
        };
        self.project_cursor = selection;
        self.status = format!("project: {}", self.project_cursor_label());
    }

    fn scroll_transcript_up(&mut self, amount: usize) {
        self.transcript_scroll.scroll_up(amount);
    }

    fn scroll_transcript_down(&mut self, amount: usize) {
        self.transcript_scroll.scroll_down(amount);
    }

    fn scroll_transcript_to_top(&mut self) {
        self.transcript_scroll.scroll_to_top();
    }

    fn scroll_transcript_to_bottom(&mut self) {
        self.transcript_scroll.scroll_to_bottom();
    }

    fn set_transcript_viewport(&mut self, line_count: usize, viewport_height: u16) {
        self.transcript_scroll
            .set_viewport(line_count, viewport_height);
    }

    fn scroll_activity_up(&mut self, amount: usize) {
        self.activity_scroll.scroll_up(amount);
    }

    fn scroll_activity_down(&mut self, amount: usize) {
        self.activity_scroll.scroll_down(amount);
    }

    fn scroll_activity_to_top(&mut self) {
        self.activity_scroll.scroll_to_top();
    }

    fn scroll_activity_to_bottom(&mut self) {
        self.activity_scroll.scroll_to_bottom();
    }

    fn set_activity_viewport(&mut self, line_count: usize, viewport_height: u16) {
        self.activity_scroll
            .set_viewport(line_count, viewport_height);
    }

    fn open_control_pane(&mut self, pane: ControlPane) {
        match pane {
            ControlPane::Project => self.inspector_subject = InspectorSubject::Selection,
            ControlPane::Inspector(subject) => self.inspector_subject = subject,
            ControlPane::Files => {}
        }
        self.control_pane = pane;
        self.focus = Focus::Controls;
        self.view_mode = match self.view_mode {
            ViewMode::Maximized(Surface::Controls) => ViewMode::Maximized(Surface::Controls),
            _ => ViewMode::Normal,
        };
        self.status = format!("controls: {}", pane.label(self));
    }

    fn focus_controls(&mut self) {
        self.focus = Focus::Controls;
        if self.view_mode != ViewMode::Maximized(Surface::Controls) {
            self.view_mode = ViewMode::Normal;
        }
        self.status = format!("controls: {}", self.control_pane.label(self));
    }

    fn leave_controls(&mut self) {
        if self.focus == Focus::Controls {
            self.focus = Focus::Run;
            if self.view_mode == ViewMode::Maximized(Surface::Controls) {
                self.view_mode = ViewMode::Normal;
            }
            self.status = "focus: run".to_string();
        }
    }

    fn next_control_pane(&mut self) {
        self.move_control_pane(1);
    }

    fn previous_control_pane(&mut self) {
        self.move_control_pane(-1);
    }

    fn move_control_pane(&mut self, delta: isize) {
        let current = CONTROL_PANES
            .iter()
            .position(|pane| *pane == self.control_pane)
            .unwrap_or(0);
        let len = CONTROL_PANES.len() as isize;
        let next = (current as isize + delta).rem_euclid(len) as usize;
        let pane = CONTROL_PANES
            .get(next)
            .copied()
            .unwrap_or(ControlPane::Project);
        self.open_control_pane(pane);
    }

    fn toggle_maximize(&mut self) {
        let surface = if self.focus == Focus::Controls {
            Surface::Controls
        } else {
            Surface::Run
        };
        self.view_mode = match self.view_mode {
            ViewMode::Maximized(current) if current == surface => ViewMode::Normal,
            _ => ViewMode::Maximized(surface),
        };
        self.status = match self.view_mode {
            ViewMode::Normal => "normal layout".to_string(),
            ViewMode::Maximized(Surface::Run) => "run maximized".to_string(),
            ViewMode::Maximized(Surface::Controls) => "controls maximized".to_string(),
        };
    }

    fn selected_scroll_target(&self) -> ScrollTarget {
        if self.focus == Focus::Controls {
            match self.control_pane {
                ControlPane::Inspector(InspectorSubject::Activity) => ScrollTarget::Activity,
                _ => ScrollTarget::Transcript,
            }
        } else {
            ScrollTarget::Transcript
        }
    }

    fn run_prompt_height(&self, available_height: u16) -> u16 {
        let max_height = available_height
            .saturating_sub(2)
            .min(RUN_PROMPT_MAX_HEIGHT);
        if max_height == 0 {
            return 0;
        }

        let content_height = self.composer.line_count().min(usize::from(u16::MAX)) as u16;
        let desired = content_height
            .saturating_add(RUN_PROMPT_CHROME_HEIGHT)
            .clamp(RUN_PROMPT_MIN_HEIGHT, RUN_PROMPT_MAX_HEIGHT);
        desired
            .min(max_height)
            .max(RUN_PROMPT_MIN_HEIGHT.min(max_height))
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

    fn boundary_summary(&self) -> BoundarySummary {
        match &self.selected {
            SelectedInstanceState::Ready(ready) => ready.boundary.clone(),
            SelectedInstanceState::Blocked { .. } => BoundarySummary {
                workspace: "blocked".to_string(),
                network: "blocked".to_string(),
                secrets: "blocked".to_string(),
                turn_timeout: "-".to_string(),
                preset: "-".to_string(),
            },
        }
    }

    async fn activate_project_cursor(&mut self) {
        self.ensure_project_cursor();
        match self.project_cursor {
            ProjectSelection::Instance(index) => self.request_instance_switch(index),
            ProjectSelection::Session(session_id) => self.request_session_switch(session_id).await,
        }
    }

    fn request_instance_switch(&mut self, next_index: usize) {
        if self.active() {
            self.status = "finish the active turn before switching instances".to_string();
            return;
        }
        if next_index >= self.instances.len() {
            return;
        }
        if next_index == self.selected_index {
            self.status = format!("already on {}", self.selected_name());
            return;
        }

        let target_name = self
            .instances
            .get(next_index)
            .map(|summary| summary.display_name().to_string())
            .unwrap_or_else(|| "selected instance".to_string());
        self.overlay = Some(Overlay::InstanceSwitchConfirm {
            target_index: next_index,
        });
        self.status = format!("confirm switch to {target_name}");
    }

    async fn request_session_switch(&mut self, session_id: Uuid) {
        if self.active() {
            self.status = "finish the active turn before switching sessions".to_string();
            return;
        }
        let Some(ready) = self.ready_instance() else {
            self.status = "launch is blocked for the selected instance".to_string();
            return;
        };
        if ready.session_id == session_id {
            self.status = "already on this session".to_string();
            return;
        }
        if self.project_session(session_id).is_none() {
            self.status = "session is no longer available".to_string();
            self.ensure_project_cursor();
            return;
        }
        if !self.composer.text().trim().is_empty() {
            self.overlay = Some(Overlay::SessionSwitchConfirm { session_id });
            self.status = "confirm session switch".to_string();
            return;
        }
        self.switch_selected_session(session_id, false).await;
    }

    async fn switch_selected_confirmed(&mut self, next_index: usize) {
        if self.active() {
            self.status = "finish the active turn before switching instances".to_string();
            return;
        }
        if next_index >= self.instances.len() || next_index == self.selected_index {
            return;
        }

        self.selected_index = next_index;
        self.project_cursor = ProjectSelection::Instance(next_index);
        let summary = self
            .instances
            .get(next_index)
            .cloned()
            .unwrap_or_else(|| self.selected.summary().clone());
        self.status = format!("selected {}", summary.display_name());
        self.selected = self
            .launch
            .open_instance(self.project_root.as_deref(), summary)
            .await;
        if self.selected.is_ready() {
            self.saw_ready_instance = true;
        }
        self.refresh_project_objects().await;
        self.refresh_audit().await;
        self.composer.clear();
        self.transcript.clear();
        self.transcript_scroll.reset_top();
        self.active_turn_anchor = None;
        self.activity = ActivitySummary::new();
        self.activity_scroll.reset_tail();
        self.inspector_subject = InspectorSubject::Selection;
        self.load_selected_history().await;
    }

    async fn switch_selected_session(&mut self, session_id: Uuid, clear_composer: bool) {
        if self.active() {
            self.status = "finish the active turn before switching sessions".to_string();
            return;
        }
        let Some(session) = self.project_session(session_id).cloned() else {
            self.status = "session is no longer available".to_string();
            self.ensure_project_cursor();
            return;
        };
        let SelectedInstanceState::Ready(ready) = &mut self.selected else {
            self.status = "launch is blocked for the selected instance".to_string();
            return;
        };
        if ready.session_id == session_id {
            self.status = "already on this session".to_string();
            return;
        }

        ready.session_id = session_id;
        if clear_composer {
            self.composer.clear();
        }
        self.transcript.clear();
        self.transcript_scroll.reset_top();
        self.active_turn_anchor = None;
        self.activity = ActivitySummary::new();
        self.activity_scroll.reset_tail();
        self.inspector_subject = InspectorSubject::Selection;
        self.refresh_project_objects().await;
        self.refresh_audit().await;
        self.project_cursor = ProjectSelection::Session(session_id);
        self.load_selected_history().await;
        self.status = format!("selected session {}", session.label());
    }

    async fn refresh_project_objects(&mut self) {
        self.project_objects = load_project_objects(&self.selected).await;
        self.ensure_project_cursor();
    }

    async fn refresh_audit(&mut self) {
        self.audit = load_audit_events(&self.selected).await;
    }

    fn project_session(&self, session_id: Uuid) -> Option<&ProjectSessionItem> {
        match &self.project_objects.sessions {
            ProjectObjectSection::Ready(sessions) => sessions
                .iter()
                .find(|session| session.session_id == session_id),
            ProjectObjectSection::Empty(_)
            | ProjectObjectSection::Unavailable(_)
            | ProjectObjectSection::Error(_) => None,
        }
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
                self.status = "prompt is empty".to_string();
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

        self.begin_run_turn(prompt.clone(), format!("running turn on {instance_name}"));
        let (handle, cancellation) = spawn_streamed_turn(
            kernel,
            session_id,
            runtime_id,
            StreamedSubmission::Prompt(prompt),
            backend_tx.clone(),
        );
        self.active_turn = Some(handle);
        self.active_turn_cancel = Some(cancellation);
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

        self.begin_run_turn(label, format!("running {label}"));
        let (handle, cancellation) = spawn_streamed_turn(
            kernel,
            session_id,
            runtime_id,
            StreamedSubmission::Action(action),
            backend_tx.clone(),
        );
        self.active_turn = Some(handle);
        self.active_turn_cancel = Some(cancellation);
    }

    fn start_reset(&mut self, backend_tx: &mpsc::UnboundedSender<BackendEvent>) {
        let Some(ready) = self.ready_instance() else {
            self.status = "launch is blocked for the selected instance".to_string();
            return;
        };
        let kernel = ready.kernel.clone();
        let session_id = ready.session_id;

        self.begin_run_turn("/lionclaw reset", "resetting session");
        let backend_tx = backend_tx.clone();
        self.active_turn = Some(tokio::spawn(async move {
            let result = kernel
                .session_action(SessionActionRequest::ResetSession { session_id })
                .await
                .map(|response| response.session_id)
                .map_err(|err| err.to_string());
            drop(backend_tx.send(BackendEvent::SessionReset(result)));
        }));
    }

    fn begin_run_turn(&mut self, user_text: impl Into<String>, status: impl Into<String>) {
        self.transcript
            .push(TranscriptLine::new(TranscriptLineKind::User, user_text));
        self.active_turn_anchor = self.transcript.len().checked_sub(1);
        self.transcript_scroll.reset_tail();
        self.activity.start();
        self.activity_scroll.reset_tail();
        self.show_runtime_activity_inspector();
        self.focus = Focus::Run;
        self.status = status.into();
    }

    fn show_runtime_activity_inspector(&mut self) {
        self.inspector_subject = InspectorSubject::Activity;
        if matches!(self.control_pane, ControlPane::Inspector(_)) {
            self.control_pane = ControlPane::Inspector(InspectorSubject::Activity);
        }
    }

    fn ready_instance(&self) -> Option<&ReadyInstance> {
        match &self.selected {
            SelectedInstanceState::Ready(ready) => Some(ready),
            SelectedInstanceState::Blocked { .. } => None,
        }
    }

    async fn apply_backend_event(&mut self, event: BackendEvent) {
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
                        self.refresh_project_objects().await;
                        self.refresh_audit().await;
                    }
                    Err(message) => {
                        self.activity.fail();
                        self.activity.push_item(ActivityItemKind::Error, message);
                        self.status = "turn failed".to_string();
                        self.refresh_audit().await;
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
                        self.active_turn_anchor = None;
                        self.activity.complete();
                        self.activity.push_item(
                            ActivityItemKind::Done,
                            "opened a fresh session".to_string(),
                        );
                        self.status = "idle".to_string();
                        self.refresh_project_objects().await;
                        self.refresh_audit().await;
                    }
                    Err(message) => {
                        self.activity.fail();
                        self.activity.push_item(ActivityItemKind::Error, message);
                        self.status = "reset failed".to_string();
                        self.refresh_audit().await;
                    }
                }
            }
        }
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
        project_objects: ProjectObjects::unavailable("No configured instances"),
        project_cursor: ProjectSelection::Instance(0),
        project_list_state: ListState::default(),
        launch: ConsoleLaunchOptions::default(),
        focus: Focus::Run,
        control_pane: ControlPane::Project,
        view_mode: ViewMode::Normal,
        overlay: None,
        composer: ConsoleComposer::new(),
        transcript: Vec::new(),
        transcript_scroll: VerticalScroll::top(DEFAULT_TRANSCRIPT_PAGE_SCROLL),
        active_turn_anchor: None,
        activity: ActivitySummary::new(),
        activity_scroll: VerticalScroll::tail(DEFAULT_ACTIVITY_PAGE_SCROLL),
        audit: AuditTrail::Unavailable("Launch blocked".to_string()),
        inspector_subject: InspectorSubject::Selection,
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
        project_objects: ProjectObjects::unavailable("No configured instances"),
        project_cursor: ProjectSelection::Instance(0),
        project_list_state: ListState::default(),
        launch: ConsoleLaunchOptions::default(),
        focus: Focus::Run,
        control_pane: ControlPane::Project,
        view_mode: ViewMode::Normal,
        overlay: None,
        composer: ConsoleComposer::new(),
        transcript: Vec::new(),
        transcript_scroll: VerticalScroll::top(DEFAULT_TRANSCRIPT_PAGE_SCROLL),
        active_turn_anchor: None,
        activity: ActivitySummary::new(),
        activity_scroll: VerticalScroll::tail(DEFAULT_ACTIVITY_PAGE_SCROLL),
        audit: AuditTrail::Unavailable("No configured instances".to_string()),
        inspector_subject: InspectorSubject::Selection,
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
    let mut guard = TerminalSetupGuard::enter_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    guard.entered_alternate_screen = true;
    execute!(stdout, EnableMouseCapture)?;
    guard.enabled_mouse_capture = true;
    execute!(stdout, EnableBracketedPaste)?;
    guard.enabled_bracketed_paste = true;
    let backend = CrosstermBackend::new(stdout);
    let terminal = Terminal::new(backend)?;
    guard.disarm();
    Ok(terminal)
}

fn leave_terminal(mut terminal: ConsoleTerminal) -> Result<()> {
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        DisableBracketedPaste,
        DisableMouseCapture,
        LeaveAlternateScreen
    )?;
    terminal.show_cursor()?;
    Ok(())
}

struct TerminalSetupGuard {
    raw_mode: bool,
    entered_alternate_screen: bool,
    enabled_bracketed_paste: bool,
    enabled_mouse_capture: bool,
}

impl TerminalSetupGuard {
    fn enter_raw_mode() -> Result<Self> {
        enable_raw_mode()?;
        Ok(Self {
            raw_mode: true,
            entered_alternate_screen: false,
            enabled_bracketed_paste: false,
            enabled_mouse_capture: false,
        })
    }

    fn disarm(mut self) {
        self.raw_mode = false;
        self.entered_alternate_screen = false;
        self.enabled_bracketed_paste = false;
        self.enabled_mouse_capture = false;
    }
}

impl Drop for TerminalSetupGuard {
    fn drop(&mut self) {
        let mut stdout = io::stdout();
        if self.enabled_bracketed_paste {
            drop(execute!(stdout, DisableBracketedPaste));
        }
        if self.enabled_mouse_capture {
            drop(execute!(stdout, DisableMouseCapture));
        }
        if self.entered_alternate_screen {
            drop(execute!(stdout, LeaveAlternateScreen));
        }
        if self.raw_mode {
            drop(disable_raw_mode());
        }
    }
}

async fn run_terminal_loop(
    terminal: &mut ConsoleTerminal,
    app: &mut ConsoleApp,
    backend_tx: &mpsc::UnboundedSender<BackendEvent>,
    backend_rx: &mut mpsc::UnboundedReceiver<BackendEvent>,
) -> Result<()> {
    loop {
        while let Ok(event) = backend_rx.try_recv() {
            app.apply_backend_event(event).await;
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

fn truncate_to(value: &str, width: usize) -> String {
    value.chars().take(width).collect()
}

fn middle_elide(value: &str, width: usize) -> String {
    let char_count = value.chars().count();
    if char_count <= width {
        return value.to_string();
    }
    if width <= 3 {
        return truncate_to(value, width);
    }

    let marker = "...";
    let visible = width - marker.len();
    let head_count = visible / 2;
    let tail_count = visible - head_count;
    let head = value.chars().take(head_count).collect::<String>();
    let tail = value
        .chars()
        .rev()
        .take(tail_count)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<String>();
    format!("{head}{marker}{tail}")
}

fn is_non_failure_terminal_code(code: Option<&str>) -> bool {
    matches!(
        code,
        Some("runtime.cancelled" | "queue.cancelled" | "runtime.interrupted" | "queue.interrupted")
    )
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

fn plural_s(count: usize) -> &'static str {
    if count == 1 {
        ""
    } else {
        "s"
    }
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

fn short_session_id(session_id: Uuid) -> String {
    session_id.to_string().chars().take(8).collect::<String>()
}

fn turn_count_label(turn_count: u64) -> String {
    match turn_count {
        1 => "1 turn".to_string(),
        count => format!("{count} turns"),
    }
}

fn audit_event_summary(event: &AuditEventView) -> String {
    match event.event_type.as_str() {
        "session.open" => format_audit_summary(
            "session opened",
            [
                audit_detail_string(&event.details, "channel_id"),
                audit_detail_string(&event.details, "peer_id"),
            ],
        ),
        "session.turn" => format_audit_summary(
            "session turn recorded",
            [
                audit_detail_string(&event.details, "runtime_id"),
                audit_detail_number(&event.details, "prompt_len")
                    .map(|value| format!("{value} chars")),
            ],
        ),
        "runtime.plan.allow" => format_audit_summary(
            "runtime plan allowed",
            [
                audit_detail_string(&event.details, "runtime_id"),
                audit_detail_string(&event.details, "effective_preset_name"),
                audit_detail_string(&event.details, "network_mode")
                    .map(|value| format!("net {value}")),
            ],
        ),
        "runtime.plan.deny" => format_audit_summary(
            "runtime plan denied",
            [audit_detail_string(&event.details, "reason"), None, None],
        ),
        "runtime.turn.start" => format_audit_summary(
            "runtime turn started",
            [
                audit_detail_string(&event.details, "runtime_id"),
                None,
                None,
            ],
        ),
        "runtime.turn.finish" => format_audit_summary(
            "runtime turn finished",
            [
                audit_detail_string(&event.details, "runtime_id"),
                audit_detail_number(&event.details, "event_count")
                    .map(|value| format!("{value} events")),
            ],
        ),
        "runtime.turn.error" => format_audit_summary(
            "runtime turn failed",
            [
                audit_detail_string(&event.details, "runtime_id"),
                audit_detail_string(&event.details, "error"),
            ],
        ),
        other => other.to_string(),
    }
}

fn format_audit_summary<const N: usize>(label: &str, details: [Option<String>; N]) -> String {
    let details = details.into_iter().flatten().collect::<Vec<_>>();
    if details.is_empty() {
        label.to_string()
    } else {
        format!("{label}: {}", details.join("  "))
    }
}

fn audit_detail_string(details: &serde_json::Value, key: &str) -> Option<String> {
    details
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn audit_detail_number(details: &serde_json::Value, key: &str) -> Option<u64> {
    details.get(key).and_then(serde_json::Value::as_u64)
}

#[cfg(test)]
mod tests;
