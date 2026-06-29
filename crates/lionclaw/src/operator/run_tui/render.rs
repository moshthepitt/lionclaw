use super::*;

pub(crate) fn render_app(frame: &mut Frame<'_>, app: &mut ConsoleApp) {
    let area = frame.area();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(1),
            Constraint::Min(10),
            Constraint::Length(1),
            Constraint::Length(3),
        ])
        .split(area);

    let [header_area, _, body_area, _, footer_area] = chunks.as_ref() else {
        return;
    };

    render_header(frame, *header_area, app);
    render_body(frame, *body_area, app);
    render_footer(frame, *footer_area, app);

    if let Some(overlay) = app.overlay {
        render_overlay(frame, area, app, overlay);
    }
}

fn render_header(frame: &mut Frame<'_>, area: Rect, app: &ConsoleApp) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(PANEL_LINE))
        .style(Style::default().bg(PANEL_BG));
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
        Span::styled("  |  ", Style::default().fg(PANEL_LINE)),
        Span::styled(app.context_label(), Style::default().fg(PANEL_TEXT)),
        Span::raw("    "),
        Span::styled(app.runtime_label(), Style::default().fg(PANEL_TEXT)),
        Span::raw("    "),
        Span::styled("net:", Style::default().fg(PANEL_MUTED)),
        Span::styled(boundary.network, Style::default().fg(PANEL_READY)),
        Span::raw("    "),
        Span::styled("secrets:", Style::default().fg(PANEL_MUTED)),
        Span::styled(
            boundary.secrets,
            Style::default().fg(if app.selected.is_ready() {
                PANEL_WARN
            } else {
                PANEL_ERROR
            }),
        ),
        Span::raw("    "),
        Span::styled(workspace, Style::default().fg(PANEL_TEXT)),
        Span::raw("    "),
        Span::styled("turn:", Style::default().fg(PANEL_MUTED)),
        Span::styled(boundary.turn_timeout, Style::default().fg(PANEL_TEXT)),
    ]);
    frame.render_widget(
        Paragraph::new(line).style(Style::default().bg(PANEL_BG)),
        Rect {
            x: area.x.saturating_add(2),
            y: area.y.saturating_add(1),
            width: area.width.saturating_sub(4),
            height: 1,
        },
    );
}

fn render_body(frame: &mut Frame<'_>, area: Rect, app: &mut ConsoleApp) {
    match app.view_mode {
        ViewMode::Maximized(Surface::Run) => {
            render_run_surface(frame, area, app);
            return;
        }
        ViewMode::Maximized(Surface::Controls) => {
            render_active_control_pane(frame, area, app);
            return;
        }
        ViewMode::Normal => {}
    }

    if wide_control_panel(area.width) {
        let project_width = project_pane_width(area.width);
        let inspector_width = inspector_pane_width(area.width);
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(project_width),
                Constraint::Length(1),
                Constraint::Min(48),
                Constraint::Length(1),
                Constraint::Length(inspector_width),
            ])
            .split(area);
        let [project_area, _, transcript_pane_area, _, inspector_area] = chunks.as_ref() else {
            render_run_surface(frame, area, app);
            return;
        };
        render_project_pane(frame, *project_area, app);
        render_run_surface(frame, *transcript_pane_area, app);
        render_side_control_stack(frame, *inspector_area, app);
    } else if compact_control_panel_visible(app) {
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Min(48),
                Constraint::Length(1),
                Constraint::Length(compact_control_pane_width(area.width)),
            ])
            .split(area);
        let [transcript_pane_area, _, control_area] = chunks.as_ref() else {
            render_run_surface(frame, area, app);
            return;
        };
        render_run_surface(frame, *transcript_pane_area, app);
        render_active_control_pane(frame, *control_area, app);
    } else {
        render_run_surface(frame, area, app);
    }
}

fn wide_control_panel(total_width: u16) -> bool {
    total_width >= 118
}

fn compact_control_panel_visible(app: &ConsoleApp) -> bool {
    app.focus == Focus::Controls || app.control_pane != ControlPane::Project
}

fn project_pane_width(total_width: u16) -> u16 {
    total_width
        .saturating_mul(23)
        .saturating_div(100)
        .clamp(28, 44)
}

fn inspector_pane_width(total_width: u16) -> u16 {
    total_width
        .saturating_mul(23)
        .saturating_div(100)
        .clamp(32, 48)
}

fn compact_control_pane_width(total_width: u16) -> u16 {
    total_width
        .saturating_mul(2)
        .saturating_div(5)
        .clamp(34, 52)
}

fn render_active_control_pane(frame: &mut Frame<'_>, area: Rect, app: &mut ConsoleApp) {
    match app.control_pane {
        ControlPane::Project => render_project_pane(frame, area, app),
        ControlPane::Inspector(subject) => render_inspector_subject(frame, area, app, subject),
        ControlPane::Files => render_files_pane(frame, area, app),
    }
}

fn render_side_control_pane(frame: &mut Frame<'_>, area: Rect, app: &mut ConsoleApp) {
    match app.control_pane {
        ControlPane::Project => {
            render_inspector_subject(frame, area, app, app.inspector_subject);
        }
        ControlPane::Inspector(subject) => render_inspector_subject(frame, area, app, subject),
        ControlPane::Files => render_files_pane(frame, area, app),
    }
}

fn render_side_control_stack(frame: &mut Frame<'_>, area: Rect, app: &mut ConsoleApp) {
    if area.height < 22 {
        render_side_control_pane(frame, area, app);
        return;
    }

    let files_focused = app.control_pane == ControlPane::Files;
    let files_height = stacked_files_pane_height(area.height, files_focused);
    let inspector_height = area.height.saturating_sub(files_height).saturating_sub(1);
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(inspector_height),
            Constraint::Length(1),
            Constraint::Length(files_height),
        ])
        .split(area);
    let [inspector_area, _, files_area] = chunks.as_ref() else {
        render_side_control_pane(frame, area, app);
        return;
    };

    render_inspector_subject(frame, *inspector_area, app, side_inspector_subject(app));
    render_files_pane(frame, *files_area, app);
}

fn stacked_files_pane_height(total_height: u16, focused: bool) -> u16 {
    let minimum_inspector_height = if focused { 8 } else { 12 };
    let maximum_files_height = total_height
        .saturating_sub(minimum_inspector_height)
        .saturating_sub(1);
    let desired_files_height = if focused {
        total_height
            .saturating_mul(3)
            .saturating_div(5)
            .clamp(12, 20)
    } else {
        total_height
            .saturating_mul(3)
            .saturating_div(10)
            .clamp(8, 14)
    };

    desired_files_height.min(maximum_files_height)
}

fn side_inspector_subject(app: &ConsoleApp) -> InspectorSubject {
    match app.control_pane {
        ControlPane::Inspector(subject) => subject,
        ControlPane::Project | ControlPane::Files => app.inspector_subject,
    }
}

struct ProjectListRow {
    item: ListItem<'static>,
    selection: Option<ProjectSelection>,
}

fn project_list_rows(app: &ConsoleApp, width: usize) -> Vec<ProjectListRow> {
    let mut rows = Vec::new();

    rows.push(project_list_heading(
        "Instances",
        Style::default().fg(PANEL_INK),
    ));
    rows.push(project_list_spacer());

    for (index, instance) in app.instances.iter().enumerate() {
        rows.push(project_list_selection_row(
            instance_project_line(
                instance,
                index == app.selected_index,
                app.selected.is_ready(),
                width,
            ),
            ProjectSelection::Instance(index),
        ));
    }

    rows.push(project_list_spacer());
    rows.push(project_list_heading(
        "─".repeat(width),
        Style::default().fg(PANEL_LINE),
    ));
    rows.push(project_list_spacer());
    rows.push(project_list_heading(
        "Sessions",
        Style::default().fg(PANEL_INK),
    ));
    rows.push(project_list_spacer());
    push_project_session_rows(&mut rows, &app.project_objects.sessions, width);

    rows
}

fn project_list_heading(text: impl Into<String>, style: Style) -> ProjectListRow {
    ProjectListRow {
        item: ListItem::new(Line::styled(text.into(), style)),
        selection: None,
    }
}

fn project_list_static_row(line: Line<'static>) -> ProjectListRow {
    ProjectListRow {
        item: ListItem::new(line),
        selection: None,
    }
}

fn project_list_spacer() -> ProjectListRow {
    ProjectListRow {
        item: ListItem::new(Line::raw("")),
        selection: None,
    }
}

fn project_list_selection_row(line: Line<'static>, selection: ProjectSelection) -> ProjectListRow {
    ProjectListRow {
        item: ListItem::new(line),
        selection: Some(selection),
    }
}

fn project_list_selected_row(
    rows: &[ProjectListRow],
    selection: ProjectSelection,
) -> Option<usize> {
    rows.iter().position(|row| row.selection == Some(selection))
}

fn instance_project_line(
    instance: &InstanceSummary,
    active: bool,
    selected_ready: bool,
    width: usize,
) -> Line<'static> {
    let selected_blocked = active && !selected_ready;
    let blocked = instance.work_root_finding.is_some() || selected_blocked;
    let icon = if blocked {
        "!"
    } else if active && selected_ready {
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
        width,
    );
    Line::styled(row, instance_row_style(state))
}

fn push_project_session_rows(
    rows: &mut Vec<ProjectListRow>,
    section: &ProjectObjectSection<ProjectSessionItem>,
    width: usize,
) {
    match section {
        ProjectObjectSection::Ready(sessions) => {
            for session in sessions {
                rows.push(project_list_selection_row(
                    session_project_line(session, width),
                    ProjectSelection::Session(session.session_id),
                ));
            }
        }
        ProjectObjectSection::Empty(message)
        | ProjectObjectSection::Unavailable(message)
        | ProjectObjectSection::Error(message) => {
            rows.push(project_list_static_row(project_object_line(
                message, "", true, width,
            )));
        }
    }
}

fn session_project_line(session: &ProjectSessionItem, width: usize) -> Line<'static> {
    project_object_line(&session.label(), &session.detail(), false, width)
}

fn project_object_line(name: &str, detail: &str, muted: bool, width: usize) -> Line<'static> {
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
    Line::styled(
        row,
        Style::default().fg(if muted { PANEL_MUTED } else { PANEL_TEXT }),
    )
}

fn render_project_pane(frame: &mut Frame<'_>, area: Rect, app: &mut ConsoleApp) {
    let content = render_panel_shell(
        frame,
        area,
        "Project",
        app.focus == Focus::Controls && app.control_pane == ControlPane::Project,
    );
    if content.width < 8 || content.height < 2 {
        return;
    }

    app.ensure_project_cursor();
    let (list_area, scrollbar_area) = split_scrollable_area(content);
    if list_area.width == 0 || list_area.height == 0 {
        return;
    }

    let rows = project_list_rows(app, list_area.width as usize);
    let row_count = rows.len();
    let selected_row = project_list_selected_row(&rows, app.project_cursor);
    app.project_list_state.select(selected_row);
    let items = rows.into_iter().map(|row| row.item).collect::<Vec<_>>();
    let list = List::new(items)
        .style(Style::default().bg(PANEL_BG))
        .highlight_style(Style::default().fg(PANEL_BORDER).bg(PANEL_SELECTED));
    frame.render_stateful_widget(list, list_area, &mut app.project_list_state);
    render_vertical_scrollbar(
        frame,
        scrollbar_area,
        row_count,
        app.project_list_state.offset(),
    );
}

fn render_run_surface(frame: &mut Frame<'_>, area: Rect, app: &mut ConsoleApp) {
    let content = render_panel_shell(frame, area, "Run", app.focus == Focus::Run);
    let prompt_height = app.run_prompt_height(content.height);
    if prompt_height == 0 {
        render_transcript_content(frame, content, app);
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(1),
            Constraint::Length(1),
            Constraint::Length(prompt_height),
        ])
        .split(content);
    let [transcript_area, divider_area, prompt_area] = chunks.as_ref() else {
        render_transcript_content(frame, content, app);
        return;
    };

    render_transcript_content(frame, *transcript_area, app);
    draw_horizontal_rule(
        frame,
        divider_area.x,
        divider_area.y,
        divider_area.width,
        PANEL_LINE,
    );
    render_run_prompt(frame, *prompt_area, app);
}

fn render_transcript_content(frame: &mut Frame<'_>, content: Rect, app: &mut ConsoleApp) {
    if let SelectedInstanceState::Blocked { blocker, .. } = &app.selected {
        let text = Text::from(launch_blocker_lines(blocker));
        frame.render_widget(
            Paragraph::new(text)
                .style(Style::default().fg(PANEL_TEXT).bg(PANEL_BG))
                .wrap(Wrap { trim: false }),
            content.inner(Margin {
                vertical: 1,
                horizontal: 1,
            }),
        );
        return;
    }

    let lines = if app.transcript.is_empty() {
        vec![Line::styled(
            "No turns yet. Submit a prompt below.",
            Style::default().fg(PANEL_MUTED),
        )]
    } else if let Some(activity) = live_activity_for_transcript(app) {
        transcript_with_activity_lines(&app.transcript, Some(activity), app.active_turn_anchor)
    } else {
        transcript_render_lines(&app.transcript)
    };
    let transcript_viewport = Rect {
        x: content.x,
        y: content.y,
        width: content.width,
        height: content.height,
    };
    let (transcript_area, scrollbar_area) = split_scrollable_area(transcript_viewport);
    if transcript_area.width == 0 || transcript_area.height == 0 {
        app.set_transcript_viewport(0, transcript_area.height);
        return;
    }
    let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false });
    let rendered_line_count = paragraph.line_count(transcript_area.width);
    app.set_transcript_viewport(rendered_line_count, transcript_area.height);
    let scroll = app.transcript_scroll.offset;
    frame.render_widget(
        paragraph
            .style(Style::default().fg(PANEL_TEXT).bg(PANEL_BG))
            .scroll((scroll.min(u16::MAX as usize) as u16, 0)),
        transcript_area,
    );
    render_vertical_scrollbar(frame, scrollbar_area, rendered_line_count, scroll);
}

fn launch_blocker_lines(blocker: &LaunchBlocker) -> Vec<Line<'static>> {
    let mut lines = vec![
        Line::styled(blocker.title.clone(), Style::default().fg(PANEL_ERROR)),
        Line::raw(""),
    ];
    lines.extend(multiline_prefixed_lines(
        "",
        "",
        Style::default().fg(PANEL_TEXT),
        &blocker.detail,
    ));
    lines.push(Line::raw(""));
    lines.extend(multiline_prefixed_lines(
        "",
        "",
        Style::default().fg(PANEL_TEXT),
        &blocker.suggestion,
    ));
    lines
}

pub(crate) fn transcript_render_lines(lines: &[TranscriptLine]) -> Vec<Line<'static>> {
    let mut rendered = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        push_transcript_line(&mut rendered, index, line);
    }
    rendered
}

pub(crate) fn transcript_with_activity_lines(
    lines: &[TranscriptLine],
    activity: Option<&ActivitySummary>,
    activity_anchor: Option<usize>,
) -> Vec<Line<'static>> {
    let mut rendered = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        push_transcript_line(&mut rendered, index, line);
        if activity_anchor == Some(index) {
            if let Some(activity) = activity {
                rendered.push(Line::raw(""));
                rendered.extend(live_activity_lines(activity));
            }
        }
    }
    rendered
}

fn push_transcript_line(rendered: &mut Vec<Line<'static>>, index: usize, line: &TranscriptLine) {
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
                Style::default().fg(PANEL_TEXT),
                &line.text,
            ));
        }
    }
}

fn live_activity_for_transcript(app: &ConsoleApp) -> Option<&ActivitySummary> {
    if app.active() || app.activity.status == ActivityStatus::Failed {
        (!app.activity.is_empty()).then_some(&app.activity)
    } else {
        None
    }
}

fn live_activity_lines(activity: &ActivitySummary) -> Vec<Line<'static>> {
    let mut lines = vec![Line::from(vec![
        Span::styled(
            "runtime",
            Style::default()
                .fg(PANEL_BORDER)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("  "),
        Span::styled(activity.status.label(), activity.status.style()),
        Span::raw(format!("  {} events", activity.event_count)),
    ])];
    let mut detail = Vec::new();
    if let Some(elapsed) = activity.elapsed_label() {
        detail.push(format!("elapsed {elapsed}"));
    }
    if let Some(last) = activity.last_event_at {
        detail.push(format!("last event {}", format_event_age(last.elapsed())));
    }
    if activity.command_count > 0 {
        detail.push(format!("{} commands", activity.command_count));
    }
    if activity.file_change_count > 0 {
        detail.push(format!(
            "{} file{}",
            activity.file_change_count,
            plural_s(activity.file_change_count)
        ));
    }
    if activity.progress_count > 0 {
        detail.push(format!("{} progress", activity.progress_count));
    }
    if !detail.is_empty() {
        lines.push(Line::styled(
            detail.join("  "),
            Style::default().fg(PANEL_MUTED),
        ));
    }
    if let Some(change) = activity.file_changes.last() {
        lines.push(Line::styled(
            format!("files  {}", change.label()),
            Style::default().fg(PANEL_TEXT),
        ));
    }
    if let Some(item) = activity.items.last() {
        lines.push(Line::styled(
            format!("latest  {}", activity_display_text(&item.text)),
            Style::default().fg(PANEL_TEXT),
        ));
    }
    lines
}

fn format_event_age(age: Duration) -> String {
    if age < Duration::from_secs(2) {
        "now".to_string()
    } else {
        format!("{} ago", format_elapsed(age))
    }
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
            let style = normalize_transcript_markdown_style(span.style);
            Span::styled(span.content.into_owned(), style)
        })
        .collect();
    Line {
        style: line.style,
        alignment: line.alignment,
        spans,
    }
}

fn normalize_transcript_markdown_style(mut style: Style) -> Style {
    if style.fg.is_none() {
        style.fg = Some(PANEL_TEXT);
    }

    if matches!(
        style.fg,
        Some(Color::Blue | Color::LightBlue | Color::Cyan | Color::LightCyan)
    ) {
        style.fg = Some(if style.add_modifier.contains(Modifier::UNDERLINED) {
            PANEL_BORDER
        } else {
            PANEL_TEXT
        });
    }

    style
}

pub(super) fn vertical_scroll_limit(line_count: usize, viewport_height: u16) -> usize {
    line_count
        .saturating_sub(viewport_height as usize)
        .min(u16::MAX as usize)
}

fn split_scrollable_area(area: Rect) -> (Rect, Rect) {
    let scrollbar_width = if area.width > 0 { 1 } else { 0 };
    let text_area = Rect {
        x: area.x,
        y: area.y,
        width: area.width.saturating_sub(scrollbar_width),
        height: area.height,
    };
    let scrollbar_area = Rect {
        x: area.x + area.width.saturating_sub(scrollbar_width),
        y: area.y,
        width: scrollbar_width,
        height: area.height,
    };
    (text_area, scrollbar_area)
}

fn render_vertical_scrollbar(frame: &mut Frame<'_>, area: Rect, line_count: usize, scroll: usize) {
    if line_count <= area.height as usize || area.width == 0 || area.height == 0 {
        return;
    }
    let position = scrollbar_position_for_pane_offset(scroll, line_count, area.height);
    let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
        .begin_symbol(Some("^"))
        .end_symbol(Some("v"))
        .thumb_style(Style::default().fg(PANEL_BORDER))
        .track_style(Style::default().fg(PANEL_LINE));
    let mut state = ScrollbarState::new(line_count)
        .viewport_content_length(area.height as usize)
        .position(position);
    frame.render_stateful_widget(scrollbar, area, &mut state);
}

pub(super) fn scrollbar_position_for_pane_offset(
    scroll: usize,
    line_count: usize,
    viewport_height: u16,
) -> usize {
    let max_offset = vertical_scroll_limit(line_count, viewport_height);
    if max_offset == 0 {
        return 0;
    }
    let max_position = line_count.saturating_sub(1);
    let numerator = (scroll.min(max_offset) as u128) * (max_position as u128);
    ((numerator + (max_offset as u128 / 2)) / max_offset as u128) as usize
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
                Span::styled(line.to_string(), Style::default().fg(PANEL_TEXT)),
            ])
        })
        .collect()
}

fn render_inspector_subject(
    frame: &mut Frame<'_>,
    area: Rect,
    app: &mut ConsoleApp,
    subject: InspectorSubject,
) {
    let title = format!("Inspector  {}", subject.label(app));
    let content = render_panel_shell(
        frame,
        area,
        &title,
        app.focus == Focus::Controls && app.control_pane == ControlPane::Inspector(subject),
    );
    if content.width < 8 || content.height < 4 {
        return;
    }

    match subject {
        InspectorSubject::Selection => render_project_cursor_inspector(frame, content, app),
        InspectorSubject::Runtime => render_runtime_inspector(frame, content, app),
        InspectorSubject::Boundary => render_boundary_inspector(frame, content, app),
        InspectorSubject::Activity => render_activity_inspector(frame, content, app),
        InspectorSubject::Audit => render_audit_inspector(frame, content, app),
    }
}

fn render_project_cursor_inspector(frame: &mut Frame<'_>, content: Rect, app: &ConsoleApp) {
    let row_width = inspector_body(content).width as usize;
    match app.project_cursor {
        ProjectSelection::Instance(index) => {
            let Some(instance) = app.instances.get(index) else {
                render_inspector_lines(
                    frame,
                    content,
                    vec![Line::styled(
                        "Project item is no longer available.",
                        Style::default().fg(PANEL_MUTED),
                    )],
                );
                return;
            };
            let active = index == app.selected_index;
            let blocked =
                instance.work_root_finding.is_some() || (active && !app.selected.is_ready());
            let state = if blocked {
                "blocked"
            } else if instance.work_root.is_some() {
                "ready"
            } else {
                "idle"
            };
            let icon = if blocked {
                "!"
            } else if active {
                "●"
            } else {
                "○"
            };
            let mut lines = vec![
                section_line(icon, if active { "Active" } else { "Instance" }),
                Line::raw(""),
                kv_line("instance", instance.display_name()),
                kv_line("state", state),
            ];
            if let Some(runtime) = instance.default_runtime.as_deref() {
                lines.push(kv_line("runtime", runtime));
            }
            if instance.is_default {
                lines.push(kv_line("default", "yes"));
            }
            if let Some(work_root) = instance.work_root.as_ref() {
                lines.push(kv_path_line("work root", work_root, row_width));
            }
            if let Some(reason) = instance.work_root_finding.as_deref() {
                lines.push(kv_line("reason", reason));
            }
            lines.push(Line::raw(""));
            lines.push(Line::styled(
                if active {
                    "Current runtime context."
                } else {
                    "Enter asks before switching context."
                },
                Style::default().fg(PANEL_MUTED),
            ));
            render_inspector_lines(frame, content, lines);
        }
        ProjectSelection::Session(session_id) => {
            let Some(session) = app.project_session(session_id) else {
                render_inspector_lines(
                    frame,
                    content,
                    vec![Line::styled(
                        "Session is no longer available.",
                        Style::default().fg(PANEL_MUTED),
                    )],
                );
                return;
            };
            let mut lines = vec![
                section_line(if session.current { "●" } else { "◷" }, "Session"),
                Line::raw(""),
                kv_line("session", &short_session_id(session.session_id)),
                kv_line(
                    "state",
                    if session.current {
                        "current"
                    } else {
                        "available"
                    },
                ),
                kv_line("turns", &session.detail()),
                Line::raw(""),
                Line::styled(
                    if session.current {
                        "Current conversation for this instance."
                    } else {
                        "Enter opens this conversation."
                    },
                    Style::default().fg(PANEL_MUTED),
                ),
            ];
            if !session.current {
                lines.push(Line::styled(
                    "Composer text is preserved unless you confirm clearing it.",
                    Style::default().fg(PANEL_MUTED),
                ));
            }
            render_inspector_lines(frame, content, lines);
        }
    }
}

fn render_inspector_lines(frame: &mut Frame<'_>, content: Rect, lines: Vec<Line<'static>>) {
    frame.render_widget(
        Paragraph::new(lines)
            .style(Style::default().fg(PANEL_TEXT).bg(PANEL_BG))
            .wrap(Wrap { trim: false }),
        inspector_body(content),
    );
}

fn render_runtime_inspector(frame: &mut Frame<'_>, content: Rect, app: &ConsoleApp) {
    let SelectedInstanceState::Ready(ready) = &app.selected else {
        render_inspector_lines(
            frame,
            content,
            vec![Line::styled(
                "Runtime unavailable while launch is blocked.",
                Style::default().fg(PANEL_MUTED),
            )],
        );
        return;
    };

    let mut lines = vec![
        section_line("◉", "Runtime"),
        Line::raw(""),
        kv_line("profile", &ready.runtime_id),
        kv_line("driver", &ready.runtime_driver),
        kv_line("executable", &ready.runtime_executable),
    ];
    if let Some(model) = ready.runtime_model.as_deref() {
        lines.push(kv_line("model", model));
    }
    if let Some(mode) = ready.runtime_mode.as_deref() {
        lines.push(kv_line("mode", mode));
    }
    lines.push(kv_line(
        "selected by",
        if ready.runtime_override.is_some() {
            "run argument"
        } else {
            "instance default"
        },
    ));
    lines.push(kv_line("session", &short_session_id(ready.session_id)));
    render_inspector_lines(frame, content, lines);
}

fn render_boundary_inspector(frame: &mut Frame<'_>, content: Rect, app: &ConsoleApp) {
    if !app.selected.is_ready() {
        render_inspector_lines(
            frame,
            content,
            vec![Line::styled(
                "Boundary unavailable while launch is blocked.",
                Style::default().fg(PANEL_MUTED),
            )],
        );
        return;
    }

    let boundary = app.boundary_summary();
    let mut lines = vec![section_line("▱", "Boundary"), Line::raw("")];
    for row in boundary_display_rows(&boundary) {
        lines.push(kv_line(row.label, &row.value));
    }
    render_inspector_lines(frame, content, lines);
}

fn render_run_prompt(frame: &mut Frame<'_>, area: Rect, app: &ConsoleApp) {
    if area.width < 4 || area.height == 0 {
        return;
    }

    let status_height = u16::from(area.height >= 3);
    let rule_height = u16::from(area.height >= 3);
    let input_height = area
        .height
        .saturating_sub(status_height)
        .saturating_sub(rule_height)
        .max(1);
    frame.render_widget(
        Paragraph::new(Line::styled(">", Style::default().fg(PANEL_BORDER)))
            .style(Style::default().bg(PANEL_BG)),
        Rect {
            x: area.x,
            y: area.y,
            width: 1,
            height: 1,
        },
    );
    frame.render_widget(
        app.composer.widget(),
        Rect {
            x: area.x.saturating_add(3),
            y: area.y,
            width: area.width.saturating_sub(3),
            height: input_height,
        },
    );

    if rule_height == 0 || status_height == 0 {
        return;
    }

    let rule_y = area.y.saturating_add(input_height);
    draw_horizontal_rule(frame, area.x, rule_y, area.width, PANEL_LINE);
    frame.render_widget(
        Paragraph::new(run_status_line(app)).style(Style::default().bg(PANEL_BG)),
        Rect {
            x: area.x,
            y: rule_y.saturating_add(1),
            width: area.width,
            height: 1,
        },
    );
}

fn run_status_line(app: &ConsoleApp) -> Line<'static> {
    let mode = if app.active() { "running" } else { "normal" };
    let mode_style = if app.active() {
        Style::default()
            .fg(PANEL_BORDER)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(PANEL_MUTED)
    };
    let mut spans = vec![Span::styled(mode, mode_style)];
    if !app.status.is_empty() {
        spans.push(Span::raw("  "));
        spans.push(Span::styled(
            app.status.clone(),
            Style::default().fg(PANEL_MUTED),
        ));
    }
    if let Some(change) = app.activity.file_changes.last() {
        spans.push(Span::raw("  "));
        spans.push(Span::styled(
            format!("files {}", change.label()),
            Style::default().fg(PANEL_TEXT),
        ));
    } else if app.active() {
        if let Some(item) = app.activity.items.last() {
            spans.push(Span::raw("  "));
            spans.push(Span::styled(
                format!("latest {}", item.text),
                Style::default().fg(PANEL_TEXT),
            ));
        }
    }
    Line::from(spans)
}

fn render_activity_inspector(frame: &mut Frame<'_>, content: Rect, app: &mut ConsoleApp) {
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
        if app.activity.file_change_count > 0 {
            lines.push(Line::raw(format!(
                "file changes {}",
                app.activity.file_change_count
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

    let viewport = content.inner(Margin {
        vertical: 1,
        horizontal: 1,
    });
    let (text_area, scrollbar_area) = split_scrollable_area(viewport);
    if text_area.width == 0 || text_area.height == 0 {
        app.set_activity_viewport(0, text_area.height);
        return;
    }
    render_scrollable_lines(
        frame,
        text_area,
        scrollbar_area,
        lines,
        &mut app.activity_scroll,
    );
}

fn render_audit_inspector(frame: &mut Frame<'_>, content: Rect, app: &ConsoleApp) {
    let lines = match &app.audit {
        AuditTrail::Ready(events) => {
            let mut lines = vec![section_line("▣", "Recent Audit"), Line::raw("")];
            for event in events {
                lines.extend(audit_event_lines(event));
            }
            lines
        }
        AuditTrail::Empty => vec![Line::styled(
            "No recent audit events for this session.",
            Style::default().fg(PANEL_MUTED),
        )],
        AuditTrail::Unavailable(message) => vec![Line::styled(
            message.clone(),
            Style::default().fg(PANEL_MUTED),
        )],
    };
    render_inspector_lines(frame, content, lines);
}

fn render_files_pane(frame: &mut Frame<'_>, area: Rect, app: &mut ConsoleApp) {
    let content = render_panel_shell(
        frame,
        area,
        "Files",
        app.focus == Focus::Controls && app.control_pane == ControlPane::Files,
    );
    let viewport = inspector_body(content);
    let (text_area, scrollbar_area) = split_scrollable_area(viewport);
    if text_area.width == 0 || text_area.height == 0 {
        app.set_files_viewport(0, text_area.height);
        return;
    }
    render_scrollable_lines(
        frame,
        text_area,
        scrollbar_area,
        file_change_pane_lines(app),
        &mut app.files_scroll,
    );
}

fn render_scrollable_lines(
    frame: &mut Frame<'_>,
    text_area: Rect,
    scrollbar_area: Rect,
    lines: Vec<Line<'static>>,
    scroll: &mut VerticalScroll,
) {
    let paragraph = Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false });
    let rendered_line_count = paragraph.line_count(text_area.width);
    scroll.set_viewport(rendered_line_count, text_area.height);
    let offset = scroll.offset;
    frame.render_widget(
        paragraph
            .style(Style::default().fg(PANEL_TEXT).bg(PANEL_BG))
            .scroll((offset.min(u16::MAX as usize) as u16, 0)),
        text_area,
    );
    render_vertical_scrollbar(frame, scrollbar_area, rendered_line_count, offset);
}

fn file_change_pane_lines(app: &ConsoleApp) -> Vec<Line<'static>> {
    if app.activity.file_changes.is_empty() {
        return vec![Line::styled(
            "No file changes reported for the current turn.",
            Style::default().fg(PANEL_MUTED),
        )];
    }

    let mut lines = vec![
        section_line("▤", "Changed Files"),
        Line::raw(""),
        kv_line("reported", &app.activity.file_change_count.to_string()),
        Line::raw(""),
    ];
    for change in &app.activity.file_changes {
        lines.push(Line::styled(
            file_change_heading(change),
            Style::default()
                .fg(PANEL_BORDER)
                .add_modifier(Modifier::BOLD),
        ));
        for path in &change.paths {
            lines.push(Line::from(vec![
                Span::styled("  ", Style::default().fg(PANEL_MUTED)),
                Span::styled(path.clone(), Style::default().fg(PANEL_TEXT)),
            ]));
        }
        let hidden_count = change.hidden_count();
        if hidden_count > 0 {
            lines.push(Line::styled(
                format!("  +{hidden_count} more"),
                Style::default().fg(PANEL_MUTED),
            ));
        }
        lines.push(Line::raw(""));
    }
    lines
}

fn file_change_heading(change: &FileChangeSummary) -> String {
    if change.path_count() > 1 {
        format!(
            "{} {} {} files",
            change.runtime,
            change.status.label(),
            change.path_count()
        )
    } else {
        format!("{} {}", change.runtime, change.status.label())
    }
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, app: &ConsoleApp) {
    frame.render_widget(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(PANEL_LINE))
            .style(Style::default().bg(PANEL_BG)),
        area,
    );
    if area.height < 3 || area.width < 4 {
        return;
    }
    let mut spans = footer_hint_spans();
    if app.focus == Focus::Controls && app.control_pane == ControlPane::Project {
        spans.push(Span::raw("  "));
        spans.push(key_span("Enter"));
        spans.push(Span::styled(
            match app.project_cursor {
                ProjectSelection::Instance(index) if index == app.selected_index => " Current",
                ProjectSelection::Instance(_) => " Switch",
                ProjectSelection::Session(_) => " Open",
            },
            Style::default().fg(PANEL_TEXT),
        ));
    }
    let line = Line::from(spans);
    frame.render_widget(
        Paragraph::new(line).style(Style::default().bg(PANEL_BG)),
        Rect {
            x: area.x.saturating_add(2),
            y: area.y.saturating_add(1),
            width: area.width.saturating_sub(4),
            height: 1,
        },
    );
}

fn inspector_body(content: Rect) -> Rect {
    content.inner(Margin {
        vertical: 1,
        horizontal: 1,
    })
}

fn render_panel_shell(frame: &mut Frame<'_>, area: Rect, title: &str, focused: bool) -> Rect {
    let border_style = if focused {
        Style::default().fg(PANEL_BORDER)
    } else {
        Style::default().fg(PANEL_LINE)
    };
    frame.render_widget(
        Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .style(Style::default().bg(PANEL_BG)),
        area,
    );
    if area.width > 12 && area.height > 3 {
        let title_style = if focused {
            Style::default()
                .fg(PANEL_BORDER)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(PANEL_MUTED)
        };
        frame.render_widget(
            Paragraph::new(Line::styled(
                if focused {
                    format!("▶ {title}")
                } else {
                    title.to_string()
                },
                title_style,
            ))
            .style(Style::default().bg(PANEL_BG)),
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
            PANEL_LINE,
        );
        return Rect {
            x: area.x.saturating_add(1),
            y: area.y.saturating_add(3),
            width: area.width.saturating_sub(2),
            height: area.height.saturating_sub(4),
        };
    }
    Rect {
        x: area.x.saturating_add(1),
        y: area.y.saturating_add(1),
        width: area.width.saturating_sub(2),
        height: area.height.saturating_sub(2),
    }
}

fn draw_horizontal_rule(frame: &mut Frame<'_>, x: u16, y: u16, width: u16, color: Color) {
    if width == 0 {
        return;
    }
    frame.render_widget(
        Paragraph::new("─".repeat(width as usize)).style(Style::default().fg(color).bg(PANEL_BG)),
        Rect {
            x,
            y,
            width,
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

fn instance_row_style(state: &str) -> Style {
    let fg = match state {
        "ready" => PANEL_READY,
        "blocked" => PANEL_ERROR,
        _ => PANEL_TEXT,
    };
    Style::default().fg(fg)
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
        Span::styled(
            format!("  {label:<KV_LABEL_WIDTH$}"),
            Style::default().fg(PANEL_MUTED),
        ),
        Span::styled(value.to_string(), Style::default().fg(PANEL_TEXT)),
    ])
}

fn kv_path_line(label: &'static str, path: &Path, row_width: usize) -> Line<'static> {
    let value_width = row_width.saturating_sub(KV_LABEL_WIDTH + 2);
    kv_line(
        label,
        &middle_elide(&path.display().to_string(), value_width),
    )
}

pub(super) fn boundary_display_rows(boundary: &BoundarySummary) -> Vec<BoundaryDisplayRow> {
    vec![
        BoundaryDisplayRow::new("workspace", boundary.workspace_display()),
        BoundaryDisplayRow::new("network", boundary.network.clone()),
        BoundaryDisplayRow::new("secrets", boundary.secrets.clone()),
        BoundaryDisplayRow::new("runtime home", "private"),
        BoundaryDisplayRow::new("skills", "read-only"),
        BoundaryDisplayRow::new("preset", boundary.preset.clone()),
        BoundaryDisplayRow::new("turn timeout", boundary.turn_timeout.clone()),
    ]
}

pub(super) fn activity_item_lines(item: &ActivityItem) -> Vec<Line<'static>> {
    let (icon, style) = match item.kind {
        ActivityItemKind::Done => ("✓", Style::default().fg(PANEL_READY)),
        ActivityItemKind::Command => ("→", Style::default().fg(PANEL_BORDER)),
        ActivityItemKind::FileChange => ("▤", Style::default().fg(PANEL_BORDER)),
        ActivityItemKind::Progress => ("•", Style::default().fg(PANEL_BORDER)),
        ActivityItemKind::Status => ("→", Style::default().fg(PANEL_MUTED)),
        ActivityItemKind::Error => ("!", Style::default().fg(PANEL_ERROR)),
    };
    let text = activity_display_text(&item.text);
    multiline_prefixed_lines(&format!("{icon}  "), "   ", style, &text)
}

fn activity_display_text(text: &str) -> String {
    let mut display = String::with_capacity(text.len());
    let mut previous = None;
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '-'
            && previous.is_some_and(|previous: char| previous.is_ascii_alphabetic())
            && chars.peek().is_some_and(|next| next.is_ascii_alphabetic())
        {
            display.push('‑');
        } else {
            display.push(ch);
        }
        previous = Some(ch);
    }
    display
}

fn audit_event_lines(event: &AuditEventItem) -> Vec<Line<'static>> {
    let (icon, style) = audit_event_icon(&event.event_type);
    let mut lines = multiline_prefixed_lines(
        &format!("{icon}  "),
        "   ",
        style,
        &format!("{}  {}", event.timestamp, event.summary),
    );
    if let Some(actor) = event.actor.as_deref() {
        lines.push(Line::styled(
            format!("   actor {actor}"),
            Style::default().fg(PANEL_MUTED),
        ));
    }
    if let Some(session_id) = event.session_id {
        lines.push(Line::styled(
            format!("   session {}", short_session_id(session_id)),
            Style::default().fg(PANEL_MUTED),
        ));
    }
    lines
}

fn audit_event_icon(event_type: &str) -> (&'static str, Style) {
    if event_type.contains("deny") || event_type.contains("error") || event_type.contains("failed")
    {
        ("!", Style::default().fg(PANEL_ERROR))
    } else {
        ("✓", Style::default().fg(PANEL_READY))
    }
}

fn key_span(key: &'static str) -> Span<'static> {
    Span::styled(
        key,
        Style::default()
            .fg(PANEL_BORDER)
            .add_modifier(Modifier::BOLD),
    )
}

pub(super) fn footer_hint_spans() -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    for binding in GLOBAL_KEY_BINDINGS
        .iter()
        .filter(|binding| binding.show_in_footer)
    {
        if !spans.is_empty() {
            spans.push(Span::raw("   "));
        }
        let hint = binding.hint;
        spans.push(key_span(hint.key));
        spans.push(Span::raw(" "));
        spans.push(Span::styled(hint.label, Style::default().fg(PANEL_TEXT)));
    }
    spans
}

fn help_overlay_lines() -> Vec<Line<'static>> {
    let mut lines = vec![
        section_line("▣", "LionClaw"),
        Line::from("/lionclaw continue"),
        Line::from("/lionclaw retry"),
        Line::from("/lionclaw reset"),
        Line::from("/lionclaw exit"),
        Line::raw(""),
    ];
    push_global_help_section(&mut lines);
    lines.push(Line::raw(""));
    push_help_section(&mut lines, "Context", HELP_CONTEXT_KEY_HINTS);
    lines
}

fn push_global_help_section(lines: &mut Vec<Line<'static>>) {
    lines.push(section_line("▣", "Global"));
    for binding in GLOBAL_KEY_BINDINGS {
        push_help_hint(lines, binding.hint);
    }
}

fn push_help_section(lines: &mut Vec<Line<'static>>, title: &'static str, hints: &[KeyHint]) {
    lines.push(section_line("▣", title));
    for hint in hints {
        push_help_hint(lines, *hint);
    }
}

fn push_help_hint(lines: &mut Vec<Line<'static>>, hint: KeyHint) {
    lines.push(Line::from(vec![
        key_span(hint.key),
        Span::raw("  "),
        Span::styled(hint.description, Style::default().fg(PANEL_TEXT)),
    ]));
}

fn render_overlay(frame: &mut Frame<'_>, area: Rect, app: &ConsoleApp, overlay: Overlay) {
    let popup = centered_rect(70, 55, area);
    frame.render_widget(Clear, popup);
    let (title, lines) = match overlay {
        Overlay::Help => (" Commands ", help_overlay_lines()),
        Overlay::ExitConfirm => (
            " Exit ",
            vec![
                Line::from("Press y to exit."),
                Line::from("Press Esc to return to the console."),
            ],
        ),
        Overlay::InstanceSwitchConfirm { target_index } => {
            let target_name = app
                .instances
                .get(target_index)
                .map(InstanceSummary::display_name)
                .unwrap_or("selected instance");
            (
                " Switch Instance ",
                vec![
                    Line::from(format!(
                        "Switch from {} to {target_name}?",
                        app.selected_name()
                    )),
                    Line::from("This changes the active runtime context, transcript, prompt, and activity."),
                    Line::from("Press y to switch."),
                    Line::from("Press Esc to stay on the current instance."),
                ],
            )
        }
        Overlay::SessionSwitchConfirm { session_id } => (
            " Switch Session ",
            vec![
                Line::from(format!("Open session {}?", short_session_id(session_id))),
                Line::from("This clears the current prompt text and loads that transcript."),
                Line::from("Press y to switch."),
                Line::from("Press Esc to stay on the current session."),
            ],
        ),
    };
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(PANEL_BORDER))
        .style(Style::default().bg(PANEL_BG));
    let paragraph = Paragraph::new(lines)
        .style(Style::default().fg(PANEL_TEXT).bg(PANEL_BG))
        .block(block)
        .wrap(Wrap { trim: true });
    frame.render_widget(paragraph, popup);
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
