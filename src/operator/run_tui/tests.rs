use super::*;
use crate::{
    contracts::{SessionHistoryPolicy, SessionOpenRequest, TrustTier},
    kernel::KernelOptions,
};
#[cfg(unix)]
use crate::{
    kernel::runtime::{ConfinementConfig, OciConfinementConfig},
    operator::config::{OperatorConfig, RuntimeProfileConfig},
};
use ratatui::backend::TestBackend;
#[cfg(unix)]
use std::path::Path;
use std::path::PathBuf;

#[cfg(unix)]
const TEST_DEFAULT_RUNTIME_ID: &str = "default";
#[cfg(unix)]
const TEST_OVERRIDE_RUNTIME_ID: &str = "override";

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
fn answer_deltas_preserve_literal_stream_text() {
    let mut transcript = vec![TranscriptLine::new(
        TranscriptLineKind::Answer,
        "decisions.",
    )];

    append_transcript_delta(&mut transcript, TranscriptLineKind::Answer, "The markdown");

    assert_eq!(
        transcript,
        vec![TranscriptLine::new(
            TranscriptLineKind::Answer,
            "decisions.The markdown",
        )]
    );

    append_transcript_delta(&mut transcript, TranscriptLineKind::Answer, "\nstd.");
    append_transcript_delta(&mut transcript, TranscriptLineKind::Answer, "io::Result");
    assert!(transcript[0].text.ends_with("\nstd.io::Result"));

    append_transcript_delta(&mut transcript, TranscriptLineKind::Answer, ".");
    append_transcript_delta(&mut transcript, TranscriptLineKind::Answer, "**Report**");
    assert!(transcript[0].text.ends_with("std.io::Result.**Report**"));
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
        project_objects: ProjectObjects::unavailable("Launch blocked"),
        project_cursor: ProjectSelection::Instance(0),
        project_list_state: ListState::default(),
        launch: ConsoleLaunchOptions::default(),
        focus: Focus::Transcript,
        overlay: None,
        composer: ConsoleComposer::new(),
        transcript: Vec::new(),
        transcript_scroll: VerticalScroll::top(DEFAULT_TRANSCRIPT_PAGE_SCROLL),
        activity: ActivitySummary::new(),
        activity_scroll: VerticalScroll::tail(DEFAULT_ACTIVITY_PAGE_SCROLL),
        audit: AuditTrail::Unavailable("not loaded".to_string()),
        inspector_subject: InspectorSubject::Selection,
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
async fn reference_sized_layout_renders_ribbon_three_panes_composer_and_footer() {
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
        default_runtime: Some("codex".to_string()),
    };
    let mut app = ConsoleApp {
        project_root: Some(PathBuf::from("/workspace/lionclaw")),
        instances: vec![main.clone(), reviewer, qa],
        selected_index: 0,
        selected: SelectedInstanceState::Ready(Box::new(ReadyInstance {
            summary: main,
            runtime_id: "codex".to_string(),
            runtime_kind: "codex".to_string(),
            runtime_executable: "codex".to_string(),
            runtime_model: Some("gpt-5".to_string()),
            runtime_agent: None,
            runtime_override: None,
            boundary: BoundarySummary {
                workspace: "rw".to_string(),
                network: "none".to_string(),
                secrets: "staged".to_string(),
                turn_timeout: "2h".to_string(),
                preset: "everyday".to_string(),
            },
            kernel,
            session_id: Uuid::new_v4(),
            peer_id: "local-project".to_string(),
        })),
        project_objects: ProjectObjects::default(),
        project_cursor: ProjectSelection::Instance(0),
        project_list_state: ListState::default(),
        launch: ConsoleLaunchOptions::default(),
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
        transcript_scroll: VerticalScroll::top(DEFAULT_TRANSCRIPT_PAGE_SCROLL),
        activity: ActivitySummary::new(),
        activity_scroll: VerticalScroll::tail(DEFAULT_ACTIVITY_PAGE_SCROLL),
        audit: AuditTrail::Unavailable("not loaded".to_string()),
        inspector_subject: InspectorSubject::Selection,
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
    assert!(rendered.contains("session"));
    assert!(rendered.contains("work root"));
    assert!(rendered.contains("turn:2h"));
    assert!(!rendered.contains("/lionclaw/skills"));
    assert!(!rendered.contains("runtime.plan.allow"));
    assert!(rendered.contains("Ask through the selected runtime"));
    assert!(!rendered.contains("runtime controls pass through"));
    assert!(rendered.contains("Ctrl+P"));
    assert!(rendered.contains("Commands"));
    assert!(rendered.contains("Ctrl+D"));
    assert_eq!(rendered.lines().count(), 50);
}

#[tokio::test]
async fn runtime_boundary_and_audit_inspectors_render_real_context() {
    let mut app = ready_test_app(Vec::new()).await;
    app.focus = Focus::Inspectors;

    app.inspector_subject = InspectorSubject::Runtime;
    let runtime = render_to_text(&mut app, 120, 32);
    assert!(runtime.contains("Inspector  Runtime"));
    assert!(runtime.contains("profile"));
    assert!(runtime.contains("codex"));
    assert!(runtime.contains("executable"));

    app.inspector_subject = InspectorSubject::Boundary;
    let boundary = render_to_text(&mut app, 120, 32);
    assert!(boundary.contains("Inspector  Boundary"));
    assert!(boundary.contains("workspace"));
    assert!(boundary.contains("read-write"));
    assert!(boundary.contains("skills"));
    assert!(boundary.contains("read-only"));
    assert!(boundary.contains("turn timeout"));
    assert!(!boundary.contains("runtime.plan.allow"));

    app.audit = AuditTrail::Ready(vec![AuditEventItem {
        event_type: "session.open".to_string(),
        actor: Some("api".to_string()),
        session_id: app.ready_instance().map(|ready| ready.session_id),
        timestamp: "12:34:56 UTC".to_string(),
        summary: "session opened: local-cli  local-project".to_string(),
    }]);
    app.inspector_subject = InspectorSubject::Audit;
    let audit = render_to_text(&mut app, 120, 32);
    assert!(audit.contains("Inspector  Audit"));
    assert!(audit.contains("Recent Audit"));
    assert!(audit.contains("session opened"));
    assert!(audit.contains("actor api"));
}

#[test]
fn project_pane_scrolls_and_marks_long_instance_lists() {
    let mut app = blocked_test_app();
    app.instances = (0..40)
        .map(|index| InstanceSummary {
            name: Some(format!("inst-{index:02}")),
            is_default: index == 0,
            home: PathBuf::from(format!("/tmp/instances/inst-{index:02}")),
            work_root: Some(PathBuf::from(format!("/tmp/project/inst-{index:02}"))),
            work_root_finding: None,
            shared_work_root_count: 0,
            default_runtime: Some("codex".to_string()),
        })
        .collect();
    let selected = app.instances.last().expect("instance").clone();
    app.selected_index = app.instances.len() - 1;
    app.project_cursor = ProjectSelection::Instance(app.selected_index);
    app.selected = SelectedInstanceState::Blocked {
        summary: selected,
        blocker: LaunchBlocker::for_instance("inst-39", "blocked"),
    };

    let rendered = render_to_text(&mut app, 100, 28);

    assert!(app.project_list_state.offset() > 0);
    assert!(rendered.contains("inst-39"));
    assert!(rendered.contains("^"));
    assert!(rendered.contains("v"));
}

#[tokio::test]
async fn project_pane_scrolls_and_marks_long_session_lists() {
    let mut app = ready_test_app(Vec::new()).await;
    app.project_root = Some(PathBuf::from("/tmp/project"));
    let sessions = (0..40)
        .map(|index| {
            let session_id = Uuid::from_u128(index + 1);
            ProjectSessionItem {
                session_id,
                turn_count: index as u64,
                current: session_id == app.ready_instance().expect("ready").session_id,
            }
        })
        .collect::<Vec<_>>();
    let target = sessions.last().expect("session").session_id;
    app.project_objects.sessions = ProjectObjectSection::Ready(sessions);
    app.project_cursor = ProjectSelection::Session(target);
    app.focus = Focus::Project;

    let rendered = render_to_text(&mut app, 100, 28);

    assert!(app.project_list_state.offset() > 0);
    assert!(rendered.contains(&short_session_id(target)));
    assert!(rendered.contains("^"));
    assert!(rendered.contains("v"));
}

#[tokio::test]
async fn transcript_scroll_is_bounded_to_wrapped_content_and_renders_scrollbar() {
    let body = (0..40)
        .map(|index| format!("visible-line-{index:02}"))
        .collect::<Vec<_>>()
        .join("\n");
    let mut app = ready_test_app(vec![TranscriptLine::new(TranscriptLineKind::Answer, body)]).await;
    app.focus = Focus::Transcript;
    app.transcript_scroll.offset = usize::MAX;

    let rendered = render_to_text(&mut app, 100, 24);

    assert!(app.transcript_scroll.limit > 0);
    assert_eq!(app.transcript_scroll.offset, app.transcript_scroll.limit);
    assert!(rendered.contains("visible-line-39"));
    assert!(rendered.contains("^"));
    assert!(rendered.contains("v"));
}

#[test]
fn scrollbar_position_maps_pane_bottom_to_ratatui_bottom() {
    assert_eq!(vertical_scroll_limit(100, 20), 80);
    assert_eq!(scrollbar_position_for_pane_offset(0, 100, 20), 0);
    assert_eq!(scrollbar_position_for_pane_offset(40, 100, 20), 50);
    assert_eq!(scrollbar_position_for_pane_offset(80, 100, 20), 99);
    assert_eq!(scrollbar_position_for_pane_offset(usize::MAX, 100, 20), 99);
}

#[tokio::test]
async fn activity_inspector_follows_tail_and_renders_scrollbar() {
    let mut app = ready_test_app(Vec::new()).await;
    app.focus = Focus::Inspectors;
    app.inspector_subject = InspectorSubject::Activity;
    app.activity.start();
    for index in 0..80 {
        app.activity.push_item(
            ActivityItemKind::Command,
            format!("runtime ran: command-{index:02}\nexit 0"),
        );
    }

    let rendered = render_to_text(&mut app, 120, 24);

    assert!(app.activity_scroll.limit > 0);
    assert_eq!(app.activity_scroll.offset, app.activity_scroll.limit);
    assert!(rendered.contains("command-79"));
    assert!(rendered.contains("^"));
    assert!(rendered.contains("v"));
}

#[tokio::test]
async fn activity_inspector_keyboard_scroll_is_bounded() {
    let (backend_tx, _backend_rx) = mpsc::unbounded_channel();
    let mut app = ready_test_app(Vec::new()).await;
    app.focus = Focus::Inspectors;
    app.inspector_subject = InspectorSubject::Activity;
    app.activity_scroll.limit = 30;
    app.activity_scroll.offset = 30;
    app.activity_scroll.page_size = 7;
    app.activity_scroll.follow_tail = true;

    handle_key(
        &mut app,
        KeyEvent::new(KeyCode::PageUp, KeyModifiers::NONE),
        &backend_tx,
    )
    .await;
    assert_eq!(app.activity_scroll.offset, 23);
    assert!(!app.activity_scroll.follow_tail);

    handle_key(
        &mut app,
        KeyEvent::new(KeyCode::Home, KeyModifiers::NONE),
        &backend_tx,
    )
    .await;
    assert_eq!(app.activity_scroll.offset, 0);

    handle_key(
        &mut app,
        KeyEvent::new(KeyCode::End, KeyModifiers::NONE),
        &backend_tx,
    )
    .await;
    assert_eq!(app.activity_scroll.offset, 30);
    assert!(app.activity_scroll.follow_tail);

    handle_key(
        &mut app,
        KeyEvent::new(KeyCode::PageDown, KeyModifiers::NONE),
        &backend_tx,
    )
    .await;
    assert_eq!(app.activity_scroll.offset, 30);
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

#[tokio::test]
async fn project_console_starts_with_composer_focused() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let project = crate::operator::target::init_project(temp_dir.path()).expect("init");
    let target = crate::operator::target::resolve_target(
        &crate::operator::target::TargetSelection {
            home: None,
            project: Some(project.project_root),
            instance: None,
        },
        crate::operator::target::WorkRootRequirement::Optional,
    )
    .expect("resolve target");

    let app = ConsoleApp::load(RunConsoleInvocation {
        target: &target,
        requested_runtime: None,
        continue_last_session: false,
        timeout_override: None,
    })
    .await
    .expect("console");

    assert_eq!(app.focus, Focus::Composer);
    assert_eq!(app.project_cursor, ProjectSelection::Instance(0));
}

#[tokio::test]
async fn project_console_load_does_not_repair_or_write_config() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let project = crate::operator::target::init_project(temp_dir.path()).expect("init");
    let project_config_path = project
        .project_root
        .join(crate::operator::target::PROJECT_DIR)
        .join(crate::operator::target::PROJECT_FILE);
    let instance_config_path = project.instance.home.join("config").join("instance.toml");
    let before_project_config =
        std::fs::read_to_string(&project_config_path).expect("project config");
    let before_instance_config =
        std::fs::read_to_string(&instance_config_path).expect("instance config");
    let operator_config_path = LionClawHome::new(project.instance.home.clone()).config_path();
    assert!(!operator_config_path.exists());
    let target = crate::operator::target::resolve_target(
        &crate::operator::target::TargetSelection {
            home: None,
            project: Some(project.project_root.clone()),
            instance: None,
        },
        crate::operator::target::WorkRootRequirement::Optional,
    )
    .expect("resolve target");

    let app = ConsoleApp::load(RunConsoleInvocation {
        target: &target,
        requested_runtime: None,
        continue_last_session: false,
        timeout_override: None,
    })
    .await
    .expect("console");

    assert!(!app.selected.is_ready());
    assert_eq!(
        std::fs::read_to_string(&project_config_path).expect("project config after"),
        before_project_config
    );
    assert_eq!(
        std::fs::read_to_string(&instance_config_path).expect("instance config after"),
        before_instance_config
    );
    assert!(
        !operator_config_path.exists(),
        "run TUI must not create operator config while rendering a launch blocker"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn instance_switch_preserves_invocation_runtime_override() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let project = crate::operator::target::init_project(temp_dir.path()).expect("init");
    let reviewer = crate::operator::target::create_project_instance(
        &project.project_root,
        "reviewer",
        Some(project.project_root.as_path()),
        false,
    )
    .expect("create reviewer");
    let runtime_executable = temp_dir.path().join("runtime-stub.sh");
    write_executable_script(&runtime_executable, "#!/usr/bin/env bash\ncat >/dev/null\n");
    let podman = write_fake_podman(temp_dir.path());
    save_project_runtime_config(
        &LionClawHome::new(project.instance.home.clone()),
        &runtime_executable,
        &podman,
    )
    .await;
    save_project_runtime_config(
        &LionClawHome::new(reviewer.home.clone()),
        &runtime_executable,
        &podman,
    )
    .await;

    let target = crate::operator::target::resolve_target(
        &crate::operator::target::TargetSelection {
            home: None,
            project: Some(project.project_root),
            instance: None,
        },
        crate::operator::target::WorkRootRequirement::Optional,
    )
    .expect("resolve target");
    let mut app = ConsoleApp::load(RunConsoleInvocation {
        target: &target,
        requested_runtime: Some(TEST_OVERRIDE_RUNTIME_ID.to_string()),
        continue_last_session: false,
        timeout_override: None,
    })
    .await
    .expect("console");

    assert_eq!(
        app.ready_instance()
            .expect("initial ready instance")
            .runtime_id,
        TEST_OVERRIDE_RUNTIME_ID
    );
    let reviewer_index = app
        .instances
        .iter()
        .position(|instance| instance.name.as_deref() == Some("reviewer"))
        .expect("reviewer index");

    app.switch_selected_confirmed(reviewer_index).await;

    let ready = app.ready_instance().expect("switched ready instance");
    assert_eq!(ready.summary.name.as_deref(), Some("reviewer"));
    assert_eq!(ready.runtime_id, TEST_OVERRIDE_RUNTIME_ID);
    assert_eq!(
        ready.runtime_override.as_deref(),
        Some(TEST_OVERRIDE_RUNTIME_ID)
    );
}

#[tokio::test]
async fn project_objects_load_real_sessions() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let workspace_root = temp_dir.path().join("workspace");
    let runtime_root = temp_dir.path().join("runtime");
    let project_root = temp_dir.path().join("project");
    let kernel = Kernel::new_with_options(
        &temp_dir.path().join("lionclaw.db"),
        KernelOptions {
            workspace_root: Some(workspace_root),
            runtime_root: Some(runtime_root),
            project_workspace_root: Some(project_root.clone()),
            workspace_name: Some("main".to_string()),
            default_runtime_id: Some("codex".to_string()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel");
    let peer_id = "local-project".to_string();
    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: LOCAL_CLI_CHANNEL_ID.to_string(),
            peer_id: peer_id.clone(),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("session");
    let summary = InstanceSummary {
        name: Some("main".to_string()),
        is_default: true,
        home: temp_dir.path().join("instances/main"),
        work_root: Some(project_root),
        work_root_finding: None,
        shared_work_root_count: 0,
        default_runtime: Some("codex".to_string()),
    };
    let selected = SelectedInstanceState::Ready(Box::new(ReadyInstance {
        summary,
        runtime_id: "codex".to_string(),
        runtime_kind: "codex".to_string(),
        runtime_executable: "codex".to_string(),
        runtime_model: Some("gpt-5".to_string()),
        runtime_agent: None,
        runtime_override: None,
        boundary: BoundarySummary {
            workspace: "rw".to_string(),
            network: "none".to_string(),
            secrets: "off".to_string(),
            turn_timeout: "2h".to_string(),
            preset: "test".to_string(),
        },
        kernel,
        session_id: session.session_id,
        peer_id,
    }));

    let objects = load_project_objects(&selected).await;

    let ProjectObjectSection::Ready(sessions) = objects.sessions else {
        panic!("expected sessions");
    };
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].session_id, session.session_id);
    assert!(sessions[0].current);
}

#[tokio::test]
async fn audit_loads_real_session_events() {
    let (mut app, current_session_id, _) = ready_project_session_app().await;

    app.refresh_audit().await;

    let AuditTrail::Ready(events) = &app.audit else {
        panic!("expected audit events, got {:?}", app.audit);
    };
    assert!(events.iter().any(|event| {
        event.event_type == "session.open" && event.session_id == Some(current_session_id)
    }));
    assert!(events.iter().all(|event| !event.summary.trim().is_empty()));
}

#[tokio::test]
async fn project_session_activation_switches_selected_session() {
    let (backend_tx, _backend_rx) = mpsc::unbounded_channel();
    let (mut app, current_session_id, next_session_id) = ready_project_session_app().await;
    app.project_cursor = ProjectSelection::Session(next_session_id);
    app.focus = Focus::Project;

    handle_key(
        &mut app,
        KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
        &backend_tx,
    )
    .await;

    let ready = app.ready_instance().expect("ready instance");
    assert_eq!(ready.session_id, next_session_id);
    assert_ne!(ready.session_id, current_session_id);
    assert_eq!(
        app.project_cursor,
        ProjectSelection::Session(next_session_id)
    );
    assert!(app.overlay.is_none());
    assert!(app.status.contains(&short_session_id(next_session_id)));
}

#[tokio::test]
async fn project_session_activation_confirms_before_clearing_composer() {
    let (backend_tx, _backend_rx) = mpsc::unbounded_channel();
    let (mut app, current_session_id, next_session_id) = ready_project_session_app().await;
    app.project_cursor = ProjectSelection::Session(next_session_id);
    app.focus = Focus::Project;
    app.composer = ConsoleComposer::from_text("unsent prompt");

    handle_key(
        &mut app,
        KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
        &backend_tx,
    )
    .await;

    assert_eq!(
        app.ready_instance().expect("ready instance").session_id,
        current_session_id
    );
    assert_eq!(
        app.overlay,
        Some(Overlay::SessionSwitchConfirm {
            session_id: next_session_id
        })
    );
    assert_eq!(app.composer.text(), "unsent prompt");

    handle_key(
        &mut app,
        KeyEvent::new(KeyCode::Char('y'), KeyModifiers::NONE),
        &backend_tx,
    )
    .await;

    assert_eq!(
        app.ready_instance().expect("ready instance").session_id,
        next_session_id
    );
    assert!(app.overlay.is_none());
    assert_eq!(app.composer.text(), "");
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
        app.request_instance_switch(1);
    });

    assert_eq!(app.selected_index, 0);
    assert!(app.status.contains("finish the active turn"));
    if let Some(handle) = app.active_turn.take() {
        handle.abort();
    }
}

#[test]
fn instance_switching_requires_confirmation() {
    let mut app = blocked_test_app();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    let (backend_tx, _backend_rx) = mpsc::unbounded_channel();

    runtime.block_on(async {
        handle_key(
            &mut app,
            KeyEvent::new(KeyCode::Down, KeyModifiers::NONE),
            &backend_tx,
        )
        .await;
    });

    assert_eq!(app.selected_index, 0);
    assert_eq!(app.project_cursor, ProjectSelection::Instance(1));
    assert!(app.overlay.is_none());

    runtime.block_on(async {
        handle_key(
            &mut app,
            KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
            &backend_tx,
        )
        .await;
    });

    assert_eq!(app.selected_index, 0);
    assert!(matches!(
        app.overlay,
        Some(Overlay::InstanceSwitchConfirm { target_index: 1 })
    ));
    let rendered = render_to_text(&mut app, 100, 30);
    assert!(rendered.contains("Switch from main to reviewer?"));

    runtime.block_on(async {
        handle_key(
            &mut app,
            KeyEvent::new(KeyCode::Char('y'), KeyModifiers::NONE),
            &backend_tx,
        )
        .await;
    });

    assert_eq!(app.selected_index, 1);
    assert!(app.overlay.is_none());
    assert!(app.status.contains("selected reviewer"));
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
fn documented_global_shortcuts_route_through_keymap() {
    assert_eq!(
        global_command_for(KeyEvent::new(KeyCode::Char('p'), KeyModifiers::CONTROL)),
        Some(GlobalCommand::Commands)
    );
    assert_eq!(
        global_command_for(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE)),
        Some(GlobalCommand::NextFocus)
    );
    assert_eq!(
        global_command_for(KeyEvent::new(KeyCode::BackTab, KeyModifiers::NONE)),
        Some(GlobalCommand::PreviousFocus)
    );
    assert_eq!(
        global_command_for(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)),
        Some(GlobalCommand::InterruptOrConfirmExit)
    );
    assert_eq!(
        global_command_for(KeyEvent::new(KeyCode::Char('d'), KeyModifiers::CONTROL)),
        Some(GlobalCommand::Exit)
    );

    let footer = footer_hint_spans()
        .iter()
        .map(|span| span.content.as_ref())
        .collect::<String>();
    assert!(footer.contains("Ctrl+P Commands"));
    assert!(footer.contains("Tab Focus"));
    assert!(footer.contains("Ctrl+C Interrupt"));
    assert!(footer.contains("Ctrl+D Exit"));
}

#[test]
fn ctrl_d_exits_when_idle() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    let (backend_tx, _backend_rx) = mpsc::unbounded_channel();
    let mut app = blocked_test_app();

    runtime.block_on(async {
        handle_key(
            &mut app,
            KeyEvent::new(KeyCode::Char('d'), KeyModifiers::CONTROL),
            &backend_tx,
        )
        .await;
    });
    assert!(app.should_quit);
}

#[test]
fn compact_path_display_preserves_edges() {
    let path = Path::new("/home/mosh/mosh/misc/projects/example-with-a-very-long-name");

    assert_eq!(
        middle_elide(&path.display().to_string(), 24),
        "/home/mosh...y-long-name"
    );
}

#[test]
fn boundary_rows_use_product_terms() {
    let boundary = BoundarySummary {
        workspace: "rw".to_string(),
        network: "off".to_string(),
        secrets: "off".to_string(),
        turn_timeout: "30m/2h".to_string(),
        preset: "everyday".to_string(),
    };

    let rows = boundary_display_rows(&boundary)
        .into_iter()
        .map(|row| (row.label, row.value))
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        vec![
            ("workspace", "read-write".to_string()),
            ("network", "off".to_string()),
            ("secrets", "off".to_string()),
            ("runtime home", "private".to_string()),
            ("skills", "read-only".to_string()),
            ("preset", "everyday".to_string()),
            ("turn timeout", "30m/2h".to_string()),
        ]
    );
}

#[test]
fn question_mark_is_printable_and_ctrl_p_opens_commands() {
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
    nav_app.focus = Focus::Project;
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
            KeyEvent::new(KeyCode::Char('p'), KeyModifiers::CONTROL),
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

#[test]
fn inspector_left_right_cycles_subjects() {
    let mut app = blocked_test_app();
    app.focus = Focus::Inspectors;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    let (backend_tx, _backend_rx) = mpsc::unbounded_channel();

    runtime.block_on(async {
        handle_key(
            &mut app,
            KeyEvent::new(KeyCode::Right, KeyModifiers::NONE),
            &backend_tx,
        )
        .await;
    });
    assert_eq!(app.inspector_subject, InspectorSubject::Runtime);
    assert_eq!(app.status, "inspector: Runtime");

    runtime.block_on(async {
        handle_key(
            &mut app,
            KeyEvent::new(KeyCode::Left, KeyModifiers::NONE),
            &backend_tx,
        )
        .await;
    });
    assert_eq!(app.inspector_subject, InspectorSubject::Selection);

    runtime.block_on(async {
        handle_key(
            &mut app,
            KeyEvent::new(KeyCode::Left, KeyModifiers::NONE),
            &backend_tx,
        )
        .await;
    });
    assert_eq!(app.inspector_subject, InspectorSubject::Audit);

    runtime.block_on(async {
        handle_key(
            &mut app,
            KeyEvent::new(KeyCode::Right, KeyModifiers::NONE),
            &backend_tx,
        )
        .await;
    });
    assert_eq!(app.inspector_subject, InspectorSubject::Selection);
}

async fn ready_project_session_app() -> (ConsoleApp, Uuid, Uuid) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let workspace_root = temp_dir.path().join("workspace");
    let runtime_root = temp_dir.path().join("runtime");
    let project_root = temp_dir.path().join("project");
    let kernel = Kernel::new_with_options(
        &temp_dir.path().join("lionclaw.db"),
        KernelOptions {
            workspace_root: Some(workspace_root),
            runtime_root: Some(runtime_root),
            project_workspace_root: Some(project_root.clone()),
            workspace_name: Some("main".to_string()),
            default_runtime_id: Some("codex".to_string()),
            ..KernelOptions::default()
        },
    )
    .await
    .expect("kernel");
    let peer_id = "local-project".to_string();
    let first_session = kernel
        .open_session(SessionOpenRequest {
            channel_id: LOCAL_CLI_CHANNEL_ID.to_string(),
            peer_id: peer_id.clone(),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("first session");
    let next_session = kernel
        .open_session(SessionOpenRequest {
            channel_id: LOCAL_CLI_CHANNEL_ID.to_string(),
            peer_id: peer_id.clone(),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
        .expect("next session");
    let summary = InstanceSummary {
        name: Some("main".to_string()),
        is_default: true,
        home: temp_dir.path().join("instances/main"),
        work_root: Some(project_root.clone()),
        work_root_finding: None,
        shared_work_root_count: 0,
        default_runtime: Some("codex".to_string()),
    };
    let selected = SelectedInstanceState::Ready(Box::new(ReadyInstance {
        summary: summary.clone(),
        runtime_id: "codex".to_string(),
        runtime_kind: "codex".to_string(),
        runtime_executable: "codex".to_string(),
        runtime_model: Some("gpt-5".to_string()),
        runtime_agent: None,
        runtime_override: None,
        boundary: BoundarySummary {
            workspace: "rw".to_string(),
            network: "none".to_string(),
            secrets: "off".to_string(),
            turn_timeout: "2h".to_string(),
            preset: "test".to_string(),
        },
        kernel,
        session_id: first_session.session_id,
        peer_id,
    }));
    let project_objects = load_project_objects(&selected).await;
    let app = ConsoleApp {
        project_root: Some(project_root),
        instances: vec![summary],
        selected_index: 0,
        selected,
        project_objects,
        project_cursor: ProjectSelection::Instance(0),
        project_list_state: ListState::default(),
        launch: ConsoleLaunchOptions::default(),
        focus: Focus::Composer,
        overlay: None,
        composer: ConsoleComposer::new(),
        transcript: Vec::new(),
        transcript_scroll: VerticalScroll::top(DEFAULT_TRANSCRIPT_PAGE_SCROLL),
        activity: ActivitySummary::new(),
        activity_scroll: VerticalScroll::tail(DEFAULT_ACTIVITY_PAGE_SCROLL),
        audit: AuditTrail::Unavailable("not loaded".to_string()),
        inspector_subject: InspectorSubject::Selection,
        status: "idle".to_string(),
        active_turn: None,
        active_turn_cancel: None,
        saw_ready_instance: true,
        should_quit: false,
    };
    (app, first_session.session_id, next_session.session_id)
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
        project_objects: ProjectObjects::unavailable("Launch blocked"),
        project_cursor: ProjectSelection::Instance(0),
        project_list_state: ListState::default(),
        launch: ConsoleLaunchOptions::default(),
        focus: Focus::Project,
        overlay: None,
        composer: ConsoleComposer::new(),
        transcript: Vec::new(),
        transcript_scroll: VerticalScroll::top(DEFAULT_TRANSCRIPT_PAGE_SCROLL),
        activity: ActivitySummary::new(),
        activity_scroll: VerticalScroll::tail(DEFAULT_ACTIVITY_PAGE_SCROLL),
        audit: AuditTrail::Unavailable("not loaded".to_string()),
        inspector_subject: InspectorSubject::Selection,
        status: "idle".to_string(),
        active_turn: None,
        active_turn_cancel: None,
        saw_ready_instance: false,
        should_quit: false,
    }
}

#[cfg(unix)]
async fn save_project_runtime_config(
    home: &LionClawHome,
    runtime_executable: &Path,
    podman: &Path,
) {
    let mut config = OperatorConfig::default();
    config.upsert_runtime(
        TEST_DEFAULT_RUNTIME_ID.to_string(),
        test_opencode_runtime(runtime_executable, podman),
    );
    config.upsert_runtime(
        TEST_OVERRIDE_RUNTIME_ID.to_string(),
        test_opencode_runtime(runtime_executable, podman),
    );
    config
        .set_default_runtime(TEST_DEFAULT_RUNTIME_ID)
        .expect("set default runtime");
    home.ensure_base_dirs().await.expect("base dirs");
    crate::workspace::bootstrap_workspace(&config.workspace_root(home))
        .await
        .expect("bootstrap workspace");
    config.save(home).await.expect("save config");
}

#[cfg(unix)]
fn test_opencode_runtime(runtime_executable: &Path, podman: &Path) -> RuntimeProfileConfig {
    RuntimeProfileConfig::OpenCode {
        executable: runtime_executable.display().to_string(),
        model: None,
        agent: None,
        confinement: ConfinementConfig::Oci(OciConfinementConfig {
            engine: podman.display().to_string(),
            image: Some("ghcr.io/lionclaw/operator-console-test-runtime:latest".to_string()),
            ..OciConfinementConfig::default()
        }),
    }
}

#[cfg(unix)]
fn write_fake_podman(root: &Path) -> PathBuf {
    let path = root.join("podman");
    write_executable_script(
        &path,
        r#"#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  image)
    if [ "${2:-}" = "inspect" ]; then
      printf 'sha256:operator-console-test-runtime\n'
    fi
    exit 0
    ;;
  run)
    exit 0
    ;;
  *)
    exit 0
    ;;
esac
"#,
    );
    path
}

#[cfg(unix)]
fn write_executable_script(path: &Path, body: &str) {
    use std::os::unix::fs::PermissionsExt;

    std::fs::write(path, body).expect("write script");
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).expect("chmod script");
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
        default_runtime: Some("codex".to_string()),
    };
    ConsoleApp {
        project_root: None,
        instances: vec![main.clone()],
        selected_index: 0,
        selected: SelectedInstanceState::Ready(Box::new(ReadyInstance {
            summary: main,
            runtime_id: "codex".to_string(),
            runtime_kind: "codex".to_string(),
            runtime_executable: "codex".to_string(),
            runtime_model: Some("gpt-5".to_string()),
            runtime_agent: None,
            runtime_override: None,
            boundary: BoundarySummary {
                workspace: "rw".to_string(),
                network: "none".to_string(),
                secrets: "off".to_string(),
                turn_timeout: "2h".to_string(),
                preset: "test".to_string(),
            },
            kernel,
            session_id: Uuid::new_v4(),
            peer_id: "local-project".to_string(),
        })),
        project_objects: ProjectObjects::default(),
        project_cursor: ProjectSelection::Instance(0),
        project_list_state: ListState::default(),
        launch: ConsoleLaunchOptions::default(),
        focus: Focus::Composer,
        overlay: None,
        composer: ConsoleComposer::new(),
        transcript,
        transcript_scroll: VerticalScroll::top(DEFAULT_TRANSCRIPT_PAGE_SCROLL),
        activity: ActivitySummary::new(),
        activity_scroll: VerticalScroll::tail(DEFAULT_ACTIVITY_PAGE_SCROLL),
        audit: AuditTrail::Unavailable("not loaded".to_string()),
        inspector_subject: InspectorSubject::Selection,
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
