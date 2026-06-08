use std::{
    future::Future,
    io::{BufRead, BufReader, Write},
    path::Path,
};

use anyhow::{anyhow, Result};
use tokio::sync::mpsc;

use crate::{
    contracts::{
        SessionActionKind, SessionActionRequest, SessionHistoryPolicy, SessionHistoryRequest,
        SessionOpenRequest, SessionTurnRequest, SessionTurnStatus, SessionTurnView, StreamEventDto,
        StreamEventKindDto, StreamLaneDto, TrustTier,
    },
    home::{runtime_project_partition_key, LionClawHome},
    kernel::{
        input_routing::{classify_input, ClassifiedInput, LionClawControlInput},
        Kernel,
    },
    operator::{
        config::OperatorConfig,
        reconcile::{ensure_runtime_project_dirs_for_work_root, open_runtime_kernel_for_work_root},
        runtime::{resolve_runtime_id, validate_runtime_launch_prerequisites_for_work_root},
    },
    project_inventory::ProjectInstanceRuntimeContext,
    runtime_timeouts::RuntimeTurnTimeouts,
};

pub(crate) async fn run_local(invocation: RunLocalInvocation<'_>) -> Result<()> {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut input = BufReader::new(stdin);
    let mut output = stdout;
    run_local_with_io_and_timeouts(invocation, &mut input, &mut output).await
}

#[cfg(test)]
pub(crate) async fn run_local_with_io<R: BufRead + Send, W: Write + Send>(
    home: &LionClawHome,
    requested_runtime: Option<String>,
    continue_last_session: bool,
    input: &mut R,
    output: &mut W,
) -> Result<()> {
    let work_root = crate::config::resolve_project_workspace_root()
        .map_err(|err| anyhow!("failed to resolve project workspace root: {err}"))?;
    run_local_with_io_and_timeouts(
        RunLocalInvocation {
            home,
            project_root: None,
            work_root: &work_root,
            instance_name: Some("main"),
            project_instance_runtime: None,
            requested_runtime,
            continue_last_session,
            timeout_override: None,
        },
        input,
        output,
    )
    .await
}

pub(crate) struct RunLocalInvocation<'a> {
    pub(crate) home: &'a LionClawHome,
    pub(crate) project_root: Option<&'a Path>,
    pub(crate) work_root: &'a Path,
    pub(crate) instance_name: Option<&'a str>,
    pub(crate) project_instance_runtime: Option<ProjectInstanceRuntimeContext>,
    pub(crate) requested_runtime: Option<String>,
    pub(crate) continue_last_session: bool,
    pub(crate) timeout_override: Option<RuntimeTurnTimeouts>,
}

async fn run_local_with_io_and_timeouts<R: BufRead + Send, W: Write + Send>(
    invocation: RunLocalInvocation<'_>,
    input: &mut R,
    output: &mut W,
) -> Result<()> {
    let RunLocalInvocation {
        home,
        project_root,
        work_root,
        instance_name,
        project_instance_runtime,
        requested_runtime,
        continue_last_session,
        timeout_override,
    } = invocation;
    let config = OperatorConfig::load(home).await?;
    let runtime_id = resolve_run_runtime_id(
        &config,
        requested_runtime.as_deref(),
        instance_name.unwrap_or("selected home"),
    )?;
    validate_runtime_launch_prerequisites_for_work_root(
        home,
        &config,
        &runtime_id,
        project_root,
        Some(work_root),
    )
    .await?;
    ensure_runtime_project_dirs_for_work_root(home, &config, &runtime_id, work_root).await?;

    let effective_timeouts = timeout_override.unwrap_or_else(RuntimeTurnTimeouts::interactive);
    let kernel = open_runtime_kernel_for_work_root(
        home,
        &config,
        Some(runtime_id.clone()),
        work_root,
        project_instance_runtime,
        Some(effective_timeouts),
    )
    .await?;
    let peer_id = local_peer_id_for_project(work_root);
    let project_workspace_root = work_root.display().to_string();
    run_repl(
        &kernel,
        ReplContext {
            runtime_id: &runtime_id,
            project_workspace_root: &project_workspace_root,
            peer_id: &peer_id,
            timeouts: effective_timeouts,
        },
        continue_last_session,
        input,
        output,
    )
    .await
}

pub(crate) fn resolve_run_runtime_id(
    config: &crate::operator::config::OperatorConfig,
    requested_runtime: Option<&str>,
    instance_name: &str,
) -> Result<String> {
    if requested_runtime
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some()
    {
        return resolve_runtime_id(config, requested_runtime);
    }

    config.defaults.runtime.clone().ok_or_else(|| {
        anyhow!(
            "no default runtime configured for instance \"{instance_name}\"\nRun:\n  lionclaw configure --runtime codex"
        )
    })
}

struct ReplContext<'a> {
    runtime_id: &'a str,
    project_workspace_root: &'a str,
    peer_id: &'a str,
    timeouts: RuntimeTurnTimeouts,
}

async fn run_repl<R: BufRead + Send, W: Write + Send>(
    kernel: &Kernel,
    context: ReplContext<'_>,
    continue_last_session: bool,
    input: &mut R,
    output: &mut W,
) -> Result<()> {
    let mut session_id = resolve_repl_session(kernel, context.peer_id, continue_last_session)
        .await
        .map_err(kernel_to_anyhow)?
        .session_id;

    writeln!(
        output,
        "LionClaw interactive mode\nruntime: {}\nwork root: {}\nturn limits: quiet {}, max {}\nType /lionclaw continue, /lionclaw retry, /lionclaw reset, or /lionclaw exit.\n",
        context.runtime_id,
        context.project_workspace_root,
        crate::runtime_timeouts::format_duration(context.timeouts.idle),
        crate::runtime_timeouts::format_duration(context.timeouts.hard),
    )?;

    if continue_last_session {
        render_session_history(kernel, session_id, output).await?;
    }

    loop {
        write!(output, "you> ")?;
        output.flush()?;

        let mut line = String::new();
        if input.read_line(&mut line)? == 0 {
            writeln!(output)?;
            break;
        }

        let input_text = line.trim_end_matches(['\r', '\n']);
        match classify_input(input_text) {
            ClassifiedInput::Empty => continue,
            ClassifiedInput::Prompt(prompt) => {
                render_streaming_turn(kernel, session_id, context.runtime_id, &prompt, output)
                    .await?;
            }
            ClassifiedInput::RuntimeControl(control) => {
                render_streaming_turn(kernel, session_id, context.runtime_id, &control.raw, output)
                    .await?;
            }
            ClassifiedInput::LionClawControl(control) => {
                if handle_lionclaw_repl_control(
                    kernel,
                    &mut session_id,
                    context.runtime_id,
                    control,
                    output,
                )
                .await?
                {
                    break;
                }
            }
        }
    }

    Ok(())
}

async fn handle_lionclaw_repl_control<W: Write + Send>(
    kernel: &Kernel,
    session_id: &mut uuid::Uuid,
    runtime_id: &str,
    control: LionClawControlInput,
    output: &mut W,
) -> Result<bool> {
    match control.command_name.as_str() {
        "exit" | "quit" => Ok(true),
        "continue" => {
            render_session_action(
                kernel,
                *session_id,
                runtime_id,
                SessionActionKind::ContinueLastPartial,
                output,
            )
            .await?;
            Ok(false)
        }
        "retry" => {
            render_session_action(
                kernel,
                *session_id,
                runtime_id,
                SessionActionKind::RetryLastTurn,
                output,
            )
            .await?;
            Ok(false)
        }
        "reset" => {
            let response = kernel
                .session_action(SessionActionRequest::ResetSession {
                    session_id: *session_id,
                })
                .await
                .map_err(kernel_to_anyhow)?;
            *session_id = response.session_id;
            writeln!(output, "[status] opened a fresh session")?;
            Ok(false)
        }
        "" => {
            writeln!(output, "[error] missing LionClaw command")?;
            Ok(false)
        }
        other => {
            writeln!(output, "[error] unknown LionClaw command: {other}")?;
            Ok(false)
        }
    }
}

fn local_user_id() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "local-user".to_string())
}

pub(crate) fn local_peer_id_for_project(project_root: &Path) -> String {
    format!(
        "{}@project:{}",
        local_user_id(),
        runtime_project_partition_key(Some(project_root))
    )
}

pub(crate) fn kernel_to_anyhow(err: crate::kernel::KernelError) -> anyhow::Error {
    anyhow!(err.to_string())
}

pub(crate) async fn resolve_repl_session(
    kernel: &Kernel,
    peer_id: &str,
    continue_last_session: bool,
) -> Result<crate::contracts::SessionOpenResponse, crate::kernel::KernelError> {
    if continue_last_session {
        if let Some(session) = kernel
            .find_latest_session_summary("local-cli", peer_id)
            .await?
        {
            return Ok(session);
        }
    }

    kernel
        .open_session(SessionOpenRequest {
            channel_id: "local-cli".to_string(),
            peer_id: peer_id.to_string(),
            trust_tier: TrustTier::Main,
            history_policy: Some(SessionHistoryPolicy::Interactive),
        })
        .await
}

async fn render_session_history<W: Write + Send>(
    kernel: &Kernel,
    session_id: uuid::Uuid,
    output: &mut W,
) -> Result<()> {
    let history = kernel
        .session_history(SessionHistoryRequest {
            session_id,
            limit: Some(12),
        })
        .await
        .map_err(kernel_to_anyhow)?;

    if history.turns.is_empty() {
        return Ok(());
    }

    writeln!(output, "Recent session history:")?;
    for turn in history.turns {
        render_session_history_turn(&turn, output)?;
    }
    writeln!(output)?;
    Ok(())
}

async fn render_session_action<W: Write + Send>(
    kernel: &Kernel,
    session_id: uuid::Uuid,
    runtime_id: &str,
    action: SessionActionKind,
    output: &mut W,
) -> Result<()> {
    match action {
        SessionActionKind::ContinueLastPartial => {
            writeln!(output, "you> /lionclaw continue")?;
        }
        SessionActionKind::RetryLastTurn => {
            writeln!(output, "you> /lionclaw retry")?;
        }
        SessionActionKind::ResetSession => {}
    }
    let (tx, mut rx) = mpsc::unbounded_channel();
    let turn_future =
        kernel.run_session_action_streaming(session_id, action, Some(runtime_id.to_string()), tx);
    tokio::pin!(turn_future);
    render_streaming_future(&mut turn_future, &mut rx, output).await
}

fn render_session_history_turn<W: Write>(turn: &SessionTurnView, output: &mut W) -> Result<()> {
    writeln!(output, "you> {}", turn.display_user_text)?;
    if matches!(
        turn.status,
        SessionTurnStatus::TimedOut
            | SessionTurnStatus::Failed
            | SessionTurnStatus::Cancelled
            | SessionTurnStatus::Interrupted
    ) && !turn.assistant_text.trim().is_empty()
    {
        writeln!(output, "{}", partial_history_marker(turn.status))?;
    }
    if !turn.assistant_text.trim().is_empty() {
        write_prefixed_lines(output, "lionclaw> ", &turn.assistant_text)?;
    }
    if let Some(error_text) = turn.error_text.as_deref() {
        writeln!(output, "[error] {error_text}")?;
    }
    Ok(())
}

pub(crate) fn partial_history_marker(status: SessionTurnStatus) -> &'static str {
    match status {
        SessionTurnStatus::TimedOut => {
            "[partial] previous assistant reply timed out before completion"
        }
        SessionTurnStatus::Failed => "[partial] previous assistant reply failed before completion",
        SessionTurnStatus::Cancelled => {
            "[partial] previous assistant reply was cancelled before completion"
        }
        SessionTurnStatus::Interrupted => {
            "[partial] previous assistant reply was interrupted before completion"
        }
        _ => "",
    }
}

async fn render_streaming_turn<W: Write + Send>(
    kernel: &Kernel,
    session_id: uuid::Uuid,
    runtime_id: &str,
    user_text: &str,
    output: &mut W,
) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let turn_request = SessionTurnRequest {
        session_id,
        user_text: user_text.to_string(),
        runtime_id: Some(runtime_id.to_string()),
        runtime_working_dir: None,
        runtime_timeout_ms: None,
        runtime_env_passthrough: None,
    };
    let turn_future = kernel.turn_session_streaming(turn_request, tx);
    tokio::pin!(turn_future);
    render_streaming_future(&mut turn_future, &mut rx, output).await
}

async fn render_streaming_future<W, F>(
    turn_future: &mut F,
    rx: &mut mpsc::UnboundedReceiver<StreamEventDto>,
    output: &mut W,
) -> Result<()>
where
    W: Write + Send,
    F: Future<Output = Result<crate::contracts::SessionTurnResponse, crate::kernel::KernelError>>
        + Unpin,
{
    let mut visible_output_seen = false;
    let mut answer_output_seen = false;
    let mut error_seen = false;
    let mut turn_error: Option<crate::kernel::KernelError> = None;
    let mut event_stream_open = true;

    loop {
        tokio::select! {
            result = &mut *turn_future => {
                match result {
                    Ok(_) => {}
                    Err(err) => turn_error = Some(err),
                }
                break;
            }
            maybe_event = rx.recv(), if event_stream_open => {
                match maybe_event {
                    Some(event) => {
                        if is_visible_message_delta(&event) {
                            visible_output_seen = true;
                        }
                        if is_answer_message_delta(&event) {
                            answer_output_seen = true;
                        }
                        if matches!(event.kind, StreamEventKindDto::Error) {
                            error_seen = true;
                        }
                        render_turn_event(&event, output)?;
                        output.flush()?;
                    }
                    None => {
                        event_stream_open = false;
                    }
                }
            }
        }
    }

    while let Ok(event) = rx.try_recv() {
        if is_visible_message_delta(&event) {
            visible_output_seen = true;
        }
        if is_answer_message_delta(&event) {
            answer_output_seen = true;
        }
        if matches!(event.kind, StreamEventKindDto::Error) {
            error_seen = true;
        }
        render_turn_event(&event, output)?;
    }
    output.flush()?;

    if let Some(err) = turn_error {
        if answer_output_seen {
            match err {
                crate::kernel::KernelError::RuntimeTimeout(_) => {
                    writeln!(
                        output,
                        "Timed out. Partial output is shown above. Use /lionclaw continue, /lionclaw retry, or /lionclaw reset."
                    )?;
                    output.flush()?;
                    return Ok(());
                }
                crate::kernel::KernelError::Runtime(_) => {
                    writeln!(
                        output,
                        "Runtime error. Partial output is shown above. Use /lionclaw continue, /lionclaw retry, or /lionclaw reset."
                    )?;
                    output.flush()?;
                    return Ok(());
                }
                _ => {}
            }
        }
        if visible_output_seen {
            match err {
                crate::kernel::KernelError::RuntimeTimeout(_) => {
                    writeln!(
                        output,
                        "Timed out. Partial output is shown above. Use /lionclaw retry or /lionclaw reset."
                    )?;
                    output.flush()?;
                    return Ok(());
                }
                crate::kernel::KernelError::Runtime(_) => {
                    writeln!(
                        output,
                        "Runtime error. Partial output is shown above. Use /lionclaw retry or /lionclaw reset."
                    )?;
                    output.flush()?;
                    return Ok(());
                }
                _ => {}
            }
        }
        if !error_seen {
            writeln!(output, "error: {err}")?;
            output.flush()?;
        }
    }

    Ok(())
}

fn is_visible_message_delta(event: &StreamEventDto) -> bool {
    matches!(
        (&event.kind, &event.lane),
        (
            StreamEventKindDto::MessageDelta,
            Some(StreamLaneDto::Answer | StreamLaneDto::Reasoning)
        )
    ) && event.text.as_deref().is_some_and(|text| !text.is_empty())
}

fn is_answer_message_delta(event: &StreamEventDto) -> bool {
    matches!(
        (&event.kind, &event.lane),
        (
            StreamEventKindDto::MessageDelta,
            Some(StreamLaneDto::Answer)
        )
    ) && event.text.as_deref().is_some_and(|text| !text.is_empty())
}

#[cfg(test)]
fn render_turn_stream<W: Write>(events: &[StreamEventDto], output: &mut W) -> Result<()> {
    for event in events {
        render_turn_event(event, output)?;
    }

    Ok(())
}

fn render_turn_event<W: Write>(event: &StreamEventDto, output: &mut W) -> Result<()> {
    match (&event.kind, &event.lane, event.text.as_deref()) {
        (StreamEventKindDto::MessageDelta, Some(StreamLaneDto::Answer), Some(text)) => {
            write_prefixed_lines(output, "lionclaw> ", text)?;
        }
        (StreamEventKindDto::MessageDelta, Some(StreamLaneDto::Reasoning), Some(text)) => {
            write_prefixed_lines(output, "thinking> ", text)?;
        }
        (StreamEventKindDto::Status, _, Some(text)) => {
            writeln!(output, "[status] {text}")?;
        }
        (StreamEventKindDto::FileChange, _, Some(text)) => {
            writeln!(output, "[files] {text}")?;
        }
        (StreamEventKindDto::Error, _, Some(text)) => {
            writeln!(output, "[error] {text}")?;
        }
        (StreamEventKindDto::TurnCompleted, _, _)
        | (StreamEventKindDto::MessageBoundary, _, _)
        | (StreamEventKindDto::Done, _, _)
        | (_, _, None) => {}
        (_, _, Some(text)) => {
            writeln!(output, "{text}")?;
        }
    }

    Ok(())
}

fn write_prefixed_lines<W: Write>(output: &mut W, prefix: &str, text: &str) -> Result<()> {
    if text.is_empty() {
        writeln!(output, "{prefix}")?;
        return Ok(());
    }

    for line in text.lines() {
        writeln!(output, "{prefix}{line}")?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Cursor;

    use sqlx::{Row, SqlitePool};
    use uuid::Uuid;

    use super::{
        local_peer_id_for_project, render_streaming_future, render_turn_stream,
        resolve_repl_session, run_local_with_io, run_local_with_io_and_timeouts,
        RunLocalInvocation,
    };
    use crate::{
        config::resolve_project_workspace_root,
        contracts::{
            SessionTurnResponse, SessionTurnStatus, StreamEventDto, StreamEventKindDto,
            StreamLaneDto,
        },
        home::{runtime_project_partition_key, LionClawHome},
        kernel::{
            db::Db,
            runtime::{ConfinementConfig, OciConfinementConfig},
            Kernel, KernelOptions,
        },
        operator::config::{OperatorConfig, RuntimeProfileConfig},
        runtime_timeouts::RuntimeTurnTimeouts,
    };

    #[tokio::test]
    async fn render_streaming_future_waits_after_event_stream_closes() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<StreamEventDto>();
        drop(tx);
        let session_id = Uuid::new_v4();
        let turn_id = Uuid::new_v4();
        let mut turn_future = Box::pin(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            Ok(SessionTurnResponse {
                session_id,
                turn_id,
                status: SessionTurnStatus::Completed,
                assistant_text: String::new(),
                error_code: None,
                error_text: None,
                runtime_skill_ids: Vec::new(),
                runtime_id: "mock".to_string(),
                stream_events: Vec::new(),
            })
        });
        let mut output = Vec::new();

        tokio::time::timeout(
            std::time::Duration::from_millis(200),
            render_streaming_future(&mut turn_future, &mut rx, &mut output),
        )
        .await
        .expect("renderer should wait for the turn future after stream close")
        .expect("rendering should succeed");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_local_uses_existing_setup_and_executes_turns() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let stub = temp_dir.path().join("codex-stub.sh");
        write_codex_app_server_stub(
            &stub,
            &[
                r#"{"method":"item/agentMessage/delta","params":{"threadId":"thr_test","turnId":"turn_test","delta":"hello from repl"}}"#,
            ],
            0,
        );

        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), stubbed_codex_runtime(&stub));
        save_prepared_config(&home, &config).await;
        write_codex_runtime_auth(&home).await;

        let mut input = Cursor::new(b"hello\n/lionclaw exit\n".to_vec());
        let mut output = Vec::new();
        run_local_with_io(&home, None, false, &mut input, &mut output)
            .await
            .expect("run local");

        let output = String::from_utf8(output).expect("utf8 output");
        assert!(output.contains("you> "));
        assert!(output.contains("lionclaw> hello from repl"));
        assert!(output.contains("hello from repl"));
        let workspace = home.workspace_dir("main");
        assert!(workspace.join("AGENTS.md").exists());
        assert!(!workspace.join("IDENTITY.md").exists());
        assert!(!workspace.join("SOUL.md").exists());
        assert!(!workspace.join("USER.md").exists());

        let pool = Db::connect_file(&home.db_path())
            .await
            .expect("connect db")
            .pool();
        let row = sqlx::query("SELECT COUNT(*) AS count FROM sessions")
            .fetch_one(&pool)
            .await
            .expect("fetch count");
        let count: i64 = row.get("count");
        assert_eq!(count, 1, "run should create one kernel session");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_local_home_target_inside_project_instances_does_not_expose_project_inventory() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let project = crate::operator::target::init_project(temp_dir.path()).expect("init project");
        let home = LionClawHome::new(project.instance.home.clone());
        let stub = temp_dir.path().join("codex-stub.sh");
        let engine = temp_dir.path().join("podman");
        let engine_log = temp_dir.path().join("podman.args");
        write_codex_app_server_stub(
            &stub,
            &[
                r#"{"method":"item/agentMessage/delta","params":{"threadId":"thr_test","turnId":"turn_test","delta":"home mode"}}"#,
            ],
            0,
        );
        write_fake_podman(&engine, Some(&engine_log));

        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: stub.display().to_string(),
                model: None,
                confinement: ConfinementConfig::Oci(OciConfinementConfig {
                    engine: engine.display().to_string(),
                    image: Some("ghcr.io/lionclaw/test-codex-runtime:latest".to_string()),
                    ..OciConfinementConfig::default()
                }),
            },
        );
        save_prepared_config(&home, &config).await;
        write_codex_runtime_auth(&home).await;

        let mut input = Cursor::new(b"hello\n/lionclaw exit\n".to_vec());
        let mut output = Vec::new();
        run_local_with_io_and_timeouts(
            RunLocalInvocation {
                home: &home,
                project_root: None,
                work_root: project.instance.work_root.as_path(),
                instance_name: None,
                project_instance_runtime: None,
                requested_runtime: None,
                continue_last_session: false,
                timeout_override: None,
            },
            &mut input,
            &mut output,
        )
        .await
        .expect("run local");

        let invocation_args = fs::read_to_string(&engine_log).expect("read podman args");
        assert!(
            !invocation_args.contains("/lionclaw/project"),
            "standalone --home runs must not receive project inventory mounts: {invocation_args}"
        );
        assert!(
            !invocation_args.contains("LIONCLAW_PROJECT_INSTANCE"),
            "standalone --home runs must not receive project inventory env: {invocation_args}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_local_timeout_override_sets_kernel_turn_limit() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let stub = temp_dir.path().join("codex-stub.sh");
        write_script(
            &stub,
            r#"#!/usr/bin/env bash
cat >/dev/null
sleep 1
"#,
        );

        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), stubbed_codex_runtime(&stub));
        save_prepared_config(&home, &config).await;
        write_codex_runtime_auth(&home).await;

        let mut input = Cursor::new(b"hello\n/lionclaw exit\n".to_vec());
        let mut output = Vec::new();
        let work_root = std::env::current_dir().expect("current dir");
        run_local_with_io_and_timeouts(
            RunLocalInvocation {
                home: &home,
                project_root: None,
                work_root: &work_root,
                instance_name: Some("main"),
                project_instance_runtime: None,
                requested_runtime: None,
                continue_last_session: false,
                timeout_override: Some(RuntimeTurnTimeouts::with_turn_timeout(
                    std::time::Duration::from_millis(60),
                )),
            },
            &mut input,
            &mut output,
        )
        .await
        .expect("run local");

        let output = String::from_utf8(output).expect("utf8 output");
        assert!(
            output.contains("turn limits: quiet 60ms, max 60ms"),
            "REPL should print the effective timeout budget: {output}"
        );
        assert!(
            output.contains("[error] Runtime reached the 60ms safety limit.")
                || output.contains("[error] Runtime idle timed out after 60ms with no output."),
            "timeout should use the override instead of the default: {output}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_local_can_continue_last_session_history() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let stub = temp_dir.path().join("codex-stub.sh");
        write_codex_app_server_stub(
            &stub,
            &[
                r#"{"method":"item/agentMessage/delta","params":{"threadId":"thr_test","turnId":"turn_test","delta":"hello from repl"}}"#,
            ],
            0,
        );

        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), stubbed_codex_runtime(&stub));
        save_prepared_config(&home, &config).await;
        write_codex_runtime_auth(&home).await;

        let mut first_input = Cursor::new(b"hello\n/lionclaw exit\n".to_vec());
        let mut first_output = Vec::new();
        run_local_with_io(&home, None, false, &mut first_input, &mut first_output)
            .await
            .expect("first run");

        let mut second_input = Cursor::new(b"/lionclaw exit\n".to_vec());
        let mut second_output = Vec::new();
        run_local_with_io(&home, None, true, &mut second_input, &mut second_output)
            .await
            .expect("second run");

        let second_output = String::from_utf8(second_output).expect("utf8 output");
        assert!(second_output.contains("Recent session history:"));
        assert!(second_output.contains("you> hello"));
        assert!(second_output.contains("lionclaw> hello from repl"));

        let db_url = format!("sqlite://{}", home.db_path().display());
        let pool = SqlitePool::connect(&db_url).await.expect("connect db");
        let row = sqlx::query("SELECT COUNT(*) AS count FROM sessions")
            .fetch_one(&pool)
            .await
            .expect("fetch count");
        let count: i64 = row.get("count");
        assert_eq!(count, 1, "continue mode should reuse the latest session");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn continue_last_session_prefers_recent_activity_over_newer_idle_session() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let stub = temp_dir.path().join("codex-stub.sh");
        write_codex_app_server_stub(
            &stub,
            &[
                r#"{"method":"item/agentMessage/delta","params":{"threadId":"thr_test","turnId":"turn_test","delta":"hello from repl"}}"#,
            ],
            0,
        );

        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), stubbed_codex_runtime(&stub));
        save_prepared_config(&home, &config).await;
        write_codex_runtime_auth(&home).await;

        let mut first_input = Cursor::new(b"hello\n/lionclaw exit\n".to_vec());
        let mut first_output = Vec::new();
        run_local_with_io(&home, None, false, &mut first_input, &mut first_output)
            .await
            .expect("first run");

        let db_url = format!("sqlite://{}", home.db_path().display());
        let pool = SqlitePool::connect(&db_url).await.expect("connect db");
        let first_session_row = sqlx::query(
            "SELECT session_id, created_at_ms, last_activity_at_ms \
             FROM sessions \
             ORDER BY created_at_ms ASC \
             LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .expect("fetch first session");
        let first_session_id: String = first_session_row.get("session_id");
        let last_activity_at_ms: i64 = first_session_row
            .get::<Option<i64>, _>("last_activity_at_ms")
            .expect("last activity timestamp");
        let newer_created_at_ms = last_activity_at_ms + 1_000;
        let project_scope = runtime_project_partition_key(Some(
            &resolve_project_workspace_root().expect("project root"),
        ));
        sqlx::query(
            "INSERT INTO sessions \
             (session_id, channel_id, peer_id, project_scope, trust_tier, history_policy, created_at_ms, last_turn_at_ms, last_activity_at_ms, turn_count) \
             VALUES (?1, 'local-cli', ?2, ?3, 'main', 'interactive', ?4, NULL, NULL, 0)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(local_peer_id_for_project(
            &resolve_project_workspace_root().expect("project root"),
        ))
        .bind(project_scope)
        .bind(newer_created_at_ms)
        .execute(&pool)
        .await
        .expect("insert newer idle session");

        let mut second_input = Cursor::new(b"/lionclaw exit\n".to_vec());
        let mut second_output = Vec::new();
        run_local_with_io(&home, None, true, &mut second_input, &mut second_output)
            .await
            .expect("second run");

        let second_output = String::from_utf8(second_output).expect("utf8 output");
        assert!(second_output.contains("Recent session history:"));
        assert!(second_output.contains("you> hello"));

        let reused_session_row = sqlx::query(
            "SELECT session_id \
             FROM sessions \
             WHERE turn_count > 0 \
             ORDER BY turn_count DESC, created_at_ms ASC \
             LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .expect("fetch reused session");
        let reused_session_id: String = reused_session_row.get("session_id");
        assert_eq!(
            reused_session_id, first_session_id,
            "continue mode should reuse the most recently active session"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn continue_last_session_renders_interrupted_partial_history() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let stub = temp_dir.path().join("codex-stub.sh");
        write_script(&stub, "#!/usr/bin/env bash\ncat >/dev/null\n");

        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), stubbed_codex_runtime(&stub));
        save_prepared_config(&home, &config).await;
        write_codex_runtime_auth(&home).await;
        let pool = Db::connect_file(&home.db_path())
            .await
            .expect("connect db")
            .pool();
        let peer_id =
            local_peer_id_for_project(&resolve_project_workspace_root().expect("project root"));
        let project_scope = runtime_project_partition_key(Some(
            &resolve_project_workspace_root().expect("project root"),
        ));
        let session_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO sessions \
             (session_id, channel_id, peer_id, project_scope, trust_tier, history_policy, created_at_ms, last_turn_at_ms, last_activity_at_ms, turn_count) \
             VALUES (?1, 'local-cli', ?2, ?3, 'main', 'interactive', 1, 2, 2, 1)",
        )
        .bind(session_id.to_string())
        .bind(&peer_id)
        .bind(project_scope)
        .execute(&pool)
        .await
        .expect("insert session");
        sqlx::query(
            "INSERT INTO session_turns \
             (turn_id, session_id, sequence_no, kind, status, display_user_text, prompt_user_text, assistant_text, error_code, error_text, runtime_id, started_at_ms, finished_at_ms) \
             VALUES (?1, ?2, 1, 'normal', 'interrupted', 'hello', 'hello', 'partial reply', 'runtime.interrupted', 'turn interrupted by kernel restart', 'codex', 1, 2)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(session_id.to_string())
        .execute(&pool)
        .await
        .expect("insert interrupted turn");

        let mut input = Cursor::new(b"/lionclaw exit\n".to_vec());
        let mut output = Vec::new();
        run_local_with_io(&home, None, true, &mut input, &mut output)
            .await
            .expect("continue run");

        let output = String::from_utf8(output).expect("utf8 output");
        assert!(output.contains("Recent session history:"));
        assert!(output.contains("you> hello"));
        assert!(
            output.contains("[partial] previous assistant reply was interrupted before completion")
        );
        assert!(output.contains("lionclaw> partial reply"));
        assert!(output.contains("[error] turn interrupted by kernel restart"));
    }

    #[tokio::test]
    async fn local_continue_last_session_is_project_scoped() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let db_path = temp_dir.path().join("lionclaw.db");
        let kernel = Kernel::new_with_options(&db_path, KernelOptions::default())
            .await
            .expect("kernel init");
        let project_a = temp_dir.path().join("project-a");
        let project_b = temp_dir.path().join("project-b");
        fs::create_dir_all(&project_a).expect("project a");
        fs::create_dir_all(&project_b).expect("project b");

        let project_a_peer = local_peer_id_for_project(&project_a);
        let project_b_peer = local_peer_id_for_project(&project_b);
        assert_ne!(project_a_peer, project_b_peer);

        let existing = resolve_repl_session(&kernel, &project_a_peer, false)
            .await
            .expect("open project a session");
        let resumed = resolve_repl_session(&kernel, &project_a_peer, true)
            .await
            .expect("resume project a session");
        let isolated = resolve_repl_session(&kernel, &project_b_peer, true)
            .await
            .expect("open project b session");

        assert_eq!(resumed.session_id, existing.session_id);
        assert_ne!(isolated.session_id, existing.session_id);
    }

    #[tokio::test]
    async fn run_local_reset_opens_a_fresh_session() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let stub = temp_dir.path().join("codex-stub.sh");
        fs::write(&stub, "#!/usr/bin/env bash\ncat >/dev/null\n").expect("write stub");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let permissions = fs::Permissions::from_mode(0o755);
            fs::set_permissions(&stub, permissions).expect("chmod");
        }

        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), stubbed_codex_runtime(&stub));
        save_prepared_config(&home, &config).await;
        write_codex_runtime_auth(&home).await;

        let mut input = Cursor::new(b"/lionclaw reset\n/lionclaw exit\n".to_vec());
        let mut output = Vec::new();
        run_local_with_io(&home, None, false, &mut input, &mut output)
            .await
            .expect("run local");

        let output = String::from_utf8(output).expect("utf8 output");
        assert!(output.contains("[status] opened a fresh session"));

        let db_url = format!("sqlite://{}", home.db_path().display());
        let pool = SqlitePool::connect(&db_url).await.expect("connect db");
        let row = sqlx::query("SELECT COUNT(*) AS count FROM sessions")
            .fetch_one(&pool)
            .await
            .expect("fetch count");
        let count: i64 = row.get("count");
        assert_eq!(count, 2, "reset should create a new session");
    }

    #[tokio::test]
    async fn run_local_uses_default_runtime_when_omitted() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let stub = temp_dir.path().join("codex-stub.sh");
        fs::write(&stub, "#!/usr/bin/env bash\ncat >/dev/null\n").expect("write stub");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let permissions = fs::Permissions::from_mode(0o755);
            fs::set_permissions(&stub, permissions).expect("chmod");
        }

        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), stubbed_codex_runtime(&stub));
        save_prepared_config(&home, &config).await;
        write_codex_runtime_auth(&home).await;

        let mut input = Cursor::new(b"/lionclaw exit\n".to_vec());
        let mut output = Vec::new();
        run_local_with_io(&home, None, false, &mut input, &mut output)
            .await
            .expect("run local");

        let output = String::from_utf8(output).expect("utf8 output");
        assert!(output.contains("runtime: codex"));
    }

    #[tokio::test]
    async fn run_local_errors_when_runtime_is_missing() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();
        let err = run_local_with_io(&home, None, false, &mut input, &mut output)
            .await
            .expect_err("missing runtime should error");

        let message = err.to_string();
        assert!(message.contains("no default runtime configured for instance \"main\""));
        assert!(message.contains("lionclaw configure --runtime codex"));
        assert!(
            !home.config_path().exists(),
            "run should not create runtime config implicitly"
        );
    }

    #[tokio::test]
    async fn run_local_errors_when_runtime_engine_is_missing() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));

        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: "codex".to_string(),
                model: None,
                confinement: ConfinementConfig::Oci(OciConfinementConfig {
                    engine: temp_dir
                        .path()
                        .join("missing-podman")
                        .to_string_lossy()
                        .to_string(),
                    image: Some("ghcr.io/lionclaw/test-codex-runtime:latest".to_string()),
                    ..OciConfinementConfig::default()
                }),
            },
        );
        save_prepared_config(&home, &config).await;

        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();
        let err = run_local_with_io(&home, None, false, &mut input, &mut output)
            .await
            .expect_err("missing engine should error");

        assert!(err.to_string().contains("configured runtime profile"));
    }

    #[tokio::test]
    async fn run_local_errors_when_codex_runtime_auth_is_missing() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let stub = temp_dir.path().join("codex-stub.sh");
        fs::write(&stub, "#!/usr/bin/env bash\ncat >/dev/null\n").expect("write stub");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let permissions = fs::Permissions::from_mode(0o755);
            fs::set_permissions(&stub, permissions).expect("chmod");
        }

        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), stubbed_codex_runtime(&stub));
        save_prepared_config(&home, &config).await;

        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();
        let err = run_local_with_io(&home, None, false, &mut input, &mut output)
            .await
            .expect_err("missing runtime auth should error");

        assert!(err.to_string().contains("codex login"));
        assert!(err.to_string().contains("auth.json"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_local_fails_early_when_private_network_is_unavailable() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let stub = temp_dir.path().join("codex-stub.sh");
        write_script(&stub, "#!/usr/bin/env bash\ncat >/dev/null\n");
        let broken_podman = temp_dir.path().join("podman");
        write_script(
            &broken_podman,
            r#"#!/usr/bin/env bash
set -euo pipefail

if [ "${1:-}" = "image" ] && [ "${2:-}" = "exists" ]; then
  exit 0
fi

if [ "${1:-}" = "run" ]; then
  cat >&2 <<'EOF'
Error: pasta failed with exit code 1:
Failed to open() /dev/net/tun: No such device
Failed to set up tap device in namespace
EOF
  exit 125
fi

exit 0
"#,
        );

        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), stubbed_codex_runtime(&stub));
        save_prepared_config(&home, &config).await;
        write_codex_runtime_auth(&home).await;

        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();
        let err = run_local_with_io(&home, None, false, &mut input, &mut output)
            .await
            .expect_err("private-network failure should block run");

        assert!(err.to_string().contains("requires network-mode 'on'"));
        assert!(err
            .to_string()
            .contains("could not start a private network"));
        assert!(err.to_string().contains("/dev/net/tun"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_local_streams_codex_reasoning_and_answer_lanes() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let stub = temp_dir.path().join("codex-stub.sh");
        write_codex_app_server_stub(
            &stub,
            &[
                r#"{"method":"item/agentReasoning/delta","params":{"threadId":"thr_test","turnId":"turn_test","delta":"planning next step"}}"#,
                r#"{"method":"item/agentMessage/delta","params":{"threadId":"thr_test","turnId":"turn_test","delta":"hello from codex"}}"#,
            ],
            0,
        );

        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), stubbed_codex_runtime(&stub));
        save_prepared_config(&home, &config).await;
        write_codex_runtime_auth(&home).await;

        let mut input = Cursor::new(b"hello\n/lionclaw exit\n".to_vec());
        let mut output = Vec::new();
        run_local_with_io(&home, None, false, &mut input, &mut output)
            .await
            .expect("run local");

        let output = String::from_utf8(output).expect("utf8 output");
        assert!(output.contains("thinking> planning next step"));
        assert!(output.contains("lionclaw> hello from codex"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_local_executes_opencode_on_the_shared_program_backed_path() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let stub = temp_dir.path().join("opencode-stub.sh");
        write_script(
            &stub,
            r#"#!/usr/bin/env bash
cat >/dev/null
echo '{"type":"response.output_text.delta","text":"hello from opencode"}'
"#,
        );

        let mut config = OperatorConfig::default();
        config.upsert_runtime("opencode".to_string(), stubbed_opencode_runtime(&stub));
        save_prepared_config(&home, &config).await;

        let mut input = Cursor::new(b"hello\n/lionclaw exit\n".to_vec());
        let mut output = Vec::new();
        run_local_with_io(&home, None, false, &mut input, &mut output)
            .await
            .expect("run local");

        let output = String::from_utf8(output).expect("utf8 output");
        assert!(output.contains("runtime: opencode"));
        assert!(output.contains("lionclaw> hello from opencode"));
        assert!(!output.contains("[status] opencode event: response.output_text.delta"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_local_ignores_unselected_runtime_image_probe_failures() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let healthy_dir = temp_dir.path().join("healthy-runtime");
        fs::create_dir_all(&healthy_dir).expect("healthy runtime dir");
        let opencode_stub = healthy_dir.join("opencode-stub.sh");
        write_script(
            &opencode_stub,
            r#"#!/usr/bin/env bash
cat >/dev/null
echo '{"type":"response.output_text.delta","text":"hello from opencode"}'
"#,
        );
        let broken_podman = temp_dir.path().join("podman");
        write_script(
            &broken_podman,
            r#"#!/usr/bin/env bash
if [ "${1:-}" = "image" ] && [ "${2:-}" = "inspect" ]; then
  echo "storage denied" >&2
  exit 1
fi
exit 0
"#,
        );

        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: "codex".to_string(),
                model: None,
                confinement: ConfinementConfig::Oci(OciConfinementConfig {
                    engine: broken_podman.to_string_lossy().to_string(),
                    image: Some("ghcr.io/lionclaw/test-codex-runtime:latest".to_string()),
                    ..OciConfinementConfig::default()
                }),
            },
        );
        config.upsert_runtime(
            "opencode".to_string(),
            stubbed_opencode_runtime(&opencode_stub),
        );
        config
            .set_default_runtime("opencode")
            .expect("set default runtime");
        save_prepared_config(&home, &config).await;

        let mut input = Cursor::new(b"hello\n/lionclaw exit\n".to_vec());
        let mut output = Vec::new();
        run_local_with_io(&home, None, false, &mut input, &mut output)
            .await
            .expect("run local");

        let output = String::from_utf8(output).expect("utf8 output");
        assert!(output.contains("runtime: opencode"));
        assert!(output.contains("lionclaw> hello from opencode"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_local_streams_opencode_reasoning_and_answer_lanes() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let stub = temp_dir.path().join("opencode-stub.sh");
        write_script(
            &stub,
            r#"#!/usr/bin/env bash
cat >/dev/null
echo '{"type":"reasoning","text":"planning next step"}'
echo '{"type":"text","text":"hello from opencode"}'
"#,
        );

        let mut config = OperatorConfig::default();
        config.upsert_runtime("opencode".to_string(), stubbed_opencode_runtime(&stub));
        save_prepared_config(&home, &config).await;

        let mut input = Cursor::new(b"hello\n/lionclaw exit\n".to_vec());
        let mut output = Vec::new();
        run_local_with_io(&home, None, false, &mut input, &mut output)
            .await
            .expect("run local");

        let output = String::from_utf8(output).expect("utf8 output");
        assert!(output.contains("thinking> planning next step"));
        assert!(output.contains("lionclaw> hello from opencode"));
        assert!(!output.contains("[status] opencode event: reasoning"));
        assert!(!output.contains("[status] opencode event: text"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_local_reports_opencode_reasoning_only_failures_as_partial_output() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let stub = temp_dir.path().join("opencode-stub.sh");
        write_script(
            &stub,
            r#"#!/usr/bin/env bash
cat >/dev/null
echo '{"type":"reasoning","text":"checking the workspace"}'
exit 7
"#,
        );

        let mut config = OperatorConfig::default();
        config.upsert_runtime("opencode".to_string(), stubbed_opencode_runtime(&stub));
        save_prepared_config(&home, &config).await;

        let mut input = Cursor::new(b"hello\n/lionclaw exit\n".to_vec());
        let mut output = Vec::new();
        run_local_with_io(&home, None, false, &mut input, &mut output)
            .await
            .expect("run local");

        let output = String::from_utf8(output).expect("utf8 output");
        assert!(output.contains("thinking> checking the workspace"));
        assert!(output.contains("Runtime error. Partial output is shown above."));
        assert!(output.contains("Use /lionclaw retry or /lionclaw reset."));
        assert!(!output.contains("Use /lionclaw continue, /lionclaw retry, or /lionclaw reset."));
        assert!(!output.contains("Timed out. Partial output is shown above."));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_local_reports_reasoning_only_runtime_failures_as_partial_output() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let stub = temp_dir.path().join("codex-stub.sh");
        write_codex_app_server_stub(
            &stub,
            &[
                r#"{"method":"item/agentReasoning/delta","params":{"threadId":"thr_test","turnId":"turn_test","delta":"checking the workspace"}}"#,
            ],
            7,
        );

        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), stubbed_codex_runtime(&stub));
        save_prepared_config(&home, &config).await;
        write_codex_runtime_auth(&home).await;

        let mut input = Cursor::new(b"hello\n/lionclaw exit\n".to_vec());
        let mut output = Vec::new();
        run_local_with_io(&home, None, false, &mut input, &mut output)
            .await
            .expect("run local");

        let output = String::from_utf8(output).expect("utf8 output");
        assert!(output.contains("thinking> checking the workspace"));
        assert!(output.contains("Runtime error. Partial output is shown above."));
        assert!(output.contains("Use /lionclaw retry or /lionclaw reset."));
        assert!(!output.contains("Use /lionclaw continue, /lionclaw retry, or /lionclaw reset."));
        assert!(!output.contains("Timed out. Partial output is shown above."));
    }

    #[test]
    fn render_turn_stream_formats_lanes_and_status() {
        let mut output = Vec::new();
        render_turn_stream(
            &[
                StreamEventDto {
                    kind: StreamEventKindDto::Status,
                    lane: None,
                    code: None,
                    text: Some("runtime started".to_string()),
                    file_change: None,
                },
                StreamEventDto {
                    kind: StreamEventKindDto::MessageDelta,
                    lane: Some(StreamLaneDto::Reasoning),
                    code: None,
                    text: Some("planning next step".to_string()),
                    file_change: None,
                },
                StreamEventDto {
                    kind: StreamEventKindDto::MessageDelta,
                    lane: Some(StreamLaneDto::Answer),
                    code: None,
                    text: Some("hello\nworld".to_string()),
                    file_change: None,
                },
                StreamEventDto {
                    kind: StreamEventKindDto::Error,
                    lane: None,
                    code: None,
                    text: Some("something failed".to_string()),
                    file_change: None,
                },
                StreamEventDto {
                    kind: StreamEventKindDto::Done,
                    lane: None,
                    code: None,
                    text: None,
                    file_change: None,
                },
            ],
            &mut output,
        )
        .expect("render stream");

        let output = String::from_utf8(output).expect("utf8 output");
        assert!(output.contains("[status] runtime started"));
        assert!(output.contains("thinking> planning next step"));
        assert!(output.contains("lionclaw> hello"));
        assert!(output.contains("lionclaw> world"));
        assert!(output.contains("[error] something failed"));
    }

    async fn save_prepared_config(home: &LionClawHome, config: &OperatorConfig) {
        home.ensure_base_dirs().await.expect("prepare home dirs");
        crate::workspace::bootstrap_workspace(&config.workspace_root(home))
            .await
            .expect("bootstrap workspace");
        config.save(home).await.expect("save config");
    }

    #[cfg(unix)]
    fn write_script(path: &std::path::Path, body: &str) {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;

        let temp_path = path.with_extension("tmp");
        let mut file = std::fs::File::create(&temp_path).expect("create temp script");
        file.write_all(body.as_bytes()).expect("write script");
        file.sync_all().expect("sync temp script");
        std::fs::rename(&temp_path, path).expect("rename script");
        let permissions = std::fs::Permissions::from_mode(0o755);
        std::fs::set_permissions(path, permissions).expect("chmod script");
    }

    #[cfg(unix)]
    fn write_codex_app_server_stub(path: &std::path::Path, turn_events: &[&str], exit_code: i32) {
        let mut script = r#"#!/usr/bin/env bash
set -euo pipefail

while IFS= read -r line; do
  case "${line}" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"id":1,"result":{"serverInfo":{"name":"codex-test","version":"0.0.0"}}}'
      ;;
    *'"method":"initialized"'*)
      ;;
    *'"method":"thread/start"'*|*'"method":"thread/resume"'*)
      printf '%s\n' '{"id":2,"result":{"thread":{"id":"thr_test"}}}'
      printf '%s\n' '{"method":"thread/started","params":{"threadId":"thr_test"}}'
      ;;
    *'"method":"turn/start"'*)
      printf '%s\n' '{"id":3,"result":{"turn":{"id":"turn_test"}}}'
      printf '%s\n' '{"method":"turn/started","params":{"threadId":"thr_test","turnId":"turn_test"}}'
"#
        .to_string();

        for event in turn_events {
            assert!(
                !event.contains('\''),
                "test event JSON cannot contain single quotes"
            );
            script.push_str("      printf '%s\\n' '");
            script.push_str(event);
            script.push_str("'\n");
        }

        if exit_code == 0 {
            script.push_str(
                r#"      printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thr_test","turnId":"turn_test"}}'
      ;;
"#,
            );
        } else {
            script.push_str(&format!(
                r#"      exit {exit_code}
      ;;
"#
            ));
        }

        script.push_str(
            r#"    *'"method":"model/list"'*)
      printf '%s\n' '{"id":2,"result":{"data":[{"id":"gpt-test"}]}}'
      ;;
    *'"method":"thread/name/set"'*)
      printf '%s\n' '{"id":3,"result":{}}'
      printf '%s\n' '{"method":"thread/name/updated","params":{"threadId":"thr_test"}}'
      ;;
    *'"method":"thread/compact/start"'*)
      printf '%s\n' '{"id":3,"result":{"threadId":"thr_test"}}'
      printf '%s\n' '{"method":"turn/started","params":{"threadId":"thr_test","turnId":"turn_compact_test"}}'
      printf '%s\n' '{"method":"item/completed","params":{"threadId":"thr_test","turnId":"turn_compact_test","item":{"id":"item_compact_test","type":"contextCompaction"}}}'
      printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thr_test","turnId":"turn_compact_test","turn":{"id":"turn_compact_test","status":"completed"}}}'
      ;;
  esac
done
"#,
        );

        write_script(path, &script);
    }

    #[cfg(unix)]
    fn ensure_fake_podman(reference: &std::path::Path) -> std::path::PathBuf {
        let engine = reference.parent().expect("stub parent").join("podman");
        if !engine.exists() {
            write_fake_podman(&engine, None);
        }
        engine
    }

    #[cfg(unix)]
    fn write_fake_podman(path: &std::path::Path, log_path: Option<&std::path::Path>) {
        let log_args = log_path
            .map(|path| {
                let path = path.display().to_string();
                assert!(
                    !path.contains('\''),
                    "test log path cannot contain single quotes"
                );
                format!("printf '%s\\n' \"$@\" >> '{path}'\n")
            })
            .unwrap_or_default();
        write_script(
            path,
            &format!(
                r#"#!/usr/bin/env bash
set -euo pipefail
{log_args}

command_name="${{1:-}}"
shift || true

case "${{command_name}}" in
  image)
    subcommand="${{1:-}}"
    shift || true
    case "${{subcommand}}" in
      inspect)
        printf 'sha256:test-runtime-image\n'
        exit 0
        ;;
      exists)
        exit 0
        ;;
      *)
        exit 0
        ;;
    esac
    ;;
  secret)
    exit 0
    ;;
  run)
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --rm|--interactive|--read-only|--detach)
          shift
          ;;
        --network|--workdir|--tmpfs|--env|--secret|--memory|--cpus|--pids-limit|--pod|--name|--userns|--user|--entrypoint)
          shift 2
          ;;
        --volume)
          shift 2
          ;;
        --)
          shift
          break
          ;;
        -*)
          shift
          ;;
        *)
          shift
          break
          ;;
      esac
    done
    if [ "$#" -eq 1 ] || [ "${{1:-}}" = "-lc" ]; then
      exit 0
    fi
    exec "$@"
    ;;
  *)
    exit 0
    ;;
esac
"#,
            ),
        );
    }

    fn stubbed_codex_runtime(executable: &std::path::Path) -> RuntimeProfileConfig {
        RuntimeProfileConfig::Codex {
            executable: executable.display().to_string(),
            model: None,
            confinement: ConfinementConfig::Oci(OciConfinementConfig {
                engine: ensure_fake_podman(executable).to_string_lossy().to_string(),
                image: Some("ghcr.io/lionclaw/test-codex-runtime:latest".to_string()),
                ..OciConfinementConfig::default()
            }),
        }
    }

    async fn write_codex_runtime_auth(home: &LionClawHome) {
        let codex_home = home.root().join(".codex");
        tokio::fs::create_dir_all(&codex_home)
            .await
            .expect("create codex home");
        tokio::fs::write(
            codex_home.join("auth.json"),
            r#"{
  "OPENAI_API_KEY": "sk-test"
}"#,
        )
        .await
        .expect("write codex runtime auth");
    }

    fn stubbed_opencode_runtime(executable: &std::path::Path) -> RuntimeProfileConfig {
        RuntimeProfileConfig::OpenCode {
            executable: executable.display().to_string(),
            model: None,
            agent: None,
            confinement: ConfinementConfig::Oci(OciConfinementConfig {
                engine: ensure_fake_podman(executable).to_string_lossy().to_string(),
                image: Some("ghcr.io/lionclaw/test-opencode-runtime:latest".to_string()),
                ..OciConfinementConfig::default()
            }),
        }
    }
}
