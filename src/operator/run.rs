use std::{
    future::Future,
    io::{BufRead, Write},
    path::Path,
};

use anyhow::{anyhow, Result};
use tokio::sync::mpsc;

use crate::{
    config::resolve_project_workspace_root,
    contracts::{
        SessionActionKind, SessionActionRequest, SessionHistoryPolicy, SessionHistoryRequest,
        SessionOpenRequest, SessionTurnRequest, SessionTurnStatus, SessionTurnView, StreamEventDto,
        StreamEventKindDto, StreamLaneDto, TrustTier,
    },
    home::{runtime_project_partition_key, LionClawHome},
    kernel::Kernel,
    operator::{
        reconcile::{apply, onboard, open_runtime_kernel, render_runtime_cache},
        runtime::{resolve_runtime_id, validate_runtime_launch_prerequisites},
    },
};

pub async fn run_local(
    home: &LionClawHome,
    requested_runtime: Option<String>,
    continue_last_session: bool,
) -> Result<()> {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut input = stdin.lock();
    let mut output = stdout.lock();
    run_local_with_io(
        home,
        requested_runtime,
        continue_last_session,
        &mut input,
        &mut output,
    )
    .await
}

pub(crate) async fn run_local_with_io<R: BufRead, W: Write>(
    home: &LionClawHome,
    requested_runtime: Option<String>,
    continue_last_session: bool,
    input: &mut R,
    output: &mut W,
) -> Result<()> {
    onboard(home, None).await?;
    let config = crate::operator::config::OperatorConfig::load(home).await?;
    let runtime_id = resolve_runtime_id(&config, requested_runtime.as_deref())?;
    validate_runtime_launch_prerequisites(home, &config, &runtime_id).await?;
    let applied = apply(home).await?;
    render_runtime_cache(home, &applied.config, &applied.lockfile, &runtime_id).await?;

    let kernel = open_runtime_kernel(home, &applied.config, Some(runtime_id.clone())).await?;
    let project_workspace_root = resolve_project_workspace_root()
        .map_err(|err| anyhow!("failed to resolve project workspace root: {}", err))?;
    let peer_id = local_peer_id_for_project(&project_workspace_root);
    run_repl(
        &kernel,
        &runtime_id,
        &project_workspace_root.display().to_string(),
        &peer_id,
        continue_last_session,
        input,
        output,
    )
    .await
}

async fn run_repl<R: BufRead, W: Write>(
    kernel: &Kernel,
    runtime_id: &str,
    project_workspace_root: &str,
    peer_id: &str,
    continue_last_session: bool,
    input: &mut R,
    output: &mut W,
) -> Result<()> {
    let mut session_id = resolve_repl_session(kernel, peer_id, continue_last_session)
        .await
        .map_err(kernel_to_anyhow)?
        .session_id;

    writeln!(
        output,
        "LionClaw interactive mode\nruntime: {}\nproject: {}\nType /continue, /retry, /reset, or /exit.\n",
        runtime_id, project_workspace_root
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

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if matches!(trimmed, "/exit" | "/quit") {
            break;
        }
        if trimmed == "/continue" {
            render_session_action(
                kernel,
                session_id,
                runtime_id,
                SessionActionKind::ContinueLastPartial,
                output,
            )
            .await?;
            continue;
        }
        if trimmed == "/retry" {
            render_session_action(
                kernel,
                session_id,
                runtime_id,
                SessionActionKind::RetryLastTurn,
                output,
            )
            .await?;
            continue;
        }
        if trimmed == "/reset" {
            let response = kernel
                .session_action(SessionActionRequest {
                    session_id,
                    action: SessionActionKind::ResetSession,
                })
                .await
                .map_err(kernel_to_anyhow)?;
            session_id = response.session_id;
            writeln!(output, "[status] opened a fresh session")?;
            continue;
        }

        render_streaming_turn(kernel, session_id, runtime_id, trimmed, output).await?;
    }

    Ok(())
}

fn local_user_id() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "local-user".to_string())
}

fn local_peer_id_for_project(project_root: &Path) -> String {
    format!(
        "{}@project:{}",
        local_user_id(),
        runtime_project_partition_key(Some(project_root))
    )
}

fn kernel_to_anyhow(err: crate::kernel::KernelError) -> anyhow::Error {
    anyhow!(err.to_string())
}

async fn resolve_repl_session(
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

async fn render_session_history<W: Write>(
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

async fn render_session_action<W: Write>(
    kernel: &Kernel,
    session_id: uuid::Uuid,
    runtime_id: &str,
    action: SessionActionKind,
    output: &mut W,
) -> Result<()> {
    match action {
        SessionActionKind::ContinueLastPartial => {
            writeln!(output, "you> /continue")?;
        }
        SessionActionKind::RetryLastTurn => {
            writeln!(output, "you> /retry")?;
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
        writeln!(output, "[error] {}", error_text)?;
    }
    Ok(())
}

fn partial_history_marker(status: SessionTurnStatus) -> &'static str {
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

async fn render_streaming_turn<W: Write>(
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
    W: Write,
    F: Future<Output = Result<crate::contracts::SessionTurnResponse, crate::kernel::KernelError>>
        + Unpin,
{
    let mut visible_output_seen = false;
    let mut answer_output_seen = false;
    let mut error_seen = false;
    let mut turn_error: Option<crate::kernel::KernelError> = None;

    loop {
        tokio::select! {
            result = &mut *turn_future => {
                match result {
                    Ok(_) => {}
                    Err(err) => turn_error = Some(err),
                }
                break;
            }
            maybe_event = rx.recv() => {
                let Some(event) = maybe_event else {
                    continue;
                };
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
                        "Timed out. Partial output is shown above. Use /continue, /retry, or /reset."
                    )?;
                    output.flush()?;
                    return Ok(());
                }
                crate::kernel::KernelError::Runtime(_) => {
                    writeln!(
                        output,
                        "Runtime error. Partial output is shown above. Use /continue, /retry, or /reset."
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
                        "Timed out. Partial output is shown above. Use /retry or /reset."
                    )?;
                    output.flush()?;
                    return Ok(());
                }
                crate::kernel::KernelError::Runtime(_) => {
                    writeln!(
                        output,
                        "Runtime error. Partial output is shown above. Use /retry or /reset."
                    )?;
                    output.flush()?;
                    return Ok(());
                }
                _ => {}
            }
        }
        if !error_seen {
            writeln!(output, "error: {}", err)?;
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
            writeln!(output, "[status] {}", text)?;
        }
        (StreamEventKindDto::Error, _, Some(text)) => {
            writeln!(output, "[error] {}", text)?;
        }
        (StreamEventKindDto::Done, _, _) | (_, _, None) => {}
        (_, _, Some(text)) => {
            writeln!(output, "{}", text)?;
        }
    }

    Ok(())
}

fn write_prefixed_lines<W: Write>(output: &mut W, prefix: &str, text: &str) -> Result<()> {
    if text.is_empty() {
        writeln!(output, "{}", prefix)?;
        return Ok(());
    }

    for line in text.lines() {
        writeln!(output, "{}{}", prefix, line)?;
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
        local_peer_id_for_project, render_turn_stream, resolve_repl_session, run_local_with_io,
    };
    use crate::{
        config::resolve_project_workspace_root,
        contracts::{StreamEventDto, StreamEventKindDto, StreamLaneDto},
        home::{runtime_project_partition_key, LionClawHome},
        kernel::{
            db::Db,
            runtime::{ConfinementConfig, OciConfinementConfig},
            Kernel, KernelOptions,
        },
        operator::{
            config::{ManagedSkillConfig, OperatorConfig, RuntimeProfileConfig},
            lockfile::OperatorLockfile,
        },
    };

    #[cfg(unix)]
    #[tokio::test]
    async fn run_local_auto_onboards_and_executes_turns() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let stub = temp_dir.path().join("codex-stub.sh");
        write_script(
            &stub,
            r#"#!/usr/bin/env bash
cat >/dev/null
echo '{"type":"item.completed","item":{"type":"agent_message","text":"hello from repl"}}'
"#,
        );

        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), stubbed_codex_runtime(&stub));
        config.save(&home).await.expect("save config");
        write_codex_runtime_auth(&home).await;

        let mut input = Cursor::new(b"hello\n/exit\n".to_vec());
        let mut output = Vec::new();
        run_local_with_io(&home, None, false, &mut input, &mut output)
            .await
            .expect("run local");

        let output = String::from_utf8(output).expect("utf8 output");
        assert!(output.contains("you> "));
        assert!(output.contains("lionclaw> hello from repl"));
        assert!(output.contains("hello from repl"));
        assert!(home.workspace_dir("main").join("SOUL.md").exists());
        assert!(home.lock_path().exists());

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
    async fn run_local_can_continue_last_session_history() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let stub = temp_dir.path().join("codex-stub.sh");
        write_script(
            &stub,
            r#"#!/usr/bin/env bash
cat >/dev/null
echo '{"type":"item.completed","item":{"type":"agent_message","text":"hello from repl"}}'
"#,
        );

        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), stubbed_codex_runtime(&stub));
        config.save(&home).await.expect("save config");
        write_codex_runtime_auth(&home).await;

        let mut first_input = Cursor::new(b"hello\n/exit\n".to_vec());
        let mut first_output = Vec::new();
        run_local_with_io(&home, None, false, &mut first_input, &mut first_output)
            .await
            .expect("first run");

        let mut second_input = Cursor::new(b"/exit\n".to_vec());
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
        write_script(
            &stub,
            r#"#!/usr/bin/env bash
cat >/dev/null
echo '{"type":"item.completed","item":{"type":"agent_message","text":"hello from repl"}}'
"#,
        );

        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), stubbed_codex_runtime(&stub));
        config.save(&home).await.expect("save config");
        write_codex_runtime_auth(&home).await;

        let mut first_input = Cursor::new(b"hello\n/exit\n".to_vec());
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

        let mut second_input = Cursor::new(b"/exit\n".to_vec());
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
        config.save(&home).await.expect("save config");
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

        let mut input = Cursor::new(b"/exit\n".to_vec());
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
        config.save(&home).await.expect("save config");
        write_codex_runtime_auth(&home).await;

        let mut input = Cursor::new(b"/reset\n/exit\n".to_vec());
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
        config.save(&home).await.expect("save config");
        write_codex_runtime_auth(&home).await;

        let mut input = Cursor::new(b"/exit\n".to_vec());
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

        assert!(err
            .to_string()
            .contains("runtime is required when no default runtime is configured"));
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
        config.save(&home).await.expect("save config");

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
        let skill_source = temp_dir.path().join("test-skill");
        fs::create_dir_all(&skill_source).expect("skill dir");
        fs::write(
            skill_source.join("SKILL.md"),
            "---\nname: test-skill\ndescription: test\n---\n",
        )
        .expect("skill md");
        config.skills.push(ManagedSkillConfig {
            alias: "test-skill".to_string(),
            source: skill_source.display().to_string(),
            reference: "local".to_string(),
            enabled: true,
        });
        config.save(&home).await.expect("save config");

        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();
        let err = run_local_with_io(&home, None, false, &mut input, &mut output)
            .await
            .expect_err("missing runtime auth should error");

        assert!(err.to_string().contains("codex login"));
        assert!(err.to_string().contains("auth.json"));
        let lockfile = OperatorLockfile::load(&home).await.expect("load lockfile");
        assert!(
            lockfile.skills.is_empty(),
            "auth preflight should fail before apply() installs skill snapshots"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_local_streams_codex_reasoning_and_answer_lanes() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let stub = temp_dir.path().join("codex-stub.sh");
        write_script(
            &stub,
            r#"#!/usr/bin/env bash
cat >/dev/null
echo '{"type":"item.updated","item":{"type":"reasoning","text":"planning next step"}}'
echo '{"type":"item.completed","item":{"type":"agent_message","text":"hello from codex"}}'
"#,
        );

        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), stubbed_codex_runtime(&stub));
        config.save(&home).await.expect("save config");
        write_codex_runtime_auth(&home).await;

        let mut input = Cursor::new(b"hello\n/exit\n".to_vec());
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
        config.save(&home).await.expect("save config");

        let mut input = Cursor::new(b"hello\n/exit\n".to_vec());
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
        config.save(&home).await.expect("save config");

        let mut input = Cursor::new(b"hello\n/exit\n".to_vec());
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
        config.save(&home).await.expect("save config");

        let mut input = Cursor::new(b"hello\n/exit\n".to_vec());
        let mut output = Vec::new();
        run_local_with_io(&home, None, false, &mut input, &mut output)
            .await
            .expect("run local");

        let output = String::from_utf8(output).expect("utf8 output");
        assert!(output.contains("thinking> checking the workspace"));
        assert!(output.contains("Runtime error. Partial output is shown above."));
        assert!(output.contains("Use /retry or /reset."));
        assert!(!output.contains("Use /continue, /retry, or /reset."));
        assert!(!output.contains("Timed out. Partial output is shown above."));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_local_reports_reasoning_only_runtime_failures_as_partial_output() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let stub = temp_dir.path().join("codex-stub.sh");
        write_script(
            &stub,
            r#"#!/usr/bin/env bash
cat >/dev/null
echo '{"type":"item.updated","item":{"type":"reasoning","text":"checking the workspace"}}'
exit 7
"#,
        );

        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), stubbed_codex_runtime(&stub));
        config.save(&home).await.expect("save config");
        write_codex_runtime_auth(&home).await;

        let mut input = Cursor::new(b"hello\n/exit\n".to_vec());
        let mut output = Vec::new();
        run_local_with_io(&home, None, false, &mut input, &mut output)
            .await
            .expect("run local");

        let output = String::from_utf8(output).expect("utf8 output");
        assert!(output.contains("thinking> checking the workspace"));
        assert!(output.contains("Runtime error. Partial output is shown above."));
        assert!(output.contains("Use /retry or /reset."));
        assert!(!output.contains("Use /continue, /retry, or /reset."));
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
                },
                StreamEventDto {
                    kind: StreamEventKindDto::MessageDelta,
                    lane: Some(StreamLaneDto::Reasoning),
                    code: None,
                    text: Some("planning next step".to_string()),
                },
                StreamEventDto {
                    kind: StreamEventKindDto::MessageDelta,
                    lane: Some(StreamLaneDto::Answer),
                    code: None,
                    text: Some("hello\nworld".to_string()),
                },
                StreamEventDto {
                    kind: StreamEventKindDto::Error,
                    lane: None,
                    code: None,
                    text: Some("something failed".to_string()),
                },
                StreamEventDto {
                    kind: StreamEventKindDto::Done,
                    lane: None,
                    code: None,
                    text: None,
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
    fn ensure_fake_podman(reference: &std::path::Path) -> std::path::PathBuf {
        let engine = reference.parent().expect("stub parent").join("podman");
        if !engine.exists() {
            write_script(
                &engine,
                r#"#!/usr/bin/env bash
set -euo pipefail

command_name="${1:-}"
shift || true

case "${command_name}" in
  secret)
    exit 0
    ;;
  pod)
    exit 0
    ;;
  run)
    sidecar_state_dir=""
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --rm|--interactive|--read-only|--detach)
          shift
          ;;
        --network|--workdir|--tmpfs|--env|--secret|--memory|--cpus|--pids-limit|--pod|--name)
          shift 2
          ;;
        --volume)
          case "$2" in
            *:/lionclaw-auth-sidecar:rw)
              sidecar_state_dir="${2%%:*}"
              ;;
          esac
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
    if [ "$#" -eq 0 ] && [ -n "$sidecar_state_dir" ]; then
      setsid python3 -c 'import os, pathlib, socket, sys; state_dir = sys.argv[1]; socket_path = os.path.join(state_dir, "admin.sock"); pathlib.Path(socket_path).unlink(missing_ok=True); server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM); server.bind(socket_path); server.listen(1); conn, _ = server.accept(); conn.recv(1024); conn.sendall(b"Name: fake-haproxy\n"); conn.close(); server.close()' "$sidecar_state_dir" >/dev/null 2>&1 < /dev/null &
      exit 0
    fi
    if [ "$#" -eq 1 ]; then
      exit 0
    fi
    exec "$@"
    ;;
  *)
    exit 0
    ;;
esac
"#,
            );
        }
        engine
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
