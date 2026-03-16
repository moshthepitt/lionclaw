use std::io::{BufRead, Write};

use anyhow::{anyhow, Result};

use crate::{
    contracts::{SessionOpenRequest, SessionTurnRequest, TrustTier},
    home::LionClawHome,
    kernel::Kernel,
    operator::{
        reconcile::{apply, onboard, open_kernel, render_runtime_cache},
        runtime::{resolve_runtime_id, validate_runtime_availability},
    },
};

pub async fn run_local(home: &LionClawHome, requested_runtime: Option<String>) -> Result<()> {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut input = stdin.lock();
    let mut output = stdout.lock();
    run_local_with_io(home, requested_runtime, &mut input, &mut output).await
}

pub(crate) async fn run_local_with_io<R: BufRead, W: Write>(
    home: &LionClawHome,
    requested_runtime: Option<String>,
    input: &mut R,
    output: &mut W,
) -> Result<()> {
    onboard(home).await?;
    let applied = apply(home).await?;
    let runtime_id = resolve_runtime_id(&applied.config, requested_runtime.as_deref())?;
    validate_runtime_availability(&applied.config, &runtime_id)?;
    render_runtime_cache(home, &applied.config, &applied.lockfile, &runtime_id).await?;

    let kernel = open_kernel(home, &applied.config, Some(runtime_id.clone())).await?;
    let workspace = applied.config.daemon.workspace.clone();
    let peer_id = local_peer_id();
    run_repl(&kernel, &runtime_id, &workspace, &peer_id, input, output).await
}

async fn run_repl<R: BufRead, W: Write>(
    kernel: &Kernel,
    runtime_id: &str,
    workspace: &str,
    peer_id: &str,
    input: &mut R,
    output: &mut W,
) -> Result<()> {
    let session = kernel
        .open_session(SessionOpenRequest {
            channel_id: "local-cli".to_string(),
            peer_id: peer_id.to_string(),
            trust_tier: TrustTier::Main,
        })
        .await
        .map_err(kernel_to_anyhow)?;

    writeln!(
        output,
        "LionClaw interactive mode\nruntime: {}\nworkspace: {}\nType /exit to quit.\n",
        runtime_id, workspace
    )?;

    loop {
        write!(output, "lionclaw> ")?;
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

        match kernel
            .turn_session(SessionTurnRequest {
                session_id: session.session_id,
                user_text: trimmed.to_string(),
                runtime_id: Some(runtime_id.to_string()),
                runtime_working_dir: None,
                runtime_timeout_ms: None,
                runtime_env_passthrough: None,
            })
            .await
        {
            Ok(turn) => {
                if !turn.assistant_text.trim().is_empty() {
                    writeln!(output, "{}", turn.assistant_text.trim_end())?;
                }
            }
            Err(err) => {
                writeln!(output, "error: {}", err)?;
            }
        }
    }

    Ok(())
}

fn local_peer_id() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "local-user".to_string())
}

fn kernel_to_anyhow(err: crate::kernel::KernelError) -> anyhow::Error {
    anyhow!(err.to_string())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Cursor;

    use sqlx::{Row, SqlitePool};

    use super::run_local_with_io;
    use crate::{
        home::LionClawHome,
        operator::config::{OperatorConfig, RuntimeProfileConfig},
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
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: stub.to_string_lossy().to_string(),
                model: None,
                sandbox: "read-only".to_string(),
                skip_git_repo_check: true,
                ephemeral: true,
            },
        );
        config.save(&home).await.expect("save config");

        let mut input = Cursor::new(b"hello\n/exit\n".to_vec());
        let mut output = Vec::new();
        run_local_with_io(&home, None, &mut input, &mut output)
            .await
            .expect("run local");

        let output = String::from_utf8(output).expect("utf8 output");
        assert!(output.contains("hello from repl"));
        assert!(home.workspace_dir("main").join("SOUL.md").exists());
        assert!(home.lock_path().exists());

        let db_url = format!("sqlite://{}", home.db_path().display());
        let pool = SqlitePool::connect(&db_url).await.expect("connect db");
        let row = sqlx::query("SELECT COUNT(*) AS count FROM sessions")
            .fetch_one(&pool)
            .await
            .expect("fetch count");
        let count: i64 = row.get("count");
        assert_eq!(count, 1, "run should create one kernel session");
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
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: stub.to_string_lossy().to_string(),
                model: None,
                sandbox: "read-only".to_string(),
                skip_git_repo_check: true,
                ephemeral: true,
            },
        );
        config.save(&home).await.expect("save config");

        let mut input = Cursor::new(b"/exit\n".to_vec());
        let mut output = Vec::new();
        run_local_with_io(&home, None, &mut input, &mut output)
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
        let err = run_local_with_io(&home, None, &mut input, &mut output)
            .await
            .expect_err("missing runtime should error");

        assert!(err
            .to_string()
            .contains("runtime is required when no default runtime is configured"));
    }

    #[tokio::test]
    async fn run_local_errors_when_runtime_executable_is_missing() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));

        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: temp_dir
                    .path()
                    .join("missing-codex")
                    .to_string_lossy()
                    .to_string(),
                model: None,
                sandbox: "read-only".to_string(),
                skip_git_repo_check: true,
                ephemeral: true,
            },
        );
        config.save(&home).await.expect("save config");

        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();
        let err = run_local_with_io(&home, None, &mut input, &mut output)
            .await
            .expect_err("missing executable should error");

        assert!(err.to_string().contains("configured runtime command"));
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
}
