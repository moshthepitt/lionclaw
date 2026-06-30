use std::{fmt, io::ErrorKind, process::Stdio, time::Duration};

use anyhow::{Context, Result};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command},
};

pub use lionclaw_runtime_api::ExecutionOutput as ProcessOutput;

#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;

#[cfg(unix)]
use nix::sys::signal::{self, SigHandler, Signal};

#[derive(Clone)]
pub struct ProcessInvocation {
    pub executable: String,
    pub args: Vec<String>,
    pub working_dir: Option<String>,
    pub environment: Vec<(String, String)>,
    pub input: String,
}

impl fmt::Debug for ProcessInvocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProcessInvocation")
            .field("executable", &self.executable)
            .field("args", &self.args)
            .field("working_dir", &self.working_dir)
            .field("environment_count", &self.environment.len())
            .field("input_len", &self.input.len())
            .finish()
    }
}

pub async fn run_process_streaming<F>(
    invocation: &ProcessInvocation,
    mut on_stdout_line: F,
) -> Result<ProcessOutput>
where
    F: FnMut(&str) -> Result<()>,
{
    let mut command = Command::new(&invocation.executable);
    command.args(&invocation.args);

    if let Some(working_dir) = invocation.working_dir.as_deref() {
        command.current_dir(working_dir);
    }
    if !invocation.environment.is_empty() {
        command.envs(
            invocation
                .environment
                .iter()
                .map(|(key, value)| (key, value)),
        );
    }
    command.kill_on_drop(true);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = spawn_with_retry(&mut command, &invocation.executable).await?;

    if let Some(stdin) = child.stdin.take() {
        write_input_and_close_stdin(stdin, invocation.input.as_bytes()).await?;
    }

    let stdout = child
        .stdout
        .take()
        .context("subprocess stdout was not captured")?;
    let stderr = child
        .stderr
        .take()
        .context("subprocess stderr was not captured")?;

    let stderr_task = tokio::spawn(async move {
        let mut stderr = stderr;
        let mut captured = Vec::new();
        stderr
            .read_to_end(&mut captured)
            .await
            .context("failed to read subprocess stderr")?;
        Ok::<Vec<u8>, anyhow::Error>(captured)
    });

    let mut stdout_reader = BufReader::new(stdout);
    let mut captured_stdout = Vec::new();
    let mut line_buffer = Vec::new();

    loop {
        let Some(line) =
            read_next_process_line(&mut stdout_reader, &mut line_buffer, &mut captured_stdout)
                .await?
        else {
            break;
        };
        on_stdout_line(&line)?;
    }

    let status = child
        .wait()
        .await
        .context("failed to wait for subprocess")?;
    let captured_stderr = stderr_task.await.context("stderr reader task failed")??;

    Ok(ProcessOutput {
        stdout: captured_stdout,
        stderr: captured_stderr,
        exit_code: status.code(),
        exit_signal: exit_signal(&status),
    })
}

pub async fn run_process_attached(invocation: &ProcessInvocation) -> Result<ProcessOutput> {
    let mut command = Command::new(&invocation.executable);
    command.args(&invocation.args);

    if let Some(working_dir) = invocation.working_dir.as_deref() {
        command.current_dir(working_dir);
    }
    if !invocation.environment.is_empty() {
        command.envs(
            invocation
                .environment
                .iter()
                .map(|(key, value)| (key, value)),
        );
    }
    command.kill_on_drop(true);
    command
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    let mut child = spawn_with_retry(&mut command, &invocation.executable).await?;
    let status = wait_for_attached_child(&mut child).await?;

    Ok(ProcessOutput {
        stdout: Vec::new(),
        stderr: Vec::new(),
        exit_code: status.code(),
        exit_signal: exit_signal(&status),
    })
}

pub struct ProcessSession {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout_reader: BufReader<ChildStdout>,
    stdout_line_buffer: Vec<u8>,
    stderr_task: tokio::task::JoinHandle<Result<Vec<u8>>>,
    captured_stdout: Vec<u8>,
}

impl ProcessSession {
    pub async fn write_line(&mut self, line: &str) -> Result<()> {
        let stdin = self
            .stdin
            .as_mut()
            .context("subprocess stdin is already closed")?;
        stdin
            .write_all(line.as_bytes())
            .await
            .context("failed to write line to subprocess stdin")?;
        stdin
            .write_all(b"\n")
            .await
            .context("failed to write line terminator to subprocess stdin")?;
        stdin
            .flush()
            .await
            .context("failed to flush subprocess stdin")?;
        Ok(())
    }

    pub async fn read_line(&mut self) -> Result<Option<String>> {
        read_next_process_line(
            &mut self.stdout_reader,
            &mut self.stdout_line_buffer,
            &mut self.captured_stdout,
        )
        .await
    }

    pub async fn close_stdin(&mut self) -> Result<()> {
        let Some(stdin) = self.stdin.take() else {
            return Ok(());
        };
        shutdown_child_stdin(stdin).await
    }

    pub async fn wait(mut self) -> Result<ProcessOutput> {
        self.close_stdin().await?;
        self.stdout_reader
            .read_to_end(&mut self.captured_stdout)
            .await
            .context("failed to read remaining subprocess stdout")?;
        let status = self
            .child
            .wait()
            .await
            .context("failed to wait for subprocess")?;
        let captured_stderr = self
            .stderr_task
            .await
            .context("stderr reader task failed")??;

        Ok(ProcessOutput {
            stdout: self.captured_stdout,
            stderr: captured_stderr,
            exit_code: status.code(),
            exit_signal: exit_signal(&status),
        })
    }
}

#[cfg(unix)]
async fn wait_for_attached_child(child: &mut Child) -> Result<std::process::ExitStatus> {
    // The child remains in the foreground process group, so terminal-generated
    // interrupts already reach it. Consume the parent's copy so LionClaw can
    // run exit reconciliation after the native UI finishes.
    let _guard = AttachedSignalGuard::install()?;
    child.wait().await.context("failed to wait for subprocess")
}

#[cfg(not(unix))]
async fn wait_for_attached_child(child: &mut Child) -> Result<std::process::ExitStatus> {
    child.wait().await.context("failed to wait for subprocess")
}

#[cfg(unix)]
fn exit_signal(status: &std::process::ExitStatus) -> Option<i32> {
    status.signal()
}

#[cfg(not(unix))]
fn exit_signal(_status: &std::process::ExitStatus) -> Option<i32> {
    None
}

#[cfg(unix)]
struct AttachedSignalGuard {
    previous: Vec<(Signal, SigHandler)>,
}

#[cfg(unix)]
impl AttachedSignalGuard {
    fn install() -> Result<Self> {
        let mut guard = Self {
            previous: Vec::new(),
        };
        guard.ignore(Signal::SIGINT)?;
        guard.ignore(Signal::SIGQUIT)?;
        Ok(guard)
    }

    fn ignore(&mut self, signal: Signal) -> Result<()> {
        let previous = set_signal_handler(signal, SigHandler::SigIgn)?;
        self.previous.push((signal, previous));
        Ok(())
    }
}

#[cfg(unix)]
impl Drop for AttachedSignalGuard {
    fn drop(&mut self) {
        for (signal, handler) in self.previous.drain(..).rev() {
            drop(set_signal_handler(signal, handler));
        }
    }
}

#[cfg(unix)]
#[allow(
    unsafe_code,
    reason = "scoped terminal signal disposition requires sigaction through nix"
)]
fn set_signal_handler(signal: Signal, handler: SigHandler) -> Result<SigHandler> {
    // SAFETY: This sets a process-level disposition to SIG_IGN or restores a
    // previously returned disposition for terminal-generated signals.
    unsafe { signal::signal(signal, handler) }.context("failed to update signal handler")
}

pub async fn spawn_process_session(invocation: &ProcessInvocation) -> Result<ProcessSession> {
    let mut command = Command::new(&invocation.executable);
    command.args(&invocation.args);

    if let Some(working_dir) = invocation.working_dir.as_deref() {
        command.current_dir(working_dir);
    }
    if !invocation.environment.is_empty() {
        command.envs(
            invocation
                .environment
                .iter()
                .map(|(key, value)| (key, value)),
        );
    }
    command.kill_on_drop(true);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = spawn_with_retry(&mut command, &invocation.executable).await?;
    let mut stdin = child
        .stdin
        .take()
        .context("subprocess stdin was not captured")?;
    if !invocation.input.is_empty() {
        stdin
            .write_all(invocation.input.as_bytes())
            .await
            .context("failed to write input to subprocess stdin")?;
        stdin
            .flush()
            .await
            .context("failed to flush subprocess stdin")?;
    }
    let stdout = child
        .stdout
        .take()
        .context("subprocess stdout was not captured")?;
    let stderr = child
        .stderr
        .take()
        .context("subprocess stderr was not captured")?;

    Ok(ProcessSession {
        child,
        stdin: Some(stdin),
        stdout_reader: BufReader::new(stdout),
        stdout_line_buffer: Vec::new(),
        stderr_task: spawn_stderr_reader(stderr),
        captured_stdout: Vec::new(),
    })
}

async fn read_next_process_line<R>(
    reader: &mut R,
    line_buffer: &mut Vec<u8>,
    captured_stdout: &mut Vec<u8>,
) -> Result<Option<String>>
where
    R: AsyncBufRead + Unpin,
{
    let bytes_read = reader
        .read_until(b'\n', line_buffer)
        .await
        .context("failed to read subprocess stdout")?;
    if bytes_read == 0 && line_buffer.is_empty() {
        return Ok(None);
    }

    captured_stdout.extend_from_slice(line_buffer);
    let line = String::from_utf8_lossy(line_buffer).to_string();
    line_buffer.clear();
    Ok(Some(line))
}

fn spawn_stderr_reader(mut stderr: ChildStderr) -> tokio::task::JoinHandle<Result<Vec<u8>>> {
    tokio::spawn(async move {
        let mut captured = Vec::new();
        stderr
            .read_to_end(&mut captured)
            .await
            .context("failed to read subprocess stderr")?;
        Ok::<Vec<u8>, anyhow::Error>(captured)
    })
}

async fn write_input_and_close_stdin(mut stdin: ChildStdin, input: &[u8]) -> Result<()> {
    if let Err(err) = stdin.write_all(input).await {
        if err.kind() == ErrorKind::BrokenPipe {
            return Ok(());
        }
        return Err(err).context("failed to write input to subprocess stdin");
    }
    shutdown_child_stdin(stdin).await
}

async fn shutdown_child_stdin(mut stdin: ChildStdin) -> Result<()> {
    if let Err(err) = stdin.shutdown().await {
        if err.kind() == ErrorKind::BrokenPipe {
            return Ok(());
        }
        return Err(err).context("failed to close subprocess stdin");
    }
    Ok(())
}

async fn spawn_with_retry(
    command: &mut Command,
    executable: &str,
) -> Result<tokio::process::Child> {
    const ETXTBUSY_RETRIES: usize = 3;
    const ETXTBUSY_BACKOFF_MS: u64 = 10;

    for attempt in 0..=ETXTBUSY_RETRIES {
        match command.spawn() {
            Ok(child) => return Ok(child),
            Err(err)
                if err.kind() == ErrorKind::ExecutableFileBusy
                    || err.raw_os_error() == Some(26) =>
            {
                if attempt == ETXTBUSY_RETRIES {
                    return Err(err).with_context(|| {
                        format!("failed to spawn subprocess executable '{executable}'")
                    });
                }
                tokio::time::sleep(Duration::from_millis(ETXTBUSY_BACKOFF_MS)).await;
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to spawn subprocess executable '{executable}'")
                });
            }
        }
    }

    unreachable!("spawn_with_retry should return or error within retry loop")
}

#[cfg(test)]
mod tests {
    use super::{
        read_next_process_line, run_process_attached, run_process_streaming, spawn_process_session,
        ProcessInvocation,
    };
    use tokio::io::AsyncWriteExt;

    #[test]
    fn process_invocation_debug_redacts_environment_and_input_values() {
        let debug = format!(
            "{:?}",
            ProcessInvocation {
                executable: "podman".to_string(),
                args: vec!["run".to_string()],
                working_dir: None,
                environment: vec![("GITHUB_TOKEN".to_string(), "ghp_secret".to_string())],
                input: "sensitive stdin".to_string(),
            }
        );

        assert!(debug.contains("environment_count"));
        assert!(!debug.contains("ghp_secret"));
        assert!(!debug.contains("sensitive stdin"));
    }

    #[tokio::test]
    async fn process_line_reader_preserves_partial_line_after_cancelled_read() {
        let (reader, mut writer) = tokio::io::duplex(64);
        let mut reader = tokio::io::BufReader::new(reader);
        let mut line_buffer = Vec::new();
        let mut captured_stdout = Vec::new();

        writer
            .write_all(b"{\"jsonrpc\"")
            .await
            .expect("write partial");

        {
            let read = read_next_process_line(&mut reader, &mut line_buffer, &mut captured_stdout);
            tokio::pin!(read);
            tokio::select! {
                result = &mut read => panic!("partial line completed unexpectedly: {result:?}"),
                _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
            }
        }

        assert_eq!(line_buffer, b"{\"jsonrpc\"");
        assert!(captured_stdout.is_empty());

        writer
            .write_all(b":\"2.0\"}\n")
            .await
            .expect("write line terminator");

        let line = read_next_process_line(&mut reader, &mut line_buffer, &mut captured_stdout)
            .await
            .expect("read line")
            .expect("line");
        assert_eq!(line, "{\"jsonrpc\":\"2.0\"}\n");
        assert_eq!(captured_stdout, line.as_bytes());
    }

    #[tokio::test]
    async fn process_session_wait_drains_stdout_written_after_stdin_closes() {
        let session = spawn_process_session(&ProcessInvocation {
            executable: "/bin/sh".to_string(),
            args: vec![
                "-c".to_string(),
                "cat >/dev/null; printf 'tail-after-close\\n'".to_string(),
            ],
            working_dir: None,
            environment: Vec::new(),
            input: String::new(),
        })
        .await
        .expect("spawn session");

        let output = session.wait().await.expect("wait");

        assert!(output.success());
        assert_eq!(
            String::from_utf8(output.stdout).expect("stdout"),
            "tail-after-close\n"
        );
    }

    #[tokio::test]
    async fn process_session_wait_collects_status_after_child_closes_stdin() {
        let session = spawn_process_session(&ProcessInvocation {
            executable: "/bin/sh".to_string(),
            args: vec![
                "-c".to_string(),
                "exec 0<&-; printf 'closed-session-stdin\\n'; printf 'session-details\\n' >&2; exit 7"
                    .to_string(),
            ],
            working_dir: None,
            environment: Vec::new(),
            input: String::new(),
        })
        .await
        .expect("spawn session");

        let output = session.wait().await.expect("wait");

        assert_eq!(output.exit_code, Some(7));
        assert_eq!(
            String::from_utf8(output.stdout).expect("stdout"),
            "closed-session-stdin\n"
        );
        assert_eq!(
            String::from_utf8(output.stderr).expect("stderr"),
            "session-details\n"
        );
    }

    #[tokio::test]
    async fn streaming_process_collects_status_after_child_closes_stdin() {
        let output = run_process_streaming(
            &ProcessInvocation {
                executable: "/bin/sh".to_string(),
                args: vec![
                    "-c".to_string(),
                    "exec 0<&-; printf 'closed-stdin\\n'; printf 'details\\n' >&2; exit 7"
                        .to_string(),
                ],
                working_dir: None,
                environment: Vec::new(),
                input: "ignored\n".repeat(1024 * 1024),
            },
            |_| Ok(()),
        )
        .await
        .expect("run process");

        assert_eq!(output.exit_code, Some(7));
        assert_eq!(
            String::from_utf8(output.stdout).expect("stdout"),
            "closed-stdin\n"
        );
        assert_eq!(
            String::from_utf8(output.stderr).expect("stderr"),
            "details\n"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn attached_process_records_signal_exit_status() {
        let output = run_process_attached(&ProcessInvocation {
            executable: "/bin/sh".to_string(),
            args: vec!["-c".to_string(), "kill -TERM $$".to_string()],
            working_dir: None,
            environment: Vec::new(),
            input: String::new(),
        })
        .await
        .expect("run attached");

        assert_eq!(output.exit_code, None);
        assert_eq!(output.exit_signal, Some(15));
        assert_eq!(output.status_description(), "signal 15");
    }
}
