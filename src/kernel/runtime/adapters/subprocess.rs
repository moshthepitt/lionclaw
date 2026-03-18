use std::process::Stdio;

use anyhow::{Context, Result};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::Command,
};

#[derive(Debug, Clone)]
pub struct SubprocessInvocation {
    pub executable: String,
    pub args: Vec<String>,
    pub working_dir: Option<String>,
    pub environment: Vec<(String, String)>,
    pub input: String,
}

#[derive(Debug, Clone)]
pub struct SubprocessOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: Option<i32>,
}

impl SubprocessOutput {
    pub fn success(&self) -> bool {
        self.exit_code == Some(0)
    }
}

pub async fn run_non_interactive_streaming<F>(
    invocation: &SubprocessInvocation,
    mut on_stdout_line: F,
) -> Result<SubprocessOutput>
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

    let mut child = command.spawn().with_context(|| {
        format!(
            "failed to spawn subprocess executable '{}'",
            invocation.executable
        )
    })?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(invocation.input.as_bytes())
            .await
            .context("failed to write input to subprocess stdin")?;
        stdin
            .shutdown()
            .await
            .context("failed to close subprocess stdin")?;
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
    let mut line = Vec::new();

    loop {
        line.clear();
        let bytes_read = stdout_reader
            .read_until(b'\n', &mut line)
            .await
            .context("failed to read subprocess stdout")?;
        if bytes_read == 0 {
            break;
        }
        captured_stdout.extend_from_slice(&line);
        let text = String::from_utf8_lossy(&line);
        on_stdout_line(text.as_ref())?;
    }

    let status = child
        .wait()
        .await
        .context("failed to wait for subprocess")?;
    let captured_stderr = stderr_task.await.context("stderr reader task failed")??;

    Ok(SubprocessOutput {
        stdout: captured_stdout,
        stderr: captured_stderr,
        exit_code: status.code(),
    })
}
