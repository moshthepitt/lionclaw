use std::{fmt, io::ErrorKind, process::Stdio, time::Duration};

use anyhow::{Context, Result};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::Command,
};

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

#[derive(Debug, Clone, Default)]
pub struct ProcessOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: Option<i32>,
}

impl ProcessOutput {
    pub fn success(&self) -> bool {
        self.exit_code == Some(0)
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

    Ok(ProcessOutput {
        stdout: captured_stdout,
        stderr: captured_stderr,
        exit_code: status.code(),
    })
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
                        format!("failed to spawn subprocess executable '{}'", executable)
                    });
                }
                tokio::time::sleep(Duration::from_millis(ETXTBUSY_BACKOFF_MS)).await;
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to spawn subprocess executable '{}'", executable)
                });
            }
        }
    }

    unreachable!("spawn_with_retry should return or error within retry loop")
}

#[cfg(test)]
mod tests {
    use super::ProcessInvocation;

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
}
