use std::process::Stdio;

use anyhow::{Context, Result};
use tokio::{io::AsyncWriteExt, process::Command};

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

pub async fn run_non_interactive(invocation: &SubprocessInvocation) -> Result<SubprocessOutput> {
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

    let output = child
        .wait_with_output()
        .await
        .context("failed to wait for subprocess")?;

    Ok(SubprocessOutput {
        stdout: output.stdout,
        stderr: output.stderr,
        exit_code: output.status.code(),
    })
}
