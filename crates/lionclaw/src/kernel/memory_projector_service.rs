use std::{
    cmp,
    ffi::OsString,
    fs,
    path::{Component, Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command},
    sync::Mutex,
};

use crate::applied::{AppliedMemoryProjector, AppliedState};

use super::memory_projection::{
    validate_memory_projection, MemoryProjection, MemoryProjectionError, MemoryProjectionRequest,
    MemoryProjector, NoopMemoryProjector,
};

pub(crate) const MEMORY_PROJECTOR_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_PROJECTOR_STDERR_BYTES: usize = 4096;
const MAX_PROJECTOR_RESPONSE_LINE_BYTES: usize = 64 * 1024;

pub(crate) fn memory_projector_for_applied_state(
    applied_state: &AppliedState,
) -> Arc<dyn MemoryProjector> {
    match applied_state.memory_projector() {
        Some(projector) => Arc::new(SkillMemoryProjector::from_applied(projector)),
        None => Arc::new(NoopMemoryProjector),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SkillMemoryProjectorConfig {
    projector_id: String,
    command_path: PathBuf,
    skill_root: PathBuf,
    state_dir: PathBuf,
    request_timeout: Duration,
}

impl SkillMemoryProjectorConfig {
    pub(crate) fn from_applied(projector: &AppliedMemoryProjector) -> Self {
        Self {
            projector_id: projector.skill_alias.clone(),
            command_path: projector.command_path.clone(),
            skill_root: projector.skill_root.clone(),
            state_dir: projector.state_dir.clone(),
            request_timeout: MEMORY_PROJECTOR_REQUEST_TIMEOUT,
        }
    }

    #[cfg(test)]
    fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = request_timeout;
        self
    }
}

pub(crate) struct SkillMemoryProjector {
    config: SkillMemoryProjectorConfig,
    process: Mutex<Option<ResidentMemoryProjectorProcess>>,
}

impl SkillMemoryProjector {
    pub(crate) fn from_applied(projector: &AppliedMemoryProjector) -> Self {
        Self::new(SkillMemoryProjectorConfig::from_applied(projector))
    }

    pub(crate) fn new(config: SkillMemoryProjectorConfig) -> Self {
        Self {
            config,
            process: Mutex::new(None),
        }
    }

    async fn project_with_process(
        &self,
        process: &mut Option<ResidentMemoryProjectorProcess>,
        request: MemoryProjectionRequest,
    ) -> Result<MemoryProjection, MemoryProjectionError> {
        if process.is_none() {
            *process = Some(self.spawn_process().await?);
        }
        let response = {
            let Some(process) = process.as_mut() else {
                return Err(MemoryProjectionError::failed(
                    "memory projector process was not available after spawn",
                ));
            };
            let request_json = serde_json::to_string(&request)
                .map_err(|err| MemoryProjectionError::failed(format!("encode request: {err}")))?;
            process.write_request(&request_json).await?;
            process.read_response().await?
        };
        let projection = serde_json::from_str::<MemoryProjection>(&response)
            .map_err(|err| MemoryProjectionError::failed(format!("decode response: {err}")))?;
        if validate_memory_projection(&request, &self.config.projector_id, &projection).is_err() {
            Self::retire_process(process).await;
        }
        Ok(projection)
    }

    async fn spawn_process(&self) -> Result<ResidentMemoryProjectorProcess, MemoryProjectionError> {
        ensure_private_state_dir(&self.config.state_dir)
            .map_err(|err| MemoryProjectionError::failed(format!("state directory: {err}")))?;
        let mut command = Command::new(&self.config.command_path);
        command
            .current_dir(&self.config.skill_root)
            .kill_on_drop(true)
            .env_clear()
            .envs(projector_ambient_env())
            .env("LIONCLAW_MEMORY_PROJECTOR_ID", &self.config.projector_id)
            .env("LIONCLAW_SKILL_STATE_DIR", &self.config.state_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().map_err(|err| {
            MemoryProjectionError::failed(format!(
                "spawn memory projector {}: {err}",
                self.config.command_path.display()
            ))
        })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| MemoryProjectionError::failed("memory projector stdin missing"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| MemoryProjectionError::failed("memory projector stdout missing"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| MemoryProjectionError::failed("memory projector stderr missing"))?;

        Ok(ResidentMemoryProjectorProcess {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            stderr: spawn_bounded_stderr_reader(stderr),
        })
    }

    async fn retire_process(process: &mut Option<ResidentMemoryProjectorProcess>) {
        if let Some(process) = process.take() {
            process.retire().await;
        }
    }
}

#[async_trait::async_trait]
impl MemoryProjector for SkillMemoryProjector {
    fn projector_id(&self) -> &str {
        &self.config.projector_id
    }

    async fn project(
        &self,
        request: MemoryProjectionRequest,
    ) -> Result<MemoryProjection, MemoryProjectionError> {
        let mut process = self.process.lock().await;
        let result = tokio::time::timeout(
            self.config.request_timeout,
            self.project_with_process(&mut process, request),
        )
        .await;
        match result {
            Ok(Ok(projection)) => Ok(projection),
            Ok(Err(err)) => {
                Self::retire_process(&mut process).await;
                Err(err)
            }
            Err(_) => {
                Self::retire_process(&mut process).await;
                Err(MemoryProjectionError::failed(
                    "memory projector request timed out",
                ))
            }
        }
    }
}

struct ResidentMemoryProjectorProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    stderr: tokio::task::JoinHandle<Vec<u8>>,
}

impl ResidentMemoryProjectorProcess {
    async fn write_request(&mut self, request_json: &str) -> Result<(), MemoryProjectionError> {
        self.stdin
            .write_all(request_json.as_bytes())
            .await
            .map_err(|err| MemoryProjectionError::failed(format!("write request: {err}")))?;
        self.stdin.write_all(b"\n").await.map_err(|err| {
            MemoryProjectionError::failed(format!("write request newline: {err}"))
        })?;
        self.stdin
            .flush()
            .await
            .map_err(|err| MemoryProjectionError::failed(format!("flush request: {err}")))
    }

    async fn read_response(&mut self) -> Result<String, MemoryProjectionError> {
        let line = read_capped_line(&mut self.stdout, MAX_PROJECTOR_RESPONSE_LINE_BYTES).await?;
        let Some(line) = line else {
            return Err(MemoryProjectionError::failed(
                "memory projector stdout closed",
            ));
        };
        String::from_utf8(line)
            .map(|line| line.trim_end_matches(['\r', '\n']).to_string())
            .map_err(|err| MemoryProjectionError::failed(format!("response was not UTF-8: {err}")))
    }

    async fn retire(mut self) {
        drop(self.child.start_kill());
        drop(self.child.wait().await);
        self.stderr.abort();
        drop(self.stderr.await);
    }
}

async fn read_capped_line(
    reader: &mut BufReader<ChildStdout>,
    max_bytes: usize,
) -> Result<Option<Vec<u8>>, MemoryProjectionError> {
    let mut line = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .await
            .map_err(|err| MemoryProjectionError::failed(format!("read response: {err}")))?;
        if available.is_empty() {
            if line.is_empty() {
                return Ok(None);
            }
            return Err(MemoryProjectionError::failed(
                "memory projector response ended without newline",
            ));
        }
        if let Some(newline_index) = available.iter().position(|byte| *byte == b'\n') {
            let take = newline_index + 1;
            if line.len().saturating_add(take) > max_bytes {
                return Err(MemoryProjectionError::failed(
                    "memory projector response exceeded byte limit",
                ));
            }
            line.extend_from_slice(&available[..take]);
            reader.consume(take);
            return Ok(Some(line));
        }
        if line.len().saturating_add(available.len()) > max_bytes {
            return Err(MemoryProjectionError::failed(
                "memory projector response exceeded byte limit",
            ));
        }
        let take = available.len();
        line.extend_from_slice(available);
        reader.consume(take);
    }
}

fn spawn_bounded_stderr_reader(mut stderr: ChildStderr) -> tokio::task::JoinHandle<Vec<u8>> {
    tokio::spawn(async move {
        let mut captured = Vec::new();
        let mut buffer = [0u8; 1024];
        loop {
            let read = match stderr.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(read) => read,
            };
            let remaining = MAX_PROJECTOR_STDERR_BYTES.saturating_sub(captured.len());
            if remaining > 0 {
                let take = cmp::min(read, remaining);
                captured.extend_from_slice(&buffer[..take]);
            }
        }
        captured
    })
}

fn projector_ambient_env() -> Vec<(String, OsString)> {
    let mut env = Vec::new();
    for key in ["PATH", "PATHEXT", "SYSTEMROOT", "SystemRoot"] {
        if let Some(value) = std::env::var_os(key) {
            env.push((key.to_string(), value));
        }
    }
    env
}

fn ensure_private_state_dir(path: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => {
                current.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                anyhow::bail!("state directory {} contains '..'", path.display());
            }
            Component::Normal(name) => {
                current.push(name);
                ensure_private_state_component(&current, current == path)?;
            }
        }
    }
    Ok(())
}

fn ensure_private_state_component(path: &Path, harden_existing: bool) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                anyhow::bail!("state directory {} must not be a symlink", path.display());
            }
            if !metadata.is_dir() {
                anyhow::bail!("state directory {} is not a directory", path.display());
            }
            if harden_existing {
                harden_private_state_dir(path, &metadata)?;
            }
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path).with_context(|| format!("failed to create {}", path.display()))?;
            let metadata = fs::symlink_metadata(path)
                .with_context(|| format!("failed to stat {}", path.display()))?;
            harden_private_state_dir(path, &metadata)
        }
        Err(err) => Err(err).with_context(|| format!("failed to stat {}", path.display())),
    }
}

#[cfg(unix)]
fn harden_private_state_dir(path: &Path, metadata: &fs::Metadata) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    if metadata.permissions().mode() & 0o077 != 0 {
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("failed to chmod {}", path.display()))?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn harden_private_state_dir(_path: &Path, _metadata: &fs::Metadata) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs, time::Duration};

    use serde_json::Value;
    use uuid::Uuid;

    use super::{SkillMemoryProjector, SkillMemoryProjectorConfig};
    use crate::{
        contracts::{SessionHistoryPolicy, TrustTier},
        kernel::memory_projection::{MemoryProjectionRequest, MemoryProjector, MemorySourceRef},
    };

    #[cfg(unix)]
    fn make_executable(path: &std::path::Path) {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata(path).expect("metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("chmod");
    }

    #[cfg(unix)]
    fn write_projector_script(root: &std::path::Path, body: &str) -> SkillMemoryProjectorConfig {
        let skill_root = root.join("skill");
        let state_dir = root.join("state");
        fs::create_dir_all(skill_root.join("scripts")).expect("scripts");
        let command_path = skill_root.join("scripts/projector");
        fs::write(&command_path, body).expect("script");
        make_executable(&command_path);
        SkillMemoryProjectorConfig {
            projector_id: "memory-core".to_string(),
            command_path,
            skill_root,
            state_dir,
            request_timeout: Duration::from_secs(1),
        }
    }

    fn request() -> MemoryProjectionRequest {
        MemoryProjectionRequest {
            session_id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").expect("uuid"),
            runtime_id: "mock".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: SessionHistoryPolicy::Interactive,
            max_items: 16,
            max_bytes: 1024,
            sources: vec![MemorySourceRef::SessionTurnRange {
                before_sequence_no: Some(10),
                limit: 4,
                sequence_nos: vec![6, 7, 8, 9],
            }],
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn resident_projector_starts_once_for_multiple_requests() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let config = write_projector_script(
            temp_dir.path(),
            r#"#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$LIONCLAW_SKILL_STATE_DIR"
printf 'start\n' >> "$LIONCLAW_SKILL_STATE_DIR/starts"
printf '%s\n' "$LIONCLAW_MEMORY_PROJECTOR_ID" > "$LIONCLAW_SKILL_STATE_DIR/projector_id"
while IFS= read -r line; do
  printf '%s\n' "$line" >> "$LIONCLAW_SKILL_STATE_DIR/requests.jsonl"
  printf '{"projector_id":"%s","items":[{"kind":"stable_fact","text":"remembered","provenance":[{"source":"session_turn","sequence_no":7,"event_id":null}]}]}\n' "$LIONCLAW_MEMORY_PROJECTOR_ID"
done
"#,
        );
        let projector = SkillMemoryProjector::new(config.clone());

        let first = projector.project(request()).await.expect("first response");
        let second = projector.project(request()).await.expect("second response");

        assert_eq!(first.projector_id, "memory-core");
        assert_eq!(second.items.len(), 1);
        assert_eq!(
            fs::read_to_string(config.state_dir.join("starts")).expect("starts"),
            "start\n"
        );
        assert_eq!(
            fs::read_to_string(config.state_dir.join("projector_id")).expect("projector id"),
            "memory-core\n"
        );
        let requests =
            fs::read_to_string(config.state_dir.join("requests.jsonl")).expect("requests");
        let lines = requests.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);
        let first_request: Value = serde_json::from_str(lines[0]).expect("request json");
        assert_eq!(first_request["runtime_id"], "mock");
        assert_eq!(first_request["trust_tier"], "main");
        assert_eq!(first_request["history_policy"], "interactive");
        assert_eq!(first_request["sources"][0]["kind"], "session_turn_range");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn malformed_response_retires_process_and_later_restarts() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let config = write_projector_script(
            temp_dir.path(),
            r#"#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$LIONCLAW_SKILL_STATE_DIR"
starts="$LIONCLAW_SKILL_STATE_DIR/starts"
count=0
if [ -f "$starts" ]; then count=$(wc -l < "$starts"); fi
printf 'start\n' >> "$starts"
IFS= read -r _line || exit 0
if [ "$count" = "0" ]; then
  printf 'not-json\n'
else
  printf '{"projector_id":"%s","items":[]}\n' "$LIONCLAW_MEMORY_PROJECTOR_ID"
fi
"#,
        );
        let projector = SkillMemoryProjector::new(config.clone());

        let err = projector
            .project(request())
            .await
            .expect_err("malformed response should fail");
        assert!(err.to_string().contains("decode response"));

        let second = projector
            .project(request())
            .await
            .expect("second response should restart");
        assert!(second.items.is_empty());
        assert_eq!(
            fs::read_to_string(config.state_dir.join("starts")).expect("starts"),
            "start\nstart\n"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_retires_process_before_later_request() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let config = write_projector_script(
            temp_dir.path(),
            r#"#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$LIONCLAW_SKILL_STATE_DIR"
starts="$LIONCLAW_SKILL_STATE_DIR/starts"
count=0
if [ -f "$starts" ]; then count=$(wc -l < "$starts"); fi
printf 'start\n' >> "$starts"
IFS= read -r _line || exit 0
if [ "$count" = "0" ]; then
  sleep 1
fi
printf '{"projector_id":"%s","items":[]}\n' "$LIONCLAW_MEMORY_PROJECTOR_ID"
"#,
        )
        .with_request_timeout(Duration::from_millis(50));
        let projector = SkillMemoryProjector::new(config.clone());

        let err = projector
            .project(request())
            .await
            .expect_err("first request should time out");
        assert!(err.to_string().contains("timed out"));

        let second = projector
            .project(request())
            .await
            .expect("second response should restart");
        assert!(second.items.is_empty());
        assert_eq!(
            fs::read_to_string(config.state_dir.join("starts")).expect("starts"),
            "start\nstart\n"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn invalid_response_retires_process_and_still_returns_projection() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let config = write_projector_script(
            temp_dir.path(),
            r#"#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$LIONCLAW_SKILL_STATE_DIR"
starts="$LIONCLAW_SKILL_STATE_DIR/starts"
count=0
if [ -f "$starts" ]; then count=$(wc -l < "$starts"); fi
printf 'start\n' >> "$starts"
IFS= read -r _line || exit 0
if [ "$count" = "0" ]; then
  printf '{"projector_id":"wrong","items":[]}\n'
else
  printf '{"projector_id":"%s","items":[]}\n' "$LIONCLAW_MEMORY_PROJECTOR_ID"
fi
"#,
        );
        let projector = SkillMemoryProjector::new(config.clone());

        let first = projector
            .project(request())
            .await
            .expect("invalid semantic response still decodes");
        assert_eq!(first.projector_id, "wrong");

        let second = projector
            .project(request())
            .await
            .expect("second response should restart");
        assert_eq!(second.projector_id, "memory-core");
        assert_eq!(
            fs::read_to_string(config.state_dir.join("starts")).expect("starts"),
            "start\nstart\n"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn retirement_does_not_wait_for_inherited_stderr_handles() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let config = write_projector_script(
            temp_dir.path(),
            r#"#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$LIONCLAW_SKILL_STATE_DIR"
(sleep 5 >&2) &
IFS= read -r _line || exit 0
printf 'not-json\n'
"#,
        );
        let projector = SkillMemoryProjector::new(config);

        let err = tokio::time::timeout(Duration::from_millis(300), projector.project(request()))
            .await
            .expect("retirement should not wait for background stderr handles")
            .expect_err("malformed response should fail");

        assert!(err.to_string().contains("decode response"));
    }
}
