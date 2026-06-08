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
    sync::{mpsc, Mutex},
};

#[cfg(unix)]
use rustix::process::{kill_process_group, Pid, Signal};

use crate::applied::{AppliedPrivateContextProjector, AppliedState};

use super::private_context_projection::{
    validate_private_context_projection, NoopPrivateContextProjector, PrivateContextProjection,
    PrivateContextProjectionError, PrivateContextProjectionErrorKind,
    PrivateContextProjectionInvalidReason, PrivateContextProjectionRequest,
    PrivateContextProjector,
};

pub(crate) const PRIVATE_CONTEXT_PROJECTOR_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_PROJECTOR_STDERR_BYTES: usize = 4096;
const MAX_PROJECTOR_RESPONSE_LINE_BYTES: usize = 64 * 1024;
const PROJECTOR_STRAY_STDOUT_GRACE: Duration = Duration::from_millis(10);

pub(crate) fn private_context_projector_for_applied_state(
    applied_state: &AppliedState,
) -> Arc<dyn PrivateContextProjector> {
    match applied_state.private_context_projector() {
        Some(projector) => Arc::new(SkillPrivateContextProjector::from_applied(projector)),
        None => Arc::new(NoopPrivateContextProjector),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SkillPrivateContextProjectorConfig {
    projector_id: String,
    command_path: PathBuf,
    skill_root: PathBuf,
    state_dir: PathBuf,
    request_timeout: Duration,
}

impl SkillPrivateContextProjectorConfig {
    pub(crate) fn from_applied(projector: &AppliedPrivateContextProjector) -> Self {
        Self {
            projector_id: projector.skill_alias.clone(),
            command_path: projector.command_path.clone(),
            skill_root: projector.skill_root.clone(),
            state_dir: projector.state_dir.clone(),
            request_timeout: PRIVATE_CONTEXT_PROJECTOR_REQUEST_TIMEOUT,
        }
    }

    #[cfg(test)]
    fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = request_timeout;
        self
    }
}

pub(crate) struct SkillPrivateContextProjector {
    config: SkillPrivateContextProjectorConfig,
    process: Mutex<Option<ResidentPrivateContextProjectorProcess>>,
}

impl SkillPrivateContextProjector {
    pub(crate) fn from_applied(projector: &AppliedPrivateContextProjector) -> Self {
        Self::new(SkillPrivateContextProjectorConfig::from_applied(projector))
    }

    pub(crate) fn new(config: SkillPrivateContextProjectorConfig) -> Self {
        Self {
            config,
            process: Mutex::new(None),
        }
    }

    async fn project_with_process(
        &self,
        process: &mut Option<ResidentPrivateContextProjectorProcess>,
        request: PrivateContextProjectionRequest,
    ) -> Result<PrivateContextProjection, PrivateContextProjectionError> {
        let request_json = serde_json::to_string(&request).map_err(|err| {
            PrivateContextProjectionError::failed(format!("encode request: {err}"))
        })?;
        let mut retried_cached_process_failure = false;
        let response = loop {
            let origin = self.ensure_ready_process(process).await?;
            let read = {
                let Some(process) = process.as_mut() else {
                    return Err(PrivateContextProjectionError::failed(
                        "private context projector process was not available before request",
                    ));
                };
                process.exchange_request(&request_json).await
            };
            let read = match read {
                Ok(read) => read,
                Err(err)
                    if origin == ReadyProcessOrigin::Cached
                        && !retried_cached_process_failure
                        && err.kind() == PrivateContextProjectionErrorKind::ProjectorFailed =>
                {
                    Self::retire_process(process).await;
                    retried_cached_process_failure = true;
                    continue;
                }
                Err(err) => return Err(err),
            };
            if read.status == ResidentProcessStatus::Stale {
                Self::retire_process(process).await;
            }
            break read.response;
        };
        let projection =
            serde_json::from_str::<PrivateContextProjection>(&response).map_err(|err| {
                PrivateContextProjectionError::invalid_output(
                    "decode_response",
                    format!("decode response: {err}"),
                )
            })?;
        if let Err(reason) =
            validate_private_context_projection(&request, &self.config.projector_id, &projection)
        {
            Self::retire_process(process).await;
            if reason == PrivateContextProjectionInvalidReason::RequestIdMismatch {
                return Err(PrivateContextProjectionError::invalid_output(
                    reason.as_str(),
                    "private context projector response request id did not match the request",
                ));
            }
        }
        Ok(projection)
    }

    async fn ensure_ready_process(
        &self,
        process: &mut Option<ResidentPrivateContextProjectorProcess>,
    ) -> Result<ReadyProcessOrigin, PrivateContextProjectionError> {
        let had_cached_process = process.is_some();
        let mut restarted_stale_process = false;
        loop {
            if process.is_none() {
                *process = Some(self.spawn_process().await?);
            }
            let status = {
                let Some(process) = process.as_mut() else {
                    return Err(PrivateContextProjectionError::failed(
                        "private context projector process was not available after spawn",
                    ));
                };
                process.inspect_stdout()?
            };
            if status == ResidentProcessStatus::Ready {
                return Ok(if had_cached_process && !restarted_stale_process {
                    ReadyProcessOrigin::Cached
                } else {
                    ReadyProcessOrigin::Spawned
                });
            }
            if restarted_stale_process {
                return Err(PrivateContextProjectionError::failed(
                    "private context projector exited before request",
                ));
            }
            Self::retire_process(process).await;
            restarted_stale_process = true;
        }
    }

    async fn spawn_process(
        &self,
    ) -> Result<ResidentPrivateContextProjectorProcess, PrivateContextProjectionError> {
        ensure_private_state_dir(&self.config.state_dir).map_err(|err| {
            PrivateContextProjectionError::failed(format!("state directory: {err}"))
        })?;
        let mut command = Command::new(&self.config.command_path);
        command
            .current_dir(&self.config.skill_root)
            .kill_on_drop(true)
            .env_clear()
            .envs(projector_ambient_env())
            .env(
                "LIONCLAW_PRIVATE_CONTEXT_PROJECTOR_ID",
                &self.config.projector_id,
            )
            .env("LIONCLAW_SKILL_STATE_DIR", &self.config.state_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        configure_projector_process_group(&mut command);
        let mut child = command.spawn().map_err(|err| {
            PrivateContextProjectionError::failed(format!(
                "spawn private context projector {}: {err}",
                self.config.command_path.display()
            ))
        })?;
        let stdin = child.stdin.take().ok_or_else(|| {
            PrivateContextProjectionError::failed("private context projector stdin missing")
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            PrivateContextProjectionError::failed("private context projector stdout missing")
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            PrivateContextProjectionError::failed("private context projector stderr missing")
        })?;
        #[cfg(unix)]
        let process_group = child_process_group(&child);

        Ok(ResidentPrivateContextProjectorProcess {
            child,
            stdin,
            stdout: spawn_stdout_reader(stdout),
            stderr: Some(spawn_bounded_stderr_reader(stderr)),
            #[cfg(unix)]
            process_group,
        })
    }

    async fn retire_process(process: &mut Option<ResidentPrivateContextProjectorProcess>) {
        if let Some(process) = process.take() {
            process.retire().await;
        }
    }
}

#[async_trait::async_trait]
impl PrivateContextProjector for SkillPrivateContextProjector {
    fn projector_id(&self) -> &str {
        &self.config.projector_id
    }

    async fn project(
        &self,
        request: PrivateContextProjectionRequest,
    ) -> Result<PrivateContextProjection, PrivateContextProjectionError> {
        let mut process = self.process.lock().await;
        let result = tokio::time::timeout(
            self.config.request_timeout,
            self.project_with_process(&mut process, request),
        )
        .await;
        let projection_result = match result {
            Ok(Ok(projection)) => Ok(projection),
            Ok(Err(err)) => {
                Self::retire_process(&mut process).await;
                Err(err)
            }
            Err(_) => {
                Self::retire_process(&mut process).await;
                Err(PrivateContextProjectionError::timeout(
                    "private context projector request timed out",
                ))
            }
        };
        drop(process);
        projection_result
    }
}

struct ResidentPrivateContextProjectorProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: ProjectorStdoutReader,
    stderr: Option<tokio::task::JoinHandle<Vec<u8>>>,
    #[cfg(unix)]
    process_group: Option<Pid>,
}

impl ResidentPrivateContextProjectorProcess {
    async fn exchange_request(
        &mut self,
        request_json: &str,
    ) -> Result<ProjectorRead, PrivateContextProjectionError> {
        self.write_request(request_json).await?;
        self.read_response().await
    }

    async fn write_request(
        &mut self,
        request_json: &str,
    ) -> Result<(), PrivateContextProjectionError> {
        self.stdin
            .write_all(request_json.as_bytes())
            .await
            .map_err(|err| {
                PrivateContextProjectionError::failed(format!("write request: {err}"))
            })?;
        self.stdin.write_all(b"\n").await.map_err(|err| {
            PrivateContextProjectionError::failed(format!("write request newline: {err}"))
        })?;
        self.stdin
            .flush()
            .await
            .map_err(|err| PrivateContextProjectionError::failed(format!("flush request: {err}")))
    }

    async fn read_response(&mut self) -> Result<ProjectorRead, PrivateContextProjectionError> {
        let response = self.stdout.recv().await.ok_or_else(|| {
            PrivateContextProjectionError::failed("private context projector stdout reader stopped")
        })??;
        let status = self.inspect_stdout_after_response().await?;
        Ok(ProjectorRead { response, status })
    }

    async fn inspect_stdout_after_response(
        &mut self,
    ) -> Result<ResidentProcessStatus, PrivateContextProjectionError> {
        match self.inspect_stdout()? {
            ResidentProcessStatus::Ready => {}
            ResidentProcessStatus::Stale => return Ok(ResidentProcessStatus::Stale),
        }

        match tokio::time::timeout(PROJECTOR_STRAY_STDOUT_GRACE, self.stdout.recv()).await {
            Ok(Some(line)) => Self::status_from_stdout_item(line),
            Ok(None) => Ok(ResidentProcessStatus::Stale),
            Err(_) => Ok(ResidentProcessStatus::Ready),
        }
    }

    fn inspect_stdout(&mut self) -> Result<ResidentProcessStatus, PrivateContextProjectionError> {
        match self.stdout.try_recv() {
            Ok(line) => Self::status_from_stdout_item(line),
            Err(mpsc::error::TryRecvError::Empty) => Ok(ResidentProcessStatus::Ready),
            Err(mpsc::error::TryRecvError::Disconnected) => Ok(ResidentProcessStatus::Stale),
        }
    }

    fn status_from_stdout_item(
        line: Result<String, PrivateContextProjectionError>,
    ) -> Result<ResidentProcessStatus, PrivateContextProjectionError> {
        match line {
            Ok(_line) => Err(PrivateContextProjectionError::invalid_output(
                "unexpected_stdout",
                "private context projector wrote stdout outside the request/response contract",
            )),
            Err(err) if err.kind() == PrivateContextProjectionErrorKind::ProjectorFailed => {
                Ok(ResidentProcessStatus::Stale)
            }
            Err(err) => Err(err),
        }
    }

    async fn retire(mut self) {
        self.terminate();
        let _wait_result = self.child.wait().await;
        self.abort_output_readers();
        if let Some(stderr) = self.stderr.take() {
            let _stderr_result = stderr.await;
        }
    }

    fn terminate(&mut self) {
        self.terminate_process_group();
        let _kill_result = self.child.start_kill();
    }

    fn abort_output_readers(&mut self) {
        self.stdout.abort();
        if let Some(stderr) = &self.stderr {
            stderr.abort();
        }
    }

    #[cfg(unix)]
    fn terminate_process_group(&mut self) {
        if let Some(process_group) = self.process_group.take() {
            let _group_kill_result = kill_process_group(process_group, Signal::KILL);
        }
    }

    #[cfg(not(unix))]
    fn terminate_process_group(&mut self) {}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResidentProcessStatus {
    Ready,
    Stale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadyProcessOrigin {
    Cached,
    Spawned,
}

struct ProjectorRead {
    response: String,
    status: ResidentProcessStatus,
}

impl Drop for ResidentPrivateContextProjectorProcess {
    fn drop(&mut self) {
        self.terminate();
        self.abort_output_readers();
    }
}

fn configure_projector_process_group(command: &mut Command) {
    configure_projector_process_group_for_platform(command);
}

#[cfg(unix)]
fn configure_projector_process_group_for_platform(command: &mut Command) {
    // This is the v1 cleanup boundary; projector helpers must not detach from it.
    command.process_group(0);
}

#[cfg(not(unix))]
fn configure_projector_process_group_for_platform(_command: &mut Command) {}

#[cfg(unix)]
fn child_process_group(child: &Child) -> Option<Pid> {
    child
        .id()
        .and_then(|pid| i32::try_from(pid).ok())
        .and_then(Pid::from_raw)
}

struct ProjectorStdoutReader {
    lines: mpsc::Receiver<Result<String, PrivateContextProjectionError>>,
    task: tokio::task::JoinHandle<()>,
}

impl ProjectorStdoutReader {
    async fn recv(&mut self) -> Option<Result<String, PrivateContextProjectionError>> {
        self.lines.recv().await
    }

    fn try_recv(
        &mut self,
    ) -> Result<Result<String, PrivateContextProjectionError>, mpsc::error::TryRecvError> {
        self.lines.try_recv()
    }

    fn abort(&self) {
        self.task.abort();
    }
}

fn spawn_stdout_reader(stdout: ChildStdout) -> ProjectorStdoutReader {
    let (sender, lines) = mpsc::channel(1);
    let task = tokio::spawn(async move {
        let mut reader = BufReader::new(stdout);
        loop {
            let result = read_projector_response_line(&mut reader).await;
            let terminal = result.is_err();
            if sender.send(result).await.is_err() {
                break;
            }
            if terminal {
                break;
            }
        }
    });
    ProjectorStdoutReader { lines, task }
}

async fn read_projector_response_line(
    reader: &mut BufReader<ChildStdout>,
) -> Result<String, PrivateContextProjectionError> {
    let line = read_capped_line(reader, MAX_PROJECTOR_RESPONSE_LINE_BYTES).await?;
    let Some(line) = line else {
        return Err(PrivateContextProjectionError::failed(
            "private context projector stdout closed",
        ));
    };
    String::from_utf8(line)
        .map(|line| line.trim_end_matches(['\r', '\n']).to_string())
        .map_err(|err| {
            PrivateContextProjectionError::invalid_output(
                "response_not_utf8",
                format!("response was not UTF-8: {err}"),
            )
        })
}

async fn read_capped_line(
    reader: &mut BufReader<ChildStdout>,
    max_bytes: usize,
) -> Result<Option<Vec<u8>>, PrivateContextProjectionError> {
    let mut line = Vec::new();
    loop {
        let available = reader.fill_buf().await.map_err(|err| {
            PrivateContextProjectionError::failed(format!("read response: {err}"))
        })?;
        if available.is_empty() {
            if line.is_empty() {
                return Ok(None);
            }
            return Err(PrivateContextProjectionError::invalid_output(
                "response_without_newline",
                "private context projector response ended without newline",
            ));
        }
        if let Some(newline_index) = available.iter().position(|byte| *byte == b'\n') {
            let take = newline_index + 1;
            if line.len().saturating_add(take) > max_bytes {
                return Err(PrivateContextProjectionError::invalid_output(
                    "response_too_large",
                    "private context projector response exceeded byte limit",
                ));
            }
            line.extend(available.iter().take(take).copied());
            reader.consume(take);
            return Ok(Some(line));
        }
        if line.len().saturating_add(available.len()) > max_bytes {
            return Err(PrivateContextProjectionError::invalid_output(
                "response_too_large",
                "private context projector response exceeded byte limit",
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
                captured.extend(buffer.iter().take(take).copied());
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

    use super::{SkillPrivateContextProjector, SkillPrivateContextProjectorConfig};
    use crate::{
        contracts::{SessionHistoryPolicy, TrustTier},
        kernel::private_context_projection::{
            PrivateContextProjectionErrorKind, PrivateContextProjectionRequest,
            PrivateContextProjector, PrivateContextSourceRef, ProjectedContextBudget,
            ProjectedContextClass,
        },
        kernel::prompt_context::PromptContextMode,
    };

    #[cfg(unix)]
    fn make_executable(path: &std::path::Path) {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata(path).expect("metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("chmod");
    }

    #[cfg(unix)]
    fn write_projector_script(
        root: &std::path::Path,
        body: &str,
    ) -> SkillPrivateContextProjectorConfig {
        let skill_root = root.join("skill");
        let state_dir = root.join("state");
        fs::create_dir_all(skill_root.join("scripts")).expect("scripts");
        let command_path = skill_root.join("scripts/projector");
        fs::write(&command_path, body).expect("script");
        make_executable(&command_path);
        SkillPrivateContextProjectorConfig {
            projector_id: "private-context-core".to_string(),
            command_path,
            skill_root,
            state_dir,
            request_timeout: Duration::from_secs(1),
        }
    }

    fn restart_script_with_first_response(first_response: &str) -> String {
        r#"#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$LIONCLAW_SKILL_STATE_DIR"
starts="$LIONCLAW_SKILL_STATE_DIR/starts"
count=0
if [ -f "$starts" ]; then count=$(wc -l < "$starts"); fi
printf 'start\n' >> "$starts"
IFS= read -r line || exit 0
request_id=${line#*\"request_id\":\"}
request_id=${request_id%%\"*}
if [ "$count" = "0" ]; then
  first_response='__FIRST_RESPONSE__'
  first_response=${first_response//__REQUEST_ID__/$request_id}
  printf '%s\n' "$first_response"
else
  printf '{"request_id":"%s","projector_id":"%s","items":[]}\n' "$request_id" "$LIONCLAW_PRIVATE_CONTEXT_PROJECTOR_ID"
fi
"#
        .replace("__FIRST_RESPONSE__", first_response)
    }

    fn request() -> PrivateContextProjectionRequest {
        PrivateContextProjectionRequest {
            request_id: Uuid::new_v4(),
            session_id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").expect("uuid"),
            runtime_id: "mock".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: SessionHistoryPolicy::Interactive,
            surface: PromptContextMode::ProgramPrimary,
            project_scope: Some("project:test".to_string()),
            current_input: None,
            budgets: vec![ProjectedContextBudget {
                class: ProjectedContextClass::Memory,
                max_items: 16,
                max_bytes: 1024,
            }],
            sources: vec![PrivateContextSourceRef::SessionTurnRange {
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
printf '%s\n' "$LIONCLAW_PRIVATE_CONTEXT_PROJECTOR_ID" > "$LIONCLAW_SKILL_STATE_DIR/projector_id"
while IFS= read -r line; do
  printf '%s\n' "$line" >> "$LIONCLAW_SKILL_STATE_DIR/requests.jsonl"
  request_id=${line#*\"request_id\":\"}
  request_id=${request_id%%\"*}
  printf '{"request_id":"%s","projector_id":"%s","items":[{"class":"memory","text":"remembered","provenance":[{"source":"session_turn","sequence_no":7,"event_id":null}]}]}\n' "$request_id" "$LIONCLAW_PRIVATE_CONTEXT_PROJECTOR_ID"
done
"#,
        );
        let projector = SkillPrivateContextProjector::new(config.clone());

        let first = projector.project(request()).await.expect("first response");
        let second = projector.project(request()).await.expect("second response");

        assert_eq!(first.projector_id, "private-context-core");
        assert_eq!(second.items.len(), 1);
        assert_eq!(
            fs::read_to_string(config.state_dir.join("starts")).expect("starts"),
            "start\n"
        );
        assert_eq!(
            fs::read_to_string(config.state_dir.join("projector_id")).expect("projector id"),
            "private-context-core\n"
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
    async fn extra_stdout_line_retires_process_before_later_request() {
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
IFS= read -r line || exit 0
request_id=${line#*\"request_id\":\"}
request_id=${request_id%%\"*}
printf '{"request_id":"%s","projector_id":"%s","items":[]}\n' "$request_id" "$LIONCLAW_PRIVATE_CONTEXT_PROJECTOR_ID"
if [ "$count" = "0" ]; then
  printf '{"request_id":"%s","projector_id":"%s","items":[]}\n' "$request_id" "$LIONCLAW_PRIVATE_CONTEXT_PROJECTOR_ID"
fi
"#,
        );
        let projector = SkillPrivateContextProjector::new(config.clone());

        let err = projector
            .project(request())
            .await
            .expect_err("extra stdout response should fail the request");

        assert!(err.to_string().contains("outside the request/response"));
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
    async fn delayed_extra_stdout_line_cannot_satisfy_later_request() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let config = write_projector_script(
            temp_dir.path(),
            r#"#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$LIONCLAW_SKILL_STATE_DIR"
starts="$LIONCLAW_SKILL_STATE_DIR/starts"
requests="$LIONCLAW_SKILL_STATE_DIR/requests"
printf 'start\n' >> "$starts"
count=0
while IFS= read -r _line; do
  count=$((count + 1))
  printf 'request\n' >> "$requests"
  request_id=${_line#*\"request_id\":\"}
  request_id=${request_id%%\"*}
  if [ "$count" = "1" ]; then
    stale_request_id="$request_id"
    printf '{"request_id":"%s","projector_id":"%s","items":[]}\n' "$request_id" "$LIONCLAW_PRIVATE_CONTEXT_PROJECTOR_ID"
    (sleep 0.02; printf '{"request_id":"%s","projector_id":"%s","items":[{"class":"memory","text":"stale delayed output","provenance":[{"source":"session_turn","sequence_no":7,"event_id":null}]}]}\n' "$stale_request_id" "$LIONCLAW_PRIVATE_CONTEXT_PROJECTOR_ID") &
  else
    sleep 0.1
    printf '{"request_id":"%s","projector_id":"%s","items":[]}\n' "$request_id" "$LIONCLAW_PRIVATE_CONTEXT_PROJECTOR_ID"
  fi
done
"#,
        );
        let projector = SkillPrivateContextProjector::new(config.clone());

        projector
            .project(request())
            .await
            .expect("first response should succeed");
        let second = projector
            .project(request())
            .await
            .expect_err("delayed extra stdout must fail the next request");

        assert!(second.to_string().contains("request id"));
        assert_eq!(
            fs::read_to_string(config.state_dir.join("starts")).expect("starts"),
            "start\n"
        );
        assert_eq!(
            fs::read_to_string(config.state_dir.join("requests")).expect("requests"),
            "request\nrequest\n"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn malformed_response_retires_process_and_later_restarts() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let config = write_projector_script(
            temp_dir.path(),
            &restart_script_with_first_response("not-json"),
        );
        let projector = SkillPrivateContextProjector::new(config.clone());

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
    async fn fatal_decode_errors_retire_process_and_later_restart() {
        for first_response in [
            r#"{"request_id":"__REQUEST_ID__","projector_id":"private-context-core"}"#,
            r#"{"request_id":"__REQUEST_ID__","projector_id":"private-context-core","items":[{"class":"unknown","text":"remembered","provenance":[{"source":"session_turn","sequence_no":7,"event_id":null}]}]}"#,
            r#"{"request_id":"__REQUEST_ID__","projector_id":"private-context-core","items":[{"class":"memory","text":"remembered"}]}"#,
        ] {
            let temp_dir = tempfile::tempdir().expect("temp dir");
            let config = write_projector_script(
                temp_dir.path(),
                &restart_script_with_first_response(first_response),
            );
            let projector = SkillPrivateContextProjector::new(config.clone());

            let err = projector
                .project(request())
                .await
                .expect_err("fatal decode error should fail");
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
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn process_eof_retires_process_and_later_restarts() {
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
  exit 0
fi
request_id=${_line#*\"request_id\":\"}
request_id=${request_id%%\"*}
printf '{"request_id":"%s","projector_id":"%s","items":[]}\n' "$request_id" "$LIONCLAW_PRIVATE_CONTEXT_PROJECTOR_ID"
"#,
        );
        let projector = SkillPrivateContextProjector::new(config.clone());

        let err = projector
            .project(request())
            .await
            .expect_err("EOF should fail");
        assert!(err.to_string().contains("stdout closed"));

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
    async fn valid_response_then_eof_restarts_before_later_request() {
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
IFS= read -r line || exit 0
request_id=${line#*\"request_id\":\"}
request_id=${request_id%%\"*}
printf '{"request_id":"%s","projector_id":"%s","items":[]}\n' "$request_id" "$LIONCLAW_PRIVATE_CONTEXT_PROJECTOR_ID"
if [ "$count" = "0" ]; then
  sleep 0.05
  exit 0
fi
"#,
        );
        let projector = SkillPrivateContextProjector::new(config.clone());

        projector
            .project(request())
            .await
            .expect("first response should succeed");

        let second = projector
            .project(request())
            .await
            .expect("second response should restart before writing");
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
IFS= read -r line || exit 0
request_id=${line#*\"request_id\":\"}
request_id=${request_id%%\"*}
if [ "$count" = "0" ]; then
  sleep 1
fi
printf '{"request_id":"%s","projector_id":"%s","items":[]}\n' "$request_id" "$LIONCLAW_PRIVATE_CONTEXT_PROJECTOR_ID"
"#,
        )
        .with_request_timeout(Duration::from_millis(50));
        let projector = SkillPrivateContextProjector::new(config.clone());

        let err = projector
            .project(request())
            .await
            .expect_err("first request should time out");
        assert_eq!(err.kind(), PrivateContextProjectionErrorKind::Timeout);
        assert_eq!(err.audit_status(), "projector_timeout");
        assert_eq!(err.audit_reason(), "projector_timeout");
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
    async fn timeout_retires_projector_process_group() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let config = write_projector_script(
            temp_dir.path(),
            r#"#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$LIONCLAW_SKILL_STATE_DIR"
(sleep 0.3; printf 'alive\n' > "$LIONCLAW_SKILL_STATE_DIR/child-alive") &
IFS= read -r _line || exit 0
sleep 5
"#,
        )
        .with_request_timeout(Duration::from_millis(50));
        let marker = config.state_dir.join("child-alive");
        let projector = SkillPrivateContextProjector::new(config);

        let err = projector
            .project(request())
            .await
            .expect_err("first request should time out");
        assert_eq!(err.kind(), PrivateContextProjectionErrorKind::Timeout);
        assert_eq!(err.audit_status(), "projector_timeout");
        assert_eq!(err.audit_reason(), "projector_timeout");
        assert!(err.to_string().contains("timed out"));

        tokio::time::sleep(Duration::from_millis(600)).await;
        assert!(
            !marker.exists(),
            "projector background child survived timeout retirement"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn drop_retires_projector_process_group() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let config = write_projector_script(
            temp_dir.path(),
            r#"#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$LIONCLAW_SKILL_STATE_DIR"
(sleep 0.3; printf 'alive\n' > "$LIONCLAW_SKILL_STATE_DIR/child-alive") &
while IFS= read -r line; do
  request_id=${line#*\"request_id\":\"}
  request_id=${request_id%%\"*}
  printf '{"request_id":"%s","projector_id":"%s","items":[]}\n' "$request_id" "$LIONCLAW_PRIVATE_CONTEXT_PROJECTOR_ID"
done
"#,
        );
        let marker = config.state_dir.join("child-alive");
        let projector = SkillPrivateContextProjector::new(config);

        projector
            .project(request())
            .await
            .expect("first response should start resident projector");
        drop(projector);

        tokio::time::sleep(Duration::from_millis(600)).await;
        assert!(
            !marker.exists(),
            "projector background child survived projector drop"
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
IFS= read -r line || exit 0
request_id=${line#*\"request_id\":\"}
request_id=${request_id%%\"*}
if [ "$count" = "0" ]; then
  printf '{"request_id":"%s","projector_id":"wrong","items":[]}\n' "$request_id"
else
  printf '{"request_id":"%s","projector_id":"%s","items":[]}\n' "$request_id" "$LIONCLAW_PRIVATE_CONTEXT_PROJECTOR_ID"
fi
"#,
        );
        let projector = SkillPrivateContextProjector::new(config.clone());

        let first = projector
            .project(request())
            .await
            .expect("invalid semantic response still decodes");
        assert_eq!(first.projector_id, "wrong");

        let second = projector
            .project(request())
            .await
            .expect("second response should restart");
        assert_eq!(second.projector_id, "private-context-core");
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
        let projector = SkillPrivateContextProjector::new(config);

        let err = tokio::time::timeout(Duration::from_millis(300), projector.project(request()))
            .await
            .expect("retirement should not wait for background stderr handles")
            .expect_err("malformed response should fail");

        assert!(err.to_string().contains("decode response"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn concurrent_projection_calls_are_serialized() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let config = write_projector_script(
            temp_dir.path(),
            r#"#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$LIONCLAW_SKILL_STATE_DIR"
while IFS= read -r line; do
  printf '%s\n' "$line" >> "$LIONCLAW_SKILL_STATE_DIR/requests.jsonl"
  request_id=${line#*\"request_id\":\"}
  request_id=${request_id%%\"*}
  if [ ! -f "$LIONCLAW_SKILL_STATE_DIR/first-response-sent" ]; then
    if IFS= read -r -t 0.2 queued; then
      printf '%s\n' "$queued" >> "$LIONCLAW_SKILL_STATE_DIR/requests.jsonl"
      queued_request_id=${queued#*\"request_id\":\"}
      queued_request_id=${queued_request_id%%\"*}
      printf 'overlap\n' > "$LIONCLAW_SKILL_STATE_DIR/overlap"
      printf '{"request_id":"%s","projector_id":"%s","items":[]}\n' "$request_id" "$LIONCLAW_PRIVATE_CONTEXT_PROJECTOR_ID"
      printf '{"request_id":"%s","projector_id":"%s","items":[]}\n' "$queued_request_id" "$LIONCLAW_PRIVATE_CONTEXT_PROJECTOR_ID"
      touch "$LIONCLAW_SKILL_STATE_DIR/first-response-sent"
      continue
    fi
    touch "$LIONCLAW_SKILL_STATE_DIR/first-response-sent"
  fi
  printf '{"request_id":"%s","projector_id":"%s","items":[]}\n' "$request_id" "$LIONCLAW_PRIVATE_CONTEXT_PROJECTOR_ID"
done
"#,
        );
        let projector = SkillPrivateContextProjector::new(config.clone());

        let results = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(projector.project(request()), projector.project(request()))
        })
        .await
        .expect("concurrent projection calls should finish");

        results.0.expect("first response");
        results.1.expect("second response");
        assert!(
            !config.state_dir.join("overlap").exists(),
            "second request reached projector before first response completed"
        );
        let requests =
            fs::read_to_string(config.state_dir.join("requests.jsonl")).expect("requests");
        assert_eq!(requests.lines().count(), 2);
    }
}
