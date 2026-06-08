use std::{
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use rustix::fs::{flock, FlockOperation};
use tokio::{
    io::AsyncWriteExt,
    process::{Child, Command},
    sync::Mutex,
    time::Instant,
};

use crate::applied::{AppliedPrivateContextSkill, AppliedState};

use super::{
    private_context_command::{
        child_process_group, configure_private_context_command, ensure_private_context_state_dir,
        spawn_bounded_output_reader, terminate_private_context_process_group,
        PrivateContextCommandConfig, PrivateContextProcessGroup, PRIVATE_CONTEXT_STDERR_MAX_BYTES,
    },
    private_context_recording::{PrivateContextRecordOutcome, PrivateContextRecordRequest},
};

pub(crate) const PRIVATE_CONTEXT_RECORDER_TIMEOUT: Duration = Duration::from_secs(2);
const PRIVATE_CONTEXT_RECORDER_STDOUT_MAX_BYTES: usize = 4096;
const PRIVATE_CONTEXT_RECORDER_LOCK_FILE: &str = ".lionclaw-private-context-recorder.lock";

#[derive(Clone)]
pub(crate) struct PrivateContextRecorderService {
    private_context_id: Option<String>,
    recorder: Option<Arc<dyn PrivateContextRecorder>>,
    record_lock: Arc<Mutex<()>>,
}

impl PrivateContextRecorderService {
    pub(crate) fn private_context_id(&self) -> Option<&str> {
        self.private_context_id.as_deref()
    }

    pub(crate) fn has_recorder(&self) -> bool {
        self.recorder.is_some()
    }

    pub(crate) async fn record(
        &self,
        request: PrivateContextRecordRequest,
    ) -> Option<PrivateContextRecordOutcome> {
        let recorder = self.recorder.as_ref()?;
        let _guard = self.record_lock.lock().await;
        Some(recorder.record(request).await)
    }

    #[cfg(test)]
    pub(crate) fn with_recorder(
        private_context_id: impl Into<String>,
        recorder: Arc<dyn PrivateContextRecorder>,
    ) -> Self {
        Self {
            private_context_id: Some(private_context_id.into()),
            recorder: Some(recorder),
            record_lock: Arc::new(Mutex::new(())),
        }
    }

    #[cfg(test)]
    pub(crate) fn without_recorder(private_context_id: impl Into<String>) -> Self {
        Self {
            private_context_id: Some(private_context_id.into()),
            recorder: None,
            record_lock: Arc::new(Mutex::new(())),
        }
    }
}

pub(crate) fn private_context_recorder_for_applied_state(
    applied_state: &AppliedState,
) -> PrivateContextRecorderService {
    let Some(private_context) = applied_state.private_context_skill() else {
        return PrivateContextRecorderService {
            private_context_id: None,
            recorder: None,
            record_lock: Arc::new(Mutex::new(())),
        };
    };
    let private_context_id = Some(private_context.skill_alias.clone());
    let recorder = private_context
        .recorder
        .as_ref()
        .map(|_| Arc::new(SkillPrivateContextRecorder::from_applied(private_context)) as Arc<_>);
    PrivateContextRecorderService {
        private_context_id,
        recorder,
        record_lock: Arc::new(Mutex::new(())),
    }
}

#[async_trait::async_trait]
pub(crate) trait PrivateContextRecorder: Send + Sync {
    async fn record(&self, request: PrivateContextRecordRequest) -> PrivateContextRecordOutcome;
}

#[derive(Debug, Clone)]
pub(crate) struct SkillPrivateContextRecorderConfig {
    recorder_id: String,
    command_path: PathBuf,
    skill_root: PathBuf,
    state_dir: PathBuf,
    timeout: Duration,
}

impl SkillPrivateContextRecorderConfig {
    pub(crate) fn from_applied(private_context: &AppliedPrivateContextSkill) -> Option<Self> {
        let recorder = private_context.recorder.as_ref()?;
        Some(Self {
            recorder_id: private_context.skill_alias.clone(),
            command_path: recorder.command_path.clone(),
            skill_root: private_context.skill_root.clone(),
            state_dir: private_context.state_dir.clone(),
            timeout: PRIVATE_CONTEXT_RECORDER_TIMEOUT,
        })
    }

    #[cfg(test)]
    fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

pub(crate) struct SkillPrivateContextRecorder {
    config: SkillPrivateContextRecorderConfig,
}

impl SkillPrivateContextRecorder {
    pub(crate) fn from_applied(private_context: &AppliedPrivateContextSkill) -> Self {
        let config = SkillPrivateContextRecorderConfig::from_applied(private_context)
            .expect("private context recorder config requires recorder entrypoint");
        Self::new(config)
    }

    pub(crate) fn new(config: SkillPrivateContextRecorderConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl PrivateContextRecorder for SkillPrivateContextRecorder {
    async fn record(&self, request: PrivateContextRecordRequest) -> PrivateContextRecordOutcome {
        let started_at = Instant::now();
        let request_json = match serde_json::to_vec(&request) {
            Ok(value) => value,
            Err(_) => return failed_outcome("encode_request", started_at),
        };
        let _state_lock =
            match acquire_private_context_recorder_state_lock(&self.config.state_dir).await {
                Ok(lock) => lock,
                Err(_) => return failed_outcome("state_lock", started_at),
            };
        let mut process = match OneShotPrivateContextRecorderProcess::spawn(&self.config).await {
            Ok(process) => process,
            Err(_) => return failed_outcome("spawn_failed", started_at),
        };
        if process.write_request(&request_json).await.is_err() {
            process.terminate();
            process.finish_after_termination().await;
            return failed_outcome("io_error", started_at);
        }

        let wait_result = tokio::time::timeout(self.config.timeout, process.wait()).await;
        match wait_result {
            Ok(outcome) => outcome_from_wait(outcome, started_at),
            Err(_) => {
                process.terminate();
                process.finish_after_termination().await;
                PrivateContextRecordOutcome::timed_out(elapsed_ms(started_at))
            }
        }
    }
}

struct OneShotPrivateContextRecorderProcess {
    child: Child,
    stdin: Option<tokio::process::ChildStdin>,
    stdout: Option<tokio::task::JoinHandle<Vec<u8>>>,
    stderr: Option<tokio::task::JoinHandle<Vec<u8>>>,
    process_group: Option<PrivateContextProcessGroup>,
}

impl OneShotPrivateContextRecorderProcess {
    async fn spawn(config: &SkillPrivateContextRecorderConfig) -> Result<Self, ()> {
        ensure_private_context_state_dir(&config.state_dir).map_err(|_| ())?;
        let mut command = Command::new(&config.command_path);
        configure_private_context_command(
            &mut command,
            &PrivateContextCommandConfig {
                skill_root: &config.skill_root,
                state_dir: &config.state_dir,
                private_context_id: &config.recorder_id,
                private_context_id_env: "LIONCLAW_PRIVATE_CONTEXT_ID",
            },
        );
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().map_err(|_| ())?;
        let stdin = child.stdin.take().ok_or(())?;
        let stdout = child.stdout.take().ok_or(())?;
        let stderr = child.stderr.take().ok_or(())?;
        let process_group = child_process_group(&child);
        Ok(Self {
            child,
            stdin: Some(stdin),
            stdout: Some(spawn_bounded_output_reader(
                stdout,
                PRIVATE_CONTEXT_RECORDER_STDOUT_MAX_BYTES,
            )),
            stderr: Some(spawn_bounded_output_reader(
                stderr,
                PRIVATE_CONTEXT_STDERR_MAX_BYTES,
            )),
            process_group,
        })
    }

    async fn write_request(&mut self, request_json: &[u8]) -> Result<(), ()> {
        let mut stdin = self.stdin.take().ok_or(())?;
        stdin.write_all(request_json).await.map_err(|_| ())?;
        stdin.shutdown().await.map_err(|_| ())?;
        Ok(())
    }

    async fn wait(&mut self) -> RecorderProcessWait {
        let exit_status = self.child.wait().await.map_err(|_| ())?;
        let stdout = take_output(self.stdout.take()).await;
        let _stderr = take_output(self.stderr.take()).await;
        Ok(RecorderProcessOutput {
            exit_status: exit_status.code(),
            stdout,
        })
    }

    fn terminate(&mut self) {
        terminate_private_context_process_group(&mut self.process_group);
        let _kill_result = self.child.start_kill();
    }

    async fn finish_after_termination(mut self) {
        let _wait_result = self.child.wait().await;
        self.abort_output_readers();
    }

    fn abort_output_readers(&mut self) {
        if let Some(stdout) = &self.stdout {
            stdout.abort();
        }
        if let Some(stderr) = &self.stderr {
            stderr.abort();
        }
    }
}

impl Drop for OneShotPrivateContextRecorderProcess {
    fn drop(&mut self) {
        self.terminate();
        self.abort_output_readers();
    }
}

type RecorderProcessWait = Result<RecorderProcessOutput, ()>;

struct RecorderProcessOutput {
    exit_status: Option<i32>,
    stdout: Vec<u8>,
}

struct PrivateContextRecorderStateLock {
    _file: fs::File,
}

async fn acquire_private_context_recorder_state_lock(
    state_dir: &Path,
) -> Result<PrivateContextRecorderStateLock, ()> {
    let state_dir = state_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        acquire_private_context_recorder_state_lock_blocking(&state_dir)
    })
    .await
    .map_err(|_| ())?
}

fn acquire_private_context_recorder_state_lock_blocking(
    state_dir: &Path,
) -> Result<PrivateContextRecorderStateLock, ()> {
    use rustix::fs::{openat, Mode, OFlags};

    ensure_private_context_state_dir(state_dir).map_err(|_| ())?;
    let state_root = fs::File::open(state_dir).map_err(|_| ())?;
    if !state_root.metadata().map_err(|_| ())?.is_dir() {
        return Err(());
    }
    let lock = openat(
        &state_root,
        PRIVATE_CONTEXT_RECORDER_LOCK_FILE,
        OFlags::CREATE | OFlags::RDWR | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::from_raw_mode(0o600),
    )
    .map_err(|_| ())?;
    let lock = fs::File::from(lock);
    if !lock.metadata().map_err(|_| ())?.is_file() {
        return Err(());
    }
    flock(&lock, FlockOperation::LockExclusive).map_err(|_| ())?;
    Ok(PrivateContextRecorderStateLock { _file: lock })
}

async fn take_output(reader: Option<tokio::task::JoinHandle<Vec<u8>>>) -> Vec<u8> {
    match reader {
        Some(reader) => reader.await.unwrap_or_default(),
        None => Vec::new(),
    }
}

fn outcome_from_wait(
    wait: RecorderProcessWait,
    started_at: Instant,
) -> PrivateContextRecordOutcome {
    let elapsed_ms = elapsed_ms(started_at);
    let Ok(output) = wait else {
        return PrivateContextRecordOutcome::failed("io_error", None, elapsed_ms);
    };
    if output.exit_status != Some(0) {
        return PrivateContextRecordOutcome::failed("nonzero_exit", output.exit_status, elapsed_ms);
    }
    if !output.stdout.is_empty() {
        return PrivateContextRecordOutcome::invalid_output(
            "unexpected_stdout",
            output.exit_status,
            elapsed_ms,
        );
    }
    PrivateContextRecordOutcome::completed(elapsed_ms)
}

fn failed_outcome(reason: &'static str, started_at: Instant) -> PrivateContextRecordOutcome {
    PrivateContextRecordOutcome::failed(reason, None, elapsed_ms(started_at))
}

fn elapsed_ms(started_at: Instant) -> u64 {
    started_at
        .elapsed()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use serde_json::Value;
    use uuid::Uuid;

    use super::{
        PrivateContextRecorder, PrivateContextRecorderService, SkillPrivateContextRecorder,
        SkillPrivateContextRecorderConfig,
    };
    use crate::{
        contracts::{SessionHistoryPolicy, TrustTier},
        kernel::{
            private_context_recorder_service::PRIVATE_CONTEXT_RECORDER_TIMEOUT,
            private_context_recording::{
                PrivateContextRecordOutcome, PrivateContextRecordRequest,
                PrivateContextRecordStatus, PrivateContextRecordSurface,
                PrivateContextRecordTranscript,
            },
        },
    };

    #[cfg(unix)]
    fn make_executable(path: &std::path::Path) {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata(path).expect("metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("chmod");
    }

    #[cfg(unix)]
    fn write_recorder_script(
        root: &std::path::Path,
        body: &str,
    ) -> SkillPrivateContextRecorderConfig {
        let skill_root = root.join("skill");
        let state_dir = root.join("state");
        fs::create_dir_all(skill_root.join("scripts")).expect("scripts");
        let command_path = skill_root.join("scripts/recorder");
        fs::write(&command_path, body).expect("script");
        make_executable(&command_path);
        SkillPrivateContextRecorderConfig {
            recorder_id: "private-context-core".to_string(),
            command_path,
            skill_root,
            state_dir,
            timeout: PRIVATE_CONTEXT_RECORDER_TIMEOUT,
        }
    }

    fn request() -> PrivateContextRecordRequest {
        PrivateContextRecordRequest {
            session_id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").expect("uuid"),
            turn_id: Uuid::parse_str("22222222-2222-2222-2222-222222222222").expect("uuid"),
            sequence_no: 7,
            runtime_id: "mock".to_string(),
            trust_tier: TrustTier::Main,
            history_policy: SessionHistoryPolicy::Interactive,
            surface: PrivateContextRecordSurface::ProgramTurn,
            project_scope: Some("project:test".to_string()),
            transcript: PrivateContextRecordTranscript::from_committed_text("hello", "answer"),
        }
    }

    struct ConcurrentRecorder {
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl PrivateContextRecorder for ConcurrentRecorder {
        async fn record(
            &self,
            _request: PrivateContextRecordRequest,
        ) -> PrivateContextRecordOutcome {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(25)).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            PrivateContextRecordOutcome::completed(0)
        }
    }

    #[tokio::test]
    async fn recorder_service_serializes_concurrent_requests() {
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let service = PrivateContextRecorderService::with_recorder(
            "private-context-core",
            Arc::new(ConcurrentRecorder {
                active: Arc::clone(&active),
                max_active: Arc::clone(&max_active),
            }),
        );

        let mut tasks = Vec::new();
        for _ in 0..4 {
            let service = service.clone();
            tasks.push(tokio::spawn(async move {
                service.record(request()).await.expect("record outcome")
            }));
        }
        for task in tasks {
            assert_eq!(
                task.await.expect("join").status,
                PrivateContextRecordStatus::Completed
            );
        }

        assert_eq!(max_active.load(Ordering::SeqCst), 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn recorder_state_lock_serializes_independent_recorder_instances() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let config = write_recorder_script(
            temp_dir.path(),
            r#"#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$LIONCLAW_SKILL_STATE_DIR"
cat >/dev/null
if mkdir "$LIONCLAW_SKILL_STATE_DIR/active"; then
  trap 'rmdir "$LIONCLAW_SKILL_STATE_DIR/active"' EXIT
else
  printf overlap > "$LIONCLAW_SKILL_STATE_DIR/overlap"
  exit 11
fi
sleep 0.1
"#,
        );
        let first = SkillPrivateContextRecorder::new(config.clone());
        let second = SkillPrivateContextRecorder::new(config.clone());

        let (first_outcome, second_outcome) =
            tokio::join!(first.record(request()), second.record(request()));

        assert_eq!(first_outcome.status, PrivateContextRecordStatus::Completed);
        assert_eq!(second_outcome.status, PrivateContextRecordStatus::Completed);
        assert!(!config.state_dir.join("overlap").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn recorder_receives_request_and_expected_environment() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let config = write_recorder_script(
            temp_dir.path(),
            r#"#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$LIONCLAW_SKILL_STATE_DIR"
cat > "$LIONCLAW_SKILL_STATE_DIR/request.json"
printf '%s\n' "$LIONCLAW_PRIVATE_CONTEXT_ID" > "$LIONCLAW_SKILL_STATE_DIR/id"
"#,
        );
        let recorder = SkillPrivateContextRecorder::new(config.clone());

        let outcome = recorder.record(request()).await;

        assert_eq!(outcome.status, PrivateContextRecordStatus::Completed);
        assert_eq!(
            fs::read_to_string(config.state_dir.join("id")).expect("id"),
            "private-context-core\n"
        );
        let captured: Value = serde_json::from_str(
            &fs::read_to_string(config.state_dir.join("request.json")).expect("request"),
        )
        .expect("request json");
        assert_eq!(captured["turn_id"], "22222222-2222-2222-2222-222222222222");
        assert_eq!(captured["sequence_no"], 7);
        assert_eq!(captured["surface"], "program_turn");
        assert_eq!(captured["transcript"]["user"]["text"], "hello");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn recorder_nonzero_exit_is_failed() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let config = write_recorder_script(temp_dir.path(), "#!/usr/bin/env bash\nexit 42\n");
        let recorder = SkillPrivateContextRecorder::new(config);

        let outcome = recorder.record(request()).await;

        assert_eq!(outcome.status, PrivateContextRecordStatus::Failed);
        assert_eq!(outcome.reason, Some("nonzero_exit"));
        assert_eq!(outcome.exit_status, Some(42));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn recorder_stdout_is_invalid_output() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let config =
            write_recorder_script(temp_dir.path(), "#!/usr/bin/env bash\nprintf unexpected\n");
        let recorder = SkillPrivateContextRecorder::new(config);

        let outcome = recorder.record(request()).await;

        assert_eq!(outcome.status, PrivateContextRecordStatus::InvalidOutput);
        assert_eq!(outcome.reason, Some("unexpected_stdout"));
        assert_eq!(outcome.exit_status, Some(0));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn recorder_timeout_kills_process_group() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let config = write_recorder_script(
            temp_dir.path(),
            r#"#!/usr/bin/env bash
set -euo pipefail
(sleep 10; printf leaked > "$LIONCLAW_SKILL_STATE_DIR/leaked") &
sleep 10
"#,
        )
        .with_timeout(Duration::from_millis(50));
        let state_dir = config.state_dir.clone();
        let recorder = SkillPrivateContextRecorder::new(config);

        let outcome = recorder.record(request()).await;
        tokio::time::sleep(Duration::from_millis(80)).await;

        assert_eq!(outcome.status, PrivateContextRecordStatus::TimedOut);
        assert!(!state_dir.join("leaked").exists());
    }
}
