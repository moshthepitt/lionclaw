use std::{
    collections::HashMap,
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

use super::policy::Capability;

pub mod adapters;
pub mod builtins;
mod codex_host_auth;
pub mod execution;
mod skill_projection;

pub use adapters::{
    CodexRuntimeAdapter, CodexRuntimeConfig, MockRuntimeAdapter, OpenCodeRuntimeAdapter,
    OpenCodeRuntimeConfig,
};
pub use builtins::{register_builtin_runtime_adapters, BUILTIN_RUNTIME_MOCK};
pub use codex_host_auth::{ensure_codex_host_auth_ready, sync_codex_home_into_runtime};
pub use execution::{
    resolve_oci_image_compatibility_identity, skill_mount_target, spawn_interactive,
    validate_oci_launch_prerequisites, ConfinementBackend, ConfinementConfig,
    EffectiveExecutionPlan, EscapeClass, ExecutionBackend, ExecutionLimits, ExecutionOutput,
    ExecutionPlanPurpose, ExecutionPlanRequest, ExecutionPlanner, ExecutionPlannerConfig,
    ExecutionPreset, ExecutionRequest, ExecutionSession, MountAccess, MountSpec, NetworkMode,
    OciConfinementConfig, OciExecutionBackend, RuntimeAuthKind, RuntimeExecutionProfile,
    RuntimeProgramSpec, RuntimeSecretsMount, WorkspaceAccess, BUILTIN_PRESET_EVERYDAY,
    BUILTIN_PRESET_HIDDEN_COMPACTION, SKILLS_MOUNT_TARGET_ROOT,
};
pub use skill_projection::project_runtime_skills;

async fn validate_runtime_auth_prerequisites(
    runtime_id: &str,
    required_auth: Option<RuntimeAuthKind>,
    codex_home_override: Option<&Path>,
) -> Result<()> {
    let Some(required_auth) = required_auth else {
        return Ok(());
    };

    match required_auth {
        RuntimeAuthKind::Codex => ensure_codex_host_auth_ready(codex_home_override)
            .await
            .map_err(|err| anyhow!("configured runtime profile '{runtime_id}' requires {err}")),
    }
}

pub async fn validate_runtime_launch_prerequisites(
    runtime_id: &str,
    confinement: &ConfinementConfig,
    required_auth: Option<RuntimeAuthKind>,
    codex_home_override: Option<&Path>,
) -> Result<()> {
    validate_runtime_prerequisites(
        runtime_id,
        confinement,
        required_auth,
        codex_home_override,
        None,
    )
    .await
}

pub async fn validate_runtime_execution_prerequisites(
    runtime_id: &str,
    confinement: &ConfinementConfig,
    required_auth: Option<RuntimeAuthKind>,
    codex_home_override: Option<&Path>,
    network_mode: NetworkMode,
) -> Result<()> {
    validate_runtime_prerequisites(
        runtime_id,
        confinement,
        required_auth,
        codex_home_override,
        Some(network_mode),
    )
    .await
}

async fn validate_runtime_prerequisites(
    runtime_id: &str,
    confinement: &ConfinementConfig,
    required_auth: Option<RuntimeAuthKind>,
    codex_home_override: Option<&Path>,
    network_mode: Option<NetworkMode>,
) -> Result<()> {
    validate_runtime_auth_prerequisites(runtime_id, required_auth, codex_home_override).await?;

    match confinement {
        ConfinementConfig::Oci(config) => {
            validate_oci_launch_prerequisites(runtime_id, config, required_auth).await?;
            if network_mode == Some(NetworkMode::On) {
                execution::oci::validate_oci_private_network_prerequisites(runtime_id, config)
                    .await?;
            }
            Ok(())
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeAdapterInfo {
    pub id: String,
    pub version: String,
    pub healthy: bool,
}

#[derive(Debug, Clone)]
pub struct RuntimeSessionStartInput {
    pub session_id: Uuid,
    pub working_dir: Option<String>,
    pub environment: Vec<(String, String)>,
    pub runtime_skill_ids: Vec<String>,
    pub runtime_state_root: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct RuntimeSessionHandle {
    pub runtime_session_id: String,
    pub resumes_existing_session: bool,
}

#[derive(Debug, Clone)]
pub struct RuntimeTurnInput {
    pub runtime_session_id: String,
    pub prompt: String,
    pub fresh_prompt: Option<String>,
    pub runtime_skill_ids: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeControlOrigin {
    SessionTurn,
    ChannelInbound,
}

impl RuntimeControlOrigin {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SessionTurn => "session_turn",
            Self::ChannelInbound => "channel_inbound",
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeControlInput {
    pub runtime_session_id: String,
    pub raw: String,
    pub command_name: String,
    pub arguments: String,
    pub origin: RuntimeControlOrigin,
    pub runtime_skill_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeControlOutcome {
    Handled {
        message: String,
    },
    Unsupported {
        message: String,
    },
    InteractiveOnly {
        message: String,
    },
    Failed {
        code: Option<String>,
        message: String,
    },
}

impl RuntimeControlOutcome {
    pub fn message(&self) -> &str {
        match self {
            Self::Handled { message }
            | Self::Unsupported { message }
            | Self::InteractiveOnly { message }
            | Self::Failed { message, .. } => message,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Handled { .. } => "handled",
            Self::Unsupported { .. } => "unsupported",
            Self::InteractiveOnly { .. } => "interactive_only",
            Self::Failed { .. } => "failed",
        }
    }

    pub fn failed_error_code(&self) -> Option<&str> {
        match self {
            Self::Failed { code, .. } => Some(code.as_deref().unwrap_or("runtime.control.failed")),
            Self::Handled { .. } | Self::Unsupported { .. } | Self::InteractiveOnly { .. } => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeControlExecution {
    pub input: RuntimeControlInput,
    pub plan: EffectiveExecutionPlan,
    pub runtime_secrets_mount: Option<RuntimeSecretsMount>,
    pub codex_home_override: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct RuntimeProgramTurnExecution {
    pub input: RuntimeTurnInput,
    pub plan: EffectiveExecutionPlan,
    pub runtime_secrets_mount: Option<RuntimeSecretsMount>,
    pub codex_home_override: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct RuntimeCapabilityRequest {
    pub request_id: String,
    pub skill_id: String,
    pub capability: Capability,
    pub scope: Option<String>,
    pub payload: Value,
}

#[derive(Debug, Clone)]
pub struct RuntimeCapabilityResult {
    pub request_id: String,
    pub allowed: bool,
    pub reason: Option<String>,
    pub output: Value,
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeTurnResult {
    pub capability_requests: Vec<RuntimeCapabilityRequest>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeMessageLane {
    Answer,
    Reasoning,
}

impl RuntimeMessageLane {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Answer => "answer",
            Self::Reasoning => "reasoning",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeArtifact {
    pub artifact_id: String,
    pub path: PathBuf,
    pub filename: Option<String>,
    pub mime_type: Option<String>,
}

#[derive(Debug, Clone)]
pub enum RuntimeEvent {
    MessageDelta {
        lane: RuntimeMessageLane,
        text: String,
    },
    MessageBoundary {
        lane: RuntimeMessageLane,
    },
    Status {
        code: Option<String>,
        text: String,
    },
    Artifact {
        artifact: RuntimeArtifact,
    },
    Done,
    Error {
        code: Option<String>,
        text: String,
    },
}

pub type RuntimeEventSender = mpsc::UnboundedSender<RuntimeEvent>;

pub fn append_streamed_text_delta(existing: &mut String, delta: &str) {
    existing.push_str(delta);
}

pub fn append_streamed_text_boundary(existing: &mut String) {
    if existing.trim().is_empty() || existing.ends_with("\n\n") {
        return;
    }
    if existing.ends_with('\n') {
        existing.push('\n');
    } else {
        existing.push_str("\n\n");
    }
}

pub trait RuntimeProgramOutputParser: Send {
    fn parse_line(&mut self, line: &str) -> Vec<RuntimeEvent>;
    fn finish(&mut self) -> Vec<RuntimeEvent> {
        Vec::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HiddenTurnSupport {
    Unsupported,
    SideEffectFree,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeTurnMode {
    Direct,
    ProgramBacked,
}

#[async_trait]
pub trait RuntimeAdapter: Send + Sync {
    async fn info(&self) -> RuntimeAdapterInfo;
    fn hidden_turn_support(&self) -> HiddenTurnSupport {
        HiddenTurnSupport::Unsupported
    }
    fn turn_mode(&self) -> RuntimeTurnMode {
        RuntimeTurnMode::Direct
    }
    async fn session_start(&self, input: RuntimeSessionStartInput) -> Result<RuntimeSessionHandle>;
    async fn turn(
        &self,
        _input: RuntimeTurnInput,
        _events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
        Err(anyhow!("runtime does not implement direct turns"))
    }
    fn build_turn_program(&self, _input: &RuntimeTurnInput) -> Result<RuntimeProgramSpec> {
        Err(anyhow!("runtime does not support program-backed turns"))
    }
    fn program_output_parser(
        &self,
        _input: &RuntimeTurnInput,
    ) -> Option<Box<dyn RuntimeProgramOutputParser>> {
        None
    }
    fn parse_program_output_line(&self, _line: &str) -> Vec<RuntimeEvent> {
        Vec::new()
    }
    fn format_program_exit_error(
        &self,
        output: &ExecutionOutput,
        observed_error_text: Option<&str>,
    ) -> String {
        let code = output.exit_code.unwrap_or(1);
        if let Some(text) = observed_error_text.filter(|text| !text.trim().is_empty()) {
            format!("runtime process exited with code {code}: {text}")
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            if stderr.is_empty() {
                format!("runtime process exited with code {code}")
            } else {
                format!("runtime process exited with code {code}: {stderr}")
            }
        }
    }
    fn prepare_program_retry_after_failure(
        &self,
        _input: &RuntimeTurnInput,
        _output: &ExecutionOutput,
        _observed_error_text: Option<&str>,
        _events: &RuntimeEventSender,
    ) -> Result<bool> {
        Ok(false)
    }
    async fn program_backed_turn(
        &self,
        execution: RuntimeProgramTurnExecution,
        events: RuntimeEventSender,
    ) -> Result<RuntimeTurnResult> {
        execute_program_backed_turn(
            self,
            execution.plan,
            execution.runtime_secrets_mount,
            execution.codex_home_override,
            execution.input,
            events,
        )
        .await
    }
    async fn runtime_control(
        &self,
        execution: RuntimeControlExecution,
        _events: RuntimeEventSender,
    ) -> Result<RuntimeControlOutcome> {
        Ok(RuntimeControlOutcome::Unsupported {
            message: format!(
                "runtime does not support native control command '/{}'",
                execution.input.command_name
            ),
        })
    }
    async fn resolve_capability_requests(
        &self,
        handle: &RuntimeSessionHandle,
        results: Vec<RuntimeCapabilityResult>,
        events: RuntimeEventSender,
    ) -> Result<()>;
    async fn cancel(&self, handle: &RuntimeSessionHandle, reason: Option<String>) -> Result<()>;
    async fn close(&self, handle: &RuntimeSessionHandle) -> Result<()>;
}

#[derive(Default, Clone)]
pub struct RuntimeRegistry {
    adapters: Arc<RwLock<HashMap<String, Arc<dyn RuntimeAdapter>>>>,
}

impl RuntimeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(&self, id: impl Into<String>, adapter: Arc<dyn RuntimeAdapter>) {
        self.adapters.write().await.insert(id.into(), adapter);
    }

    pub async fn get(&self, id: &str) -> Option<Arc<dyn RuntimeAdapter>> {
        self.adapters.read().await.get(id).cloned()
    }

    pub async fn list(&self) -> Vec<String> {
        self.adapters.read().await.keys().cloned().collect()
    }
}

pub async fn execute_program_backed_turn<A>(
    adapter: &A,
    plan: EffectiveExecutionPlan,
    runtime_secrets_mount: Option<RuntimeSecretsMount>,
    codex_home_override: Option<PathBuf>,
    input: RuntimeTurnInput,
    events: RuntimeEventSender,
) -> Result<RuntimeTurnResult>
where
    A: RuntimeAdapter + Send + Sync + ?Sized,
{
    execute_program_backed_turn_with_executor(
        adapter,
        plan,
        runtime_secrets_mount,
        codex_home_override,
        input,
        events,
        execution::execute_streaming,
    )
    .await
}

async fn execute_program_backed_turn_with_executor<A, E, Fut>(
    adapter: &A,
    plan: EffectiveExecutionPlan,
    runtime_secrets_mount: Option<RuntimeSecretsMount>,
    codex_home_override: Option<PathBuf>,
    input: RuntimeTurnInput,
    events: RuntimeEventSender,
    mut executor: E,
) -> Result<RuntimeTurnResult>
where
    A: RuntimeAdapter + Send + Sync + ?Sized,
    E: FnMut(ExecutionRequest, execution::backend::ExecutionStdoutSender) -> Fut,
    Fut: Future<Output = Result<ExecutionOutput>>,
{
    let mut attempted_retry = false;
    let mut current_input = input;

    loop {
        let attempt = run_program_backed_attempt(
            adapter,
            &plan,
            runtime_secrets_mount.clone(),
            codex_home_override.clone(),
            &current_input,
            &events,
            &mut executor,
        )
        .await?;

        if attempt.output.success() {
            flush_buffered_program_output_events(&events, attempt.buffered_errors);
            return finish_program_backed_turn(
                adapter,
                attempt.output,
                attempt.last_error_text.as_deref(),
                attempt.saw_done,
                &events,
            );
        }

        if !attempted_retry
            && adapter.prepare_program_retry_after_failure(
                &current_input,
                &attempt.output,
                attempt.last_error_text.as_deref(),
                &events,
            )?
        {
            attempted_retry = true;
            if let Some(fresh_prompt) = current_input.fresh_prompt.take() {
                current_input.prompt = fresh_prompt;
            }
            continue;
        }

        flush_buffered_program_output_events(&events, attempt.buffered_errors);
        return finish_program_backed_turn(
            adapter,
            attempt.output,
            attempt.last_error_text.as_deref(),
            attempt.saw_done,
            &events,
        );
    }
}

struct ProgramBackedAttemptOutcome {
    buffered_errors: Option<Vec<RuntimeEvent>>,
    output: ExecutionOutput,
    saw_done: bool,
    last_error_text: Option<String>,
}

async fn run_program_backed_attempt<A, E, Fut>(
    adapter: &A,
    plan: &EffectiveExecutionPlan,
    runtime_secrets_mount: Option<RuntimeSecretsMount>,
    codex_home_override: Option<PathBuf>,
    input: &RuntimeTurnInput,
    events: &RuntimeEventSender,
    executor: &mut E,
) -> Result<ProgramBackedAttemptOutcome>
where
    A: RuntimeAdapter + Send + Sync + ?Sized,
    E: FnMut(ExecutionRequest, execution::backend::ExecutionStdoutSender) -> Fut,
    Fut: Future<Output = Result<ExecutionOutput>>,
{
    let program = adapter.build_turn_program(input)?;
    let (stdout_tx, mut stdout_rx) = mpsc::unbounded_channel();
    let execution = executor(
        ExecutionRequest {
            plan: plan.clone(),
            program,
            runtime_secrets_mount,
            codex_home_override,
        },
        stdout_tx,
    );
    tokio::pin!(execution);

    let mut buffered_errors = input.fresh_prompt.is_some().then(Vec::new);
    let mut saw_done = false;
    let mut last_error_text: Option<String> = None;
    let mut output_parser = adapter.program_output_parser(input);

    loop {
        tokio::select! {
            maybe_line = stdout_rx.recv() => {
                match maybe_line {
                    Some(line) => observe_program_output_line(
                        adapter,
                        &mut output_parser,
                        events,
                        &mut buffered_errors,
                        &line,
                        &mut saw_done,
                        &mut last_error_text,
                    ),
                    None => {
                        finish_program_output_parser(
                            &mut output_parser,
                            events,
                            &mut buffered_errors,
                            &mut saw_done,
                            &mut last_error_text,
                        );
                        let output = execution.await?;
                        return Ok(ProgramBackedAttemptOutcome {
                            buffered_errors,
                            output,
                            saw_done,
                            last_error_text,
                        });
                    }
                }
            }
            output = &mut execution => {
                let output = output?;
                while let Some(line) = stdout_rx.recv().await {
                    observe_program_output_line(
                        adapter,
                        &mut output_parser,
                        events,
                        &mut buffered_errors,
                        &line,
                        &mut saw_done,
                        &mut last_error_text,
                    );
                }
                finish_program_output_parser(
                    &mut output_parser,
                    events,
                    &mut buffered_errors,
                    &mut saw_done,
                    &mut last_error_text,
                );
                return Ok(ProgramBackedAttemptOutcome {
                    buffered_errors,
                    output,
                    saw_done,
                    last_error_text,
                });
            }
        }
    }
}

fn observe_program_output_line<A>(
    adapter: &A,
    output_parser: &mut Option<Box<dyn RuntimeProgramOutputParser>>,
    events: &RuntimeEventSender,
    buffered_errors: &mut Option<Vec<RuntimeEvent>>,
    line: &str,
    saw_done: &mut bool,
    last_error_text: &mut Option<String>,
) where
    A: RuntimeAdapter + Send + Sync + ?Sized,
{
    let parsed_events = if let Some(parser) = output_parser.as_mut() {
        parser.parse_line(line)
    } else {
        adapter.parse_program_output_line(line)
    };

    observe_program_output_events(
        events,
        buffered_errors,
        parsed_events,
        saw_done,
        last_error_text,
    );
}

fn finish_program_output_parser(
    output_parser: &mut Option<Box<dyn RuntimeProgramOutputParser>>,
    events: &RuntimeEventSender,
    buffered_errors: &mut Option<Vec<RuntimeEvent>>,
    saw_done: &mut bool,
    last_error_text: &mut Option<String>,
) {
    if let Some(parser) = output_parser.as_mut() {
        observe_program_output_events(
            events,
            buffered_errors,
            parser.finish(),
            saw_done,
            last_error_text,
        );
    }
}

fn observe_program_output_events(
    events: &RuntimeEventSender,
    buffered_errors: &mut Option<Vec<RuntimeEvent>>,
    parsed_events: Vec<RuntimeEvent>,
    saw_done: &mut bool,
    last_error_text: &mut Option<String>,
) {
    for event in parsed_events {
        if matches!(event, RuntimeEvent::Done) {
            *saw_done = true;
        }
        if let RuntimeEvent::Error { text, .. } = &event {
            *last_error_text = Some(text.clone());
        }
        if matches!(event, RuntimeEvent::Error { .. }) {
            if let Some(buffer) = buffered_errors.as_mut() {
                buffer.push(event);
            } else {
                drop(events.send(event));
            }
        } else {
            drop(events.send(event));
        }
    }
}

fn flush_buffered_program_output_events(
    events: &RuntimeEventSender,
    buffered_errors: Option<Vec<RuntimeEvent>>,
) {
    if let Some(buffered_errors) = buffered_errors {
        for event in buffered_errors {
            drop(events.send(event));
        }
    }
}

fn finish_program_backed_turn<A>(
    adapter: &A,
    output: ExecutionOutput,
    observed_error_text: Option<&str>,
    saw_done: bool,
    events: &RuntimeEventSender,
) -> Result<RuntimeTurnResult>
where
    A: RuntimeAdapter + Send + Sync + ?Sized,
{
    if !output.success() {
        return Err(anyhow!(
            adapter.format_program_exit_error(&output, observed_error_text)
        ));
    }

    if !saw_done {
        drop(events.send(RuntimeEvent::Done));
    }

    Ok(RuntimeTurnResult {
        capability_requests: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use super::{
        execute_program_backed_turn_with_executor, ConfinementConfig, EffectiveExecutionPlan,
        ExecutionLimits, ExecutionOutput, MountAccess, MountSpec, NetworkMode,
        OciConfinementConfig, RuntimeAdapter, RuntimeAdapterInfo, RuntimeCapabilityResult,
        RuntimeEvent, RuntimeEventSender, RuntimeMessageLane, RuntimeProgramSpec,
        RuntimeSessionHandle, RuntimeSessionStartInput, RuntimeTurnInput, RuntimeTurnMode,
        RuntimeTurnResult, WorkspaceAccess,
    };
    use anyhow::{anyhow, Result};
    use async_trait::async_trait;
    use tokio::{
        sync::mpsc,
        time::{sleep, timeout},
    };

    #[derive(Clone)]
    struct StubAttempt {
        stdout_lines: Vec<String>,
        output: ExecutionOutput,
    }

    struct TestProgramAdapter {
        retry_used: std::sync::atomic::AtomicBool,
    }

    impl Default for TestProgramAdapter {
        fn default() -> Self {
            Self {
                retry_used: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }

    #[async_trait]
    impl RuntimeAdapter for TestProgramAdapter {
        async fn info(&self) -> RuntimeAdapterInfo {
            RuntimeAdapterInfo {
                id: "test-program".to_string(),
                version: "0.1".to_string(),
                healthy: true,
            }
        }

        fn turn_mode(&self) -> RuntimeTurnMode {
            RuntimeTurnMode::ProgramBacked
        }

        async fn session_start(
            &self,
            _input: RuntimeSessionStartInput,
        ) -> Result<RuntimeSessionHandle> {
            Ok(RuntimeSessionHandle {
                runtime_session_id: "test-program-session".to_string(),
                resumes_existing_session: false,
            })
        }

        fn build_turn_program(&self, input: &RuntimeTurnInput) -> Result<RuntimeProgramSpec> {
            Ok(RuntimeProgramSpec {
                executable: "agent".to_string(),
                args: vec!["run".to_string()],
                environment: Vec::new(),
                stdin: input.prompt.clone(),
                auth: None,
            })
        }

        fn parse_program_output_line(&self, line: &str) -> Vec<RuntimeEvent> {
            let line = line.trim();
            if let Some(text) = line.strip_prefix("status:") {
                return vec![RuntimeEvent::Status {
                    code: None,
                    text: text.trim().to_string(),
                }];
            }
            if let Some(text) = line.strip_prefix("answer:") {
                return vec![RuntimeEvent::MessageDelta {
                    lane: RuntimeMessageLane::Answer,
                    text: text.trim().to_string(),
                }];
            }
            if let Some(text) = line.strip_prefix("error:") {
                return vec![RuntimeEvent::Error {
                    code: Some("runtime.error".to_string()),
                    text: text.trim().to_string(),
                }];
            }
            Vec::new()
        }

        fn prepare_program_retry_after_failure(
            &self,
            _input: &RuntimeTurnInput,
            _output: &ExecutionOutput,
            observed_error_text: Option<&str>,
            events: &RuntimeEventSender,
        ) -> Result<bool> {
            if self
                .retry_used
                .swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                return Ok(false);
            }
            if observed_error_text != Some("stale thread") {
                return Ok(false);
            }
            drop(events.send(RuntimeEvent::Status {
                code: None,
                text: "program retry requested".to_string(),
            }));
            Ok(true)
        }

        async fn resolve_capability_requests(
            &self,
            _handle: &RuntimeSessionHandle,
            results: Vec<RuntimeCapabilityResult>,
            _events: RuntimeEventSender,
        ) -> Result<()> {
            if results.is_empty() {
                Ok(())
            } else {
                Err(anyhow!("test adapter does not resolve capabilities"))
            }
        }

        async fn cancel(
            &self,
            _handle: &RuntimeSessionHandle,
            _reason: Option<String>,
        ) -> Result<()> {
            Ok(())
        }

        async fn close(&self, _handle: &RuntimeSessionHandle) -> Result<()> {
            Ok(())
        }
    }

    fn test_execution_plan() -> EffectiveExecutionPlan {
        EffectiveExecutionPlan {
            runtime_id: "codex".to_string(),
            preset_name: "everyday".to_string(),
            confinement: ConfinementConfig::Oci(OciConfinementConfig::default()),
            workspace_access: WorkspaceAccess::ReadWrite,
            network_mode: NetworkMode::On,
            working_dir: None,
            environment: Vec::new(),
            idle_timeout: Duration::from_secs(30),
            hard_timeout: Duration::from_secs(90),
            mounts: vec![MountSpec {
                source: "/tmp/runtime".into(),
                target: "/runtime".to_string(),
                access: MountAccess::ReadWrite,
            }],
            mount_runtime_secrets: false,
            escape_classes: Default::default(),
            limits: ExecutionLimits::default(),
        }
    }

    #[tokio::test]
    async fn program_backed_retry_uses_fresh_prompt_when_adapter_requests_retry() {
        let adapter = TestProgramAdapter::default();
        let attempts = Arc::new(Mutex::new(VecDeque::from([
            StubAttempt {
                stdout_lines: vec!["error: stale thread".to_string()],
                output: ExecutionOutput {
                    exit_code: Some(1),
                    ..ExecutionOutput::default()
                },
            },
            StubAttempt {
                stdout_lines: vec!["answer: Done.".to_string()],
                output: ExecutionOutput {
                    exit_code: Some(0),
                    ..ExecutionOutput::default()
                },
            },
        ])));
        let recorded_programs = Arc::new(Mutex::new(Vec::<RuntimeProgramSpec>::new()));
        let recorded_stdin = Arc::new(Mutex::new(Vec::<String>::new()));

        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let result = execute_program_backed_turn_with_executor(
            &adapter,
            test_execution_plan(),
            None,
            None,
            RuntimeTurnInput {
                runtime_session_id: "test-program-session".to_string(),
                prompt: "hello".to_string(),
                fresh_prompt: Some("fresh history prompt".to_string()),
                runtime_skill_ids: Vec::new(),
            },
            event_tx,
            {
                let attempts = Arc::clone(&attempts);
                let recorded_programs = Arc::clone(&recorded_programs);
                let recorded_stdin = Arc::clone(&recorded_stdin);
                move |request, stdout| {
                    let attempts = Arc::clone(&attempts);
                    let recorded_programs = Arc::clone(&recorded_programs);
                    let recorded_stdin = Arc::clone(&recorded_stdin);
                    async move {
                        recorded_programs
                            .lock()
                            .expect("recorded programs lock")
                            .push(request.program.clone());
                        recorded_stdin
                            .lock()
                            .expect("recorded stdin lock")
                            .push(request.program.stdin.clone());
                        let attempt = attempts
                            .lock()
                            .expect("attempts lock")
                            .pop_front()
                            .expect("attempt");
                        for line in attempt.stdout_lines {
                            stdout.send(format!("{line}\n")).expect("send stdout line");
                        }
                        Ok(attempt.output)
                    }
                }
            },
        )
        .await;

        assert!(matches!(
            result,
            Ok(RuntimeTurnResult {
                capability_requests
            }) if capability_requests.is_empty()
        ));

        let programs = recorded_programs
            .lock()
            .expect("recorded programs lock")
            .clone();
        assert_eq!(programs.len(), 2, "expected one retry");
        assert!(programs
            .iter()
            .all(|program| program.args == ["run".to_string()]));
        let stdin = recorded_stdin.lock().expect("recorded stdin lock").clone();
        assert_eq!(stdin.as_slice(), ["hello", "fresh history prompt"]);

        let mut events = Vec::new();
        while let Some(event) = event_rx.recv().await {
            events.push(event);
        }
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::Status { code: None, text }
                if text == "program retry requested"
        )));
        assert!(!events.iter().any(|event| matches!(
            event,
            RuntimeEvent::Error { text, .. }
                if text.contains("stale thread")
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::MessageDelta { text, .. } if text == "Done."
        )));
    }

    #[tokio::test]
    async fn program_backed_turn_streams_events_before_attempt_finishes() {
        let adapter = Arc::new(TestProgramAdapter::default());
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let adapter_for_turn = Arc::clone(&adapter);
        let turn = tokio::spawn(async move {
            execute_program_backed_turn_with_executor(
                adapter_for_turn.as_ref(),
                test_execution_plan(),
                None,
                None,
                RuntimeTurnInput {
                    runtime_session_id: "test-program-session".to_string(),
                    prompt: "hello".to_string(),
                    fresh_prompt: Some("fresh history prompt".to_string()),
                    runtime_skill_ids: Vec::new(),
                },
                event_tx,
                move |_request, stdout| async move {
                    stdout
                        .send("status: started\n".to_string())
                        .expect("send started");
                    sleep(Duration::from_millis(100)).await;
                    stdout
                        .send("answer: Done.\n".to_string())
                        .expect("send complete");
                    Ok(ExecutionOutput {
                        exit_code: Some(0),
                        ..ExecutionOutput::default()
                    })
                },
            )
            .await
        });

        let first_event = timeout(Duration::from_millis(50), event_rx.recv())
            .await
            .expect("expected streamed event before attempt finished")
            .expect("streamed event");
        assert!(matches!(
            first_event,
            RuntimeEvent::Status { code: None, text } if text == "started"
        ));

        let result = turn.await.expect("join turn");
        assert!(matches!(
            result,
            Ok(RuntimeTurnResult {
                capability_requests
            }) if capability_requests.is_empty()
        ));

        let mut remaining_events = Vec::new();
        while let Some(event) = event_rx.recv().await {
            remaining_events.push(event);
        }
        assert!(remaining_events.iter().any(|event| matches!(
            event,
            RuntimeEvent::MessageDelta { text, .. } if text == "Done."
        )));
    }
}
