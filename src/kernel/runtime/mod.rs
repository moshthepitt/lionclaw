use std::{
    collections::HashMap,
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

pub use adapters::{
    CodexRuntimeAdapter, CodexRuntimeConfig, MockRuntimeAdapter, OpenCodeRuntimeAdapter,
    OpenCodeRuntimeConfig,
};
pub use builtins::{register_builtin_runtime_adapters, BUILTIN_RUNTIME_MOCK};
pub use codex_host_auth::{resolve_codex_host_auth, CodexHostAuth, CodexHostAuthMode};
pub use execution::{
    ConfinementBackend, ConfinementConfig, EffectiveExecutionPlan, EscapeClass, ExecutionBackend,
    ExecutionLimits, ExecutionOutput, ExecutionPlanPurpose, ExecutionPlanRequest, ExecutionPlanner,
    ExecutionPlannerConfig, ExecutionPreset, ExecutionRequest, MountAccess, MountSpec, NetworkMode,
    OciConfinementConfig, OciExecutionBackend, RuntimeAuthKind, RuntimeExecutionProfile,
    RuntimeProgramSpec, RuntimeSecretsMount, WorkspaceAccess, BUILTIN_PRESET_EVERYDAY,
    BUILTIN_PRESET_HIDDEN_COMPACTION,
};

pub async fn validate_runtime_auth_prerequisites(
    runtime_id: &str,
    required_auth: Option<RuntimeAuthKind>,
    codex_home_override: Option<&Path>,
) -> Result<()> {
    let Some(required_auth) = required_auth else {
        return Ok(());
    };

    match required_auth {
        RuntimeAuthKind::Codex => resolve_codex_host_auth(codex_home_override)
            .await
            .map(|_| ())
            .map_err(|err| {
                anyhow!(
                    "configured runtime profile '{}' requires {}",
                    runtime_id,
                    err
                )
            }),
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
    pub selected_skills: Vec<String>,
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
    pub selected_skills: Vec<String>,
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

#[derive(Debug, Clone)]
pub enum RuntimeEvent {
    MessageDelta {
        lane: RuntimeMessageLane,
        text: String,
    },
    Status {
        code: Option<String>,
        text: String,
    },
    Done,
    Error {
        code: Option<String>,
        text: String,
    },
}

pub type RuntimeEventSender = mpsc::UnboundedSender<RuntimeEvent>;

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
            format!("runtime process exited with code {}: {}", code, text)
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            if stderr.is_empty() {
                format!("runtime process exited with code {}", code)
            } else {
                format!("runtime process exited with code {}: {}", code, stderr)
            }
        }
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

pub async fn execute_program_backed_turn(
    adapter: &(dyn RuntimeAdapter + Send + Sync),
    plan: EffectiveExecutionPlan,
    runtime_secrets_mount: Option<RuntimeSecretsMount>,
    codex_home_override: Option<PathBuf>,
    input: RuntimeTurnInput,
    events: RuntimeEventSender,
) -> Result<RuntimeTurnResult> {
    let program = adapter.build_turn_program(&input)?;
    let (stdout_tx, mut stdout_rx) = mpsc::unbounded_channel();
    let execution = execution::execute_streaming(
        ExecutionRequest {
            plan,
            program,
            runtime_secrets_mount,
            codex_home_override,
        },
        stdout_tx,
    );
    tokio::pin!(execution);

    let mut saw_done = false;
    let mut last_error_text: Option<String> = None;

    loop {
        tokio::select! {
            maybe_line = stdout_rx.recv() => {
                match maybe_line {
                    Some(line) => observe_program_output_line(
                        adapter,
                        &events,
                        &line,
                        &mut saw_done,
                        &mut last_error_text,
                    ),
                    None => {
                        let output = execution.await?;
                        return finish_program_backed_turn(
                            adapter,
                            output,
                            last_error_text.as_deref(),
                            saw_done,
                            &events,
                        );
                    }
                }
            }
            output = &mut execution => {
                let output = output?;
                while let Some(line) = stdout_rx.recv().await {
                    observe_program_output_line(
                        adapter,
                        &events,
                        &line,
                        &mut saw_done,
                        &mut last_error_text,
                    );
                }
                return finish_program_backed_turn(
                    adapter,
                    output,
                    last_error_text.as_deref(),
                    saw_done,
                    &events,
                );
            }
        }
    }
}

fn observe_program_output_line(
    adapter: &(dyn RuntimeAdapter + Send + Sync),
    events: &RuntimeEventSender,
    line: &str,
    saw_done: &mut bool,
    last_error_text: &mut Option<String>,
) {
    for event in adapter.parse_program_output_line(line) {
        if matches!(event, RuntimeEvent::Done) {
            *saw_done = true;
        }
        if let RuntimeEvent::Error { text, .. } = &event {
            *last_error_text = Some(text.clone());
        }
        let _ = events.send(event);
    }
}

fn finish_program_backed_turn(
    adapter: &(dyn RuntimeAdapter + Send + Sync),
    output: ExecutionOutput,
    observed_error_text: Option<&str>,
    saw_done: bool,
    events: &RuntimeEventSender,
) -> Result<RuntimeTurnResult> {
    if !output.success() {
        return Err(anyhow!(
            adapter.format_program_exit_error(&output, observed_error_text)
        ));
    }

    if !saw_done {
        let _ = events.send(RuntimeEvent::Done);
    }

    Ok(RuntimeTurnResult {
        capability_requests: Vec::new(),
    })
}
