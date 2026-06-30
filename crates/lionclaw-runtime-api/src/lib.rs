#![cfg_attr(
    not(test),
    warn(
        clippy::allow_attributes_without_reason,
        clippy::clone_on_ref_ptr,
        clippy::expect_used,
        clippy::future_not_send,
        clippy::get_unwrap,
        clippy::indexing_slicing,
        clippy::large_futures,
        clippy::large_stack_arrays,
        clippy::large_types_passed_by_value,
        clippy::let_underscore_must_use,
        clippy::mutex_atomic,
        clippy::mutex_integer,
        clippy::panic,
        clippy::panic_in_result_fn,
        clippy::pathbuf_init_then_push,
        clippy::rc_buffer,
        clippy::rc_mutex,
        clippy::redundant_clone,
        clippy::same_name_method,
        clippy::significant_drop_in_scrutinee,
        clippy::significant_drop_tightening,
        clippy::uninlined_format_args,
        clippy::unused_result_ok,
        clippy::unwrap_in_result,
        clippy::unwrap_used,
        reason = "production code follows LionClaw's strict Clippy profile; tests keep fail-fast ergonomics"
    )
)]

use std::{
    collections::BTreeMap,
    ffi::OsString,
    fmt,
    fs::File,
    io::Read,
    path::{Component, Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use lionclaw_durable_fs::write_file_atomically;
use rustix::{
    fs::{open, openat, unlinkat, AtFlags, Mode, OFlags},
    io::Errno,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    Any,
    FsRead,
    FsWrite,
    NetEgress,
    SecretRequest,
    ChannelSend,
    SchedulerRun,
}

impl Capability {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Any => "*",
            Self::FsRead => "fs.read",
            Self::FsWrite => "fs.write",
            Self::NetEgress => "net.egress",
            Self::SecretRequest => "secret.request",
            Self::ChannelSend => "channel.send",
            Self::SchedulerRun => "scheduler.run",
        }
    }
}

impl FromStr for Capability {
    type Err = String;

    fn from_str(raw: &str) -> std::result::Result<Self, Self::Err> {
        match raw.trim() {
            "*" => Ok(Self::Any),
            "fs.read" => Ok(Self::FsRead),
            "fs.write" => Ok(Self::FsWrite),
            "net.egress" => Ok(Self::NetEgress),
            "secret.request" => Ok(Self::SecretRequest),
            "channel.send" => Ok(Self::ChannelSend),
            "scheduler.run" => Ok(Self::SchedulerRun),
            other => Err(format!("unsupported capability '{other}'")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NetworkMode {
    None,
    On,
}

impl NetworkMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::On => "on",
        }
    }
}

/// Adapter-produced program invocation details, independent from how LionClaw
/// chooses to confine the process.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct RuntimeProgramSpec {
    pub executable: String,
    pub args: Vec<String>,
    pub environment: Vec<(String, String)>,
    pub stdin: String,
    pub auth: Option<RuntimeAuthKind>,
}

impl fmt::Debug for RuntimeProgramSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeProgramSpec")
            .field("executable", &self.executable)
            .field("args", &self.args)
            .field("environment_count", &self.environment.len())
            .field("stdin_len", &self.stdin.len())
            .field("auth", &self.auth)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RuntimeAuthKind(String);

impl RuntimeAuthKind {
    pub fn new(kind: impl Into<String>) -> Result<Self, String> {
        let kind = kind.into().trim().to_string();
        if kind.is_empty() {
            return Err("runtime auth kind is required".to_string());
        }
        Ok(Self(kind))
    }

    pub fn from_static(kind: &'static str) -> Self {
        Self(kind.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RuntimeAuthKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeAuthContext {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    home_overrides: BTreeMap<String, PathBuf>,
}

impl RuntimeAuthContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_home_override(mut self, kind: impl AsRef<str>, path: impl Into<PathBuf>) -> Self {
        self.insert_home_override(kind, path);
        self
    }

    pub fn insert_home_override(&mut self, kind: impl AsRef<str>, path: impl Into<PathBuf>) {
        let kind = kind.as_ref().trim();
        if !kind.is_empty() {
            self.home_overrides.insert(kind.to_string(), path.into());
        }
    }

    pub fn home_override(&self, kind: &str) -> Option<&Path> {
        self.home_overrides.get(kind).map(PathBuf::as_path)
    }

    pub fn home_overrides(&self) -> &BTreeMap<String, PathBuf> {
        &self.home_overrides
    }

    pub fn is_empty(&self) -> bool {
        self.home_overrides.is_empty()
    }
}

pub struct RuntimeAuthPreparation<'a> {
    pub runtime_id: &'a str,
    pub network_mode: NetworkMode,
    pub runtime_home_root: Option<&'a Path>,
    pub host_context: &'a RuntimeAuthContext,
}

#[async_trait]
pub trait RuntimeAuthProvider: Send + Sync {
    fn kind(&self) -> &'static str;

    async fn validate(&self, context: &RuntimeAuthContext) -> Result<()>;

    async fn prepare(&self, input: RuntimeAuthPreparation<'_>) -> Result<Vec<(String, String)>>;

    fn host_home_override_env(&self) -> Option<&'static str> {
        None
    }

    fn identity(&self, _context: &RuntimeAuthContext) -> Result<Option<String>> {
        Ok(None)
    }

    fn guidance(&self) -> Option<&'static str> {
        None
    }
}

#[derive(Clone, Default)]
pub struct RuntimeAuthRegistry {
    providers: Arc<BTreeMap<String, Arc<dyn RuntimeAuthProvider>>>,
}

impl RuntimeAuthRegistry {
    pub fn new(providers: impl IntoIterator<Item = Arc<dyn RuntimeAuthProvider>>) -> Self {
        let providers = providers
            .into_iter()
            .map(|provider| (provider.kind().to_string(), provider))
            .collect();
        Self {
            providers: Arc::new(providers),
        }
    }

    pub fn empty() -> Self {
        Self::default()
    }

    pub fn get(&self, auth: &RuntimeAuthKind) -> Option<Arc<dyn RuntimeAuthProvider>> {
        self.get_kind(auth.as_str())
    }

    pub fn get_kind(&self, kind: &str) -> Option<Arc<dyn RuntimeAuthProvider>> {
        self.providers.get(kind).cloned()
    }

    pub fn providers(&self) -> impl Iterator<Item = Arc<dyn RuntimeAuthProvider>> + '_ {
        self.providers.values().cloned()
    }

    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }
}

impl fmt::Debug for RuntimeAuthRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeAuthRegistry")
            .field("providers", &self.providers.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[derive(Debug, Clone, Default)]
pub struct ExecutionOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: Option<i32>,
    pub exit_signal: Option<i32>,
}

impl ExecutionOutput {
    pub fn success(&self) -> bool {
        self.exit_code == Some(0) && self.exit_signal.is_none()
    }

    pub fn status_description(&self) -> String {
        if let Some(code) = self.exit_code {
            return format!("code {code}");
        }
        if let Some(signal) = self.exit_signal {
            return format!("signal {signal}");
        }
        "unknown status".to_string()
    }
}

#[async_trait]
pub trait RuntimeProgramSession: Send {
    async fn write_line(&mut self, line: &str) -> Result<()>;
    async fn read_line(&mut self) -> Result<Option<String>>;
    async fn shutdown(self: Box<Self>) -> Result<ExecutionOutput>;
}

pub type RuntimeProgramStdoutSender = mpsc::UnboundedSender<String>;

#[async_trait]
pub trait RuntimeProgramExecutor: Send {
    async fn execute_streaming(
        &mut self,
        program: RuntimeProgramSpec,
        stdout: RuntimeProgramStdoutSender,
    ) -> Result<ExecutionOutput>;

    async fn execute_captured(&mut self, program: RuntimeProgramSpec) -> Result<ExecutionOutput>;

    async fn spawn(
        &mut self,
        program: RuntimeProgramSpec,
    ) -> Result<Box<dyn RuntimeProgramSession>>;
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
    pub runtime_session_ready: RuntimeSessionReady,
}

#[derive(Debug, Clone)]
pub struct RuntimeSessionHandle {
    pub runtime_session_id: String,
    pub resumes_existing_session: bool,
}

#[derive(Debug, Clone)]
pub struct RuntimeTerminalProgramInput {
    pub session_id: Uuid,
    pub runtime_state_root: PathBuf,
}

pub const RUNTIME_SESSION_READY_MARKER: &str = ".lionclaw-runtime-session";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RuntimeSessionReady {
    marker_present: bool,
}

impl RuntimeSessionReady {
    pub const fn not_ready() -> Self {
        Self {
            marker_present: false,
        }
    }

    pub fn from_runtime_state_root(runtime_state_root: &Path) -> Result<Self> {
        Ok(Self {
            marker_present: runtime_session_ready_marker_exists(runtime_state_root)?,
        })
    }

    pub const fn is_ready(self) -> bool {
        self.marker_present
    }
}

pub fn load_ready_state_value(
    runtime_state_root: &Path,
    file_name: &str,
    label: &str,
    runtime_session_ready: RuntimeSessionReady,
) -> Result<Option<String>> {
    if !runtime_session_ready.is_ready() {
        return Ok(None);
    }
    load_state_value(runtime_state_root, file_name, label)
}

pub fn runtime_session_ready_marker_exists(runtime_state_root: &Path) -> Result<bool> {
    let marker_path = runtime_state_root.join(RUNTIME_SESSION_READY_MARKER);
    let Some(root) = open_existing_state_root(runtime_state_root)? else {
        return Ok(false);
    };
    let marker = match openat(
        &root,
        RUNTIME_SESSION_READY_MARKER,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    ) {
        Ok(marker) => File::from(marker),
        Err(Errno::NOENT) => return Ok(false),
        Err(Errno::LOOP) => {
            return Err(anyhow!(
                "runtime session marker '{}' cannot be a symlink",
                marker_path.display()
            ))
        }
        Err(err) => {
            return Err(err).with_context(|| format!("failed to open {}", marker_path.display()))
        }
    };
    let metadata = marker
        .metadata()
        .with_context(|| format!("failed to stat {}", marker_path.display()))?;
    Ok(metadata.is_file())
}

pub fn load_state_value(
    runtime_state_root: &Path,
    file_name: &str,
    label: &str,
) -> Result<Option<String>> {
    let target_name = state_file_name(file_name)?;
    let Some(root) = open_existing_state_root(runtime_state_root)? else {
        return Ok(None);
    };
    let file = match openat(
        &root,
        &target_name,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    ) {
        Ok(file) => file,
        Err(Errno::NOENT) => return Ok(None),
        Err(Errno::LOOP) => {
            return Err(anyhow!(
                "{label} state file '{}' cannot be a symlink",
                runtime_state_root.join(file_name).display()
            ))
        }
        Err(err) => {
            return Err(anyhow!(
                "failed to open {label} state file '{}' in '{}': {err}",
                file_name,
                runtime_state_root.display()
            ))
        }
    };

    let mut file = File::from(file);
    if !file
        .metadata()
        .with_context(|| {
            format!(
                "failed to stat {label} state file '{}' in '{}'",
                file_name,
                runtime_state_root.display()
            )
        })?
        .is_file()
    {
        return Ok(None);
    }

    let mut contents = String::new();
    file.read_to_string(&mut contents).with_context(|| {
        format!(
            "failed to read {label} state file '{}' in '{}'",
            file_name,
            runtime_state_root.display()
        )
    })?;
    normalize_state_value(contents, label)
}

pub fn save_state_value(
    runtime_state_root: &Path,
    file_name: &str,
    value: &str,
    label: &str,
) -> Result<()> {
    let target_name = state_file_name(file_name)?;
    let value = match normalize_state_value(value, label)? {
        Some(value) => value,
        None => return Ok(()),
    };
    let root = open_state_root(runtime_state_root)?;
    validate_existing_state_file(&root, runtime_state_root, file_name, &target_name, label)?;

    let mut contents = value.into_bytes();
    contents.push(b'\n');
    write_file_atomically(
        &root,
        runtime_state_root,
        &target_name,
        &contents,
        0o600,
        None,
        &format!("{label} state file"),
    )
}

pub fn clear_state_value(runtime_state_root: &Path, file_name: &str, label: &str) -> Result<()> {
    let target_name = state_file_name(file_name)?;
    let Some(root) = open_existing_state_root(runtime_state_root)? else {
        return Ok(());
    };
    validate_existing_state_file(&root, runtime_state_root, file_name, &target_name, label)?;
    match unlinkat(&root, &target_name, AtFlags::empty()) {
        Ok(()) | Err(Errno::NOENT) => Ok(()),
        Err(err) => Err(anyhow!(
            "failed to remove {label} state file '{}' in '{}': {err}",
            file_name,
            runtime_state_root.display()
        )),
    }
}

fn validate_existing_state_file(
    root: &File,
    runtime_state_root: &Path,
    file_name: &str,
    target_name: &OsString,
    label: &str,
) -> Result<()> {
    let file = match openat(
        root,
        target_name,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    ) {
        Ok(file) => file,
        Err(Errno::NOENT) => return Ok(()),
        Err(Errno::LOOP) => {
            return Err(anyhow!(
                "{label} state file '{}' cannot be a symlink",
                runtime_state_root.join(file_name).display()
            ))
        }
        Err(err) => {
            return Err(anyhow!(
                "failed to open existing {label} state file '{}' in '{}': {err}",
                file_name,
                runtime_state_root.display()
            ))
        }
    };
    let file = File::from(file);
    if !file
        .metadata()
        .with_context(|| {
            format!(
                "failed to stat existing {label} state file '{}' in '{}'",
                file_name,
                runtime_state_root.display()
            )
        })?
        .is_file()
    {
        return Err(anyhow!(
            "{label} state path '{}' must be a regular file",
            runtime_state_root.join(file_name).display()
        ));
    }
    Ok(())
}

fn open_state_root(runtime_state_root: &Path) -> Result<File> {
    let root = match open(
        runtime_state_root,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    ) {
        Ok(root) => root,
        Err(Errno::LOOP | Errno::NOTDIR) => {
            return Err(anyhow!(
                "runtime state root '{}' must be a directory and cannot be a symlink",
                runtime_state_root.display()
            ))
        }
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to open '{}'", runtime_state_root.display()))
        }
    };
    Ok(File::from(root))
}

fn open_existing_state_root(runtime_state_root: &Path) -> Result<Option<File>> {
    match open(
        runtime_state_root,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    ) {
        Ok(root) => Ok(Some(File::from(root))),
        Err(Errno::NOENT) => Ok(None),
        Err(Errno::LOOP | Errno::NOTDIR) => Err(anyhow!(
            "runtime state root '{}' must be a directory and cannot be a symlink",
            runtime_state_root.display()
        )),
        Err(err) => Err(anyhow!(
            "failed to open runtime state root '{}': {err}",
            runtime_state_root.display()
        )),
    }
}

fn state_file_name(file_name: &str) -> Result<OsString> {
    let path = Path::new(file_name);
    let mut components = path.components();
    let Some(Component::Normal(name)) = components.next() else {
        return Err(anyhow!("runtime state file name '{file_name}' is invalid"));
    };
    if components.next().is_some() {
        return Err(anyhow!("runtime state file name '{file_name}' is invalid"));
    }
    Ok(OsString::from(name))
}

fn normalize_state_value(value: impl AsRef<str>, label: &str) -> Result<Option<String>> {
    let value = value.as_ref().trim();
    if value.is_empty() {
        return Ok(None);
    }
    if value.contains(['\n', '\r']) {
        return Err(anyhow!("{label} state value must be a single line"));
    }
    Ok(Some(value.to_string()))
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

pub struct RuntimeControlExecution {
    pub input: RuntimeControlInput,
    pub context: RuntimeExecutionContext,
    pub executor: Box<dyn RuntimeProgramExecutor>,
}

pub struct RuntimeProgramTurnExecution {
    pub input: RuntimeTurnInput,
    pub context: RuntimeExecutionContext,
    pub executor: Box<dyn RuntimeProgramExecutor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeMcpServerSpec {
    pub name: String,
    pub command: String,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeExecutionContext {
    pub network_mode: NetworkMode,
    /// Runtime-visible current working directory for program-backed turns.
    pub working_dir: Option<String>,
    pub environment: Vec<(String, String)>,
    pub runtime_state_root: Option<PathBuf>,
    pub runtime_path_projections: Vec<RuntimePathProjection>,
    pub mcp_servers: Vec<RuntimeMcpServerSpec>,
}

impl RuntimeExecutionContext {
    pub fn host_path_for_runtime_path(&self, runtime_path: impl AsRef<Path>) -> Option<PathBuf> {
        let runtime_path = runtime_path.as_ref();
        let mut best_match = None;
        let mut best_component_count = 0;

        for projection in &self.runtime_path_projections {
            let Some((component_count, resolution)) = projection.resolve_host_path(runtime_path)
            else {
                continue;
            };
            if component_count < best_component_count {
                continue;
            }
            if component_count == best_component_count
                && matches!(&best_match, Some(RuntimePathProjectionResolution::Blocked))
                && matches!(&resolution, RuntimePathProjectionResolution::Resolved(_))
            {
                continue;
            }
            best_match = Some(resolution);
            best_component_count = component_count;
        }

        match best_match {
            Some(RuntimePathProjectionResolution::Resolved(host_path)) => return Some(host_path),
            Some(RuntimePathProjectionResolution::Blocked) => return None,
            None => {}
        }

        let runtime_state_root = self.runtime_state_root.as_ref()?;
        if runtime_path == Path::new("/runtime") {
            return Some(runtime_state_root.clone());
        }
        let relative_path = runtime_path.strip_prefix("/runtime").ok()?;
        Some(runtime_state_root.join(safe_relative_path(relative_path)?))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimePathProjectionKind {
    /// The runtime path maps a directory tree onto a host directory tree.
    Directory,
    /// The runtime path maps one exact runtime path onto one host path.
    Exact,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePathProjection {
    runtime_path: String,
    host_path: PathBuf,
    kind: RuntimePathProjectionKind,
}

impl RuntimePathProjection {
    pub fn directory(
        runtime_path: impl Into<String>,
        host_path: impl Into<PathBuf>,
    ) -> Result<Self> {
        Self::new(
            runtime_path,
            host_path,
            RuntimePathProjectionKind::Directory,
        )
    }

    pub fn exact(runtime_path: impl Into<String>, host_path: impl Into<PathBuf>) -> Result<Self> {
        Self::new(runtime_path, host_path, RuntimePathProjectionKind::Exact)
    }

    fn new(
        runtime_path: impl Into<String>,
        host_path: impl Into<PathBuf>,
        kind: RuntimePathProjectionKind,
    ) -> Result<Self> {
        let runtime_path = normalize_absolute_runtime_path(runtime_path.into())?;
        let host_path = host_path.into();
        if !host_path.is_absolute() {
            return Err(anyhow!(
                "runtime path projection host path '{}' must be absolute",
                host_path.display()
            ));
        }
        Ok(Self {
            runtime_path,
            host_path,
            kind,
        })
    }

    fn resolve_host_path(
        &self,
        runtime_path: &Path,
    ) -> Option<(usize, RuntimePathProjectionResolution)> {
        let projected_runtime_path = Path::new(&self.runtime_path);
        let Ok(relative_path) = runtime_path.strip_prefix(projected_runtime_path) else {
            return None;
        };
        let component_count = projected_runtime_path.components().count();
        if relative_path.as_os_str().is_empty() {
            return Some((
                component_count,
                RuntimePathProjectionResolution::Resolved(self.host_path.clone()),
            ));
        }

        match self.kind {
            RuntimePathProjectionKind::Directory => {
                let Some(relative_path) = safe_relative_path(relative_path) else {
                    return Some((component_count, RuntimePathProjectionResolution::Blocked));
                };
                Some((
                    component_count,
                    RuntimePathProjectionResolution::Resolved(self.host_path.join(relative_path)),
                ))
            }
            RuntimePathProjectionKind::Exact => {
                Some((component_count, RuntimePathProjectionResolution::Blocked))
            }
        }
    }
}

fn normalize_absolute_runtime_path(runtime_path: String) -> Result<String> {
    let path = Path::new(&runtime_path);
    if !path.is_absolute() {
        return Err(anyhow!(
            "runtime path projection '{runtime_path}' must be absolute"
        ));
    }

    let mut normalized = PathBuf::from("/");
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(segment) => normalized.push(segment),
            Component::CurDir | Component::ParentDir | Component::Prefix(_) => {
                return Err(anyhow!(
                    "runtime path projection '{runtime_path}' must be normalized"
                ));
            }
        }
    }

    normalized
        .to_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("runtime path projection '{runtime_path}' is not valid UTF-8"))
}

enum RuntimePathProjectionResolution {
    Resolved(PathBuf),
    Blocked,
}

/// Normalize a relative path while rejecting absolute paths and parent traversal.
pub fn safe_relative_path(path: impl AsRef<Path>) -> Option<PathBuf> {
    let path = path.as_ref();
    let mut safe_path = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(segment) => safe_path.push(segment),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    Some(safe_path)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeArtifact {
    pub artifact_id: String,
    pub path: PathBuf,
    pub filename: Option<String>,
    pub mime_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeNativeHomeArtifactDir {
    relative_path: PathBuf,
}

impl RuntimeNativeHomeArtifactDir {
    pub fn new(relative_path: impl AsRef<Path>) -> Result<Self> {
        let raw_path = relative_path.as_ref();
        let relative_path = safe_relative_path(raw_path).ok_or_else(|| {
            anyhow!(
                "native-home artifact directory '{}' must be relative and stay within the native home",
                raw_path.display()
            )
        })?;
        if relative_path.as_os_str().is_empty() {
            return Err(anyhow!(
                "native-home artifact directory must not be the native home root"
            ));
        }
        Ok(Self { relative_path })
    }

    pub fn relative_path(&self) -> &Path {
        &self.relative_path
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeFileChangeStatus {
    Editing,
    Edited,
    Failed,
    Declined,
    Changed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeFileChange {
    pub runtime: String,
    pub operation_id: Option<String>,
    pub status: RuntimeFileChangeStatus,
    pub paths: Vec<String>,
    pub total_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    FileChange {
        change: RuntimeFileChange,
    },
    Done,
    Error {
        code: Option<String>,
        text: String,
    },
}

/// Raw, driver-specific payload retained alongside a canonical event for
/// debugging. Retention is debug-only: it is never parsed back into canonical
/// text or replayed into a prompt. Only the paired [`RuntimeEvent`] is canonical.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawTurnPayload {
    /// Driver/protocol that produced the payload, e.g. `"driver-protocol"`.
    pub driver: String,
    /// The payload exactly as the driver emitted it (e.g. one JSON-RPC line).
    pub payload: String,
}

/// One record in a runtime turn's canonical journal.
///
/// A protocol driver translates each harness message into journal records.
/// `event` is the canonical, public output LionClaw persists, replays, and
/// shows operators. `raw`, when present, retains the originating driver payload
/// for debugging only and is excluded from every canonical projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnEvent {
    pub event: RuntimeEvent,
    pub raw: Option<RawTurnPayload>,
}

impl TurnEvent {
    /// A record for a kernel-synthesized event with no retained raw payload.
    pub fn canonical(event: RuntimeEvent) -> Self {
        Self { event, raw: None }
    }

    /// A record retaining the raw driver payload the event was derived from.
    pub fn with_raw(event: RuntimeEvent, raw: RawTurnPayload) -> Self {
        Self {
            event,
            raw: Some(raw),
        }
    }
}

/// Project a canonical journal to its public [`RuntimeEvent`] stream, dropping
/// every retained raw payload. This is the only sanctioned way to derive
/// operator-visible output from a journal: raw retention never contributes.
pub fn canonical_events(journal: &[TurnEvent]) -> impl Iterator<Item = &RuntimeEvent> {
    journal.iter().map(|record| &record.event)
}

pub type RuntimeTurnJournalSender = mpsc::UnboundedSender<TurnEvent>;
pub type RuntimeEventSender = mpsc::UnboundedSender<RuntimeEvent>;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct RuntimeTerminalConfig {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
}

impl RuntimeTerminalConfig {
    pub fn is_empty(&self) -> bool {
        self.args.is_empty()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeDriverConfig {
    pub runtime_id: String,
    pub executable: String,
    pub args: Vec<String>,
    pub environment: Vec<(String, String)>,
    pub model: Option<String>,
    pub mode: Option<String>,
    pub auth: Option<RuntimeAuthKind>,
    pub terminal: RuntimeTerminalConfig,
}

pub trait RuntimeDriverProvider: Send + Sync {
    fn driver(&self) -> &'static str;

    fn validate_config(&self, _config: &RuntimeDriverConfig) -> Result<()> {
        Ok(())
    }

    fn create_adapter(&self, config: RuntimeDriverConfig) -> Arc<dyn RuntimeAdapter>;

    fn auth_provider(&self) -> Option<Arc<dyn RuntimeAuthProvider>> {
        None
    }
}

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
    fn native_home_artifact_dirs(&self) -> Result<Vec<RuntimeNativeHomeArtifactDir>> {
        Ok(Vec::new())
    }
    async fn session_start(&self, input: RuntimeSessionStartInput) -> Result<RuntimeSessionHandle>;
    async fn turn(
        &self,
        _input: RuntimeTurnInput,
        _journal: RuntimeTurnJournalSender,
    ) -> Result<RuntimeTurnResult> {
        Err(anyhow!("runtime does not implement direct turns"))
    }
    fn build_turn_program(
        &self,
        _input: &RuntimeTurnInput,
        _context: &RuntimeExecutionContext,
    ) -> Result<RuntimeProgramSpec> {
        Err(anyhow!("runtime does not support program-backed turns"))
    }
    fn build_terminal_program(
        &self,
        _input: RuntimeTerminalProgramInput,
    ) -> Result<RuntimeProgramSpec> {
        Err(anyhow!("runtime does not expose a native terminal UI"))
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
        if let Some(text) = observed_error_text.filter(|text| !text.trim().is_empty()) {
            format!(
                "runtime process exited with {}: {text}",
                output.status_description()
            )
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            if stderr.is_empty() {
                format!(
                    "runtime process exited with {}",
                    output.status_description()
                )
            } else {
                format!(
                    "runtime process exited with {}: {stderr}",
                    output.status_description()
                )
            }
        }
    }
    fn prepare_program_retry_after_failure(
        &self,
        _input: &RuntimeTurnInput,
        _output: &ExecutionOutput,
        _observed_error_text: Option<&str>,
        _journal: &RuntimeTurnJournalSender,
    ) -> Result<bool> {
        Ok(false)
    }
    async fn program_backed_turn(
        &self,
        execution: RuntimeProgramTurnExecution,
        journal: RuntimeTurnJournalSender,
    ) -> Result<RuntimeTurnResult> {
        execute_program_backed_turn(self, execution, journal).await
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
    adapters: Arc<RwLock<BTreeMap<String, Arc<dyn RuntimeAdapter>>>>,
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
    execution: RuntimeProgramTurnExecution,
    journal: RuntimeTurnJournalSender,
) -> Result<RuntimeTurnResult>
where
    A: RuntimeAdapter + Send + Sync + ?Sized,
{
    let RuntimeProgramTurnExecution {
        input,
        context,
        mut executor,
    } = execution;

    execute_program_backed_turn_with_executor(adapter, executor.as_mut(), input, &context, journal)
        .await
}

async fn execute_program_backed_turn_with_executor<A>(
    adapter: &A,
    executor: &mut dyn RuntimeProgramExecutor,
    input: RuntimeTurnInput,
    context: &RuntimeExecutionContext,
    journal: RuntimeTurnJournalSender,
) -> Result<RuntimeTurnResult>
where
    A: RuntimeAdapter + Send + Sync + ?Sized,
{
    let mut attempted_retry = false;
    let mut current_input = input;

    loop {
        let attempt =
            run_program_backed_attempt(adapter, executor, &current_input, context, &journal)
                .await?;

        if attempt.output.success() {
            flush_buffered_program_output_events(&journal, attempt.buffered_errors);
            return finish_program_backed_turn(
                adapter,
                attempt.output,
                attempt.last_error_text.as_deref(),
                attempt.saw_done,
                &journal,
            );
        }

        if !attempted_retry
            && adapter.prepare_program_retry_after_failure(
                &current_input,
                &attempt.output,
                attempt.last_error_text.as_deref(),
                &journal,
            )?
        {
            attempted_retry = true;
            if let Some(fresh_prompt) = current_input.fresh_prompt.take() {
                current_input.prompt = fresh_prompt;
            }
            continue;
        }

        flush_buffered_program_output_events(&journal, attempt.buffered_errors);
        return finish_program_backed_turn(
            adapter,
            attempt.output,
            attempt.last_error_text.as_deref(),
            attempt.saw_done,
            &journal,
        );
    }
}

struct ProgramBackedAttemptOutcome {
    buffered_errors: Option<Vec<RuntimeEvent>>,
    output: ExecutionOutput,
    saw_done: bool,
    last_error_text: Option<String>,
}

async fn run_program_backed_attempt<A>(
    adapter: &A,
    executor: &mut dyn RuntimeProgramExecutor,
    input: &RuntimeTurnInput,
    context: &RuntimeExecutionContext,
    journal: &RuntimeTurnJournalSender,
) -> Result<ProgramBackedAttemptOutcome>
where
    A: RuntimeAdapter + Send + Sync + ?Sized,
{
    let program = adapter.build_turn_program(input, context)?;
    let (stdout_tx, mut stdout_rx) = mpsc::unbounded_channel();
    let execution = executor.execute_streaming(program, stdout_tx);
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
                        journal,
                        &mut buffered_errors,
                        &line,
                        &mut saw_done,
                        &mut last_error_text,
                    ),
                    None => {
                        finish_program_output_parser(
                            &mut output_parser,
                            journal,
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
                        journal,
                        &mut buffered_errors,
                        &line,
                        &mut saw_done,
                        &mut last_error_text,
                    );
                }
                finish_program_output_parser(
                    &mut output_parser,
                    journal,
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
    journal: &RuntimeTurnJournalSender,
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
        journal,
        buffered_errors,
        parsed_events,
        saw_done,
        last_error_text,
    );
}

fn finish_program_output_parser(
    output_parser: &mut Option<Box<dyn RuntimeProgramOutputParser>>,
    journal: &RuntimeTurnJournalSender,
    buffered_errors: &mut Option<Vec<RuntimeEvent>>,
    saw_done: &mut bool,
    last_error_text: &mut Option<String>,
) {
    if let Some(parser) = output_parser.as_mut() {
        observe_program_output_events(
            journal,
            buffered_errors,
            parser.finish(),
            saw_done,
            last_error_text,
        );
    }
}

fn observe_program_output_events(
    journal: &RuntimeTurnJournalSender,
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
                drop(journal.send(TurnEvent::canonical(event)));
            }
        } else {
            drop(journal.send(TurnEvent::canonical(event)));
        }
    }
}

fn flush_buffered_program_output_events(
    journal: &RuntimeTurnJournalSender,
    buffered_errors: Option<Vec<RuntimeEvent>>,
) {
    if let Some(buffered_errors) = buffered_errors {
        for event in buffered_errors {
            drop(journal.send(TurnEvent::canonical(event)));
        }
    }
}

fn finish_program_backed_turn<A>(
    adapter: &A,
    output: ExecutionOutput,
    observed_error_text: Option<&str>,
    saw_done: bool,
    journal: &RuntimeTurnJournalSender,
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
        drop(journal.send(TurnEvent::canonical(RuntimeEvent::Done)));
    }

    Ok(RuntimeTurnResult {
        capability_requests: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        path::PathBuf,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use super::{
        canonical_events, clear_state_value, execute_program_backed_turn, load_ready_state_value,
        safe_relative_path, ExecutionOutput, NetworkMode, RawTurnPayload, RuntimeAdapter,
        RuntimeAdapterInfo, RuntimeCapabilityResult, RuntimeEvent, RuntimeEventSender,
        RuntimeExecutionContext, RuntimeMessageLane, RuntimeNativeHomeArtifactDir,
        RuntimePathProjection, RuntimeProgramExecutor, RuntimeProgramSession, RuntimeProgramSpec,
        RuntimeProgramTurnExecution, RuntimeRegistry, RuntimeSessionHandle, RuntimeSessionReady,
        RuntimeSessionStartInput, RuntimeTerminalConfig, RuntimeTurnInput,
        RuntimeTurnJournalSender, RuntimeTurnMode, TurnEvent, RUNTIME_SESSION_READY_MARKER,
    };
    use anyhow::{anyhow, Result};
    use async_trait::async_trait;
    use tokio::{
        sync::mpsc,
        time::{sleep, timeout},
    };

    #[test]
    fn canonical_events_drops_retained_raw_payloads() {
        let journal = vec![
            TurnEvent::with_raw(
                RuntimeEvent::MessageDelta {
                    lane: RuntimeMessageLane::Answer,
                    text: "hi".to_string(),
                },
                RawTurnPayload {
                    driver: "test-driver".to_string(),
                    payload: "{\"method\":\"message/delta\"}".to_string(),
                },
            ),
            TurnEvent::canonical(RuntimeEvent::Done),
        ];

        let events: Vec<RuntimeEvent> = canonical_events(&journal).cloned().collect();

        assert_eq!(
            events,
            vec![
                RuntimeEvent::MessageDelta {
                    lane: RuntimeMessageLane::Answer,
                    text: "hi".to_string(),
                },
                RuntimeEvent::Done,
            ],
        );
    }

    #[test]
    fn runtime_terminal_config_rejects_removed_resume_args() {
        let err = serde_json::from_value::<RuntimeTerminalConfig>(serde_json::json!({
            "resume-args": ["--session", "{session_id}"]
        }))
        .expect_err("removed terminal resume args should not be ignored");

        assert!(
            err.to_string().contains("unknown field"),
            "unexpected error: {err}"
        );
    }

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

        fn build_turn_program(
            &self,
            input: &RuntimeTurnInput,
            context: &RuntimeExecutionContext,
        ) -> Result<RuntimeProgramSpec> {
            if context.network_mode != NetworkMode::On {
                return Err(anyhow!("unexpected runtime execution context"));
            }
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
            if let Some(rest) = line.strip_prefix("answer:") {
                vec![RuntimeEvent::MessageDelta {
                    lane: RuntimeMessageLane::Answer,
                    text: rest.to_string(),
                }]
            } else if let Some(rest) = line.strip_prefix("error:") {
                vec![RuntimeEvent::Error {
                    code: Some("test.error".to_string()),
                    text: rest.to_string(),
                }]
            } else if line == "done" {
                vec![RuntimeEvent::Done]
            } else {
                Vec::new()
            }
        }

        fn prepare_program_retry_after_failure(
            &self,
            _input: &RuntimeTurnInput,
            _output: &ExecutionOutput,
            _observed_error_text: Option<&str>,
            _journal: &RuntimeTurnJournalSender,
        ) -> Result<bool> {
            Ok(!self
                .retry_used
                .swap(true, std::sync::atomic::Ordering::SeqCst))
        }

        async fn resolve_capability_requests(
            &self,
            _handle: &RuntimeSessionHandle,
            _results: Vec<RuntimeCapabilityResult>,
            _events: RuntimeEventSender,
        ) -> Result<()> {
            Ok(())
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

    struct StubExecutor {
        attempts: Arc<Mutex<VecDeque<StubAttempt>>>,
    }

    impl StubExecutor {
        fn new(attempts: Vec<StubAttempt>) -> Self {
            Self {
                attempts: Arc::new(Mutex::new(VecDeque::from(attempts))),
            }
        }
    }

    #[async_trait]
    impl RuntimeProgramExecutor for StubExecutor {
        async fn execute_streaming(
            &mut self,
            _program: RuntimeProgramSpec,
            stdout: mpsc::UnboundedSender<String>,
        ) -> Result<ExecutionOutput> {
            let attempt = self
                .attempts
                .lock()
                .map_err(|_| anyhow!("attempt lock poisoned"))?
                .pop_front()
                .ok_or_else(|| anyhow!("no attempt configured"))?;
            for line in attempt.stdout_lines {
                stdout.send(line).map_err(|_| anyhow!("stdout closed"))?;
            }
            Ok(attempt.output)
        }

        async fn execute_captured(
            &mut self,
            _program: RuntimeProgramSpec,
        ) -> Result<ExecutionOutput> {
            Err(anyhow!("captured execution is not used by this test"))
        }

        async fn spawn(
            &mut self,
            _program: RuntimeProgramSpec,
        ) -> Result<Box<dyn RuntimeProgramSession>> {
            Err(anyhow!("interactive execution is not used by this test"))
        }
    }

    fn success_output() -> ExecutionOutput {
        ExecutionOutput {
            exit_code: Some(0),
            ..ExecutionOutput::default()
        }
    }

    fn failed_output() -> ExecutionOutput {
        ExecutionOutput {
            exit_code: Some(1),
            stderr: b"failed".to_vec(),
            ..ExecutionOutput::default()
        }
    }

    fn turn_input(fresh_prompt: Option<String>) -> RuntimeTurnInput {
        RuntimeTurnInput {
            runtime_session_id: "session".to_string(),
            prompt: "hello".to_string(),
            fresh_prompt,
            runtime_skill_ids: Vec::new(),
        }
    }

    fn execution_context() -> RuntimeExecutionContext {
        RuntimeExecutionContext {
            network_mode: NetworkMode::On,
            working_dir: None,
            environment: Vec::new(),
            runtime_state_root: None,
            runtime_path_projections: Vec::new(),
            mcp_servers: Vec::new(),
        }
    }

    #[tokio::test]
    async fn runtime_registry_lists_ids_deterministically() {
        let registry = RuntimeRegistry::new();
        registry
            .register("z-runtime", Arc::new(TestProgramAdapter::default()))
            .await;
        registry
            .register("a-runtime", Arc::new(TestProgramAdapter::default()))
            .await;

        assert_eq!(
            registry.list().await,
            vec!["a-runtime".to_string(), "z-runtime".to_string()]
        );
    }

    #[test]
    fn runtime_execution_context_resolves_longest_runtime_path_projection() {
        let context = RuntimeExecutionContext {
            network_mode: NetworkMode::On,
            working_dir: None,
            environment: Vec::new(),
            runtime_state_root: Some(PathBuf::from("/host/runtime-root")),
            runtime_path_projections: vec![
                RuntimePathProjection::directory("/runtime", "/host/runtime-root")
                    .expect("valid runtime projection"),
                RuntimePathProjection::exact(
                    "/runtime/lionclaw/channel-send.sock",
                    "/tmp/lionclaw/cs.sock",
                )
                .expect("valid runtime projection"),
            ],
            mcp_servers: Vec::new(),
        };

        assert_eq!(
            context.host_path_for_runtime_path("/runtime/artifacts/sketch.txt"),
            Some(PathBuf::from("/host/runtime-root/artifacts/sketch.txt"))
        );
        assert_eq!(
            context.host_path_for_runtime_path("/runtime/lionclaw/channel-send.sock"),
            Some(PathBuf::from("/tmp/lionclaw/cs.sock"))
        );
        assert_eq!(
            context.host_path_for_runtime_path("/runtime/lionclaw/channel-send.sock/file"),
            None
        );
        assert_eq!(context.host_path_for_runtime_path("/runtime2/file"), None);
        assert_eq!(
            context.host_path_for_runtime_path("/runtime/./artifacts/sketch.txt"),
            Some(PathBuf::from("/host/runtime-root/artifacts/sketch.txt"))
        );
        assert_eq!(
            context.host_path_for_runtime_path("/runtime/../outside"),
            None
        );
        assert_eq!(
            context.host_path_for_runtime_path("/runtime/lionclaw/channel-send.sock/../outside"),
            None
        );
    }

    #[test]
    fn runtime_execution_context_keeps_equal_depth_blocks_order_independent() {
        let context = RuntimeExecutionContext {
            network_mode: NetworkMode::On,
            working_dir: None,
            environment: Vec::new(),
            runtime_state_root: None,
            runtime_path_projections: vec![
                RuntimePathProjection::exact("/runtime/channel.sock", "/tmp/channel.sock")
                    .expect("valid runtime projection"),
                RuntimePathProjection::directory("/runtime/channel.sock", "/tmp/channel-tree")
                    .expect("valid runtime projection"),
            ],
            mcp_servers: Vec::new(),
        };

        assert_eq!(
            context.host_path_for_runtime_path("/runtime/channel.sock/file"),
            None
        );
    }

    #[test]
    fn runtime_session_ready_gates_ready_state_loading() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path();
        std::fs::write(runtime_state_root.join("session-id"), "session_saved\n")
            .expect("write state file");

        assert_eq!(
            load_ready_state_value(
                runtime_state_root,
                "session-id",
                "test session",
                RuntimeSessionReady::not_ready()
            )
            .expect("load state"),
            None
        );

        let missing_marker_ready = RuntimeSessionReady::from_runtime_state_root(runtime_state_root)
            .expect("missing marker is not an error");
        assert!(!missing_marker_ready.is_ready());
        assert_eq!(
            load_ready_state_value(
                runtime_state_root,
                "session-id",
                "test session",
                missing_marker_ready
            )
            .expect("load state"),
            None
        );

        std::fs::write(
            runtime_state_root.join(RUNTIME_SESSION_READY_MARKER),
            "ready\n",
        )
        .expect("write ready marker");
        let runtime_session_ready =
            RuntimeSessionReady::from_runtime_state_root(runtime_state_root)
                .expect("ready marker should load");
        assert!(runtime_session_ready.is_ready());
        assert_eq!(
            load_ready_state_value(
                runtime_state_root,
                "session-id",
                "test session",
                runtime_session_ready
            )
            .expect("load state"),
            Some("session_saved".to_string())
        );
    }

    #[test]
    fn clear_state_value_removes_regular_state_file() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path();
        let state_file = runtime_state_root.join("session-id");
        std::fs::write(&state_file, "session_saved\n").expect("write state file");

        clear_state_value(runtime_state_root, "session-id", "test session").expect("clear state");

        assert!(
            !state_file.exists(),
            "state file should be removed after clear"
        );
        clear_state_value(runtime_state_root, "session-id", "test session")
            .expect("clearing a missing state file is idempotent");
    }

    #[cfg(unix)]
    #[test]
    fn clear_state_value_rejects_symlinked_state_file() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let runtime_state_root = temp_dir.path();
        let outside = temp_dir.path().join("outside");
        std::fs::write(&outside, "do not remove\n").expect("write outside file");
        std::os::unix::fs::symlink(&outside, runtime_state_root.join("session-id"))
            .expect("create state symlink");

        let err = clear_state_value(runtime_state_root, "session-id", "test session")
            .expect_err("state symlink should be rejected");

        assert!(err.to_string().contains("cannot be a symlink"));
        assert!(outside.exists(), "clear must not follow state symlinks");
    }

    #[test]
    fn runtime_path_projection_rejects_unvalidated_paths() {
        let relative_runtime_path =
            RuntimePathProjection::directory("runtime", "/host/runtime-root")
                .expect_err("relative runtime path should be rejected");
        assert!(
            relative_runtime_path
                .to_string()
                .contains("must be absolute"),
            "unexpected error: {relative_runtime_path}"
        );

        let parent_runtime_path =
            RuntimePathProjection::directory("/runtime/../outside", "/host/runtime-root")
                .expect_err("parent runtime path should be rejected");
        assert!(
            parent_runtime_path
                .to_string()
                .contains("must be normalized"),
            "unexpected error: {parent_runtime_path}"
        );

        let relative_host_path =
            RuntimePathProjection::exact("/runtime/channel.sock", "channel.sock")
                .expect_err("relative host path should be rejected");
        assert!(
            relative_host_path.to_string().contains("host path"),
            "unexpected error: {relative_host_path}"
        );
    }

    #[test]
    fn safe_relative_path_normalizes_without_parent_traversal() {
        assert_eq!(
            safe_relative_path("artifacts/./sketch.txt"),
            Some(PathBuf::from("artifacts/sketch.txt"))
        );
        assert_eq!(safe_relative_path("."), Some(PathBuf::new()));
        assert_eq!(safe_relative_path("../outside"), None);
        assert_eq!(safe_relative_path("/absolute"), None);
    }

    #[test]
    fn native_home_artifact_dir_accepts_only_non_empty_relative_paths() {
        let dir = RuntimeNativeHomeArtifactDir::new("artifacts/./images")
            .expect("relative artifact dir should be accepted");
        assert_eq!(dir.relative_path(), PathBuf::from("artifacts/images"));

        assert!(RuntimeNativeHomeArtifactDir::new(".").is_err());
        assert!(RuntimeNativeHomeArtifactDir::new("../outside").is_err());
        assert!(RuntimeNativeHomeArtifactDir::new("/runtime/home/images").is_err());
    }

    #[tokio::test]
    async fn program_backed_turn_streams_output_and_finishes() {
        let adapter = TestProgramAdapter::default();
        let executor = StubExecutor::new(vec![StubAttempt {
            stdout_lines: vec!["answer:hello".to_string()],
            output: success_output(),
        }]);
        let (events, mut event_rx) = mpsc::unbounded_channel();

        let result = execute_program_backed_turn(
            &adapter,
            RuntimeProgramTurnExecution {
                input: turn_input(None),
                context: execution_context(),
                executor: Box::new(executor),
            },
            events,
        )
        .await
        .expect("turn should succeed");

        assert!(result.capability_requests.is_empty());
        assert!(matches!(
            event_rx.recv().await,
            Some(TurnEvent { event: RuntimeEvent::MessageDelta { text, .. }, raw: None }) if text == "hello"
        ));
        assert!(matches!(
            event_rx.recv().await,
            Some(TurnEvent {
                event: RuntimeEvent::Done,
                raw: None
            })
        ));
    }

    #[tokio::test]
    async fn program_backed_turn_retries_with_fresh_prompt_before_emitting_error() {
        let adapter = TestProgramAdapter::default();
        let executor = StubExecutor::new(vec![
            StubAttempt {
                stdout_lines: vec!["error:stale".to_string()],
                output: failed_output(),
            },
            StubAttempt {
                stdout_lines: vec!["answer:fresh".to_string()],
                output: success_output(),
            },
        ]);
        let (events, mut event_rx) = mpsc::unbounded_channel();

        execute_program_backed_turn(
            &adapter,
            RuntimeProgramTurnExecution {
                input: turn_input(Some("fresh prompt".to_string())),
                context: execution_context(),
                executor: Box::new(executor),
            },
            events,
        )
        .await
        .expect("turn should retry and succeed");

        assert!(matches!(
            event_rx.recv().await,
            Some(TurnEvent { event: RuntimeEvent::MessageDelta { text, .. }, raw: None }) if text == "fresh"
        ));
        assert!(matches!(
            event_rx.recv().await,
            Some(TurnEvent {
                event: RuntimeEvent::Done,
                raw: None
            })
        ));
        assert!(
            event_rx.try_recv().is_err(),
            "stale error should stay buffered"
        );
    }

    #[tokio::test]
    async fn program_backed_turn_surfaces_failure_after_retry() {
        let adapter = TestProgramAdapter::default();
        let executor = StubExecutor::new(vec![
            StubAttempt {
                stdout_lines: vec!["error:first".to_string()],
                output: failed_output(),
            },
            StubAttempt {
                stdout_lines: vec!["error:second".to_string()],
                output: failed_output(),
            },
        ]);
        let (events, mut event_rx) = mpsc::unbounded_channel();

        let err = execute_program_backed_turn(
            &adapter,
            RuntimeProgramTurnExecution {
                input: turn_input(Some("fresh prompt".to_string())),
                context: execution_context(),
                executor: Box::new(executor),
            },
            events,
        )
        .await
        .expect_err("turn should fail");

        assert!(err.to_string().contains("second"));
        assert!(matches!(
            event_rx.recv().await,
            Some(TurnEvent { event: RuntimeEvent::Error { text, .. }, raw: None }) if text == "second"
        ));
        assert!(
            event_rx.try_recv().is_err(),
            "stale retry error should stay buffered"
        );
    }

    #[tokio::test]
    async fn program_backed_turn_does_not_deadlock_when_process_finishes_before_stdout() {
        let adapter = TestProgramAdapter::default();
        let executor = StubExecutor::new(vec![StubAttempt {
            stdout_lines: Vec::new(),
            output: success_output(),
        }]);
        let (events, _event_rx) = mpsc::unbounded_channel();

        timeout(
            Duration::from_millis(100),
            execute_program_backed_turn(
                &adapter,
                RuntimeProgramTurnExecution {
                    input: turn_input(None),
                    context: execution_context(),
                    executor: Box::new(executor),
                },
                events,
            ),
        )
        .await
        .expect("turn should not hang")
        .expect("turn should succeed");
    }

    #[tokio::test]
    async fn program_backed_turn_observes_slow_stdout_before_completion() {
        struct SlowExecutor;

        #[async_trait]
        impl RuntimeProgramExecutor for SlowExecutor {
            async fn execute_streaming(
                &mut self,
                _program: RuntimeProgramSpec,
                stdout: mpsc::UnboundedSender<String>,
            ) -> Result<ExecutionOutput> {
                sleep(Duration::from_millis(20)).await;
                stdout
                    .send("answer:slow".to_string())
                    .map_err(|_| anyhow!("stdout closed"))?;
                Ok(success_output())
            }

            async fn execute_captured(
                &mut self,
                _program: RuntimeProgramSpec,
            ) -> Result<ExecutionOutput> {
                Err(anyhow!("captured execution is not used by this test"))
            }

            async fn spawn(
                &mut self,
                _program: RuntimeProgramSpec,
            ) -> Result<Box<dyn RuntimeProgramSession>> {
                Err(anyhow!("interactive execution is not used by this test"))
            }
        }

        let adapter = TestProgramAdapter::default();
        let executor = SlowExecutor;
        let (events, mut event_rx) = mpsc::unbounded_channel();

        execute_program_backed_turn(
            &adapter,
            RuntimeProgramTurnExecution {
                input: turn_input(None),
                context: execution_context(),
                executor: Box::new(executor),
            },
            events,
        )
        .await
        .expect("turn should succeed");

        assert!(matches!(
            event_rx.recv().await,
            Some(TurnEvent { event: RuntimeEvent::MessageDelta { text, .. }, raw: None }) if text == "slow"
        ));
    }
}
