use std::{fs, path::Path, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
#[cfg(unix)]
use rustix::process::{getgid, getuid};
#[cfg(unix)]
use std::os::unix::fs::FileTypeExt;
use tokio::{runtime::Handle, time::timeout};
use tracing::warn;

use super::{
    backend::{
        ExecutionBackend, ExecutionOutput, ExecutionRequest, ExecutionSession,
        ExecutionStdoutSender,
    },
    mount_validation::{podman_bind_mount_argument, PodmanBindMountArgumentForm},
    plan::{
        map_host_path_into_runtime_mount, ConfinementBackend, MountAccess, MountSpec, NetworkMode,
        RuntimeAuthKind,
    },
    process::{
        run_process_attached, run_process_streaming, spawn_process_session, ProcessInvocation,
        ProcessSession,
    },
    runtime_auth::prepare_runtime_auth,
    OciConfinementConfig,
};
use crate::kernel::runtime::RuntimeSecretsMount;

#[derive(Debug, Default, Clone, Copy)]
pub struct OciExecutionBackend;

pub struct OciExecutionSession {
    process: ProcessSession,
    runtime_secrets: Option<OciRuntimeSecretsSession>,
}

impl OciExecutionSession {
    pub async fn write_line(&mut self, line: &str) -> Result<()> {
        self.process.write_line(line).await
    }

    pub async fn read_line(&mut self) -> Result<Option<String>> {
        self.process.read_line().await
    }

    pub async fn shutdown(self) -> Result<ExecutionOutput> {
        let Self {
            process,
            runtime_secrets,
        } = self;
        let result = process.wait().await;
        let runtime_secrets_cleanup_result = match runtime_secrets {
            Some(cleanup) => cleanup.shutdown().await,
            None => Ok(()),
        };

        match (result, runtime_secrets_cleanup_result) {
            (Ok(output), Ok(())) => Ok(output),
            (Ok(output), Err(err)) => {
                warn!(
                    error = %err,
                    "runtime secret cleanup failed after successful interactive OCI runtime turn"
                );
                Ok(output)
            }
            (Err(err), _) => Err(err),
        }
    }
}

const OCI_PREFLIGHT_TIMEOUT: Duration = Duration::from_secs(5);

// LionClaw bind-mounts local workspace and runtime state into confined OCI
// containers. On SELinux hosts those mounts are unreadable by default unless
// Podman relabels them for the container context. Session-scoped LionClaw
// control mounts stay private; persistent/shared mounts use shared relabeling
// so concurrent containers cannot steal labels from each other.
const WORKSPACE_MOUNT_TARGET: &str = "/workspace";
const RUNTIME_HOME_MOUNT_TARGET: &str = "/runtime/home";
const DRAFTS_MOUNT_TARGET: &str = "/drafts";
const SKILLS_MOUNT_TARGET_ROOT: &str = "/lionclaw/skills";
const LIONCLAW_METADATA_DIR: &str = ".lionclaw";
const WORKSPACE_LIONCLAW_METADATA_TMPFS: &str = "/workspace/.lionclaw:size=1m,mode=700,notmpcopyup";

#[derive(Debug, Clone)]
struct PreparedOciProcessLaunch {
    engine: String,
    args: Vec<String>,
    network_mode: NetworkMode,
    environment: Vec<(String, String)>,
    image: String,
    program_executable: String,
    program_args: Vec<String>,
    stdin: String,
}

#[async_trait]
impl ExecutionBackend for OciExecutionBackend {
    fn kind(&self) -> ConfinementBackend {
        ConfinementBackend::Oci
    }

    async fn execute_streaming(
        &self,
        request: ExecutionRequest,
        stdout: ExecutionStdoutSender,
    ) -> Result<ExecutionOutput> {
        execute_oci_process(
            request,
            move |line| {
                drop(stdout.send(line.to_string()));
                Ok(())
            },
            "streaming OCI runtime turn",
        )
        .await
    }

    async fn execute_captured(&self, request: ExecutionRequest) -> Result<ExecutionOutput> {
        execute_oci_process(request, |_| Ok(()), "captured OCI runtime command").await
    }

    async fn spawn_interactive(&self, request: ExecutionRequest) -> Result<ExecutionSession> {
        let runtime_secrets = ensure_runtime_secrets_registered(&request).await?;
        let runtime_auth_environment = prepare_runtime_auth(&request).await?;
        let prepared = prepare_oci_process_launch(
            &request,
            runtime_secrets
                .as_ref()
                .map(|secrets| secrets.secret_name.as_str()),
        )?;
        let invocation = build_oci_process_invocation(prepared, &runtime_auth_environment);
        let process = spawn_process_session(&invocation).await?;
        Ok(ExecutionSession::Oci(OciExecutionSession {
            process,
            runtime_secrets,
        }))
    }

    async fn execute_attached(&self, request: ExecutionRequest) -> Result<ExecutionOutput> {
        let runtime_secrets = ensure_runtime_secrets_registered(&request).await?;
        let runtime_auth_environment = prepare_runtime_auth(&request).await?;
        let prepared = prepare_oci_process_launch(
            &request,
            runtime_secrets
                .as_ref()
                .map(|secrets| secrets.secret_name.as_str()),
        )?;
        let invocation = build_oci_attached_process_invocation(prepared, &runtime_auth_environment);
        let result = run_process_attached(&invocation).await;
        let runtime_secrets_cleanup_result = match runtime_secrets {
            Some(cleanup) => cleanup.shutdown().await,
            None => Ok(()),
        };

        match (result, runtime_secrets_cleanup_result) {
            (Ok(output), Ok(())) => Ok(output),
            (Ok(output), Err(err)) => {
                warn!(
                    error = %err,
                    "runtime secret cleanup failed after attached OCI runtime"
                );
                Ok(output)
            }
            (Err(err), _) => Err(err),
        }
    }
}

async fn execute_oci_process<F>(
    request: ExecutionRequest,
    on_stdout_line: F,
    cleanup_context: &'static str,
) -> Result<ExecutionOutput>
where
    F: FnMut(&str) -> Result<()> + Send,
{
    let runtime_secrets = ensure_runtime_secrets_registered(&request).await?;
    let runtime_auth_environment = prepare_runtime_auth(&request).await?;
    let prepared = prepare_oci_process_launch(
        &request,
        runtime_secrets
            .as_ref()
            .map(|secrets| secrets.secret_name.as_str()),
    )?;
    let invocation = build_oci_process_invocation(prepared, &runtime_auth_environment);
    let result = run_process_streaming(&invocation, on_stdout_line).await;
    let runtime_secrets_cleanup_result = match runtime_secrets {
        Some(cleanup) => cleanup.shutdown().await,
        None => Ok(()),
    };

    match (result, runtime_secrets_cleanup_result) {
        (Ok(output), Ok(())) => Ok(output),
        (Ok(output), Err(err)) => {
            warn!(
                error = %err,
                context = cleanup_context,
                "runtime secret cleanup failed after successful OCI runtime"
            );
            Ok(output)
        }
        (Err(err), _) => Err(err),
    }
}

pub async fn validate_oci_launch_prerequisites(
    runtime_id: &str,
    confinement: &OciConfinementConfig,
    _required_auth: Option<RuntimeAuthKind>,
) -> Result<()> {
    let image = confinement.image.as_deref().ok_or_else(|| {
        anyhow!("runtime '{runtime_id}' requires a Podman runtime image in its confinement config")
    })?;

    // The runtime image is operator-managed, so launch preflight requires it to
    // exist locally instead of pulling an arbitrary mutable reference behind the
    // user's back.
    ensure_oci_image_exists(
        &confinement.engine,
        image,
        format!("configured runtime image '{image}' for runtime '{runtime_id}'"),
    )
    .await?;

    Ok(())
}

pub async fn validate_oci_private_network_prerequisites(
    runtime_id: &str,
    confinement: &OciConfinementConfig,
) -> Result<()> {
    let image = confinement.image.as_deref().ok_or_else(|| {
        anyhow!("runtime '{runtime_id}' requires a Podman runtime image in its confinement config")
    })?;
    let output = run_oci_preflight_command(
        &build_oci_private_network_probe_invocation(&confinement.engine, image),
        &format!("validate private OCI network for runtime '{runtime_id}'"),
        OCI_PREFLIGHT_TIMEOUT,
    )
    .await?;

    // The probe only needs to prove that Podman can stand up its private
    // network namespace. Distroless runtime images may not ship /bin/sh, so a
    // missing probe shell still counts as network success once Podman reached
    // process exec inside the container.
    if output.success()
        || private_network_probe_reached_process_exec(&String::from_utf8_lossy(&output.stderr))
    {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if stderr.is_empty() {
        bail!(
            "runtime '{runtime_id}' requires network-mode 'on', but OCI engine '{}' exited with {} while starting a private network on this host",
            confinement.engine,
            output.status_description()
        );
    }

    bail!(
        "runtime '{runtime_id}' requires network-mode 'on', but OCI engine '{}' could not start a private network on this host: {stderr}",
        confinement.engine
    )
}

pub async fn resolve_oci_image_compatibility_identity(engine: &str, image: &str) -> Result<String> {
    let output = run_oci_preflight_command(
        &ProcessInvocation {
            executable: engine.to_string(),
            args: vec![
                "image".to_string(),
                "inspect".to_string(),
                "--format".to_string(),
                "{{.Id}}".to_string(),
                image.to_string(),
            ],
            working_dir: None,
            environment: Vec::new(),
            input: String::new(),
        },
        &format!("resolve OCI image identity for '{image}'"),
        OCI_PREFLIGHT_TIMEOUT,
    )
    .await?;

    if !output.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if stderr.is_empty() {
            bail!(
                "failed to resolve OCI image identity for '{}'; OCI engine exited with {}",
                image,
                output.status_description()
            );
        }
        bail!("failed to resolve OCI image identity for '{image}': {stderr}");
    }

    let identity = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if identity.is_empty() {
        bail!("failed to resolve OCI image identity for '{image}': empty OCI inspect output");
    }

    Ok(identity)
}

fn prepare_oci_process_launch(
    request: &ExecutionRequest,
    runtime_secret_name: Option<&str>,
) -> Result<PreparedOciProcessLaunch> {
    let config = request.plan.confinement.oci();
    let image = config.image.as_deref().ok_or_else(|| {
        anyhow!(
            "runtime '{}' requires a Podman runtime image in its confinement config",
            request.plan.runtime_id
        )
    })?;

    let mut args = vec![
        "run".to_string(),
        "--rm".to_string(),
        "--interactive".to_string(),
    ];

    if config.read_only_rootfs {
        args.push("--read-only".to_string());
    }

    if plan_mounts_unix_socket(&request.plan.mounts) {
        args.push("--security-opt".to_string());
        args.push("label=disable".to_string());
    }

    if let Some(working_dir) = request.plan.working_dir.as_deref() {
        args.push("--workdir".to_string());
        args.push(map_host_path_into_runtime_mount(
            working_dir,
            &request.plan.mounts,
            "working directory",
        )?);
    }

    for mount in &request.plan.mounts {
        let (flag, spec) = format_bind_mount_arg(mount)?;
        args.push(flag.to_string());
        args.push(spec);
    }

    if workspace_lionclaw_metadata_mask_needed(&request.plan.mounts, &config.tmpfs) {
        args.push("--tmpfs".to_string());
        args.push(WORKSPACE_LIONCLAW_METADATA_TMPFS.to_string());
    }

    for tmpfs in &config.tmpfs {
        let value = tmpfs.trim();
        if value.is_empty() {
            bail!(
                "runtime '{}' declares an empty tmpfs entry",
                request.plan.runtime_id
            );
        }
        args.push("--tmpfs".to_string());
        args.push(value.to_string());
    }

    let environment = merged_environment(&request.plan.environment, &request.program.environment);

    match (&request.runtime_secrets_mount, runtime_secret_name) {
        (Some(_), Some(secret_name)) => {
            args.push("--secret".to_string());
            args.push(secret_name.to_string());
        }
        (Some(_), None) => {
            bail!("runtime secrets mount requires a registered OCI secret name");
        }
        (None, Some(_)) => {
            bail!("registered OCI secret name provided without a runtime secrets mount");
        }
        (None, None) => {}
    }

    if let Some(memory_limit) = config.limits.memory_limit.as_deref() {
        args.push("--memory".to_string());
        args.push(memory_limit.to_string());
    }
    if let Some(cpu_limit) = config.limits.cpu_limit.as_deref() {
        args.push("--cpus".to_string());
        args.push(cpu_limit.to_string());
    }
    if let Some(pids_limit) = config.limits.pids_limit {
        args.push("--pids-limit".to_string());
        args.push(pids_limit.to_string());
    }

    Ok(PreparedOciProcessLaunch {
        engine: config.engine.clone(),
        args,
        network_mode: request.plan.network_mode,
        environment,
        image: image.to_string(),
        program_executable: request.program.executable.clone(),
        program_args: request.program.args.clone(),
        stdin: request.program.stdin.clone(),
    })
}

fn append_bind_mount_identity_args(args: &mut Vec<String>) {
    #[cfg(unix)]
    {
        // LionClaw bind-mounts host workspace/runtime paths into confined
        // containers. Under rootless Podman, leaving user namespaces implicit
        // can make those mounts unreadable or unwritable to a non-root image
        // user even though the local operator owns the files. LionClaw's
        // canonical standalone runtime contract is therefore explicit keep-id
        // userns plus the invoking local uid/gid.
        args.push("--userns".to_string());
        args.push("keep-id".to_string());
        args.push("--user".to_string());
        args.push(format!("{}:{}", getuid().as_raw(), getgid().as_raw()));
    }
}

async fn ensure_oci_image_exists(engine: &str, image: &str, description: String) -> Result<()> {
    match run_oci_image_probe(engine, image).await? {
        OciImageProbeResult::Present => Ok(()),
        OciImageProbeResult::Missing => bail!(
            "{description} is not available locally; build or pull it before running LionClaw"
        ),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OciImageProbeResult {
    Present,
    Missing,
}

async fn run_oci_image_probe(engine: &str, image: &str) -> Result<OciImageProbeResult> {
    let output = run_oci_preflight_command(
        &ProcessInvocation {
            executable: engine.to_string(),
            args: vec!["image".to_string(), "exists".to_string(), image.to_string()],
            working_dir: None,
            environment: Vec::new(),
            input: String::new(),
        },
        &format!("inspect OCI image '{image}'"),
        OCI_PREFLIGHT_TIMEOUT,
    )
    .await?;

    match output.exit_code {
        Some(0) => Ok(OciImageProbeResult::Present),
        Some(1) if output.stderr.is_empty() => Ok(OciImageProbeResult::Missing),
        _ => {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            if stderr.is_empty() {
                bail!(
                    "failed to inspect OCI image '{}'; OCI engine exited with {}",
                    image,
                    output.status_description()
                );
            }
            bail!("failed to inspect OCI image '{image}': {stderr}");
        }
    }
}

async fn run_oci_preflight_command(
    invocation: &ProcessInvocation,
    action: &str,
    timeout_duration: Duration,
) -> Result<super::process::ProcessOutput> {
    match timeout(
        timeout_duration,
        run_process_streaming(invocation, |_| Ok(())),
    )
    .await
    {
        Ok(result) => result.with_context(|| {
            format!(
                "failed to {} using OCI engine '{}'",
                action, invocation.executable
            )
        }),
        Err(_) => bail!(
            "timed out after {}s while attempting to {} using OCI engine '{}'",
            timeout_duration.as_secs_f32(),
            action,
            invocation.executable
        ),
    }
}

fn build_oci_private_network_probe_invocation(engine: &str, image: &str) -> ProcessInvocation {
    ProcessInvocation {
        executable: engine.to_string(),
        args: vec![
            "run".to_string(),
            "--rm".to_string(),
            "--pull=never".to_string(),
            "--network".to_string(),
            "private".to_string(),
            "--entrypoint".to_string(),
            "/bin/sh".to_string(),
            image.to_string(),
            "-lc".to_string(),
            ":".to_string(),
        ],
        working_dir: None,
        environment: Vec::new(),
        input: String::new(),
    }
}

fn private_network_probe_reached_process_exec(stderr: &str) -> bool {
    let stderr = stderr.trim().to_ascii_lowercase();
    stderr.contains("/bin/sh") && (stderr.contains("not found") || stderr.contains("no such file"))
}

fn build_oci_process_invocation(
    prepared: PreparedOciProcessLaunch,
    runtime_auth_environment: &[(String, String)],
) -> ProcessInvocation {
    build_oci_process_invocation_with_terminal(prepared, runtime_auth_environment, false)
}

fn build_oci_attached_process_invocation(
    prepared: PreparedOciProcessLaunch,
    runtime_auth_environment: &[(String, String)],
) -> ProcessInvocation {
    build_oci_process_invocation_with_terminal(prepared, runtime_auth_environment, true)
}

fn build_oci_process_invocation_with_terminal(
    prepared: PreparedOciProcessLaunch,
    runtime_auth_environment: &[(String, String)],
    attach_terminal: bool,
) -> ProcessInvocation {
    let mut args = prepared.args;
    if attach_terminal {
        args.push("--tty".to_string());
    }

    append_bind_mount_identity_args(&mut args);

    match prepared.network_mode {
        NetworkMode::None => {
            args.push("--network".to_string());
            args.push("none".to_string());
        }
        NetworkMode::On => {
            args.push("--network".to_string());
            args.push("private".to_string());
        }
    }

    let mut environment = prepared.environment;
    environment = merged_environment(&environment, runtime_auth_environment);
    for (key, value) in environment {
        args.push("--env".to_string());
        args.push(format!("{key}={value}"));
    }

    args.push(prepared.image);
    args.push(prepared.program_executable);
    args.extend(prepared.program_args);

    ProcessInvocation {
        executable: prepared.engine,
        args,
        working_dir: None,
        environment: Vec::new(),
        input: prepared.stdin,
    }
}

async fn ensure_runtime_secrets_registered(
    request: &ExecutionRequest,
) -> Result<Option<OciRuntimeSecretsSession>> {
    let Some(mount) = request.runtime_secrets_mount.as_ref() else {
        return Ok(None);
    };
    let engine = request.plan.confinement.oci().engine.clone();
    let secret_name = mount.fresh_mounted_name();
    let output = run_process_streaming(
        &build_runtime_secret_create_invocation(&engine, mount, &secret_name)?,
        |_| Ok(()),
    )
    .await
    .with_context(|| {
        format!(
            "failed to register OCI runtime secrets for runtime '{}'",
            request.plan.runtime_id
        )
    })?;

    if output.success() {
        return Ok(Some(OciRuntimeSecretsSession {
            secret_name: secret_name.clone(),
            cleanup: Some(OciRuntimeSecretsCleanup {
                engine,
                secret_name,
            }),
        }));
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if stderr.is_empty() {
        bail!(
            "failed to register OCI runtime secrets; podman secret create exited with {}",
            output.status_description()
        );
    }

    bail!("failed to register OCI runtime secrets: {stderr}")
}

#[derive(Debug)]
struct OciRuntimeSecretsCleanup {
    engine: String,
    secret_name: String,
}

#[derive(Debug)]
struct OciRuntimeSecretsSession {
    secret_name: String,
    cleanup: Option<OciRuntimeSecretsCleanup>,
}

impl OciRuntimeSecretsSession {
    async fn shutdown(mut self) -> Result<()> {
        let Some(cleanup) = self.cleanup.take() else {
            return Ok(());
        };
        cleanup.shutdown().await
    }
}

impl Drop for OciRuntimeSecretsSession {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup.spawn();
        }
    }
}

impl OciRuntimeSecretsCleanup {
    async fn shutdown(self) -> Result<()> {
        let output = run_oci_preflight_command(
            &build_runtime_secret_remove_invocation(&self.engine, &self.secret_name),
            &format!("remove OCI runtime secret '{}'", self.secret_name),
            OCI_PREFLIGHT_TIMEOUT,
        )
        .await?;

        if output.success() {
            return Ok(());
        }

        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if stderr.is_empty() {
            bail!(
                "failed to remove OCI runtime secret '{}'; OCI engine exited with {}",
                self.secret_name,
                output.status_description()
            );
        }

        bail!(
            "failed to remove OCI runtime secret '{}': {}",
            self.secret_name,
            stderr
        )
    }

    fn spawn(self) {
        if let Ok(handle) = Handle::try_current() {
            handle.spawn(async move {
                if let Err(err) = self.shutdown().await {
                    warn!(?err, "failed to clean up runtime secrets");
                }
            });
            return;
        }

        std::thread::spawn(move || {
            match std::process::Command::new(&self.engine)
                .args(["secret", "rm", &self.secret_name])
                .status()
            {
                Ok(status) if status.success() => {}
                Ok(status) => warn!(
                    engine = %self.engine,
                    secret_name = %self.secret_name,
                    status = %status,
                    "runtime secret cleanup command failed"
                ),
                Err(err) => warn!(
                    ?err,
                    engine = %self.engine,
                    secret_name = %self.secret_name,
                    "failed to run runtime secret cleanup command"
                ),
            }
        });
    }
}

fn build_runtime_secret_create_invocation(
    engine: &str,
    mount: &RuntimeSecretsMount,
    secret_name: &str,
) -> Result<ProcessInvocation> {
    Ok(ProcessInvocation {
        executable: engine.to_string(),
        args: vec![
            "secret".to_string(),
            "create".to_string(),
            secret_name.to_string(),
            path_to_arg(&mount.source)?,
        ],
        working_dir: None,
        environment: Vec::new(),
        input: String::new(),
    })
}

fn build_runtime_secret_remove_invocation(engine: &str, secret_name: &str) -> ProcessInvocation {
    ProcessInvocation {
        executable: engine.to_string(),
        args: vec![
            "secret".to_string(),
            "rm".to_string(),
            secret_name.to_string(),
        ],
        working_dir: None,
        environment: Vec::new(),
        input: String::new(),
    }
}

fn path_to_arg(path: &Path) -> Result<String> {
    path.to_str()
        .map(|value| value.to_string())
        .ok_or_else(|| anyhow!("path '{}' is not valid UTF-8", path.display()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BindMountRelabel {
    Private,
    Shared,
}

impl BindMountRelabel {
    fn volume_option(self) -> &'static str {
        match self {
            Self::Private => "Z",
            Self::Shared => "z",
        }
    }

    fn mount_option(self) -> &'static str {
        match self {
            Self::Private => "relabel=private",
            Self::Shared => "relabel=shared",
        }
    }
}

fn format_volume_spec(source: &str, mount: &MountSpec) -> String {
    let access = bind_mount_volume_access_option(mount.access);
    let relabel = bind_mount_relabel(mount).volume_option();
    format!("{source}:{}:{access},{relabel}", mount.target)
}

fn format_bind_mount_arg(mount: &MountSpec) -> Result<(&'static str, String)> {
    let argument =
        podman_bind_mount_argument(&mount.source, &mount.target).map_err(anyhow::Error::msg)?;
    match argument.form {
        PodmanBindMountArgumentForm::Volume => {
            Ok(("--volume", format_volume_spec(argument.source, mount)))
        }
        PodmanBindMountArgumentForm::Mount => {
            Ok(("--mount", format_mount_spec(argument.source, mount)))
        }
    }
}

fn format_mount_spec(source: &str, mount: &MountSpec) -> String {
    let access = bind_mount_mount_access_option(mount.access);
    let relabel = bind_mount_relabel(mount).mount_option();
    format!(
        "type=bind,src={source},target={},{access},{relabel}",
        mount.target
    )
}

fn bind_mount_volume_access_option(access: MountAccess) -> &'static str {
    match access {
        MountAccess::ReadOnly => "ro",
        MountAccess::ReadWrite => "rw",
    }
}

fn bind_mount_mount_access_option(access: MountAccess) -> &'static str {
    match access {
        MountAccess::ReadOnly => "readonly",
        MountAccess::ReadWrite => "rw",
    }
}

fn bind_mount_relabel(mount: &MountSpec) -> BindMountRelabel {
    if mount.target == WORKSPACE_MOUNT_TARGET
        || mount_target_is_or_under(&mount.target, RUNTIME_HOME_MOUNT_TARGET)
        || mount.target == DRAFTS_MOUNT_TARGET
        || mount_target_is_or_under(&mount.target, SKILLS_MOUNT_TARGET_ROOT)
    {
        return BindMountRelabel::Shared;
    }
    BindMountRelabel::Private
}

fn mount_target_is_or_under(target: &str, root: &str) -> bool {
    target == root
        || target
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

#[cfg(unix)]
fn plan_mounts_unix_socket(mounts: &[MountSpec]) -> bool {
    mounts.iter().any(|mount| {
        fs::symlink_metadata(&mount.source)
            .map(|metadata| metadata.file_type().is_socket())
            .unwrap_or(false)
    })
}

#[cfg(not(unix))]
fn plan_mounts_unix_socket(_mounts: &[MountSpec]) -> bool {
    false
}

fn workspace_lionclaw_metadata_mask_needed(
    mounts: &[MountSpec],
    configured_tmpfs: &[String],
) -> bool {
    mounts.iter().any(|mount| {
        mount.target == WORKSPACE_MOUNT_TARGET
            && fs::symlink_metadata(mount.source.join(LIONCLAW_METADATA_DIR)).is_ok()
    }) && !configured_tmpfs
        .iter()
        .any(|entry| tmpfs_target(entry) == WORKSPACE_LIONCLAW_METADATA_TMPFS_TARGET)
}

const WORKSPACE_LIONCLAW_METADATA_TMPFS_TARGET: &str = "/workspace/.lionclaw";

fn tmpfs_target(entry: &str) -> &str {
    entry
        .trim()
        .split_once(':')
        .map_or(entry.trim(), |(target, _)| target.trim())
}

fn merged_environment(
    plan_environment: &[(String, String)],
    program_environment: &[(String, String)],
) -> Vec<(String, String)> {
    let mut merged = Vec::with_capacity(plan_environment.len() + program_environment.len());

    for (key, value) in plan_environment.iter().chain(program_environment.iter()) {
        if let Some(existing) = merged
            .iter_mut()
            .find(|(existing_key, _)| existing_key == key)
        {
            existing.1 = value.clone();
        } else {
            merged.push((key.clone(), value.clone()));
        }
    }

    merged
}

#[cfg(test)]
mod tests {
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::{fs::PermissionsExt, net::UnixListener};
    use std::time::Duration;

    use super::{
        build_oci_attached_process_invocation, build_oci_process_invocation,
        prepare_oci_process_launch, private_network_probe_reached_process_exec,
        OciExecutionBackend,
    };
    use crate::kernel::runtime::execution::backend::{
        ExecutionBackend, RUNTIME_SECRETS_NAME_PREFIX,
    };
    use crate::kernel::runtime::{
        ConfinementConfig, EffectiveExecutionPlan, ExecutionLimits, ExecutionRequest, NetworkMode,
        OciConfinementConfig, RuntimeProgramSpec, RuntimeSecretsMount, WorkspaceAccess,
    };
    use crate::kernel::runtime::{MountAccess, MountSpec};
    use crate::project_inventory::{
        PROJECT_INSTANCES_FILE_ENV, PROJECT_INSTANCES_FILE_PATH, PROJECT_INSTANCE_ENV,
        PROJECT_INSTANCE_INVENTORY_DIR,
    };
    #[cfg(unix)]
    use rustix::process::{getgid, getuid};
    use tempfile::tempdir;
    use tokio::sync::mpsc;

    #[test]
    fn oci_backend_masks_lionclaw_metadata_under_workspace_mount() {
        let project = tempdir().expect("project");
        fs::create_dir(project.path().join(".lionclaw")).expect("metadata dir");
        let mut request = sample_execution_request();
        request.plan.mounts[0].source = project.path().to_path_buf();
        request.plan.working_dir = None;

        let prepared = prepare_oci_process_launch(&request, None).expect("prepare");

        assert!(prepared.args.windows(2).any(|pair| {
            pair == [
                "--tmpfs".to_string(),
                super::WORKSPACE_LIONCLAW_METADATA_TMPFS.to_string(),
            ]
        }));
        assert!(super::WORKSPACE_LIONCLAW_METADATA_TMPFS.contains("notmpcopyup"));
    }

    #[test]
    fn oci_backend_does_not_duplicate_configured_lionclaw_metadata_mask() {
        let project = tempdir().expect("project");
        fs::create_dir(project.path().join(".lionclaw")).expect("metadata dir");
        let mut request = sample_execution_request();
        request.plan.mounts[0].source = project.path().to_path_buf();
        request.plan.working_dir = None;
        match &mut request.plan.confinement {
            ConfinementConfig::Oci(config) => {
                config
                    .tmpfs
                    .push(super::WORKSPACE_LIONCLAW_METADATA_TMPFS.to_string());
            }
        }

        let prepared = prepare_oci_process_launch(&request, None).expect("prepare");

        let mask_count = prepared
            .args
            .windows(2)
            .filter(|pair| {
                *pair
                    == [
                        "--tmpfs".to_string(),
                        super::WORKSPACE_LIONCLAW_METADATA_TMPFS.to_string(),
                    ]
            })
            .count();
        assert_eq!(mask_count, 1);
    }

    #[test]
    fn oci_backend_builds_podman_run_invocation() {
        let request = sample_execution_request_with_runtime_secrets();

        let secret_name = request
            .runtime_secrets_mount
            .as_ref()
            .expect("runtime secrets mount")
            .mounted_name();
        let invocation = build_oci_process_invocation(
            prepare_oci_process_launch(&request, Some(&secret_name)).expect("prepare"),
            &[],
        );

        assert_eq!(invocation.executable, "podman");
        assert_eq!(invocation.working_dir, None);
        assert_eq!(invocation.environment, Vec::<(String, String)>::new());
        assert_eq!(invocation.input, "hello");

        assert!(invocation.args.starts_with(&[
            "run".to_string(),
            "--rm".to_string(),
            "--interactive".to_string(),
        ]));
        #[cfg(unix)]
        assert!(invocation
            .args
            .windows(2)
            .any(|pair| pair == ["--userns".to_string(), "keep-id".to_string()]));
        #[cfg(unix)]
        assert!(invocation.args.windows(2).any(|pair| {
            pair == [
                "--user".to_string(),
                format!("{}:{}", getuid().as_raw(), getgid().as_raw()),
            ]
        }));
        assert!(invocation.args.iter().any(|arg| arg == "--read-only"));
        assert!(invocation
            .args
            .windows(2)
            .any(|pair| { pair == ["--workdir".to_string(), "/workspace/src".to_string()] }));
        assert!(invocation.args.windows(2).any(|pair| {
            pair == [
                "--volume".to_string(),
                "/host/workspace:/workspace:rw,z".to_string(),
            ]
        }));
        assert!(invocation.args.windows(2).any(|pair| {
            pair == [
                "--volume".to_string(),
                "/host/runtime/codex/dev:/runtime:rw,Z".to_string(),
            ]
        }));
        assert!(invocation.args.windows(2).any(|pair| {
            pair == [
                "--volume".to_string(),
                "/host/runtime/codex/native-home:/runtime/home:rw,z".to_string(),
            ]
        }));
        assert!(invocation.args.windows(2).any(|pair| {
            pair == [
                "--volume".to_string(),
                "/host/runtime/codex/dev/drafts:/drafts:rw,z".to_string(),
            ]
        }));
        assert!(invocation
            .args
            .windows(2)
            .any(|pair| { pair == ["--volume".to_string(), "/host/refs:/refs:ro,Z".to_string()] }));
        assert!(invocation.args.windows(2).any(|pair| {
            pair == [
                "--volume".to_string(),
                "/host/cache:/mnt/cache:rw,Z".to_string(),
            ]
        }));
        assert!(invocation
            .args
            .windows(2)
            .any(|pair| { pair == ["--tmpfs".to_string(), "/tmp:size=64m".to_string()] }));
        assert!(invocation
            .args
            .windows(2)
            .any(|pair| { pair == ["--env".to_string(), "FOO=from-plan".to_string()] }));
        assert!(invocation
            .args
            .windows(2)
            .any(|pair| { pair == ["--env".to_string(), "MODEL=gpt-5-codex".to_string()] }));
        assert!(invocation.args.windows(2).any(|pair| {
            pair == [
                "--secret".to_string(),
                request
                    .runtime_secrets_mount
                    .as_ref()
                    .expect("runtime secrets mount")
                    .mounted_name(),
            ]
        }));
        assert!(invocation
            .args
            .windows(2)
            .any(|pair| { pair == ["--memory".to_string(), "4g".to_string()] }));
        assert!(invocation
            .args
            .windows(2)
            .any(|pair| { pair == ["--cpus".to_string(), "2".to_string()] }));
        assert!(invocation
            .args
            .windows(2)
            .any(|pair| { pair == ["--pids-limit".to_string(), "256".to_string()] }));

        let image_index = invocation
            .args
            .iter()
            .position(|arg| arg == "ghcr.io/lionclaw/codex-runtime:v1")
            .expect("image arg");
        assert_eq!(
            &invocation.args[image_index..],
            &[
                "ghcr.io/lionclaw/codex-runtime:v1".to_string(),
                "/usr/local/bin/codex".to_string(),
                "exec".to_string(),
                "--json".to_string(),
            ]
        );
    }

    #[test]
    fn oci_backend_adds_tty_only_for_attached_invocation() {
        let request = sample_execution_request();
        let prepared = prepare_oci_process_launch(&request, None).expect("prepare");

        let captured = build_oci_process_invocation(prepared.clone(), &[]);
        let attached = build_oci_attached_process_invocation(prepared, &[]);

        assert!(!captured.args.iter().any(|arg| arg == "--tty"));
        assert!(attached.args.iter().any(|arg| arg == "--tty"));
    }

    #[test]
    fn oci_backend_relabels_session_mounts_private_and_persistent_mounts_shared() {
        for target in [
            "/runtime",
            "/runtime/lionclaw/channel-send.sock",
            "/attachments/provider",
            "/lionclaw/project",
            "/refs",
            "/mnt/cache",
        ] {
            let mount = MountSpec {
                source: "/host/session".into(),
                target: target.to_string(),
                access: MountAccess::ReadWrite,
            };

            assert_eq!(
                super::bind_mount_relabel(&mount),
                super::BindMountRelabel::Private
            );
        }

        for target in [
            "/runtime/home",
            "/runtime/home/.codex",
            "/workspace",
            "/drafts",
            "/lionclaw/skills/loopback",
        ] {
            let mount = MountSpec {
                source: "/host/shared".into(),
                target: target.to_string(),
                access: MountAccess::ReadWrite,
            };

            assert_eq!(
                super::bind_mount_relabel(&mount),
                super::BindMountRelabel::Shared
            );
        }
    }

    #[test]
    fn oci_backend_emits_channel_send_socket_mount_and_env() {
        let mut request = sample_execution_request();
        request.plan.mounts.push(MountSpec {
            source: "/host/runtime/sockets/channel-send-test.sock".into(),
            target: "/runtime/lionclaw/channel-send.sock".to_string(),
            access: MountAccess::ReadWrite,
        });
        request.plan.environment.push((
            "LIONCLAW_CHANNEL_SEND_SOCKET".to_string(),
            "/runtime/lionclaw/channel-send.sock".to_string(),
        ));

        let invocation = build_oci_process_invocation(
            prepare_oci_process_launch(&request, None).expect("prepare"),
            &[],
        );

        assert!(invocation.args.windows(2).any(|pair| {
            pair == [
                "--volume".to_string(),
                "/host/runtime/sockets/channel-send-test.sock:/runtime/lionclaw/channel-send.sock:rw,Z"
                    .to_string(),
            ]
        }));
        assert!(invocation.args.windows(2).any(|pair| {
            pair == [
                "--env".to_string(),
                "LIONCLAW_CHANNEL_SEND_SOCKET=/runtime/lionclaw/channel-send.sock".to_string(),
            ]
        }));
    }

    #[cfg(unix)]
    #[test]
    fn oci_backend_disables_selinux_labeling_for_unix_socket_mounts() {
        let temp_dir = tempdir().expect("temp dir");
        let socket_path = temp_dir.path().join("channel-send.sock");
        let _listener = UnixListener::bind(&socket_path).expect("bind unix socket");
        let mut request = sample_execution_request();
        request.plan.mounts.push(MountSpec {
            source: socket_path,
            target: "/runtime/lionclaw/channel-send.sock".to_string(),
            access: MountAccess::ReadWrite,
        });

        let invocation = build_oci_process_invocation(
            prepare_oci_process_launch(&request, None).expect("prepare"),
            &[],
        );

        assert!(invocation
            .args
            .windows(2)
            .any(|pair| { pair == ["--security-opt".to_string(), "label=disable".to_string(),] }));
    }

    #[test]
    fn oci_backend_emits_project_instance_inventory_mount_and_env() {
        let mut request = sample_execution_request();
        request.plan.mounts.push(MountSpec {
            source: "/host/runtime/project-instance-projections/session/turn".into(),
            target: PROJECT_INSTANCE_INVENTORY_DIR.to_string(),
            access: MountAccess::ReadOnly,
        });
        request.plan.environment.extend([
            (PROJECT_INSTANCE_ENV.to_string(), "reviewer".to_string()),
            (
                PROJECT_INSTANCES_FILE_ENV.to_string(),
                PROJECT_INSTANCES_FILE_PATH.to_string(),
            ),
        ]);

        let invocation = build_oci_process_invocation(
            prepare_oci_process_launch(&request, None).expect("prepare"),
            &[],
        );

        assert!(invocation.args.windows(2).any(|pair| {
            pair == [
                "--volume".to_string(),
                "/host/runtime/project-instance-projections/session/turn:/lionclaw/project:ro,Z"
                    .to_string(),
            ]
        }));
        assert!(invocation.args.windows(2).any(|pair| {
            pair == [
                "--env".to_string(),
                format!("{PROJECT_INSTANCE_ENV}=reviewer"),
            ]
        }));
        assert!(invocation.args.windows(2).any(|pair| {
            pair == [
                "--env".to_string(),
                format!("{PROJECT_INSTANCES_FILE_ENV}={PROJECT_INSTANCES_FILE_PATH}"),
            ]
        }));
    }

    #[test]
    fn oci_backend_adds_none_network_flag() {
        let mut plan = sample_plan();
        plan.network_mode = NetworkMode::None;

        let request = ExecutionRequest {
            plan,
            program: RuntimeProgramSpec {
                executable: "codex".to_string(),
                args: Vec::new(),
                environment: Vec::new(),
                stdin: String::new(),
                auth: None,
            },
            runtime_secrets_mount: None,
            runtime_auth_provider: None,
            runtime_auth_context: Default::default(),
        };

        let invocation = build_oci_process_invocation(
            prepare_oci_process_launch(&request, None).expect("prepare"),
            &[],
        );

        assert!(invocation
            .args
            .windows(2)
            .any(|pair| { pair == ["--network".to_string(), "none".to_string()] }));
    }

    #[test]
    fn oci_backend_uses_mount_arg_for_colon_paths() {
        let mut request = sample_execution_request();
        request.plan.working_dir = None;
        request.plan.mounts = vec![
            MountSpec {
                source: "/host/refs:archive".into(),
                target: "/refs".to_string(),
                access: MountAccess::ReadOnly,
            },
            MountSpec {
                source: "/host/cache".into(),
                target: "/mnt/cache:archive".to_string(),
                access: MountAccess::ReadWrite,
            },
        ];

        let prepared = prepare_oci_process_launch(&request, None).expect("prepare");

        assert!(prepared.args.windows(2).any(|pair| {
            pair == [
                "--mount".to_string(),
                "type=bind,src=/host/refs:archive,target=/refs,readonly,relabel=private"
                    .to_string(),
            ]
        }));
        assert!(prepared.args.windows(2).any(|pair| {
            pair == [
                "--mount".to_string(),
                "type=bind,src=/host/cache,target=/mnt/cache:archive,rw,relabel=private"
                    .to_string(),
            ]
        }));
    }

    #[test]
    fn oci_backend_rejects_bind_mount_paths_with_colon_and_comma() {
        let mut request = sample_execution_request();
        request.plan.working_dir = None;
        request.plan.mounts = vec![MountSpec {
            source: "/host/refs:archive,current".into(),
            target: "/refs".to_string(),
            access: MountAccess::ReadOnly,
        }];

        let err = prepare_oci_process_launch(&request, None).expect_err("unrepresentable mount");

        assert!(err.to_string().contains("Podman --mount"));
        assert!(err.to_string().contains("contains ','"));
    }

    #[test]
    fn oci_backend_adds_private_network_flag_for_on_mode() {
        let request = ExecutionRequest {
            plan: sample_plan(),
            program: RuntimeProgramSpec::default(),
            runtime_secrets_mount: None,
            runtime_auth_provider: None,
            runtime_auth_context: Default::default(),
        };

        let invocation = build_oci_process_invocation(
            prepare_oci_process_launch(&request, None).expect("prepare"),
            &[],
        );

        assert!(invocation
            .args
            .windows(2)
            .any(|pair| { pair == ["--network".to_string(), "private".to_string()] }));
    }

    #[test]
    fn oci_backend_merges_runtime_auth_environment() {
        let request = ExecutionRequest {
            plan: sample_plan(),
            program: RuntimeProgramSpec {
                executable: "/usr/local/bin/codex".to_string(),
                args: vec!["exec".to_string(), "--json".to_string()],
                environment: Vec::new(),
                stdin: String::new(),
                auth: None,
            },
            runtime_secrets_mount: None,
            runtime_auth_provider: None,
            runtime_auth_context: Default::default(),
        };

        let invocation = build_oci_process_invocation(
            prepare_oci_process_launch(&request, None).expect("prepare"),
            &[(
                "RUNTIME_AUTH_HOME".to_string(),
                "/runtime/home/auth".to_string(),
            )],
        );

        #[cfg(unix)]
        assert!(invocation.args.windows(2).any(|pair| {
            pair == [
                "--user".to_string(),
                format!("{}:{}", getuid().as_raw(), getgid().as_raw()),
            ]
        }));
        assert!(invocation.args.windows(2).any(|pair| {
            pair == [
                "--env".to_string(),
                "RUNTIME_AUTH_HOME=/runtime/home/auth".to_string(),
            ]
        }));
        assert!(
            invocation
                .args
                .windows(2)
                .any(|pair| { pair == ["--network".to_string(), "private".to_string()] }),
            "direct runtime launch should keep explicit network wiring"
        );
    }

    #[test]
    fn oci_backend_rejects_missing_image() {
        let mut plan = sample_plan();
        plan.confinement.oci_mut().image = None;

        let err = prepare_oci_process_launch(
            &ExecutionRequest {
                plan,
                program: RuntimeProgramSpec::default(),
                runtime_secrets_mount: None,
                runtime_auth_provider: None,
                runtime_auth_context: Default::default(),
            },
            None,
        )
        .expect_err("missing image should fail");

        assert!(err.to_string().contains("requires a Podman runtime image"));
    }

    #[test]
    fn private_network_probe_accepts_missing_probe_shell_after_network_setup() {
        assert!(private_network_probe_reached_process_exec(
            "Error: executable file `/bin/sh` not found in $PATH: No such file or directory"
        ));
        assert!(private_network_probe_reached_process_exec(
            "Error: stat /bin/sh: no such file or directory"
        ));
    }

    #[test]
    fn private_network_probe_rejects_real_network_failures() {
        assert!(!private_network_probe_reached_process_exec(
            "Error: pasta failed with exit code 1:\nFailed to open() /dev/net/tun: No such device"
        ));
    }

    #[test]
    fn oci_backend_rejects_working_dir_outside_mounts() {
        let mut plan = sample_plan();
        plan.working_dir = Some("/outside".to_string());

        let err = prepare_oci_process_launch(
            &ExecutionRequest {
                plan,
                program: RuntimeProgramSpec::default(),
                runtime_secrets_mount: None,
                runtime_auth_provider: None,
                runtime_auth_context: Default::default(),
            },
            None,
        )
        .expect_err("working dir should fail");

        assert!(err
            .to_string()
            .contains("is not inside any configured runtime mount"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn oci_backend_removes_runtime_secret_after_turn_completion() {
        let temp_dir = tempdir().expect("tempdir");
        let log_path = temp_dir.path().join("podman.log");
        let engine_path = temp_dir.path().join("podman-stub.sh");
        let script = format!(
            r#"#!/usr/bin/env bash
set -eu
echo "$@" >> "{log_path}"
case "${{1:-}}" in
  secret)
    exit 0
    ;;
  run)
    exit 0
    ;;
  *)
    exit 0
    ;;
esac
"#,
            log_path = log_path.display()
        );
        fs::write(&engine_path, script).expect("write engine");
        let mut permissions = fs::metadata(&engine_path)
            .expect("engine metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&engine_path, permissions).expect("chmod engine");

        let request = ExecutionRequest {
            plan: EffectiveExecutionPlan {
                confinement: ConfinementConfig::Oci(OciConfinementConfig {
                    engine: engine_path.display().to_string(),
                    ..sample_plan().confinement.oci().clone()
                }),
                ..sample_plan()
            },
            program: RuntimeProgramSpec {
                executable: "codex".to_string(),
                args: vec!["exec".to_string(), "--json".to_string()],
                environment: Vec::new(),
                stdin: "hello".to_string(),
                auth: None,
            },
            runtime_secrets_mount: Some(RuntimeSecretsMount {
                source: temp_dir.path().join("runtime-secrets.env"),
            }),
            runtime_auth_provider: None,
            runtime_auth_context: Default::default(),
        };
        fs::write(
            request
                .runtime_secrets_mount
                .as_ref()
                .expect("mount")
                .source
                .as_path(),
            "TOKEN=value\n",
        )
        .expect("write runtime secrets");

        let (stdout_tx, _stdout_rx) = mpsc::unbounded_channel();
        OciExecutionBackend
            .execute_streaming(request.clone(), stdout_tx)
            .await
            .expect("execute");

        let log = fs::read_to_string(&log_path).expect("read log");
        let create_line = log
            .lines()
            .find(|line| line.starts_with("secret create "))
            .expect("secret create should be logged");
        assert!(
            !create_line.contains("--replace"),
            "secret create should be compatible with old Podman versions: {log}"
        );
        let secret_name = create_line
            .split_whitespace()
            .nth(2)
            .expect("secret name in create command");
        assert!(
            secret_name.starts_with(RUNTIME_SECRETS_NAME_PREFIX),
            "secret name should be LionClaw-managed: {log}"
        );
        assert!(
            log.contains(&format!("--secret {secret_name}")),
            "run should mount the created secret: {log}"
        );
        assert!(
            log.contains(&format!("secret rm {secret_name}")),
            "secret remove should be logged: {log}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn oci_backend_does_not_fail_successful_turn_when_runtime_secret_cleanup_races() {
        let temp_dir = tempdir().expect("tempdir");
        let log_path = temp_dir.path().join("podman.log");
        let engine_path = temp_dir.path().join("podman-stub.sh");
        let script = format!(
            r#"#!/usr/bin/env bash
set -eu
echo "$@" >> "{log_path}"
if [ "${{1:-}}" = "secret" ] && [ "${{2:-}}" = "rm" ]; then
  echo "secret not found" >&2
  exit 1
fi
case "${{1:-}}" in
  run) cat >/dev/null; exit 0 ;;
  secret) exit 0 ;;
  *) exit 0 ;;
esac
"#,
            log_path = log_path.display()
        );
        fs::write(&engine_path, script).expect("write engine");
        let mut permissions = fs::metadata(&engine_path)
            .expect("engine metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&engine_path, permissions).expect("chmod engine");

        let request = ExecutionRequest {
            plan: EffectiveExecutionPlan {
                confinement: ConfinementConfig::Oci(OciConfinementConfig {
                    engine: engine_path.display().to_string(),
                    ..sample_plan().confinement.oci().clone()
                }),
                ..sample_plan()
            },
            program: RuntimeProgramSpec {
                executable: "codex".to_string(),
                args: vec!["exec".to_string(), "--json".to_string()],
                environment: Vec::new(),
                stdin: "hello".to_string(),
                auth: None,
            },
            runtime_secrets_mount: Some(RuntimeSecretsMount {
                source: temp_dir.path().join("runtime-secrets.env"),
            }),
            runtime_auth_provider: None,
            runtime_auth_context: Default::default(),
        };
        fs::write(
            request
                .runtime_secrets_mount
                .as_ref()
                .expect("mount")
                .source
                .as_path(),
            "TOKEN=value\n",
        )
        .expect("write runtime secrets");

        let (stdout_tx, _stdout_rx) = mpsc::unbounded_channel();
        OciExecutionBackend
            .execute_streaming(request.clone(), stdout_tx)
            .await
            .expect("successful turn should not fail on cleanup race");

        let log = fs::read_to_string(&log_path).expect("read log");
        let secret_name = request
            .runtime_secrets_mount
            .as_ref()
            .expect("mount")
            .mounted_name();
        assert!(
            log.contains(&format!("secret rm {secret_name}")),
            "secret remove should still be attempted: {log}"
        );
    }

    fn sample_plan() -> EffectiveExecutionPlan {
        EffectiveExecutionPlan {
            runtime_id: "codex".to_string(),
            preset_name: "everyday".to_string(),
            confinement: ConfinementConfig::Oci(OciConfinementConfig {
                engine: "podman".to_string(),
                image: Some("ghcr.io/lionclaw/codex-runtime:v1".to_string()),
                read_only_rootfs: true,
                tmpfs: vec!["/tmp:size=64m".to_string()],
                additional_mounts: Vec::new(),
                limits: ExecutionLimits {
                    memory_limit: Some("4g".to_string()),
                    cpu_limit: Some("2".to_string()),
                    pids_limit: Some(256),
                },
            }),
            skill_projection: None,
            workspace_access: WorkspaceAccess::ReadWrite,
            network_mode: NetworkMode::On,
            working_dir: Some("/host/workspace/src".to_string()),
            environment: vec![("FOO".to_string(), "from-plan".to_string())],
            mcp_servers: Vec::new(),
            idle_timeout: Duration::from_secs(30),
            hard_timeout: Duration::from_secs(90),
            mounts: vec![
                MountSpec {
                    source: "/host/workspace".into(),
                    target: "/workspace".to_string(),
                    access: MountAccess::ReadWrite,
                },
                MountSpec {
                    source: "/host/runtime/codex/dev".into(),
                    target: "/runtime".to_string(),
                    access: MountAccess::ReadWrite,
                },
                MountSpec {
                    source: "/host/runtime/codex/native-home".into(),
                    target: "/runtime/home".to_string(),
                    access: MountAccess::ReadWrite,
                },
                MountSpec {
                    source: "/host/runtime/codex/dev/drafts".into(),
                    target: "/drafts".to_string(),
                    access: MountAccess::ReadWrite,
                },
                MountSpec {
                    source: "/host/refs".into(),
                    target: "/refs".to_string(),
                    access: MountAccess::ReadOnly,
                },
                MountSpec {
                    source: "/host/cache".into(),
                    target: "/mnt/cache".to_string(),
                    access: MountAccess::ReadWrite,
                },
            ],
            mount_runtime_secrets: true,
            escape_classes: Default::default(),
            limits: ExecutionLimits {
                memory_limit: Some("4g".to_string()),
                cpu_limit: Some("2".to_string()),
                pids_limit: Some(256),
            },
        }
    }

    fn sample_execution_request() -> ExecutionRequest {
        ExecutionRequest {
            plan: sample_plan(),
            program: RuntimeProgramSpec {
                executable: "/usr/local/bin/codex".to_string(),
                args: vec!["exec".to_string(), "--json".to_string()],
                environment: vec![("MODEL".to_string(), "gpt-5-codex".to_string())],
                stdin: "hello".to_string(),
                auth: None,
            },
            runtime_secrets_mount: None,
            runtime_auth_provider: None,
            runtime_auth_context: Default::default(),
        }
    }

    fn sample_execution_request_with_runtime_secrets() -> ExecutionRequest {
        ExecutionRequest {
            runtime_secrets_mount: Some(RuntimeSecretsMount {
                source: "/home/mosh/.lionclaw/config/runtime-secrets.env".into(),
            }),
            ..sample_execution_request()
        }
    }
}
