use std::{
    collections::BTreeSet,
    fs,
    path::Path,
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
#[cfg(unix)]
use rustix::process::{getgid, getuid};
#[cfg(unix)]
use std::os::unix::fs::FileTypeExt;
use tokio::{runtime::Handle, time::sleep};
use tracing::warn;

use super::{
    backend::{
        ExecutionBackend, ExecutionOutput, ExecutionRequest, ExecutionSession,
        ExecutionStdoutSender,
    },
    mount_validation::{podman_bind_mount_argument, PodmanBindMountArgumentForm},
    plan::{
        map_host_path_into_runtime_mount, ConfinementBackend, MountAccess, MountSpec, NetworkGrant,
    },
    process::{
        run_process_attached, run_process_bounded, run_process_streaming, spawn_process_session,
        BoundedProcessFailure, ProcessInvocation, ProcessSession,
    },
    runtime_auth::{prepare_runtime_auth, PreparedCredentialMount, PreparedRuntimeAuth},
    OciConfinementConfig, RuntimeTmpfsEntry,
};
use crate::RuntimeSecretsMount;

#[derive(Debug, Default, Clone, Copy)]
pub struct OciExecutionBackend;

/// Bounded readiness of one image through the configured OCI engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OciImageReadiness {
    Ready { identity: String },
    Missing,
    EngineUnavailable,
    InspectionFailed,
    Retryable,
}

pub struct OciExecutionSession {
    process: ProcessSession,
    runtime_secrets: Option<OciRuntimeSecretsSession>,
    network: Option<OciNetworkSession>,
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
            network,
        } = self;
        let result = process.wait().await;
        let cleanup_result = cleanup_runtime_resources(runtime_secrets, network).await;

        finish_oci_execution(result, cleanup_result, "interactive OCI runtime turn")
    }
}

const OCI_PREFLIGHT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, thiserror::Error)]
enum OciPreflightFailure {
    #[error("failed to {action} using OCI engine '{engine}'")]
    EngineUnavailable {
        action: String,
        engine: String,
        #[source]
        source: anyhow::Error,
    },
    #[error("timed out after {seconds}s while attempting to {action} using OCI engine '{engine}'")]
    TimedOut {
        seconds: f32,
        action: String,
        engine: String,
    },
}

// LionClaw bind-mounts local workspace and runtime state into confined OCI
// containers. On SELinux hosts those mounts are unreadable by default unless
// Podman relabels them for the container context. Session-scoped LionClaw
// control mounts stay private; persistent/shared mounts use shared relabeling
// so concurrent containers cannot steal labels from each other.
const WORKSPACE_MOUNT_TARGET: &str = "/workspace";
const RUNTIME_HOME_MOUNT_TARGET: &str = "/runtime/home";
const DRAFTS_MOUNT_TARGET: &str = "/drafts";
const LIONCLAW_METADATA_DIR: &str = ".lionclaw";
const WORKSPACE_LIONCLAW_METADATA_TMPFS: &str = "/workspace/.lionclaw:size=1m,mode=700,notmpcopyup";
const NETWORK_PROXY_ALIAS: &str = "lionclaw-proxy";
const NETWORK_PROXY_HTTP_PORT: u16 = 3128;
const NETWORK_PROXY_SOCKS_PORT: u16 = 3129;
const NETWORK_PROXY_BINARY: &str = "/usr/local/bin/lionclaw";

#[derive(Debug, Clone)]
struct PreparedOciProcessLaunch {
    engine: String,
    args: Vec<String>,
    root_in_userns: bool,
    network: PreparedOciNetwork,
    environment: Vec<(String, String)>,
    image: String,
    program_executable: String,
    program_args: Vec<String>,
    stdin: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PreparedOciNetwork {
    Deny,
    Proxy {
        destinations: BTreeSet<lionclaw_runtime_api::Destination>,
        internal_network_name: String,
        egress_network_name: String,
        proxy_name: String,
    },
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
        execute_oci_process(request, Some(stdout), "streaming OCI runtime turn").await
    }

    async fn execute_captured(&self, request: ExecutionRequest) -> Result<ExecutionOutput> {
        execute_oci_process(request, None, "captured OCI runtime command").await
    }

    async fn spawn_interactive(&self, request: ExecutionRequest) -> Result<ExecutionSession> {
        let runtime_secrets = ensure_runtime_secrets_registered(&request).await?;
        let runtime_auth = prepare_runtime_auth(&request)?;
        let prepared = prepare_oci_process_launch_with_runtime_auth(
            &request,
            runtime_secrets
                .as_ref()
                .map(|secrets| secrets.secret_name.as_str()),
            &runtime_auth,
        )?;
        let network = ensure_oci_network_registered(&prepared).await?;
        let invocation = build_oci_process_invocation(prepared, runtime_auth.environment());
        let process = spawn_process_session(&invocation).await?;
        Ok(ExecutionSession::Oci(OciExecutionSession {
            process,
            runtime_secrets,
            network,
        }))
    }

    async fn execute_attached(&self, request: ExecutionRequest) -> Result<ExecutionOutput> {
        let runtime_secrets = ensure_runtime_secrets_registered(&request).await?;
        let runtime_auth = prepare_runtime_auth(&request)?;
        let prepared = prepare_oci_process_launch_with_runtime_auth(
            &request,
            runtime_secrets
                .as_ref()
                .map(|secrets| secrets.secret_name.as_str()),
            &runtime_auth,
        )?;
        let network = ensure_oci_network_registered(&prepared).await?;
        let invocation =
            build_oci_attached_process_invocation(prepared, runtime_auth.environment());
        let result = run_process_attached(&invocation).await;
        let cleanup_result = cleanup_runtime_resources(runtime_secrets, network).await;

        finish_oci_execution(result, cleanup_result, "attached OCI runtime")
    }
}

async fn execute_oci_process(
    request: ExecutionRequest,
    stdout: Option<ExecutionStdoutSender>,
    cleanup_context: &'static str,
) -> Result<ExecutionOutput> {
    let runtime_secrets = ensure_runtime_secrets_registered(&request).await?;
    let runtime_auth = prepare_runtime_auth(&request)?;
    let prepared = prepare_oci_process_launch_with_runtime_auth(
        &request,
        runtime_secrets
            .as_ref()
            .map(|secrets| secrets.secret_name.as_str()),
        &runtime_auth,
    )?;
    let network = ensure_oci_network_registered(&prepared).await?;
    let invocation = build_oci_process_invocation(prepared, runtime_auth.environment());
    let result = run_process_streaming(&invocation, stdout.as_ref()).await;
    let cleanup_result = cleanup_runtime_resources(runtime_secrets, network).await;

    finish_oci_execution(result, cleanup_result, cleanup_context)
}

async fn cleanup_runtime_resources(
    runtime_secrets: Option<OciRuntimeSecretsSession>,
    network: Option<OciNetworkSession>,
) -> Result<()> {
    let runtime_secrets_cleanup_result = match runtime_secrets {
        Some(cleanup) => cleanup.shutdown().await,
        None => Ok(()),
    };
    let network_cleanup_result = match network {
        Some(cleanup) => cleanup.shutdown().await,
        None => Ok(()),
    };
    runtime_secrets_cleanup_result.and(network_cleanup_result)
}

fn finish_oci_execution(
    result: Result<ExecutionOutput>,
    cleanup_result: Result<()>,
    context: &'static str,
) -> Result<ExecutionOutput> {
    if let Err(err) = cleanup_result {
        warn!(
            error = %err,
            context,
            "OCI resource cleanup failed after runtime"
        );
    }
    result
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
            "runtime '{runtime_id}' requires destination-scoped OCI networking, but OCI engine '{}' exited with {} while starting a private network on this host",
            confinement.engine,
            output.status_description()
        );
    }

    bail!(
        "runtime '{runtime_id}' requires destination-scoped OCI networking, but OCI engine '{}' could not start a private network on this host: {stderr}",
        confinement.engine
    )
}

pub async fn inspect_oci_image(engine: &str, image: &str) -> OciImageReadiness {
    match resolve_oci_image_compatibility_identity_unchecked(engine, image).await {
        Ok(identity) => OciImageReadiness::Ready { identity },
        Err(inspect_error) if preflight_timed_out(&inspect_error) => OciImageReadiness::Retryable,
        Err(_) => match run_oci_image_probe(engine, image).await {
            Ok(OciImageProbeResult::Missing) => OciImageReadiness::Missing,
            Ok(OciImageProbeResult::Present | OciImageProbeResult::Indeterminate) => {
                OciImageReadiness::InspectionFailed
            }
            Err(OciPreflightFailure::TimedOut { .. }) => OciImageReadiness::Retryable,
            Err(OciPreflightFailure::EngineUnavailable { .. }) => {
                OciImageReadiness::EngineUnavailable
            }
        },
    }
}

pub async fn resolve_oci_image_compatibility_identity(engine: &str, image: &str) -> Result<String> {
    match inspect_oci_image(engine, image).await {
        OciImageReadiness::Ready { identity } => Ok(identity),
        OciImageReadiness::Missing => {
            bail!("OCI image '{image}' is not available locally through engine '{engine}'")
        }
        OciImageReadiness::EngineUnavailable => {
            bail!("OCI engine '{engine}' could not inspect image '{image}'")
        }
        OciImageReadiness::InspectionFailed => {
            bail!("OCI image '{image}' is present through engine '{engine}', but its stable identity could not be inspected")
        }
        OciImageReadiness::Retryable => {
            bail!("OCI engine '{engine}' timed out while inspecting image '{image}'")
        }
    }
}

async fn resolve_oci_image_compatibility_identity_unchecked(
    engine: &str,
    image: &str,
) -> Result<String> {
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

#[cfg(test)]
fn prepare_oci_process_launch(
    request: &ExecutionRequest,
    runtime_secret_name: Option<&str>,
) -> Result<PreparedOciProcessLaunch> {
    prepare_oci_process_launch_with_runtime_auth(
        request,
        runtime_secret_name,
        &PreparedRuntimeAuth::empty(),
    )
}

fn prepare_oci_process_launch_with_runtime_auth(
    request: &ExecutionRequest,
    runtime_secret_name: Option<&str>,
    runtime_auth: &PreparedRuntimeAuth,
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

    if let Some(name) = &request.resource_name {
        validate_oci_resource_name(name)?;
        args.push("--name".to_string());
        args.push(name.clone());
    }

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
        let mount = if mount.target == RUNTIME_HOME_MOUNT_TARGET {
            runtime_auth.runtime_home_mount().unwrap_or(mount)
        } else {
            mount
        };
        let (flag, spec) = format_bind_mount_arg(mount)?;
        args.push(flag.to_string());
        args.push(spec);
    }
    for mount in runtime_auth.credential_mounts() {
        let (flag, spec) = format_private_bind_mount_arg(mount)?;
        args.push(flag.to_string());
        args.push(spec);
    }

    let tmpfs = config
        .tmpfs
        .iter()
        .map(|entry| {
            crate::parse_runtime_tmpfs_entry(entry).map_err(|detail| {
                anyhow!(
                    "runtime '{}' declares invalid tmpfs entry '{}': {detail}",
                    request.plan.runtime_id,
                    entry
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;

    if workspace_lionclaw_metadata_mask_needed(&request.plan.mounts, &tmpfs) {
        args.push("--tmpfs".to_string());
        args.push(WORKSPACE_LIONCLAW_METADATA_TMPFS.to_string());
    }

    for tmpfs in &tmpfs {
        args.push("--tmpfs".to_string());
        args.push(tmpfs.argument().to_string());
    }

    for device in &request.plan.devices {
        args.push("--device".to_string());
        args.push(device.clone());
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

    let network = prepare_oci_network(&request.plan.network, request.resource_name.as_deref())?;

    Ok(PreparedOciProcessLaunch {
        engine: config.engine.clone(),
        args,
        root_in_userns: request.plan.root_in_userns,
        network,
        environment,
        image: image.to_string(),
        program_executable: request.program.executable.clone(),
        program_args: request.program.args.clone(),
        stdin: request.program.stdin.clone(),
    })
}

fn prepare_oci_network(
    network: &NetworkGrant,
    resource_name: Option<&str>,
) -> Result<PreparedOciNetwork> {
    let Some(destinations) = network.destinations() else {
        return Ok(PreparedOciNetwork::Deny);
    };
    let resource_name = resource_name.ok_or_else(|| {
        anyhow!("destination-scoped OCI network requires an effect resource name")
    })?;
    validate_oci_resource_name(resource_name)?;
    let internal_network_name = format!("{resource_name}-net");
    let egress_network_name = format!("{resource_name}-egress");
    let proxy_name = format!("{resource_name}-proxy");
    validate_oci_resource_name(&internal_network_name)?;
    validate_oci_resource_name(&egress_network_name)?;
    validate_oci_resource_name(&proxy_name)?;
    Ok(PreparedOciNetwork::Proxy {
        destinations: destinations.clone(),
        internal_network_name,
        egress_network_name,
        proxy_name,
    })
}

fn append_bind_mount_identity_args(args: &mut Vec<String>, root_in_userns: bool) {
    #[cfg(unix)]
    {
        if root_in_userns {
            args.push("--user".to_string());
            args.push("0:0".to_string());
            return;
        }
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
    #[cfg(not(unix))]
    {
        let _ = (args, root_in_userns);
    }
}

fn preflight_timed_out(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<OciPreflightFailure>(),
        Some(OciPreflightFailure::TimedOut { .. })
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OciImageProbeResult {
    Present,
    Missing,
    Indeterminate,
}

async fn run_oci_image_probe(
    engine: &str,
    image: &str,
) -> std::result::Result<OciImageProbeResult, OciPreflightFailure> {
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
        _ => Ok(OciImageProbeResult::Indeterminate),
    }
}

async fn run_oci_preflight_command(
    invocation: &ProcessInvocation,
    action: &str,
    timeout_duration: Duration,
) -> std::result::Result<super::process::ProcessOutput, OciPreflightFailure> {
    match run_process_bounded(invocation, timeout_duration).await {
        Ok(output) => Ok(output),
        Err(BoundedProcessFailure::Failed(source)) => Err(OciPreflightFailure::EngineUnavailable {
            action: action.to_string(),
            engine: invocation.executable.clone(),
            source,
        }),
        Err(BoundedProcessFailure::TimedOut) => Err(OciPreflightFailure::TimedOut {
            seconds: timeout_duration.as_secs_f32(),
            action: action.to_string(),
            engine: invocation.executable.clone(),
        }),
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

    append_bind_mount_identity_args(&mut args, prepared.root_in_userns);

    match &prepared.network {
        PreparedOciNetwork::Deny => {
            args.push("--network".to_string());
            args.push("none".to_string());
        }
        PreparedOciNetwork::Proxy {
            internal_network_name,
            ..
        } => {
            args.push("--network".to_string());
            args.push(internal_network_name.clone());
        }
    }

    let runtime_auth_environment = merged_environment(&[], runtime_auth_environment);
    let proxy_environment = network_proxy_environment(&prepared.network);
    let plan_environment = merged_environment(&prepared.environment, &proxy_environment);
    let environment = merged_environment(&plan_environment, &runtime_auth_environment);
    for (key, value) in environment {
        args.push("--env".to_string());
        if runtime_auth_environment
            .iter()
            .any(|(auth_key, _)| auth_key == &key)
        {
            // Podman copies a value-free --env KEY from its own environment.
            // Keep auth values out of argv and process listings.
            args.push(key);
        } else {
            args.push(format!("{key}={value}"));
        }
    }

    args.push(prepared.image);
    args.push(prepared.program_executable);
    args.extend(prepared.program_args);

    ProcessInvocation {
        executable: prepared.engine,
        args,
        working_dir: None,
        environment: runtime_auth_environment,
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
    let secret_name = request
        .resource_name
        .clone()
        .unwrap_or_else(|| mount.fresh_mounted_name());
    validate_oci_resource_name(&secret_name)?;
    let output = run_process_streaming(
        &build_runtime_secret_create_invocation(&engine, mount, &secret_name)?,
        None,
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

async fn ensure_oci_network_registered(
    prepared: &PreparedOciProcessLaunch,
) -> Result<Option<OciNetworkSession>> {
    let PreparedOciNetwork::Proxy {
        destinations,
        internal_network_name,
        egress_network_name,
        proxy_name,
    } = &prepared.network
    else {
        return Ok(None);
    };

    let network = OciNetworkSession {
        engine: prepared.engine.clone(),
        internal_network_name: internal_network_name.clone(),
        egress_network_name: egress_network_name.clone(),
        proxy_name: proxy_name.clone(),
        cleanup: Some(OciNetworkCleanup {
            engine: prepared.engine.clone(),
            proxy_name: proxy_name.clone(),
            internal_network_name: internal_network_name.clone(),
            egress_network_name: egress_network_name.clone(),
        }),
    };
    network.create_networks().await?;
    let start_result = network.start_proxy(&prepared.image, destinations).await;
    if let Err(error) = start_result {
        if let Err(cleanup_error) = network.shutdown().await {
            warn!(
                error = %cleanup_error,
                "failed to clean up OCI network after proxy launch failure"
            );
        }
        return Err(error);
    }
    Ok(Some(network))
}

#[derive(Debug)]
struct OciNetworkSession {
    engine: String,
    internal_network_name: String,
    egress_network_name: String,
    proxy_name: String,
    cleanup: Option<OciNetworkCleanup>,
}

impl OciNetworkSession {
    async fn create_networks(&self) -> Result<()> {
        self.create_network(&self.internal_network_name, true)
            .await?;
        self.create_network(&self.egress_network_name, false).await
    }

    async fn create_network(&self, network_name: &str, internal: bool) -> Result<()> {
        let output = run_oci_preflight_command(
            &build_network_create_invocation(&self.engine, network_name, internal),
            &format!("create OCI network '{network_name}'"),
            OCI_PREFLIGHT_TIMEOUT,
        )
        .await?;
        if output.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        bail!(
            "failed to create OCI network '{}' ({}): {stderr}",
            network_name,
            output.status_description()
        )
    }

    async fn start_proxy(
        &self,
        image: &str,
        destinations: &BTreeSet<lionclaw_runtime_api::Destination>,
    ) -> Result<()> {
        let output = run_oci_preflight_command(
            &build_network_proxy_invocation(
                &self.engine,
                &self.internal_network_name,
                &self.egress_network_name,
                &self.proxy_name,
                image,
                destinations,
            )?,
            &format!("start OCI network proxy '{}'", self.proxy_name),
            OCI_PREFLIGHT_TIMEOUT,
        )
        .await?;
        if output.success() {
            self.ensure_proxy_running().await?;
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        bail!(
            "failed to start OCI network proxy '{}' ({}): {stderr}",
            self.proxy_name,
            output.status_description()
        )
    }

    async fn ensure_proxy_running(&self) -> Result<()> {
        let ready_by = Instant::now() + OCI_PREFLIGHT_TIMEOUT;
        let probe = loop {
            let output = run_oci_preflight_command(
                &build_container_running_inspect_invocation(&self.engine, &self.proxy_name),
                &format!("inspect OCI network proxy '{}'", self.proxy_name),
                OCI_PREFLIGHT_TIMEOUT,
            )
            .await?;
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !(output.success() && stdout == "true") {
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                bail!(
                    "OCI network proxy '{}' exited before readiness (inspect {}): stdout='{}' stderr='{}'",
                    self.proxy_name,
                    output.status_description(),
                    stdout,
                    stderr
                )
            }

            let probe = run_oci_preflight_command(
                &build_proxy_listener_probe_invocation(&self.engine, &self.proxy_name),
                &format!("probe OCI network proxy '{}'", self.proxy_name),
                OCI_PREFLIGHT_TIMEOUT,
            )
            .await?;
            if probe.success() {
                return Ok(());
            }
            if Instant::now() >= ready_by {
                break probe;
            }
            sleep(Duration::from_millis(50)).await;
        };

        let stdout = String::from_utf8_lossy(&probe.stdout).trim().to_string();
        let stderr = String::from_utf8_lossy(&probe.stderr).trim().to_string();
        bail!(
            "OCI network proxy '{}' did not bind proxy listeners before readiness (probe {}): stdout='{}' stderr='{}'",
            self.proxy_name,
            probe.status_description(),
            stdout,
            stderr
        )
    }

    async fn shutdown(mut self) -> Result<()> {
        let Some(cleanup) = self.cleanup.take() else {
            return Ok(());
        };
        cleanup.remove().await
    }
}

impl Drop for OciNetworkSession {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup.spawn();
        }
    }
}

#[derive(Debug)]
struct OciNetworkCleanup {
    engine: String,
    proxy_name: String,
    internal_network_name: String,
    egress_network_name: String,
}

impl OciNetworkCleanup {
    async fn remove(&self) -> Result<()> {
        let container = remove_oci_container(&self.engine, &self.proxy_name).await;
        let internal_network = remove_oci_network(&self.engine, &self.internal_network_name).await;
        let egress_network = remove_oci_network(&self.engine, &self.egress_network_name).await;
        container.and(internal_network).and(egress_network)
    }

    fn spawn(self) {
        if let Ok(handle) = Handle::try_current() {
            handle.spawn(async move {
                if let Err(error) = remove_oci_container(&self.engine, &self.proxy_name).await {
                    warn!(?error, "failed to clean up OCI network proxy container");
                }
                for (network_name, network_kind) in [
                    (&self.internal_network_name, "internal"),
                    (&self.egress_network_name, "egress"),
                ] {
                    if let Err(error) = remove_oci_network(&self.engine, network_name).await {
                        warn!(
                            ?error,
                            network_name, network_kind, "failed to clean up OCI network"
                        );
                    }
                }
            });
            return;
        }

        std::thread::spawn(move || {
            match std::process::Command::new(&self.engine)
                .args(["rm", "--force", "--ignore", &self.proxy_name])
                .status()
            {
                Ok(status) if status.success() => {}
                Ok(status) => warn!(
                    engine = %self.engine,
                    proxy_name = %self.proxy_name,
                    status = %status,
                    "OCI network proxy cleanup command failed"
                ),
                Err(error) => warn!(
                    ?error,
                    engine = %self.engine,
                    proxy_name = %self.proxy_name,
                    "failed to run OCI network proxy cleanup command"
                ),
            }
            for (network_name, network_kind) in [
                (&self.internal_network_name, "internal"),
                (&self.egress_network_name, "egress"),
            ] {
                match std::process::Command::new(&self.engine)
                    .args(["network", "rm", "--force", network_name])
                    .status()
                {
                    Ok(status) if status.success() => {}
                    Ok(status) => warn!(
                        engine = %self.engine,
                        network_name,
                        network_kind,
                        status = %status,
                        "OCI network cleanup command failed"
                    ),
                    Err(error) => warn!(
                        ?error,
                        engine = %self.engine,
                        network_name,
                        network_kind,
                        "failed to run OCI network cleanup command"
                    ),
                }
            }
        });
    }
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
        let Err(first_err) = self.remove().await else {
            return Ok(());
        };
        warn!(
            error = %first_err,
            secret_name = %self.secret_name,
            "runtime secret cleanup failed; retrying once"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        self.remove().await.with_context(|| {
            format!("runtime secret cleanup retry failed after initial error: {first_err}")
        })
    }

    async fn remove(&self) -> Result<()> {
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

fn build_network_create_invocation(
    engine: &str,
    network_name: &str,
    internal: bool,
) -> ProcessInvocation {
    let mut args = vec!["network".to_string(), "create".to_string()];
    if internal {
        args.push("--internal".to_string());
    }
    args.push(network_name.to_string());
    ProcessInvocation {
        executable: engine.to_string(),
        args,
        working_dir: None,
        environment: Vec::new(),
        input: String::new(),
    }
}

fn build_network_proxy_invocation(
    engine: &str,
    internal_network_name: &str,
    egress_network_name: &str,
    proxy_name: &str,
    image: &str,
    destinations: &BTreeSet<lionclaw_runtime_api::Destination>,
) -> Result<ProcessInvocation> {
    let mut args = vec![
        "run".to_string(),
        "--detach".to_string(),
        "--rm".to_string(),
        "--pull=never".to_string(),
        "--name".to_string(),
        proxy_name.to_string(),
        "--network".to_string(),
        format!("{internal_network_name}:alias={NETWORK_PROXY_ALIAS}"),
        "--network".to_string(),
        egress_network_name.to_string(),
        image.to_string(),
        NETWORK_PROXY_BINARY.to_string(),
        "__network-proxy".to_string(),
        "--http".to_string(),
        format!("0.0.0.0:{NETWORK_PROXY_HTTP_PORT}"),
        "--socks".to_string(),
        format!("0.0.0.0:{NETWORK_PROXY_SOCKS_PORT}"),
    ];
    for destination in destinations {
        for port in destination.ports() {
            args.push("--allow".to_string());
            args.push(format!("{}:{port}", destination.host()));
        }
    }
    Ok(ProcessInvocation {
        executable: engine.to_string(),
        args,
        working_dir: None,
        environment: Vec::new(),
        input: String::new(),
    })
}

fn build_container_running_inspect_invocation(
    engine: &str,
    container_name: &str,
) -> ProcessInvocation {
    ProcessInvocation {
        executable: engine.to_string(),
        args: vec![
            "inspect".to_string(),
            "--format".to_string(),
            "{{.State.Running}}".to_string(),
            container_name.to_string(),
        ],
        working_dir: None,
        environment: Vec::new(),
        input: String::new(),
    }
}

fn build_proxy_listener_probe_invocation(engine: &str, container_name: &str) -> ProcessInvocation {
    ProcessInvocation {
        executable: engine.to_string(),
        args: vec![
            "exec".to_string(),
            container_name.to_string(),
            NETWORK_PROXY_BINARY.to_string(),
            "__network-proxy-health".to_string(),
            "--http".to_string(),
            format!("127.0.0.1:{NETWORK_PROXY_HTTP_PORT}"),
            "--socks".to_string(),
            format!("127.0.0.1:{NETWORK_PROXY_SOCKS_PORT}"),
        ],
        working_dir: None,
        environment: Vec::new(),
        input: String::new(),
    }
}

/// Remove a named OCI container. Absence is success, making this suitable for
/// crash recovery and unconditional cleanup.
pub async fn remove_oci_container(engine: &str, name: &str) -> Result<()> {
    remove_oci_resource(
        engine,
        name,
        vec!["rm", "--force", "--ignore", name],
        "container",
    )
    .await
}

/// Remove a named OCI secret. Absence is success, making this suitable for
/// crash recovery and unconditional cleanup.
pub async fn remove_oci_secret(engine: &str, name: &str) -> Result<()> {
    remove_oci_resource(
        engine,
        name,
        vec!["secret", "rm", "--ignore", name],
        "secret",
    )
    .await
}

/// Remove a named OCI network. Absence is success, making this suitable for
/// crash recovery and unconditional cleanup.
pub async fn remove_oci_network(engine: &str, name: &str) -> Result<()> {
    remove_oci_resource(
        engine,
        name,
        vec!["network", "rm", "--force", name],
        "network",
    )
    .await
}

async fn remove_oci_resource(engine: &str, name: &str, args: Vec<&str>, kind: &str) -> Result<()> {
    validate_oci_resource_name(name)?;
    let output = run_oci_preflight_command(
        &ProcessInvocation {
            executable: engine.to_string(),
            args: args.into_iter().map(str::to_string).collect(),
            working_dir: None,
            environment: Vec::new(),
            input: String::new(),
        },
        &format!("remove OCI {kind} '{name}'"),
        OCI_PREFLIGHT_TIMEOUT,
    )
    .await?;
    if output.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    bail!(
        "failed to remove OCI {kind} '{name}' ({}): {stderr}",
        output.status_description()
    )
}

fn validate_oci_resource_name(name: &str) -> Result<()> {
    let mut chars = name.chars();
    if !chars.next().is_some_and(|c| c.is_ascii_alphanumeric())
        || !chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
    {
        bail!("invalid OCI resource name '{name}'");
    }
    Ok(())
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

fn format_volume_spec(source: &str, mount: &MountSpec, relabel: BindMountRelabel) -> String {
    let access = bind_mount_volume_access_option(mount.access);
    let relabel = relabel.volume_option();
    format!("{source}:{}:{access},{relabel}", mount.target)
}

fn format_bind_mount_arg(mount: &MountSpec) -> Result<(&'static str, String)> {
    format_bind_mount_arg_with_relabel(mount, bind_mount_relabel(mount))
}

fn format_private_bind_mount_arg(
    mount: &PreparedCredentialMount,
) -> Result<(&'static str, String)> {
    format_bind_mount_arg_with_relabel(mount.mount_spec(), BindMountRelabel::Private)
}

fn format_bind_mount_arg_with_relabel(
    mount: &MountSpec,
    relabel: BindMountRelabel,
) -> Result<(&'static str, String)> {
    let argument =
        podman_bind_mount_argument(&mount.source, &mount.target).map_err(anyhow::Error::msg)?;
    match argument.form {
        PodmanBindMountArgumentForm::Volume => Ok((
            "--volume",
            format_volume_spec(argument.source, mount, relabel),
        )),
        PodmanBindMountArgumentForm::Mount => Ok((
            "--mount",
            format_mount_spec(argument.source, mount, relabel),
        )),
    }
}

fn format_mount_spec(source: &str, mount: &MountSpec, relabel: BindMountRelabel) -> String {
    let access = bind_mount_mount_access_option(mount.access);
    let relabel = relabel.mount_option();
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
    configured_tmpfs: &[RuntimeTmpfsEntry],
) -> bool {
    mounts.iter().any(|mount| {
        mount.target == WORKSPACE_MOUNT_TARGET
            && fs::symlink_metadata(mount.source.join(LIONCLAW_METADATA_DIR)).is_ok()
    }) && !configured_tmpfs
        .iter()
        .any(|entry| entry.target() == WORKSPACE_LIONCLAW_METADATA_TMPFS_TARGET)
}

const WORKSPACE_LIONCLAW_METADATA_TMPFS_TARGET: &str = "/workspace/.lionclaw";

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

fn network_proxy_environment(network: &PreparedOciNetwork) -> Vec<(String, String)> {
    if matches!(network, PreparedOciNetwork::Deny) {
        return Vec::new();
    }
    let http = format!("http://{NETWORK_PROXY_ALIAS}:{NETWORK_PROXY_HTTP_PORT}");
    let socks = format!("socks5://{NETWORK_PROXY_ALIAS}:{NETWORK_PROXY_SOCKS_PORT}");
    [
        ("HTTP_PROXY".to_string(), http.clone()),
        ("HTTPS_PROXY".to_string(), http.clone()),
        ("http_proxy".to_string(), http.clone()),
        ("https_proxy".to_string(), http),
        ("ALL_PROXY".to_string(), socks.clone()),
        ("all_proxy".to_string(), socks),
        (
            "NO_PROXY".to_string(),
            "localhost,127.0.0.1,::1".to_string(),
        ),
        (
            "no_proxy".to_string(),
            "localhost,127.0.0.1,::1".to_string(),
        ),
    ]
    .into_iter()
    .collect()
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::{fs::PermissionsExt, net::UnixListener};
    use std::{collections::BTreeSet, fs, path::Path};

    use super::{
        build_oci_attached_process_invocation, build_oci_process_invocation, inspect_oci_image,
        prepare_oci_process_launch, prepare_oci_process_launch_with_runtime_auth,
        private_network_probe_reached_process_exec, OciExecutionBackend, OciImageReadiness,
    };
    use crate::backend::{ExecutionBackend, RUNTIME_SECRETS_NAME_PREFIX};
    use crate::runtime_auth::PreparedRuntimeAuth;
    use crate::{
        ConfinementConfig, EffectiveExecutionPlan, ExecutionLimits, ExecutionRequest,
        InstallPolicy, NetworkGrant, OciConfinementConfig, RuntimeProgramSpec, RuntimeSecretsMount,
        WorkspaceAccess,
    };
    use crate::{MountAccess, MountSpec};
    // Mirrors crate `lionclaw`'s project_inventory constants; tests only need the
    // literal values to exercise env/mount pass-through.
    const PROJECT_INSTANCE_ENV: &str = "LIONCLAW_PROJECT_INSTANCE";
    const PROJECT_INSTANCES_FILE_ENV: &str = "LIONCLAW_PROJECT_INSTANCES_FILE";
    const PROJECT_INSTANCE_INVENTORY_DIR: &str = "/lionclaw/project";
    const PROJECT_INSTANCES_FILE_PATH: &str = "/lionclaw/project/instances.json";
    #[cfg(unix)]
    use rustix::process::{getgid, getuid};
    use tempfile::tempdir;
    use tokio::sync::mpsc;

    #[cfg(unix)]
    fn write_executable(path: &Path, contents: &str) {
        fs::write(path, contents).expect("write executable");
        let mut permissions = fs::metadata(path)
            .expect("executable metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("chmod executable");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn oci_image_readiness_uses_one_bounded_engine_contract() {
        let temp_dir = tempdir().expect("tempdir");
        let engine = temp_dir.path().join("oci-engine");
        write_executable(
            &engine,
            r#"#!/usr/bin/env bash
set -eu
case "$1 $2" in
  "image exists")
    [ "$3" = "available:image" ]
    ;;
  "image inspect")
    [ "$5" = "available:image" ]
    printf '%s\n' 'sha256:0123456789abcdef'
    ;;
  *)
    exit 2
    ;;
esac
"#,
        );

        assert_eq!(
            inspect_oci_image(engine.to_str().expect("engine path"), "available:image").await,
            OciImageReadiness::Ready {
                identity: "sha256:0123456789abcdef".to_string(),
            }
        );
        assert_eq!(
            inspect_oci_image(engine.to_str().expect("engine path"), "missing:image").await,
            OciImageReadiness::Missing
        );
        assert_eq!(
            inspect_oci_image(
                temp_dir
                    .path()
                    .join("absent-engine")
                    .to_str()
                    .expect("engine path"),
                "available:image",
            )
            .await,
            OciImageReadiness::EngineUnavailable
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn oci_image_readiness_reaps_descendants_after_an_inspection_failure() {
        let temp_dir = tempdir().expect("tempdir");
        let engine = temp_dir.path().join("oci-engine");
        let descendant_pid = temp_dir.path().join("descendant.pid");
        write_executable(
            &engine,
            &format!(
                r#"#!/usr/bin/env bash
set -eu
case "$1 $2" in
  "image inspect")
    sleep 30 &
    child=$!
    printf '%s' "$child" > '{}'
    exit 42
    ;;
  "image exists")
    exit 0
    ;;
  *)
    exit 2
    ;;
esac
"#,
                descendant_pid.display()
            ),
        );

        assert_eq!(
            inspect_oci_image(engine.to_str().expect("engine path"), "broken:image").await,
            OciImageReadiness::InspectionFailed
        );

        assert_descendant_stopped(&descendant_pid).await;
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn oci_image_readiness_bounds_a_stalled_engine_and_its_descendants() {
        let temp_dir = tempdir().expect("tempdir");
        let engine = temp_dir.path().join("oci-engine");
        let descendant_pid = temp_dir.path().join("descendant.pid");
        write_executable(
            &engine,
            &format!(
                r#"#!/usr/bin/env bash
sleep 30 &
child=$!
printf '%s' "$child" > '{}'
wait "$child"
"#,
                descendant_pid.display()
            ),
        );

        assert_eq!(
            inspect_oci_image(engine.to_str().expect("engine path"), "available:image").await,
            OciImageReadiness::Retryable
        );

        assert_descendant_stopped(&descendant_pid).await;
    }

    #[cfg(target_os = "linux")]
    async fn assert_descendant_stopped(pid_file: &Path) {
        let pid = fs::read_to_string(pid_file)
            .expect("descendant pid")
            .parse::<u32>()
            .expect("numeric descendant pid");
        let proc_entry = std::path::PathBuf::from(format!("/proc/{pid}"));
        for _ in 0..50 {
            if !proc_entry.exists() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("completed OCI preflight left descendant process {pid} alive");
    }

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
    fn oci_backend_projects_one_explicit_effect_name_to_container_and_secret() {
        let mut request = sample_execution_request_with_runtime_secrets();
        let name = "lionclaw-effect-0123456789abcdef".to_string();
        request.resource_name = Some(name.clone());

        let invocation = build_oci_process_invocation(
            prepare_oci_process_launch(&request, Some(&name)).expect("prepare"),
            &[],
        );

        assert!(invocation
            .args
            .windows(2)
            .any(|pair| pair == ["--name".to_string(), name.clone()]));
        assert!(invocation
            .args
            .windows(2)
            .any(|pair| pair == ["--secret".to_string(), name.clone()]));
    }

    #[cfg(unix)]
    #[test]
    fn install_policy_user_keeps_keep_id_identity_args() {
        let mut request = sample_execution_request();
        request.plan.install_policy = InstallPolicy::User;
        request.plan.root_in_userns = false;

        let invocation = build_oci_process_invocation(
            prepare_oci_process_launch(&request, None).expect("prepare"),
            &[],
        );

        assert_keep_id_with_host_user(&invocation.args);
    }

    #[cfg(unix)]
    #[test]
    fn install_policy_none_keeps_keep_id_identity_args() {
        let mut request = sample_execution_request();
        request.plan.install_policy = InstallPolicy::None;
        request.plan.root_in_userns = false;

        let invocation = build_oci_process_invocation(
            prepare_oci_process_launch(&request, None).expect("prepare"),
            &[],
        );

        assert_keep_id_with_host_user(&invocation.args);
    }

    #[test]
    fn compiled_devices_become_explicit_oci_device_arguments() {
        let mut request = sample_execution_request();
        request.plan.devices.insert("/dev/dri".to_string());

        let launch = prepare_oci_process_launch(&request, None).expect("prepare");
        assert!(launch
            .args
            .windows(2)
            .any(|pair| { pair == ["--device".to_string(), "/dev/dri".to_string()] }));
    }

    #[test]
    fn absent_device_grants_emit_no_oci_device_arguments() {
        let request = sample_execution_request();
        let launch = prepare_oci_process_launch(&request, None).expect("prepare");
        assert!(!launch.args.iter().any(|arg| arg == "--device"));
    }

    #[cfg(unix)]
    #[test]
    fn install_policy_system_root_posture_uses_root_identity_args() {
        let mut request = sample_execution_request();
        request.plan.install_policy = InstallPolicy::System;
        request.plan.root_in_userns = true;

        let invocation = build_oci_process_invocation(
            prepare_oci_process_launch(&request, None).expect("prepare"),
            &[],
        );

        assert!(!invocation.args.iter().any(|arg| arg == "--userns"));
        assert!(invocation
            .args
            .windows(2)
            .any(|pair| pair == ["--user".to_string(), "0:0".to_string()]));
        assert!(invocation
            .args
            .windows(2)
            .any(|pair| pair == ["--network".to_string(), "none".to_string()]));
    }

    #[cfg(unix)]
    fn assert_keep_id_with_host_user(args: &[String]) {
        assert!(args
            .windows(2)
            .any(|pair| pair == ["--userns".to_string(), "keep-id".to_string()]));
        assert!(args.windows(2).any(|pair| {
            pair == [
                "--user".to_string(),
                format!("{}:{}", getuid().as_raw(), getgid().as_raw()),
            ]
        }));
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
    fn oci_backend_mounts_credentials_after_home_with_private_relabeling() {
        let request = sample_execution_request();
        let auth = PreparedRuntimeAuth::for_test(
            Some(MountSpec {
                source: "/validated/runtime/home".into(),
                target: "/runtime/home".to_string(),
                access: MountAccess::ReadWrite,
            }),
            vec![MountSpec {
                source: "/host/effect/auth/codex-auth".into(),
                target: "/runtime/home/.codex/auth.json".to_string(),
                access: MountAccess::ReadOnly,
            }],
        );

        let prepared = prepare_oci_process_launch_with_runtime_auth(&request, None, &auth)
            .expect("prepare auth overlay");
        let home = prepared
            .args
            .iter()
            .position(|arg| arg == "/validated/runtime/home:/runtime/home:rw,z")
            .expect("validated persistent home mount");
        let credential = prepared
            .args
            .iter()
            .position(|arg| {
                arg == "/host/effect/auth/codex-auth:/runtime/home/.codex/auth.json:ro,Z"
            })
            .expect("private credential mount");

        assert!(
            credential > home,
            "credential overlay must follow home mount"
        );
    }

    #[test]
    fn oci_backend_emits_channel_send_socket_mount() {
        let mut request = sample_execution_request();
        request.plan.mounts.push(MountSpec {
            source: "/host/runtime/sockets/channel-send-test.sock".into(),
            target: "/runtime/lionclaw/channel-send.sock".to_string(),
            access: MountAccess::ReadWrite,
        });

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
        assert!(!invocation
            .args
            .iter()
            .any(|arg| arg.contains("LIONCLAW_CHANNEL_SEND_SOCKET")));
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
        plan.network = NetworkGrant::Deny;

        let request = ExecutionRequest {
            plan,
            program: RuntimeProgramSpec {
                executable: "codex".to_string(),
                args: Vec::new(),
                environment: Vec::new(),
                stdin: String::new(),
                auth: None,
            },
            resource_name: None,
            runtime_secrets_mount: None,
            auth_staging_root: None,
            runtime_auth: None,
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
    fn oci_backend_adds_internal_network_and_proxy_env_for_destination_grant() {
        let mut plan = sample_plan();
        plan.network = NetworkGrant::allow_single("api.openai.com", 443).unwrap();
        let request = ExecutionRequest {
            plan,
            program: RuntimeProgramSpec::default(),
            resource_name: Some("lionclaw-effect-0123456789abcdef".to_string()),
            runtime_secrets_mount: None,
            auth_staging_root: None,
            runtime_auth: None,
        };

        let invocation = build_oci_process_invocation(
            prepare_oci_process_launch(&request, None).expect("prepare"),
            &[],
        );

        assert!(invocation.args.windows(2).any(|pair| pair
            == [
                "--network".to_string(),
                "lionclaw-effect-0123456789abcdef-net".to_string()
            ]));
        assert!(invocation.args.windows(2).any(|pair| {
            pair == [
                "--env".to_string(),
                "HTTPS_PROXY=http://lionclaw-proxy:3128".to_string(),
            ]
        }));
        assert!(invocation.args.windows(2).any(|pair| {
            pair == [
                "--env".to_string(),
                "ALL_PROXY=socks5://lionclaw-proxy:3129".to_string(),
            ]
        }));
    }

    #[test]
    fn oci_backend_rejects_destination_grant_without_effect_resource_name() {
        let mut plan = sample_plan();
        plan.network = NetworkGrant::allow_single("api.openai.com", 443).unwrap();
        let request = ExecutionRequest {
            plan,
            program: RuntimeProgramSpec::default(),
            resource_name: None,
            runtime_secrets_mount: None,
            auth_staging_root: None,
            runtime_auth: None,
        };

        let err = prepare_oci_process_launch(&request, None).expect_err("resource name required");

        assert!(err
            .to_string()
            .contains("destination-scoped OCI network requires an effect resource name"));
    }

    #[test]
    fn proxy_readiness_inspects_container_running_state() {
        let inspect = super::build_container_running_inspect_invocation("podman", "effect-proxy");

        assert_eq!(inspect.executable, "podman");
        assert_eq!(
            inspect.args,
            [
                "inspect".to_string(),
                "--format".to_string(),
                "{{.State.Running}}".to_string(),
                "effect-proxy".to_string(),
            ]
        );
    }

    #[test]
    fn proxy_readiness_probes_http_and_socks_listeners() {
        let probe = super::build_proxy_listener_probe_invocation("podman", "effect-proxy");

        assert_eq!(probe.executable, "podman");
        assert_eq!(probe.args[0], "exec");
        assert!(probe.args.contains(&"effect-proxy".to_string()));
        assert!(probe.args.contains(&"__network-proxy-health".to_string()));
        assert!(probe.args.contains(&"127.0.0.1:3128".to_string()));
        assert!(probe.args.contains(&"127.0.0.1:3129".to_string()));
    }

    #[test]
    fn oci_backend_builds_internal_network_and_dual_homed_proxy_invocations() {
        let destinations = BTreeSet::from([
            lionclaw_runtime_api::Destination::single("api.openai.com", 443).unwrap(),
            lionclaw_runtime_api::Destination::single("auth.openai.com", 443).unwrap(),
        ]);

        let internal_network = super::build_network_create_invocation("podman", "effect-net", true);
        assert_eq!(internal_network.executable, "podman");
        assert_eq!(
            internal_network.args,
            [
                "network".to_string(),
                "create".to_string(),
                "--internal".to_string(),
                "effect-net".to_string(),
            ]
        );
        let egress_network =
            super::build_network_create_invocation("podman", "effect-egress", false);
        assert_eq!(
            egress_network.args,
            [
                "network".to_string(),
                "create".to_string(),
                "effect-egress".to_string(),
            ]
        );

        let proxy = super::build_network_proxy_invocation(
            "podman",
            "effect-net",
            "effect-egress",
            "effect-proxy",
            "localhost/lionclaw-runtime:v1",
            &destinations,
        )
        .expect("proxy invocation");

        assert!(proxy.args.windows(2).any(|pair| {
            pair == [
                "--network".to_string(),
                "effect-net:alias=lionclaw-proxy".to_string(),
            ]
        }));
        assert!(proxy
            .args
            .windows(2)
            .any(|pair| pair == ["--network".to_string(), "effect-egress".to_string()]));
        assert!(!proxy
            .args
            .windows(2)
            .any(|pair| pair == ["--network".to_string(), "private".to_string()]));
        assert!(!proxy.args.iter().any(|arg| arg == "--mount"));
        let image = proxy
            .args
            .iter()
            .position(|arg| arg == "localhost/lionclaw-runtime:v1")
            .expect("image");
        assert_eq!(
            &proxy.args[image..image + 8],
            &[
                "localhost/lionclaw-runtime:v1".to_string(),
                "/usr/local/bin/lionclaw".to_string(),
                "__network-proxy".to_string(),
                "--http".to_string(),
                "0.0.0.0:3128".to_string(),
                "--socks".to_string(),
                "0.0.0.0:3129".to_string(),
                "--allow".to_string(),
            ]
        );
        assert!(proxy
            .args
            .windows(2)
            .any(|pair| pair == ["--allow".to_string(), "api.openai.com:443".to_string()]));
        assert!(proxy
            .args
            .windows(2)
            .any(|pair| pair == ["--allow".to_string(), "auth.openai.com:443".to_string()]));
    }

    #[test]
    fn oci_backend_keeps_runtime_auth_values_out_of_process_arguments() {
        let request = ExecutionRequest {
            plan: sample_plan(),
            program: RuntimeProgramSpec {
                executable: "/usr/local/bin/codex".to_string(),
                args: vec!["exec".to_string(), "--json".to_string()],
                environment: vec![(
                    "RUNTIME_AUTH_TOKEN".to_string(),
                    "stale-program-value".to_string(),
                )],
                stdin: String::new(),
                auth: None,
            },
            resource_name: None,
            runtime_secrets_mount: None,
            auth_staging_root: None,
            runtime_auth: None,
        };

        let invocation = build_oci_process_invocation(
            prepare_oci_process_launch(&request, None).expect("prepare"),
            &[(
                "RUNTIME_AUTH_TOKEN".to_string(),
                "provider-secret".to_string(),
            )],
        );

        #[cfg(unix)]
        assert!(invocation.args.windows(2).any(|pair| {
            pair == [
                "--user".to_string(),
                format!("{}:{}", getuid().as_raw(), getgid().as_raw()),
            ]
        }));
        assert!(invocation
            .args
            .windows(2)
            .any(|pair| { pair == ["--env".to_string(), "RUNTIME_AUTH_TOKEN".to_string()] }));
        assert!(!invocation
            .args
            .iter()
            .any(|arg| arg.contains("provider-secret") || arg.contains("stale-program-value")));
        assert!(invocation
            .args
            .windows(2)
            .any(|pair| { pair == ["--env".to_string(), "FOO=from-plan".to_string()] }));
        assert_eq!(
            invocation.environment,
            [(
                "RUNTIME_AUTH_TOKEN".to_string(),
                "provider-secret".to_string()
            )]
        );
        assert!(
            invocation
                .args
                .windows(2)
                .any(|pair| pair == ["--network".to_string(), "none".to_string()]),
            "runtime auth launch without destinations should have no egress"
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
                resource_name: None,
                runtime_secrets_mount: None,
                auth_staging_root: None,
                runtime_auth: None,
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
                resource_name: None,
                runtime_secrets_mount: None,
                auth_staging_root: None,
                runtime_auth: None,
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
        write_executable(&engine_path, &script);

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
            resource_name: None,
            runtime_secrets_mount: Some(RuntimeSecretsMount {
                source: temp_dir.path().join("runtime-secrets.env"),
            }),
            auth_staging_root: None,
            runtime_auth: None,
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

        let (stdout_tx, _stdout_rx) = mpsc::channel(8);
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
    async fn oci_backend_retries_runtime_secret_cleanup_without_failing_successful_turn() {
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
        write_executable(&engine_path, &script);

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
            resource_name: None,
            runtime_secrets_mount: Some(RuntimeSecretsMount {
                source: temp_dir.path().join("runtime-secrets.env"),
            }),
            auth_staging_root: None,
            runtime_auth: None,
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

        let (stdout_tx, _stdout_rx) = mpsc::channel(8);
        OciExecutionBackend
            .execute_streaming(request, stdout_tx)
            .await
            .expect("successful turn should not fail on cleanup race");

        let log = fs::read_to_string(&log_path).expect("read log");
        let remove_attempts = log
            .lines()
            .filter(|line| line.starts_with("secret rm "))
            .count();
        assert_eq!(remove_attempts, 2, "secret removal should be retried once");
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
            workspace_access: WorkspaceAccess::ReadWrite,
            network: NetworkGrant::Deny,
            install_policy: InstallPolicy::User,
            root_in_userns: false,
            working_dir: Some("/host/workspace/src".to_string()),
            environment: vec![("FOO".to_string(), "from-plan".to_string())],
            mcp_servers: Vec::new(),
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
            devices: Default::default(),
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
            resource_name: None,
            runtime_secrets_mount: None,
            auth_staging_root: None,
            runtime_auth: None,
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
