use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use tokio::{runtime::Handle, time::timeout};

use super::{
    auth_sidecar::{start_for_oci_execution, OciRuntimeAuthLaunch},
    backend::{ExecutionBackend, ExecutionOutput, ExecutionRequest, ExecutionStdoutSender},
    codex_auth_sidecar_image_ref,
    plan::{ConfinementBackend, MountAccess, MountSpec, NetworkMode, RuntimeAuthKind},
    process::{run_process_streaming, ProcessInvocation},
    OciConfinementConfig,
};
use crate::kernel::runtime::RuntimeSecretsMount;

#[derive(Debug, Default, Clone, Copy)]
pub struct OciExecutionBackend;

#[cfg(test)]
const OCI_IMAGE_PROBE_TIMEOUT: Duration = Duration::from_millis(250);
#[cfg(not(test))]
const OCI_IMAGE_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(test)]
const OCI_IMAGE_PULL_TIMEOUT: Duration = Duration::from_millis(250);
#[cfg(not(test))]
const OCI_IMAGE_PULL_TIMEOUT: Duration = Duration::from_secs(20);

// LionClaw bind-mounts local workspace and runtime state into confined OCI
// containers. On SELinux hosts those mounts are unreadable by default unless
// Podman relabels them for the container context, so we make private relabeling
// part of the canonical volume contract instead of special-casing individual
// mounts like /runtime.
const READ_ONLY_BIND_MOUNT_OPTIONS: &str = "ro,Z";
const READ_WRITE_BIND_MOUNT_OPTIONS: &str = "rw,Z";

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
        let runtime_secrets = ensure_runtime_secrets_registered(&request).await?;
        let prepared = prepare_oci_process_launch(&request)?;
        let runtime_auth = start_for_oci_execution(&request).await?;
        let invocation =
            build_oci_process_invocation(prepared, runtime_auth.as_ref().map(|auth| auth.launch()));
        let result = run_process_streaming(&invocation, move |line| {
            let _ = stdout.send(line.to_string());
            Ok(())
        })
        .await;
        let shutdown_result = match runtime_auth {
            Some(auth) => auth.shutdown().await,
            None => Ok(()),
        };
        let runtime_secrets_cleanup_result = match runtime_secrets {
            Some(cleanup) => cleanup.shutdown().await,
            None => Ok(()),
        };

        match (result, shutdown_result, runtime_secrets_cleanup_result) {
            (Ok(output), Ok(()), Ok(())) => Ok(output),
            (Err(err), _, _) => Err(err),
            (Ok(_), Err(err), _) => Err(err),
            (Ok(_), Ok(()), Err(err)) => Err(err),
        }
    }
}

pub async fn validate_oci_launch_prerequisites(
    runtime_id: &str,
    confinement: &OciConfinementConfig,
    required_auth: Option<RuntimeAuthKind>,
) -> Result<()> {
    let image = confinement.image.as_deref().ok_or_else(|| {
        anyhow!(
            "runtime '{}' requires a Podman runtime image in its confinement config",
            runtime_id
        )
    })?;

    // The runtime image is operator-managed, so launch preflight requires it to
    // exist locally instead of pulling an arbitrary mutable reference behind the
    // user's back.
    ensure_oci_image_exists(
        &confinement.engine,
        image,
        format!(
            "configured runtime image '{}' for runtime '{}'",
            image, runtime_id
        ),
    )
    .await?;

    if required_auth == Some(RuntimeAuthKind::Codex) {
        // The auth sidecar image is part of LionClaw's trusted boundary and is
        // pinned in code. We auto-pull it when missing so local interactive
        // Codex launches stay command-first instead of requiring separate setup.
        ensure_oci_image_pulled(
            &confinement.engine,
            codex_auth_sidecar_image_ref(),
            "pinned Codex auth sidecar image".to_string(),
        )
        .await?;
    }

    Ok(())
}

fn prepare_oci_process_launch(request: &ExecutionRequest) -> Result<PreparedOciProcessLaunch> {
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

    if let Some(working_dir) = request.plan.working_dir.as_deref() {
        args.push("--workdir".to_string());
        args.push(map_host_path_into_container(
            working_dir,
            &request.plan.mounts,
        )?);
    }

    for mount in &request.plan.mounts {
        args.push("--volume".to_string());
        args.push(format_volume_spec(mount)?);
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

    if let Some(runtime_secrets_mount) = &request.runtime_secrets_mount {
        args.push("--secret".to_string());
        args.push(runtime_secrets_mount.mounted_name());
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

async fn ensure_oci_image_exists(engine: &str, image: &str, description: String) -> Result<()> {
    match run_oci_image_probe(engine, image).await? {
        OciImageProbeResult::Present => Ok(()),
        OciImageProbeResult::Missing => bail!(
            "{} is not available locally; build or pull it before running LionClaw",
            description
        ),
    }
}

async fn ensure_oci_image_pulled(engine: &str, image: &str, description: String) -> Result<()> {
    match run_oci_image_probe(engine, image).await? {
        OciImageProbeResult::Present => return Ok(()),
        OciImageProbeResult::Missing => {}
    }

    let output = run_oci_preflight_command(
        &ProcessInvocation {
            executable: engine.to_string(),
            args: vec!["pull".to_string(), image.to_string()],
            working_dir: None,
            environment: Vec::new(),
            input: String::new(),
        },
        &format!("pull {}", description),
        OCI_IMAGE_PULL_TIMEOUT,
    )
    .await?;

    if output.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if stderr.is_empty() {
        bail!(
            "failed to pull {}; OCI engine exited with code {:?}",
            description,
            output.exit_code
        );
    }

    bail!("failed to pull {}: {}", description, stderr)
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
        &format!("inspect OCI image '{}'", image),
        OCI_IMAGE_PROBE_TIMEOUT,
    )
    .await?;

    match output.exit_code {
        Some(0) => Ok(OciImageProbeResult::Present),
        Some(1) => Ok(OciImageProbeResult::Missing),
        _ => {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            if stderr.is_empty() {
                bail!(
                    "failed to inspect OCI image '{}'; OCI engine exited with code {:?}",
                    image,
                    output.exit_code
                );
            }
            bail!("failed to inspect OCI image '{}': {}", image, stderr);
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

fn build_oci_process_invocation(
    prepared: PreparedOciProcessLaunch,
    runtime_auth: Option<&OciRuntimeAuthLaunch>,
) -> ProcessInvocation {
    let mut args = prepared.args;

    if let Some(pod_name) = runtime_auth.and_then(|auth| auth.pod_name.as_deref()) {
        args.push("--pod".to_string());
        args.push(pod_name.to_string());
    } else {
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
    }

    let mut environment = prepared.environment;
    if let Some(runtime_auth) = runtime_auth {
        environment = merged_environment(&environment, &runtime_auth.runtime_environment);
    }
    for (key, value) in environment {
        args.push("--env".to_string());
        args.push(format!("{key}={value}"));
    }

    args.push(prepared.image);
    args.push(prepared.program_executable);
    if let Some(runtime_auth) = runtime_auth {
        args.extend(runtime_auth.runtime_args_prefix.iter().cloned());
    }
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
    let output = run_process_streaming(
        &build_runtime_secret_create_invocation(&engine, mount)?,
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
            cleanup: Some(OciRuntimeSecretsCleanup {
                engine,
                secret_name: mount.mounted_name(),
            }),
        }));
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if stderr.is_empty() {
        bail!(
            "failed to register OCI runtime secrets; podman secret create exited with code {:?}",
            output.exit_code
        );
    }

    bail!("failed to register OCI runtime secrets: {}", stderr)
}

#[derive(Debug)]
struct OciRuntimeSecretsCleanup {
    engine: String,
    secret_name: String,
}

#[derive(Debug)]
struct OciRuntimeSecretsSession {
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
            OCI_IMAGE_PROBE_TIMEOUT,
        )
        .await?;

        if output.success() {
            return Ok(());
        }

        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if stderr.is_empty() {
            bail!(
                "failed to remove OCI runtime secret '{}'; OCI engine exited with code {:?}",
                self.secret_name,
                output.exit_code
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
                let _ = self.shutdown().await;
            });
            return;
        }

        std::thread::spawn(move || {
            let _ = std::process::Command::new(&self.engine)
                .args(["secret", "rm", &self.secret_name])
                .status();
        });
    }
}

fn build_runtime_secret_create_invocation(
    engine: &str,
    mount: &RuntimeSecretsMount,
) -> Result<ProcessInvocation> {
    Ok(ProcessInvocation {
        executable: engine.to_string(),
        args: vec![
            "secret".to_string(),
            "create".to_string(),
            "--replace".to_string(),
            mount.mounted_name(),
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

fn map_host_path_into_container(path: &str, mounts: &[MountSpec]) -> Result<String> {
    let requested = PathBuf::from(path);
    let (mount, relative) = longest_mount_prefix(&requested, mounts).ok_or_else(|| {
        anyhow!(
            "working directory '{}' is not inside any configured runtime mount",
            requested.display()
        )
    })?;

    let container_root = Path::new(&mount.target);
    let mapped = if relative.as_os_str().is_empty() {
        container_root.to_path_buf()
    } else {
        container_root.join(relative)
    };

    Ok(mapped.to_string_lossy().to_string())
}

fn longest_mount_prefix<'a>(
    requested: &Path,
    mounts: &'a [MountSpec],
) -> Option<(&'a MountSpec, PathBuf)> {
    mounts
        .iter()
        .filter_map(|mount| {
            strip_mount_prefix(requested, &mount.source).map(|relative| (mount, relative))
        })
        .max_by_key(|(mount, _)| mount.source.components().count())
}

fn strip_mount_prefix(requested: &Path, source: &Path) -> Option<PathBuf> {
    if requested == source {
        return Some(PathBuf::new());
    }

    requested.strip_prefix(source).ok().map(Path::to_path_buf)
}

fn format_volume_spec(mount: &MountSpec) -> Result<String> {
    let source = mount.source.to_str().ok_or_else(|| {
        anyhow!(
            "mount source '{}' is not valid UTF-8",
            mount.source.display()
        )
    })?;
    if source.contains(':') {
        bail!(
            "mount source '{}' contains ':' and cannot be represented safely as an OCI volume argument",
            mount.source.display()
        );
    }
    if mount.target.contains(':') {
        bail!(
            "mount target '{}' contains ':' and cannot be represented safely as an OCI volume argument",
            mount.target
        );
    }

    let access = match mount.access {
        MountAccess::ReadOnly => READ_ONLY_BIND_MOUNT_OPTIONS,
        MountAccess::ReadWrite => READ_WRITE_BIND_MOUNT_OPTIONS,
    };
    Ok(format!("{source}:{}:{access}", mount.target))
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
    use std::os::unix::fs::PermissionsExt;
    use std::time::Duration;

    use super::{build_oci_process_invocation, prepare_oci_process_launch, OciExecutionBackend};
    use crate::kernel::runtime::execution::auth_sidecar::OciRuntimeAuthLaunch;
    use crate::kernel::runtime::execution::backend::ExecutionBackend;
    use crate::kernel::runtime::{
        ConfinementConfig, EffectiveExecutionPlan, ExecutionLimits, ExecutionRequest, NetworkMode,
        OciConfinementConfig, RuntimeAuthKind, RuntimeProgramSpec, RuntimeSecretsMount,
        WorkspaceAccess,
    };
    use crate::kernel::runtime::{MountAccess, MountSpec};
    use tempfile::tempdir;
    use tokio::sync::mpsc;

    #[test]
    fn oci_backend_builds_podman_run_invocation() {
        let request = ExecutionRequest {
            plan: sample_plan(),
            program: RuntimeProgramSpec {
                executable: "/usr/local/bin/codex".to_string(),
                args: vec!["exec".to_string(), "--json".to_string()],
                environment: vec![("MODEL".to_string(), "gpt-5-codex".to_string())],
                stdin: "hello".to_string(),
                auth: None,
            },
            runtime_secrets_mount: Some(RuntimeSecretsMount {
                source: "/home/mosh/.lionclaw/config/runtime-secrets.env".into(),
            }),
            codex_home_override: None,
        };

        let invocation = build_oci_process_invocation(
            prepare_oci_process_launch(&request).expect("prepare"),
            None,
        );

        assert_eq!(invocation.executable, "podman");
        assert_eq!(invocation.working_dir, None);
        assert_eq!(invocation.environment, Vec::<(String, String)>::new());
        assert_eq!(invocation.input, "hello");

        assert!(invocation.args.starts_with(&[
            "run".to_string(),
            "--rm".to_string(),
            "--interactive".to_string(),
            "--read-only".to_string(),
        ]));
        assert!(invocation
            .args
            .windows(2)
            .any(|pair| { pair == ["--workdir".to_string(), "/workspace/src".to_string()] }));
        assert!(invocation.args.windows(2).any(|pair| {
            pair == [
                "--volume".to_string(),
                "/host/workspace:/workspace:rw,Z".to_string(),
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
                "/host/runtime/codex/dev/drafts:/drafts:rw,Z".to_string(),
            ]
        }));
        assert!(invocation
            .args
            .windows(2)
            .any(|pair| { pair == ["--volume".to_string(), "/host/refs:/refs:ro,Z".to_string()] }));
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
            codex_home_override: None,
        };

        let invocation = build_oci_process_invocation(
            prepare_oci_process_launch(&request).expect("prepare"),
            None,
        );

        assert!(invocation
            .args
            .windows(2)
            .any(|pair| { pair == ["--network".to_string(), "none".to_string()] }));
    }

    #[test]
    fn oci_backend_adds_private_network_flag_for_on_mode() {
        let request = ExecutionRequest {
            plan: sample_plan(),
            program: RuntimeProgramSpec::default(),
            runtime_secrets_mount: None,
            codex_home_override: None,
        };

        let invocation = build_oci_process_invocation(
            prepare_oci_process_launch(&request).expect("prepare"),
            None,
        );

        assert!(invocation
            .args
            .windows(2)
            .any(|pair| { pair == ["--network".to_string(), "private".to_string()] }));
    }

    #[test]
    fn oci_backend_runs_join_runtime_auth_pod_and_merges_runtime_env() {
        let request = ExecutionRequest {
            plan: sample_plan(),
            program: RuntimeProgramSpec {
                executable: "/usr/local/bin/codex".to_string(),
                args: vec!["exec".to_string(), "--json".to_string()],
                environment: Vec::new(),
                stdin: String::new(),
                auth: Some(RuntimeAuthKind::Codex),
            },
            runtime_secrets_mount: None,
            codex_home_override: None,
        };

        let invocation = build_oci_process_invocation(
            prepare_oci_process_launch(&request).expect("prepare"),
            Some(&OciRuntimeAuthLaunch {
                pod_name: Some("lionclaw-pod".to_string()),
                runtime_environment: vec![(
                    "OPENAI_API_KEY".to_string(),
                    "placeholder".to_string(),
                )],
                runtime_args_prefix: vec![
                    "-c".to_string(),
                    "openai_base_url=\"http://127.0.0.1:38080/v1\"".to_string(),
                ],
            }),
        );

        assert!(invocation
            .args
            .windows(2)
            .any(|pair| { pair == ["--pod".to_string(), "lionclaw-pod".to_string(),] }));
        assert!(invocation.args.windows(2).any(|pair| {
            pair == [
                "--env".to_string(),
                "OPENAI_API_KEY=placeholder".to_string(),
            ]
        }));
        let executable_index = invocation
            .args
            .iter()
            .position(|arg| arg == "/usr/local/bin/codex")
            .expect("executable arg");
        assert_eq!(
            &invocation.args[executable_index..executable_index + 5],
            &[
                "/usr/local/bin/codex".to_string(),
                "-c".to_string(),
                "openai_base_url=\"http://127.0.0.1:38080/v1\"".to_string(),
                "exec".to_string(),
                "--json".to_string(),
            ]
        );
        assert!(
            !invocation.args.iter().any(|arg| arg == "--network"),
            "runtime auth pod should replace direct network mode wiring"
        );
    }

    #[test]
    fn oci_backend_rejects_missing_image() {
        let mut plan = sample_plan();
        plan.confinement.oci_mut().image = None;

        let err = prepare_oci_process_launch(&ExecutionRequest {
            plan,
            program: RuntimeProgramSpec::default(),
            runtime_secrets_mount: None,
            codex_home_override: None,
        })
        .expect_err("missing image should fail");

        assert!(err.to_string().contains("requires a Podman runtime image"));
    }

    #[test]
    fn oci_backend_rejects_working_dir_outside_mounts() {
        let mut plan = sample_plan();
        plan.working_dir = Some("/outside".to_string());

        let err = prepare_oci_process_launch(&ExecutionRequest {
            plan,
            program: RuntimeProgramSpec::default(),
            runtime_secrets_mount: None,
            codex_home_override: None,
        })
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
            codex_home_override: None,
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
        let secret_name = request
            .runtime_secrets_mount
            .as_ref()
            .expect("mount")
            .mounted_name();
        assert!(
            log.contains(&format!("secret create --replace {}", secret_name)),
            "secret create should be logged: {log}"
        );
        assert!(
            log.contains(&format!("secret rm {}", secret_name)),
            "secret remove should be logged: {log}"
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
            workspace_access: WorkspaceAccess::ReadWrite,
            network_mode: NetworkMode::On,
            working_dir: Some("/host/workspace/src".to_string()),
            environment: vec![("FOO".to_string(), "from-plan".to_string())],
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
                    source: "/host/runtime/codex/dev/drafts".into(),
                    target: "/drafts".to_string(),
                    access: MountAccess::ReadWrite,
                },
                MountSpec {
                    source: "/host/refs".into(),
                    target: "/refs".to_string(),
                    access: MountAccess::ReadOnly,
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
}
