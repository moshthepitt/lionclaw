use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;

use super::{
    backend::{ExecutionBackend, ExecutionOutput, ExecutionRequest, ExecutionStdoutSender},
    plan::{ConfinementBackend, MountAccess, MountSpec, NetworkMode},
    process::{run_process_streaming, ProcessInvocation},
};

#[derive(Debug, Default, Clone, Copy)]
pub struct OciExecutionBackend;

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
        ensure_runtime_secrets_registered(&request).await?;
        let invocation = build_oci_process_invocation(&request)?;
        run_process_streaming(&invocation, move |line| {
            let _ = stdout.send(line.to_string());
            Ok(())
        })
        .await
    }
}

fn build_oci_process_invocation(request: &ExecutionRequest) -> Result<ProcessInvocation> {
    let config = request.plan.confinement.oci();

    let image = config.image.as_deref().ok_or_else(|| {
        anyhow!(
            "runtime '{}' requires an OCI image in its confinement config",
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

    match request.plan.network_mode {
        NetworkMode::None => {
            args.push("--network".to_string());
            args.push("none".to_string());
        }
        NetworkMode::On => {}
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

    for (key, value) in merged_environment(&request.plan.environment, &request.program.environment)
    {
        args.push("--env".to_string());
        args.push(format!("{key}={value}"));
    }

    if request.runtime_secrets_mount.is_some() {
        args.push("--secret".to_string());
        args.push(
            request
                .runtime_secrets_mount
                .as_ref()
                .expect("checked above")
                .mounted_name(),
        );
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

    args.push(image.to_string());
    args.push(request.program.executable.clone());
    args.extend(request.program.args.clone());

    Ok(ProcessInvocation {
        executable: config.engine.clone(),
        args,
        working_dir: None,
        environment: Vec::new(),
        input: request.program.stdin.clone(),
    })
}

async fn ensure_runtime_secrets_registered(request: &ExecutionRequest) -> Result<()> {
    let Some(mount) = request.runtime_secrets_mount.as_ref() else {
        return Ok(());
    };
    let engine = request.plan.confinement.oci().engine.clone();
    let output = run_process_streaming(
        &ProcessInvocation {
            executable: engine.clone(),
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
        },
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
        return Ok(());
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
        MountAccess::ReadOnly => "ro",
        MountAccess::ReadWrite => "rw",
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
    use std::time::Duration;

    use super::build_oci_process_invocation;
    use crate::kernel::runtime::{
        ConfinementConfig, EffectiveExecutionPlan, ExecutionLimits, ExecutionRequest, NetworkMode,
        OciConfinementConfig, RuntimeProgramSpec, RuntimeSecretsMount, WorkspaceAccess,
    };
    use crate::kernel::runtime::{MountAccess, MountSpec};

    #[test]
    fn oci_backend_builds_podman_run_invocation() {
        let request = ExecutionRequest {
            plan: sample_plan(),
            program: RuntimeProgramSpec {
                executable: "/usr/local/bin/codex".to_string(),
                args: vec!["exec".to_string(), "--json".to_string()],
                environment: vec![("MODEL".to_string(), "gpt-5-codex".to_string())],
                stdin: "hello".to_string(),
            },
            runtime_secrets_mount: Some(RuntimeSecretsMount {
                source: "/home/mosh/.lionclaw/config/runtime-secrets.env".into(),
            }),
        };

        let invocation = build_oci_process_invocation(&request).expect("invocation");

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
                "/host/workspace:/workspace:rw".to_string(),
            ]
        }));
        assert!(invocation.args.windows(2).any(|pair| {
            pair == [
                "--volume".to_string(),
                "/host/runtime/codex/dev:/runtime:rw".to_string(),
            ]
        }));
        assert!(invocation.args.windows(2).any(|pair| {
            pair == [
                "--volume".to_string(),
                "/host/runtime/codex/dev/drafts:/drafts:rw".to_string(),
            ]
        }));
        assert!(invocation
            .args
            .windows(2)
            .any(|pair| { pair == ["--volume".to_string(), "/host/refs:/refs:ro".to_string()] }));
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

        let invocation = build_oci_process_invocation(&ExecutionRequest {
            plan,
            program: RuntimeProgramSpec {
                executable: "codex".to_string(),
                args: Vec::new(),
                environment: Vec::new(),
                stdin: String::new(),
            },
            runtime_secrets_mount: None,
        })
        .expect("invocation");

        assert!(invocation
            .args
            .windows(2)
            .any(|pair| { pair == ["--network".to_string(), "none".to_string()] }));
    }

    #[test]
    fn oci_backend_leaves_network_unset_for_on_mode() {
        let invocation = build_oci_process_invocation(&ExecutionRequest {
            plan: sample_plan(),
            program: RuntimeProgramSpec::default(),
            runtime_secrets_mount: None,
        })
        .expect("invocation");

        assert!(
            !invocation.args.iter().any(|arg| arg == "--network"),
            "network=on should use the engine default private network mode"
        );
    }

    #[test]
    fn oci_backend_rejects_missing_image() {
        let mut plan = sample_plan();
        plan.confinement.oci_mut().image = None;

        let err = build_oci_process_invocation(&ExecutionRequest {
            plan,
            program: RuntimeProgramSpec::default(),
            runtime_secrets_mount: None,
        })
        .expect_err("missing image should fail");

        assert!(err.to_string().contains("requires an OCI image"));
    }

    #[test]
    fn oci_backend_rejects_working_dir_outside_mounts() {
        let mut plan = sample_plan();
        plan.working_dir = Some("/outside".to_string());

        let err = build_oci_process_invocation(&ExecutionRequest {
            plan,
            program: RuntimeProgramSpec::default(),
            runtime_secrets_mount: None,
        })
        .expect_err("working dir should fail");

        assert!(err
            .to_string()
            .contains("is not inside any configured runtime mount"));
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
