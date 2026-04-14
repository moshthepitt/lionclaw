use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use uuid::Uuid;

use super::{
    backend::ExecutionRequest,
    plan::{MountSpec, NetworkMode, RuntimeAuthKind},
    process::{run_process_streaming, ProcessInvocation, ProcessOutput},
};
use crate::{home::LionClawHome, kernel::continuity_fs::ContinuityFs};

const RUNTIME_MOUNT_TARGET: &str = "/runtime";
const OPENAI_API_KEY_ENV: &str = "OPENAI_API_KEY";
const OPENAI_UPSTREAM_HOST: &str = "api.openai.com";
const OPENAI_RESPONSES_PATH: &str = "/v1/responses";
const CODEX_PROXY_TOKEN_ENV: &str = "LIONCLAW_CODEX_OPENAI_PROXY_TOKEN";
const CODEX_PROXY_PORT: u16 = 38080;
const HAPROXY_IMAGE: &str = "docker.io/library/haproxy:3.3.5-alpine";
const HAPROXY_CONFIG_RELATIVE_PATH: &str = "home/.lionclaw/auth-proxy/haproxy.cfg";
const HAPROXY_CONFIG_CONTAINER_PATH: &str = "/usr/local/etc/haproxy/haproxy.cfg";
const CODEX_CONFIG_RELATIVE_PATH: &str = "home/.codex/config.toml";

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OciRuntimeAuthLaunch {
    pub pod_name: Option<String>,
    pub runtime_environment: Vec<(String, String)>,
}

pub struct OciRuntimeAuthSession {
    launch: OciRuntimeAuthLaunch,
    engine: String,
    pod_name: String,
}

impl OciRuntimeAuthSession {
    pub fn launch(&self) -> &OciRuntimeAuthLaunch {
        &self.launch
    }

    pub async fn shutdown(self) -> Result<()> {
        run_oci_admin_command(
            &build_pod_remove_invocation(&self.engine, &self.pod_name),
            "remove runtime auth pod",
        )
        .await
        .map(|_| ())
    }
}

pub async fn start_for_oci_execution(
    request: &ExecutionRequest,
) -> Result<Option<OciRuntimeAuthSession>> {
    match request.program.auth {
        None => Ok(None),
        Some(RuntimeAuthKind::CodexOpenAi) => Ok(Some(start_codex_openai_sidecar(request).await?)),
    }
}

async fn start_codex_openai_sidecar(request: &ExecutionRequest) -> Result<OciRuntimeAuthSession> {
    if request.plan.network_mode != NetworkMode::On {
        bail!(
            "runtime '{}' requires network-mode 'on' when Codex runtime auth is enabled",
            request.plan.runtime_id
        );
    }

    let runtime_auth_home = request.runtime_auth_home.clone().ok_or_else(|| {
        anyhow!(
            "runtime '{}' requires LIONCLAW_HOME/config/runtime-auth.env with OPENAI_API_KEY configured",
            request.plan.runtime_id
        )
    })?;
    let openai_api_key = read_required_runtime_auth_var(
        &runtime_auth_home,
        &request.plan.runtime_id,
        OPENAI_API_KEY_ENV,
    )
    .await?;

    let runtime_mount_fs = runtime_mount_fs(&request.plan.mounts)?;
    let placeholder_token = format!("lionclaw-placeholder-{}", Uuid::new_v4().simple());
    write_codex_config(&runtime_mount_fs)?;
    let haproxy_config_path =
        write_haproxy_config(&runtime_mount_fs, &placeholder_token, OPENAI_UPSTREAM_HOST)?;

    let engine = request.plan.confinement.oci().engine.clone();
    let pod_name = format!("lionclaw-runtime-{}", Uuid::new_v4().simple());
    let proxy_name = format!("lionclaw-auth-{}", Uuid::new_v4().simple());

    run_oci_admin_command(
        &build_pod_create_invocation(&engine, &pod_name),
        "create auth pod",
    )
    .await?;

    if let Err(err) = run_oci_admin_command(
        &build_sidecar_run_invocation(
            &engine,
            &pod_name,
            &proxy_name,
            &haproxy_config_path,
            &openai_api_key,
        )?,
        "start auth sidecar",
    )
    .await
    {
        let _ = run_oci_admin_command(
            &build_pod_remove_invocation(&engine, &pod_name),
            "remove failed auth pod",
        )
        .await;
        return Err(err);
    }

    Ok(OciRuntimeAuthSession {
        launch: OciRuntimeAuthLaunch {
            pod_name: Some(pod_name.clone()),
            runtime_environment: vec![(CODEX_PROXY_TOKEN_ENV.to_string(), placeholder_token)],
        },
        engine,
        pod_name,
    })
}

async fn read_required_runtime_auth_var(
    home: &LionClawHome,
    runtime_id: &str,
    key: &str,
) -> Result<String> {
    home.read_runtime_auth_var(key)
        .await?
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            anyhow!(
                "runtime '{}' requires {} in '{}'",
                runtime_id,
                key,
                home.runtime_auth_env_path().display()
            )
        })
}

fn build_pod_create_invocation(engine: &str, pod_name: &str) -> ProcessInvocation {
    ProcessInvocation {
        executable: engine.to_string(),
        args: vec![
            "pod".to_string(),
            "create".to_string(),
            "--name".to_string(),
            pod_name.to_string(),
            "--network".to_string(),
            "private".to_string(),
            "--no-hosts".to_string(),
        ],
        working_dir: None,
        environment: Vec::new(),
        input: String::new(),
    }
}

fn build_pod_remove_invocation(engine: &str, pod_name: &str) -> ProcessInvocation {
    ProcessInvocation {
        executable: engine.to_string(),
        args: vec![
            "pod".to_string(),
            "rm".to_string(),
            "--force".to_string(),
            pod_name.to_string(),
        ],
        working_dir: None,
        environment: Vec::new(),
        input: String::new(),
    }
}

fn build_sidecar_run_invocation(
    engine: &str,
    pod_name: &str,
    proxy_name: &str,
    haproxy_config_path: &Path,
    openai_api_key: &str,
) -> Result<ProcessInvocation> {
    Ok(ProcessInvocation {
        executable: engine.to_string(),
        args: vec![
            "run".to_string(),
            "--detach".to_string(),
            "--pod".to_string(),
            pod_name.to_string(),
            "--name".to_string(),
            proxy_name.to_string(),
            "--env".to_string(),
            OPENAI_API_KEY_ENV.to_string(),
            "--volume".to_string(),
            bind_mount_arg(haproxy_config_path, HAPROXY_CONFIG_CONTAINER_PATH, true)?,
            HAPROXY_IMAGE.to_string(),
        ],
        working_dir: None,
        environment: vec![(OPENAI_API_KEY_ENV.to_string(), openai_api_key.to_string())],
        input: String::new(),
    })
}

async fn run_oci_admin_command(
    invocation: &ProcessInvocation,
    action: &str,
) -> Result<ProcessOutput> {
    let output = run_process_streaming(invocation, |_| Ok(()))
        .await
        .with_context(|| {
            format!(
                "failed to {} using OCI engine '{}'",
                action, invocation.executable
            )
        })?;

    if output.success() {
        return Ok(output);
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if stderr.is_empty() {
        bail!(
            "failed to {}; OCI engine exited with code {:?}",
            action,
            output.exit_code
        );
    }

    bail!("failed to {}: {}", action, stderr)
}

fn runtime_mount_root(mounts: &[MountSpec]) -> Result<&Path> {
    mounts
        .iter()
        .find(|mount| mount.target == RUNTIME_MOUNT_TARGET)
        .map(|mount| mount.source.as_path())
        .ok_or_else(|| anyhow!("Codex runtime auth sidecar requires a /runtime mount"))
}

fn runtime_mount_fs(mounts: &[MountSpec]) -> Result<ContinuityFs> {
    let runtime_mount_root = runtime_mount_root(mounts)?;
    ContinuityFs::bootstrap(runtime_mount_root)
        .with_context(|| format!("failed to open {}", runtime_mount_root.display()))
}

fn write_codex_config(runtime_mount_fs: &ContinuityFs) -> Result<()> {
    let contents = format!(
        "[model_providers.openai]\nbase_url = \"http://127.0.0.1:{CODEX_PROXY_PORT}/v1\"\nenv_key = \"{CODEX_PROXY_TOKEN_ENV}\"\nwire_api = \"responses\"\n"
    );
    runtime_mount_fs.write_string(Path::new(CODEX_CONFIG_RELATIVE_PATH), &contents)
}

fn write_haproxy_config(
    runtime_mount_fs: &ContinuityFs,
    placeholder_token: &str,
    upstream_host: &str,
) -> Result<PathBuf> {
    let contents = format!(
        "global\n    log stdout format raw local0\n\ndefaults\n    mode http\n    timeout connect 10s\n    timeout client 5m\n    timeout server 5m\n\nfrontend codex_openai_ingress\n    bind 127.0.0.1:{CODEX_PROXY_PORT}\n    acl expected_auth req.hdr(authorization) -m str \"Bearer {placeholder_token}\"\n    acl allowed_method method POST\n    acl allowed_path path {OPENAI_RESPONSES_PATH}\n    http-request deny deny_status 401 unless expected_auth\n    http-request deny deny_status 405 unless allowed_method\n    http-request deny deny_status 404 unless allowed_path\n    default_backend codex_openai_upstream\n\nbackend codex_openai_upstream\n    http-request set-header Authorization \"Bearer %[env({OPENAI_API_KEY_ENV})]\"\n    http-request set-header Host {upstream_host}\n    server openai {upstream_host}:443 ssl verify required ca-file @system-ca sni str({upstream_host})\n"
    );
    let relative = Path::new(HAPROXY_CONFIG_RELATIVE_PATH);
    runtime_mount_fs.write_string(relative, &contents)?;
    Ok(runtime_mount_fs.absolute_path(relative))
}

fn bind_mount_arg(source: &Path, target: &str, read_only: bool) -> Result<String> {
    let source = source
        .to_str()
        .ok_or_else(|| anyhow!("mount source '{}' is not valid UTF-8", source.display()))?;
    if source.contains(':') {
        bail!(
            "mount source '{}' contains ':' and cannot be represented safely as an OCI volume argument",
            source
        );
    }
    if target.contains(':') {
        bail!(
            "mount target '{}' contains ':' and cannot be represented safely as an OCI volume argument",
            target
        );
    }

    let access = if read_only { "ro" } else { "rw" };
    Ok(format!("{source}:{target}:{access}"))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        home::LionClawHome,
        kernel::runtime::{
            ConfinementConfig, EffectiveExecutionPlan, ExecutionLimits, OciConfinementConfig,
            RuntimeProgramSpec, WorkspaceAccess,
        },
    };

    fn sample_request(runtime_root: PathBuf, home: LionClawHome) -> ExecutionRequest {
        ExecutionRequest {
            plan: EffectiveExecutionPlan {
                runtime_id: "codex".to_string(),
                preset_name: "everyday".to_string(),
                confinement: ConfinementConfig::Oci(OciConfinementConfig::default()),
                workspace_access: WorkspaceAccess::ReadWrite,
                network_mode: NetworkMode::On,
                working_dir: None,
                environment: Vec::new(),
                idle_timeout: std::time::Duration::from_secs(30),
                hard_timeout: std::time::Duration::from_secs(90),
                mounts: vec![MountSpec {
                    source: runtime_root,
                    target: "/runtime".to_string(),
                    access: super::super::plan::MountAccess::ReadWrite,
                }],
                mount_runtime_secrets: false,
                escape_classes: Default::default(),
                limits: ExecutionLimits::default(),
            },
            program: RuntimeProgramSpec {
                executable: "codex".to_string(),
                args: vec!["exec".to_string()],
                environment: Vec::new(),
                stdin: String::new(),
                auth: Some(RuntimeAuthKind::CodexOpenAi),
            },
            runtime_secrets_mount: None,
            runtime_auth_home: Some(home),
        }
    }

    #[test]
    fn pod_create_invocation_uses_private_network_without_hosts_file() {
        let invocation = build_pod_create_invocation("podman", "lionclaw-pod");

        assert_eq!(invocation.executable, "podman");
        assert_eq!(
            invocation.args,
            vec![
                "pod".to_string(),
                "create".to_string(),
                "--name".to_string(),
                "lionclaw-pod".to_string(),
                "--network".to_string(),
                "private".to_string(),
                "--no-hosts".to_string(),
            ]
        );
    }

    #[test]
    fn sidecar_run_invocation_mounts_only_config_and_real_key() {
        let invocation = build_sidecar_run_invocation(
            "podman",
            "lionclaw-pod",
            "lionclaw-proxy",
            Path::new("/tmp/runtime/home/.lionclaw/auth-proxy/haproxy.cfg"),
            "sk-real",
        )
        .expect("invocation");

        assert!(invocation
            .args
            .windows(2)
            .any(|pair| { pair == ["--pod".to_string(), "lionclaw-pod".to_string(),] }));
        assert!(invocation
            .args
            .windows(2)
            .any(|pair| { pair == ["--name".to_string(), "lionclaw-proxy".to_string(),] }));
        assert!(invocation
            .args
            .windows(2)
            .any(|pair| { pair == ["--env".to_string(), "OPENAI_API_KEY".to_string(),] }));
        assert_eq!(
            invocation.environment,
            vec![("OPENAI_API_KEY".to_string(), "sk-real".to_string())]
        );
        assert!(invocation.args.windows(2).any(|pair| {
            pair
                == [
                    "--volume".to_string(),
                    "/tmp/runtime/home/.lionclaw/auth-proxy/haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro".to_string(),
                ]
        }));
        assert_eq!(
            invocation.args.last().map(String::as_str),
            Some(HAPROXY_IMAGE)
        );
    }

    #[tokio::test]
    async fn codex_runtime_auth_sidecar_writes_runtime_and_proxy_config() {
        let temp_dir = tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        home.ensure_base_dirs().await.expect("base dirs");
        tokio::fs::write(home.runtime_auth_env_path(), "OPENAI_API_KEY=sk-real\n")
            .await
            .expect("write runtime auth");

        let runtime_root = temp_dir.path().join("runtime-session");
        tokio::fs::create_dir_all(&runtime_root)
            .await
            .expect("create runtime root");

        let request = sample_request(runtime_root.clone(), home);
        let runtime_mount_fs = runtime_mount_fs(&request.plan.mounts).expect("runtime fs");
        let placeholder = "lionclaw-placeholder-test";

        write_codex_config(&runtime_mount_fs).expect("write codex config");
        let haproxy_path =
            write_haproxy_config(&runtime_mount_fs, placeholder, OPENAI_UPSTREAM_HOST)
                .expect("write haproxy config");

        let codex_config = tokio::fs::read_to_string(runtime_root.join(CODEX_CONFIG_RELATIVE_PATH))
            .await
            .expect("read codex config");
        assert!(codex_config.contains("http://127.0.0.1:38080/v1"));
        assert!(codex_config.contains(CODEX_PROXY_TOKEN_ENV));
        assert!(!codex_config.contains(OPENAI_API_KEY_ENV));

        let haproxy_config = tokio::fs::read_to_string(&haproxy_path)
            .await
            .expect("read haproxy config");
        assert!(haproxy_config.contains(OPENAI_RESPONSES_PATH));
        assert!(haproxy_config.contains(placeholder));
        assert!(haproxy_config.contains("%[env(OPENAI_API_KEY)]"));
        assert!(!haproxy_config.contains("sk-real"));
    }

    #[tokio::test]
    async fn codex_runtime_auth_sidecar_rejects_symlinked_runtime_proxy_dir() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let temp_dir = tempdir().expect("temp dir");
            let runtime_root = temp_dir.path().join("runtime-session");
            tokio::fs::create_dir_all(runtime_root.join("home/.lionclaw"))
                .await
                .expect("create parent");
            let outside = temp_dir.path().join("outside");
            tokio::fs::create_dir_all(&outside)
                .await
                .expect("create outside");
            symlink(&outside, runtime_root.join("home/.lionclaw/auth-proxy")).expect("symlink");

            let runtime_mount_fs = ContinuityFs::bootstrap(&runtime_root).expect("runtime fs");
            let err = write_haproxy_config(&runtime_mount_fs, "placeholder", OPENAI_UPSTREAM_HOST)
                .expect_err("symlinked proxy dir should fail");
            assert!(err.to_string().contains("auth-proxy"));
        }
    }

    #[tokio::test]
    async fn codex_runtime_auth_sidecar_rejects_symlinked_runtime_codex_dir() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let temp_dir = tempdir().expect("temp dir");
            let runtime_root = temp_dir.path().join("runtime-session");
            tokio::fs::create_dir_all(runtime_root.join("home"))
                .await
                .expect("create parent");
            let outside = temp_dir.path().join("outside");
            tokio::fs::create_dir_all(&outside)
                .await
                .expect("create outside");
            symlink(&outside, runtime_root.join("home/.codex")).expect("symlink");

            let runtime_mount_fs = ContinuityFs::bootstrap(&runtime_root).expect("runtime fs");
            let err =
                write_codex_config(&runtime_mount_fs).expect_err("symlinked codex dir should fail");
            assert!(err.to_string().contains(".codex"));
        }
    }
}
