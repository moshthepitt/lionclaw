use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    runtime::Handle,
    time::{sleep, timeout, Duration, Instant},
};
use uuid::Uuid;

use super::{
    backend::ExecutionRequest,
    plan::{MountSpec, NetworkMode, RuntimeAuthKind},
    process::{run_process_streaming, ProcessInvocation, ProcessOutput},
};
use crate::{
    kernel::continuity_fs::ContinuityFs,
    kernel::runtime::{resolve_codex_host_auth, CodexHostAuthMode},
};

const RUNTIME_MOUNT_TARGET: &str = "/runtime";
const UPSTREAM_BEARER_TOKEN_ENV: &str = "LIONCLAW_CODEX_UPSTREAM_BEARER_TOKEN";
const CODEX_PROXY_TOKEN_ENV: &str = "LIONCLAW_CODEX_OPENAI_PROXY_TOKEN";
const CODEX_PROXY_PORT: u16 = 38080;
const HAPROXY_IMAGE: &str = "docker.io/library/haproxy:3.3.5-alpine";
const SIDECAR_CONFIG_CONTAINER_DIR: &str = "/usr/local/etc/haproxy";
const SIDECAR_RUN_CONTAINER_DIR: &str = "/var/lib/haproxy";
const HAPROXY_CONFIG_FILE_NAME: &str = "haproxy.cfg";
const HAPROXY_ADMIN_SOCKET_FILE_NAME: &str = "admin.sock";
const HAPROXY_ADMIN_SOCKET_CONTAINER_PATH: &str = "/var/lib/haproxy/admin.sock";
const CODEX_CONFIG_RELATIVE_PATH: &str = "home/.codex/config.toml";
const SIDECAR_READY_TIMEOUT: Duration = Duration::from_secs(5);
const SIDECAR_READY_POLL_INTERVAL: Duration = Duration::from_millis(50);
const SIDECAR_READY_IO_TIMEOUT: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OciRuntimeAuthLaunch {
    pub pod_name: Option<String>,
    pub runtime_environment: Vec<(String, String)>,
}

#[derive(Debug)]
struct SidecarStateDir {
    temp_dir: TempDir,
}

impl SidecarStateDir {
    fn new() -> Result<Self> {
        let temp_dir = tempfile::tempdir().context("failed to create auth sidecar temp dir")?;
        let config_dir = temp_dir.path().join("config");
        std::fs::create_dir(&config_dir)
            .with_context(|| format!("failed to create {}", config_dir.display()))?;
        let run_dir = temp_dir.path().join("run");
        std::fs::create_dir(&run_dir)
            .with_context(|| format!("failed to create {}", run_dir.display()))?;
        set_sidecar_config_dir_permissions(&config_dir)?;
        set_sidecar_run_dir_permissions(&run_dir)?;
        Ok(Self { temp_dir })
    }

    fn root(&self) -> &Path {
        self.temp_dir.path()
    }

    fn config_dir(&self) -> PathBuf {
        self.root().join("config")
    }

    fn run_dir(&self) -> PathBuf {
        self.root().join("run")
    }

    fn config_path(&self) -> PathBuf {
        self.config_dir().join(HAPROXY_CONFIG_FILE_NAME)
    }

    fn admin_socket_path(&self) -> PathBuf {
        self.run_dir().join(HAPROXY_ADMIN_SOCKET_FILE_NAME)
    }
}

#[derive(Debug)]
struct OciRuntimeAuthCleanup {
    engine: String,
    pod_name: String,
    sidecar_state: SidecarStateDir,
}

impl OciRuntimeAuthCleanup {
    async fn shutdown(self) -> Result<()> {
        run_oci_admin_command(
            &build_pod_remove_invocation(&self.engine, &self.pod_name),
            "remove runtime auth pod",
        )
        .await
        .map(|_| ())
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
                .args(["pod", "rm", "--force", &self.pod_name])
                .status();
        });
    }
}

#[derive(Debug)]
pub struct OciRuntimeAuthSession {
    launch: OciRuntimeAuthLaunch,
    cleanup: Option<OciRuntimeAuthCleanup>,
}

impl OciRuntimeAuthSession {
    pub fn launch(&self) -> &OciRuntimeAuthLaunch {
        &self.launch
    }

    pub async fn shutdown(mut self) -> Result<()> {
        let Some(cleanup) = self.cleanup.take() else {
            return Ok(());
        };
        cleanup.shutdown().await
    }
}

impl Drop for OciRuntimeAuthSession {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup.spawn();
        }
    }
}

pub async fn start_for_oci_execution(
    request: &ExecutionRequest,
) -> Result<Option<OciRuntimeAuthSession>> {
    match request.program.auth {
        None => Ok(None),
        Some(RuntimeAuthKind::Codex) => Ok(Some(start_codex_sidecar(request).await?)),
    }
}

async fn start_codex_sidecar(request: &ExecutionRequest) -> Result<OciRuntimeAuthSession> {
    if request.plan.network_mode != NetworkMode::On {
        bail!(
            "runtime '{}' requires network-mode 'on' when Codex runtime auth is enabled",
            request.plan.runtime_id
        );
    }

    let host_auth = resolve_codex_host_auth(request.codex_home_override.as_deref()).await?;

    let runtime_mount_fs = runtime_mount_fs(&request.plan.mounts)?;
    let placeholder_token = format!("lionclaw-placeholder-{}", Uuid::new_v4().simple());
    write_codex_config(&runtime_mount_fs, host_auth.mode())?;

    let sidecar_state = SidecarStateDir::new()?;
    write_haproxy_config(&sidecar_state, &placeholder_token, host_auth.mode())?;

    let engine = request.plan.confinement.oci().engine.clone();
    let pod_name = format!("lionclaw-runtime-{}", Uuid::new_v4().simple());
    let proxy_name = format!("lionclaw-auth-{}", Uuid::new_v4().simple());
    let sidecar_run_invocation = build_sidecar_run_invocation(
        &engine,
        &pod_name,
        &proxy_name,
        &sidecar_state,
        host_auth.bearer_token(),
    )?;

    run_oci_admin_command(
        &build_pod_create_invocation(&engine, &pod_name),
        "create auth pod",
    )
    .await?;

    let cleanup = OciRuntimeAuthCleanup {
        engine: engine.clone(),
        pod_name: pod_name.clone(),
        sidecar_state,
    };

    if let Err(err) = run_oci_admin_command(&sidecar_run_invocation, "start auth sidecar").await {
        let _ = cleanup.shutdown().await;
        return Err(err);
    }

    if let Err(err) = wait_for_sidecar_ready(cleanup.sidecar_state.admin_socket_path()).await {
        let _ = cleanup.shutdown().await;
        return Err(err);
    }

    Ok(OciRuntimeAuthSession {
        launch: OciRuntimeAuthLaunch {
            pod_name: Some(pod_name),
            runtime_environment: vec![(CODEX_PROXY_TOKEN_ENV.to_string(), placeholder_token)],
        },
        cleanup: Some(cleanup),
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
    sidecar_state: &SidecarStateDir,
    upstream_bearer_token: &str,
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
            UPSTREAM_BEARER_TOKEN_ENV.to_string(),
            "--volume".to_string(),
            bind_mount_arg(
                &sidecar_state.config_dir(),
                SIDECAR_CONFIG_CONTAINER_DIR,
                true,
            )?,
            "--volume".to_string(),
            bind_mount_arg(&sidecar_state.run_dir(), SIDECAR_RUN_CONTAINER_DIR, false)?,
            HAPROXY_IMAGE.to_string(),
        ],
        working_dir: None,
        environment: vec![(
            UPSTREAM_BEARER_TOKEN_ENV.to_string(),
            upstream_bearer_token.to_string(),
        )],
        input: String::new(),
    })
}

async fn wait_for_sidecar_ready(admin_socket_path: PathBuf) -> Result<()> {
    let start = Instant::now();
    let mut last_error: Option<anyhow::Error> = None;

    while start.elapsed() < SIDECAR_READY_TIMEOUT {
        match probe_sidecar_admin_socket(&admin_socket_path).await {
            Ok(()) => return Ok(()),
            Err(err) => last_error = Some(err),
        }
        sleep(SIDECAR_READY_POLL_INTERVAL).await;
    }

    let detail = last_error
        .map(|err| err.to_string())
        .unwrap_or_else(|| "sidecar did not become ready".to_string());
    bail!("auth sidecar failed readiness probe: {}", detail)
}

async fn probe_sidecar_admin_socket(admin_socket_path: &Path) -> Result<()> {
    let mut stream = UnixStream::connect(admin_socket_path)
        .await
        .with_context(|| format!("failed to connect to {}", admin_socket_path.display()))?;
    stream
        .write_all(b"show info\n")
        .await
        .context("failed to write auth sidecar readiness probe")?;
    let mut response = [0u8; 1];
    match timeout(SIDECAR_READY_IO_TIMEOUT, stream.read(&mut response)).await {
        Ok(Ok(read)) if read > 0 => Ok(()),
        Ok(Ok(_)) => bail!("auth sidecar readiness probe returned no data"),
        Ok(Err(err)) => Err(err).context("failed to read auth sidecar readiness probe"),
        Err(_) => bail!("auth sidecar readiness probe timed out"),
    }
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

fn write_codex_config(runtime_mount_fs: &ContinuityFs, auth_mode: CodexHostAuthMode) -> Result<()> {
    let contents = format!(
        "[model_providers.openai]\nbase_url = \"http://127.0.0.1:{CODEX_PROXY_PORT}{base_path}\"\nenv_key = \"{CODEX_PROXY_TOKEN_ENV}\"\nwire_api = \"responses\"\n",
        base_path = auth_mode.upstream_base_path(),
    );
    runtime_mount_fs.write_string(Path::new(CODEX_CONFIG_RELATIVE_PATH), &contents)
}

fn write_haproxy_config(
    sidecar_state: &SidecarStateDir,
    placeholder_token: &str,
    auth_mode: CodexHostAuthMode,
) -> Result<()> {
    let upstream_host = auth_mode.upstream_host();
    let responses_path = format!("{}/responses", auth_mode.upstream_base_path());
    let contents = format!(
        "global\n    log stdout format raw local0\n    stats socket {HAPROXY_ADMIN_SOCKET_CONTAINER_PATH} mode 600 level admin\n\ndefaults\n    mode http\n    timeout connect 10s\n    timeout client 5m\n    timeout server 5m\n\nfrontend codex_openai_ingress\n    bind 127.0.0.1:{CODEX_PROXY_PORT}\n    acl expected_auth req.hdr(authorization) -m str \"Bearer {placeholder_token}\"\n    acl allowed_method method POST\n    acl allowed_path path {responses_path}\n    http-request deny deny_status 401 unless expected_auth\n    http-request deny deny_status 405 unless allowed_method\n    http-request deny deny_status 404 unless allowed_path\n    default_backend codex_openai_upstream\n\nbackend codex_openai_upstream\n    http-request set-header Authorization \"Bearer %[env({UPSTREAM_BEARER_TOKEN_ENV})]\"\n    http-request set-header Host {upstream_host}\n    server upstream {upstream_host}:443 ssl verify required ca-file @system-ca sni str({upstream_host})\n"
    );
    std::fs::write(sidecar_state.config_path(), contents)
        .with_context(|| format!("failed to write {}", sidecar_state.config_path().display()))?;
    set_sidecar_config_file_permissions(&sidecar_state.config_path())?;
    Ok(())
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

#[cfg(unix)]
fn set_sidecar_config_dir_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).map_err(Into::into)
}

#[cfg(not(unix))]
fn set_sidecar_config_dir_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_sidecar_config_file_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o644)).map_err(Into::into)
}

#[cfg(not(unix))]
fn set_sidecar_config_file_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_sidecar_run_dir_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o1777)).map_err(Into::into)
}

#[cfg(not(unix))]
fn set_sidecar_run_dir_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use chrono::Utc;
    use serde_json::json;
    use tempfile::tempdir;
    use tokio::{net::UnixListener, sync::Mutex};

    use super::*;
    use crate::kernel::runtime::{
        ConfinementConfig, EffectiveExecutionPlan, ExecutionLimits, OciConfinementConfig,
        RuntimeProgramSpec, WorkspaceAccess,
    };

    async fn write_test_codex_auth(codex_home: &Path, payload: serde_json::Value) {
        tokio::fs::create_dir_all(codex_home)
            .await
            .expect("create codex home");
        tokio::fs::write(
            codex_home.join("auth.json"),
            serde_json::to_vec_pretty(&payload).expect("encode auth"),
        )
        .await
        .expect("write auth");
    }

    fn sample_request(runtime_root: PathBuf, codex_home_override: PathBuf) -> ExecutionRequest {
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
                auth: Some(RuntimeAuthKind::Codex),
            },
            runtime_secrets_mount: None,
            codex_home_override: Some(codex_home_override),
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
    fn sidecar_run_invocation_mounts_default_config_dir_and_writable_run_dir() {
        let sidecar_state = SidecarStateDir::new().expect("sidecar state");
        let config_mount = bind_mount_arg(
            &sidecar_state.config_dir(),
            SIDECAR_CONFIG_CONTAINER_DIR,
            true,
        )
        .expect("config mount");
        let run_mount = bind_mount_arg(&sidecar_state.run_dir(), SIDECAR_RUN_CONTAINER_DIR, false)
            .expect("run mount");

        let invocation = build_sidecar_run_invocation(
            "podman",
            "lionclaw-pod",
            "lionclaw-proxy",
            &sidecar_state,
            "auth-real",
        )
        .expect("invocation");

        assert!(invocation
            .args
            .windows(2)
            .any(|pair| pair == ["--pod".to_string(), "lionclaw-pod".to_string(),]));
        assert!(invocation
            .args
            .windows(2)
            .any(|pair| pair == ["--name".to_string(), "lionclaw-proxy".to_string(),]));
        assert!(invocation
            .args
            .windows(2)
            .any(|pair| pair == ["--env".to_string(), UPSTREAM_BEARER_TOKEN_ENV.to_string(),]));
        assert_eq!(
            invocation.environment,
            vec![(
                UPSTREAM_BEARER_TOKEN_ENV.to_string(),
                "auth-real".to_string()
            )]
        );
        assert!(invocation
            .args
            .windows(2)
            .any(|pair| pair == ["--volume".to_string(), config_mount.clone()]));
        assert!(invocation
            .args
            .windows(2)
            .any(|pair| pair == ["--volume".to_string(), run_mount.clone()]));
        assert_eq!(
            invocation.args.last().map(String::as_str),
            Some(HAPROXY_IMAGE)
        );
    }

    #[test]
    fn sidecar_run_invocation_rejects_colon_mount_sources() {
        let err = bind_mount_arg(
            Path::new("/tmp/lionclaw:auth-sidecar"),
            SIDECAR_RUN_CONTAINER_DIR,
            false,
        )
        .expect_err("colon mount source should be rejected");

        assert!(err.to_string().contains("contains ':'"));
    }

    #[cfg(unix)]
    #[test]
    fn sidecar_state_dir_uses_readable_config_and_writable_run_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let sidecar_state = SidecarStateDir::new().expect("sidecar state");

        let config_mode = std::fs::metadata(sidecar_state.config_dir())
            .expect("config metadata")
            .permissions()
            .mode()
            & 0o7777;
        let run_mode = std::fs::metadata(sidecar_state.run_dir())
            .expect("run metadata")
            .permissions()
            .mode()
            & 0o7777;

        assert_eq!(config_mode, 0o755);
        assert_eq!(run_mode, 0o1777);
    }

    #[tokio::test]
    async fn codex_runtime_auth_sidecar_writes_openai_api_runtime_and_sidecar_config() {
        let temp_dir = tempdir().expect("temp dir");
        let runtime_root = temp_dir.path().join("runtime-session");
        let codex_home = temp_dir.path().join(".codex");
        tokio::fs::create_dir_all(&runtime_root)
            .await
            .expect("create runtime root");
        write_test_codex_auth(
            &codex_home,
            json!({
                "OPENAI_API_KEY": "sk-real"
            }),
        )
        .await;

        let request = sample_request(runtime_root.clone(), codex_home);
        let runtime_mount_fs = runtime_mount_fs(&request.plan.mounts).expect("runtime fs");
        let placeholder = "lionclaw-placeholder-test";
        let sidecar_state = SidecarStateDir::new().expect("sidecar state");

        write_codex_config(&runtime_mount_fs, CodexHostAuthMode::OpenAiApi)
            .expect("write codex config");
        write_haproxy_config(&sidecar_state, placeholder, CodexHostAuthMode::OpenAiApi)
            .expect("write haproxy config");

        let codex_config = tokio::fs::read_to_string(runtime_root.join(CODEX_CONFIG_RELATIVE_PATH))
            .await
            .expect("read codex config");
        assert!(codex_config.contains("http://127.0.0.1:38080/v1"));
        assert!(codex_config.contains(CODEX_PROXY_TOKEN_ENV));
        assert!(!codex_config.contains(UPSTREAM_BEARER_TOKEN_ENV));

        let haproxy_config = tokio::fs::read_to_string(sidecar_state.config_path())
            .await
            .expect("read haproxy config");
        assert!(haproxy_config.contains("/v1/responses"));
        assert!(haproxy_config.contains(placeholder));
        assert!(haproxy_config.contains("%[env(LIONCLAW_CODEX_UPSTREAM_BEARER_TOKEN)]"));
        assert!(haproxy_config.contains(HAPROXY_ADMIN_SOCKET_CONTAINER_PATH));
        assert!(!haproxy_config.contains("sk-real"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let config_mode = std::fs::metadata(sidecar_state.config_path())
                .expect("config metadata")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(config_mode, 0o644);
        }
    }

    #[tokio::test]
    async fn codex_runtime_auth_sidecar_writes_chatgpt_runtime_and_sidecar_config() {
        let temp_dir = tempdir().expect("temp dir");
        let runtime_root = temp_dir.path().join("runtime-session");
        let codex_home = temp_dir.path().join(".codex");
        tokio::fs::create_dir_all(&runtime_root)
            .await
            .expect("create runtime root");
        write_test_codex_auth(
            &codex_home,
            json!({
                "OPENAI_API_KEY": null,
                "last_refresh": Utc::now().to_rfc3339(),
                "tokens": {
                    "access_token": "header.payload.signature",
                    "refresh_token": "refresh-test"
                }
            }),
        )
        .await;

        let request = sample_request(runtime_root.clone(), codex_home);
        let runtime_mount_fs = runtime_mount_fs(&request.plan.mounts).expect("runtime fs");
        let placeholder = "lionclaw-placeholder-test";
        let sidecar_state = SidecarStateDir::new().expect("sidecar state");

        write_codex_config(&runtime_mount_fs, CodexHostAuthMode::ChatGpt)
            .expect("write codex config");
        write_haproxy_config(&sidecar_state, placeholder, CodexHostAuthMode::ChatGpt)
            .expect("write haproxy config");

        let codex_config = tokio::fs::read_to_string(runtime_root.join(CODEX_CONFIG_RELATIVE_PATH))
            .await
            .expect("read codex config");
        assert!(codex_config.contains("http://127.0.0.1:38080/backend-api/codex"));
        assert!(codex_config.contains(CODEX_PROXY_TOKEN_ENV));

        let haproxy_config = tokio::fs::read_to_string(sidecar_state.config_path())
            .await
            .expect("read haproxy config");
        assert!(haproxy_config.contains("/backend-api/codex/responses"));
        assert!(haproxy_config.contains("chatgpt.com"));
    }

    #[tokio::test]
    async fn wait_for_sidecar_ready_accepts_admin_socket_response() {
        let temp_dir = tempdir().expect("temp dir");
        let admin_socket = temp_dir.path().join("admin.sock");
        let ready_flag = Arc::new(Mutex::new(false));
        let ready_flag_task = ready_flag.clone();

        let listener = UnixListener::bind(&admin_socket).expect("bind admin socket");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept probe");
            let mut request = [0u8; 16];
            let _ = stream.read(&mut request).await.expect("read probe");
            stream
                .write_all(b"Name: HAProxy\n")
                .await
                .expect("write probe response");
            *ready_flag_task.lock().await = true;
        });

        wait_for_sidecar_ready(admin_socket)
            .await
            .expect("sidecar ready");
        server.await.expect("server join");
        assert!(*ready_flag.lock().await);
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
            let err = write_codex_config(&runtime_mount_fs, CodexHostAuthMode::OpenAiApi)
                .expect_err("symlinked codex dir should fail");
            assert!(err.to_string().contains(".codex"));
        }
    }
}
