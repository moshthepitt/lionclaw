use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::{Path, PathBuf},
    sync::Arc,
    sync::Once,
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use axum::{
    body::{to_bytes, Body},
    extract::State,
    http::{
        header::{AUTHORIZATION, CONNECTION, CONTENT_LENGTH, HOST},
        HeaderMap, Request, Response, StatusCode,
    },
    routing::any,
    Router,
};
use axum_server::{tls_rustls::RustlsConfig, Handle};
use rcgen::{BasicConstraints, Certificate, CertificateParams, IsCa};
use reqwest::Client;
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::{
    backend::ExecutionRequest,
    plan::{MountSpec, NetworkMode, RuntimeAuthProxyKind},
};

const RUNTIME_MOUNT_TARGET: &str = "/runtime";
const PROXY_HOST: &str = "host.containers.internal";
const OPENAI_UPSTREAM_ORIGIN: &str = "https://api.openai.com";
const OPENAI_API_KEY_ENV: &str = "OPENAI_API_KEY";
const CODEX_CA_CERTIFICATE_ENV: &str = "CODEX_CA_CERTIFICATE";
const CODEX_CONFIG_RELATIVE_PATH: &str = "home/.codex/config.toml";
const PROXY_DIR_RELATIVE_PATH: &str = "home/.lionclaw/auth-proxy";
const CA_CERT_FILENAME: &str = "ca.pem";
const SERVER_CERT_FILENAME: &str = "server.pem";
const SERVER_KEY_FILENAME: &str = "server.key";
const LOOPBACK_NETWORK_MODE: &str = "slirp4netns:allow_host_loopback=true";
const MAX_PROXY_REQUEST_BYTES: usize = 16 * 1024 * 1024;
static RUSTLS_PROVIDER_INIT: Once = Once::new();

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OciAuthProxyLaunch {
    pub environment: Vec<(String, String)>,
    pub network_override: Option<String>,
}

pub struct OciAuthProxySession {
    launch: OciAuthProxyLaunch,
    shutdown_handle: Handle,
    task: JoinHandle<Result<()>>,
}

impl OciAuthProxySession {
    pub fn launch(&self) -> &OciAuthProxyLaunch {
        &self.launch
    }

    pub async fn shutdown(self) -> Result<()> {
        self.shutdown_handle
            .graceful_shutdown(Some(Duration::from_secs(1)));
        match self.task.await {
            Ok(result) => result,
            Err(err) => Err(err).context("codex auth proxy task failed"),
        }
    }
}

pub async fn start_for_oci_execution(
    request: &ExecutionRequest,
) -> Result<Option<OciAuthProxySession>> {
    match request.program.auth_proxy {
        None => Ok(None),
        Some(RuntimeAuthProxyKind::CodexOpenAi) => {
            Ok(Some(start_codex_openai_proxy(request).await?))
        }
    }
}

async fn start_codex_openai_proxy(request: &ExecutionRequest) -> Result<OciAuthProxySession> {
    ensure_rustls_provider();

    if request.plan.network_mode != NetworkMode::On {
        bail!(
            "runtime '{}' requires network-mode 'on' when Codex host auth proxy is enabled",
            request.plan.runtime_id
        );
    }

    let runtime_auth_home = request.runtime_auth_home.clone().ok_or_else(|| {
        anyhow!(
            "runtime '{}' requires LIONCLAW_HOME/config/runtime-auth.env with OPENAI_API_KEY configured",
            request.plan.runtime_id
        )
    })?;
    let openai_api_key = runtime_auth_home
        .read_runtime_auth_var(OPENAI_API_KEY_ENV)
        .await?
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            anyhow!(
                "runtime '{}' requires OPENAI_API_KEY in LIONCLAW_HOME/config/runtime-auth.env",
                request.plan.runtime_id
            )
        })?;

    let runtime_mount_root = runtime_mount_root(&request.plan.mounts)?;
    let proxy_dir = runtime_mount_root.join(PROXY_DIR_RELATIVE_PATH);
    let codex_config_path = runtime_mount_root.join(CODEX_CONFIG_RELATIVE_PATH);
    tokio::fs::create_dir_all(&proxy_dir)
        .await
        .with_context(|| format!("failed to create {}", proxy_dir.display()))?;
    if let Some(parent) = codex_config_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let certificate_paths = write_proxy_certificates(&proxy_dir).await?;
    let listener =
        std::net::TcpListener::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)))
            .context("failed to bind Codex auth proxy listener")?;
    listener
        .set_nonblocking(true)
        .context("failed to configure Codex auth proxy listener")?;
    let local_addr = listener
        .local_addr()
        .context("failed to inspect Codex auth proxy listener")?;
    let placeholder_token = format!("lionclaw-placeholder-{}", Uuid::new_v4().simple());

    write_codex_config(&codex_config_path, local_addr.port()).await?;

    let state = Arc::new(CodexProxyState {
        client: Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("failed to build Codex auth proxy HTTP client")?,
        expected_authorization: format!("Bearer {}", placeholder_token),
        upstream_authorization: format!("Bearer {}", openai_api_key),
        upstream_origin: OPENAI_UPSTREAM_ORIGIN.to_string(),
    });

    let app = Router::new()
        .fallback(any(proxy_codex_openai_request))
        .with_state(state);
    let rustls_config = RustlsConfig::from_pem_file(
        &certificate_paths.server_cert,
        &certificate_paths.server_key,
    )
    .await
    .context("failed to load Codex auth proxy TLS config")?;
    let shutdown_handle = Handle::new();
    let server = axum_server::from_tcp_rustls(listener, rustls_config)
        .handle(shutdown_handle.clone())
        .serve(app.into_make_service());
    let task = tokio::spawn(async move {
        server
            .await
            .context("Codex auth proxy server exited with error")
    });

    Ok(OciAuthProxySession {
        launch: OciAuthProxyLaunch {
            environment: vec![
                (OPENAI_API_KEY_ENV.to_string(), placeholder_token),
                (
                    CODEX_CA_CERTIFICATE_ENV.to_string(),
                    format!("/runtime/{PROXY_DIR_RELATIVE_PATH}/{CA_CERT_FILENAME}"),
                ),
            ],
            network_override: Some(LOOPBACK_NETWORK_MODE.to_string()),
        },
        shutdown_handle,
        task,
    })
}

fn ensure_rustls_provider() {
    RUSTLS_PROVIDER_INIT.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

#[derive(Clone)]
struct CodexProxyState {
    client: Client,
    expected_authorization: String,
    upstream_authorization: String,
    upstream_origin: String,
}

async fn proxy_codex_openai_request(
    State(state): State<Arc<CodexProxyState>>,
    request: Request<Body>,
) -> Response<Body> {
    let (parts, body) = request.into_parts();
    if !authorized(&parts.headers, &state.expected_authorization) {
        return plain_response(
            StatusCode::UNAUTHORIZED,
            "invalid LionClaw auth placeholder",
        );
    }
    if !is_supported_openai_path(&parts.uri) {
        return plain_response(StatusCode::NOT_FOUND, "unsupported auth proxy path");
    }

    let body = match to_bytes(body, MAX_PROXY_REQUEST_BYTES).await {
        Ok(bytes) => bytes,
        Err(err) => {
            return plain_response(
                StatusCode::BAD_REQUEST,
                &format!("failed to read proxy request body: {err}"),
            );
        }
    };
    let upstream_url = format!(
        "{}{}",
        state.upstream_origin,
        parts
            .uri
            .path_and_query()
            .map(|value| value.as_str())
            .unwrap_or("/")
    );

    let mut upstream_request = state.client.request(parts.method, upstream_url);
    for (name, value) in parts.headers.iter() {
        if should_forward_request_header(name) {
            upstream_request = upstream_request.header(name, value);
        }
    }
    upstream_request = upstream_request
        .header(AUTHORIZATION, &state.upstream_authorization)
        .body(body);

    let upstream_response = match upstream_request.send().await {
        Ok(response) => response,
        Err(err) => {
            return plain_response(
                StatusCode::BAD_GATEWAY,
                &format!("failed to reach OpenAI upstream: {err}"),
            );
        }
    };

    let mut response = Response::builder().status(upstream_response.status());
    let response_headers = response
        .headers_mut()
        .expect("response builder should have mutable headers");
    for (name, value) in upstream_response.headers().iter() {
        if should_forward_response_header(name) {
            response_headers.insert(name, value.clone());
        }
    }

    response
        .body(Body::from_stream(upstream_response.bytes_stream()))
        .expect("proxy response should build")
}

fn authorized(headers: &HeaderMap, expected_authorization: &str) -> bool {
    headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        == Some(expected_authorization)
}

fn is_supported_openai_path(uri: &axum::http::Uri) -> bool {
    matches!(uri.path(), "/v1" | "/v1/") || uri.path().starts_with("/v1/")
}

fn should_forward_request_header(name: &axum::http::HeaderName) -> bool {
    !matches!(name, &AUTHORIZATION | &HOST | &CONTENT_LENGTH | &CONNECTION)
}

fn should_forward_response_header(name: &axum::http::HeaderName) -> bool {
    !matches!(name, &CONNECTION | &CONTENT_LENGTH)
}

fn plain_response(status: StatusCode, text: &str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(
            axum::http::header::CONTENT_TYPE,
            "text/plain; charset=utf-8",
        )
        .body(Body::from(text.to_string()))
        .expect("plain response should build")
}

fn runtime_mount_root(mounts: &[MountSpec]) -> Result<&Path> {
    mounts
        .iter()
        .find(|mount| mount.target == RUNTIME_MOUNT_TARGET)
        .map(|mount| mount.source.as_path())
        .ok_or_else(|| anyhow!("Codex auth proxy requires a /runtime mount"))
}

struct ProxyCertificatePaths {
    server_cert: PathBuf,
    server_key: PathBuf,
}

async fn write_proxy_certificates(proxy_dir: &Path) -> Result<ProxyCertificatePaths> {
    let ca = build_ca_certificate()?;
    let server = build_server_certificate(&ca)?;

    let ca_cert_path = proxy_dir.join(CA_CERT_FILENAME);
    let server_cert_path = proxy_dir.join(SERVER_CERT_FILENAME);
    let server_key_path = proxy_dir.join(SERVER_KEY_FILENAME);

    tokio::fs::write(&ca_cert_path, ca.serialize_pem()?)
        .await
        .with_context(|| format!("failed to write {}", ca_cert_path.display()))?;
    tokio::fs::write(&server_cert_path, server.serialize_pem_with_signer(&ca)?)
        .await
        .with_context(|| format!("failed to write {}", server_cert_path.display()))?;
    tokio::fs::write(&server_key_path, server.serialize_private_key_pem())
        .await
        .with_context(|| format!("failed to write {}", server_key_path.display()))?;

    Ok(ProxyCertificatePaths {
        server_cert: server_cert_path,
        server_key: server_key_path,
    })
}

fn build_ca_certificate() -> Result<Certificate> {
    let mut params = CertificateParams::new(Vec::new());
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    Certificate::from_params(params).context("failed to generate Codex proxy CA certificate")
}

fn build_server_certificate(ca: &Certificate) -> Result<Certificate> {
    let mut params = CertificateParams::new(vec![PROXY_HOST.to_string(), "localhost".to_string()]);
    params.is_ca = IsCa::NoCa;
    Certificate::from_params(params)
        .context("failed to generate Codex proxy server certificate")
        .and_then(|cert| {
            cert.serialize_pem_with_signer(ca)
                .context("failed to sign Codex proxy server certificate")?;
            Ok(cert)
        })
}

async fn write_codex_config(path: &Path, port: u16) -> Result<()> {
    let contents = format!(
        "[model_providers.openai]\nbase_url = \"https://{PROXY_HOST}:{port}/v1\"\nenv_key = \"{OPENAI_API_KEY_ENV}\"\nwire_api = \"responses\"\n"
    );
    tokio::fs::write(path, contents)
        .await
        .with_context(|| format!("failed to write {}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use axum::{body::Body, http::StatusCode, routing::post, Router};
    use tempfile::tempdir;

    use super::*;
    use crate::{
        home::LionClawHome,
        kernel::runtime::{
            ConfinementConfig, EffectiveExecutionPlan, ExecutionLimits, OciConfinementConfig,
            RuntimeProgramSpec, WorkspaceAccess,
        },
    };

    #[tokio::test]
    async fn codex_auth_proxy_swaps_placeholder_for_real_key() {
        let seen_authorization = Arc::new(tokio::sync::Mutex::new(None::<String>));
        let requests = Arc::new(AtomicUsize::new(0));
        let upstream_app = {
            let seen_authorization = seen_authorization.clone();
            let requests = requests.clone();
            Router::new().route(
                "/v1/responses",
                post(move |headers: HeaderMap, body: String| {
                    let seen_authorization = seen_authorization.clone();
                    let requests = requests.clone();
                    async move {
                        requests.fetch_add(1, Ordering::SeqCst);
                        *seen_authorization.lock().await = headers
                            .get(AUTHORIZATION)
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_string);
                        assert_eq!(body, "{\"prompt\":\"hello\"}");
                        (StatusCode::OK, Body::from("proxied"))
                    }
                }),
            )
        };
        let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind upstream");
        let upstream_addr = upstream_listener.local_addr().expect("upstream addr");
        let upstream_task = tokio::spawn(async move {
            axum::serve(upstream_listener, upstream_app)
                .await
                .expect("serve upstream");
        });

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

        let request = ExecutionRequest {
            plan: EffectiveExecutionPlan {
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
                    source: runtime_root.clone(),
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
                auth_proxy: Some(RuntimeAuthProxyKind::CodexOpenAi),
            },
            runtime_secrets_mount: None,
            runtime_auth_home: Some(home),
        };

        let proxy = start_codex_openai_proxy_for_test(
            &request,
            &format!("http://{upstream_addr}"),
            "localhost",
        )
        .await
        .expect("start proxy");

        let placeholder = proxy
            .launch()
            .environment
            .iter()
            .find(|(key, _)| key == OPENAI_API_KEY_ENV)
            .map(|(_, value)| value.clone())
            .expect("placeholder token");
        let ca_path = runtime_root
            .join(PROXY_DIR_RELATIVE_PATH)
            .join(CA_CERT_FILENAME);
        let ca = tokio::fs::read(&ca_path).await.expect("read ca");

        let client = reqwest::Client::builder()
            .add_root_certificate(
                reqwest::Certificate::from_pem(&ca).expect("load root certificate"),
            )
            .build()
            .expect("client");
        let response = client
            .post(format!("https://localhost:{}/v1/responses", proxy.port()))
            .header(AUTHORIZATION, format!("Bearer {placeholder}"))
            .body("{\"prompt\":\"hello\"}")
            .send()
            .await
            .expect("send proxy request");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.text().await.expect("read body"), "proxied");
        assert_eq!(
            seen_authorization.lock().await.as_deref(),
            Some("Bearer sk-real")
        );
        assert_eq!(requests.load(Ordering::SeqCst), 1);

        proxy.shutdown().await.expect("shutdown proxy");
        upstream_task.abort();
    }

    #[tokio::test]
    async fn codex_auth_proxy_rejects_wrong_placeholder() {
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

        let request = ExecutionRequest {
            plan: EffectiveExecutionPlan {
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
                    source: runtime_root.clone(),
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
                auth_proxy: Some(RuntimeAuthProxyKind::CodexOpenAi),
            },
            runtime_secrets_mount: None,
            runtime_auth_home: Some(home),
        };

        let proxy = start_codex_openai_proxy_for_test(&request, "http://127.0.0.1:9", "localhost")
            .await
            .expect("start proxy");
        let ca_path = runtime_root
            .join(PROXY_DIR_RELATIVE_PATH)
            .join(CA_CERT_FILENAME);
        let ca = tokio::fs::read(&ca_path).await.expect("read ca");
        let client = reqwest::Client::builder()
            .add_root_certificate(
                reqwest::Certificate::from_pem(&ca).expect("load root certificate"),
            )
            .build()
            .expect("client");

        let response = client
            .post(format!("https://localhost:{}/v1/responses", proxy.port()))
            .header(AUTHORIZATION, "Bearer wrong")
            .body("{}")
            .send()
            .await
            .expect("send proxy request");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        proxy.shutdown().await.expect("shutdown proxy");
    }

    struct TestProxySession {
        inner: OciAuthProxySession,
        port: u16,
    }

    impl TestProxySession {
        fn launch(&self) -> &OciAuthProxyLaunch {
            self.inner.launch()
        }

        fn port(&self) -> u16 {
            self.port
        }

        async fn shutdown(self) -> Result<()> {
            self.inner.shutdown().await
        }
    }

    async fn start_codex_openai_proxy_for_test(
        request: &ExecutionRequest,
        upstream_origin: &str,
        proxy_host: &str,
    ) -> Result<TestProxySession> {
        start_codex_openai_proxy_with_origin(request, upstream_origin, proxy_host)
            .await
            .map(|(inner, port)| TestProxySession { inner, port })
    }

    async fn start_codex_openai_proxy_with_origin(
        request: &ExecutionRequest,
        upstream_origin: &str,
        proxy_host: &str,
    ) -> Result<(OciAuthProxySession, u16)> {
        ensure_rustls_provider();

        if request.plan.network_mode != NetworkMode::On {
            bail!("proxy test requires network on");
        }

        let runtime_auth_home = request
            .runtime_auth_home
            .clone()
            .expect("runtime auth home");
        let openai_api_key = runtime_auth_home
            .read_runtime_auth_var(OPENAI_API_KEY_ENV)
            .await?
            .expect("api key");
        let runtime_mount_root = runtime_mount_root(&request.plan.mounts)?;
        let proxy_dir = runtime_mount_root.join(PROXY_DIR_RELATIVE_PATH);
        let codex_config_path = runtime_mount_root.join(CODEX_CONFIG_RELATIVE_PATH);
        tokio::fs::create_dir_all(&proxy_dir).await?;
        if let Some(parent) = codex_config_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let certificate_paths = write_proxy_certificates(&proxy_dir).await?;
        let listener =
            std::net::TcpListener::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)))?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?;
        let placeholder_token = format!("lionclaw-placeholder-{}", Uuid::new_v4().simple());
        let contents = format!(
            "[model_providers.openai]\nbase_url = \"https://{proxy_host}:{}/v1\"\nenv_key = \"{OPENAI_API_KEY_ENV}\"\nwire_api = \"responses\"\n",
            local_addr.port()
        );
        tokio::fs::write(&codex_config_path, contents).await?;
        let state = Arc::new(CodexProxyState {
            client: Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            expected_authorization: format!("Bearer {}", placeholder_token),
            upstream_authorization: format!("Bearer {}", openai_api_key),
            upstream_origin: upstream_origin.to_string(),
        });
        let app = Router::new()
            .fallback(any(proxy_codex_openai_request))
            .with_state(state);
        let rustls_config = RustlsConfig::from_pem_file(
            &certificate_paths.server_cert,
            &certificate_paths.server_key,
        )
        .await?;
        let shutdown_handle = Handle::new();
        let server = axum_server::from_tcp_rustls(listener, rustls_config)
            .handle(shutdown_handle.clone())
            .serve(app.into_make_service());
        let task = tokio::spawn(async move { server.await.map_err(Into::into) });
        Ok((
            OciAuthProxySession {
                launch: OciAuthProxyLaunch {
                    environment: vec![
                        (OPENAI_API_KEY_ENV.to_string(), placeholder_token),
                        (
                            CODEX_CA_CERTIFICATE_ENV.to_string(),
                            format!("/runtime/{PROXY_DIR_RELATIVE_PATH}/{CA_CERT_FILENAME}"),
                        ),
                    ],
                    network_override: Some(LOOPBACK_NETWORK_MODE.to_string()),
                },
                shutdown_handle,
                task,
            },
            local_addr.port(),
        ))
    }
}
