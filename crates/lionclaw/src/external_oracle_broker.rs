use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use lionclaw_durable_fs::RootedDirectory;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Method, Url};
use rustix::fs::{flock, FlockOperation};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinHandle;

use crate::config::ExternalOracleDriverAuthConfig;
use crate::credential_file::read_absolute_regular_file_bounded;
use crate::model::{EffectId, ExternalOracleDriverIdentity, MissionId, NetworkGrant};

pub(crate) const EXTERNAL_ORACLE_BROKER_MOUNT_TARGET: &str =
    "/run/lionclaw/external-oracle-broker.sock";
pub(crate) const EXTERNAL_ORACLE_BROKER_ENV: &str = "LIONCLAW_EXTERNAL_ORACLE_BROKER";

const MAX_REQUEST_BYTES: usize = 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const MAX_REQUESTS_PER_EFFECT: usize = 16;
const REQUEST_BUDGET_FILE: &str = "external-oracle-request-budget.json";
const REQUEST_BUDGET_LOCK_FILE: &str = "external-oracle-request-budget.lock";
const MAX_REQUEST_BUDGET_BYTES: usize = 256 * 1024;
const MAX_HEADERS: usize = 64;
const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_CREDENTIAL_BYTES: usize = 16 * 1024;
const MAX_URL_BYTES: usize = 4 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

pub(crate) fn socket_path(effect_id: &EffectId) -> PathBuf {
    Path::new("/tmp").join(format!("lionclaw-xo-{}.sock", effect_id.as_str()))
}

pub(crate) async fn remove_socket(effect_id: &EffectId) -> Result<()> {
    remove_stale_socket(&socket_path(effect_id)).await
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExternalOracleBrokerRequest {
    pub method: String,
    pub url: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub body: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ExternalOracleBrokerResponse {
    Complete { http_status: u16, body: String },
    Rejected { code: String, detail: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExternalOracleRequestBudgetIdentity {
    mission_id: MissionId,
    effect_id: EffectId,
    driver_identity: ExternalOracleDriverIdentity,
    idempotency_key: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExternalOracleRequestBudgetState {
    identity: ExternalOracleRequestBudgetIdentity,
    remaining_requests: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct ExternalOracleRequestBudget {
    files: RootedDirectory,
    identity: ExternalOracleRequestBudgetIdentity,
}

impl ExternalOracleRequestBudget {
    pub(crate) fn new(
        files: RootedDirectory,
        mission_id: MissionId,
        effect_id: EffectId,
        driver_identity: ExternalOracleDriverIdentity,
        idempotency_key: String,
    ) -> Self {
        Self {
            files,
            identity: ExternalOracleRequestBudgetIdentity {
                mission_id,
                effect_id,
                driver_identity,
                idempotency_key,
            },
        }
    }

    fn network(&self) -> &NetworkGrant {
        &self.identity.driver_identity.network
    }

    async fn prepare(&self) -> Result<()> {
        let budget = self.clone();
        tokio::task::spawn_blocking(move || budget.transact(|_| ()))
            .await
            .context("external oracle request budget task failed")?
    }

    async fn reserve(&self) -> Result<bool> {
        let budget = self.clone();
        tokio::task::spawn_blocking(move || {
            budget.transact(|state| {
                let Some(remaining) = state.remaining_requests.checked_sub(1) else {
                    return false;
                };
                state.remaining_requests = remaining;
                true
            })
        })
        .await
        .context("external oracle request budget task failed")?
    }

    fn transact<T>(
        &self,
        update: impl FnOnce(&mut ExternalOracleRequestBudgetState) -> T,
    ) -> Result<T> {
        let lock = self.files.open_private_lock_file(
            OsStr::new(REQUEST_BUDGET_LOCK_FILE),
            "external oracle request budget lock",
        )?;
        flock(&lock, FlockOperation::LockExclusive)
            .context("locking external oracle request budget")?;

        let mut state = match self.files.read_private_bounded_with_metadata(
            OsStr::new(REQUEST_BUDGET_FILE),
            MAX_REQUEST_BUDGET_BYTES,
            "external oracle request budget",
        )? {
            Some((bytes, _)) => serde_json::from_slice(&bytes)
                .context("external oracle request budget is malformed")?,
            None => ExternalOracleRequestBudgetState {
                identity: self.identity.clone(),
                remaining_requests: MAX_REQUESTS_PER_EFFECT,
            },
        };
        if state.identity != self.identity {
            bail!("external oracle request budget identity does not match the active effect");
        }
        if state.remaining_requests > MAX_REQUESTS_PER_EFFECT {
            bail!("external oracle request budget exceeds its fixed limit");
        }

        let result = update(&mut state);
        let bytes =
            serde_json::to_vec(&state).context("encoding external oracle request budget")?;
        self.files.write_private_atomic(
            OsStr::new(REQUEST_BUDGET_FILE),
            &bytes,
            MAX_REQUEST_BUDGET_BYTES,
            "external oracle request budget",
        )?;
        Ok(result)
    }
}

#[derive(Debug)]
pub(crate) struct ExternalOracleBroker {
    socket_path: PathBuf,
    task: JoinHandle<()>,
}

impl ExternalOracleBroker {
    pub(crate) async fn start(
        config: &ExternalOracleDriverAuthConfig,
        socket_path: PathBuf,
        request_budget: ExternalOracleRequestBudget,
    ) -> Result<Self> {
        request_budget.prepare().await?;
        let credential = read_credential(&config.source).await?;
        let auth_name = HeaderName::from_bytes(config.header.as_bytes())
            .context("external oracle auth header is invalid")?;
        let auth_value = HeaderValue::from_str(&format!("{}{}", config.prefix, credential))
            .map_err(|_| anyhow!("external oracle credential is not valid HTTP header material"))?;
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("building external oracle credential broker client")?;

        if let Some(parent) = socket_path.parent() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!("creating credential broker directory {}", parent.display())
            })?;
        }
        remove_stale_socket(&socket_path).await?;
        let listener = UnixListener::bind(&socket_path).with_context(|| {
            format!("binding credential broker socket {}", socket_path.display())
        })?;
        std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| {
                format!(
                    "restricting credential broker socket {}",
                    socket_path.display()
                )
            })?;

        let state = Arc::new(BrokerState {
            network: request_budget.network().clone(),
            auth_name,
            auth_value,
            credential,
            client,
            request_budget,
        });
        let task = tokio::spawn(serve(listener, state));
        Ok(Self { socket_path, task })
    }
}

impl Drop for ExternalOracleBroker {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

struct BrokerState {
    network: NetworkGrant,
    auth_name: HeaderName,
    auth_value: HeaderValue,
    credential: String,
    client: reqwest::Client,
    request_budget: ExternalOracleRequestBudget,
}

async fn serve(listener: UnixListener, state: Arc<BrokerState>) {
    while let Ok((stream, _)) = listener.accept().await {
        let _ = serve_connection(stream, &state).await;
    }
}

async fn serve_connection(stream: UnixStream, state: &BrokerState) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    loop {
        let mut line = Vec::new();
        let read = (&mut reader)
            .take((MAX_REQUEST_BYTES + 1) as u64)
            .read_until(b'\n', &mut line)
            .await
            .context("reading credential broker request")?;
        if read == 0 {
            return Ok(());
        }
        let response = if line.len() > MAX_REQUEST_BYTES {
            let response = rejected("request_too_large", "broker request exceeds its size limit");
            write_response(&mut writer, &response).await?;
            return Ok(());
        } else {
            match serde_json::from_slice::<ExternalOracleBrokerRequest>(&line) {
                Ok(request) => match execute_request(state, request).await {
                    Ok(response) => response,
                    Err(error) => rejected("request_rejected", &error.to_string()),
                },
                Err(_) => rejected("invalid_request", "broker request is not valid JSON"),
            }
        };
        write_response(&mut writer, &response).await?;
    }
}

async fn write_response(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    response: &ExternalOracleBrokerResponse,
) -> Result<()> {
    let mut encoded =
        serde_json::to_vec(response).context("encoding credential broker response")?;
    encoded.push(b'\n');
    writer
        .write_all(&encoded)
        .await
        .context("writing credential broker response")
}

async fn execute_request(
    state: &BrokerState,
    request: ExternalOracleBrokerRequest,
) -> Result<ExternalOracleBrokerResponse> {
    if request.url.len() > MAX_URL_BYTES {
        bail!("broker URL exceeds its size limit");
    }
    if request.body.len() > MAX_REQUEST_BYTES {
        bail!("broker body exceeds its size limit");
    }
    let url = Url::parse(&request.url).context("broker URL is invalid")?;
    validate_destination(&state.network, &url)?;
    let method = parse_method(&request.method)?;
    let mut headers = parse_headers(request.headers, &state.auth_name)?;
    headers.insert(state.auth_name.clone(), state.auth_value.clone());
    if !state.request_budget.reserve().await? {
        return Ok(rejected(
            "request_limit",
            "credential broker request limit is exhausted",
        ));
    }

    let mut response = state
        .client
        .request(method, url)
        .headers(headers)
        .body(request.body)
        .send()
        .await
        .context("credential broker request failed")?;
    let status = response.status();
    if status.is_redirection() {
        bail!("credential broker refuses redirect responses");
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .context("reading credential broker response")?
    {
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            bail!("credential broker response exceeds its size limit");
        }
        body.extend_from_slice(&chunk);
    }
    if contains_bytes(&body, state.credential.as_bytes()) {
        bail!("credential broker response contains credential material");
    }
    let body = String::from_utf8(body)
        .map_err(|_| anyhow!("credential broker response body is not UTF-8"))?;
    Ok(ExternalOracleBrokerResponse::Complete {
        http_status: status.as_u16(),
        body,
    })
}

fn validate_destination(network: &NetworkGrant, url: &Url) -> Result<()> {
    if url.username() != "" || url.password().is_some() || url.fragment().is_some() {
        bail!("broker URL may not contain user info or a fragment");
    }
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("broker URL requires a DNS host"))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| anyhow!("broker URL requires a declared port"))?;
    match url.scheme() {
        "https" => {}
        "http" if host == "localhost" => {}
        _ => bail!("credential broker requires HTTPS except for localhost"),
    }
    if !network.allows(host, port) {
        bail!("broker destination is outside the external driver network grant");
    }
    Ok(())
}

fn parse_method(raw: &str) -> Result<Method> {
    let method = Method::from_bytes(raw.as_bytes()).context("broker HTTP method is invalid")?;
    if matches!(
        method,
        Method::GET | Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    ) {
        Ok(method)
    } else {
        bail!("broker HTTP method is not allowed")
    }
}

fn parse_headers(raw: BTreeMap<String, String>, auth_name: &HeaderName) -> Result<HeaderMap> {
    if raw.len() > MAX_HEADERS {
        bail!("broker request declares too many headers");
    }
    let declared_bytes = raw.iter().fold(0usize, |total, (name, value)| {
        total.saturating_add(name.len()).saturating_add(value.len())
    });
    if declared_bytes > MAX_HEADER_BYTES {
        bail!("broker request headers exceed their size limit");
    }
    let mut headers = HeaderMap::new();
    for (name, value) in raw {
        let name =
            HeaderName::from_bytes(name.as_bytes()).context("broker header name is invalid")?;
        if name == auth_name
            || matches!(
                name.as_str(),
                "connection"
                    | "content-length"
                    | "host"
                    | "proxy-authorization"
                    | "transfer-encoding"
            )
        {
            bail!("broker request may not override kernel-controlled headers");
        }
        let value = HeaderValue::from_str(&value).context("broker header value is invalid")?;
        headers.insert(name, value);
    }
    Ok(headers)
}

async fn read_credential(path: &Path) -> Result<String> {
    let path = path.to_path_buf();
    let bytes = tokio::task::spawn_blocking(move || read_credential_file(&path))
        .await
        .context("external oracle credential read task failed")??;
    let mut credential =
        String::from_utf8(bytes).map_err(|_| anyhow!("external oracle credential is not UTF-8"))?;
    if credential.ends_with('\n') {
        credential.pop();
        if credential.ends_with('\r') {
            credential.pop();
        }
    }
    if credential.is_empty()
        || credential
            .chars()
            .any(|character| matches!(character, '\0' | '\r' | '\n'))
    {
        bail!("external oracle credential contains invalid control separators");
    }
    Ok(credential)
}

fn read_credential_file(path: &Path) -> Result<Vec<u8>> {
    read_absolute_regular_file_bounded(
        path,
        MAX_CREDENTIAL_BYTES,
        "external oracle credential source",
    )
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

async fn remove_stale_socket(path: &Path) -> Result<()> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_socket() => tokio::fs::remove_file(path)
            .await
            .with_context(|| format!("removing stale credential broker socket {}", path.display())),
        Ok(_) => bail!(
            "credential broker path '{}' exists and is not a socket",
            path.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error)
            .with_context(|| format!("checking credential broker socket {}", path.display())),
    }
}

fn rejected(code: &str, detail: &str) -> ExternalOracleBrokerResponse {
    ExternalOracleBrokerResponse::Rejected {
        code: code.to_string(),
        detail: detail.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lionclaw_durable_fs::RootedDirectory;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    fn auth_config(source: PathBuf) -> ExternalOracleDriverAuthConfig {
        ExternalOracleDriverAuthConfig {
            source,
            header: "authorization".to_string(),
            prefix: "Bearer ".to_string(),
        }
    }

    fn request_budget(root: &Path, network: NetworkGrant) -> ExternalOracleRequestBudget {
        let driver = crate::model::ExternalOracleDriverId::new("test-driver").unwrap();
        ExternalOracleRequestBudget::new(
            RootedDirectory::new(root, root).unwrap(),
            crate::model::MissionId::parse("m123456789abc").unwrap(),
            EffectId::for_parts(&["external-oracle-broker-test"]),
            crate::model::ExternalOracleDriverIdentity {
                driver,
                image_id: "sha256:test-driver".to_string(),
                network,
                auth: Some(crate::model::ExternalOracleDriverAuthIdentity {
                    kind: "header-file".to_string(),
                    config_digest: "sha256:test-auth-config".to_string(),
                }),
            },
            "test-idempotency-key".to_string(),
        )
    }

    async fn local_service(expected_requests: usize) -> (u16, JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let service = tokio::spawn(async move {
            let mut targets = Vec::with_capacity(expected_requests);
            for _ in 0..expected_requests {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                loop {
                    let mut byte = [0u8; 1];
                    if stream.read(&mut byte).await.unwrap() == 0 {
                        break;
                    }
                    request.push(byte[0]);
                    if request.ends_with(b"\r\n\r\n") {
                        break;
                    }
                }
                let request = String::from_utf8(request).unwrap();
                targets.push(
                    request
                        .lines()
                        .next()
                        .unwrap()
                        .split_whitespace()
                        .nth(1)
                        .unwrap()
                        .to_string(),
                );
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                    .await
                    .unwrap();
            }
            targets
        });
        (port, service)
    }

    async fn broker_call(
        socket: &Path,
        request: ExternalOracleBrokerRequest,
    ) -> (ExternalOracleBrokerResponse, String) {
        let stream = UnixStream::connect(socket).await.unwrap();
        let (reader, mut writer) = stream.into_split();
        let mut encoded = serde_json::to_vec(&request).unwrap();
        encoded.push(b'\n');
        writer.write_all(&encoded).await.unwrap();
        let mut raw = String::new();
        BufReader::new(reader).read_line(&mut raw).await.unwrap();
        (serde_json::from_str(&raw).unwrap(), raw)
    }

    #[tokio::test]
    async fn broker_injects_header_without_exposing_credential_to_client() {
        let temp = tempfile::tempdir().unwrap();
        let credential = temp.path().join("token");
        std::fs::write(&credential, "top-secret-token\n").unwrap();
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = upstream.local_addr().unwrap().port();
        let service = tokio::spawn(async move {
            let (mut stream, _) = upstream.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut byte = [0u8; 1];
                stream.readable().await.unwrap();
                match stream.try_read(&mut byte) {
                    Ok(0) => break,
                    Ok(_) => {
                        request.push(byte[0]);
                        if request.ends_with(b"\r\n\r\n") {
                            break;
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => continue,
                    Err(error) => panic!("upstream read failed: {error}"),
                }
            }
            let request = String::from_utf8(request).unwrap().to_ascii_lowercase();
            assert!(request.contains("authorization: bearer top-secret-token\r\n"));
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                .await
                .unwrap();
        });
        let socket = temp.path().join("broker.sock");
        let network = NetworkGrant::allow_single("localhost", port).unwrap();
        let _broker = ExternalOracleBroker::start(
            &auth_config(credential),
            socket.clone(),
            request_budget(temp.path(), network),
        )
        .await
        .unwrap();

        let request = ExternalOracleBrokerRequest {
            method: "POST".to_string(),
            url: format!("http://localhost:{port}/submit"),
            headers: BTreeMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: "{}".to_string(),
        };
        let (response, raw) = broker_call(&socket, request).await;

        assert!(!raw.contains("top-secret-token"));
        assert!(matches!(
            response,
            ExternalOracleBrokerResponse::Complete {
                http_status: 200,
                body
            } if body == "ok"
        ));
        service.await.unwrap();
    }

    #[test]
    fn broker_rejects_destinations_outside_grant() {
        let grant = NetworkGrant::allow_single("grader.example.com", 443).unwrap();
        assert!(validate_destination(
            &grant,
            &Url::parse("https://grader.example.com/jobs").unwrap()
        )
        .is_ok());
        assert!(validate_destination(
            &grant,
            &Url::parse("https://other.example.com/jobs").unwrap()
        )
        .is_err());
        assert!(validate_destination(
            &grant,
            &Url::parse("http://grader.example.com/jobs").unwrap()
        )
        .is_err());
    }

    #[tokio::test]
    async fn broker_rejects_redirects_and_credential_echoes() {
        let temp = tempfile::tempdir().unwrap();
        let credential = temp.path().join("token");
        std::fs::write(&credential, "top-secret-token").unwrap();
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = upstream.local_addr().unwrap().port();
        let service = tokio::spawn(async move {
            for response in [
                b"HTTP/1.1 302 Found\r\nlocation: http://localhost/\r\ncontent-length: 0\r\n\r\n"
                    .as_slice(),
                b"HTTP/1.1 200 OK\r\ncontent-length: 16\r\n\r\ntop-secret-token".as_slice(),
            ] {
                let (mut stream, _) = upstream.accept().await.unwrap();
                let mut request = [0u8; 4096];
                let _ = stream.read(&mut request).await.unwrap();
                stream.write_all(response).await.unwrap();
            }
        });
        let socket = temp.path().join("broker.sock");
        let network = NetworkGrant::allow_single("localhost", port).unwrap();
        let _broker = ExternalOracleBroker::start(
            &auth_config(credential),
            socket.clone(),
            request_budget(temp.path(), network),
        )
        .await
        .unwrap();
        for _ in 0..2 {
            let (response, raw) = broker_call(
                &socket,
                ExternalOracleBrokerRequest {
                    method: "GET".to_string(),
                    url: format!("http://localhost:{port}/"),
                    headers: BTreeMap::new(),
                    body: String::new(),
                },
            )
            .await;
            assert!(!raw.contains("top-secret-token"));
            assert!(matches!(
                response,
                ExternalOracleBrokerResponse::Rejected { .. }
            ));
        }
        service.await.unwrap();
    }

    #[tokio::test]
    async fn broker_rejects_symlink_credentials() {
        let temp = tempfile::tempdir().unwrap();
        let credential = temp.path().join("credential");
        let alias = temp.path().join("alias");
        std::fs::write(&credential, "secret").unwrap();
        std::os::unix::fs::symlink(&credential, &alias).unwrap();

        let error = ExternalOracleBroker::start(
            &auth_config(alias),
            temp.path().join("broker.sock"),
            request_budget(temp.path(), NetworkGrant::Deny),
        )
        .await
        .expect_err("symlink credential must be rejected");
        assert!(error.to_string().contains("symlink"), "got {error:#}");
    }

    #[tokio::test]
    async fn broker_request_limit_is_shared_across_connections() {
        let temp = tempfile::tempdir().unwrap();
        let credential = temp.path().join("credential");
        std::fs::write(&credential, "secret").unwrap();
        let (port, service) = local_service(MAX_REQUESTS_PER_EFFECT).await;
        let network = NetworkGrant::allow_single("localhost", port).unwrap();
        let socket = temp.path().join("broker.sock");
        let _broker = ExternalOracleBroker::start(
            &auth_config(credential),
            socket.clone(),
            request_budget(temp.path(), network),
        )
        .await
        .unwrap();

        for index in 0..=MAX_REQUESTS_PER_EFFECT {
            let (response, _) = broker_call(
                &socket,
                ExternalOracleBrokerRequest {
                    method: "POST".to_string(),
                    url: format!("http://localhost:{port}/submit"),
                    headers: BTreeMap::new(),
                    body: "{}".to_string(),
                },
            )
            .await;
            match response {
                ExternalOracleBrokerResponse::Rejected { code, .. }
                    if index == MAX_REQUESTS_PER_EFFECT =>
                {
                    assert_eq!(code, "request_limit");
                }
                ExternalOracleBrokerResponse::Complete { http_status, .. } => {
                    assert_eq!(http_status, 200);
                }
                response => panic!("unexpected broker response: {response:?}"),
            }
        }
        assert_eq!(service.await.unwrap().len(), MAX_REQUESTS_PER_EFFECT);
    }

    #[tokio::test]
    async fn request_budget_survives_submit_and_poll_crash_windows() {
        let temp = tempfile::tempdir().unwrap();
        let credential = temp.path().join("credential");
        std::fs::write(&credential, "secret").unwrap();
        let (port, service) = local_service(MAX_REQUESTS_PER_EFFECT).await;
        let network = NetworkGrant::allow_single("localhost", port).unwrap();
        let socket = temp.path().join("broker.sock");

        for (operation, requests) in [
            ("submit", 1),
            ("poll", 1),
            ("poll", MAX_REQUESTS_PER_EFFECT - 2),
        ] {
            let broker = ExternalOracleBroker::start(
                &auth_config(credential.clone()),
                socket.clone(),
                request_budget(temp.path(), network.clone()),
            )
            .await
            .unwrap();
            for _ in 0..requests {
                let (response, _) = broker_call(
                    &socket,
                    ExternalOracleBrokerRequest {
                        method: "POST".to_string(),
                        url: format!("http://localhost:{port}/{operation}"),
                        headers: BTreeMap::new(),
                        body: "{}".to_string(),
                    },
                )
                .await;
                assert!(matches!(
                    response,
                    ExternalOracleBrokerResponse::Complete {
                        http_status: 200,
                        ..
                    }
                ));
            }
            drop(broker);
        }

        let _broker = ExternalOracleBroker::start(
            &auth_config(credential),
            socket.clone(),
            request_budget(temp.path(), network),
        )
        .await
        .unwrap();
        let (response, _) = broker_call(
            &socket,
            ExternalOracleBrokerRequest {
                method: "POST".to_string(),
                url: format!("http://localhost:{port}/poll"),
                headers: BTreeMap::new(),
                body: "{}".to_string(),
            },
        )
        .await;
        assert!(matches!(
            response,
            ExternalOracleBrokerResponse::Rejected { code, .. } if code == "request_limit"
        ));

        let targets = service.await.unwrap();
        assert_eq!(targets.len(), MAX_REQUESTS_PER_EFFECT);
        assert_eq!(targets[0], "/submit");
        assert!(targets[1..].iter().all(|target| target == "/poll"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn request_budget_reservations_are_atomic_across_brokers() {
        let temp = tempfile::tempdir().unwrap();
        let budget = request_budget(temp.path(), NetworkGrant::Deny);
        budget.prepare().await.unwrap();

        let reservations = (0..MAX_REQUESTS_PER_EFFECT * 2)
            .map(|_| {
                let budget = budget.clone();
                tokio::spawn(async move { budget.reserve().await.unwrap() })
            })
            .collect::<Vec<_>>();
        let mut granted = 0;
        for reservation in reservations {
            granted += usize::from(reservation.await.unwrap());
        }

        assert_eq!(granted, MAX_REQUESTS_PER_EFFECT);
        assert!(!budget.reserve().await.unwrap());
    }

    #[tokio::test]
    async fn request_budget_rejects_identity_drift_on_recreation() {
        let temp = tempfile::tempdir().unwrap();
        let budget = request_budget(temp.path(), NetworkGrant::Deny);
        budget.prepare().await.unwrap();

        let mut changed = budget.clone();
        changed.identity.idempotency_key = "different-request".to_string();
        let error = changed
            .prepare()
            .await
            .expect_err("request identity drift must be rejected");

        assert!(
            error.to_string().contains("identity does not match"),
            "got {error:#}"
        );
    }

    #[tokio::test]
    async fn broker_rejects_symlinked_credential_ancestors() {
        let temp = tempfile::tempdir().unwrap();
        let real_parent = temp.path().join("real");
        let alias_parent = temp.path().join("alias");
        std::fs::create_dir(&real_parent).unwrap();
        std::fs::write(real_parent.join("credential"), "secret").unwrap();
        std::os::unix::fs::symlink(&real_parent, &alias_parent).unwrap();

        let error = ExternalOracleBroker::start(
            &auth_config(alias_parent.join("credential")),
            temp.path().join("broker.sock"),
            request_budget(temp.path(), NetworkGrant::Deny),
        )
        .await
        .expect_err("symlinked credential ancestor must be rejected");
        assert!(error.to_string().contains("symlink"), "got {error:#}");
    }
}
