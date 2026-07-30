use std::{
    collections::BTreeMap,
    ffi::OsStr,
    fmt::Write as _,
    path::{Path, PathBuf},
    time::Duration as StdDuration,
};

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Duration, Utc};
#[cfg(test)]
use lionclaw_runtime_api::NetworkGrant;
use lionclaw_runtime_api::{
    RuntimeAuthContext, RuntimeAuthIdentity, RuntimeAuthKind, RuntimeAuthMaterialization,
    RuntimeAuthPreparation, RuntimeAuthProjection, RuntimeAuthProvider,
    RuntimeCredentialProjection, MAX_RUNTIME_CREDENTIAL_BYTES,
};
use reqwest::StatusCode;
use rustix::fs::{flock, FlockOperation};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use lionclaw_durable_fs::RootedDirectory;

const CODEX_HOME_ENV: &str = "CODEX_HOME";
const CODEX_AUTH_FILE_NAME: &str = "auth.json";
const STAGED_CODEX_AUTH_FILE_NAME: &str = "codex-auth.json";
const CODEX_AUTH_LOCK_FILE_NAME: &str = ".lionclaw-auth.lock";
const OPENAI_OAUTH_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const OPENAI_OAUTH_TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
const ACCESS_TOKEN_REFRESH_SKEW: Duration = Duration::seconds(120);
const ACCESS_TOKEN_FALLBACK_TTL: Duration = Duration::hours(1);
const OPENAI_OAUTH_CONNECT_TIMEOUT: StdDuration = StdDuration::from_secs(5);
const OPENAI_OAUTH_REQUEST_TIMEOUT: StdDuration = StdDuration::from_secs(15);
const CONTAINER_CODEX_HOME: &str = "/runtime/home/.codex";

#[derive(Debug, Clone, Copy)]
pub struct CodexRuntimeAuthProvider;

#[async_trait]
impl RuntimeAuthProvider for CodexRuntimeAuthProvider {
    fn kind(&self) -> &'static str {
        crate::CODEX_RUNTIME_AUTH_KIND
    }

    async fn prepare(
        &self,
        input: RuntimeAuthPreparation<'_>,
    ) -> Result<RuntimeAuthMaterialization> {
        prepare_codex_runtime_auth(
            input.runtime_id,
            !input.network.is_denied(),
            input.auth_staging_root,
            codex_home_override(input.host_context),
        )
        .await
    }
}

fn codex_home_override(context: &RuntimeAuthContext) -> Option<&Path> {
    context.home_override(crate::CODEX_RUNTIME_AUTH_KIND)
}

#[derive(Debug, Clone)]
struct CodexAuthStore {
    home: PathBuf,
    files: RootedDirectory,
}

struct CodexAuthStoreLock {
    _file: std::fs::File,
}

impl CodexAuthStore {
    fn resolve(codex_home_override: Option<&Path>) -> Result<Self> {
        let codex_home = codex_home_override
            .map(Path::to_path_buf)
            .or_else(|| {
                std::env::var_os(CODEX_HOME_ENV)
                    .filter(|value| !value.is_empty())
                    .map(PathBuf::from)
            })
            .or_else(default_codex_home)
            .ok_or_else(|| anyhow!("could not resolve host Codex home; HOME is not set"))?;
        let files = RootedDirectory::new(codex_home.clone(), codex_home.clone())?;
        Ok(Self {
            home: codex_home,
            files,
        })
    }

    async fn lock(&self) -> Result<CodexAuthStoreLock> {
        let files = self.files.clone();
        tokio::task::spawn_blocking(move || acquire_codex_auth_lock(&files))
            .await
            .context("failed to join Codex auth lock task")?
    }

    async fn read(&self) -> Result<(CodexAuthFile, Option<DateTime<Utc>>)> {
        let files = self.files.clone();
        let auth_path = self.auth_path();
        tokio::task::spawn_blocking(move || {
            let Some((raw, metadata)) = files.read_private_bounded_with_metadata(
                OsStr::new(CODEX_AUTH_FILE_NAME),
                MAX_RUNTIME_CREDENTIAL_BYTES,
                "host Codex auth",
            )?
            else {
                bail!(
                    "no usable host Codex auth found at '{}'; sign in locally with `codex login`",
                    auth_path.display()
                );
            };
            let modified_at = metadata.modified().ok().map(DateTime::<Utc>::from);
            let auth = serde_json::from_slice::<CodexAuthFile>(&raw)
                .with_context(|| format!("failed to parse {}", auth_path.display()))?;
            Ok((auth, modified_at))
        })
        .await
        .context("failed to join Codex auth read task")?
    }

    async fn write(&self, auth: &CodexAuthFile) -> Result<()> {
        let encoded =
            serde_json::to_vec_pretty(auth).context("failed to encode refreshed Codex auth")?;
        let files = self.files.clone();
        tokio::task::spawn_blocking(move || {
            files.write_private_atomic(
                OsStr::new(CODEX_AUTH_FILE_NAME),
                &encoded,
                MAX_RUNTIME_CREDENTIAL_BYTES,
                "host Codex auth",
            )
        })
        .await
        .context("failed to join Codex auth write task")?
    }

    fn auth_path(&self) -> PathBuf {
        self.home.join(CODEX_AUTH_FILE_NAME)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CodexAuthFile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    auth_mode: Option<String>,
    #[serde(rename = "OPENAI_API_KEY", default)]
    openai_api_key: Option<String>,
    #[serde(default)]
    last_refresh: Option<String>,
    #[serde(default)]
    tokens: Option<CodexAuthTokens>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct CodexAuthTokens {
    #[serde(default)]
    access_token: Option<String>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    id_token: Option<String>,
    #[serde(default)]
    account_id: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize)]
struct OpenAiRefreshResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
}

async fn stage_ready_codex_auth(
    auth_staging_root: &Path,
    contents: Vec<u8>,
) -> Result<RuntimeCredentialProjection> {
    let staging_root = auth_staging_root.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let files = RootedDirectory::new(staging_root.clone(), staging_root)?;
        files.write_private_atomic(
            OsStr::new(STAGED_CODEX_AUTH_FILE_NAME),
            &contents,
            MAX_RUNTIME_CREDENTIAL_BYTES,
            "staged Codex auth",
        )
    })
    .await
    .context("failed to join Codex auth staging task")??;
    RuntimeCredentialProjection::new(
        STAGED_CODEX_AUTH_FILE_NAME,
        Path::new(".codex").join(CODEX_AUTH_FILE_NAME),
    )
    .map_err(anyhow::Error::msg)
}

async fn prepare_codex_runtime_auth(
    runtime_id: &str,
    network_enabled: bool,
    auth_staging_root: Option<&Path>,
    codex_home_override: Option<&Path>,
) -> Result<RuntimeAuthMaterialization> {
    if !network_enabled {
        bail!(
            "runtime '{runtime_id}' requires explicit model-provider network destinations when Codex runtime auth is enabled"
        );
    }

    let auth_staging_root = auth_staging_root.ok_or_else(|| {
        anyhow!(
            "runtime '{runtime_id}' requires an effect-owned auth staging root when Codex runtime auth is enabled"
        )
    })?;
    let ready = load_ready_codex_home(codex_home_override, OPENAI_OAUTH_TOKEN_URL).await?;
    let contents = ready.serialized_auth()?;
    let identity = ready.identity(&contents)?;
    let credential = stage_ready_codex_auth(auth_staging_root, contents).await?;
    let projection = RuntimeAuthProjection::new(
        vec![(CODEX_HOME_ENV.to_string(), CONTAINER_CODEX_HOME.to_string())],
        vec![credential],
    );

    Ok(RuntimeAuthMaterialization::new(
        RuntimeAuthKind::from_static(crate::CODEX_RUNTIME_AUTH_KIND),
        identity,
        projection,
    ))
}

fn normalize_identity_path(path: &Path) -> Result<String> {
    let resolved = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let normalized = resolved.canonicalize().unwrap_or(resolved);
    Ok(format!("codex-home:{}", normalized.display()))
}

#[derive(Debug, Clone)]
struct ReadyCodexHome {
    home: PathBuf,
    auth: CodexAuthFile,
}

impl ReadyCodexHome {
    fn serialized_auth(&self) -> Result<Vec<u8>> {
        serde_json::to_vec_pretty(&self.auth).context("failed to encode staged Codex auth")
    }

    fn identity(&self, serialized_auth: &[u8]) -> Result<RuntimeAuthIdentity> {
        let home = normalize_identity_path(&self.home)?;
        let scope = match effective_auth_mode(&self.auth)? {
            EffectiveCodexAuthMode::ApiKey => {
                let api_key = nonempty(self.auth.openai_api_key.as_deref())
                    .ok_or_else(|| anyhow!("host Codex API-key auth has no API key"))?;
                format!("api-key-sha256:{}", sha256_hex(api_key.as_bytes())?)
            }
            EffectiveCodexAuthMode::Chatgpt => self
                .auth
                .tokens
                .as_ref()
                .and_then(|tokens| nonempty(tokens.account_id.as_deref()))
                .map_or_else(
                    || {
                        sha256_hex(serialized_auth)
                            .map(|digest| format!("credential-sha256:{digest}"))
                    },
                    |account_id| Ok(format!("account:{account_id}")),
                )?,
        };
        RuntimeAuthIdentity::new(format!("lionclaw-codex-auth-v1\n{home}\n{scope}"))
            .map_err(anyhow::Error::msg)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EffectiveCodexAuthMode {
    ApiKey,
    Chatgpt,
}

fn effective_auth_mode(auth: &CodexAuthFile) -> Result<EffectiveCodexAuthMode> {
    match auth.auth_mode.as_deref() {
        Some("apikey") => Ok(EffectiveCodexAuthMode::ApiKey),
        Some("chatgpt") => Ok(EffectiveCodexAuthMode::Chatgpt),
        Some(mode) => bail!("unsupported host Codex auth mode '{mode}'"),
        None if nonempty(auth.openai_api_key.as_deref()).is_some() => {
            Ok(EffectiveCodexAuthMode::ApiKey)
        }
        None => Ok(EffectiveCodexAuthMode::Chatgpt),
    }
}

fn sha256_hex(input: &[u8]) -> Result<String> {
    let digest = Sha256::digest(input);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").context("failed to encode credential digest")?;
    }
    Ok(encoded)
}

async fn load_ready_codex_home(
    codex_home_override: Option<&Path>,
    refresh_url: &str,
) -> Result<ReadyCodexHome> {
    let store = CodexAuthStore::resolve(codex_home_override)?;
    let (auth, modified_at) = store.read().await?;
    if !codex_auth_needs_refresh(&store, &auth, modified_at)? {
        return Ok(ReadyCodexHome {
            home: store.home.clone(),
            auth,
        });
    }

    let _lock = store.lock().await?;
    let (mut auth, modified_at) = store.read().await?;
    if !codex_auth_needs_refresh(&store, &auth, modified_at)? {
        return Ok(ReadyCodexHome {
            home: store.home.clone(),
            auth,
        });
    }

    let refresh_token = auth
        .tokens
        .as_ref()
        .and_then(|tokens| nonempty(tokens.refresh_token.as_deref()))
        .ok_or_else(|| missing_codex_auth(&store))?
        .to_string();

    let refreshed = refresh_codex_tokens(refresh_url, &refresh_token).await?;
    apply_refreshed_codex_tokens(&mut auth, refreshed)?;
    store.write(&auth).await?;
    Ok(ReadyCodexHome {
        home: store.home,
        auth,
    })
}

fn missing_codex_auth(store: &CodexAuthStore) -> anyhow::Error {
    anyhow!(
        "no usable host Codex auth found at '{}'; sign in locally with `codex login`",
        store.auth_path().display()
    )
}

fn codex_auth_needs_refresh(
    store: &CodexAuthStore,
    auth: &CodexAuthFile,
    modified_at: Option<DateTime<Utc>>,
) -> Result<bool> {
    if effective_auth_mode(auth)? == EffectiveCodexAuthMode::ApiKey {
        return nonempty(auth.openai_api_key.as_deref())
            .map(|_| false)
            .ok_or_else(|| missing_codex_auth(store));
    }

    let access_token = auth
        .tokens
        .as_ref()
        .and_then(|tokens| nonempty(tokens.access_token.as_deref()))
        .ok_or_else(|| missing_codex_auth(store))?;
    Ok(token_needs_refresh(
        access_token,
        auth.last_refresh.as_deref(),
        modified_at,
    ))
}

fn token_needs_refresh(
    access_token: &str,
    last_refresh: Option<&str>,
    modified_at: Option<DateTime<Utc>>,
) -> bool {
    let expiry = decode_jwt_expiry(access_token)
        .or_else(|| {
            parse_refresh_timestamp(last_refresh)
                .map(|timestamp| timestamp + ACCESS_TOKEN_FALLBACK_TTL)
        })
        .or_else(|| modified_at.map(|timestamp| timestamp + ACCESS_TOKEN_FALLBACK_TTL))
        .unwrap_or_else(|| Utc::now() + ACCESS_TOKEN_FALLBACK_TTL);
    expiry <= Utc::now() + ACCESS_TOKEN_REFRESH_SKEW
}

fn decode_jwt_expiry(token: &str) -> Option<DateTime<Utc>> {
    let mut parts = token.split('.');
    let _header = parts.next()?;
    let payload = parts.next()?;
    let decoded = URL_SAFE_NO_PAD.decode(payload.as_bytes()).ok()?;
    let json = serde_json::from_slice::<Value>(&decoded).ok()?;
    let exp = json.get("exp")?.as_i64()?;
    DateTime::<Utc>::from_timestamp(exp, 0)
}

fn parse_refresh_timestamp(raw: Option<&str>) -> Option<DateTime<Utc>> {
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

async fn refresh_codex_tokens(
    refresh_url: &str,
    refresh_token: &str,
) -> Result<OpenAiRefreshResponse> {
    refresh_codex_tokens_with_timeouts(
        refresh_url,
        refresh_token,
        OPENAI_OAUTH_CONNECT_TIMEOUT,
        OPENAI_OAUTH_REQUEST_TIMEOUT,
    )
    .await
}

async fn refresh_codex_tokens_with_timeouts(
    refresh_url: &str,
    refresh_token: &str,
    connect_timeout: StdDuration,
    request_timeout: StdDuration,
) -> Result<OpenAiRefreshResponse> {
    let response = reqwest::Client::builder()
        .connect_timeout(connect_timeout)
        .timeout(request_timeout)
        .build()
        .context("failed to construct Codex auth refresh client")?
        .post(refresh_url)
        .form(&[
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
            ("client_id", OPENAI_OAUTH_CLIENT_ID),
        ])
        .send()
        .await
        .context("failed to refresh host Codex auth")?;

    if response.status() != StatusCode::OK {
        bail!(
            "failed to refresh host Codex auth: upstream returned {}",
            response.status()
        );
    }

    let payload = response
        .json::<OpenAiRefreshResponse>()
        .await
        .context("failed to decode refreshed Codex auth")?;
    if nonempty(Some(&payload.access_token)).is_none() {
        bail!("failed to refresh host Codex auth: upstream returned an empty access token");
    }
    Ok(payload)
}

fn apply_refreshed_codex_tokens(
    auth: &mut CodexAuthFile,
    refreshed: OpenAiRefreshResponse,
) -> Result<()> {
    let tokens = auth.tokens.as_mut().ok_or_else(|| {
        anyhow!("cannot apply refreshed Codex tokens without an existing token store")
    })?;
    tokens.access_token = Some(refreshed.access_token);
    if let Some(refresh_token) = nonempty(refreshed.refresh_token.as_deref()) {
        tokens.refresh_token = Some(refresh_token.to_string());
    }
    auth.last_refresh = Some(Utc::now().to_rfc3339());
    Ok(())
}

fn default_codex_home() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".codex"))
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn acquire_codex_auth_lock(files: &RootedDirectory) -> Result<CodexAuthStoreLock> {
    let file = files.open_private_lock_file(
        OsStr::new(CODEX_AUTH_LOCK_FILE_NAME),
        "Codex auth lock file",
    )?;
    flock(&file, FlockOperation::LockExclusive).with_context(|| {
        format!(
            "failed to lock {}",
            files.path().join(CODEX_AUTH_LOCK_FILE_NAME).display()
        )
    })?;
    Ok(CodexAuthStoreLock { _file: file })
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use axum::{extract::Form, routing::post, Router};
    use serde_json::json;
    use tokio::net::TcpListener;

    use super::*;

    fn fake_jwt(expiry: DateTime<Utc>) -> String {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&json!({ "exp": expiry.timestamp() })).expect("payload json"),
        );
        format!("{header}.{payload}.signature")
    }

    async fn write_auth_file(codex_home: &Path, auth: serde_json::Value) {
        tokio::fs::create_dir_all(codex_home)
            .await
            .expect("create codex home");
        tokio::fs::write(
            codex_home.join(CODEX_AUTH_FILE_NAME),
            serde_json::to_vec_pretty(&auth).expect("encode auth"),
        )
        .await
        .expect("write auth file");
    }

    async fn materialize_auth(
        root: &Path,
        codex_home: &Path,
        staging_name: &str,
    ) -> RuntimeAuthMaterialization {
        let staging = root.join(staging_name);
        tokio::fs::create_dir(&staging)
            .await
            .expect("create staging");
        try_materialize_auth(&staging, codex_home)
            .await
            .expect("prepare Codex auth")
    }

    async fn try_materialize_auth(
        staging: &Path,
        codex_home: &Path,
    ) -> Result<RuntimeAuthMaterialization> {
        let context = RuntimeAuthContext::new()
            .with_home_override(crate::CODEX_RUNTIME_AUTH_KIND, codex_home);
        let network = NetworkGrant::allow_single("api.openai.com", 443).unwrap();
        CodexRuntimeAuthProvider
            .prepare(RuntimeAuthPreparation {
                runtime_id: "codex",
                network: &network,
                auth_staging_root: Some(staging),
                host_context: &context,
            })
            .await
    }

    #[tokio::test]
    async fn materialization_keeps_account_identity_stable_across_token_refresh() {
        let root = tempfile::tempdir().expect("temp dir");
        let codex_home = root.path().join(".codex");
        write_auth_file(
            &codex_home,
            json!({
                "tokens": {
                    "account_id": "account-a",
                    "access_token": fake_jwt(Utc::now() + Duration::minutes(30))
                }
            }),
        )
        .await;
        let first = materialize_auth(root.path(), &codex_home, "first-staging").await;

        write_auth_file(
            &codex_home,
            json!({
                "tokens": {
                    "account_id": "account-a",
                    "access_token": fake_jwt(Utc::now() + Duration::minutes(45))
                }
            }),
        )
        .await;
        let second = materialize_auth(root.path(), &codex_home, "second-staging").await;

        assert_eq!(first.kind().as_str(), crate::CODEX_RUNTIME_AUTH_KIND);
        assert_eq!(
            first.identity(),
            second.identity(),
            "token refresh for one stable account must preserve its native profile"
        );
        assert!(first.identity().as_str().contains("account:account-a"));
    }

    #[tokio::test]
    async fn materialization_rotates_identity_with_the_effective_api_key() {
        let root = tempfile::tempdir().expect("temp dir");
        let codex_home = root.path().join(".codex");
        write_auth_file(&codex_home, json!({ "OPENAI_API_KEY": "sk-first" })).await;
        let first = materialize_auth(root.path(), &codex_home, "first-staging").await;

        write_auth_file(&codex_home, json!({ "OPENAI_API_KEY": "sk-second" })).await;
        let second = materialize_auth(root.path(), &codex_home, "second-staging").await;

        assert_ne!(
            first.identity(),
            second.identity(),
            "credential rotation without a non-secret account ID must select a fresh profile"
        );
        assert!(first.identity().as_str().ends_with(&format!(
            "api-key-sha256:{}",
            sha256_hex(b"sk-first").expect("first digest")
        )));
        assert!(second.identity().as_str().ends_with(&format!(
            "api-key-sha256:{}",
            sha256_hex(b"sk-second").expect("second digest")
        )));
    }

    #[tokio::test]
    async fn api_key_mode_ignores_stale_token_principal_and_rotates_with_the_key() {
        let root = tempfile::tempdir().expect("temp dir");
        let codex_home = root.path().join(".codex");
        write_auth_file(
            &codex_home,
            json!({
                "auth_mode": "apikey",
                "OPENAI_API_KEY": "sk-first",
                "tokens": {
                    "account_id": "stale-account",
                    "access_token": fake_jwt(Utc::now() + Duration::minutes(30))
                }
            }),
        )
        .await;
        let first = materialize_auth(root.path(), &codex_home, "first-staging").await;

        write_auth_file(
            &codex_home,
            json!({
                "auth_mode": "apikey",
                "OPENAI_API_KEY": "sk-second",
                "tokens": {
                    "account_id": "stale-account",
                    "access_token": fake_jwt(Utc::now() + Duration::minutes(30))
                }
            }),
        )
        .await;
        let second = materialize_auth(root.path(), &codex_home, "second-staging").await;

        assert_ne!(first.identity(), second.identity());
        assert!(!first.identity().as_str().contains("stale-account"));
        assert!(first.identity().as_str().ends_with(&format!(
            "api-key-sha256:{}",
            sha256_hex(b"sk-first").expect("first key digest")
        )));
    }

    #[tokio::test]
    async fn chatgpt_mode_uses_the_selected_account_when_an_api_key_is_co_present() {
        let root = tempfile::tempdir().expect("temp dir");
        let codex_home = root.path().join(".codex");
        write_auth_file(
            &codex_home,
            json!({
                "auth_mode": "chatgpt",
                "OPENAI_API_KEY": "stale-api-key",
                "tokens": {
                    "account_id": "account-a",
                    "access_token": fake_jwt(Utc::now() + Duration::minutes(30))
                }
            }),
        )
        .await;

        let materialized = materialize_auth(root.path(), &codex_home, "staging").await;
        assert!(materialized
            .identity()
            .as_str()
            .contains("account:account-a"));
        assert!(!materialized.identity().as_str().contains("api-key-sha256"));
    }

    #[tokio::test]
    async fn chatgpt_account_change_selects_a_fresh_native_profile() {
        let root = tempfile::tempdir().expect("temp dir");
        let codex_home = root.path().join(".codex");
        write_auth_file(
            &codex_home,
            json!({
                "auth_mode": "chatgpt",
                "OPENAI_API_KEY": null,
                "tokens": {
                    "account_id": "account-a",
                    "access_token": fake_jwt(Utc::now() + Duration::minutes(30))
                }
            }),
        )
        .await;
        let first = materialize_auth(root.path(), &codex_home, "first-staging").await;

        write_auth_file(
            &codex_home,
            json!({
                "auth_mode": "chatgpt",
                "OPENAI_API_KEY": null,
                "tokens": {
                    "account_id": "account-b",
                    "access_token": fake_jwt(Utc::now() + Duration::minutes(30))
                }
            }),
        )
        .await;
        let second = materialize_auth(root.path(), &codex_home, "second-staging").await;

        assert_ne!(first.identity(), second.identity());
        assert!(first.identity().as_str().contains("account:account-a"));
        assert!(second.identity().as_str().contains("account:account-b"));
    }

    #[tokio::test]
    async fn ensures_openai_api_key_auth_is_ready() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let codex_home = temp_dir.path().join(".codex");
        write_auth_file(
            &codex_home,
            json!({
                "OPENAI_API_KEY": "sk-test",
                "tokens": {
                    "access_token": fake_jwt(Utc::now() + Duration::minutes(30)),
                    "refresh_token": "refresh-test"
                }
            }),
        )
        .await;

        load_ready_codex_home(Some(&codex_home), OPENAI_OAUTH_TOKEN_URL)
            .await
            .expect("auth should validate");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn ensures_openai_api_key_from_read_only_codex_home_without_lock_file() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let codex_home = temp_dir.path().join(".codex");
        write_auth_file(
            &codex_home,
            json!({
                "OPENAI_API_KEY": "sk-test"
            }),
        )
        .await;
        std::fs::set_permissions(
            codex_home.join(CODEX_AUTH_FILE_NAME),
            std::fs::Permissions::from_mode(0o600),
        )
        .expect("chmod auth");
        std::fs::set_permissions(&codex_home, std::fs::Permissions::from_mode(0o500))
            .expect("chmod codex home");

        load_ready_codex_home(Some(&codex_home), OPENAI_OAUTH_TOKEN_URL)
            .await
            .expect("auth should validate");
        assert!(!codex_home.join(CODEX_AUTH_LOCK_FILE_NAME).exists());
    }

    #[tokio::test]
    async fn ensures_chatgpt_token_auth_is_ready() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let codex_home = temp_dir.path().join(".codex");
        write_auth_file(
            &codex_home,
            json!({
                "OPENAI_API_KEY": null,
                "last_refresh": Utc::now().to_rfc3339(),
                "tokens": {
                    "access_token": fake_jwt(Utc::now() + Duration::minutes(30)),
                    "refresh_token": "refresh-test",
                }
            }),
        )
        .await;

        load_ready_codex_home(Some(&codex_home), OPENAI_OAUTH_TOKEN_URL)
            .await
            .expect("auth should validate");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn ensures_fresh_chatgpt_token_from_read_only_codex_home_without_lock_file() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let codex_home = temp_dir.path().join(".codex");
        write_auth_file(
            &codex_home,
            json!({
                "OPENAI_API_KEY": null,
                "last_refresh": Utc::now().to_rfc3339(),
                "tokens": {
                    "access_token": fake_jwt(Utc::now() + Duration::minutes(30)),
                }
            }),
        )
        .await;
        std::fs::set_permissions(
            codex_home.join(CODEX_AUTH_FILE_NAME),
            std::fs::Permissions::from_mode(0o600),
        )
        .expect("chmod auth");
        std::fs::set_permissions(&codex_home, std::fs::Permissions::from_mode(0o500))
            .expect("chmod codex home");

        load_ready_codex_home(Some(&codex_home), OPENAI_OAUTH_TOKEN_URL)
            .await
            .expect("auth should validate");
        assert!(!codex_home.join(CODEX_AUTH_LOCK_FILE_NAME).exists());
    }

    #[tokio::test]
    async fn refreshes_expiring_chatgpt_token_and_rotates_file() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let codex_home = temp_dir.path().join(".codex");
        let stale_access = fake_jwt(Utc::now() - Duration::minutes(5));
        write_auth_file(
            &codex_home,
            json!({
                "OPENAI_API_KEY": null,
                "last_refresh": (Utc::now() - Duration::hours(2)).to_rfc3339(),
                "tokens": {
                    "access_token": stale_access,
                    "refresh_token": "refresh-old",
                    "id_token": "id-old",
                }
            }),
        )
        .await;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/oauth/token",
                    post(
                        move |Form(_form): Form<BTreeMap<String, String>>| async move {
                            axum::Json(json!({
                                "access_token": fake_jwt(Utc::now() + Duration::hours(1))
                            }))
                        },
                    ),
                ),
            )
            .await
            .expect("serve refresh endpoint");
        });

        load_ready_codex_home(Some(&codex_home), &format!("http://{address}/oauth/token"))
            .await
            .expect("resolve auth");

        server.abort();

        let written = tokio::fs::read_to_string(codex_home.join(CODEX_AUTH_FILE_NAME))
            .await
            .expect("read auth file");
        assert!(written.contains("\"last_refresh\""));
        assert!(written.contains("refresh-old"));
        assert!(!written.contains(&stale_access));
    }

    #[tokio::test]
    async fn concurrent_resolves_share_a_single_refresh() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let codex_home = temp_dir.path().join(".codex");
        write_auth_file(
            &codex_home,
            json!({
                "OPENAI_API_KEY": null,
                "last_refresh": (Utc::now() - Duration::hours(2)).to_rfc3339(),
                "tokens": {
                    "access_token": fake_jwt(Utc::now() - Duration::minutes(5)),
                    "refresh_token": "refresh-old",
                }
            }),
        )
        .await;

        let refresh_hits = Arc::new(AtomicUsize::new(0));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("local addr");
        let refresh_hits_server = Arc::clone(&refresh_hits);
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/oauth/token",
                    post(move |Form(_form): Form<BTreeMap<String, String>>| {
                        let refresh_hits = Arc::clone(&refresh_hits_server);
                        async move {
                            refresh_hits.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(StdDuration::from_millis(50)).await;
                            axum::Json(json!({
                                "access_token": fake_jwt(Utc::now() + Duration::hours(1)),
                                "refresh_token": "refresh-new"
                            }))
                        }
                    }),
                ),
            )
            .await
            .expect("serve refresh endpoint");
        });

        let refresh_url = format!("http://{address}/oauth/token");
        let (first, second) = tokio::join!(
            load_ready_codex_home(Some(&codex_home), &refresh_url),
            load_ready_codex_home(Some(&codex_home), &refresh_url),
        );

        server.abort();

        let _first = first.expect("first auth");
        let _second = second.expect("second auth");
        assert_eq!(refresh_hits.load(Ordering::SeqCst), 1);

        let written = tokio::fs::read_to_string(codex_home.join(CODEX_AUTH_FILE_NAME))
            .await
            .expect("read auth file");
        assert!(written.contains("refresh-new"));
    }

    #[test]
    fn applies_rotated_refresh_token_to_existing_auth_store() {
        let mut auth = CodexAuthFile {
            auth_mode: Some("chatgpt".to_string()),
            openai_api_key: None,
            last_refresh: Some("2026-04-14T00:00:00Z".to_string()),
            tokens: Some(CodexAuthTokens {
                access_token: Some("old-access".to_string()),
                refresh_token: Some("old-refresh".to_string()),
                id_token: Some("id-old".to_string()),
                account_id: Some("acct-old".to_string()),
                extra: BTreeMap::new(),
            }),
            extra: BTreeMap::new(),
        };

        apply_refreshed_codex_tokens(
            &mut auth,
            OpenAiRefreshResponse {
                access_token: "new-access".to_string(),
                refresh_token: Some("new-refresh".to_string()),
            },
        )
        .expect("apply refreshed tokens");

        let tokens = auth.tokens.expect("tokens");
        assert_eq!(tokens.access_token.as_deref(), Some("new-access"));
        assert_eq!(tokens.refresh_token.as_deref(), Some("new-refresh"));
        assert_eq!(tokens.id_token.as_deref(), Some("id-old"));
        assert_eq!(tokens.account_id.as_deref(), Some("acct-old"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn resolves_harden_overly_broad_auth_file_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let codex_home = temp_dir.path().join(".codex");
        write_auth_file(
            &codex_home,
            json!({
                "OPENAI_API_KEY": "sk-test"
            }),
        )
        .await;
        std::fs::set_permissions(
            codex_home.join(CODEX_AUTH_FILE_NAME),
            std::fs::Permissions::from_mode(0o644),
        )
        .expect("chmod auth");

        load_ready_codex_home(Some(&codex_home), OPENAI_OAUTH_TOKEN_URL)
            .await
            .expect("auth should validate");
        let mode = std::fs::metadata(codex_home.join(CODEX_AUTH_FILE_NAME))
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[tokio::test]
    async fn refresh_timeout_fails_fast() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.expect("accept");
            tokio::time::sleep(StdDuration::from_millis(200)).await;
        });

        let err = refresh_codex_tokens_with_timeouts(
            &format!("http://{address}/oauth/token"),
            "refresh-old",
            StdDuration::from_millis(20),
            StdDuration::from_millis(20),
        )
        .await
        .expect_err("timeout should fail");

        server.abort();

        assert!(err
            .to_string()
            .contains("failed to refresh host Codex auth"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_symlinked_auth_file() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let codex_home = temp_dir.path().join(".codex");
        tokio::fs::create_dir_all(&codex_home)
            .await
            .expect("create codex home");
        let real = temp_dir.path().join("auth.json");
        tokio::fs::write(&real, "{}")
            .await
            .expect("write real auth");
        symlink(&real, codex_home.join(CODEX_AUTH_FILE_NAME)).expect("symlink auth");

        let err = load_ready_codex_home(Some(&codex_home), OPENAI_OAUTH_TOKEN_URL)
            .await
            .expect_err("symlinked auth should fail");
        assert!(err.to_string().contains("symlink"));
    }

    #[tokio::test]
    async fn missing_auth_prompts_local_codex_login() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let codex_home = temp_dir.path().join(".codex");

        let err = load_ready_codex_home(Some(&codex_home), OPENAI_OAUTH_TOKEN_URL)
            .await
            .expect_err("missing auth should fail");
        assert!(err.to_string().contains("codex login"));
        assert!(err.to_string().contains("auth.json"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stages_exact_auth_without_writing_the_persistent_runtime_home() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let codex_home = temp_dir.path().join(".codex");
        let staging = temp_dir.path().join("auth-staging");
        let runtime_home = temp_dir.path().join("runtime-home");
        std::fs::create_dir(&staging).expect("staging");
        write_auth_file(
            &codex_home,
            json!({
                "OPENAI_API_KEY": "sk-test"
            }),
        )
        .await;
        tokio::fs::write(
            codex_home.join("config.toml"),
            br#"
model = "gpt-5.4"

[mcp_servers.host-only]
command = "/host/tool"
"#,
        )
        .await
        .expect("write config");
        let runtime_codex_home = runtime_home.join(".codex");
        tokio::fs::create_dir_all(&runtime_codex_home)
            .await
            .expect("runtime Codex home");
        tokio::fs::write(
            runtime_codex_home.join("config.toml"),
            "model = \"gpt-5.5\"\n",
        )
        .await
        .expect("write runtime config");

        let materialization = try_materialize_auth(&staging, &codex_home)
            .await
            .expect("stage auth");
        let projection = materialization
            .projection()
            .credentials()
            .first()
            .expect("Codex credential projection");

        let staged_auth = tokio::fs::read_to_string(staging.join(STAGED_CODEX_AUTH_FILE_NAME))
            .await
            .expect("read staged auth");
        let preserved_config = tokio::fs::read_to_string(runtime_codex_home.join("config.toml"))
            .await
            .expect("read preserved config");
        assert!(staged_auth.contains("\"OPENAI_API_KEY\": \"sk-test\""));
        assert_eq!(preserved_config, "model = \"gpt-5.5\"\n");
        assert_eq!(
            projection.staged_source(),
            Path::new(STAGED_CODEX_AUTH_FILE_NAME)
        );
        assert_eq!(
            projection.native_home_target(),
            Path::new(".codex/auth.json")
        );
        assert!(
            !runtime_codex_home.join(CODEX_AUTH_FILE_NAME).exists(),
            "credentials must not be copied into the persistent home"
        );

        let auth_mode = std::fs::metadata(staging.join(STAGED_CODEX_AUTH_FILE_NAME))
            .expect("staged auth metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(auth_mode, 0o600);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_auth_staging_rejects_symlinked_staging_root() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let codex_home = temp_dir.path().join(".codex");
        let outside = temp_dir.path().join("outside");
        let staging = temp_dir.path().join("auth-staging");
        write_auth_file(
            &codex_home,
            json!({
                "OPENAI_API_KEY": "sk-test"
            }),
        )
        .await;
        std::fs::create_dir(&outside).expect("outside");
        symlink(&outside, &staging).expect("staging symlink");

        let err = try_materialize_auth(&staging, &codex_home)
            .await
            .expect_err("symlinked staging root should fail");

        assert!(format!("{err:#}").contains("must be a real directory"));
        assert!(
            !outside.join(STAGED_CODEX_AUTH_FILE_NAME).exists(),
            "staging must not write through a symlinked root"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_auth_staging_rejects_symlinked_credential_leaf() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let codex_home = temp_dir.path().join(".codex");
        let staging = temp_dir.path().join("auth-staging");
        let outside_auth = temp_dir.path().join("outside-auth.json");
        write_auth_file(
            &codex_home,
            json!({
                "OPENAI_API_KEY": "sk-test"
            }),
        )
        .await;
        std::fs::create_dir(&staging).expect("staging");
        std::fs::write(&outside_auth, "{\"outside\":true}\n").expect("outside auth");
        symlink(&outside_auth, staging.join(STAGED_CODEX_AUTH_FILE_NAME))
            .expect("staged auth symlink");

        let err = try_materialize_auth(&staging, &codex_home)
            .await
            .expect_err("symlinked credential leaf must fail");

        assert!(format!("{err:#}").contains("cannot be a symlink"));
        assert_eq!(
            std::fs::read_to_string(&outside_auth).expect("outside auth"),
            "{\"outside\":true}\n"
        );
    }
}
