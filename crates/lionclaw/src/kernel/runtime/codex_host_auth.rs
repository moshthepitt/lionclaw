use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs::{File, Metadata},
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
    time::Duration as StdDuration,
};

use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Duration, Utc};
use reqwest::StatusCode;
use rustix::fs::{flock, FlockOperation};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

const CODEX_HOME_ENV: &str = "CODEX_HOME";
const CODEX_AUTH_FILE_NAME: &str = "auth.json";
const CODEX_CONFIG_FILE_NAME: &str = "config.toml";
const CODEX_AUTH_LOCK_FILE_NAME: &str = ".lionclaw-auth.lock";
const CODEX_RUNTIME_WORKSPACE_PATH: &str = "/workspace";
const CODEX_TRUSTED_LEVEL: &str = "trusted";
const OPENAI_OAUTH_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const OPENAI_OAUTH_TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
const ACCESS_TOKEN_REFRESH_SKEW: Duration = Duration::seconds(120);
const ACCESS_TOKEN_FALLBACK_TTL: Duration = Duration::hours(1);
const OPENAI_OAUTH_CONNECT_TIMEOUT: StdDuration = StdDuration::from_secs(5);
const OPENAI_OAUTH_REQUEST_TIMEOUT: StdDuration = StdDuration::from_secs(15);

#[derive(Debug, Clone)]
struct CodexAuthStore {
    auth_path: PathBuf,
    lock_path: PathBuf,
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
        Ok(Self {
            auth_path: codex_home.join(CODEX_AUTH_FILE_NAME),
            lock_path: codex_home.join(CODEX_AUTH_LOCK_FILE_NAME),
        })
    }

    async fn lock(&self) -> Result<CodexAuthStoreLock> {
        let lock_path = self.lock_path.clone();
        tokio::task::spawn_blocking(move || acquire_codex_auth_lock(&lock_path))
            .await
            .context("failed to join Codex auth lock task")?
    }

    async fn read(&self) -> Result<(CodexAuthFile, Option<DateTime<Utc>>)> {
        let metadata = self.auth_metadata().await?;
        let modified_at = metadata.modified().ok().map(DateTime::<Utc>::from);
        let raw = tokio::fs::read_to_string(&self.auth_path)
            .await
            .with_context(|| format!("failed to read {}", self.auth_path.display()))?;
        let auth = serde_json::from_str::<CodexAuthFile>(&raw)
            .with_context(|| format!("failed to parse {}", self.auth_path.display()))?;
        Ok((auth, modified_at))
    }

    async fn write(&self, auth: &CodexAuthFile) -> Result<()> {
        let metadata = self.auth_metadata().await?;
        let encoded =
            serde_json::to_vec_pretty(auth).context("failed to encode refreshed Codex auth")?;
        let temp_path = self.auth_path.with_file_name(format!(
            ".lionclaw-codex-auth-{}.tmp",
            Uuid::new_v4().simple()
        ));
        write_private_temp_file(&temp_path, encoded, metadata.permissions()).await?;
        if let Err(err) = tokio::fs::rename(&temp_path, &self.auth_path).await {
            drop(tokio::fs::remove_file(&temp_path).await);
            return Err(err).with_context(|| {
                format!(
                    "failed to replace refreshed Codex auth at {}",
                    self.auth_path.display()
                )
            });
        }
        Ok(())
    }

    async fn auth_metadata(&self) -> Result<Metadata> {
        let metadata = match tokio::fs::symlink_metadata(&self.auth_path).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                bail!(
                    "no usable host Codex auth found at '{}'; sign in locally with `codex login`",
                    self.auth_path.display()
                );
            }
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("failed to stat {}", self.auth_path.display()));
            }
        };
        if metadata.file_type().is_symlink() {
            bail!(
                "host Codex auth file '{}' must not be a symlink",
                self.auth_path.display()
            );
        }
        if !metadata.file_type().is_file() {
            bail!(
                "host Codex auth file '{}' must be a regular file",
                self.auth_path.display()
            );
        }
        harden_private_file_permissions(&self.auth_path, &metadata, "host Codex auth").await?;
        tokio::fs::symlink_metadata(&self.auth_path)
            .await
            .with_context(|| format!("failed to stat {}", self.auth_path.display()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CodexAuthFile {
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

pub async fn ensure_codex_host_auth_ready(codex_home_override: Option<&Path>) -> Result<()> {
    load_ready_codex_home(codex_home_override, OPENAI_OAUTH_TOKEN_URL)
        .await
        .map(|_| ())
}

pub async fn sync_codex_home_into_runtime(
    runtime_state_root: &Path,
    codex_home_override: Option<&Path>,
) -> Result<()> {
    let ready = load_ready_codex_home(codex_home_override, OPENAI_OAUTH_TOKEN_URL).await?;
    ensure_runtime_codex_directory(runtime_state_root).await?;
    write_runtime_codex_file(
        runtime_state_root,
        CODEX_AUTH_FILE_NAME,
        serde_json::to_vec_pretty(&ready.auth).context("failed to encode synced Codex auth")?,
        private_file_permissions(),
    )
    .await?;
    write_runtime_codex_file(
        runtime_state_root,
        CODEX_CONFIG_FILE_NAME,
        runtime_codex_config_contents(),
        runtime_config_file_permissions(),
    )
    .await?;
    Ok(())
}

#[derive(Debug, Clone)]
struct ReadyCodexHome {
    auth: CodexAuthFile,
}

async fn load_ready_codex_home(
    codex_home_override: Option<&Path>,
    refresh_url: &str,
) -> Result<ReadyCodexHome> {
    let store = CodexAuthStore::resolve(codex_home_override)?;
    let (auth, modified_at) = store.read().await?;
    if !codex_auth_needs_refresh(&store, &auth, modified_at)? {
        return Ok(ReadyCodexHome { auth });
    }

    let _lock = store.lock().await?;
    let (mut auth, modified_at) = store.read().await?;
    if !codex_auth_needs_refresh(&store, &auth, modified_at)? {
        return Ok(ReadyCodexHome { auth });
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
    Ok(ReadyCodexHome { auth })
}

fn missing_codex_auth(store: &CodexAuthStore) -> anyhow::Error {
    anyhow!(
        "no usable host Codex auth found at '{}'; sign in locally with `codex login`",
        store.auth_path.display()
    )
}

fn runtime_codex_config_contents() -> Vec<u8> {
    format!(
        "check_for_update_on_startup = false\n\n[projects.\"{CODEX_RUNTIME_WORKSPACE_PATH}\"]\ntrust_level = \"{CODEX_TRUSTED_LEVEL}\"\n"
    )
    .into_bytes()
}

fn codex_auth_needs_refresh(
    store: &CodexAuthStore,
    auth: &CodexAuthFile,
    modified_at: Option<DateTime<Utc>>,
) -> Result<bool> {
    if nonempty(auth.openai_api_key.as_deref()).is_some() {
        return Ok(false);
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

fn acquire_codex_auth_lock(lock_path: &Path) -> Result<CodexAuthStoreLock> {
    if let Ok(metadata) = std::fs::symlink_metadata(lock_path) {
        if metadata.file_type().is_symlink() {
            bail!(
                "Codex auth lock file '{}' must not be a symlink",
                lock_path.display()
            );
        }
        if !metadata.file_type().is_file() {
            bail!(
                "Codex auth lock file '{}' must be a regular file",
                lock_path.display()
            );
        }
    }

    let file = open_private_file(lock_path, true)
        .with_context(|| format!("failed to open {}", lock_path.display()))?;
    flock(&file, FlockOperation::LockExclusive)
        .with_context(|| format!("failed to lock {}", lock_path.display()))?;
    Ok(CodexAuthStoreLock { _file: file })
}

async fn write_private_temp_file(
    path: &Path,
    contents: Vec<u8>,
    permissions: std::fs::Permissions,
) -> Result<()> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        write_private_temp_file_blocking(&path, contents, permissions)
    })
    .await
    .context("failed to join Codex auth temp-file write task")?
}

fn write_private_temp_file_blocking(
    path: &Path,
    contents: Vec<u8>,
    permissions: std::fs::Permissions,
) -> Result<()> {
    let mut file = open_private_file(path, false)
        .with_context(|| format!("failed to open {}", path.display()))?;
    file.write_all(&contents)
        .with_context(|| format!("failed to write {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to sync {}", path.display()))?;
    set_private_file_permissions(path, permissions)
        .with_context(|| format!("failed to set permissions on {}", path.display()))?;
    Ok(())
}

fn open_private_file(path: &Path, create: bool) -> Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true);
    if create {
        options.create(true);
    } else {
        options.create_new(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.mode(0o600);
    }
    options.open(path).map_err(Into::into)
}

#[cfg(unix)]
fn set_private_file_permissions(path: &Path, permissions: std::fs::Permissions) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = permissions;
    permissions.set_mode(permissions.mode() & 0o700);
    std::fs::set_permissions(path, permissions).map_err(Into::into)
}

#[cfg(not(unix))]
fn set_private_file_permissions(path: &Path, permissions: std::fs::Permissions) -> Result<()> {
    std::fs::set_permissions(path, permissions).map_err(Into::into)
}

async fn harden_private_file_permissions(
    path: &Path,
    metadata: &Metadata,
    label: &str,
) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let file_mode = metadata.permissions().mode();
        if file_mode & 0o077 != 0 {
            tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
                .await
                .with_context(|| format!("failed to chmod {} '{}'", label, path.display()))?;
        }
    }

    #[cfg(not(unix))]
    let _ = (path, metadata, label);

    Ok(())
}

#[cfg(unix)]
async fn ensure_runtime_codex_directory(runtime_state_root: &Path) -> Result<()> {
    let runtime_state_root = runtime_state_root.to_path_buf();
    tokio::task::spawn_blocking(move || {
        ensure_runtime_codex_directory_blocking(&runtime_state_root)
    })
    .await
    .context("failed to join runtime Codex directory task")?
}

#[cfg(unix)]
fn ensure_runtime_codex_directory_blocking(runtime_state_root: &Path) -> Result<()> {
    std::fs::create_dir_all(runtime_state_root)
        .with_context(|| format!("failed to create {}", runtime_state_root.display()))?;
    let root = open_runtime_state_root(runtime_state_root)?;
    let home_path = runtime_state_root.join("home");
    let home = ensure_runtime_codex_child_dir(&root, "home", &home_path)?;
    let codex_home_path = runtime_codex_home_path(runtime_state_root);
    let _codex_home = ensure_runtime_codex_child_dir(&home, ".codex", &codex_home_path)?;
    Ok(())
}

#[cfg(unix)]
fn ensure_runtime_codex_child_dir(parent: &File, name: &str, display_path: &Path) -> Result<File> {
    use rustix::{
        fs::{mkdirat, openat, Mode, OFlags},
        io::Errno,
    };
    use std::os::unix::fs::PermissionsExt;

    match mkdirat(parent, name, Mode::from_raw_mode(0o755)) {
        Ok(()) | Err(Errno::EXIST) => {}
        Err(err) => {
            return Err(err).with_context(|| format!("failed to create {}", display_path.display()))
        }
    }
    let dir = openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .with_context(|| format!("failed to open {}", display_path.display()))?;
    let dir = File::from(dir);
    dir.set_permissions(std::fs::Permissions::from_mode(0o755))
        .with_context(|| format!("failed to chmod {}", display_path.display()))?;
    Ok(dir)
}

#[cfg(unix)]
fn open_runtime_state_root(runtime_state_root: &Path) -> Result<File> {
    use rustix::fs::{open, Mode, OFlags};

    let root = open(
        runtime_state_root,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .with_context(|| format!("failed to open {}", runtime_state_root.display()))?;
    Ok(File::from(root))
}

#[cfg(unix)]
fn open_runtime_codex_home(runtime_state_root: &Path) -> Result<File> {
    use rustix::fs::{openat, Mode, OFlags};

    let root = open_runtime_state_root(runtime_state_root)?;
    let home = openat(
        &root,
        "home",
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .with_context(|| {
        format!(
            "failed to open {}",
            runtime_state_root.join("home").display()
        )
    })?;
    let home = File::from(home);
    let codex_home = openat(
        &home,
        ".codex",
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .with_context(|| {
        format!(
            "failed to open {}",
            runtime_codex_home_path(runtime_state_root).display()
        )
    })?;
    Ok(File::from(codex_home))
}

#[cfg(not(unix))]
async fn ensure_runtime_codex_directory(runtime_state_root: &Path) -> Result<()> {
    let path = runtime_codex_home_path(runtime_state_root);
    tokio::fs::create_dir_all(&path)
        .await
        .with_context(|| format!("failed to create {}", path.display()))?;
    Ok(())
}

#[cfg(unix)]
async fn write_runtime_codex_file(
    runtime_state_root: &Path,
    file_name: &str,
    contents: Vec<u8>,
    permissions: std::fs::Permissions,
) -> Result<()> {
    let runtime_state_root = runtime_state_root.to_path_buf();
    let file_name = file_name.to_string();
    tokio::task::spawn_blocking(move || {
        write_runtime_codex_file_blocking(&runtime_state_root, &file_name, contents, permissions)
    })
    .await
    .context("failed to join runtime Codex file write task")?
}

#[cfg(unix)]
fn write_runtime_codex_file_blocking(
    runtime_state_root: &Path,
    file_name: &str,
    contents: Vec<u8>,
    permissions: std::fs::Permissions,
) -> Result<()> {
    use rustix::fs::{openat, renameat, unlinkat, AtFlags, Mode, OFlags};

    let target_name = runtime_codex_file_name(file_name)?;
    let runtime_codex_home = open_runtime_codex_home(runtime_state_root)?;
    let runtime_codex_home_path = runtime_codex_home_path(runtime_state_root);
    let temp_name = OsString::from(format!(
        ".lionclaw-runtime-codex-{}.tmp",
        Uuid::new_v4().simple()
    ));
    let temp = openat(
        &runtime_codex_home,
        &temp_name,
        OFlags::WRONLY
            | OFlags::CREATE
            | OFlags::EXCL
            | OFlags::TRUNC
            | OFlags::CLOEXEC
            | OFlags::NOFOLLOW,
        Mode::from_raw_mode(0o600),
    )
    .with_context(|| {
        format!(
            "failed to create runtime Codex temp file '{}' in {}",
            Path::new(&temp_name).display(),
            runtime_codex_home_path.display()
        )
    })?;
    let mut temp = File::from(temp);

    let write_result = (|| -> Result<()> {
        temp.write_all(&contents).with_context(|| {
            format!(
                "failed to write runtime Codex file '{}' in {}",
                file_name,
                runtime_codex_home_path.display()
            )
        })?;
        temp.set_permissions(permissions).with_context(|| {
            format!(
                "failed to chmod runtime Codex file '{}' in {}",
                file_name,
                runtime_codex_home_path.display()
            )
        })?;
        temp.sync_all().with_context(|| {
            format!(
                "failed to sync runtime Codex file '{}' in {}",
                file_name,
                runtime_codex_home_path.display()
            )
        })?;
        renameat(
            &runtime_codex_home,
            &temp_name,
            &runtime_codex_home,
            &target_name,
        )
        .with_context(|| {
            format!(
                "failed to replace runtime Codex file '{}' in {}",
                file_name,
                runtime_codex_home_path.display()
            )
        })?;
        Ok(())
    })();

    if write_result.is_err() {
        match unlinkat(&runtime_codex_home, &temp_name, AtFlags::empty()) {
            Ok(()) | Err(_) => {}
        }
    }
    write_result
}

#[cfg(unix)]
fn runtime_codex_file_name(file_name: &str) -> Result<OsString> {
    let path = Path::new(file_name);
    let mut components = path.components();
    let Some(std::path::Component::Normal(name)) = components.next() else {
        bail!("runtime Codex file name '{file_name}' is invalid");
    };
    if components.next().is_some() {
        bail!("runtime Codex file name '{file_name}' is invalid");
    }
    Ok(OsString::from(name))
}

#[cfg(not(unix))]
async fn write_runtime_codex_file(
    runtime_state_root: &Path,
    file_name: &str,
    contents: Vec<u8>,
    permissions: std::fs::Permissions,
) -> Result<()> {
    let runtime_codex_home = runtime_codex_home_path(runtime_state_root);
    let path = runtime_codex_home.join(file_name);
    let temp_path = runtime_codex_home.join(format!(
        ".lionclaw-runtime-codex-{}.tmp",
        Uuid::new_v4().simple()
    ));
    write_private_temp_file(&temp_path, contents, private_file_permissions()).await?;
    if let Err(err) = tokio::fs::rename(&temp_path, &path).await {
        drop(tokio::fs::remove_file(&temp_path).await);
        return Err(err).with_context(|| format!("failed to replace {}", path.display()));
    }
    tokio::fs::set_permissions(&path, permissions)
        .await
        .with_context(|| format!("failed to chmod {}", path.display()))
}

fn runtime_codex_home_path(runtime_state_root: &Path) -> PathBuf {
    runtime_state_root.join("home").join(".codex")
}

#[cfg(unix)]
fn private_file_permissions() -> std::fs::Permissions {
    use std::os::unix::fs::PermissionsExt;

    std::fs::Permissions::from_mode(0o600)
}

#[cfg(not(unix))]
fn private_file_permissions() -> std::fs::Permissions {
    std::fs::metadata(".")
        .map(|metadata| metadata.permissions())
        .unwrap_or_else(|_| std::fs::Permissions::readonly())
}

#[cfg(unix)]
fn runtime_config_file_permissions() -> std::fs::Permissions {
    use std::os::unix::fs::PermissionsExt;

    std::fs::Permissions::from_mode(0o644)
}

#[cfg(not(unix))]
fn runtime_config_file_permissions() -> std::fs::Permissions {
    private_file_permissions()
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

    async fn write_runtime_file(path: &Path, contents: &str) {
        let parent = path.parent().expect("runtime file parent");
        tokio::fs::create_dir_all(parent)
            .await
            .expect("create runtime parent");
        tokio::fs::write(path, contents)
            .await
            .expect("write runtime file");
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

        ensure_codex_host_auth_ready(Some(&codex_home))
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

        ensure_codex_host_auth_ready(Some(&codex_home))
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

        ensure_codex_host_auth_ready(Some(&codex_home))
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

        ensure_codex_host_auth_ready(Some(&codex_home))
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

        ensure_codex_host_auth_ready(Some(&codex_home))
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

        let err = ensure_codex_host_auth_ready(Some(&codex_home))
            .await
            .expect_err("symlinked auth should fail");
        assert!(err.to_string().contains("must not be a symlink"));
    }

    #[tokio::test]
    async fn missing_auth_prompts_local_codex_login() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let codex_home = temp_dir.path().join(".codex");

        let err = ensure_codex_host_auth_ready(Some(&codex_home))
            .await
            .expect_err("missing auth should fail");
        assert!(err.to_string().contains("codex login"));
        assert!(err.to_string().contains("auth.json"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn syncs_host_auth_and_materializes_runtime_config() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let codex_home = temp_dir.path().join(".codex");
        let runtime_root = temp_dir.path().join("runtime");
        write_auth_file(
            &codex_home,
            json!({
                "OPENAI_API_KEY": "sk-test"
            }),
        )
        .await;
        tokio::fs::write(
            codex_home.join(CODEX_CONFIG_FILE_NAME),
            br#"
model = "gpt-5.4"

[mcp_servers.host-only]
command = "/host/tool"
"#,
        )
        .await
        .expect("write config");

        sync_codex_home_into_runtime(&runtime_root, Some(&codex_home))
            .await
            .expect("sync runtime home");

        let runtime_codex_home = runtime_root.join("home").join(".codex");
        let copied_auth = tokio::fs::read_to_string(runtime_codex_home.join(CODEX_AUTH_FILE_NAME))
            .await
            .expect("read copied auth");
        let copied_config =
            tokio::fs::read_to_string(runtime_codex_home.join(CODEX_CONFIG_FILE_NAME))
                .await
                .expect("read copied config");
        assert!(copied_auth.contains("\"OPENAI_API_KEY\": \"sk-test\""));
        assert!(copied_config.contains("check_for_update_on_startup = false"));
        assert!(copied_config.contains("[projects.\"/workspace\"]"));
        assert!(copied_config.contains("trust_level = \"trusted\""));
        assert!(!copied_config.contains("gpt-5.4"));
        assert!(!copied_config.contains("host-only"));
        assert!(!copied_config.contains("/host/tool"));

        let auth_mode = std::fs::metadata(runtime_codex_home.join(CODEX_AUTH_FILE_NAME))
            .expect("runtime auth metadata")
            .permissions()
            .mode()
            & 0o777;
        let config_mode = std::fs::metadata(runtime_codex_home.join(CODEX_CONFIG_FILE_NAME))
            .expect("runtime config metadata")
            .permissions()
            .mode()
            & 0o777;
        let dir_mode = std::fs::metadata(&runtime_codex_home)
            .expect("runtime dir metadata")
            .permissions()
            .mode()
            & 0o777;
        let home_mode = std::fs::metadata(runtime_root.join("home"))
            .expect("runtime home metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(auth_mode, 0o600);
        assert_eq!(config_mode, 0o644);
        assert_eq!(home_mode, 0o755);
        assert_eq!(dir_mode, 0o755);

        tokio::fs::remove_file(codex_home.join(CODEX_CONFIG_FILE_NAME))
            .await
            .expect("remove host config");
        write_runtime_file(
            &runtime_codex_home.join(CODEX_CONFIG_FILE_NAME),
            "stale = true\n",
        )
        .await;

        sync_codex_home_into_runtime(&runtime_root, Some(&codex_home))
            .await
            .expect("resync runtime home");
        let regenerated_config =
            tokio::fs::read_to_string(runtime_codex_home.join(CODEX_CONFIG_FILE_NAME))
                .await
                .expect("read regenerated config");
        assert!(!regenerated_config.contains("stale = true"));
        assert!(regenerated_config.contains("[projects.\"/workspace\"]"));
        assert!(regenerated_config.contains("trust_level = \"trusted\""));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn runtime_codex_sync_rejects_symlinked_runtime_root() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let codex_home = temp_dir.path().join(".codex");
        let outside_runtime = temp_dir.path().join("outside-runtime");
        let runtime_root = temp_dir.path().join("runtime");
        write_auth_file(
            &codex_home,
            json!({
                "OPENAI_API_KEY": "sk-test"
            }),
        )
        .await;
        std::fs::create_dir(&outside_runtime).expect("outside runtime");
        symlink(&outside_runtime, &runtime_root).expect("runtime root symlink");

        let err = sync_codex_home_into_runtime(&runtime_root, Some(&codex_home))
            .await
            .expect_err("symlinked runtime root should fail");

        assert!(err.to_string().contains("failed to open"));
        assert!(
            !outside_runtime.join("home").exists(),
            "runtime sync must not create files through a symlinked runtime root"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn runtime_codex_sync_replaces_symlinked_config_without_following() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let codex_home = temp_dir.path().join(".codex");
        let runtime_root = temp_dir.path().join("runtime");
        let outside_config = temp_dir.path().join("outside-config.toml");
        write_auth_file(
            &codex_home,
            json!({
                "OPENAI_API_KEY": "sk-test"
            }),
        )
        .await;
        std::fs::create_dir_all(runtime_root.join("home/.codex")).expect("runtime codex home");
        std::fs::write(&outside_config, "outside = true\n").expect("outside config");
        symlink(
            &outside_config,
            runtime_root
                .join("home/.codex")
                .join(CODEX_CONFIG_FILE_NAME),
        )
        .expect("runtime config symlink");

        sync_codex_home_into_runtime(&runtime_root, Some(&codex_home))
            .await
            .expect("sync runtime home");

        assert_eq!(
            std::fs::read_to_string(&outside_config).expect("outside config"),
            "outside = true\n"
        );
        let runtime_config = runtime_root
            .join("home/.codex")
            .join(CODEX_CONFIG_FILE_NAME);
        let metadata = std::fs::symlink_metadata(&runtime_config).expect("runtime config metadata");
        assert!(metadata.is_file());
        assert!(!metadata.file_type().is_symlink());
        assert!(std::fs::read_to_string(&runtime_config)
            .expect("runtime config")
            .contains("trust_level = \"trusted\""));
    }

    #[test]
    fn runtime_codex_config_is_lionclaw_owned() {
        let config = runtime_codex_config_contents();
        let config = String::from_utf8(config).expect("config utf8");

        assert!(config.contains("check_for_update_on_startup = false"));
        assert!(config.contains("[projects.\"/workspace\"]"));
        assert!(config.contains("trust_level = \"trusted\""));
    }
}
