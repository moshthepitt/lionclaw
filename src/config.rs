use std::path::PathBuf;

use crate::home::{LionClawHome, DEFAULT_WORKSPACE};

const DEFAULT_BIND_HOST: &str = "127.0.0.1";
const DEFAULT_BIND_PORT: &str = "8979";

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: String,
    pub home: LionClawHome,
    pub db_path: PathBuf,
    pub runtime_turn_idle_timeout_ms: u64,
    pub runtime_turn_hard_timeout_ms: u64,
    pub default_runtime_id: Option<String>,
    pub workspace_root: PathBuf,
    pub project_workspace_root: Option<PathBuf>,
    pub codex_home_override: Option<PathBuf>,
}

impl Config {
    pub fn from_env() -> std::io::Result<Self> {
        let home = LionClawHome::from_env();
        let bind_addr = resolve_bind_addr(
            std::env::var("LIONCLAW_BIND_ADDR").ok(),
            std::env::var("LIONCLAW_HOST").ok(),
            std::env::var("LIONCLAW_PORT").ok(),
        );

        let db_path = std::env::var("LIONCLAW_DB_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| home.db_path());

        let runtime_turn_idle_timeout_ms = std::env::var("LIONCLAW_RUNTIME_TURN_IDLE_TIMEOUT_MS")
            .ok()
            .or_else(|| std::env::var("LIONCLAW_RUNTIME_TURN_TIMEOUT_MS").ok())
            .and_then(|raw| raw.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(120_000);
        let runtime_turn_hard_timeout_ms = std::env::var("LIONCLAW_RUNTIME_TURN_HARD_TIMEOUT_MS")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .map(|value| value.max(runtime_turn_idle_timeout_ms))
            .unwrap_or_else(|| runtime_turn_idle_timeout_ms.max(600_000));

        let default_runtime_id = std::env::var("LIONCLAW_DEFAULT_RUNTIME_ID")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());

        let workspace_name = std::env::var("LIONCLAW_WORKSPACE")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| DEFAULT_WORKSPACE.to_string());

        let workspace_root = home.workspace_dir(&workspace_name);
        let project_workspace_root = match configured_project_workspace_root() {
            Some(path) => Some(canonicalize_project_workspace_root(path)?),
            None => resolve_project_workspace_root().ok(),
        };
        let codex_home_override = resolve_optional_env_override_path("CODEX_HOME")?;

        Ok(Self {
            bind_addr,
            home,
            db_path,
            runtime_turn_idle_timeout_ms,
            runtime_turn_hard_timeout_ms,
            default_runtime_id,
            workspace_root,
            project_workspace_root,
            codex_home_override,
        })
    }
}

pub fn resolve_optional_env_override_path(var: &str) -> std::io::Result<Option<PathBuf>> {
    std::env::var_os(var)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .map(resolve_override_path)
        .transpose()
}

pub fn resolve_project_workspace_root() -> std::io::Result<PathBuf> {
    let path = match configured_project_workspace_root() {
        Some(path) => path,
        None => std::env::current_dir()?,
    };

    canonicalize_project_workspace_root(path)
}

fn configured_project_workspace_root() -> Option<PathBuf> {
    std::env::var("LIONCLAW_WORKSPACE_ROOT")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn canonicalize_project_workspace_root(path: PathBuf) -> std::io::Result<PathBuf> {
    let resolved = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()?.join(path)
    };

    resolved.canonicalize()
}

fn resolve_override_path(path: PathBuf) -> std::io::Result<PathBuf> {
    let resolved = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()?.join(path)
    };

    match resolved.canonicalize() {
        Ok(path) => Ok(path),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(resolved),
        Err(err) => Err(err),
    }
}

fn resolve_bind_addr(
    bind_addr: Option<String>,
    host: Option<String>,
    port: Option<String>,
) -> String {
    if let Some(value) = bind_addr
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return value.to_string();
    }

    let host = host
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_BIND_HOST);
    let port = port
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_BIND_PORT);
    join_host_port(host, port)
}

fn join_host_port(host: &str, port: &str) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
mod tests {
    use super::{
        canonicalize_project_workspace_root, join_host_port, resolve_bind_addr,
        resolve_optional_env_override_path, DEFAULT_BIND_HOST, DEFAULT_BIND_PORT,
    };
    use tempfile::tempdir;

    #[test]
    fn prefers_explicit_bind_addr_env() {
        assert_eq!(
            resolve_bind_addr(
                Some("localhost:4000".to_string()),
                Some(DEFAULT_BIND_HOST.to_string()),
                Some(DEFAULT_BIND_PORT.to_string()),
            ),
            "localhost:4000"
        );
    }

    #[test]
    fn falls_back_to_host_and_port_env() {
        assert_eq!(
            resolve_bind_addr(None, Some("0.0.0.0".to_string()), Some("8080".to_string())),
            "0.0.0.0:8080"
        );
    }

    #[test]
    fn wraps_ipv6_host_when_joining() {
        assert_eq!(
            join_host_port("::1", DEFAULT_BIND_PORT),
            format!("[::1]:{DEFAULT_BIND_PORT}")
        );
    }

    #[test]
    fn canonicalize_project_workspace_root_rejects_missing_path() {
        let err = canonicalize_project_workspace_root(std::path::PathBuf::from(
            "/definitely/missing/path",
        ))
        .expect_err("missing path should fail");
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn canonicalize_project_workspace_root_resolves_absolute_path() {
        let temp_dir = tempdir().expect("temp dir");
        let project_root = temp_dir.path().join("project");
        std::fs::create_dir_all(&project_root).expect("create project");
        let resolved = canonicalize_project_workspace_root(project_root.clone())
            .expect("resolve absolute path");

        assert_eq!(resolved, project_root);
    }

    #[test]
    fn resolve_optional_env_override_path_absolutizes_relative_values() {
        let temp_dir = tempdir().expect("temp dir");
        let project_root = temp_dir.path().join("project");
        std::fs::create_dir_all(project_root.join(".codex")).expect("create codex home");
        let original_dir = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(&project_root).expect("set cwd");
        // SAFETY: tests in this module control this process env key.
        unsafe { std::env::set_var("LIONCLAW_TEST_OVERRIDE_PATH", ".codex") };

        let resolved = resolve_optional_env_override_path("LIONCLAW_TEST_OVERRIDE_PATH")
            .expect("resolve env override");

        // SAFETY: tests in this module control this process env key.
        unsafe { std::env::remove_var("LIONCLAW_TEST_OVERRIDE_PATH") };
        std::env::set_current_dir(original_dir).expect("restore cwd");

        assert_eq!(resolved, Some(project_root.join(".codex")));
    }
}
