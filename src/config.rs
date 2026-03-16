use std::path::PathBuf;

use crate::home::{LionClawHome, DEFAULT_WORKSPACE};

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: String,
    pub home: LionClawHome,
    pub db_path: PathBuf,
    pub runtime_turn_timeout_ms: u64,
    pub default_runtime_id: Option<String>,
    pub workspace_root: PathBuf,
}

impl Config {
    pub fn from_env() -> Self {
        let home = LionClawHome::from_env();
        let bind_addr = resolve_bind_addr(
            std::env::var("LIONCLAW_BIND_ADDR").ok(),
            std::env::var("LIONCLAW_HOST").ok(),
            std::env::var("LIONCLAW_PORT").ok(),
        );

        let db_path = std::env::var("LIONCLAW_DB_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| home.db_path());

        let runtime_turn_timeout_ms = std::env::var("LIONCLAW_RUNTIME_TURN_TIMEOUT_MS")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(120_000);

        let default_runtime_id = std::env::var("LIONCLAW_DEFAULT_RUNTIME_ID")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());

        let workspace_name = std::env::var("LIONCLAW_WORKSPACE")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| DEFAULT_WORKSPACE.to_string());

        let workspace_root = std::env::var("LIONCLAW_WORKSPACE_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|_| home.workspace_dir(&workspace_name));

        Self {
            bind_addr,
            home,
            db_path,
            runtime_turn_timeout_ms,
            default_runtime_id,
            workspace_root,
        }
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
        .unwrap_or("127.0.0.1");
    let port = port
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("3000");
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
    use super::{join_host_port, resolve_bind_addr};

    #[test]
    fn prefers_explicit_bind_addr_env() {
        assert_eq!(
            resolve_bind_addr(
                Some("localhost:4000".to_string()),
                Some("127.0.0.1".to_string()),
                Some("3000".to_string()),
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
        assert_eq!(join_host_port("::1", "3000"), "[::1]:3000");
    }
}
