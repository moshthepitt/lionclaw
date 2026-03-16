use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use crate::home::{LionClawHome, DEFAULT_WORKSPACE};

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub home: LionClawHome,
    pub db_path: PathBuf,
    pub runtime_turn_timeout_ms: u64,
    pub default_runtime_id: Option<String>,
    pub workspace_root: PathBuf,
}

impl Config {
    pub fn from_env() -> Self {
        let home = LionClawHome::from_env();
        let host = std::env::var("LIONCLAW_HOST")
            .ok()
            .and_then(|raw| raw.parse::<IpAddr>().ok())
            .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));

        let port = std::env::var("LIONCLAW_PORT")
            .ok()
            .and_then(|raw| raw.parse::<u16>().ok())
            .unwrap_or(3000);

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
            bind_addr: SocketAddr::new(host, port),
            home,
            db_path,
            runtime_turn_timeout_ms,
            default_runtime_id,
            workspace_root,
        }
    }
}
