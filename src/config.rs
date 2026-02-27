use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub db_path: PathBuf,
    pub runtime_turn_timeout_ms: u64,
}

impl Config {
    pub fn from_env() -> Self {
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
            .unwrap_or_else(|_| PathBuf::from("lionclaw.db"));

        let runtime_turn_timeout_ms = std::env::var("LIONCLAW_RUNTIME_TURN_TIMEOUT_MS")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(120_000);

        Self {
            bind_addr: SocketAddr::new(host, port),
            db_path,
            runtime_turn_timeout_ms,
        }
    }
}
