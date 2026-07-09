//! Mission runtime configuration: which agent runtime backs roles and the
//! confinement it runs under. The confinement image is **not** set here — it
//! is declared per mission type in `mission.toml` and injected when the engine
//! is opened, so each domain (Rust, Python, …) carries its own toolchain.

use std::time::Duration;

use lionclaw_confinement::{ConfinementConfig, ExecutionLimits, OciConfinementConfig};

#[derive(Debug, Clone)]
pub struct MissionRuntimeProfile {
    /// Runtime id (also the driver selector): "codex" or an ACP profile.
    pub name: String,
    /// Driver protocol: "codex" (app-server) or "acp".
    pub driver: String,
    /// Agent executable inside the container.
    pub command: String,
    pub model: Option<String>,
    pub confinement: ConfinementConfig,
    /// Ceiling on one agent turn.
    pub hard_timeout: Duration,
    pub idle_timeout: Duration,
    /// Ceiling on one oracle run.
    pub oracle_timeout: Duration,
}

impl MissionRuntimeProfile {
    pub fn codex_default() -> Self {
        Self {
            name: "codex".to_string(),
            driver: "codex".to_string(),
            command: "codex".to_string(),
            model: None,
            confinement: ConfinementConfig::Oci(OciConfinementConfig {
                engine: "podman".to_string(),
                // Filled from the mission type's `mission.toml` at open time.
                image: None,
                read_only_rootfs: true,
                tmpfs: vec!["/tmp:rw,size=512m".to_string()],
                additional_mounts: Vec::new(),
                // Resource ceilings are left unset by default: enforcing
                // `--memory` needs cgroup swap accounting the host may lack.
                // Operators tune these per deployment.
                limits: ExecutionLimits::default(),
            }),
            hard_timeout: Duration::from_secs(30 * 60),
            idle_timeout: Duration::from_secs(10 * 60),
            oracle_timeout: Duration::from_secs(15 * 60),
        }
    }
}
