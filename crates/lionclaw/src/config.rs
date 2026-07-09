//! Mission runtime configuration: which agent runtime backs roles and the
//! confinement it runs under. The confinement image is **not** set here — it
//! is declared per mission type in `mission.toml` and injected when the engine
//! is opened, so each domain (Rust, Python, …) carries its own toolchain.

use std::time::Duration;

use lionclaw_confinement::{ConfinementConfig, ExecutionLimits, OciConfinementConfig};

#[derive(Debug, Clone)]
pub struct MissionRuntimeProfile {
    /// Runtime id (also the per-role selector): "codex" or "opencode".
    pub name: String,
    /// Driver protocol: "codex" (app-server) or "acp".
    pub driver: String,
    /// Agent executable inside the container.
    pub command: String,
    /// Fixed arguments the executable is launched with (e.g. the ACP subcommand).
    pub args: Vec<String>,
    /// Driver-level environment for the agent process.
    pub environment: Vec<(String, String)>,
    pub model: Option<String>,
    pub confinement: ConfinementConfig,
    /// Ceiling on one agent turn.
    pub hard_timeout: Duration,
    /// Ceiling on one oracle run.
    pub oracle_timeout: Duration,
}

impl MissionRuntimeProfile {
    /// The confinement + timeouts shared by every runtime. The image is filled
    /// from the mission type's `mission.toml` when the engine is opened.
    fn base(name: &str, driver: &str, command: &str) -> Self {
        Self {
            name: name.to_string(),
            driver: driver.to_string(),
            command: command.to_string(),
            args: Vec::new(),
            environment: Vec::new(),
            model: None,
            confinement: ConfinementConfig::Oci(OciConfinementConfig {
                engine: "podman".to_string(),
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
            oracle_timeout: Duration::from_secs(15 * 60),
        }
    }

    /// Codex over its app-server driver.
    pub fn codex_default() -> Self {
        Self::base("codex", "codex", "codex")
    }

    /// opencode over the ACP driver: `opencode acp`, with auto-update disabled
    /// (it would fail in the network-less container anyway).
    pub fn opencode_default() -> Self {
        Self {
            args: vec!["acp".to_string()],
            environment: vec![("OPENCODE_DISABLE_AUTOUPDATE".to_string(), "1".to_string())],
            ..Self::base("opencode", "acp", "opencode")
        }
    }
}
