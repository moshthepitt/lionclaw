//! The confined role runner: turns a moat-vetted plan into one full
//! autonomous agent run inside a container, and captures its handoff.

mod executor;
mod handoff;
mod role_runner;

pub use executor::MissionProgramExecutor;
pub use role_runner::OciRoleRunner;

use std::path::{Path, PathBuf};

use lionclaw_confinement::{
    MountAccess, MountSpec, RUNTIME_HOME_MOUNT_TARGET, RUNTIME_MOUNT_TARGET,
};

/// Container mount targets the mission owns.
pub const HANDOFF_MOUNT_TARGET: &str = "/mission/handoff";
pub const SCRATCH_MOUNT_TARGET: &str = "/scratch";

/// Per-attempt host directories under `<state_dir>/missions/<mission>/attempts/<attempt>`.
pub struct AttemptDirs {
    pub root: PathBuf,
    pub handoff: PathBuf,
    pub scratch: PathBuf,
    pub runtime: PathBuf,
    pub runtime_home: PathBuf,
}

impl AttemptDirs {
    pub fn prepare(state_dir: &Path, mission_id: &str, attempt_tag: &str) -> std::io::Result<Self> {
        let root = state_dir
            .join("missions")
            .join(mission_id)
            .join("attempts")
            .join(attempt_tag);
        let dirs = Self {
            handoff: root.join("handoff"),
            scratch: root.join("scratch"),
            runtime: root.join("runtime"),
            runtime_home: root.join("runtime-home"),
            root,
        };
        for dir in [
            &dirs.handoff,
            &dirs.scratch,
            &dirs.runtime,
            &dirs.runtime_home,
        ] {
            std::fs::create_dir_all(dir)?;
        }
        Ok(dirs)
    }

    /// The read-write mounts every role gets (handoff, scratch, runtime
    /// state, runtime home for agent auth/config).
    pub fn agent_mounts(&self) -> Vec<MountSpec> {
        vec![
            rw(&self.handoff, HANDOFF_MOUNT_TARGET),
            rw(&self.scratch, SCRATCH_MOUNT_TARGET),
            rw(&self.runtime, RUNTIME_MOUNT_TARGET),
            rw(&self.runtime_home, RUNTIME_HOME_MOUNT_TARGET),
        ]
    }
}

fn rw(source: &Path, target: &str) -> MountSpec {
    MountSpec {
        source: source.to_path_buf(),
        target: target.to_string(),
        access: MountAccess::ReadWrite,
    }
}
