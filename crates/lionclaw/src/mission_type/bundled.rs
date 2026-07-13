use std::io::Write as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

struct BundledFile {
    path: &'static str,
    bytes: &'static [u8],
    executable: bool,
}

include!(concat!(env!("OUT_DIR"), "/bundled_mission_types.rs"));

pub struct BundledMissionTypes {
    _temporary: tempfile::TempDir,
    root: PathBuf,
}

impl BundledMissionTypes {
    pub fn materialize() -> Result<Self> {
        let temporary = tempfile::Builder::new()
            .prefix("lionclaw-bundled-mission-types-")
            .tempdir()
            .context("creating bundled mission-type staging directory")?;
        let root = temporary.path().join("mission-types");
        std::fs::create_dir(&root).context("creating bundled mission-type root")?;

        for bundled in BUNDLED_FILES {
            materialize_file(&root, bundled)?;
        }
        Ok(Self {
            _temporary: temporary,
            root,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

fn materialize_file(root: &Path, bundled: &BundledFile) -> Result<()> {
    let destination = root.join(bundled.path);
    let parent = destination
        .parent()
        .expect("bundled file always has the mission-type root as a parent");
    std::fs::create_dir_all(parent).with_context(|| format!("creating '{}'", parent.display()))?;
    let mut file = std::fs::File::create(&destination)
        .with_context(|| format!("creating '{}'", destination.display()))?;
    file.write_all(bundled.bytes)
        .with_context(|| format!("writing '{}'", destination.display()))?;
    let mode = if bundled.executable { 0o755 } else { 0o644 };
    file.set_permissions(std::fs::Permissions::from_mode(mode))
        .with_context(|| format!("setting permissions on '{}'", destination.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn materialized_bundle_preserves_files_and_executable_bits() {
        let bundle = BundledMissionTypes::materialize().unwrap();
        let root = bundle.root().join("software-dev");
        assert!(root.join("mission.toml").is_file());
        assert!(root.join("skills/scrutiny-validator/SKILL.md").is_file());

        let oracle = std::fs::metadata(root.join("oracles/cargo-test")).unwrap();
        let input = std::fs::metadata(root.join("inputs/cargo-home")).unwrap();
        let playbook = std::fs::metadata(root.join("playbook.md")).unwrap();
        assert_ne!(oracle.permissions().mode() & 0o111, 0);
        assert_ne!(input.permissions().mode() & 0o111, 0);
        assert_eq!(playbook.permissions().mode() & 0o111, 0);
    }
}
