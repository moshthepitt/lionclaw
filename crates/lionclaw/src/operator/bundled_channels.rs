use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};

use crate::operator::{channel_metadata::bundled_channel_skill_dir, snapshot::SnapshotOverlay};

const BUNDLED_SKILL_BINARIES: &[BundledSkillBinary] = &[
    BundledSkillBinary {
        source_dir_name: "channel-email",
        binary_name: "lionclaw-channel-email",
        installed_path: "bin/lionclaw-channel-email",
    },
    BundledSkillBinary {
        source_dir_name: "channel-team-local",
        binary_name: "lionclaw-channel-team-local",
        installed_path: "runtime/team-local/bin/lionclaw-channel-team-local",
    },
    BundledSkillBinary {
        source_dir_name: "lionclaw-private-context",
        binary_name: "lionclaw-private-context",
        installed_path: "bin/lionclaw-private-context",
    },
];

#[derive(Debug, Clone, Copy)]
struct BundledSkillBinary {
    source_dir_name: &'static str,
    binary_name: &'static str,
    installed_path: &'static str,
}

pub(crate) fn snapshot_overlays_for_source(source_path: &Path) -> Result<Vec<SnapshotOverlay>> {
    let Some(binary) = binary_for_bundled_source(source_path)? else {
        return Ok(Vec::new());
    };
    Ok(vec![SnapshotOverlay::new(
        worker_binary_path(binary.binary_name)?,
        PathBuf::from(binary.installed_path),
    )])
}

pub(crate) fn worker_binary_path(binary_name: &str) -> Result<PathBuf> {
    let exe_name = format!("{binary_name}{}", std::env::consts::EXE_SUFFIX);
    let current_exe = std::env::current_exe().context("failed to locate current executable")?;
    let current_dir = current_exe
        .parent()
        .context("current executable path has no parent")?;
    let sibling = current_dir.join(&exe_name);
    if sibling.is_file() {
        return Ok(sibling);
    }
    if current_dir.file_name().and_then(|value| value.to_str()) == Some("deps") {
        if let Some(target_dir) = current_dir.parent() {
            let workspace_sibling = target_dir.join(&exe_name);
            if workspace_sibling.is_file() {
                return Ok(workspace_sibling);
            }
        }
    }
    Err(anyhow::anyhow!(
        "missing {}; build LionClaw with `cargo build --workspace --bins` before installing bundled skills",
        sibling.display()
    ))
}

fn binary_for_bundled_source(source_path: &Path) -> Result<Option<BundledSkillBinary>> {
    let source_path = fs::canonicalize(source_path)
        .with_context(|| format!("failed to resolve {}", source_path.display()))?;
    for binary in BUNDLED_SKILL_BINARIES {
        let bundled = bundled_skill_dir(binary.source_dir_name);
        if !bundled.exists() {
            continue;
        }
        let bundled = fs::canonicalize(&bundled)
            .with_context(|| format!("failed to resolve {}", bundled.display()))?;
        if source_path == bundled {
            return Ok(Some(*binary));
        }
    }
    Ok(None)
}

fn bundled_skill_dir(source_dir_name: &str) -> PathBuf {
    if let Some(channel_id) = source_dir_name.strip_prefix("channel-") {
        return bundled_channel_skill_dir(channel_id);
    }
    source_bundled_skill_dir(source_dir_name)
}

fn source_bundled_skill_dir(source_dir_name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("skills")
        .join(source_dir_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_worker_overlay_paths_match_skill_scripts() {
        let email = BUNDLED_SKILL_BINARIES
            .iter()
            .find(|binary| binary.source_dir_name == "channel-email")
            .expect("email bundled worker");
        assert_eq!(email.installed_path, "bin/lionclaw-channel-email");
        assert!(
            include_str!("../../../../skills/channel-email/scripts/worker")
                .contains(email.installed_path)
        );

        let team_local = BUNDLED_SKILL_BINARIES
            .iter()
            .find(|binary| binary.source_dir_name == "channel-team-local")
            .expect("team-local bundled worker");
        assert_eq!(
            team_local.installed_path,
            "runtime/team-local/bin/lionclaw-channel-team-local"
        );
        assert!(
            include_str!("../../../../skills/channel-team-local/scripts/worker")
                .contains(team_local.installed_path)
        );
        assert!(include_str!(
            "../../../../skills/channel-team-local/runtime/team-local/scripts/list"
        )
        .contains("bin/lionclaw-channel-team-local"));
        assert!(include_str!(
            "../../../../skills/channel-team-local/runtime/team-local/scripts/resolve"
        )
        .contains("bin/lionclaw-channel-team-local"));
        assert!(include_str!(
            "../../../../skills/channel-team-local/runtime/team-local/scripts/send"
        )
        .contains("bin/lionclaw-channel-team-local"));

        let private_context = BUNDLED_SKILL_BINARIES
            .iter()
            .find(|binary| binary.source_dir_name == "lionclaw-private-context")
            .expect("private context bundled binary");
        assert_eq!(
            private_context.installed_path,
            "bin/lionclaw-private-context"
        );
        assert!(
            include_str!("../../../../skills/lionclaw-private-context/scripts/projector")
                .contains(private_context.installed_path)
        );
        assert!(
            include_str!("../../../../skills/lionclaw-private-context/scripts/context")
                .contains(private_context.installed_path)
        );
    }
}
