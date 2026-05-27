use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};

use crate::operator::{channel_metadata::bundled_channel_skill_dir, snapshot::SnapshotOverlay};

const BUNDLED_CHANNEL_WORKERS: &[BundledChannelWorker] = &[
    BundledChannelWorker {
        channel_id: "email",
        binary_name: "lionclaw-channel-email",
    },
    BundledChannelWorker {
        channel_id: "team-local",
        binary_name: "lionclaw-channel-team-local",
    },
];

#[derive(Debug, Clone, Copy)]
struct BundledChannelWorker {
    channel_id: &'static str,
    binary_name: &'static str,
}

pub(crate) fn snapshot_overlays_for_source(source_path: &Path) -> Result<Vec<SnapshotOverlay>> {
    let Some(worker) = worker_for_bundled_source(source_path)? else {
        return Ok(Vec::new());
    };
    Ok(vec![SnapshotOverlay::new(
        worker_binary_path(worker.binary_name)?,
        PathBuf::from(format!("bin/{}", worker.binary_name)),
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
        "missing {}; build LionClaw with `cargo build --workspace --bins` before installing bundled channel skills",
        sibling.display()
    ))
}

fn worker_for_bundled_source(source_path: &Path) -> Result<Option<BundledChannelWorker>> {
    let source_path = fs::canonicalize(source_path)
        .with_context(|| format!("failed to resolve {}", source_path.display()))?;
    for worker in BUNDLED_CHANNEL_WORKERS {
        let bundled = bundled_channel_skill_dir(worker.channel_id);
        if !bundled.exists() {
            continue;
        }
        let bundled = fs::canonicalize(&bundled)
            .with_context(|| format!("failed to resolve {}", bundled.display()))?;
        if source_path == bundled {
            return Ok(Some(*worker));
        }
    }
    Ok(None)
}
