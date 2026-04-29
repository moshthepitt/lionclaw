use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tokio::fs;

use super::{MountSpec, SKILLS_MOUNT_TARGET_ROOT};
use crate::kernel::skills::validate_skill_alias;

pub async fn project_runtime_skills(
    runtime_id: &str,
    runtime_state_root: &Path,
    mounts: &[MountSpec],
) -> Result<()> {
    let Some(native_root) = native_runtime_skills_root(runtime_id, runtime_state_root) else {
        return Ok(());
    };

    let desired = desired_skill_symlinks(mounts)?;
    if desired.is_empty() {
        return Ok(());
    }

    if let Some(parent) = native_root.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::create_dir_all(&native_root)
        .await
        .with_context(|| format!("failed to create {}", native_root.display()))?;

    reconcile_skill_symlinks(&native_root, &desired).await
}

fn native_runtime_skills_root(runtime_id: &str, runtime_state_root: &Path) -> Option<PathBuf> {
    match runtime_id {
        "codex" => Some(
            runtime_state_root
                .join("home")
                .join(".codex")
                .join("skills"),
        ),
        "opencode" => Some(
            runtime_state_root
                .join("home")
                .join(".config")
                .join("opencode")
                .join("skills"),
        ),
        _ => None,
    }
}

fn desired_skill_symlinks(mounts: &[MountSpec]) -> Result<BTreeMap<String, String>> {
    let mut desired = BTreeMap::new();

    for mount in mounts {
        let Some(alias) = mount
            .target
            .strip_prefix(SKILLS_MOUNT_TARGET_ROOT)
            .and_then(|suffix| suffix.strip_prefix('/'))
        else {
            continue;
        };

        validate_skill_alias(alias)
            .with_context(|| format!("invalid runtime skill mount target '{}'", mount.target))?;
        desired.insert(alias.to_string(), mount.target.clone());
    }

    Ok(desired)
}

async fn reconcile_skill_symlinks(root: &Path, desired: &BTreeMap<String, String>) -> Result<()> {
    let mut existing = BTreeSet::new();
    let mut entries = fs::read_dir(root)
        .await
        .with_context(|| format!("failed to read {}", root.display()))?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("failed to read directory entry under {}", root.display()))?
    {
        let path = entry.path();
        let file_name = entry.file_name();
        let alias = file_name.to_string_lossy().to_string();
        existing.insert(alias.clone());

        let Some(target) = desired.get(&alias) else {
            remove_managed_path(&path).await?;
            continue;
        };

        if symlink_points_to(&path, target).await? {
            continue;
        }

        remove_managed_path(&path).await?;
        create_symlink(target, &path).await?;
    }

    for (alias, target) in desired {
        if existing.contains(alias) {
            continue;
        }
        create_symlink(target, &root.join(alias)).await?;
    }

    Ok(())
}

async fn symlink_points_to(path: &Path, expected_target: &str) -> Result<bool> {
    let metadata = match fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => {
            return Err(err).with_context(|| format!("failed to stat {}", path.display()));
        }
    };

    if !metadata.file_type().is_symlink() {
        return Ok(false);
    }

    let target = fs::read_link(path)
        .await
        .with_context(|| format!("failed to read link {}", path.display()))?;
    Ok(target == PathBuf::from(expected_target))
}

async fn remove_managed_path(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .await
        .with_context(|| format!("failed to stat {}", path.display()))?;
    if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() {
        fs::remove_dir_all(path)
            .await
            .with_context(|| format!("failed to remove {}", path.display()))?;
    } else {
        fs::remove_file(path)
            .await
            .with_context(|| format!("failed to remove {}", path.display()))?;
    }
    Ok(())
}

async fn create_symlink(target: &str, path: &Path) -> Result<()> {
    let target = target.to_string();
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&target, &path)
                .with_context(|| format!("failed to symlink {} -> {}", path.display(), target))
        }

        #[cfg(not(unix))]
        {
            let _ = target;
            let _ = path;
            Err(anyhow::anyhow!(
                "runtime skill projection requires unix-style symlink support"
            ))
        }
    })
    .await
    .context("failed to join runtime skill projection task")??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tempfile::tempdir;

    use super::project_runtime_skills;
    use crate::kernel::runtime::{MountAccess, MountSpec};

    #[tokio::test]
    async fn projects_runtime_skills_into_codex_home() {
        let temp_dir = tempdir().expect("temp dir");
        let runtime_root = temp_dir.path().join("runtime");
        let mounts = vec![MountSpec {
            source: temp_dir.path().join("skills/terminal"),
            target: "/lionclaw/skills/terminal".to_string(),
            access: MountAccess::ReadOnly,
        }];

        project_runtime_skills("codex", &runtime_root, &mounts)
            .await
            .expect("project codex skills");

        let link = runtime_root.join("home/.codex/skills/terminal");
        assert_eq!(
            tokio::fs::read_link(&link).await.expect("read link"),
            PathBuf::from("/lionclaw/skills/terminal")
        );
    }

    #[tokio::test]
    async fn removes_stale_runtime_skill_symlinks() {
        let temp_dir = tempdir().expect("temp dir");
        let runtime_root = temp_dir.path().join("runtime");
        let stale_root = runtime_root.join("home/.config/opencode/skills");
        tokio::fs::create_dir_all(&stale_root)
            .await
            .expect("create stale root");
        tokio::task::spawn_blocking({
            let stale = stale_root.join("old");
            move || std::os::unix::fs::symlink("/lionclaw/skills/old", stale)
        })
        .await
        .expect("join stale link")
        .expect("create stale link");

        let mounts = vec![MountSpec {
            source: temp_dir.path().join("skills/new"),
            target: "/lionclaw/skills/new".to_string(),
            access: MountAccess::ReadOnly,
        }];

        project_runtime_skills("opencode", &runtime_root, &mounts)
            .await
            .expect("project opencode skills");

        assert!(!tokio::fs::try_exists(stale_root.join("old"))
            .await
            .expect("check stale link"));
        assert_eq!(
            tokio::fs::read_link(stale_root.join("new"))
                .await
                .expect("read new link"),
            PathBuf::from("/lionclaw/skills/new")
        );
    }
}
