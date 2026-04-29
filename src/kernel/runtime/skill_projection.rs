use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tokio::fs;

use super::{MountSpec, SKILLS_MOUNT_TARGET_ROOT};
use crate::kernel::skills::validate_skill_alias;

pub async fn project_runtime_skills(
    runtime_kind: &str,
    runtime_state_root: &Path,
    mounts: &[MountSpec],
) -> Result<()> {
    let Some(native_relative_root) = native_runtime_skills_relative_root(runtime_kind) else {
        return Ok(());
    };

    let desired = desired_skill_symlinks(mounts)?;
    if desired.is_empty() {
        let Some(native_root) =
            existing_safe_runtime_dir(runtime_state_root, native_relative_root).await?
        else {
            return Ok(());
        };
        return reconcile_skill_symlinks(&native_root, &desired).await;
    }

    let native_root = ensure_safe_runtime_dir(runtime_state_root, native_relative_root).await?;

    reconcile_skill_symlinks(&native_root, &desired).await
}

fn native_runtime_skills_relative_root(runtime_kind: &str) -> Option<&'static [&'static str]> {
    match runtime_kind {
        "codex" => Some(&["home", ".codex", "skills"]),
        "opencode" => Some(&["home", ".config", "opencode", "skills"]),
        _ => None,
    }
}

async fn existing_safe_runtime_dir(
    runtime_state_root: &Path,
    relative_components: &[&str],
) -> Result<Option<PathBuf>> {
    ensure_safe_directory_state(runtime_state_root, relative_components, false).await
}

async fn ensure_safe_runtime_dir(
    runtime_state_root: &Path,
    relative_components: &[&str],
) -> Result<PathBuf> {
    ensure_safe_directory_state(runtime_state_root, relative_components, true)
        .await?
        .ok_or_else(|| anyhow::anyhow!("runtime skill root disappeared during preparation"))
}

async fn ensure_safe_directory_state(
    runtime_state_root: &Path,
    relative_components: &[&str],
    create_missing: bool,
) -> Result<Option<PathBuf>> {
    let mut current = runtime_state_root.to_path_buf();
    if !ensure_safe_directory(&current, create_missing).await? {
        return Ok(None);
    }

    for component in relative_components {
        current.push(component);
        match ensure_safe_directory(&current, create_missing).await? {
            true => {}
            false => return Ok(None),
        }
    }

    Ok(Some(current))
}

async fn ensure_safe_directory(path: &Path, create_missing: bool) -> Result<bool> {
    match fs::symlink_metadata(path).await {
        Ok(metadata) => {
            validate_directory_metadata(path, &metadata)?;
            Ok(true)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            if !create_missing {
                return Ok(false);
            }
            match fs::create_dir(path).await {
                Ok(()) => Ok(true),
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    let metadata = fs::symlink_metadata(path)
                        .await
                        .with_context(|| format!("failed to stat {}", path.display()))?;
                    validate_directory_metadata(path, &metadata)?;
                    Ok(true)
                }
                Err(err) => {
                    Err(err).with_context(|| format!("failed to create {}", path.display()))
                }
            }
        }
        Err(err) => Err(err).with_context(|| format!("failed to stat {}", path.display())),
    }
}

fn validate_directory_metadata(path: &Path, metadata: &std::fs::Metadata) -> Result<()> {
    if metadata.file_type().is_symlink() {
        anyhow::bail!(
            "runtime skill projection refuses symlinked path component '{}'",
            path.display()
        );
    }
    if !metadata.is_dir() {
        anyhow::bail!(
            "runtime skill projection expected directory '{}'",
            path.display()
        );
    }
    Ok(())
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

    #[tokio::test]
    async fn removes_stale_runtime_skill_symlinks_when_no_skills_remain() {
        let temp_dir = tempdir().expect("temp dir");
        let runtime_root = temp_dir.path().join("runtime");
        let stale_root = runtime_root.join("home/.codex/skills");
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

        project_runtime_skills("codex", &runtime_root, &[])
            .await
            .expect("project empty skill set");

        assert!(!tokio::fs::try_exists(stale_root.join("old"))
            .await
            .expect("check stale link"));
    }

    #[tokio::test]
    async fn rejects_symlinked_runtime_path_components() {
        let temp_dir = tempdir().expect("temp dir");
        let runtime_root = temp_dir.path().join("runtime");
        let outside_root = temp_dir.path().join("outside-home");
        tokio::fs::create_dir_all(&runtime_root)
            .await
            .expect("create runtime root");
        tokio::fs::create_dir_all(&outside_root)
            .await
            .expect("create outside root");
        tokio::task::spawn_blocking({
            let home_link = runtime_root.join("home");
            let outside_root = outside_root.clone();
            move || std::os::unix::fs::symlink(outside_root, home_link)
        })
        .await
        .expect("join home symlink")
        .expect("create home symlink");

        let mounts = vec![MountSpec {
            source: temp_dir.path().join("skills/terminal"),
            target: "/lionclaw/skills/terminal".to_string(),
            access: MountAccess::ReadOnly,
        }];

        let err = project_runtime_skills("codex", &runtime_root, &mounts)
            .await
            .expect_err("reject symlinked home");
        assert!(
            err.to_string().contains("refuses symlinked path component"),
            "unexpected error: {err:#}"
        );
        assert!(
            !tokio::fs::try_exists(outside_root.join(".codex"))
                .await
                .expect("check outside codex dir"),
            "projection must not follow runtime-owned symlinks"
        );
    }
}
