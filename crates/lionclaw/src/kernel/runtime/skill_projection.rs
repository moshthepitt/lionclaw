use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result};
use tokio::fs;

use super::{
    runtime_skill_mount_target_alias, MountSpec, RuntimeSkillProjectionConfig,
    SKILLS_MOUNT_TARGET_ROOT,
};

pub async fn project_runtime_skills(
    skill_projection: Option<&RuntimeSkillProjectionConfig>,
    runtime_home_root: &Path,
    mounts: &[MountSpec],
) -> Result<()> {
    let Some(RuntimeSkillProjectionConfig::NativeDir { root, .. }) = skill_projection else {
        return Ok(());
    };
    let native_relative_root = native_dir_components(root)?;

    let desired = desired_skill_symlinks(mounts)?;
    if desired.is_empty() {
        let Some(native_root) =
            existing_safe_runtime_dir(runtime_home_root, &native_relative_root).await?
        else {
            return Ok(());
        };
        return reconcile_skill_symlinks(&native_root, &desired).await;
    }

    let native_root = ensure_safe_runtime_dir(runtime_home_root, &native_relative_root).await?;

    reconcile_skill_symlinks(&native_root, &desired).await
}

fn native_dir_components(root: &str) -> Result<Vec<String>> {
    let mut projection = RuntimeSkillProjectionConfig::NativeDir {
        root: root.to_string(),
        format: Default::default(),
    };
    projection.normalize();
    projection.validate()?;
    Path::new(projection.native_dir_root())
        .components()
        .map(|component| match component {
            Component::Normal(value) => Ok(value.to_string_lossy().to_string()),
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => anyhow::bail!(
                "runtime skill projection root must not contain traversal or absolute components"
            ),
        })
        .collect()
}

async fn existing_safe_runtime_dir(
    runtime_home_root: &Path,
    relative_components: &[String],
) -> Result<Option<PathBuf>> {
    ensure_safe_directory_state(runtime_home_root, relative_components, false).await
}

async fn ensure_safe_runtime_dir(
    runtime_home_root: &Path,
    relative_components: &[String],
) -> Result<PathBuf> {
    ensure_safe_directory_state(runtime_home_root, relative_components, true)
        .await?
        .ok_or_else(|| anyhow::anyhow!("runtime skill root disappeared during preparation"))
}

async fn ensure_safe_directory_state(
    runtime_home_root: &Path,
    relative_components: &[String],
    create_missing: bool,
) -> Result<Option<PathBuf>> {
    let mut current = runtime_home_root.to_path_buf();
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
        if !mount
            .target
            .strip_prefix(SKILLS_MOUNT_TARGET_ROOT)
            .is_some_and(|suffix| suffix.starts_with('/'))
        {
            continue;
        }

        let Some(alias) = runtime_skill_mount_target_alias(&mount.target) else {
            anyhow::bail!(
                "invalid runtime skill mount target '{}' must be under {SKILLS_MOUNT_TARGET_ROOT}/<alias>",
                mount.target
            );
        };
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
            remove_stale_managed_symlink(&path).await?;
            continue;
        };

        if symlink_points_to(&path, target).await? {
            continue;
        }

        replace_managed_symlink(&path, &alias, target).await?;
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
    Ok(target == Path::new(expected_target))
}

async fn remove_stale_managed_symlink(path: &Path) -> Result<()> {
    if !managed_skill_symlink(path).await? {
        return Ok(());
    }
    remove_symlink(path).await
}

async fn replace_managed_symlink(path: &Path, alias: &str, target: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .await
        .with_context(|| format!("failed to stat {}", path.display()))?;
    if !metadata.file_type().is_symlink() {
        anyhow::bail!(
            "runtime skill alias '{}' already exists at '{}' and is not LionClaw-managed",
            alias,
            path.display()
        );
    }
    if !managed_skill_symlink(path).await? {
        anyhow::bail!(
            "runtime skill alias '{}' already exists at '{}' and is not LionClaw-managed",
            alias,
            path.display()
        );
    }
    remove_symlink(path).await?;
    create_symlink(target, path).await
}

async fn managed_skill_symlink(path: &Path) -> Result<bool> {
    let metadata = fs::symlink_metadata(path)
        .await
        .with_context(|| format!("failed to stat {}", path.display()))?;
    if !metadata.file_type().is_symlink() {
        return Ok(false);
    }
    let target = fs::read_link(path)
        .await
        .with_context(|| format!("failed to read link {}", path.display()))?;
    Ok(target
        .to_str()
        .is_some_and(|target| runtime_skill_mount_target_alias(target).is_some()))
}

async fn remove_symlink(path: &Path) -> Result<()> {
    fs::remove_file(path)
        .await
        .with_context(|| format!("failed to remove {}", path.display()))?;
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
    use crate::kernel::runtime::{MountAccess, MountSpec, RuntimeSkillProjectionConfig};

    fn native_projection() -> RuntimeSkillProjectionConfig {
        RuntimeSkillProjectionConfig::native_dir(".native/skills")
    }

    #[tokio::test]
    async fn projects_runtime_skills_into_profile_declared_root() {
        let temp_dir = tempdir().expect("temp dir");
        let runtime_home = temp_dir.path().join("runtime-home");
        let projection = native_projection();
        let mounts = vec![MountSpec {
            source: temp_dir.path().join("skills/loopback"),
            target: "/lionclaw/skills/loopback".to_string(),
            access: MountAccess::ReadOnly,
        }];

        project_runtime_skills(Some(&projection), &runtime_home, &mounts)
            .await
            .expect("project runtime skills");

        let link = runtime_home.join(".native/skills/loopback");
        assert_eq!(
            tokio::fs::read_link(&link).await.expect("read link"),
            PathBuf::from("/lionclaw/skills/loopback")
        );
    }

    #[tokio::test]
    async fn skips_native_projection_when_profile_declares_none() {
        let temp_dir = tempdir().expect("temp dir");
        let runtime_home = temp_dir.path().join("runtime-home");
        let mounts = vec![MountSpec {
            source: temp_dir.path().join("skills/loopback"),
            target: "/lionclaw/skills/loopback".to_string(),
            access: MountAccess::ReadOnly,
        }];

        project_runtime_skills(None, &runtime_home, &mounts)
            .await
            .expect("skip runtime skills");

        assert!(!tokio::fs::try_exists(runtime_home.join(".native"))
            .await
            .expect("check native root"));
    }

    #[tokio::test]
    async fn removes_stale_runtime_skill_symlinks() {
        let temp_dir = tempdir().expect("temp dir");
        let runtime_home = temp_dir.path().join("runtime-home");
        let projection = native_projection();
        let stale_root = runtime_home.join(".native/skills");
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

        project_runtime_skills(Some(&projection), &runtime_home, &mounts)
            .await
            .expect("project runtime skills");

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
        let runtime_home = temp_dir.path().join("runtime-home");
        let projection = native_projection();
        let stale_root = runtime_home.join(".native/skills");
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

        project_runtime_skills(Some(&projection), &runtime_home, &[])
            .await
            .expect("project empty skill set");

        assert!(!tokio::fs::try_exists(stale_root.join("old"))
            .await
            .expect("check stale link"));
    }

    #[tokio::test]
    async fn preserves_skill_symlinks_with_escaping_targets_as_unmanaged() {
        let temp_dir = tempdir().expect("temp dir");
        let runtime_home = temp_dir.path().join("runtime-home");
        let projection = native_projection();
        let stale_root = runtime_home.join(".native/skills");
        let escaping_link = stale_root.join("old");
        tokio::fs::create_dir_all(&stale_root)
            .await
            .expect("create stale root");
        tokio::task::spawn_blocking({
            let escaping_link = escaping_link.clone();
            move || std::os::unix::fs::symlink("/lionclaw/skills/../custom", escaping_link)
        })
        .await
        .expect("join escaping link")
        .expect("create escaping link");

        project_runtime_skills(Some(&projection), &runtime_home, &[])
            .await
            .expect("project empty skill set");

        assert_eq!(
            tokio::fs::read_link(&escaping_link)
                .await
                .expect("read escaping link"),
            PathBuf::from("/lionclaw/skills/../custom")
        );
    }

    #[tokio::test]
    async fn preserves_unmanaged_runtime_skill_entries() {
        let temp_dir = tempdir().expect("temp dir");
        let runtime_home = temp_dir.path().join("runtime-home");
        let projection = native_projection();
        let native_root = runtime_home.join(".native/skills");
        tokio::fs::create_dir_all(native_root.join("native"))
            .await
            .expect("create native dir");
        tokio::task::spawn_blocking({
            let stale = native_root.join("old");
            move || std::os::unix::fs::symlink("/lionclaw/skills/old", stale)
        })
        .await
        .expect("join stale link")
        .expect("create stale link");

        project_runtime_skills(Some(&projection), &runtime_home, &[])
            .await
            .expect("project empty skill set");

        assert!(tokio::fs::try_exists(native_root.join("native"))
            .await
            .expect("check native dir"));
        assert!(!tokio::fs::try_exists(native_root.join("old"))
            .await
            .expect("check stale link"));
    }

    #[tokio::test]
    async fn rejects_alias_collision_with_unmanaged_runtime_entry() {
        let temp_dir = tempdir().expect("temp dir");
        let runtime_home = temp_dir.path().join("runtime-home");
        let projection = native_projection();
        let native_root = runtime_home.join(".native/skills");
        tokio::fs::create_dir_all(native_root.join("loopback"))
            .await
            .expect("create native dir");
        let mounts = vec![MountSpec {
            source: temp_dir.path().join("skills/loopback"),
            target: "/lionclaw/skills/loopback".to_string(),
            access: MountAccess::ReadOnly,
        }];

        let err = project_runtime_skills(Some(&projection), &runtime_home, &mounts)
            .await
            .expect_err("unmanaged alias collision should fail");

        assert!(err.to_string().contains("not LionClaw-managed"));
        assert!(tokio::fs::try_exists(native_root.join("loopback"))
            .await
            .expect("check native dir"));
    }

    #[tokio::test]
    async fn rejects_symlinked_runtime_path_components() {
        let temp_dir = tempdir().expect("temp dir");
        let runtime_home = temp_dir.path().join("runtime-home");
        let projection = native_projection();
        let outside_root = temp_dir.path().join("outside-home");
        tokio::fs::create_dir_all(&runtime_home)
            .await
            .expect("create runtime home");
        tokio::fs::create_dir_all(&outside_root)
            .await
            .expect("create outside root");
        tokio::task::spawn_blocking({
            let native_link = runtime_home.join(".native");
            let outside_root = outside_root.clone();
            move || std::os::unix::fs::symlink(outside_root, native_link)
        })
        .await
        .expect("join native symlink")
        .expect("create native symlink");

        let mounts = vec![MountSpec {
            source: temp_dir.path().join("skills/loopback"),
            target: "/lionclaw/skills/loopback".to_string(),
            access: MountAccess::ReadOnly,
        }];

        let err = project_runtime_skills(Some(&projection), &runtime_home, &mounts)
            .await
            .expect_err("reject symlinked native home");
        assert!(
            err.to_string().contains("refuses symlinked path component"),
            "unexpected error: {err:#}"
        );
        assert!(
            !tokio::fs::try_exists(outside_root.join("skills"))
                .await
                .expect("check outside skill dir"),
            "projection must not follow runtime-owned symlinks"
        );
    }
}
