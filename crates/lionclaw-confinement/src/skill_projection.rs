use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result};
use tokio::fs;
use tracing::warn;

use crate::{
    runtime_native_home_mount_source, runtime_skill_mount_target_alias, validate_skill_alias,
    EffectiveExecutionPlan, MountAccess, MountSpec, RuntimeSkillProjectionConfig,
    INHERITED_SKILLS_MOUNT_TARGET_ROOT, SKILLS_MOUNT_TARGET_ROOT,
};

/// Resolve configured human skill roots into read-only package mounts. The
/// harness still owns discovery and use; this only makes its existing packages
/// visible inside the confined runtime.
pub fn inherited_skill_mounts(
    projection: Option<&RuntimeSkillProjectionConfig>,
) -> Result<Vec<MountSpec>> {
    let Some(projection) = projection else {
        return Ok(Vec::new());
    };
    projection.validate()?;
    let mut mounts = Vec::new();
    for (root_index, inherited) in projection.inherited_roots().iter().enumerate() {
        let entries = match std::fs::read_dir(&inherited.source) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound && inherited.optional => {
                continue;
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "failed to read inherited skill root '{}'",
                        inherited.source.display()
                    )
                });
            }
        };
        let mut entries = entries.collect::<std::io::Result<Vec<_>>>()?;
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            let Some(alias) = entry.file_name().to_str().map(str::to_string) else {
                warn!(
                    root = %inherited.source.display(),
                    "skipping inherited skill with a non-UTF-8 name"
                );
                continue;
            };
            if let Err(err) = validate_skill_alias(&alias) {
                warn!(
                    root = %inherited.source.display(),
                    skill = %alias,
                    error = %err,
                    "skipping inherited skill with an invalid name"
                );
                continue;
            }
            let source = match entry.path().canonicalize() {
                Ok(source) => source,
                Err(err) => {
                    warn!(
                        path = %entry.path().display(),
                        error = %err,
                        "skipping unreadable inherited skill"
                    );
                    continue;
                }
            };
            if !source.is_dir() || !source.join("SKILL.md").is_file() {
                continue;
            }
            mounts.push(MountSpec {
                source,
                target: inherited_skill_mount_target(root_index, &alias),
                access: MountAccess::ReadOnly,
            });
        }
    }
    Ok(mounts)
}

/// Materialize the runtime-native symlink view described by the compiled plan.
/// This is filesystem projection only; LionClaw does not interpret or activate
/// skill instructions.
pub async fn project_runtime_skills(plan: &EffectiveExecutionPlan) -> Result<()> {
    let Some(projection) = plan.skill_projection.as_ref() else {
        return Ok(());
    };
    let runtime_home = runtime_native_home_mount_source(&plan.mounts)
        .context("skill projection requires a runtime home mount")?;
    let mut desired_by_root: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();

    for (root_index, inherited) in projection.inherited_roots().iter().enumerate() {
        let desired = desired_by_root.entry(inherited.target.clone()).or_default();
        for mount in &plan.mounts {
            let Some(alias) = inherited_skill_mount_alias(&mount.target, root_index) else {
                continue;
            };
            desired
                .entry(alias.to_string())
                .or_insert_with(|| mount.target.clone());
        }
    }

    let mission_root = projection.native_dir_root().to_string();
    let mission_desired = desired_by_root.entry(mission_root).or_default();
    for mount in &plan.mounts {
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
        // An explicitly assigned mission skill wins only when a single native
        // directory cannot represent both packages under the same name.
        if let Some(inherited_target) =
            mission_desired.insert(alias.to_string(), mount.target.clone())
        {
            if inherited_target != mount.target {
                warn!(
                    skill = %alias,
                    root = %projection.native_dir_root(),
                    "mission skill shadows an inherited skill in the same native directory"
                );
            }
        }
    }

    for (relative_root, desired) in desired_by_root {
        let components = native_dir_components(&relative_root)?;
        if desired.is_empty() {
            let Some(native_root) = existing_safe_runtime_dir(runtime_home, &components).await?
            else {
                continue;
            };
            reconcile_skill_symlinks(&native_root, &desired).await?;
            continue;
        }
        let native_root = ensure_safe_runtime_dir(runtime_home, &components).await?;
        reconcile_skill_symlinks(&native_root, &desired).await?;
    }
    Ok(())
}

fn inherited_skill_mount_target(root_index: usize, alias: &str) -> String {
    format!("{INHERITED_SKILLS_MOUNT_TARGET_ROOT}/{root_index}/{alias}")
}

fn inherited_skill_mount_alias(target: &str, expected_index: usize) -> Option<&str> {
    let suffix = target.strip_prefix(INHERITED_SKILLS_MOUNT_TARGET_ROOT)?;
    let suffix = suffix.strip_prefix('/')?;
    let (index, alias) = suffix.split_once('/')?;
    (index.parse::<usize>().ok() == Some(expected_index)
        && !alias.contains('/')
        && validate_skill_alias(alias).is_ok())
    .then_some(alias)
}

fn native_dir_components(root: &str) -> Result<Vec<String>> {
    let projection = RuntimeSkillProjectionConfig::native_dir(root);
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
        if !ensure_safe_directory(&current, create_missing).await? {
            return Ok(None);
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
        let alias = entry.file_name().to_string_lossy().to_string();
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
        if !existing.contains(alias) {
            create_symlink(target, &root.join(alias)).await?;
        }
    }
    Ok(())
}

async fn symlink_points_to(path: &Path, expected_target: &str) -> Result<bool> {
    let metadata = match fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err).with_context(|| format!("failed to stat {}", path.display())),
    };
    if !metadata.file_type().is_symlink() {
        return Ok(false);
    }
    Ok(fs::read_link(path)
        .await
        .with_context(|| format!("failed to read link {}", path.display()))?
        == Path::new(expected_target))
}

async fn remove_stale_managed_symlink(path: &Path) -> Result<()> {
    if managed_skill_symlink(path).await? {
        fs::remove_file(path)
            .await
            .with_context(|| format!("failed to remove {}", path.display()))?;
    }
    Ok(())
}

async fn replace_managed_symlink(path: &Path, alias: &str, target: &str) -> Result<()> {
    if !managed_skill_symlink(path).await? {
        anyhow::bail!(
            "runtime skill alias '{}' already exists at '{}' and is not LionClaw-managed",
            alias,
            path.display()
        );
    }
    fs::remove_file(path)
        .await
        .with_context(|| format!("failed to remove {}", path.display()))?;
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
    Ok(target.to_str().is_some_and(|target| {
        runtime_skill_mount_target_alias(target).is_some()
            || target
                .strip_prefix(INHERITED_SKILLS_MOUNT_TARGET_ROOT)
                .is_some_and(|suffix| suffix.starts_with('/'))
    }))
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
    use tempfile::tempdir;

    use super::*;
    use crate::{
        ConfinementConfig, ExecutionLimits, InheritedSkillRoot, InstallPolicy, NetworkMode,
        OciConfinementConfig, WorkspaceAccess, RUNTIME_HOME_MOUNT_TARGET,
    };

    fn plan(
        runtime_home: &Path,
        projection: RuntimeSkillProjectionConfig,
        mounts: Vec<MountSpec>,
    ) -> EffectiveExecutionPlan {
        let mut all_mounts = vec![MountSpec {
            source: runtime_home.to_path_buf(),
            target: RUNTIME_HOME_MOUNT_TARGET.to_string(),
            access: MountAccess::ReadWrite,
        }];
        all_mounts.extend(mounts);
        EffectiveExecutionPlan {
            runtime_id: "configured-runtime".to_string(),
            preset_name: "test".to_string(),
            confinement: ConfinementConfig::Oci(OciConfinementConfig::default()),
            skill_projection: Some(projection),
            workspace_access: WorkspaceAccess::ReadOnly,
            network_mode: NetworkMode::None,
            install_policy: InstallPolicy::None,
            root_in_userns: false,
            working_dir: None,
            environment: Vec::new(),
            mcp_servers: Vec::new(),
            hard_timeout: std::time::Duration::from_secs(1),
            mounts: all_mounts,
            mount_runtime_secrets: false,
            escape_classes: Default::default(),
            limits: ExecutionLimits::default(),
        }
    }

    #[tokio::test]
    async fn projects_mission_and_inherited_skills_through_configured_roots() {
        let temp = tempdir().unwrap();
        let inherited = temp.path().join("human-skills");
        let human_skill = inherited.join("human-skill");
        std::fs::create_dir_all(&human_skill).unwrap();
        std::fs::write(human_skill.join("SKILL.md"), "human").unwrap();

        let mut projection = RuntimeSkillProjectionConfig::native_dir(".agents/skills");
        projection.inherited_roots_mut().push(InheritedSkillRoot {
            source: inherited,
            target: ".native/skills".to_string(),
            optional: false,
        });
        let mut mounts = inherited_skill_mounts(Some(&projection)).unwrap();
        mounts.push(MountSpec {
            source: temp.path().join("mission-skill"),
            target: "/lionclaw/skills/mission-skill".to_string(),
            access: MountAccess::ReadOnly,
        });
        let runtime_home = temp.path().join("runtime-home");
        let plan = plan(&runtime_home, projection, mounts);

        project_runtime_skills(&plan).await.unwrap();

        assert_eq!(
            std::fs::read_link(runtime_home.join(".native/skills/human-skill")).unwrap(),
            PathBuf::from("/lionclaw/inherited-skills/0/human-skill")
        );
        assert_eq!(
            std::fs::read_link(runtime_home.join(".agents/skills/mission-skill")).unwrap(),
            PathBuf::from("/lionclaw/skills/mission-skill")
        );
    }

    #[tokio::test]
    async fn mission_skill_wins_a_same_root_same_alias_collision() {
        let temp = tempdir().unwrap();
        let inherited = temp.path().join("human-skills");
        let inherited_skill = inherited.join("shared-skill");
        std::fs::create_dir_all(&inherited_skill).unwrap();
        std::fs::write(inherited_skill.join("SKILL.md"), "human").unwrap();

        let mut projection = RuntimeSkillProjectionConfig::native_dir(".agents/skills");
        projection.inherited_roots_mut().push(InheritedSkillRoot {
            source: inherited,
            target: ".agents/skills".to_string(),
            optional: false,
        });
        let mut mounts = inherited_skill_mounts(Some(&projection)).unwrap();
        mounts.push(MountSpec {
            source: temp.path().join("mission-skill"),
            target: "/lionclaw/skills/shared-skill".to_string(),
            access: MountAccess::ReadOnly,
        });
        let runtime_home = temp.path().join("runtime-home");

        project_runtime_skills(&plan(&runtime_home, projection, mounts))
            .await
            .unwrap();

        assert_eq!(
            std::fs::read_link(runtime_home.join(".agents/skills/shared-skill")).unwrap(),
            PathBuf::from("/lionclaw/skills/shared-skill")
        );
    }

    #[tokio::test]
    async fn reconciliation_removes_only_stale_lionclaw_links() {
        let temp = tempdir().unwrap();
        let runtime_home = temp.path().join("runtime-home");
        let native_root = runtime_home.join(".agents/skills");
        std::fs::create_dir_all(&native_root).unwrap();
        std::os::unix::fs::symlink("/lionclaw/skills/stale", native_root.join("stale")).unwrap();
        std::os::unix::fs::symlink(
            "/lionclaw/inherited-skills-user/ambient",
            native_root.join("human-link"),
        )
        .unwrap();
        std::fs::create_dir(native_root.join("human-directory")).unwrap();

        project_runtime_skills(&plan(
            &runtime_home,
            RuntimeSkillProjectionConfig::native_dir(".agents/skills"),
            Vec::new(),
        ))
        .await
        .unwrap();

        assert!(!native_root.join("stale").exists());
        assert!(std::fs::symlink_metadata(native_root.join("human-link")).is_ok());
        assert!(native_root.join("human-directory").is_dir());
    }

    #[tokio::test]
    async fn projection_refuses_a_symlinked_native_path_component() {
        let temp = tempdir().unwrap();
        let runtime_home = temp.path().join("runtime-home");
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(&runtime_home).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, runtime_home.join(".agents")).unwrap();
        let mounts = vec![MountSpec {
            source: temp.path().join("mission-skill"),
            target: "/lionclaw/skills/mission-skill".to_string(),
            access: MountAccess::ReadOnly,
        }];

        let err = project_runtime_skills(&plan(
            &runtime_home,
            RuntimeSkillProjectionConfig::native_dir(".agents/skills"),
            mounts,
        ))
        .await
        .expect_err("symlinked native root");

        assert!(err.to_string().contains("symlinked path component"));
        assert!(!outside.join("skills").exists());
    }

    #[test]
    fn optional_missing_inherited_root_is_not_an_error() {
        let mut projection = RuntimeSkillProjectionConfig::native_dir(".agents/skills");
        projection.inherited_roots_mut().push(InheritedSkillRoot {
            source: "/definitely/missing/lionclaw-skills".into(),
            target: ".native/skills".to_string(),
            optional: true,
        });
        assert!(inherited_skill_mounts(Some(&projection))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn inherited_mount_resolution_rejects_unvalidated_relative_sources() {
        let mut projection = RuntimeSkillProjectionConfig::native_dir(".agents/skills");
        projection.inherited_roots_mut().push(InheritedSkillRoot {
            source: "relative/skills".into(),
            target: ".native/skills".to_string(),
            optional: true,
        });

        let err = inherited_skill_mounts(Some(&projection)).expect_err("relative source");
        assert!(err.to_string().contains("must be absolute"));
    }
}
