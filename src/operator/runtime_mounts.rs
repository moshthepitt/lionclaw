use std::path::Path;

use anyhow::{anyhow, bail, Result};

use crate::{
    home::LionClawHome,
    kernel::{
        runtime::{
            execution::mount_validation::{
                canonical_mount_source, normalize_runtime_mount_target,
                project_metadata_root_from_instance_home, validate_configured_mount_target,
                validate_configured_mounts, MountSourceProtection,
            },
            ConfinementConfig, MountAccess, MountSpec,
        },
        skills::validate_skill_alias,
    },
    operator::config::{OperatorConfig, RuntimeProfileConfig},
};

#[derive(Clone, Copy)]
pub(crate) struct RuntimeMountContext<'a> {
    pub home: &'a LionClawHome,
    pub project_root: Option<&'a Path>,
    pub work_root: Option<&'a Path>,
}

pub(crate) fn resolve_runtime_mount_target(raw: &str) -> Result<String> {
    let target = raw.trim();
    if target.is_empty() {
        bail!("mount target is required");
    }
    if target.starts_with('/') {
        return validate_configured_mount_target(target).map_err(anyhow::Error::msg);
    }

    validate_skill_alias(target)
        .map_err(|err| anyhow!("mount target shorthand '{target}' is invalid: {err}"))?;
    validate_configured_mount_target(&format!("/mnt/{target}")).map_err(anyhow::Error::msg)
}

pub(crate) fn add_runtime_mount(
    config: &mut OperatorConfig,
    context: RuntimeMountContext<'_>,
    runtime_id: &str,
    target: &str,
    source: &Path,
    access: MountAccess,
) -> Result<MountSpec> {
    let target = resolve_runtime_mount_target(target)?;
    let source = canonical_mount_source(source).map_err(anyhow::Error::msg)?;
    let mount = MountSpec {
        source,
        target,
        access,
    };
    let protections =
        mount_source_protections(context.home, context.project_root, context.work_root);
    validate_configured_mounts(std::slice::from_ref(&mount), &protections)
        .map_err(anyhow::Error::msg)?;

    let profile = runtime_profile_mut(config, runtime_id)?;
    let oci = runtime_oci_mut(profile);
    if oci
        .additional_mounts
        .iter()
        .any(|existing| same_mount_target(&existing.target, &mount.target))
    {
        bail!(
            "runtime profile '{runtime_id}' already has mount target '{}'",
            mount.target
        );
    }

    oci.additional_mounts.push(mount.clone());
    sort_mounts(&mut oci.additional_mounts);
    validate_configured_mounts(&oci.additional_mounts, &protections).map_err(anyhow::Error::msg)?;
    Ok(mount)
}

pub(crate) fn remove_runtime_mount(
    config: &mut OperatorConfig,
    runtime_id: &str,
    target: &str,
) -> Result<MountSpec> {
    let targets = remove_target_candidates(target)?;
    let display_target = targets
        .first()
        .map(String::as_str)
        .unwrap_or_else(|| target.trim());
    let profile = runtime_profile_mut(config, runtime_id)?;
    let mounts = &mut runtime_oci_mut(profile).additional_mounts;
    let matches = mounts
        .iter()
        .enumerate()
        .filter_map(|(index, mount)| {
            targets
                .iter()
                .any(|target| same_mount_target(&mount.target, target))
                .then_some(index)
        })
        .collect::<Vec<_>>();

    match matches.as_slice() {
        [] => bail!(
            "runtime profile '{runtime_id}' has no mount target '{display_target}'"
        ),
        [index] => Ok(mounts.remove(*index)),
        _ => bail!(
            "runtime profile '{runtime_id}' has multiple mounts for target '{display_target}'; edit the config to remove the ambiguity"
        ),
    }
}

pub(crate) fn configured_runtime_mounts<'a>(
    config: &'a OperatorConfig,
    runtime_id: &str,
) -> Result<&'a [MountSpec]> {
    let profile = config
        .runtime(runtime_id)
        .ok_or_else(|| anyhow!("runtime profile '{runtime_id}' is not configured"))?;
    let ConfinementConfig::Oci(oci) = profile.confinement();
    Ok(&oci.additional_mounts)
}

pub(crate) fn validate_runtime_profile_mounts(
    home: &LionClawHome,
    project_root: Option<&Path>,
    work_root: Option<&Path>,
    profile: &RuntimeProfileConfig,
) -> Result<()> {
    let ConfinementConfig::Oci(oci) = profile.confinement();
    validate_configured_mounts(
        &oci.additional_mounts,
        &mount_source_protections(home, project_root, work_root),
    )
    .map_err(anyhow::Error::msg)
}

pub(crate) fn validate_configured_runtime_mounts(
    home: &LionClawHome,
    project_root: Option<&Path>,
    work_root: Option<&Path>,
    config: &OperatorConfig,
    runtime_id: &str,
) -> Result<()> {
    let profile = config
        .runtime(runtime_id)
        .ok_or_else(|| anyhow!("runtime profile '{runtime_id}' is not configured"))?;
    validate_runtime_profile_mounts(home, project_root, work_root, profile)
}

fn runtime_profile_mut<'a>(
    config: &'a mut OperatorConfig,
    runtime_id: &str,
) -> Result<&'a mut RuntimeProfileConfig> {
    config
        .runtimes
        .get_mut(runtime_id)
        .ok_or_else(|| anyhow!("runtime profile '{runtime_id}' is not configured"))
}

fn runtime_oci_mut(
    profile: &mut RuntimeProfileConfig,
) -> &mut crate::kernel::runtime::OciConfinementConfig {
    match profile.confinement_mut() {
        ConfinementConfig::Oci(oci) => oci,
    }
}

fn remove_target_candidates(raw: &str) -> Result<Vec<String>> {
    let target = raw.trim();
    if target.is_empty() {
        bail!("mount target is required");
    }

    let mut candidates = Vec::new();
    if target.starts_with('/') {
        if let Ok(normalized) = normalize_runtime_mount_target(target) {
            candidates.push(normalized);
        }
    } else if let Ok(resolved) = resolve_runtime_mount_target(target) {
        candidates.push(resolved);
    }
    if !candidates.iter().any(|candidate| candidate == target) {
        candidates.push(target.to_string());
    }
    Ok(candidates)
}

fn same_mount_target(left: &str, right: &str) -> bool {
    match (
        validate_configured_mount_target(left),
        validate_configured_mount_target(right),
    ) {
        (Ok(left), Ok(right)) => left == right,
        _ => left.trim() == right.trim(),
    }
}

fn sort_mounts(mounts: &mut [MountSpec]) {
    mounts.sort_by(|left, right| {
        left.target
            .cmp(&right.target)
            .then_with(|| left.source.cmp(&right.source))
    });
}

fn mount_source_protections(
    home: &LionClawHome,
    project_root: Option<&Path>,
    work_root: Option<&Path>,
) -> Vec<MountSourceProtection> {
    let home_root = home.root();
    let mut roots = vec![MountSourceProtection::new(
        home_root.clone(),
        "the selected instance home",
    )];
    if let Some(metadata_root) = project_metadata_root_from_instance_home(&home_root) {
        roots.push(MountSourceProtection::new(
            metadata_root,
            "project metadata",
        ));
    }
    if let Some(project_root) = project_root {
        roots.push(MountSourceProtection::new(
            project_root.join(".lionclaw"),
            "project metadata",
        ));
    }
    if let Some(work_root) = work_root {
        roots.push(MountSourceProtection::new(
            work_root.join(".lionclaw"),
            "project metadata",
        ));
    }
    roots
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        kernel::runtime::{ExecutionLimits, OciConfinementConfig},
        operator::config::RuntimeProfileConfig,
    };
    use std::path::PathBuf;

    fn profile(engine: String, source: Option<PathBuf>) -> RuntimeProfileConfig {
        RuntimeProfileConfig::Codex {
            executable: "codex".to_string(),
            model: None,
            confinement: ConfinementConfig::Oci(OciConfinementConfig {
                engine,
                image: Some("runtime:v1".to_string()),
                read_only_rootfs: false,
                tmpfs: Vec::new(),
                additional_mounts: source
                    .map(|source| {
                        vec![MountSpec {
                            source,
                            target: "/mnt/docs".to_string(),
                            access: MountAccess::ReadOnly,
                        }]
                    })
                    .unwrap_or_default(),
                limits: ExecutionLimits::default(),
            }),
        }
    }

    #[test]
    fn target_shorthand_resolves_under_mnt() {
        assert_eq!(
            resolve_runtime_mount_target("docs").expect("shorthand"),
            "/mnt/docs"
        );
        assert_eq!(
            resolve_runtime_mount_target("/reference/docs/").expect("absolute"),
            "/reference/docs"
        );
        assert!(resolve_runtime_mount_target("../docs").is_err());
        assert!(resolve_runtime_mount_target("/workspace/docs").is_err());
    }

    #[test]
    fn add_list_and_remove_runtime_mounts_by_target_identity() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let source = temp_dir.path().join("docs");
        std::fs::create_dir(&source).expect("source");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw/instances/main"));
        std::fs::create_dir_all(home.root()).expect("home");

        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), profile("podman".to_string(), None));

        let added = add_runtime_mount(
            &mut config,
            RuntimeMountContext {
                home: &home,
                project_root: None,
                work_root: None,
            },
            "codex",
            "docs",
            &source,
            MountAccess::ReadWrite,
        )
        .expect("add mount");
        assert_eq!(added.target, "/mnt/docs");
        assert_eq!(
            added.source,
            source.canonicalize().expect("canonical source")
        );
        assert_eq!(added.access, MountAccess::ReadWrite);

        let listed = configured_runtime_mounts(&config, "codex").expect("list");
        assert_eq!(listed, std::slice::from_ref(&added));

        let removed = remove_runtime_mount(&mut config, "codex", "docs").expect("remove");
        assert_eq!(removed, added);
        assert!(configured_runtime_mounts(&config, "codex")
            .expect("list")
            .is_empty());
    }

    #[test]
    fn add_rejects_duplicate_targets_and_private_sources() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let source = temp_dir.path().join("docs");
        std::fs::create_dir(&source).expect("source");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw/instances/main"));
        let private = home.root().join("config");
        std::fs::create_dir_all(&private).expect("private");

        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            profile("podman".to_string(), Some(source.clone())),
        );

        let duplicate = add_runtime_mount(
            &mut config,
            RuntimeMountContext {
                home: &home,
                project_root: None,
                work_root: None,
            },
            "codex",
            "/mnt/docs",
            &source,
            MountAccess::ReadOnly,
        )
        .expect_err("duplicate");
        assert!(duplicate.to_string().contains("already has mount target"));

        let private_err = add_runtime_mount(
            &mut config,
            RuntimeMountContext {
                home: &home,
                project_root: None,
                work_root: None,
            },
            "codex",
            "private",
            &private,
            MountAccess::ReadOnly,
        )
        .expect_err("private");
        assert!(private_err.to_string().contains("selected instance home"));
    }

    #[test]
    fn add_rejects_project_metadata_sources_inferred_from_project_instance_home() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let project = temp_dir.path().join("project");
        let source = project.join("docs");
        let metadata_source = project.join(".lionclaw/private");
        std::fs::create_dir_all(&source).expect("source");
        std::fs::create_dir_all(&metadata_source).expect("metadata source");
        let home = LionClawHome::new(project.join(".lionclaw/instances/main"));

        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), profile("podman".to_string(), None));

        let err = add_runtime_mount(
            &mut config,
            RuntimeMountContext {
                home: &home,
                project_root: None,
                work_root: None,
            },
            "codex",
            "private",
            &metadata_source,
            MountAccess::ReadOnly,
        )
        .expect_err("project metadata source");

        assert!(err.to_string().contains("project metadata"));
    }

    #[test]
    fn add_rejects_work_root_metadata_sources() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let project = temp_dir.path().join("project");
        let work_root = project.join("work");
        let metadata_source = work_root.join(".lionclaw/private");
        let home = LionClawHome::new(project.join(".lionclaw/instances/main"));
        std::fs::create_dir_all(home.root()).expect("home");
        std::fs::create_dir_all(&metadata_source).expect("work-root metadata source");

        let mut config = OperatorConfig::default();
        config.upsert_runtime("codex".to_string(), profile("podman".to_string(), None));

        let err = add_runtime_mount(
            &mut config,
            RuntimeMountContext {
                home: &home,
                project_root: Some(&project),
                work_root: Some(&work_root),
            },
            "codex",
            "private",
            &metadata_source,
            MountAccess::ReadOnly,
        )
        .expect_err("work-root metadata source");

        assert!(err.to_string().contains("project metadata"));
    }

    #[test]
    fn remove_accepts_invalid_hand_edited_targets() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let source = temp_dir.path().join("docs");
        std::fs::create_dir(&source).expect("source");

        let mut config = OperatorConfig::default();
        config.upsert_runtime(
            "codex".to_string(),
            profile("podman".to_string(), Some(source)),
        );
        let profile = runtime_profile_mut(&mut config, "codex").expect("runtime");
        let oci = runtime_oci_mut(profile);
        oci.additional_mounts[0].target = "/workspace/cache".to_string();

        let removed = remove_runtime_mount(&mut config, "codex", "/workspace/cache/")
            .expect("remove reserved target");

        assert_eq!(removed.target, "/workspace/cache");
        assert!(configured_runtime_mounts(&config, "codex")
            .expect("list")
            .is_empty());
        assert!(remove_runtime_mount(&mut config, "codex", "docs")
            .expect_err("missing")
            .to_string()
            .contains("/mnt/docs"));
    }
}
