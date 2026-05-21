use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path, PathBuf},
};

use super::plan::MountSpec;

const RESERVED_EXTRA_MOUNT_TARGETS: &[&str] = &[
    "/workspace",
    "/runtime",
    "/drafts",
    "/attachments",
    "/lionclaw",
    "/run/secrets",
    "/proc",
    "/sys",
    "/dev",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PodmanBindMountArgumentForm {
    Volume,
    Mount,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PodmanBindMountArgument<'a> {
    pub source: &'a str,
    pub form: PodmanBindMountArgumentForm,
}

#[derive(Debug, Clone)]
pub struct MountSourceProtection {
    pub path: PathBuf,
    pub label: String,
}

impl MountSourceProtection {
    pub fn new(path: impl Into<PathBuf>, label: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            label: label.into(),
        }
    }
}

pub fn normalize_runtime_mount_target(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("mount target is required".to_string());
    }
    if !trimmed.starts_with('/') {
        return Err(format!("mount target '{trimmed}' must be an absolute path"));
    }

    let mut parts = Vec::new();
    for component in Path::new(trimmed).components() {
        match component {
            Component::RootDir => {}
            Component::Normal(part) => parts.push(part.to_string_lossy().to_string()),
            Component::CurDir | Component::ParentDir => {
                return Err(format!(
                    "mount target '{trimmed}' must not contain '.' or '..' components"
                ));
            }
            Component::Prefix(_) => {
                return Err(format!("mount target '{trimmed}' is not a Unix-style path"));
            }
        }
    }

    if parts.is_empty() {
        return Err("mount target must not be the container root '/'".to_string());
    }

    Ok(format!("/{}", parts.join("/")))
}

pub fn validate_configured_mount_target(raw: &str) -> Result<String, String> {
    let target = normalize_runtime_mount_target(raw)?;
    if RESERVED_EXTRA_MOUNT_TARGETS
        .iter()
        .any(|reserved| mount_target_is_or_under(&target, reserved))
    {
        return Err(format!(
            "mount target '{target}' conflicts with reserved runtime path"
        ));
    }
    Ok(target)
}

pub fn validate_configured_mounts(
    mounts: &[MountSpec],
    protected_roots: &[MountSourceProtection],
) -> Result<(), String> {
    let mut seen_targets = BTreeSet::new();
    for mount in mounts {
        let target = validate_configured_mount_target(&mount.target)
            .map_err(|err| format!("runtime mount target '{}' is invalid: {err}", mount.target))?;
        if !seen_targets.insert(target.clone()) {
            return Err(format!("duplicate runtime mount target '{target}'"));
        }
        validate_configured_mount_source(&mount.source, protected_roots)
            .map_err(|err| format!("runtime mount target '{target}' has invalid source: {err}"))?;
        podman_bind_mount_argument(&mount.source, &target).map_err(|err| {
            format!("runtime mount target '{target}' cannot be represented as a Podman bind mount: {err}")
        })?;
    }
    Ok(())
}

pub(crate) fn podman_bind_mount_argument<'a>(
    source: &'a Path,
    target: &str,
) -> Result<PodmanBindMountArgument<'a>, String> {
    let source_text = source
        .to_str()
        .ok_or_else(|| format!("mount source '{}' is not valid UTF-8", source.display()))?;
    let mount_form_required = source_text.contains(':') || target.contains(':');
    if mount_form_required {
        if source_text.contains(',') {
            return Err(format!(
                "mount source '{}' contains ',' but Podman --mount is required when the source or target contains ':'",
                source.display()
            ));
        }
        if target.contains(',') {
            return Err(format!(
                "mount target '{target}' contains ',' but Podman --mount is required when the source or target contains ':'"
            ));
        }
        return Ok(PodmanBindMountArgument {
            source: source_text,
            form: PodmanBindMountArgumentForm::Mount,
        });
    }

    Ok(PodmanBindMountArgument {
        source: source_text,
        form: PodmanBindMountArgumentForm::Volume,
    })
}

pub fn canonical_mount_source(source: &Path) -> Result<PathBuf, String> {
    if source.as_os_str().is_empty() {
        return Err("mount source is required".to_string());
    }
    if !source.is_absolute() {
        return Err(format!(
            "mount source '{}' must be an absolute path",
            source.display()
        ));
    }
    let canonical = fs::canonicalize(source).map_err(|err| {
        format!(
            "failed to resolve mount source '{}': {err}",
            source.display()
        )
    })?;
    let metadata = fs::metadata(&canonical).map_err(|err| {
        format!(
            "failed to stat mount source '{}': {err}",
            canonical.display()
        )
    })?;
    if !metadata.is_dir() {
        return Err(format!(
            "mount source '{}' is not a directory",
            canonical.display()
        ));
    }
    Ok(canonical)
}

fn validate_configured_mount_source(
    source: &Path,
    protected_roots: &[MountSourceProtection],
) -> Result<(), String> {
    let canonical = canonical_mount_source(source)?;
    for protected in canonical_protected_roots(protected_roots)? {
        if path_is_or_under(source, &protected.path)
            || path_is_or_under(&canonical, &protected.path)
        {
            return Err(format!(
                "mount source '{}' is inside {}; choose a directory outside {}",
                source.display(),
                protected.path.display(),
                protected.label
            ));
        }
    }
    Ok(())
}

fn canonical_protected_roots(
    roots: &[MountSourceProtection],
) -> Result<Vec<MountSourceProtection>, String> {
    let mut canonical = Vec::new();
    for root in roots {
        if root.path.as_os_str().is_empty() {
            continue;
        }
        match fs::canonicalize(&root.path) {
            Ok(path) => canonical.push(MountSourceProtection::new(path, root.label.clone())),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(format!(
                    "failed to resolve protected mount root '{}': {err}",
                    root.path.display()
                ));
            }
        }
    }
    Ok(canonical)
}

pub fn mount_target_is_or_under(target: &str, root: &str) -> bool {
    target == root
        || target
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

pub fn project_metadata_root_from_instance_home(home_root: &Path) -> Option<PathBuf> {
    let instances_root = home_root.parent()?;
    if instances_root.file_name()? != "instances" {
        return None;
    }
    let metadata_root = instances_root.parent()?;
    if metadata_root.file_name()? != ".lionclaw" {
        return None;
    }
    Some(metadata_root.to_path_buf())
}

fn path_is_or_under(path: &Path, root: &Path) -> bool {
    path == root || path.starts_with(root)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::runtime::{MountAccess, MountSpec};

    #[test]
    fn normalizes_absolute_mount_targets() {
        assert_eq!(
            normalize_runtime_mount_target("/mnt/docs/").expect("target"),
            "/mnt/docs"
        );
        assert_eq!(
            normalize_runtime_mount_target("//reference//docs").expect("target"),
            "/reference/docs"
        );
        assert!(normalize_runtime_mount_target("docs").is_err());
        assert!(normalize_runtime_mount_target("/mnt/../workspace").is_err());
        assert!(normalize_runtime_mount_target("/").is_err());
    }

    #[test]
    fn rejects_reserved_mount_targets() {
        for target in [
            "/workspace",
            "/runtime/state",
            "/drafts",
            "/attachments/inbound",
            "/lionclaw/skills",
            "/run/secrets/token",
            "/proc",
            "/sys/fs",
            "/dev/null",
        ] {
            let err = validate_configured_mount_target(target).expect_err("reserved target");
            assert!(err.contains("reserved runtime path"), "{target}: {err}");
        }
    }

    #[test]
    fn validates_configured_sources_and_duplicates() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let source = temp_dir.path().join("docs");
        std::fs::create_dir(&source).expect("source dir");

        let mounts = vec![MountSpec {
            source: source.clone(),
            target: "/mnt/docs".to_string(),
            access: MountAccess::ReadOnly,
        }];

        validate_configured_mounts(&mounts, &[]).expect("valid mount");

        let mut duplicate = mounts.clone();
        duplicate.push(MountSpec {
            source,
            target: "/mnt/docs/".to_string(),
            access: MountAccess::ReadOnly,
        });
        let err = validate_configured_mounts(&duplicate, &[]).expect_err("duplicate");
        assert!(err.contains("duplicate runtime mount target"));

        let missing = MountSpec {
            source: temp_dir.path().join("missing"),
            target: "/mnt/missing".to_string(),
            access: MountAccess::ReadOnly,
        };
        let err = validate_configured_mounts(&[missing], &[]).expect_err("missing source");
        assert!(err.contains("runtime mount target '/mnt/missing'"));
        assert!(err.contains("failed to resolve mount source"));

        let colon_source = temp_dir.path().join("docs:archive");
        std::fs::create_dir(&colon_source).expect("colon source dir");
        validate_configured_mounts(
            &[MountSpec {
                source: colon_source,
                target: "/mnt/colon".to_string(),
                access: MountAccess::ReadOnly,
            }],
            &[],
        )
        .expect("colon source is valid at the configured-mount layer");
    }

    #[test]
    fn rejects_comma_when_podman_mount_form_is_required() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let comma_source = temp_dir.path().join("docs,current");
        let colon_comma_source = temp_dir.path().join("docs:archive,current");
        let colon_source = temp_dir.path().join("docs:archive");
        for source in [&comma_source, &colon_comma_source, &colon_source] {
            std::fs::create_dir(source).expect("source dir");
        }

        validate_configured_mounts(
            &[MountSpec {
                source: comma_source.clone(),
                target: "/mnt/docs".to_string(),
                access: MountAccess::ReadOnly,
            }],
            &[],
        )
        .expect("comma source is valid when --volume can represent it");

        let err = validate_configured_mounts(
            &[MountSpec {
                source: colon_comma_source,
                target: "/mnt/docs".to_string(),
                access: MountAccess::ReadOnly,
            }],
            &[],
        )
        .expect_err("source comma with required --mount");
        assert!(err.contains("runtime mount target '/mnt/docs'"));
        assert!(err.contains("Podman --mount"));
        assert!(err.contains("contains ','"));

        let err = validate_configured_mounts(
            &[MountSpec {
                source: comma_source,
                target: "/mnt/docs:archive".to_string(),
                access: MountAccess::ReadOnly,
            }],
            &[],
        )
        .expect_err("source comma with target colon");
        assert!(err.contains("runtime mount target '/mnt/docs:archive'"));
        assert!(err.contains("Podman --mount"));
        assert!(err.contains("contains ','"));

        let err = validate_configured_mounts(
            &[MountSpec {
                source: colon_source,
                target: "/mnt/docs,current".to_string(),
                access: MountAccess::ReadOnly,
            }],
            &[],
        )
        .expect_err("target comma with source colon");
        assert!(err.contains("runtime mount target '/mnt/docs,current'"));
        assert!(err.contains("Podman --mount"));
        assert!(err.contains("contains ','"));
    }

    #[test]
    fn rejects_sources_inside_protected_roots_by_raw_or_canonical_path() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = temp_dir.path().join("home");
        let hidden = home.join("config");
        std::fs::create_dir_all(&hidden).expect("hidden");
        let outside = temp_dir.path().join("outside");
        std::fs::create_dir(&outside).expect("outside");
        let symlink = home.join("outside-link");

        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside, &symlink).expect("symlink");

        let protections = vec![MountSourceProtection::new(
            home.clone(),
            "selected instance home",
        )];
        let err = validate_configured_mounts(
            &[MountSpec {
                source: hidden,
                target: "/mnt/hidden".to_string(),
                access: MountAccess::ReadOnly,
            }],
            &protections,
        )
        .expect_err("protected source");
        assert!(err.contains("selected instance home"));

        #[cfg(unix)]
        {
            let err = validate_configured_mounts(
                &[MountSpec {
                    source: symlink,
                    target: "/mnt/link".to_string(),
                    access: MountAccess::ReadOnly,
                }],
                &protections,
            )
            .expect_err("raw protected symlink");
            assert!(err.contains("selected instance home"));
        }
    }

    #[test]
    fn derives_project_metadata_root_from_standard_instance_home() {
        let root = Path::new("/repo/.lionclaw/instances/main");
        assert_eq!(
            project_metadata_root_from_instance_home(root).as_deref(),
            Some(Path::new("/repo/.lionclaw"))
        );
        assert!(
            project_metadata_root_from_instance_home(Path::new("/tmp/lionclaw-home")).is_none()
        );
    }
}
