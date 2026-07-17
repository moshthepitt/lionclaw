use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::authority::AuthorityCeiling;

use super::{load_mission_type, MissionType};

pub(crate) const INSTALL_WORK_DIR_PREFIX: &str = ".lionclaw-install@";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallOutcome {
    pub name: String,
    pub installed: bool,
}

/// Publish one complete mission bundle into a new destination.
pub fn materialize_mission_type(
    source: &Path,
    destination: &Path,
    ceiling: &AuthorityCeiling,
) -> Result<MissionType> {
    if destination.exists() {
        bail!("destination '{}' already exists", destination.display());
    }
    if let Err(err) = copy_tree_strict(source, destination) {
        let _ = std::fs::remove_dir_all(destination);
        return Err(err);
    }
    match load_mission_type(destination, ceiling) {
        Ok(mission_type) => Ok(mission_type),
        Err(err) => {
            let _ = std::fs::remove_dir_all(destination);
            Err(err).context("published mission type failed validation")
        }
    }
}

/// Atomically copy one complete mission bundle into the named home catalog.
pub fn install_mission_type(
    source: &Path,
    destination_dir: &Path,
    force: bool,
    ceiling: &AuthorityCeiling,
) -> Result<InstallOutcome> {
    let source_type = load_mission_type(source, ceiling)
        .with_context(|| format!("mission type at '{}' is invalid", source.display()))?;
    std::fs::create_dir_all(destination_dir)
        .with_context(|| format!("creating '{}'", destination_dir.display()))?;
    let destination = destination_dir.join(&source_type.name);
    if destination.exists() && !force {
        return Ok(InstallOutcome {
            name: source_type.name.clone(),
            installed: false,
        });
    }

    let staging_parent = tempfile::Builder::new()
        .prefix(&format!("{INSTALL_WORK_DIR_PREFIX}staging-"))
        .tempdir_in(destination_dir)
        .context("creating mission-type staging directory")?;
    let staging = staging_parent.path().join("bundle");
    let prepared = materialize_mission_type(source, &staging, ceiling)?;
    if prepared.name != source_type.name {
        bail!("mission type name changed while it was installed");
    }
    replace_directory(&staging, &destination)?;
    Ok(InstallOutcome {
        name: source_type.name.clone(),
        installed: true,
    })
}

pub(crate) fn replace_directory(prepared: &Path, destination: &Path) -> Result<()> {
    let parent = destination
        .parent()
        .context("destination has no parent directory")?;
    let mut backup_parent = destination
        .exists()
        .then(|| {
            tempfile::Builder::new()
                .prefix(&format!("{INSTALL_WORK_DIR_PREFIX}backup-"))
                .tempdir_in(parent)
        })
        .transpose()
        .context("creating replacement backup directory")?;
    let backup = backup_parent
        .as_ref()
        .map(|directory| directory.path().join("previous"));
    if let Some(backup) = &backup {
        std::fs::rename(destination, backup)
            .with_context(|| format!("moving existing '{}' to a backup", destination.display()))?;
    }

    if let Err(err) = std::fs::rename(prepared, destination) {
        if let Some(backup) = &backup {
            if let Err(restore_err) = std::fs::rename(backup, destination) {
                let preserved = backup_parent
                    .take()
                    .context("missing replacement backup")?
                    .keep()
                    .join("previous");
                return Err(err).with_context(|| {
                    format!(
                        "publishing '{}'; restoring the previous directory also failed ({restore_err}); it remains at '{}'",
                        destination.display(),
                        preserved.display()
                    )
                });
            }
        }
        return Err(err).with_context(|| format!("publishing '{}'", destination.display()));
    }
    Ok(())
}

pub(crate) fn copy_tree_strict(source: &Path, destination: &Path) -> Result<()> {
    walk_tree_strict(source, Some(destination))
}

pub(crate) fn validate_closed_tree(source: &Path) -> Result<()> {
    walk_tree_strict(source, None)
}

fn walk_tree_strict(source: &Path, destination: Option<&Path>) -> Result<()> {
    let metadata = std::fs::symlink_metadata(source)
        .with_context(|| format!("statting '{}'", source.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!(
            "source '{}' must be a directory, not a symlink",
            source.display()
        );
    }
    if let Some(destination) = destination {
        std::fs::create_dir_all(destination)
            .with_context(|| format!("creating '{}'", destination.display()))?;
    }
    let mut entries = std::fs::read_dir(source)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        if entry.file_name() == ".git" {
            continue;
        }
        let from = entry.path();
        let to = destination.map(|destination| destination.join(entry.file_name()));
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            bail!("source entry '{}' must not be a symlink", from.display());
        }
        if file_type.is_dir() {
            walk_tree_strict(&from, to.as_deref())?;
        } else if file_type.is_file() {
            if let Some(to) = &to {
                std::fs::copy(&from, to)
                    .with_context(|| format!("copying '{}'", from.display()))?;
                std::fs::set_permissions(to, entry.metadata()?.permissions())?;
            }
        } else {
            bail!(
                "source entry '{}' must be a regular file or directory",
                from.display()
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn installed_bundle_no_longer_depends_on_its_source() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        std::fs::create_dir_all(source.join("roles")).unwrap();
        std::fs::write(
            source.join("mission.toml"),
            "[mission-type]\nname = \"install-test\"\nstop = \"verified\"\nimage = \"img\"\n",
        )
        .unwrap();
        std::fs::write(
            source.join("roles/worker.md"),
            "---\noutput: produces-artifact\n---\nDo it.\n",
        )
        .unwrap();
        std::fs::write(source.join("playbook.md"), "# Install test\n").unwrap();
        let installed = temp.path().join("mission-types");
        install_mission_type(&source, &installed, false, &AuthorityCeiling::default()).unwrap();

        std::fs::remove_dir_all(&source).unwrap();
        load_mission_type(
            &installed.join("install-test"),
            &AuthorityCeiling::default(),
        )
        .expect("installed bundle remains loadable");
    }
}
