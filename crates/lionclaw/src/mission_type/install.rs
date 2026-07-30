use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::authority::AuthorityCeiling;

use super::bounded_tree::BoundedTree;
use super::{load_materialized_mission_type, MissionType};

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
    match load_materialized_mission_type(destination, ceiling) {
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
    std::fs::create_dir_all(destination_dir)
        .with_context(|| format!("creating '{}'", destination_dir.display()))?;
    let staging_parent = tempfile::Builder::new()
        .prefix(&format!("{INSTALL_WORK_DIR_PREFIX}staging-"))
        .tempdir_in(destination_dir)
        .context("creating mission-type staging directory")?;
    let staging = staging_parent.path().join("bundle");
    let prepared = materialize_mission_type(source, &staging, ceiling)?;
    let destination = destination_dir.join(&prepared.name);
    if destination.exists() && !force {
        return Ok(InstallOutcome {
            name: prepared.name.clone(),
            installed: false,
        });
    }
    replace_directory(&staging, &destination)?;
    Ok(InstallOutcome {
        name: prepared.name.clone(),
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
    BoundedTree::open(source)?.copy_to(destination)
}

pub(crate) fn validate_closed_tree(source: &Path) -> Result<()> {
    BoundedTree::open(source).map(|_| ())
}

#[cfg(test)]
mod tests {
    use crate::mission_type::load_mission_type;

    use super::*;

    #[test]
    fn installed_bundle_no_longer_depends_on_its_source() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        std::fs::create_dir_all(source.join("roles")).unwrap();
        std::fs::write(
            source.join("mission.toml"),
            "[mission-type]\nname = \"install-test\"\nstop = \"verified\"\nimage = \"img\"\n\
             \n[team]\nplanning-assignment = \"planner\"\nrequires-gap-review = false\n\
             \n[ceilings]\ninstall = true\nwrites = true\n",
        )
        .unwrap();
        std::fs::write(
            source.join("roles/worker.md"),
            "---\noutput: produces-artifact\nruntime: codex\n---\nDo it.\n",
        )
        .unwrap();
        std::fs::write(
            source.join("roles/planner.md"),
            "---\noutput: proposes-plan\nruntime: codex\n---\nPlan it.\n",
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
