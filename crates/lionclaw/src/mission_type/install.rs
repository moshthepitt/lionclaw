use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};

use crate::authority::AuthorityCeiling;

use super::manifest::{
    is_path_safe_name, GitSkillSource, LockedSkill, LockedSkillSource, ManifestFile,
    ManifestSkillSource, MissionLockFile, MISSION_LOCK_FILE,
};
use super::skills::resolve_package_path;
use super::{load_mission_type, MissionType};

enum ResolvedSkillSource {
    Path(PathBuf),
    Git(GitCheckout),
}

impl ResolvedSkillSource {
    fn path(&self) -> &Path {
        match self {
            Self::Path(path) => path,
            Self::Git(checkout) => &checkout.package,
        }
    }
}

struct GitCheckout {
    _directory: tempfile::TempDir,
    package: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallOutcome {
    pub name: String,
    pub installed: bool,
}

/// Materialize one source mission type into a self-contained destination and
/// validate the resulting closure. The destination must not already exist.
pub async fn materialize_mission_type(
    source: &Path,
    destination: &Path,
    ceiling: &AuthorityCeiling,
) -> Result<MissionType> {
    if destination.exists() {
        bail!("destination '{}' already exists", destination.display());
    }
    let result = materialize_inner(source, destination, ceiling).await;
    if result.is_err() {
        let _ = std::fs::remove_dir_all(destination);
    }
    result
}

/// Atomically install one mission type into the global mission-type directory.
pub async fn install_mission_type(
    source: &Path,
    destination_dir: &Path,
    force: bool,
    ceiling: &AuthorityCeiling,
) -> Result<InstallOutcome> {
    std::fs::create_dir_all(destination_dir)
        .with_context(|| format!("creating '{}'", destination_dir.display()))?;
    let declared_name = read_manifest(source)?.mission_type.name;
    if !is_path_safe_name(&declared_name) {
        bail!("mission type name '{declared_name}' is not path-safe");
    }
    let destination = destination_dir.join(&declared_name);
    if destination.exists() && !force {
        return Ok(InstallOutcome {
            name: declared_name,
            installed: false,
        });
    }
    let staging_parent = tempfile::Builder::new()
        .prefix(".mission-type.tmp-")
        .tempdir_in(destination_dir)
        .context("creating mission-type staging directory")?;
    let staging = staging_parent.path().join("bundle");
    let mission_type = materialize_mission_type(source, &staging, ceiling).await?;
    let name = mission_type.name.clone();
    debug_assert_eq!(name, declared_name);

    let had_destination = destination.exists();
    let mut backup_parent = had_destination
        .then(|| {
            tempfile::Builder::new()
                .prefix(&format!(".{name}.backup-"))
                .tempdir_in(destination_dir)
        })
        .transpose()
        .context("creating mission-type backup directory")?;
    let backup = backup_parent
        .as_ref()
        .map(|parent| parent.path().join("previous"));
    if had_destination {
        let backup = backup.as_ref().context("missing replacement backup")?;
        std::fs::rename(&destination, backup).with_context(|| {
            format!(
                "moving existing '{}' to temporary backup",
                destination.display()
            )
        })?;
    }
    if let Err(err) = std::fs::rename(&staging, &destination) {
        if let Some(backup) = &backup {
            if let Err(restore_err) = std::fs::rename(backup, &destination) {
                let preserved = backup_parent
                    .take()
                    .context("missing replacement backup")?
                    .keep()
                    .join("previous");
                return Err(err).with_context(|| {
                    format!(
                        "moving prepared mission type into '{}'; restoring the previous bundle also failed ({restore_err}); it remains at '{}'",
                        destination.display(),
                        preserved.display()
                    )
                });
            }
        }
        return Err(err).with_context(|| {
            format!(
                "moving prepared mission type into '{}'",
                destination.display()
            )
        });
    }
    drop(backup_parent);
    Ok(InstallOutcome {
        name,
        installed: true,
    })
}

async fn materialize_inner(
    source: &Path,
    destination: &Path,
    ceiling: &AuthorityCeiling,
) -> Result<MissionType> {
    let manifest = read_manifest(source)?;
    copy_tree_strict(source, destination)?;
    let mut locked = BTreeMap::new();
    for (name, declaration) in &manifest.skills {
        lionclaw_confinement::validate_skill_alias(name)?;
        let package_source = match &declaration.source {
            ManifestSkillSource::Path(source_path) => ResolvedSkillSource::Path(
                resolve_package_path(source, &source_path.path).map_err(anyhow::Error::msg)?,
            ),
            ManifestSkillSource::Git(source) => {
                ResolvedSkillSource::Git(checkout_git_skill(source).await?)
            }
        };
        let package_relative = PathBuf::from("skills").join(name);
        let package_destination = destination.join(&package_relative);
        let package_staging = destination.join("skills").join(format!(".{name}.tmp"));
        if package_staging.exists() {
            std::fs::remove_dir_all(&package_staging)?;
        }
        copy_tree_strict(package_source.path(), &package_staging)
            .with_context(|| format!("materializing skill '{name}'"))?;
        if package_destination.exists() {
            std::fs::remove_dir_all(&package_destination).with_context(|| {
                format!(
                    "replacing bundled skill '{}'",
                    package_destination.display()
                )
            })?;
        }
        std::fs::rename(&package_staging, &package_destination).with_context(|| {
            format!(
                "moving materialized skill into '{}'",
                package_destination.display()
            )
        })?;
        let source = match &declaration.source {
            ManifestSkillSource::Path(source) => LockedSkillSource::Path {
                path: source.path.clone(),
            },
            ManifestSkillSource::Git(source) => LockedSkillSource::Git {
                git: source.git.clone(),
                rev: source.rev.clone(),
                subdir: source.subdir.clone(),
            },
        };
        locked.insert(
            name.clone(),
            LockedSkill {
                path: package_relative,
                source,
            },
        );
    }
    let lock = MissionLockFile {
        version: 1,
        skills: locked,
    };
    let lock_text = toml::to_string_pretty(&lock).context("serializing mission skill lock")?;
    std::fs::write(destination.join(MISSION_LOCK_FILE), lock_text)
        .context("writing mission skill lock")?;
    load_mission_type(destination, ceiling).map_err(Into::into)
}

fn read_manifest(root: &Path) -> Result<ManifestFile> {
    let path = root.join("mission.toml");
    let text =
        std::fs::read_to_string(&path).with_context(|| format!("reading '{}'", path.display()))?;
    toml::from_str(&text).with_context(|| format!("invalid '{}'", path.display()))
}

async fn checkout_git_skill(source: &GitSkillSource) -> Result<GitCheckout> {
    validate_git_revision(&source.rev)?;
    if source.git.trim().is_empty() {
        bail!("git skill source URL is required");
    }
    let checkout = tempfile::tempdir().context("creating git skill checkout")?;
    run_git(None, ["init", "--quiet"], checkout.path()).await?;
    run_git(
        Some(checkout.path()),
        ["remote", "add", "origin", source.git.as_str()],
        checkout.path(),
    )
    .await?;
    run_git(
        Some(checkout.path()),
        [
            "fetch",
            "--quiet",
            "--depth",
            "1",
            "origin",
            source.rev.as_str(),
        ],
        checkout.path(),
    )
    .await?;
    run_git(
        Some(checkout.path()),
        ["checkout", "--quiet", "--detach", "FETCH_HEAD"],
        checkout.path(),
    )
    .await?;
    let actual = git_output(checkout.path(), ["rev-parse", "HEAD"]).await?;
    if !actual.eq_ignore_ascii_case(&source.rev) {
        bail!(
            "git skill source resolved '{}' but expected pinned revision '{}'",
            actual,
            source.rev
        );
    }
    let package = if source.subdir.as_os_str().is_empty() {
        checkout.path().to_path_buf()
    } else {
        resolve_package_path(checkout.path(), &source.subdir).map_err(anyhow::Error::msg)?
    };
    Ok(GitCheckout {
        _directory: checkout,
        package,
    })
}

fn validate_git_revision(revision: &str) -> Result<()> {
    if !matches!(revision.len(), 40 | 64)
        || !revision
            .chars()
            .all(|character| character.is_ascii_hexdigit())
    {
        bail!("git skill revision must be a full 40- or 64-character commit id");
    }
    Ok(())
}

async fn run_git<const N: usize>(
    repository: Option<&Path>,
    args: [&str; N],
    directory: &Path,
) -> Result<()> {
    let mut command = tokio::process::Command::new("git");
    if let Some(repository) = repository {
        command.arg("-C").arg(repository);
    }
    let output = command
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .current_dir(directory)
        .output()
        .await
        .context("running git for skill source")?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    Err(anyhow!(
        "git skill source command failed with {}{}",
        output.status,
        if stderr.is_empty() {
            String::new()
        } else {
            format!(": {stderr}")
        }
    ))
}

async fn git_output<const N: usize>(repository: &Path, args: [&str; N]) -> Result<String> {
    let output = tokio::process::Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .await
        .context("running git for skill source")?;
    if !output.status.success() {
        bail!("git skill source inspection failed with {}", output.status);
    }
    Ok(String::from_utf8(output.stdout)
        .context("git skill source output was not UTF-8")?
        .trim()
        .to_string())
}

fn copy_tree_strict(source: &Path, destination: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(source)
        .with_context(|| format!("statting '{}'", source.display()))?;
    if metadata.file_type().is_symlink() {
        bail!("source '{}' must not be a symlink", source.display());
    }
    if !metadata.is_dir() {
        bail!("source '{}' must be a directory", source.display());
    }
    std::fs::create_dir_all(destination)
        .with_context(|| format!("creating '{}'", destination.display()))?;
    let mut entries = std::fs::read_dir(source)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let from = entry.path();
        let to = destination.join(entry.file_name());
        let file_type = entry.file_type()?;
        if entry.file_name() == ".git" {
            continue;
        }
        if file_type.is_symlink() {
            bail!("source entry '{}' must not be a symlink", from.display());
        }
        if file_type.is_dir() {
            copy_tree_strict(&from, &to)?;
        } else if file_type.is_file() {
            std::fs::copy(&from, &to).with_context(|| format!("copying '{}'", from.display()))?;
            std::fs::set_permissions(&to, entry.metadata()?.permissions())?;
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
    use std::process::Command;

    use super::*;

    fn write_type(root: &Path, skill_source: &str) {
        std::fs::create_dir_all(root.join("roles")).unwrap();
        std::fs::write(
            root.join("mission.toml"),
            format!(
                "[mission-type]\nname = \"install-test\"\nstop = \"reviewed\"\nimage = \"img\"\n\
                 \n[skills.test-skill]\nsource = {skill_source}\n"
            ),
        )
        .unwrap();
        std::fs::write(
            root.join("roles/worker.md"),
            "---\noutput: produces-artifact\nskills: [test-skill]\n---\nDo it.\n",
        )
        .unwrap();
    }

    fn write_skill(root: &Path) {
        std::fs::create_dir_all(root).unwrap();
        std::fs::write(
            root.join("SKILL.md"),
            "---\nname: test-skill\ndescription: Test.\n---\n\n# Test skill\n\nFollow the test procedure.\n",
        )
        .unwrap();
    }

    #[tokio::test]
    async fn materializes_bundled_skills_and_emits_a_loadable_lock() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        write_type(&source, "{ path = \"bundled/test-skill\" }");
        write_skill(&source.join("bundled/test-skill"));
        let destination = temp.path().join("installed");

        let mission_type =
            materialize_mission_type(&source, &destination, &AuthorityCeiling::default())
                .await
                .unwrap();

        assert!(destination.join(MISSION_LOCK_FILE).is_file());
        assert_eq!(
            mission_type.skills["test-skill"].root,
            destination.join("skills/test-skill")
        );
    }

    #[tokio::test]
    async fn materializes_a_pinned_git_skill_without_runtime_resolution() {
        let temp = tempfile::tempdir().unwrap();
        let repository = temp.path().join("skill-repo");
        write_skill(&repository.join("package"));
        Command::new("git")
            .arg("init")
            .arg("-q")
            .arg(&repository)
            .status()
            .unwrap();
        Command::new("git")
            .args(["-C", repository.to_str().unwrap(), "add", "."])
            .status()
            .unwrap();
        Command::new("git")
            .args([
                "-C",
                repository.to_str().unwrap(),
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.com",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-qm",
                "skill",
            ])
            .status()
            .unwrap();
        let revision = git_output(&repository, ["rev-parse", "HEAD"])
            .await
            .unwrap();
        let source = temp.path().join("source");
        write_type(
            &source,
            &format!(
                "{{ git = {:?}, rev = {:?}, subdir = \"package\" }}",
                repository.to_string_lossy(),
                revision
            ),
        );
        let destination = temp.path().join("installed");

        let mission_type =
            materialize_mission_type(&source, &destination, &AuthorityCeiling::default())
                .await
                .unwrap();

        assert!(mission_type.skills["test-skill"]
            .root
            .join("SKILL.md")
            .is_file());
        std::fs::remove_dir_all(&repository).unwrap();
        load_mission_type(&destination, &AuthorityCeiling::default())
            .expect("installed mission no longer needs source repository");
    }

    #[tokio::test]
    async fn failed_forced_install_preserves_the_previous_valid_bundle() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        write_type(&source, "{ path = \"bundled/test-skill\" }");
        write_skill(&source.join("bundled/test-skill"));
        let installed = temp.path().join("mission-types");
        let first = install_mission_type(&source, &installed, false, &AuthorityCeiling::default())
            .await
            .unwrap();
        assert!(first.installed);
        let original =
            std::fs::read(installed.join("install-test/skills/test-skill/SKILL.md")).unwrap();

        std::fs::write(
            source.join("bundled/test-skill/SKILL.md"),
            "not valid skill frontmatter\n",
        )
        .unwrap();
        let skipped =
            install_mission_type(&source, &installed, false, &AuthorityCeiling::default())
                .await
                .expect("an existing install should not re-resolve its source");
        assert!(!skipped.installed);
        install_mission_type(&source, &installed, true, &AuthorityCeiling::default())
            .await
            .expect_err("invalid replacement must fail");

        assert_eq!(
            std::fs::read(installed.join("install-test/skills/test-skill/SKILL.md")).unwrap(),
            original
        );
        load_mission_type(
            &installed.join("install-test"),
            &AuthorityCeiling::default(),
        )
        .expect("previous bundle remains loadable");
    }

    #[tokio::test]
    async fn installed_lock_requires_canonical_package_paths() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        write_type(&source, "{ path = \"bundled/test-skill\" }");
        write_skill(&source.join("bundled/test-skill"));
        let destination = temp.path().join("installed");
        materialize_mission_type(&source, &destination, &AuthorityCeiling::default())
            .await
            .unwrap();
        let lock_path = destination.join(MISSION_LOCK_FILE);
        let lock = std::fs::read_to_string(&lock_path).unwrap().replace(
            "path = \"skills/test-skill\"",
            "path = \"bundled/test-skill\"",
        );
        std::fs::write(lock_path, lock).unwrap();

        let err = load_mission_type(&destination, &AuthorityCeiling::default())
            .expect_err("noncanonical locked path");
        assert!(err.to_string().contains("must be 'skills/test-skill'"));
    }

    #[tokio::test]
    async fn installed_lock_source_must_match_the_manifest() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        write_type(&source, "{ path = \"bundled/test-skill\" }");
        write_skill(&source.join("bundled/test-skill"));
        let destination = temp.path().join("installed");
        materialize_mission_type(&source, &destination, &AuthorityCeiling::default())
            .await
            .unwrap();
        let manifest_path = destination.join("mission.toml");
        let manifest = std::fs::read_to_string(&manifest_path)
            .unwrap()
            .replace("bundled/test-skill", "other/test-skill");
        std::fs::write(manifest_path, manifest).unwrap();

        let err = load_mission_type(&destination, &AuthorityCeiling::default())
            .expect_err("stale lock source");
        assert!(err.to_string().contains("does not match mission.toml"));
    }
}
