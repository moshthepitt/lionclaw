use std::io::Write;
use std::path::{Component, Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};

use crate::authority::AuthorityCeiling;

use super::install::{copy_tree_strict, validate_closed_tree};
use super::load_mission_type;
use super::manifest::{LockedSkill, LockedSkillSource, MissionLockFile, MISSION_LOCK_FILE};
use super::skills::{load_lock, validate_skill_package, ValidatedSkillPackage};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkillSource {
    Path(PathBuf),
    Git {
        git: String,
        rev: String,
        subdir: PathBuf,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillChange {
    pub name: String,
    pub digest: String,
    pub changed: bool,
}

struct ResolvedSkill {
    _checkout: Option<tempfile::TempDir>,
    package: PathBuf,
    source: LockedSkillSource,
}

pub async fn add_skill(
    mission_root: &Path,
    source: SkillSource,
    force: bool,
    ceiling: &AuthorityCeiling,
) -> Result<SkillChange> {
    validate_closed_tree(mission_root).with_context(|| {
        format!(
            "mission type at '{}' is not a closed directory tree",
            mission_root.display()
        )
    })?;
    let resolved = resolve_skill(source).await?;
    let source_package = validate_skill_package(&resolved.package).with_context(|| {
        format!(
            "skill package at '{}' is invalid",
            resolved.package.display()
        )
    })?;
    let mut lock = load_lock(mission_root)?;
    let receipt = LockedSkill {
        digest: source_package.digest.clone(),
        source: resolved.source,
    };
    let destination = mission_root.join("skills").join(&source_package.name);

    if destination.exists() {
        let existing = validate_skill_package(&destination)?;
        if existing.digest != source_package.digest && !force {
            bail!(
                "skill '{}' already exists with different content; pass --force to replace it",
                source_package.name
            );
        }
        if existing.digest == source_package.digest
            && lock.skills.get(&source_package.name) == Some(&receipt)
        {
            load_mission_type(mission_root, ceiling).with_context(|| {
                format!("mission type at '{}' is invalid", mission_root.display())
            })?;
            return Ok(SkillChange {
                name: source_package.name,
                digest: source_package.digest,
                changed: false,
            });
        }
    }

    lock.skills.insert(source_package.name.clone(), receipt);
    edit_skill_package(
        mission_root,
        &source_package,
        Some(&resolved.package),
        &lock,
        ceiling,
    )?;
    Ok(SkillChange {
        name: source_package.name,
        digest: source_package.digest,
        changed: true,
    })
}

pub fn remove_skill(
    mission_root: &Path,
    name: &str,
    ceiling: &AuthorityCeiling,
) -> Result<SkillChange> {
    let mission_type = load_mission_type(mission_root, ceiling)
        .with_context(|| format!("mission type at '{}' is invalid", mission_root.display()))?;
    let package = mission_type
        .skills
        .get(name)
        .ok_or_else(|| anyhow!("mission type has no skill '{name}'"))?;
    let assigned = mission_type
        .roles
        .values()
        .filter(|role| role.skills.iter().any(|skill| skill == name))
        .map(|role| role.name.as_str())
        .collect::<Vec<_>>();
    if !assigned.is_empty() {
        bail!(
            "skill '{name}' is still assigned to role(s): {}",
            assigned.join(", ")
        );
    }
    let validated = validate_skill_package(&package.root)?;
    let mut lock = load_lock(mission_root)?;
    lock.skills.remove(name);
    edit_skill_package(mission_root, &validated, None, &lock, ceiling)?;
    Ok(SkillChange {
        name: name.to_string(),
        digest: validated.digest,
        changed: true,
    })
}

fn edit_skill_package(
    mission_root: &Path,
    package: &ValidatedSkillPackage,
    new_source: Option<&Path>,
    lock: &MissionLockFile,
    ceiling: &AuthorityCeiling,
) -> Result<()> {
    let mission_root = mission_root
        .canonicalize()
        .with_context(|| format!("resolving mission type '{}'", mission_root.display()))?;
    let skills_root = mission_root.join("skills");
    let had_skills_root = match std::fs::symlink_metadata(&skills_root) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            bail!(
                "'{}' must be a directory, not a symlink",
                skills_root.display()
            )
        }
        Ok(_) => true,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
        Err(err) => {
            return Err(err).with_context(|| format!("statting '{}'", skills_root.display()))
        }
    };

    let transaction = tempfile::Builder::new()
        .prefix(".lionclaw-skill@")
        .tempdir_in(mission_root.parent().unwrap_or(&mission_root))
        .context("creating skill edit transaction")?;
    let prepared_package = transaction.path().join("new-package");
    if let Some(source) = new_source {
        copy_tree_strict(source, &prepared_package)
            .with_context(|| format!("staging skill '{}'", package.name))?;
        let staged = validate_skill_package(&prepared_package)?;
        if staged.name != package.name || staged.digest != package.digest {
            bail!(
                "staged skill '{}' changed while it was copied",
                package.name
            );
        }
    }
    let prepared_lock = prepare_lock(transaction.path(), lock)?;

    let destination = skills_root.join(&package.name);
    let previous_package = transaction.path().join("previous-package");
    let lock_path = mission_root.join(MISSION_LOCK_FILE);
    let previous_lock = transaction.path().join("previous-lock");
    let had_package = destination.exists();
    let had_lock = lock_path.exists();

    let mut created_skills_root = false;
    let publish = (|| -> Result<()> {
        if !had_skills_root {
            std::fs::create_dir(&skills_root)
                .with_context(|| format!("creating '{}'", skills_root.display()))?;
            created_skills_root = true;
        }
        if had_package {
            std::fs::rename(&destination, &previous_package)
                .context("backing up existing skill package")?;
        }
        if new_source.is_some() {
            std::fs::rename(&prepared_package, &destination).context("publishing skill package")?;
        }
        if had_lock {
            std::fs::rename(&lock_path, &previous_lock).context("backing up mission lock")?;
        }
        if let Some(prepared_lock) = &prepared_lock {
            std::fs::rename(prepared_lock, &lock_path).context("publishing mission lock")?;
        }
        load_mission_type(&mission_root, ceiling)
            .context("edited mission type failed validation")?;
        Ok(())
    })();

    if let Err(err) = publish {
        let rollback = rollback_skill_edit(
            &destination,
            &previous_package,
            had_package,
            &lock_path,
            &previous_lock,
            had_lock,
            created_skills_root.then_some(skills_root.as_path()),
        );
        return match rollback {
            Ok(()) => Err(err),
            Err(rollback_err) => Err(err).context(format!(
                "rolling back the failed skill edit also failed: {rollback_err:#}"
            )),
        };
    }
    Ok(())
}

fn prepare_lock(directory: &Path, lock: &MissionLockFile) -> Result<Option<PathBuf>> {
    if lock.skills.is_empty() {
        return Ok(None);
    }
    let path = directory.join("new-lock");
    let text = toml::to_string_pretty(lock).context("serializing mission skill lock")?;
    let mut file = std::fs::File::create(&path).context("creating staged mission lock")?;
    file.write_all(text.as_bytes())
        .context("writing staged mission lock")?;
    file.sync_all().context("syncing staged mission lock")?;
    Ok(Some(path))
}

fn rollback_skill_edit(
    destination: &Path,
    previous_package: &Path,
    had_package: bool,
    lock_path: &Path,
    previous_lock: &Path,
    had_lock: bool,
    created_skills_root: Option<&Path>,
) -> Result<()> {
    if destination.exists() {
        std::fs::remove_dir_all(destination).context("removing failed skill package")?;
    }
    if had_package && previous_package.exists() {
        std::fs::rename(previous_package, destination).context("restoring skill package")?;
    }
    if lock_path.exists() {
        std::fs::remove_file(lock_path).context("removing failed mission lock")?;
    }
    if had_lock && previous_lock.exists() {
        std::fs::rename(previous_lock, lock_path).context("restoring mission lock")?;
    }
    if let Some(skills_root) = created_skills_root.filter(|root| root.exists()) {
        std::fs::remove_dir(skills_root).context("removing empty skills directory")?;
    }
    Ok(())
}

async fn resolve_skill(source: SkillSource) -> Result<ResolvedSkill> {
    match source {
        SkillSource::Path(path) => {
            let metadata = std::fs::symlink_metadata(&path)
                .with_context(|| format!("statting skill source '{}'", path.display()))?;
            if metadata.file_type().is_symlink() {
                bail!("skill source '{}' must not be a symlink", path.display());
            }
            let package = path
                .canonicalize()
                .with_context(|| format!("resolving skill source '{}'", path.display()))?;
            Ok(ResolvedSkill {
                _checkout: None,
                package: package.clone(),
                source: LockedSkillSource::Path { path: package },
            })
        }
        SkillSource::Git {
            mut git,
            rev,
            subdir,
        } => {
            if git.trim().is_empty() {
                bail!("git skill source URL is required");
            }
            if rev.trim().is_empty() {
                bail!("git skill source ref is required");
            }
            let local_git = Path::new(&git);
            if local_git.exists() {
                git = local_git
                    .canonicalize()
                    .with_context(|| format!("resolving git skill source '{git}'"))?
                    .to_string_lossy()
                    .into_owned();
            }
            let checkout = tempfile::tempdir().context("creating git skill checkout")?;
            run_git(None, ["init", "--quiet"], checkout.path()).await?;
            run_git(
                Some(checkout.path()),
                ["remote", "add", "origin", git.as_str()],
                checkout.path(),
            )
            .await?;
            run_git(
                Some(checkout.path()),
                ["fetch", "--quiet", "--depth", "1", "origin", rev.as_str()],
                checkout.path(),
            )
            .await?;
            run_git(
                Some(checkout.path()),
                ["checkout", "--quiet", "--detach", "FETCH_HEAD"],
                checkout.path(),
            )
            .await?;
            let commit = git_output(checkout.path(), ["rev-parse", "HEAD"]).await?;
            let package = resolve_git_subdir(checkout.path(), &subdir)?;
            Ok(ResolvedSkill {
                package,
                source: LockedSkillSource::Git {
                    git,
                    rev: commit,
                    subdir,
                },
                _checkout: Some(checkout),
            })
        }
    }
}

fn resolve_git_subdir(checkout: &Path, subdir: &Path) -> Result<PathBuf> {
    if subdir.is_absolute()
        || subdir
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        bail!(
            "git skill subdir '{}' must stay inside its checkout",
            subdir.display()
        );
    }
    let root = checkout
        .canonicalize()
        .context("resolving git skill checkout")?;
    let package = if subdir.as_os_str().is_empty() {
        root.clone()
    } else {
        root.join(subdir)
            .canonicalize()
            .with_context(|| format!("resolving git skill subdir '{}'", subdir.display()))?
    };
    if !package.starts_with(&root) {
        bail!(
            "git skill subdir '{}' resolves outside its checkout",
            subdir.display()
        );
    }
    Ok(package)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn write_mission(root: &Path, assigned_skill: Option<&str>) {
        std::fs::create_dir_all(root.join("roles")).unwrap();
        std::fs::write(
            root.join("mission.toml"),
            "[mission-type]\nname = \"skill-test\"\nstop = \"verified\"\nimage = \"img\"\n",
        )
        .unwrap();
        let skills = assigned_skill
            .map(|name| format!("skills: [{name}]\n"))
            .unwrap_or_default();
        std::fs::write(
            root.join("roles/worker.md"),
            format!("---\noutput: produces-artifact\n{skills}---\nWork.\n"),
        )
        .unwrap();
        std::fs::write(root.join("playbook.md"), "# Skill test\n").unwrap();
    }

    fn write_skill(root: &Path, name: &str, body: &str) {
        std::fs::create_dir_all(root).unwrap();
        std::fs::write(
            root.join("SKILL.md"),
            format!("---\nname: {name}\ndescription: Test {name}.\n---\n\n{body}\n"),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn local_add_is_idempotent_and_remove_cleans_the_last_receipt() {
        let temp = tempfile::tempdir().unwrap();
        let mission = temp.path().join("mission");
        let source = temp.path().join("source");
        write_mission(&mission, None);
        write_skill(&source, "research", "Research carefully.");

        let added = add_skill(
            &mission,
            SkillSource::Path(source.clone()),
            false,
            &AuthorityCeiling::default(),
        )
        .await
        .unwrap();
        assert!(added.changed);
        assert!(mission.join("skills/research/SKILL.md").is_file());
        let lock = load_lock(&mission).unwrap();
        assert_eq!(lock.skills["research"].digest, added.digest);
        assert_eq!(
            lock.skills["research"].source,
            LockedSkillSource::Path {
                path: source.clone()
            }
        );

        let repeated = add_skill(
            &mission,
            SkillSource::Path(source),
            false,
            &AuthorityCeiling::default(),
        )
        .await
        .unwrap();
        assert!(!repeated.changed);
        std::fs::remove_dir_all(temp.path().join("source")).unwrap();
        load_mission_type(&mission, &AuthorityCeiling::default())
            .expect("receipts never resolve at load time");

        remove_skill(&mission, "research", &AuthorityCeiling::default()).unwrap();
        assert!(!mission.join("skills/research").exists());
        assert!(!mission.join(MISSION_LOCK_FILE).exists());
        load_mission_type(&mission, &AuthorityCeiling::default()).unwrap();
    }

    #[tokio::test]
    async fn add_repairs_a_missing_assigned_skill() {
        let temp = tempfile::tempdir().unwrap();
        let mission = temp.path().join("mission");
        let source = temp.path().join("source");
        write_mission(&mission, Some("research"));
        write_skill(&source, "research", "Research carefully.");

        let added = add_skill(
            &mission,
            SkillSource::Path(source),
            false,
            &AuthorityCeiling::default(),
        )
        .await
        .expect("adding the missing package should repair the bundle");

        assert!(added.changed);
        load_mission_type(&mission, &AuthorityCeiling::default())
            .expect("the repaired bundle should be valid");
    }

    #[tokio::test]
    async fn failed_add_leaves_a_bundle_without_a_skills_directory_unchanged() {
        let temp = tempfile::tempdir().unwrap();
        let mission = temp.path().join("mission");
        let source = temp.path().join("source");
        write_mission(&mission, None);
        std::fs::write(
            mission.join("roles/worker.md"),
            "---\noutput: not-an-output\n---\nWork.\n",
        )
        .unwrap();
        write_skill(&source, "research", "Research carefully.");

        add_skill(
            &mission,
            SkillSource::Path(source),
            false,
            &AuthorityCeiling::default(),
        )
        .await
        .expect_err("an unrelated invalid role must reject the edited bundle");

        assert!(!mission.join("skills").exists());
        assert!(!mission.join(MISSION_LOCK_FILE).exists());
    }

    #[tokio::test]
    async fn replacement_requires_force_and_removal_refuses_assigned_skills() {
        let temp = tempfile::tempdir().unwrap();
        let mission = temp.path().join("mission");
        let source = temp.path().join("source");
        write_mission(&mission, None);
        write_skill(&source, "research", "Version one.");
        add_skill(
            &mission,
            SkillSource::Path(source.clone()),
            false,
            &AuthorityCeiling::default(),
        )
        .await
        .unwrap();

        write_skill(&source, "research", "Version two.");
        let err = add_skill(
            &mission,
            SkillSource::Path(source.clone()),
            false,
            &AuthorityCeiling::default(),
        )
        .await
        .expect_err("replacement must be explicit");
        assert!(err.to_string().contains("--force"));
        assert!(
            std::fs::read_to_string(mission.join("skills/research/SKILL.md"))
                .unwrap()
                .contains("Version one")
        );

        add_skill(
            &mission,
            SkillSource::Path(source),
            true,
            &AuthorityCeiling::default(),
        )
        .await
        .unwrap();
        write_mission(&mission, Some("research"));
        let err = remove_skill(&mission, "research", &AuthorityCeiling::default())
            .expect_err("assigned skill must remain");
        assert!(err.to_string().contains("still assigned"));
        assert!(mission.join("skills/research/SKILL.md").is_file());
    }

    #[tokio::test]
    async fn git_add_records_the_exact_commit_and_subdirectory() {
        let temp = tempfile::tempdir().unwrap();
        let mission = temp.path().join("mission");
        let repository = temp.path().join("repository");
        let package = repository.join("packages/research");
        write_mission(&mission, None);
        write_skill(&package, "research", "Research from Git.");
        git(&repository, &["init", "--quiet"]);
        git(&repository, &["config", "user.email", "test@example.com"]);
        git(&repository, &["config", "user.name", "Test"]);
        git(&repository, &["config", "commit.gpgsign", "false"]);
        git(&repository, &["add", "."]);
        git(&repository, &["commit", "--quiet", "-m", "skill"]);
        let commit = git_stdout(&repository, &["rev-parse", "HEAD"]);
        let subdir = PathBuf::from("packages/research");

        add_skill(
            &mission,
            SkillSource::Git {
                git: repository.to_string_lossy().into_owned(),
                rev: "HEAD".to_string(),
                subdir: subdir.clone(),
            },
            false,
            &AuthorityCeiling::default(),
        )
        .await
        .unwrap();

        let lock = load_lock(&mission).unwrap();
        let LockedSkillSource::Git {
            git: locked_git,
            rev,
            subdir: locked_subdir,
        } = &lock.skills["research"].source
        else {
            panic!("expected a Git receipt");
        };
        assert_eq!(locked_git, &repository.to_string_lossy());
        assert_eq!(rev, &commit);
        assert_eq!(locked_subdir, &subdir);
    }

    #[tokio::test]
    async fn git_add_accepts_a_skill_at_repository_root() {
        let temp = tempfile::tempdir().unwrap();
        let mission = temp.path().join("mission");
        let repository = temp.path().join("repository");
        write_mission(&mission, None);
        write_skill(&repository, "root-skill", "Use the root package.");
        git(&repository, &["init", "--quiet"]);
        git(&repository, &["config", "user.email", "test@example.com"]);
        git(&repository, &["config", "user.name", "Test"]);
        git(&repository, &["config", "commit.gpgsign", "false"]);
        git(&repository, &["add", "."]);
        git(&repository, &["commit", "--quiet", "-m", "skill"]);

        add_skill(
            &mission,
            SkillSource::Git {
                git: repository.to_string_lossy().into_owned(),
                rev: "HEAD".to_string(),
                subdir: PathBuf::new(),
            },
            false,
            &AuthorityCeiling::default(),
        )
        .await
        .unwrap();

        assert!(mission.join("skills/root-skill/SKILL.md").is_file());
        assert!(!mission.join("skills/root-skill/.git").exists());
    }

    fn git(repository: &Path, args: &[&str]) {
        let status = std::process::Command::new("git")
            .arg("-C")
            .arg(repository)
            .args(args)
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?} failed with {status}");
    }

    fn git_stdout(repository: &Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(repository)
            .args(args)
            .output()
            .unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }
}
