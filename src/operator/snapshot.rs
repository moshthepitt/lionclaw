use std::ffi::OsStr;
use std::fs;
use std::io::ErrorKind;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    home::LionClawHome,
    kernel::db::now_ms,
    kernel::skills::{derive_skill_id, parse_skill_frontmatter},
    operator::config::normalize_local_source,
};

pub const SKILL_INSTALL_METADATA_FILE: &str = ".lionclaw-skill.toml";

#[derive(Debug, Clone)]
pub struct InstalledSnapshot {
    pub alias: String,
    pub source_uri: String,
    pub reference: String,
    pub skill_id: String,
    pub hash: String,
    pub snapshot_rel_dir: String,
    pub snapshot_abs_dir: PathBuf,
    pub skill_md: String,
}

pub fn install_snapshot(
    home: &LionClawHome,
    alias: &str,
    source: &str,
    reference: &str,
) -> Result<InstalledSnapshot> {
    let source_uri = normalize_local_source(source)?;
    let source_path = resolve_local_source(&source_uri)?;
    let skill_md = read_source_skill_md(&source_path)?;
    let (name, _) = parse_skill_frontmatter(&skill_md);
    let hash = hash_directory(&source_path)?;
    let skill_id = derive_skill_id(&name, &hash);
    let snapshot_abs_dir = home.skills_dir().join(alias);
    let staging_dir = home
        .skills_dir()
        .join(format!(".{alias}.tmp-{}", Uuid::new_v4()));

    if staging_dir.exists() {
        fs::remove_dir_all(&staging_dir)
            .with_context(|| format!("failed to clean {}", staging_dir.display()))?;
    }
    copy_directory(&source_path, &staging_dir)?;
    write_install_metadata(&staging_dir, &source_uri, reference)?;

    match fs::symlink_metadata(&snapshot_abs_dir) {
        Ok(metadata) => {
            validate_existing_snapshot_dir(&snapshot_abs_dir, &metadata)?;
            fs::remove_dir_all(&snapshot_abs_dir)
                .with_context(|| format!("failed to replace {}", snapshot_abs_dir.display()))?;
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to stat {}", snapshot_abs_dir.display()));
        }
    }
    fs::rename(&staging_dir, &snapshot_abs_dir).with_context(|| {
        format!(
            "failed to move '{}' into '{}'",
            staging_dir.display(),
            snapshot_abs_dir.display()
        )
    })?;

    Ok(InstalledSnapshot {
        alias: alias.to_string(),
        source_uri,
        reference: reference.to_string(),
        skill_id,
        hash,
        snapshot_rel_dir: format!("skills/{alias}"),
        snapshot_abs_dir,
        skill_md,
    })
}

pub fn resolve_local_source(source_uri: &str) -> Result<PathBuf> {
    let raw = source_uri
        .strip_prefix("local:")
        .ok_or_else(|| anyhow!("unsupported skill source '{source_uri}'"))?;
    let path = PathBuf::from(raw);
    let metadata = fs::symlink_metadata(&path)
        .with_context(|| format!("failed to stat skill source '{}'", path.display()))?;
    if metadata.file_type().is_symlink() {
        return Err(anyhow!(
            "skill source '{}' must not be a symlink",
            path.display()
        ));
    }
    if !metadata.is_dir() {
        return Err(anyhow!(
            "skill source '{}' is not a directory",
            path.display()
        ));
    }
    Ok(path)
}

fn read_source_skill_md(source_path: &Path) -> Result<String> {
    let skill_md_path = source_path.join("SKILL.md");
    let metadata = fs::symlink_metadata(&skill_md_path)
        .with_context(|| format!("failed to stat {}", skill_md_path.display()))?;
    if metadata.file_type().is_symlink() {
        return Err(anyhow!(
            "skill source entry '{}' must not be a symlink",
            skill_md_path.display()
        ));
    }
    if !metadata.is_file() {
        return Err(anyhow!(
            "skill source entry '{}' is not a regular file",
            skill_md_path.display()
        ));
    }

    fs::read_to_string(&skill_md_path)
        .with_context(|| format!("failed to read {}", skill_md_path.display()))
}

pub(crate) fn hash_directory(root: &Path) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(b"lionclaw-skill-snapshot-v2\0");
    let mut files = Vec::new();
    collect_files(root, &mut files)?;
    files.sort();

    for path in files {
        let relative = path
            .strip_prefix(root)
            .with_context(|| format!("failed to relativize {}", path.display()))?;
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("failed to stat {}", path.display()))?;
        if metadata.file_type().is_symlink() {
            return Err(anyhow!(
                "skill source entry '{}' must not be a symlink",
                path.display()
            ));
        }
        if !metadata.is_file() {
            return Err(anyhow!(
                "skill source entry '{}' is not a regular file",
                path.display()
            ));
        }

        hasher.update(b"file\0");
        hasher.update(relative.to_string_lossy().as_bytes());
        hasher.update(b"\0len\0");
        hasher.update(metadata.len().to_le_bytes());
        hasher.update(b"\0mode\0");
        hasher.update(snapshot_mode_bits(&metadata).to_le_bytes());

        let mut file =
            fs::File::open(&path).with_context(|| format!("failed to open {}", path.display()))?;
        let mut buffer = [0_u8; 8192];
        loop {
            let count = file
                .read(&mut buffer)
                .with_context(|| format!("failed to read {}", path.display()))?;
            if count == 0 {
                break;
            }
            if let Some(chunk) = buffer.get(..count) {
                hasher.update(chunk);
            }
        }
    }

    Ok(hex::encode(hasher.finalize()))
}

fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    let mut entries = fs::read_dir(dir)
        .with_context(|| format!("failed to read directory {}", dir.display()))?
        .collect::<std::io::Result<Vec<_>>>()
        .with_context(|| format!("failed to iterate directory {}", dir.display()))?;
    entries.sort_by_key(|left| left.path());

    for entry in entries {
        let path = entry.path();
        let file_name = path.file_name().and_then(OsStr::to_str).unwrap_or_default();
        if should_ignore_snapshot_entry(file_name) {
            continue;
        }

        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("failed to stat {}", path.display()))?;
        if metadata.file_type().is_symlink() {
            return Err(anyhow!(
                "skill source entry '{}' must not be a symlink",
                path.display()
            ));
        }
        if metadata.is_dir() {
            collect_files(&path, out)?;
        } else if metadata.is_file() {
            out.push(path);
        } else {
            return Err(anyhow!(
                "skill source entry '{}' is not a regular file or directory",
                path.display()
            ));
        }
    }

    Ok(())
}

fn copy_directory(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination)
        .with_context(|| format!("failed to create {}", destination.display()))?;

    let mut entries = fs::read_dir(source)
        .with_context(|| format!("failed to read directory {}", source.display()))?
        .collect::<std::io::Result<Vec<_>>>()
        .with_context(|| format!("failed to iterate directory {}", source.display()))?;
    entries.sort_by_key(|left| left.path());

    for entry in entries {
        let path = entry.path();
        let target = destination.join(entry.file_name());
        let file_name = path.file_name().and_then(OsStr::to_str).unwrap_or_default();
        if should_ignore_snapshot_entry(file_name) {
            continue;
        }

        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("failed to stat {}", path.display()))?;
        if metadata.file_type().is_symlink() {
            return Err(anyhow!(
                "skill source entry '{}' must not be a symlink",
                path.display()
            ));
        }

        if metadata.is_dir() {
            copy_directory(&path, &target)?;
        } else if metadata.is_file() {
            fs::copy(&path, &target).with_context(|| {
                format!(
                    "failed to copy '{}' to '{}'",
                    path.display(),
                    target.display()
                )
            })?;
        } else {
            return Err(anyhow!(
                "skill source entry '{}' is not a regular file or directory",
                path.display()
            ));
        }
    }

    Ok(())
}

fn validate_existing_snapshot_dir(path: &Path, metadata: &fs::Metadata) -> Result<()> {
    if metadata.file_type().is_symlink() {
        return Err(anyhow!(
            "skill snapshot directory '{}' must not be a symlink",
            path.display()
        ));
    }
    if !metadata.is_dir() {
        return Err(anyhow!(
            "skill snapshot path '{}' is not a directory",
            path.display()
        ));
    }

    Ok(())
}

fn snapshot_mode_bits(metadata: &fs::Metadata) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        metadata.permissions().mode() & 0o111
    }

    #[cfg(not(unix))]
    {
        let _ = metadata;
        0
    }
}

fn should_ignore_snapshot_entry(file_name: &str) -> bool {
    matches!(
        file_name,
        ".git"
            | "target"
            | ".venv"
            | "__pycache__"
            | ".pytest_cache"
            | ".mypy_cache"
            | ".ruff_cache"
            | SKILL_INSTALL_METADATA_FILE
    )
}

fn write_install_metadata(snapshot_dir: &Path, source: &str, reference: &str) -> Result<()> {
    let metadata_path = snapshot_dir.join(SKILL_INSTALL_METADATA_FILE);
    let content = format!(
        "source = {source:?}\nreference = {reference:?}\ninstalled_at_ms = {}\ninstalled_at = {:?}\n",
        now_ms(),
        Utc::now().to_rfc3339()
    );
    fs::write(&metadata_path, content)
        .with_context(|| format!("failed to write {}", metadata_path.display()))
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
    };

    use super::install_snapshot;

    fn write_skill_source(root: &Path, name: &str) -> PathBuf {
        let source_dir = root.join(name);
        fs::create_dir_all(source_dir.join("scripts")).expect("source dir");
        fs::write(
            source_dir.join("SKILL.md"),
            format!("---\nname: {name}\ndescription: test\n---\n"),
        )
        .expect("skill");
        fs::write(source_dir.join("scripts/worker"), "#!/usr/bin/env bash\n").expect("worker");
        source_dir
    }

    #[test]
    fn installs_idempotent_snapshot_directory() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let source_dir = write_skill_source(temp_dir.path(), "channel-telegram");

        let home = crate::home::LionClawHome::new(temp_dir.path().join(".lionclaw"));
        fs::create_dir_all(home.skills_dir()).expect("skills dir");

        let first = install_snapshot(
            &home,
            "telegram",
            source_dir.to_string_lossy().as_ref(),
            "local",
        )
        .expect("snapshot");
        let second = install_snapshot(
            &home,
            "telegram",
            source_dir.to_string_lossy().as_ref(),
            "local",
        )
        .expect("snapshot");

        assert_eq!(first.hash, second.hash);
        assert_eq!(first.snapshot_abs_dir, second.snapshot_abs_dir);
        assert!(first.snapshot_abs_dir.join("scripts/worker").exists());
    }

    #[test]
    fn ignores_local_runtime_cache_directories() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let source_dir = write_skill_source(temp_dir.path(), "channel-terminal");
        fs::create_dir_all(source_dir.join(".venv/bin")).expect("venv dir");
        fs::create_dir_all(source_dir.join("pkg/__pycache__")).expect("pycache dir");
        fs::create_dir_all(source_dir.join(".pytest_cache")).expect("pytest cache dir");
        fs::write(source_dir.join(".venv/bin/python"), "shim\n").expect("venv file");
        fs::write(source_dir.join("pkg/__pycache__/state.pyc"), "cache\n").expect("pycache file");
        fs::write(source_dir.join(".pytest_cache/CACHEDIR.TAG"), "cache\n").expect("pytest file");

        let home = crate::home::LionClawHome::new(temp_dir.path().join(".lionclaw"));
        fs::create_dir_all(home.skills_dir()).expect("skills dir");

        let snapshot = install_snapshot(
            &home,
            "terminal",
            source_dir.to_string_lossy().as_ref(),
            "local",
        )
        .expect("snapshot");

        assert!(snapshot.snapshot_abs_dir.join("scripts/worker").exists());
        assert!(!snapshot.snapshot_abs_dir.join(".venv").exists());
        assert!(!snapshot.snapshot_abs_dir.join("pkg/__pycache__").exists());
        assert!(!snapshot.snapshot_abs_dir.join(".pytest_cache").exists());
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlinked_source_entries() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let source_dir = write_skill_source(temp_dir.path(), "channel-terminal");
        let outside_assets = temp_dir.path().join("outside-assets");
        fs::create_dir_all(&outside_assets).expect("outside assets");
        fs::write(outside_assets.join("token.txt"), "leaked-token\n").expect("token");
        symlink(&outside_assets, source_dir.join("assets")).expect("assets symlink");

        let home = crate::home::LionClawHome::new(temp_dir.path().join(".lionclaw"));
        fs::create_dir_all(home.skills_dir()).expect("skills dir");

        let err = install_snapshot(
            &home,
            "terminal",
            source_dir.to_string_lossy().as_ref(),
            "local",
        )
        .expect_err("symlinked source entry should fail");

        assert!(
            err.to_string().contains("must not be a symlink"),
            "unexpected error: {err}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlinked_skill_metadata() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let source_dir = temp_dir.path().join("channel-terminal");
        fs::create_dir_all(source_dir.join("scripts")).expect("source dir");
        fs::write(source_dir.join("scripts/worker"), "#!/usr/bin/env bash\n").expect("worker");
        let outside_skill = temp_dir.path().join("outside-SKILL.md");
        fs::write(
            &outside_skill,
            "---\nname: outside\ndescription: outside\n---\n",
        )
        .expect("outside skill md");
        symlink(&outside_skill, source_dir.join("SKILL.md")).expect("skill md symlink");

        let home = crate::home::LionClawHome::new(temp_dir.path().join(".lionclaw"));
        fs::create_dir_all(home.skills_dir()).expect("skills dir");

        let err = install_snapshot(
            &home,
            "terminal",
            source_dir.to_string_lossy().as_ref(),
            "local",
        )
        .expect_err("symlinked skill metadata should fail");

        assert!(
            err.to_string().contains("must not be a symlink"),
            "unexpected error: {err}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn snapshot_identity_includes_executable_bits() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let source_dir = write_skill_source(temp_dir.path(), "channel-terminal");
        let worker = source_dir.join("scripts/worker");
        fs::set_permissions(&worker, fs::Permissions::from_mode(0o644)).expect("chmod 644");

        let home = crate::home::LionClawHome::new(temp_dir.path().join(".lionclaw"));
        fs::create_dir_all(home.skills_dir()).expect("skills dir");

        let first = install_snapshot(
            &home,
            "terminal",
            source_dir.to_string_lossy().as_ref(),
            "local",
        )
        .expect("snapshot 644");
        fs::set_permissions(&worker, fs::Permissions::from_mode(0o755)).expect("chmod 755");
        let second = install_snapshot(
            &home,
            "terminal",
            source_dir.to_string_lossy().as_ref(),
            "local",
        )
        .expect("snapshot 755");

        assert_ne!(first.hash, second.hash);
        assert_ne!(first.skill_id, second.skill_id);
        assert_eq!(first.snapshot_abs_dir, second.snapshot_abs_dir);
        let installed_mode = fs::metadata(second.snapshot_abs_dir.join("scripts/worker"))
            .expect("installed worker metadata")
            .permissions()
            .mode();
        assert_ne!(installed_mode & 0o111, 0);
    }

    #[cfg(unix)]
    #[test]
    fn rejects_existing_snapshot_symlink() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let source_dir = write_skill_source(temp_dir.path(), "channel-terminal");
        let home = crate::home::LionClawHome::new(temp_dir.path().join(".lionclaw"));
        fs::create_dir_all(home.skills_dir()).expect("skills dir");
        let snapshot = install_snapshot(
            &home,
            "terminal",
            source_dir.to_string_lossy().as_ref(),
            "local",
        )
        .expect("snapshot");
        fs::remove_dir_all(&snapshot.snapshot_abs_dir).expect("remove snapshot");
        let outside = temp_dir.path().join("outside-snapshot");
        fs::create_dir_all(&outside).expect("outside snapshot");
        symlink(&outside, &snapshot.snapshot_abs_dir).expect("snapshot symlink");

        let err = install_snapshot(
            &home,
            "terminal",
            source_dir.to_string_lossy().as_ref(),
            "local",
        )
        .expect_err("snapshot symlink should fail");

        assert!(
            err.to_string().contains("must not be a symlink"),
            "unexpected error: {err}"
        );
    }
}
