use std::ffi::OsStr;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use sha2::{Digest, Sha256};

use crate::{
    home::LionClawHome,
    kernel::skills::{derive_skill_id, parse_skill_frontmatter},
    operator::config::normalize_local_source,
};

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
    let skill_md_path = source_path.join("SKILL.md");
    let skill_md = fs::read_to_string(&skill_md_path)
        .with_context(|| format!("failed to read {}", skill_md_path.display()))?;
    let (name, _) = parse_skill_frontmatter(&skill_md);
    let hash = hash_directory(&source_path)?;
    let skill_id = derive_skill_id(&name, &hash);
    let snapshot_dir_name = format!("{}@{}", skill_id, hash);
    let snapshot_abs_dir = home.skills_dir().join(&snapshot_dir_name);

    if !snapshot_abs_dir.exists() {
        copy_directory(&source_path, &snapshot_abs_dir)?;
    }

    Ok(InstalledSnapshot {
        alias: alias.to_string(),
        source_uri,
        reference: reference.to_string(),
        skill_id,
        hash,
        snapshot_rel_dir: format!("skills/{}", snapshot_dir_name),
        snapshot_abs_dir,
        skill_md,
    })
}

pub fn resolve_local_source(source_uri: &str) -> Result<PathBuf> {
    let raw = source_uri
        .strip_prefix("local:")
        .ok_or_else(|| anyhow!("unsupported skill source '{}'", source_uri))?;
    let path = PathBuf::from(raw);
    if !path.is_dir() {
        return Err(anyhow!(
            "skill source '{}' is not a directory",
            path.display()
        ));
    }
    Ok(path)
}

fn hash_directory(root: &Path) -> Result<String> {
    let mut hasher = Sha256::new();
    let mut files = Vec::new();
    collect_files(root, &mut files)?;
    files.sort();

    for path in files {
        let relative = path
            .strip_prefix(root)
            .with_context(|| format!("failed to relativize {}", path.display()))?;
        hasher.update(relative.to_string_lossy().as_bytes());
        let metadata =
            fs::metadata(&path).with_context(|| format!("failed to stat {}", path.display()))?;
        hasher.update(metadata.len().to_le_bytes());

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
            hasher.update(&buffer[..count]);
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

        if path.is_dir() {
            collect_files(&path, out)?;
        } else if path.is_file() {
            out.push(path);
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

        if path.is_dir() {
            copy_directory(&path, &target)?;
        } else if path.is_file() {
            fs::copy(&path, &target).with_context(|| {
                format!(
                    "failed to copy '{}' to '{}'",
                    path.display(),
                    target.display()
                )
            })?;
        }
    }

    Ok(())
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
    )
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::install_snapshot;

    #[test]
    fn installs_idempotent_snapshot_directory() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let source_dir = temp_dir.path().join("channel-telegram");
        fs::create_dir_all(source_dir.join("scripts")).expect("source dir");
        fs::write(
            source_dir.join("SKILL.md"),
            "---\nname: channel-telegram\ndescription: test\n---\n",
        )
        .expect("skill");
        fs::write(
            source_dir.join("scripts/worker.sh"),
            "#!/usr/bin/env bash\n",
        )
        .expect("worker");

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
        assert!(first.snapshot_abs_dir.join("scripts/worker.sh").exists());
    }

    #[test]
    fn ignores_local_runtime_cache_directories() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let source_dir = temp_dir.path().join("channel-terminal");
        fs::create_dir_all(source_dir.join("scripts")).expect("scripts dir");
        fs::create_dir_all(source_dir.join(".venv/bin")).expect("venv dir");
        fs::create_dir_all(source_dir.join("pkg/__pycache__")).expect("pycache dir");
        fs::create_dir_all(source_dir.join(".pytest_cache")).expect("pytest cache dir");
        fs::write(
            source_dir.join("SKILL.md"),
            "---\nname: channel-terminal\ndescription: test\n---\n",
        )
        .expect("skill");
        fs::write(source_dir.join("scripts/worker"), "#!/usr/bin/env bash\n").expect("worker");
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
}
