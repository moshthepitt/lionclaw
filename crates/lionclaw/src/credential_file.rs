use std::fs::File;
use std::io::Read;
use std::path::{Component, Path};

use anyhow::{anyhow, bail, Context, Result};
use rustix::{
    fs::{open, openat, FileType, Mode, OFlags},
    io::Errno,
};

pub(crate) fn open_regular_file_beneath(
    root: &Path,
    relative: &Path,
    required: bool,
    label: &str,
) -> Result<Option<File>> {
    validate_relative_path(relative, label)?;
    let mut directory = open(root, directory_flags(), Mode::empty()).map_err(|error| {
        anyhow!(
            "{label} root '{}' must be an exact real directory: {error}",
            root.display()
        )
    })?;
    let components = relative.components().collect::<Vec<_>>();
    let mut display = root.to_path_buf();
    for (index, component) in components.iter().enumerate() {
        let Component::Normal(name) = component else {
            unreachable!("credential path was validated");
        };
        display.push(name);
        let is_leaf = index + 1 == components.len();
        let flags = if is_leaf {
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK
        } else {
            directory_flags()
        };
        let descriptor = match openat(&directory, *name, flags, Mode::empty()) {
            Ok(descriptor) => descriptor,
            Err(Errno::NOENT) if !required => return Ok(None),
            Err(Errno::LOOP | Errno::NOTDIR) => {
                bail!(
                    "{label} path '{}' contains symlink or invalid directory '{}'",
                    relative.display(),
                    display.display()
                )
            }
            Err(error) => {
                return Err(anyhow!(
                    "failed to open {label} '{}': {error}",
                    display.display()
                ))
            }
        };
        if is_leaf {
            let stat = rustix::fs::fstat(&descriptor)
                .with_context(|| format!("failed to inspect {label} '{}'", display.display()))?;
            if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile {
                bail!("{label} '{}' must be a regular file", display.display());
            }
            return Ok(Some(File::from(descriptor)));
        }
        directory = descriptor;
    }
    unreachable!("relative path validation requires a component")
}

pub(crate) fn read_absolute_regular_file_bounded(
    path: &Path,
    limit: usize,
    label: &str,
) -> Result<Vec<u8>> {
    let relative = path
        .strip_prefix(Path::new("/"))
        .with_context(|| format!("{label} '{}' must be absolute", path.display()))?;
    let source = open_regular_file_beneath(Path::new("/"), relative, true, label)?
        .expect("required credential file must be present");
    read_open_file_bounded(source, path, limit, label)
}

pub(crate) fn read_open_file_bounded(
    mut source: File,
    source_path: &Path,
    limit: usize,
    label: &str,
) -> Result<Vec<u8>> {
    let before = rustix::fs::fstat(&source)
        .with_context(|| format!("failed to inspect {label} '{}'", source_path.display()))?;
    if before.st_size > limit as i64 {
        bail!(
            "{label} '{}' exceeds the {limit} byte limit",
            source_path.display()
        );
    }

    let mut contents = Vec::new();
    source
        .by_ref()
        .take((limit + 1) as u64)
        .read_to_end(&mut contents)
        .with_context(|| format!("failed to read {label} '{}'", source_path.display()))?;
    if contents.len() > limit {
        bail!(
            "{label} '{}' exceeds the {limit} byte limit",
            source_path.display()
        );
    }

    let after = rustix::fs::fstat(&source)
        .with_context(|| format!("failed to reinspect {label} '{}'", source_path.display()))?;
    if source_identity(&before) != source_identity(&after) {
        bail!(
            "{label} '{}' changed while it was read",
            source_path.display()
        );
    }
    Ok(contents)
}

fn validate_relative_path(path: &Path, label: &str) -> Result<()> {
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        bail!("{label} '{}' must be a clean relative path", path.display());
    }
    Ok(())
}

fn directory_flags() -> OFlags {
    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK
}

fn source_identity(stat: &rustix::fs::Stat) -> (u64, u64, i64, i64, u64, i64, u64) {
    (
        stat.st_dev,
        stat.st_ino,
        stat.st_size,
        stat.st_mtime,
        stat.st_mtime_nsec,
        stat.st_ctime,
        stat.st_ctime_nsec,
    )
}
