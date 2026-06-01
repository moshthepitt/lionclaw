use std::{
    ffi::{OsStr, OsString},
    fs::{File, Permissions},
    io::Write,
    path::{Component, Path},
};

use anyhow::{anyhow, Context, Result};
use rustix::{
    fs::{openat, renameat, unlinkat, AtFlags, Mode, OFlags},
    io::Errno,
};
use tracing::warn;
use uuid::Uuid;

const TEMP_CREATE_ATTEMPTS: usize = 4;

pub fn write_file_atomically(
    parent: &File,
    parent_path: &Path,
    file_name: &OsStr,
    contents: &[u8],
    mode: u32,
    final_permissions: Option<Permissions>,
    label: &str,
) -> Result<()> {
    ensure_file_name(file_name, label)?;
    let (temp_name, mut temp_file) = create_temp_file(parent, parent_path, mode, label)?;
    let temp_path = Path::new(&temp_name);
    let target_path = Path::new(file_name);

    let write_result = (|| -> Result<()> {
        temp_file.write_all(contents).with_context(|| {
            format!(
                "failed to write {label} '{}' in '{}'",
                target_path.display(),
                parent_path.display()
            )
        })?;
        temp_file.flush().with_context(|| {
            format!(
                "failed to flush {label} '{}' in '{}'",
                target_path.display(),
                parent_path.display()
            )
        })?;
        if let Some(permissions) = final_permissions {
            temp_file.set_permissions(permissions).with_context(|| {
                format!(
                    "failed to chmod {label} '{}' in '{}'",
                    target_path.display(),
                    parent_path.display()
                )
            })?;
        }
        temp_file.sync_all().with_context(|| {
            format!(
                "failed to sync {label} '{}' in '{}'",
                target_path.display(),
                parent_path.display()
            )
        })?;
        renameat(parent, temp_path, parent, file_name).with_context(|| {
            format!(
                "failed to publish {label} '{}' in '{}'",
                target_path.display(),
                parent_path.display()
            )
        })?;
        sync_directory(parent, parent_path, label)
    })();

    if write_result.is_err() {
        match unlinkat(parent, temp_path, AtFlags::empty()) {
            Ok(()) | Err(Errno::NOENT) => {}
            Err(err) => warn!(
                ?err,
                path = %parent_path.join(temp_path).display(),
                "failed to remove temporary file"
            ),
        }
    }

    write_result
}

pub fn remove_file_if_exists(
    parent: &File,
    parent_path: &Path,
    file_name: &OsStr,
    label: &str,
) -> Result<bool> {
    ensure_file_name(file_name, label)?;
    match unlinkat(parent, file_name, AtFlags::empty()) {
        Ok(()) => {
            sync_directory(parent, parent_path, label)?;
            Ok(true)
        }
        Err(Errno::NOENT) => Ok(false),
        Err(err) => Err(anyhow!(
            "failed to remove {label} '{}' in '{}': {err}",
            Path::new(file_name).display(),
            parent_path.display()
        )),
    }
}

pub fn rename_file(
    source_parent: &File,
    source_parent_path: &Path,
    source_name: &OsStr,
    target_parent: &File,
    target_parent_path: &Path,
    target_name: &OsStr,
    label: &str,
) -> Result<()> {
    ensure_file_name(source_name, label)?;
    ensure_file_name(target_name, label)?;
    renameat(source_parent, source_name, target_parent, target_name).with_context(|| {
        format!(
            "failed to rename {label} '{}' to '{}'",
            source_parent_path.join(Path::new(source_name)).display(),
            target_parent_path.join(Path::new(target_name)).display()
        )
    })?;
    sync_directory(source_parent, source_parent_path, label)?;
    if source_parent_path != target_parent_path {
        sync_directory(target_parent, target_parent_path, label)?;
    }
    Ok(())
}

pub fn sync_directory(directory: &File, path: &Path, label: &str) -> Result<()> {
    directory.sync_all().with_context(|| {
        format!(
            "failed to sync directory '{}' after updating {label}",
            path.display()
        )
    })
}

fn ensure_file_name(file_name: &OsStr, label: &str) -> Result<()> {
    let path = Path::new(file_name);
    let mut components = path.components();
    let Some(Component::Normal(_)) = components.next() else {
        return Err(anyhow!("{label} file name '{}' is invalid", path.display()));
    };
    if components.next().is_some() {
        return Err(anyhow!("{label} file name '{}' is invalid", path.display()));
    }
    Ok(())
}

fn create_temp_file(
    parent: &File,
    parent_path: &Path,
    mode: u32,
    label: &str,
) -> Result<(OsString, File)> {
    for _ in 0..TEMP_CREATE_ATTEMPTS {
        let temp_name = OsString::from(format!(".lionclaw-atomic-{}.tmp", Uuid::new_v4().simple()));
        match openat(
            parent,
            &temp_name,
            OFlags::WRONLY
                | OFlags::CREATE
                | OFlags::EXCL
                | OFlags::TRUNC
                | OFlags::CLOEXEC
                | OFlags::NOFOLLOW,
            Mode::from_raw_mode(mode),
        ) {
            Ok(file) => return Ok((temp_name, File::from(file))),
            Err(Errno::EXIST) => continue,
            Err(err) => {
                return Err(anyhow!(
                    "failed to create temporary {label} file in '{}': {err}",
                    parent_path.display()
                ))
            }
        }
    }

    Err(anyhow!(
        "failed to allocate temporary {label} file name in '{}'",
        parent_path.display()
    ))
}
