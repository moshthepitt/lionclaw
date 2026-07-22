#![cfg_attr(
    not(test),
    warn(
        clippy::allow_attributes_without_reason,
        clippy::clone_on_ref_ptr,
        clippy::expect_used,
        clippy::future_not_send,
        clippy::get_unwrap,
        clippy::indexing_slicing,
        clippy::large_futures,
        clippy::large_stack_arrays,
        clippy::large_types_passed_by_value,
        clippy::let_underscore_must_use,
        clippy::mutex_atomic,
        clippy::mutex_integer,
        clippy::panic,
        clippy::panic_in_result_fn,
        clippy::pathbuf_init_then_push,
        clippy::rc_buffer,
        clippy::rc_mutex,
        clippy::redundant_clone,
        clippy::same_name_method,
        clippy::significant_drop_in_scrutinee,
        clippy::significant_drop_tightening,
        clippy::uninlined_format_args,
        clippy::unused_result_ok,
        clippy::unwrap_in_result,
        clippy::unwrap_used,
        reason = "production code follows LionClaw's strict Clippy profile; tests keep fail-fast ergonomics"
    )
)]

use std::{
    ffi::{OsStr, OsString},
    fs::{File, Permissions},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use rustix::{
    fs::{openat, renameat, unlinkat, AtFlags, Mode, OFlags},
    io::Errno,
};
use tracing::warn;
use uuid::Uuid;

const TEMP_CREATE_ATTEMPTS: usize = 4;

/// A directory reached from an explicit trusted anchor without following any
/// symlink below that anchor.
///
/// The value stores both coordinates so callers never need to infer an anchor
/// from a leaf path. Each operation reopens and walks the directory to avoid
/// trusting a stale path resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootedDirectory {
    anchor: PathBuf,
    path: PathBuf,
    relative: PathBuf,
}

impl RootedDirectory {
    pub fn new(anchor: impl Into<PathBuf>, path: impl Into<PathBuf>) -> Result<Self> {
        let anchor = anchor.into();
        let path = path.into();
        if !anchor.is_absolute() || !path.is_absolute() {
            return Err(anyhow!(
                "durable directory anchor '{}' and path '{}' must be absolute",
                anchor.display(),
                path.display()
            ));
        }
        let relative = path.strip_prefix(&anchor).map_err(|_| {
            anyhow!(
                "durable directory '{}' is not beneath anchor '{}'",
                path.display(),
                anchor.display()
            )
        })?;
        validate_relative_directory(relative, &path)?;
        let relative = relative.to_path_buf();
        Ok(Self {
            anchor,
            path,
            relative,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Check whether a regular file exists without reading its contents.
    pub fn contains_regular_file(&self, file_name: &OsStr, label: &str) -> Result<bool> {
        ensure_file_name(file_name, label)?;
        let Some(parent) = self.open_existing()? else {
            return Ok(false);
        };
        Ok(open_regular_file(&parent, &self.path, file_name, label)?.is_some())
    }

    /// Read at most `limit` bytes from an existing regular file.
    pub fn read_bounded(
        &self,
        file_name: &OsStr,
        limit: usize,
        label: &str,
    ) -> Result<Option<Vec<u8>>> {
        ensure_file_name(file_name, label)?;
        let Some(parent) = self.open_existing()? else {
            return Ok(None);
        };
        let Some(file) = open_regular_file(&parent, &self.path, file_name, label)? else {
            return Ok(None);
        };
        let metadata = file.metadata().with_context(|| {
            format!(
                "failed to stat {label} '{}'",
                self.path.join(file_name).display()
            )
        })?;
        if metadata.len() > limit as u64 {
            return Err(file_too_large(&self.path, file_name, label, limit));
        }

        let read_limit = u64::try_from(limit).unwrap_or(u64::MAX).saturating_add(1);
        let mut bytes = Vec::with_capacity(limit.min(8 * 1024));
        file.take(read_limit)
            .read_to_end(&mut bytes)
            .with_context(|| {
                format!(
                    "failed to read {label} '{}'",
                    self.path.join(file_name).display()
                )
            })?;
        if bytes.len() > limit {
            return Err(file_too_large(&self.path, file_name, label, limit));
        }
        Ok(Some(bytes))
    }

    /// Atomically replace a private regular file in this directory.
    pub fn write_private_atomic(
        &self,
        file_name: &OsStr,
        contents: &[u8],
        limit: usize,
        label: &str,
    ) -> Result<()> {
        ensure_file_name(file_name, label)?;
        if contents.len() > limit {
            return Err(file_too_large(&self.path, file_name, label, limit));
        }
        let parent = self.open_required()?;
        let _ = open_regular_file(&parent, &self.path, file_name, label)?;
        write_file_atomically(&parent, &self.path, file_name, contents, 0o600, None, label)
    }

    /// Remove exactly one regular file, if present.
    pub fn remove_file(&self, file_name: &OsStr, label: &str) -> Result<bool> {
        ensure_file_name(file_name, label)?;
        let Some(parent) = self.open_existing()? else {
            return Ok(false);
        };
        let Some(_file) = open_regular_file(&parent, &self.path, file_name, label)? else {
            return Ok(false);
        };
        remove_file_if_exists(&parent, &self.path, file_name, label)
    }

    fn open_required(&self) -> Result<File> {
        self.open_existing()?.ok_or_else(|| {
            anyhow!(
                "durable directory '{}' does not exist beneath anchor '{}'",
                self.path.display(),
                self.anchor.display()
            )
        })
    }

    fn open_existing(&self) -> Result<Option<File>> {
        let mut directory = match rustix::fs::open(&self.anchor, directory_flags(), Mode::empty()) {
            Ok(directory) => File::from(directory),
            Err(Errno::NOENT) => return Ok(None),
            Err(Errno::LOOP | Errno::NOTDIR) => {
                return Err(anyhow!(
                    "durable directory anchor '{}' must be a real directory",
                    self.anchor.display()
                ))
            }
            Err(error) => {
                return Err(anyhow!(
                    "failed to open durable directory anchor '{}': {error}",
                    self.anchor.display()
                ))
            }
        };
        let mut display = self.anchor.clone();
        for component in self.relative.components() {
            let Component::Normal(name) = component else {
                unreachable!("constructor validated rooted directory components");
            };
            display.push(name);
            directory = match openat(&directory, name, directory_flags(), Mode::empty()) {
                Ok(directory) => File::from(directory),
                Err(Errno::NOENT) => return Ok(None),
                Err(Errno::LOOP | Errno::NOTDIR) => {
                    return Err(anyhow!(
                        "durable directory component '{}' must be a real directory",
                        display.display()
                    ))
                }
                Err(error) => {
                    return Err(anyhow!(
                        "failed to open durable directory component '{}': {error}",
                        display.display()
                    ))
                }
            };
        }
        Ok(Some(directory))
    }
}

fn directory_flags() -> OFlags {
    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK
}

fn validate_relative_directory(relative: &Path, path: &Path) -> Result<()> {
    for component in relative.components() {
        if !matches!(component, Component::Normal(_)) {
            return Err(anyhow!(
                "durable directory '{}' has an invalid component",
                path.display()
            ));
        }
    }
    Ok(())
}

fn open_regular_file(
    parent: &File,
    parent_path: &Path,
    file_name: &OsStr,
    label: &str,
) -> Result<Option<File>> {
    let file = match openat(
        parent,
        file_name,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    ) {
        Ok(file) => File::from(file),
        Err(Errno::NOENT) => return Ok(None),
        Err(Errno::LOOP) => {
            return Err(anyhow!(
                "{label} '{}' cannot be a symlink",
                parent_path.join(file_name).display()
            ))
        }
        Err(error) => {
            return Err(anyhow!(
                "failed to open {label} '{}': {error}",
                parent_path.join(file_name).display()
            ))
        }
    };
    if !file
        .metadata()
        .with_context(|| {
            format!(
                "failed to stat {label} '{}'",
                parent_path.join(file_name).display()
            )
        })?
        .is_file()
    {
        return Err(anyhow!(
            "{label} '{}' must be a regular file",
            parent_path.join(file_name).display()
        ));
    }
    Ok(Some(file))
}

fn file_too_large(parent: &Path, file_name: &OsStr, label: &str, limit: usize) -> anyhow::Error {
    anyhow!(
        "{label} '{}' exceeds the {limit}-byte limit",
        parent.join(file_name).display()
    )
}

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

#[cfg(test)]
mod tests {
    use super::*;

    fn rooted(root: &tempfile::TempDir) -> RootedDirectory {
        RootedDirectory::new(root.path(), root.path().join("missions/m1/runtime")).unwrap()
    }

    #[test]
    fn private_file_round_trip_and_exact_remove() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::tempdir().unwrap();
        let directory = rooted(&root);
        std::fs::create_dir_all(directory.path()).unwrap();

        directory
            .write_private_atomic(OsStr::new("session"), b"first", 32, "session state")
            .unwrap();
        directory
            .write_private_atomic(OsStr::new("session"), b"second", 32, "session state")
            .unwrap();
        assert_eq!(
            directory
                .read_bounded(OsStr::new("session"), 32, "session state")
                .unwrap(),
            Some(b"second".to_vec())
        );
        assert!(directory
            .contains_regular_file(OsStr::new("session"), "session state")
            .unwrap());
        assert_eq!(
            std::fs::metadata(directory.path().join("session"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert!(directory
            .remove_file(OsStr::new("session"), "session state")
            .unwrap());
        assert!(!directory
            .remove_file(OsStr::new("session"), "session state")
            .unwrap());
        assert!(!directory
            .contains_regular_file(OsStr::new("session"), "session state")
            .unwrap());
    }

    #[test]
    fn bounded_read_rejects_oversized_file() {
        let root = tempfile::tempdir().unwrap();
        let directory = rooted(&root);
        std::fs::create_dir_all(directory.path()).unwrap();
        std::fs::write(directory.path().join("session"), b"oversized").unwrap();

        let error = directory
            .read_bounded(OsStr::new("session"), 4, "session state")
            .unwrap_err();
        assert!(error.to_string().contains("4-byte limit"));
    }

    #[test]
    fn missing_root_is_absent_for_read_and_remove_but_not_write() {
        let root = tempfile::tempdir().unwrap();
        let directory = rooted(&root);

        assert_eq!(
            directory
                .read_bounded(OsStr::new("session"), 32, "session state")
                .unwrap(),
            None
        );
        assert!(!directory
            .remove_file(OsStr::new("session"), "session state")
            .unwrap());
        assert!(directory
            .write_private_atomic(OsStr::new("session"), b"value", 32, "session state")
            .unwrap_err()
            .to_string()
            .contains("does not exist"));
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_ancestor_and_leaf_are_rejected() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("missions")).unwrap();
        symlink(outside.path(), root.path().join("missions/m1")).unwrap();
        let directory = rooted(&root);

        let error = directory
            .read_bounded(OsStr::new("session"), 32, "session state")
            .unwrap_err();
        assert!(error.to_string().contains("must be a real directory"));

        std::fs::remove_file(root.path().join("missions/m1")).unwrap();
        std::fs::create_dir_all(directory.path()).unwrap();
        let outside_file = outside.path().join("outside");
        std::fs::write(&outside_file, b"outside").unwrap();
        symlink(&outside_file, directory.path().join("session")).unwrap();
        assert!(directory
            .write_private_atomic(OsStr::new("session"), b"value", 32, "session state")
            .unwrap_err()
            .to_string()
            .contains("cannot be a symlink"));
        assert_eq!(std::fs::read(&outside_file).unwrap(), b"outside");
    }
}
