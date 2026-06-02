use std::{
    ffi::OsString,
    fs,
    path::{Component, Path, PathBuf},
};

use anyhow::{anyhow, bail, Context, Result};
use lionclaw_durable_fs::{
    remove_file_if_exists as remove_file_if_exists_durably, write_file_atomically,
};

use crate::home::LionClawHome;

pub(crate) fn create_private_dir_all(home: &LionClawHome, path: &Path, label: &str) -> Result<()> {
    let root = home.root();
    ensure_path_under_home(&root, path, label)?;
    create_home_root(&root)?;

    let relative = path.strip_prefix(&root).with_context(|| {
        format!(
            "{label} {} is not under LionClaw home {}",
            path.display(),
            root.display()
        )
    })?;
    let mut current = root;
    ensure_private_dir(&current, "LionClaw home")?;
    for component in relative.components() {
        let Component::Normal(name) = component else {
            bail!(
                "{label} {} contains an unsupported path component",
                path.display()
            );
        };
        current.push(name);
        match fs::symlink_metadata(&current) {
            Ok(metadata) => ensure_private_dir_metadata(&current, label, metadata)?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current)
                    .with_context(|| format!("failed to create {}", current.display()))?;
                ensure_private_dir(&current, label)?;
            }
            Err(err) => {
                return Err(err).with_context(|| format!("failed to stat {}", current.display()));
            }
        }
    }
    Ok(())
}

pub(crate) fn ensure_private_file_write_target(
    home: &LionClawHome,
    path: &Path,
    label: &str,
) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        anyhow!(
            "{label} {} does not have a parent directory",
            path.display()
        )
    })?;
    create_private_dir_all(home, parent, &format!("{label} directory"))?;
    ensure_file_target_regular_or_absent(path, label)
}

pub(crate) fn write_private_file(
    home: &LionClawHome,
    path: &Path,
    contents: &[u8],
    label: &str,
) -> Result<()> {
    ensure_private_file_write_target(home, path, label)?;
    write_private_file_contents(home, path, contents, label)
}

pub(crate) fn read_private_file_to_string(
    home: &LionClawHome,
    path: &Path,
    label: &str,
) -> Result<Option<String>> {
    if !private_parent_chain_exists(home, path, label)? {
        return Ok(None);
    }
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("failed to stat {}", path.display())),
    };
    ensure_private_file_metadata(path, label, metadata)?;
    harden_private_file(path, label)?;
    fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))
        .map(Some)
}

pub(crate) fn private_file_exists(home: &LionClawHome, path: &Path, label: &str) -> Result<bool> {
    if !private_parent_chain_exists(home, path, label)? {
        return Ok(false);
    }
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err).with_context(|| format!("failed to stat {}", path.display())),
    };
    ensure_private_file_metadata(path, label, metadata)?;
    Ok(true)
}

pub(crate) fn remove_private_file_if_exists(
    home: &LionClawHome,
    path: &Path,
    label: &str,
) -> Result<()> {
    if !private_parent_chain_exists(home, path, label)? {
        return Ok(());
    }
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err).with_context(|| format!("failed to stat {}", path.display())),
    };
    ensure_private_file_metadata(path, label, metadata)?;
    remove_private_file_after_validation(home, path, label)
}

pub(crate) fn read_private_dir_file_paths(
    home: &LionClawHome,
    path: &Path,
    label: &str,
) -> Result<Vec<std::path::PathBuf>> {
    if !private_dir_exists(home, path, label)? {
        return Ok(Vec::new());
    }

    let mut paths = Vec::new();
    for entry in fs::read_dir(path).with_context(|| format!("failed to read {}", path.display()))? {
        let entry = entry.with_context(|| format!("failed to iterate {}", path.display()))?;
        let entry_path = entry.path();
        let metadata = fs::symlink_metadata(&entry_path)
            .with_context(|| format!("failed to stat {}", entry_path.display()))?;
        if metadata.file_type().is_symlink() {
            bail!(
                "{label} entry {} must not be a symlink",
                entry_path.display()
            );
        }
        if metadata.is_file() {
            paths.push(entry_path);
        }
    }
    paths.sort();
    Ok(paths)
}

pub(crate) fn ensure_private_file_readable(
    home: &LionClawHome,
    path: &Path,
    label: &str,
) -> Result<()> {
    if !private_parent_chain_exists(home, path, label)? {
        bail!("{label} {} does not exist", path.display());
    }
    ensure_private_file_exists(path, label)?;
    harden_private_file(path, label)
}

pub(crate) fn set_private_file_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to chmod {}", path.display()))?;
    }
    Ok(())
}

fn create_home_root(root: &Path) -> Result<()> {
    match fs::symlink_metadata(root) {
        Ok(metadata) => ensure_private_dir_metadata(root, "LionClaw home", metadata),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(root)
                .with_context(|| format!("failed to create {}", root.display()))?;
            ensure_private_dir(root, "LionClaw home")
        }
        Err(err) => Err(err).with_context(|| format!("failed to stat {}", root.display())),
    }
}

fn private_parent_chain_exists(home: &LionClawHome, path: &Path, label: &str) -> Result<bool> {
    let root = home.root();
    ensure_path_under_home(&root, path, label)?;
    let parent = path.parent().ok_or_else(|| {
        anyhow!(
            "{label} {} does not have a parent directory",
            path.display()
        )
    })?;
    let relative = parent.strip_prefix(&root).with_context(|| {
        format!(
            "{label} {} is not under LionClaw home {}",
            path.display(),
            root.display()
        )
    })?;

    let mut current = root;
    let metadata = match fs::symlink_metadata(&current) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => {
            return Err(err).with_context(|| format!("failed to stat {}", current.display()));
        }
    };
    ensure_private_dir_metadata(&current, "LionClaw home", metadata)?;

    for component in relative.components() {
        let Component::Normal(name) = component else {
            bail!(
                "{label} {} contains an unsupported path component",
                path.display()
            );
        };
        current.push(name);
        let metadata = match fs::symlink_metadata(&current) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(err) => {
                return Err(err).with_context(|| format!("failed to stat {}", current.display()));
            }
        };
        ensure_private_dir_metadata(&current, &format!("{label} directory"), metadata)?;
    }

    Ok(true)
}

fn ensure_path_under_home(root: &Path, path: &Path, label: &str) -> Result<()> {
    if path.strip_prefix(root).is_err() {
        bail!(
            "{label} {} must be under LionClaw home {}",
            path.display(),
            root.display()
        );
    }
    Ok(())
}

fn ensure_private_dir(path: &Path, label: &str) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("failed to stat {}", path.display()))?;
    ensure_private_dir_metadata(path, label, metadata)
}

fn ensure_private_dir_metadata(path: &Path, label: &str, metadata: fs::Metadata) -> Result<()> {
    if metadata.file_type().is_symlink() {
        bail!("{label} {} must not be a symlink", path.display());
    }
    if !metadata.is_dir() {
        bail!("{label} {} is not a directory", path.display());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode = metadata.permissions().mode();
        if mode & 0o077 != 0 {
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))
                .with_context(|| format!("failed to chmod {}", path.display()))?;
        }
    }
    Ok(())
}

fn ensure_private_file_exists(path: &Path, label: &str) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("failed to stat {}", path.display()))?;
    ensure_private_file_metadata(path, label, metadata)
}

fn ensure_private_file_metadata(path: &Path, label: &str, metadata: fs::Metadata) -> Result<()> {
    if metadata.file_type().is_symlink() {
        bail!("{label} {} must not be a symlink", path.display());
    }
    if !metadata.is_file() {
        bail!("{label} {} is not a file", path.display());
    }
    Ok(())
}

fn harden_private_file(path: &Path, label: &str) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        anyhow!(
            "{label} {} does not have a parent directory",
            path.display()
        )
    })?;
    ensure_private_dir(parent, &format!("{label} directory"))?;
    set_private_file_permissions(path)
}

fn private_dir_exists(home: &LionClawHome, path: &Path, label: &str) -> Result<bool> {
    let root = home.root();
    ensure_path_under_home(&root, path, label)?;
    let relative = path.strip_prefix(&root).with_context(|| {
        format!(
            "{label} {} is not under LionClaw home {}",
            path.display(),
            root.display()
        )
    })?;

    let mut current = root;
    let metadata = match fs::symlink_metadata(&current) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => {
            return Err(err).with_context(|| format!("failed to stat {}", current.display()));
        }
    };
    ensure_private_dir_metadata(&current, "LionClaw home", metadata)?;

    for component in relative.components() {
        let Component::Normal(name) = component else {
            bail!(
                "{label} {} contains an unsupported path component",
                path.display()
            );
        };
        current.push(name);
        let metadata = match fs::symlink_metadata(&current) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(err) => {
                return Err(err).with_context(|| format!("failed to stat {}", current.display()));
            }
        };
        ensure_private_dir_metadata(&current, label, metadata)?;
    }

    Ok(true)
}

fn ensure_file_target_regular_or_absent(path: &Path, label: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                bail!("{label} {} must not be a symlink", path.display());
            }
            if !metadata.is_file() {
                bail!("{label} {} is not a file", path.display());
            }
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("failed to stat {}", path.display())),
    }
}

#[cfg(unix)]
fn write_private_file_contents(
    home: &LionClawHome,
    path: &Path,
    contents: &[u8],
    label: &str,
) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let Some((parent, parent_path, file_name)) = open_private_file_parent(home, path, label)?
    else {
        bail!(
            "{label} {} does not have an existing private parent directory",
            path.display()
        );
    };
    write_file_atomically(
        &parent,
        &parent_path,
        &file_name,
        contents,
        0o600,
        Some(fs::Permissions::from_mode(0o600)),
        label,
    )
}

#[cfg(not(unix))]
fn write_private_file_contents(
    _home: &LionClawHome,
    path: &Path,
    contents: &[u8],
    _label: &str,
) -> Result<()> {
    fs::write(path, contents).with_context(|| format!("failed to write {}", path.display()))?;
    set_private_file_permissions(path)
}

#[cfg(unix)]
fn remove_private_file_after_validation(
    home: &LionClawHome,
    path: &Path,
    label: &str,
) -> Result<()> {
    let Some((parent, parent_path, file_name)) = open_private_file_parent(home, path, label)?
    else {
        return Ok(());
    };
    remove_file_if_exists_durably(&parent, &parent_path, &file_name, label).map(|_| ())
}

#[cfg(not(unix))]
fn remove_private_file_after_validation(
    _home: &LionClawHome,
    path: &Path,
    _label: &str,
) -> Result<()> {
    fs::remove_file(path).with_context(|| format!("failed to remove {}", path.display()))
}

#[cfg(unix)]
fn open_private_file_parent(
    home: &LionClawHome,
    path: &Path,
    label: &str,
) -> Result<Option<(fs::File, PathBuf, OsString)>> {
    let root = home.root();
    ensure_path_under_home(&root, path, label)?;
    let relative = path.strip_prefix(&root).with_context(|| {
        format!(
            "{label} {} is not under LionClaw home {}",
            path.display(),
            root.display()
        )
    })?;
    let file_name = relative
        .file_name()
        .map(OsString::from)
        .ok_or_else(|| anyhow!("{label} {} does not name a file", path.display()))?;
    let parent_relative = relative.parent().unwrap_or_else(|| Path::new(""));
    let Some(parent) = open_private_dir_relative(&root, parent_relative, label)? else {
        return Ok(None);
    };
    Ok(Some((parent, root.join(parent_relative), file_name)))
}

#[cfg(unix)]
fn open_private_dir_relative(
    root: &Path,
    relative: &Path,
    label: &str,
) -> Result<Option<fs::File>> {
    use rustix::fs::{open, Mode, OFlags};
    use rustix::{fs::openat, io::Errno};

    let root_dir = match open(
        root,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    ) {
        Ok(root_dir) => root_dir,
        Err(Errno::NOENT) => return Ok(None),
        Err(Errno::LOOP | Errno::NOTDIR) => bail!(
            "LionClaw home {} must be a directory and cannot be a symlink",
            root.display()
        ),
        Err(err) => return Err(err).with_context(|| format!("failed to open {}", root.display())),
    };
    let mut current = fs::File::from(root_dir);
    let mut current_path = root.to_path_buf();

    for component in relative.components() {
        let Component::Normal(name) = component else {
            bail!(
                "{label} {} contains an unsupported path component",
                root.join(relative).display()
            );
        };
        current_path.push(name);
        let next = match openat(
            &current,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        ) {
            Ok(next) => next,
            Err(Errno::NOENT) => return Ok(None),
            Err(Errno::LOOP | Errno::NOTDIR) => bail!(
                "{label} directory {} must be a directory and cannot be a symlink",
                current_path.display()
            ),
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("failed to open {}", current_path.display()))
            }
        };
        current = fs::File::from(next);
    }

    Ok(Some(current))
}
