use std::{
    fs,
    path::{Component, Path},
};

use anyhow::{anyhow, bail, Context, Result};

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
    ensure_file_target_not_symlink(path, label)
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

fn ensure_file_target_not_symlink(path: &Path, label: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                bail!("{label} {} must not be a symlink", path.display());
            }
            if metadata.is_dir() {
                bail!("{label} {} is not a file", path.display());
            }
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("failed to stat {}", path.display())),
    }
}
