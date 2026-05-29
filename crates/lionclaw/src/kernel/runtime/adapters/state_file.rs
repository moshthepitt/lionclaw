use std::{
    ffi::OsString,
    io::Read,
    path::{Component, Path},
};

use anyhow::{anyhow, Context, Result};
use rustix::{
    fs::{open, openat, Mode, OFlags},
    io::Errno,
};

use crate::durable_fs::write_file_atomically;

pub(super) fn load_state_value(
    runtime_state_root: &Path,
    file_name: &str,
    label: &str,
) -> Result<Option<String>> {
    let target_name = state_file_name(file_name)?;
    let Some(root) = open_existing_state_root(runtime_state_root)? else {
        return Ok(None);
    };
    let file = match openat(
        &root,
        &target_name,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    ) {
        Ok(file) => file,
        Err(Errno::NOENT) => return Ok(None),
        Err(Errno::LOOP) => {
            return Err(anyhow!(
                "{label} state file '{}' cannot be a symlink",
                runtime_state_root.join(file_name).display()
            ))
        }
        Err(err) => {
            return Err(anyhow!(
                "failed to open {label} state file '{}' in '{}': {err}",
                file_name,
                runtime_state_root.display()
            ))
        }
    };

    let mut file = std::fs::File::from(file);
    if !file
        .metadata()
        .with_context(|| {
            format!(
                "failed to stat {label} state file '{}' in '{}'",
                file_name,
                runtime_state_root.display()
            )
        })?
        .is_file()
    {
        return Ok(None);
    }

    let mut contents = String::new();
    file.read_to_string(&mut contents).with_context(|| {
        format!(
            "failed to read {label} state file '{}' in '{}'",
            file_name,
            runtime_state_root.display()
        )
    })?;
    normalize_state_value(contents, label)
}

pub(super) fn save_state_value(
    runtime_state_root: &Path,
    file_name: &str,
    value: &str,
    label: &str,
) -> Result<()> {
    let target_name = state_file_name(file_name)?;
    let value = match normalize_state_value(value, label)? {
        Some(value) => value,
        None => return Ok(()),
    };
    let root = open_state_root(runtime_state_root)?;
    validate_existing_state_file(&root, runtime_state_root, file_name, &target_name, label)?;

    let mut contents = value.into_bytes();
    contents.push(b'\n');
    write_file_atomically(
        &root,
        runtime_state_root,
        &target_name,
        &contents,
        0o600,
        None,
        &format!("{label} state file"),
    )
}

fn validate_existing_state_file(
    root: &std::fs::File,
    runtime_state_root: &Path,
    file_name: &str,
    target_name: &OsString,
    label: &str,
) -> Result<()> {
    let file = match openat(
        root,
        target_name,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    ) {
        Ok(file) => file,
        Err(Errno::NOENT) => return Ok(()),
        Err(Errno::LOOP) => {
            return Err(anyhow!(
                "{label} state file '{}' cannot be a symlink",
                runtime_state_root.join(file_name).display()
            ))
        }
        Err(err) => {
            return Err(anyhow!(
                "failed to open existing {label} state file '{}' in '{}': {err}",
                file_name,
                runtime_state_root.display()
            ))
        }
    };
    let file = std::fs::File::from(file);
    if !file
        .metadata()
        .with_context(|| {
            format!(
                "failed to stat existing {label} state file '{}' in '{}'",
                file_name,
                runtime_state_root.display()
            )
        })?
        .is_file()
    {
        return Err(anyhow!(
            "{label} state path '{}' must be a regular file",
            runtime_state_root.join(file_name).display()
        ));
    }
    Ok(())
}

fn open_state_root(runtime_state_root: &Path) -> Result<std::fs::File> {
    let root = open(
        runtime_state_root,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .with_context(|| format!("failed to open '{}'", runtime_state_root.display()))?;
    Ok(std::fs::File::from(root))
}

fn open_existing_state_root(runtime_state_root: &Path) -> Result<Option<std::fs::File>> {
    match open(
        runtime_state_root,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    ) {
        Ok(root) => Ok(Some(std::fs::File::from(root))),
        Err(Errno::NOENT) => Ok(None),
        Err(err) => Err(anyhow!(
            "failed to open runtime state root '{}': {err}",
            runtime_state_root.display()
        )),
    }
}

fn state_file_name(file_name: &str) -> Result<OsString> {
    let path = Path::new(file_name);
    let mut components = path.components();
    let Some(Component::Normal(name)) = components.next() else {
        return Err(anyhow!("runtime state file name '{file_name}' is invalid"));
    };
    if components.next().is_some() {
        return Err(anyhow!("runtime state file name '{file_name}' is invalid"));
    }
    Ok(OsString::from(name))
}

fn normalize_state_value(value: impl AsRef<str>, label: &str) -> Result<Option<String>> {
    let value = value.as_ref().trim();
    if value.is_empty() {
        return Ok(None);
    }
    if value.contains(['\n', '\r']) {
        return Err(anyhow!("{label} state value must be a single line"));
    }
    Ok(Some(value.to_string()))
}
