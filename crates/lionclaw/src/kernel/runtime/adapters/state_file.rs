use std::{
    ffi::OsString,
    io::{Read, Write},
    path::{Component, Path},
};

use anyhow::{anyhow, Context, Result};
use rustix::{
    fs::{open, openat, renameat, unlinkat, AtFlags, Mode, OFlags},
    io::Errno,
};
use tracing::warn;
use uuid::Uuid;

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
    let root = ensure_state_root(runtime_state_root)?;
    validate_existing_state_file(&root, runtime_state_root, file_name, &target_name, label)?;

    let temp_name = OsString::from(format!(".{file_name}.{}.tmp", Uuid::new_v4().simple()));
    let temp = openat(
        &root,
        &temp_name,
        OFlags::WRONLY
            | OFlags::CREATE
            | OFlags::EXCL
            | OFlags::TRUNC
            | OFlags::CLOEXEC
            | OFlags::NOFOLLOW,
        Mode::from_raw_mode(0o600),
    )
    .with_context(|| {
        format!(
            "failed to create {label} state temp file '{}' in '{}'",
            Path::new(&temp_name).display(),
            runtime_state_root.display()
        )
    })?;
    let mut temp = std::fs::File::from(temp);

    let write_result = (|| -> Result<()> {
        temp.write_all(value.as_bytes()).with_context(|| {
            format!(
                "failed to write {label} state file '{}' in '{}'",
                file_name,
                runtime_state_root.display()
            )
        })?;
        temp.write_all(b"\n").with_context(|| {
            format!(
                "failed to write {label} state file '{}' in '{}'",
                file_name,
                runtime_state_root.display()
            )
        })?;
        temp.sync_all().with_context(|| {
            format!(
                "failed to sync {label} state file '{}' in '{}'",
                file_name,
                runtime_state_root.display()
            )
        })?;
        renameat(&root, &temp_name, &root, &target_name).with_context(|| {
            format!(
                "failed to publish {label} state file '{}' in '{}'",
                file_name,
                runtime_state_root.display()
            )
        })?;
        Ok(())
    })();

    if write_result.is_err() {
        match unlinkat(&root, &temp_name, AtFlags::empty()) {
            Ok(()) | Err(Errno::NOENT) => {}
            Err(err) => warn!(
                ?err,
                path = %runtime_state_root.join(Path::new(&temp_name)).display(),
                "failed to remove temporary adapter state file"
            ),
        }
    }
    write_result
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

fn ensure_state_root(runtime_state_root: &Path) -> Result<std::fs::File> {
    std::fs::create_dir_all(runtime_state_root)
        .with_context(|| format!("failed to create '{}'", runtime_state_root.display()))?;
    open_state_root(runtime_state_root)
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
