use std::{
    ffi::OsString,
    fs::File,
    io::{Read, Write},
    os::unix::ffi::OsStringExt,
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::UNIX_EPOCH,
};

use anyhow::{anyhow, bail, Context, Result};
use rustix::{
    fs::{mkdirat, openat, renameat, unlinkat, AtFlags, Dir, Mode, OFlags},
    io::Errno,
};
use uuid::Uuid;

const DIR_MODE: Mode = Mode::from_raw_mode(0o755);
const FILE_MODE: Mode = Mode::from_raw_mode(0o644);

#[derive(Debug, Clone)]
pub struct ContinuityFs {
    root_path: PathBuf,
    root_dir: Arc<File>,
}

impl ContinuityFs {
    pub fn bootstrap(workspace_root: &Path) -> Result<Self> {
        Self::open_workspace_root(workspace_root, true)
    }

    pub fn open_existing(workspace_root: &Path) -> Result<Self> {
        Self::open_workspace_root(workspace_root, false)
    }

    fn open_workspace_root(workspace_root: &Path, create: bool) -> Result<Self> {
        let parent = workspace_root.parent().ok_or_else(|| {
            anyhow!(
                "workspace root '{}' has no parent",
                workspace_root.display()
            )
        })?;
        let parent_dir = open_directory_path(parent, create)?;
        let root_name = workspace_root.file_name().ok_or_else(|| {
            anyhow!(
                "workspace root '{}' has no file name",
                workspace_root.display()
            )
        })?;

        if create {
            match mkdirat(&parent_dir, root_name, DIR_MODE) {
                Ok(()) | Err(Errno::EXIST) => {}
                Err(err) => {
                    return Err(err)
                        .with_context(|| format!("failed to create {}", workspace_root.display()))
                }
            }
        }

        let root_fd = openat(
            &parent_dir,
            root_name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .with_context(|| format!("failed to open {}", workspace_root.display()))?;

        Ok(Self {
            root_path: workspace_root.to_path_buf(),
            root_dir: Arc::new(File::from(root_fd)),
        })
    }

    pub fn read_to_string_if_exists(&self, relative: &Path) -> Result<Option<String>> {
        match self.read_to_string(relative) {
            Ok(content) => Ok(Some(content)),
            Err(err) if is_not_found(&err) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub fn root_path(&self) -> &Path {
        &self.root_path
    }

    pub fn create_dir_all(&self, relative: &Path) -> Result<()> {
        self.open_dir(relative, true).map(|_| ())
    }

    pub fn ensure_file(&self, relative: &Path, content: &str) -> Result<()> {
        match self.read_to_string(relative) {
            Ok(_) => Ok(()),
            Err(err) if is_not_found(&err) => self.write_string(relative, content),
            Err(err) => Err(err),
        }
    }

    pub fn write_string(&self, relative: &Path, content: &str) -> Result<()> {
        let (parent, name) = self.open_parent_dir(relative, true)?;
        self.write_atomic_in_parent(&parent, &name, content.as_bytes())
    }

    pub fn append_string_with_header(
        &self,
        relative: &Path,
        header_if_new: Option<&str>,
        body: &str,
    ) -> Result<()> {
        let (parent, name) = self.open_parent_dir(relative, true)?;
        match self.open_regular_file(
            &parent,
            &name,
            OFlags::WRONLY | OFlags::APPEND | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        ) {
            Ok(mut file) => {
                file.write_all(body.as_bytes()).with_context(|| {
                    format!(
                        "failed to append {}",
                        self.absolute_path(relative).display()
                    )
                })?;
                file.flush().with_context(|| {
                    format!("failed to flush {}", self.absolute_path(relative).display())
                })?;
                Ok(())
            }
            Err(err) if matches!(err.downcast_ref::<Errno>(), Some(&Errno::NOENT)) => {
                let flags = OFlags::WRONLY
                    | OFlags::APPEND
                    | OFlags::CREATE
                    | OFlags::EXCL
                    | OFlags::CLOEXEC
                    | OFlags::NOFOLLOW;
                match self.open_regular_file(&parent, &name, flags) {
                    Ok(mut file) => {
                        if let Some(header) = header_if_new {
                            file.write_all(header.as_bytes()).with_context(|| {
                                format!(
                                    "failed to initialize {}",
                                    self.absolute_path(relative).display()
                                )
                            })?;
                        }
                        file.write_all(body.as_bytes()).with_context(|| {
                            format!(
                                "failed to append {}",
                                self.absolute_path(relative).display()
                            )
                        })?;
                        file.flush().with_context(|| {
                            format!("failed to flush {}", self.absolute_path(relative).display())
                        })?;
                        Ok(())
                    }
                    Err(create_err)
                        if matches!(create_err.downcast_ref::<Errno>(), Some(&Errno::EXIST)) =>
                    {
                        let mut file = self.open_regular_file(
                            &parent,
                            &name,
                            OFlags::WRONLY | OFlags::APPEND | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                        )?;
                        file.write_all(body.as_bytes()).with_context(|| {
                            format!(
                                "failed to append {}",
                                self.absolute_path(relative).display()
                            )
                        })?;
                        file.flush().with_context(|| {
                            format!("failed to flush {}", self.absolute_path(relative).display())
                        })?;
                        Ok(())
                    }
                    Err(create_err) => Err(create_err),
                }
            }
            Err(err) => Err(err),
        }
    }

    pub fn read_to_string(&self, relative: &Path) -> Result<String> {
        let mut file = self.open_relative_file(
            relative,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        )?;
        let mut content = String::new();
        file.read_to_string(&mut content).with_context(|| {
            format!("failed to read {}", self.absolute_path(relative).display())
        })?;
        Ok(content)
    }

    pub fn rename(&self, source: &Path, target: &Path) -> Result<()> {
        let (source_parent, source_name) = self.open_parent_dir(source, false)?;
        let (target_parent, target_name) = self.open_parent_dir(target, true)?;
        renameat(&source_parent, &source_name, &target_parent, &target_name).with_context(
            || {
                format!(
                    "failed to rename {} to {}",
                    self.absolute_path(source).display(),
                    self.absolute_path(target).display()
                )
            },
        )?;
        Ok(())
    }

    pub fn list_markdown_files(&self, relative_root: &Path) -> Result<Vec<PathBuf>> {
        let dir = self.open_dir(relative_root, false)?;
        let normalized_root = normalize_relative_path(relative_root)?;
        let mut files = Vec::new();
        self.collect_markdown_files(&dir, &normalized_root, &mut files)?;
        Ok(files)
    }

    pub fn modified_at_ms(&self, relative: &Path) -> Result<i64> {
        let file = self.open_relative_file(
            relative,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        )?;
        let modified = file
            .metadata()
            .with_context(|| format!("failed to stat {}", self.absolute_path(relative).display()))?
            .modified()
            .with_context(|| {
                format!(
                    "failed to read mtime for {}",
                    self.absolute_path(relative).display()
                )
            })?;
        let duration = modified.duration_since(UNIX_EPOCH).with_context(|| {
            format!(
                "mtime for {} is before unix epoch",
                self.absolute_path(relative).display()
            )
        })?;
        i64::try_from(duration.as_millis()).context("mtime is too large to fit in i64")
    }

    pub fn absolute_path(&self, relative: &Path) -> PathBuf {
        let relative = normalize_relative_path(relative).unwrap_or_else(|_| PathBuf::new());
        self.root_path.join(relative)
    }

    fn collect_markdown_files(
        &self,
        dir_file: &File,
        relative_root: &Path,
        files: &mut Vec<PathBuf>,
    ) -> Result<()> {
        let mut dir = Dir::read_from(dir_file).with_context(|| {
            format!(
                "failed to read {}",
                self.absolute_path(relative_root).display()
            )
        })?;
        while let Some(entry) = dir.next() {
            let entry = entry.with_context(|| {
                format!(
                    "failed to iterate {}",
                    self.absolute_path(relative_root).display()
                )
            })?;
            let name = os_string_from_dir_entry(&entry);
            if name.as_os_str() == "." || name.as_os_str() == ".." {
                continue;
            }

            let child_relative = if relative_root.as_os_str().is_empty() {
                PathBuf::from(&name)
            } else {
                relative_root.join(&name)
            };

            let file_type = entry.file_type();
            if file_type.is_symlink() {
                bail!(
                    "continuity path '{}' is a symlink",
                    self.absolute_path(&child_relative).display()
                );
            }
            if file_type.is_dir() {
                let child_dir = self.open_dir(&child_relative, false)?;
                self.collect_markdown_files(&child_dir, &child_relative, files)?;
                continue;
            }
            if !file_type.is_file() {
                bail!(
                    "continuity path '{}' is not a regular file",
                    self.absolute_path(&child_relative).display()
                );
            }
            if child_relative.extension().is_some_and(|ext| ext == "md") {
                files.push(child_relative);
            }
        }
        Ok(())
    }

    fn open_relative_file(&self, relative: &Path, flags: OFlags) -> Result<File> {
        let (parent, name) = self.open_parent_dir(relative, false)?;
        self.open_regular_file(&parent, &name, flags)
    }

    fn open_regular_file(&self, parent: &File, name: &OsString, flags: OFlags) -> Result<File> {
        let fd = openat(parent, name, flags, FILE_MODE).with_context(|| {
            format!(
                "failed to open {}",
                self.root_path.join(Path::new(name)).display()
            )
        })?;
        let file = File::from(fd);
        let metadata = file
            .metadata()
            .with_context(|| format!("failed to stat file '{}'", Path::new(name).display()))?;
        if !metadata.is_file() {
            bail!(
                "continuity path '{}' is not a regular file",
                Path::new(name).display()
            );
        }
        Ok(file)
    }

    fn open_parent_dir(&self, relative: &Path, create: bool) -> Result<(File, OsString)> {
        let relative = normalize_relative_path(relative)?;
        let name = relative
            .file_name()
            .map(OsString::from)
            .ok_or_else(|| anyhow!("continuity path '{}' has no file name", relative.display()))?;
        let parent = relative.parent().unwrap_or_else(|| Path::new(""));
        let parent_dir = self.open_dir(parent, create)?;
        Ok((parent_dir, name))
    }

    fn open_dir(&self, relative: &Path, create: bool) -> Result<File> {
        let relative = normalize_relative_path(relative)?;
        let mut current = self
            .root_dir
            .try_clone()
            .context("failed to clone continuity root fd")?;
        for component in relative.components() {
            let name = match component {
                Component::Normal(name) => name,
                Component::CurDir => continue,
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    bail!("continuity path '{}' is invalid", relative.display())
                }
            };
            if create {
                match mkdirat(&current, name, DIR_MODE) {
                    Ok(()) | Err(Errno::EXIST) => {}
                    Err(err) => {
                        return Err(err).with_context(|| {
                            format!(
                                "failed to create {}",
                                self.root_path.join(relative.clone()).display()
                            )
                        })
                    }
                }
            }
            let next = openat(
                &current,
                name,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .with_context(|| {
                format!(
                    "failed to open {}",
                    self.root_path.join(relative.clone()).display()
                )
            })?;
            current = File::from(next);
        }
        Ok(current)
    }

    fn write_atomic_in_parent(&self, parent: &File, name: &OsString, content: &[u8]) -> Result<()> {
        let temp_name = OsString::from(format!(".tmp-{}", Uuid::new_v4()));
        let temp_path = Path::new(&temp_name);
        let mut temp_file = match self.open_regular_file(
            parent,
            &temp_name,
            OFlags::WRONLY
                | OFlags::CREATE
                | OFlags::EXCL
                | OFlags::TRUNC
                | OFlags::CLOEXEC
                | OFlags::NOFOLLOW,
        ) {
            Ok(file) => file,
            Err(err) => {
                if matches!(err.downcast_ref::<Errno>(), Some(&Errno::EXIST)) {
                    return self.write_atomic_in_parent(parent, name, content);
                }
                return Err(err);
            }
        };

        let write_result = (|| -> Result<()> {
            temp_file
                .write_all(content)
                .with_context(|| format!("failed to write {}", Path::new(name).display()))?;
            temp_file
                .flush()
                .with_context(|| format!("failed to flush {}", Path::new(name).display()))?;
            renameat(parent, temp_path, parent, name).with_context(|| {
                format!(
                    "failed to rename {} to {}",
                    Path::new(&temp_name).display(),
                    Path::new(name).display()
                )
            })?;
            Ok(())
        })();

        if write_result.is_err() {
            let _ = unlinkat(parent, temp_path, AtFlags::empty());
        }
        write_result
    }
}

fn normalize_relative_path(path: &Path) -> Result<PathBuf> {
    if path.as_os_str().is_empty() {
        return Ok(PathBuf::new());
    }
    if path.is_absolute() {
        bail!("continuity path '{}' is invalid", path.display());
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => normalized.push(value),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!("continuity path '{}' is invalid", path.display())
            }
        }
    }
    Ok(normalized)
}

fn open_directory_path(path: &Path, create: bool) -> Result<File> {
    let mut current = if path.is_absolute() {
        File::open("/").context("failed to open filesystem root")?
    } else {
        File::open(".").context("failed to open current working directory")?
    };

    for component in path.components() {
        let name = match component {
            Component::RootDir | Component::CurDir => continue,
            Component::Normal(name) => name,
            Component::ParentDir | Component::Prefix(_) => {
                bail!("directory path '{}' is invalid", path.display())
            }
        };

        if create {
            match mkdirat(&current, name, DIR_MODE) {
                Ok(()) | Err(Errno::EXIST) => {}
                Err(err) => {
                    return Err(err).with_context(|| format!("failed to create {}", path.display()))
                }
            }
        }

        let next = openat(
            &current,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .with_context(|| format!("failed to open {}", path.display()))?;
        current = File::from(next);
    }

    Ok(current)
}

fn is_not_found(err: &anyhow::Error) -> bool {
    matches!(err.downcast_ref::<Errno>(), Some(&Errno::NOENT))
}

fn os_string_from_dir_entry(entry: &rustix::fs::DirEntry) -> OsString {
    OsString::from_vec(entry.file_name().to_bytes().to_vec())
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;
    use std::path::Path;

    use tempfile::tempdir;

    use super::ContinuityFs;

    #[test]
    fn bootstrap_rejects_symlinked_workspace_root() {
        let temp_dir = tempdir().expect("temp dir");
        let outside = temp_dir.path().join("outside");
        std::fs::create_dir_all(&outside).expect("create outside");
        let workspace = temp_dir.path().join("workspace");
        symlink(&outside, &workspace).expect("symlink workspace");

        let err = ContinuityFs::bootstrap(&workspace).expect_err("bootstrap should fail");
        assert!(err.to_string().contains("failed to open"));
    }

    #[test]
    fn bootstrap_rejects_symlinked_workspace_parent() {
        let temp_dir = tempdir().expect("temp dir");
        let outside_parent = temp_dir.path().join("outside-parent");
        std::fs::create_dir_all(&outside_parent).expect("create outside parent");
        let linked_parent = temp_dir.path().join("linked-parent");
        symlink(&outside_parent, &linked_parent).expect("symlink parent");
        let workspace = linked_parent.join("workspace");

        let err = ContinuityFs::bootstrap(&workspace).expect_err("bootstrap should fail");
        assert!(err.to_string().contains("failed to open"));
    }

    #[test]
    fn write_and_read_stay_inside_root() {
        let temp_dir = tempdir().expect("temp dir");
        let workspace = temp_dir.path().join("workspace");
        let fs = ContinuityFs::bootstrap(&workspace).expect("bootstrap");
        fs.create_dir_all(Path::new("continuity/open-loops"))
            .expect("create dirs");
        fs.write_string(Path::new("continuity/open-loops/test.md"), "# Test\n")
            .expect("write file");
        let content = fs
            .read_to_string(Path::new("continuity/open-loops/test.md"))
            .expect("read file");
        assert_eq!(content, "# Test\n");
    }

    #[test]
    fn write_rejects_symlinked_subtree() {
        let temp_dir = tempdir().expect("temp dir");
        let workspace = temp_dir.path().join("workspace");
        let fs = ContinuityFs::bootstrap(&workspace).expect("bootstrap");
        fs.create_dir_all(Path::new("continuity"))
            .expect("continuity dir");

        let outside = temp_dir.path().join("outside");
        std::fs::create_dir_all(&outside).expect("outside dir");
        std::fs::remove_dir(workspace.join("continuity")).expect("remove continuity");
        symlink(&outside, workspace.join("continuity")).expect("symlink continuity");

        let err = fs
            .write_string(Path::new("continuity/ACTIVE.md"), "content\n")
            .expect_err("write should fail");
        assert!(err.to_string().contains("failed to open"));
    }
}
