use std::ffi::OsStr;
use std::io::{Read, Write};
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};

use anyhow::{bail, Context, Result};
use rustix::fd::AsFd;
use rustix::fs::{open, openat, Dir, FileType, Mode, OFlags};

use super::digest::ContentDigest;

pub(crate) const MAX_BUNDLE_ENTRIES: usize = 4_096;
pub(crate) const MAX_BUNDLE_DEPTH: usize = 32;
pub(crate) const MAX_BUNDLE_BYTES: u64 = 256 * 1024 * 1024;
pub(crate) const MAX_CONTROL_FILE_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_CONTROL_TEXT_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TreeEntryKind {
    Directory,
    File,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TreeEntry {
    pub relative: PathBuf,
    pub kind: TreeEntryKind,
    identity: FileIdentity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
    mode: u32,
    length: i64,
    modified_seconds: i64,
    modified_nanoseconds: u64,
    changed_seconds: i64,
    changed_nanoseconds: u64,
}

impl FileIdentity {
    fn from_stat(stat: &rustix::fs::Stat) -> Self {
        Self {
            device: stat.st_dev,
            inode: stat.st_ino,
            mode: stat.st_mode,
            length: stat.st_size,
            modified_seconds: stat.st_mtime,
            modified_nanoseconds: stat.st_mtime_nsec,
            changed_seconds: stat.st_ctime,
            changed_nanoseconds: stat.st_ctime_nsec,
        }
    }
}

/// One finite, descriptor-rooted view of a closed mission-content tree.
/// Inventory, reads, digests, and copies all resolve beneath the same root
/// descriptor, so a concurrent rename can fail admission but cannot redirect
/// LionClaw through a symlink or special file.
pub(crate) struct BoundedTree {
    root: PathBuf,
    root_fd: OwnedFd,
    root_identity: FileIdentity,
    entries: Vec<TreeEntry>,
}

impl BoundedTree {
    pub(crate) fn open(root: &Path) -> Result<Self> {
        let root_fd = open(
            root,
            OFlags::RDONLY
                | OFlags::DIRECTORY
                | OFlags::CLOEXEC
                | OFlags::NOFOLLOW
                | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(|error| open_error(root, error))?;
        let root_identity = FileIdentity::from_stat(&rustix::fs::fstat(&root_fd)?);
        let entries = inventory(&root_fd, root, root_identity)?;
        Ok(Self {
            root: root.to_path_buf(),
            root_fd,
            root_identity,
            entries,
        })
    }

    pub(crate) fn entries(&self) -> &[TreeEntry] {
        &self.entries
    }

    pub(crate) fn contains(&self, relative: &Path, kind: TreeEntryKind) -> bool {
        self.entries
            .binary_search_by(|entry| entry.relative.as_path().cmp(relative))
            .ok()
            .is_some_and(|index| self.entries[index].kind == kind)
    }

    pub(crate) fn open_regular(&self, relative: &Path) -> Result<std::fs::File> {
        let descriptor = self.open_relative(
            relative,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        )?;
        let stat = rustix::fs::fstat(&descriptor)
            .with_context(|| format!("inspecting '{}'", self.root.join(relative).display()))?;
        if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile {
            bail!(
                "bundle entry '{}' is not a regular file",
                self.root.join(relative).display()
            );
        }
        Ok(std::fs::File::from(descriptor))
    }

    fn verify_file(&self, relative: &Path, file: &std::fs::File) -> Result<()> {
        let expected = self.entry(relative)?;
        let actual = FileIdentity::from_stat(&rustix::fs::fstat(file)?);
        if expected.kind != TreeEntryKind::File || actual != expected.identity {
            bail!(
                "bundle entry '{}' changed while it was being read",
                self.root.join(relative).display()
            );
        }
        Ok(())
    }

    pub(crate) fn copy_to(&self, destination: &Path) -> Result<()> {
        std::fs::create_dir(destination)
            .with_context(|| format!("creating '{}'", destination.display()))?;
        for entry in &self.entries {
            let target = destination.join(&entry.relative);
            match entry.kind {
                TreeEntryKind::Directory => std::fs::create_dir(&target)
                    .with_context(|| format!("creating '{}'", target.display()))?,
                TreeEntryKind::File => {
                    let mut source = self.open_regular(&entry.relative)?;
                    let metadata = source.metadata().with_context(|| {
                        format!("reading source metadata '{}'", entry.relative.display())
                    })?;
                    let expected = metadata.len();
                    let mut target_file = std::fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&target)
                        .with_context(|| format!("creating '{}'", target.display()))?;
                    let copied = std::io::copy(
                        &mut Read::by_ref(&mut source).take(expected.saturating_add(1)),
                        &mut target_file,
                    )
                    .with_context(|| format!("copying '{}'", entry.relative.display()))?;
                    if copied != expected {
                        bail!(
                            "bundle entry '{}' changed while it was copied",
                            self.root.join(&entry.relative).display()
                        );
                    }
                    self.verify_file(&entry.relative, &source)?;
                    target_file.flush()?;
                    std::fs::set_permissions(
                        &target,
                        std::fs::Permissions::from_mode(metadata.permissions().mode()),
                    )?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn feed_digest(
        &self,
        digest: &mut ContentDigest,
        logical_prefix: &str,
    ) -> Result<()> {
        for entry in &self.entries {
            let relative = entry.relative.as_os_str().as_bytes();
            let logical = if logical_prefix.is_empty() {
                relative.to_vec()
            } else {
                let mut logical = Vec::with_capacity(logical_prefix.len() + relative.len() + 1);
                logical.extend_from_slice(logical_prefix.as_bytes());
                logical.push(b'/');
                logical.extend_from_slice(relative);
                logical
            };
            match entry.kind {
                TreeEntryKind::Directory => {
                    let mut directory = logical;
                    directory.push(b'/');
                    digest.feed_bytes(&directory, b"directory", false)
                }
                TreeEntryKind::File => {
                    let mut file = self.open_regular(&entry.relative)?;
                    let metadata = file.metadata()?;
                    digest
                        .feed_opened_file(
                            &logical,
                            &mut file,
                            &metadata,
                            metadata.permissions().mode() & 0o111 != 0,
                        )
                        .with_context(|| {
                            format!("hashing '{}'", self.root.join(&entry.relative).display())
                        })?;
                    self.verify_file(&entry.relative, &file)?;
                }
            }
        }
        Ok(())
    }

    fn open_relative(&self, relative: &Path, final_flags: OFlags) -> Result<OwnedFd> {
        let components = relative.components().collect::<Vec<_>>();
        if components.is_empty()
            || components
                .iter()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            bail!(
                "bundle path '{}' is not a safe relative path",
                relative.display()
            );
        }

        if FileIdentity::from_stat(&rustix::fs::fstat(&self.root_fd)?) != self.root_identity {
            bail!(
                "bundle root '{}' changed after admission",
                self.root.display()
            );
        }

        let mut current: Option<OwnedFd> = None;
        let mut partial = PathBuf::new();
        for (index, component) in components.iter().enumerate() {
            let Component::Normal(name) = component else {
                unreachable!("components were validated")
            };
            let parent = current
                .as_ref()
                .map_or_else(|| self.root_fd.as_fd(), AsFd::as_fd);
            let flags = if index + 1 == components.len() {
                final_flags
            } else {
                OFlags::RDONLY
                    | OFlags::DIRECTORY
                    | OFlags::CLOEXEC
                    | OFlags::NOFOLLOW
                    | OFlags::NONBLOCK
            };
            partial.push(name);
            current = Some(
                openat(parent, *name, flags, Mode::empty())
                    .map_err(|error| open_error(&self.root.join(&partial), error))?,
            );
            let expected = self.entry(&partial)?;
            let actual = FileIdentity::from_stat(&rustix::fs::fstat(
                current.as_ref().expect("descriptor was opened"),
            )?);
            if actual != expected.identity {
                bail!(
                    "bundle entry '{}' changed after admission",
                    self.root.join(&partial).display()
                );
            }
        }
        Ok(current.expect("validated path has a component"))
    }

    fn entry(&self, relative: &Path) -> Result<&TreeEntry> {
        let index = self
            .entries
            .binary_search_by(|entry| entry.relative.as_path().cmp(relative))
            .map_err(|_| {
                anyhow::anyhow!("bundle entry '{}' was not admitted", relative.display())
            })?;
        Ok(&self.entries[index])
    }
}

fn inventory(
    root_fd: &OwnedFd,
    root: &Path,
    root_identity: FileIdentity,
) -> Result<Vec<TreeEntry>> {
    let mut pending = vec![(
        root_fd.as_fd().try_clone_to_owned()?,
        PathBuf::new(),
        0,
        root_identity,
    )];
    let mut result = Vec::new();
    let mut admitted_bytes = 0_u64;
    while let Some((directory_fd, relative_dir, depth, directory_identity)) = pending.pop() {
        let mut names = Vec::new();
        for entry in Dir::read_from(&directory_fd)? {
            let entry = entry?;
            let bytes = entry.file_name().to_bytes();
            if matches!(bytes, b"." | b".." | b".git") {
                continue;
            }
            if result.len() + names.len() == MAX_BUNDLE_ENTRIES {
                bail!(
                    "bundle entry limit of {MAX_BUNDLE_ENTRIES} exceeded in '{}'",
                    root.join(&relative_dir).display()
                );
            }
            names.push(OsStr::from_bytes(bytes).to_os_string());
        }
        names.sort();

        for name in names {
            let relative = relative_dir.join(&name);
            let child_depth = depth + 1;
            if child_depth > MAX_BUNDLE_DEPTH {
                bail!(
                    "bundle depth limit of {MAX_BUNDLE_DEPTH} exceeded at '{}'",
                    relative.display()
                );
            }
            let descriptor = openat(
                &directory_fd,
                &name,
                OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
                Mode::empty(),
            )
            .map_err(|error| open_error(&root.join(&relative), error))?;
            let stat = rustix::fs::fstat(&descriptor)?;
            let kind = match FileType::from_raw_mode(stat.st_mode) {
                FileType::Directory => {
                    pending.push((
                        descriptor,
                        relative.clone(),
                        child_depth,
                        FileIdentity::from_stat(&stat),
                    ));
                    TreeEntryKind::Directory
                }
                FileType::RegularFile => {
                    let length = u64::try_from(stat.st_size).map_err(|_| {
                        anyhow::anyhow!(
                            "bundle entry '{}' reports a negative length",
                            root.join(&relative).display()
                        )
                    })?;
                    admitted_bytes = admitted_bytes.checked_add(length).ok_or_else(|| {
                        anyhow::anyhow!("bundle byte total overflowed at '{}'", relative.display())
                    })?;
                    if admitted_bytes > MAX_BUNDLE_BYTES {
                        bail!(
                            "bundle byte limit of {MAX_BUNDLE_BYTES} exceeded by '{}'",
                            root.join(&relative).display()
                        );
                    }
                    TreeEntryKind::File
                }
                _ => bail!(
                    "bundle entry '{}' must be a regular file or directory",
                    root.join(&relative).display()
                ),
            };
            result.push(TreeEntry {
                relative,
                kind,
                identity: FileIdentity::from_stat(&stat),
            });
        }
        if FileIdentity::from_stat(&rustix::fs::fstat(&directory_fd)?) != directory_identity {
            bail!(
                "bundle directory '{}' changed during admission",
                root.join(&relative_dir).display()
            );
        }
    }
    result.sort_by(|left, right| left.relative.cmp(&right.relative));
    Ok(result)
}

fn open_error(path: &Path, error: rustix::io::Errno) -> anyhow::Error {
    if error == rustix::io::Errno::LOOP {
        anyhow::anyhow!(
            "bundle entry '{}' is a symlink or changed to one during admission",
            path.display()
        )
    } else {
        anyhow::anyhow!("opening bundle entry '{}': {error}", path.display())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ControlTextBudget {
    remaining: usize,
}

impl Default for ControlTextBudget {
    fn default() -> Self {
        Self {
            remaining: MAX_CONTROL_TEXT_BYTES,
        }
    }
}

impl ControlTextBudget {
    pub(crate) fn read(&mut self, tree: &BoundedTree, relative: &Path) -> Result<String> {
        let display = tree.root.join(relative);
        let mut file = tree.open_regular(relative)?;
        let metadata = file
            .metadata()
            .with_context(|| format!("reading control text metadata '{}'", display.display()))?;
        if metadata.len() > MAX_CONTROL_FILE_BYTES as u64 {
            bail!(
                "control text '{}' exceeds the per-file text limit of {MAX_CONTROL_FILE_BYTES} bytes",
                display.display()
            );
        }

        let mut bytes = Vec::with_capacity((metadata.len() as usize).min(MAX_CONTROL_FILE_BYTES));
        Read::by_ref(&mut file)
            .take((MAX_CONTROL_FILE_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .with_context(|| format!("reading control text '{}'", display.display()))?;
        if bytes.len() > MAX_CONTROL_FILE_BYTES {
            bail!(
                "control text '{}' exceeds the per-file text limit of {MAX_CONTROL_FILE_BYTES} bytes",
                display.display()
            );
        }
        tree.verify_file(relative, &file)?;
        if bytes.len() > self.remaining {
            bail!(
                "control text aggregate limit of {MAX_CONTROL_TEXT_BYTES} bytes exceeded by '{}'",
                display.display()
            );
        }
        self.remaining -= bytes.len();
        String::from_utf8(bytes)
            .with_context(|| format!("control text '{}' is not UTF-8", display.display()))
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::ffi::OsStringExt;
    use std::os::unix::fs::symlink;
    use std::time::Duration;

    use super::*;

    fn digest(tree: &BoundedTree) -> String {
        let mut digest = ContentDigest::new();
        tree.feed_digest(&mut digest, "").unwrap();
        digest.finish()
    }

    #[test]
    fn admitted_tree_rejects_a_replaced_real_directory() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("bundle");
        std::fs::create_dir_all(root.join("roles")).unwrap();
        std::fs::write(root.join("roles/worker.md"), "admitted").unwrap();
        let tree = BoundedTree::open(&root).unwrap();

        std::fs::rename(root.join("roles"), root.join("old-roles")).unwrap();
        std::fs::create_dir(root.join("roles")).unwrap();
        std::fs::write(root.join("roles/worker.md"), "replacement").unwrap();

        let error = tree
            .open_regular(Path::new("roles/worker.md"))
            .expect_err("a different directory must not satisfy an admitted path");
        assert!(error.to_string().contains("changed"), "got {error:#}");
    }

    #[test]
    fn admitted_tree_rejects_leaf_indirection_without_blocking() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("bundle");
        std::fs::create_dir_all(root.join("roles")).unwrap();
        let file = root.join("roles/worker.md");
        std::fs::write(&file, "admitted").unwrap();
        let tree = BoundedTree::open(&root).unwrap();

        std::fs::remove_file(&file).unwrap();
        symlink("/etc/passwd", &file).unwrap();
        tree.open_regular(Path::new("roles/worker.md"))
            .expect_err("a symlink must not satisfy an admitted file");

        std::fs::remove_file(&file).unwrap();
        rustix::fs::mkfifoat(rustix::fs::CWD, &file, Mode::RUSR | Mode::WUSR).unwrap();
        let (finished_tx, finished_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let rejected = tree.open_regular(Path::new("roles/worker.md")).is_err();
            finished_tx.send(rejected).unwrap();
        });
        assert!(
            finished_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            "a FIFO must be rejected without blocking"
        );
        worker.join().unwrap();
    }

    #[test]
    fn tree_digest_distinguishes_raw_path_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let raw_root = temp.path().join("raw");
        let utf8_root = temp.path().join("utf8");
        std::fs::create_dir(&raw_root).unwrap();
        std::fs::create_dir(&utf8_root).unwrap();
        std::fs::write(
            raw_root.join(std::ffi::OsString::from_vec(vec![0x80])),
            "same",
        )
        .unwrap();
        std::fs::write(utf8_root.join("\u{fffd}"), "same").unwrap();

        assert_ne!(
            digest(&BoundedTree::open(&raw_root).unwrap()),
            digest(&BoundedTree::open(&utf8_root).unwrap())
        );
    }
}
