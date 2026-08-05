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
    collections::BTreeSet,
    ffi::{OsStr, OsString},
    fs::{File, Metadata, Permissions},
    io::{Read, Write},
    os::unix::ffi::OsStringExt,
    path::{Component, Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use rustix::{
    fd::{AsFd, BorrowedFd, OwnedFd},
    fs::{fchmod, fstat, openat, renameat, unlinkat, AtFlags, Dir, FileType, Mode, OFlags},
    io::Errno,
};
use tracing::warn;
use uuid::Uuid;

const TEMP_CREATE_ATTEMPTS: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundedRead {
    Missing,
    Contents(Vec<u8>),
    TooLarge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataTreeLimits {
    pub max_bytes: u64,
    pub max_entries: usize,
    pub max_depth: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetadataTreeUsage {
    pub bytes: u64,
    pub entries: usize,
    pub max_depth: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataTreeLimit {
    Bytes,
    Entries,
    Depth,
}

impl std::fmt::Display for MetadataTreeLimit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Bytes => "byte",
            Self::Entries => "entry",
            Self::Depth => "depth",
        };
        formatter.write_str(name)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataTreeLimitExceeded {
    pub limit: MetadataTreeLimit,
    pub observed: u64,
    pub maximum: u64,
}

impl std::fmt::Display for MetadataTreeLimitExceeded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "metadata tree exceeds the {} limit: observed {}, maximum {}",
            self.limit, self.observed, self.maximum
        )
    }
}

impl std::error::Error for MetadataTreeLimitExceeded {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImmediateDirectoryLimitExceeded {
    pub observed_at_least: usize,
    pub maximum: usize,
}

impl std::fmt::Display for ImmediateDirectoryLimitExceeded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "immediate-directory limit exceeded: observed at least {}, maximum {}",
            self.observed_at_least, self.maximum
        )
    }
}

impl std::error::Error for ImmediateDirectoryLimitExceeded {}

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
        match self.read_bounded_status(file_name, limit, label)? {
            BoundedRead::Missing => Ok(None),
            BoundedRead::Contents(bytes) => Ok(Some(bytes)),
            BoundedRead::TooLarge => Err(file_too_large(&self.path, file_name, label, limit)),
        }
    }

    /// Read one regular file and its metadata without changing the file.
    pub fn read_bounded_with_metadata(
        &self,
        file_name: &OsStr,
        limit: usize,
        label: &str,
    ) -> Result<Option<(Vec<u8>, Metadata)>> {
        ensure_file_name(file_name, label)?;
        let Some(parent) = self.open_existing()? else {
            return Ok(None);
        };
        let Some(file) = open_regular_file(&parent, &self.path, file_name, label)? else {
            return Ok(None);
        };
        let (contents, metadata) =
            read_open_file_bounded_with_metadata(file, &self.path, file_name, limit, label)?;
        match contents {
            BoundedRead::Contents(contents) => Ok(Some((contents, metadata))),
            BoundedRead::TooLarge => Err(file_too_large(&self.path, file_name, label, limit)),
            BoundedRead::Missing => unreachable!("an open file cannot become missing"),
        }
    }

    /// Read one owner-only regular file and return metadata from the same
    /// descriptor used for the bounded read.
    pub fn read_private_bounded_with_metadata(
        &self,
        file_name: &OsStr,
        limit: usize,
        label: &str,
    ) -> Result<Option<(Vec<u8>, Metadata)>> {
        ensure_file_name(file_name, label)?;
        let Some(parent) = self.open_existing()? else {
            return Ok(None);
        };
        let Some(file) = open_regular_file(&parent, &self.path, file_name, label)? else {
            return Ok(None);
        };
        harden_private_file(&file, &self.path, file_name, label)?;
        let (contents, metadata) =
            read_open_file_bounded_with_metadata(file, &self.path, file_name, limit, label)?;
        match contents {
            BoundedRead::Contents(contents) => Ok(Some((contents, metadata))),
            BoundedRead::TooLarge => Err(file_too_large(&self.path, file_name, label, limit)),
            BoundedRead::Missing => unreachable!("an open file cannot become missing"),
        }
    }

    /// Open or create an owner-only regular file for a caller-owned advisory
    /// lock. The returned descriptor, not its pathname, is the lock identity.
    pub fn open_private_lock_file(&self, file_name: &OsStr, label: &str) -> Result<File> {
        ensure_file_name(file_name, label)?;
        let parent = self.open_required()?;
        let file = match openat(
            &parent,
            file_name,
            OFlags::RDWR | OFlags::CREATE | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
            Mode::from_raw_mode(0o600),
        ) {
            Ok(file) => File::from(file),
            Err(Errno::LOOP) => {
                return Err(anyhow!(
                    "{label} '{}' cannot be a symlink",
                    self.path.join(file_name).display()
                ))
            }
            Err(error) => {
                return Err(anyhow!(
                    "failed to open {label} '{}': {error}",
                    self.path.join(file_name).display()
                ))
            }
        };
        ensure_regular_file(&file, &self.path, file_name, label)?;
        harden_private_file(&file, &self.path, file_name, label)?;
        Ok(file)
    }

    /// Read a regular file while distinguishing absent and oversized content.
    /// Authority violations such as symlinks and non-regular files remain
    /// errors; callers may safely treat only `TooLarge` as corrupt content.
    pub fn read_bounded_status(
        &self,
        file_name: &OsStr,
        limit: usize,
        label: &str,
    ) -> Result<BoundedRead> {
        ensure_file_name(file_name, label)?;
        let Some(parent) = self.open_existing()? else {
            return Ok(BoundedRead::Missing);
        };
        let Some(file) = open_regular_file(&parent, &self.path, file_name, label)? else {
            return Ok(BoundedRead::Missing);
        };
        read_open_file_bounded(file, &self.path, file_name, limit, label)
    }

    /// Read and durably remove one bounded regular file through the same
    /// descriptor-rooted directory authority.
    ///
    /// Concurrent takers are serialized by the unlink result: a loser observes
    /// `Missing` even if it opened the old file first. Writers remain governed
    /// by the caller's higher-level single-writer authority.
    pub fn take_bounded_status(
        &self,
        file_name: &OsStr,
        limit: usize,
        label: &str,
    ) -> Result<BoundedRead> {
        ensure_file_name(file_name, label)?;
        let Some(parent) = self.open_existing()? else {
            return Ok(BoundedRead::Missing);
        };
        let Some(file) = open_regular_file(&parent, &self.path, file_name, label)? else {
            return Ok(BoundedRead::Missing);
        };
        let contents = read_open_file_bounded(file, &self.path, file_name, limit, label)?;
        if remove_file_if_exists(&parent, &self.path, file_name, label)? {
            Ok(contents)
        } else {
            Ok(BoundedRead::Missing)
        }
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

    /// Account for a directory tree without following links or opening file
    /// contents.
    ///
    /// File bytes are logical regular-file lengths. Entries count every
    /// regular file and directory below the root; the root itself is depth
    /// zero and is not an entry.
    ///
    /// The caller must exclude writers for the duration of this operation when
    /// it needs a stable accounting point. Descriptor identity checks reject
    /// directory path replacement observed during traversal; they cannot make
    /// concurrent entry addition, removal, or file growth into an atomic
    /// snapshot.
    pub fn account_metadata(
        &self,
        limits: MetadataTreeLimits,
        label: &str,
    ) -> Result<MetadataTreeUsage> {
        let Some(root) = self.open_existing()? else {
            return Ok(MetadataTreeUsage::default());
        };
        let mut stack = vec![MetadataFrame::root(root, self.path.clone(), label)?];
        let mut usage = MetadataTreeUsage::default();

        while let Some(frame) = stack.last_mut() {
            let entry = frame.entries.next().transpose().with_context(|| {
                format!(
                    "failed to enumerate {label} directory '{}'",
                    frame.display.display()
                )
            })?;
            let Some(entry) = entry else {
                revalidate_completed_frame(&stack, self, label)?;
                stack.truncate(stack.len() - 1);
                continue;
            };
            let name_bytes = entry.file_name().to_bytes();
            if matches!(name_bytes, b"." | b"..") {
                continue;
            }
            let name = OsString::from_vec(name_bytes.to_vec());

            usage.entries = usage.entries.checked_add(1).ok_or_else(|| {
                metadata_limit_error(MetadataTreeLimit::Entries, u64::MAX, limits.max_entries)
            })?;
            if usage.entries > limits.max_entries {
                return Err(metadata_limit_error(
                    MetadataTreeLimit::Entries,
                    usize_as_u64(usage.entries),
                    limits.max_entries,
                ));
            }

            let Some(frame) = stack.last() else {
                return Err(anyhow!(
                    "{label} metadata traversal lost its open directory authority"
                ));
            };
            let entry_depth = frame.depth.checked_add(1).ok_or_else(|| {
                metadata_limit_error(MetadataTreeLimit::Depth, u64::MAX, limits.max_depth)
            })?;
            if entry_depth > limits.max_depth {
                return Err(metadata_limit_error(
                    MetadataTreeLimit::Depth,
                    usize_as_u64(entry_depth),
                    limits.max_depth,
                ));
            }
            usage.max_depth = usage.max_depth.max(entry_depth);
            let display = frame.display.join(&name);
            let parent = frame.directory_fd(label)?;

            match inspect_metadata_entry(parent, &name, &display, label)? {
                AccountedEntry::Leaf(bytes) => {
                    usage.bytes = usage.bytes.checked_add(bytes).ok_or_else(|| {
                        anyhow!(MetadataTreeLimitExceeded {
                            limit: MetadataTreeLimit::Bytes,
                            observed: u64::MAX,
                            maximum: limits.max_bytes,
                        })
                    })?;
                    if usage.bytes > limits.max_bytes {
                        return Err(anyhow!(MetadataTreeLimitExceeded {
                            limit: MetadataTreeLimit::Bytes,
                            observed: usage.bytes,
                            maximum: limits.max_bytes,
                        }));
                    }
                }
                AccountedEntry::Directory(directory) => {
                    stack.push(MetadataFrame::child(
                        name,
                        directory,
                        display,
                        entry_depth,
                        label,
                    )?);
                }
            }
        }

        Ok(usage)
    }

    /// Return the names of immediate child directories under this exact root.
    ///
    /// Every child must be a real directory. The bounded result is suitable
    /// for validating a host-owned namespace such as retained runtime
    /// profiles without reading anything stored within those directories. The
    /// caller must exclude writers when it needs a stable namespace count.
    pub fn immediate_directory_names(
        &self,
        max_entries: usize,
        label: &str,
    ) -> Result<BTreeSet<OsString>> {
        let Some(root) = self.open_existing()? else {
            return Ok(BTreeSet::new());
        };
        let mut entries = Dir::new(root).with_context(|| {
            format!(
                "failed to enumerate {label} directory '{}'",
                self.path.display()
            )
        })?;
        let mut names = BTreeSet::new();
        let mut opened = Vec::new();

        while let Some(entry) = entries.next() {
            let entry = entry.with_context(|| {
                format!(
                    "failed to enumerate {label} directory '{}'",
                    self.path.display()
                )
            })?;
            let name_bytes = entry.file_name().to_bytes();
            if matches!(name_bytes, b"." | b"..") {
                continue;
            }
            let name = OsString::from_vec(name_bytes.to_vec());
            if names.len() >= max_entries {
                return Err(anyhow!(ImmediateDirectoryLimitExceeded {
                    observed_at_least: max_entries.saturating_add(1),
                    maximum: max_entries,
                })
                .context(format!(
                    "{label} '{}' exceeds the immediate-directory limit",
                    self.path.display()
                )));
            }
            let display = self.path.join(&name);
            let directory = open_directory_entry(
                entries.fd().map_err(|error| {
                    anyhow!(
                        "failed to retain {label} directory authority '{}': {error}",
                        self.path.display()
                    )
                })?,
                &name,
                &display,
                label,
            )?;
            let _inserted = names.insert(name.clone());
            opened.push((name, directory));
        }

        for (name, directory) in opened {
            revalidate_named_directory(
                entries.fd().map_err(|error| {
                    anyhow!(
                        "failed to retain {label} directory authority '{}': {error}",
                        self.path.display()
                    )
                })?,
                &name,
                &directory,
                &self.path.join(&name),
                label,
            )?;
        }
        self.revalidate_open_directory(entries.fd().map_err(|error| {
            anyhow!(
                "failed to retain {label} directory authority '{}': {error}",
                self.path.display()
            )
        })?)?;
        Ok(names)
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

    fn revalidate_open_directory(&self, opened: impl AsFd) -> Result<()> {
        let Some(current) = self.open_existing()? else {
            return Err(anyhow!(
                "durable directory '{}' changed during metadata accounting",
                self.path.display()
            ));
        };
        ensure_same_directory(opened, &current, &self.path)
    }
}

#[derive(Debug)]
enum AccountedEntry {
    Leaf(u64),
    Directory(File),
}

struct MetadataFrame {
    name: Option<OsString>,
    display: PathBuf,
    depth: usize,
    entries: Dir,
}

impl MetadataFrame {
    fn root(directory: File, display: PathBuf, label: &str) -> Result<Self> {
        let entries = Dir::new(directory).with_context(|| {
            format!(
                "failed to enumerate {label} directory '{}'",
                display.display()
            )
        })?;
        Ok(Self {
            name: None,
            display,
            depth: 0,
            entries,
        })
    }

    fn child(
        name: OsString,
        directory: File,
        display: PathBuf,
        depth: usize,
        label: &str,
    ) -> Result<Self> {
        let entries = Dir::new(directory).with_context(|| {
            format!(
                "failed to enumerate {label} directory '{}'",
                display.display()
            )
        })?;
        Ok(Self {
            name: Some(name),
            display,
            depth,
            entries,
        })
    }

    fn directory_fd(&self, label: &str) -> Result<BorrowedFd<'_>> {
        self.entries.fd().map_err(|error| {
            anyhow!(
                "failed to retain {label} directory authority '{}': {error}",
                self.display.display()
            )
        })
    }
}

fn revalidate_completed_frame(
    frames: &[MetadataFrame],
    root: &RootedDirectory,
    label: &str,
) -> Result<()> {
    let Some((completed, parents)) = frames.split_last() else {
        return Err(anyhow!(
            "{label} metadata traversal lost its open directory authority"
        ));
    };
    let completed_fd = completed.directory_fd(label)?;
    let Some(parent) = parents.last() else {
        return root.revalidate_open_directory(completed_fd);
    };
    let Some(name) = completed.name.as_deref() else {
        return Err(anyhow!(
            "{label} metadata traversal lost its child directory identity"
        ));
    };
    revalidate_named_directory(
        parent.directory_fd(label)?,
        name,
        completed_fd,
        &completed.display,
        label,
    )
}

fn inspect_metadata_entry(
    parent: BorrowedFd<'_>,
    name: &OsStr,
    display: &Path,
    label: &str,
) -> Result<AccountedEntry> {
    let authority = open_metadata_entry(parent, name, display, label)?;
    let metadata = fstat(&authority)
        .with_context(|| format!("failed to inspect {label} entry '{}'", display.display()))?;
    let file_type = FileType::from_raw_mode(metadata.st_mode);
    if file_type.is_dir() {
        return readable_directory_from_authority(&authority, display, label)
            .map(AccountedEntry::Directory);
    }
    if file_type.is_file() || file_type.is_symlink() {
        let bytes = u64::try_from(metadata.st_size)
            .map_err(|_| anyhow!("{label} entry '{}' has an invalid size", display.display()))?;
        return Ok(AccountedEntry::Leaf(bytes));
    }
    Err(unsafe_metadata_entry(label, display))
}

fn open_metadata_entry(
    parent: impl AsFd,
    name: &OsStr,
    display: &Path,
    label: &str,
) -> Result<OwnedFd> {
    openat(
        parent,
        name,
        OFlags::PATH | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| match error {
        Errno::NOENT => anyhow!(
            "{label} entry '{}' changed during metadata accounting",
            display.display()
        ),
        _ => anyhow!(
            "failed to open {label} metadata entry '{}': {error}",
            display.display()
        ),
    })
}

fn open_directory_entry(
    parent: impl AsFd,
    name: &OsStr,
    display: &Path,
    label: &str,
) -> Result<File> {
    let authority = open_metadata_entry(parent, name, display, label)?;
    let metadata = fstat(&authority).with_context(|| {
        format!(
            "failed to inspect {label} directory '{}'",
            display.display()
        )
    })?;
    if !FileType::from_raw_mode(metadata.st_mode).is_dir() {
        return Err(anyhow!(
            "{label} entry '{}' must be a real directory",
            display.display()
        ));
    }
    readable_directory_from_authority(&authority, display, label)
}

fn readable_directory_from_authority(
    authority: impl AsFd,
    display: &Path,
    label: &str,
) -> Result<File> {
    let directory = openat(authority.as_fd(), ".", directory_flags(), Mode::empty())
        .map(File::from)
        .map_err(|error| {
            anyhow!(
                "failed to open {label} directory '{}': {error}",
                display.display()
            )
        })?;
    ensure_same_directory(authority, &directory, display)?;
    Ok(directory)
}

fn revalidate_named_directory(
    parent: impl AsFd,
    name: &OsStr,
    opened: impl AsFd,
    display: &Path,
    label: &str,
) -> Result<()> {
    let current = open_directory_entry(parent, name, display, label)?;
    ensure_same_directory(opened, &current, display)
}

fn ensure_same_directory(opened: impl AsFd, current: impl AsFd, display: &Path) -> Result<()> {
    let opened = fstat(opened).with_context(|| {
        format!(
            "failed to inspect open directory authority '{}'",
            display.display()
        )
    })?;
    let current = fstat(current).with_context(|| {
        format!(
            "failed to inspect current directory authority '{}'",
            display.display()
        )
    })?;
    if !FileType::from_raw_mode(opened.st_mode).is_dir()
        || !FileType::from_raw_mode(current.st_mode).is_dir()
        || opened.st_dev != current.st_dev
        || opened.st_ino != current.st_ino
    {
        return Err(anyhow!(
            "durable directory '{}' changed during metadata accounting",
            display.display()
        ));
    }
    Ok(())
}

fn unsafe_metadata_entry(label: &str, display: &Path) -> anyhow::Error {
    anyhow!(
        "{label} entry '{}' must be a regular file or directory",
        display.display()
    )
}

fn metadata_limit_error(limit: MetadataTreeLimit, observed: u64, maximum: usize) -> anyhow::Error {
    anyhow!(MetadataTreeLimitExceeded {
        limit,
        observed,
        maximum: usize_as_u64(maximum),
    })
}

fn usize_as_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn read_open_file_bounded(
    file: File,
    parent_path: &Path,
    file_name: &OsStr,
    limit: usize,
    label: &str,
) -> Result<BoundedRead> {
    read_open_file_bounded_with_metadata(file, parent_path, file_name, limit, label)
        .map(|(contents, _metadata)| contents)
}

fn read_open_file_bounded_with_metadata(
    file: File,
    parent_path: &Path,
    file_name: &OsStr,
    limit: usize,
    label: &str,
) -> Result<(BoundedRead, Metadata)> {
    let metadata = file.metadata().with_context(|| {
        format!(
            "failed to stat {label} '{}'",
            parent_path.join(file_name).display()
        )
    })?;
    if metadata.len() > limit as u64 {
        return Ok((BoundedRead::TooLarge, metadata));
    }

    let read_limit = u64::try_from(limit).unwrap_or(u64::MAX).saturating_add(1);
    let mut bytes = Vec::with_capacity(limit.min(8 * 1024));
    file.take(read_limit)
        .read_to_end(&mut bytes)
        .with_context(|| {
            format!(
                "failed to read {label} '{}'",
                parent_path.join(file_name).display()
            )
        })?;
    if bytes.len() > limit {
        return Ok((BoundedRead::TooLarge, metadata));
    }
    Ok((BoundedRead::Contents(bytes), metadata))
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
    ensure_regular_file(&file, parent_path, file_name, label)?;
    Ok(Some(file))
}

fn ensure_regular_file(
    file: &File,
    parent_path: &Path,
    file_name: &OsStr,
    label: &str,
) -> Result<()> {
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
    Ok(())
}

fn harden_private_file(
    file: &File,
    parent_path: &Path,
    file_name: &OsStr,
    label: &str,
) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let metadata = file.metadata().with_context(|| {
            format!(
                "failed to stat {label} '{}'",
                parent_path.join(file_name).display()
            )
        })?;
        if metadata.permissions().mode() & 0o777 != 0o600 {
            file.set_permissions(Permissions::from_mode(0o600))
                .with_context(|| {
                    format!(
                        "failed to protect {label} '{}'",
                        parent_path.join(file_name).display()
                    )
                })?;
        }
    }
    Ok(())
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
            Ok(file) => {
                let file = File::from(file);
                if let Err(error) = fchmod(&file, Mode::from_raw_mode(mode)) {
                    if let Err(cleanup_error) = unlinkat(parent, &temp_name, AtFlags::empty()) {
                        warn!(
                            ?cleanup_error,
                            path = %parent_path.join(&temp_name).display(),
                            "failed to remove unprotected temporary file"
                        );
                    }
                    return Err(anyhow!(
                        "failed to protect temporary {label} file in '{}': {error}",
                        parent_path.display()
                    ));
                }
                return Ok((temp_name, file));
            }
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
    fn private_read_and_lock_use_exact_owner_only_regular_files() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        let root = tempfile::tempdir().unwrap();
        let directory = rooted(&root);
        std::fs::create_dir_all(directory.path()).unwrap();
        let secret = directory.path().join("secret");
        std::fs::write(&secret, b"private").unwrap();
        std::fs::set_permissions(&secret, Permissions::from_mode(0o644)).unwrap();

        let (contents, metadata) = directory
            .read_bounded_with_metadata(OsStr::new("secret"), 32, "secret")
            .unwrap()
            .unwrap();
        assert_eq!(contents, b"private");
        assert_eq!(metadata.permissions().mode() & 0o777, 0o644);
        assert_eq!(
            std::fs::metadata(&secret).unwrap().permissions().mode() & 0o777,
            0o644,
            "ordinary bounded reads must not harden the source file"
        );

        let (contents, metadata) = directory
            .read_private_bounded_with_metadata(OsStr::new("secret"), 32, "secret")
            .unwrap()
            .unwrap();
        assert_eq!(contents, b"private");
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);

        let lock = directory
            .open_private_lock_file(OsStr::new("lock"), "lock")
            .unwrap();
        assert_eq!(lock.metadata().unwrap().permissions().mode() & 0o777, 0o600);

        let outside = root.path().join("outside");
        std::fs::write(&outside, b"outside").unwrap();
        symlink(&outside, directory.path().join("linked-lock")).unwrap();
        assert!(directory
            .open_private_lock_file(OsStr::new("linked-lock"), "lock")
            .is_err());

        let fifo = directory.path().join("fifo-lock");
        rustix::fs::mkfifoat(
            rustix::fs::CWD,
            &fifo,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .unwrap();
        let error = directory
            .open_private_lock_file(OsStr::new("fifo-lock"), "lock")
            .expect_err("a FIFO cannot become lock authority");
        assert!(error.to_string().contains("must be a regular file"));
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
    fn concurrent_takers_have_exactly_one_winner() {
        let root = tempfile::tempdir().unwrap();
        let directory = rooted(&root);
        std::fs::create_dir_all(directory.path()).unwrap();
        directory
            .write_private_atomic(OsStr::new("session"), b"ready", 32, "session state")
            .unwrap();
        let directory = std::sync::Arc::new(directory);
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let takers = (0..8)
            .map(|_| {
                let directory = std::sync::Arc::clone(&directory);
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    directory
                        .take_bounded_status(OsStr::new("session"), 32, "session state")
                        .unwrap()
                })
            })
            .collect::<Vec<_>>();
        let results = takers
            .into_iter()
            .map(|taker| taker.join().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            results
                .iter()
                .filter(|result| **result == BoundedRead::Contents(b"ready".to_vec()))
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|result| **result == BoundedRead::Missing)
                .count(),
            7
        );
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

    #[test]
    fn metadata_accounting_enforces_each_exact_tree_boundary() {
        let root = tempfile::tempdir().unwrap();
        let directory = rooted(&root);
        std::fs::create_dir_all(directory.path().join("one")).unwrap();
        std::fs::write(directory.path().join("root-file"), b"123").unwrap();
        std::fs::write(directory.path().join("one/child-file"), b"45").unwrap();

        let exact = MetadataTreeLimits {
            max_bytes: 5,
            max_entries: 3,
            max_depth: 2,
        };
        assert_eq!(
            directory
                .account_metadata(exact, "runtime profile")
                .unwrap(),
            MetadataTreeUsage {
                bytes: 5,
                entries: 3,
                max_depth: 2,
            }
        );

        for (limits, dimension, maximum) in [
            (
                MetadataTreeLimits {
                    max_bytes: 4,
                    max_entries: 10,
                    max_depth: 10,
                },
                MetadataTreeLimit::Bytes,
                4,
            ),
            (
                MetadataTreeLimits {
                    max_bytes: 10,
                    max_entries: 2,
                    max_depth: 10,
                },
                MetadataTreeLimit::Entries,
                2,
            ),
            (
                MetadataTreeLimits {
                    max_bytes: 10,
                    max_entries: 10,
                    max_depth: 1,
                },
                MetadataTreeLimit::Depth,
                1,
            ),
        ] {
            let error = directory
                .account_metadata(limits, "runtime profile")
                .unwrap_err();
            let error = error.downcast_ref::<MetadataTreeLimitExceeded>().unwrap();
            assert_eq!(error.limit, dimension);
            assert_eq!(error.observed, maximum + 1);
            assert_eq!(error.maximum, maximum);
        }
    }

    #[cfg(unix)]
    #[test]
    fn metadata_accounting_counts_symlinks_without_following_them_and_rejects_special_entries() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let directory = rooted(&root);
        std::fs::create_dir_all(directory.path()).unwrap();
        let target = root.path().join("outside");
        std::fs::write(&target, vec![0_u8; 1_024]).unwrap();
        symlink(&target, directory.path().join("symlink")).unwrap();
        let link_bytes = std::fs::symlink_metadata(directory.path().join("symlink"))
            .unwrap()
            .len();

        assert_eq!(
            directory
                .account_metadata(
                    MetadataTreeLimits {
                        max_bytes: link_bytes,
                        max_entries: 1,
                        max_depth: 1,
                    },
                    "runtime profile",
                )
                .unwrap(),
            MetadataTreeUsage {
                bytes: link_bytes,
                entries: 1,
                max_depth: 1,
            }
        );

        rustix::fs::mkfifoat(
            rustix::fs::CWD,
            directory.path().join("fifo"),
            Mode::RUSR | Mode::WUSR,
        )
        .unwrap();
        assert!(directory
            .account_metadata(
                MetadataTreeLimits {
                    max_bytes: 10_000,
                    max_entries: 10,
                    max_depth: 10,
                },
                "runtime profile",
            )
            .unwrap_err()
            .to_string()
            .contains("must be a regular file or directory"));
    }

    #[cfg(unix)]
    #[test]
    fn metadata_descriptor_keeps_the_opened_type_across_a_path_swap() {
        let root = tempfile::tempdir().unwrap();
        let directory = rooted(&root);
        std::fs::create_dir_all(directory.path()).unwrap();
        let entry = directory.path().join("entry");
        std::fs::write(&entry, b"12345").unwrap();
        let parent = directory.open_existing().unwrap().unwrap();
        let authority =
            open_metadata_entry(&parent, OsStr::new("entry"), &entry, "runtime profile").unwrap();

        std::fs::rename(&entry, directory.path().join("old-entry")).unwrap();
        rustix::fs::mkfifoat(rustix::fs::CWD, &entry, Mode::RUSR | Mode::WUSR).unwrap();

        let metadata = fstat(&authority).unwrap();
        assert!(FileType::from_raw_mode(metadata.st_mode).is_file());
        assert_eq!(metadata.st_size, 5);
        assert!(inspect_metadata_entry(
            parent.as_fd(),
            OsStr::new("entry"),
            &entry,
            "runtime profile"
        )
        .unwrap_err()
        .to_string()
        .contains("must be a regular file or directory"));
    }

    #[test]
    fn root_identity_check_rejects_replacement_as_defense_in_depth() {
        let root = tempfile::tempdir().unwrap();
        let directory = rooted(&root);
        std::fs::create_dir_all(directory.path()).unwrap();
        let opened = directory.open_existing().unwrap().unwrap();
        let old = directory.path().with_extension("old");
        std::fs::rename(directory.path(), &old).unwrap();
        std::fs::create_dir(directory.path()).unwrap();

        let error = directory.revalidate_open_directory(&opened).unwrap_err();
        assert!(error
            .to_string()
            .contains("changed during metadata accounting"));
    }

    #[test]
    fn immediate_directory_names_are_bounded_and_authoritative() {
        let root = tempfile::tempdir().unwrap();
        let directory = rooted(&root);
        assert_eq!(
            directory
                .immediate_directory_names(2, "runtime profiles")
                .unwrap(),
            BTreeSet::new()
        );
        std::fs::create_dir_all(directory.path().join("a")).unwrap();
        std::fs::create_dir(directory.path().join("b")).unwrap();

        assert_eq!(
            directory
                .immediate_directory_names(2, "runtime profiles")
                .unwrap(),
            [OsString::from("a"), OsString::from("b")]
                .into_iter()
                .collect()
        );
        let error = directory
            .immediate_directory_names(1, "runtime profiles")
            .unwrap_err();
        let limit = error
            .downcast_ref::<ImmediateDirectoryLimitExceeded>()
            .unwrap();
        assert_eq!((limit.observed_at_least, limit.maximum), (2, 1));

        std::fs::write(directory.path().join("not-a-directory"), b"x").unwrap();
        assert!(directory
            .immediate_directory_names(3, "runtime profiles")
            .unwrap_err()
            .to_string()
            .contains("must be a real directory"));
    }
}
