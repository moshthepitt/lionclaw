//! Content-addressed blob store on durable-fs: big payloads live at
//! `blobs/sha256/ab/cd/<hex>` and events carry references, never bytes.

use std::ffi::OsString;
use std::fs::{self, File};
use std::io::Read;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use rustix::fs::{fstat, open, FileType, Mode, OFlags};
use sha2::{Digest, Sha256};

use crate::model::{BlobRef, PayloadRef};

/// Payloads above this many bytes are externalized at append time.
pub const BLOB_INLINE_MAX: usize = 100 * 1024;

/// Structured failure from reading immutable content-addressed storage.
#[derive(Debug, thiserror::Error)]
pub enum BlobReadError {
    #[error("{operation} blob '{path}'")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{0}")]
    InvalidContent(String),
}

#[derive(Debug, Clone)]
pub struct BlobStore {
    root: PathBuf,
}

impl BlobStore {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn put(&self, bytes: &[u8]) -> Result<BlobRef> {
        let hex = hex::encode(Sha256::digest(bytes));
        let dir = self.root.join("sha256").join(&hex[..2]).join(&hex[2..4]);
        let path = dir.join(&hex);
        if !path.exists() {
            fs::create_dir_all(&dir)
                .with_context(|| format!("failed to create blob dir '{}'", dir.display()))?;
            let parent = File::open(&dir)
                .with_context(|| format!("failed to open blob dir '{}'", dir.display()))?;
            lionclaw_durable_fs::write_file_atomically(
                &parent,
                &dir,
                &OsString::from(&hex),
                bytes,
                0o444,
                None,
                "mission blob",
            )?;
        }
        Ok(BlobRef {
            algo: "sha256".to_string(),
            hex,
            len: bytes.len() as u64,
        })
    }

    pub fn get(&self, blob: &BlobRef) -> std::result::Result<Vec<u8>, BlobReadError> {
        self.read_blob_bounded(blob, blob.len)
    }

    fn read_blob_bounded(
        &self,
        blob: &BlobRef,
        max_bytes: u64,
    ) -> std::result::Result<Vec<u8>, BlobReadError> {
        if blob.algo != "sha256" {
            return Err(BlobReadError::InvalidContent(format!(
                "unsupported blob algo '{}'",
                blob.algo
            )));
        }
        // The ref can come from an agent-authored handoff (PayloadRef::Blob), so
        // validate the hex before slicing it into a path — a short or non-hex
        // value must be a clean error, never a panic (byte-index/char-boundary).
        if blob.hex.len() != 64 || !blob.hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(BlobReadError::InvalidContent(format!(
                "malformed blob hex '{}'",
                blob.hex
            )));
        }
        if blob.len > max_bytes {
            return Err(BlobReadError::InvalidContent(format!(
                "blob '{}' declares {} bytes, which exceeds the {max_bytes}-byte limit",
                blob.hex, blob.len
            )));
        }
        let path = self
            .root
            .join("sha256")
            .join(&blob.hex[..2])
            .join(&blob.hex[2..4])
            .join(&blob.hex);
        let descriptor = open(
            &path,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(|source| {
            if source == rustix::io::Errno::LOOP {
                BlobReadError::InvalidContent(format!("blob '{}' is not a regular file", blob.hex))
            } else {
                BlobReadError::Io {
                    operation: "failed to open",
                    path: path.clone(),
                    source: source.into(),
                }
            }
        })?;
        let stat = fstat(&descriptor).map_err(|source| BlobReadError::Io {
            operation: "failed to stat",
            path: path.clone(),
            source: source.into(),
        })?;
        if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile {
            return Err(BlobReadError::InvalidContent(format!(
                "blob '{}' is not a regular file",
                blob.hex
            )));
        }
        let stored_len = u64::try_from(stat.st_size).map_err(|_| {
            BlobReadError::InvalidContent(format!(
                "blob '{}' has an invalid stored length",
                blob.hex
            ))
        })?;
        if stored_len != blob.len {
            return Err(BlobReadError::InvalidContent(format!(
                "blob '{}' length mismatch (declared {}, stored {stored_len})",
                blob.hex, blob.len
            )));
        }

        let read_limit = usize::try_from(blob.len)
            .ok()
            .and_then(|length| length.checked_add(1))
            .ok_or_else(|| {
                BlobReadError::InvalidContent(format!(
                    "blob '{}' declares an unsupported length",
                    blob.hex
                ))
            })?;
        let mut bytes = Vec::new();
        File::from(descriptor)
            .take(u64::try_from(read_limit).expect("validated blob read limit fits u64"))
            .read_to_end(&mut bytes)
            .map_err(|source| BlobReadError::Io {
                operation: "failed to read",
                path: path.clone(),
                source,
            })?;
        let read_len = u64::try_from(bytes.len()).expect("usize always fits in u64");
        if read_len != blob.len {
            return Err(BlobReadError::InvalidContent(format!(
                "blob '{}' changed length while reading (declared {}, read {})",
                blob.hex, blob.len, read_len
            )));
        }
        let actual = hex::encode(Sha256::digest(&bytes));
        if actual != blob.hex {
            return Err(BlobReadError::InvalidContent(format!(
                "blob '{}' failed content verification (got {actual})",
                blob.hex
            )));
        }
        Ok(bytes)
    }

    /// Enforce the externalization threshold on an inline payload.
    pub fn externalize(&self, payload: PayloadRef) -> Result<PayloadRef> {
        match payload {
            PayloadRef::Inline { text } if text.len() > BLOB_INLINE_MAX => {
                Ok(PayloadRef::Blob(self.put(text.as_bytes())?))
            }
            other => Ok(other),
        }
    }

    /// Store bytes, inlining small UTF-8 payloads and externalizing the rest.
    pub fn payload_from_bytes(&self, bytes: &[u8]) -> Result<PayloadRef> {
        if bytes.len() <= BLOB_INLINE_MAX {
            if let Ok(text) = std::str::from_utf8(bytes) {
                return Ok(PayloadRef::inline(text));
            }
        }
        Ok(PayloadRef::Blob(self.put(bytes)?))
    }

    /// Resolve a payload back to text (lossy for non-UTF-8 blob content).
    pub fn resolve(&self, payload: &PayloadRef) -> Result<String> {
        match payload {
            PayloadRef::Inline { text } => Ok(text.clone()),
            PayloadRef::Blob(blob) => Ok(String::from_utf8_lossy(&self.get(blob)?).into_owned()),
        }
    }

    /// Resolve text only when its declared size fits the caller's remaining
    /// aggregate budget. Blob metadata is checked before any content read.
    pub fn resolve_bounded(&self, payload: &PayloadRef, max_bytes: usize) -> Result<String> {
        match payload {
            PayloadRef::Inline { text } => {
                let declared = text.len() as u64;
                if declared > max_bytes as u64 {
                    bail!(
                        "payload declares {declared} bytes, which exceeds the {max_bytes}-byte limit"
                    );
                }
                Ok(text.clone())
            }
            PayloadRef::Blob(blob) => Ok(String::from_utf8_lossy(
                &self.read_blob_bounded(blob, max_bytes as u64)?,
            )
            .into_owned()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_roundtrip_and_idempotent_put() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = BlobStore::new(dir.path().to_path_buf());
        let blob = store.put(b"hello mission").expect("put");
        assert_eq!(blob.len, 13);
        let again = store.put(b"hello mission").expect("re-put");
        assert_eq!(blob, again);
        assert_eq!(store.get(&blob).expect("get"), b"hello mission");
    }

    #[test]
    fn externalize_respects_threshold() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = BlobStore::new(dir.path().to_path_buf());
        let small = store
            .externalize(PayloadRef::inline("small"))
            .expect("externalize");
        assert!(matches!(small, PayloadRef::Inline { .. }));
        let big_text = "x".repeat(BLOB_INLINE_MAX + 1);
        let big = store
            .externalize(PayloadRef::inline(big_text))
            .expect("externalize");
        assert!(matches!(big, PayloadRef::Blob(_)));
    }

    #[test]
    fn get_detects_corruption() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = BlobStore::new(dir.path().to_path_buf());
        let blob = store.put(b"payload").expect("put");
        let path = dir
            .path()
            .join("sha256")
            .join(&blob.hex[..2])
            .join(&blob.hex[2..4])
            .join(&blob.hex);
        let mut perms = std::fs::metadata(&path).expect("meta").permissions();
        use std::os::unix::fs::PermissionsExt;
        perms.set_mode(0o644);
        std::fs::set_permissions(&path, perms).expect("chmod");
        std::fs::write(&path, b"forgery").expect("tamper");
        let error = store.get(&blob).expect_err("digest mismatch");
        assert!(matches!(&error, BlobReadError::InvalidContent(_)));
        assert!(error.to_string().contains("content verification"));
    }

    #[test]
    fn get_rejects_a_forged_declared_length_before_reading() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = BlobStore::new(dir.path().to_path_buf());
        let mut blob = store.put(b"content").expect("put");
        blob.len += 1;
        let error = store.get(&blob).expect_err("length mismatch");
        assert!(error.to_string().contains("length mismatch"));
    }

    #[test]
    fn bounded_resolution_rejects_declared_overflow_before_reading() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = BlobStore::new(dir.path().to_path_buf());
        let blob = BlobRef {
            algo: "sha256".into(),
            hex: "a".repeat(64),
            len: 1025,
        };
        let error = store
            .resolve_bounded(&PayloadRef::Blob(blob), 1024)
            .expect_err("declared overflow must be rejected before blob I/O");
        assert!(error.to_string().contains("exceeds the 1024-byte limit"));
        assert!(!error.to_string().contains("failed to stat"));
    }

    #[cfg(unix)]
    #[test]
    fn get_rejects_a_symlinked_blob() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("tempdir");
        let store = BlobStore::new(dir.path().to_path_buf());
        let blob = store.put(b"payload").expect("put");
        let path = dir
            .path()
            .join("sha256")
            .join(&blob.hex[..2])
            .join(&blob.hex[2..4])
            .join(&blob.hex);
        let replacement = dir.path().join("replacement");
        std::fs::write(&replacement, b"payload").expect("write replacement");
        std::fs::remove_file(&path).expect("remove blob");
        symlink(&replacement, &path).expect("replace blob with symlink");

        assert!(matches!(
            store.get(&blob),
            Err(BlobReadError::InvalidContent(_))
        ));
    }

    #[test]
    fn get_rejects_a_nonregular_blob() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = BlobStore::new(dir.path().to_path_buf());
        let blob = store.put(b"payload").expect("put");
        let path = dir
            .path()
            .join("sha256")
            .join(&blob.hex[..2])
            .join(&blob.hex[2..4])
            .join(&blob.hex);
        std::fs::remove_file(&path).expect("remove blob");
        std::fs::create_dir(&path).expect("replace blob with directory");

        assert!(matches!(
            store.get(&blob),
            Err(BlobReadError::InvalidContent(_))
        ));
    }

    // A BlobRef can come from an agent-authored handoff, so a malformed hex must
    // return an error, never panic on the `[..2]`/`[2..4]` path slicing.
    #[test]
    fn get_rejects_a_malformed_hex_ref_without_panicking() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = BlobStore::new(dir.path().to_path_buf());
        for hex in ["", "a", "ab", "é", &"z".repeat(64), &"a".repeat(63)] {
            let bad = BlobRef {
                algo: "sha256".to_string(),
                hex: hex.to_string(),
                len: 0,
            };
            assert!(store.get(&bad).is_err(), "hex {hex:?} must be rejected");
        }
    }
}
