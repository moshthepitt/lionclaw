//! Content-addressed blob store on durable-fs: big payloads live at
//! `blobs/sha256/ab/cd/<hex>` and events carry references, never bytes.

use std::ffi::OsString;
use std::fs::{self, File};
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};

use crate::model::{BlobRef, PayloadRef};

/// Payloads above this many bytes are externalized at append time.
pub const BLOB_INLINE_MAX: usize = 100 * 1024;

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

    pub fn get(&self, blob: &BlobRef) -> Result<Vec<u8>> {
        if blob.algo != "sha256" {
            bail!("unsupported blob algo '{}'", blob.algo);
        }
        let path = self
            .root
            .join("sha256")
            .join(&blob.hex[..2])
            .join(&blob.hex[2..4])
            .join(&blob.hex);
        let bytes =
            fs::read(&path).with_context(|| format!("failed to read blob '{}'", path.display()))?;
        let actual = hex::encode(Sha256::digest(&bytes));
        if actual != blob.hex {
            bail!(
                "blob '{}' failed content verification (got {actual})",
                blob.hex
            );
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
        std::fs::write(&path, b"tampered").expect("tamper");
        assert!(store.get(&blob).is_err());
    }
}
