use std::io::{self, Read};
use std::path::Path;

use sha2::{Digest, Sha256};

const FILE_BUFFER_BYTES: usize = 64 * 1024;

/// Deterministic content identity shared by packages and whole mission bundles.
pub(crate) struct ContentDigest(Sha256);

impl ContentDigest {
    pub(crate) fn new() -> Self {
        Self(Sha256::new())
    }

    pub(crate) fn feed(&mut self, logical_path: &str, bytes: &[u8], executable: bool) {
        self.feed_header(logical_path, bytes.len() as u64, executable);
        self.feed_chunk(bytes);
    }

    pub(crate) fn feed_header(&mut self, logical_path: &str, len: u64, executable: bool) {
        self.0.update((logical_path.len() as u64).to_le_bytes());
        self.0.update(logical_path.as_bytes());
        self.0.update([executable as u8]);
        self.0.update(len.to_le_bytes());
    }

    pub(crate) fn feed_chunk(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    /// Hash one regular file with fixed auxiliary memory. The caller owns the
    /// file-tree and size policy; this method owns exact-length streaming.
    pub(crate) fn feed_file(
        &mut self,
        logical_path: &str,
        path: &Path,
        executable: bool,
    ) -> io::Result<()> {
        let mut file = std::fs::File::open(path)?;
        let expected = file.metadata()?.len();
        self.feed_header(logical_path, expected, executable);
        let mut buffer = [0_u8; FILE_BUFFER_BYTES];
        let mut read = 0_u64;
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            read = read.saturating_add(count as u64);
            if read > expected {
                return Err(io::Error::other("file grew while it was being hashed"));
            }
            self.feed_chunk(&buffer[..count]);
        }
        if read != expected {
            return Err(io::Error::other("file shrank while it was being hashed"));
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> String {
        hex::encode(self.0.finalize())
    }
}

#[cfg(test)]
mod tests {
    use super::ContentDigest;

    #[test]
    fn streamed_files_keep_the_existing_content_identity() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("content");
        let bytes = b"deterministic content\n";
        std::fs::write(&path, bytes).unwrap();

        let mut direct = ContentDigest::new();
        direct.feed("logical/path", bytes, true);
        let mut streamed = ContentDigest::new();
        streamed.feed_file("logical/path", &path, true).unwrap();

        assert_eq!(streamed.finish(), direct.finish());
    }
}
