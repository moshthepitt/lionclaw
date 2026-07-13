use sha2::{Digest, Sha256};

/// Deterministic content identity shared by packages and whole mission bundles.
pub(crate) struct ContentDigest(Sha256);

impl ContentDigest {
    pub(crate) fn new() -> Self {
        Self(Sha256::new())
    }

    pub(crate) fn feed(&mut self, logical_path: &str, bytes: &[u8], executable: bool) {
        self.0.update((logical_path.len() as u64).to_le_bytes());
        self.0.update(logical_path.as_bytes());
        self.0.update([executable as u8]);
        self.0.update((bytes.len() as u64).to_le_bytes());
        self.0.update(bytes);
    }

    pub(crate) fn finish(self) -> String {
        hex::encode(self.0.finalize())
    }
}
