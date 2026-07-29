use sha2::{Digest, Sha256};

use super::ids::lowercase_hex;
use crate::prelude::*;

pub(crate) struct CanonicalDigest(Sha256);

impl CanonicalDigest {
    pub(crate) fn new(domain: &str) -> Self {
        let mut digest = Self(Sha256::new());
        digest.str("schema", domain);
        digest
    }

    pub(crate) fn str(&mut self, label: &str, value: &str) {
        self.0.update((label.len() as u64).to_be_bytes());
        self.0.update(label.as_bytes());
        self.0.update((value.len() as u64).to_be_bytes());
        self.0.update(value.as_bytes());
    }

    pub(crate) fn bool(&mut self, label: &str, value: bool) {
        self.str(label, if value { "true" } else { "false" });
    }

    pub(crate) fn u64(&mut self, label: &str, value: u64) {
        self.str(label, &value.to_string());
    }

    pub(crate) fn option_u64(&mut self, label: &str, value: Option<u64>) {
        match value {
            Some(value) => self.u64(label, value),
            None => self.str(label, ""),
        }
    }

    pub(crate) fn map<'a, I>(&mut self, label: &str, entries: I)
    where
        I: Iterator<Item = (&'a String, &'a String)>,
    {
        self.str(label, "map");
        for (key, value) in entries {
            self.str("key", key);
            self.str("value", value);
        }
    }

    pub(crate) fn sequence<'a, I, S>(&mut self, label: &str, entries: I)
    where
        I: Iterator<Item = S>,
        S: AsRef<str> + 'a,
    {
        self.str(label, "sequence");
        for entry in entries {
            self.str("item", entry.as_ref());
        }
    }

    pub(crate) fn set<'a, I, S>(&mut self, label: &str, entries: I)
    where
        I: Iterator<Item = S>,
        S: AsRef<str> + 'a,
    {
        self.str(label, "set");
        let mut entries = entries
            .map(|entry| entry.as_ref().to_string())
            .collect::<Vec<_>>();
        entries.sort_unstable();
        for entry in entries {
            self.str("item", &entry);
        }
    }

    pub(crate) fn finish(self) -> String {
        lowercase_hex(&self.0.finalize())
    }
}
