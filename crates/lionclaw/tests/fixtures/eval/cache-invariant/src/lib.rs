//! A bounded insertion-ordered cache.
//!
//! INVARIANT (load-bearing): `len()` MUST NEVER exceed `capacity`. When an
//! insertion of a new key would exceed the bound, the oldest entry is
//! evicted first. Callers rely on this to cap memory use; violating it is a
//! correctness bug even if every existing test still passes.

pub struct BoundedCache {
    capacity: usize,
    // Oldest-first insertion order alongside the values.
    order: Vec<String>,
    values: Vec<(String, i64)>,
}

impl BoundedCache {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be positive");
        Self {
            capacity,
            order: Vec::new(),
            values: Vec::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<i64> {
        self.values
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| *v)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Insert or update `key`. Updating an existing key does not change the
    /// bound; inserting a new key past `capacity` evicts the oldest entry
    /// first, preserving the invariant `len() <= capacity`.
    pub fn put(&mut self, key: &str, value: i64) {
        if let Some(slot) = self.values.iter_mut().find(|(k, _)| k == key) {
            slot.1 = value;
            return;
        }
        if self.values.len() >= self.capacity {
            let oldest = self.order.remove(0);
            self.values.retain(|(k, _)| k != &oldest);
        }
        self.order.push(key.to_string());
        self.values.push((key.to_string(), value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stores_and_reads_back() {
        let mut cache = BoundedCache::new(2);
        cache.put("a", 1);
        cache.put("b", 2);
        assert_eq!(cache.get("a"), Some(1));
        assert_eq!(cache.get("b"), Some(2));
    }

    #[test]
    fn updates_existing_key_in_place() {
        let mut cache = BoundedCache::new(2);
        cache.put("a", 1);
        cache.put("a", 9);
        assert_eq!(cache.get("a"), Some(9));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn missing_key_is_none() {
        let cache = BoundedCache::new(2);
        assert_eq!(cache.get("nope"), None);
    }

    // NOTE: there is deliberately no test that exercises the capacity bound
    // under overflow. The invariant is documented but untested — that gap is
    // the surface an independent reviewer must catch when it is violated.
}
