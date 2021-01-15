use std::collections::HashMap;

/// The `KvStore` is used to store Key/Value pairs in a `HashMap`.
/// Example:
///
/// ```rust
/// # use kvs::KvStore;
/// let mut store = KvStore::new();
/// store.set("key".to_string(), "value".to_string());
/// let val = store.get("key".to_string());
/// assert_eq!(val, Some("value".to_string()));
///```
pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    /// Creates a `KvStore
    pub fn new() -> KvStore {
        KvStore {
            map: HashMap::new(),
        }
    }

    /// Get the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&self, key: String) -> Option<String> {
        // Maps &T to T by cloning
        self.map.get(&key).cloned()
    }

    /// Sets the string value of a given string key.
    ///
    /// If the given key already exists, the previous value will be overwitten.
    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    /// Remove a given string key.
    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
}

mod test {
    #[test]
    fn set_and_get() {
        use crate::KvStore;
        let mut store = KvStore::new();
        store.set("key".to_string(), "value".to_string());
        let val = store.get("key".to_string());
        assert_eq!(val, Some("value".to_string()));
    }
}
