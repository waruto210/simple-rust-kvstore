//! Provide different engines for our k/v store
use crate::Result;

/// trait for k/v store engin
pub trait KvsEngine {
    /// Sets the string value of a given string key.
    ///
    /// If the given key already exists, the previous value will be overwitten.
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// Get the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// Remove a given string key.
    ///
    /// # Errors
    ///
    /// Returns `KvsError::KeyNotFound` if the given ket does not exixt.
    fn remove(&mut self, key: String) -> Result<()>;
}
pub use self::sled::SledKvsEngine;
pub use kv::KvStore;
mod kv;
mod sled;
