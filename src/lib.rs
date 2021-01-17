#![deny(missing_docs)]
//! A simple string key/value store
pub use err::{KvsError, Result};
pub use kv::KvStore;

mod err;
mod kv;
