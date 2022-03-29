#![deny(missing_docs)]
//! A simple string key/value store
pub use client::KvsClient;
pub use engine::{KvStore, KvsEngine, SledKvsEngine};
pub use err::{KvsError, Result};
pub use server::{close_server, KvsServer};

mod client;
mod engine;
mod err;
mod protocol;
mod server;
/// ThreadPool
pub mod thread_pool;
