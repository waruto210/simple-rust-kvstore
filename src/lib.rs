#![deny(missing_docs)]
//! A simple string key/value store
pub use client::KvsClient;
pub use engine::{KvStore, KvsEngine, SledKvsEngine};
pub use err::{KvsError, Result};
pub use server::KvsServer;

mod client;
mod engine;
mod err;
mod protocol;
/// A simple string key/value store Server
pub mod server;
