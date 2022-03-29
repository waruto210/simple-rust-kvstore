use sled;
use std::io;
use std::string::FromUtf8Error;
use thiserror::Error;

/// `KvsError`
#[derive(Error, Debug)]
pub enum KvsError {
    /// Io Error
    #[error("io error {0}")]
    Io(#[source] io::Error),

    /// serde_json Error
    #[error("serde_json error {0}")]
    Serde(#[source] serde_json::Error),

    /// Sled engine error
    #[error("sled error {0}")]
    Sled(#[source] sled::Error),

    /// Can't Parse String from invalid UTF-8 sequence
    #[error("Utf8 error {0}")]
    Utf8(#[source] FromUtf8Error),

    /// Command in log has broken
    #[error("broken command")]
    BrokenCommand,

    /// Engine log file is not correct
    #[error("broken engine log file")]
    BrokenEngine,

    /// IndexEntry in memory is not correct
    #[error("broken Index")]
    BrokenIndex,

    /// Key not found Error when remove a key
    #[error("Key not found")]
    KeyNotFound,

    /// Error with a string message
    #[error("other error {0}")]
    OtherError(String),
}

pub type Result<T> = anyhow::Result<T>;
