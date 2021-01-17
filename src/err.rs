use failure::Fail;
use std::io;

/// `KvsError`
#[derive(Debug, Fail)]
pub enum KvsError {
    /// Io Error
    #[fail(display = "{}", _0)]
    Io(io::Error),

    /// serde_json Error
    #[fail(display = "{}", _0)]
    Serde(serde_json::Error),

    /// Command Error
    #[fail(display = "broken command")]
    BrokenCommand,

    /// IndexEntry Error
    #[fail(display = "broken Index")]
    BrokenIndex,

    /// Key not found Error
    #[fail(display = "Key not found")]
    KeyNotFound,
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> Self {
        KvsError::Io(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> Self {
        KvsError::Serde(err)
    }
}

/// Custom Result Type for Kvs
pub type Result<T> = std::result::Result<T, KvsError>;
