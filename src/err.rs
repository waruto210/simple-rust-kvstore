use failure::Fail;
use sled;
use std::io;
use std::string::FromUtf8Error;

/// `KvsError`
#[derive(Debug, Fail)]
pub enum KvsError {
    /// Io Error
    #[fail(display = "{}", _0)]
    Io(io::Error),

    /// serde_json Error
    #[fail(display = "{}", _0)]
    Serde(serde_json::Error),

    /// Sled engine error
    #[fail(display = "{}", _0)]
    Sled(sled::Error),

    /// Can't Parse String from invalid UTF-8 sequence
    #[fail(display = "{}", _0)]
    Utf8(#[cause] FromUtf8Error),

    /// Command in log has broken
    #[fail(display = "broken command")]
    BrokenCommand,

    /// Engine log file is not correct
    #[fail(display = "broken engine log file")]
    BrokenEngine,

    /// IndexEntry in memory is not correct
    #[fail(display = "broken Index")]
    BrokenIndex,

    /// Key not found Error when remove a key
    #[fail(display = "Key not found")]
    KeyNotFound,

    /// Error with a string message
    #[fail(display = "{}", _0)]
    StringError(String),
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

impl From<sled::Error> for KvsError {
    fn from(err: sled::Error) -> Self {
        KvsError::Sled(err)
    }
}
impl From<FromUtf8Error> for KvsError {
    fn from(err: FromUtf8Error) -> Self {
        KvsError::Utf8(err)
    }
}

/// Custom Result Type for Kvs
pub type Result<T> = std::result::Result<T, KvsError>;
