use crate::Result;
use crate::{KvsEngine, KvsError};
use async_trait::async_trait;
use sled;
use std::path::PathBuf;
use tokio::task::block_in_place;

/// The `SledKvsEngine` is used to store Key/Value pairs based on `sled`.
/// Example:
///
/// ```rust
/// # use anyhow::{Result, Context};
/// use std::env::current_dir;
/// use kvs::{KvsEngine, SledKvsEngine};
/// # fn main() -> Result<()> {
/// let rt = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;
/// rt.block_on(async move {
///     let mut store = SledKvsEngine::open(current_dir()?)?;
///     store.set("key".to_string(), "value".to_string()).await?;
///     store.set("key1".to_string(), "value1".to_string()).await?;
///     store.remove("key1".to_string()).await?;
///     assert_eq!(store.get("key".to_string()).await?, Some("value".to_string()));
///     assert_eq!(store.get("key1".to_string()).await?, None);
///     Ok(())
/// })
/// # }
///```

#[derive(Clone)]
pub struct SledKvsEngine {
    db: sled::Db,
}

impl SledKvsEngine {
    /// create a new `SledKvsEngine` engine
    pub fn open(path: impl Into<PathBuf>) -> Result<SledKvsEngine> {
        Ok(SledKvsEngine {
            db: sled::open(path.into())?,
        })
    }
}

#[async_trait]
impl KvsEngine for SledKvsEngine {
    async fn set(&self, key: String, value: String) -> Result<()> {
        block_in_place(move || {
            self.db.insert(key.as_str(), value.as_str())?;
            self.db.flush()?;
            Ok(())
        })
    }

    async fn get(&self, key: String) -> Result<Option<String>> {
        block_in_place(move || {
            let value = self.db.get(key.as_str())?.map(|ivec| ivec.to_vec());
            if let Some(value) = value {
                let s = String::from_utf8(value)?;
                Ok(Some(s))
            } else {
                Ok(None)
            }
        })
    }

    async fn remove(&self, key: String) -> Result<()> {
        block_in_place(move || {
            let old_value = self.db.remove(key.as_str())?;
            if let Some(_) = old_value {
                self.db.flush()?;
                Ok(())
            } else {
                Err(KvsError::KeyNotFound.into())
            }
        })
    }
}
