use anyhow::{Context, Result};
use awaitgroup::WaitGroup;
use kvs::{KvStore, KvsEngine};
use tempfile::TempDir;
use tokio;
use walkdir::WalkDir;

// Should get previously stored value
#[test]
fn get_stored_value() -> Result<()> {
    let rt = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;

    rt.block_on(async move {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let store = KvStore::open(temp_dir.path())?;

        store.set("key1".to_owned(), "value1".to_owned()).await?;
        store.set("key2".to_owned(), "value2".to_owned()).await?;

        assert_eq!(
            store.get("key1".to_owned()).await?,
            Some("value1".to_owned())
        );
        assert_eq!(
            store.get("key2".to_owned()).await?,
            Some("value2".to_owned())
        );

        // Open from disk again and check persistent data
        drop(store);
        let store = KvStore::open(temp_dir.path())?;
        assert_eq!(
            store.get("key1".to_owned()).await?,
            Some("value1".to_owned())
        );
        assert_eq!(
            store.get("key2".to_owned()).await?,
            Some("value2".to_owned())
        );
        Ok(())
    })
}

// Should overwrite existent value
#[test]
fn overwrite_value() -> Result<()> {
    let rt = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;
    rt.block_on(async move {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let store = KvStore::open(temp_dir.path())?;

        store.set("key1".to_owned(), "value1".to_owned()).await?;
        assert_eq!(
            store.get("key1".to_owned()).await?,
            Some("value1".to_owned())
        );
        store.set("key1".to_owned(), "value2".to_owned()).await?;
        assert_eq!(
            store.get("key1".to_owned()).await?,
            Some("value2".to_owned())
        );

        // Open from disk again and check persistent data
        drop(store);
        let store = KvStore::open(temp_dir.path())?;
        assert_eq!(
            store.get("key1".to_owned()).await?,
            Some("value2".to_owned())
        );
        store.set("key1".to_owned(), "value3".to_owned()).await?;
        assert_eq!(
            store.get("key1".to_owned()).await?,
            Some("value3".to_owned())
        );

        Ok(())
    })
}

// Should get `None` when getting a non-existent key
#[test]
fn get_non_existent_value() -> Result<()> {
    let rt = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;

    rt.block_on(async move {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let store = KvStore::open(temp_dir.path())?;

        store.set("key1".to_owned(), "value1".to_owned()).await?;
        assert_eq!(store.get("key2".to_owned()).await?, None);

        // Open from disk again and check persistent data
        drop(store);
        let store = KvStore::open(temp_dir.path())?;
        assert_eq!(store.get("key2".to_owned()).await?, None);

        Ok(())
    })
}

#[test]
fn remove_non_existent_key() -> Result<()> {
    let rt = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;
    rt.block_on(async move {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let store = KvStore::open(temp_dir.path())?;

        assert!(store.remove("key1".to_owned()).await.is_err());
        Ok(())
    })
}

#[test]
fn remove_key() -> Result<()> {
    let rt = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;
    rt.block_on(async move {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let store = KvStore::open(temp_dir.path())?;

        store.set("key1".to_owned(), "value1".to_owned()).await?;
        assert!(store.remove("key1".to_owned()).await.is_ok());
        assert_eq!(store.get("key1".to_owned()).await?, None);
        Ok(())
    })
}

// Insert data until total size of the directory decreases.
// Test data correctness after compaction.
#[test]
fn compaction() -> Result<()> {
    let rt = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;
    rt.block_on(async move {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let store = KvStore::open(temp_dir.path())?;

        let dir_size = || {
            let entries = WalkDir::new(temp_dir.path()).into_iter();
            let len: walkdir::Result<u64> = entries
                .map(|res| {
                    res.and_then(|entry| entry.metadata())
                        .map(|metadata| metadata.len())
                })
                .sum();
            len.expect("fail to get directory size")
        };

        let mut current_size = dir_size();

        for iter in 0..1000 {
            for key_id in 0..1000 {
                let key = format!("key{}", key_id);
                let value = format!("{}", iter);
                store.set(key, value).await?;
            }

            let new_size = dir_size();
            if new_size > current_size {
                current_size = new_size;
                continue;
            }
            // Compaction triggered

            drop(store);
            // reopen and check content
            let store = KvStore::open(temp_dir.path())?;
            for key_id in 0..1000 {
                let key = format!("key{}", key_id);
                assert_eq!(store.get(key).await?, Some(format!("{}", iter)));
            }
            return Ok(());
        }
        panic!("No compaction detected");
    })
}

#[test]
fn concurrent_set() -> Result<()> {
    let rt = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;

    rt.block_on(async move {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let store = KvStore::open(temp_dir.path())?;

        let mut wg = WaitGroup::new();
        for i in 0..1000 {
            let store = store.clone();
            let worker = wg.worker();
            let _ = tokio::spawn(async move {
                store
                    .set(format!("key{}", i), format!("value{}", i))
                    .await
                    .unwrap();
                worker.done();
            });
        }
        wg.wait().await;

        for i in 0..1000 {
            assert_eq!(
                store.get(format!("key{}", i)).await?,
                Some(format!("value{}", i))
            );
        }

        // Open from disk again and check persistent data
        drop(store);
        let store = KvStore::open(temp_dir.path())?;
        for i in 0..1000 {
            assert_eq!(
                store.get(format!("key{}", i)).await?,
                Some(format!("value{}", i))
            );
        }

        Ok(())
    })
}

#[test]
fn concurrent_get() -> Result<()> {
    let rt = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;
    rt.block_on(async move {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let store = KvStore::open(temp_dir.path())?;

        for i in 0..100 {
            store
                .set(format!("key{}", i), format!("value{}", i))
                .await
                .unwrap();
        }
        let mut wg = WaitGroup::new();

        for thread_id in 0..100 {
            let store = store.clone();
            let worker = wg.worker();
            let _ = tokio::spawn(async move {
                for i in 0..100 {
                    let key_id = (i + thread_id) % 100;
                    assert_eq!(
                        store.get(format!("key{}", key_id)).await.unwrap(),
                        Some(format!("value{}", key_id))
                    );
                }
                worker.done();
            });
        }
        wg.wait().await;

        // Open from disk again and check persistent data
        drop(store);
        let store = KvStore::open(temp_dir.path())?;
        let mut wg = WaitGroup::new();
        for thread_id in 0..100 {
            let store = store.clone();
            let _ = tokio::spawn(async move {
                for i in 0..100 {
                    let key_id = (i + thread_id) % 100;
                    assert_eq!(
                        store.get(format!("key{}", key_id)).await.unwrap(),
                        Some(format!("value{}", key_id))
                    );
                }
            });
        }
        wg.wait().await;

        Ok(())
    })
}
