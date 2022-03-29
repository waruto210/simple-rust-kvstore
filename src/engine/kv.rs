use super::KvsEngine;
use crate::{KvsError, Result};
use fs::OpenOptions;
use io::BufWriter;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{atomic, Arc, Mutex};
use std::{cell::RefCell, ffi::OsStr};
use std::{io::BufReader, path::PathBuf, u64, usize};
// use crossbeam_skiplist::{SkipList, SkipMap};
use crossbeam_skiplist::SkipMap;

/// At most 2 MB inactive data
const MAX_INACTIVE_DATA_SIZE: u64 = 1024 * 2048;
/// max size of a single log's size
const MAX_FILE_SIZE: u64 = 1024 * 2048;

struct LogReader {
    index: Arc<SkipMap<String, IndexEntry>>,
    file_id_bar: Arc<atomic::AtomicU64>,
    log_dir: PathBuf,
    // need interior mutability of refcell
    readers: RefCell<HashMap<u64, CursorBufferReader<File>>>,
}

impl Clone for LogReader {
    fn clone(&self) -> Self {
        LogReader {
            index: Arc::clone(&self.index),
            file_id_bar: Arc::clone(&self.file_id_bar),
            log_dir: self.log_dir.clone(),
            readers: RefCell::new(HashMap::new()),
        }
    }
}

impl LogReader {
    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(index) = self.index.get(&key) {
            let cmd = self.read_command(index.value())?;
            if let Command::Set { value, .. } = cmd {
                Ok(Some(value))
            } else {
                Err(KvsError::BrokenCommand)
            }
        } else {
            // not an error, beaause we need to exit normally with code 0
            Ok(None)
        }
    }

    fn read_command(&self, index: &IndexEntry) -> Result<Command> {
        let mut readers = self.readers.borrow_mut();
        if !readers.contains_key(&index.file_id) {
            let rerader = CursorBufferReader::new(File::open(
                self.log_dir.join(format!("{}.log", index.file_id)),
            )?)?;
            readers.insert(index.file_id, rerader);
        }
        let reader = readers.get_mut(&index.file_id).unwrap();
        reader.seek(SeekFrom::Start(index.offset))?;
        let cmd_reader = reader.take(index.len);
        Ok(serde_json::from_reader(cmd_reader)?)
    }

    fn clean_readers(&self) {
        let file_id_bar = self.file_id_bar.load(atomic::Ordering::SeqCst);

        let keys = self
            .readers
            .borrow()
            .keys()
            .cloned()
            .into_iter()
            .filter(|&id| id <= file_id_bar)
            .collect::<Vec<u64>>();
        for key in keys {
            self.readers.borrow_mut().remove(&key);
        }
    }
}

struct LogWriter {
    reader: LogReader,
    writer: CursorBufferWriter<File>,
    file_id: u64,
    index: Arc<SkipMap<String, IndexEntry>>,
    inactive_data: u64,
    log_dir: PathBuf,
}

impl LogWriter {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command::set(key, value);
        let offset = self.writer.cursor;
        serde_json::to_writer(&mut self.writer, &command)?;
        self.writer.flush()?;
        let key = command.key();
        if let Some(index) = self.index.get(&key) {
            self.inactive_data += index.value().len;
        }
        self.index.insert(
            key,
            IndexEntry::new(self.file_id, offset, self.writer.cursor),
        );
        if self.writer.cursor > MAX_FILE_SIZE {
            self.file_id += 1;
            self.writer = new_log_file(self.file_id, &self.log_dir)?;
        }
        if self.inactive_data >= MAX_INACTIVE_DATA_SIZE {
            self.compact()?;
        }

        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        {
            if self.index.contains_key(&key) {
                let command = Command::rm(key);
                serde_json::to_writer(&mut self.writer, &command)?;
                self.writer.flush()?;
                if self.writer.cursor > MAX_FILE_SIZE {
                    self.file_id += 1;
                    self.writer = new_log_file(self.file_id, &self.log_dir)?;
                }
                if let Some(index) = self.index.remove(&command.key()) {
                    self.inactive_data += index.value().len;
                } else {
                    return Err(KvsError::BrokenIndex);
                }
            } else {
                return Err(KvsError::KeyNotFound);
            }
        }
        if self.inactive_data >= MAX_INACTIVE_DATA_SIZE {
            self.compact()?;
        }
        Ok(())
    }

    /// compact log by copy all active data to a new log file and
    /// remove all old log files.
    fn compact(&mut self) -> Result<()> {
        let origin_compaction_file_id = self.file_id;
        let mut compaction_file_id = self.file_id + 1;
        let mut compaction_writer = new_log_file(compaction_file_id, &self.log_dir)?;
        // traversing index entries
        for entry in self.index.iter() {
            let index = entry.value();
            let cmd = self.reader.read_command(index)?;
            let offset = compaction_writer.cursor;
            serde_json::to_writer(&mut compaction_writer, &cmd)?;
            compaction_writer.flush()?;
            let new_index = IndexEntry::new(compaction_file_id, offset, compaction_writer.cursor);
            self.index.insert(entry.key().clone(), new_index);

            if compaction_writer.cursor > MAX_FILE_SIZE {
                compaction_writer.flush()?;
                compaction_file_id += 1;
                compaction_writer = new_log_file(compaction_file_id, &self.log_dir)?;
            }
        }
        compaction_writer.flush()?;
        self.file_id = compaction_file_id + 1;
        self.writer = new_log_file(self.file_id, &self.log_dir)?;

        self.reader
            .file_id_bar
            .store(origin_compaction_file_id, atomic::Ordering::SeqCst);

        let inactive_file_ids = get_file_ids(&self.log_dir)?
            .into_iter()
            .filter(|&id| id <= origin_compaction_file_id)
            .collect::<Vec<u64>>();
        self.reader.clean_readers();

        for file_id in inactive_file_ids {
            fs::remove_file(self.log_dir.join(format!("{}.log", file_id)))?;
        }
        self.inactive_data = 0;
        Ok(())
    }
}

/// The `KvStore` is a to store Key/Value pairs based on log-structured storage.
/// Example:
///
/// ```rust
/// # use kvs::{KvStore, Result};
/// use std::env::current_dir;
/// use kvs::KvsEngine;
/// fn main() -> Result<()> {
/// let mut store = KvStore::open(current_dir()?)?;
/// store.set("key".to_string(), "value".to_string())?;
/// store.set("key1".to_string(), "value1".to_string())?;
/// store.remove("key1".to_string())?;
/// assert_eq!(store.get("key".to_string())?, Some("value".to_string()));
/// assert_eq!(store.get("key1".to_string())?, None);
/// Ok(())
/// }
///```

#[derive(Clone)]
pub struct KvStore {
    reader: LogReader,
    writer: Arc<Mutex<LogWriter>>,
}

impl KvsEngine for KvStore {
    /// Set the string value of a given string key.
    ///
    /// If the given key already exists, the previous value will be overwitten.
    ///
    /// # Errors
    ///
    /// Errors may be thrown when I/O and serializing
    fn set(&self, key: String, value: String) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)
    }

    /// Get the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    ///
    /// # Errors
    ///
    /// Errors may be thrown when I/O and serializing
    fn get(&self, key: String) -> Result<Option<String>> {
        self.reader.get(key)
    }

    /// Remove a given string key.
    ///
    /// # Errors
    ///
    /// Returns `KvsError::KeyNotFound` if the given ket does not exixt.
    /// Errors may be thrown when I/O and serializing
    fn remove(&self, key: String) -> Result<()> {
        self.writer.lock().unwrap().remove(key)
    }
}

impl KvStore {
    /// Open the `KvStore` at a given path. Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let log_dir: PathBuf = path.into();
        fs::create_dir_all(&log_dir)?;
        let file_ids = get_file_ids(&log_dir)?;
        let file_id = file_ids.last().unwrap_or(&0_u64) + 1;

        let mut index = SkipMap::new();
        let mut readers = HashMap::new();
        let inactive_data = load_logs(&log_dir, &file_ids, &mut index, &mut readers)?;
        let writer = new_log_file(file_id, &log_dir)?;
        readers.insert(
            file_id,
            CursorBufferReader::new(File::open(log_dir.join(format!("{}.log", file_id)))?)?,
        );
        let index = Arc::new(index);
        let reader = LogReader {
            index: index.clone(),
            log_dir: PathBuf::clone(&log_dir),
            readers: RefCell::new(readers),
            file_id_bar: Arc::new(atomic::AtomicU64::new(0)),
        };
        let writer = Arc::new(Mutex::new(LogWriter {
            reader: reader.clone(),
            writer,
            file_id,
            index,
            inactive_data,
            log_dir: PathBuf::clone(&log_dir),
        }));

        Ok(KvStore { reader, writer })
    }
}

fn new_log_file(file_id: u64, dir: &Path) -> Result<CursorBufferWriter<File>> {
    let path = dir.join(format!("{}.log", file_id));
    let writter = CursorBufferWriter::new(
        OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .open(&path)?,
    )?;
    Ok(writter)
}

// load all previously log data
fn load_logs(
    dir: &Path,
    file_ids: &[u64],
    index: &mut SkipMap<String, IndexEntry>,
    readers: &mut HashMap<u64, CursorBufferReader<File>>,
) -> Result<u64> {
    let mut inactive_data = 0_u64;
    for &file_id in file_ids {
        let mut reader =
            CursorBufferReader::new(File::open(dir.join(format!("{}.log", file_id)))?)?;

        let mut de_stream =
            serde_json::Deserializer::from_reader(&mut reader).into_iter::<Command>();
        let mut offset = 0_u64;
        while let Some(cmd) = de_stream.next() {
            let curr_offset = de_stream.byte_offset() as u64;
            match cmd? {
                Command::Set { key, .. } => {
                    // modify index to point to new data
                    if let Some(ind) = index.get(&key) {
                        inactive_data += ind.value().len;
                    }

                    index.insert(key, IndexEntry::new(file_id, offset, curr_offset));
                }
                Command::Rm { key } => {
                    if let Some(ind) = index.remove(&key) {
                        inactive_data += ind.value().len;
                    }
                }
            }
            offset = curr_offset;
        }
        readers.insert(file_id, reader);
    }
    Ok(inactive_data)
}

// get all previously log files' ids to reconstruct index
fn get_file_ids(path: &Path) -> Result<Vec<u64>> {
    // use flatten to unwarap Option or result
    let mut ids: Vec<u64> = fs::read_dir(path)?
        .map(|res| res.map(|e| e.path()))
        .flatten()
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|name| name.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .flatten()
        .collect();

    ids.sort_unstable();
    Ok(ids)
}
#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Rm { key: String },
}

impl Command {
    fn set(key: String, value: String) -> Command {
        Command::Set { key, value }
    }

    fn rm(key: String) -> Command {
        Command::Rm { key }
    }

    fn key(self) -> String {
        match self {
            Command::Set { key, .. } => key,
            Command::Rm { key, .. } => key,
        }
    }
}

// Corresponding to Bitcask keydir
struct IndexEntry {
    file_id: u64,
    offset: u64,
    len: u64,
}
impl IndexEntry {
    fn new(file_id: u64, offset: u64, end: u64) -> IndexEntry {
        IndexEntry {
            file_id: file_id,
            offset: offset,
            len: end - offset,
        }
    }
}

// serde_json recommand us to use buffer
struct CursorBufferReader<T: Read + Seek> {
    reader: BufReader<T>,
    cursor: u64,
}

impl<T: Read + Seek> CursorBufferReader<T> {
    // seek need mut
    fn new(mut inner: T) -> Result<Self> {
        let cursor = inner.seek(SeekFrom::Current(0))?;
        Ok(CursorBufferReader {
            reader: BufReader::new(inner),
            cursor: cursor,
        })
    }
}

impl<T: Read + Seek> Read for CursorBufferReader<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.reader.read(buf)?;
        self.cursor += n as u64;
        Ok(n)
    }
}

impl<T: Read + Seek> Seek for CursorBufferReader<T> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.cursor = self.reader.seek(pos)?;
        Ok(self.cursor)
    }
}

struct CursorBufferWriter<T: Write + Seek> {
    writer: BufWriter<T>,
    cursor: u64,
}

impl<T: Write + Seek> CursorBufferWriter<T> {
    // seek need mut
    fn new(mut inner: T) -> Result<Self> {
        let cursor = inner.seek(SeekFrom::Current(0))?;
        Ok(CursorBufferWriter {
            writer: BufWriter::new(inner),
            cursor: cursor,
        })
    }
}

impl<T: Write + Seek> Write for CursorBufferWriter<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.writer.write(buf)?;
        self.cursor += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<T: Write + Seek> Seek for CursorBufferWriter<T> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.cursor = self.writer.seek(pos)?;
        Ok(self.cursor)
    }
}
