use super::ThreadPool;
use crate::Result;
use std::thread;

/// A naive ThreadPool
pub struct NaiveThreadPool {}

impl ThreadPool for NaiveThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        assert!(threads > 0, "Thread pool threads must bigger than zero");
        Ok(NaiveThreadPool {})
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}
