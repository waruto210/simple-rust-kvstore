use super::ThreadPool;
use crate::Result;
use rayon;

/// A ThreadPool based on `rayon::ThreadPool`
pub struct RayonThreadPool(rayon::ThreadPool);

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        assert!(threads > 0, "Thread pool threads must bigger than zero");
        Ok(RayonThreadPool(
            rayon::ThreadPoolBuilder::new().num_threads(8).build()?,
        ))
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.0.spawn(job)
    }
}
