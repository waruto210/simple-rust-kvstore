use crate::Result;
mod naive;
mod rayon;
mod shared_queue;
pub use self::rayon::RayonThreadPool;
pub use naive::NaiveThreadPool;
pub use shared_queue::SharedQueueThreadPool;
/// `ThreadPoll` trait  
pub trait ThreadPool {
    /// new a ThreadPool
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;

    /// spawn a new job
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}
