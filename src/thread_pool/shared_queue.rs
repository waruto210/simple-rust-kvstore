use crossbeam::channel::unbounded;
use crossbeam::channel::{Receiver, Sender};

use super::ThreadPool;
use crate::Result;
use log::debug;
use std::thread;

type Job = Box<dyn FnOnce() + Send + 'static>;

fn handle_job(worker: JobWorker) {
    loop {
        match worker.recv.recv() {
            Ok(job) => {
                job();
            }
            Err(_) => {
                debug!("Thread worker {} exits", worker.worker_id);
                break;
            }
        }
    }
}

#[derive(Clone)]
struct JobWorker {
    recv: Receiver<Job>,
    worker_id: u32,
}

impl Drop for JobWorker {
    fn drop(&mut self) {
        if thread::panicking() {
            let worker = self.clone();
            thread::spawn(move || handle_job(worker));
        }
    }
}

/// A ThreadPool based on SharedQueueThreadPool
pub struct SharedQueueThreadPool {
    sender: Sender<Job>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        assert!(threads > 0, "Thread pool threads must bigger than zero");
        let (tx, rx) = unbounded();
        for i in 0..threads {
            let worker = JobWorker {
                recv: rx.clone(),
                worker_id: i,
            };
            thread::spawn(move || handle_job(worker));
        }
        Ok(SharedQueueThreadPool { sender: tx })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender.send(Box::new(job)).unwrap();
    }
}
