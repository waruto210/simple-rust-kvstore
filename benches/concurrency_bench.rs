use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode};
use kvs::thread_pool::*;
use kvs::{KvStore, KvsEngine, SledKvsEngine};
use kvs::{KvsClient, KvsServer};
use num_cpus;
use rand::prelude::*;
use std::net::TcpStream;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::{sync::mpsc, usize};
use tempfile::TempDir;

const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789)(*&^%$#@!~";
const KEY_LEN: usize = 10;

#[inline]
fn get_key(rng: &mut ThreadRng) -> String {
    let key: String = (0..KEY_LEN)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect();
    key
}

fn write(c: &mut Criterion) {
    let mut group = c.benchmark_group("write");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    let num_cores = num_cpus::get();
    let mut threads = (1..=num_cores).map(|i| 2 * i).collect::<Vec<usize>>();
    threads.insert(0, 1);

    let mut rng = rand::thread_rng();
    let mut keys = Vec::with_capacity(1000);
    for _ in 0..1000 {
        keys.push(get_key(&mut rng));
    }
    let values = vec!["value".to_string(); 1000];
    let request_pool = SharedQueueThreadPool::new(500).unwrap();
    let port = 4000;
    for thread_num in &threads {
        group.bench_with_input(
            BenchmarkId::new("write_queued_kvstore", thread_num),
            thread_num,
            |b, &thread_num| {
                let temp_dir = TempDir::new().unwrap();
                let engine = KvStore::open(temp_dir.path()).unwrap();
                let pool = SharedQueueThreadPool::new(thread_num as u32).unwrap();
                let state = Arc::new(AtomicBool::new(true));
                let mut server = KvsServer::new(engine, pool, state.clone());
                thread::spawn(move || {
                    while let Err(_) = server.start(format!("127.0.0.1:{}", port)) {
                        eprintln!("bind error");
                    }
                });
                b.iter(|| {
                    let (tx, rx) = mpsc::channel();
                    for i in 0..1000 {
                        let sender = tx.clone();
                        let key = keys[i].clone();
                        let value = values[i].clone();
                        request_pool.spawn(move || {
                            let mut client = KvsClient::connect(format!("127.0.0.1:{}", port));
                            while let Err(_) = client {
                                client = KvsClient::connect(format!("127.0.0.1:{}", port));
                            }
                            let mut client = client.unwrap();
                            while let Err(e) = client.set(key.clone(), value.clone()) {
                                eprintln!("set error {}", e);
                            }
                            client.close();
                            sender.send(i).unwrap();
                        });
                    }
                    drop(tx);
                    let mut count = 0;
                    for _ in rx {
                        count += 1;
                        if count >= 1000 {
                            break;
                        }
                    }
                });
                state.store(false, Ordering::SeqCst);
                let _ = TcpStream::connect(format!("127.0.0.1:{}", port));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("write_rayon_kvstore", thread_num),
            thread_num,
            |b, &thread_num| {
                let temp_dir = TempDir::new().unwrap();
                let engine = KvStore::open(temp_dir.path()).unwrap();
                let pool = RayonThreadPool::new(thread_num as u32).unwrap();
                let state = Arc::new(AtomicBool::new(true));
                let mut server = KvsServer::new(engine, pool, state.clone());
                thread::spawn(move || {
                    while let Err(_) = server.start(format!("127.0.0.1:{}", port)) {
                        eprintln!("bind error");
                    }
                });
                b.iter(|| {
                    let (tx, rx) = mpsc::channel();
                    for i in 0..1000 {
                        let sender = tx.clone();
                        let key = keys[i].clone();
                        let value = values[i].clone();
                        request_pool.spawn(move || {
                            let mut client = KvsClient::connect(format!("127.0.0.1:{}", port));
                            while let Err(_) = client {
                                client = KvsClient::connect(format!("127.0.0.1:{}", port));
                            }
                            let mut client = client.unwrap();
                            while let Err(e) = client.set(key.clone(), value.clone()) {
                                eprintln!("set error {}", e);
                            }
                            client.close();
                            sender.send(i).unwrap();
                        });
                    }
                    drop(tx);
                    let mut count = 0;
                    for _ in rx {
                        count += 1;
                        if count >= 1000 {
                            break;
                        }
                    }
                });
                state.store(false, Ordering::SeqCst);
                let _ = TcpStream::connect(format!("127.0.0.1:{}", port));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("write_rayon_sledkvengine", thread_num),
            thread_num,
            |b, &thread_num| {
                let temp_dir = TempDir::new().unwrap();
                let engine = SledKvsEngine::open(temp_dir.path()).unwrap();
                let pool = RayonThreadPool::new(thread_num as u32).unwrap();
                let state = Arc::new(AtomicBool::new(true));
                let mut server = KvsServer::new(engine, pool, state.clone());
                thread::spawn(move || {
                    while let Err(_) = server.start(format!("127.0.0.1:{}", port)) {
                        eprintln!("bind error");
                    }
                });
                b.iter(|| {
                    let (tx, rx) = mpsc::channel();
                    for i in 0..1000 {
                        let sender = tx.clone();
                        let key = keys[i].clone();
                        let value = values[i].clone();
                        request_pool.spawn(move || {
                            let mut client = KvsClient::connect(format!("127.0.0.1:{}", port));
                            while let Err(_) = client {
                                client = KvsClient::connect(format!("127.0.0.1:{}", port));
                            }
                            let mut client = client.unwrap();
                            while let Err(e) = client.set(key.clone(), value.clone()) {
                                eprintln!("set error {}", e);
                            }
                            client.close();
                            sender.send(i).unwrap();
                        });
                    }
                    drop(tx);
                    let mut count = 0;
                    for _ in rx {
                        count += 1;
                        if count >= 1000 {
                            break;
                        }
                    }
                });
                state.store(false, Ordering::SeqCst);
                let _ = TcpStream::connect(format!("127.0.0.1:{}", port));
            },
        );
    }

    group.finish();
}

fn read(c: &mut Criterion) {
    let mut group = c.benchmark_group("read");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    let num_cores = num_cpus::get();
    let mut threads = (1..=num_cores).map(|i| 2 * i).collect::<Vec<usize>>();
    threads.insert(0, 1);

    let mut rng = rand::thread_rng();
    let mut keys = Vec::with_capacity(1000);
    for _ in 0..1000 {
        keys.push(get_key(&mut rng));
    }
    let values = vec!["value".to_string(); 1000];
    let request_pool = SharedQueueThreadPool::new(500).unwrap();
    let port = 4001;
    for thread_num in &threads {
        group.bench_with_input(
            BenchmarkId::new("read_queued_kvstore", thread_num),
            thread_num,
            |b, &thread_num| {
                let temp_dir = TempDir::new().unwrap();
                let engine = KvStore::open(temp_dir.path()).unwrap();
                let pool = SharedQueueThreadPool::new(thread_num as u32).unwrap();
                let state = Arc::new(AtomicBool::new(true));
                // directly use engine to set, use client is too slow to prepare
                for i in 0..1000 {
                    engine.set(keys[i].clone(), values[i].clone()).unwrap();
                }
                let mut server = KvsServer::new(engine, pool, state.clone());
                thread::spawn(move || {
                    while let Err(_) = server.start(format!("127.0.0.1:{}", port)) {
                        eprintln!("error bind");
                    }
                });

                b.iter(|| {
                    let (tx, rx) = mpsc::channel();
                    for i in 0..1000 {
                        let sender = tx.clone();
                        let key = keys[i].clone();
                        let value = values[i].clone();
                        request_pool.spawn(move || {
                            let mut client = KvsClient::connect(format!("127.0.0.1:{}", port));
                            while let Err(_) = client {
                                client = KvsClient::connect(format!("127.0.0.1:{}", port));
                            }
                            let mut client = client.unwrap();
                            assert_eq!(client.get(key.clone()).unwrap(), Some(value.clone()));
                            client.close();
                            sender.send(i).unwrap();
                        });
                    }
                    drop(tx);
                    let mut count = 0;
                    for _ in rx {
                        count += 1;
                        if count >= 1000 {
                            break;
                        }
                    }
                });
                state.store(false, Ordering::SeqCst);
                let _ = TcpStream::connect(format!("127.0.0.1:{}", port));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("read_rayon_kvstore", thread_num),
            thread_num,
            |b, &thread_num| {
                let temp_dir = TempDir::new().unwrap();
                let engine = KvStore::open(temp_dir.path()).unwrap();
                let pool = RayonThreadPool::new(thread_num as u32).unwrap();
                let state = Arc::new(AtomicBool::new(true));
                // directly use engine to set, use client is too slow to prepare
                for i in 0..1000 {
                    engine.set(keys[i].clone(), values[i].clone()).unwrap();
                }
                let mut server = KvsServer::new(engine, pool, state.clone());
                thread::spawn(move || {
                    while let Err(_) = server.start(format!("127.0.0.1:{}", port)) {
                        eprintln!("error bind");
                    }
                });

                b.iter(|| {
                    let (tx, rx) = mpsc::channel();
                    for i in 0..1000 {
                        let sender = tx.clone();
                        let key = keys[i].clone();
                        let value = values[i].clone();
                        request_pool.spawn(move || {
                            let mut client = KvsClient::connect(format!("127.0.0.1:{}", port));
                            while let Err(_) = client {
                                client = KvsClient::connect(format!("127.0.0.1:{}", port));
                            }
                            let mut client = client.unwrap();
                            assert_eq!(client.get(key.clone()).unwrap(), Some(value.clone()));
                            client.close();
                            sender.send(i).unwrap();
                        });
                    }
                    drop(tx);
                    let mut count = 0;
                    for _ in rx {
                        count += 1;
                        if count >= 1000 {
                            break;
                        }
                    }
                });
                state.store(false, Ordering::SeqCst);
                let _ = TcpStream::connect(format!("127.0.0.1:{}", port));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("read_rayon_sledkvengine", thread_num),
            thread_num,
            |b, &thread_num| {
                let temp_dir = TempDir::new().unwrap();
                let engine = SledKvsEngine::open(temp_dir.path()).unwrap();
                let pool = RayonThreadPool::new(thread_num as u32).unwrap();
                let state = Arc::new(AtomicBool::new(true));
                // directly use engine to set, use client is too slow to prepare
                for i in 0..1000 {
                    engine.set(keys[i].clone(), values[i].clone()).unwrap();
                }
                let mut server = KvsServer::new(engine, pool, state.clone());
                thread::spawn(move || {
                    while let Err(_) = server.start(format!("127.0.0.1:{}", port)) {
                        eprintln!("error bind");
                    }
                });

                b.iter(|| {
                    let (tx, rx) = mpsc::channel();
                    for i in 0..1000 {
                        let sender = tx.clone();
                        let key = keys[i].clone();
                        let value = values[i].clone();
                        request_pool.spawn(move || {
                            let mut client = KvsClient::connect(format!("127.0.0.1:{}", port));
                            while let Err(_) = client {
                                client = KvsClient::connect(format!("127.0.0.1:{}", port));
                            }
                            let mut client = client.unwrap();
                            assert_eq!(client.get(key.clone()).unwrap(), Some(value.clone()));
                            client.close();
                            sender.send(i).unwrap();
                        });
                    }
                    drop(tx);
                    let mut count = 0;
                    for _ in rx {
                        count += 1;
                        if count >= 1000 {
                            break;
                        }
                    }
                });
                state.store(false, Ordering::SeqCst);
                let _ = TcpStream::connect(format!("127.0.0.1:{}", port));
            },
        );
    }

    group.finish();
}

criterion_group!(benches, write, read);
criterion_main!(benches);
