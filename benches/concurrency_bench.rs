use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode};
use crossbeam::sync::WaitGroup;
use kvs::server::close_server;
use kvs::{KvStore, KvsEngine, SledKvsEngine};
use kvs::{KvsClient, KvsServer};
use rand::prelude::*;
use std::iter::Iterator;
use std::sync::{atomic::AtomicBool, Arc};
use tempfile::TempDir;
use tokio;

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

    let threads = vec![1, 2, 4, 8, 16];

    let mut rng = rand::thread_rng();
    let mut keys = Vec::with_capacity(1000);
    for _ in 0..1000 {
        keys.push(get_key(&mut rng));
    }
    let values = vec!["value"; 1000];

    let port = 4000;
    for thread_num in threads {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(thread_num)
            .enable_all()
            .build()
            .unwrap();

        let temp_dir = TempDir::new().unwrap();
        let engine = KvStore::open(temp_dir.path()).unwrap();
        let state = Arc::new(AtomicBool::new(true));
        let mut server = KvsServer::new(engine, state.clone());
        rt.spawn(async move {
            while let Err(e) = server
                .start(format!("127.0.0.1:{}", port + thread_num))
                .await
            {
                eprintln!("bind error {}", e);
            }
        });

        let client_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(16)
            .enable_all()
            .build()
            .unwrap();

        group.bench_with_input(
            BenchmarkId::new("write_async_kvstore", thread_num),
            &thread_num,
            |b, &thread_num| {
                b.to_async(&client_rt)
                    .iter(|| async_sets(&keys, &values, port, thread_num))
            },
        );

        rt.block_on(close_server(
            state,
            format!("127.0.0.1:{}", port + thread_num),
        ));

        drop(rt);
        drop(client_rt);

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(thread_num)
            .enable_all()
            .build()
            .unwrap();

        let temp_dir = TempDir::new().unwrap();
        let engine = SledKvsEngine::open(temp_dir.path()).unwrap();
        let state = Arc::new(AtomicBool::new(true));
        let mut server = KvsServer::new(engine, state.clone());
        rt.spawn(async move {
            if let Err(_) = server
                .start(format!("127.0.0.1:{}", port + thread_num))
                .await
            {
                eprintln!("bind error");
            }
        });

        let client_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(16)
            .enable_all()
            .build()
            .unwrap();

        group.bench_with_input(
            BenchmarkId::new("write_async_sled", thread_num),
            &thread_num,
            |b, &thread_num| {
                b.to_async(&client_rt)
                    .iter(|| async_sets(&keys, &values, port, thread_num))
            },
        );

        rt.block_on(close_server(
            state,
            format!("127.0.0.1:{}", port + thread_num),
        ));

        drop(rt);
        drop(client_rt);
    }

    group.finish();
}

fn read(c: &mut Criterion) {
    let mut group = c.benchmark_group("read");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);

    let threads = vec![1, 2, 4, 8, 16];

    let mut rng = rand::thread_rng();
    let mut keys = Vec::with_capacity(1000);
    for _ in 0..1000 {
        keys.push(get_key(&mut rng));
    }
    let values = vec!["value"; 1000];
    let port = 5001;

    for thread_num in threads {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(thread_num)
            .enable_all()
            .build()
            .unwrap();

        let temp_dir = TempDir::new().unwrap();
        let engine = KvStore::open(temp_dir.path()).unwrap();
        // directly use engine to set values
        rt.block_on(async {
            for i in 0..1000 {
                if let Err(e) = engine.set(keys[i].clone(), values[i].to_owned()).await {
                    eprintln!("init set error {}", e);
                }
            }
        });

        let state = Arc::new(AtomicBool::new(true));
        let mut server = KvsServer::new(engine, state.clone());
        rt.spawn(async move {
            if let Err(_) = server
                .start(format!("127.0.0.1:{}", port + thread_num))
                .await
            {
                eprintln!("bind error");
            }
        });

        let client_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(16)
            .enable_all()
            .build()
            .unwrap();

        group.bench_with_input(
            BenchmarkId::new("read_async_kvstore", thread_num),
            &thread_num,
            |b, &thread_num| {
                b.to_async(&client_rt)
                    .iter(|| async_sets(&keys, &values, port, thread_num))
            },
        );

        rt.block_on(close_server(
            state,
            format!("127.0.0.1:{}", port + thread_num),
        ));

        drop(rt);
        drop(client_rt);

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(thread_num)
            .enable_all()
            .build()
            .unwrap();

        let temp_dir = TempDir::new().unwrap();
        let engine = SledKvsEngine::open(temp_dir.path()).unwrap();

        rt.block_on(async {
            for i in 0..1000 {
                if let Err(e) = engine.set(keys[i].clone(), values[i].to_owned()).await {
                    eprintln!("init set error {}", e);
                }
            }
        });

        let state = Arc::new(AtomicBool::new(true));
        let mut server = KvsServer::new(engine, state.clone());
        rt.spawn(async move {
            if let Err(_) = server
                .start(format!("127.0.0.1:{}", port + thread_num))
                .await
            {
                eprintln!("bind error");
            }
        });

        let client_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(16)
            .enable_all()
            .build()
            .unwrap();

        group.bench_with_input(
            BenchmarkId::new("read_async_sled", thread_num),
            &thread_num,
            |b, &thread_num| {
                b.to_async(&client_rt)
                    .iter(|| async_gets(&keys, &values, port, thread_num))
            },
        );

        rt.block_on(close_server(
            state,
            format!("127.0.0.1:{}", port + thread_num),
        ));

        drop(rt);
        drop(client_rt);
    }

    group.finish();
}

async fn async_sets(
    keys: &Vec<String>,
    values: &Vec<&'static str>,
    port: usize,
    thread_num: usize,
) {
    let wg = WaitGroup::new();
    for i in 0..1000 {
        let wg = wg.clone();
        let key = keys[i].clone();
        let value = values[i].to_owned();
        tokio::spawn(async move {
            let mut client = KvsClient::connect(format!("127.0.0.1:{}", port + thread_num)).await;
            while let Err(e) = client {
                client = KvsClient::connect(format!("127.0.0.1:{}", port + thread_num)).await;
                eprintln!("connect error {}", e);
            }
            let mut client = client.unwrap();
            if let Err(e) = client.set(key.clone(), value.clone()).await {
                eprintln!("set error {}", e);
            }
            drop(wg);
        });
    }
    wg.wait();
}

async fn async_gets(
    keys: &Vec<String>,
    values: &Vec<&'static str>,
    port: usize,
    thread_num: usize,
) {
    let wg = WaitGroup::new();
    for i in 0..1000 {
        let wg = wg.clone();
        let key = keys[i].clone();
        let value = values[i].to_owned();
        tokio::spawn(async move {
            let mut client = KvsClient::connect(format!("127.0.0.1:{}", port + thread_num)).await;
            while let Err(_) = client {
                client = KvsClient::connect(format!("127.0.0.1:{}", port + thread_num)).await;
            }
            let mut client = client.unwrap();
            assert_eq!(client.get(key.clone()).await.unwrap(), Some(value.clone()));
            client.close();
            drop(wg);
        });
    }
    wg.wait();
}

criterion_group!(benches, write, read);
criterion_main!(benches);
