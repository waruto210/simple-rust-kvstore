use criterion::{criterion_group, criterion_main, BatchSize, Criterion, SamplingMode};
use kvs::{KvStore, KvsEngine, SledKvsEngine};
use rand::prelude::*;
use tempfile::TempDir;
const SEED: u64 = 42;

fn set_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_bench");
    group.significance_level(0.1).sample_size(10);

    group.sampling_mode(SamplingMode::Flat);
    group.bench_function("kvs", |b| {
        b.iter_batched(
            || {
                // the setup step need to prepare all the data and object
                let temp_dir = TempDir::new().unwrap();
                let mut rng = SmallRng::seed_from_u64(SEED);
                let k_chars = ['k'; 100000];
                let v_chars = ['v'; 100000];
                let mut keys = Vec::with_capacity(100);
                let mut values = Vec::with_capacity(100);
                for _i in 0..100 {
                    let index = rng.gen_range(1..=100000);
                    keys.push(k_chars[0..index].iter().collect::<String>());
                    values.push(v_chars[0..index].iter().collect::<String>());
                }
                (
                    KvStore::open(temp_dir.path()).unwrap(),
                    temp_dir,
                    keys,
                    values,
                )
            },
            |(mut store, _temp_dir, mut keys, mut values)| {
                for _i in 0..100 {
                    store
                        .set(keys.pop().unwrap(), values.pop().unwrap())
                        .unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("sled", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let mut rng = SmallRng::seed_from_u64(SEED);
                let k_chars = ['k'; 100000];
                let v_chars = ['v'; 100000];
                let mut keys = Vec::with_capacity(100);
                let mut values = Vec::with_capacity(100);
                for _i in 0..100 {
                    let index = rng.gen_range(1..=100000);
                    keys.push(k_chars[0..index].iter().collect::<String>());
                    values.push(v_chars[0..index].iter().collect::<String>());
                }
                (
                    SledKvsEngine::open(temp_dir.path()).unwrap(),
                    temp_dir,
                    keys,
                    values,
                )
            },
            |(mut db, _temp_dir, mut keys, mut values)| {
                for _i in 0..100 {
                    db.set(keys.pop().unwrap(), values.pop().unwrap()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn get_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_bench");
    group.significance_level(0.1).sample_size(10);
    group.sampling_mode(SamplingMode::Flat);

    group.bench_function("kvs", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let mut rng = SmallRng::seed_from_u64(SEED);
                let k_chars = ['k'; 100000];
                let v_chars = ['v'; 100000];
                let mut keys = Vec::with_capacity(1000);
                let mut values = Vec::with_capacity(1000);
                for _i in 0..1000 {
                    let index = rng.gen_range(1..=100000);
                    keys.push(k_chars[0..index].iter().collect::<String>());
                    values.push(v_chars[0..index].iter().collect::<String>());
                }
                let mut store = KvStore::open(temp_dir.path()).unwrap();
                for i in 0..1000 {
                    store.set(keys[i].clone(), values[i].clone()).unwrap();
                }
                (store, temp_dir, keys, values)
            },
            |(mut store, _temp_dir, mut keys, mut values)| {
                for _i in 0..1000 {
                    let value = store.get(keys.pop().unwrap()).unwrap();
                    assert_eq!(value, values.pop());
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("sled", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let mut rng = SmallRng::seed_from_u64(SEED);
                let k_chars = ['k'; 100000];
                let v_chars = ['v'; 100000];
                let mut keys = Vec::with_capacity(1000);
                let mut values = Vec::with_capacity(1000);
                for _i in 0..1000 {
                    let index = rng.gen_range(1..=100000);
                    keys.push(k_chars[0..index].iter().collect::<String>());
                    values.push(v_chars[0..index].iter().collect::<String>());
                }
                let mut db = SledKvsEngine::open(temp_dir.path()).unwrap();
                for i in 0..1000 {
                    db.set(keys[i].clone(), values[i].clone()).unwrap();
                }
                (db, temp_dir, keys, values)
            },
            |(mut db, _temp_dir, mut keys, mut values)| {
                for _i in 0..100 {
                    let value = db.get(keys.pop().unwrap()).unwrap();
                    assert_eq!(value, values.pop());
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(benches, set_bench, get_bench);
criterion_main!(benches);
