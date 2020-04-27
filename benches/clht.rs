use clht_rs::HashMap;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use crossbeam::epoch;
use rayon;
use std::sync::Arc;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const ITER: u64 = 32 * 1024;

fn task_insert_u64_u64(threads: usize) -> HashMap<u64, u64> {
    let map = Arc::new(HashMap::with_pow_buckets(18));
    let inc = ITER / (threads as u64);

    rayon::scope(|s| {
        for t in 1..=(threads as u64) {
            let m = map.clone();
            s.spawn(move |_| {
                let start = t * inc;
                let guard = epoch::pin();
                for i in start..(start + inc) {
                    assert_eq!(m.insert(i, i + 7, &guard), None);
                }
            });
        }
    });

    Arc::try_unwrap(map).unwrap()
}

fn task_insert_u64_u64_same_map(map: Arc<HashMap<u64, u64>>, threads: usize) -> HashMap<u64, u64> {
    let inc = ITER / (threads as u64);

    rayon::scope(|s| {
        for t in 1..=(threads as u64) {
            let m = map.clone();
            s.spawn(move |_| {
                let start = t * inc;
                let guard = epoch::pin();
                let end = start + inc;
                for i in start..end {
                    assert_eq!(m.insert(i, i + 7, &guard), None);
                }
            });
        }
    });

    Arc::try_unwrap(map).unwrap()
}

fn insert_u64_u64(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_clht_u64_u64");
    group.throughput(Throughput::Elements(ITER as u64));
    let max = num_cpus::get();

    for threads in max..=max {
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &threads| {
                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .unwrap();
                pool.install(|| {
                    b.iter_batched(
                        || Arc::new(HashMap::with_pow_buckets(12)),
                        |map| {
                            let m = task_insert_u64_u64_same_map(map, threads);
                            m
                        },
                        BatchSize::SmallInput,
                    )
                });
            },
        );
    }
    group.finish();
}

fn task_get_u64_u64(threads: usize, map: Arc<HashMap<u64, u64>>) {
    let inc = ITER / (threads as u64);

    rayon::scope(|s| {
        for t in 1..=(threads as u64) {
            let m = map.clone();
            s.spawn(move |_| {
                let start = t * inc;
                let guard = epoch::pin();
                for i in start..(start + inc) {
                    if let Some(&v) = m.get(&i, &guard) {
                        assert_eq!(v, i + 7);
                    }
                }
            });
        }
    });
}

fn get_u64_u64(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_u64_u64");
    group.throughput(Throughput::Elements(ITER as u64));
    let max = num_cpus::get();

    for threads in max..=max {
        let map = Arc::new(task_insert_u64_u64(threads));

        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &threads| {
                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .unwrap();
                pool.install(|| b.iter(|| task_get_u64_u64(threads, map.clone())));
            },
        );
    }

    group.finish();
}

criterion_group!(benches, insert_u64_u64, get_u64_u64);
criterion_main!(benches);
