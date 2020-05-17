use clht_rs::HashMap;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use crossbeam::epoch;
use rand::Rng;
use rayon;
use std::sync::Arc;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const ITER: u64 = 24 * 1024 * 10;

fn task_insert_u64_u64(threads: usize) -> HashMap<u64, u64> {
    let map = Arc::new(HashMap::new());
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

    rayon::scope(|s| {
        for t in 1..=(threads as u64) {
            let m = map.clone();
            s.spawn(move |_| {
                let start = t * inc;
                let guard = epoch::pin();
                for i in start..(start + inc) {
                    let v = m.get(&i, &guard);
                    assert_eq!(v, Some(&(i + 7)));
                }
            });
        }
    });

    Arc::try_unwrap(map).unwrap()
}

fn insert_u64_u64(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_clht_u64_u64");
    group.throughput(Throughput::Elements(ITER as u64));

    let num_threads = num_cpus::get();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .unwrap();

    group.bench_function(BenchmarkId::from_parameter("with_reserved_capacity"), |b| {
        pool.install(|| {
            b.iter_batched(
                || Arc::new(HashMap::with_capacity(ITER as usize)),
                |map| {
                    let m = task_insert_u64_u64_same_map(map, num_threads);
                    m
                },
                BatchSize::SmallInput,
            )
        });
    });

    group.bench_function(BenchmarkId::from_parameter("growing"), |b| {
        pool.install(|| {
            b.iter_batched(
                || Arc::new(HashMap::new()),
                |map| {
                    let m = task_insert_u64_u64_same_map(map, num_threads);
                    m
                },
                BatchSize::SmallInput,
            )
        });
    });
    group.finish();
}

fn task_get_u64_u64(threads: usize, map: Arc<HashMap<u64, u64>>) {
    let inc = ITER / (threads as u64);
    rayon::scope(|s| {
        for t in 1..=(threads as u64) {
            let m = map.clone();
            s.spawn(move |_| {
                let start = t * inc;
                let map_ref = m.pin();
                for i in start..(start + inc) {
                    let v = map_ref.get(&i);
                    assert_eq!(v, Some(&(i + 7)));
                }
            });
        }
    });
}

fn get_u64_u64_single_thread(c: &mut Criterion) {
    let map = HashMap::new();
    for i in 0..ITER {
        map.insert(i, i + 7, unsafe { crossbeam::epoch::unprotected() });
    }
    let map_ref = map.pin();
    c.bench_function("get_u64_u64_single_thread", |bencher| {
        bencher.iter_batched(
            || {
                let mut rng = rand::thread_rng();
                rng.gen_range(0, ITER)
            },
            |key| {
                let v = map_ref.get(&key);
                assert_eq!(v, Some(&(key + 7)));
            },
            BatchSize::SmallInput,
        )
    });
}

fn get_u64_u64(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_u64_u64");
    group.throughput(Throughput::Elements(ITER as u64));

    for threads in &[1, 4, 8, 12, 24] {
        let map = Arc::new(task_insert_u64_u64(*threads));
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &threads| {
                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(*threads)
                    .build()
                    .unwrap();
                pool.install(|| b.iter(|| task_get_u64_u64(*threads, map.clone())));
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    get_u64_u64_single_thread,
    insert_u64_u64,
    get_u64_u64
);
criterion_main!(benches);
