use bustle::*;
use clht_rs::HashMap as CHLTHashMap;
use crossbeam::epoch;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone)]
struct CLHTTable(std::sync::Arc<CHLTHashMap<u64, u64>>);

impl Collection for CLHTTable {
    type Handle = CLHTTableHandle;
    fn with_capacity(capacity: usize) -> Self {
        Self(std::sync::Arc::new(CHLTHashMap::with_capacity(capacity)))
    }

    fn pin(&self) -> Self::Handle {
        CLHTTableHandle {
            guard: epoch::pin(),
            table: self.clone(),
            num_insertions: AtomicU64::new(0),
        }
    }
}

struct CLHTTableHandle {
    guard: epoch::Guard,
    table: CLHTTable,
    num_insertions: AtomicU64,
}

impl CollectionHandle for CLHTTableHandle {
    type Key = u64;

    fn get(&mut self, key: &Self::Key) -> bool {
        self.table.0.get(key, &self.guard).is_some()
    }

    fn insert(&mut self, key: &Self::Key) -> bool {
        let r = self.table.0.insert(
            *key,
            self.num_insertions.fetch_add(1, Ordering::Relaxed),
            &self.guard,
        );
        r.is_none()
    }

    fn remove(&mut self, key: &Self::Key) -> bool {
        self.table.0.remove(key, &self.guard).is_some()
    }

    fn update(&mut self, key: &Self::Key) -> bool {
        if self.get(key) {
            self.insert(key);
            true
        } else {
            false
        }
    }
}

#[derive(Clone)]
struct FlurryTable(std::sync::Arc<flurry::HashSet<u64>>);

impl Collection for FlurryTable {
    type Handle = FlurryTableHandle;

    fn with_capacity(capacity: usize) -> Self {
        Self(std::sync::Arc::new(flurry::HashSet::with_capacity(
            capacity,
        )))
    }

    fn pin(&self) -> Self::Handle {
        FlurryTableHandle {
            guard: epoch::pin(),
            table: self.clone(),
        }
    }
}

struct FlurryTableHandle {
    guard: epoch::Guard,
    table: FlurryTable,
}

impl CollectionHandle for FlurryTableHandle {
    type Key = u64;

    fn get(&mut self, key: &Self::Key) -> bool {
        self.table.0.get(key, &self.guard).is_some()
    }

    fn insert(&mut self, key: &Self::Key) -> bool {
        self.table.0.insert(*key, &self.guard)
    }

    fn remove(&mut self, key: &Self::Key) -> bool {
        self.table.0.remove(key, &self.guard)
    }

    fn update(&mut self, key: &Self::Key) -> bool {
        if self.get(key) {
            self.insert(key);
            true
        } else {
            false
        }
    }
}

fn main() {
    tracing_subscriber::fmt::init();
    println!("\r\nrunning clht read heavy benchmark");
    for n in 1..=num_cpus::get() {
        Workload::new(n, Mix::read_heavy()).run::<CLHTTable>();
    }

    println!("\r\nrunning flurry read heavy benchmark");
    for n in 1..=num_cpus::get() {
        Workload::new(n, Mix::read_heavy()).run::<FlurryTable>();
    }

    println!("\r\nrunning clht insert_heavy heavy benchmark");
    for n in 1..=num_cpus::get() {
        Workload::new(n, Mix::insert_heavy()).run::<CLHTTable>();
    }

    println!("\r\nrunning flurry insert_heavy heavy benchmark");
    for n in 1..=num_cpus::get() {
        Workload::new(n, Mix::insert_heavy()).run::<FlurryTable>();
    }

    println!("\r\nrunning clht update heavy benchmark");
    for n in 1..=num_cpus::get() {
        Workload::new(n, Mix::update_heavy()).run::<CLHTTable>();
    }

    println!("\r\nrunning flurry update heavy benchmark");
    for n in 1..=num_cpus::get() {
        Workload::new(n, Mix::update_heavy()).run::<FlurryTable>();
    }

    println!("\r\nrunning clht uniform benchmark");
    for n in 1..=num_cpus::get() {
        Workload::new(n, Mix::uniform()).run::<CLHTTable>();
    }

    println!("\r\nrunning flurry uniform benchmark");
    for n in 1..=num_cpus::get() {
        Workload::new(n, Mix::uniform()).run::<FlurryTable>();
    }
}
