use crossbeam::epoch::*;
//use parking_lot::Mutex;
use crate::lock::Mutex;
use std::sync::atomic::{
    AtomicU8,
    Ordering::{Acquire, Release}
};

struct Cell<K, V>(Atomic<(K, V)>);

impl<K, V> Cell<K, V> {
    fn load<'g>(&self, guard: &'g Guard) -> Shared<'g, (K, V)> {
        self.0.load(Acquire, guard)
    }

    fn store(&self, key: K, val: V) {
        self.0.store(Owned::new((key, val)), Release);
    }

    fn new() -> Self {
        Cell(Atomic::null())
    }
}

#[repr(C)]
pub struct Bucket<K, V> {
    lock: Mutex,
    signatures: [AtomicU8; 5],
    cells: [Cell<K, V>; 5],
    next: Atomic<Bucket<K, V>>,
}

impl<K, V> Bucket<K, V>
where
    K: Eq + 'static,
    V: 'static,
{
    pub fn new() -> Self {
        Self {
            lock: Mutex::new(),
            cells: [
                Cell::new(),
                Cell::new(),
                Cell::new(),
                Cell::new(),
                Cell::new(),
                Cell::new(),
            ],
            next: Atomic::null(),
        }
    }

    pub fn insert(&self, key: K, val: V, guard: &Guard) -> bool {
        let _lock = self.lock.lock();
        let mut curr_bucket = self;
        let mut empty_cell = None;
        loop {
            for cell in &curr_bucket.cells {
                let pair = cell.load(&guard);
                let pair_ref = unsafe { pair.as_ref() };
                if let Some(pair) = pair_ref {
                    if pair.0 == key {
                        return false;
                    }
                } else {
                    empty_cell = Some(cell);
                }
            }
            match empty_cell {
                Some(empty_cell) => {
                    empty_cell.store(key, val);
                    return true;
                }
                None => {
                    let next_bucket = curr_bucket.next.load(Acquire, guard);
                    let next_bucket_ref = unsafe { next_bucket.as_ref() };
                    match next_bucket_ref {
                        Some(next) => {
                            curr_bucket = next;
                            continue;
                        }
                        None => {
                            let new_bucket = Bucket::new();
                            new_bucket.cells[0].store(key, val);
                            curr_bucket.next.store(Owned::new(new_bucket), Release);
                            return true;
                        }
                    }
                }
            }
        }
    }

    pub fn find<'g>(&self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        let mut curr_bucket = Some(self);
        loop {
            match curr_bucket {
                Some(curr) => {
                    let cells = &curr.cells;
                    for cell in cells {
                        let shared_cell = cell.load(guard);
                        let pair_ref = unsafe { shared_cell.as_ref() };
                        match pair_ref {
                            Some(pair_ref) => {
                                if pair_ref.0 == *key {
                                    return Some(&pair_ref.1);
                                }
                            }
                            None => {}
                        }
                    }
                    let next_bucket = curr.next.load(Acquire, &guard);
                    curr_bucket = unsafe { next_bucket.as_ref() };
                }
                None => {
                    return None;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_simple_insert() {
        let bucket = Bucket::new();
        let guard = pin();
        assert!(bucket.insert(0, 0, &guard));
        assert_eq!(bucket.find(&0, &guard), Some(&0));
    }

    #[test]
    fn test_insert_when_bucket_full_no_next() {
        let bucket = Bucket::new();
        let guard = pin();
        let pairs = (0..=6).map(|v| (v, v)).collect::<Vec<_>>();
        for pair in pairs.iter().copied() {
            assert!(bucket.insert(pair.0, pair.1, &guard));
        }

        for pair in pairs.iter() {
            assert_eq!(bucket.find(&pair.0, &guard), Some(&pair.1));
        }
    }

    #[test]
    fn test_insert_when_bucket_full_nas_next() {
        let bucket = Bucket::new();
        let guard = pin();
        let pairs = (0..=7).map(|v| (v, v)).collect::<Vec<_>>();
        for pair in pairs.iter().copied() {
            assert!(bucket.insert(pair.0, pair.1, &guard));
        }

        for pair in pairs.iter() {
            assert_eq!(bucket.find(&pair.0, &guard), Some(&pair.1));
        }
    }

    #[test]
    fn test_insert_when_overflow_bucket_was_full() {
        let bucket = Bucket::new();
        let guard = pin();
        let pairs = (0..=20).map(|v| (v, v)).collect::<Vec<_>>();
        for pair in pairs.iter().copied() {
            assert!(bucket.insert(pair.0, pair.1, &guard));
        }

        for pair in pairs.iter() {
            assert_eq!(bucket.find(&pair.0, &guard), Some(&pair.1));
        }
    }

    #[test]
    fn test_cell() {
        struct CellS<K, V>{
            data: Atomic<(K, V)>,
            signature: AtomicU8
        }

        dbg!(std::mem::align_of::<Bucket<u128, u128>>());
        dbg!(std::mem::size_of::<Bucket<u128, u128>>());
    }
}
