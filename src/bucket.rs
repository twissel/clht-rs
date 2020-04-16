use crate::lock::{Mutex, MutexGuard};
use crossbeam::epoch::*;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{
    AtomicU8,
    Ordering::{Acquire, Release},
};

pub const ENTRIES_PER_BUCKET: usize = 5;

pub struct BucketEntry<K, V> {
    pub key: K,
    pub val: V,
    pub sign: u8,
}

#[derive(Clone, Copy)]
pub struct BucketEntryRef<'a, K, V> {
    cell: &'a Atomic<(K, V)>,
    signature: &'a AtomicU8,
    bucket: &'a Bucket<K, V>,
}

impl<'a, K, V> BucketEntryRef<'a, K, V> {
    fn load_signature(&self) -> u8 {
        self.signature.load(Acquire)
    }

    fn load_cell<'g>(&self, guard: &'g Guard) -> Shared<'g, (K, V)> {
        self.cell.load(Acquire, guard)
    }

    fn store(&self, entry: BucketEntry<K, V>) {
        self.signature.store(entry.sign, Release);
        self.cell.store(Owned::new((entry.key, entry.val)), Release);
    }

    fn clear(&self) {
        self.signature.store(0, Release);
        self.cell.store(Shared::null(), Release);
    }
}

pub struct BucketEntryRefIter<'g, K, V> {
    bucket: &'g Bucket<K, V>,
    curr_entry_idx: usize,
    guard: &'g Guard,
}

impl<'g, K, V> Iterator for BucketEntryRefIter<'g, K, V> {
    type Item = BucketEntryRef<'g, K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let len = self.bucket.cells.len();
            if self.curr_entry_idx < len {
                unsafe {
                    let cell = self.bucket.cells.get_unchecked(self.curr_entry_idx);
                    let signature = self.bucket.signatures.get_unchecked(self.curr_entry_idx);
                    self.curr_entry_idx += 1;
                    return Some(BucketEntryRef {
                        cell,
                        signature,
                        bucket: self.bucket,
                    });
                }
            } else {
                let next_bucket = self.bucket.next.load(Acquire, self.guard);
                let next_bucket_ref = unsafe { next_bucket.as_ref() };
                match next_bucket_ref {
                    Some(next) => {
                        self.curr_entry_idx = 0;
                        self.bucket = next;
                        continue;
                    }
                    None => {
                        return None;
                    }
                }
            }
        }
    }
}

#[repr(C)]
pub struct Bucket<K, V> {
    lock: Mutex,
    signatures: [AtomicU8; 5],
    _pad: u64,
    cells: [Atomic<(K, V)>; 5],
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
                Atomic::null(),
                Atomic::null(),
                Atomic::null(),
                Atomic::null(),
                Atomic::null(),
            ],
            signatures: [
                AtomicU8::new(0),
                AtomicU8::new(0),
                AtomicU8::new(0),
                AtomicU8::new(0),
                AtomicU8::new(0),
            ],
            next: Atomic::null(),
            _pad: 0,
        }
    }

    pub fn entries<'g>(
        &'g self,
        guard: &'g Guard,
    ) -> impl Iterator<Item = BucketEntryRef<'g, K, V>> {
        BucketEntryRefIter {
            bucket: self,
            curr_entry_idx: 0,
            guard,
        }
    }

    pub fn find<'g>(&self, key: &K, signature: u8, guard: &'g Guard) -> Option<&'g V> {
        self.entries(guard).find_map(|entry| {
            let cell_signature = entry.load_signature();
            if cell_signature == signature {
                let cell = entry.load_cell(guard);
                let cell_ref = unsafe { cell.as_ref() };
                match cell_ref {
                    Some(cell_ref) => {
                        if cell_signature == signature {
                            if *key == cell_ref.0 {
                                Some(&cell_ref.1)
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    None => None,
                }
            } else {
                None
            }
        })
    }

    pub fn write(&self) -> WriteGuard<'_, K, V> {
        let guard = self.lock.lock();
        WriteGuard {
            _guard: guard,
            bucket: self,
        }
    }

    pub fn stats(&self, guard: &Guard) -> Stats {
        let mut stats = Stats::default();

        let mut cur_bucket = Some(self);
        while let Some(bucket) = cur_bucket {
            for cell in &bucket.cells {
                // as we don't touch cell data, we can use relaxed ordering here
                let pair = cell.load(Relaxed, guard);
                if !pair.is_null() {
                    stats.num_occupied += 1;
                }
            }

            let next_bucket = bucket.next.load(Acquire, guard);
            if !next_bucket.is_null() {
                stats.num_overflows += 1;
            }

            // safe as we used Acquire on bucket load
            cur_bucket = unsafe { next_bucket.as_ref() };
        }
        stats
    }
}

#[derive(Default, Clone, Copy, Debug, Eq, PartialEq)]
pub struct Stats {
    pub num_overflows: usize,
    pub num_occupied: usize,
}

impl<K, V> Bucket<K, V> {
    fn clean_cells(&mut self) {
        unsafe {
            for cell in &self.cells {
                let pair = cell.load(Acquire, unprotected());
                if pair.as_ref().is_some() {
                    let _ = pair.into_owned();
                }
            }
        }
    }
}

impl<K, V> Drop for Bucket<K, V> {
    fn drop(&mut self) {
        unsafe {
            self.clean_cells();
            let next = self.next.load(Acquire, unprotected());
            if next.as_ref().is_some() {
                let _ = next.into_owned();
            }
        }
    }
}

pub struct WriteGuard<'a, K, V> {
    _guard: MutexGuard<'a>,
    bucket: &'a Bucket<K, V>,
}

impl<'a, K, V> WriteGuard<'a, K, V>
where
    K: Eq + 'static,
    V: 'static,
{
    pub fn insert<'g>(
        &self,
        new_entry: BucketEntry<K, V>,
        guard: &'g Guard,
    ) -> UpdateResult<'g, K, V> {
        let mut empty_entry = None;
        let mut last_bucket = self.bucket;
        for entry in self.bucket.entries(guard) {
            last_bucket = entry.bucket;
            let cell = entry.load_cell(guard);
            let cell_ref = unsafe { cell.as_ref() };

            match cell_ref {
                Some(cell_ref) => {
                    let sign = entry.load_signature();
                    if sign == new_entry.sign && cell_ref.0 == new_entry.key {
                        entry.store(new_entry);
                        unsafe {
                            guard.defer_destroy(cell);
                        }
                        return UpdateResult::Updated(Some(cell_ref));
                    }
                }
                None => {
                    if empty_entry.is_none() {
                        empty_entry = Some(entry);
                    }
                }
            }
        }

        match empty_entry {
            Some(entry) => {
                entry.store(new_entry);
                UpdateResult::Updated(None)
            }
            None => {
                let new_bucket = Bucket::new();
                let entry = new_bucket.entries(guard).next().unwrap();
                entry.store(new_entry);
                last_bucket.next.store(Owned::new(new_bucket), Release);
                UpdateResult::UpdatedWithOverflow
            }
        }
    }

    pub fn remove<'g>(&self, key: &K, key_sign: u8, guard: &'g Guard) -> Option<&'g V> {
        for entry in self.bucket.entries(guard) {
            let cell = entry.load_cell(guard);
            let cell_ref = unsafe { cell.as_ref() };

            match cell_ref {
                Some(cell_ref) => {
                    let entry_sign = entry.load_signature();
                    if entry_sign == key_sign && cell_ref.0 == *key {
                        entry.clear();
                        unsafe {
                            guard.defer_destroy(cell);
                        }
                        return Some(&cell_ref.1);
                    }
                }
                None => {
                    continue;
                }
            }
        }
        None
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum UpdateResult<'g, K, V> {
    Updated(Option<&'g (K, V)>),
    UpdatedWithOverflow,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_when_bucket_full_no_next() {
        let bucket = Bucket::new();
        let guard = pin();
        let pairs = (0..=5).map(|v| (v, v)).collect::<Vec<_>>();
        for (idx, pair) in pairs.iter().copied().enumerate() {
            let w = bucket.write();
            if idx < 5 {
                assert_eq!(
                    w.insert(
                        BucketEntry {
                            key: pair.0,
                            val: pair.1,
                            sign: 0
                        },
                        &guard
                    ),
                    UpdateResult::Updated(None)
                );
            } else {
                assert_eq!(
                    w.insert(
                        BucketEntry {
                            key: pair.0,
                            val: pair.1,
                            sign: 0
                        },
                        &guard
                    ),
                    UpdateResult::UpdatedWithOverflow
                );
            }
        }

        for pair in pairs.iter() {
            assert_eq!(bucket.find(&pair.0, 0, &guard), Some(&pair.1));
        }

        let stats = bucket.stats(&guard);
        assert_eq!(stats, Stats {
            num_overflows: 1,
            num_occupied: 6
        })
    }

    #[test]
    fn test_insert_when_bucket_full_nas_next() {
        let bucket = Bucket::new();
        let guard = pin();
        let pairs = (0..=6).map(|v| (v, v)).collect::<Vec<_>>();
        for (idx, pair) in pairs.iter().copied().enumerate() {
            let w = bucket.write();
            if idx != 5 {
                assert_eq!(
                    w.insert(
                        BucketEntry {
                            key: pair.0,
                            val: pair.1,
                            sign: 0
                        },
                        &guard
                    ),
                    UpdateResult::Updated(None)
                );
            } else {
                assert_eq!(
                    w.insert(
                        BucketEntry {
                            key: pair.0,
                            val: pair.1,
                            sign: 0
                        },
                        &guard
                    ),
                    UpdateResult::UpdatedWithOverflow
                );
            }
        }

        for pair in pairs.iter() {
            assert_eq!(bucket.find(&pair.0, 0, &guard), Some(&pair.1));
        }

        let stats = bucket.stats(&guard);
        assert_eq!(stats, Stats {
            num_overflows: 1,
            num_occupied: 7
        })
    }

    #[test]
    fn test_insert_when_overflow_bucket_was_full() {
        let bucket = Bucket::new();
        let guard = pin();
        let pairs = (0..=20).map(|v| (v, v)).collect::<Vec<_>>();
        for pair in pairs.iter().copied() {
            let w = bucket.write();
            w.insert(
                BucketEntry {
                    key: pair.0,
                    val: pair.1,
                    sign: 0,
                },
                &guard,
            );
        }

        for pair in pairs.iter() {
            assert_eq!(bucket.find(&pair.0, 0, &guard), Some(&pair.1));
        }
    }
}
