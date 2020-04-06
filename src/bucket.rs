use crossbeam::epoch::*;
//use parking_lot::Mutex;
use crate::lock::Mutex;
use std::sync::atomic::{
    AtomicU8,
    Ordering::{Acquire, Release},
};

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

    // TODO: use swap
    fn store(&self, entry: BucketEntry<K, V>) {
        self.signature.store(entry.sign, Release);
        self.cell.store(Owned::new((entry.key, entry.val)), Release);
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

    pub fn insert(&self, new_entry: BucketEntry<K, V>, guard: &Guard) -> bool {
        let _lock = self.lock.lock();
        let mut empty_entry = None;
        let mut last_bucket = self;
        for entry in self.entries(guard) {
            last_bucket = entry.bucket;
            let cell = entry.load_cell(guard);
            let cell_ref = unsafe { cell.as_ref() };

            match cell_ref {
                Some(cell_ref) => {
                    let sign = entry.load_signature();
                    if sign == new_entry.sign {
                        if cell_ref.0 == new_entry.key {
                            return false;
                        }
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
                true
            }
            None => {
                let new_bucket = Bucket::new();
                let entry = new_bucket.entries(guard).next().unwrap();
                entry.store(new_entry);
                last_bucket.next.store(Owned::new(new_bucket), Release);
                true
            }
        }
    }

    pub fn find<'g>(&self, key: &K, signature: u8, guard: &'g Guard) -> Option<&'g V> {
        self.entries(guard).find_map(|entry| {
            let cell = entry.load_cell(guard);
            let cell_ref = unsafe { cell.as_ref() };
            match cell_ref {
                Some(cell_ref) => {
                    let cell_signature = entry.load_signature();
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
        })
    }
}

impl<K, V> Bucket<K, V> {
    fn clean_cells(&mut self) {
        unsafe {
            for cell in &self.cells {
                let pair = cell.load(Acquire, unprotected());
                if let Some(pair_ref) = pair.as_ref() {
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
            let mut current = self.next.load(Acquire, unprotected());
            while let Some(bucket) = current.as_ref() {
                let next = bucket.next.load(Acquire, unprotected());
                let mut owned_bucket = current.into_owned();
                owned_bucket.clean_cells();
                current = next;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_simple_insert() {
        /*let bucket = Bucket::new();
        let guard = pin();
        assert!(bucket.insert(0, 0, &guard));
        assert_eq!(bucket.find(&0, &guard), Some(&0));*/
        unimplemented!()
    }

    #[test]
    fn test_insert_when_bucket_full_no_next() {
        /*let bucket = Bucket::new();
        let guard = pin();
        let pairs = (0..=6).map(|v| (v, v)).collect::<Vec<_>>();
        for pair in pairs.iter().copied() {
            assert!(bucket.insert(pair.0, pair.1, &guard));
        }

        for pair in pairs.iter() {
            assert_eq!(bucket.find(&pair.0, &guard), Some(&pair.1));
        }*/
        unimplemented!()
    }

    #[test]
    fn test_insert_when_bucket_full_nas_next() {
        /*let bucket = Bucket::new();
        let guard = pin();
        let pairs = (0..=7).map(|v| (v, v)).collect::<Vec<_>>();
        for pair in pairs.iter().copied() {
            assert!(bucket.insert(pair.0, pair.1, &guard));
        }

        for pair in pairs.iter() {
            assert_eq!(bucket.find(&pair.0, &guard), Some(&pair.1));
        }*/
    }

    #[test]
    fn test_insert_when_overflow_bucket_was_full() {
        /*let bucket = Bucket::new();
        let guard = pin();
        let pairs = (0..=20).map(|v| (v, v)).collect::<Vec<_>>();
        for pair in pairs.iter().copied() {
            assert!(bucket.insert(pair.0, pair.1, &guard));
        }

        for pair in pairs.iter() {
            assert_eq!(bucket.find(&pair.0, &guard), Some(&pair.1));
        }*/
        unimplemented!()
    }
}
