use crossbeam::epoch::*;
use parking_lot::{Mutex, MutexGuard};
use std::borrow::Borrow;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::AddAssign;
use std::sync::atomic::{
    AtomicU8,
    Ordering::{Acquire, Release},
};

pub const ENTRIES_PER_BUCKET: usize = 5;

pub enum Pair<'g, K, V> {
    Shared { pair: Shared<'g, (K, V)>, sign: u8 },
    Owned { pair: (K, V), sign: u8 },
}

#[derive(Clone, Copy)]
pub struct Mut_;

#[derive(Clone, Copy)]
pub struct Shared_;

#[derive(Clone, Copy)]
pub struct EntryRef<'g, K, V, M> {
    cell: &'g Atomic<(K, V)>,
    signature: &'g AtomicU8,
    _mut_mark: PhantomData<M>,
    guard: &'g Guard,
}

impl<'g, K, V, M> EntryRef<'g, K, V, M> {
    fn load_signature(&self) -> u8 {
        self.signature.load(Acquire)
    }

    fn load_key_value(&self) -> Shared<'g, (K, V)> {
        self.cell.load(Acquire, self.guard)
    }
}

impl<'g, K, V> EntryRef<'g, K, V, Mut_> {
    fn store(&self, pair: Pair<K, V>) {
        match pair {
            Pair::Owned { pair, sign } => {
                self.cell.store(Owned::new(pair), Release);
                self.signature.store(sign, Release);
            }
            Pair::Shared { sign, pair } => {
                self.cell.store(pair, Release);
                self.signature.store(sign, Release);
            }
        };
    }

    fn clear(&self) {
        self.cell.store(Shared::null(), Release);
        self.signature.store(0, Release);
    }

    pub fn swap_with_null(&self) -> Shared<'g, (K, V)> {
        let old = self.cell.swap(Shared::null(), Release, self.guard);
        self.signature.store(0, Release);
        old
    }
}

/*
pub struct EntryRefIter<'g, K, V, M> {
    bucket: &'g Bucket<K, V>,
    curr_entry_idx: usize,
    guard: &'g Guard,
    mut_mark: PhantomData<M>,
}

impl<'g, K, V, M> Iterator for EntryRefIter<'g, K, V, M>
where
    M: Copy,
{
    type Item = EntryRef<'g, K, V, M>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let len = self.bucket.cells.len();
            if self.curr_entry_idx < len {
                let entry = Some(EntryRef {
                    bucket: self.bucket,
                    bucket_pos: self.curr_entry_idx,
                    _mut_mark: PhantomData,
                    guard: self.guard,
                });
                self.curr_entry_idx += 1;
                return entry;
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
}*/

#[repr(align(64))]
pub struct Bucket<K, V> {
    lock: Mutex<()>,
    signatures: [AtomicU8; ENTRIES_PER_BUCKET],
    cells: [Atomic<(K, V)>; ENTRIES_PER_BUCKET],
    next: Atomic<Bucket<K, V>>,
}

impl<K, V> Bucket<K, V> {
    pub fn new() -> Self {
        Self {
            lock: Mutex::new(()),
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
        }
    }
}

impl<K, V> Bucket<K, V>
where
    K: 'static,
    V: 'static,
{
    #[inline]
    pub fn entries_with_overflow<'g, M: 'static>(
        &'g self,
        guard: &'g Guard,
    ) -> impl Iterator<Item = EntryRef<'g, K, V, M>> + 'g {
        self.iter(guard)
            .flat_map(move |bucket| bucket.entries(guard))
    }

    #[inline]
    pub fn entries<'g, M: 'static>(
        &'g self,
        guard: &'g Guard,
    ) -> impl Iterator<Item = EntryRef<'g, K, V, M>> + 'g {
        struct EntryRefIter<'g, K, V, M> {
            curr: usize,
            bucket: &'g Bucket<K, V>,
            mark: PhantomData<M>,
            guard: &'g Guard,
        }

        impl<'g, K, V, M> Iterator for EntryRefIter<'g, K, V, M> {
            type Item = EntryRef<'g, K, V, M>;

            fn next(&mut self) -> Option<Self::Item> {
                if self.curr < ENTRIES_PER_BUCKET {
                    unsafe {
                        let cell = self.bucket.cells.get_unchecked(self.curr);
                        let signature = self.bucket.signatures.get_unchecked(self.curr);
                        let entry_ref = Some(EntryRef {
                            cell,
                            signature,
                            _mut_mark: PhantomData,
                            guard: self.guard,
                        });
                        self.curr += 1;
                        entry_ref
                    }
                } else {
                    None
                }
            }
        }

        EntryRefIter {
            curr: 0,
            bucket: self,
            mark: PhantomData,
            guard,
        }

        // TODO: somehow this is twice slower than handwritten version, investigate why
        /*self.cells
        .iter()
        .zip(&self.signatures)
        .map(move |(cell, signature)| EntryRef {
            cell,
            signature,
            guard,
            _mut_mark: PhantomData,
        })*/
    }

    pub fn find<'g, Q>(&'g self, key: &Q, signature: u8, guard: &'g Guard) -> Option<(&'g K, &'g V)>
    where
        Q: ?Sized + Hash + Eq,
        K: Borrow<Q>,
    {
        self.entries_with_overflow(guard)
            .find_map(|entry: EntryRef<'g, K, V, Shared_>| {
                let cell_signature = entry.load_signature();
                if cell_signature == signature {
                    let cell = entry.load_key_value();
                    let cell_ref = unsafe { cell.as_ref() };
                    match cell_ref {
                        Some(cell_ref) => {
                            if key == cell_ref.0.borrow() {
                                Some((&cell_ref.0, &cell_ref.1))
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

    pub fn write<'g>(&'g self, guard: &'g Guard) -> WriteGuard<'g, K, V> {
        let lock_guard = self.lock.lock();
        WriteGuard {
            _lock_guard: lock_guard,
            bucket: self,
            guard,
        }
    }

    pub fn transfer(&mut self, pair: Shared<(K, V)>, sign: u8) -> bool {
        let mut last_bucket = &*self;
        let guard = unsafe { unprotected() };
        for bucket in self.iter(guard) {
            last_bucket = bucket;
            for entry_ref in bucket.entries(guard) {
                let cell = entry_ref.load_key_value();
                let cell_ref = unsafe { cell.as_ref() };

                match cell_ref {
                    Some(_) => {}
                    None => {
                        entry_ref.store(Pair::Shared { pair, sign });
                        return false;
                    }
                }
            }
        }

        let new_bucket = Bucket::new();
        let entry_ref = new_bucket.entries::<Mut_>(guard).next().unwrap();
        entry_ref.store(Pair::Shared { pair, sign });
        last_bucket.next.store(Owned::new(new_bucket), Release);
        true
    }

    pub fn stats(&self, guard: &Guard) -> Stats {
        let mut stats = Stats {
            num_overflows: 0,
            num_non_empty: 0,
        };
        for bucket in self.iter(guard) {
            stats.num_overflows += 1;
            for entry_ref in bucket.entries::<Shared_>(guard) {
                let pair = entry_ref.load_key_value();
                if !pair.is_null() {
                    stats.num_non_empty += 1;
                }
            }
        }
        stats.num_overflows -= 1;
        stats
    }

    pub fn iter<'g>(&'g self, guard: &'g Guard) -> BucketIter<'g, K, V> {
        BucketIter {
            current: Some(self),
            guard,
        }
    }
}

pub struct BucketIter<'g, K, V> {
    current: Option<&'g Bucket<K, V>>,
    guard: &'g Guard,
}

impl<'g, K, V> Iterator for BucketIter<'g, K, V> {
    type Item = &'g Bucket<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current {
            Some(current) => {
                let next = current.next.load(Acquire, self.guard);
                self.current = unsafe { next.as_ref() };
                Some(current)
            }
            None => None,
        }
    }
}

#[derive(Debug)]
pub struct Stats {
    pub num_non_empty: usize,
    pub num_overflows: usize,
}

impl Stats {
    pub fn zeroed() -> Self {
        Self {
            num_overflows: 0,
            num_non_empty: 0,
        }
    }
}

impl AddAssign for Stats {
    fn add_assign(&mut self, rhs: Self) {
        self.num_non_empty += rhs.num_non_empty;
        self.num_overflows += rhs.num_overflows;
    }
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

pub struct WriteGuard<'g, K, V> {
    _lock_guard: MutexGuard<'g, ()>,
    guard: &'g Guard,
    bucket: &'g Bucket<K, V>,
}

impl<'g, K, V> WriteGuard<'g, K, V>
where
    K: Eq + 'static,
    V: 'static,
{
    pub fn insert(&self, pair: (K, V), pair_sign: u8) -> InsertResult<'g, K, V> {
        let mut empty_entry = None;
        let mut last_bucket = self.bucket;
        for bucket in self.bucket.iter(self.guard) {
            last_bucket = bucket;
            for entry_ref in bucket.entries::<Mut_>(self.guard) {
                let cell = entry_ref.load_key_value();
                let cell_ref = unsafe { cell.as_ref() };
                match cell_ref {
                    Some(cell_ref) => {
                        let sign = entry_ref.load_signature();
                        if sign == pair_sign && cell_ref.0 == pair.0 {
                            entry_ref.store(Pair::Owned { pair, sign });
                            unsafe {
                                self.guard.defer_destroy(cell);
                            }
                            return InsertResult::Inserted(Some(cell_ref));
                        }
                    }
                    None => {
                        if empty_entry.is_none() {
                            empty_entry = Some(entry_ref);
                        }
                    }
                }
            }
        }

        match empty_entry {
            Some(entry) => {
                entry.store(Pair::Owned {
                    pair,
                    sign: pair_sign,
                });
                InsertResult::Inserted(None)
            }
            None => {
                let new_bucket = Bucket::new();
                let entry = new_bucket.entries::<Mut_>(self.guard).next().unwrap();

                entry.store(Pair::Owned {
                    pair,
                    sign: pair_sign,
                });
                last_bucket.next.store(Owned::new(new_bucket), Release);
                InsertResult::InsertedWithOverflow
            }
        }
    }

    pub fn remove_entry<Q>(&self, key: &Q, key_sign: u8) -> Option<(&'g K, &'g V)>
    where
        Q: ?Sized + Hash + Eq,
        K: Borrow<Q>,
    {
        for entry in self.entries_mut() {
            let cell = entry.load_key_value();
            let cell_ref = unsafe { cell.as_ref() };

            match cell_ref {
                Some(cell_ref) => {
                    let entry_sign = entry.load_signature();
                    if entry_sign == key_sign && cell_ref.0.borrow() == key {
                        entry.clear();
                        unsafe {
                            self.guard.defer_destroy(cell);
                        }
                        return Some((&cell_ref.0, &cell_ref.1));
                    }
                }
                None => {
                    continue;
                }
            }
        }
        None
    }

    pub fn entries_mut(&self) -> impl Iterator<Item = EntryRef<'g, K, V, Mut_>> {
        self.bucket.entries_with_overflow(self.guard)
    }

    pub fn stats(&self, guard: &Guard) -> Stats {
        self.bucket.stats(guard)
    }

    pub fn find<Q>(&self, key: &Q, signature: u8) -> Option<(&K, &V)>
    where
        Q: ?Sized + Hash + Eq,
        K: Borrow<Q>,
    {
        self.bucket.find(key, signature, &self.guard)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum InsertResult<'g, K, V> {
    Inserted(Option<&'g (K, V)>),
    InsertedWithOverflow,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_shape() {
        let array: [Bucket<u64, u64>; 2] = [Bucket::new(), Bucket::new()];
        let addr1 = &array[0] as *const Bucket<u64, u64> as usize;
        let addr2 = &array[1] as *const Bucket<u64, u64> as usize;
        assert_eq!(addr2 - addr1, 64);
        assert_eq!(addr1 % 64, 0);
        assert_eq!(addr2 % 64, 0);
    }

    #[test]
    fn test_insert_when_bucket_full_no_next() {
        let bucket = Bucket::new();
        let guard = pin();
        let pairs = (0..=5).map(|v| (v, v)).collect::<Vec<_>>();
        for (idx, pair) in pairs.iter().copied().enumerate() {
            let w = bucket.write(&guard);
            if idx < 5 {
                assert_eq!(w.insert(pair, 0), InsertResult::Inserted(None));
            } else {
                assert_eq!(w.insert(pair, 0), InsertResult::InsertedWithOverflow);
            }
        }

        for pair in pairs.iter() {
            assert_eq!(bucket.find(&pair.0, 0, &guard), Some((&pair.0, &pair.1)));
        }
    }

    #[test]
    fn test_insert_when_bucket_full_nas_next() {
        let bucket = Bucket::new();
        let guard = pin();
        let pairs = (0..=6).map(|v| (v, v)).collect::<Vec<_>>();
        for (idx, pair) in pairs.iter().copied().enumerate() {
            let w = bucket.write(&guard);
            if idx != 5 {
                assert_eq!(w.insert(pair, 0), InsertResult::Inserted(None));
            } else {
                assert_eq!(w.insert(pair, 0), InsertResult::InsertedWithOverflow);
            }
        }

        for pair in pairs.iter() {
            assert_eq!(bucket.find(&pair.0, 0, &guard), Some((&pair.0, &pair.1)));
        }
    }

    #[test]
    fn test_insert_when_overflow_bucket_was_full() {
        let bucket = Bucket::new();
        let guard = pin();
        let pairs = (0..=20).map(|v| (v, v)).collect::<Vec<_>>();
        for pair in pairs.iter().copied() {
            let w = bucket.write(&guard);
            w.insert(pair, 0);
        }

        for pair in pairs.iter() {
            assert_eq!(bucket.find(&pair.0, 0, &guard), Some((&pair.0, &pair.1)));
        }
    }
}
