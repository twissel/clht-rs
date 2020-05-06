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
    bucket: &'g Bucket<K, V>,
    bucket_pos: usize,
    _mut_mark: PhantomData<M>,
    guard: &'g Guard,
}

impl<'g, K, V, M> EntryRef<'g, K, V, M> {
    fn load_signature(&self) -> u8 {
        self.signature().load(Acquire)
    }

    fn load_key_value(&self) -> Shared<'g, (K, V)> {
        self.cell().load(Acquire, self.guard)
    }

    fn cell(&self) -> &Atomic<(K, V)> {
        unsafe { self.bucket.cells.get_unchecked(self.bucket_pos) }
    }

    fn signature(&self) -> &AtomicU8 {
        unsafe { self.bucket.signatures.get_unchecked(self.bucket_pos) }
    }
}

impl<'g, K, V> EntryRef<'g, K, V, Mut_> {
    fn store(&self, pair: Pair<K, V>) {
        match pair {
            Pair::Owned { pair, sign } => {
                self.cell().store(Owned::new(pair), Release);
                self.signature().store(sign, Release);
            }
            Pair::Shared { sign, pair } => {
                self.cell().store(pair, Release);
                self.signature().store(sign, Release);
            }
        };
    }

    fn clear(&self) {
        self.cell().store(Shared::null(), Release);
        self.signature().store(0, Release);
    }

    pub fn swap_with_null(&self) -> Shared<'g, (K, V)> {
        let old = self.cell().swap(Shared::null(), Release, self.guard);
        self.signature().store(0, Release);
        old
    }
}

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
}

#[repr(C)]
pub struct Bucket<K, V> {
    // 1 byte
    lock: Mutex<()>,
    // 5 bytes
    signatures: [AtomicU8; ENTRIES_PER_BUCKET],
    _pad: u64,
    // 40 bytes
    cells: [Atomic<(K, V)>; ENTRIES_PER_BUCKET],
    // 8 bytes
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
            _pad: 0,
        }
    }
}

impl<K, V> Bucket<K, V>
where
    K: 'static,
    V: 'static,
{
    pub fn entries<'g>(&'g self, guard: &'g Guard) -> EntryRefIter<'g, K, V, Shared_> {
        EntryRefIter {
            bucket: self,
            curr_entry_idx: 0,
            guard,
            mut_mark: PhantomData,
        }
    }

    pub fn find<'g, Q>(&'g self, key: &Q, signature: u8, guard: &'g Guard) -> Option<(&'g K, &'g V)>
    where
        Q: ?Sized + Hash + Eq,
        K: Borrow<Q>,
    {
        self.entries(guard).find_map(|entry| {
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

    pub unsafe fn entries_mut(&self) -> EntryRefIter<K, V, Mut_> {
        EntryRefIter {
            bucket: self,
            curr_entry_idx: 0,
            guard: unprotected(),
            mut_mark: PhantomData,
        }
    }

    pub fn transfer(&mut self, pair: Shared<(K, V)>, sign: u8) -> bool {
        let mut last_bucket = &*self;
        let entries = unsafe { self.entries_mut() };
        for entry_ref in entries {
            last_bucket = entry_ref.bucket;
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
        let new_bucket = Bucket::new();
        let entry_ref = unsafe { new_bucket.entries_mut().next().unwrap() };
        entry_ref.store(Pair::Shared { pair, sign });
        last_bucket.next.store(Owned::new(new_bucket), Release);
        true
    }

    pub fn stats(&self, guard: &Guard) -> Stats {
        let mut last_bucket = self;
        let mut stats = Stats {
            num_overflows: 0,
            num_non_empty: 0,
        };
        for entry_ref in self.entries(guard) {
            if entry_ref.bucket as *const _ != last_bucket as *const _ {
                last_bucket = entry_ref.bucket;
                stats.num_overflows += 1;
            }
            let pair = entry_ref.load_key_value();
            if !pair.is_null() {
                stats.num_non_empty += 1;
            }
        }
        stats
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
        for entry in self.entries_mut() {
            last_bucket = entry.bucket;
            let cell = entry.load_key_value();
            let cell_ref = unsafe { cell.as_ref() };
            match cell_ref {
                Some(cell_ref) => {
                    let sign = entry.load_signature();
                    if sign == pair_sign && cell_ref.0 == pair.0 {
                        entry.store(Pair::Owned { pair, sign });
                        unsafe {
                            self.guard.defer_destroy(cell);
                        }
                        return InsertResult::Inserted(Some(cell_ref));
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
                entry.store(Pair::Owned {
                    pair,
                    sign: pair_sign,
                });
                InsertResult::Inserted(None)
            }
            None => {
                let new_bucket = Bucket::new();
                let entry = unsafe { new_bucket.entries_mut().next().unwrap() };

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

    pub fn entries_mut(&self) -> EntryRefIter<'g, K, V, Mut_> {
        EntryRefIter {
            bucket: self.bucket,
            curr_entry_idx: 0,
            mut_mark: PhantomData,
            guard: self.guard,
        }
    }

    pub fn stats(&self, guard: &Guard) -> Stats {
        self.bucket.stats(guard)
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
        assert_eq!(std::mem::size_of::<Bucket<u32, u32>>(), 64);
        assert_eq!(std::mem::align_of::<[AtomicU8; ENTRIES_PER_BUCKET]>(), 1);
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
