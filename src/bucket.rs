use crossbeam::epoch::*;
use parking_lot::{Mutex, MutexGuard};
use std::borrow::Borrow;
use std::hash::Hash;
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
pub struct EntryRef<'a, K, V, M> {
    cell: &'a Atomic<(K, V)>,
    signature: &'a AtomicU8,
    bucket: &'a Bucket<K, V>,
    _mut_mark: M,
}

impl<'a, K, V, M> EntryRef<'a, K, V, M> {
    fn load_signature(&self) -> u8 {
        self.signature.load(Acquire)
    }

    fn load_key_value<'g>(&self, guard: &'g Guard) -> Shared<'g, (K, V)> {
        self.cell.load(Acquire, guard)
    }
}

impl<'a, K, V> EntryRef<'a, K, V, Mut_> {
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

    pub fn swap_with_null<'g>(&self, guard: &'g Guard) -> Shared<'g, (K, V)> {
        let old = self.cell.swap(Shared::null(), Release, guard);
        self.signature.store(0, Release);
        old
    }
}

pub struct EntryRefIter<'g, K, V, M> {
    bucket: &'g Bucket<K, V>,
    curr_entry_idx: usize,
    guard: &'g Guard,
    mut_mark: M,
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
                unsafe {
                    let cell = self.bucket.cells.get_unchecked(self.curr_entry_idx);
                    let signature = self.bucket.signatures.get_unchecked(self.curr_entry_idx);
                    self.curr_entry_idx += 1;
                    return Some(EntryRef {
                        cell,
                        signature,
                        bucket: self.bucket,
                        _mut_mark: self.mut_mark,
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
    lock: Mutex<()>,
    signatures: [AtomicU8; ENTRIES_PER_BUCKET],
    _pad: u64,
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
            _pad: 0,
        }
    }
}

impl<K, V> Bucket<K, V>
where
    K: Eq + 'static,
    V: 'static,
{
    pub fn entries<'g>(&'g self, guard: &'g Guard) -> EntryRefIter<'g, K, V, Shared_> {
        EntryRefIter {
            bucket: self,
            curr_entry_idx: 0,
            guard,
            mut_mark: Shared_,
        }
    }

    pub fn find<'g, Q>(&self, key: &Q, signature: u8, guard: &'g Guard) -> Option<(&'g K, &'g V)>
    where
        Q: ?Sized + Hash + Eq,
        K: Borrow<Q>,
    {
        self.entries(guard).find_map(|entry| {
            let cell_signature = entry.load_signature();
            if cell_signature == signature {
                let cell = entry.load_key_value(guard);
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

    pub fn write(&self) -> WriteGuard<'_, K, V> {
        let guard = self.lock.lock();
        WriteGuard {
            _guard: guard,
            bucket: self,
        }
    }

    pub unsafe fn entries_mut(&self) -> EntryRefIter<K, V, Mut_> {
        EntryRefIter {
            bucket: self,
            curr_entry_idx: 0,
            guard: unprotected(),
            mut_mark: Mut_,
        }
    }

    pub fn transfer(&mut self, pair: Shared<(K, V)>, sign: u8) {
        let mut last_bucket = &*self;
        let entries = unsafe { self.entries_mut() };
        for entry_ref in entries {
            last_bucket = entry_ref.bucket;
            let cell = entry_ref.load_key_value(unsafe { unprotected() });
            let cell_ref = unsafe { cell.as_ref() };

            match cell_ref {
                Some(_) => {}
                None => {
                    entry_ref.store(Pair::Shared { pair, sign });
                    return;
                }
            }
        }

        let new_bucket = Bucket::new();
        let entry_ref = unsafe { new_bucket.entries_mut().next().unwrap() };
        entry_ref.store(Pair::Shared { pair, sign });
        last_bucket.next.store(Owned::new(new_bucket), Release);
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

pub struct WriteGuard<'a, K, V> {
    _guard: MutexGuard<'a, ()>,
    bucket: &'a Bucket<K, V>,
}

impl<'a, K, V> WriteGuard<'a, K, V>
where
    K: Eq + 'static,
    V: 'static,
{
    pub fn insert<'g>(
        &self,
        pair: (K, V),
        pair_sign: u8,
        guard: &'g Guard,
    ) -> InsertResult<'g, K, V> {
        let mut empty_entry = None;
        let mut last_bucket = self.bucket;
        for entry in self.entries_mut(guard) {
            last_bucket = entry.bucket;
            let cell = entry.load_key_value(guard);
            let cell_ref = unsafe { cell.as_ref() };
            match cell_ref {
                Some(cell_ref) => {
                    let sign = entry.load_signature();
                    if sign == pair_sign && cell_ref.0 == pair.0 {
                        entry.store(Pair::Owned { pair, sign });
                        unsafe {
                            guard.defer_destroy(cell);
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

    pub fn remove_entry<'g, Q>(
        &self,
        key: &Q,
        key_sign: u8,
        guard: &'g Guard,
    ) -> Option<(&'g K, &'g V)>
    where
        Q: ?Sized + Hash + Eq,
        K: Borrow<Q>,
    {
        for entry in self.entries_mut(guard) {
            let cell = entry.load_key_value(guard);
            let cell_ref = unsafe { cell.as_ref() };

            match cell_ref {
                Some(cell_ref) => {
                    let entry_sign = entry.load_signature();
                    if entry_sign == key_sign && cell_ref.0.borrow() == key {
                        entry.clear();
                        unsafe {
                            guard.defer_destroy(cell);
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

    pub fn entries_mut(&self, guard: &'a Guard) -> EntryRefIter<'a, K, V, Mut_> {
        EntryRefIter {
            bucket: self.bucket,
            curr_entry_idx: 0,
            guard,
            mut_mark: Mut_,
        }
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
    fn test_insert_when_bucket_full_no_next() {
        let bucket = Bucket::new();
        let guard = pin();
        let pairs = (0..=5).map(|v| (v, v)).collect::<Vec<_>>();
        for (idx, pair) in pairs.iter().copied().enumerate() {
            let w = bucket.write();
            if idx < 5 {
                assert_eq!(w.insert(pair, 0, &guard), InsertResult::Inserted(None));
            } else {
                assert_eq!(
                    w.insert(pair, 0, &guard),
                    InsertResult::InsertedWithOverflow
                );
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
            let w = bucket.write();
            if idx != 5 {
                assert_eq!(w.insert(pair, 0, &guard), InsertResult::Inserted(None));
            } else {
                assert_eq!(
                    w.insert(pair, 0, &guard),
                    InsertResult::InsertedWithOverflow
                );
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
            let w = bucket.write();
            w.insert(pair, 0, &guard);
        }

        for pair in pairs.iter() {
            assert_eq!(bucket.find(&pair.0, 0, &guard), Some((&pair.0, &pair.1)));
        }
    }
}
