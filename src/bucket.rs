use crossbeam::epoch::*;
use parking_lot::{Mutex, MutexGuard};
use std::borrow::Borrow;
use std::hash::Hash;
use std::ops::AddAssign;
use std::sync::atomic::{
    AtomicU8,
    Ordering::{Acquire, Release},
};

pub const ENTRIES_PER_BUCKET: usize = 5;

pub struct Node<K, V> {
    signatures: [AtomicU8; ENTRIES_PER_BUCKET],
    cells: [Atomic<(K, V)>; ENTRIES_PER_BUCKET],
    next: Atomic<Node<K, V>>,
}

impl<K, V> Node<K, V> {
    pub fn new() -> Self {
        Self {
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

    #[inline]
    pub fn iter<'g>(&'g self, guard: &'g Guard) -> impl Iterator<Item = EntryRef<'g, K, V>> + 'g {
        struct EntryRefIter<'g, K, V> {
            curr: usize,
            entries: &'g Node<K, V>,
            guard: &'g Guard,
        }

        impl<'g, K, V> Iterator for EntryRefIter<'g, K, V> {
            type Item = EntryRef<'g, K, V>;

            #[inline]
            fn next(&mut self) -> Option<Self::Item> {
                if self.curr < ENTRIES_PER_BUCKET {
                    unsafe {
                        let cell = self.entries.cells.get_unchecked(self.curr);
                        let signature = self.entries.signatures.get_unchecked(self.curr);
                        let entry_ref = Some(EntryRef {
                            cell,
                            signature,
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
            entries: self,
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

    fn clean(&mut self) {
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

pub struct NodeIter<'g, K, V> {
    current: Option<&'g Node<K, V>>,
    guard: &'g Guard,
}

impl<'g, K, V> Iterator for NodeIter<'g, K, V> {
    type Item = &'g Node<K, V>;

    #[inline]
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

pub enum Pair<'g, K, V> {
    Shared { pair: Shared<'g, (K, V)>, sign: u8 },
    Owned { pair: (K, V), sign: u8 },
}

#[derive(Clone, Copy)]
pub struct EntryRef<'g, K, V> {
    cell: &'g Atomic<(K, V)>,
    signature: &'g AtomicU8,
    guard: &'g Guard,
}

impl<'g, K, V> EntryRef<'g, K, V> {
    fn load_signature(&self) -> u8 {
        self.signature.load(Acquire)
    }

    fn load_key_value(&self) -> Shared<'g, (K, V)> {
        self.cell.load(Acquire, self.guard)
    }

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

#[repr(align(64))]
pub struct Bucket<K, V> {
    lock: Mutex<()>,
    head: Node<K, V>,
}

impl<K, V> Bucket<K, V> {
    pub fn new() -> Self {
        Self {
            lock: Mutex::new(()),
            head: Node::new(),
        }
    }

    pub fn node_iter<'g>(&'g self, guard: &'g Guard) -> NodeIter<'g, K, V> {
        NodeIter {
            current: Some(&self.head),
            guard,
        }
    }

    #[inline]
    pub fn entries<'g>(
        &'g self,
        guard: &'g Guard,
    ) -> impl Iterator<Item = EntryRef<'g, K, V>> + 'g {
        self.node_iter(guard).flat_map(move |node| node.iter(guard))
    }

    pub fn find<'g, Q>(&'g self, key: &Q, signature: u8, guard: &'g Guard) -> Option<(&'g K, &'g V)>
    where
        Q: ?Sized + Hash + Eq,
        K: Borrow<Q>,
    {
        self.entries(guard).find_map(|entry: EntryRef<'g, K, V>| {
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

    pub fn stats(&self, guard: &Guard) -> Stats {
        let mut stats = Stats {
            num_overflows: 0,
            num_non_empty: 0,
        };
        for node in self.node_iter(guard) {
            stats.num_overflows += 1;
            for entry_ref in node.iter(guard) {
                let pair = entry_ref.load_key_value();
                if !pair.is_null() {
                    stats.num_non_empty += 1;
                }
            }
        }
        stats.num_overflows -= 1;
        stats
    }

    pub fn transfer(&mut self, pair: Shared<(K, V)>, sign: u8) -> bool {
        let mut last_node = &self.head;
        let guard = unsafe { unprotected() };
        for node in self.node_iter(guard) {
            last_node = node;
            for entry_ref in node.iter(guard) {
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

        let new_node = Node::new();
        let entry_ref = new_node.iter(guard).next().unwrap();
        entry_ref.store(Pair::Shared { pair, sign });
        last_node.next.store(Owned::new(new_node), Release);
        true
    }
}

impl<K, V> Bucket<K, V> {
    pub fn write<'g>(&'g self, guard: &'g Guard) -> WriteGuard<'g, K, V> {
        let lock_guard = self.lock.lock();
        WriteGuard {
            _lock_guard: lock_guard,
            bucket: self,
            guard,
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

impl<K, V> Drop for Bucket<K, V> {
    fn drop(&mut self) {
        self.head.clean();
        unsafe {
            let mut curr = self.head.next.load(Acquire, unprotected());
            while let Some(_) = curr.as_ref() {
                let mut owned = curr.into_owned();
                owned.clean();
                curr = owned.next.load(Acquire, unprotected());
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
        let mut last_node = &self.bucket.head;
        for node in self.bucket.node_iter(self.guard) {
            last_node = node;
            for entry_ref in node.iter(self.guard) {
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
                let new_node = Node::new();
                let entry = new_node.iter(self.guard).next().unwrap();

                entry.store(Pair::Owned {
                    pair,
                    sign: pair_sign,
                });
                last_node.next.store(Owned::new(new_node), Release);
                InsertResult::InsertedWithOverflow
            }
        }
    }

    pub fn remove_entry<Q>(&self, key: &Q, key_sign: u8) -> Option<(&'g K, &'g V)>
    where
        Q: ?Sized + Hash + Eq,
        K: Borrow<Q>,
    {
        for entry in self.entries() {
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

    pub fn entries(&self) -> impl Iterator<Item = EntryRef<'g, K, V>> {
        self.bucket.entries(&self.guard)
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

    #[test]
    fn temp() {
        dbg!(std::mem::size_of::<Node<u64, u64>>());
    }
}
