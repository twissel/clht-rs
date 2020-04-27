use crate::bucket::{Bucket, InsertResult, WriteGuard};
use crossbeam::epoch::*;
use std::borrow::Borrow;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{Ordering::*, *};

const RAW_TABLE_STATE_QUIESCENCE: usize = 0;
const RAW_TABLE_STATE_GROWING: usize = 1;

fn hash<K: Hash + ?Sized, S: BuildHasher>(key: &K, build_hasher: &S) -> u64 {
    let mut hasher = build_hasher.build_hasher();
    key.hash(&mut hasher);
    hasher.finish()
}

struct RawTable<K, V> {
    buckets: Box<[Bucket<K, V>]>,
    cap_log2: u32,
    num_overflows: AtomicUsize,
}

impl<K, V> RawTable<K, V> {
    fn with_pow_buckets(pow: u32) -> Self {
        let cap = 2usize.pow(pow);
        let buckets = (0..cap).map(|_| Bucket::new()).collect::<Vec<_>>();
        RawTable {
            buckets: buckets.into_boxed_slice(),
            cap_log2: pow,
            num_overflows: AtomicUsize::new(0),
        }
    }
}

impl<K, V> RawTable<K, V>
where
    K: Hash + Eq + 'static,
    V: 'static,
{
    fn bucket_index(&self, hash: u64) -> usize {
        let index = hash >> (64 - self.cap_log2 as u64);
        index as usize
    }

    fn bucket_for_hash(&self, hash: u64) -> &Bucket<K, V> {
        let index = self.bucket_index(hash);
        unsafe { self.buckets.get_unchecked(index) }
    }

    fn bucket_mut_for_hash(&mut self, hash: u64) -> &mut Bucket<K, V> {
        let index = self.bucket_index(hash);
        unsafe { self.buckets.get_unchecked_mut(index) }
    }

    fn signature(&self, hash: u64) -> u8 {
        let sign = hash >> (64 - 8 - self.cap_log2 as u64) & ((1 << 8) - 1);
        sign as u8
    }

    fn transfer_bucket<S: BuildHasher>(
        &mut self,
        build_hasher: &S,
        bucket: &WriteGuard<K, V>,
        guard: &Guard,
    ) {
        for entry in bucket.entries_mut(guard) {
            let pair_opt = entry.swap_with_null(guard);
            unsafe {
                if let Some(pair) = pair_opt.as_ref() {
                    let key_hash = hash(&pair.0, build_hasher);
                    let key_sign = self.signature(key_hash);
                    let new_key_bucket = self.bucket_mut_for_hash(key_hash);
                    new_key_bucket.transfer(pair_opt, key_sign);
                }
            }
        }
    }
}

pub struct HashMap<K, V, S = crate::DefaultHashBuilder> {
    raw_ptr: Atomic<RawTable<K, V>>,
    build_hasher: S,
}

impl<K, V> HashMap<K, V, crate::DefaultHashBuilder> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_pow_buckets(pow: u32) -> Self {
        Self::with_pow_buckets_and_hasher(pow, crate::DefaultHashBuilder::default())
    }
}

impl<K, V, S> HashMap<K, V, S> {
    pub fn with_hasher(hash_builder: S) -> Self {
        Self::with_pow_buckets_and_hasher(2, hash_builder)
    }

    pub fn with_pow_buckets_and_hasher(pow: u32, build_hasher: S) -> Self {
        assert!(pow > 0);
        Self {
            raw_ptr: Atomic::new(RawTable::with_pow_buckets(pow)),
            build_hasher,
        }
    }
}

impl<K, V, S> Default for HashMap<K, V, S>
where
    S: Default,
{
    fn default() -> Self {
        Self::with_hasher(S::default())
    }
}

impl<K, V, S> HashMap<K, V, S>
where
    K: Eq + Hash + 'static + Send + Sync,
    V: 'static + Send + Sync,
    S: BuildHasher,
{
    fn hash<Q: ?Sized + Hash>(&self, key: &Q) -> u64 {
        hash(key, &self.build_hasher)
    }

    pub fn insert<'g>(&self, key: K, val: V, guard: &'g Guard) -> Option<&'g V> {
        let key_hash = self.hash(&key);

        let ins_res = self.run_locked(
            key_hash,
            move |table_ptr, bucket, sign, g| {
                let ins_res = bucket.insert((key, val), sign, g);
                match ins_res {
                    InsertResult::InsertedWithOverflow => {
                        unsafe { table_ptr.deref().num_overflows.fetch_add(1, Relaxed) };
                        (table_ptr, None, true)
                    }
                    InsertResult::Inserted(old_pair) => {
                        (table_ptr, old_pair.map(|pair| &pair.1), false)
                    }
                }
            },
            guard,
        );

        let (table_ptr, old, overflowed) = ins_res;

        if overflowed {
            self.try_grow(table_ptr, guard);
        }

        old
    }

    pub fn get<'g, Q>(&self, key: &Q, guard: &'g Guard) -> Option<&'g V>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        self.get_key_value(key, guard).map(|(_, val)| val)
    }

    pub fn get_key_value<'g, Q>(&self, key: &Q, guard: &'g Guard) -> Option<(&'g K, &'g V)>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        let key_hash = self.hash(key);
        let raw_ref = unsafe { self.raw_ptr.load(Acquire, guard).deref() };
        let bucket = raw_ref.bucket_for_hash(key_hash);
        let sign = raw_ref.signature(key_hash);
        bucket.find(key, sign, guard)
    }

    pub fn contains_key<'g, Q>(&self, key: &Q, guard: &'g Guard) -> bool
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        self.get_key_value(key, guard).is_some()
    }

    pub fn remove_entry<'g, Q>(&self, key: &Q, guard: &'g Guard) -> Option<(&'g K, &'g V)>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        let key_hash = self.hash(&key);

        self.run_locked(
            key_hash,
            move |_, bucket, sign, g| bucket.remove_entry(&key, sign, g),
            guard,
        )
    }
    pub fn remove<'g, Q>(&self, key: &Q, guard: &'g Guard) -> Option<&'g V>
        where
            K: Borrow<Q>,
            Q: ?Sized + Hash + Eq,
    {
        self.remove_entry(key, guard).map(|(_, val)| val)
    }


    fn run_locked<'g, F, R: 'g>(&self, key_hash: u64, func: F, guard: &'g Guard) -> R
    where
        for<'a> F: FnOnce(Shared<'g, RawTable<K, V>>, WriteGuard<'a, K, V>, u8, &'g Guard) -> R,
    {
        loop {
            let old_raw = self.raw_ptr.load(Acquire, guard);
            {
                // at least at line above table was not in growing state,
                // try to find and lock bucket for insert

                let raw_ref = unsafe { old_raw.deref() };
                let bucket = raw_ref.bucket_for_hash(key_hash);
                let bucket_write = bucket.write();

                // we locked the bucket, check that table was not resized before we locked the bucket,
                //TODO: add note why we can't miss resize here
                let cur_raw = self.raw_ptr.load(Acquire, guard);

                if old_raw == cur_raw {
                    // old_raw == old_raw, we can use raw_ref safely
                    let key_sig = raw_ref.signature(key_hash);
                    return func(cur_raw, bucket_write, key_sig, guard);
                }
                // table was resized, try again
                continue;
            }
        }
    }

    fn try_grow<'g>(&self, raw_shared: Shared<'g, RawTable<K, V>>, guard: &'g Guard) {
        if raw_shared.tag() == RAW_TABLE_STATE_QUIESCENCE {
            let grow_needed = unsafe {
                let raw_ref = raw_shared.deref();
                raw_ref.num_overflows.load(Relaxed) >= raw_ref.buckets.len()
            };
            if grow_needed {
                if let Ok(raw_shared) = self.raw_ptr.compare_and_set(
                    raw_shared,
                    raw_shared.with_tag(RAW_TABLE_STATE_GROWING),
                    Acquire,
                    guard,
                ) {
                    let raw_ref = unsafe { raw_shared.deref() };
                    let mut new_raw_table = RawTable::with_pow_buckets(raw_ref.cap_log2 + 1);
                    let mut guards = Vec::new();

                    for bucket in &*raw_ref.buckets {
                        let bucket_write = bucket.write();
                        new_raw_table.transfer_bucket(&self.build_hasher, &bucket_write, guard);
                        guards.push(bucket_write);
                    }
                    self.raw_ptr.store(Owned::new(new_raw_table), Release);
                    drop(guards);
                    unsafe {
                        guard.defer_destroy(raw_shared);
                    }
                }
            }
        }
    }
}

impl<K, V, S> Drop for HashMap<K, V, S> {
    fn drop(&mut self) {
        unsafe {
            // TODO: use Relaxed in all drops
            let raw = self.raw_ptr.load(Acquire, unprotected());
            let _ = raw.into_owned();
        }
    }
}

impl<K, V, S> std::fmt::Debug for HashMap<K, V, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "clht_rs")
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_simple_insert() {
        let map = Arc::new(HashMap::with_pow_buckets(1));
        let handles = (0..=2)
            .map(|v| {
                let m = Arc::clone(&map);
                thread::spawn(move || {
                    let g = pin();
                    m.insert(v, v, &g);
                })
            })
            .collect::<Vec<_>>();
        for h in handles {
            h.join().unwrap();
        }

        let g = pin();
        for k in 0..=2 {
            assert_eq!(map.get(&k, &g), Some(&k));
        }
    }
}
