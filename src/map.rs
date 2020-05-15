use crate::bucket::{Bucket, InsertResult, Stats, WriteGuard, ENTRIES_PER_BUCKET};
use crossbeam::epoch::*;
use std::borrow::Borrow;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{Ordering::*, *};

const RAW_TABLE_STATE_QUIESCENCE: usize = 0;
const RAW_TABLE_STATE_GROWING: usize = 1;
const OCCUPANCY_AFTER_RESIZE: f64 = 40.0;

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
        bucket: &WriteGuard<K, V>
    ) {
        for entry in bucket.entries_mut() {
            let pair_opt = entry.swap_with_null();
            unsafe {
                if let Some(pair) = pair_opt.as_ref() {
                    let key_hash = hash(&pair.0, build_hasher);
                    let key_sign = self.signature(key_hash);
                    let new_key_bucket = self.bucket_mut_for_hash(key_hash);
                    let overflowed = new_key_bucket.transfer(pair_opt, key_sign);
                    if overflowed {
                        self.num_overflows.fetch_add(1, Relaxed);
                    }
                }
            }
        }
    }
}

/// A concurrent hash table.
pub struct HashMap<K, V, S = crate::DefaultHashBuilder> {
    raw_ptr: Atomic<RawTable<K, V>>,
    build_hasher: S,
}

impl<K, V> HashMap<K, V, crate::DefaultHashBuilder> {

    /// Creates an `HashMap`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty `HashMap` with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, crate::DefaultHashBuilder::default())
    }
}

impl<K, V, S> HashMap<K, V, S> {

    /// Creates an empty `HashMap` which will use the given hash builder to hash
    /// keys.
    ///
    /// The created map has the default initial capacity.
    pub fn with_hasher(hash_builder: S) -> Self {
        Self::with_capacity_and_hasher(2, hash_builder)
    }

    /// Creates an empty `HashMap` with the specified capacity, using `hash_builder`
    /// to hash the keys.
    pub fn with_capacity_and_hasher(capacity: usize, build_hasher: S) -> Self {
        let capacity = capacity.next_power_of_two();
        let cap_log2 = (capacity as f64).log2().ceil() as u32;
        let cap_log2 = if cap_log2 < 2 { 2 } else { cap_log2 };
        Self {
            raw_ptr: Atomic::new(RawTable::with_pow_buckets(cap_log2)),
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

    /// Inserts a key-value pair into the map.
    ///
    /// If the map did not have this key present, [`None`] is returned.
    ///
    /// If the map did have this key present, the value is updated, and the old
    /// value is returned. The key is not updated, though;
    pub fn insert<'g>(&'g self, key: K, val: V, guard: &'g Guard) -> Option<&'g V> {
        let key_hash = self.hash(&key);

        let ins_res = self.run_locked(
            key_hash,
            move |table_ptr, bucket, sign| {
                let ins_res = bucket.insert((key, val), sign);
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

    /// Returns a reference to the value corresponding to the key.
    ///
    /// The key may be any borrowed form of the map's key type, but
    /// [`Hash`] and [`Eq`] on the borrowed form *must* match those for
    /// the key type.
    ///
    /// [`Eq`]: std::cmp::Eq
    /// [`Hash`]: std::hash::Hash
    pub fn get<'g, Q>(&'g self, key: &Q, guard: &'g Guard) -> Option<&'g V>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        self.get_key_value(key, guard).map(|(_, val)| val)
    }


    /// Returns the key-value pair corresponding to the supplied key.
    ///
    /// The supplied key may be any borrowed form of the map's key type, but
    /// [`Hash`] and [`Eq`] on the borrowed form *must* match those for
    /// the key type.
    ///
    /// [`Eq`]: std::cmp::Eq
    /// [`Hash`]: std::hash::Hash
    pub fn get_key_value<'g, Q>(&'g self, key: &Q, guard: &'g Guard) -> Option<(&'g K, &'g V)>
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

    /// Returns `true` if the map contains a value for the specified key.
    ///
    /// The key may be any borrowed form of the map's key type, but
    /// [`Hash`] and [`Eq`] on the borrowed form *must* match those for
    /// the key type.
    ///
    /// [`Eq`]: std::cmp::Eq
    /// [`Hash`]: std::hash::Hash
    pub fn contains_key<Q>(&self, key: &Q, guard: &Guard) -> bool
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        self.get_key_value(key, guard).is_some()
    }

    /// Removes a key from the map, returning the stored key and value if the
    /// key was previously in the map.
    ///
    /// The key may be any borrowed form of the map's key type, but
    /// [`Hash`] and [`Eq`] on the borrowed form *must* match those for
    /// the key type.
    ///
    /// [`Eq`]: std::cmp::Eq
    /// [`Hash`]: std::hash::Hash
    pub fn remove_entry<'g, Q>(&'g self, key: &Q, guard: &'g Guard) -> Option<(&'g K, &'g V)>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        let key_hash = self.hash(&key);

        self.run_locked(
            key_hash,
            move |_, bucket, sign| bucket.remove_entry(&key, sign),
            guard,
        )
    }

    /// Removes a key from the map, returning the value at the key if the key
    /// was previously in the map.
    ///
    /// The key may be any borrowed form of the map's key type, but
    /// [`Hash`] and [`Eq`] on the borrowed form *must* match those for
    /// the key type.
    ///
    /// [`Eq`]: std::cmp::Eq
    /// [`Hash`]: std::hash::Hash
    pub fn remove<'g, Q>(&'g self, key: &Q, guard: &'g Guard) -> Option<&'g V>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        self.remove_entry(key, guard).map(|(_, val)| val)
    }

    fn run_locked<'g, F, R: 'g>(&'g self, key_hash: u64, func: F, guard: &'g Guard) -> R
    where F: FnOnce(Shared<'g, RawTable<K, V>>, WriteGuard<'g, K, V>, u8) -> R,
    {
        loop {
            let old_raw = self.raw_ptr.load(Acquire, guard);

            // find and lock bucket for insert
            let raw_ref = unsafe { old_raw.deref() };
            let bucket = raw_ref.bucket_for_hash(key_hash);
            let bucket_write = bucket.write(guard);

            // we locked the bucket, check that table was not resized before we locked the bucket,
            //TODO: add note why we can't miss resize here
            let cur_raw = self.raw_ptr.load(Acquire, guard);

            if old_raw == cur_raw && cur_raw.tag() == RAW_TABLE_STATE_QUIESCENCE {
                // old_raw == cur_raw, we can use raw_ref safely
                let key_sig = raw_ref.signature(key_hash);
                return func(cur_raw, bucket_write, key_sig);
            }
            // table was resized, try again
            continue;
        }
    }

    fn try_grow<'g>(&self, raw_shared: Shared<'g, RawTable<K, V>>, guard: &'g Guard) {
        let grow_needed = unsafe {
            let raw_ref = raw_shared.deref();
            raw_ref.num_overflows.load(Relaxed) >= raw_ref.buckets.len()
        };
        if grow_needed {
            if let Ok(raw_shared) = self.raw_ptr.compare_and_set(
                raw_shared.with_tag(RAW_TABLE_STATE_QUIESCENCE),
                raw_shared.with_tag(RAW_TABLE_STATE_GROWING),
                (Acquire, Relaxed),
                guard,
            ) {
                let raw_ref = unsafe { raw_shared.deref() };

                let mut guards = Vec::new();
                let mut stats = Stats::zeroed();
                for bucket in &*raw_ref.buckets {
                    let bucket_write = bucket.write(guard);
                    let bucket_stats = bucket_write.stats(&guard);
                    stats += bucket_stats;
                    guards.push(bucket_write);
                }

                let full_ratio = (100 * stats.num_non_empty) as f64
                    / (raw_ref.buckets.len() * ENTRIES_PER_BUCKET) as f64;
                let inc_by =
                    ((full_ratio / OCCUPANCY_AFTER_RESIZE).ceil() as u64).next_power_of_two();
                let new_size = (inc_by * raw_ref.buckets.len() as u64) as f64;
                let new_pow = {
                    let new_pow = new_size.log2().ceil() as u32;
                    if new_pow == 1 {
                        2
                    } else {
                        new_pow
                    }
                };

                let mut new_raw_table = RawTable::with_pow_buckets(new_pow);

                for bucket_write in &guards {
                    new_raw_table.transfer_bucket(&self.build_hasher, bucket_write);
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
        let map = Arc::new(HashMap::new());
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

    #[test]
    fn test_simple_delete() {
        let map = Arc::new(HashMap::new());
        let g = pin();
        for k in 0..=36 {
            assert_eq!(map.insert(k, k, &g), None);
        }
        let handles = (0..=36)
            .map(|k| {
                let m = Arc::clone(&map);
                thread::spawn(move || {
                    let g = pin();
                    assert!(m.remove(&k, &g).is_some());
                })
            })
            .collect::<Vec<_>>();
        for h in handles {
            h.join().unwrap();
        }

        let g = pin();
        for k in 0..=36 {
            assert!(map.get(&k, &g).is_none());
        }
    }
}
