use crate::bucket::{Bucket, UpdateResult, WriteGuard};
use crossbeam::epoch::*;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{Ordering::*, *};

const RAW_TABLE_STATE_QUIESCENCE: usize = 0;
const RAW_TABLE_STATE_GROWING: usize = 1;

fn hash<T: Hash>(data: T) -> u64 {
    let mut hasher = ahash::AHasher::default();
    data.hash(&mut hasher);
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

    fn transfer_bucket(&mut self, bucket: &WriteGuard<K, V>, guard: &Guard) {
        for entry in bucket.entries_mut(guard) {
            let pair_opt = entry.swap_with_null(guard);
            unsafe {
                match pair_opt.as_ref() {
                    Some(pair) => {
                        let key_hash = hash(&pair.0);
                        let key_sign = self.signature(key_hash);
                        let new_key_bucket = self.bucket_mut_for_hash(key_hash);
                        new_key_bucket.transfer(pair_opt, key_sign);
                    }
                    None => {}
                }
            }
        }
    }
}

pub struct HashMap<K, V> {
    raw_ptr: Atomic<RawTable<K, V>>,
}

impl<K, V> HashMap<K, V>
where
    K: Eq + Hash + 'static + Send,
    V: 'static + Send,
{
    pub fn new() -> Self {
        Self::with_pow_buckets(2)
    }

    pub fn with_pow_buckets(pow: u32) -> Self {
        assert!(pow > 0);
        Self {
            raw_ptr: Atomic::new(RawTable::with_pow_buckets(pow)),
        }
    }

    pub fn insert<'g>(&self, key: K, val: V, guard: &'g Guard) -> Option<&'g V> {
        let key_hash = hash(&key);

        let ins_res = self.run_locked(
            key_hash,
            move |table_ptr, bucket, sign, g| {
                let ins_res = bucket.insert((key, val), sign, g);
                match ins_res {
                    UpdateResult::UpdatedWithOverflow => {
                        unsafe { table_ptr.deref().num_overflows.fetch_add(1, Relaxed) };
                        (table_ptr, None, true)
                    }
                    UpdateResult::Updated(old_pair) => {
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

    pub fn get<'g>(&self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        let key_hash = hash(key);
        let raw_ref = unsafe { self.raw_ptr.load(Acquire, guard).deref() };
        let bucket = raw_ref.bucket_for_hash(key_hash);
        let sign = raw_ref.signature(key_hash);
        bucket.find(key, sign, guard)
    }

    pub fn remove<'g>(&self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        let key_hash = hash(&key);

        self.run_locked(
            key_hash,
            move |_, bucket, sign, g| bucket.remove(&key, sign, g),
            guard,
        )
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
                match self.raw_ptr.compare_and_set(
                    raw_shared,
                    raw_shared.with_tag(RAW_TABLE_STATE_GROWING),
                    Acquire,
                    guard,
                ) {
                    Ok(raw_shared) => {
                        let raw_ref = unsafe { raw_shared.deref() };
                        let mut new_raw_table = RawTable::with_pow_buckets(raw_ref.cap_log2 + 1);
                        let mut guards = Vec::new();

                        for bucket in &*raw_ref.buckets {
                            let bucket_write = bucket.write();
                            new_raw_table.transfer_bucket(&bucket_write, guard);
                            guards.push(bucket_write);
                        }
                        self.raw_ptr.store(Owned::new(new_raw_table), Release);
                        drop(guards);
                        unsafe {
                            guard.defer_destroy(raw_shared);
                        }
                    }
                    Err(_) => {}
                }
            }
        }
    }
}

impl<K, V> Drop for HashMap<K, V> {
    fn drop(&mut self) {
        unsafe {
            // TODO: use Relaxed in all drops
            let raw = self.raw_ptr.load(Acquire, unprotected());
            let _ = raw.into_owned();
        }
    }
}

impl<K, V> std::fmt::Debug for HashMap<K, V> {
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
