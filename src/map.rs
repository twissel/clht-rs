use crate::bucket::{Bucket, BucketEntry, UpdateResult};
use crossbeam::epoch::*;
use crossbeam::utils::Backoff;
use std::hash::{Hash, Hasher};
use std::sync::atomic::Ordering::*;

fn hash<T: Hash>(data: T) -> u64 {
    let mut hasher = ahash::AHasher::default();
    data.hash(&mut hasher);
    hasher.finish()
}

struct RawTable<K, V> {
    buckets: Box<[Bucket<K, V>]>,
    cap_log2: u32,
}

impl<K, V> RawTable<K, V>
where
    K: Hash,
{
    fn bucket_index(&self, hash: u64) -> usize {
        let index = hash >> (64 - self.cap_log2 as u64);
        index as usize
    }

    fn bucket_for_hash(&self, hash: u64) -> &Bucket<K, V> {
        let index = self.bucket_index(hash);
        unsafe { self.buckets.get_unchecked(index) }
    }

    fn signature(&self, hash: u64) -> u8 {
        let sign = hash >> (64 - 8 - self.cap_log2 as u64) & ((1 << 8) - 1);
        sign as u8
    }
}
const RAW_TABLE_STATE_QUIESCENCE: usize = 0;
const RAW_TABLE_STATE_GROWING: usize = 1;

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
        let cap = 2usize.pow(pow);
        let buckets = (0..cap).map(|_| Bucket::new()).collect::<Vec<_>>();
        Self {
            raw_ptr: Atomic::new(RawTable {
                buckets: buckets.into_boxed_slice(),
                cap_log2: pow,
            }),
        }
    }

    pub fn insert<'g>(&self, key: K, val: V, guard: &'g Guard) -> Option<&'g (K, V)> {
        let key_hash = hash(&key);
        let backoff = Backoff::new();
        loop {
            let old_raw = self.raw_ptr.load(Acquire, guard);
            match old_raw.tag() {
                RAW_TABLE_STATE_QUIESCENCE => {
                    // at least at line above table was in quiescence,
                    // try to find and lock bucket for insert
                    let raw_ref = unsafe { old_raw.deref() };
                    let bucket = raw_ref.bucket_for_hash(key_hash);
                    let bucket_write = bucket.write();

                    // we locked the bucket, check that table was not resized before we locked the bucket,
                    // or concurrent resize started
                    let cur_raw = self.raw_ptr.load(Acquire, guard);
                    if cur_raw.tag() != RAW_TABLE_STATE_GROWING && old_raw != cur_raw {

                        // old_raw == cur_raw, we can use raw_ref safely
                        let key_sig = raw_ref.signature(key_hash);
                        let ins_res = bucket_write.insert(BucketEntry {
                            key,
                            val,
                            sign: key_sig
                        }, guard);

                        match ins_res {
                            UpdateResult::UpdatedWithOverflow => {
                                // TODO: grow if needed
                                return None;
                            },
                            UpdateResult::Updated(old_pair) => {
                                return old_pair;
                            }
                        }

                    } else {
                        // concurrent resize is going or table was resized, retry later
                        backoff.snooze();
                        continue;
                    }
                }
                RAW_TABLE_STATE_GROWING => {
                    backoff.snooze();
                    continue;
                }
                _ => unreachable!("Invalid RawTable state/tag"),
            }
        }
    }

    pub fn get<'g>(&self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        /*unsafe {
            let key_hash = hash(key);
            let bucket = self.bucket_for_hash(key_hash, guard);

            bucket.find(key, sign, guard)
        }*/
        unimplemented!()
    }

    /*fn bucket_for_hash<'g>(&self, hash: u64, guard: &'g Guard) -> &'g Bucket<K, V> {
        unsafe {
            let raw_ref = self.raw_ptr.load(Acquire, guard).deref();
            raw_ref.bucket_for_hash(hash)
        }
    }*/
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
        let map = Arc::new(HashMap::with_pow_buckets(8));
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
