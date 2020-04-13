use crate::bucket::{Bucket, BucketEntry};
use crossbeam::epoch::*;
use std::hash::{Hash, Hasher};
use std::sync::atomic::Ordering::*;

pub struct HashMap<K, V> {
    buckets: Atomic<Box<[Bucket<K, V>]>>,
    cap_log2: u32,
}

impl<K, V> HashMap<K, V>
where
    K: Eq + Hash + 'static,
    V: 'static,
{
    pub fn new() -> Self {
        Self::with_pow_buckets(2)
    }

    pub fn with_pow_buckets(pow: u32) -> Self {
        assert!(pow > 0);
        let cap = 2usize.pow(pow);
        let buckets = (0..cap).map(|_| Bucket::new()).collect::<Vec<_>>();
        Self {
            buckets: Atomic::new(buckets.into_boxed_slice()),
            cap_log2: pow,
        }
    }

    pub fn insert(&self, key: K, val: V, guard: &Guard) -> bool {
        unsafe {
            let buckets = self.buckets.load(SeqCst, guard).deref();
            let (bin, sign) = self.bin_and_signature(&key);
            let bucket = buckets.get_unchecked(bin);
            let entry = BucketEntry { key, val, sign };
            let w = bucket.write();
            w.insert(entry, &guard)
        }
    }

    pub fn get<'g>(&self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        unsafe {
            let buckets = self.buckets.load(SeqCst, guard).deref();
            let (bin, signature) = self.bin_and_signature(key);
            let bucket = buckets.get_unchecked(bin);
            bucket.find(key, signature, &guard)
        }
    }

    #[inline(never)]
    fn bin_and_signature(&self, key: &K) -> (usize, u8) {
        let mut hasher = ahash::AHasher::default();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let cap_log2 = self.cap_log2;
        let index = hash >> (64 - cap_log2 as u64);
        let sign = hash >> (64 - 8 - cap_log2 as u64) & ((1 << 8) - 1);
        (index as usize, sign as u8)
    }
}

impl<K, V> Drop for HashMap<K, V> {
    fn drop(&mut self) {
        unsafe {
            // TODO: use Relaxed in all drops
            let buckets = self.buckets.load(Acquire, unprotected());
            let _ = buckets.into_owned();
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
