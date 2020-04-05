use crate::bucket::Bucket;
use crossbeam::epoch::*;
use std::collections::hash_map::DefaultHasher;
use std::fmt::{Error, Formatter, Write};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::sync::atomic::Ordering::*;
use wyhash::WyHash;

pub struct HashMap<K, V> {
    buckets: Atomic<Box<[Bucket<K, V>]>>,
}

impl<K, V> HashMap<K, V>
where
    K: Eq + Hash + 'static,
    V: 'static,
{
    pub fn new() -> Self {
        let buckets = Atomic::new(vec![Bucket::new(), Bucket::new()].into_boxed_slice());
        Self { buckets }
    }

    pub fn with_num_buckets(num: usize) -> Self {
        assert_eq!(num % 2, 0);
        let buckets = (0..num).map(|_| Bucket::new()).collect::<Vec<_>>();
        Self {
            buckets: Atomic::new(buckets.into_boxed_slice()),
        }
    }

    pub fn insert(&self, key: K, val: V, guard: &Guard) -> bool {
        let mut hasher = WyHash::default();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        unsafe {
            let buckets = self.buckets.load(SeqCst, guard).deref();
            let mask = buckets.len() as u64 - 1;
            let index = (hash & mask) as usize;
            let bucket = buckets.get_unchecked(index);
            bucket.insert(key, val, &guard)
        }
    }

    pub fn get<'g>(&self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        let mut hasher = WyHash::default();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        unsafe {
            let buckets = self.buckets.load(SeqCst, guard).deref();
            let mask = buckets.len() as u64 - 1;
            let index = (hash & mask) as usize;
            let bucket = buckets.get_unchecked(index);
            bucket.find(key, &guard)
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
    use std::cell::Cell;

    #[test]
    fn test_simple_insert() {
        let map = Arc::new(HashMap::new());
        let handles = (0..=2)
            .map(|v| {
                let m = Arc::clone(&map);
                thread::spawn(move || {
                    let g = pin();
                    assert!(m.insert(v, v, &g));
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
