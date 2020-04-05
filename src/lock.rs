use crossbeam::utils::Backoff;
use std::sync::atomic::{AtomicBool, Ordering::*};

#[repr(transparent)]
pub struct Mutex {
    locked: AtomicBool,
}

pub struct MutexGuard<'a> {
    lock: &'a Mutex,
}

impl Drop for MutexGuard<'_> {
    fn drop(&mut self) {
        self.lock.locked.store(false, Release);
    }
}

impl Mutex {
    pub fn new() -> Self {
        Self {
            locked: AtomicBool::new(false),
        }
    }

    pub fn lock(&self) -> MutexGuard<'_> {
        let backoff = Backoff::new();
        loop {
            let locked = self.locked.swap(true, Acquire);
            if !locked {
                return MutexGuard { lock: self };
            }

            while self.locked.load(Relaxed) {
                backoff.snooze();
            }
        }
    }
}
