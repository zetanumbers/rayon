use std::{
    process::abort,
    sync::atomic::{AtomicUsize, Ordering},
};

use lock_api::RawMutex as _;

use crate::{
    latch::{FiberLatch, Latch},
    registry::WorkerThread,
};

pub struct RawMutex {
    latch: FiberLatch,
}

pub type Mutex<T> = lock_api::Mutex<RawMutex, T>;
pub type MutexGuard<'a, T> = lock_api::MutexGuard<'a, RawMutex, T>;
pub type MappedMutexGuard<'a, T> = lock_api::MappedMutexGuard<'a, RawMutex, T>;

unsafe impl lock_api::RawMutex for RawMutex {
    const INIT: Self = RawMutex {
        latch: FiberLatch::already_set(),
    };

    type GuardMarker = lock_api::GuardSend;

    fn lock(&self) {
        while !self.latch.probe_and_reset() {
            latch_await(&self.latch);
        }
    }

    fn try_lock(&self) -> bool {
        self.latch.probe_and_reset()
    }

    unsafe fn unlock(&self) {
        FiberLatch::set(&self.latch)
    }

    fn is_locked(&self) -> bool {
        !self.latch.probe()
    }
}

#[derive(Debug)]
pub struct Condvar {
    latch: FiberLatch,
}

impl Condvar {
    pub const fn new() -> Self {
        Condvar {
            latch: FiberLatch::new(),
        }
    }

    pub fn wait<T: ?Sized>(&self, guard: &mut MutexGuard<'_, T>) {
        unsafe { self.wait_raw(MutexGuard::mutex(guard).raw()) };
    }

    unsafe fn wait_raw(&self, mutex: &RawMutex) {
        let wt = WorkerThread::current();
        if wt.is_null() {
            todo!("awaiting outside of the worker")
        }
        mutex.unlock();
        self.latch.await_(&*wt);
        mutex.lock();
    }

    pub fn notify_all(&self) {
        unsafe { FiberLatch::set(&self.latch) }
    }

    pub fn notify_one(&self) {
        // Consider residual notifications to be spontanious
        self.notify_all();
    }
}

const WRITER_BIT: usize = 1;
const ONE_READER: usize = 2;

pub struct RawRwLock {
    mutex: RawMutex,
    no_readers: FiberLatch,
    no_writer: FiberLatch,
    state: AtomicUsize,
}

pub type RwLock<T> = lock_api::RwLock<RawRwLock, T>;
pub type RwLockReadGuard<'a, T> = lock_api::RwLockReadGuard<'a, RawRwLock, T>;
pub type RwLockWriteGuard<'a, T> = lock_api::RwLockWriteGuard<'a, RawRwLock, T>;
pub type MappedRwLockReadGuard<'a, T> = lock_api::MappedRwLockReadGuard<'a, RawRwLock, T>;
pub type MappedRwLockWriteGuard<'a, T> = lock_api::MappedRwLockWriteGuard<'a, RawRwLock, T>;

unsafe impl lock_api::RawRwLock for RawRwLock {
    const INIT: Self = RawRwLock {
        mutex: RawMutex::INIT,
        no_readers: FiberLatch::already_set(),
        no_writer: FiberLatch::already_set(),
        state: AtomicUsize::new(0),
    };

    type GuardMarker = lock_api::GuardSend;

    fn lock_shared(&self) {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & WRITER_BIT == 0 {
                // Make sure the number of readers doesn't overflow.
                if state > core::isize::MAX as usize {
                    abort();
                }

                // If nobody is holding a write lock or attempting to acquire it, increment the
                // number of readers.
                match self.state.compare_exchange(
                    state,
                    state + ONE_READER,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return,
                    Err(s) => state = s,
                }
            } else {
                latch_await(&self.no_writer);
                state = self.state.load(Ordering::SeqCst);
            }
        }
    }

    fn try_lock_shared(&self) -> bool {
        let mut state = self.state.load(Ordering::Acquire);

        loop {
            // If there's a writer holding the lock or attempting to acquire it, we cannot acquire
            // a read lock here.
            if state & WRITER_BIT != 0 {
                return false;
            }

            // Make sure the number of readers doesn't overflow.
            if state > core::isize::MAX as usize {
                abort();
            }

            // Increment the number of readers.
            match self.state.compare_exchange(
                state,
                state + ONE_READER,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(s) => state = s,
            }
        }
    }

    unsafe fn unlock_shared(&self) {
        // Decrement the number of readers.
        if self.state.fetch_sub(ONE_READER, Ordering::SeqCst) & !WRITER_BIT == ONE_READER {
            // If this was the last reader, trigger the "no readers" event.
            FiberLatch::set(&self.no_readers);
        }
    }

    fn lock_exclusive(&self) {
        // First grab the mutex.
        self.mutex.lock();

        // Set `WRITER_BIT` and create a guard that unsets it in case this future is canceled.
        let new_state = self.state.fetch_or(WRITER_BIT, Ordering::SeqCst);

        // If we just acquired the lock, return.
        if new_state == WRITER_BIT {
            return;
        }

        // Start waiting for the readers to finish.
        let mut load_ordering = Ordering::SeqCst;

        loop {
            // Check the state again.
            if self.state.load(load_ordering) == WRITER_BIT {
                return;
            }

            load_ordering = Ordering::Acquire;
            // Wait for the readers to finish.
            latch_await(&self.no_readers);
        }
    }

    fn try_lock_exclusive(&self) -> bool {
        // First try grabbing the mutex.
        if !self.mutex.try_lock() {
            return false;
        }

        // If there are no readers, grab the write lock.
        if self
            .state
            .compare_exchange(0, WRITER_BIT, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            true
        } else {
            unsafe { self.mutex.unlock() };
            false
        }
    }

    unsafe fn unlock_exclusive(&self) {
        // Unset `WRITER_BIT`.
        self.state.fetch_and(!WRITER_BIT, Ordering::SeqCst);
        // Trigger the "no writer" event.
        FiberLatch::set(&self.no_writer);

        // Release the writer lock.
        // SAFETY: `RwLockWriteGuard` always holds a lock on writer mutex.
        unsafe { self.mutex.unlock() };
    }
}

fn latch_await(latch: &FiberLatch) {
    let wt = WorkerThread::current();
    if wt.is_null() {
        todo!("awaiting outside of the worker")
    }
    latch.await_(unsafe { &*wt });
}
