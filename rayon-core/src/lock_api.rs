use lock_api::{MutexGuard, RawMutex as _};

use crate::{
    latch::{FiberLatch, Latch},
    registry::WorkerThread,
};

pub struct RawMutex {
    latch: FiberLatch,
}

pub type Mutex<T> = lock_api::Mutex<RawMutex, T>;

unsafe impl lock_api::RawMutex for RawMutex {
    const INIT: Self = RawMutex {
        latch: FiberLatch::already_set(),
    };

    type GuardMarker = lock_api::GuardSend;

    fn lock(&self) {
        while !self.latch.probe_and_reset() {
            let wt = WorkerThread::current();
            if wt.is_null() {
                todo!("awaiting outside of the worker")
            }
            self.latch.await_(unsafe { &*wt });
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

    pub fn wait<T: ?Sized>(&self, guard: &mut MutexGuard<'_, RawMutex, T>) {
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
