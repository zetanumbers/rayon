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
        if !self.latch.probe_and_reset() {
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
}
