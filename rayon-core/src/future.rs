use std::{
    future::{Future, IntoFuture},
    pin::pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};

use futures_lite::future::block_on;

use crate::{
    latch::{FiberLatch, Latch},
    registry::WorkerThread,
};

pub trait IntoFutureExt: IntoFuture {
    fn await_(self) -> Self::Output;
}

impl<F: IntoFuture> IntoFutureExt for F {
    fn await_(self) -> Self::Output {
        // Since we pin future onto a stack, you can only poll future from the current fiber
        // and thus current thread.
        let mut fut = pin!(self.into_future());

        let wt = WorkerThread::current();
        if wt.is_null() {
            return block_on(fut);
        }
        loop {
            let rayon_waker = Arc::new(RayonWaker {
                latch: FiberLatch::new(),
            });
            let waker = Waker::from(Arc::clone(&rayon_waker));
            let mut cx = Context::from_waker(&waker);

            if let Poll::Ready(out) = fut.as_mut().poll(&mut cx) {
                return out;
            }
            unsafe { rayon_waker.latch.await_(&*wt) };
        }
    }
}

struct RayonWaker {
    latch: FiberLatch,
}

impl Wake for RayonWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        unsafe { FiberLatch::set(&self.latch) }
    }
}
