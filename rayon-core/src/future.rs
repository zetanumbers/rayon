use std::{
    future::{Future, IntoFuture},
    pin::pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};

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
        // FIXME: cross executor await
        let wt = WorkerThread::current();
        if wt.is_null() {
            todo!("awaiting outside of the worker")
        }
        let mut fut = pin!(self.into_future());
        loop {
            let rayon_waker = Arc::new(RayonWaker {
                latch: FiberLatch::new(unsafe { Arc::clone((*wt).registry()) }),
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
