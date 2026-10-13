// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! Executor-free concurrency: [`join_all`] drives many futures on the
//! caller's task, and [`spin_on`] runs a future that never waits on real I/O
//! (tests over in-memory stores and scripted transports). Runtime-driven
//! I/O from a synchronous `main` is `http::block_on` (feature `blocking`).
//!
//! [`join_all`] in detail:
//!
//! No spawning, no `Arc`, no runtime dependency: the futures may borrow from
//! the caller (a session, its buffers) because they never leave its task. Each
//! future is polled only when its own waker fired, so with k model streams a
//! chunk arriving on one stream does not re-poll the other k-1.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

/// Per-slot wake flag that forwards to the task's waker. (`Arc` here is the
/// std `Wake` contract for building a `Waker`, not shared ownership of data.)
struct SlotWaker {
    ready: AtomicBool,
    parent: std::sync::Mutex<Option<Waker>>,
}

impl Wake for SlotWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.ready.store(true, Ordering::Release);
        if let Some(w) =
            self.parent.lock().unwrap_or_else(std::sync::PoisonError::into_inner).as_ref()
        {
            w.wake_by_ref();
        }
    }
}

struct Slot<F: Future> {
    fut: Option<Pin<Box<F>>>,
    out: Option<F::Output>,
    flag: Arc<SlotWaker>,
    waker: Waker,
}

/// Future returned by [`join_all`].
pub struct JoinAll<F: Future> {
    slots: Vec<Slot<F>>,
    pending: usize,
}

impl<F: Future> std::fmt::Debug for JoinAll<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinAll")
            .field("len", &self.slots.len())
            .field("pending", &self.pending)
            .finish()
    }
}

/// Run every future to completion concurrently; outputs keep input order.
/// Dropping the returned future drops (cancels) every unfinished one.
pub fn join_all<F: Future>(futures: impl IntoIterator<Item = F>) -> JoinAll<F> {
    let slots: Vec<Slot<F>> = futures
        .into_iter()
        .map(|f| {
            let flag = Arc::new(SlotWaker {
                ready: AtomicBool::new(true),
                parent: std::sync::Mutex::new(None),
            });
            let waker = Waker::from(Arc::clone(&flag));
            Slot { fut: Some(Box::pin(f)), out: None, flag, waker }
        })
        .collect();
    let pending = slots.len();
    JoinAll { slots, pending }
}

impl<F: Future> Unpin for JoinAll<F> {}

impl<F: Future> Future for JoinAll<F> {
    type Output = Vec<F::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        for slot in &mut this.slots {
            let Some(fut) = slot.fut.as_mut() else { continue };
            {
                let mut parent =
                    slot.flag.parent.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                if parent.as_ref().is_none_or(|w| !w.will_wake(cx.waker())) {
                    *parent = Some(cx.waker().clone());
                }
            }
            if !slot.flag.ready.swap(false, Ordering::AcqRel) {
                continue;
            }
            if let Poll::Ready(v) = fut.as_mut().poll(&mut Context::from_waker(&slot.waker)) {
                slot.out = Some(v);
                slot.fut = None;
                this.pending -= 1;
            }
        }
        if this.pending == 0 {
            Poll::Ready(this.slots.iter_mut().filter_map(|s| s.out.take()).collect())
        } else {
            Poll::Pending
        }
    }
}

/// Drive `f` to completion on the calling thread by polling it with a no-op
/// waker. For futures that make progress on every poll — test doubles,
/// in-memory stores, scripted transports; a future that waits on real I/O
/// would spin. Production callers bring their own executor.
pub fn spin_on<F: Future>(f: F) -> F::Output {
    let mut cx = Context::from_waker(Waker::noop());
    let mut f = std::pin::pin!(f);
    loop {
        if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    /// Minimal single-threaded executor for tests.
    /// Yields `n` times (waking itself) before completing with `v`.
    struct YieldN(u32, u32);
    impl Future for YieldN {
        type Output = u32;
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<u32> {
            if self.0 == 0 {
                Poll::Ready(self.1)
            } else {
                self.0 -= 1;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    #[test]
    fn outputs_keep_input_order() {
        let out = spin_on(join_all([YieldN(5, 1), YieldN(0, 2), YieldN(2, 3)]));
        assert_eq!(out, vec![1, 2, 3]);
    }

    #[test]
    fn empty_input_is_ready() {
        let out: Vec<u32> = spin_on(join_all(Vec::<YieldN>::new()));
        assert!(out.is_empty());
    }

    #[test]
    fn futures_may_borrow_caller_state() {
        let counter = Cell::new(0);
        let futs = (0..4).map(|i| {
            let c = &counter;
            async move {
                c.set(c.get() + i);
                i * 2
            }
        });
        let out = spin_on(join_all(futs));
        assert_eq!(out, vec![0, 2, 4, 6]);
        assert_eq!(counter.get(), 6);
    }

    #[test]
    fn only_woken_futures_are_polled() {
        struct Counted<'a>(&'a Cell<u32>, bool);
        impl Future for Counted<'_> {
            type Output = ();
            fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
                self.0.set(self.0.get() + 1);
                if self.1 {
                    Poll::Ready(())
                } else {
                    self.1 = true; // never wakes itself: must not be re-polled
                    Poll::Pending
                }
            }
        }
        let polls = Cell::new(0);
        let mut j = join_all([Counted(&polls, false)]);
        let waker = Waker::from(Arc::new(SlotWaker {
            ready: AtomicBool::new(false),
            parent: std::sync::Mutex::new(None),
        }));
        let mut cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut j).poll(&mut cx).is_pending());
        assert!(Pin::new(&mut j).poll(&mut cx).is_pending());
        assert_eq!(polls.get(), 1, "a future that did not wake is not polled again");
    }
}
