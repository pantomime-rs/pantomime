use crate::dispatcher::Thunk;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct Cancellable {
    pub(crate) cancel: Arc<AtomicBool>,
}

impl Cancellable {
    pub(crate) fn new() -> Self {
        Self {
            cancel: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn cancel(self) {
        self.cancel.store(true, Ordering::SeqCst);
    }

    /// Determines if this is cancelled at this point of time.
    ///
    /// If this returns true, it will always return true.
    ///
    /// If this returns false, it may return true at any point in a subsequent call.
    pub(crate) fn cancelled(&self) -> bool {
        self.cancel.load(Ordering::SeqCst)
    }
}

pub(crate) struct MaybeCancelled<A> {
    pub(crate) item: A,
    pub(crate) cancelled: Option<Arc<AtomicBool>>,
    pub(crate) handled: Option<Thunk>,
}

impl<A> MaybeCancelled<A> {
    #[inline(always)]
    pub(crate) fn new(item: A, cancelled: Option<Arc<AtomicBool>>, handled: Option<Thunk>) -> Self {
        Self {
            item,
            cancelled,
            handled,
        }
    }
}
