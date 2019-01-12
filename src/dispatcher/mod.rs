//! Dispatchers schedule and execute work

mod single_threaded;
mod work_stealing;

#[cfg(feature = "futures-support")]
mod fs;

#[cfg(feature = "tokio-support")]
mod tokio;

use crossbeam::channel::{unbounded, Receiver, RecvError, Sender};
use std::thread;

pub use self::single_threaded::SingleThreadedDispatcher;
pub use self::work_stealing::WorkStealingDispatcher;

#[cfg(feature = "futures-support")]
pub use self::fs::FutureDispatcherBridge;

#[cfg(feature = "tokio-support")]
pub use self::tokio::RunTokioFuture;

/// A `Dispatcher` is a service that can execute `Thunk`s, which
/// are boxed functions.
///
/// All actors are scheduled onto dispatchers, and all internal
/// work is performed on a dispatcher.
///
/// Typically, a `WorkStealingDispatcher` is used which uses a
/// scheduler implemented ontop of Crossbeam's deque.
///
/// Given the shared nature of a dispatcher, to ensure high
/// throughput, it shouldn't be blocked with long running tasks.
///
/// For some workloads, in particular those that make heavy
/// use of synchronous I/O or are particular compute bound,
/// separate dispatchers should be used.
///
/// An actor can be pinned to a particular dispatcher by overriding
/// the `config_dispatcher` method.
pub trait Dispatcher {
    /// Execute the thunk on this dispatcher
    fn execute(&self, thunk: Thunk);

    fn safe_clone(&self) -> Box<Dispatcher + Send + Sync>;

    fn shutdown(self);

    fn throughput(&self) -> usize;
}

pub trait BoxedFn {
    #[inline(always)]
    fn apply(self: Box<Self>);
}

impl<F: FnOnce()> BoxedFn for F {
    #[inline(always)]
    fn apply(self: Box<F>) {
        (*self)()
    }
}

pub type Thunk = Box<BoxedFn + Send + 'static>;

pub type ThunkWithSync = Box<BoxedFn + Send + Sync + 'static>;
