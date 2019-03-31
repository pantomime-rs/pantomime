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

    /// Execute the trampoline on this dispatcher. Implementations
    /// are free to implement their own calling semantics.
    fn execute_trampoline(&self, trampoline: Trampoline);

    fn safe_clone(&self) -> Box<Dispatcher + Send + Sync>;

    fn shutdown(self);

    fn throughput(&self) -> usize;
}
// @TODO generalize?
pub trait BoxedFn1<A> {
    fn apply(self: Box<Self>) -> A;
}

impl<A, F: FnOnce() -> A> BoxedFn1<A> for F {
    #[inline(always)]
    fn apply(self: Box<F>) -> A {
        (*self)()
    }
}

pub trait BoxedFn {
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

pub enum TrampolineStep {
    Done,
    Bounce(Box<BoxedFn1<Trampoline> + 'static + Send>),
}

pub struct Trampoline {
    step: TrampolineStep,
}

impl Trampoline {
    pub fn done() -> Trampoline {
        Self {
            step: TrampolineStep::Done,
        }
    }

    pub fn bounce<F: FnOnce() -> Trampoline>(f: F) -> Self
    where
        F: 'static + Send,
    {
        Self {
            step: TrampolineStep::Bounce(Box::new(f)),
        }
    }
}
