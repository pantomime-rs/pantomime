use crate::dispatcher::{BoxedFn1In0Out, Trampoline};
use crate::stream::detached::*;
use crate::stream::*;
use std::marker::PhantomData;
use std::panic::UnwindSafe;

/// A `Sink` that terminates the provided logic, dropping anything
/// it may emit.
pub struct DetachedLogicSink<A, M, L: DetachedLogic<A, (), M>, Up: Producer<A>>
where
    A: 'static + Send,
    M: 'static + Send,
    L: 'static + Send,
{
    logic: L,
    on_termination: Option<Box<BoxedFn1In0Out<Terminated> + 'static + Send + UnwindSafe>>,
    upstream: Up,
    phantom: PhantomData<(A, M)>,
}

impl<A, M, L: DetachedLogic<A, (), M>, Up: Producer<A>> DetachedLogicSink<A, M, L, Up>
where
    A: 'static + Send,
    M: 'static + Send,
    L: 'static + Send,
{
    pub fn new(logic: L) -> impl FnOnce(Up) -> Self {
        move |upstream| Self {
            logic,
            on_termination: None,
            upstream,
            phantom: PhantomData,
        }
    }

    /// @TODO unwind safe for F
    pub fn watch_termination<F: FnOnce(Terminated)>(mut self, f: F) -> Self
    where
        F: 'static + Send + UnwindSafe,
    {
        self.on_termination = Some(Box::new(f));
        self
    }
}

// @TODO document this thing
impl<A, M, L: DetachedLogic<A, (), M>, Up: Producer<A>> Consumer<A>
    for DetachedLogicSink<A, M, L, Up>
where
    A: 'static + Send,
    M: 'static + Send,
    L: 'static + Send,
{
    fn started<Produce: Producer<A>>(
        self,
        _producer: Produce,
        _context: &StreamContext,
    ) -> Trampoline {
        panic!()
    }

    fn produced<Produce: Producer<A>>(self, _producer: Produce, _: A) -> Trampoline {
        panic!()
    }

    fn completed(self) -> Trampoline {
        panic!()
    }

    fn failed(self, _e: Error) -> Trampoline {
        panic!()
    }
}

impl<A, M, L: DetachedLogic<A, (), M>, Up: Producer<A>> Sink<A> for DetachedLogicSink<A, M, L, Up>
where
    A: 'static + Send,
    M: 'static + Send,
    L: 'static + Send,
{
    fn start(self, stream_context: &StreamContext) -> Trampoline {
        let stream = Detached::new(self.logic)(self.upstream);

        let sink = match self.on_termination {
            Some(on_termination) => stream
                .to(Sinks::ignore())
                .watch_termination(|t| on_termination.apply(t)),
            None => stream.to(Sinks::ignore()),
        };

        sink.start(stream_context)
    }
}
