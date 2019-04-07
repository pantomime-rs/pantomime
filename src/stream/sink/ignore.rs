use crate::dispatcher::{BoxedFn1In0Out, Thunk, Trampoline};
use crate::stream::oxidized::*;
use crate::stream::*;
use std::marker::PhantomData;
use std::panic::{catch_unwind, UnwindSafe};

/// `Ignore` is a `Consumer` that requests elements
/// one at a time and drops them as it receives them.
///
/// It can be connected to producers to constantly
/// pull values from upstream.
pub struct Ignore<A>
where
    A: 'static + Send,
{
    on_termination: Option<Box<BoxedFn1In0Out<Terminated> + 'static + Send + UnwindSafe>>,
    phantom: PhantomData<A>,
}

impl<A> Ignore<A>
where
    A: 'static + Send,
{
    pub fn new() -> Self {
        Self {
            on_termination: None,
            phantom: PhantomData,
        }
    }

    pub fn watch_termination<F: FnOnce(Terminated)>(mut self, f: F) -> Self
    where
        F: 'static + Send + UnwindSafe,
    {
        self.on_termination = Some(Box::new(f));
        self
    }
}

impl<A> Consumer<A> for Ignore<A>
where
    A: 'static + Send,
{
    fn started<Produce: Producer<A>>(self, producer: Produce) -> Trampoline {
        producer.pull(self)
    }

    fn produced<Produce: Producer<A>>(self, producer: Produce, _: A) -> Trampoline {
        Trampoline::bounce(move || producer.pull(self))
    }

    fn completed(self) -> Trampoline {
        if let Some(f) = self.on_termination {
            if let Err(e) = catch_unwind(move || f.apply(Terminated::Completed)) {
                // @TODO we should have some mechanism for this
                debug!("user-supplied watch_termination function panicked");
            }
        }

        Trampoline::done()
    }

    fn failed(self, e: Error) -> Trampoline {
        if let Some(f) = self.on_termination {
            if let Err(e) = catch_unwind(move || f.apply(Terminated::Failed(e))) {
                // @TODO we should have some mechanism for this
                debug!("user-supplied watch_termination function panicked");
            }
        }

        Trampoline::done()
    }
}

impl<A> Sink<A> for Ignore<A> where A: 'static + Send {}
