use crate::dispatcher::{BoxedFn1In0Out, Thunk, Trampoline};
use crate::stream::oxidized::*;
use crate::stream::*;
use std::marker::PhantomData;
use std::panic::{catch_unwind, UnwindSafe};

pub struct ForEach<A, F: FnMut(A)>
where
    A: 'static + Send,
    F: 'static + Send,
{
    func: F,
    on_termination: Option<Box<BoxedFn1In0Out<Terminated> + 'static + Send + UnwindSafe>>,
    phantom: PhantomData<A>,
}

impl<A, F: FnMut(A)> ForEach<A, F>
where
    A: 'static + Send,
    F: 'static + Send,
{
    pub fn new(func: F) -> Self {
        Self {
            func,
            on_termination: None,
            phantom: PhantomData,
        }
    }

    pub fn watch_termination<T: FnOnce(Terminated)>(mut self, f: T) -> Self
    where
        T: 'static + Send + UnwindSafe,
    {
        self.on_termination = Some(Box::new(f));
        self
    }
}

impl<A, F: FnMut(A)> Consumer<A> for ForEach<A, F>
where
    A: 'static + Send,
    F: 'static + Send,
{
    fn started<Produce: Producer<A>>(self, producer: Produce) -> Trampoline {
        producer.pull(self)
    }

    fn produced<Produce: Producer<A>>(mut self, producer: Produce, element: A) -> Trampoline {
        (self.func)(element);

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

impl<A, F: FnMut(A)> Sink<A> for ForEach<A, F>
where
    A: 'static + Send,
    F: 'static + Send,
{
}
