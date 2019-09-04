use crate::dispatcher::{BoxedFn2In0Out, Trampoline};
use crate::stream::oxidized::*;
use crate::stream::*;
use std::marker::PhantomData;

/// A `Sink` that stores the last emitted element, offering it to
/// the registered termination handler when it terminates.
pub struct Last<A, Up: Producer<A>>
where
    A: 'static + Send,
{
    element: Option<A>,
    on_termination: Option<Box<dyn BoxedFn2In0Out<Terminated, Option<A>> + 'static + Send>>,
    upstream: Up,
    phantom: PhantomData<A>,
}

impl<A, Up: Producer<A>> Last<A, Up>
where
    A: 'static + Send,
{
    pub fn new() -> impl FnOnce(Up) -> Self {
        move |upstream| Self {
            element: None,
            on_termination: None,
            upstream,
            phantom: PhantomData,
        }
    }

    // @TODO UnwindSafe
    pub fn watch_termination<T: FnOnce(Terminated, Option<A>)>(mut self, f: T) -> Self
    where
        T: 'static + Send,
    {
        self.on_termination = Some(Box::new(f));
        self
    }
}

impl<A, Up: Producer<A>> Consumer<A> for Last<A, Up>
where
    A: 'static + Send,
{
    fn started<Produce: Producer<A>>(self, producer: Produce, _: &StreamContext) -> Trampoline {
        producer.pull(self)
    }

    fn produced<Produce: Producer<A>>(mut self, producer: Produce, element: A) -> Trampoline {
        self.element = Some(element);

        Trampoline::bounce(move || producer.pull(self))
    }

    fn completed(self) -> Trampoline {
        if let Some(f) = self.on_termination {
            f.apply(Terminated::Completed, self.element);
            /*
            if let Err(e) = catch_unwind(move || f.apply(Terminated::Completed)) {
                // @TODO we should have some mechanism for this
                debug!("user-supplied watch_termination function panicked");
            }*/
        }

        Trampoline::done()
    }

    fn failed(self, e: Error) -> Trampoline {
        if let Some(f) = self.on_termination {
            f.apply(Terminated::Failed(e), self.element);

            /*
            if let Err(e) = catch_unwind(move || f.apply(Terminated::Failed(e))) {
                // @TODO we should have some mechanism for this
                debug!("user-supplied watch_termination function panicked");
            }*/
        }

        Trampoline::done()
    }
}

impl<A, Up: Producer<A>> Sink<A> for Last<A, Up>
where
    A: 'static + Send,
{
    fn start(self, stream_context: &StreamContext) -> Trampoline {
        let sink = Last {
            element: self.element,
            on_termination: self.on_termination,
            upstream: Disconnected,
            phantom: PhantomData,
        };

        self.upstream.attach(sink, &stream_context)
    }
}
