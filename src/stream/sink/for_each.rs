use crate::dispatcher::{BoxedFn1In0Out, Trampoline};
use crate::stream::oxidized::*;
use crate::stream::*;
use std::marker::PhantomData;

pub struct ForEach<A, F: FnMut(A), Up: Producer<A>>
where
    A: 'static + Send,
    F: 'static + Send,
{
    func: F,
    on_termination: Option<Box<dyn BoxedFn1In0Out<Terminated> + 'static + Send>>,
    upstream: Up,
    phantom: PhantomData<A>,
}

impl<A, F: FnMut(A), Up: Producer<A>> ForEach<A, F, Up>
where
    A: 'static + Send,
    F: 'static + Send,
{
    pub fn new(func: F) -> impl FnOnce(Up) -> Self {
        move |upstream| Self {
            func,
            on_termination: None,
            upstream,
            phantom: PhantomData,
        }
    }

    // @TODO UnwindSafe
    pub fn watch_termination<T: FnOnce(Terminated)>(mut self, f: T) -> Self
    where
        T: 'static + Send,
    {
        self.on_termination = Some(Box::new(f));
        self
    }
}

impl<A, F: FnMut(A), Up: Producer<A>> Consumer<A> for ForEach<A, F, Up>
where
    A: 'static + Send,
    F: 'static + Send,
{
    fn started<Produce: Producer<A>>(self, producer: Produce, _: &StreamContext) -> Trampoline {
        producer.pull(self)
    }

    fn produced<Produce: Producer<A>>(mut self, producer: Produce, element: A) -> Trampoline {
        (self.func)(element);

        Trampoline::bounce(move || producer.pull(self))
    }

    fn completed(self) -> Trampoline {
        if let Some(f) = self.on_termination {
            f.apply(Terminated::Completed);
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
            f.apply(Terminated::Failed(e));

            /*
            if let Err(e) = catch_unwind(move || f.apply(Terminated::Failed(e))) {
                // @TODO we should have some mechanism for this
                debug!("user-supplied watch_termination function panicked");
            }*/
        }

        Trampoline::done()
    }
}

impl<A, F: FnMut(A), Up: Producer<A>> Sink<A> for ForEach<A, F, Up>
where
    A: 'static + Send,
    F: 'static + Send,
{
    fn start(mut self, stream_context: &StreamContext) -> Trampoline {
        let sink = ForEach {
            func: self.func,
            on_termination: self.on_termination.take(),
            upstream: Disconnected,
            phantom: PhantomData,
        };

        self.upstream.attach(sink, &stream_context)
    }
}
