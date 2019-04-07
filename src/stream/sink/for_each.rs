use crate::dispatcher::Trampoline;
use crate::stream::oxidized::*;
use crate::stream::*;
use std::marker::PhantomData;

pub struct ForEach<A, F: FnMut(A)>
where
    A: 'static + Send,
    F: 'static + Send,
{
    func: F,
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
            phantom: PhantomData,
        }
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
        Trampoline::done()
    }

    fn failed(self, _: Error) -> Trampoline {
        Trampoline::done()
        // @TODO
    }
}

impl<A, F: FnMut(A)> Sink<A> for ForEach<A, F>
where
    A: 'static + Send,
    F: 'static + Send,
{
}
