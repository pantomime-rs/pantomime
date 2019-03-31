use crate::stream::*;
use std::marker::PhantomData;

/// `Ignore` is a `Consumer` that requests elements
/// one at a time and drops them as it receives them.
///
/// It can be connected to producers to constantly
/// pull values from upstream.
pub struct Ignore<A>
where
    A: 'static + Send,
{
    phantom: PhantomData<A>,
}

impl<A> Ignore<A>
where
    A: 'static + Send,
{
    pub fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl<A> Consumer<A> for Ignore<A>
where
    A: 'static + Send,
{
    fn started<Produce: Producer<A>>(self, producer: Produce) -> Bounce<Completed> {
        Bounce::Bounce(Box::new(move || producer.request(self, 1)))
    }

    fn produced<Produce: Producer<A>>(
        mut self,
        producer: Produce,
        element: A,
    ) -> Bounce<Completed> {
        Bounce::Bounce(Box::new(move || producer.request(self, 1)))
    }

    fn completed(self) -> Bounce<Completed> {
        std::process::exit(0); // @TODO temp
        Bounce::Done(Completed)
    }

    fn failed(self, error: Error) -> Bounce<Completed> {
        Bounce::Done(Completed)
        // @TODO
    }
}
