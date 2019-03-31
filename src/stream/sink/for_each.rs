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
    fn started<Produce: Producer<A>>(self, producer: Produce) -> Bounce<Completed> {
        println!("started, request 1");

        Bounce::Bounce(Box::new(move || producer.request(self, 1)))
    }

    fn produced<Produce: Producer<A>>(
        mut self,
        producer: Produce,
        element: A,
    ) -> Bounce<Completed> {
        (self.func)(element);

        Bounce::Bounce(Box::new(move || producer.request(self, 1)))
    }

    fn completed(self) -> Bounce<Completed> {
        Bounce::Done(Completed)
    }

    fn failed(self, error: Error) -> Bounce<Completed> {
        Bounce::Done(Completed)
        // @TODO
    }
}
