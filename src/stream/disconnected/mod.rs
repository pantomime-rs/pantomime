use crate::actor::ActorSystemContext;
use crate::stream::*;
use std::sync::Arc;

pub struct Disconnected;

impl<A> Producer<A> for Disconnected
where
    A: 'static + Send,
{
    fn attach<Consume: Consumer<A>>(
        self,
        consumer: Consume,
        context: Arc<ActorSystemContext>,
    ) -> Bounce<Completed> {
        consumer.started(self)
    }

    fn request<Consume: Consumer<A>>(self, consumer: Consume, demand: usize) -> Bounce<Completed> {
        consumer.completed()
    }

    fn cancel<Consume: Consumer<A>>(self, consumer: Consume) -> Bounce<Completed> {
        consumer.completed()
    }
}

impl<A> Consumer<A> for Disconnected
where
    A: 'static + Send,
{
    fn started<Produce: Producer<A>>(self, producer: Produce) -> Bounce<Completed> {
        producer.cancel(self)
    }

    fn produced<Produce: Producer<A>>(
        mut self,
        producer: Produce,
        element: A,
    ) -> Bounce<Completed> {
        producer.cancel(self)
    }

    fn completed(self) -> Bounce<Completed> {
        Bounce::Done(Completed)
    }

    fn failed(self, error: Error) -> Bounce<Completed> {
        // @TODO
        Bounce::Done(Completed)
    }
}
