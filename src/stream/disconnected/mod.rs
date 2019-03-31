use crate::actor::ActorSystemContext;
use crate::dispatcher::Trampoline;
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
    ) -> Trampoline {
        consumer.started(self)
    }

    fn pull<Consume: Consumer<A>>(self, consumer: Consume) -> Trampoline {
        consumer.completed()
    }

    fn cancel<Consume: Consumer<A>>(self, consumer: Consume) -> Trampoline {
        consumer.completed()
    }
}

impl<A> Consumer<A> for Disconnected
where
    A: 'static + Send,
{
    fn started<Produce: Producer<A>>(self, producer: Produce) -> Trampoline {
        producer.cancel(self)
    }

    fn produced<Produce: Producer<A>>(mut self, producer: Produce, element: A) -> Trampoline {
        producer.cancel(self)
    }

    fn completed(self) -> Trampoline {
        Trampoline::done()
    }

    fn failed(self, error: Error) -> Trampoline {
        // @TODO

        Trampoline::done()
    }
}
