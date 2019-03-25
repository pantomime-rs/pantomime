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
    fn receive<Produce: Producer<A>>(self, event: ProducerEvent<A, Produce>) -> Completed {
        match event {
            ProducerEvent::Produced(producer, _) => {
                producer.tell(ProducerCommand::Request(self, 1));
            }

            ProducerEvent::Started(producer) => {
                producer.tell(ProducerCommand::Request(self, 1));
            }

            ProducerEvent::Completed => {}

            ProducerEvent::Failed(_e) => {
                // @TODO
            }
        }

        Completed
    }
}
