use crate::stream::*;

pub struct Disconnected;

impl<A> Producer<A> for Disconnected
where
    A: 'static + Send,
{
    fn receive<Consume: Consumer<A>>(self, command: ProducerCommand<A, Consume>) -> Completed {
        match command {
            ProducerCommand::Attach(consumer, _) => {
                consumer.tell(ProducerEvent::Started(self));
            }

            ProducerCommand::Request(consumer, _) => {
                consumer.tell::<Self>(ProducerEvent::Completed);
            }

            ProducerCommand::Cancel(consumer, _) => {
                consumer.tell::<Self>(ProducerEvent::Completed);
            }
        }

        Completed
    }

    fn runtime(&mut self) -> Option<&mut ProducerRuntime> {
        None
    }
}

impl<A> Consumer<A> for Disconnected
where
    A: 'static + Send,
{
    fn receive<Produce: Producer<A>>(self, event: ProducerEvent<A, Produce>) -> Completed {
        match event {
            ProducerEvent::Produced(producer, _) => {
                producer.tell(ProducerCommand::Cancel(self, None));
            }

            ProducerEvent::Started(producer) => {
                producer.tell(ProducerCommand::Cancel(self, None));
            }

            ProducerEvent::Completed => {}

            ProducerEvent::Failed(_e) => {
                // @TODO
            }
        }

        Completed
    }
}
