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
    fn receive<Produce: Producer<A>>(mut self, event: ProducerEvent<A, Produce>) -> Completed {
        match event {
            ProducerEvent::Produced(producer, element) => {
                (self.func)(element);

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
