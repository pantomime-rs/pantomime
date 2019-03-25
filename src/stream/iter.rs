use crate::stream::*;

pub struct Iter<A, I: Iterator<Item = A>>
where
    A: 'static + Send,
{
    iterator: I,
    demand: usize,
    runtime: ProducerRuntime,
}

impl<A, I: Iterator<Item = A>> Iter<A, I>
where
    A: 'static + Send,
    I: 'static + Send,
{
    pub fn new(iterator: I) -> Self {
        Self {
            iterator,
            demand: 0,
            runtime: ProducerRuntime::new(),
        }
    }
}

impl<A, I: Iterator<Item = A>> Producer<A> for Iter<A, I>
where
    A: 'static + Send,
    I: 'static + Send,
{
    fn receive<Consume: Consumer<A>>(mut self, command: ProducerCommand<A, Consume>) -> Completed {
        match command {
            ProducerCommand::Attach(consumer, dispatcher) => {
                self.runtime.setup(dispatcher);

                consumer.tell(ProducerEvent::Started(self));
            }

            ProducerCommand::Request(consumer, demand) => {
                self.demand = self
                    .demand
                    .checked_add(demand)
                    .unwrap_or_else(usize::max_value);

                if self.demand == 0 {
                    // @TODO think about what sort of requirements wanted
                    // @TODO right now, i'm thinking that if we receive a
                    // @TODO request that yields no net demand, that
                    // @TODO shall be treated the same as a cancellation
                    // @TODO request (given that the caller no longer has
                    // @TODO          a reference to us)

                    consumer.tell::<Self>(ProducerEvent::Completed);
                } else if let Some(element) = self.iterator.next() {
                    self.demand -= 1;

                    consumer.tell::<Self>(ProducerEvent::Produced(self, element));
                } else {
                    consumer.tell::<Self>(ProducerEvent::Completed);
                }
            }

            ProducerCommand::Cancel(consumer, _) => {
                consumer.tell::<Self>(ProducerEvent::Completed);
            }
        }

        Completed
    }

    fn runtime(&mut self) -> Option<&mut ProducerRuntime> {
        Some(&mut self.runtime)
    }
}
