use crate::actor::ActorSystemContext;
use crate::dispatcher::Dispatcher;
use crate::stream::*;
use std::sync::Arc;

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
    fn attach<Consume: Consumer<A>>(
        mut self,
        consumer: Consume,
        context: Arc<ActorSystemContext>,
    ) -> Bounce<Completed> {
        self.runtime.setup(context.dispatcher.safe_clone());

        consumer.started(self)
    }

    fn request<Consume: Consumer<A>>(
        mut self,
        consumer: Consume,
        demand: usize,
    ) -> Bounce<Completed> {
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

            consumer.completed()
        } else if let Some(element) = self.iterator.next() {
            self.demand -= 1;

            consumer.produced(self, element)
        } else {
            consumer.completed()
        }
    }

    fn cancel<Consume: Consumer<A>>(self, consumer: Consume) -> Bounce<Completed> {
        consumer.completed()
    }
}
