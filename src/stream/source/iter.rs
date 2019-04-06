use crate::actor::ActorSystemContext;
use crate::dispatcher::{Dispatcher, Trampoline};
use crate::stream::*;
use std::sync::Arc;

pub struct Iter<A, I: Iterator<Item = A>>
where
    A: 'static + Send,
{
    iterator: I,
}

impl<A, I: Iterator<Item = A>> Iter<A, I>
where
    A: 'static + Send,
    I: 'static + Send,
{
    pub fn new(iterator: I) -> Self {
        Self { iterator }
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
        context: ActorSystemContext,
    ) -> Trampoline {
        consumer.started(self)
    }

    fn pull<Consume: Consumer<A>>(mut self, consumer: Consume) -> Trampoline {
        if let Some(element) = self.iterator.next() {
            consumer.produced(self, element)
        } else {
            consumer.completed()
        }
    }

    fn cancel<Consume: Consumer<A>>(self, consumer: Consume) -> Trampoline {
        consumer.completed()
    }
}
