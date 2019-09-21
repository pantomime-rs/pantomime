use crate::stream::internal::{Producer, ProducerWithFlow, ProducerWithSink, SourceLike};
use crate::stream::sink::Sink;
use crate::stream::{flow, flow::Flow};
use crate::stream::{Logic, Stream};
use std::iter::Iterator as Iter;
use std::marker::PhantomData;

mod iterator;
mod merge;
mod queue;
mod repeat;

pub use iterator::Iterator;
pub use merge::Merge;
pub use queue::SourceQueue;
pub use repeat::Repeat;

pub struct Source<A> {
    pub(in crate::stream) producers: Vec<Box<Producer<(), A> + Send>>,
}

impl<A> Source<A>
where
    A: 'static + Send,
{
    pub fn new<Msg, L: Logic<(), A, Msg>>(logic: L) -> Self
    where
        Msg: 'static + Send,
        L: 'static + Send,
    {
        Self {
            producers: vec![Box::new(SourceLike {
                logic,
                phantom: PhantomData,
            })],
        }
    }

    pub fn iterator<I: Iter<Item = A>>(iterator: I) -> Self
    where
        I: 'static + Send,
    {
        Self::new(Iterator::new(iterator))
    }

    pub fn queue(capacity: usize) -> SourceQueue<A> {
        SourceQueue::new(capacity)
    }

    pub fn repeat(element: A) -> Self
    where
        A: Clone,
    {
        Self::new(Repeat::new(element))
    }

    pub fn to<Out>(self, sink: Sink<A, Out>) -> Stream<Out>
    where
        Out: 'static + Send,
    {
        Stream {
            runnable_stream: Box::new(ProducerWithSink {
                producer: self.producer(),
                sink,
                phantom: PhantomData,
            }),
        }
    }

    pub fn filter<F: FnMut(&A) -> bool>(self, filter: F) -> Source<A>
    where
        F: 'static + Send,
    {
        self.via(Flow::new(flow::Filter::new(filter)))
    }

    pub fn map<B, F: FnMut(A) -> B>(self, map_fn: F) -> Source<B>
    where
        B: 'static + Send,
        F: 'static + Send,
    {
        self.via(Flow::new(flow::Map::new(map_fn)))
    }

    pub fn merge(self, source: Source<A>) -> Self {
        let mut producers = self.producers;

        for p in source.producers {
            producers.push(p);
        }

        Source { producers }
    }

    pub fn via<B>(self, flow: Flow<A, B>) -> Source<B>
    where
        B: 'static + Send,
    {
        Source {
            producers: vec![Box::new(ProducerWithFlow {
                producer: self.producer(),
                flow,
            })],
        }
    }

    pub(in crate::stream) fn producer(mut self) -> Box<Producer<(), A> + Send> {
        if self.producers.len() > 1 {
            Box::new(SourceLike {
                logic: Merge::new(self.producers),
                phantom: PhantomData,
            })
        } else {
            self.producers
                .pop()
                .expect("pantomime bug: Source::producers is empty")
        }
    }
}
