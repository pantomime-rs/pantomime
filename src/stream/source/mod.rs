use crate::stream::internal::{ContainedLogicImpl, LogicType, SourceLike, UnionLogic};
use crate::stream::sink::Sink;
use crate::stream::{flow, flow::Flow, flow::Fused};
use crate::stream::{Datagram, Logic, Stream};
use std::iter::Iterator as Iter;
use std::marker::PhantomData;

pub mod iterator;
pub mod merge;
pub mod queue;
pub mod repeat;
pub mod single;
pub mod udp;

pub struct Source<A> {
    pub(in crate::stream) producers: Vec<LogicType<(), A>>,
}

impl<A> Source<A>
where
    A: 'static + Send,
{
    pub fn new<L: Logic<(), A>>(logic: L) -> Self
    where
        L: 'static + Send,
        L::Ctl: Send,
    {
        Self {
            producers: vec![if logic.fusible() {
                LogicType::Fusible(Box::new(ContainedLogicImpl::new(logic)))
            } else {
                LogicType::Spawnable(Box::new(SourceLike {
                    logic,
                    phantom: PhantomData,
                }))
            }],
        }
    }

    pub fn iterator<I: Iter<Item = A>>(iterator: I) -> Self
    where
        I: 'static + Send,
    {
        Self::new(iterator::Iterator::new(iterator))
    }

    pub fn queue(capacity: usize) -> queue::SourceQueue<A> {
        queue::SourceQueue::new(capacity)
    }

    pub fn repeat(element: A) -> Self
    where
        A: Clone,
    {
        Self::new(repeat::Repeat::new(element))
    }

    pub fn single(element: A) -> Self {
        Self::new(single::Single::new(element))
    }

    pub fn to<Out>(self, sink: Sink<A, Out>) -> Stream<Out>
    where
        Out: 'static + Send,
    {
        match (self.producer(), sink.logic) {
            (LogicType::Fusible(upstream), LogicType::Fusible(downstream)) => Stream {
                runnable_stream: Box::new(Fused::new(upstream, downstream)),
            },

            (LogicType::Fusible(upstream), LogicType::Spawnable(downstream)) => Stream {
                runnable_stream: Box::new(UnionLogic {
                    upstream: upstream.into_facade(),
                    downstream,
                }),
            },

            (LogicType::Spawnable(upstream), LogicType::Fusible(downstream)) => Stream {
                runnable_stream: Box::new(UnionLogic {
                    upstream,
                    downstream: downstream.into_facade(),
                }),
            },

            (LogicType::Spawnable(upstream), LogicType::Spawnable(downstream)) => Stream {
                runnable_stream: Box::new(UnionLogic {
                    upstream,
                    downstream,
                }),
            },
        }
    }

    // A NOTE FOR MAINTAINERS
    //
    // ALL METHODS BELOW SHOULD ALSO EXIST
    // ON Flow

    pub fn filter<F: FnMut(&A) -> bool>(self, filter: F) -> Source<A>
    where
        F: 'static + Send,
    {
        self.via(Flow::from_logic(flow::Filter::new(filter)))
    }

    pub fn filter_map<B, F: FnMut(A) -> Option<B>>(self, filter_map: F) -> Source<B>
    where
        B: 'static + Send,
        F: 'static + Send,
    {
        self.via(Flow::from_logic(flow::FilterMap::new(filter_map)))
    }

    pub fn map<B, F: FnMut(A) -> B>(self, map_fn: F) -> Source<B>
    where
        B: 'static + Send,
        F: 'static + Send,
    {
        self.via(Flow::from_logic(flow::Map::new(map_fn)))
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
        match (self.producer(), flow.logic) {
            (LogicType::Fusible(upstream), LogicType::Fusible(downstream)) => Source {
                producers: vec![LogicType::Fusible(Box::new(ContainedLogicImpl::new(
                    Fused::new(upstream, downstream),
                )))],
            },

            (LogicType::Fusible(upstream), LogicType::Spawnable(downstream)) => Source {
                producers: vec![LogicType::Spawnable(Box::new(UnionLogic {
                    upstream: upstream.into_facade(),
                    downstream,
                }))],
            },

            (LogicType::Spawnable(upstream), LogicType::Fusible(downstream)) => Source {
                producers: vec![LogicType::Spawnable(Box::new(UnionLogic {
                    upstream,
                    downstream: downstream.into_facade(),
                }))],
            },

            (LogicType::Spawnable(upstream), LogicType::Spawnable(downstream)) => Source {
                producers: vec![LogicType::Spawnable(Box::new(UnionLogic {
                    upstream,
                    downstream,
                }))],
            },
        }
    }

    pub(in crate::stream) fn producer(mut self) -> LogicType<(), A> {
        if self.producers.len() > 1 {
            panic!("merge not supported until ports are implemented")
        } else {
            self.producers
                .pop()
                .expect("pantomime bug: Source::producers is empty")
        }
    }
}

impl Source<Datagram> {
    pub fn udp(socket: &mio::net::UdpSocket) -> Self {
        Self::new(udp::Udp::new(socket))
    }
}
