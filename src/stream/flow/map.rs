use crate::actor::ActorSystemContext;
use crate::dispatcher::{Dispatcher, Trampoline};
use crate::stream::*;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct Map<A, B, F: FnMut(A) -> B, Up: Producer<A>, Down: Consumer<B>>
where
    A: 'static + Send,
    B: 'static + Send,
    F: 'static + Send,
{
    map: F,
    upstream: Up,
    downstream: Down,
    phantom: PhantomData<A>,
}

impl<A, B, F: FnMut(A) -> B, Up> Map<A, B, F, Up, Disconnected>
where
    A: 'static + Send,
    B: 'static + Send,
    F: 'static + Send,
    Up: Producer<A>,
{
    pub fn new(func: F) -> impl FnOnce(Up) -> Self {
        move |upstream| Self {
            map: func,
            upstream: upstream,
            downstream: Disconnected,
            phantom: PhantomData,
        }
    }
}

impl<A, B, F: FnMut(A) -> B, Up: Producer<A>, Down: Consumer<B>> Producer<B>
    for Map<A, B, F, Up, Down>
where
    A: 'static + Send,
    B: 'static + Send,
    F: 'static + Send,
    Up: 'static + Send,
    Down: 'static + Send,
{
    fn attach<Consume: Consumer<B>>(
        mut self,
        consumer: Consume,
        context: Arc<ActorSystemContext>,
    ) -> Trampoline {
        self.upstream.attach(
            Map {
                map: self.map,
                upstream: Disconnected,
                downstream: consumer,
                phantom: PhantomData,
            },
            context.clone(),
        )
    }

    fn pull<Consume: Consumer<B>>(self, consumer: Consume) -> Trampoline {
        self.upstream.pull(Map {
            map: self.map,
            upstream: Disconnected,
            downstream: consumer,
            phantom: PhantomData,
        })
    }

    fn cancel<Consume: Consumer<B>>(self, consumer: Consume) -> Trampoline {
        self.upstream.cancel(Map {
            map: self.map,
            upstream: Disconnected,
            downstream: consumer,
            phantom: PhantomData,
        })
    }
}

impl<A, B, F: FnMut(A) -> B, Up: Producer<A>, Down: Consumer<B>> Consumer<A>
    for Map<A, B, F, Up, Down>
where
    A: 'static + Send,
    B: 'static + Send,
    F: 'static + Send,
    Up: 'static + Send,
    Down: 'static + Send,
{
    fn started<Produce: Producer<A>>(self, producer: Produce) -> Trampoline {
        self.downstream.started(Map {
            map: self.map,
            upstream: producer,
            downstream: Disconnected,
            phantom: PhantomData,
        })
    }

    fn produced<Produce: Producer<A>>(mut self, producer: Produce, element: A) -> Trampoline {
        let element = (self.map)(element);

        self.downstream.produced(
            Map {
                map: self.map,
                upstream: producer,
                downstream: Disconnected,
                phantom: PhantomData,
            },
            element,
        )
    }

    fn completed(self) -> Trampoline {
        self.downstream.completed()
    }

    fn failed(self, error: Error) -> Trampoline {
        self.downstream.failed(error)
    }
}
