use crate::actor::ActorSystemContext;
use crate::dispatcher::Dispatcher;
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
    runtime: ProducerRuntime,
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
            runtime: ProducerRuntime::new(),
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
    ) -> Bounce<Completed> {
        self.runtime.setup(context.dispatcher.safe_clone());

        self.upstream.attach(
            Map {
                map: self.map,
                upstream: Disconnected,
                downstream: consumer,
                runtime: self.runtime,
                phantom: PhantomData,
            },
            context.clone(),
        )
    }

    fn request<Consume: Consumer<B>>(self, consumer: Consume, demand: usize) -> Bounce<Completed> {
        self.upstream.request(
            Map {
                map: self.map,
                upstream: Disconnected,
                downstream: consumer,
                runtime: self.runtime,
                phantom: PhantomData,
            },
            demand,
        )
    }

    fn cancel<Consume: Consumer<B>>(self, consumer: Consume) -> Bounce<Completed> {
        self.upstream.cancel(Map {
            map: self.map,
            upstream: Disconnected,
            downstream: consumer,
            runtime: self.runtime,
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
    fn started<Produce: Producer<A>>(self, producer: Produce) -> Bounce<Completed> {
        self.downstream.started(Map {
            map: self.map,
            upstream: producer,
            downstream: Disconnected,
            runtime: self.runtime,
            phantom: PhantomData,
        })
    }

    fn produced<Produce: Producer<A>>(
        mut self,
        producer: Produce,
        element: A,
    ) -> Bounce<Completed> {
        let element = (self.map)(element);

        self.downstream.produced(
            Map {
                map: self.map,
                upstream: producer,
                downstream: Disconnected,
                runtime: self.runtime,
                phantom: PhantomData,
            },
            element,
        )
    }

    fn completed(self) -> Bounce<Completed> {
        self.downstream.completed()
    }

    fn failed(self, error: Error) -> Bounce<Completed> {
        self.downstream.failed(error)
    }
}
