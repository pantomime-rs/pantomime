use crate::actor::ActorSystemContext;
use crate::stream::*;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct FilterMap<A, B, F: FnMut(A) -> Option<B>, Up: Producer<A>, Down: Consumer<B>>
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

impl<A, B, F: FnMut(A) -> Option<B>, Up> FilterMap<A, B, F, Up, Disconnected>
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

impl<A, B, F: FnMut(A) -> Option<B>, Up, Down> FilterMap<A, B, F, Up, Down>
where
    A: 'static + Send,
    B: 'static + Send,
    F: 'static + Send,
    Up: Producer<A>,
    Down: Consumer<B>,
{
    fn disconnect_downstream<Produce: Producer<A>>(
        self,
        producer: Produce,
    ) -> (FilterMap<A, B, F, Produce, Disconnected>, Down) {
        (
            FilterMap {
                map: self.map,
                upstream: producer,
                downstream: Disconnected,
                phantom: PhantomData,
            },
            self.downstream,
        )
    }

    fn disconnect_upstream<Consume: Consumer<B>>(
        self,
        consumer: Consume,
    ) -> (FilterMap<A, B, F, Disconnected, Consume>, Up) {
        (
            FilterMap {
                map: self.map,
                upstream: Disconnected,
                downstream: consumer,
                phantom: PhantomData,
            },
            self.upstream,
        )
    }
}

impl<A, B, F: FnMut(A) -> Option<B>, Up: Producer<A>, Down: Consumer<B>> Producer<B>
    for FilterMap<A, B, F, Up, Down>
where
    A: 'static + Send,
    B: 'static + Send,
    F: 'static + Send,
    Up: 'static + Send,
    Down: 'static + Send,
{
    fn attach<Consume: Consumer<B>>(
        self,
        consumer: Consume,
        context: Arc<ActorSystemContext>,
    ) -> Bounce<Completed> {
        let (filter_map, upstream) = self.disconnect_upstream(consumer);
        upstream.attach(filter_map, context)
    }

    fn request<Consume: Consumer<B>>(self, consumer: Consume, demand: usize) -> Bounce<Completed> {
        let (filter_map, upstream) = self.disconnect_upstream(consumer);
        upstream.request(filter_map, demand)
    }

    fn cancel<Consume: Consumer<B>>(self, consumer: Consume) -> Bounce<Completed> {
        let (filter_map, upstream) = self.disconnect_upstream(consumer);
        upstream.cancel(filter_map)
    }
}

impl<A, B, F: FnMut(A) -> Option<B>, Up: Producer<A>, Down: Consumer<B>> Consumer<A>
    for FilterMap<A, B, F, Up, Down>
where
    A: 'static + Send,
    B: 'static + Send,
    F: 'static + Send,
    Up: 'static + Send,
    Down: 'static + Send,
{
    fn started<Produce: Producer<A>>(self, producer: Produce) -> Bounce<Completed> {
        let (filter_map, downstream) = self.disconnect_downstream(producer);
        downstream.started(filter_map)
    }

    fn produced<Produce: Producer<A>>(
        mut self,
        producer: Produce,
        element: A,
    ) -> Bounce<Completed> {
        match (self.map)(element) {
            Some(element) => {
                let (filter_map, downstream) = self.disconnect_downstream(producer);
                downstream.produced(filter_map, element)
            }

            None => Trampoline::bounce(|| producer.request(self, 1)),
        }
    }

    fn completed(self) -> Bounce<Completed> {
        self.downstream.completed()
    }

    fn failed(self, error: Error) -> Bounce<Completed> {
        self.downstream.failed(error)
    }
}
