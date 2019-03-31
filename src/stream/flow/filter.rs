use crate::actor::ActorSystemContext;
use crate::dispatcher::Trampoline;
use crate::stream::*;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct Filter<A, F: FnMut(&A) -> bool, Up: Producer<A>, Down: Consumer<A>>
where
    A: 'static + Send,
{
    filter: F,
    upstream: Up,
    downstream: Down,
    phantom: PhantomData<A>,
}

impl<A, F, Up> Filter<A, F, Up, Disconnected>
where
    A: 'static + Send,
    F: 'static + FnMut(&A) -> bool + Send,
    Up: Producer<A>,
{
    pub fn new(filter: F) -> impl FnOnce(Up) -> Self {
        move |upstream| Self {
            filter,
            upstream,
            downstream: Disconnected,
            phantom: PhantomData,
        }
    }
}

impl<A, F, Up, Down> Filter<A, F, Up, Down>
where
    A: 'static + Send,
    F: 'static + FnMut(&A) -> bool + Send,
    Up: Producer<A>,
    Down: Consumer<A>,
{
    fn disconnect_downstream<Produce: Producer<A>>(
        self,
        producer: Produce,
    ) -> (Down, Filter<A, F, Produce, Disconnected>) {
        (
            self.downstream,
            Filter {
                filter: self.filter,
                upstream: producer,
                downstream: Disconnected,
                phantom: PhantomData,
            },
        )
    }

    fn disconnect_upstream<Consume: Consumer<A>>(
        self,
        consumer: Consume,
    ) -> (Up, Filter<A, F, Disconnected, Consume>) {
        (
            self.upstream,
            Filter {
                filter: self.filter,
                upstream: Disconnected,
                downstream: consumer,
                phantom: PhantomData,
            },
        )
    }
}

impl<A, F, P, D> Producer<A> for Filter<A, F, P, D>
where
    A: 'static + Send,
    F: FnMut(&A) -> bool + 'static + Send,
    P: Producer<A>,
    D: Consumer<A>,
{
    fn attach<Consume: Consumer<A>>(
        self,
        consumer: Consume,
        context: Arc<ActorSystemContext>,
    ) -> Trampoline {
        let (upstream, filter) = self.disconnect_upstream(consumer);

        upstream.attach(filter, context.clone())
    }

    fn pull<Consume: Consumer<A>>(self, consumer: Consume) -> Trampoline {
        let (upstream, filter) = self.disconnect_upstream(consumer);

        upstream.pull(filter)
    }

    fn cancel<Consume: Consumer<A>>(self, consumer: Consume) -> Trampoline {
        let (upstream, filter) = self.disconnect_upstream(consumer);

        upstream.cancel(filter)
    }
}

impl<A, F, P, D> Consumer<A> for Filter<A, F, P, D>
where
    A: 'static + Send,
    F: 'static + FnMut(&A) -> bool + Send,
    P: Producer<A>,
    D: Consumer<A>,
{
    fn started<Produce: Producer<A>>(self, producer: Produce) -> Trampoline {
        let (downstream, filter) = self.disconnect_downstream(producer);

        downstream.started(filter)
    }

    fn produced<Produce: Producer<A>>(mut self, producer: Produce, element: A) -> Trampoline {
        if (self.filter)(&element) {
            let (downstream, filter) = self.disconnect_downstream(producer);
            downstream.produced(filter, element)
        } else {
            Trampoline::bounce(|| producer.pull(self))
        }
    }

    fn completed(self) -> Trampoline {
        self.downstream.completed()
    }

    fn failed(self, error: Error) -> Trampoline {
        self.downstream.failed(error)
    }
}
