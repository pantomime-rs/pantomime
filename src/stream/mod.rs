pub mod disconnected;
pub mod flow;
pub mod sink;
pub mod source;

pub use disconnected::Disconnected;
pub use flow::*;
pub use sink::*;
pub use source::*;

use crate::actor::ActorSystemContext;
use crate::dispatcher::{Dispatcher, Trampoline, WorkStealingDispatcher};
use filter::Filter;
use std::sync::Arc;

struct StreamContext {
    dispatcher: WorkStealingDispatcher,
    system_context: ActorSystemContext,
}

pub struct Error; // TODO
pub type BoxedDispatcher = Box<Dispatcher + Send + Sync>; // @TODO

/// A Producer produces elements when requested, often asynchronously.
///
/// This is an implementation detail of Pantomime streams and should
/// not be used directly.
pub trait Producer<A>: Sized + 'static + Send
where
    A: 'static + Send,
{
    /// Attach this producer to the provided consumer. The producer must
    /// transfer ownership of itself back to the consumer at some point
    /// in the future via its `started` method.
    #[must_use]
    fn attach<Consume: Consumer<A>>(
        self,
        consumer: Consume,
        system: ActorSystemContext,
    ) -> Trampoline;

    /// Pull in an element. At some time in the future, the producer must
    /// transfer ownership of itself back to the consumer at some point
    /// in the future via one of the following methods:
    ///
    /// - produced
    /// - completed
    /// - failed
    #[must_use]
    fn pull<Consume: Consumer<A>>(self, consumer: Consume) -> Trampoline;

    /// A request to cancel this producer (within the context of the
    /// provided consumer)
    ///
    /// The consumer will eventually receive a completed or failed
    /// invocation, but may receive additional publishes as well.
    #[must_use]
    fn cancel<Consume: Consumer<A>>(self, consumer: Consume) -> Trampoline;

    fn filter<F: FnMut(&A) -> bool>(
        self,
        filter: F,
    ) -> Attached<A, A, Filter<A, F>, Self, Disconnected>
    where
        F: 'static + Send,
    {
        Attached::new(Filter::new(filter))(self)
    }

    fn filter_map<B, F: FnMut(A) -> Option<B>>(
        self,
        filter_map: F,
    ) -> Attached<A, B, FilterMap<A, B, F>, Self, Disconnected>
    where
        B: 'static + Send,
        F: 'static + Send,
    {
        Attached::new(FilterMap::new(filter_map))(self)
    }

    /// Inserts a stage that converts incoming elements using
    /// the provided function and emits them downstream,
    /// 1 to 1.
    fn map<B, F: FnMut(A) -> B>(self, map: F) -> Attached<A, B, Map<A, B, F>, Self, Disconnected>
    where
        B: 'static + Send,
        F: 'static + Send,
    {
        Attached::new(Map::new(map))(self)
    }

    /// Inserts a stage that decouples upstream and downstream,
    /// allowing them to execute concurrently. This is similar
    /// to an asynchronous boundary in other streaming
    /// abstractions.
    ///
    /// To do this, a `Detached` flow is created which buffers
    /// elements from upstream and sends them downstream in
    /// batches. This work is coordinated by an actor to manage
    /// signals from both directions.
    fn detach(self) -> Detached<A, Self, Disconnected> {
        self.via(Detached::new())
    }

    fn via<B, Down: Consumer<A> + Producer<B>, F: FnOnce(Self) -> Down>(self, f: F) -> Down
    where
        B: 'static + Send,
    {
        f(self)
    }

    /// Run this producer with the provided actor system context
    /// and consumer.
    ///
    /// A thunk is executed by the system dispatcher that uses
    /// fair (yielding) trampolines to run the various stages.
    ///
    /// This producer's attach method is called with the provided
    /// consumer. Contractually, this producer will then eventually
    /// call started on the consumer, giving it the opportunity to
    /// pull elements.
    ///
    /// If this producer has "upstream" producers, i.e. it is itself
    /// a consumer, it should first call the producer's attach method
    /// with itself, and upon receiving the started signal then
    /// forward it downstream. In essence, running a stream consists
    /// of a sequence of attach signals from downstream to upstream,
    /// followed by a sequence of started signals from upstream to
    /// downstream.
    ///
    /// This is intended to be used with so called "Sink" consumers,
    /// those that have a single input and no outputs.
    ///
    /// T
    /// @TODO provide a run_with_dispatcher
    fn run_with<Down: Consumer<A>>(self, consumer: Down, context: ActorSystemContext) {
        let inner_dispatcher = context.dispatcher().safe_clone();
        let inner_context = context.clone();

        context.dispatcher().execute(Box::new(move || {
            inner_dispatcher.execute_trampoline(self.attach(consumer, inner_context));
        }));
    }
}

pub trait Consumer<A>: 'static + Send + Sized
where
    A: 'static + Send,
{
    /// Signals that a producer has started. The consumer must then at some
    /// point in the future call one of the following:
    ///
    /// - pull
    /// - cancel
    #[must_use]
    fn started<Produce: Producer<A>>(self, producer: Produce) -> Trampoline;

    /// Indicates that the producer has produced an element. The consumer must
    /// then at some point in the future call one of the following:
    ///
    /// - pull
    /// - cancel
    #[must_use]
    fn produced<Produce: Producer<A>>(self, producer: Produce, element: A) -> Trampoline;

    /// Indicates that the producer has completed, i.e. it will not produce
    /// any more elements.
    #[must_use]
    fn completed(self) -> Trampoline;

    /// Indicates that the producer has failed, i.e. it has produced an
    /// error.
    #[must_use]
    fn failed(self, error: Error) -> Trampoline;
}

#[cfg(test)]
mod temp_tests {
    use crate::actor::ActorSystem;
    use crate::stream::flow::detached::Detached;
    use crate::stream::for_each::ForEach;
    use crate::stream::iter::Iter;
    use crate::stream::*;

    fn spin(value: usize) -> usize {
        let start = std::time::Instant::now();

        while start.elapsed().subsec_millis() < 10 {}

        value
    }

    #[test]
    fn test() {
        if false {
            return;
        };

        let mut system = ActorSystem::new().start();

        let iterator = 0..usize::max_value();

        Iter::new(iterator).map(spin).detach().map(spin).run_with(
            ForEach::new(|n| {
                println!("sink received {}", n);
                if n == 2000 {
                    std::process::exit(0);
                }
            }),
            system.context.clone(),
        );

        loop {
            // @TODO remove
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
    }

    #[test]
    fn test2() {
        if true {
            return;
        };

        let mut system = ActorSystem::new().start();

        let my_source = Source::iterator(0..10_000);

        fn my_flow<Up: Producer<usize>>(upstream: Up) -> impl Consumer<usize> + Producer<usize> {
            upstream
                .filter(|&n| n % 3 == 0)
                .filter(|&n| n % 5 == 0)
                .filter_map(|n| if n % 7 == 0 { None } else { Some(n * 2) })
                .map(|n| n * 2)
        }

        let my_sink = Sink::for_each(|n: usize| println!("sink received {}", n));

        my_source
            .via(my_flow)
            .run_with(my_sink, system.context.clone());

        loop {
            // @TODO remove
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
    }

    #[test]
    fn test3() {
        if true {
            return;
        }
        let mut system = ActorSystem::new().start();

        let my_source = Source::iterator(0..100_000_000);

        fn my_flow<Up: Producer<usize>>(upstream: Up) -> impl Consumer<usize> + Producer<()> {
            upstream
                .filter(|&n| n % 3 == 0)
                .filter(|&n| n % 5 == 0)
                .filter_map(|n| if n % 7 == 0 { None } else { Some(n * 2) })
                .map(|n| n * 2)
                .map(|n| {
                    if false && n % 1_000_000 == 0 {
                        println!("n: {}", n)
                    } else if n == 399999960 {
                        std::process::exit(0);
                    }
                })
        }

        let my_sink = Sink::ignore();

        my_source
            .via(my_flow)
            .run_with(my_sink, system.context.clone());

        loop {
            // @TODO remove
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
    }
}
