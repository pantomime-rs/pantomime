pub mod disconnected;
pub mod flow;
pub mod oxidized;
pub mod sink;
pub mod source;

pub use disconnected::Disconnected;
pub use flow::*;
pub use sink::*;
pub use source::*;

use crate::actor::ActorSystemContext;
use crate::dispatcher::{Dispatcher, Trampoline, WorkStealingDispatcher};
use filter::Filter;
use oxidized::{Consumer, Producer};

pub struct StreamContext {
    dispatcher: WorkStealingDispatcher,
    system_context: ActorSystemContext,
}

impl StreamContext {
    pub fn dispatcher(&self) -> &WorkStealingDispatcher {
        &self.dispatcher
    }

    pub fn system_context(&self) -> &ActorSystemContext {
        &self.system_context
    }

    fn new(dispatcher: &WorkStealingDispatcher, system_context: &ActorSystemContext) -> Self {
        Self {
            dispatcher: dispatcher.clone(),
            system_context: system_context.clone(),
        }
    }
}

impl Clone for StreamContext {
    fn clone(&self) -> Self {
        Self {
            dispatcher: self.dispatcher.clone(),
            system_context: self.system_context.clone(),
        }
    }
}

pub struct Error; // TODO
pub type BoxedDispatcher = Box<Dispatcher + Send + Sync>; // @TODO

pub trait Stage<A>: Producer<A>
where
    A: 'static + Send,
{
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
        A: 'static + Send,
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
        A: 'static + Send,
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
    fn detach(self) -> Detached<A, A, (), Identity, Self, Disconnected> {
        self.via(Detached::new(Identity))
    }

    fn via<B, Down: Flow<A, B>, F: FnOnce(Self) -> Down>(self, f: F) -> Down
    where
        B: 'static + Send,
    {
        f(self)
    }

    /// Run the stream by spawning an actor and attaching the provided sink.
    ///
    /// The actor topology of a stream's execution depends on the stages
    /// that comprise it.
    ///
    /// If the stream is only comprised of attached stages. In general, all
    /// attached stages are run within the context of the current actor. All
    /// detached stages spawn a new actor to transparently buffer elements
    /// to improve performance when handing them over an asynchronous
    /// boundary.
    fn run_with<S: Sink<A>>(self, sink: S, context: ActorSystemContext) {
        let dispatcher = context.dispatcher();
        let inner_dispatcher = dispatcher.clone();
        let stream_context = StreamContext::new(&dispatcher, &context);

        dispatcher.execute(Box::new(move || {
            inner_dispatcher.execute_trampoline(self.attach(sink, &stream_context));
        }));
    }
}

pub trait Source<A>: Stage<A>
where
    A: 'static + Send,
{
}

pub trait Sink<A>: Consumer<A>
where
    A: 'static + Send,
{
}

pub trait Flow<A, B>: Consumer<A> + Stage<B>
where
    A: 'static + Send,
    B: 'static + Send,
{
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

        let my_source = Sources::iterator(0..10_000);

        fn my_flow<Up: Stage<usize>>(upstream: Up) -> impl Flow<usize, usize> {
            upstream
                .filter(|&n| n % 3 == 0)
                .filter(|&n| n % 5 == 0)
                .filter_map(|n| if n % 7 == 0 { None } else { Some(n * 2) })
                .map(|n| n * 2)
        }

        let my_sink = Sinks::for_each(|n: usize| println!("sink received {}", n));

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

        let my_source = Sources::iterator(0..100_000_000);

        fn my_flow<Up: Stage<usize>>(upstream: Up) -> impl Flow<usize, ()> {
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

        let my_sink = Sinks::ignore();

        my_source
            .via(my_flow)
            .run_with(my_sink, system.context.clone());

        loop {
            // @TODO remove
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
    }
}
