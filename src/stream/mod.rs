pub mod disconnected;
pub mod flow;
pub mod oxidized;
pub mod sink;
pub mod source;

#[cfg(test)]
mod tests;

pub use disconnected::Disconnected;
pub use flow::*;
pub use sink::*;
pub use source::*;

use crate::actor::ActorSystemContext;
use crate::dispatcher::{Dispatcher, Trampoline};
use filter::Filter;
use merge::Merge;
use oxidized::{Consumer, Producer};
use sink::tell::TellEvent;

pub struct StreamContext {
    dispatcher: Dispatcher,
    system_context: ActorSystemContext,
}

impl StreamContext {
    pub fn dispatcher(&self) -> &Dispatcher {
        &self.dispatcher
    }

    pub fn system_context(&self) -> &ActorSystemContext {
        &self.system_context
    }

    fn new(dispatcher: &Dispatcher, system_context: &ActorSystemContext) -> Self {
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

pub enum Terminated {
    Completed,
    Failed(Error),
}

pub trait Stage<A>: Producer<A>
where
    A: 'static + Send,
{
    #[must_use]
    fn filter<F: FnMut(&A) -> bool>(
        self,
        filter: F,
    ) -> Attached<A, A, Filter<A, F>, Self, Disconnected>
    where
        F: 'static + Send,
    {
        Attached::new(Filter::new(filter))(self)
    }

    #[must_use]
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
    #[must_use]
    fn map<B, F: FnMut(A) -> B>(self, map: F) -> Attached<A, B, Map<A, B, F>, Self, Disconnected>
    where
        A: 'static + Send,
        B: 'static + Send,
        F: 'static + Send,
    {
        Attached::new(Map::new(map))(self)
    }

    #[must_use]
    fn merge<Other: Stage<A>>(
        self,
        other: Other,
    ) -> Detached<A, A, TellEvent<A>, Merge<A, Other>, Self, Disconnected>
    where
        A: 'static + Send,
        Other: 'static + Send,
    {
        Detached::new(Merge::new(other))(self)
    }

    #[must_use]
    fn take_while<F: FnMut(&A) -> bool>(
        self,
        func: F,
    ) -> Attached<A, A, TakeWhile<A, F>, Self, Disconnected>
    where
        A: 'static + Send,
        F: 'static + Send,
    {
        Attached::new(TakeWhile::new(func))(self)
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
    #[must_use]
    fn detach(self) -> Detached<A, A, (), Identity, Self, Disconnected> {
        self.via(Detached::new(Identity))
    }

    #[must_use]
    fn via<B, Down: Flow<A, B>, F: FnOnce(Self) -> Down>(self, f: F) -> Down
    where
        B: 'static + Send,
    {
        f(self)
    }

    #[must_use]
    fn to<Down: Sink<A>, F: FnOnce(Self) -> Down>(self, sink: F) -> Down {
        sink(self)
    }

    fn run_with<Down: Sink<A>, F: FnOnce(Self) -> Down>(
        self,
        sink: F,
        context: &ActorSystemContext,
    ) {
        self.to(sink).run(context)
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
    fn run(self, context: &ActorSystemContext) {
        let dispatcher = context.dispatcher();
        let inner_dispatcher = dispatcher.clone();
        let stream_context = StreamContext::new(&dispatcher, &context);

        dispatcher.execute(move || {
            inner_dispatcher.execute_trampoline(self.start(&stream_context));
        });
    }

    fn start(self, stream_context: &StreamContext) -> Trampoline;
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    fn spin(value: usize) -> usize {
        let start = std::time::Instant::now();

        while start.elapsed().subsec_millis() < 10 {}

        value
    }

    #[test]
    fn test_for_each_add() {
        struct TestReaper;

        impl Actor<()> for TestReaper {
            fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}

            fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
                if let Signal::Started = signal {}
            }
        }
        if true {
            return;
        };

        let mut system = ActorSystem::new().start();

        let iterator = 0..2000;

        let completed = Arc::new(AtomicBool::new(false));
        let sum = Arc::new(AtomicUsize::new(0));

        {
            let completed = completed.clone();
            let sum = sum.clone();

            Iter::new(iterator)
                .map(spin)
                .detach()
                .map(spin)
                .to(ForEach::new(move |n| {
                    sum.fetch_add(n, Ordering::SeqCst);
                }))
                .watch_termination(move |terminated| match terminated {
                    Terminated::Completed => {
                        completed.store(true, Ordering::SeqCst);
                    }

                    Terminated::Failed(_) => {
                        panic!("unexpected");
                    }
                })
                .run(&system.context);
        }

        loop {
            // @TODO remove
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
    }

    #[test]
    fn test() {
        struct TestReaper;

        impl Actor<()> for TestReaper {
            fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}

            fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
                if let Signal::Started = signal {}
            }
        }
        if true {
            return;
        };

        let mut system = ActorSystem::new().start();

        let iterator = 0..2000;

        let completed = Arc::new(AtomicBool::new(false));
        let sum = Arc::new(AtomicUsize::new(0));

        Iter::new(iterator).map(spin).detach().map(spin).run_with(
            ForEach::new(|n| {
                println!("sink received {}", n);
            }),
            &system.context,
        );

        loop {
            // @TODO remove
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
    }

    #[test]
    fn test2() {
        struct TestReaper;

        impl Actor<()> for TestReaper {
            fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}

            fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
                if let Signal::Started = signal {}
            }
        }
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

        my_source.via(my_flow).run_with(my_sink, &system.context);

        loop {
            // @TODO remove
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
    }

    #[test]
    fn test3() {
        struct TestReaper;

        impl Actor<()> for TestReaper {
            fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}

            fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
                if let Signal::Started = signal {}
            }
        }
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

        my_source.via(my_flow).run_with(my_sink, &system.context);

        loop {
            // @TODO remove
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
    }
}
