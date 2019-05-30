pub mod disconnected;
pub mod flow;
pub mod oxidized;
pub mod sink;
pub mod source;

#[cfg(test)]
mod tests;

pub use disconnected::Disconnected;
pub use flow::*;
pub use sink::tell::TellEvent;
pub use sink::*;
pub use source::*;

use crate::actor::{ActorContext, ActorSystemContext};
use crate::dispatcher::{Dispatcher, Trampoline};
use filter::Filter;
use oxidized::{Consumer, Producer};

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
    fn start(self, stream_context: &StreamContext) -> Trampoline;
}

pub trait Flow<A, B>: Consumer<A> + Stage<B>
where
    A: 'static + Send,
    B: 'static + Send,
{
}

pub trait SpawnStream {
    fn spawn_stream<M, Stream: Sink<M>>(&self, stream: Stream)
    where
        M: 'static + Send,
        Stream: 'static + Send;
}

impl<N> SpawnStream for ActorContext<N>
where
    N: 'static + Send,
{
    fn spawn_stream<M, Stream: Sink<M>>(&self, stream: Stream)
    where
        M: 'static + Send,
        Stream: 'static + Send,
    {
        let context = self.system_context();
        let dispatcher = context.dispatcher();
        let inner_dispatcher = dispatcher.clone();
        let stream_context = StreamContext::new(&dispatcher, &context);

        dispatcher.execute(move || {
            inner_dispatcher.execute_trampoline(stream.start(&stream_context));
        });
    }
}

impl SpawnStream for StreamContext {
    fn spawn_stream<M, Stream: Sink<M>>(&self, stream: Stream)
    where
        M: 'static + Send,
        Stream: 'static + Send,
    {
        let inner_dispatcher = self.dispatcher.clone();
        let stream_context = StreamContext::new(&self.dispatcher, &self.system_context);

        self.dispatcher.execute(move || {
            inner_dispatcher.execute_trampoline(stream.start(&stream_context));
        });
    }
}

impl SpawnStream for ActorSystemContext {
    fn spawn_stream<M, Stream: Sink<M>>(&self, stream: Stream)
    where
        M: 'static + Send,
        Stream: 'static + Send,
    {
        let dispatcher = self.dispatcher();
        let inner_dispatcher = dispatcher.clone();
        let stream_context = StreamContext::new(&dispatcher, &self);

        dispatcher.execute(move || {
            inner_dispatcher.execute_trampoline(stream.start(&stream_context));
        });
    }
}

#[cfg(test)]
mod temp_tests {
    use crate::actor::*;
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
                if let Signal::Started = signal {
                    let iterator = 0..2000;

                    let completed = Arc::new(AtomicBool::new(false));
                    let sum = Arc::new(AtomicUsize::new(0));

                    {
                        let completed = completed.clone();
                        let sum = sum.clone();

                        let stream = Iter::new(iterator)
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
                            });

                        ctx.spawn_stream(stream);
                    }

                    loop {
                        // @TODO remove
                        std::thread::sleep(std::time::Duration::from_millis(1000));
                    }
                }
            }
        }
        if true {
            return;
        };

        assert!(ActorSystem::new().spawn(TestReaper).is_ok());
    }

    #[test]
    fn test() {
        struct TestReaper;

        impl Actor<()> for TestReaper {
            fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}

            fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
                if let Signal::Started = signal {
                    let iterator = 0..2000;

                    let stream = Iter::new(iterator)
                        .map(spin)
                        .detach()
                        .map(spin)
                        .to(ForEach::new(|_n| {}));

                    ctx.spawn_stream(stream);

                    loop {
                        // @TODO remove
                        std::thread::sleep(std::time::Duration::from_millis(1000));
                    }
                }
            }
        }

        if true {
            return;
        };

        assert!(ActorSystem::new().spawn(TestReaper).is_ok());
    }

    #[test]
    fn test2() {
        struct TestReaper;

        impl Actor<()> for TestReaper {
            fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}

            fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
                if let Signal::Started = signal {
                    let my_source = Sources::iterator(0..10_000);

                    fn my_flow<Up: Stage<usize>>(upstream: Up) -> impl Flow<usize, usize> {
                        upstream
                            .filter(|&n| n % 3 == 0)
                            .filter(|&n| n % 5 == 0)
                            .filter_map(|n| if n % 7 == 0 { None } else { Some(n * 2) })
                            .map(|n| n * 2)
                    }

                    let my_sink = Sinks::for_each(|n: usize| println!("sink received {}", n));

                    ctx.spawn_stream(my_source.via(my_flow).to(my_sink));

                    loop {
                        // @TODO remove
                        std::thread::sleep(std::time::Duration::from_millis(1000));
                    }
                }
            }
        }
        if true {
            return;
        };

        assert!(ActorSystem::new().spawn(TestReaper).is_ok());
    }

    #[test]
    fn test3() {
        struct TestReaper;

        impl Actor<()> for TestReaper {
            fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}

            fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
                if let Signal::Started = signal {
                    let my_source = Sources::iterator(0..100_000_000);

                    fn my_flow<Up: Stage<usize>>(upstream: Up) -> impl Flow<usize, ()> {
                        upstream
                            .filter(|&n| n % 3 == 0)
                            .filter(|&n| n % 5 == 0)
                            .filter_map(|n| if n % 7 == 0 { None } else { Some(n * 2) })
                            .map(|n| n * 2)
                            .map(|n| {
                                if n == 399_999_960 {
                                    std::process::exit(0);
                                }
                            })
                    }

                    let my_sink = Sinks::ignore();

                    ctx.spawn_stream(my_source.via(my_flow).to(my_sink));

                    loop {
                        // @TODO remove
                        std::thread::sleep(std::time::Duration::from_millis(1000));
                    }
                }
            }
        }

        if true {
            return;
        }

        assert!(ActorSystem::new().spawn(TestReaper).is_ok());
    }
}
