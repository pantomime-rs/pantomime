pub mod disconnected;
pub mod flow;
pub mod sink;
pub mod source;

pub use disconnected::Disconnected;
pub use flow::*;
pub use sink::*;
pub use source::*;

use crate::actor::ActorSystemContext;
use crate::dispatcher::{BoxedFn1, Dispatcher};
use filter::Filter;
use std::sync::Arc;

const MAX_STACK_SIZE: usize = 10; // @TODO configurable?

pub struct Error; // TODO
pub type BoxedDispatcher = Box<Dispatcher + Send + Sync>; // @TODO

pub enum ProducerCommand<A, Consume: Consumer<A>>
where
    A: 'static + Send,
{
    Attach(Consume, Arc<ActorSystemContext>),
    Request(Consume, usize),
    Cancel(Consume, Option<A>),
}

pub enum ProducerEvent<A, Produce: Producer<A>>
where
    A: 'static + Send,
{
    Started(Produce),
    Produced(Produce, A),
    Completed,
    Failed(Error),
}

pub struct ProducerRuntime {
    dispatcher: Option<BoxedDispatcher>,
    invocations: usize,
}

impl ProducerRuntime {
    pub fn new() -> Self {
        Self {
            dispatcher: None,
            invocations: 0,
        }
    }
}

impl ProducerRuntime {
    fn invoke(&mut self) -> usize {
        self.invocations += 1;
        self.invocations
    }

    fn reset(&mut self) {
        self.invocations = 0;
    }

    fn setup(&mut self, dispatcher: BoxedDispatcher) {
        self.dispatcher = Some(dispatcher);
    }
}

pub enum Bounce<A> {
    Done(A),
    Bounce(Box<BoxedFn1<Bounce<A>> + 'static + Send>),
}

pub struct Completed;

pub(crate) struct Trampoline;

impl Trampoline {
    pub fn bounce<A, F: FnOnce() -> Bounce<A>>(f: F) -> Bounce<A>
    where
        F: 'static + Send,
    {
        Bounce::Bounce(Box::new(f))
    }
}

pub(crate) fn run_fair_trampoline(mut result: Bounce<Completed>, dispatcher: BoxedDispatcher) {
    let mut i = 0;

    loop {
        i += 1;

        match result {
            Bounce::Bounce(next) => {
                if i == 2 {
                    let inner_dispatcher = dispatcher.safe_clone();

                    dispatcher.execute(Box::new(move || {
                        run_fair_trampoline(Bounce::Bounce(next), inner_dispatcher);
                    }));
                    return;
                } else {
                    result = next.apply();
                }
            }

            Bounce::Done(Completed) => {
                return;
            }
        }
    }
}

pub trait Producer<A>: Sized + 'static + Send
where
    A: 'static + Send,
{
    fn attach<Consume: Consumer<A>>(
        self,
        consumer: Consume,
        system: Arc<ActorSystemContext>,
    ) -> Bounce<Completed>;

    fn request<Consume: Consumer<A>>(self, consumer: Consume, demand: usize) -> Bounce<Completed>;

    fn cancel<Consume: Consumer<A>>(self, consumer: Consume) -> Bounce<Completed>;

    fn filter<F: FnMut(&A) -> bool>(self, filter: F) -> Filter<A, F, Self, Disconnected>
    where
        F: 'static + Send,
    {
        Filter::new(filter)(self)
    }

    fn filter_map<B, F: FnMut(A) -> Option<B>>(
        self,
        filter_map: F,
    ) -> FilterMap<A, B, F, Self, Disconnected>
    where
        B: 'static + Send,
        F: 'static + Send,
    {
        FilterMap::new(filter_map)(self)
    }

    fn map<B, F: FnMut(A) -> B>(self, map: F) -> Map<A, B, F, Self, Disconnected>
    where
        B: 'static + Send,
        F: 'static + Send,
    {
        Map::new(map)(self)
    }

    fn detach(self) -> Detached<A, Self, Disconnected> {
        self.via(Detached::new())
    }

    fn via<B, Down: Consumer<A> + Producer<B>, F: FnOnce(Self) -> Down>(self, f: F) -> Down
    where
        B: 'static + Send,
    {
        f(self)
    }

    fn run_with<Down: Consumer<A>>(self, consumer: Down, context: Arc<ActorSystemContext>) {
        let inner_dispatcher = context.dispatcher.safe_clone();
        let inner_context = context.clone();

        context.dispatcher.execute(Box::new(move || {
            let next_inner_dispatcher = inner_dispatcher.safe_clone();

            run_fair_trampoline(self.attach(consumer, inner_context), next_inner_dispatcher);
        }));
    }
}

pub trait Consumer<A>: 'static + Send + Sized
where
    A: 'static + Send,
{
    fn started<Produce: Producer<A>>(self, producer: Produce) -> Bounce<Completed>;

    fn produced<Produce: Producer<A>>(self, producer: Produce, element: A) -> Bounce<Completed>;

    fn completed(self) -> Bounce<Completed>;

    fn failed(self, error: Error) -> Bounce<Completed>;
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

        Iter::new(iterator)
            //.via(Detached::new())
            .map(spin)
            .detach()
            .map(spin)
            //.via(Detached::new())
            //.filter(|n: &usize| n % 3 == 0)
            //.filter(|n: &usize| n % 5 == 0)
            .run_with(
                ForEach::new(|n| {
                    println!("sink received {}", n);
                    if n == 5500 {
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
                    if n % 500_000 == 0 {
                        println!("n: {}", n)
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
