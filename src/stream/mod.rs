pub mod disconnected;
pub mod filter;
pub mod flow;
pub mod for_each;
pub mod identity;
pub mod iter;
pub mod map;
pub mod sink;
pub mod source;

pub use disconnected::Disconnected;
pub use flow::Flow;
pub use map::Map;
pub use sink::Sink;
pub use source::Source;

use crate::dispatcher::Dispatcher;
use filter::Filter;

const MAX_STACK_SIZE: usize = 10; // @TODO configurable?

pub struct Error; // TODO
pub type BoxedDispatcher = Box<Dispatcher + Send + Sync>; // @TODO

pub enum ProducerCommand<A, Consume: Consumer<A>>
where
    A: 'static + Send,
{
    Attach(Consume, BoxedDispatcher),
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

pub struct Completed;

pub trait Producer<A>: Sized + 'static + Send
where
    A: 'static + Send,
{
    #[must_use]
    fn receive<Consume: Consumer<A>>(self, command: ProducerCommand<A, Consume>) -> Completed;

    fn tell<Consume: Consumer<A>>(mut self, command: ProducerCommand<A, Consume>) {
        match self.runtime() {
            Some(runtime) => {
                match (runtime.invoke(), &runtime.dispatcher) {
                    (n, Some(d)) if n >= MAX_STACK_SIZE => {
                        let d = d.safe_clone();

                        d.execute(Box::new(move || {
                            if let Some(runtime) = self.runtime() {
                                runtime.reset();
                            }

                            self.tell(command);
                        }));
                    }

                    _ => {
                        let _ = self.receive(command);
                        // @TODO should reset runtime here, but we've been moved into receive
                        // @TODO need to profile to figure out best course of action
                        // runtime.reset();
                    }
                }
            }

            None => {
                let _ = self.receive(command);
            }
        }
    }

    fn filter<F: FnMut(&A) -> bool>(self, filter: F) -> Filter<A, F, Self, Disconnected>
    where
        F: 'static + Send,
    {
        Filter::new(filter)(self)
    }

    fn map<B, F: FnMut(A) -> B>(self, map: F) -> Map<A, B, F, Self, Disconnected>
    where
        B: 'static + Send,
        F: 'static + Send,
    {
        Map::new(map)(self)
    }

    fn via<B, Down: Consumer<A> + Producer<B>, F: FnOnce(Self) -> Down>(self, f: F) -> Down
    where
        B: 'static + Send,
    {
        f(self)
    }

    fn run_with<Down: Consumer<A>>(self, consumer: Down, dispatcher: BoxedDispatcher) {
        let inner_dispatcher = dispatcher.safe_clone();
        dispatcher.execute(Box::new(move || {
            self.tell(ProducerCommand::Attach(consumer, inner_dispatcher));
        }));
    }

    fn runtime(&mut self) -> Option<&mut ProducerRuntime>;
}

pub trait Consumer<A>: 'static + Send + Sized
where
    A: 'static + Send,
{
    #[must_use]
    fn receive<Produce: Producer<A>>(self, event: ProducerEvent<A, Produce>) -> Completed;

    #[inline(always)]
    fn tell<Produce: Producer<A>>(self, event: ProducerEvent<A, Produce>) {
        let _ = self.receive(event);
    }
}

///
/// Pantomime streams types (Akka-streams inspired)
///

#[cfg(test)]
mod temp_tests {
    use crate::stream::for_each::ForEach;
    use crate::stream::identity::Identity;
    use crate::stream::iter::Iter;
    use crate::stream::*;

    #[test]
    fn test() {
        return;

        let dispatcher = crate::dispatcher::WorkStealingDispatcher::new(4, true).safe_clone(); // @TODO integrate with actor system instead

        let iterator = 0..usize::max_value();

        Iter::new(iterator)
            .filter(|n: &usize| n % 3 == 0)
            .filter(|n: &usize| n % 5 == 0)
            .run_with(
                ForEach::new(|n| println!("sink received {}", n)),
                dispatcher,
            );

        loop {
            // @TODO remove
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
    }

    #[test]
    fn test2() {
        let dispatcher = crate::dispatcher::WorkStealingDispatcher::new(4, true).safe_clone(); // @TODO integrate with actor system instead

        let my_source = Source::iterator(0..10_000);

        fn my_flow<Up: Producer<usize>>(upstream: Up) -> impl Consumer<usize> + Producer<usize> {
            upstream
                .filter(|&n| n % 3 == 0)
                .filter(|&n| n % 5 == 0)
                .map(|n| n * 2)
        }

        let my_sink = Sink::for_each(|n: usize| println!("sink received {}", n));

        my_source.via(my_flow).run_with(my_sink, dispatcher);

        loop {
            // @TODO remove
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }

        /*
        fn my_flow<Up: Producer<usize>, F: 'static + FnMut(&usize) -> bool + Send,>() -> impl Fn(Up) -> usize {
            move |upstream: Up| upstream.filter(|&a| a % 7 == 0)
        }*/

        //let flow =
        //  Flow::filter(|n: &usize| n % 7 == 0);

        //let flow =
    }
}
