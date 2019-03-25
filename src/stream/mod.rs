pub mod disconnected;
pub mod filter;
pub mod for_each;
pub mod iter;

pub use disconnected::Disconnected;

use crate::dispatcher::Dispatcher;
use filter::Filter;

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

    fn tell_reset<Consume: Consumer<A>>(mut self, command: ProducerCommand<A, Consume>) {
        if let Some(runtime) = self.runtime() {
            runtime.reset();
        }

        self.tell(command);
    }

    fn tell<Consume: Consumer<A>>(mut self, command: ProducerCommand<A, Consume>) {
        match self.runtime() {
            Some(runtime) => {
                match (runtime.invoke(), &runtime.dispatcher) {
                    (n, Some(d)) if n >= 10 => {
                        // @TODO 10 configurable?
                        let d = d.safe_clone();

                        d.execute(Box::new(move || {
                            self.tell_reset(command);
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

    fn via<B, Down: Flow<A, B>, F: FnOnce(Self) -> Down>(self, f: F) -> Down
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

pub trait Flow<A, B>: Sized + 'static + Send + Producer<B> + Consumer<A>
where
    A: 'static + Send,
    B: 'static + Send,
{
}

///
/// Pantomime streams types (Akka-streams inspired)
///

#[cfg(test)]
mod temp_tests {
    use crate::stream::for_each::ForEach;
    use crate::stream::iter::Iter;
    use crate::stream::*;

    #[test]
    fn test() {
        let dispatcher = crate::dispatcher::WorkStealingDispatcher::new(4, true); // @TODO integrate with actor system instead

        let iterator = 0..usize::max_value();

        Iter::new(iterator)
            .filter(|n: &usize| n % 3 == 0)
            .filter(|n: &usize| n % 5 == 0)
            .run_with(
                ForEach::new(|n| println!("sink received {}", n)),
                dispatcher.safe_clone(),
            );

        loop {
            // @TODO remove
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
    }
}
