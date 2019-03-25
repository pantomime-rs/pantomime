use crate::stream::*;
use std::marker::PhantomData;

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
    fn receive<Consume: Consumer<B>>(mut self, command: ProducerCommand<B, Consume>) -> Completed {
        match command {
            ProducerCommand::Attach(consumer, dispatcher) => {
                self.runtime.setup(dispatcher.safe_clone());

                self.upstream.tell(ProducerCommand::Attach(
                    Map {
                        map: self.map,
                        upstream: Disconnected,
                        downstream: consumer,
                        runtime: self.runtime,
                        phantom: PhantomData,
                    },
                    dispatcher,
                ));
            }

            ProducerCommand::Cancel(consumer, _) => {
                self.upstream.tell(ProducerCommand::Cancel(
                    Map {
                        map: self.map,
                        upstream: Disconnected,
                        downstream: consumer,
                        runtime: self.runtime,
                        phantom: PhantomData,
                    },
                    None,
                ));
            }

            ProducerCommand::Request(consumer, demand) => {
                self.upstream.tell(ProducerCommand::Request(
                    Map {
                        map: self.map,
                        upstream: Disconnected,
                        downstream: consumer,
                        runtime: self.runtime,
                        phantom: PhantomData,
                    },
                    demand,
                ));
            }
        }

        Completed
    }

    fn runtime(&mut self) -> Option<&mut ProducerRuntime> {
        Some(&mut self.runtime)
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
    fn receive<Produce: Producer<A>>(mut self, event: ProducerEvent<A, Produce>) -> Completed {
        match event {
            ProducerEvent::Produced(producer, element) => {
                let element = (self.map)(element);

                self.downstream.tell(ProducerEvent::Produced(
                    Map {
                        map: self.map,
                        upstream: producer,
                        downstream: Disconnected,
                        runtime: self.runtime,
                        phantom: PhantomData,
                    },
                    element,
                ));
            }

            ProducerEvent::Started(producer) => {
                self.downstream.tell(ProducerEvent::Started(Map {
                    map: self.map,
                    upstream: producer,
                    downstream: Disconnected,
                    runtime: self.runtime,
                    phantom: PhantomData,
                }));
            }

            ProducerEvent::Completed => {
                self.downstream.tell::<Self>(ProducerEvent::Completed);
            }

            ProducerEvent::Failed(e) => {
                self.downstream.tell::<Self>(ProducerEvent::Failed(e));
            }
        }

        Completed
    }
}
