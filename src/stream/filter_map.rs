use crate::stream::*;
use std::marker::PhantomData;

pub struct FilterMap<A, B, F: FnMut(A) -> Option<B>, Up: Producer<A>, Down: Consumer<B>>
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
            runtime: ProducerRuntime::new(),
        }
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
    fn receive<Consume: Consumer<B>>(mut self, command: ProducerCommand<B, Consume>) -> Completed {
        match command {
            ProducerCommand::Attach(consumer, dispatcher) => {
                self.runtime.setup(dispatcher.safe_clone());

                self.upstream.tell(ProducerCommand::Attach(
                    FilterMap {
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
                    FilterMap {
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
                    FilterMap {
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

impl<A, B, F: FnMut(A) -> Option<B>, Up: Producer<A>, Down: Consumer<B>> Consumer<A>
    for FilterMap<A, B, F, Up, Down>
where
    A: 'static + Send,
    B: 'static + Send,
    F: 'static + Send,
    Up: 'static + Send,
    Down: 'static + Send,
{
    fn receive<Produce: Producer<A>>(mut self, event: ProducerEvent<A, Produce>) -> Completed {
        match event {
            ProducerEvent::Produced(producer, element) => match (self.map)(element) {
                Some(element) => {
                    self.downstream.tell(ProducerEvent::Produced(
                        FilterMap {
                            map: self.map,
                            upstream: producer,
                            downstream: Disconnected,
                            runtime: self.runtime,
                            phantom: PhantomData,
                        },
                        element,
                    ));
                }

                None => {
                    producer.tell(ProducerCommand::Request(self, 1));
                }
            },

            ProducerEvent::Started(producer) => {
                self.downstream.tell(ProducerEvent::Started(FilterMap {
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
