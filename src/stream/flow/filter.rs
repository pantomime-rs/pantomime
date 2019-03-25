use crate::stream::*;
use std::marker::PhantomData;

pub struct Filter<A, F: FnMut(&A) -> bool, Up: Producer<A>, Down: Consumer<A>>
where
    A: 'static + Send,
{
    filter: F,
    upstream: Up,
    downstream: Down,
    runtime: ProducerRuntime,
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
            upstream: upstream,
            downstream: Disconnected,
            phantom: PhantomData,
            runtime: ProducerRuntime::new(),
        }
    }
}

impl<A, F, P, D> Producer<A> for Filter<A, F, P, D>
where
    A: 'static + Send,
    F: FnMut(&A) -> bool + 'static + Send,
    P: Producer<A>,
    D: Consumer<A>,
{
    fn receive<Consume: Consumer<A>>(mut self, command: ProducerCommand<A, Consume>) -> Completed {
        match command {
            ProducerCommand::Attach(consumer, dispatcher) => {
                self.runtime.setup(dispatcher.safe_clone());

                self.upstream.tell(ProducerCommand::Attach(
                    Filter {
                        filter: self.filter,
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
                    Filter {
                        filter: self.filter,
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
                    Filter {
                        filter: self.filter,
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

impl<A, F, P, D> Consumer<A> for Filter<A, F, P, D>
where
    A: 'static + Send,
    F: 'static + FnMut(&A) -> bool + Send,
    P: Producer<A>,
    D: Consumer<A>,
{
    fn receive<Produce: Producer<A>>(mut self, event: ProducerEvent<A, Produce>) -> Completed {
        match event {
            ProducerEvent::Produced(producer, element) => {
                if (self.filter)(&element) {
                    self.downstream.tell(ProducerEvent::Produced(
                        Filter {
                            filter: self.filter,
                            upstream: producer,
                            downstream: Disconnected,
                            runtime: self.runtime,
                            phantom: PhantomData,
                        },
                        element,
                    ));
                } else {
                    producer.tell(ProducerCommand::Request(self, 1));
                }
            }

            ProducerEvent::Started(producer) => {
                self.downstream.tell(ProducerEvent::Started(Filter {
                    filter: self.filter,
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
