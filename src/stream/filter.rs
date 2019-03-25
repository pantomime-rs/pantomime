use crate::stream::*;
use std::marker::PhantomData;

pub struct Filter<A, F: FnMut(&A) -> bool, U: Producer<A>, D: Consumer<A>>
where
    A: 'static + Send,
{
    filter: F,
    upstream: Option<U>,
    downstream: Option<D>,
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
            upstream: Some(upstream),
            downstream: None,
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

                self.upstream.unwrap().tell(ProducerCommand::Attach(
                    Filter {
                        filter: self.filter,
                        upstream: None::<Disconnected>,
                        downstream: Some(consumer),
                        runtime: self.runtime,
                        phantom: PhantomData,
                    },
                    dispatcher,
                ));
            }

            ProducerCommand::Cancel(consumer, _) => {
                self.upstream.unwrap().tell(ProducerCommand::Cancel(
                    Filter {
                        filter: self.filter,
                        upstream: None::<Disconnected>,
                        downstream: Some(consumer),
                        runtime: self.runtime,
                        phantom: PhantomData,
                    },
                    None,
                ));
            }

            ProducerCommand::Request(consumer, demand) => {
                self.upstream.unwrap().tell(ProducerCommand::Request(
                    Filter {
                        filter: self.filter,
                        upstream: None::<Disconnected>,
                        downstream: Some(consumer),
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
                    self.downstream.unwrap().tell(ProducerEvent::Produced(
                        Filter {
                            filter: self.filter,
                            upstream: Some(producer),
                            downstream: None::<Disconnected>,
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
                self.downstream
                    .unwrap()
                    .tell(ProducerEvent::Started(Filter {
                        filter: self.filter,
                        upstream: Some(producer),
                        downstream: None::<Disconnected>,
                        runtime: self.runtime,
                        phantom: PhantomData,
                    }));
            }

            ProducerEvent::Completed => {
                self.downstream
                    .unwrap()
                    .tell::<Self>(ProducerEvent::Completed);
            }

            ProducerEvent::Failed(e) => {
                self.downstream
                    .unwrap()
                    .tell::<Self>(ProducerEvent::Failed(e));
            }
        }

        Completed
    }
}

impl<A, F, P, D> Flow<A, A> for Filter<A, F, P, D>
where
    A: 'static + Send,
    F: 'static + FnMut(&A) -> bool + Send,
    P: Producer<A>,
    D: Consumer<A>,
{
}
