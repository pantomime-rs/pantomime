use crate::stream::*;
use std::marker::PhantomData;

pub struct Identity<A, U: Producer<A>, D: Consumer<A>>
where
    A: 'static + Send,
{
    upstream: Option<U>,
    downstream: Option<D>,
    runtime: ProducerRuntime,
    phantom: PhantomData<A>,
}

impl<A, Up> Identity<A, Up, Disconnected>
where
    A: 'static + Send,
    Up: Producer<A>,
{
    pub fn new() -> impl FnOnce(Up) -> Self {
        move |upstream| Self {
            upstream: Some(upstream),
            downstream: None,
            phantom: PhantomData,
            runtime: ProducerRuntime::new(),
        }
    }
}

impl<A, U, D> Producer<A> for Identity<A, U, D>
where
    A: 'static + Send,
    U: Producer<A>,
    D: Consumer<A>,
{
    fn receive<Consume: Consumer<A>>(mut self, command: ProducerCommand<A, Consume>) -> Completed {
        match command {
            ProducerCommand::Attach(consumer, dispatcher) => {
                self.runtime.setup(dispatcher.safe_clone());

                self.upstream.unwrap().tell(ProducerCommand::Attach(
                    Identity {
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
                    Identity {
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
                    Identity {
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

impl<A, U, D> Consumer<A> for Identity<A, U, D>
where
    A: 'static + Send,
    U: Producer<A>,
    D: Consumer<A>,
{
    fn receive<Produce: Producer<A>>(mut self, event: ProducerEvent<A, Produce>) -> Completed {
        match event {
            ProducerEvent::Produced(producer, element) => {
                self.downstream.unwrap().tell(ProducerEvent::Produced(
                    Identity {
                        upstream: Some(producer),
                        downstream: None::<Disconnected>,
                        runtime: self.runtime,
                        phantom: PhantomData,
                    },
                    element,
                ));
            }

            ProducerEvent::Started(producer) => {
                self.downstream
                    .unwrap()
                    .tell(ProducerEvent::Started(Identity {
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
