use crate::actor::*;
use crate::dispatcher::{Dispatcher, Trampoline};
use crate::stream::disconnected::Disconnected;
use crate::stream::*;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;

const BUFFER_SIZE: usize = 2; // @TODO config

trait GenProducer {
    fn attach(self: Box<Self>, context: Arc<ActorSystemContext>) -> Trampoline;

    fn pull(self: Box<Self>) -> Trampoline;

    fn cancel(self: Box<Self>) -> Trampoline;
}

trait GenConsumer<A> {
    fn started(self: Box<Self>) -> Trampoline;

    fn produced(self: Box<Self>, element: A) -> Trampoline;

    fn completed(self: Box<Self>) -> Trampoline;

    fn failed(self: Box<Self>, error: Error) -> Trampoline;
}

enum DetachedActorMsg<A> {
    Started(Box<GenProducer + 'static + Send>),
    Produced(Box<GenProducer + 'static + Send>, A),
    Completed,
    Failed(Error),
    Pulled(Box<GenConsumer<A> + 'static + Send>),
    Cancelled(Box<GenConsumer<A> + 'static + Send>),
    Attached(Box<GenConsumer<A> + 'static + Send>),
}

struct DetachedActor<A> {
    phantom: PhantomData<A>,
    buffer: VecDeque<A>,
    producer: Option<Box<GenProducer + 'static + Send>>,
    consumer: Option<Box<GenConsumer<A> + 'static + Send>>,
    cancelled: bool,
    completed: bool,
    failed: Option<Error>,
}

impl<A> DetachedActor<A> {
    fn new() -> Self {
        Self {
            phantom: PhantomData,
            buffer: VecDeque::new(),
            producer: None,
            consumer: None,
            cancelled: false,
            completed: false,
            failed: None,
        }
    }
}

impl<A> Actor<DetachedActorMsg<A>> for DetachedActor<A>
where
    A: 'static + Send,
{
    fn receive(
        &mut self,
        msg: DetachedActorMsg<A>,
        context: &mut ActorContext<DetachedActorMsg<A>>,
    ) {
        match msg {
            DetachedActorMsg::Started(producer) => {
                if self.cancelled {
                    context
                        .system_dispatcher()
                        .execute_trampoline(producer.cancel());
                } else {
                    context
                        .system_dispatcher()
                        .execute_trampoline(producer.pull());
                }
            }

            DetachedActorMsg::Produced(producer, el) => {
                self.buffer.push_back(el);

                if self.cancelled {
                    context
                        .system_dispatcher()
                        .execute_trampoline(producer.cancel());
                } else if self.buffer.len() < BUFFER_SIZE {
                    let next_dispatcher = context.system_dispatcher().safe_clone();

                    let dispatcher = context.system_dispatcher();
                    let inner_dispatcher = dispatcher.clone();

                    context.system_dispatcher().execute(Box::new(move || {
                        inner_dispatcher.execute_trampoline(producer.pull());
                    }));
                } else {
                    self.producer = Some(producer);
                }

                if let Some(consumer) = self.consumer.take() {
                    context.actor_ref().tell(DetachedActorMsg::Pulled(consumer));
                }
            }

            DetachedActorMsg::Completed => {
                self.completed = true;

                if let Some(consumer) = self.consumer.take() {
                    context
                        .system_dispatcher()
                        .execute_trampoline(consumer.completed());
                }
            }

            DetachedActorMsg::Failed(error) => match self.consumer.take() {
                Some(consumer) => {
                    context
                        .system_dispatcher()
                        .execute_trampoline(consumer.failed(error));
                }

                None => {
                    self.failed = Some(error);
                }
            },

            DetachedActorMsg::Pulled(consumer) => {
                if self.completed {
                    context
                        .system_dispatcher()
                        .execute_trampoline(consumer.completed());
                } else if let Some(error) = self.failed.take() {
                    context
                        .system_dispatcher()
                        .execute_trampoline(consumer.failed(error));
                } else {
                    match self.buffer.pop_front() {
                        Some(elem) => {
                            let dispatcher = context.system_dispatcher();

                            if self.buffer.len() < BUFFER_SIZE {
                                if let Some(producer) = self.producer.take() {
                                    let inner_dispatcher = dispatcher.clone();

                                    dispatcher.execute(Box::new(move || {
                                        inner_dispatcher.execute_trampoline(producer.pull());
                                    }));
                                }
                            }

                            let inner_dispatcher = dispatcher.clone();

                            dispatcher.execute(Box::new(move || {
                                inner_dispatcher.execute_trampoline(consumer.produced(elem));
                            }));
                        }

                        None => {
                            self.consumer = Some(consumer);
                        }
                    }
                }
            }

            DetachedActorMsg::Cancelled(consumer) => {
                self.consumer = Some(consumer);

                if let Some(producer) = self.producer.take() {
                    context
                        .system_dispatcher()
                        .execute_trampoline(producer.cancel());
                } else {
                    self.cancelled = true;
                }
            }

            DetachedActorMsg::Attached(consumer) => {
                context
                    .system_dispatcher()
                    .execute_trampoline(consumer.started());
            }
        }
    }
}

/// Detached inserts a boundary, allowing upstream and downstream to make progress
/// simultaneously.
///
/// Detached always tries to keep a buffer full, typically 16 elements, to reduce
/// the overhead of asynchronous communication.
///
/// When it receives a demand request from downstream, it requests `bufferSize`
/// elements from upstream, and schedules a thunk to message itself a push
/// message.
///
/// When receiving a push message, detached takes all of the buffered messages
/// and puts them in a new publisher, and sends that downstream. Once the buffer
/// is empty, more demand comes from downstream.
pub struct Detached<A, Up: Producer<A>, Down: Consumer<A>>
where
    A: 'static + Send,
{
    upstream: Up,
    downstream: Down,
    actor_ref: ActorRef<DetachedActorMsg<A>>,
    phantom: PhantomData<A>,
}

impl<A, Up> Detached<A, Up, Disconnected>
where
    A: 'static + Send,
    Up: Producer<A>,
{
    pub fn new() -> impl FnOnce(Up) -> Self {
        move |upstream| Self {
            upstream,
            downstream: Disconnected,
            actor_ref: ActorRef::empty(),
            phantom: PhantomData,
        }
    }
}

impl<A, Up: Producer<A>, Down: Consumer<A>> Detached<A, Up, Down>
where
    A: 'static + Send,
{
    fn disconnect_downstream<Produce: Producer<A>>(
        self,
        producer: Produce,
    ) -> (Down, Detached<A, Produce, Disconnected>) {
        (
            self.downstream,
            Detached {
                upstream: producer,
                downstream: Disconnected,
                actor_ref: self.actor_ref,
                phantom: PhantomData,
            },
        )
    }

    fn disconnect_upstream<Consume: Consumer<A>>(
        self,
        consumer: Consume,
    ) -> (Up, Detached<A, Disconnected, Consume>) {
        (
            self.upstream,
            Detached {
                upstream: Disconnected,
                downstream: consumer,
                actor_ref: self.actor_ref,
                phantom: PhantomData,
            },
        )
    }
}

impl<A, Up: Producer<A>, Down: Consumer<A>> Producer<A> for Detached<A, Up, Down>
where
    A: 'static + Send,
    Up: 'static + Send,
{
    fn attach<Consume: Consumer<A>>(
        mut self,
        consumer: Consume,
        context: Arc<ActorSystemContext>,
    ) -> Trampoline {
        let actor_ref = ActorSystemContext::spawn(&context, DetachedActor::<A>::new());

        let upstream = Detached {
            upstream: Disconnected,
            downstream: Disconnected,
            actor_ref: actor_ref.clone(),
            phantom: PhantomData,
        };

        let downstream = Detached {
            upstream: Disconnected,
            downstream: consumer,
            actor_ref,
            phantom: PhantomData,
        };

        upstream
            .actor_ref
            .tell(DetachedActorMsg::Attached(Box::new(downstream)));

        self.upstream.attach(upstream, context)
    }

    fn pull<Consume: Consumer<A>>(self, consumer: Consume) -> Trampoline {
        let actor_ref = self.actor_ref.clone();

        let (_, detached) = self.disconnect_upstream(consumer);

        actor_ref.tell(DetachedActorMsg::Pulled(Box::new(detached)));

        Trampoline::done()
    }

    fn cancel<Consume: Consumer<A>>(self, consumer: Consume) -> Trampoline {
        let actor_ref = self.actor_ref.clone();

        let (_, detached) = self.disconnect_upstream(consumer);

        actor_ref.tell(DetachedActorMsg::Cancelled(Box::new(detached)));

        Trampoline::done()
    }
}

impl<A, Up: Producer<A>, Down: Consumer<A>> Consumer<A> for Detached<A, Up, Down>
where
    A: 'static + Send,
    Up: 'static + Send,
{
    fn started<Produce: Producer<A>>(self, producer: Produce) -> Trampoline {
        let actor_ref = self.actor_ref.clone();

        let (_, detached) = self.disconnect_downstream(producer);

        actor_ref.tell(DetachedActorMsg::Started(Box::new(detached)));

        Trampoline::done()
    }

    fn produced<Produce: Producer<A>>(mut self, producer: Produce, element: A) -> Trampoline {
        let actor_ref = self.actor_ref.clone();

        let (_, detached) = self.disconnect_downstream(producer);

        actor_ref.tell(DetachedActorMsg::Produced(Box::new(detached), element));

        Trampoline::done()
    }

    fn completed(self) -> Trampoline {
        self.actor_ref.tell(DetachedActorMsg::Completed);

        Trampoline::done()
    }

    fn failed(self, error: Error) -> Trampoline {
        self.actor_ref.tell(DetachedActorMsg::Failed(error));

        Trampoline::done()
    }
}

impl<A, Up: Producer<A>, Down: Consumer<A>> GenConsumer<A> for Detached<A, Up, Down>
where
    A: 'static + Send,
{
    fn started(self: Box<Self>) -> Trampoline {
        let (downstream, detached) = self.disconnect_downstream(Disconnected);

        downstream.started(detached)
    }

    fn produced(self: Box<Self>, element: A) -> Trampoline {
        let (downstream, detached) = self.disconnect_downstream(Disconnected);

        downstream.produced(detached, element)
    }

    fn completed(self: Box<Self>) -> Trampoline {
        let (downstream, detached) = self.disconnect_downstream(Disconnected);

        downstream.completed()
    }

    fn failed(self: Box<Self>, error: Error) -> Trampoline {
        let (downstream, detached) = self.disconnect_downstream(Disconnected);

        downstream.failed(error)
    }
}

impl<A, Up: Producer<A>, Down: Consumer<A>> GenProducer for Detached<A, Up, Down>
where
    A: 'static + Send,
{
    fn attach(self: Box<Self>, context: Arc<ActorSystemContext>) -> Trampoline {
        let (upstream, detached) = self.disconnect_upstream(Disconnected);

        upstream.attach(Disconnected, context)
    }

    fn pull(self: Box<Self>) -> Trampoline {
        let (upstream, detached) = self.disconnect_upstream(Disconnected);

        upstream.pull(detached)
    }

    fn cancel(self: Box<Self>) -> Trampoline {
        let (upstream, detached) = self.disconnect_upstream(Disconnected);

        upstream.cancel(Disconnected)
    }
}
