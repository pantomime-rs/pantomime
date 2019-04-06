use crate::actor::*;
use crate::dispatcher::{Dispatcher, Trampoline};
use crate::stream::disconnected::Disconnected;
use crate::stream::*;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const BUFFER_SIZE: usize = 16; // @TODO config

pub enum AsyncAction<B, Msg> {
    Cancel,
    Complete,
    Fail(Error),
    Forward(Msg),
    Pull,
    Push(B),
}

pub enum CompletingAsyncAction<B, Msg> {
    Done(Option<B>),
    Forward(Msg),
}

pub trait DetachedFlowLogic<A, B, Msg>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    fn attach(&mut self, context: ActorSystemContext);

    fn forwarded(&mut self, msg: Msg, actor_ref: ActorRef<AsyncAction<B, Msg>>);

    fn produced(&mut self, elem: A, actor_ref: ActorRef<AsyncAction<B, Msg>>);

    fn pulled(&mut self, actor_ref: ActorRef<AsyncAction<B, Msg>>);

    fn completed(&mut self, actor_ref: ActorRef<CompletingAsyncAction<B, Msg>>);

    fn failed(error: &Error, actor_ref: ActorRef<CompletingAsyncAction<B, Msg>>);
}

trait GenProducer {
    fn attach(self: Box<Self>, context: ActorSystemContext) -> Trampoline;

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
    Produced(A),
    DoneProducing(Box<GenProducer + 'static + Send>),
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

    /// Tracks demand and is shared with the upstream producer
    /// which continues to consume from its upstream if there
    /// is a demand.
    ///
    /// It's important that demand is only ever added to from
    /// the actor, and only ever subtracted from the upstream.
    demand: Arc<AtomicUsize>,
}

impl<A> DetachedActor<A> {
    fn new(demand: Arc<AtomicUsize>) -> Self {
        Self {
            phantom: PhantomData,
            buffer: VecDeque::new(),
            producer: None,
            consumer: None,
            cancelled: false,
            completed: false,
            failed: None,
            demand,
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
                self.demand.fetch_add(BUFFER_SIZE, Ordering::SeqCst);

                if self.cancelled {
                    context
                        .system_context()
                        .dispatcher()
                        .execute_trampoline(producer.cancel());
                } else {
                    context
                        .system_context()
                        .dispatcher()
                        .execute_trampoline(producer.pull());
                }
            }

            DetachedActorMsg::Produced(el) => {
                self.buffer.push_back(el);

                if let Some(consumer) = self.consumer.take() {
                    self.receive(DetachedActorMsg::Pulled(consumer), context);
                }
            }

            DetachedActorMsg::DoneProducing(producer) => {
                if self.cancelled {
                    context
                        .system_context()
                        .dispatcher()
                        .execute_trampoline(producer.cancel());
                } else if self.buffer.len() < BUFFER_SIZE {
                    let dispatcher = context.system_context().dispatcher();
                    let inner_dispatcher = dispatcher.clone();

                    context
                        .system_context()
                        .dispatcher()
                        .execute(Box::new(move || {
                            inner_dispatcher.execute_trampoline(producer.pull());
                        }));
                } else {
                    self.producer = Some(producer);
                }
            }

            DetachedActorMsg::Completed => {
                self.completed = true;

                if let Some(consumer) = self.consumer.take() {
                    context
                        .system_context()
                        .dispatcher()
                        .execute_trampoline(consumer.completed());
                }
            }

            DetachedActorMsg::Failed(error) => match self.consumer.take() {
                Some(consumer) => {
                    context
                        .system_context()
                        .dispatcher()
                        .execute_trampoline(consumer.failed(error));
                }

                None => {
                    self.failed = Some(error);
                }
            },

            DetachedActorMsg::Pulled(consumer) => {
                if self.completed {
                    context
                        .system_context()
                        .dispatcher()
                        .execute_trampoline(consumer.completed());
                } else if let Some(error) = self.failed.take() {
                    context
                        .system_context()
                        .dispatcher()
                        .execute_trampoline(consumer.failed(error));
                } else {
                    match self.buffer.pop_front() {
                        Some(elem) => {
                            let dispatcher = context.system_context().dispatcher();

                            self.demand.fetch_add(1, Ordering::SeqCst);

                            if let Some(producer) = self.producer.take() {
                                dispatcher.execute_trampoline(producer.pull());
                            }

                            // in general, the streaming model expressed in this library is driven
                            // from downstream demand, i.e. it's a pull-based model. there are
                            // some optimizations taken to turn it into a "push-pull" model
                            // but fundamentally this is always driven from downstream demand,
                            // so it still remains pull based. i feel this is also true for the effort
                            // that inspired this library, reactive streams.
                            //
                            // because of this, we can reason that a LIFO model for execution is
                            // the best, because it biases execution towards downstream which is
                            // pulling demand through the system. therefore, pantomime streams
                            // runs its streams on a LIFO dispatcher. this is contrary to the
                            // default system dispatcher, which uses a FIFO scheme for fairness
                            // between disparate actors

                            dispatcher.execute_trampoline(consumer.produced(elem));
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
                        .system_context()
                        .dispatcher()
                        .execute_trampoline(producer.cancel());
                } else {
                    self.cancelled = true;
                }
            }

            DetachedActorMsg::Attached(consumer) => {
                context
                    .system_context()
                    .dispatcher()
                    .execute_trampoline(consumer.started());
            }
        }
    }
}

pub struct Detached<A, Up: Producer<A>, Down: Consumer<A>>
where
    A: 'static + Send,
{
    upstream: Up,
    downstream: Down,
    actor_ref: ActorRef<DetachedActorMsg<A>>,
    demand: Arc<AtomicUsize>,
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
            demand: Arc::new(AtomicUsize::new(0)),
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
                demand: self.demand,
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
                demand: self.demand,
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
        self,
        consumer: Consume,
        context: ActorSystemContext,
    ) -> Trampoline {
        let actor_ref = context.spawn(DetachedActor::<A>::new(self.demand.clone()));

        let upstream = Detached {
            upstream: Disconnected,
            downstream: Disconnected,
            actor_ref: actor_ref.clone(),
            demand: self.demand.clone(),
            phantom: PhantomData,
        };

        let downstream = Detached {
            upstream: Disconnected,
            downstream: consumer,
            actor_ref,
            demand: self.demand,
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

    fn produced<Produce: Producer<A>>(self, producer: Produce, element: A) -> Trampoline {
        let actor_ref = self.actor_ref.clone();

        actor_ref.tell(DetachedActorMsg::Produced(element));

        let demand = self.demand.fetch_sub(1, Ordering::SeqCst);

        if demand == 1 {
            let (_, detached) = self.disconnect_downstream(producer);

            actor_ref.tell(DetachedActorMsg::DoneProducing(Box::new(detached)));

            Trampoline::done()
        } else {
            Trampoline::bounce(|| producer.pull(self))
        }
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
    fn attach(self: Box<Self>, context: ActorSystemContext) -> Trampoline {
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
