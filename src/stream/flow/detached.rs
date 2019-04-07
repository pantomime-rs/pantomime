// @TODO TODO TODO
// @TODO TODO TODO
// @TODO TODO TODO

// this is nearly complete, but some details need to be figured on
// wrt DetachedLogic and buffering.
//
// I *think* that the buffer should be for upstream, to keep demand
// hot, but then how does that complicate the logic implementations
// which are meant to be quite minimal?

use crate::actor::*;
use crate::dispatcher::{Dispatcher, Trampoline};
use crate::stream::disconnected::Disconnected;
use crate::stream::oxidized::*;
use crate::stream::*;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const BUFFER_SIZE: usize = 16; // @TODO config

pub enum AsyncAction<B, Msg> {
    Complete,
    Fail(Error),
    Forward(Msg),
    Pull,
    Push(B),
}

pub trait DetachedLogic<A, B, Msg>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    fn attach(&mut self, context: &ActorSystemContext);

    fn forwarded(&mut self, msg: Msg, actor_ref: ActorRef<AsyncAction<B, Msg>>);

    fn produced(&mut self, elem: A, actor_ref: ActorRef<AsyncAction<B, Msg>>);

    fn pulled(&mut self, actor_ref: ActorRef<AsyncAction<B, Msg>>);

    fn completed(&mut self, actor_ref: ActorRef<AsyncAction<B, Msg>>);

    fn failed(&mut self, error: Error, actor_ref: ActorRef<AsyncAction<B, Msg>>);
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

enum DetachedActorMsg<A, B, Msg> {
    ReceivedAsyncAction(AsyncAction<B, Msg>),

    Started(Box<GenProducer + 'static + Send>),
    Produced(A),
    DoneProducing(Box<GenProducer + 'static + Send>),
    Completed,
    Failed(Error),
    Pulled(Box<GenConsumer<B> + 'static + Send>),
    Cancelled(Box<GenConsumer<B> + 'static + Send>),
    Attached(Box<GenConsumer<B> + 'static + Send>),
}

struct DetachedActor<A, B, Msg, Logic: DetachedLogic<A, B, Msg>>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
    Logic: 'static + Send,
{
    buffer: VecDeque<A>,
    logic: Logic,
    /// Tracks demand and is shared with the upstream producer
    /// which continues to consume from its upstream if there
    /// is a demand.
    ///
    /// It's important that demand is only ever added to from
    /// the actor, and only ever subtracted from the upstream.
    demand: Arc<AtomicUsize>,
    producer: Option<Box<GenProducer + 'static + Send>>,
    consumer: Option<Box<GenConsumer<B> + 'static + Send>>,
    cancelled: bool,
    completed: bool,
    pulled: bool,
    failed: Option<Error>,
    phantom: PhantomData<(A, B, Msg)>,
}

impl<A, B, Msg, Logic: DetachedLogic<A, B, Msg>> DetachedActor<A, B, Msg, Logic>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
    Logic: 'static + Send,
{
    fn new(demand: Arc<AtomicUsize>, logic: Logic) -> Self {
        Self {
            buffer: VecDeque::new(),
            logic,
            demand,
            producer: None,
            consumer: None,
            cancelled: false,
            completed: false,
            pulled: false,
            failed: None,
            phantom: PhantomData,
        }
    }

    fn convert_async_action(m: AsyncAction<B, Msg>) -> DetachedActorMsg<A, B, Msg> {
        DetachedActorMsg::ReceivedAsyncAction(m)
    }
}

impl<A, B, Msg, Logic: DetachedLogic<A, B, Msg>> Actor<DetachedActorMsg<A, B, Msg>>
    for DetachedActor<A, B, Msg, Logic>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
    Logic: 'static + Send,
{
    fn receive(
        &mut self,
        msg: DetachedActorMsg<A, B, Msg>,
        context: &mut ActorContext<DetachedActorMsg<A, B, Msg>>,
    ) {
        match msg {
            DetachedActorMsg::ReceivedAsyncAction(AsyncAction::Complete) => {
                self.completed = true;

                if let Some(consumer) = self.consumer.take() {
                    context
                        .system_context()
                        .dispatcher()
                        .execute_trampoline(consumer.completed());
                }
            }

            DetachedActorMsg::ReceivedAsyncAction(AsyncAction::Fail(error)) => {
                match self.consumer.take() {
                    Some(consumer) => {
                        context
                            .system_context()
                            .dispatcher()
                            .execute_trampoline(consumer.failed(error));
                    }

                    None => {
                        self.failed = Some(error);
                    }
                }
            }

            DetachedActorMsg::ReceivedAsyncAction(AsyncAction::Forward(msg)) => {
                self.logic
                    .forwarded(msg, context.actor_ref().convert(Self::convert_async_action));
            }

            DetachedActorMsg::ReceivedAsyncAction(AsyncAction::Pull) => {
                if let Some(element) = self.buffer.pop_front() {
                    self.logic.produced(
                        element,
                        context.actor_ref().convert(Self::convert_async_action),
                    );
                }
            }

            DetachedActorMsg::ReceivedAsyncAction(AsyncAction::Push(elem)) => {
                // @TODO
                //self.buffer.push_back(elem);

                if let Some(consumer) = self.consumer.take() {
                    self.receive(DetachedActorMsg::Pulled(consumer), context);
                }
            }

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
                self.logic
                    .produced(el, context.actor_ref().convert(Self::convert_async_action));
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
                self.logic
                    .completed(context.actor_ref().convert(Self::convert_async_action));
            }

            DetachedActorMsg::Failed(error) => {
                self.logic.failed(
                    error,
                    context.actor_ref().convert(Self::convert_async_action),
                );
            }

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

                            // @TODO
                            //dispatcher.execute_trampoline(consumer.produced(elem));
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

    fn receive_signal(
        &mut self,
        signal: Signal,
        context: &mut ActorContext<DetachedActorMsg<A, B, Msg>>,
    ) {
        if let Signal::Started = signal {
            self.logic.attach(context.system_context());
        }
    }
}

pub struct Detached<A, B, M, L: DetachedLogic<A, B, M>, Up: Producer<A>, Down: Consumer<B>>
where
    A: 'static + Send,
    B: 'static + Send,
    M: 'static + Send,
    L: 'static + Send,
{
    upstream: Up,
    downstream: Down,
    actor_ref: ActorRef<DetachedActorMsg<A, B, M>>,
    logic: L,
    demand: Arc<AtomicUsize>,
    phantom: PhantomData<(A, B, M)>,
}

impl<A, B, M, L: DetachedLogic<A, B, M>, Up> Detached<A, B, M, L, Up, Disconnected>
where
    A: 'static + Send,
    B: 'static + Send,
    M: 'static + Send,
    L: 'static + Send,
    Up: Producer<A>,
{
    pub fn new(logic: L) -> impl FnOnce(Up) -> Self {
        move |upstream| Self {
            upstream,
            downstream: Disconnected,
            actor_ref: ActorRef::empty(),
            logic,
            demand: Arc::new(AtomicUsize::new(0)),
            phantom: PhantomData,
        }
    }
}

impl<A, B, M, L: DetachedLogic<A, B, M>, Up: Producer<A>, Down: Consumer<B>>
    Detached<A, B, M, L, Up, Down>
where
    A: 'static + Send,
    B: 'static + Send,
    L: 'static + Send,
    M: 'static + Send,
{
    fn disconnect_downstream<Produce: Producer<A>>(
        self,
        producer: Produce,
    ) -> (Down, Detached<A, B, M, Disconnected, Produce, Disconnected>) {
        (
            self.downstream,
            Detached {
                upstream: producer,
                downstream: Disconnected,
                actor_ref: self.actor_ref,
                logic: Disconnected,
                demand: self.demand,
                phantom: PhantomData,
            },
        )
    }

    fn disconnect_upstream<Consume: Consumer<B>>(
        self,
        consumer: Consume,
    ) -> (Up, Detached<A, B, M, Disconnected, Disconnected, Consume>) {
        (
            self.upstream,
            Detached {
                upstream: Disconnected,
                downstream: consumer,
                actor_ref: self.actor_ref,
                logic: Disconnected,
                demand: self.demand,
                phantom: PhantomData,
            },
        )
    }
}

impl<A, B, M, L: DetachedLogic<A, B, M>, Up: Producer<A>, Down: Consumer<B>> Producer<B>
    for Detached<A, B, M, L, Up, Down>
where
    A: 'static + Send,
    B: 'static + Send,
    M: 'static + Send,
    L: 'static + Send,
    Up: 'static + Send,
{
    fn attach<Consume: Consumer<B>>(
        self,
        consumer: Consume,
        context: ActorSystemContext,
    ) -> Trampoline {
        let actor_ref = context.spawn(DetachedActor::<A, B, M, L>::new(
            self.demand.clone(),
            self.logic,
        ));

        let upstream = Detached::<A, B, M, Disconnected, Disconnected, Disconnected> {
            upstream: Disconnected,
            downstream: Disconnected,
            actor_ref: actor_ref.clone(),
            logic: Disconnected,
            demand: self.demand.clone(),
            phantom: PhantomData,
        };

        let downstream = Detached::<A, B, M, Disconnected, Disconnected, Consume> {
            upstream: Disconnected,
            downstream: consumer,
            actor_ref,
            logic: Disconnected,
            demand: self.demand,
            phantom: PhantomData,
        };

        upstream
            .actor_ref
            .tell(DetachedActorMsg::Attached(Box::new(downstream)));

        self.upstream.attach(upstream, context)
    }

    fn pull<Consume: Consumer<B>>(self, consumer: Consume) -> Trampoline {
        let actor_ref = self.actor_ref.clone();

        let (_, detached) = self.disconnect_upstream(consumer);

        actor_ref.tell(DetachedActorMsg::Pulled(Box::new(detached)));

        Trampoline::done()
    }

    fn cancel<Consume: Consumer<B>>(self, consumer: Consume) -> Trampoline {
        let actor_ref = self.actor_ref.clone();

        let (_, detached) = self.disconnect_upstream(consumer);

        actor_ref.tell(DetachedActorMsg::Cancelled(Box::new(detached)));

        Trampoline::done()
    }
}

impl<A, B, M, L: DetachedLogic<A, B, M>, Up: Producer<A>, Down: Consumer<B>> Consumer<A>
    for Detached<A, B, M, L, Up, Down>
where
    A: 'static + Send,
    B: 'static + Send,
    M: 'static + Send,
    L: 'static + Send,
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

impl<A, B, M, L: DetachedLogic<A, B, M>, Up: Producer<A>, Down: Consumer<B>> Stage<B>
    for Detached<A, B, M, L, Up, Down>
where
    A: 'static + Send,
    B: 'static + Send,
    M: 'static + Send,
    L: 'static + Send,
{
}

impl<A, B, M, L: DetachedLogic<A, B, M>, Up: Producer<A>, Down: Consumer<B>> Flow<A, B>
    for Detached<A, B, M, L, Up, Down>
where
    A: 'static + Send,
    B: 'static + Send,
    M: 'static + Send,
    L: 'static + Send,
{
}

impl<A, B, M, L: DetachedLogic<A, B, M>, Up: Producer<A>, Down: Consumer<B>> GenConsumer<B>
    for Detached<A, B, M, L, Up, Down>
where
    A: 'static + Send,
    B: 'static + Send,
    M: 'static + Send,
    L: 'static + Send,
{
    fn started(self: Box<Self>) -> Trampoline {
        let (downstream, detached) = self.disconnect_downstream(Disconnected);

        downstream.started(detached)
    }

    fn produced(self: Box<Self>, element: B) -> Trampoline {
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

impl<A, B, M, L: DetachedLogic<A, B, M>, Up: Producer<A>, Down: Consumer<B>> GenProducer
    for Detached<A, B, M, L, Up, Down>
where
    A: 'static + Send,
    B: 'static + Send,
    M: 'static + Send,
    L: 'static + Send,
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
