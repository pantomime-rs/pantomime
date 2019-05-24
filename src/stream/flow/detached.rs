use crate::actor::*;
use crate::dispatcher::Trampoline;
use crate::stream::disconnected::Disconnected;
use crate::stream::oxidized::*;
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

/// `DetachedLogic` describes transformations of a stream with one input and
/// one output.
///
/// When the stream is starting up, `attach` is called with a context that can
/// be used to spawn thunks, start actors, timers, etc.
///
/// When methods are called on a logic, they should reply by sending
/// a message (`tell`) to the supplied `ActorRef`.
///
/// In general, a logic responds and forwards demand signals from downstream
/// and handles incoming production signals from upstream.
///
/// @TODO think about logic requiring cancel of upstream, does that happen?
pub trait DetachedLogic<A, B, Msg>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    #[must_use]
    fn attach(
        &mut self,
        context: &StreamContext,
        actor_ref: &ActorRef<AsyncAction<B, Msg>>,
    ) -> Option<AsyncAction<B, Msg>>;

    #[must_use]
    fn forwarded(&mut self, msg: Msg) -> Option<AsyncAction<B, Msg>>;

    #[must_use]
    fn produced(&mut self, elem: A) -> Option<AsyncAction<B, Msg>>;

    #[must_use]
    fn pulled(&mut self) -> Option<AsyncAction<B, Msg>>;

    #[must_use]
    fn completed(&mut self) -> Option<AsyncAction<B, Msg>>;

    #[must_use]
    fn failed(&mut self, error: Error) -> Option<AsyncAction<B, Msg>>;
}

trait GenProducer {
    fn attach(self: Box<Self>, context: &StreamContext) -> Trampoline;

    fn pull(self: Box<Self>) -> Trampoline;

    fn cancel(self: Box<Self>) -> Trampoline;
}

trait GenConsumer<A> {
    fn started(self: Box<Self>, context: &StreamContext) -> Trampoline;

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
    buffer_size: usize,
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
    upstream_completed: bool,
    upstream_failed: Option<Error>,
    pulled: bool,
    failed: Option<Error>,
    stream_context: StreamContext,
    phantom: PhantomData<(A, B, Msg)>,
}

impl<A, B, Msg, Logic: DetachedLogic<A, B, Msg>> DetachedActor<A, B, Msg, Logic>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
    Logic: 'static + Send,
{
    fn new(
        demand: Arc<AtomicUsize>,
        logic: Logic,
        buffer_size: usize,
        stream_context: &StreamContext,
    ) -> Self {
        Self {
            buffer: VecDeque::new(),
            buffer_size,
            logic,
            demand,
            producer: None,
            consumer: None,
            cancelled: false,
            completed: false,
            upstream_completed: false,
            upstream_failed: None,
            pulled: false,
            failed: None,
            stream_context: stream_context.clone(),
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
    #[allow(clippy::cognitive_complexity)]
    fn receive(
        &mut self,
        msg: DetachedActorMsg<A, B, Msg>,
        context: &mut ActorContext<DetachedActorMsg<A, B, Msg>>,
    ) {
        match msg {
            DetachedActorMsg::ReceivedAsyncAction(AsyncAction::Cancel) => {
                if let Some(producer) = self.producer.take() {
                    context
                        .system_context()
                        .dispatcher()
                        .execute_trampoline(producer.cancel());
                } else {
                    self.cancelled = true;
                }
            }

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
                if let Some(reply) = self.logic.forwarded(msg) {
                    context
                        .actor_ref()
                        .tell(DetachedActorMsg::ReceivedAsyncAction(reply));
                }
            }

            DetachedActorMsg::ReceivedAsyncAction(AsyncAction::Pull) => {
                if self.pulled {
                    panic!("cannot pull twice");
                }

                if let Some(element) = self.buffer.pop_front() {
                    if let Some(reply) = self.logic.produced(element) {
                        context
                            .actor_ref()
                            .tell(DetachedActorMsg::ReceivedAsyncAction(reply));
                    }
                } else if self.upstream_completed {
                    if let Some(reply) = self.logic.completed() {
                        context
                            .actor_ref()
                            .tell(DetachedActorMsg::ReceivedAsyncAction(reply));
                    }
                } else if let Some(error) = self.upstream_failed.take() {
                    if let Some(reply) = self.logic.failed(error) {
                        context
                            .actor_ref()
                            .tell(DetachedActorMsg::ReceivedAsyncAction(reply));
                    }
                } else {
                    self.pulled = true;
                }

                if self.buffer_size > 0 {
                    self.demand.fetch_add(1, Ordering::SeqCst);

                    if let Some(producer) = self.producer.take() {
                        context
                            .system_context()
                            .dispatcher()
                            .execute_trampoline(producer.pull());
                    }
                }
            }

            DetachedActorMsg::ReceivedAsyncAction(AsyncAction::Push(elem)) => {
                let consumer = self.consumer.take().expect("cannot push without pull");

                // in general, the streaming model expressed in this library is driven
                // from downstream demand, i.e. it's a pull-based model. there are
                // some optimizations taken to turn it into a "push-pull" model
                // but fundamentally this is always driven from downstream demand,
                // so it still remains pull based.
                //
                // because of this, we can reason that a LIFO model for execution is
                // the best, because it biases execution towards downstream which is
                // pulling demand through the system. therefore, pantomime streams
                // runs its streams on a LIFO dispatcher. this is contrary to the
                // default system dispatcher, which uses a FIFO scheme for fairness
                // between disparate actors

                // @TODO use diff dispatcher
                context
                    .system_context()
                    .dispatcher()
                    .execute_trampoline(consumer.produced(elem));
            }

            DetachedActorMsg::Started(producer) => {
                self.demand.fetch_add(self.buffer_size, Ordering::SeqCst);

                if self.cancelled {
                    context
                        .system_context()
                        .dispatcher()
                        .execute_trampoline(producer.cancel());
                } else if self.buffer_size > 0 {
                    context
                        .system_context()
                        .dispatcher()
                        .execute_trampoline(producer.pull());
                } else {
                    // empty buffer, meaning we just wish to wait for
                    // cancellation. this is particularly useful for
                    // the sources that are powered by detached logic
                    self.producer = Some(producer);
                }
            }

            DetachedActorMsg::Produced(el) => {
                let reply = if self.pulled && self.buffer.is_empty() {
                    self.pulled = false;
                    self.logic.produced(el)
                } else if self.pulled {
                    self.demand.fetch_add(1, Ordering::SeqCst);

                    if let Some(producer) = self.producer.take() {
                        context
                            .system_context()
                            .dispatcher()
                            .execute_trampoline(producer.pull());
                    }

                    self.pulled = false;
                    self.buffer.push_back(el);
                    self.logic.produced(
                        self.buffer
                            .pop_front()
                            .expect("pantomime bug: just pushed, but buffer is empty"),
                    )
                } else {
                    self.buffer.push_back(el);

                    None
                };

                if let Some(reply) = reply {
                    context
                        .actor_ref()
                        .tell(DetachedActorMsg::ReceivedAsyncAction(reply));
                }
            }

            DetachedActorMsg::DoneProducing(producer) => {
                if self.cancelled {
                    context
                        .system_context()
                        .dispatcher()
                        .execute_trampoline(producer.cancel());
                } else if self.buffer.len() < self.buffer_size {
                    let dispatcher = context.system_context().dispatcher();
                    let inner_dispatcher = dispatcher.clone();

                    context.system_context().dispatcher().execute(move || {
                        inner_dispatcher.execute_trampoline(producer.pull());
                    });
                } else {
                    self.producer = Some(producer);
                }
            }

            DetachedActorMsg::Completed => {
                if self.pulled && self.buffer.is_empty() {
                    if let Some(reply) = self.logic.completed() {
                        context
                            .actor_ref()
                            .tell(DetachedActorMsg::ReceivedAsyncAction(reply));
                    }
                } else {
                    self.upstream_completed = true;
                }
            }

            DetachedActorMsg::Failed(error) => {
                if self.pulled && self.buffer.is_empty() {
                    if let Some(reply) = self.logic.failed(error) {
                        context
                            .actor_ref()
                            .tell(DetachedActorMsg::ReceivedAsyncAction(reply));
                    }
                } else {
                    self.upstream_failed = Some(error);
                }
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
                    self.consumer = Some(consumer);

                    if let Some(reply) = self.logic.pulled() {
                        context
                            .actor_ref()
                            .tell(DetachedActorMsg::ReceivedAsyncAction(reply));
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
                    .execute_trampoline(consumer.started(&self.stream_context));
            }
        }
    }

    fn receive_signal(
        &mut self,
        signal: Signal,
        context: &mut ActorContext<DetachedActorMsg<A, B, Msg>>,
    ) {
        if let Signal::Started = signal {
            let actor_ref = context.actor_ref().convert(Self::convert_async_action);

            if let Some(reply) = self.logic.attach(&self.stream_context, &actor_ref) {
                context
                    .actor_ref()
                    .tell(DetachedActorMsg::ReceivedAsyncAction(reply));
            }
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
    buffer_size: usize,
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
            buffer_size: BUFFER_SIZE,
            actor_ref: ActorRef::empty(),
            logic,
            demand: Arc::new(AtomicUsize::new(0)),
            phantom: PhantomData,
        }
    }

    pub fn new_sized(logic: L, buffer_size: usize) -> impl FnOnce(Up) -> Self {
        move |upstream| Self {
            upstream,
            downstream: Disconnected,
            buffer_size,
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
                buffer_size: self.buffer_size,
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
                buffer_size: self.buffer_size,
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
        context: &StreamContext,
    ) -> Trampoline {
        let actor_ref = context
            .system_context()
            .spawn(DetachedActor::<A, B, M, L>::new(
                self.demand.clone(),
                self.logic,
                self.buffer_size,
                context,
            ));

        let upstream = Detached::<A, B, M, Disconnected, Disconnected, Disconnected> {
            upstream: Disconnected,
            downstream: Disconnected,
            buffer_size: 0,
            actor_ref: actor_ref.clone(),
            logic: Disconnected,
            demand: self.demand.clone(),
            phantom: PhantomData,
        };

        let downstream = Detached::<A, B, M, Disconnected, Disconnected, Consume> {
            upstream: Disconnected,
            downstream: consumer,
            buffer_size: 0,
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

        // streams run actors with an increased throughput, so with
        // the default work stealing dispatcher this is effectively
        // synchronous if downstream is pulling continuously

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
    fn started<Produce: Producer<A>>(self, producer: Produce, _: &StreamContext) -> Trampoline {
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
    fn started(self: Box<Self>, context: &StreamContext) -> Trampoline {
        let (downstream, detached) = self.disconnect_downstream(Disconnected);

        downstream.started(detached, context)
    }

    fn produced(self: Box<Self>, element: B) -> Trampoline {
        let (downstream, detached) = self.disconnect_downstream(Disconnected);

        downstream.produced(detached, element)
    }

    fn completed(self: Box<Self>) -> Trampoline {
        let (downstream, _) = self.disconnect_downstream(Disconnected);

        downstream.completed()
    }

    fn failed(self: Box<Self>, error: Error) -> Trampoline {
        let (downstream, _) = self.disconnect_downstream(Disconnected);

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
    // @TODO these all ignored detached before, and sent Disconnected upstream. seems weird?

    fn attach(self: Box<Self>, context: &StreamContext) -> Trampoline {
        let (upstream, detached) = self.disconnect_upstream(Disconnected);

        upstream.attach(detached, context)
    }

    fn pull(self: Box<Self>) -> Trampoline {
        let (upstream, detached) = self.disconnect_upstream(Disconnected);

        upstream.pull(detached)
    }

    fn cancel(self: Box<Self>) -> Trampoline {
        let (upstream, detached) = self.disconnect_upstream(Disconnected);

        upstream.cancel(detached)
    }
}
