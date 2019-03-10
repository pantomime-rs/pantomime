use super::*;
use crate::dispatcher::{Thunk, WorkStealingDispatcher};
use crate::util::Cancellable;
use std::collections::HashMap;
use std::time::Duration;

#[cfg(feature = "futures-support")]
use crate::dispatcher::FutureDispatcherBridge;

#[cfg(feature = "futures-support")]
use futures::Future;

/// Provides references to the actor's context when it is executing, allowing
/// it to dispatcher work, stash messages, etc.
pub struct ActorContext<M: 'static + Send> {
    pub(in crate::actor) actor_ref: Option<ActorRef<M>>,
    pub(in crate::actor) children: HashMap<usize, SystemActorRef>,
    deliveries: HashMap<String, Cancellable>,
}

impl<'a, M: 'static + Send> ActorContext<M> {
    pub(in crate::actor) fn new() -> Self {
        Self {
            actor_ref: None,
            children: HashMap::new(),
            deliveries: HashMap::new(),
        }
    }

    pub fn actor_ref(&self) -> &ActorRef<M> {
        self.actor_ref
            .as_ref()
            .expect("pantomime bug: actor_ref not initialized")
    }

    /// Cancels the specified delivery.
    ///
    /// It is guaranteed that any messages produced by the delivery will not be received by the
    /// actor (given compliant mailbox implementations).
    pub fn cancel_delivery<S: AsRef<str>>(&mut self, name: S) {
        if let Some(c) = self.deliveries.remove(name.as_ref()) {
            c.cancel();
        }
    }

    /// Schedule the delivery of a message. The initial delivery will happen after `interval` has
    /// elapsed. Each subsequent delivery is scheduled immediately before the actor processes it.
    ///
    /// Thus, if an actor takes a long time to process the message, it will only receive one "tick"
    /// even if multiple intervals have elapsed.
    ///
    /// Deliveries are identified by a name. If a previous delivery with the same name was already
    /// scheduled, it is cancelled and there is a guarantee that any messages from it will not be
    /// received by the actor.
    pub fn schedule_periodic_delivery<F: Fn() -> M, S: AsRef<str>>(
        &mut self,
        name: S,
        interval: Duration,
        msg: F,
    ) where
        F: 'static + Send + Sync,
    {
        let cancellable = Cancellable::new();

        {
            let cancellable = cancellable.clone();

            Self::periodic_delivery(
                &self.actor_ref().inner.system_context(),
                self.actor_ref().clone(),
                msg,
                interval,
                cancellable,
            );
        }

        if let Some(c) = self
            .deliveries
            .insert(name.as_ref().to_owned(), cancellable)
        {
            c.cancel();
        }
    }

    fn periodic_delivery<F: Fn() -> M>(
        context: &Arc<ActorSystemContext>,
        actor_ref: ActorRef<M>,
        msg: F,
        interval: Duration,
        cancellable: Cancellable,
    ) where
        F: 'static + Send + Sync,
    {
        let next_context = context.clone();

        context.schedule_thunk(interval, move || {
            let m = msg();

            let delivered = {
                let actor_ref = actor_ref.clone();
                let cancellable = cancellable.clone();

                Box::new(move || {
                    if !cancellable.cancelled() {
                        // we'll schedule the delivery, but note that thta doesn't
                        // mean the actor will necessarily receive it, as it could
                        // be cancelled. to offer the guarantees of this, the mailbox
                        // itself provides the ability to cancel the delivery of
                        // messages

                        Self::periodic_delivery(
                            &next_context,
                            actor_ref,
                            msg,
                            interval,
                            cancellable,
                        );
                    }
                })
            };

            actor_ref.tell_cancellable(cancellable, m, Some(delivered));
        });
    }

    /// Schedule the delivery of a message after `timeout` has elapsed.
    ///
    /// Deliveries are identified by a name. If a previous delivery with the same name was already
    /// scheduled, it is cancelled and there is a guarantee that any messages from it will not be
    /// received by the actor.
    pub fn schedule_delivery<F: Fn() -> M, S: AsRef<str>>(
        &mut self,
        name: S,
        timeout: Duration,
        msg: F,
    ) where
        F: 'static + Send + Sync,
    {
        let cancellable = Cancellable::new();

        {
            let actor_ref = self.actor_ref().clone();
            let cancellable = cancellable.clone();

            self.actor_ref()
                .inner
                .system_context()
                .schedule_thunk(timeout, move || {
                    // we explicitly cancel to ensure that our entry will be
                    // cleaned up, as cancelled entries are filtered out
                    // periodically
                    // @TODO make above reality

                    let delivered = {
                        let cancellable = cancellable.clone();

                        Box::new(move || cancellable.cancel())
                    };

                    actor_ref.tell_cancellable(cancellable, msg(), Some(delivered));
                });
        }

        if let Some(c) = self
            .deliveries
            .insert(name.as_ref().to_owned(), cancellable)
        {
            c.cancel();
        }
    }

    pub fn schedule_thunk<F: FnOnce()>(&self, timeout: Duration, f: F)
    where
        F: 'static + Send + Sync,
    {
        self.actor_ref()
            .inner
            .system_context()
            .schedule_thunk(timeout, f);
    }

    /// Subscribe to lifecycle events for the supplied actor.
    ///
    /// If the supplied actor fails or is stopped, a signal will be sent to
    /// the actor that this context represents.
    ///
    /// If the supplied actor has already failed or stopped, a signal will
    /// still be delivered, but the reason will be unknown.
    pub fn watch<N: 'static + Send, A: AsRef<ActorRef<N>>>(&mut self, actor_ref: A) {
        let actor_ref = actor_ref.as_ref();

        match self.actor_ref().inner.system_context().watcher_ref.as_ref() {
            Some(watcher_ref) => watcher_ref.tell(ActorWatcherMessage::Subscribe(
                self.actor_ref().system_ref(),
                actor_ref.system_ref(),
            )),

            None => {
                error!(
                        "#[{watcher}] attempted to watch #[{watching}] but it does not have a reference to ActorWatcher; this is unexpected",
                        watcher = self.actor_ref().id(),
                        watching = actor_ref.id()
                    );
            }
        }
    }

    #[cfg(feature = "posix-signals-support")]
    pub fn watch_posix_signals(&mut self) {
        if let Some(ref watcher_ref) = self.actor_ref().inner.system_context().watcher_ref {
            watcher_ref.tell(ActorWatcherMessage::SubscribePosixSignals(
                self.actor_ref().system_ref(),
            ));
        }
    }

    /// Schedule a Future for immediate execution. Usually this
    /// is used in conjunction with pipe_result, but can be
    /// used for any arbitrary futures.
    /// @TODO use a trait?
    #[cfg(feature = "futures-support")]
    pub fn spawn_future<F>(&mut self, future: F)
    where
        F: Future<Item = (), Error = ()> + 'static + Send,
    {
        self.actor_ref()
            .inner
            .system_context()
            .dispatcher
            .spawn_future(future);
    }

    /// Spawn an actor as a child of the actor attached
    /// to this context. An `ActorRef` is returned, which
    /// can be used to message the child and manage its
    /// lifecycle.
    pub fn spawn<A: 'static + Send, N: 'static + Send>(&mut self, actor: A) -> ActorRef<N>
    where
        A: Actor<N>,
    {
        let child = ActorSystemContext::spawn_actor(
            &self.actor_ref().inner.system_context(),
            ActorType::Child,
            actor,
            self.actor_ref().system_ref(),
        );

        self.children.insert(child.id(), child.system_ref());

        child
    }

    /// Returns a reference to the dispatcher for the system, which is not
    /// configurable.
    pub fn system_dispatcher(&self) -> WorkStealingDispatcher {
        self.actor_ref().inner.system_context().dispatcher.clone()
    }
}

pub(in crate::actor) trait ActorRefScheduler: Downcast {
    fn actor_type(&self) -> ActorType;
    fn drain(&self);
    fn safe_clone(&self) -> Box<ActorRefScheduler + Send + Sync>;
    fn shard(&self) -> &Arc<ActorShard>;
    fn stop(&self);
    fn tell_system(&self, message: SystemMsg);
}

impl_downcast!(ActorRefScheduler);

pub(in crate::actor) struct NoopActorRefScheduler;

impl ActorRefScheduler for NoopActorRefScheduler {
    fn actor_type(&self) -> ActorType {
        panic!("pantomime bug: actor_type called on NoopActorRefScheduler");
    }

    fn drain(&self) {}

    fn safe_clone(&self) -> Box<ActorRefScheduler + Send + Sync> {
        Box::new(NoopActorRefScheduler)
    }

    fn shard(&self) -> &Arc<ActorShard> {
        panic!("pantomime bug: shard called on NoopActorRefScheduler");
    }

    fn stop(&self) {}

    fn tell_system(&self, _message: SystemMsg) {}
}

/// A generic type of actor reference that permits the delivery of system
/// messages.
///
/// It can be upgraded by a call to `actor_ref`.
pub struct SystemActorRef {
    pub id: usize,
    pub(in crate::actor) scheduler: Box<ActorRefScheduler + Send + Sync>,
}

impl SystemActorRef {
    /// "Downcast" this reference to a typed ActorRef, which can be used
    /// to send it domain messages.
    ///
    /// If the specified type does not match the underlying actor, this
    /// will return None.
    pub fn actor_ref<N: 'static + Send>(&self) -> Option<ActorRef<N>> {
        self.scheduler
            .safe_clone()
            .into_any()
            .downcast::<ActorRef<N>>()
            .map(|v| *v)
            .ok()
    }

    pub(in crate::actor) fn actor_type(&self) -> ActorType {
        self.scheduler.actor_type()
    }

    pub fn drain(&self) {
        self.scheduler.drain();
    }

    pub fn stop(&self) {
        self.scheduler.stop();
    }

    /// Enqueues a `SystemMsg` into this actor's mailbox and schedules
    /// it for execution (if it hasn't been already).
    pub(in crate::actor) fn tell_system(&self, message: SystemMsg) {
        self.scheduler.tell_system(message);
    }
}

impl Clone for SystemActorRef {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            scheduler: self.scheduler.safe_clone(),
        }
    }
}

pub(in crate::actor) trait ActorRefInnerShim<M: 'static + Send> {
    fn actor_type(&self) -> ActorType;
    fn enqueue(&self, msg: M);
    fn enqueue_cancellable(&self, cancellable: Cancellable, msg: M, thunk: Option<Thunk>);
    fn enqueue_done(&self);
    fn enqueue_system(&self, msg: SystemMsg);
    fn id(&self) -> usize;
    fn initialize(&mut self, cell: Weak<ActorCell<M>>);
    fn parent_ref(&self) -> SystemActorRef;
    fn shard(&self) -> &Arc<ActorShard>;
    fn system_context(&self) -> &Arc<ActorSystemContext>;
}

/// A reference to an underlying actor through which messages can be sent.
///
/// All interactions with an actor are done through an `ActorRef`. If
/// an actor is stopped, any messages sent to actor refs will be
/// delivered to dead letters intead.
pub struct ActorRef<M: 'static + Send> {
    inner: Arc<ActorRefInnerShim<M> + 'static + Send + Sync>,
}

pub(in crate::actor) struct WrappedActorRefInner<F: 'static + Send, M: 'static + Send> {
    pub(in crate::actor) inner: Arc<ActorRefInnerShim<M> + 'static + Send + Sync>,
    pub(in crate::actor) converter: Box<Fn(F) -> M + 'static + Send + Sync>,
}

impl<F: 'static + Send, M: 'static + Send> WrappedActorRefInner<F, M> {
    fn new(
        inner: Arc<ActorRefInnerShim<M> + 'static + Send + Sync>,
        converter: Box<Fn(F) -> M + 'static + Send + Sync>,
    ) -> Self {
        Self { inner, converter }
    }
}

impl<F: 'static + Send, M: 'static + Send> ActorRefInnerShim<F> for WrappedActorRefInner<F, M> {
    fn actor_type(&self) -> ActorType {
        self.inner.actor_type()
    }

    fn enqueue(&self, msg: F) {
        self.inner.enqueue((self.converter)(msg));
    }

    fn enqueue_cancellable(&self, cancellable: Cancellable, msg: F, thunk: Option<Thunk>) {
        self.inner
            .enqueue_cancellable(cancellable, (self.converter)(msg), thunk);
    }

    fn enqueue_done(&self) {
        self.inner.enqueue_done();
    }

    fn enqueue_system(&self, msg: SystemMsg) {
        self.inner.enqueue_system(msg);
    }

    fn id(&self) -> usize {
        self.inner.id()
    }

    fn initialize(&mut self, _cell: Weak<ActorCell<F>>) {
        panic!("pantomime bug: initialize called on WrappedActorRefInner")
    }

    fn parent_ref(&self) -> SystemActorRef {
        self.inner.parent_ref()
    }

    fn shard(&self) -> &Arc<ActorShard> {
        self.inner.shard()
    }

    fn system_context(&self) -> &Arc<ActorSystemContext> {
        self.inner.system_context()
    }
}

pub(in crate::actor) struct ActorRefInner<M: 'static + Send> {
    pub(in crate::actor) actor_type: ActorType,
    pub(in crate::actor) id: usize,
    pub(in crate::actor) new_cell: Weak<ActorCell<M>>,
    pub(in crate::actor) shard: Arc<ActorShard>,
    pub(in crate::actor) system_context: Arc<ActorSystemContext>,
    pub(in crate::actor) custom_mailbox_appender:
        Option<Box<'static + MailboxAppender<M> + Send + Sync>>,
}

impl<M: 'static + Send> ActorRefInnerShim<M> for ActorRefInner<M> {
    fn actor_type(&self) -> ActorType {
        self.actor_type
    }

    fn enqueue(&self, msg: M) {
        if let Some(cell) = self.new_cell.upgrade() {
            match self.custom_mailbox_appender {
                None => {
                    self.shard().tell(Box::new(ActorCellWithMessage::new(
                        cell,
                        ActorCellMessage::Message(msg),
                    )));
                }

                Some(ref a) => {
                    a.append(msg);

                    self.shard().tell(Box::new(ActorCellWithMessage::new(
                        cell,
                        ActorCellMessage::CustomMessage,
                    )));
                }
            }
        } else {
            // @TODO dead letters
        }
    }

    fn enqueue_cancellable(&self, cancellable: Cancellable, msg: M, thunk: Option<Thunk>) {
        // @FIXME feels a bit too similar to above

        if let Some(cell) = self.new_cell.upgrade() {
            match self.custom_mailbox_appender {
                None => self.shard().tell_cancellable(
                    cancellable,
                    Box::new(ActorCellWithMessage::new(
                        cell,
                        ActorCellMessage::Message(msg),
                    )),
                    thunk,
                ),

                Some(ref a) => {
                    a.append_cancellable(cancellable, msg, thunk);

                    self.shard().tell(Box::new(ActorCellWithMessage::new(
                        cell,
                        ActorCellMessage::CustomMessage,
                    )));
                }
            };
        } else {
            // @TODO dead letters
        }
    }

    /// Internal method.
    ///
    /// This is used to signal to the actor that draining
    /// has been initiated. The actor will process any messages
    /// that are destined to it first before stopping itself.
    fn enqueue_done(&self) {
        if let Some(cell) = self.new_cell.upgrade() {
            self.shard().tell(Box::new(ActorCellWithMessage::new(
                cell,
                ActorCellMessage::Done,
            )));
        } else {
            // @TODO dead letters
        }
    }

    /// Internal method for sending a system message
    /// to the actor. System messages are concerned
    /// with the lifecycle of the actor and affect
    /// state that the system tracks, rather than
    /// state that the actor owns.
    ///
    /// Some system messages materialize as signals
    /// that the specific actors can consume.
    fn enqueue_system(&self, msg: SystemMsg) {
        let msg = Box::new(msg);

        if let Some(cell) = self.new_cell.upgrade() {
            self.shard()
                .tell_system(Box::new(ActorCellWithSystemMessage::new(cell.clone(), msg)));
        } else {
            // @TODO dead letters
        }
    }

    fn id(&self) -> usize {
        self.id
    }

    fn initialize(&mut self, cell: Weak<ActorCell<M>>) {
        self.new_cell = cell;
    }

    fn parent_ref(&self) -> SystemActorRef {
        if let Some(cell) = self.new_cell.upgrade() {
            cell.parent_ref.clone()
        } else {
            // @TODO is this right?
            SystemActorRef {
                id: 1,
                scheduler: Box::new(NoopActorRefScheduler),
            }
        }
    }

    fn shard(&self) -> &Arc<ActorShard> {
        &self.shard
    }

    fn system_context(&self) -> &Arc<ActorSystemContext> {
        &self.system_context
    }
}

impl<M: 'static + Send> ActorRef<M> {
    pub(in crate::actor) fn new(inner: Arc<ActorRefInnerShim<M> + 'static + Send + Sync>) -> Self {
        Self { inner }
    }

    pub(in crate::actor) fn actor_type(&self) -> ActorType {
        self.inner.actor_type()
    }

    /// Convert this ActorRef into one that handles another type of message by
    /// providing a conversion function.
    pub fn convert<N: 'static + Send, F: 'static + Fn(N) -> M + Send + Sync>(
        &self,
        converter: F,
    ) -> ActorRef<N> {
        ActorRef::new(Arc::new(WrappedActorRefInner::new(
            self.inner.clone(),
            Box::new(converter),
        )))
    }

    pub fn id(&self) -> usize {
        self.inner.id()
    }

    /// Executes an event, and optionally a followup on the provided dispatcher.
    ///
    /// The first execution is done on the current thread immediately. This is
    /// okay though! An actor will only ever be woken on the custom dispatcher
    /// if one is configured.
    ///
    /// This is guaranteed because ActorScheduled is only ever done as a followup
    /// message, and followup messages are always run on the custom dispatcher if
    /// there is one. Even if that results in a new call to execute_system, that will
    /// be started from within the custom_dispatcher, so this is okay.
    #[inline(always)]
    fn shard_execute(
        &self,
        event: ActorShardEvent,
        dispatcher: Option<Box<'static + Dispatcher + Send + Sync>>,
    ) {
        let follow_up = match event {
            ActorShardEvent::Messaged => self.shard().messaged(),
            ActorShardEvent::Scheduled => self.shard().scheduled(10), // @TODO throughput
        };

        if let Some(follow_up) = follow_up {
            let next_self = self.clone();

            match dispatcher {
                None => {
                    self.system_context().dispatcher.execute(Box::new(move || {
                        next_self.shard_execute(follow_up, None);
                    }));
                }

                Some(ref d) => {
                    let d2 = d.safe_clone();

                    d.execute(Box::new(move || {
                        next_self.shard_execute(follow_up, Some(d2));
                    }));
                }
            }
        }
    }

    /// Send a message to this actor. The actor will
    /// then be scheduled for execution.
    ///
    /// Ordering of message delivery depends on mailbox
    /// implementation. The default mailbox is based
    /// on a Crossbeam channel. If the same thread
    /// enqueues "A" and then "B", A will be received by
    /// the actor before B.
    ///
    #[inline(always)]
    pub fn tell(&self, msg: M) {
        self.inner.enqueue(msg);
        self.shard_execute(ActorShardEvent::Messaged, self.shard().custom_dispatcher());
    }

    #[inline(always)]
    pub(in crate::actor) fn tell_cancellable(
        &self,
        cancellable: Cancellable,
        msg: M,
        thunk: Option<Thunk>,
    ) {
        self.inner.enqueue_cancellable(cancellable, msg, thunk);

        self.shard_execute(ActorShardEvent::Messaged, self.shard().custom_dispatcher());
    }

    /// Stop this actor, first processing all of the
    /// messages in its mailbox.
    ///
    /// First, all children are drained. Once a given
    /// child has processed all of its messages, and
    /// all of its children have been stopped, it stops
    /// itself.
    ///
    /// Once all of this actor's children have stopped,
    /// all messages in the mailbox are processed and
    /// then the actor is stopped.
    ///
    /// If any messages are enqueued after the call to
    /// drain, they are sent to dead letters.
    pub fn drain(&self) {
        self.tell_system(SystemMsg::Drain);
    }

    // @TODO not sure about this method's need to exist
    #[inline(always)]
    pub fn parent_ref(&self) -> SystemActorRef {
        self.inner.parent_ref()
    }

    #[inline(always)]
    pub(in crate::actor) fn shard(&self) -> &Arc<ActorShard> {
        &self.inner.shard()
    }

    #[inline(always)]
    pub(crate) fn system_context(&self) -> &Arc<ActorSystemContext> {
        &self.inner.system_context()
    }

    /// Stop this actor. This is an asynchronous
    /// operation -- any messages (upto throughput) that
    /// the actor is currently processing will be consumed
    /// first before it is stopped.
    ///
    /// When stopping an actor, it transitions into a stopping
    /// state upon which no other regular messages in its
    /// mailbox are processed.
    ///
    /// Then, all children are told to stop (recursively). When
    /// a child has stopped, it notifies its parent actor.
    ///
    /// When an actor is in the stopping state and has been
    /// notified that all of its children have stopped, it too
    /// transitions to a Stopped state and notifies its parent.
    pub fn stop(&self) {
        self.tell_system(SystemMsg::Stop { failure: false });
    }

    /// Internal method.
    ///
    /// This is used to signal to the actor that draining
    /// has been initiated. The actor will process any messages
    /// that are destined to it first before stopping itself.
    pub(in crate::actor) fn tell_done(&self) {
        self.inner.enqueue_done();

        self.shard_execute(ActorShardEvent::Messaged, self.shard().custom_dispatcher());
    }

    /// Internal method for sending a system message
    /// to the actor. System messages are concerned
    /// with the lifecycle of the actor and affect
    /// state that the system tracks, rather than
    /// state that the actor owns.
    ///
    /// Some system messages materialize as signals
    /// that the specific actors can consume.
    pub(in crate::actor) fn tell_system(&self, msg: SystemMsg) {
        self.inner.enqueue_system(msg);

        self.shard_execute(ActorShardEvent::Messaged, None);
    }

    /// Creates an actor ref that can be used to
    /// deliver system messages to this actor.
    ///
    /// If the actor is stopped, messages are sent
    /// to dead letters instead.
    pub fn system_ref(&self) -> SystemActorRef {
        SystemActorRef {
            id: self.id(),
            scheduler: Box::new(self.clone()),
        }
    }
}

impl<M: 'static + Send> Clone for ActorRef<M> {
    #[inline(always)]
    fn clone(&self) -> ActorRef<M> {
        Self {
            inner: self.inner.clone(),
        }
    }
}

// @TODO probably should be a StrongActorRefScheduler such bouncing atm

impl<M: 'static + Send> ActorRefScheduler for ActorRef<M> {
    fn actor_type(&self) -> ActorType {
        self.actor_type()
    }

    fn drain(&self) {
        self.drain();
    }

    fn safe_clone(&self) -> Box<ActorRefScheduler + Send + Sync> {
        Box::new(self.clone())
    }

    fn shard(&self) -> &Arc<ActorShard> {
        self.shard()
    }

    fn stop(&self) {
        self.stop();
    }

    fn tell_system(&self, message: SystemMsg) {
        self.tell_system(message);
    }
}
