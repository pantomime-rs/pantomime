use crate::actor::*;
use crate::dispatcher::Dispatcher;
use crate::mailbox::Mailbox;
use crate::util::Deferred;
use crossbeam::atomic::AtomicCell;
use downcast_rs::Downcast;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::marker::PhantomData;
use std::mem;
use std::panic;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::usize;

#[cfg(feature = "posix-signals-support")]
use crate::posix_signals::{PosixSignal, PosixSignals};

pub enum SystemMsg {
    Stop(Option<FailureReason>),
    ChildStopped(usize),
    Signaled(Signal),
    ActorStopped(usize, StopReason),
    Watch(SystemActorRef),
    SendDelivery(String, usize),

    #[cfg(feature = "posix-signals-support")]
    PosixSignal(i32),
}

pub enum Envelope<Msg>
where
    Msg: Send,
{
    Msg(Msg),
    SystemMsg(SystemMsg),
}

pub enum FailureAction {
    Fail(FailureReason),
    Resume,
}

pub struct FailureError {
    error: Option<Box<dyn Error + 'static + Send>>,
}

impl FailureError {
    pub fn new<E: Error>(error: E) -> Self
    where
        E: 'static + Send,
    {
        Self {
            error: Some(Box::new(error)),
        }
    }

    fn empty() -> Self {
        Self { error: None }
    }

    pub fn source(&self) -> Option<&(dyn Error + 'static + Send)> {
        self.error.as_ref().map(|e| e.as_ref())
    }
}

pub enum FailureReason {
    Panicked,
    Errored(FailureError),
}

pub enum StopReason {
    /// Signifies that the actor was stopped normally.
    Stopped,

    /// Signifies that the actor was stopped due to a failure.
    Failed,
}

pub enum Signal {
    Started,
    Stopped(Option<FailureReason>),
    Resumed,
}

pub trait Actor: Send
where
    Self::Msg: Send,
{
    type Msg;

    fn config_dispatcher(&self, ctx: &ActorSystemContext) -> Option<Dispatcher> {
        {
            let _ = ctx;
        }

        None
    }

    fn config_mailbox(&self, ctx: &ActorSystemContext) -> Option<Mailbox<Envelope<Self::Msg>>> {
        {
            let _ = ctx;
        }

        None
    }

    fn config_throughput(&self, ctx: &ActorSystemContext) -> Option<usize> {
        {
            let _ = ctx;
        }

        None
    }

    fn handle_failure(
        &mut self,
        reason: FailureReason,
        ctx: &mut ActorContext<Self::Msg>,
    ) -> FailureAction {
        {
            let _ = ctx;
        }

        FailureAction::Fail(reason)
    }

    fn receive_signal(&mut self, sig: Signal, ctx: &mut ActorContext<Self::Msg>) {
        {
            let _ = sig;
        }
        {
            let _ = ctx;
        }
    }

    fn receive(&mut self, msg: Self::Msg, context: &mut ActorContext<Self::Msg>);
}

pub(in crate::actor) enum Delivery<Msg> {
    Single(usize, Msg),
    Periodic(usize, Duration, Box<dyn FnMut() -> Msg + 'static + Send>),
}

pub struct ActorContext<Msg>
where
    Msg: Send,
{
    pub(in crate::actor) actor_ref: ActorRef<Msg>,
    pub(in crate::actor) children: HashMap<usize, SystemActorRef>,
    pub(in crate::actor) deliveries: HashMap<String, Delivery<Msg>>,
    pub(in crate::actor) dispatcher: Dispatcher,

    // disagree with Clippy here - I care about the three states
    // but want take etc that the outer option provides
    #[allow(clippy::option_option)]
    pub(in crate::actor) pending_stop: Option<Option<FailureReason>>,

    pub(in crate::actor) state: SpawnedActorState,

    pub(in crate::actor) system_context: ActorSystemContext,

    pub(in crate::actor) watching:
        HashMap<usize, Vec<Box<dyn Fn(StopReason) -> Msg + 'static + Send>>>,

    #[cfg(feature = "posix-signals-support")]
    pub(in crate::actor) watching_posix_signals:
        Vec<Box<dyn Fn(PosixSignal) -> Msg + 'static + Send>>,
}

pub trait Spawnable<A, B> {
    fn perform_spawn(&mut self, a: A) -> B;
}

pub trait Watchable<S, R, Msg, F>
where
    F: Fn(R) -> Msg,
{
    fn perform_watch(&mut self, subject: S, convert: F);
}

pub struct ActorSpawnContext<'a> {
    pub(in crate::actor) children: &'a mut HashMap<usize, SystemActorRef>,
    pub(in crate::actor) system_context: &'a ActorSystemContext,
    pub(in crate::actor) system_ref: SystemActorRef,
    pub(in crate::actor) state: &'a SpawnedActorState,
}

impl<'a> ActorSpawnContext<'a> {
    pub fn spawn<A, B>(&mut self, a: A) -> B
    where
        Self: Spawnable<A, B>,
    {
        self.perform_spawn(a)
    }

    pub(crate) fn system_context(&self) -> &ActorSystemContext {
        &self.system_context
    }

    fn do_spawn<A: Actor>(&mut self, actor: A) -> ActorRef<A::Msg>
    where
        A: 'static + Send,
    {
        ///////////////////////////////////////////////////////////////////////////////////////////////
        // NOTE: this is quite similiar to ActorSystemContext::spawn, and changes should be mirrored //
        ///////////////////////////////////////////////////////////////////////////////////////////////

        let actor = Box::new(actor);

        let empty_ref = ActorRef::empty();

        let dispatcher = actor
            .config_dispatcher(&self.system_context)
            .unwrap_or_else(|| self.system_context.new_actor_dispatcher());

        let mailbox = actor
            .config_mailbox(&self.system_context)
            .unwrap_or_else(|| self.system_context.new_actor_mailbox());

        let throughput = actor
            .config_throughput(&self.system_context)
            .unwrap_or(self.system_context.config().default_actor_throughput);

        let mut spawned_actor = SpawnedActor {
            actor,
            context: ActorContext {
                actor_ref: empty_ref,
                children: HashMap::new(),
                deliveries: HashMap::new(),
                dispatcher: dispatcher.clone(),
                pending_stop: None,
                state: SpawnedActorState::Active,
                system_context: self.system_context.clone(),
                watching: HashMap::new(),

                #[cfg(feature = "posix-signals-support")]
                watching_posix_signals: Vec::new(),
            },
            dispatcher,
            execution_state: Arc::new(AtomicCell::new(SpawnedActorExecutionState::Running)),
            mailbox,
            parent_ref: self.system_ref.clone(),
            stash: VecDeque::new(),
            throughput,
            watchers: Vec::new(),
        };

        let actor_ref = ActorRef {
            inner: Arc::new(Box::new(ActorRefCell {
                id: self.system_context.new_actor_id(),
                state: spawned_actor.execution_state.clone(),
                mailbox_appender: spawned_actor.mailbox.appender(),
            })),
        };

        let empty_ref = {
            let mut actor_ref = actor_ref.clone();

            mem::swap(&mut spawned_actor.context.actor_ref, &mut actor_ref);

            actor_ref
        };

        spawned_actor
            .execution_state
            .clone()
            .store(SpawnedActorExecutionState::Idle(Box::new(spawned_actor)));

        actor_ref.tell_system(SystemMsg::Signaled(Signal::Started));

        self.children.insert(actor_ref.id(), actor_ref.system_ref());

        let r = match self.state {
            SpawnedActorState::Stopped
            | SpawnedActorState::Failed(_)
            | SpawnedActorState::Stopping(_)
            | SpawnedActorState::WaitingForStop => {
                actor_ref.stop();

                empty_ref
            }

            SpawnedActorState::Active => actor_ref,
        };

        r
    }

}

impl<'a, A: Actor> Spawnable<A, ActorRef<A::Msg>> for ActorSpawnContext<'a>
where
    A: 'static + Send,
{
    fn perform_spawn(&mut self, actor: A) -> ActorRef<A::Msg> {
        self.do_spawn(actor)
    }
}



impl<Msg, A: Actor> Spawnable<A, ActorRef<A::Msg>> for ActorContext<Msg>
where
    Msg: 'static + Send,
    A: 'static + Send,
{
    fn perform_spawn(&mut self, actor: A) -> ActorRef<A::Msg> {
        self.spawn_context().spawn(actor)
    }
}

impl<Msg, AMsg, F: Fn(StopReason) -> Msg> Watchable<&ActorRef<AMsg>, StopReason, Msg, F>
    for ActorContext<Msg>
where
    AMsg: 'static + Send,
    Msg: 'static + Send,
    F: 'static + Send,
{
    fn perform_watch(&mut self, subject: &ActorRef<AMsg>, convert: F) {
        self.watch_with(subject, convert)
    }
}

impl<Msg, AMsg, F: Fn(StopReason) -> Msg> Watchable<ActorRef<AMsg>, StopReason, Msg, F>
    for ActorContext<Msg>
where
    AMsg: 'static + Send,
    Msg: 'static + Send,
    F: 'static + Send,
{
    fn perform_watch(&mut self, subject: ActorRef<AMsg>, convert: F) {
        self.watch_with(&subject, convert)
    }
}

impl<Msg, F: Fn(StopReason) -> Msg> Watchable<&SystemActorRef, StopReason, Msg, F>
    for ActorContext<Msg>
where
    Msg: 'static + Send,
    F: 'static + Send,
{
    fn perform_watch(&mut self, subject: &SystemActorRef, convert: F) {
        self.watch_system_with(subject, convert)
    }
}

impl<Msg, F: Fn(StopReason) -> Msg> Watchable<SystemActorRef, StopReason, Msg, F>
    for ActorContext<Msg>
where
    Msg: 'static + Send,
    F: 'static + Send,
{
    fn perform_watch(&mut self, subject: SystemActorRef, convert: F) {
        self.watch_system_with(&subject, convert)
    }
}

impl<Msg> ActorContext<Msg>
where
    Msg: 'static + Send,
{
    /// Obtain a reference to the ActorRef that this context
    /// is attached to.
    pub fn actor_ref(&self) -> &ActorRef<Msg> {
        &self.actor_ref
    }

    /// Obtain a reference to the `Dispatcher` that this context
    /// is attached to.
    ///
    /// Typically this will be equal to the system's dispatcher,
    /// but may differ if the actor has configured a custom one.
    pub fn dispatcher(&self) -> &Dispatcher {
        &self.dispatcher
    }

    /// Cancels a previously scheduled delivery. It is guaranteed
    /// that the actor will not receive the message associated
    /// with the delivery.
    pub fn cancel_delivery<S: AsRef<str>>(&mut self, name: S) {
        self.deliveries.remove(name.as_ref());
    }

    /// Asynchronously fail this actor. No other messages will
    /// be processed by it, but it may take some time for the
    /// failure to be delivered.
    ///
    /// Note that this does not necessarily mean that the actor
    /// will stop, as it may define a `FailureAction` to instead
    /// resume execution.
    pub fn fail<E: Error>(&mut self, reason: E)
    where
        E: 'static + Send,
    {
        self.pending_stop = Some(Some(FailureReason::Errored(FailureError::new(reason))));
    }

    /// Asynchronously stop this actor. No other messages will
    /// be processed by it, but it may take some time for the
    /// failure to be delivered.
    pub fn stop(&mut self) {
        self.pending_stop = Some(None);
        println!("set pending_stop");
    }

    /// Schedules the periodic delivery of a message to this
    /// actor. A name is provided to identify it, allowing it
    /// to be later cancelled.
    ///
    /// An initial timeout specifies how long to wait before
    /// sending the first message, and the interval specifies
    /// how long to wait before sending subsequent messages.
    ///
    /// If a previous delivery with the same name was already
    /// scheduled, it is cancelled and guaranteed tha the
    /// actor will not receive it.
    pub fn schedule_periodic_delivery<F: Fn() -> Msg, S: AsRef<str>>(
        &mut self,
        name: S,
        timeout: Duration,
        interval: Duration,
        msg: F,
    ) where
        F: 'static + Send + Sync,
    {
        let name = name.as_ref();

        let next_id = match self.deliveries.remove_entry(name) {
            Some((name, Delivery::Single(id, _))) => {
                let next_id = if id == usize::MAX { 0 } else { id + 1 };

                self.deliveries
                    .insert(name, Delivery::Periodic(next_id, interval, Box::new(msg)));

                next_id
            }

            Some((name, Delivery::Periodic(id, _, _))) => {
                let next_id = if id == usize::MAX { 0 } else { id + 1 };

                self.deliveries
                    .insert(name, Delivery::Periodic(next_id, interval, Box::new(msg)));

                next_id
            }

            None => {
                let name = name.to_owned();
                let next_id = 0;

                self.deliveries
                    .insert(name, Delivery::Periodic(next_id, interval, Box::new(msg)));

                next_id
            }
        };

        let name = name.to_owned();
        let actor_ref = self.actor_ref.clone();

        self.system_context.schedule_thunk(timeout, move || {
            actor_ref.tell_system(SystemMsg::SendDelivery(name, next_id));
        });
    }

    /// Schedule a single delivery of a message to this actor. A
    /// name is provided to identify the delivery, allowing it
    /// to later be cancelled.
    ///
    /// If a previous delivery with the same name was already
    /// scheduled, it is cancelled and guaranteed tha the
    /// actor will not receive it.
    pub fn schedule_delivery<S: AsRef<str>>(&mut self, name: S, timeout: Duration, msg: Msg) {
        let name = name.as_ref();

        let next_id = match self.deliveries.remove_entry(name) {
            Some((name, Delivery::Single(id, _))) => {
                let next_id = if id == usize::MAX { 0 } else { id + 1 };

                self.deliveries.insert(name, Delivery::Single(next_id, msg));

                next_id
            }

            Some((name, Delivery::Periodic(id, _, _))) => {
                let next_id = if id == usize::MAX { 0 } else { id + 1 };

                self.deliveries.insert(name, Delivery::Single(next_id, msg));

                next_id
            }

            None => {
                let name = name.to_owned();
                let next_id = 0;

                self.deliveries.insert(name, Delivery::Single(next_id, msg));

                next_id
            }
        };

        let name = name.to_owned();
        let actor_ref = self.actor_ref.clone();

        self.system_context.schedule_thunk(timeout, move || {
            actor_ref.tell_system(SystemMsg::SendDelivery(name, next_id));
        });
    }

    /// Schedule the execution of a function. After the supplied timeout
    /// has elapsed, the function will be executed.
    pub fn schedule_thunk<F: FnOnce()>(&self, timeout: Duration, f: F)
    where
        F: 'static + Send + Sync, // @TODO why sync
    {
        self.system_context.schedule_thunk(timeout, f);
    }

    pub fn spawn_context(&mut self) -> ActorSpawnContext {
        ActorSpawnContext {
            children: &mut self.children,
            system_context: &self.system_context,
            system_ref: self.actor_ref.system_ref(),
            state: &self.state,
        }
    }

    /// Spawn the supplied instance, returning a handle to it.
    ///
    /// This is typically used to spawn the following:
    ///
    /// - Actors
    /// - Streams
    /// - Stream Combinators
    pub fn spawn<S, R>(&mut self, spawnable: S) -> R
    where
        Self: Spawnable<S, R>,
    {
        self.perform_spawn(spawnable)
    }

    /// Watch the supplied instance, receiving a domain message when
    /// it completes.
    ///
    /// This is typically used to watch the following:
    ///
    /// - Actors (`ActorRef`)
    /// - Streams (`StreamResult`)
    ///
    /// When the std::future modules are fully stable, this will
    /// also be used to watch futures.
    pub fn watch<W, R, F: Fn(R) -> Msg>(&mut self, watchable: W, convert: F)
    where
        Self: Watchable<W, R, Msg, F>,
    {
        self.perform_watch(watchable, convert);
    }

    pub(crate) fn system_context(&self) -> &ActorSystemContext {
        &self.system_context
    }

    #[cfg(all(feature = "posix-signals-support", target_family = "unix"))]
    pub(crate) fn watch_posix_signals_with<F: Fn(PosixSignal) -> Msg>(&mut self, convert: F)
    where
        F: 'static + Send,
    {
        let new = self.watching_posix_signals.is_empty();

        self.watching_posix_signals.push(Box::new(convert));

        self.system_context
            .tell_reaper_monitor(system::ReaperMsg::WatchPosixSignals(
                self.actor_ref().system_ref(),
            ));
    }

    #[cfg(all(feature = "posix-signals-support", target_family = "windows"))]
    pub(crate) fn watch_posix_signals_with<F: Fn(PosixSignal) -> Msg>(&mut self, convert: F)
    where
        F: 'static + Send,
    {
    }

    fn watch_with<N, F: Fn(StopReason) -> Msg>(&mut self, actor_ref: &ActorRef<N>, msg: F)
    where
        N: 'static + Send,
        F: 'static + Send,
    {
        let actor_id = actor_ref.id();
        let new = !self.watching.contains_key(&actor_id);
        println!("inserted a watch");

        self.watching
            .entry(actor_id)
            .or_insert_with(Vec::new)
            .push(Box::new(msg));


        if new {
            println!("telling actor to watch");
            actor_ref.tell_system(SystemMsg::Watch(self.actor_ref().system_ref()));
        } else {
            println!("not new, wont register");
        }
    }

    fn watch_system_with<F: Fn(StopReason) -> Msg>(&mut self, system_ref: &SystemActorRef, msg: F)
    where
        F: 'static + Send,
    {
        let actor_id = system_ref.id();
        let new = !self.watching.contains_key(&actor_id);

        self.watching
            .entry(actor_id)
            .or_insert_with(Vec::new)
            .push(Box::new(msg));

        if new {
            system_ref.tell_system(SystemMsg::Watch(self.actor_ref().system_ref()));
        }
    }
}

pub struct SystemActorRef {
    pub(in crate::actor) inner: Arc<Box<dyn SystemActorRefInner + Send + Sync>>,
}

impl SystemActorRef {
    /// Attempt to upgrade this `SystemActorRef` to a typed
    /// `ActorRef`.
    ///
    /// If the correct type is supplied, a `Some` will
    /// be returned, allowing you to message the actor
    /// the proper types.
    pub fn actor_ref<N>(&self) -> Option<ActorRef<N>>
    where
        N: 'static + Send,
    {
        self.inner
            .clone_box()
            .into_any()
            .downcast::<ActorRef<N>>()
            .map(|v| *v)
            .ok()
    }

    /// Returns the id of this actor. Actor ids are assigned
    /// in a monotonic fashion when the actor is spawned.
    pub fn id(&self) -> usize {
        self.inner.id()
    }

    /// Asynchronously fail this actor. Other messages that
    /// have already been enqueued will be received by
    /// the actor before the stop failure is delivered.
    ///
    /// Note that this does not necessarily mean that the actor
    /// will stop, as it may define a `FailureAction` to instead
    /// resume execution.
    pub fn fail(&self, reason: FailureError) {
        self.inner.fail(reason);
    }

    /// Asynchronously stop this actor. Other messages that
    /// have already been enqueued will be received by the
    /// actor before the stop message is delivered.
    pub fn stop(&self) {
        self.inner.stop();
    }

    pub(in crate::actor) fn tell_system(&self, msg: SystemMsg) {
        self.inner.tell_system(msg);
    }
}

impl_downcast!(SystemActorRefInner);

impl Clone for SystemActorRef {
    fn clone(&self) -> Self {
        SystemActorRef {
            inner: self.inner.clone(),
        }
    }
}

pub struct ActorRef<M>
where
    M: Send,
{
    pub(in crate::actor) inner: Arc<Box<dyn ActorRefInner<M> + Send + Sync>>,
}

impl<Msg> ActorRef<Msg>
where
    Msg: 'static + Send,
{
    /// Returns an `ActorRef` that doesn't have any receiving logic,
    /// i.e. all messages it receives are dropped.
    ///
    /// This can be useful when wiring up the system, e.g. defining
    /// a default value and then wiring up the actual value at a later
    /// time.
    pub fn empty() -> Self {
        // @TODO it's a shame this allocates
        Self {
            inner: Arc::new(Box::new(EmptyActorRefCell { id: 0 })),
        }
    }

    pub (in crate::actor) fn empty_with_id(id: usize) -> Self {
        Self {
            inner: Arc::new(Box::new(EmptyActorRefCell { id }))
        }
    }

    /// Convert this `ActorRef` into one that handles another type of message by
    /// providing a conversion function.
    pub fn convert<N, Convert: Fn(N) -> Msg>(&self, converter: Convert) -> ActorRef<N>
    where
        N: 'static + Send,
        Msg: 'static + Send,
        Convert: 'static + Send + Sync,
    {
        ActorRef {
            inner: Arc::new(Box::new(StackedActorRefCell {
                converter: Arc::new(converter),
                inner: self.inner.clone(),
            })),
        }
    }

    /// Returns the id of this actor. Actor ids are assigned
    /// in a monotonic fashion when the actor is spawned.
    pub fn id(&self) -> usize {
        self.inner.id()
    }

    /// Send the supplied message to this actor. The message
    /// will be appended to the actor's mailbox and the
    /// actor will be scheduled for execution if it isn't
    /// already.
    pub fn tell(&self, msg: Msg) {
        self.inner.tell(msg);
    }

    /// Asynchronously fail this actor. Other messages that
    /// have already been enqueued will be received by
    /// the actor before the stop failure is delivered.
    ///
    /// Note that this does not necessarily mean that the actor
    /// will stop, as it may define a `FailureAction` to instead
    /// resume execution.
    pub fn fail(&self, reason: FailureError) {
        self.inner.fail(reason);
    }

    /// Asynchronously stop this actor. Other messages that
    /// have already been enqueued will be received by the
    /// actor before the stop message is delivered.
    pub fn stop(&self) {
        self.inner.stop();
    }

    pub(in crate::actor) fn system_ref(&self) -> SystemActorRef {
        // @ TODO it's a shame we have to allocate here
        SystemActorRef {
            inner: Arc::new(Box::new(self.clone())),
        }
    }
}

impl<Msg> Clone for ActorRef<Msg>
where
    Msg: Send,
{
    fn clone(&self) -> ActorRef<Msg> {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<Msg> SystemActorRefInner for ActorRef<Msg>
where
    Msg: 'static + Send,
{
    fn clone_box(&self) -> Box<dyn SystemActorRefInner + Send + Sync> {
        Box::new(self.clone())
    }

    fn id(&self) -> usize {
        self.inner.id()
    }

    fn fail(&self, reason: FailureError) {
        self.inner.fail(reason);
    }

    fn stop(&self) {
        self.inner.stop();
    }

    fn tell_system(&self, msg: SystemMsg) {
        self.inner.tell_system(msg);
    }
}

pub(in crate::actor) enum SpawnedActorExecutionState<Msg>
where
    Msg: Send,
{
    Idle(Box<SpawnedActor<Msg>>),
    Messaged,
    Running,
    Stopped(Mailbox<Envelope<Msg>>),
    Failed(Mailbox<Envelope<Msg>>),
}

pub(in crate::actor) enum SpawnedActorState {
    Active,
    Stopping(Option<FailureReason>),
    Stopped,
    Failed(FailureReason),
    WaitingForStop,
}

pub(in crate::actor) struct SpawnedActor<Msg>
where
    Msg: Send,
{
    pub(in crate::actor) actor: Box<dyn Actor<Msg = Msg>>,
    pub(in crate::actor) context: ActorContext<Msg>,
    pub(in crate::actor) dispatcher: Dispatcher,
    pub(in crate::actor) execution_state: Arc<AtomicCell<SpawnedActorExecutionState<Msg>>>,
    pub(in crate::actor) mailbox: Mailbox<Envelope<Msg>>,
    pub(in crate::actor) parent_ref: SystemActorRef,
    pub(in crate::actor) stash: VecDeque<Envelope<Msg>>,
    pub(in crate::actor) throughput: usize,
    pub(in crate::actor) watchers: Vec<SystemActorRef>,
}

impl<Msg> SpawnedActor<Msg>
where
    Msg: 'static + Send,
{
    fn check_pending_stop(&mut self) {
        if let Some(failed) = self.context.pending_stop.take() {
            println!("got stop");
            match self.context.state {
                SpawnedActorState::Stopped
                | SpawnedActorState::Failed(_)
                | SpawnedActorState::Stopping(_) => {}

                _ => {
                    println!("now WaitingForStop");
                    self.context.state = SpawnedActorState::WaitingForStop;
                    self.context.actor_ref.tell_system(SystemMsg::Stop(failed));
                }
            }
        }
    }

    fn receive(&mut self, msg: Msg) {
        match self.context.state {
            SpawnedActorState::Active => {
                self.actor.receive(msg, &mut self.context);
                self.check_pending_stop();
            }

            SpawnedActorState::WaitingForStop => {
                self.stash.push_back(Envelope::Msg(msg));
            }

            SpawnedActorState::Stopping(_)
            | SpawnedActorState::Stopped
            | SpawnedActorState::Failed(_) => {
                // @TODO dead letters
            }
        }
    }

    fn receive_system(&mut self, msg: SystemMsg) {
        println!("receive_system");

        match (&self.context.state, msg) {
            (SpawnedActorState::WaitingForStop, SystemMsg::Stop(Some(reason))) => {
                println!("waiting for stop and got stop failed");
                match self.actor.handle_failure(reason, &mut self.context) {
                    FailureAction::Resume => {
                        self.check_pending_stop();

                        self.context.state = SpawnedActorState::Active;

                        self.actor
                            .receive_signal(Signal::Resumed, &mut self.context);

                        self.check_pending_stop();

                        self.unstash_all();
                    }

                    FailureAction::Fail(reason) => {
                        // @TODO not calling check_pending_stop here, should we?

                        if self.context.children.is_empty() {
                            self.transition(SpawnedActorState::Failed(reason));

                            self.stash.clear();
                        } else {
                            self.transition(SpawnedActorState::Stopping(Some(reason)));

                            for (_child_id, actor_ref) in self.context.children.iter() {
                                // @TODO think about whether children should be failed

                                actor_ref.stop();
                            }

                            self.stash.clear();
                        }
                    }
                }
            }

            (SpawnedActorState::WaitingForStop, SystemMsg::Stop(None)) => {
                println!("{} waiting for stop and got stop not failed", self.context.actor_ref.id());
                self.stash.clear();

                if self.context.children.is_empty() {
                    println!("{} children stopped, so now were stopped", self.context.actor_ref.id());
                    self.transition(SpawnedActorState::Stopped);

                } else {
                    println!("{} children not stopped, so now were Stopping", self.context.actor_ref.id());
                    self.transition(SpawnedActorState::Stopping(None));

                    for (_child_id, actor_ref) in self.context.children.iter() {
                        // @TODO think about whether children should be failed

                        println!("{} telling {} to stop", self.context.actor_ref.id(), _child_id);
                        actor_ref.stop();
                    }
                }
            }

            (SpawnedActorState::Active, SystemMsg::Stop(maybe_reason)) => {
                println!("active, got stop");
                if self.context.children.is_empty() {
                    let next_state = match maybe_reason {
                        Some(reason) => SpawnedActorState::Failed(reason),
                        None => SpawnedActorState::Stopped,
                    };

                    self.transition(next_state);
                } else {
                    self.transition(SpawnedActorState::Stopping(maybe_reason));

                    for (_child_id, actor_ref) in self.context.children.iter() {
                        // @TODO think about whether children should be failed

                        actor_ref.stop();
                    }
                }
            }

            (SpawnedActorState::Stopping(_), SystemMsg::ChildStopped(id)) => {
                println!("stopping, child {} stopped", id);
                self.context.children.remove(&id);

                if self.context.children.is_empty() {
                    println!("{} children empty, totally stopping", self.context.actor_ref.id());

                    let mut state = SpawnedActorState::Stopped;

                    mem::swap(&mut state, &mut self.context.state);

                    match state {
                        SpawnedActorState::Stopping(reason) => {
                            self.transition(
                                match reason {
                                    None         => SpawnedActorState::Stopped,
                                    Some(reason) => SpawnedActorState::Failed(reason),
                                }
                            );
                        }

                        _ => {
                            panic!("pantomime bug: illegal state in SpawnedActor::receive_system");
                        }
                    }


                } else {
                    println!("non empty, not stopping yet");
                }
            }

            (SpawnedActorState::Failed(_), SystemMsg::Watch(watcher)) => {
                println!("failed, got watch");
                watcher.tell_system(SystemMsg::ActorStopped(self.context.actor_ref().id(), StopReason::Failed));
            }

            (SpawnedActorState::Stopped, SystemMsg::Watch(watcher)) => {
                println!("in stopped, got watch");
                watcher.tell_system(SystemMsg::ActorStopped(
                    self.context.actor_ref().id(),
                    StopReason::Stopped,
                ));
            }

            (SpawnedActorState::Failed(_), _) => {
                println!("failed, got other");
            }

            (SpawnedActorState::Stopped, _) => {
                println!("stopped, got other");
            }

            (_, SystemMsg::Watch(watcher)) => {
                println!("pushing a watcher");

                self.watchers.push(watcher);
            }

            (_, SystemMsg::SendDelivery(name, id)) => {
                println!("send delivery");
                match self.context.deliveries.get_mut(&name) {
                    Some(Delivery::Single(i, _)) if *i == id => {
                        if let Some(Delivery::Single(_, msg)) =
                            self.context.deliveries.remove(&name)
                        {
                            self.receive(msg);
                        } else {
                            panic!("pantomime bug: inconceivable delivery pattern match failure");
                        }
                    }

                    Some(Delivery::Periodic(i, interval, ref mut msg)) if *i == id => {
                        let msg = msg();
                        let actor_ref = self.context.actor_ref.clone();
                        let interval = *interval;

                        self.context.schedule_thunk(interval, move || {
                            actor_ref.tell_system(SystemMsg::SendDelivery(name, id));
                        });

                        self.receive(msg);
                    }

                    _ => {}
                }
            }

            (_, SystemMsg::ActorStopped(actor_id, reason)) => {
                println!("any, actor stopped");

                if let Some(mut msgs) = self.context.watching.remove(&actor_id) {
                    for msg in msgs.drain(..) {
                        match reason {
                            StopReason::Failed => {
                                self.receive(msg(StopReason::Failed));
                            }

                            StopReason::Stopped => {
                                self.receive(msg(StopReason::Stopped));
                            }
                        }
                    }
                }
            }

            (_, SystemMsg::Signaled(signal)) => {
                println!("any, got signal");
                self.actor.receive_signal(signal, &mut self.context);
                self.check_pending_stop();
            }

            #[cfg(feature = "posix-signals-support")]
            (_, SystemMsg::PosixSignal(signal)) => {
                // @FIXME should we support numeric signals?

                let mut messages = Vec::new();

                for converter in self.context.watching_posix_signals.iter() {
                    if signal == PosixSignal::SIGHUP as i32 {
                        messages.push(converter(PosixSignal::SIGHUP));
                    } else if signal == PosixSignal::SIGINT as i32 {
                        messages.push(converter(PosixSignal::SIGINT));
                    } else if signal == PosixSignal::SIGTERM as i32 {
                        messages.push(converter(PosixSignal::SIGTERM));
                    }
                }

                for message in messages.drain(..) {
                    self.receive(message);
                }
            }

            (SpawnedActorState::Stopping(None), SystemMsg::Stop(Some(_))) => {
                println!("stopping, got a fail");
                // @TODO do something with reason?
                // We've failed while we were stopping, which means that a signal
                // handler panicked. We still consider ourselves stopped, to do
                // otherwise would be strange when considering failure policies.
            }

            (_, SystemMsg::Stop { .. }) => {
                println!("in any, got Stop");
                // @TODO think about what it means to have received this
                // if we're currently Stopped (ie not Failed)
            }

            (_, SystemMsg::ChildStopped(child_id)) => {
                println!("in any, got ChildStopped");
                self.context.children.remove(&child_id);

                // @TODO log this? shouldnt happen
            }
        }
    }

    fn perform_continue(actor_id: usize, execution_state: Arc<AtomicCell<SpawnedActorExecutionState<Msg>>>) {
        match execution_state.swap(SpawnedActorExecutionState::Messaged) {
            SpawnedActorExecutionState::Idle(actor) => {
                actor.dispatcher.clone().execute(|| actor.run());
            }

            SpawnedActorExecutionState::Messaged => {}

            SpawnedActorExecutionState::Running => {}

            SpawnedActorExecutionState::Stopped(mailbox) => {
                Self::drain(actor_id, execution_state, mailbox, false);
            }

            SpawnedActorExecutionState::Failed(mailbox) => {
                Self::drain(actor_id, execution_state, mailbox, true);
            }
        }
    }

    fn drain(actor_id: usize,
             execution_state: Arc<AtomicCell<SpawnedActorExecutionState<Msg>>>,
             mailbox: Mailbox<Envelope<Msg>>,
             failed: bool) {
        // @FIXME duplicate code below

        let mut maybe_mailbox = Some(mailbox);

        loop {
            let mut mailbox = maybe_mailbox.take().expect("pantomime bug: Option::take failure in SpawnActor::drain");

            loop {
                match mailbox.retrieve() {
                    Some(Envelope::Msg(msg)) => {
                        drop(msg);
                    }

                    Some(Envelope::SystemMsg(SystemMsg::Watch(watcher))) => {
                        println!("drain() got a watch");

                        watcher.tell_system(
                            SystemMsg::ActorStopped(
                                actor_id,
                                if failed {
                                    StopReason::Failed
                                } else {
                                    StopReason::Stopped
                                }
                            )
                        );
                    }

                    Some(Envelope::SystemMsg(msg)) => {
                        drop(msg);
                    }

                    None => {
                        break;
                    }
                }
            }

            let next_execution_state = if failed {
                SpawnedActorExecutionState::Failed(mailbox)
            } else {
                SpawnedActorExecutionState::Stopped(mailbox)
            };

            match execution_state.swap(next_execution_state) {
                SpawnedActorExecutionState::Idle(actor) => {
                    actor.dispatcher.clone().execute(|| actor.run());

                    break;
                }

                SpawnedActorExecutionState::Messaged => {
                    break;
                }

                SpawnedActorExecutionState::Running => {
                    break;
                }

                SpawnedActorExecutionState::Stopped(mailbox) => {
                    maybe_mailbox = Some(mailbox);

                    continue;
                }

                SpawnedActorExecutionState::Failed(mailbox) => {
                    maybe_mailbox = Some(mailbox);

                    continue;
                }
            }
        }
    }

    fn run(self: Box<Self>) {
        self.execution_state
            .store(SpawnedActorExecutionState::Running);

        let throughput = self.throughput;

        let this = Rc::new(RefCell::new((self, 0)));

        #[allow(unused_variables)]
        let deferred = {
            let this = this.clone();

            Deferred::new(move || {
                let (mut this, processed) = Rc::try_unwrap(this)
                    .ok()
                    .expect("pantomime bug: cannot retrieve SpawnedActor")
                    .into_inner();

                // @TODO if the conversion function panics, is this right?
                if thread::panicking() {
                    println!("                 PANICKING");
                    if let SpawnedActorState::Stopping(_) = this.context.state {
                    } else {
                        this.context.state = SpawnedActorState::WaitingForStop;

                        #[allow(unused_variables)]
                        let deferred = {
                            let actor_ref = this.context.actor_ref.clone();

                            Deferred::new(move || {
                                actor_ref.tell_system(SystemMsg::Stop(Some(FailureReason::Panicked)));
                            });
                        };
                    }
                }

                match this.context.state {
                    SpawnedActorState::Stopped => {
                        let cont = match this.execution_state
                                  .swap(SpawnedActorExecutionState::Stopped(this.mailbox)) {
                            SpawnedActorExecutionState::Idle(actor) => {
                                actor.dispatcher.clone().execute(|| actor.run());
                                false
                            }

                            SpawnedActorExecutionState::Running => false,

                            SpawnedActorExecutionState::Messaged => true,

                            SpawnedActorExecutionState::Stopped(mailbox) => {
                                Self::drain(
                                    this.context.actor_ref.id(),
                                    this.execution_state.clone(),
                                    mailbox,
                                    false
                                );

                                false
                            }

                            SpawnedActorExecutionState::Failed(mailbox) => {
                                Self::drain(
                                    this.context.actor_ref.id(),
                                    this.execution_state.clone(),
                                    mailbox,
                                    true
                                );

                                false
                            }
                        };

                        if cont {
                            Self::perform_continue(this.context.actor_ref.id(), this.execution_state.clone());
                        } else {
                            this.context.actor_ref = ActorRef::empty_with_id(this.context.actor_ref.id());
                        }
                    }

                    SpawnedActorState::Failed(_) => {
                        let cont = match this.execution_state
                                  .swap(SpawnedActorExecutionState::Failed(this.mailbox)) {
                            SpawnedActorExecutionState::Idle(actor) => {
                                actor.dispatcher.clone().execute(|| actor.run());
                                false
                            }

                            SpawnedActorExecutionState::Running => false,

                            SpawnedActorExecutionState::Messaged => true,

                            SpawnedActorExecutionState::Stopped(mailbox) => {
                                Self::drain(
                                    this.context.actor_ref.id(),
                                    this.execution_state.clone(),
                                    mailbox,
                                    false
                                );

                                false
                            }

                            SpawnedActorExecutionState::Failed(mailbox) => {
                                Self::drain(
                                    this.context.actor_ref.id(),
                                    this.execution_state.clone(),
                                    mailbox,
                                    true
                                );

                                false
                            }
                        };

                        if cont {
                            Self::perform_continue(this.context.actor_ref.id(), this.execution_state.clone());
                        } else {
                            this.context.actor_ref = ActorRef::empty_with_id(this.context.actor_ref.id());
                        }
                    }

                    _ => {
                        let actor_id = this.context.actor_ref.id();
                        println!("{} HEREEEEEEEEEEEEEEEEEE", actor_id);
                        let execution_state = this.execution_state.clone();

                        let cont = match this
                            .execution_state
                            .clone()
                            .swap(SpawnedActorExecutionState::Idle(this))
                        {
                            SpawnedActorExecutionState::Idle(actor) => {
                                // @FIXME this should be impossible and we should probably fail..
                                actor.dispatcher.clone().execute(|| actor.run());
                                false
                            }

                            SpawnedActorExecutionState::Running => false,

                            SpawnedActorExecutionState::Messaged => true,

                            SpawnedActorExecutionState::Stopped(mailbox) => {
                                println!("stopped!");
                                Self::drain(actor_id, execution_state.clone(), mailbox, false);

                                false
                            }

                            SpawnedActorExecutionState::Failed(mailbox) => {
                                println!("failed!");
                                Self::drain(actor_id, execution_state.clone(), mailbox, false);

                                false
                            }
                        } || processed == throughput;

                        if cont {
                            Self::perform_continue(actor_id, execution_state);
                        }
                    }
                }
            })
        };

        {
            let next_this = this.clone();
            drop(this);
            let this = next_this;

            let mut this = this.borrow_mut();

            while this.1 < throughput {
                match this.0.mailbox.retrieve() {
                    Some(Envelope::Msg(msg)) => {
                        this.0.receive(msg);
                        this.1 += 1;
                    }

                    Some(Envelope::SystemMsg(msg)) => {
                        this.0.receive_system(msg);
                        this.1 += 1;
                    }

                    None => {
                        break;
                    }
                }
            }
        }
    }

    fn transition(&mut self, mut next: SpawnedActorState) {
        match next {
            SpawnedActorState::Stopped => {
                self.parent_ref
                    .tell_system(SystemMsg::ChildStopped(self.context.actor_ref().id()));

                println!("told parent");

                println!("transitioning to Stopped");

                self.context.state = SpawnedActorState::Stopped;

                self.actor
                    .receive_signal(Signal::Stopped(None), &mut self.context);

                self.context.pending_stop = None;

                println!("# watchers: {}", self.watchers.len());

                for watcher in self.watchers.drain(..) {
                    println!("telling a watcher");
                    watcher.tell_system(SystemMsg::ActorStopped(
                        self.context.actor_ref().id(),
                        StopReason::Stopped,
                    ));
                }
            }

            SpawnedActorState::Failed(_) => {
                self.parent_ref
                    .tell_system(SystemMsg::ChildStopped(self.context.actor_ref().id()));

                println!("told parent");

                let mut state =
                    SpawnedActorState::Failed(FailureReason::Errored(FailureError::empty()));

                mem::swap(&mut state, &mut next);

                println!("transitioning to failed");

                self.context.state = next;

                match state {
                    SpawnedActorState::Failed(reason) => {
                        self.actor
                            .receive_signal(Signal::Stopped(Some(reason)), &mut self.context);

                        self.context.pending_stop = None;

                        for watcher in self.watchers.drain(..) {
                            watcher.tell_system(SystemMsg::ActorStopped(
                                self.context.actor_ref().id(),
                                StopReason::Failed,
                            ));
                        }
                    }

                    _ => {
                        panic!("pantomime bug: unexpected SpawnedActorState");
                    }
                }
            }

            _ => {
                self.context.state = next;
            }
        }
    }

    fn unstash_all(&mut self) {
        // @TODO if there are tons of messages, this can stall other actors from making progress.
        //       it's important that ordering guarantees remain the same though, so punt this
        //       for now and document the limitation

        loop {
            match self.context.state {
                // @TODO this was uncommented, seems dumb
                /*
                SpawnedActorState::WaitingForStop => {
                    return;
                }*/

                _ => match self.stash.pop_front() {
                    Some(Envelope::Msg(msg)) => {
                        self.receive(msg);
                    }

                    Some(Envelope::SystemMsg(msg)) => {
                        self.receive_system(msg);
                    }

                    None => {
                        return;
                    }
                },
            }
        }
    }
}

pub(in crate::actor) trait ActorRefInner<Msg>: SystemActorRefInner
where
    Self: Send,
    Msg: Send,
{
    fn tell(&self, msg: Msg);
}

pub(in crate::actor) trait SystemActorRefInner: Downcast {
    fn clone_box(&self) -> Box<dyn SystemActorRefInner + Send + Sync>;

    fn fail(&self, reason: FailureError);

    fn stop(&self);

    fn id(&self) -> usize;

    fn tell_system(&self, msg: SystemMsg);
}

pub(in crate::actor) struct ActorRefCell<Msg>
where
    Msg: 'static + Send,
{
    pub(in crate::actor) id: usize,
    pub(in crate::actor) state: Arc<AtomicCell<SpawnedActorExecutionState<Msg>>>,
    pub(in crate::actor) mailbox_appender: MailboxAppender<Envelope<Msg>>,
}

impl<Msg> ActorRefCell<Msg>
where
    Msg: 'static + Send,
{
    fn messaged(&self) {
        println!("messaged()");
        match self.state.swap(SpawnedActorExecutionState::Messaged) {
            SpawnedActorExecutionState::Idle(actor) => {
                println!("will execute");
                actor.dispatcher.clone().execute(|| actor.run())
            }

            SpawnedActorExecutionState::Running => {
                println!("already running");
            }

            SpawnedActorExecutionState::Messaged => {
                println!("already messaged");
            }

            SpawnedActorExecutionState::Stopped(mailbox) => {
                println!("messaged(), Stopped");
                self.drain(mailbox, false);
            }

            SpawnedActorExecutionState::Failed(mailbox) => {
                println!("messaged(), Failed");
                self.drain(mailbox, true);
            }
        }
    }

    fn drain(&self, mailbox: Mailbox<Envelope<Msg>>, failed: bool) {
        // @FIXME duplicate code above

        let mut maybe_mailbox = Some(mailbox);

        loop {
            let mut mailbox = maybe_mailbox.take().expect("pantomime bug: Option::take failure in SpawnActor::drain");

            loop {
                match mailbox.retrieve() {
                    Some(Envelope::Msg(msg)) => {
                        drop(msg);
                    }

                    Some(Envelope::SystemMsg(SystemMsg::Watch(watcher))) => {
                        println!("drain() got a watch");

                        watcher.tell_system(
                            SystemMsg::ActorStopped(
                                self.id,
                                match failed {
                                    true  => StopReason::Failed,
                                    false => StopReason::Stopped
                                }
                            )
                        );
                    }

                    Some(Envelope::SystemMsg(msg)) => {
                        drop(msg);
                    }

                    None => {
                        break;
                    }
                }
            }

            let next_execution_state = if failed {
                SpawnedActorExecutionState::Failed(mailbox)
            } else {
                SpawnedActorExecutionState::Stopped(mailbox)
            };

            match self.state.swap(next_execution_state) {
                SpawnedActorExecutionState::Idle(actor) => {
                    actor.dispatcher.clone().execute(|| actor.run());

                    break;
                }

                SpawnedActorExecutionState::Messaged => {
                    break;
                }

                SpawnedActorExecutionState::Running => {
                    break;
                }

                SpawnedActorExecutionState::Stopped(mailbox) => {
                    maybe_mailbox = Some(mailbox);
                }

                SpawnedActorExecutionState::Failed(mailbox) => {
                    maybe_mailbox = Some(mailbox);
                }
            }
        }
    }
}

impl<Msg> SystemActorRefInner for ActorRefCell<Msg>
where
    Msg: 'static + Send,
{
    fn clone_box(&self) -> Box<dyn SystemActorRefInner + Send + Sync> {
        Box::new(ActorRefCell {
            id: self.id,
            state: self.state.clone(),
            mailbox_appender: self.mailbox_appender.clone(),
        })
    }

    fn fail(&self, reason: FailureError) {
        self.tell_system(SystemMsg::Stop(Some(FailureReason::Errored(reason))));
    }

    fn stop(&self) {
        self.tell_system(SystemMsg::Stop(None));
    }

    fn id(&self) -> usize {
        self.id
    }

    fn tell_system(&self, msg: SystemMsg) {
        self.mailbox_appender.append(Envelope::SystemMsg(msg));
        self.messaged();
    }
}

impl<Msg> ActorRefInner<Msg> for ActorRefCell<Msg>
where
    Msg: 'static + Send,
{
    fn tell(&self, msg: Msg) {
        self.mailbox_appender.append(Envelope::Msg(msg));
        self.messaged();
    }
}

struct StackedActorRefCell<NewMsg, Msg>
where
    NewMsg: Send,
    Msg: Send,
{
    converter: Arc<dyn Fn(NewMsg) -> Msg + Send + Sync>,
    inner: Arc<Box<dyn ActorRefInner<Msg> + Send + Sync>>, // @TODO can we remove the Box?
}

impl<NewMsg, Msg> SystemActorRefInner for StackedActorRefCell<NewMsg, Msg>
where
    NewMsg: 'static + Send,
    Msg: 'static + Send,
{
    fn clone_box(&self) -> Box<dyn SystemActorRefInner + Send + Sync> {
        Box::new(StackedActorRefCell {
            converter: self.converter.clone(),
            inner: self.inner.clone(),
        })
    }

    fn fail(&self, reason: FailureError) {
        self.inner.fail(reason);
    }

    fn stop(&self) {
        self.inner.stop();
    }

    fn id(&self) -> usize {
        self.inner.id()
    }

    fn tell_system(&self, msg: SystemMsg) {
        self.inner.tell_system(msg);
    }
}

impl<NewMsg, Msg> ActorRefInner<NewMsg> for StackedActorRefCell<NewMsg, Msg>
where
    NewMsg: 'static + Send,
    Msg: 'static + Send,
{
    fn tell(&self, msg: NewMsg) {
        self.inner.tell((self.converter)(msg));
    }
}

struct EmptyActorRefCell {
    id: usize
}

impl<Msg> ActorRefInner<Msg> for EmptyActorRefCell
where
    Msg: 'static + Send,
{
    fn tell(&self, _: Msg) {}
}

impl SystemActorRefInner for EmptyActorRefCell {
    fn clone_box(&self) -> Box<dyn SystemActorRefInner + Send + Sync> {
        Box::new(EmptyActorRefCell { id: self.id })
    }

    fn fail(&self, _: FailureError) {}

    fn stop(&self) {}

    fn id(&self) -> usize {
        self.id
    }

    fn tell_system(&self, _: SystemMsg) {}
}
