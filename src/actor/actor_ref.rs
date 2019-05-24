use crate::actor::actor_watcher::ActorWatcherMessage;
use crate::actor::*;
use crate::dispatcher::{Dispatcher, Thunk};
use crate::mailbox::Mailbox;
use crate::timer::{TimerMsg, TimerThunk};
use crate::util::{Cancellable, Deferred};
use crossbeam::atomic::AtomicCell;
use downcast_rs::Downcast;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::panic;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub enum SystemMsg {
    Stop(bool),
    ChildStopped(usize),
    Signaled(Signal),
}

pub enum Envelope<Msg>
where
    Msg: Send,
{
    Msg(Msg),
    SystemMsg(SystemMsg),
}

pub enum StopReason {
    /// Signifies that the actor was stopped normally.
    Stopped,

    /// Signifies that the actor was stopped due to a failure.
    Failed,

    /// Signifies that the actor was already stopped but the cause is unknown.
    AlreadyStopped,
}

pub enum Signal {
    Started,
    Stopped,
    Failed,
    ActorStopped(SystemActorRef, StopReason),

    #[cfg(feature = "posix-signals-support")]
    PosixSignal(i32),
}

pub trait Actor<Msg>: Send
/* + panic::UnwindSafe + panic::RefUnwindSafe*/ // @TODO unwind safe
where
    Msg: Send,
{
    fn config_dispatcher(&self, _: &ActorSystemContext) -> Option<Dispatcher> {
        None
    }

    fn config_mailbox(&self, _: &ActorSystemContext) -> Option<Mailbox<Envelope<Msg>>> {
        None
    }

    fn config_throughput(&self, _: &ActorSystemContext) -> Option<usize> {
        None
    }

    fn receive_signal(&mut self, _: Signal, _: &mut ActorContext<Msg>) {}

    fn receive(&mut self, msg: Msg, context: &mut ActorContext<Msg>);
}

pub struct ActorContext<Msg>
where
    Msg: Send,
{
    pub(in crate::actor) actor_ref: ActorRef<Msg>,
    pub(in crate::actor) children: HashMap<usize, SystemActorRef>,
    pub(in crate::actor) deliveries: HashMap<String, Cancellable>,
    pub(in crate::actor) dispatcher: Dispatcher,
    pub(in crate::actor) system_context: ActorSystemContext,
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

    pub fn cancel_delivery<S: AsRef<str>>(&mut self, name: S) {
        if let Some(c) = self.deliveries.remove(name.as_ref()) {
            c.cancel();
        }
    }

    pub fn schedule_periodic_delivery<F: Fn() -> Msg, S: AsRef<str>>(
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
                self.system_context.clone(),
                self.actor_ref.clone(),
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

    pub fn schedule_delivery<F: Fn() -> Msg, S: AsRef<str>>(
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

            self.system_context.schedule_thunk(timeout, move || {
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
        F: 'static + Send + Sync, // @TODO why sync
    {
        if let Some(ref timer_ref) = self.system_context.timer_ref() {
            timer_ref.tell(TimerMsg::Schedule {
                after: timeout,
                thunk: TimerThunk::new(Box::new(f)),
            });
        } else {
            panic!("pantomime bug: schedule_thunk called on internal context");
        }
    }

    pub fn watch<N>(&mut self, actor_ref: &ActorRef<N>)
    where
        N: 'static + Send,
    {
        match self.system_context.watcher_ref() {
            Some(watcher_ref) => watcher_ref.tell(ActorWatcherMessage::Subscribe(
                self.actor_ref().system_ref(),
                actor_ref.system_ref(),
            )),

            None => {
                // @TODO panic instead
                error!(
                        "#[{watcher}] attempted to watch #[{watching}] but it does not have a reference to ActorWatcher; this is unexpected",
                        watcher = self.actor_ref().id(),
                        watching = actor_ref.id()
                    );
            }
        }
    }

    #[cfg(all(feature = "posix-signals-support", target_family = "unix"))]
    pub fn watch_posix_signals(&mut self) {
        // @TODO panic? should be internal if this is missing, and internal shouldn't watch signals

        if let Some(ref watcher_ref) = self.system_context.watcher_ref() {
            watcher_ref.tell(ActorWatcherMessage::SubscribePosixSignals(
                self.actor_ref().system_ref(),
            ));
        }
    }

    #[cfg(all(feature = "posix-signals-support", target_family = "windows"))]
    pub fn watch_posix_signals(&mut self) {}

    pub fn spawn<AMsg, A: Actor<AMsg>>(&mut self, actor: A) -> ActorRef<AMsg>
    where
        A: 'static + Send,
        AMsg: 'static + Send,
        Msg: 'static + Send,
    {
        ///////////////////////////////////////////////////////////////////////////////////////////////
        // NOTE: this is quite similiar to ActorSystemContext::spawn, and changes should be mirrored //
        ///////////////////////////////////////////////////////////////////////////////////////////////

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
            actor: Box::new(actor),
            context: ActorContext {
                actor_ref: empty_ref,
                children: HashMap::new(),
                deliveries: HashMap::new(),
                dispatcher: dispatcher.clone(),
                system_context: self.system_context.clone(),
            },
            dispatcher,
            execution_state: Arc::new(AtomicCell::new(SpawnedActorExecutionState::Running)),
            mailbox,
            parent_ref: self.actor_ref.system_ref(),
            stash: VecDeque::new(),
            state: SpawnedActorState::Spawned,
            throughput,
        };

        let actor_ref = ActorRef {
            inner: Arc::new(Box::new(ActorRefCell {
                id: self.system_context.new_actor_id(),
                state: spawned_actor.execution_state.clone(),
                mailbox_appender: spawned_actor.mailbox.appender(),
            })),
        };

        spawned_actor.context.actor_ref = actor_ref.clone();

        spawned_actor
            .execution_state
            .clone()
            .store(SpawnedActorExecutionState::Idle(Box::new(spawned_actor)));

        self.children.insert(actor_ref.id(), actor_ref.system_ref());

        if let Some(ref watcher_ref) = self.system_context.watcher_ref() {
            watcher_ref.tell(ActorWatcherMessage::Started(
                actor_ref.id(),
                actor_ref.system_ref(),
                false,
            ));
        }

        actor_ref
    }

    pub fn spawn_watched<AMsg, A: Actor<AMsg>>(&mut self, actor: A) -> ActorRef<AMsg>
    where
        A: 'static + Send,
        AMsg: 'static + Send,
        Msg: 'static + Send,
    {
        let actor_ref = self.spawn(actor);

        self.watch(&actor_ref);

        actor_ref
    }

    pub(crate) fn system_context(&self) -> &ActorSystemContext {
        &self.system_context
    }

    fn periodic_delivery<F: Fn() -> Msg>(
        context: ActorSystemContext,
        actor_ref: ActorRef<Msg>,
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
                            next_context,
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
}

pub struct SystemActorRef {
    pub(in crate::actor) inner: Arc<Box<SystemActorRefInner + Send + Sync>>,
}

impl SystemActorRef {
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

    pub fn id(&self) -> usize {
        self.inner.id()
    }

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
    pub(in crate::actor) inner: Arc<Box<ActorRefInner<M> + Send + Sync>>,
}

impl<Msg> ActorRef<Msg>
where
    Msg: 'static + Send,
{
    pub fn empty() -> Self {
        Self {
            inner: Arc::new(Box::new(EmptyActorRefCell)),
        }
    }

    /// Convert this ActorRef into one that handles another type of message by
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

    pub fn id(&self) -> usize {
        self.inner.id()
    }

    pub fn tell(&self, msg: Msg) {
        self.inner.tell(msg);
    }
    pub(in crate::actor) fn tell_cancellable(
        &self,
        cancellable: Cancellable,
        msg: Msg,
        thunk: Option<Thunk>,
    ) {
        self.inner.tell_cancellable(cancellable, msg, thunk);
    }

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
    fn clone_box(&self) -> Box<SystemActorRefInner + Send + Sync> {
        Box::new(self.clone())
    }

    fn id(&self) -> usize {
        self.inner.id()
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
    Stopped,
}

pub(in crate::actor) enum SpawnedActorState {
    Spawned,
    Active,
    Stopping(bool),
    Stopped,
    Failed,
}

pub(in crate::actor) struct SpawnedActor<Msg>
where
    Msg: Send,
{
    pub(in crate::actor) actor: Box<Actor<Msg>>,
    pub(in crate::actor) context: ActorContext<Msg>,
    pub(in crate::actor) dispatcher: Dispatcher,
    pub(in crate::actor) execution_state: Arc<AtomicCell<SpawnedActorExecutionState<Msg>>>,
    pub(in crate::actor) mailbox: Mailbox<Envelope<Msg>>,
    pub(in crate::actor) parent_ref: SystemActorRef,
    pub(in crate::actor) stash: VecDeque<Envelope<Msg>>,
    pub(in crate::actor) state: SpawnedActorState,
    pub(in crate::actor) throughput: usize,
}

impl<Msg> SpawnedActor<Msg>
where
    Msg: 'static + Send,
{
    fn receive(&mut self, msg: Msg) {
        match self.state {
            SpawnedActorState::Active => self.actor.receive(msg, &mut self.context),

            SpawnedActorState::Spawned => {
                self.stash.push_back(Envelope::Msg(msg));
            }

            SpawnedActorState::Stopping(_)
            | SpawnedActorState::Stopped
            | SpawnedActorState::Failed => {
                // @TODO dead letters
            }
        }
    }

    fn receive_system(&mut self, msg: SystemMsg) {
        match (&self.state, msg) {
            (SpawnedActorState::Spawned, SystemMsg::Signaled(Signal::Started)) => {
                self.transition(SpawnedActorState::Active);

                self.actor
                    .receive_signal(Signal::Started, &mut self.context);

                self.unstash_all();
            }

            (SpawnedActorState::Spawned, other) => {
                // all other messages are irrelevant right now, so put
                // them back into the mailbox until we get our start
                // signal (which comes from the watcher)
                //
                // we don't need to reschedule ourselves for execution
                // as the arrival of the start signal will do that
                self.stash.push_back(Envelope::SystemMsg(other));
            }

            (SpawnedActorState::Active, SystemMsg::Stop(failure)) => {
                if self.context.children.is_empty() {
                    self.transition(if failure {
                        SpawnedActorState::Failed
                    } else {
                        SpawnedActorState::Stopped
                    });

                    self.parent_ref
                        .tell_system(SystemMsg::ChildStopped(self.context.actor_ref().id()));
                } else {
                    self.transition(SpawnedActorState::Stopping(failure));

                    for (_child_id, actor_ref) in self.context.children.iter() {
                        // @TODO think about whether children should be failed

                        actor_ref.stop();
                    }
                }
            }

            (SpawnedActorState::Stopping(failure), SystemMsg::ChildStopped(id)) => {
                self.context.children.remove(&id);

                if self.context.children.is_empty() {
                    let next_state = if *failure {
                        SpawnedActorState::Failed
                    } else {
                        SpawnedActorState::Stopped
                    };

                    self.transition(next_state);

                    self.parent_ref
                        .tell_system(SystemMsg::ChildStopped(self.context.actor_ref.id()));
                }
            }

            (SpawnedActorState::Failed, _) => {}

            (SpawnedActorState::Stopped, _) => {}

            (_, SystemMsg::Signaled(signal)) => {
                self.actor.receive_signal(signal, &mut self.context)
            }

            (SpawnedActorState::Stopping(false), SystemMsg::Stop(true)) => {
                // We've failed while we were stopping, which means that a signal
                // handler paniced. Ensure that we flag ourselves as failed.
                self.transition(SpawnedActorState::Stopping(true));
            }

            (_, SystemMsg::Stop { .. }) => {
                // @TODO think about what it means to have received this
                // if we're currently Stopped (ie not Failed)
            }

            (_, SystemMsg::ChildStopped(child_id)) => {
                self.context.children.remove(&child_id);
                self.context.children.shrink_to_fit();

                // @TODO log this? shouldnt happe
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
                let (this, processed) = Rc::try_unwrap(this)
                    .ok()
                    .expect("pantomime bug: cannot retrieve SpawnedActor")
                    .into_inner();

                if thread::panicking() {
                    this.context.actor_ref.tell_system(SystemMsg::Stop(true));
                }

                match this.state {
                    SpawnedActorState::Stopped | SpawnedActorState::Failed => {
                        this.execution_state
                            .swap(SpawnedActorExecutionState::Stopped);
                    }

                    _ => {
                        let execution_state = this.execution_state.clone();

                        let cont = match this
                            .execution_state
                            .clone()
                            .swap(SpawnedActorExecutionState::Idle(this))
                        {
                            SpawnedActorExecutionState::Idle(actor) => {
                                actor.dispatcher.clone().execute(|| actor.run());
                                false
                            }

                            SpawnedActorExecutionState::Running => false,

                            SpawnedActorExecutionState::Messaged => true,

                            SpawnedActorExecutionState::Stopped => false,
                        } || processed == throughput;

                        if cont {
                            match execution_state.swap(SpawnedActorExecutionState::Messaged) {
                                SpawnedActorExecutionState::Idle(actor) => {
                                    actor.dispatcher.clone().execute(|| actor.run());
                                }

                                SpawnedActorExecutionState::Messaged => {}

                                SpawnedActorExecutionState::Running => {}

                                SpawnedActorExecutionState::Stopped => {}
                            }
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

    fn tell_watcher(&self, message: ActorWatcherMessage) {
        if let Some(watcher_ref) = self.context.system_context.watcher_ref() {
            watcher_ref.tell(message);
        } else {
            // @TODO
        }
    }

    fn transition(&mut self, next: SpawnedActorState) {
        self.state = next;

        match self.state {
            SpawnedActorState::Stopped => {
                self.actor
                    .receive_signal(Signal::Stopped, &mut self.context);

                self.tell_watcher(ActorWatcherMessage::Stopped(
                    self.context.actor_ref().system_ref(),
                ));
            }

            SpawnedActorState::Failed => {
                self.actor.receive_signal(Signal::Failed, &mut self.context);

                self.tell_watcher(ActorWatcherMessage::Failed(
                    self.context.actor_ref().system_ref(),
                ));
            }

            _ => {}
        }
    }

    fn unstash_all(&mut self) {
        // @TODO if there are tons of messages, this can stall other actors from making progress.
        //       it's important that ordering guarantees remain the same though, so punt this
        //       for now and document the limitation
        while let Some(msg) = self.stash.pop_front() {
            match msg {
                Envelope::Msg(msg) => {
                    self.receive(msg);
                }

                Envelope::SystemMsg(msg) => {
                    self.receive_system(msg);
                }
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

    fn tell_cancellable(&self, cancellable: Cancellable, msg: Msg, thunk: Option<Thunk>);
}

pub(in crate::actor) trait SystemActorRefInner: Downcast {
    fn clone_box(&self) -> Box<SystemActorRefInner + Send + Sync>;

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
        match self.state.swap(SpawnedActorExecutionState::Messaged) {
            SpawnedActorExecutionState::Idle(actor) => {
                actor.dispatcher.clone().execute(|| actor.run())
            }

            SpawnedActorExecutionState::Running => {}

            SpawnedActorExecutionState::Messaged => {}

            SpawnedActorExecutionState::Stopped => {}
        }
    }
}

impl<Msg> SystemActorRefInner for ActorRefCell<Msg>
where
    Msg: 'static + Send,
{
    fn clone_box(&self) -> Box<SystemActorRefInner + Send + Sync> {
        Box::new(ActorRefCell {
            id: self.id,
            state: self.state.clone(),
            mailbox_appender: self.mailbox_appender.clone(),
        })
    }

    fn stop(&self) {
        self.tell_system(SystemMsg::Stop(false));
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

    fn tell_cancellable(&self, cancellable: Cancellable, msg: Msg, thunk: Option<Thunk>) {
        self.mailbox_appender
            .append_cancellable(cancellable, Envelope::Msg(msg), thunk);
        self.messaged();
    }
}

struct StackedActorRefCell<NewMsg, Msg>
where
    NewMsg: Send,
    Msg: Send,
{
    converter: Arc<Fn(NewMsg) -> Msg + Send + Sync>,
    inner: Arc<Box<ActorRefInner<Msg> + Send + Sync>>,
}

impl<NewMsg, Msg> SystemActorRefInner for StackedActorRefCell<NewMsg, Msg>
where
    NewMsg: 'static + Send,
    Msg: 'static + Send,
{
    fn clone_box(&self) -> Box<SystemActorRefInner + Send + Sync> {
        Box::new(StackedActorRefCell {
            converter: self.converter.clone(),
            inner: self.inner.clone(),
        })
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

    fn tell_cancellable(&self, cancellable: Cancellable, msg: NewMsg, thunk: Option<Thunk>) {
        self.inner
            .tell_cancellable(cancellable, (self.converter)(msg), thunk);
    }
}

struct EmptyActorRefCell;

impl<Msg> ActorRefInner<Msg> for EmptyActorRefCell
where
    Msg: 'static + Send,
{
    fn tell(&self, _: Msg) {}

    fn tell_cancellable(&self, _: Cancellable, _: Msg, _: Option<Thunk>) {}
}

impl SystemActorRefInner for EmptyActorRefCell {
    fn clone_box(&self) -> Box<SystemActorRefInner + Send + Sync> {
        Box::new(EmptyActorRefCell)
    }

    fn stop(&self) {}

    fn id(&self) -> usize {
        0
    }

    fn tell_system(&self, _: SystemMsg) {}
}
