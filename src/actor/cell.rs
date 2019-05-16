use self::actor_watcher::ActorWatcherMessage;
use super::*;
use crate::mailbox::*;
use crossbeam::atomic::AtomicCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

#[derive(Clone, PartialEq)]
enum ActorCellState {
    Spawned,
    Active,
    DrainingChildren,
    Draining,
    Stopping { failure: bool },
    Stopped,
    Failed,
}

pub(in crate::actor) enum ActorCellMessage<M> {
    Message(M),
    CustomMessage,
    Done,
}

pub(in crate::actor) struct ActorCellWithMessage<M: 'static + Send> {
    actor_cell: Arc<ActorCell<M>>,
    msg: ActorCellMessage<M>,
}

impl<M: 'static + Send> ActorCellWithMessage<M> {
    pub(in crate::actor) fn new(actor_cell: Arc<ActorCell<M>>, msg: ActorCellMessage<M>) -> Self {
        Self { actor_cell, msg }
    }
}

impl<M: 'static + Send> ActorWithMessage for ActorCellWithMessage<M> {
    fn apply(self: Box<Self>) {
        // note that in practice, this is always locked from
        // the thread that holds the shard lock, so there is
        // never any contention on this mutex
        //
        // also note that the shard mutex is only acquired when
        // an AtomicBool is flipped, so contention on it is
        // minimal

        let mut contents = self.actor_cell.contents.swap(None).expect("pantomime bug: actor_cell#contents missing");

        match self.msg {
            ActorCellMessage::Message(msg) => {
                contents.receive(msg);
            }

            ActorCellMessage::CustomMessage => {
                // @TODO draining entire mailbox, what about fairness?

                while let Some(msg) = contents.custom_mailbox.as_mut().and_then(|m| m.retrieve()) {
                    contents.receive(msg);
                }
            }

            ActorCellMessage::Done => {
                contents.receive_done();
            }
        }

        match contents.state {
            ActorCellState::Stopped => {
                contents.release();
            }

            ActorCellState::Failed => {
                contents.release();
            }

            _ => {}
        }

        self.actor_cell.contents.swap(Some(contents));
    }
}

pub(in crate::actor) struct ActorCellWithSystemMessage<M: 'static + Send> {
    actor_cell: Arc<ActorCell<M>>,
    msg: Box<SystemMsg>,
}

pub(in crate::actor) struct ActorCellContents<M: 'static + Send> {
    actor: Box<dyn Actor<M> + 'static + Send>,
    pub(in crate::actor) context: ActorContext<M>,
    innards: Box<Option<Arc<ActorCell<M>>>>,
    state: ActorCellState,
    stash: Vec<StashedMsg<M>>,
    done: bool,
    custom_mailbox: Option<Mailbox<M>>,
}

enum StashedMsg<M: 'static + Send> {
    Msg(M),
    SystemMsg(SystemMsg),
}

impl<M: 'static + Send> ActorCellContents<M> {
    fn new<A: Actor<M> + 'static + Send>(
        actor: A,
        context: ActorContext<M>,
        custom_mailbox: Option<Mailbox<M>>,
    ) -> Self {
        Self {
            actor: Box::new(actor),
            context,
            innards: Box::new(None),
            state: ActorCellState::Spawned,
            stash: Vec::new(),
            done: false,
            custom_mailbox,
        }
    }

    fn receive_done(&mut self) {
        self.done = true;

        self.received();
    }

    pub(in crate::actor) fn initialize(&mut self, actor_ref: ActorRef<M>) {
        self.context.actor_ref = Some(actor_ref);

        if let Some(ref watcher_ref) = self.context.actor_ref().system_context().watcher_ref() {
            let actor_ref = self.context.actor_ref();

            watcher_ref.tell(ActorWatcherMessage::Started(
                actor_ref.id(),
                actor_ref.system_ref(),
            ));
        } else {
            // these are internal actors, so they cannot be watched
            self.state = ActorCellState::Active;
        }
    }

    fn receive(&mut self, msg: M) {
        match self.state {
            ActorCellState::Active
            | ActorCellState::DrainingChildren
            | ActorCellState::Draining => {
                if let Err(_e) = catch_unwind(AssertUnwindSafe(|| {
                    self.actor.receive(msg, &mut self.context)
                })) {
                    self.receive_system(SystemMsg::Stop { failure: true });
                }
            }

            ActorCellState::Spawned => {
                self.stash.push(StashedMsg::Msg(msg));
            }

            ActorCellState::Stopping { failure: _ }
            | ActorCellState::Stopped
            | ActorCellState::Failed => {
                // @TODO dead letters
            }
        }

        self.received();
    }

    fn release(&mut self) {
        *self.innards = None;
    }

    pub(in crate::actor) fn store(&mut self, cell: Arc<ActorCell<M>>) {
        *self.innards = Some(cell);
    }

    fn unstash_all(&mut self) {
        while let Some(msg) = self.stash.pop() {
            match msg {
                StashedMsg::Msg(m) => {
                    self.context.actor_ref().tell(m);
                }

                StashedMsg::SystemMsg(m) => {
                    self.context.actor_ref().tell_system(m);
                }
            }
        }
    }

    fn receive_system(&mut self, msg: SystemMsg) {
        match (&self.state, msg) {
            (ActorCellState::Spawned, SystemMsg::Signaled(Signal::Started)) => {
                if let Err(_e) = catch_unwind(AssertUnwindSafe(|| {
                    self.actor
                        .receive_signal(Signal::Started, &mut self.context)
                })) {
                    self.receive_system(SystemMsg::Stop { failure: true });
                }

                self.unstash_all();

                self.transition(ActorCellState::Active);
            }

            (ActorCellState::Spawned, other) => {
                // all other messages are irrelevant right now, so put
                // them back into the mailbox until we get our start
                // signal (which comes from the watcher)
                //
                // we don't need to reschedule ourselves for execution
                // as the arrival of the start signal will do that
                self.stash.push(StashedMsg::SystemMsg(other));
            }

            (ActorCellState::Active, SystemMsg::Drain) => {
                // Draining is similar to a PoisonPill in Akka, but child-aware.
                // This starts by draining all children, whereby once all
                // children have stopped, they stop accepting new messages. Once
                // their mailbox is empty, then they stop.

                if self.context.children.is_empty() {
                    self.context.actor_ref().tell_done();
                    self.transition(ActorCellState::Draining);
                } else {
                    self.transition(ActorCellState::DrainingChildren);

                    for (_, actor_ref) in self.context.children.iter() {
                        actor_ref.tell_system(SystemMsg::Drain);
                    }
                }
            }

            (ActorCellState::Active, SystemMsg::Stop { failure }) => {
                if self.context.children.is_empty() {
                    self.transition(if failure {
                        ActorCellState::Failed
                    } else {
                        ActorCellState::Stopped
                    });

                    if let Some(ref innards) = *self.innards {
                        innards
                            .parent_ref
                            .tell_system(SystemMsg::ChildStopped(self.context.actor_ref().id()));
                    }
                } else {
                    self.transition(ActorCellState::Stopping { failure });

                    for (_child_id, actor_ref) in self.context.children.iter() {
                        // @TODO think about whether children should be failed
                        actor_ref.tell_system(SystemMsg::Stop { failure: false });
                    }
                }
            }

            (ActorCellState::DrainingChildren, SystemMsg::ChildStopped(id)) => {
                self.context.children.remove(&id);
                self.context.children.shrink_to_fit();

                if self.context.children.is_empty() {
                    self.context.actor_ref().tell_done();
                    self.transition(ActorCellState::Draining);
                }
            }

            (ActorCellState::Stopping { failure }, SystemMsg::ChildStopped(id)) => {
                self.context.children.remove(&id);
                self.context.children.shrink_to_fit();

                if self.context.children.is_empty() {
                    self.transition(if *failure {
                        ActorCellState::Failed
                    } else {
                        ActorCellState::Stopped
                    });

                    if let Some(ref innards) = *self.innards {
                        innards
                            .parent_ref
                            .tell_system(SystemMsg::ChildStopped(self.context.actor_ref().id()));
                    } else {
                        // @TODO dead letters
                    }
                }
            }

            (ActorCellState::Failed, _) => {}

            (ActorCellState::Stopped, _) => {}

            (_, SystemMsg::Signaled(signal)) => {
                // @TODO do something with e
                if let Err(_e) = catch_unwind(AssertUnwindSafe(|| {
                    self.actor.receive_signal(signal, &mut self.context)
                })) {
                    self.receive_system(SystemMsg::Stop { failure: true });
                }
            }

            (ActorCellState::Stopping { failure: false }, SystemMsg::Stop { failure: true }) => {
                // We've failed while we were stopping, which means that a signal
                // handler paniced. Ensure that we flag ourselves as failed.
                self.transition(ActorCellState::Stopping { failure: true });
            }

            (ActorCellState::DrainingChildren, SystemMsg::Stop { failure: true }) => {
                // Similarly, we're currently processing our mailbox, but
                // we've failed. Since we cannot process any more messages due to
                // the failure, we'll transition to stopping and wait for our
                // children to stop.
                self.transition(ActorCellState::Stopping { failure: true });
            }

            (ActorCellState::Draining, SystemMsg::Stop { failure: true }) => {
                // Our children are stopped, and we're processing what's left in
                // the mailbox. We've failed, so we cannot continue. Simply
                // transition to Failed given there are no children.

                self.transition(ActorCellState::Failed);
            }

            (_, SystemMsg::Stop { failure: _ }) => {
                // @TODO think about what it means to have received this
                // if we're currently Stopped (ie not Failed)
            }

            (_, SystemMsg::Drain) => {}

            (_, SystemMsg::ChildStopped(child_id)) => {
                self.context.children.remove(&child_id);
                self.context.children.shrink_to_fit();

                // @TODO log this? shouldnt happe
            }
        }

        self.received();
    }

    fn received(&mut self) {
        if self.state == ActorCellState::Draining && self.done {
            self.transition(ActorCellState::Stopped);

            if let Some(ref innards) = *self.innards {
                innards
                    .parent_ref
                    .tell_system(SystemMsg::ChildStopped(self.context.actor_ref().id()));
            } else {
                // @TODO dead letters
            }
        }
    }

    fn tell_watcher(&self, message: ActorWatcherMessage) {
        if let Some(watcher_ref) = self.context.actor_ref().system_context().watcher_ref() {
            watcher_ref.tell(message);
        } else {
            // @TODO
        }
    }

    fn transition(&mut self, next: ActorCellState) {
        self.state = next;

        match self.state {
            ActorCellState::Stopped => {
                if let Err(_e) = catch_unwind(AssertUnwindSafe(|| {
                    self.actor
                        .receive_signal(Signal::Stopped, &mut self.context)
                })) {
                    // @TODO _e
                    self.transition(ActorCellState::Failed);
                } else {
                    self.tell_watcher(ActorWatcherMessage::Stopped(
                        self.context.actor_ref().system_ref(),
                    ));
                }
            }

            ActorCellState::Failed => {
                if let Err(_e) = catch_unwind(AssertUnwindSafe(|| {
                    self.actor.receive_signal(Signal::Failed, &mut self.context)
                })) {
                    // @TODO _e ? noting to do here really since we're already failed, it's like
                    // swallowing an exception
                }

                self.tell_watcher(ActorWatcherMessage::Failed(
                    self.context.actor_ref().system_ref(),
                ));
            }

            _ => {}
        }
    }
}

pub(in crate::actor) struct ActorCell<M: 'static + Send> {
    pub contents: AtomicCell<Option<ActorCellContents<M>>>,
    pub parent_ref: SystemActorRef,
}

impl<M: 'static + Send> ActorCell<M> {
    pub(in crate::actor) fn new<A: Actor<M> + 'static + Send>(
        actor: A,
        parent_ref: SystemActorRef,
        context: ActorContext<M>,
        custom_mailbox: Option<Mailbox<M>>,
    ) -> Self {
        Self {
            contents: AtomicCell::new(Some(ActorCellContents::new(actor, context, custom_mailbox))),

            parent_ref,
        }
    }
}

impl<M: 'static + Send> ActorCellWithSystemMessage<M> {
    pub(in crate::actor) fn new(actor_cell: Arc<ActorCell<M>>, msg: Box<SystemMsg>) -> Self {
        Self { actor_cell, msg }
    }
}

impl<M: 'static + Send> ActorWithSystemMessage for ActorCellWithSystemMessage<M> {
    fn apply(self: Box<Self>) {
        let mut contents = self.actor_cell.contents.swap(None).expect("pantomime bug: actor_cell#contents missing");

        contents.receive_system(*self.msg);

        match contents.state {
            ActorCellState::Stopped => {
                contents.release();
            }

            ActorCellState::Failed => {
                contents.release();
            }

            _ => {}
        }

        self.actor_cell.contents.swap(Some(contents));
    }
}
