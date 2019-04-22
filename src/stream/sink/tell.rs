use crate::actor::ActorRef;
use crate::stream::detached::*;
use crate::stream::*;
use std::marker::PhantomData;

pub enum TellEvent<A>
where
    A: 'static + Send,
{
    Started(TellHandle<A>),
    Produced(A, TellHandle<A>),
    Completed,
    Failed(Error),
}

pub struct TellHandle<A>
where
    A: 'static + Send,
{
    actor_ref: ActorRef<AsyncAction<(), TellCommand>>,
    done: bool,
    phantom: PhantomData<A>,
}

pub enum TellCommand {
    Pull,
    Cancel,
}

impl<A> TellHandle<A>
where
    A: 'static + Send,
{
    fn new(actor_ref: ActorRef<AsyncAction<(), TellCommand>>) -> Self {
        Self {
            actor_ref,
            done: false,
            phantom: PhantomData,
        }
    }

    pub fn pull(mut self) {
        if !self.done {
            self.done = true;
            self.actor_ref.tell(AsyncAction::Forward(TellCommand::Pull));
        }
    }

    pub fn cancel(mut self) {
        if !self.done {
            self.done = true;
            self.actor_ref
                .tell(AsyncAction::Forward(TellCommand::Cancel));
        }
    }
}

impl<A> Drop for TellHandle<A>
where
    A: 'static + Send,
{
    fn drop(&mut self) {
        if !self.done {
            self.done = true;
            self.actor_ref
                .tell(AsyncAction::Forward(TellCommand::Cancel));
        }
    }
}

pub struct Tell<A>
where
    A: 'static + Send,
{
    dest_ref: ActorRef<TellEvent<A>>,
    logic_ref: ActorRef<AsyncAction<(), TellCommand>>,
}

impl<A> Tell<A>
where
    A: 'static + Send,
{
    pub fn new(actor_ref: ActorRef<TellEvent<A>>) -> Self {
        Self {
            dest_ref: actor_ref,
            logic_ref: ActorRef::empty(),
        }
    }
}

impl<A> DetachedLogic<A, (), TellCommand> for Tell<A>
where
    A: 'static + Send,
{
    fn attach(
        &mut self,
        ctx: &StreamContext,
        actor_ref: &ActorRef<AsyncAction<(), TellCommand>>,
    ) -> Option<AsyncAction<(), TellCommand>> {
        self.logic_ref = actor_ref.clone();

        None
    }

    fn forwarded(&mut self, msg: TellCommand) -> Option<AsyncAction<(), TellCommand>> {
        match msg {
            TellCommand::Pull => Some(AsyncAction::Pull),

            TellCommand::Cancel => Some(AsyncAction::Cancel),
        }
    }

    fn produced(&mut self, elem: A) -> Option<AsyncAction<(), TellCommand>> {
        self.dest_ref.tell(TellEvent::Produced(
            elem,
            TellHandle::new(self.logic_ref.clone()),
        ));

        None
    }

    fn pulled(&mut self) -> Option<AsyncAction<(), TellCommand>> {
        // nothing to do as there is no downstream

        None
    }

    fn completed(&mut self) -> Option<AsyncAction<(), TellCommand>> {
        self.dest_ref.tell(TellEvent::Completed);

        None
    }

    fn failed(&mut self, error: Error) -> Option<AsyncAction<(), TellCommand>> {
        self.dest_ref.tell(TellEvent::Failed(error));

        None
    }
}
