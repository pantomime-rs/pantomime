use crate::actor::{ActorRef, FailureReason};
use crate::stream::{Action, Logic, Source, StreamContext};
use crate::stream::internal::{DownstreamStageMsg, Producer, UpstreamStageMsg};
use std::marker::PhantomData;

pub struct Restart<A, F: FnMut() -> Source<A>> where A: 'static + Send {
    restart_fn: F,
    actor_ref: Vec<ActorRef<DownstreamStageMsg<()>>>,
}

impl<A, F: FnMut() -> Source<A>> Logic<(), A, ()> for Restart<A, F>
where
    A: 'static + Send,
{
    fn name(&self) -> &'static str {
        "Restart"
    }

    fn started(&mut self, ctx: &mut StreamContext<(), A, ()>) -> Option<Action<A, ()>> {
        None
    }

    fn pulled(&mut self, ctx: &mut StreamContext<(), A, ()>) -> Option<Action<A, ()>> {
        None
    }

    fn pushed(&mut self, el: (), ctx: &mut StreamContext<(), A, ()>) -> Option<Action<A, ()>> {
        None
    }

    fn stopped(&mut self, ctx: &mut StreamContext<(), A, ()>) -> Option<Action<A, ()>> {
        None
    }
}


pub enum MultiAction<A, Msg> {
    Cancel(usize),
    Complete(Option<FailureReason>),
    Pull(usize),
    Push(A),
    Forward(Msg),
}

trait MultiLogic<A, B, Msg>
where
    A: Send,
    B: Send,
    Msg: Send,
{
    fn name(&self) -> &'static str;

    fn buffer_size(&self) -> Option<usize> {
        None
    }

    #[must_use]
    fn pulled(&mut self, ctx: &mut StreamContext<A, B, Msg>) -> Option<Action<B, Msg>>;

    #[must_use]
    fn pushed(&mut self, id: usize, el: A, ctx: &mut StreamContext<A, B, Msg>) -> Option<Action<B, Msg>>;

    #[must_use]
    fn started(&mut self, ctx: &mut StreamContext<A, B, Msg>) -> Option<Action<B, Msg>> {
        None
    }

    #[must_use]
    fn stopped(&mut self, id: usize, ctx: &mut StreamContext<A, B, Msg>) -> Option<Action<B, Msg>> {
        Some(Action::Complete(None))
    }

    #[must_use]
    fn cancelled(&mut self, ctx: &mut StreamContext<A, B, Msg>) -> Option<Action<B, Msg>> {
        Some(Action::Complete(None))
    }

    #[must_use]
    fn forwarded(
        &mut self,
        msg: Msg,
        ctx: &mut StreamContext<A, B, Msg>,
    ) -> Option<Action<B, Msg>> {
        None
    }
}



