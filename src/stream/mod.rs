use crate::actor::{Actor, ActorContext, ActorRef, FailureReason};
use crate::stream::internal::{InternalStreamCtl, RunnableStream, StageMsg};
use crossbeam::atomic::AtomicCell;
use std::sync::Arc;
use std::time::Duration;

pub mod flow;
pub mod sink;
pub mod source;

pub use crate::stream::flow::Flow;
pub use crate::stream::sink::Sink;
pub use crate::stream::source::Source;

mod internal;

#[cfg(test)]
mod tests;

pub enum Action<A, Msg> {
    Cancel,
    Complete(Option<FailureReason>),
    Pull,
    Push(A),
    Forward(Msg),
}

/// All stages are backed by a particular `Logic` that defines
/// the behavior of the stage.
///
/// To preserve the execution gurantees, logic implementations must
/// follow these rules:
///
/// * Only push a (single) value after being pulled.
///
/// It is rare that you'll need to write your own logic, but sometimes
/// performance or other considerations makes it necessary. Be sure to
/// look at the numerous stages provided out of the box before
/// continuing.
pub trait Logic<A, B, Msg>
where
    A: Send,
    B: Send,
    Msg: Send,
{
    fn name(&self) -> &'static str;

    /// Defines the buffer size for the stage that runs this logic. Elements
    /// are buffered to amortize the cost of passing elements over asynchronous
    /// boundaries.
    fn buffer_size(&self) -> Option<usize> {
        None
    }

    /// Indicates that downstream has requested an element.
    #[must_use]
    fn pulled(&mut self, ctx: &mut StreamContext<A, B, Msg>) -> Option<Action<B, Msg>>;

    /// Indicates that upstream has produced an element.
    #[must_use]
    fn pushed(&mut self, el: A, ctx: &mut StreamContext<A, B, Msg>) -> Option<Action<B, Msg>>;

    /// Indicates that this stage has been started.
    #[must_use]
    fn started(&mut self, ctx: &mut StreamContext<A, B, Msg>) -> Option<Action<B, Msg>> {
        None
    }

    /// Indicates that upstream has been stopped.
    #[must_use]
    fn stopped(&mut self, ctx: &mut StreamContext<A, B, Msg>) -> Option<Action<B, Msg>> {
        Some(Action::Complete(None))
    }

    /// Indicates that downstream has cancelled. This normally doesn't
    /// need to be overridden -- only do so if you know what you're
    /// doing.
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

pub struct StreamContext<'a, A, B, Msg>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    ctx: &'a mut ActorContext<StageMsg<A, B, Msg>>,
}

impl<'a, A, B, Msg> StreamContext<'a, A, B, Msg>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    pub fn actor_ref(&self) -> ActorRef<Action<B, Msg>> {
        // @FIXME i'd like this to return a reference for API symmetry

        self.ctx
            .actor_ref()
            .convert(|action: Action<B, Msg>| StageMsg::Action(action))
    }

    fn tell(&self, action: Action<B, Msg>) {
        self.ctx.actor_ref().tell(StageMsg::Action(action));
    }

    fn schedule_delivery<S: AsRef<str>>(&mut self, name: S, timeout: Duration, msg: Msg) {
        self.ctx
            .schedule_delivery(name, timeout, StageMsg::Action(Action::Forward(msg)));
    }

    fn spawn<AMsg, Ax: Actor<AMsg>>(&mut self, actor: Ax) -> ActorRef<AMsg>
    where
        AMsg: 'static + Send,
        Ax: 'static + Send,
    {
        self.ctx.spawn(actor)
    }
}

enum StreamCtl {
    Stop,
    Fail,
}

struct StreamComplete<Out>
where
    Out: 'static + Send,
{
    controller_ref: ActorRef<InternalStreamCtl<Out>>,
    state: Arc<AtomicCell<Option<Out>>>,
}

impl<Out> StreamComplete<Out>
where
    Out: 'static + Send,
{
    fn pipe_to(self, actor_ref: &ActorRef<Out>) {
        let actor_ref = actor_ref.clone();
    }
}

pub struct Stream<Out> {
    runnable_stream: Box<RunnableStream<Out> + Send>,
}

impl<Out> Stream<Out>
where
    Out: 'static + Send,
{
    pub(in crate::stream) fn run(self, context: &mut ActorContext<InternalStreamCtl<Out>>) {
        self.runnable_stream.run(context)
    }
}
