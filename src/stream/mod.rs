use crate::actor::{Actor, ActorContext, ActorRef, FailureReason};
use crate::stream::internal::{InternalStreamCtl, RunnableStream, Stage, StageMsg};
use crossbeam::atomic::AtomicCell;
use std::marker::PhantomData;
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

pub enum LogicEvent<A, Msg> {
    Pulled,
    Pushed(A),
    Started,
    Stopped,
    Cancelled,
    Forwarded(Msg),
}

pub enum LogicPortEvent<A> {
    Pulled(usize),
    Pushed(usize, A),
    Started(usize),
    Stopped(usize),
    Cancelled(usize),
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
pub trait Logic
where
    Self: Send + Sized,
{
    type In: Send;
    type Out: Send;
    type Msg: Send;

    fn name(&self) -> &'static str {
        "todo"
    }

    /// Defines the buffer size for the stage that runs this logic. Elements
    /// are buffered to amortize the cost of passing elements over asynchronous
    /// boundaries.
    fn buffer_size(&self) -> Option<usize> {
        None
    }

    fn receive(
        &mut self,
        msg: LogicEvent<Self::In, Self::Msg>,
        ctx: &mut StreamContext<Self::In, Self::Out, Self::Msg, Self>,
    );
}

pub struct StreamContext<'a, A, B, Msg, L: Logic<In = A, Out = B, Msg = Msg>>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
    L: 'static + Send,
{
    // @TODO this needs to take a mut ref to Stage as well for perf reasons
    ctx: &'a mut ActorContext<StageMsg<A, B, Msg>>,
    p: PhantomData<L>,
}

impl<'a, A, B, Msg, L: Logic<In = A, Out = B, Msg = Msg>> StreamContext<'a, A, B, Msg, L>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
    L: 'static + Send,
{
    #[inline(always)]
    pub(in crate::stream) fn new(ctx: &'a mut ActorContext<StageMsg<A, B, Msg>>) -> Self {
        StreamContext {
            ctx,
            p: PhantomData,
        }
    }

    pub fn actor_ref(&self) -> ActorRef<Action<B, Msg>> {
        // @FIXME i'd like this to return a reference for API symmetry

        self.ctx
            .actor_ref()
            .convert(|action: Action<B, Msg>| StageMsg::Action(action))
    }

    fn tell(&mut self, action: Action<B, Msg>) {
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

    fn spawn_stage<Y, Z, C: FnMut(Y) -> Msg>(&mut self, flow: Flow<Y, Z>, convert: C) -> usize {
        0
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
