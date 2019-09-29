use crate::actor::{Actor, ActorContext, ActorRef, FailureReason};
use crate::stream::internal::{InternalStreamCtl, RunnableStream, Stage, StageMsg};
use crossbeam::atomic::AtomicCell;
use std::collections::VecDeque;
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

pub enum PortAction<A> {
    Cancel,
    Complete(Option<FailureReason>),
    Pull,
    Push(A),
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
pub trait Logic<In: Send, Out: Send>
where
    Self: Send + Sized,
    Self::Ctl: Send,
{
    type Ctl;

    fn name(&self) -> &'static str;

    /// Defines the buffer size for the stage that runs this logic. Elements
    /// are buffered to amortize the cost of passing elements over asynchronous
    /// boundaries.
    fn buffer_size(&self) -> Option<usize> {
        None
    }

    fn receive(
        &mut self,
        msg: LogicEvent<In, Self::Ctl>,
        ctx: &mut StreamContext<In, Out, Self::Ctl>,
    );
}

pub struct StreamContext<'a, In, Out, Ctl>
where
    In: 'static + Send,
    Out: 'static + Send,
    Ctl: 'static + Send,
{
    ctx: &'a mut ActorContext<StageMsg<In, Out, Ctl>>,
    actions: &'a mut VecDeque<Action<Out, Ctl>>,
}

impl<'a, In, Out, Ctl> StreamContext<'a, In, Out, Ctl>
where
    In: 'static + Send,
    Out: 'static + Send,
    Ctl: 'static + Send,
{
    #[inline(always)]
    pub(in crate::stream) fn new(
        ctx: &'a mut ActorContext<StageMsg<In, Out, Ctl>>,
        actions: &'a mut VecDeque<Action<Out, Ctl>>,
    ) -> Self {
        StreamContext { ctx, actions }
    }

    fn actor_ref(&self) -> ActorRef<Action<Out, Ctl>> {
        // @FIXME i'd like this to return a reference for API symmetry

        self.ctx
            .actor_ref()
            .convert(|action: Action<Out, Ctl>| StageMsg::Action(action))
    }

    fn tell(&mut self, action: Action<Out, Ctl>) {
        self.actions.push_back(action); // @TODO measure this vs messaging instead
    }

    fn tell_port<Y>(&mut self, handle: &PortRef<Y>, action: PortAction<Y>) {
        unimplemented!();
    }

    fn schedule_delivery<S: AsRef<str>>(&mut self, name: S, timeout: Duration, msg: Ctl) {
        self.ctx
            .schedule_delivery(name, timeout, StageMsg::Action(Action::Forward(msg)));
    }

    /// Spawns a port, which is a flow whose upstream and downstream are both
    /// this logic.
    ///
    /// An id and handle are returned, and these are used to control the spawned
    /// port.
    ///
    /// Port ids are always assigned in a monotonic fashion starting from 0.
    fn spawn_port<Y, Z, C: FnMut(LogicPortEvent<Y>) -> Ctl>(
        &mut self,
        flow: Flow<Y, Z>,
        convert: C,
    ) -> PortRef<Y> {
        unimplemented!()
    }
}

struct PortRef<T> {
    t: T,
}

impl<T> PortRef<T> {
    fn id(&self) -> usize {
        0
    }

    fn tell(&mut self, action: PortAction<T>) {}
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
