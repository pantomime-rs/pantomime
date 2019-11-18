use crate::actor::{Actor, ActorContext, ActorRef, ActorSpawnContext, FailureReason};
use crate::stream::internal::{
    DownstreamStageMsg, InternalStreamCtl, RunnableStream, Stage, StageMsg,
};
use crate::util::CuteRingBuffer;
use crossbeam::atomic::AtomicCell;
use std::any::Any;
use std::collections::VecDeque;
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
    PushAndComplete(A, Option<FailureReason>),
    Forward(Msg),
    None,
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
/// To preserve the execution guarantees, logic implementations must
/// follow these rules:
///
/// * Only push a (single) value after being pulled.
///
/// It is rare that you'll need to write your own logic, but sometimes
/// performance or other considerations makes it necessary. Be sure to
/// look at the numerous stages provided out of the box before
/// proceeding.
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

    fn fusible(&self) -> bool {
        true
    }

    #[must_use]
    fn receive(
        &mut self,
        msg: LogicEvent<In, Self::Ctl>,
        ctx: &mut StreamContext<In, Out, Self::Ctl>,
    ) -> Action<Out, Self::Ctl>;
}

pub struct StreamContext<'a, In, Out, Ctl>
where
    In: 'static + Send,
    Out: 'static + Send,
    Ctl: 'static + Send,
{
    ctx: StreamContextType<'a, In, Out, Ctl>,
    calls: usize
}

pub(in crate::stream) enum StreamContextType<'a, In, Out, Ctl>
where
    In: 'static + Send,
    Out: 'static + Send,
    Ctl: 'static + Send,
{
    Spawned(&'a mut ActorContext<StageMsg<In, Out, Ctl>>),
    Fused(&'a mut VecDeque<Action<Out, Ctl>>),
}

impl<'a, 'c, In, Out, Ctl> StreamContext<'a, In, Out, Ctl>
where
    In: 'static + Send,
    Out: 'static + Send,
    Ctl: 'static + Send,
{
    fn tell(&mut self, action: Action<Out, Ctl>) {
        match self.ctx {
            StreamContextType::Fused(ref mut actions) => {
                println!("pushed back");
                actions.push_back(action);
            }

            StreamContextType::Spawned(ref mut ctx) => {
                ctx.actor_ref().tell(StageMsg::Action(action));
            }
        }
    }

    fn tell_port<Y>(&mut self, handle: &mut PortRef<In, Out, Ctl, Y>, action: PortAction<Y>)
    where
        Y: Send,
    {
        unimplemented!();
    }

    fn schedule_delivery<S: AsRef<str>>(&mut self, name: S, timeout: Duration, msg: Ctl) {
        match self.ctx {
            StreamContextType::Fused(_) => {
                panic!();
            }

            StreamContextType::Spawned(ref mut ctx) => {
                ctx.schedule_delivery(name, timeout, StageMsg::Action(Action::Forward(msg)));
            }
        }
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
    ) -> PortRef<In, Out, Ctl, Y>
    where
        Y: Send,
    {
        PortRef {
            port: unimplemented!(),
        }
    }
}

trait SpawnedPort<In, Out, Ctl, A>
where
    A: 'static + Send,
    In: 'static + Send,
    Out: 'static + Send,
    Ctl: 'static + Send,
{
    fn tell(&mut self, action: PortAction<A>, ctx: &mut StreamContext<In, Out, Ctl>);
}

struct PortRef<In, Out, Ctl, A>
where
    A: 'static + Send,
    In: 'static + Send,
    Out: 'static + Send,
    Ctl: 'static + Send,
{
    port: Box<dyn SpawnedPort<In, Out, Ctl, A> + Send>,
}

impl<In, Out, Ctl, PortIn> PortRef<In, Out, Ctl, PortIn>
where
    In: Send,
    Out: Send,
    Ctl: Send,
    PortIn: Send,
{
    fn id(&self) -> usize {
        0
    }

    fn tell(&mut self, action: PortAction<PortIn>, ctx: &mut StreamContext<In, Out, Ctl>) {
        self.port.tell(action, ctx);
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
