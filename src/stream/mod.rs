use crate::actor::{ActorContext, ActorRef, FailureReason};
use crate::stream::internal::{InternalStreamCtl, RunnableStream, StageMsg};
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
        // @TODO
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
    calls: usize,
}

pub(in crate::stream) enum StreamContextAction<Out, Ctl> {
    Action(Action<Out, Ctl>),
    ScheduleDelivery(String, Duration, Ctl),
}

pub(in crate::stream) enum StreamContextType<'a, In, Out, Ctl>
where
    In: 'static + Send,
    Out: 'static + Send,
    Ctl: 'static + Send,
{
    Spawned(&'a mut ActorContext<StageMsg<In, Out, Ctl>>),
    Fused(&'a mut VecDeque<StreamContextAction<Out, Ctl>>),
}

impl<'a, 'c, In, Out, Ctl> StreamContext<'a, In, Out, Ctl>
where
    In: 'static + Send,
    Out: 'static + Send,
    Ctl: 'static + Send,
{
    fn schedule_delivery<S: AsRef<str>>(&mut self, name: S, timeout: Duration, msg: Ctl) {
        match self.ctx {
            StreamContextType::Fused(ref mut actions) => {
                actions.push_back(StreamContextAction::ScheduleDelivery(
                    name.as_ref().to_string(),
                    timeout,
                    msg,
                ));
            }

            StreamContextType::Spawned(ref mut ctx) => {
                ctx.schedule_delivery(name, timeout, StageMsg::Action(Action::Forward(msg)));
            }
        }
    }

    fn tell(&mut self, action: Action<Out, Ctl>) {
        match self.ctx {
            StreamContextType::Fused(ref mut actions) => {
                actions.push_back(StreamContextAction::Action(action));
            }

            StreamContextType::Spawned(ref mut ctx) => {
                ctx.actor_ref().tell(StageMsg::Action(action));
            }
        }
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
    runnable_stream: Box<dyn RunnableStream<Out> + Send>,
}

impl<Out> Stream<Out>
where
    Out: 'static + Send,
{
    pub(in crate::stream) fn run(self, context: &mut ActorContext<InternalStreamCtl<Out>>) {
        self.runnable_stream.run(context)
    }
}
