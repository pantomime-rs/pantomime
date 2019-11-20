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

    /// Defines whether a particular logic implementation is fusible or not.
    ///
    /// Most logic implementations are fusible, but some that make excessive
    /// use of timers or messaging (via StreamContext::actor_ref) may wish
    /// to return false here to improve performance.
    fn fusible(&self) -> bool {
        true
    }

    /// Handle the following event, returning an action to take. Implementations
    /// must respect the following rules, which are largely based on the work
    /// of Reactive Streams. These rules ensure that the dynamic push-pull
    /// nature of Pantomime Streams preserves backpressure, allowing stream
    /// processing to take place in a bounded buffer space.
    ///
    /// TODO: expand on rules
    ///
    /// * Action::Push may only be dispatched after receiving LogicEvent::Pulled
    /// * LogicEvent::Pushed may only be received after issuing Action::Pull
    ///
    /// Note that logic implementations are fusible by default. If any
    /// of the context methods are used (such as scheduling messages),
    /// many implementations may wish to override fusible to return false,
    /// as each message needs to be boxed and passed "step by step" through
    /// each stage.
    #[must_use]
    fn receive(
        &mut self,
        msg: LogicEvent<In, Self::Ctl>,
        ctx: &mut StreamContext<In, Out, Self::Ctl>,
    ) -> Action<Out, Self::Ctl>;
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
    Fused(
        &'a mut VecDeque<StreamContextAction<Out, Ctl>>,
        &'a StageRef<Ctl>,
    ),
}

pub struct StageRef<Ctl>
where
    Ctl: Send,
{
    actor_ref: ActorRef<Ctl>,
}

impl<Ctl> StageRef<Ctl>
where
    Ctl: 'static + Send,
{
    fn empty() -> Self {
        Self {
            actor_ref: ActorRef::empty(),
        }
    }

    pub fn convert<N, Convert: Fn(N) -> Ctl>(&self, converter: Convert) -> StageRef<N>
    where
        N: 'static + Send,
        Convert: 'static + Send + Sync,
    {
        StageRef {
            actor_ref: self.actor_ref.convert(converter),
        }
    }

    pub fn tell(&self, msg: Ctl) {
        self.actor_ref.tell(msg);
    }
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

impl<'a, 'c, In, Out, Ctl> StreamContext<'a, In, Out, Ctl>
where
    In: 'static + Send,
    Out: 'static + Send,
    Ctl: 'static + Send,
{
    fn schedule_delivery<S: AsRef<str>>(&mut self, name: S, timeout: Duration, msg: Ctl) {
        match self.ctx {
            StreamContextType::Fused(ref mut actions, _) => {
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

    fn stage_ref(&mut self) -> StageRef<Ctl> {
        match self.ctx {
            StreamContextType::Fused(_, ref stage_ref) => StageRef {
                actor_ref: stage_ref.actor_ref.clone(),
            },

            StreamContextType::Spawned(ref context) => StageRef {
                actor_ref: context
                    .actor_ref()
                    .convert(|msg| StageMsg::Action(Action::Forward(msg))),
            },
        }
    }

    fn tell(&mut self, action: Action<Out, Ctl>) {
        match self.ctx {
            StreamContextType::Fused(ref mut actions, _) => {
                actions.push_back(StreamContextAction::Action(action));
            }

            StreamContextType::Spawned(ref mut ctx) => {
                ctx.actor_ref().tell(StageMsg::Action(action));
            }
        }
    }
}

pub enum StreamCtl {
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
