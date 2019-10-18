use crate::actor::{Actor, ActorContext, ActorRef, ActorSpawnContext, FailureReason};
use crate::stream::internal::{Downstream, DownstreamStageMsg, InternalStreamCtl, RunnableStream, Stage, StageContext, StageMsg, StageRef};
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
    Forwarded(usize, Box<dyn Any + Send>),
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

pub struct StreamContext<'a, 'b, 'c, In, Out, Ctl>
where
    In: 'static + Send,
    Out: 'static + Send,
    Ctl: 'static + Send,
{
    ctx: &'a mut StageContext<'c, StageMsg<In, Out, Ctl>>,
    stash: &'b mut CuteRingBuffer<StageMsg<In, Out, Ctl>>,
}

impl<'a, 'b, 'c, In, Out, Ctl> StreamContext<'a, 'b, 'c, In, Out, Ctl>
where
    In: 'static + Send,
    Out: 'static + Send,
    Ctl: 'static + Send,
{
    fn tell(&mut self, action: Action<Out, Ctl>) {
        self.stash.push(StageMsg::Action(action));
    }

    fn tell_port<Y>(&mut self, handle: &mut PortRef<In, Out, Ctl, Y>, action: PortAction<Y>) where Y: Send {
        handle.tell(action, self);
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
    ) -> PortRef<In, Out, Ctl, Y> where Y: Send {

        PortRef {
            port: unimplemented!()
        }
    }
}

trait SpawnedPort<In, Out, Ctl, A> where A: 'static + Send, In: 'static + Send, Out: 'static + Send, Ctl: 'static + Send {
    fn tell(&mut self, action: PortAction<A>, ctx: &mut StreamContext<In, Out, Ctl>);
}

struct SpawnedPortImpl<UpIn, UpOut, UpCtl, In, Out, Ctl, L: Logic<In, Out, Ctl = Ctl>, C: FnMut(LogicPortEvent<Out>) -> UpCtl> where UpIn: 'static + Send, UpOut: 'static + Send, UpCtl: 'static + Send, In: 'static + Send, Out: 'static + Send, L: 'static + Send, C: 'static + Send, Ctl: 'static + Send {
    id: usize,

    logic: L,
    convert: C,
    phantom: PhantomData<(UpCtl, Out, Ctl, UpIn, UpOut, In)>,
    midstream_context: internal::SpecialContext<StageMsg<In, Out, Ctl>>,
    actions: VecDeque<Action<Out, Ctl>>
}

impl<UpIn, UpOut, UpCtl, In, Out, Ctl, L: Logic<In, Out, Ctl = Ctl>, C: FnMut(LogicPortEvent<Out>) -> UpCtl> SpawnedPortImpl<UpIn, UpOut, UpCtl, In, Out, Ctl, L, C> where UpIn: 'static + Send, UpOut: 'static + Send, UpCtl: 'static + Send, In: 'static + Send, Out: 'static + Send, L: 'static + Send, C: 'static + Send, Ctl: 'static + Send {

    fn process_actions(&mut self, fused_level: &mut usize, ctx: &mut StreamContext<UpIn, UpOut, UpCtl>) {
        loop {
            let action = self.actions.pop_front();

            let mut stage_context = StageContext {
                inner: internal::InnerStageContext::Special(&mut self.midstream_context)
            };

            let mut context = StreamContext {
                ctx: &mut stage_context,
                stash: unimplemented!()
            };

            match action {
                Some(Action::Pull) => {
                    ctx.tell(Action::Forward((self.convert)(LogicPortEvent::Pulled(self.id))));
                }

                Some(Action::Push(el)) => {
                    ctx.tell(Action::Forward((self.convert)(LogicPortEvent::Pushed(self.id, el))));
                }

                Some(Action::Cancel) => {
                    ctx.tell(Action::Forward((self.convert)(LogicPortEvent::Cancelled(self.id))));
                }

                Some(Action::Forward(msg)) => {
                    ctx.tell(Action::Forward((self.convert)(LogicPortEvent::Forwarded(self.id, Box::new(msg)))));

                    // @TODO what about below instead? avoids box
                    //self.logic.receive(LogicEvent::Forwarded(msg), &mut context);
                }

                Some(Action::Complete(reason)) => {
                    // @TODO reason
                    ctx.tell(Action::Forward((self.convert)(LogicPortEvent::Stopped(self.id))));
                }

                None => {
                    break;
                }
            }
        }
    }
}

impl<UpIn, UpCtl, UpOut, In, Out, Ctl, L: Logic<In, Out, Ctl = Ctl>, C: FnMut(LogicPortEvent<Out>) -> UpCtl> SpawnedPort<UpIn, UpOut, UpCtl, In> for SpawnedPortImpl<UpIn, UpOut, UpCtl, In, Out, Ctl, L, C> where In: 'static + Send, Out: 'static + Send, Ctl: 'static + Send, L: 'static + Send, UpCtl: 'static + Send, UpIn: 'static + Send, UpOut: 'static + Send, C: 'static + Send {
    fn tell(&mut self, action: PortAction<In>, ctx: &mut StreamContext<UpIn, UpOut, UpCtl>) {
        let mut fused_level = 0; // @TODO

        let mut stage_context = StageContext {
            inner: internal::InnerStageContext::Special(&mut self.midstream_context)
        };

        let mut context = StreamContext {
            ctx: &mut stage_context,
            stash: unimplemented!()
        };

        match action {
            PortAction::Pull => {
                self.logic.receive(LogicEvent::Pulled, &mut context);
            }

            PortAction::Push(el) => {
                self.logic.receive(LogicEvent::Pushed(el), &mut context);
            }

            PortAction::Cancel => {
                self.logic.receive(LogicEvent::Cancelled, &mut context);
            }

            PortAction::Complete(reason) => {
                self.logic.receive(LogicEvent::Stopped, &mut context);
            }
        }

        fused_level += 1;

        self.process_actions(&mut fused_level, ctx);

        fused_level -= 1;
    }
}

struct PortRef<In, Out, Ctl, A> where A: 'static + Send, In: 'static + Send, Out: 'static + Send, Ctl: 'static + Send {
    port: Box<dyn SpawnedPort<In, Out, Ctl, A> + Send>
}

impl<In, Out, Ctl, PortIn> PortRef<In, Out, Ctl, PortIn> where In: Send, Out: Send, Ctl: Send, PortIn: Send {
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
    pub fn fuse(self) -> Self {
        Self {
            runnable_stream: self.runnable_stream.fuse()
        }
    }

    pub(in crate::stream) fn run(self, context: &mut ActorContext<InternalStreamCtl<Out>>) {
        self.runnable_stream.run(context)
    }
}
