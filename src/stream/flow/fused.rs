use crate::stream::internal::{ContainedLogic, SpecialContextAction, StageMsg};
use crate::stream::{Action, Logic, LogicEvent, StreamContext, StreamContextType};
use std::any::Any;
use std::collections::VecDeque;
use std::marker::PhantomData;

pub(in crate::stream) enum FusedMsg {
    ForwardUp(Box<dyn Any + Send>),
    ForwardDown(Box<dyn Any + Send>),
}

enum FusedAction<B, C, UpCtl, DownCtl>
where
    B: Send,
    C: Send,
    UpCtl: Send,
    DownCtl: Send,
{
    UpstreamAction(Action<B, UpCtl>),
    DownstreamAction(Action<C, DownCtl>),
}

pub(in crate::stream) struct Fused<A, B, C>
where
    A: 'static + Send,
    B: 'static + Send,
    C: 'static + Send,
{
    up: Box<dyn ContainedLogic<A, B> + Send>,
    up_actions: VecDeque<Action<B, Box<Any + Send>>>,
    down: Box<dyn ContainedLogic<B, C> + Send>,
    down_actions: VecDeque<Action<C, Box<Any + Send>>>,
    phantom: PhantomData<(A, B, C)>,
}

impl<A, B, C> Fused<A, B, C>
where
    A: 'static + Send,
    B: 'static + Send,
    C: 'static + Send,
{
    pub(in crate::stream) fn new(
        upstream: Box<dyn ContainedLogic<A, B> + Send>,
        downstream: Box<dyn ContainedLogic<B, C> + Send>,
    ) -> Self {
        Fused {
            up: upstream,
            up_actions: VecDeque::new(),
            down: downstream,
            down_actions: VecDeque::new(),
            phantom: PhantomData,
        }
    }

    fn down_receive(
        &mut self,
        event: LogicEvent<B, Box<Any + Send>>,
        ctx: &mut StreamContext<A, C, FusedMsg>,
    ) -> Action<C, FusedMsg> {
        let mut down_ctx = StreamContext {
            ctx: StreamContextType::Fused(&mut self.down_actions),
        };

        let result = match self.down.receive(event, &mut down_ctx) {
            Action::Pull => self.up_receive(LogicEvent::Pulled, ctx),

            Action::Push(el) => Action::Push(el),

            Action::None => Action::None,

            Action::Forward(msg) => self.down_receive(LogicEvent::Forwarded(msg), ctx),

            Action::Cancel => self.up_receive(LogicEvent::Cancelled, ctx),

            Action::PushAndComplete(el, reason) => Action::PushAndComplete(el, reason),

            Action::Complete(reason) => Action::Complete(reason),
        };

        while let Some(a) = self.down_actions.pop_front() {
            let next_result = match a {
                Action::Pull => self.up_receive(LogicEvent::Pulled, ctx),

                Action::Push(el) => Action::Push(el),

                Action::None => Action::None,

                Action::Forward(msg) => self.down_receive(LogicEvent::Forwarded(msg), ctx),

                Action::Cancel => self.up_receive(LogicEvent::Cancelled, ctx),

                Action::PushAndComplete(el, reason) => Action::PushAndComplete(el, reason),

                Action::Complete(reason) => Action::Complete(reason),
            };

            ctx.tell(next_result);
        }

        return result;
    }

    fn up_receive(
        &mut self,
        event: LogicEvent<A, Box<Any + Send>>,
        ctx: &mut StreamContext<A, C, FusedMsg>,
    ) -> Action<C, FusedMsg> {
        let mut up_ctx = StreamContext {
            ctx: StreamContextType::Fused(&mut self.up_actions),
        };

        let result = match self.up.receive(event, &mut up_ctx) {
            Action::Pull => Action::Pull,

            Action::Push(el) => self.down_receive(LogicEvent::Pushed(el), ctx),

            Action::None => Action::None,

            Action::Forward(msg) => self.up_receive(LogicEvent::Forwarded(msg), ctx),

            Action::Cancel => Action::Cancel,

            Action::PushAndComplete(el, reason) => {
                // @TODO reason
                let result = self.down_receive(LogicEvent::Pushed(el), ctx);
                let follow_up = self.down_receive(LogicEvent::Stopped, ctx);

                ctx.tell(follow_up);

                result
            }

            Action::Complete(reason) => self.down_receive(LogicEvent::Stopped, ctx),
        };

        while let Some(a) = self.up_actions.pop_front() {
            let next_result = match a {
                Action::Pull => Action::Pull,

                Action::Push(el) => self.down_receive(LogicEvent::Pushed(el), ctx),

                Action::None => Action::None,

                Action::Forward(msg) => self.up_receive(LogicEvent::Forwarded(msg), ctx),

                Action::Cancel => Action::Cancel,

                Action::PushAndComplete(el, reason) => {
                    // @TODO reason
                    let result = self.down_receive(LogicEvent::Pushed(el), ctx);
                    let follow_up = self.down_receive(LogicEvent::Stopped, ctx);

                    ctx.tell(follow_up);

                    result
                }

                Action::Complete(reason) => self.down_receive(LogicEvent::Stopped, ctx),
            };

            ctx.tell(next_result);
        }

        return result;
    }
}

impl<A, B, C> Logic<A, C> for Fused<A, B, C>
where
    A: 'static + Send,
    B: 'static + Send,
    C: 'static + Send,
{
    type Ctl = FusedMsg;

    fn name(&self) -> &'static str {
        "Fused"
    }

    fn receive(
        &mut self,
        msg: LogicEvent<A, Self::Ctl>,
        ctx: &mut StreamContext<A, C, Self::Ctl>,
    ) -> Action<C, Self::Ctl> {
        match msg {
            LogicEvent::Pulled => self.down_receive(LogicEvent::Pulled, ctx),

            LogicEvent::Pushed(element) => self.up_receive(LogicEvent::Pushed(element), ctx),

            LogicEvent::Stopped => self.up_receive(LogicEvent::Stopped, ctx),

            LogicEvent::Cancelled => self.down_receive(LogicEvent::Cancelled, ctx),

            LogicEvent::Started => {
                // @TODO
                let result = self.up_receive(LogicEvent::Started, ctx);
                let follow_up = self.down_receive(LogicEvent::Started, ctx);

                ctx.tell(follow_up);

                result
            }

            LogicEvent::Forwarded(FusedMsg::ForwardUp(msg)) => {
                self.up_receive(LogicEvent::Forwarded(msg), ctx)
            }

            LogicEvent::Forwarded(FusedMsg::ForwardDown(msg)) => {
                self.down_receive(LogicEvent::Forwarded(msg), ctx)
            }
        }
    }
}
