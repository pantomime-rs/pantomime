use crate::stream::internal::ContainedLogic;
use crate::stream::{
    Action, Logic, LogicEvent, StageRef, StreamContext, StreamContextAction, StreamContextType,
};
use std::any::Any;
use std::collections::VecDeque;
use std::marker::PhantomData;

const EXECUTION_STATE_ACTIVE: u8 = 0;
const EXECUTION_STATE_UPSTREAM_STOPPED: u8 = 1;
const EXECUTION_STATE_UPSTREAM_AND_DOWNSTREAM_STOPPED: u8 = 2;
const MAX_CALLS: usize = 10;

pub(in crate::stream) enum FusedMsg<A, B> {
    ForwardUp(Box<dyn Any + Send>),
    ForwardDown(Box<dyn Any + Send>),
    DownReceive(LogicEvent<B, Box<dyn Any + Send>>),
    UpReceive(LogicEvent<A, Box<dyn Any + Send>>),
}

pub(in crate::stream) struct Fused<A, B, C>
where
    A: 'static + Send,
    B: 'static + Send,
    C: 'static + Send,
{
    execution_state: u8,
    up: Box<dyn ContainedLogic<A, B> + Send>,
    up_actions: VecDeque<StreamContextAction<B, Box<dyn Any + Send>>>,
    up_ref: StageRef<Box<dyn Any + Send>>,
    down: Box<dyn ContainedLogic<B, C> + Send>,
    down_actions: VecDeque<StreamContextAction<C, Box<dyn Any + Send>>>,
    down_ref: StageRef<Box<dyn Any + Send>>,
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
            execution_state: EXECUTION_STATE_ACTIVE,
            up: upstream,
            up_actions: VecDeque::new(),
            up_ref: StageRef::empty(),
            down: downstream,
            down_actions: VecDeque::new(),
            down_ref: StageRef::empty(),
            phantom: PhantomData,
        }
    }

    fn down_receive(
        &mut self,
        event: LogicEvent<B, Box<dyn Any + Send>>,
        ctx: &mut StreamContext<A, C, FusedMsg<A, B>>,
    ) -> Action<C, FusedMsg<A, B>> {
        if let LogicEvent::Started = event {
            self.down_ref = ctx.stage_ref().convert(FusedMsg::ForwardDown);
        }

        if self.execution_state == EXECUTION_STATE_UPSTREAM_AND_DOWNSTREAM_STOPPED {
            return Action::None;
        }

        ctx.calls += 1;

        if ctx.calls >= MAX_CALLS {
            return Action::Forward(FusedMsg::DownReceive(event));
        }

        let mut down_ctx = StreamContext {
            ctx: StreamContextType::Fused(&mut self.down_actions, &self.down_ref),
            calls: ctx.calls,
        };

        let result = match self.down.receive(event, &mut down_ctx) {
            Action::Pull => self.up_receive(LogicEvent::Pulled, ctx),

            Action::Push(el) => Action::Push(el),

            Action::None => Action::None,

            Action::Forward(msg) => self.down_receive(LogicEvent::Forwarded(msg), ctx),

            Action::Cancel => self.up_receive(LogicEvent::Cancelled, ctx),

            Action::PushAndStop(el, reason) => Action::PushAndStop(el, reason),

            Action::Stop(reason) => Action::Stop(reason),
        };

        while let Some(a) = self.down_actions.pop_front() {
            let next_result = match a {
                StreamContextAction::Action(Action::Pull) => {
                    self.up_receive(LogicEvent::Pulled, ctx)
                }

                StreamContextAction::Action(Action::Push(el)) => Action::Push(el),

                StreamContextAction::Action(Action::None) => Action::None,

                StreamContextAction::Action(Action::Forward(msg)) => {
                    self.down_receive(LogicEvent::Forwarded(msg), ctx)
                }

                StreamContextAction::Action(Action::Cancel) => {
                    self.up_receive(LogicEvent::Cancelled, ctx)
                }

                StreamContextAction::Action(Action::PushAndStop(el, reason)) => {
                    Action::PushAndStop(el, reason)
                }

                StreamContextAction::Action(Action::Stop(reason)) => Action::Stop(reason),

                StreamContextAction::ScheduleDelivery(name, duration, msg) => {
                    ctx.schedule_delivery(name, duration, FusedMsg::ForwardDown(msg)); // @TODO namespace name

                    Action::None
                }
            };

            if let Action::None = next_result {
            } else {
                ctx.tell(next_result);
            }
        }

        match result {
            Action::Stop(_) | Action::PushAndStop(_, _) => {
                self.execution_state = EXECUTION_STATE_UPSTREAM_AND_DOWNSTREAM_STOPPED;
            }

            _ => {}
        }

        result
    }

    fn up_receive(
        &mut self,
        event: LogicEvent<A, Box<dyn Any + Send>>,
        ctx: &mut StreamContext<A, C, FusedMsg<A, B>>,
    ) -> Action<C, FusedMsg<A, B>> {
        if let LogicEvent::Started = event {
            self.up_ref = ctx.stage_ref().convert(FusedMsg::ForwardUp);
        }

        if self.execution_state > EXECUTION_STATE_ACTIVE {
            return Action::None;
        }

        ctx.calls += 1;

        if ctx.calls >= MAX_CALLS {
            return Action::Forward(FusedMsg::UpReceive(event));
        }

        let mut up_ctx = StreamContext {
            ctx: StreamContextType::Fused(&mut self.up_actions, &self.up_ref),
            calls: ctx.calls,
        };

        let result = match self.up.receive(event, &mut up_ctx) {
            Action::Pull => Action::Pull,

            Action::Push(el) => self.down_receive(LogicEvent::Pushed(el), ctx),

            Action::None => Action::None,

            Action::Forward(msg) => self.up_receive(LogicEvent::Forwarded(msg), ctx),

            Action::Cancel => Action::Cancel,

            Action::PushAndStop(el, _reason) => {
                // @TODO reason
                let result = self.down_receive(LogicEvent::Pushed(el), ctx);
                let follow_up = self.down_receive(LogicEvent::Stopped, ctx);

                ctx.tell(follow_up);

                result
            }

            // @TODO reason
            Action::Stop(_reason) => self.down_receive(LogicEvent::Stopped, ctx),
        };

        while let Some(a) = self.up_actions.pop_front() {
            let next_result = match a {
                StreamContextAction::Action(Action::Pull) => Action::Pull,

                StreamContextAction::Action(Action::Push(el)) => {
                    self.down_receive(LogicEvent::Pushed(el), ctx)
                }

                StreamContextAction::Action(Action::None) => Action::None,

                StreamContextAction::Action(Action::Forward(msg)) => {
                    self.up_receive(LogicEvent::Forwarded(msg), ctx)
                }

                StreamContextAction::Action(Action::Cancel) => Action::Cancel,

                StreamContextAction::Action(Action::PushAndStop(el, _reason)) => {
                    // @TODO reason
                    let result = self.down_receive(LogicEvent::Pushed(el), ctx);
                    let follow_up = self.down_receive(LogicEvent::Stopped, ctx);

                    ctx.tell(follow_up);

                    result
                }

                // @TODO reason
                StreamContextAction::Action(Action::Stop(_reason)) => {
                    self.down_receive(LogicEvent::Stopped, ctx)
                }

                StreamContextAction::ScheduleDelivery(name, duration, msg) => {
                    ctx.schedule_delivery(name, duration, FusedMsg::ForwardUp(msg)); // @TODO namespace name

                    Action::None
                }
            };

            if let Action::None = next_result {
            } else {
                ctx.tell(next_result);
            }
        }

        match result {
            Action::Stop(_) | Action::PushAndStop(_, _) => {
                self.execution_state = EXECUTION_STATE_UPSTREAM_STOPPED;
            }

            _ => {}
        }

        result
    }
}

impl<A, B, C> Logic<A, C> for Fused<A, B, C>
where
    A: 'static + Send,
    B: 'static + Send,
    C: 'static + Send,
{
    type Ctl = FusedMsg<A, B>;

    fn name(&self) -> &'static str {
        "Fused"
    }

    fn receive(
        &mut self,
        msg: LogicEvent<A, Self::Ctl>,
        ctx: &mut StreamContext<A, C, Self::Ctl>,
    ) -> Action<C, Self::Ctl> {
        ctx.calls += 1;

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

            LogicEvent::Forwarded(FusedMsg::DownReceive(event)) => self.down_receive(event, ctx),

            LogicEvent::Forwarded(FusedMsg::UpReceive(event)) => self.up_receive(event, ctx),

            LogicEvent::Forwarded(FusedMsg::ForwardUp(msg)) => {
                self.up_receive(LogicEvent::Forwarded(msg), ctx)
            }

            LogicEvent::Forwarded(FusedMsg::ForwardDown(msg)) => {
                self.down_receive(LogicEvent::Forwarded(msg), ctx)
            }
        }
    }
}
