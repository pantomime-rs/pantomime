use crate::stream::{Action, Logic, LogicEvent, StreamContext};
use std::marker::PhantomData;
use crate::stream::internal::LogicContainerFacade;
use std::any::Any;

pub(in crate::stream) enum FusedMsg<UpCtl, DownCtl> {
    ForwardUp(UpCtl),
    ForwardDown(DownCtl)
}

enum FusedAction<B, C, UpCtl, DownCtl> where B: Send, C: Send, UpCtl: Send, DownCtl: Send {
    UpstreamAction(Action<B, UpCtl>),
    DownstreamAction(Action<C, DownCtl>)
}

trait ContainedLogic<Up, Down> where Up: Send, Down: Send {
    fn receive(&mut self, event: LogicEvent<Up, Box<Any + Send>>, ctx: &mut StreamContext<Up, Down, Box<Any + Send>>);
}

struct ContainedLogicImpl<Up, Down, Ctl, L: Logic<Up, Down, Ctl = Ctl>> where Up: Send, Down: Send, Ctl: Send {
    logic: L,
    phantom: PhantomData<(Up, Down, Ctl)>
}

impl<Up, Down, Ctl, L: Logic<Up, Down, Ctl = Ctl>> ContainedLogicImpl<Up, Down, Ctl, L> where Up: Send, Down: Send, Ctl: Send {
    fn wrap_ctx<'a, 'x, 'y, 'z>(&self, ctx: &'a mut StreamContext<'x, 'y, 'z, Up, Down, Box<Any + Send>>) -> &'a mut StreamContext<'x, 'y, 'z, Up, Down, Ctl> {
        unimplemented!()
    }
}

impl<Up, Down, Ctl, L: Logic<Up, Down, Ctl = Ctl>> ContainedLogic<Up, Down> for ContainedLogicImpl<Up, Down, Ctl, L> where Up: Send, Down: Send, Ctl: 'static + Send {
    fn receive(&mut self, event: LogicEvent<Up, Box<Any + Send>>, ctx: &mut StreamContext<Up, Down, Box<Any + Send>>) {
        match event {
            LogicEvent::Pulled => {
                self.logic.receive(LogicEvent::Pulled, self.wrap_ctx(ctx));
            }

            LogicEvent::Pushed(element) => {
                self.logic.receive(LogicEvent::Pushed(element), self.wrap_ctx(ctx));
            }

            LogicEvent::Stopped => {
                self.logic.receive(LogicEvent::Stopped, self.wrap_ctx(ctx));
            }

            LogicEvent::Cancelled => {
                self.logic.receive(LogicEvent::Cancelled, self.wrap_ctx(ctx));
            }

            LogicEvent::Started => {
                self.logic.receive(LogicEvent::Started, self.wrap_ctx(ctx));
            }

            LogicEvent::Forwarded(msg) => {
                unimplemented!();
            }

            LogicEvent::Forwarded(msg) => {
                unimplemented!();
            }
        }
    }
}

pub(in crate::stream) struct Fused<A, B, C>
where
    A: 'static + Send,
    B: 'static + Send,
    C: 'static + Send {
    up: Box<dyn ContainedLogic<A, B> + Send>,
    down: Box<dyn ContainedLogic<B, C> + Send>,
    phantom: PhantomData<(A, B, C)>
}

impl<A, B, C> Fused<A, B, C>
where
    A: 'static + Send,
    B: 'static + Send,
    C: 'static + Send {

}

impl<A, B, C> Logic<A, C> for Fused<A, B, C>
where
    A: 'static + Send,
    B: 'static + Send,
    C: 'static + Send {

    type Ctl = FusedMsg<usize, usize>;

    fn name(&self) -> &'static str {
        "Fused"
    }

    fn receive(&mut self, msg: LogicEvent<A, Self::Ctl>, ctx: &mut StreamContext<A, C, Self::Ctl>) -> Action<C, Self::Ctl> {
        match msg {
            LogicEvent::Pulled => {
                //self.down.receive(LogicEvent::Pulled, ctx);

            }

            LogicEvent::Pushed(element) => {
                self.up.receive(LogicEvent::Pushed(element), unimplemented!());
            }

            LogicEvent::Stopped => {

            }

            LogicEvent::Cancelled => {

            }

            LogicEvent::Started => {

            }

            LogicEvent::Forwarded(FusedMsg::ForwardUp(msg)) => {

            }

            LogicEvent::Forwarded(FusedMsg::ForwardDown(msg)) => {

            }
        }

        Action::None
    }
}
