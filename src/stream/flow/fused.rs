use crate::stream::{Action, Logic, LogicEvent, StreamContext};
use std::marker::PhantomData;

struct Fused<A, B, C, UpCtl, DownCtl, Up: Logic<A, B, Ctl = UpCtl>, Down: Logic<B, C, Ctl = DownCtl>>
where
    A: 'static + Send,
    B: 'static + Send,
    C: 'static + Send,
    UpCtl: 'static + Send,
    DownCtl: 'static + Send,
    Up: 'static + Send,
    Down: 'static + Send {
    up: Up,
    down: Down,
    phantom: PhantomData<(A, B, C, UpCtl, DownCtl)>
}

impl<A, B, C, UpCtl, DownCtl, Up: Logic<A, B, Ctl = UpCtl>, Down: Logic<B, C, Ctl = DownCtl>> Logic<A, C> for Fused<A, B, C, UpCtl, DownCtl, Up, Down>
where
    A: 'static + Send,
    B: 'static + Send,
    C: 'static + Send,
    UpCtl: 'static + Send,
    DownCtl: 'static + Send,
    Up: 'static + Send,
    Down: 'static + Send {

    type Ctl = ();

    fn name(&self) -> &'static str {
        "Fused"
    }

    fn receive(&mut self, msg: LogicEvent<A, Self::Ctl>, ctx: &mut StreamContext<A, C, Self::Ctl>) {
        match msg {
            LogicEvent::Pulled => {

            }

            LogicEvent::Pushed(element) => {

            }

            LogicEvent::Stopped => {

            }

            LogicEvent::Cancelled => {
                ctx.tell(Action::Complete(None));
            }

            LogicEvent::Started => {

            }

            LogicEvent::Forwarded(msg) => {

            }
        }
    }
}
