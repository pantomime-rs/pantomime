use crate::stream::{Action, Logic, LogicEvent, StreamContext};

pub struct Identity;

impl Identity {
    pub fn new() -> Self {
        Self
    }
}

impl<A: Send> Logic<A, A> for Identity {
    type Ctl = ();

    fn receive(&mut self, msg: LogicEvent<A, Self::Ctl>, ctx: &mut StreamContext<A, A, Self::Ctl>) {
        match msg {
            LogicEvent::Pulled => {
                ctx.tell(Action::Pull);
            }

            LogicEvent::Pushed(element) => {
                ctx.tell(Action::Push(element));
            }

            LogicEvent::Stopped | LogicEvent::Cancelled => {
                ctx.tell(Action::Complete(None));
            }

            LogicEvent::Started | LogicEvent::Forwarded(()) => {}
        }
    }
}
