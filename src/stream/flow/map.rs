use crate::stream::{Action, Logic, LogicEvent, StreamContext};

pub struct Map<F> {
    map: F,
}

impl<F> Map<F> {
    pub fn new<A, B>(map: F) -> Self
    where
        F: FnMut(A) -> B,
    {
        Self { map }
    }
}

impl<A: Send, B: Send, F: FnMut(A) -> B + Send> Logic<A, B> for Map<F> {
    type Ctl = ();

    fn name(&self) -> &'static str {
        "Map"
    }

    fn receive(&mut self, msg: LogicEvent<A, Self::Ctl>, ctx: &mut StreamContext<A, B, Self::Ctl>) -> Action<B, Self::Ctl> {
        match msg {
            LogicEvent::Pulled => {
                Action::Pull
            }

            LogicEvent::Pushed(element) => {
                Action::Push((self.map)(element))
            }

            LogicEvent::Stopped | LogicEvent::Cancelled => {
               Action::Complete(None)
            }

            LogicEvent::Started | LogicEvent::Forwarded(()) => {
                Action::None
            }
        }
    }
}
