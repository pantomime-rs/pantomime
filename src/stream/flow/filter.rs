use crate::stream::{Action, Logic, LogicEvent, StreamContext};

pub struct Filter<F> {
    filter: F,
}

impl<F> Filter<F> {
    pub fn new<A>(filter: F) -> Self
    where
        F: FnMut(&A) -> bool,
    {
        Self { filter }
    }
}

impl<A: Send, F: FnMut(&A) -> bool + Send> Logic<A, A> for Filter<F> {
    type Ctl = ();

    fn name(&self) -> &'static str {
        "Filter"
    }

    fn receive(&mut self, msg: LogicEvent<A, Self::Ctl>, ctx: &mut StreamContext<A, A, Self::Ctl>) {
        match msg {
            LogicEvent::Pulled => {
                ctx.tell(Action::Pull);
            }

            LogicEvent::Pushed(element) => {
                if (self.filter)(&element) {
                    ctx.tell(Action::Push(element));
                } else {
                    ctx.tell(Action::Pull);
                }
            }

            LogicEvent::Stopped | LogicEvent::Cancelled => {
                ctx.tell(Action::Complete(None));
            }

            LogicEvent::Started | LogicEvent::Forwarded(()) => {}
        }
    }
}
