use crate::stream::{Action, Logic, LogicEvent, StreamContext};

struct TakeWhile<F> {
    while_fn: F,
}

impl<F> TakeWhile<F> {
    fn new<A>(while_fn: F) -> Self
    where
        F: FnMut(&A) -> bool,
    {
        Self { while_fn }
    }
}

impl<A: Send, F: FnMut(&A) -> bool + Send> Logic<A, A> for TakeWhile<F> {
    type Ctl = ();

    fn name(&self) -> &'static str {
        "TakeWhile"
    }

    fn receive(&mut self, msg: LogicEvent<A, Self::Ctl>, ctx: &mut StreamContext<A, A, Self::Ctl>) {
        match msg {
            LogicEvent::Pulled => {
                ctx.tell(Action::Pull);
            }

            LogicEvent::Pushed(element) => {
                ctx.tell(if (self.while_fn)(&element) {
                    Action::Push(element)
                } else {
                    Action::Complete(None)
                });
            }

            LogicEvent::Stopped | LogicEvent::Cancelled => {
                ctx.tell(Action::Complete(None));
            }

            LogicEvent::Started | LogicEvent::Forwarded(()) => {}
        }
    }
}
