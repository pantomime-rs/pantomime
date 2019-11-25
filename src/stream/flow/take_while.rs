use crate::stream::{Action, Logic, LogicEvent, StreamContext};

pub struct TakeWhile<F> {
    while_fn: F,
}

impl<F> TakeWhile<F> {
    pub fn new<A>(while_fn: F) -> Self
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

    fn receive(
        &mut self,
        msg: LogicEvent<A, Self::Ctl>,
        _: &mut StreamContext<A, A, Self::Ctl>,
    ) -> Action<A, Self::Ctl> {
        match msg {
            LogicEvent::Pushed(element) => {
                if (self.while_fn)(&element) {
                    Action::Push(element)
                } else {
                    Action::Complete(None)
                }
            }

            LogicEvent::Pulled => Action::Pull,
            LogicEvent::Cancelled => Action::Cancel,
            LogicEvent::Stopped => Action::Complete(None),
            LogicEvent::Started => Action::None,
            LogicEvent::Forwarded(()) => Action::None,
        }
    }
}
