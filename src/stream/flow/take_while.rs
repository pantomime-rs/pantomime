use crate::stream::{Action, Logic, LogicEvent, StreamContext};
use std::marker::PhantomData;

struct TakeWhile<A, F: FnMut(&A) -> bool>
where
    A: 'static + Send,
    F: 'static + Send,
{
    while_fn: F,
    phantom: PhantomData<A>,
}

impl<A, F: FnMut(&A) -> bool> TakeWhile<A, F>
where
    A: 'static + Send,
    F: 'static + Send,
{
    fn new(while_fn: F) -> Self {
        Self {
            while_fn,
            phantom: PhantomData,
        }
    }
}

impl<A, F: FnMut(&A) -> bool> Logic for TakeWhile<A, F>
where
    A: 'static + Send,
    F: 'static + Send,
{
    type In = A;
    type Out = A;
    type Msg = ();

    fn receive(
        &mut self,
        msg: LogicEvent<Self::In, Self::Msg>,
        ctx: &mut StreamContext<Self::In, Self::Out, Self::Msg, Self>,
    ) {
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
