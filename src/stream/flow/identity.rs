use crate::stream::{Action, Logic, LogicEvent, StreamContext};
use std::marker::PhantomData;

pub struct Identity<A> {
    phantom: PhantomData<A>,
}

impl<A> Identity<A> {
    pub fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl<A> Logic for Identity<A>
where
    A: 'static + Send,
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
                ctx.tell(Action::Push(element));
            }

            LogicEvent::Stopped | LogicEvent::Cancelled => {
                ctx.tell(Action::Complete(None));
            }

            LogicEvent::Started | LogicEvent::Forwarded(()) => {}
        }
    }
}
