use crate::stream::{Action, Logic, LogicEvent, StreamContext};
use std::marker::PhantomData;

pub struct Repeat<A>
where
    A: 'static + Clone + Send,
{
    element: A,
}

impl<A> Repeat<A>
where
    A: 'static + Clone + Send,
{
    pub fn new(element: A) -> Self {
        Self { element }
    }
}

impl<A> Logic for Repeat<A>
where
    A: 'static + Clone + Send,
{
    type In = ();
    type Out = A;
    type Msg = ();

    fn receive(
        &mut self,
        msg: LogicEvent<Self::In, Self::Msg>,
        ctx: &mut StreamContext<Self::In, Self::Out, Self::Msg, Self>,
    ) {
        match msg {
            LogicEvent::Pulled => {
                ctx.tell(Action::Push(self.element.clone()));
            }

            LogicEvent::Cancelled => {
                ctx.tell(Action::Complete(None));
            }

            LogicEvent::Pushed(())
            | LogicEvent::Stopped
            | LogicEvent::Started
            | LogicEvent::Forwarded(()) => {}
        }
    }
}
