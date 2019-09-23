use crate::stream::{Action, Logic, LogicEvent, StreamContext};
use std::marker::PhantomData;

pub struct Filter<A, F: FnMut(&A) -> bool>
where
    A: 'static + Send,
    F: 'static + Send,
{
    filter: F,
    phantom: PhantomData<A>,
}

impl<A, F: FnMut(&A) -> bool> Filter<A, F>
where
    A: 'static + Send,
    F: 'static + Send,
{
    pub fn new(filter: F) -> Self {
        Self {
            filter,
            phantom: PhantomData,
        }
    }
}

impl<A, F: FnMut(&A) -> bool> Logic for Filter<A, F>
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
