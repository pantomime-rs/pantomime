use crate::stream::{Action, Logic, LogicEvent, StreamContext};
use std::marker::PhantomData;

pub struct Map<A, B, F: FnMut(A) -> B>
where
    A: Send,
    B: Send,
{
    map_fn: F,
    phantom: PhantomData<(A, B)>,
}

impl<A, B, F: FnMut(A) -> B> Map<A, B, F>
where
    A: Send,
    B: Send,
{
    pub fn new(map_fn: F) -> Self {
        Self {
            map_fn,
            phantom: PhantomData,
        }
    }
}

impl<A, B, F: FnMut(A) -> B> Logic for Map<A, B, F>
where
    F: 'static + Send,
    A: 'static + Send,
    B: 'static + Send,
{
    type In = A;
    type Out = B;
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
                ctx.tell(Action::Push((self.map_fn)(element)));
            }

            LogicEvent::Stopped | LogicEvent::Cancelled => {
                ctx.tell(Action::Complete(None));
            }

            LogicEvent::Started | LogicEvent::Forwarded(()) => {}
        }
    }
}
