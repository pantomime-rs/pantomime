use crate::stream::{Action, Logic, LogicEvent, StreamContext};
use std::marker::PhantomData;

pub struct Last<A> {
    last: Option<A>,
    pulled: bool,
}

impl<A> Last<A> {
    pub fn new() -> Self {
        Self {
            last: None,
            pulled: false,
        }
    }
}

impl<A> Logic for Last<A>
where
    A: 'static + Send,
{
    type In = A;
    type Out = Option<A>;
    type Msg = ();

    fn receive(
        &mut self,
        msg: LogicEvent<Self::In, Self::Msg>,
        ctx: &mut StreamContext<Self::In, Self::Out, Self::Msg, Self>,
    ) {
        match msg {
            LogicEvent::Pushed(element) => {
                self.last = Some(element);

                ctx.tell(Action::Pull);
            }

            LogicEvent::Pulled => {
                self.pulled = true;
            }

            LogicEvent::Stopped | LogicEvent::Cancelled => {
                if self.pulled {
                    self.pulled = false;

                    ctx.tell(Action::Push(self.last.take()));
                }

                ctx.tell(Action::Complete(None));
            }

            LogicEvent::Started => {
                ctx.tell(Action::Pull);
            }

            LogicEvent::Forwarded(()) => {}
        }
    }
}
