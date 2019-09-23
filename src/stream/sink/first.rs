use crate::stream::{Action, Logic, LogicEvent, StreamContext};
use std::marker::PhantomData;

pub struct First<A> {
    first: Option<A>,
    pulled: bool,
}

impl<A> First<A> {
    pub fn new() -> Self {
        Self {
            first: None,
            pulled: false,
        }
    }
}

impl<A> Logic for First<A>
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
                if self.first.is_none() {
                    self.first = Some(element);
                }

                ctx.tell(Action::Cancel);
            }

            LogicEvent::Pulled => {
                self.pulled = true;
            }

            LogicEvent::Stopped | LogicEvent::Cancelled => {
                if self.pulled {
                    self.pulled = false;

                    ctx.tell(Action::Push(self.first.take()));
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
