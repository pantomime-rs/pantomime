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

impl<A> Logic<A, Option<A>> for First<A>
where
    A: 'static + Send,
{
    type Ctl = ();

    fn receive(
        &mut self,
        msg: LogicEvent<A, Self::Ctl>,
        ctx: &mut StreamContext<A, Option<A>, Self::Ctl>,
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
