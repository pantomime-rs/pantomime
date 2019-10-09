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

impl<A> Logic<A, Option<A>> for Last<A>
where
    A: 'static + Send,
{
    type Ctl = ();

    fn name(&self) -> &'static str {
        "Last"
    }

    fn receive(
        &mut self,
        msg: LogicEvent<A, Self::Ctl>,
        ctx: &mut StreamContext<A, Option<A>, Self::Ctl>,
    ) {
        match msg {
            LogicEvent::Pushed(element) => {
                self.last = Some(element);

                ctx.tell(Action::Pull);
            }

            LogicEvent::Pulled => {
                self.pulled = true;
                ctx.tell(Action::Pull);
            }

            LogicEvent::Stopped | LogicEvent::Cancelled => {
                if self.pulled {
                    self.pulled = false;

                    ctx.tell(Action::Push(self.last.take()));
                } else {
                    println!("    SINK NOT PULLED");
                }

                ctx.tell(Action::Complete(None));
            }

            LogicEvent::Started => {}

            LogicEvent::Forwarded(()) => {}
        }
    }
}
