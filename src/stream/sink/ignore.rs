use crate::stream::{Action, Logic, LogicEvent, StreamContext};
use std::marker::PhantomData;

pub struct Ignore<A>
where
    A: Send,
{
    pulled: bool,
    phantom: PhantomData<A>,
}

impl<A> Ignore<A>
where
    A: Send,
{
    pub fn new() -> Self {
        Self {
            pulled: false,
            phantom: PhantomData,
        }
    }
}

impl<A> Logic for Ignore<A>
where
    A: 'static + Send,
{
    type In = A;
    type Out = ();
    type Msg = ();

    fn receive(
        &mut self,
        msg: LogicEvent<Self::In, Self::Msg>,
        ctx: &mut StreamContext<Self::In, Self::Out, Self::Msg, Self>,
    ) {
        match msg {
            LogicEvent::Pushed(_) => {
                ctx.tell(Action::Pull);
            }

            LogicEvent::Pulled => {
                self.pulled = true;
            }

            LogicEvent::Stopped | LogicEvent::Cancelled => {
                if self.pulled {
                    self.pulled = false;

                    ctx.tell(Action::Push(()));
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
