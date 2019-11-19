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

impl<A> Logic<A, ()> for Ignore<A>
where
    A: Send,
{
    type Ctl = ();

    fn name(&self) -> &'static str {
        "Ignore"
    }

    fn receive(
        &mut self,
        msg: LogicEvent<A, Self::Ctl>,
        _: &mut StreamContext<A, (), Self::Ctl>,
    ) -> Action<(), Self::Ctl> {
        match msg {
            LogicEvent::Pushed(_) => Action::Pull,

            LogicEvent::Pulled => {
                self.pulled = true;

                Action::Pull
            }

            LogicEvent::Stopped | LogicEvent::Cancelled => {
                if self.pulled {
                    Action::PushAndComplete((), None)
                } else {
                    Action::Complete(None)
                }
            }

            LogicEvent::Started | LogicEvent::Forwarded(()) => Action::None,
        }
    }
}
