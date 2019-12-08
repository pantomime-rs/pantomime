use crate::stream::{Action, Logic, LogicEvent, StreamContext};

#[derive(Default)]
pub struct Ignore {
    pulled: bool,
    stopped: bool,
}

impl Ignore {
    pub fn new() -> Self {
        Self {
            pulled: false,
            stopped: false,
        }
    }
}

impl<A> Logic<A, ()> for Ignore
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

                if self.stopped {
                    Action::PushAndStop((), None)
                } else {
                    Action::Pull
                }
            }

            LogicEvent::Stopped => {
                self.stopped = true;

                if self.pulled {
                    Action::PushAndStop((), None)
                } else {
                    Action::None
                }
            }

            LogicEvent::Cancelled => {
                if self.stopped && self.pulled {
                    Action::PushAndStop((), None)
                } else if self.stopped {
                    Action::Stop(None)
                } else {
                    Action::Cancel
                }
            }

            LogicEvent::Started | LogicEvent::Forwarded(()) => Action::None,
        }
    }
}
