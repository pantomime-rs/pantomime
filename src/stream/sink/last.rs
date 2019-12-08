use crate::stream::{Action, Logic, LogicEvent, StreamContext};

#[derive(Default)]
pub struct Last<A> {
    last: Option<A>,
    pulled: bool,
    stopped: bool,
}

impl<A> Last<A> {
    pub fn new() -> Self {
        Self {
            last: None,
            pulled: false,
            stopped: false,
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
        _: &mut StreamContext<A, Option<A>, Self::Ctl>,
    ) -> Action<Option<A>, Self::Ctl> {
        match msg {
            LogicEvent::Pushed(element) => {
                self.last = Some(element);

                Action::Pull
            }

            LogicEvent::Pulled => {
                self.pulled = true;

                if self.stopped {
                    Action::PushAndStop(self.last.take(), None)
                } else {
                    Action::Pull
                }
            }

            LogicEvent::Stopped => {
                self.stopped = true;

                if self.pulled {
                    Action::PushAndStop(self.last.take(), None)
                } else {
                    Action::None
                }
            }

            LogicEvent::Cancelled => {
                if self.stopped && self.pulled {
                    Action::PushAndStop(self.last.take(), None)
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
