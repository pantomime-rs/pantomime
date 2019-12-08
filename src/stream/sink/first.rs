use crate::stream::{Action, Logic, LogicEvent, StreamContext};

#[derive(Default)]
pub struct First<A> {
    first: Option<A>,
    pulled: bool,
    stopped: bool,
}

impl<A> First<A> {
    pub fn new() -> Self {
        Self {
            first: None,
            pulled: false,
            stopped: false,
        }
    }
}

impl<A> Logic<A, Option<A>> for First<A>
where
    A: 'static + Send,
{
    type Ctl = ();

    fn name(&self) -> &'static str {
        "First"
    }

    fn fusible(&self) -> bool {
        false
    }

    fn receive(
        &mut self,
        msg: LogicEvent<A, Self::Ctl>,
        _: &mut StreamContext<A, Option<A>, Self::Ctl>,
    ) -> Action<Option<A>, Self::Ctl> {
        match msg {
            LogicEvent::Pushed(element) => {
                if self.first.is_none() {
                    self.first = Some(element);
                }

                Action::Cancel
            }

            LogicEvent::Pulled => {
                self.pulled = true;

                if self.stopped {
                    Action::PushAndStop(self.first.take(), None)
                } else {
                    Action::Pull
                }
            }

            LogicEvent::Stopped => {
                self.stopped = true;

                if self.pulled {
                    Action::PushAndStop(self.first.take(), None)
                } else {
                    Action::None
                }
            }

            LogicEvent::Cancelled => {
                if self.stopped && self.pulled {
                    Action::PushAndStop(self.first.take(), None)
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
