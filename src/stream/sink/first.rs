use crate::stream::{Action, Logic, LogicEvent, StreamContext};

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

    fn name(&self) -> &'static str {
        "First"
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

                Action::None
            }

            LogicEvent::Stopped | LogicEvent::Cancelled => {
                if self.pulled {
                    self.pulled = false;

                    Action::PushAndComplete(self.first.take(), None)
                } else {
                    Action::Complete(None)
                }
            }

            LogicEvent::Started => Action::Pull,

            LogicEvent::Forwarded(()) => Action::None,
        }
    }
}
