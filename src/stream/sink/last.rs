use crate::stream::{Action, Logic, LogicEvent, StreamContext};

#[derive(Default)]
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
        _: &mut StreamContext<A, Option<A>, Self::Ctl>,
    ) -> Action<Option<A>, Self::Ctl> {
        match msg {
            LogicEvent::Pushed(element) => {
                self.last = Some(element);

                Action::Pull
            }

            LogicEvent::Pulled => {
                self.pulled = true;

                Action::Pull
            }

            LogicEvent::Stopped | LogicEvent::Cancelled => {
                if self.pulled {
                    self.pulled = false;

                    Action::PushAndComplete(self.last.take(), None)
                } else {
                    Action::Complete(None)
                }
            }

            LogicEvent::Started | LogicEvent::Forwarded(()) => Action::None,
        }
    }
}
