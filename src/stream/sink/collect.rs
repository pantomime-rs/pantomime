use crate::stream::{Action, Logic, LogicEvent, StreamContext};

#[derive(Default)]
pub struct Collect<A> {
    entries: Option<Vec<A>>,
    pulled: bool,
}

impl<A> Collect<A> {
    pub fn new() -> Self {
        Self {
            entries: Some(Vec::new()),
            pulled: false,
        }
    }
}

impl<A> Logic<A, Vec<A>> for Collect<A>
where
    A: 'static + Send,
{
    type Ctl = ();

    fn name(&self) -> &'static str {
        "Collect"
    }

    fn receive(
        &mut self,
        msg: LogicEvent<A, Self::Ctl>,
        _: &mut StreamContext<A, Vec<A>, Self::Ctl>,
    ) -> Action<Vec<A>, Self::Ctl> {
        match msg {
            LogicEvent::Pushed(element) => {
                self.entries
                    .as_mut()
                    .expect("pantomime bug: Collect::entries is None")
                    .push(element);

                Action::Pull
            }

            LogicEvent::Pulled => {
                self.pulled = true;

                Action::None
            }

            LogicEvent::Stopped | LogicEvent::Cancelled => {
                if self.pulled {
                    self.pulled = false;

                    let entries = self
                        .entries
                        .take()
                        .expect("pantomime bug: Collect::entries is None");

                    Action::PushAndComplete(entries, None)
                } else {
                    Action::Complete(None)
                }
            }

            LogicEvent::Started => Action::Pull,

            LogicEvent::Forwarded(()) => Action::None,
        }
    }
}
