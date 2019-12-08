use crate::stream::{Action, Logic, LogicEvent, StreamContext};

#[derive(Default)]
pub struct Collect<A> {
    entries: Option<Vec<A>>,
    pulled: bool,
    stopped: bool,
}

impl<A> Collect<A> {
    pub fn new() -> Self {
        Self {
            entries: Some(Vec::new()),
            pulled: false,
            stopped: false,
        }
    }

    fn complete(&mut self) -> Action<Vec<A>, ()> {
        if self.pulled {
            let entries = self
                .entries
                .take()
                .expect("pantomime bug: Collect::entries is None");

            Action::PushAndStop(entries, None)
        } else {
            Action::Stop(None)
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

                if self.stopped {
                    self.complete()
                } else {
                    Action::Pull
                }
            }

            LogicEvent::Stopped => {
                // if entries is non-empty, we were pulled
                //
                // if entries is empty, we need to wait
                //   until we are pulled or cancelled to
                //   complete so as to not lose information

                self.stopped = true;

                if self.pulled {
                    self.complete()
                } else {
                    Action::None
                }
            }

            LogicEvent::Cancelled => {
                if self.stopped {
                    self.complete()
                } else {
                    Action::Cancel
                }
            }

            LogicEvent::Started | LogicEvent::Forwarded(()) => Action::None,
        }
    }
}
