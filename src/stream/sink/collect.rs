use crate::stream::{Action, Logic, LogicEvent, StreamContext};

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

    fn receive(
        &mut self,
        msg: LogicEvent<A, Self::Ctl>,
        ctx: &mut StreamContext<A, Vec<A>, Self::Ctl>,
    ) {
        match msg {
            LogicEvent::Pushed(element) => {
                self.entries
                    .as_mut()
                    .expect("pantomime bug: Collect::entries is None")
                    .push(element);

                ctx.tell(Action::Pull);
            }

            LogicEvent::Pulled => {
                self.pulled = true;
            }

            LogicEvent::Stopped | LogicEvent::Cancelled => {
                if self.pulled {
                    self.pulled = false;

                    let entries = self
                        .entries
                        .take()
                        .expect("pantomime bug: Collect::entries is None");

                    ctx.tell(Action::Push(entries));
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
