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

impl<A> Logic for Collect<A>
where
    A: 'static + Send,
{
    type In = A;
    type Out = Vec<A>;
    type Msg = ();

    fn receive(
        &mut self,
        msg: LogicEvent<Self::In, Self::Msg>,
        ctx: &mut StreamContext<Self::In, Self::Out, Self::Msg, Self>,
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
