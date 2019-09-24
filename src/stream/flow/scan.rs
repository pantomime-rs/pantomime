use crate::stream::{Action, Logic, LogicEvent, StreamContext};

pub struct Scan<B, F> {
    scan: F,
    sent: bool,
    last: Option<B>,
}

impl<B, F> Scan<B, F> {
    pub fn new<A>(zero: B, scan: F) -> Self
    where
        F: FnMut(B, A) -> B,
    {
        Self {
            scan,
            sent: false,
            last: Some(zero),
        }
    }
}

impl<A: Send, B: Clone + Send, F: FnMut(B, A) -> B + Send> Logic<A, B> for Scan<B, F> {
    type Ctl = ();

    fn receive(&mut self, msg: LogicEvent<A, Self::Ctl>, ctx: &mut StreamContext<A, B, Self::Ctl>) {
        match msg {
            LogicEvent::Pushed(element) => {
                let last = self.last.take().expect("pantomime bug: Scan::last is None");
                let next = (self.scan)(last, element);

                ctx.tell(Action::Push(next.clone()));

                self.last = Some(next);
            }

            LogicEvent::Pulled if self.sent => {
                ctx.tell(Action::Pull);
            }

            LogicEvent::Pulled => {
                self.sent = true;

                let last = self
                    .last
                    .as_ref()
                    .expect("pantomime bug: Scan::last is None")
                    .clone();

                ctx.tell(Action::Push(last))
            }

            LogicEvent::Stopped | LogicEvent::Cancelled => {
                ctx.tell(Action::Complete(None));
            }

            LogicEvent::Started | LogicEvent::Forwarded(()) => {}
        }
    }
}
