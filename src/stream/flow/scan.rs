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

    fn name(&self) -> &'static str {
        "Scan"
    }

    fn receive(
        &mut self,
        msg: LogicEvent<A, Self::Ctl>,
        _: &mut StreamContext<A, B, Self::Ctl>,
    ) -> Action<B, Self::Ctl> {
        match msg {
            LogicEvent::Pushed(element) => {
                let last = self.last.take().expect("pantomime bug: Scan::last is None");
                let next = (self.scan)(last, element);

                self.last = Some(next.clone());

                Action::Push(next)
            }

            LogicEvent::Pulled if self.sent => Action::Pull,

            LogicEvent::Pulled => {
                self.sent = true;

                let last = self
                    .last
                    .as_ref()
                    .expect("pantomime bug: Scan::last is None")
                    .clone();

                Action::Push(last)
            }

            LogicEvent::Cancelled => Action::Cancel,
            LogicEvent::Stopped => Action::Stop(None),
            LogicEvent::Started => Action::None,
            LogicEvent::Forwarded(()) => Action::None,
        }
    }
}
