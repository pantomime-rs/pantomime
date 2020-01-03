use crate::stream::{Action, Logic, LogicEvent, StreamContext};

pub struct Fold<B, F> {
    fold: F,
    pulled: bool,
    cancelled: bool,
    last: Option<B>,
}

impl<B, F> Fold<B, F> {
    pub fn new<A>(zero: B, fold: F) -> Self
    where
        F: FnMut(B, A) -> B,
    {
        Self {
            fold,
            pulled: false,
            cancelled: false,
            last: Some(zero),
        }
    }

    fn take_last(&mut self) -> B {
        self.last.take().expect("pantomime bug: Fold::last is None")
    }
}

impl<A: Send, B: Send, F: FnMut(B, A) -> B + Send> Logic<A, B> for Fold<B, F> {
    type Ctl = ();

    fn name(&self) -> &'static str {
        "Fold"
    }

    fn receive(
        &mut self,
        msg: LogicEvent<A, Self::Ctl>,
        _: &mut StreamContext<A, B, Self::Ctl>,
    ) -> Action<B, Self::Ctl> {
        match msg {
            LogicEvent::Pushed(element) => {
                let last = self.take_last();
                self.last = Some((self.fold)(last, element));
                Action::Pull
            }

            LogicEvent::Pulled => {
                self.pulled = true;
                Action::Pull
            }

            LogicEvent::Cancelled => {
                self.cancelled = true;
                Action::Cancel
            }

            LogicEvent::Stopped => {
                if self.pulled {
                    Action::PushAndStop(self.take_last(), None)
                } else if self.cancelled {
                    Action::Stop(None)
                } else {
                    Action::None
                }
            }

            LogicEvent::Started => Action::None,

            LogicEvent::Forwarded(()) => Action::None,
        }
    }
}
