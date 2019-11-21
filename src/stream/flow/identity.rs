use crate::stream::{Action, Logic, LogicEvent, StreamContext};

#[derive(Default)]
pub struct Identity;

impl Identity {
    pub fn new() -> Self {
        Self
    }
}

impl<A: Send> Logic<A, A> for Identity {
    type Ctl = ();

    fn name(&self) -> &'static str {
        "Identity"
    }

    fn receive(
        &mut self,
        msg: LogicEvent<A, Self::Ctl>,
        _: &mut StreamContext<A, A, Self::Ctl>,
    ) -> Action<A, Self::Ctl> {
        match msg {
            LogicEvent::Pulled => Action::Pull,
            LogicEvent::Pushed(element) => Action::Push(element),
            LogicEvent::Cancelled => Action::Cancel,
            LogicEvent::Stopped => Action::Complete(None),
            LogicEvent::Started | LogicEvent::Forwarded(()) => Action::None,
        }
    }
}
