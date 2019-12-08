use crate::stream::{Action, Logic, LogicEvent, StreamContext};

pub struct FilterMap<F> {
    filter_map: F,
}

impl<F> FilterMap<F> {
    pub fn new(filter_map: F) -> Self {
        Self { filter_map }
    }
}

impl<A: Send, B: Send, F: FnMut(A) -> Option<B> + Send> Logic<A, B> for FilterMap<F> {
    type Ctl = ();

    fn name(&self) -> &'static str {
        "FilterMap"
    }

    fn receive(
        &mut self,
        msg: LogicEvent<A, Self::Ctl>,
        _: &mut StreamContext<A, B, Self::Ctl>,
    ) -> Action<B, Self::Ctl> {
        match msg {
            LogicEvent::Pushed(element) => match (self.filter_map)(element) {
                Some(element) => Action::Push(element),
                None => Action::Pull,
            },

            LogicEvent::Pulled => Action::Pull,
            LogicEvent::Cancelled => Action::Cancel,
            LogicEvent::Stopped => Action::Stop(None),
            LogicEvent::Started => Action::None,
            LogicEvent::Forwarded(()) => Action::None,
        }
    }
}
