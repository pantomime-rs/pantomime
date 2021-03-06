use crate::stream::{Action, Logic, LogicEvent, StreamContext};

pub struct Repeat<A>
where
    A: 'static + Clone + Send,
{
    element: A,
}

impl<A> Repeat<A>
where
    A: 'static + Clone + Send,
{
    pub fn new(element: A) -> Self {
        Self { element }
    }
}

impl<A> Logic<(), A> for Repeat<A>
where
    A: 'static + Clone + Send,
{
    type Ctl = ();

    fn name(&self) -> &'static str {
        "Repeat"
    }

    fn receive(
        &mut self,
        msg: LogicEvent<(), Self::Ctl>,
        _: &mut StreamContext<(), A, Self::Ctl>,
    ) -> Action<A, Self::Ctl> {
        match msg {
            LogicEvent::Pulled => Action::Push(self.element.clone()),

            LogicEvent::Cancelled => Action::Stop(None),

            LogicEvent::Pushed(())
            | LogicEvent::Stopped
            | LogicEvent::Started
            | LogicEvent::Forwarded(()) => Action::None,
        }
    }
}
