use crate::stream::{Action, Logic, LogicEvent, StreamContext};

pub struct Single<A>
where
    A: 'static,
{
    element: Option<A>,
}

impl<A> Single<A>
where
    A: 'static + Send,
{
    pub fn new(element: A) -> Self {
        Self {
            element: Some(element),
        }
    }
}

impl<A> Logic<(), A> for Single<A>
where
    A: 'static + Send,
{
    type Ctl = ();

    fn name(&self) -> &'static str {
        "Single"
    }

    fn receive(
        &mut self,
        msg: LogicEvent<(), Self::Ctl>,
        _: &mut StreamContext<(), A, Self::Ctl>,
    ) -> Action<A, Self::Ctl> {
        match msg {
            LogicEvent::Pulled => {
                Action::PushAndComplete(self.element.take().expect("Single::element is None"), None)
            }

            LogicEvent::Cancelled => Action::Complete(None),

            LogicEvent::Pushed(())
            | LogicEvent::Stopped
            | LogicEvent::Started
            | LogicEvent::Forwarded(()) => Action::None,
        }
    }
}
