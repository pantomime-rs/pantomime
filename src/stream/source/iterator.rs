use crate::stream::{Action, Logic, LogicEvent, StreamContext};

pub struct Iterator<A, I: std::iter::Iterator<Item = A>>
where
    A: 'static + Send,
{
    iterator: I,
}

impl<A, I: std::iter::Iterator<Item = A>> Iterator<A, I>
where
    A: 'static + Send,
{
    pub fn new(iterator: I) -> Self {
        Self { iterator }
    }
}

impl<A, I: std::iter::Iterator<Item = A>> Logic<(), A> for Iterator<A, I>
where
    A: 'static + Send,
    I: 'static + Send,
{
    type Ctl = ();

    fn name(&self) -> &'static str {
        "Iterator"
    }

    fn receive(
        &mut self,
        msg: LogicEvent<(), Self::Ctl>,
        _: &mut StreamContext<(), A, Self::Ctl>,
    ) -> Action<A, Self::Ctl> {
        match msg {
            LogicEvent::Pulled => match self.iterator.next() {
                Some(element) => Action::Push(element),

                None => Action::Complete(None),
            },

            LogicEvent::Cancelled => Action::Complete(None),

            LogicEvent::Pushed(())
            | LogicEvent::Stopped
            | LogicEvent::Started
            | LogicEvent::Forwarded(()) => Action::None,
        }
    }
}
