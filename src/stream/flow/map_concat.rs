use crate::stream::{Action, Logic, LogicEvent, StreamContext};

pub struct MapConcat<I, F> {
    map_concat: F,
    iter: Option<I>,
}

impl<I, F> MapConcat<I, F> {
    pub fn new(map_concat: F) -> Self {
        Self {
            map_concat,
            iter: None,
        }
    }
}

impl<A: Send, B: Send, I: Iterator<Item = B>, F: FnMut(A) -> I + Send> Logic<A, B>
    for MapConcat<I, F>
where
    I: 'static + Send,
{
    type Ctl = ();

    fn name(&self) -> &'static str {
        "MapConcat"
    }

    fn receive(
        &mut self,
        msg: LogicEvent<A, Self::Ctl>,
        _: &mut StreamContext<A, B, Self::Ctl>,
    ) -> Action<B, Self::Ctl> {
        match msg {
            LogicEvent::Pulled => match self.iter.as_mut().and_then(|ref mut iter| iter.next()) {
                Some(el) => Action::Push(el),

                None => Action::Pull,
            },

            LogicEvent::Pushed(element) => {
                self.iter = Some((self.map_concat)(element));

                // this only happens after we've pulled (per contract), so
                // now it's time to push elements out

                match self.iter.as_mut().and_then(|ref mut iter| iter.next()) {
                    Some(el) => Action::Push(el),

                    None => Action::Pull,
                }
            }

            LogicEvent::Cancelled => Action::Cancel,

            LogicEvent::Stopped => Action::Stop(None),

            LogicEvent::Started => Action::None,

            LogicEvent::Forwarded(()) => Action::None,
        }
    }
}
