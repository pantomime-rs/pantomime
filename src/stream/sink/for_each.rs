use crate::stream::{Action, Logic, LogicEvent, StreamContext};
use std::marker::PhantomData;

pub struct ForEach<A, F: FnMut(A) -> ()> {
    for_each_fn: F,
    pulled: bool,
    phantom: PhantomData<A>,
}

impl<A, F: FnMut(A) -> ()> ForEach<A, F> {
    pub fn new(for_each_fn: F) -> Self {
        Self {
            for_each_fn,
            pulled: false,
            phantom: PhantomData,
        }
    }
}

impl<A, F: FnMut(A) -> ()> Logic<A, ()> for ForEach<A, F>
where
    F: 'static + Send,
    A: 'static + Send,
{
    type Ctl = ();

    fn name(&self) -> &'static str {
        "ForEach"
    }

    fn receive(
        &mut self,
        msg: LogicEvent<A, Self::Ctl>,
        ctx: &mut StreamContext<A, (), Self::Ctl>,
    ) {
        match msg {
            LogicEvent::Pushed(element) => {
                (self.for_each_fn)(element);

                ctx.tell(Action::Pull);
            }

            LogicEvent::Pulled => {
                self.pulled = true;
            }

            LogicEvent::Stopped | LogicEvent::Cancelled => {
                if self.pulled {
                    self.pulled = false;

                    ctx.tell(Action::Push(()));
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
