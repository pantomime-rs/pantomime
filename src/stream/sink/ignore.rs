use crate::stream::{Action, Logic, StreamContext};
use std::marker::PhantomData;

pub struct Ignore {
    pulled: bool,
}

impl Ignore {
    pub fn new() -> Self {
        Self { pulled: false }
    }
}

impl<A> Logic<A, (), ()> for Ignore
where
    A: 'static + Send,
{
    fn name(&self) -> &'static str {
        "Ignore"
    }

    fn pulled(&mut self, ctx: &mut StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        self.pulled = true;

        None
    }

    fn pushed(&mut self, el: A, ctx: &mut StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        Some(Action::Pull)
    }

    fn started(&mut self, ctx: &mut StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        Some(Action::Pull)
    }

    fn stopped(&mut self, ctx: &mut StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        if self.pulled {
            self.pulled = false;

            // note that the action we're returning will be synchronously processed, followed
            // later by this Complete, i.e. the Push is processed first.

            ctx.tell(Action::Complete(None));

            Some(Action::Push(()))
        } else {
            Some(Action::Complete(None))
        }
    }

    fn forwarded(&mut self, msg: (), ctx: &mut StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        None
    }
}
