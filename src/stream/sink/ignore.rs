use crate::stream::{Action, Logic, StreamContext};
use std::marker::PhantomData;

struct Ignore<A>
where
    A: 'static + Send,
{
    pulled: bool,
    phantom: PhantomData<A>,
}

impl<A> Logic<A, (), ()> for Ignore<A>
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

            Some(Action::Push(()))
        } else {
            None
        }
    }

    fn forwarded(&mut self, msg: (), ctx: &mut StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        None
    }
}
