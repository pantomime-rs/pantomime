use crate::stream::{Action, Logic, StreamContext};
use std::marker::PhantomData;

struct TakeWhile<A, F: FnMut(&A) -> bool>
where
    A: 'static + Send,
    F: 'static + Send,
{
    while_fn: F,
    cancelled: bool,
    phantom: PhantomData<A>,
}

impl<A, F: FnMut(&A) -> bool> TakeWhile<A, F>
where
    A: 'static + Send,
    F: 'static + Send,
{
    fn new(while_fn: F) -> Self {
        Self {
            while_fn,
            cancelled: false,
            phantom: PhantomData,
        }
    }
}

impl<A, F: FnMut(&A) -> bool> Logic<A, A, ()> for TakeWhile<A, F>
where
    A: 'static + Send,
    F: 'static + Send,
{
    fn name(&self) -> &'static str {
        "TakeWhile"
    }

    fn pulled(&mut self, ctx: &mut StreamContext<A, A, ()>) -> Option<Action<A, ()>> {
        Some(Action::Pull)
    }

    fn pushed(&mut self, el: A, ctx: &mut StreamContext<A, A, ()>) -> Option<Action<A, ()>> {
        if self.cancelled {
            None
        } else if (self.while_fn)(&el) {
            Some(Action::Push(el))
        } else {
            self.cancelled = true;

            Some(Action::Complete)
        }
    }

    fn started(&mut self, ctx: &mut StreamContext<A, A, ()>) -> Option<Action<A, ()>> {
        None
    }

    fn stopped(&mut self, ctx: &mut StreamContext<A, A, ()>) -> Option<Action<A, ()>> {
        Some(Action::Complete)
    }

    fn cancelled(&mut self, ctx: &mut StreamContext<A, A, ()>) -> Option<Action<A, ()>> {
        Some(Action::Complete)
    }

    fn forwarded(&mut self, msg: (), ctx: &mut StreamContext<A, A, ()>) -> Option<Action<A, ()>> {
        None
    }
}
