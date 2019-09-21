use crate::stream::{Action, Logic, StreamContext};
use std::marker::PhantomData;

pub struct Identity;

impl Identity {
    pub fn new() -> Self {
        Self
    }
}

impl<A> Logic<A, A, ()> for Identity
where
    A: 'static + Send,
{
    fn name(&self) -> &'static str {
        "Identity"
    }

    fn pulled(&mut self, ctx: &mut StreamContext<A, A, ()>) -> Option<Action<A, ()>> {
        Some(Action::Pull)
    }

    fn pushed(&mut self, el: A, ctx: &mut StreamContext<A, A, ()>) -> Option<Action<A, ()>> {
        Some(Action::Push(el))
    }
}
