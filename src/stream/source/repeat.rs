use crate::stream::{Action, Logic, StreamContext};
use std::marker::PhantomData;

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

impl<A> Logic<(), A, ()> for Repeat<A>
where
    A: 'static + Clone + Send,
{
    fn name(&self) -> &'static str {
        "Repeat"
    }

    fn pulled(&mut self, ctx: &mut StreamContext<(), A, ()>) -> Option<Action<A, ()>> {
        Some(Action::Push(self.element.clone()))
    }

    fn pushed(&mut self, el: (), ctx: &mut StreamContext<(), A, ()>) -> Option<Action<A, ()>> {
        None
    }

    fn stopped(&mut self, ctx: &mut StreamContext<(), A, ()>) -> Option<Action<A, ()>> {
        None
    }

    fn cancelled(&mut self, ctx: &mut StreamContext<(), A, ()>) -> Option<Action<A, ()>> {
        Some(Action::Complete)
    }
}
