use crate::stream::{Action, Logic, StreamContext};
use std::marker::PhantomData;

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

impl<A, I: std::iter::Iterator<Item = A>> Logic<(), A, ()> for Iterator<A, I>
where
    A: 'static + Send,
{
    fn name(&self) -> &'static str {
        "Iterator"
    }

    fn pulled(&mut self, ctx: &mut StreamContext<(), A, ()>) -> Option<Action<A, ()>> {
        Some(match self.iterator.next() {
            Some(element) => Action::Push(element),
            None => Action::Complete,
        })
    }

    fn pushed(&mut self, el: (), ctx: &mut StreamContext<(), A, ()>) -> Option<Action<A, ()>> {
        None
    }

    fn stopped(&mut self, ctx: &mut StreamContext<(), A, ()>) -> Option<Action<A, ()>> {
        None
    }
}
