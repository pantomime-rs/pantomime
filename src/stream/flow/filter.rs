use crate::stream::{Action, Logic, StreamContext};
use std::marker::PhantomData;

pub struct Filter<A, F: FnMut(&A) -> bool>
where
    A: 'static + Send,
    F: 'static + Send,
{
    filter: F,
    phantom: PhantomData<A>,
}

impl<A, F: FnMut(&A) -> bool> Filter<A, F>
where
    A: 'static + Send,
    F: 'static + Send,
{
    pub fn new(filter: F) -> Self {
        Self {
            filter,
            phantom: PhantomData,
        }
    }
}

impl<A, F: FnMut(&A) -> bool> Logic<A, A, ()> for Filter<A, F>
where
    A: 'static + Send,
    F: 'static + Send,
{
    fn name(&self) -> &'static str {
        "Filter"
    }

    fn pulled(&mut self, ctx: &mut StreamContext<A, A, ()>) -> Option<Action<A, ()>> {
        Some(Action::Pull)
    }

    fn pushed(&mut self, el: A, ctx: &mut StreamContext<A, A, ()>) -> Option<Action<A, ()>> {
        Some(if (self.filter)(&el) {
            Action::Push(el)
        } else {
            Action::Pull
        })
    }
}
