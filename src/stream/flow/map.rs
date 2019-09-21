use crate::stream::{Action, Logic, StreamContext};
use std::marker::PhantomData;

pub struct Map<A, B, F: FnMut(A) -> B>
where
    A: Send,
    B: Send,
{
    map_fn: F,
    phantom: PhantomData<(A, B)>,
}

impl<A, B, F: FnMut(A) -> B> Map<A, B, F>
where
    A: Send,
    B: Send,
{
    pub fn new(map_fn: F) -> Self {
        Self {
            map_fn,
            phantom: PhantomData,
        }
    }
}

impl<A, B, F> Logic<A, B, ()> for Map<A, B, F>
where
    F: FnMut(A) -> B,
    A: 'static + Send,
    B: 'static + Send,
{
    fn name(&self) -> &'static str {
        "Map"
    }

    fn pulled(&mut self, ctx: &mut StreamContext<A, B, ()>) -> Option<Action<B, ()>> {
        Some(Action::Pull)
    }

    fn pushed(&mut self, el: A, ctx: &mut StreamContext<A, B, ()>) -> Option<Action<B, ()>> {
        Some(Action::Push((self.map_fn)(el)))
    }
}
