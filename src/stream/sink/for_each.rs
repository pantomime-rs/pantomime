use crate::stream::{Action, Logic, StreamContext};
use std::marker::PhantomData;

pub struct ForEach<A, F>
where
    F: FnMut(A) -> (),
    A: 'static + Send,
{
    for_each_fn: F,
    pulled: bool,
    phantom: PhantomData<A>,
}

impl<A, F: FnMut(A) -> ()> ForEach<A, F>
where
    F: 'static + Send,
    A: 'static + Send,
{
    pub fn new(for_each_fn: F) -> Self {
        Self {
            for_each_fn,
            pulled: false,
            phantom: PhantomData,
        }
    }
}

impl<A, F> Logic<A, (), ()> for ForEach<A, F>
where
    F: FnMut(A) -> (),
    A: 'static + Send,
{
    fn name(&self) -> &'static str {
        "ForEach"
    }

    fn pulled(&mut self, ctx: &mut StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        self.pulled = true;

        None
    }

    fn pushed(&mut self, el: A, ctx: &mut StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        (self.for_each_fn)(el);

        Some(Action::Pull)
    }

    fn started(&mut self, ctx: &mut StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        Some(Action::Pull)
    }

    fn stopped(&mut self, ctx: &mut StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        if self.pulled {
            self.pulled = false;

            let actor_ref = ctx.actor_ref();

            actor_ref.tell(Action::Push(()));
            actor_ref.tell(Action::Complete);

            None
        } else {
            Some(Action::Complete)
        }
    }

    fn cancelled(&mut self, ctx: &mut StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        Some(Action::Complete)
    }

    fn forwarded(&mut self, msg: (), ctx: &mut StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        None
    }
}
