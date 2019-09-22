use crate::stream::{Action, Logic, StreamContext};
use std::marker::PhantomData;

pub struct Last<A> {
    last: Option<A>,
    pulled: bool,
}

impl<A> Last<A> {
    pub fn new() -> Self {
        Self {
            last: None,
            pulled: false,
        }
    }
}

impl<A> Logic<A, Option<A>, ()> for Last<A>
where
    A: 'static + Send,
{
    fn name(&self) -> &'static str {
        "Last"
    }

    fn pulled(
        &mut self,
        ctx: &mut StreamContext<A, Option<A>, ()>,
    ) -> Option<Action<Option<A>, ()>> {
        self.pulled = true;

        None
    }

    fn pushed(
        &mut self,
        el: A,
        ctx: &mut StreamContext<A, Option<A>, ()>,
    ) -> Option<Action<Option<A>, ()>> {
        self.last = Some(el);

        Some(Action::Pull)
    }

    fn started(
        &mut self,
        ctx: &mut StreamContext<A, Option<A>, ()>,
    ) -> Option<Action<Option<A>, ()>> {
        Some(Action::Pull)
    }

    fn stopped(
        &mut self,
        ctx: &mut StreamContext<A, Option<A>, ()>,
    ) -> Option<Action<Option<A>, ()>> {
        if self.pulled {
            self.pulled = false;

            // note that the action we're returning will be synchronously processed, followed
            // later by this Complete, i.e. the Push is processed first.

            ctx.tell(Action::Complete(None));

            Some(Action::Push(self.last.take()))
        } else {
            Some(Action::Complete(None))
        }
    }
}
