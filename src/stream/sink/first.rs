use crate::stream::{Action, Logic, StreamContext};
use std::marker::PhantomData;

pub struct First<A> {
    first: Option<A>,
    pulled: bool,
}

impl<A> First<A> {
    pub fn new() -> Self {
        Self {
            first: None,
            pulled: false,
        }
    }
}

impl<A> Logic<A, Option<A>, ()> for First<A>
where
    A: 'static + Send,
{
    fn name(&self) -> &'static str {
        "First"
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
        if self.first.is_none() {
            self.first = Some(el);
        }

        Some(Action::Cancel)
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

            Some(Action::Push(self.first.take()))
        } else {
            Some(Action::Complete(None))
        }
    }
}
