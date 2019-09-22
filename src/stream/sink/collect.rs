use crate::stream::{Action, Logic, StreamContext};

pub struct Collect<A> {
    entries: Option<Vec<A>>,
    pulled: bool,
}

impl<A> Collect<A> {
    pub fn new() -> Self {
        Self {
            entries: Some(Vec::new()),
            pulled: false,
        }
    }
}

impl<A> Logic<A, Vec<A>, ()> for Collect<A>
where
    A: 'static + Send,
{
    fn name(&self) -> &'static str {
        "Collect"
    }

    fn pulled(&mut self, ctx: &mut StreamContext<A, Vec<A>, ()>) -> Option<Action<Vec<A>, ()>> {
        self.pulled = true;

        None
    }

    fn pushed(
        &mut self,
        el: A,
        ctx: &mut StreamContext<A, Vec<A>, ()>,
    ) -> Option<Action<Vec<A>, ()>> {
        self.entries
            .as_mut()
            .expect("pantomime bug: Collect::entries is None")
            .push(el);

        Some(Action::Pull)
    }

    fn started(&mut self, ctx: &mut StreamContext<A, Vec<A>, ()>) -> Option<Action<Vec<A>, ()>> {
        Some(Action::Pull)
    }

    fn stopped(&mut self, ctx: &mut StreamContext<A, Vec<A>, ()>) -> Option<Action<Vec<A>, ()>> {
        if self.pulled {
            self.pulled = false;

            let entries = self
                .entries
                .take()
                .expect("pantomime bug: Collect::entries is None");

            // note that the action we're returning will be synchronously processed, followed
            // later by this Complete, i.e. the Push is processed first.

            ctx.tell(Action::Complete(None));

            Some(Action::Push(entries))
        } else {
            None
        }
    }

    fn forwarded(
        &mut self,
        msg: (),
        ctx: &mut StreamContext<A, Vec<A>, ()>,
    ) -> Option<Action<Vec<A>, ()>> {
        None
    }
}
