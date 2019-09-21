use crate::stream::{Action, Logic, StreamContext};

struct CollectSinkLogic<A> {
    entries: Option<Vec<A>>,
    pulled: bool,
}

impl<A> CollectSinkLogic<A> {
    fn new() -> Self {
        Self {
            entries: Some(Vec::new()),
            pulled: false,
        }
    }
}

impl<A> Logic<A, Vec<A>, ()> for CollectSinkLogic<A>
where
    A: 'static + Send,
{
    fn name(&self) -> &'static str {
        "CollectSinkLogic"
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
