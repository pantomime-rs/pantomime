use crate::stream::{Action, Logic, StreamContext};
use std::marker::PhantomData;

pub struct Scan<A, B, F: FnMut(B, A) -> B>
where
    A: Send,
    B: Clone + Send,
{
    scan_fn: F,
    phantom: PhantomData<(A, B)>,
    sent: bool,
    last: Option<B>,
}

impl<A, B, F: FnMut(B, A) -> B> Scan<A, B, F>
where
    A: Send,
    B: Clone + Send,
{
    pub fn new(zero: B, scan_fn: F) -> Self {
        Self {
            scan_fn,
            phantom: PhantomData,
            sent: false,
            last: Some(zero),
        }
    }
}

impl<A, B, F: FnMut(B, A) -> B> Logic<A, B, ()> for Scan<A, B, F>
where
    A: 'static + Send,
    B: 'static + Clone + Send,
{
    fn name(&self) -> &'static str {
        "Scan"
    }

    fn pulled(&mut self, ctx: &mut StreamContext<A, B, ()>) -> Option<Action<B, ()>> {
        if self.sent {
            Some(Action::Pull)
        } else {
            self.sent = true;

            let last = self
                .last
                .as_ref()
                .expect("pantomime bug: Scan::last is None")
                .clone();

            Some(Action::Push(last))
        }
    }

    fn pushed(&mut self, el: A, ctx: &mut StreamContext<A, B, ()>) -> Option<Action<B, ()>> {
        let last = self.last.take().expect("pantomime bug: Scan::last is None");
        let next = (self.scan_fn)(last, el);
        let action = Some(Action::Push(next.clone()));

        self.last = Some(next);

        action
    }
}
