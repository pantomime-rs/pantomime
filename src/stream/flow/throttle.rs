use crate::actor::ActorRef;
use crate::stream::flow::detached::*;
use crate::stream::*;
use std::marker::PhantomData;

struct Throttle<A> {
    buffer: Vec<A>,
}

impl<A> DetachedLogic<A, A, ()> for Throttle<A>
where
    A: 'static + Send,
{
    fn attach(
        &mut self,
        _: &StreamContext,
        actor_ref: &ActorRef<AsyncAction<A, ()>>,
    ) -> Option<AsyncAction<A, ()>> {
        // schedule a tick at the throttle interval
        //

        None
    }

    fn forwarded(&mut self, _: ()) -> Option<AsyncAction<A, ()>> {
        None
    }

    fn produced(&mut self, elem: A) -> Option<AsyncAction<A, ()>> {
        Some(AsyncAction::Push(elem))
    }

    fn pulled(&mut self) -> Option<AsyncAction<A, ()>> {
        Some(AsyncAction::Pull)
    }

    fn completed(&mut self) -> Option<AsyncAction<A, ()>> {
        Some(AsyncAction::Complete)
    }

    fn failed(&mut self, error: Error) -> Option<AsyncAction<A, ()>> {
        Some(AsyncAction::Fail(error))
    }
}
