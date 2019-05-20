use crate::actor::ActorRef;
use crate::stream::flow::attached::*;
use crate::stream::flow::detached::*;
use crate::stream::*;

pub struct Identity;

impl<A> AttachedLogic<A, A> for Identity
where
    A: 'static + Send,
{
    fn attach(&mut self, _: &StreamContext) {}

    fn produced(&mut self, elem: A) -> Action<A> {
        Action::Push(elem)
    }

    fn pulled(&mut self) -> Action<A> {
        Action::Pull
    }

    fn completed(&mut self) -> Action<A> {
        Action::Complete
    }

    fn failed(&mut self, error: Error) -> Action<A> {
        Action::Fail(error)
    }
}

impl<A> DetachedLogic<A, A, ()> for Identity
where
    A: 'static + Send,
{
    fn attach(
        &mut self,
        _: &StreamContext,
        _: &ActorRef<AsyncAction<A, ()>>,
    ) -> Option<AsyncAction<A, ()>> {
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
