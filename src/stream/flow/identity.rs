use crate::actor::{ActorRef, ActorSystemContext};
use crate::stream::flow::attached::*;
use crate::stream::flow::detached::*;
use crate::stream::*;

pub struct Identity;

impl<A> AttachedLogic<A, A> for Identity
where
    A: 'static + Send,
{
    fn attach(&mut self, _: &ActorSystemContext) {}

    fn produced(&mut self, elem: A) -> Action<A> {
        Action::Push(elem)
    }

    fn pulled(&mut self) -> Action<A> {
        Action::Pull
    }

    fn completed(self) -> Option<A> {
        None
    }

    fn failed(self, _: &Error) -> Option<A> {
        None
    }
}

impl<A> DetachedLogic<A, A, ()> for Identity
where
    A: 'static + Send,
{
    fn attach(&mut self, _: &ActorSystemContext) {}

    fn forwarded(&mut self, _: (), _: ActorRef<AsyncAction<A, ()>>) {}

    fn produced(&mut self, elem: A, actor_ref: ActorRef<AsyncAction<A, ()>>) {
        actor_ref.tell(AsyncAction::Push(elem));
    }

    fn pulled(&mut self, actor_ref: ActorRef<AsyncAction<A, ()>>) {
        actor_ref.tell(AsyncAction::Pull);
    }

    fn completed(&mut self, actor_ref: ActorRef<AsyncAction<A, ()>>) {
        actor_ref.tell(AsyncAction::Complete);
    }

    fn failed(&mut self, error: Error, actor_ref: ActorRef<AsyncAction<A, ()>>) {
        actor_ref.tell(AsyncAction::Fail(error));
    }
}
