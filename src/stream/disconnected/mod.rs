use crate::actor::{ActorRef, ActorSystemContext};
use crate::dispatcher::Trampoline;
use crate::stream::attached::*;
use crate::stream::detached::*;
use crate::stream::oxidized::*;
use crate::stream::*;

pub struct Disconnected;

impl<A> Producer<A> for Disconnected
where
    A: 'static + Send,
{
    fn attach<Consume: Consumer<A>>(
        self,
        consumer: Consume,
        context: ActorSystemContext,
    ) -> Trampoline {
        consumer.started(self)
    }

    fn pull<Consume: Consumer<A>>(self, consumer: Consume) -> Trampoline {
        consumer.completed()
    }

    fn cancel<Consume: Consumer<A>>(self, consumer: Consume) -> Trampoline {
        consumer.completed()
    }
}

impl<A> Consumer<A> for Disconnected
where
    A: 'static + Send,
{
    fn started<Produce: Producer<A>>(self, producer: Produce) -> Trampoline {
        producer.cancel(self)
    }

    fn produced<Produce: Producer<A>>(mut self, producer: Produce, element: A) -> Trampoline {
        producer.cancel(self)
    }

    fn completed(self) -> Trampoline {
        Trampoline::done()
    }

    fn failed(self, error: Error) -> Trampoline {
        // @TODO

        Trampoline::done()
    }
}

impl<A, B> AttachedLogic<A, B> for Disconnected
where
    A: 'static + Send,
    B: 'static + Send,
{
    fn attach(&mut self, _: &ActorSystemContext) {}

    fn produced(&mut self, elem: A) -> Action<B> {
        Action::Cancel
    }

    fn pulled(&mut self) -> Action<B> {
        Action::Cancel
    }

    fn completed(self) -> Option<B> {
        None
    }

    fn failed(self, _: &Error) -> Option<B> {
        None
    }
}

impl<A, B, M> DetachedLogic<A, B, M> for Disconnected
where
    A: 'static + Send,
    B: 'static + Send,
    M: 'static + Send,
{
    fn attach(&mut self, _: &ActorSystemContext) {}

    fn forwarded(&mut self, _: M, actor_ref: ActorRef<AsyncAction<B, M>>) {
        actor_ref.tell(AsyncAction::Complete);
    }

    fn produced(&mut self, elem: A, actor_ref: ActorRef<AsyncAction<B, M>>) {
        actor_ref.tell(AsyncAction::Complete);
    }

    fn pulled(&mut self, actor_ref: ActorRef<AsyncAction<B, M>>) {
        actor_ref.tell(AsyncAction::Complete);
    }

    fn completed(&mut self, actor_ref: ActorRef<AsyncAction<B, M>>) {
        actor_ref.tell(AsyncAction::Complete);
    }

    fn failed(&mut self, error: Error, actor_ref: ActorRef<AsyncAction<B, M>>) {
        actor_ref.tell(AsyncAction::Fail(error));
    }
}
