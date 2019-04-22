use crate::actor::ActorRef;
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
        context: &StreamContext,
    ) -> Trampoline {
        consumer.started(self, context)
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
    fn started<Produce: Producer<A>>(self, producer: Produce, _: &StreamContext) -> Trampoline {
        producer.cancel(self)
    }

    fn produced<Produce: Producer<A>>(self, producer: Produce, _: A) -> Trampoline {
        producer.cancel(self)
    }

    fn completed(self) -> Trampoline {
        Trampoline::done()
    }

    fn failed(self, _: Error) -> Trampoline {
        // @TODO

        Trampoline::done()
    }
}

impl<A, B> AttachedLogic<A, B> for Disconnected
where
    A: 'static + Send,
    B: 'static + Send,
{
    fn attach(&mut self, _: &StreamContext) {}

    fn produced(&mut self, _: A) -> Action<B> {
        Action::Cancel
    }

    fn pulled(&mut self) -> Action<B> {
        Action::Cancel
    }

    fn completed(&mut self) -> Action<B> {
        Action::Fail(Error)
    }

    fn failed(&mut self, e: Error) -> Action<B> {
        Action::Fail(e)
    }
}

impl<A, B, M> DetachedLogic<A, B, M> for Disconnected
where
    A: 'static + Send,
    B: 'static + Send,
    M: 'static + Send,
{
    fn attach(
        &mut self,
        _: &StreamContext,
        _: &ActorRef<AsyncAction<B, M>>,
    ) -> Option<AsyncAction<B, M>> {
        Some(AsyncAction::Cancel)
    }

    fn forwarded(&mut self, _: M) -> Option<AsyncAction<B, M>> {
        Some(AsyncAction::Cancel)
    }

    fn produced(&mut self, _: A) -> Option<AsyncAction<B, M>> {
        Some(AsyncAction::Cancel)
    }

    fn pulled(&mut self) -> Option<AsyncAction<B, M>> {
        Some(AsyncAction::Cancel)
    }

    fn completed(&mut self) -> Option<AsyncAction<B, M>> {
        Some(AsyncAction::Complete)
    }

    fn failed(&mut self, error: Error) -> Option<AsyncAction<B, M>> {
        Some(AsyncAction::Fail(error))
    }
}
