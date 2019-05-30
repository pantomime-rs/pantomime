use crate::dispatcher::{BoxedFn1In0Out, Trampoline};
use crate::stream::detached::*;
use crate::stream::*;
use std::marker::PhantomData;
use std::panic::UnwindSafe;

/// A `Source` that completes the provided logic's upstream, effectively
/// turning it into a `Source`.
///
/// This is useful when you want to use the simpler interface that `DetachedLogic`s
/// provide, particularly when dealing with asynchronous conditions.
pub struct DetachedLogicSource<A, M, L: DetachedLogic<A, (), M>>
where
    A: 'static + Send,
    M: 'static + Send,
    L: 'static + Send,
{
    logic: L,
    phantom: PhantomData<(A, M)>,
}

impl<A, M, L: DetachedLogic<A, (), M>> DetachedLogicSource<A, M, L>
where
    A: 'static + Send,
    M: 'static + Send,
    L: 'static + Send,
{
    pub fn new(logic: L) -> Self {
        Self { logic, phantom: PhantomData }
    }
}

impl<A, M, L: DetachedLogic<A, (), M>> Producer<A>
    for DetachedLogicSource<A, M, L>
where
    A: 'static + Send,
    M: 'static + Send,
    L: 'static + Send,
{
    fn attach<Consume: Consumer<A>>(
        self,
        consumer: Consume,
        context: &StreamContext,
    ) -> Trampoline {
        panic!()
    }

    fn pull<Consume: Consumer<A>>(mut self, consumer: Consume) -> Trampoline {
        panic!()
    }

    fn cancel<Consume: Consumer<A>>(self, consumer: Consume) -> Trampoline {
        panic!()
    }
}

