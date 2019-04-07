use crate::actor::ActorSystemContext;
use crate::stream::flow::attached::*;
use crate::stream::*;
use std::marker::PhantomData;

pub struct Filter<A, F: FnMut(&A) -> bool>
where
    A: 'static + Send,
    F: 'static + Send,
{
    filter: F,
    phantom: PhantomData<(A)>,
}

impl<A, F: FnMut(&A) -> bool> Filter<A, F>
where
    A: 'static + Send,
    F: 'static + Send,
{
    pub fn new(filter: F) -> Self {
        Self {
            filter,
            phantom: PhantomData,
        }
    }
}

impl<A, F: FnMut(&A) -> bool> AttachedLogic<A, A> for Filter<A, F>
where
    A: 'static + Send,
    F: 'static + Send,
{
    fn attach(&mut self, _: &ActorSystemContext) {}

    fn produced(&mut self, elem: A) -> Action<A> {
        if (self.filter)(&elem) {
            Action::Push(elem)
        } else {
            Action::Pull
        }
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
