use crate::stream::flow::attached::*;
use crate::stream::*;
use std::marker::PhantomData;

pub struct TakeWhile<A, F: FnMut(&A) -> bool>
where
    A: 'static + Send,
    F: 'static + Send,
{
    func: F,
    phantom: PhantomData<A>,
}

impl<A, F: FnMut(&A) -> bool> TakeWhile<A, F>
where
    A: 'static + Send,
    F: 'static + Send,
{
    pub fn new(func: F) -> Self {
        Self {
            func,
            phantom: PhantomData,
        }
    }
}

impl<A, F: FnMut(&A) -> bool> AttachedLogic<A, A> for TakeWhile<A, F>
where
    A: 'static + Send,
    F: 'static + Send,
{
    fn attach(&mut self, _: &StreamContext) {}

    fn produced(&mut self, elem: A) -> Action<A> {
        if (self.func)(&elem) {
            Action::Push(elem)
        } else {
            Action::Cancel
        }
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
