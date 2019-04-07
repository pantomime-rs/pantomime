use crate::stream::flow::attached::*;
use crate::stream::*;
use std::marker::PhantomData;

pub struct Map<A, B, F: FnMut(A) -> B>
where
    A: 'static + Send,
    B: 'static + Send,
    F: 'static + Send,
{
    map: F,
    phantom: PhantomData<(A, B)>,
}

impl<A, B, F: FnMut(A) -> B> Map<A, B, F>
where
    A: 'static + Send,
    B: 'static + Send,
    F: 'static + Send,
{
    pub fn new(map: F) -> Self {
        Self {
            map,
            phantom: PhantomData,
        }
    }
}

impl<A, B, F: FnMut(A) -> B> AttachedLogic<A, B> for Map<A, B, F>
where
    A: 'static + Send,
    B: 'static + Send,
    F: 'static + Send,
{
    fn attach(&mut self, _: &StreamContext) {}

    fn produced(&mut self, elem: A) -> Action<B> {
        Action::Push((self.map)(elem))
    }

    fn pulled(&mut self) -> Action<B> {
        Action::Pull
    }

    fn completed(&mut self) -> Action<B> {
        Action::Complete
    }

    fn failed(&mut self, error: Error) -> Action<B> {
        Action::Fail(error)
    }
}
