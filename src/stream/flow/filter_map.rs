use crate::actor::ActorSystemContext;
use crate::stream::flow::attached::*;
use crate::stream::*;
use std::marker::PhantomData;

pub struct FilterMap<A, B, F: FnMut(A) -> Option<B>>
where
    A: 'static + Send,
    B: 'static + Send,
    F: 'static + Send,
{
    filter_map: F,
    phantom: PhantomData<(A, B)>,
}

impl<A, B, F: FnMut(A) -> Option<B>> FilterMap<A, B, F>
where
    A: 'static + Send,
    B: 'static + Send,
    F: 'static + Send,
{
    pub fn new(filter_map: F) -> Self {
        Self {
            filter_map,
            phantom: PhantomData,
        }
    }
}

impl<A, B, F: FnMut(A) -> Option<B>> AttachedLogic<A, B> for FilterMap<A, B, F>
where
    A: 'static + Send,
    B: 'static + Send,
    F: 'static + Send,
{
    fn attach(&mut self, context: ActorSystemContext) {}

    fn produced(&mut self, elem: A) -> Action<B> {
        match (self.filter_map)(elem) {
            Some(elem) => Action::Push(elem),
            None => Action::Pull,
        }
    }

    fn pulled(&mut self) -> Action<B> {
        Action::Pull
    }

    fn completed(self) -> Option<B> {
        None
    }

    fn failed(self, _: &Error) -> Option<B> {
        None
    }
}
