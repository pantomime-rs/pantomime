use crate::stream::internal::{
    InternalStreamCtl, LogicContainer, LogicContainerFacade, ProtectedStreamCtl,
};
use crate::stream::Logic;
use std::marker::PhantomData;

pub mod collect;
pub mod first;
pub mod for_each;
pub mod ignore;
pub mod last;

/// A `Sink` is a stage that accepts a single output, and outputs a
/// terminal value.
///
/// The logic supplied for a `Sink` will be pulled immediately when
/// the stream is spawned. Once the stream has finished, the logic's
/// stop handler will be invoked, and a conforming implementation must
/// at some point push out a single value.
pub struct Sink<A, Out>
where
    Out: 'static + Send,
{
    pub(in crate::stream) logic: Box<LogicContainerFacade<A, Out, InternalStreamCtl<Out>> + Send>,
    phantom: PhantomData<Out>,
}

impl<In, Out> Sink<In, Out>
where
    In: 'static + Send,
    Out: 'static + Send,
{
    pub fn new<Msg, L: Logic<In, Out, Msg>>(logic: L) -> Self
    where
        Msg: 'static + Send,
        L: 'static + Send,
    {
        Self {
            logic: Box::new(LogicContainer {
                logic,
                phantom: PhantomData,
            }),
            phantom: PhantomData,
        }
    }
}

// @TODO redo this
impl<A> Sink<A, ()>
where
    A: 'static + Send,
{
    pub fn first() -> Sink<A, Option<A>> {
        Sink::new(first::First::new())
    }

    pub fn last() -> Sink<A, Option<A>> {
        Sink::new(last::Last::new())
    }

    pub fn for_each<F: FnMut(A) -> ()>(for_each_fn: F) -> Sink<A, ()>
    where
        F: 'static + Send,
    {
        Sink::new(for_each::ForEach::new(for_each_fn))
    }
}
