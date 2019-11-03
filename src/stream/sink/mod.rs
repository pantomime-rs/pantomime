use crate::stream::internal::{
    ContainedLogicImpl, IndividualLogic, InternalStreamCtl, LogicContainerFacade, LogicType,
};
use crate::stream::Logic;
use std::marker::PhantomData;

mod collect;
mod first;
mod for_each;
mod ignore;
mod last;

pub use collect::Collect;
pub use first::First;
pub use for_each::ForEach;
pub use ignore::Ignore;
pub use last::Last;

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
    pub(in crate::stream) logic: LogicType<A, Out>,
}

impl<In, Out> Sink<In, Out>
where
    In: 'static + Send,
    Out: 'static + Send,
{
    pub fn new<L: Logic<In, Out>>(logic: L) -> Self
    where
        L: 'static + Send,
        L::Ctl: 'static + Send,
    {
        Self {
            logic: if logic.fusible() {
                LogicType::Fusible(Box::new(ContainedLogicImpl::new(logic)))
            } else {
                LogicType::Spawnable(Box::new(IndividualLogic {
                    logic,
                    fused: false,
                }))
            },
        }
    }
}

impl<A> Sink<A, Option<A>>
where
    A: Send,
{
    pub fn first() -> Self {
        Sink::new(First::new())
    }

    pub fn last() -> Self {
        Sink::new(Last::new())
    }
}

impl<A> Sink<A, ()>
where
    A: 'static + Send,
{
    pub fn for_each<F: FnMut(A) -> ()>(for_each_fn: F) -> Self
    where
        F: 'static + Send,
    {
        Sink::new(ForEach::new(for_each_fn))
    }

    pub fn ignore() -> Self {
        Sink::new(Ignore::new())
    }
}
