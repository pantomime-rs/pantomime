use crate::stream::internal::{IndividualLogic, LogicContainerFacade, UnionLogic};
use crate::stream::{Logic, Sink, Source};
use std::any::Any;
use std::cell::RefCell;
use std::marker::PhantomData;

mod delay;
mod filter;
mod identity;
mod map;
mod scan;
mod take_while;

pub use self::delay::Delay;
pub use self::filter::Filter;
pub use self::identity::Identity;
pub use self::map::Map;
pub use self::scan::Scan;

pub struct Flow<A, B> {
    pub(in crate::stream) logic: Box<dyn LogicContainerFacade<A, B> + Send>,
    pub(in crate::stream) empty: bool
}

impl<A, B> Flow<A, B>
where
    A: 'static + Send,
    B: 'static + Send,
{
    pub fn from_logic<L: Logic<A, B>>(logic: L) -> Self
    where
        L: 'static + Send,
        L::Ctl: 'static + Send,
    {
        Self {
            logic: Box::new(IndividualLogic {
                logic,
                fused: false
            }),
            empty: false
        }
    }

    /// Fuse this flow, meaning this flow and all of its stages
    /// will be executed by upstream, i.e. in the same actor.
    ///
    /// This can often by much more performant for stages that
    /// are computationally light weight -- those where the
    /// message passing overhead outweighs the actual work.
    pub fn fuse(self) -> Self {
        Self {
            logic: self.logic.fuse(),
            empty: self.empty
        }
    }

    pub fn to<Out: Send>(self, sink: Sink<B, Out>) -> Sink<A, Out> {
        Sink {
            logic: Box::new(UnionLogic { upstream: self.logic, downstream: sink.logic }),
        }
    }

    pub fn via<C>(self, flow: Flow<B, C>) -> Flow<A, C>
    where
        C: 'static + Send,
    {
        if self.empty && flow.empty {
            cast(flow).expect("pantomime bug: stream::flow::cast failure")
        } else if self.empty {
            // we know that A and B are the same

            cast(flow).expect("pantomime bug: stream::flow::cast failure")
        } else if flow.empty {
            // we know that B and C are the same

            cast(self).expect("pantomime bug: stream::flow::cast failure")
        } else {
            Flow {
                logic: Box::new(UnionLogic {
                    upstream: self.logic,
                    downstream: flow.logic,
                }),
                empty: false
            }
        }
    }

    pub fn scan<C, F: FnMut(C, B) -> C>(self, zero: C, scan_fn: F) -> Flow<A, C>
    where
        F: 'static + Send,
        C: 'static + Clone + Send,
    {
        self.via(Flow::from_logic(Scan::new(zero, scan_fn)))
    }

    pub fn filter<F: FnMut(&B) -> bool>(self, filter: F) -> Self
    where
        F: 'static + Send,
    {
        self.via(Flow::from_logic(Filter::new(filter)))
    }

    pub fn map<C, F: FnMut(B) -> C>(self, map_fn: F) -> Flow<A, C>
    where
        C: 'static + Send,
        F: 'static + Send,
    {
        self.via(Flow::from_logic(Map::new(map_fn)))
    }
}

fn cast<In: 'static, Out: 'static>(value: In) -> Option<Out> {
    let cell = RefCell::new(Some(value));
    let cell = &cell as &dyn Any;
    let cell = cell.downcast_ref::<RefCell<Option<Out>>>()?;
    let result = cell.borrow_mut().take()?;

    Some(result)
}

impl<A> Flow<A, A>
where
    A: 'static + Send,
{
    pub fn new() -> Self {
        let mut flow = Flow::from_logic(Identity);
        flow.empty = true;
        flow
    }
}

impl<A> Flow<(), A>
where
    A: 'static + Send,
{
    pub fn from_source(source: Source<A>) -> Self {
        unimplemented!()
    }
}
