use crate::stream::internal::{LogicContainer, LogicContainerFacade, ProtectedStreamCtl};
use crate::stream::Logic;
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
    pub(in crate::stream) logic: Box<LogicContainerFacade<A, B, ProtectedStreamCtl> + Send>,
}

impl<A, B> Flow<A, B>
where
    A: 'static + Send,
    B: 'static + Send,
{
    pub fn new<Msg, L: Logic<A, B, Msg>>(logic: L) -> Self
    where
        Msg: 'static + Send,
        L: 'static + Send,
    {
        Self {
            logic: Box::new(LogicContainer {
                logic,
                phantom: PhantomData,
            }),
        }
    }

    pub fn map<F: FnMut(A) -> B>(map_fn: F) -> Self
    where
        F: 'static + Send,
    {
        Self::new(Map::new(map_fn))
    }

    pub fn scan<F: FnMut(B, A) -> B>(zero: B, scan_fn: F) -> Self
    where
        F: 'static + Send,
        B: Clone,
    {
        Self::new(Scan::new(zero, scan_fn))
    }
}

impl<A> Flow<A, A>
where
    A: 'static + Send,
{
    pub fn filter<F: FnMut(&A) -> bool>(filter_fn: F) -> Self
    where
        F: 'static + Send,
    {
        Self::new(Filter::new(filter_fn))
    }
}
