use crate::stream::internal::{ContainedLogicImpl, IndividualLogic, LogicType, UnionLogic};
use crate::stream::{Logic, Sink};
use std::any::Any;
use std::cell::RefCell;

mod delay;
mod filter;
mod filter_map;
mod fold;
mod fused;
mod identity;
mod map;
mod map_concat;
mod scan;
mod take_while;

pub use self::delay::Delay;
pub use self::filter::Filter;
pub use self::filter_map::FilterMap;
pub use self::fold::Fold;
pub use self::identity::Identity;
pub use self::map::Map;
pub use self::map_concat::MapConcat;
pub use self::scan::Scan;
pub use self::take_while::TakeWhile;

pub(in crate::stream) use fused::Fused;

pub struct Flow<A, B> {
    pub(in crate::stream) logic: LogicType<A, B>,
    pub(in crate::stream) empty: bool,
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
            logic: if logic.fusible() {
                LogicType::Fusible(Box::new(ContainedLogicImpl::new(logic)))
            } else {
                LogicType::Spawnable(Box::new(IndividualLogic { logic }))
            },
            empty: false,
        }
    }

    pub fn to<Out: Send>(self, sink: Sink<B, Out>) -> Sink<A, Out> {
        match (self.logic, sink.logic) {
            (LogicType::Fusible(upstream), LogicType::Fusible(downstream)) => Sink {
                logic: LogicType::Fusible(Box::new(ContainedLogicImpl::new(fused::Fused::new(
                    upstream, downstream,
                )))),
            },

            (LogicType::Fusible(upstream), LogicType::Spawnable(downstream)) => Sink {
                logic: LogicType::Spawnable(Box::new(UnionLogic {
                    upstream: upstream.into_facade(),
                    downstream,
                })),
            },

            (LogicType::Spawnable(upstream), LogicType::Fusible(downstream)) => Sink {
                logic: LogicType::Spawnable(Box::new(UnionLogic {
                    upstream,
                    downstream: downstream.into_facade(),
                })),
            },

            (LogicType::Spawnable(upstream), LogicType::Spawnable(downstream)) => Sink {
                logic: LogicType::Spawnable(Box::new(UnionLogic {
                    upstream,
                    downstream,
                })),
            },
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

            cast(flow).expect("pantomime bug: stream::flow::cast (A eq B) failure")
        } else if flow.empty {
            // we know that B and C are the same

            cast(self).expect("pantomime bug: stream::flow::cast (B eq C) failure")
        } else {
            match (self.logic, flow.logic) {
                (LogicType::Fusible(upstream), LogicType::Fusible(downstream)) => Flow {
                    logic: LogicType::Fusible(Box::new(ContainedLogicImpl::new(
                        fused::Fused::new(upstream, downstream),
                    ))),
                    empty: false,
                },

                (LogicType::Fusible(upstream), LogicType::Spawnable(downstream)) => Flow {
                    logic: LogicType::Spawnable(Box::new(UnionLogic {
                        upstream: upstream.into_facade(),
                        downstream,
                    })),
                    empty: false,
                },

                (LogicType::Spawnable(upstream), LogicType::Fusible(downstream)) => Flow {
                    logic: LogicType::Spawnable(Box::new(UnionLogic {
                        upstream,
                        downstream: downstream.into_facade(),
                    })),
                    empty: false,
                },

                (LogicType::Spawnable(upstream), LogicType::Spawnable(downstream)) => Flow {
                    logic: LogicType::Spawnable(Box::new(UnionLogic {
                        upstream,
                        downstream,
                    })),
                    empty: false,
                },
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

    pub fn filter_map<C, F: FnMut(B) -> Option<C>>(self, filter_map: F) -> Flow<A, C>
    where
        C: 'static + Send,
        F: 'static + Send,
    {
        self.via(Flow::from_logic(FilterMap::new(filter_map)))
    }

    pub fn fold<C, F: FnMut(C, B) -> C>(self, zero: C, fold_fn: F) -> Flow<A, C>
    where
        F: 'static + Send,
        C: 'static + Clone + Send,
    {
        self.via(Flow::from_logic(Fold::new(zero, fold_fn)))
    }

    pub fn map<C, F: FnMut(B) -> C>(self, map_fn: F) -> Flow<A, C>
    where
        C: 'static + Send,
        F: 'static + Send,
    {
        self.via(Flow::from_logic(Map::new(map_fn)))
    }

    pub fn map_concat<C, I: Iterator<Item = C>, F: FnMut(B) -> I + Send>(
        self,
        map_concat: F,
    ) -> Flow<A, C>
    where
        C: 'static + Send,
        F: 'static + Send,
        I: 'static + Send,
    {
        self.via(Flow::from_logic(MapConcat::new(map_concat)))
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

impl<A> Default for Flow<A, A>
where
    A: 'static + Send,
{
    fn default() -> Self {
        Self::new()
    }
}
