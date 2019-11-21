use crate::stream::internal::{ContainedLogicImpl, IndividualLogic, LogicType};
use crate::stream::{Datagram, Logic};

mod collect;
mod first;
mod for_each;
mod ignore;
mod last;
mod udp;

pub use collect::Collect;
pub use first::First;
pub use for_each::ForEach;
pub use ignore::Ignore;
pub use last::Last;
pub use udp::Udp;

/// A `Sink` is a stage that accepts a single output, and outputs a
/// terminal value.
///
/// The logic supplied for a `Sink` will be pulled immediately when
/// the stream is spawned. Once the stream has finished, the logic's
/// stop handler will be invoked, and a conforming implementation must
/// at some point push out a single value.
///
/// For a sink to be correctly implemented, a few additional rules should
/// be followed.
///
/// * Sinks should always emit a single element if they are pulled
///
/// * If downstream cancels a sink without pulling it, it should
///   complete immediately.
///
/// * In practice, the streams implementation will always pull a sink,
///   but to pass the planned test suite for arbitrary logic instances,
///   these rules should be followed.
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
                LogicType::Spawnable(Box::new(IndividualLogic { logic }))
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

impl<A> Sink<A, Vec<A>>
where
    A: Send,
{
    pub fn collect() -> Self {
        Sink::new(Collect::new())
    }
}

impl Sink<Datagram, ()> {
    pub fn udp(socket: &mio::net::UdpSocket) -> Self {
        Self::new(Udp::new(socket))
    }
}
