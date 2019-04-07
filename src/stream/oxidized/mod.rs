use crate::stream::*;

/// A Producer produces elements when requested, often asynchronously.
///
/// This is an implementation detail of Pantomime streams and should
/// not be used directly.
pub trait Producer<A>: Sized + 'static + Send
where
    A: 'static + Send,
{
    /// Attach this producer to the provided consumer. The producer must
    /// transfer ownership of itself back to the consumer at some point
    /// in the future via its `started` method.
    #[must_use]
    fn attach<Consume: Consumer<A>>(self, consumer: Consume, context: &StreamContext)
        -> Trampoline;

    /// Pull in an element. At some time in the future, the producer must
    /// transfer ownership of itself back to the consumer at some point
    /// in the future via one of the following methods:
    ///
    /// - produced
    /// - completed
    /// - failed
    #[must_use]
    fn pull<Consume: Consumer<A>>(self, consumer: Consume) -> Trampoline;

    /// A request to cancel this producer (within the context of the
    /// provided consumer)
    ///
    /// The consumer will eventually receive a completed or failed
    /// invocation, but may receive additional publishes as well.
    #[must_use]
    fn cancel<Consume: Consumer<A>>(self, consumer: Consume) -> Trampoline;
}

pub trait Consumer<A>: 'static + Send + Sized
where
    A: 'static + Send,
{
    /// Signals that a producer has started. The consumer must then at some
    /// point in the future call one of the following:
    ///
    /// - pull
    /// - cancel
    #[must_use]
    fn started<Produce: Producer<A>>(self, producer: Produce) -> Trampoline;

    /// Indicates that the producer has produced an element. The consumer must
    /// then at some point in the future call one of the following:
    ///
    /// - pull
    /// - cancel
    #[must_use]
    fn produced<Produce: Producer<A>>(self, producer: Produce, element: A) -> Trampoline;

    /// Indicates that the producer has completed, i.e. it will not produce
    /// any more elements.
    #[must_use]
    fn completed(self) -> Trampoline;

    /// Indicates that the producer has failed, i.e. it has produced an
    /// error.
    #[must_use]
    fn failed(self, error: Error) -> Trampoline;
}
