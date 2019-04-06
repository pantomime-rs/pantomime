use crate::actor::ActorSystemContext;
use crate::stream::*;
use std::marker::PhantomData;

/// Represents the actions that an attached
/// flow may take upon receiving a signal.
pub enum Action<B> {
    Cancel,
    Complete,
    Fail(Error),
    Pull,
    Push(B),
}

/// Attached represents a direct transformation of elements from type A to
/// type B, allowing for N to 1 and 1 to N transformations.
///
/// By direct, this means that upon receiving an element, the flow must always
/// immediately decide what to do, whether that is to push elements and send
/// control downstream, or request elements and send control upstream.
///
/// Attached flows are an essential building block and represent a simple way
/// to build powerful operators like scan, filter, filter_map, conflate.
///
/// It may be stateful, but if a significant amount of state is used,
/// implementations may wish to keep state in an inner `Box` type to to avoid
/// copying.
///
/// It does not allow its upstream and downstream to make independent
/// progress, however a detached stage may be inserted afterwards to enable
/// this behavior.
///
/// A string of attached flows is similar to the concept of operator fusion
/// in other prominent streaming libraries.
pub trait AttachedLogic<A, B>
where
    A: 'static + Send,
    B: 'static + Send,
{
    /// Invoked when the flow is first started. This can be useful to setup
    /// some state.
    fn attach(&mut self, context: ActorSystemContext);

    /// Upstream has produced an element, so now the flow must decide what
    /// action to take.
    fn produced(&mut self, elem: A) -> Action<B>;

    /// Downstream has requested an element, so now the flow must decide what
    /// action to take. A common action is to in turn pull upstream.
    fn pulled(&mut self) -> Action<B>;

    /// Indicates that the flow has been completed. It may choose to emit an
    /// element as a result.
    fn completed(self) -> Option<B>;

    /// Indicates that the flow has been completed. It may choose to emit an
    /// element as a result, in which downstream will be considered completed
    /// instead of failed.
    fn failed(self, error: &Error) -> Option<B>;
}

pub struct Attached<A, B, Logic: AttachedLogic<A, B>, Up: Producer<A>, Down: Consumer<B>>
where
    A: 'static + Send,
    B: 'static + Send,
{
    logic: Logic,
    upstream: Up,
    downstream: Down,
    phantom: PhantomData<(A, B)>,
}

impl<A, B, Logic: AttachedLogic<A, B>, Up> Attached<A, B, Logic, Up, Disconnected>
where
    A: 'static + Send,
    B: 'static + Send,
    Logic: 'static + Send,
    Up: Producer<A>,
{
    pub fn new(logic: Logic) -> impl FnOnce(Up) -> Self {
        move |upstream| Self {
            logic,
            upstream: upstream,
            downstream: Disconnected,
            phantom: PhantomData,
        }
    }
}

impl<A, B, Logic: AttachedLogic<A, B>, Up: Producer<A>, Down: Consumer<B>> Producer<B>
    for Attached<A, B, Logic, Up, Down>
where
    A: 'static + Send,
    B: 'static + Send,
    Logic: 'static + Send,
    Up: 'static + Send,
    Down: 'static + Send,
{
    fn attach<Consume: Consumer<B>>(
        mut self,
        consumer: Consume,
        context: ActorSystemContext,
    ) -> Trampoline {
        self.logic.attach(context.clone());

        self.upstream.attach(
            Attached {
                logic: self.logic,
                upstream: Disconnected,
                downstream: consumer,
                phantom: PhantomData,
            },
            context,
        )
    }

    fn pull<Consume: Consumer<B>>(mut self, consumer: Consume) -> Trampoline {
        match self.logic.pulled() {
            Action::Push(element) => consumer.produced(
                Attached {
                    logic: self.logic,
                    upstream: self.upstream,
                    downstream: Disconnected,
                    phantom: PhantomData,
                },
                element,
            ),

            Action::Pull => self.upstream.pull(Attached {
                logic: self.logic,
                upstream: Disconnected,
                downstream: consumer,
                phantom: PhantomData,
            }),

            Action::Cancel => self.upstream.cancel(Attached {
                logic: self.logic,
                upstream: Disconnected,
                downstream: consumer,
                phantom: PhantomData,
            }),

            Action::Fail(error) => self.upstream.cancel(PendingFinish {
                error: Some(error),
                downstream: consumer,
                phantom: PhantomData,
            }),

            Action::Complete => self.upstream.cancel(PendingFinish {
                error: None,
                downstream: consumer,
                phantom: PhantomData,
            }),
        }
    }

    fn cancel<Consume: Consumer<B>>(self, consumer: Consume) -> Trampoline {
        self.upstream.cancel(Attached {
            logic: self.logic,
            upstream: Disconnected,
            downstream: consumer,
            phantom: PhantomData,
        })
    }
}

impl<A, B, Logic: AttachedLogic<A, B>, Up: Producer<A>, Down: Consumer<B>> Consumer<A>
    for Attached<A, B, Logic, Up, Down>
where
    A: 'static + Send,
    B: 'static + Send,
    Logic: 'static + Send,
    Up: 'static + Send,
    Down: 'static + Send,
{
    fn started<Produce: Producer<A>>(self, producer: Produce) -> Trampoline {
        self.downstream.started(Attached {
            logic: self.logic,
            upstream: producer,
            downstream: Disconnected,
            phantom: PhantomData,
        })
    }

    fn produced<Produce: Producer<A>>(mut self, producer: Produce, element: A) -> Trampoline {
        match self.logic.produced(element) {
            Action::Push(element) => self.downstream.produced(
                Attached {
                    logic: self.logic,
                    upstream: producer,
                    downstream: Disconnected,
                    phantom: PhantomData,
                },
                element,
            ),

            Action::Pull => producer.pull(Attached {
                logic: self.logic,
                upstream: Disconnected,
                downstream: self.downstream,
                phantom: PhantomData,
            }),

            Action::Cancel => producer.cancel(Attached {
                logic: self.logic,
                upstream: Disconnected,
                downstream: self.downstream,
                phantom: PhantomData,
            }),

            Action::Fail(error) => producer.cancel(PendingFinish {
                error: Some(error),
                downstream: self.downstream,
                phantom: PhantomData,
            }),

            Action::Complete => producer.cancel(PendingFinish {
                error: None,
                downstream: self.downstream,
                phantom: PhantomData,
            }),
        }
    }

    fn completed(self) -> Trampoline {
        match self.logic.completed() {
            Some(element) => self.downstream.produced(Disconnected, element),

            None => self.downstream.completed(),
        }
    }

    fn failed(self, error: Error) -> Trampoline {
        match self.logic.failed(&error) {
            Some(element) => self.downstream.produced(Disconnected, element),

            None => self.downstream.failed(error),
        }
    }
}

struct PendingFinish<A, B, Down: Consumer<B>>
where
    A: 'static + Send,
    B: 'static + Send,
{
    error: Option<Error>,
    downstream: Down,
    phantom: PhantomData<(A, B)>,
}

impl<A, B, Down: Consumer<B>> PendingFinish<A, B, Down>
where
    A: 'static + Send,
    B: 'static + Send,
{
    fn finish(self) -> Trampoline {
        match self.error {
            Some(error) => self.downstream.failed(error),

            None => self.downstream.completed(),
        }
    }
}

impl<A, B, Down: Consumer<B>> Consumer<A> for PendingFinish<A, B, Down>
where
    A: 'static + Send,
    B: 'static + Send,
{
    fn started<Produce: Producer<A>>(self, producer: Produce) -> Trampoline {
        self.finish()
    }

    fn produced<Produce: Producer<A>>(mut self, producer: Produce, element: A) -> Trampoline {
        self.finish()
    }

    fn completed(self) -> Trampoline {
        self.finish()
    }

    fn failed(self, error: Error) -> Trampoline {
        self.finish()
    }
}
