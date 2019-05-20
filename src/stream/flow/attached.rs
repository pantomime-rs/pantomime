use crate::stream::oxidized::*;
use crate::stream::*;
use std::marker::PhantomData;

const MAX_CYCLES: usize = 10;

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
/// Logic must not panic. If logic depends on the user supplying a function,
/// e.g. `map`, the logic should ensure that it is `UnwindSafe`. Helper
/// functions are provided to allow translation of a panic into a stream
/// failure.
///
/// A string of attached flows that are proceeded by a detached stage is
/// similar to the concept of operator fusion in other prominent streaming
/// libraries.
pub trait AttachedLogic<A, B>
where
    A: 'static + Send,
    B: 'static + Send,
{
    /// Invoked when the flow is first started. This can be useful to setup
    /// some state.
    fn attach(&mut self, context: &StreamContext);

    /// Upstream has produced an element, so now the flow must decide what
    /// action to take.
    fn produced(&mut self, elem: A) -> Action<B>;

    /// Downstream has requested an element, so now the flow must decide what
    /// action to take. A common action is to in turn pull upstream.
    fn pulled(&mut self) -> Action<B>;

    /// Indicates that upstream has completed.
    ///
    /// Typically, this should forward downstream by returning `Action::Complete`
    /// but the logic may choose to emit other actions instead.
    ///
    /// It must not, however, emit a Pull or Cancel as upstream is finished.
    fn completed(&mut self) -> Action<B>;

    /// Indicates that upstream has failed.
    ///
    /// Typically, this should be forwarded downstream with `Action::Fail`
    /// but the logic may choose to emit other actions.
    ///
    /// It must not, however, emit a Pull or Cancel as upstream is finished.
    fn failed(&mut self, error: Error) -> Action<B>;
}

pub struct Attached<A, B, Logic: AttachedLogic<A, B>, Up: Producer<A>, Down: Consumer<B>>
where
    A: 'static + Send,
    B: 'static + Send,
{
    logic: Logic,
    connected: bool,
    cycles: usize,
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
            connected: false,
            cycles: 0,
            upstream,
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
        self,
        consumer: Consume,
        context: &StreamContext,
    ) -> Trampoline {
        self.upstream.attach(
            Attached {
                logic: self.logic,
                connected: true,
                cycles: self.cycles,
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
                    connected: self.connected,
                    cycles: self.cycles,
                    upstream: self.upstream,
                    downstream: Disconnected,
                    phantom: PhantomData,
                },
                element,
            ),

            Action::Pull if self.cycles == MAX_CYCLES => Trampoline::bounce(|| {
                self.upstream.pull(Attached {
                    logic: self.logic,
                    connected: self.connected,
                    cycles: 0,
                    upstream: Disconnected,
                    downstream: consumer,
                    phantom: PhantomData,
                })
            }),

            Action::Pull => self.upstream.pull(Attached {
                logic: self.logic,
                connected: self.connected,
                cycles: self.cycles + 1,
                upstream: Disconnected,
                downstream: consumer,
                phantom: PhantomData,
            }),

            Action::Cancel if self.connected => self.upstream.cancel(Attached {
                logic: self.logic,
                connected: self.connected,
                cycles: self.cycles,
                upstream: Disconnected,
                downstream: consumer,
                phantom: PhantomData,
            }),

            Action::Cancel => self.downstream.failed(Error), // @TODO error type

            Action::Fail(error) => {
                if self.connected {
                    self.upstream.cancel(PendingFinish {
                        error: Some(error),
                        downstream: consumer,
                        phantom: PhantomData,
                    })
                } else {
                    self.downstream.failed(error)
                }
            }

            Action::Complete if self.connected => self.upstream.cancel(PendingFinish {
                error: None,
                downstream: consumer,
                phantom: PhantomData,
            }),

            Action::Complete => self.downstream.completed(),
        }
    }

    fn cancel<Consume: Consumer<B>>(self, consumer: Consume) -> Trampoline {
        self.upstream.cancel(Attached {
            logic: self.logic,
            connected: self.connected,
            cycles: self.cycles,
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
    fn started<Produce: Producer<A>>(
        self,
        producer: Produce,
        context: &StreamContext,
    ) -> Trampoline {
        self.downstream.started(
            Attached {
                logic: self.logic,
                connected: self.connected,
                cycles: self.cycles,
                upstream: producer,
                downstream: Disconnected,
                phantom: PhantomData,
            },
            context,
        )
    }

    fn produced<Produce: Producer<A>>(mut self, producer: Produce, element: A) -> Trampoline {
        match self.logic.produced(element) {
            Action::Push(element) => self.downstream.produced(
                Attached {
                    logic: self.logic,
                    connected: self.connected,
                    cycles: self.cycles,
                    upstream: producer,
                    downstream: Disconnected,
                    phantom: PhantomData,
                },
                element,
            ),

            Action::Pull if self.cycles == MAX_CYCLES => Trampoline::bounce(|| {
                producer.pull(Attached {
                    logic: self.logic,
                    connected: self.connected,
                    cycles: 0,
                    upstream: Disconnected,
                    downstream: self.downstream,
                    phantom: PhantomData,
                })
            }),

            Action::Pull => producer.pull(Attached {
                logic: self.logic,
                connected: self.connected,
                cycles: self.cycles,
                upstream: Disconnected,
                downstream: self.downstream,
                phantom: PhantomData,
            }),

            Action::Cancel if self.connected => producer.cancel(Attached {
                logic: self.logic,
                connected: self.connected,
                cycles: self.cycles,
                upstream: Disconnected,
                downstream: self.downstream,
                phantom: PhantomData,
            }),

            Action::Cancel => self.downstream.failed(Error), // @TODO error type

            Action::Fail(error) => {
                if self.connected {
                    self.upstream.cancel(PendingFinish {
                        error: Some(error),
                        downstream: self.downstream,
                        phantom: PhantomData,
                    })
                } else {
                    self.downstream.failed(error)
                }
            }

            Action::Complete if self.connected => producer.cancel(PendingFinish {
                error: None,
                downstream: self.downstream,
                phantom: PhantomData,
            }),

            Action::Complete => self.downstream.completed(),
        }
    }

    fn completed(mut self) -> Trampoline {
        match self.logic.completed() {
            Action::Push(element) => self.downstream.produced(
                Attached {
                    logic: self.logic,
                    connected: false,
                    cycles: self.cycles,
                    upstream: Disconnected,
                    downstream: Disconnected,
                    phantom: PhantomData,
                },
                element,
            ),

            Action::Pull => self.downstream.failed(Error), // @TODO error type

            Action::Cancel => self.downstream.failed(Error), // @TODO error type

            Action::Fail(error) => self.downstream.failed(error),

            Action::Complete => self.downstream.completed(),
        }
    }

    fn failed(mut self, error: Error) -> Trampoline {
        match self.logic.failed(error) {
            Action::Push(element) => self.downstream.produced(
                Attached {
                    logic: self.logic,
                    connected: false,
                    cycles: self.cycles,
                    upstream: Disconnected,
                    downstream: Disconnected,
                    phantom: PhantomData,
                },
                element,
            ),

            Action::Pull => self.downstream.failed(Error), // @TODO error type

            Action::Cancel => self.downstream.failed(Error), // @TODO error type

            Action::Fail(error) => self.downstream.failed(error),

            Action::Complete => self.downstream.completed(),
        }
    }
}

impl<A, B, Logic: AttachedLogic<A, B>, Up: Producer<A>, Down: Consumer<B>> Stage<B>
    for Attached<A, B, Logic, Up, Down>
where
    A: 'static + Send,
    B: 'static + Send,
    Logic: 'static + Send,
{
}

impl<A, B, Logic: AttachedLogic<A, B>, Up: Producer<A>, Down: Consumer<B>> Flow<A, B>
    for Attached<A, B, Logic, Up, Down>
where
    A: 'static + Send,
    B: 'static + Send,
    Logic: 'static + Send,
{
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
    fn started<Produce: Producer<A>>(self, _: Produce, _: &StreamContext) -> Trampoline {
        self.finish()
    }

    fn produced<Produce: Producer<A>>(self, _: Produce, _: A) -> Trampoline {
        self.finish()
    }

    fn completed(self) -> Trampoline {
        self.finish()
    }

    fn failed(self, _: Error) -> Trampoline {
        self.finish()
    }
}
