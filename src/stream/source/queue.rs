use crate::actor::{Actor, ActorContext, ActorRef, Spawnable};
use crate::stream::source::Source;
use crate::stream::{Action, Logic, StreamContext};
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::mem;

pub enum QueuePushResult {
    Pushed,
    Dropped,
}

enum QueueCtl<A>
where
    A: Send,
{
    Initialize(ActorRef<Action<A, QueueLogicCtl<A>>>),
    Push(A, ActorRef<QueuePushResult>),
    Complete,
}

enum QueueLogicCtl<A>
where
    A: Send,
{
    Push(A, ActorRef<QueuePushResult>),
    Complete,
}

pub struct QueueRef<A>
where
    A: Send,
{
    actor_ref: ActorRef<QueueCtl<A>>,
}

impl<A> QueueRef<A>
where
    A: 'static + Send,
{
    fn new(actor_ref: ActorRef<QueueCtl<A>>) -> Self {
        Self { actor_ref }
    }

    pub fn push(&self, element: A, reply_to: ActorRef<QueuePushResult>) {
        self.actor_ref.tell(QueueCtl::Push(element, reply_to));
    }

    pub fn complete(&self) {
        self.actor_ref.tell(QueueCtl::Complete);
    }
}

enum QueueActorState<A>
where
    A: Send,
{
    Uninitialized(Vec<QueueCtl<A>>),
    Initialized(ActorRef<Action<A, QueueLogicCtl<A>>>),
}

struct QueueActor<A>
where
    A: Send,
{
    state: QueueActorState<A>,
}

impl<A> QueueActor<A>
where
    A: Send,
{
    fn new() -> Self {
        Self {
            state: QueueActorState::Uninitialized(Vec::new()),
        }
    }
}

impl<A> Actor<QueueCtl<A>> for QueueActor<A>
where
    A: 'static + Send,
{
    fn receive(&mut self, msg: QueueCtl<A>, ctx: &mut ActorContext<QueueCtl<A>>) {
        match msg {
            QueueCtl::Initialize(stage_ref) => {
                let mut state = QueueActorState::Initialized(stage_ref);

                mem::swap(&mut self.state, &mut state);

                match state {
                    QueueActorState::Uninitialized(mut buffer) => {
                        for msg in buffer.drain(..) {
                            self.receive(msg, ctx);
                        }
                    }

                    QueueActorState::Initialized(_) => {
                        // @TODO fail
                    }
                }
            }

            QueueCtl::Push(element, reply_to) => match &mut self.state {
                QueueActorState::Initialized(stage_ref) => {
                    stage_ref.tell(Action::Forward(QueueLogicCtl::Push(element, reply_to)));
                }

                QueueActorState::Uninitialized(buffer) => {
                    buffer.push(QueueCtl::Push(element, reply_to));
                }
            },

            QueueCtl::Complete => match &mut self.state {
                QueueActorState::Initialized(stage_ref) => {
                    stage_ref.tell(Action::Forward(QueueLogicCtl::Complete));
                }

                QueueActorState::Uninitialized(buffer) => {
                    buffer.push(QueueCtl::Complete);
                }
            },
        }
    }
}

impl<A, Msg> Spawnable<SourceQueue<A>, (QueueRef<A>, Source<A>)> for ActorContext<Msg>
where
    A: 'static + Send,
    Msg: 'static + Send,
{
    fn perform_spawn(&mut self, queue: SourceQueue<A>) -> (QueueRef<A>, Source<A>) {
        // QueueActor is a proxy that receives external messages and forwards them
        // to our logic. It's shutdown when our logic is dropped.

        let actor_ref = self.spawn(QueueActor::new());
        let source = Source::new(Queue::new(actor_ref.clone(), queue.capacity));
        let queue_ref = QueueRef::new(actor_ref);

        (queue_ref, source)
    }
}

pub struct SourceQueue<A>
where
    A: 'static + Send,
{
    capacity: usize,
    phantom: PhantomData<A>,
}

impl<A> SourceQueue<A>
where
    A: 'static + Send,
{
    pub fn new(capacity: usize) -> Self {
        SourceQueue {
            capacity,
            phantom: PhantomData,
        }
    }
}

pub struct Queue<A>
where
    A: 'static + Send,
{
    queue_actor_ref: ActorRef<QueueCtl<A>>,
    buffer: VecDeque<A>, // @TODO don't use VecDeque
    overflow_strategy: OverflowStrategy,
    pulled: bool,
}

enum OverflowStrategy {
    DropNewest,
    DropOldest,
}

impl<A> Queue<A>
where
    A: 'static + Send,
{
    fn new(queue_actor_ref: ActorRef<QueueCtl<A>>, capacity: usize) -> Self {
        Self {
            queue_actor_ref,
            buffer: VecDeque::with_capacity(capacity),
            overflow_strategy: OverflowStrategy::DropOldest,
            pulled: false,
        }
    }
}

impl<A> Logic<(), A, QueueLogicCtl<A>> for Queue<A>
where
    A: 'static + Send,
{
    fn buffer_size(&self) -> Option<usize> {
        Some(0)
    }

    fn name(&self) -> &'static str {
        "Queue"
    }

    fn started(
        &mut self,
        ctx: &mut StreamContext<(), A, QueueLogicCtl<A>>,
    ) -> Option<Action<A, QueueLogicCtl<A>>> {
        self.queue_actor_ref
            .tell(QueueCtl::Initialize(ctx.actor_ref()));
        None
    }

    fn pulled(
        &mut self,
        ctx: &mut StreamContext<(), A, QueueLogicCtl<A>>,
    ) -> Option<Action<A, QueueLogicCtl<A>>> {
        match self.buffer.pop_front() {
            Some(element) => Some(Action::Push(element)),

            None => {
                self.pulled = true;

                None
            }
        }
    }

    fn pushed(
        &mut self,
        el: (),
        ctx: &mut StreamContext<(), A, QueueLogicCtl<A>>,
    ) -> Option<Action<A, QueueLogicCtl<A>>> {
        None
    }

    fn stopped(
        &mut self,
        ctx: &mut StreamContext<(), A, QueueLogicCtl<A>>,
    ) -> Option<Action<A, QueueLogicCtl<A>>> {
        None
    }

    fn forwarded(
        &mut self,
        msg: QueueLogicCtl<A>,
        ctx: &mut StreamContext<(), A, QueueLogicCtl<A>>,
    ) -> Option<Action<A, QueueLogicCtl<A>>> {
        match msg {
            QueueLogicCtl::Push(element, reply_to) => {
                let len = self.buffer.len();

                if self.pulled {
                    // @TODO assert len == 0

                    self.pulled = false;

                    Some(Action::Push(element))
                } else if len == self.buffer.capacity() {
                    match self.overflow_strategy {
                        OverflowStrategy::DropOldest => {
                            reply_to.tell(QueuePushResult::Pushed);

                            self.buffer.pop_front();
                            self.buffer.push_back(element);

                            None
                        }

                        OverflowStrategy::DropNewest => {
                            reply_to.tell(QueuePushResult::Dropped);

                            None
                        }
                    }
                } else {
                    reply_to.tell(QueuePushResult::Pushed);

                    self.buffer.push_back(element);

                    None
                }
            }

            QueueLogicCtl::Complete => Some(Action::Complete(None)),
        }
    }
}

impl<A> Drop for Queue<A>
where
    A: Send,
{
    fn drop(&mut self) {
        // @TODO should fail if the logic failed
        self.queue_actor_ref.stop();
    }
}

use crate::actor::*;
use crate::stream::*;
use std::time::Duration;

#[test]
fn test2() {
    use crate::actor::*;
    use crate::stream::flow::Delay;
    use std::io::{Error, ErrorKind};

    struct TestReaper {
        n: usize,
    }

    impl TestReaper {
        fn new() -> Self {
            Self { n: 0 }
        }
    }

    impl Actor<usize> for TestReaper {
        fn receive(&mut self, value: usize, ctx: &mut ActorContext<usize>) {
            self.n += value;

            if self.n == 160 {
                ctx.stop();
            }
        }

        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<usize>) {
            match signal {
                Signal::Started => {
                    {
                        let actor_ref = ctx.actor_ref().clone();

                        ctx.schedule_thunk(Duration::from_secs(10), move || {
                            actor_ref
                                .fail(FailureError::new(Error::new(ErrorKind::Other, "failed")))
                        });
                    }

                    let (queue_ref, queue_src) = ctx.spawn(Source::queue(16));

                    queue_ref.push(10, ActorRef::empty());
                    queue_ref.push(20, ActorRef::empty());
                    queue_ref.push(30, ActorRef::empty());
                    queue_ref.complete();

                    let (stream_ref, result) = ctx.spawn(
                        queue_src
                            .map(|n| n * 2)
                            .via(Flow::new(Delay::new(Duration::from_millis(50))))
                            .to(Sink::last()),
                    );

                    ctx.watch(stream_ref, |_: StopReason| 100);
                    ctx.watch(result, |value: Option<usize>| value.unwrap_or_default());
                }

                _ => {}
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper::new()).is_ok());
}
