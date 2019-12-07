use crate::actor::{Actor, ActorContext, ActorRef, Spawnable};
use crate::stream::{
    Action, Logic, LogicEvent, OverflowStrategy, PushResult, Source, StageRef, StreamContext,
};
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::mem;

enum QueueCtl<A>
where
    A: Send,
{
    Initialize(StageRef<QueueLogicCtl<A>>),
    Push(A, ActorRef<PushResult>),
    Complete,
}

pub enum QueueLogicCtl<A>
where
    A: Send,
{
    Push(A, ActorRef<PushResult>),
    Complete,
}

#[derive(Clone)]
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

    pub fn push(&self, element: A, reply_to: ActorRef<PushResult>) {
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
    Initialized(StageRef<QueueLogicCtl<A>>),
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

impl<A> Actor for QueueActor<A>
where
    A: 'static + Send,
{
    type Msg = QueueCtl<A>;

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
                    stage_ref.tell(QueueLogicCtl::Push(element, reply_to));
                }

                QueueActorState::Uninitialized(buffer) => {
                    buffer.push(QueueCtl::Push(element, reply_to));
                }
            },

            QueueCtl::Complete => match &mut self.state {
                QueueActorState::Initialized(stage_ref) => {
                    stage_ref.tell(QueueLogicCtl::Complete);
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
        let source = Source::new(Queue::new(
            actor_ref.clone(),
            queue.capacity,
            queue.overflow_strategy,
        ));
        let queue_ref = QueueRef::new(actor_ref);

        (queue_ref, source)
    }
}

pub struct SourceQueue<A>
where
    A: 'static + Send,
{
    capacity: usize,
    overflow_strategy: OverflowStrategy,
    phantom: PhantomData<A>,
}

impl<A> SourceQueue<A>
where
    A: 'static + Send,
{
    pub fn new(capacity: usize) -> Self {
        SourceQueue {
            capacity,
            overflow_strategy: OverflowStrategy::DropOldest,
            phantom: PhantomData,
        }
    }

    pub fn with_overflow_strategy(mut self, strategy: OverflowStrategy) -> Self {
        self.overflow_strategy = strategy;
        self
    }
}

pub struct Queue<A>
where
    A: 'static + Send,
{
    queue_actor_ref: ActorRef<QueueCtl<A>>,
    buffer: VecDeque<A>,
    overflow_strategy: OverflowStrategy,
    pulled: bool,
    completed: bool,
}

impl<A> Queue<A>
where
    A: 'static + Send,
{
    fn new(
        queue_actor_ref: ActorRef<QueueCtl<A>>,
        capacity: usize,
        overflow_strategy: OverflowStrategy,
    ) -> Self {
        Self {
            queue_actor_ref,
            buffer: VecDeque::with_capacity(capacity),
            overflow_strategy,
            pulled: false,
            completed: false,
        }
    }
}

impl<A> Logic<(), A> for Queue<A>
where
    A: 'static + Send,
{
    type Ctl = QueueLogicCtl<A>;

    fn buffer_size(&self) -> Option<usize> {
        Some(0)
    }

    fn name(&self) -> &'static str {
        "Queue"
    }

    fn receive(
        &mut self,
        msg: LogicEvent<(), Self::Ctl>,
        ctx: &mut StreamContext<(), A, Self::Ctl>,
    ) -> Action<A, Self::Ctl> {
        match msg {
            LogicEvent::Pulled => match self.buffer.pop_front() {
                Some(element) => Action::Push(element),

                None if self.completed => Action::Complete(None),

                None => {
                    self.pulled = true;

                    Action::None
                }
            },

            LogicEvent::Forwarded(QueueLogicCtl::Push(element, reply_to)) => {
                if self.completed {
                    Action::None
                } else if self.pulled {
                    // @TODO assert len == 0

                    self.pulled = false;

                    reply_to.tell(PushResult::Pushed);

                    Action::Push(element)
                } else if self.buffer.len() == self.buffer.capacity() {
                    match self.overflow_strategy {
                        OverflowStrategy::DropOldest => {
                            reply_to.tell(PushResult::Pushed);

                            self.buffer.pop_front();
                            self.buffer.push_back(element);

                            Action::None
                        }

                        OverflowStrategy::DropNewest => {
                            reply_to.tell(PushResult::Dropped);

                            Action::None
                        }
                    }
                } else {
                    reply_to.tell(PushResult::Pushed);

                    self.buffer.push_back(element);

                    Action::None
                }
            }

            LogicEvent::Forwarded(QueueLogicCtl::Complete) => {
                self.completed = true;

                if self.buffer.is_empty() {
                    Action::Complete(None)
                } else {
                    Action::None
                }
            }

            LogicEvent::Cancelled => {
                self.buffer.clear();
                self.completed = true;

                Action::Complete(None)
            }

            LogicEvent::Started => {
                self.queue_actor_ref
                    .tell(QueueCtl::Initialize(ctx.stage_ref()));

                Action::None
            }

            LogicEvent::Pushed(()) | LogicEvent::Stopped => Action::None,
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
