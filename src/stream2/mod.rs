use crate::actor::{Actor, ActorContext, ActorSpawnContext, ActorRef, Signal, StopReason, SystemActorRef, Watchable};
use crossbeam::atomic::AtomicCell;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::mem;
use std::time::Duration;
use std::sync::Arc;

enum Action<A, Msg> {
    Cancel,
    Complete,
    Pull,
    Push(A),
    Forward(Msg),
}

/// All stages are backed by a particular `Logic` that defines
/// the behavior of the stage.
///
/// To preserve the execution gurantees, logic implementations must
/// follow these rules:
///
/// * Only push a (single) value after being pulled.
///
/// It is rare that you'll need to write your own logic, but sometimes
/// performance or other considerations makes it necessary. Be sure to
/// look at the numerous stages provided out of the box before
/// continuing.
trait Logic<A, B, Msg>
where
    A: Send,
    B: Send,
    Msg: Send,
{
    fn name(&self) -> &'static str;

    /// Defines the buffer size for the stage that runs this logic. Elements
    /// are buffered to amortize the cost of passing elements over asynchronous
    /// boundaries.
    fn buffer_size(&self) -> Option<usize> {
        None
    }

    /// Indicates that downstream has requested an element.
    #[must_use]
    fn pulled(&mut self, ctx: &mut StreamContext<A, B, Msg>) -> Option<Action<B, Msg>>;

    /// Indicates that upstream has produced an element.
    #[must_use]
    fn pushed(&mut self, el: A, ctx: &mut StreamContext<A, B, Msg>) -> Option<Action<B, Msg>>;

    /// Indicates that this stage has been started.
    #[must_use]
    fn started(&mut self, ctx: &mut StreamContext<A, B, Msg>) -> Option<Action<B, Msg>> {
        None
    }

    /// Indicates that upstream has been stopped.
    #[must_use]
    fn stopped(&mut self, ctx: &mut StreamContext<A, B, Msg>) -> Option<Action<B, Msg>> {
        Some(Action::Complete)
    }

    /// Indicates that downstream has cancelled. This normally doesn't
    /// need to be overridden -- only do so if you know what you're
    /// doing.
    #[must_use]
    fn cancelled(&mut self, ctx: &mut StreamContext<A, B, Msg>) -> Option<Action<B, Msg>> {
        Some(Action::Complete)
    }

    #[must_use]
    fn forwarded(&mut self, msg: Msg, ctx: &mut StreamContext<A, B, Msg>) -> Option<Action<B, Msg>> {
        None
    }
}

trait LogicContainerFacade<A, B, Ctl>
where
    A: 'static + Send,
    B: 'static + Send,
    Ctl: 'static + Send
{
    fn spawn(
        self: Box<Self>,
        downstream: ActorRef<DownstreamStageMsg<B>>,
        context: &mut ActorSpawnContext,
    ) -> ActorRef<DownstreamStageMsg<A>>;
}

struct LogicContainer<A, B, Msg, L>
where
    L: Logic<A, B, Msg>,
    A: Send,
    B: Send,
    Msg: Send,
{
    logic: L,
    phantom: PhantomData<(A, B, Msg)>,
}

enum UpstreamStageMsg {
    Cancel,
    Pull(u64),
}

enum DownstreamStageMsg<A>
where
    A: 'static + Send,
{
    Produce(A),
    Complete,
    SetUpstream(ActorRef<UpstreamStageMsg>),
    Converted(UpstreamStageMsg)
}

enum StageMsg<A, B, Msg>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    SetUpstream(ActorRef<UpstreamStageMsg>),
    Pull(u64),
    Cancel,
    Stopped,
    Consume(A),
    Action(Action<B, Msg>)
}

fn downstream_stage_msg_to_stage_msg<A, B, Msg>(msg: DownstreamStageMsg<A>) -> StageMsg<A, B, Msg>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    match msg {
        DownstreamStageMsg::Produce(el)                               => StageMsg::Consume(el),
        DownstreamStageMsg::Complete                                  => StageMsg::Stopped,
        DownstreamStageMsg::SetUpstream(upstream)                     => StageMsg::SetUpstream(upstream),
        DownstreamStageMsg::Converted(UpstreamStageMsg::Pull(demand)) => StageMsg::Pull(demand),
        DownstreamStageMsg::Converted(UpstreamStageMsg::Cancel)       => StageMsg::Cancel,
    }
}

fn downstream_stage_msg_to_internal_stream_ctl<A>(msg: DownstreamStageMsg<A>) -> InternalStreamCtl<A>
where
    A: 'static + Send,
{
    InternalStreamCtl::FromSink(msg)
}

fn upstream_stage_msg_to_internal_stream_ctl<A>(msg: UpstreamStageMsg) -> InternalStreamCtl<A>
where
    A: 'static + Send,
{
    InternalStreamCtl::FromSource(msg)
}

fn upstream_stage_msg_to_stage_msg<A, B, Msg>(msg: UpstreamStageMsg) -> StageMsg<A, B, Msg>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    match msg {
        UpstreamStageMsg::Pull(demand) => StageMsg::Pull(demand),
        UpstreamStageMsg::Cancel       => StageMsg::Cancel,
    }
}

fn upstream_stage_msg_to_downstream_stage_msg<A>(msg: UpstreamStageMsg) -> DownstreamStageMsg<A>
where
    A: 'static + Send,
{
    DownstreamStageMsg::Converted(msg)
}

struct Stage<A, B, Msg, L>
where
    L: Logic<A, B, Msg>,
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    logic: L,
    buffer: VecDeque<A>,
    phantom: PhantomData<(A, B, Msg)>,
    downstream: ActorRef<DownstreamStageMsg<B>>,
    state: StageState<A, B, Msg>,
    pulled: bool,
    upstream_stopped: bool,
    midstream_stopped: bool,
    downstream_demand: u64,
    upstream_demand: u64,
}

enum StageState<A, B, Msg>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    Waiting(Option<VecDeque<StageMsg<A, B, Msg>>>),
    Running(ActorRef<UpstreamStageMsg>),
}

impl<A, B, Msg, L> Stage<A, B, Msg, L>
where
    L: Logic<A, B, Msg> + Send,
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    fn check_upstream_demand(&mut self, context: &mut ActorContext<StageMsg<A, B, Msg>>) {
        if self.upstream_stopped || self.midstream_stopped {
            return;
        }

        let capacity = self.buffer.capacity() as u64;
        let available = capacity - self.upstream_demand;

        println!("c={}, capacity={}, available={}, len={}, updemand={}", self.buffer.capacity(), capacity, available, self.buffer.len(), self.upstream_demand);

        if available >= capacity / 2 {
            if let StageState::Running(ref upstream) = self.state {
                upstream.tell(UpstreamStageMsg::Pull(available));
                self.upstream_demand += available;
            }
        }
    }

    fn receive_action(
        &mut self,
        action: Action<B, Msg>,
        ctx: &mut ActorContext<StageMsg<A, B, Msg>>,
    ) {
        if self.midstream_stopped {
            return;
        }

        match action {
            Action::Pull => {
                println!("{} Action::Pull", self.logic.name());

                if self.pulled {
                    //println!("already pulled, this is a bug");
                    // @TODO must fail as this is a bug
                } else {
                    match self.buffer.pop_front() {
                        Some(el) => {
                            println!("{} some!", self.logic.name());
                            self.upstream_demand -= 1;
                            self.check_upstream_demand(ctx);

                            if let Some(action) = self.logic.pushed(el, &mut StreamContext { ctx }) {
                                self.receive_action(action, ctx);
                            }
                        }

                        None if self.upstream_stopped => {
                            if let Some(action) = self.logic.stopped(&mut StreamContext { ctx }) {
                                self.receive_action(action, ctx);
                            }
                        }

                        None => {
                            println!("{} none!", self.logic.name());
                            self.pulled = true;
                        }
                    }
                }
            }

            Action::Push(el) => {
                println!("{} Action::Push", self.logic.name());

                if self.downstream_demand == 0 {
                    // @TODO must fail - logic has violated the rules
                } else {
                    self.downstream.tell(DownstreamStageMsg::Produce(el));
                    self.downstream_demand -= 1;

                    if self.downstream_demand > 0 {
                        if let Some(action) = self.logic.pulled(&mut StreamContext { ctx }) {
                            self.receive_action(action, ctx);
                        }
                    }
                }
            }

            Action::Forward(msg) => {
                println!("{} Action::Forward", self.logic.name());
                if let Some(action) = self.logic.forwarded(msg, &mut StreamContext { ctx }) {
                    self.receive_action(action, ctx);
                }
            }

            Action::Cancel => {
                if !self.upstream_stopped {
                    if let StageState::Running(ref upstream) = self.state {
                        upstream.tell(UpstreamStageMsg::Cancel);
                    }
                }
            }

            Action::Complete => {
                self.downstream.tell(DownstreamStageMsg::Complete);

                self.midstream_stopped = true;

                ctx.stop();
            }
        }
    }
}

impl<A, B, Msg, L> Actor<StageMsg<A, B, Msg>> for Stage<A, B, Msg, L>
where
    L: Logic<A, B, Msg> + Send,
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    fn receive(&mut self, msg: StageMsg<A, B, Msg>, ctx: &mut ActorContext<StageMsg<A, B, Msg>>) {
        if self.midstream_stopped {
            return;
        }

        match self.state {
            StageState::Running(ref upstream) => {
                match msg {
                    StageMsg::Pull(demand) => {
                        println!("{} StageMsg::Pull({})", self.logic.name(), demand);
                        // our downstream has requested more elements

                        self.downstream_demand += demand;

                        //println!("pull! downstream demand is now {}", self.downstream_demand);

                        if self.downstream_demand > 0 {
                            if let Some(action) = self.logic.pulled(&mut StreamContext { ctx }) {
                                self.receive_action(action, ctx);
                            }
                        }
                    }

                    StageMsg::Consume(el) => {
                        println!("{} StageMsg::Consume", self.logic.name());

                        // our upstream has produced this element

                        if !self.upstream_stopped {
                            if self.pulled {
                                self.upstream_demand -= 1;
                                self.pulled = false;

                                // @TODO assert buffer is empty

                                if let Some(action) = self.logic.pushed(el, &mut StreamContext { ctx }) {
                                    self.receive_action(action, ctx);
                                }

                                self.check_upstream_demand(ctx);
                            } else {
                                self.buffer.push_back(el);
                            }
                        }
                    }

                    StageMsg::Action(action) => {
                        self.receive_action(action, ctx);
                    }

                    StageMsg::Cancel => {
                        // downstream has cancelled us

                        if !self.upstream_stopped {
                            upstream.tell(UpstreamStageMsg::Cancel);
                        }

                        if let Some(action) = self.logic.cancelled(&mut StreamContext { ctx }) {
                            self.receive_action(action, ctx);
                        }
                    }

                    StageMsg::Stopped => {
                        // upstream has stopped, need to drain buffers and be done

                        println!("{} UPSTREAM HAS STOPPED", self.logic.name());
                        self.upstream_stopped = true;

                        if self.buffer.is_empty() {
                            if let Some(action) = self.logic.stopped(&mut StreamContext { ctx }) {
                                self.receive_action(action, ctx);
                            }
                        }
                    }

                    StageMsg::SetUpstream(_) => {
                        // this shouldn't be possible
                    }
                }
            }

            StageState::Waiting(ref stash) if stash.is_none() => {
                match msg {
                    StageMsg::SetUpstream(upstream) => {
                        self.state = StageState::Running(upstream);

                        if let Some(action) = self.logic.started(&mut StreamContext { ctx }) {
                            self.receive_action(action, ctx);
                        }

                        self.check_upstream_demand(ctx);
                    }

                    other => {
                        // this is rare, but it can happen depending upon
                        // timing, i.e. the stage can receive messages from
                        // upstream or downstream before it has received
                        // the ActorRef for upstream

                        let mut stash = VecDeque::new();

                        stash.push_back(other);

                        self.state = StageState::Waiting(Some(stash));
                    }
                }
            }

            StageState::Waiting(ref mut stash) => match msg {
                StageMsg::SetUpstream(upstream) => {
                    let mut stash = stash
                        .take()
                        .expect("pantomime bug: Option::take failed despite being Some");

                    self.state = StageState::Running(upstream);

                    if let Some(action) = self.logic.started(&mut StreamContext { ctx }) {
                        self.receive_action(action, ctx);
                    }

                    self.check_upstream_demand(ctx);

                    while let Some(msg) = stash.pop_front() {
                        self.receive(msg, ctx);
                    }
                }

                other => {
                    let mut stash = stash
                        .take()
                        .expect("pantomime bug: Option::take failed despite being Some");

                    stash.push_back(other);
                }
            },
        }
    }
}

impl<A, B, Ctl, Msg, L> LogicContainerFacade<A, B, Ctl> for LogicContainer<A, B, Msg, L>
where
    L: 'static + Logic<A, B, Msg> + Send,
    A: 'static + Send,
    B: 'static + Send,
    Ctl: 'static + Send,
    Msg: 'static + Send,
{
    fn spawn(
        self: Box<Self>,
        downstream: ActorRef<DownstreamStageMsg<B>>,
        context: &mut ActorSpawnContext,
    ) -> ActorRef<DownstreamStageMsg<A>> {
        let buffer_size = self.logic.buffer_size().unwrap_or_else(|| {
            context
                .system_context()
                .config()
                .default_streams_buffer_size
        });

        let upstream = context.spawn(Stage {
            logic: self.logic,
            buffer: VecDeque::with_capacity(buffer_size),
            phantom: PhantomData,
            downstream: downstream.clone(),
            state: StageState::Waiting(None),
            pulled: false,
            upstream_stopped: false,
            midstream_stopped: false,
            downstream_demand: 0,
            upstream_demand: 0,
        });

        downstream.tell(DownstreamStageMsg::SetUpstream(
            upstream.convert(upstream_stage_msg_to_stage_msg),
        ));

        upstream.convert(downstream_stage_msg_to_stage_msg)
    }
}

//
// SINKS
//

struct FirstSinkLogic<A> {
    first: Option<A>,
    pulled: bool,
}

impl<A> Logic<A, Option<A>, ()> for FirstSinkLogic<A>
where
    A: 'static + Send,
{
    fn name(&self) -> &'static str {
        "FirstSinkLogic"
    }

    fn pulled(&mut self, ctx: &mut StreamContext<A, Option<A>, ()>) -> Option<Action<Option<A>, ()>> {
        self.pulled = true;

        None
    }

    fn pushed(&mut self, el: A, ctx: &mut StreamContext<A, Option<A>, ()>) -> Option<Action<Option<A>, ()>> {
        if self.first.is_none() {
            self.first = Some(el);
        }

        Some(Action::Cancel)
    }

    fn started(&mut self, ctx: &mut StreamContext<A, Option<A>, ()>) -> Option<Action<Option<A>, ()>> {
        Some(Action::Pull)
    }

    fn stopped(&mut self, ctx: &mut StreamContext<A, Option<A>, ()>) -> Option<Action<Option<A>, ()>> {
        if self.pulled {
            self.pulled = false;

            //ctx.tell(Action::Completed);

            Some(Action::Push(self.first.take()))
        } else {
            Some(Action::Complete)
        }
    }

    fn forwarded(&mut self, msg: (), ctx: &mut StreamContext<A, Option<A>, ()>) -> Option<Action<Option<A>, ()>> {
        None
    }
}


struct LastSinkLogic<A> {
    last: Option<A>,
    pulled: bool
}

impl<A> Logic<A, Option<A>, ()> for LastSinkLogic<A>
where
    A: 'static + Send,
{
    fn name(&self) -> &'static str {
        "LastSinkLogic"
    }

    fn pulled(&mut self, ctx: &mut StreamContext<A, Option<A>, ()>) -> Option<Action<Option<A>, ()>> {
        self.pulled = true;

        None
    }

    fn pushed(&mut self, el: A, ctx: &mut StreamContext<A, Option<A>, ()>) -> Option<Action<Option<A>, ()>> {
        self.last = Some(el);

        Some(Action::Pull)
    }

    fn started(&mut self, ctx: &mut StreamContext<A, Option<A>, ()>) -> Option<Action<Option<A>, ()>> {
        Some(Action::Pull)
    }

    fn stopped(&mut self, ctx: &mut StreamContext<A, Option<A>, ()>) -> Option<Action<Option<A>, ()>> {
        if self.pulled {
            self.pulled = false;

            // ctx do complete

            Some(Action::Push(self.last.take()))
        } else {
            Some(Action::Complete)
        }
    }

    fn forwarded(&mut self, msg: (), ctx: &mut StreamContext<A, Option<A>, ()>) -> Option<Action<Option<A>, ()>> {
        None
    }
}


struct CollectSinkLogic<A> {
    entries: Vec<A>,
    pulled: bool
}

impl<A> Logic<A, Vec<A>, ()> for CollectSinkLogic<A>
where
    A: 'static + Send,
{
    fn name(&self) -> &'static str {
        "CollectSinkLogic"
    }

    fn pulled(&mut self, ctx: &mut StreamContext<A, Vec<A>, ()>) -> Option<Action<Vec<A>, ()>> {
        self.pulled = true;

        None
    }

    fn pushed(&mut self, el: A, ctx: &mut StreamContext<A, Vec<A>, ()>) -> Option<Action<Vec<A>, ()>> {
        self.entries.push(el);

        Some(Action::Pull)
    }

    fn started(&mut self, ctx: &mut StreamContext<A, Vec<A>, ()>) -> Option<Action<Vec<A>, ()>> {
        Some(Action::Pull)
    }

    fn stopped(&mut self, ctx: &mut StreamContext<A, Vec<A>, ()>) -> Option<Action<Vec<A>, ()>> {
        if self.pulled {
            self.pulled = false;

            let mut entries = Vec::new();

            mem::swap(&mut self.entries, &mut entries);

            Some(Action::Push(entries))
        } else {
            None
        }
    }

    fn forwarded(&mut self, msg: (), ctx: &mut StreamContext<A, Vec<A>, ()>) -> Option<Action<Vec<A>, ()>> {
        None
    }
}

struct IgnoreSinkLogic<A>
where
    A: 'static + Send,
{
    pulled: bool,
    phantom: PhantomData<A>,
}

impl<A> Logic<A, (), ()> for IgnoreSinkLogic<A>
where
    A: 'static + Send,
{
    fn name(&self) -> &'static str {
        "IgnoreSinkLogic"
    }

    fn pulled(&mut self, ctx: &mut StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        self.pulled = true;

        None
    }

    fn pushed(&mut self, el: A, ctx: &mut StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        Some(Action::Pull)
    }

    fn started(&mut self, ctx: &mut StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        Some(Action::Pull)
    }

    fn stopped(&mut self, ctx: &mut StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        if self.pulled {
            self.pulled = false;

            Some(Action::Push(()))
        } else {
            None
        }
    }

    fn forwarded(&mut self, msg: (), ctx: &mut StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        None
    }
}

struct ForEachSinkLogic<A, F>
where
    F: FnMut(A) -> (),
    A: 'static + Send,
{
    for_each_fn: F,
    pulled: bool,
    phantom: PhantomData<A>,
}

impl<A, F> Logic<A, (), ()> for ForEachSinkLogic<A, F>
where
    F: FnMut(A) -> (),
    A: 'static + Send,
{
    fn name(&self) -> &'static str {
        "ForEachSinkLogic"
    }

    fn pulled(&mut self, ctx: &mut StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        self.pulled = true;

        None
    }

    fn pushed(&mut self, el: A, ctx: &mut StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        (self.for_each_fn)(el);

        Some(Action::Pull)
    }

    fn started(&mut self, ctx: &mut StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        Some(Action::Pull)
    }

    fn stopped(&mut self, ctx: &mut StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        println!("STOPPED!");

        if self.pulled {
            self.pulled = false;

            let actor_ref = ctx.actor_ref();

            actor_ref.tell(Action::Push(()));
            actor_ref.tell(Action::Complete);

            None
        } else {
            Some(Action::Complete)
        }
    }

    fn cancelled(&mut self, ctx: &mut StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        println!("CANCELLED");
        Some(Action::Complete)
    }

    fn forwarded(&mut self, msg: (), ctx: &mut StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        None
    }
}

//
// FLOWS
//

struct IdentityFlowLogic<A>
where
    A: 'static + Send,
{
    phantom: PhantomData<A>,
}

impl<A> Logic<A, A, ()> for IdentityFlowLogic<A>
where
    A: 'static + Send,
{
    fn name(&self) -> &'static str {
        "IdentityFlowLogic"
    }

    fn pulled(&mut self, ctx: &mut StreamContext<A, A, ()>) -> Option<Action<A, ()>> {
        Some(Action::Pull)
    }

    fn pushed(&mut self, el: A, ctx: &mut StreamContext<A, A, ()>) -> Option<Action<A, ()>> {
        Some(Action::Push(el))
    }
}

enum DelayFlowLogicMsg<A> {
    Ready(A),
}

struct DelayFlowLogic {
    delay: Duration,
    pushing: bool,
    stopped: bool
}

impl DelayFlowLogic {
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            pushing: false,
            stopped: false
        }
    }
}

impl<A> Logic<A, A, DelayFlowLogicMsg<A>> for DelayFlowLogic
where
    A: 'static + Send,
{
    fn name(&self) -> &'static str {
        "DelayFlowLogic"
    }

    fn buffer_size(&self) -> Option<usize> {
        Some(0) // @FIXME should this be based on the delay duration?
    }

    fn pulled(&mut self, ctx: &mut StreamContext<A, A, DelayFlowLogicMsg<A>>) -> Option<Action<A, DelayFlowLogicMsg<A>>> {
        Some(Action::Pull)
    }

    fn pushed(&mut self, el: A, ctx: &mut StreamContext<A, A, DelayFlowLogicMsg<A>>) -> Option<Action<A, DelayFlowLogicMsg<A>>> {
        self.pushing = true;

        ctx.schedule_delivery("ready", self.delay.clone(), DelayFlowLogicMsg::Ready(el));

        None
    }

    fn stopped(&mut self, ctx: &mut StreamContext<A, A, DelayFlowLogicMsg<A>>) -> Option<Action<A, DelayFlowLogicMsg<A>>> {
        if self.pushing {
            self.stopped = true;

            None
        } else {
            Some(Action::Complete)
        }
    }

    fn forwarded(&mut self, msg: DelayFlowLogicMsg<A>, ctx: &mut StreamContext<A, A, DelayFlowLogicMsg<A>>) -> Option<Action<A, DelayFlowLogicMsg<A>>> {
        match msg {
            DelayFlowLogicMsg::Ready(el) => {
                if self.stopped {
                    ctx.tell(Action::Complete);
                }

                Some(Action::Push(el))
            }
        }
    }
}

struct FilterFlowLogic<A, F: FnMut(&A) -> bool>
where
    A: 'static + Send,
    F: 'static + Send,
{
    filter: F,
    phantom: PhantomData<A>,
}

impl<A, F: FnMut(&A) -> bool> FilterFlowLogic<A, F>
where
    A: 'static + Send,
    F: 'static + Send,
{
    fn new(filter: F) -> Self {
        Self {
            filter,
            phantom: PhantomData,
        }
    }
}

impl<A, F: FnMut(&A) -> bool> Logic<A, A, ()> for FilterFlowLogic<A, F>
where
    A: 'static + Send,
    F: 'static + Send,
{
    fn name(&self) -> &'static str {
        "FilterFlowLogic"
    }

    fn pulled(&mut self, ctx: &mut StreamContext<A, A, ()>) -> Option<Action<A, ()>> {
        Some(Action::Pull)
    }

    fn pushed(&mut self, el: A, ctx: &mut StreamContext<A, A, ()>) -> Option<Action<A, ()>> {
        Some(if (self.filter)(&el) {
            Action::Push(el)
        } else {
            Action::Pull
        })
    }
}

struct MapFlowLogic<A, B, F: FnMut(A) -> B>
where
    A: Send,
    B: Send,
{
    map_fn: F,
    phantom: PhantomData<(A, B)>,
}

impl<A, B, F: FnMut(A) -> B> MapFlowLogic<A, B, F>
where
    A: Send,
    B: Send,
{
    fn new(map_fn: F) -> Self {
        Self {
            map_fn,
            phantom: PhantomData,
        }
    }
}

impl<A, B, F> Logic<A, B, ()> for MapFlowLogic<A, B, F>
where
    F: FnMut(A) -> B,
    A: 'static + Send,
    B: 'static + Send,
{
    fn name(&self) -> &'static str {
        "MapFlowLogic"
    }

    fn pulled(&mut self, ctx: &mut StreamContext<A, B, ()>) -> Option<Action<B, ()>> {
        Some(Action::Pull)
    }

    fn pushed(&mut self, el: A, ctx: &mut StreamContext<A, B, ()>) -> Option<Action<B, ()>> {
        Some(Action::Push((self.map_fn)(el)))
    }
}

struct TakeWhileFlowLogic<A>
where
    A: 'static + Send,
{
    while_fn: Box<FnMut(&A) -> bool>, // @TODO dont box
    cancelled: bool
}

impl<A> Logic<A, A, ()> for TakeWhileFlowLogic<A>
where
    A: 'static + Send,
{
    fn name(&self) -> &'static str {
        "TakeWhileFlowLogic"
    }

    fn pulled(&mut self, ctx: &mut StreamContext<A, A, ()>) -> Option<Action<A, ()>> {
        Some(Action::Pull)
    }

    fn pushed(&mut self, el: A, ctx: &mut StreamContext<A, A, ()>) -> Option<Action<A, ()>> {
        if self.cancelled {
            None
        } else if (self.while_fn)(&el) {
            Some(Action::Push(el))
        } else {
            self.cancelled = true;

            Some(Action::Complete)
        }
    }

    fn started(&mut self, ctx: &mut StreamContext<A, A, ()>) -> Option<Action<A, ()>> {
        None
    }

    fn stopped(&mut self, ctx: &mut StreamContext<A, A, ()>) -> Option<Action<A, ()>> {
        Some(Action::Complete)
    }

    fn cancelled(&mut self, ctx: &mut StreamContext<A, A, ()>) -> Option<Action<A, ()>> {
        Some(Action::Complete)
    }

    fn forwarded(&mut self, msg: (), ctx: &mut StreamContext<A, A, ()>) -> Option<Action<A, ()>> {
        None
    }
}

//
// SOURCES
//

struct RepeatSourceLogic<A>
where
    A: 'static + Clone + Send,
{
    el: A,
}

impl<A> Logic<(), A, ()> for RepeatSourceLogic<A>
where
    A: 'static + Clone + Send,
{
    fn name(&self) -> &'static str {
        "RepeatSourceLogic"
    }

    fn pulled(&mut self, ctx: &mut StreamContext<(), A, ()>) -> Option<Action<A, ()>> {
        Some(Action::Push(self.el.clone()))
    }

    fn pushed(&mut self, el: (), ctx: &mut StreamContext<(), A, ()>) -> Option<Action<A, ()>> {
        None
    }

    fn started(&mut self, ctx: &mut StreamContext<(), A, ()>) -> Option<Action<A, ()>> {
        None
    }

    fn stopped(&mut self, ctx: &mut StreamContext<(), A, ()>) -> Option<Action<A, ()>> {
        None
    }

    fn cancelled(&mut self, ctx: &mut StreamContext<(), A, ()>) -> Option<Action<A,()>> {
        Some(Action::Complete)
    }

    fn forwarded(&mut self, msg: (), ctx: &mut StreamContext<(), A, ()>) -> Option<Action<A, ()>> {
        None
    }
}

struct IteratorSourceLogic<A, I: Iterator<Item = A>>
where
    A: 'static + Send,
{
    iterator: I,
}

impl<A, I: Iterator<Item = A>> Logic<(), A, ()> for IteratorSourceLogic<A, I>
where
    A: 'static + Send,
{
    fn name(&self) -> &'static str {
        "IteratorSourceLogic"
    }

    fn pulled(&mut self, ctx: &mut StreamContext<(), A, ()>) -> Option<Action<A, ()>> {
        Some(
            match self.iterator.next() {
                Some(element) => Action::Push(element),
                None          => Action::Complete
            }
        )
    }

    fn pushed(&mut self, el: (), ctx: &mut StreamContext<(), A, ()>) -> Option<Action<A, ()>> {
        None
    }

    fn stopped(&mut self, ctx: &mut StreamContext<(), A, ()>) -> Option<Action<A, ()>> {
        None
    }
}

struct StreamContext<'a, A, B, Msg>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    ctx: &'a mut ActorContext<StageMsg<A, B, Msg>>
}

impl<'a, A, B, Msg> StreamContext<'a, A, B, Msg>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    pub fn actor_ref(&self) -> ActorRef<Action<B, Msg>> {
        // @FIXME i'd like this to return a reference for API symmetry

        self.ctx.actor_ref().convert(|action: Action<B, Msg>| StageMsg::Action(action))
    }

    fn tell(&self, action: Action<B, Msg>) {
        self.ctx.actor_ref().tell(StageMsg::Action(action));
    }

    fn schedule_delivery<S: AsRef<str>>(&mut self, name: S, timeout: Duration, msg: Msg) {
        self.ctx.schedule_delivery(name, timeout, StageMsg::Action(Action::Forward(msg)));
    }

    fn spawn<AMsg, Ax: Actor<AMsg>>(&mut self, actor: Ax) -> ActorRef<AMsg> where AMsg: 'static + Send, Ax: 'static + Send {
        self.ctx.spawn(actor)
    }
}

enum StreamCtl {
    Stop,
    Fail,
}

enum InternalStreamCtl<Out> where Out: 'static + Send {
    Stop,
    Fail,
    FromSink(DownstreamStageMsg<Out>),
    FromSource(UpstreamStageMsg),
    SetDownstream(ActorRef<DownstreamStageMsg<()>>)
}

enum ProtectedStreamCtl {
    Stop,
    Fail,
}

fn stream_ctl_to_internal_stream_ctl<Out>(ctl: StreamCtl) -> InternalStreamCtl<Out> where Out: 'static + Send {
    match ctl {
        StreamCtl::Stop => InternalStreamCtl::Stop,
        StreamCtl::Fail => InternalStreamCtl::Fail,
    }
}

fn protected_stream_ctl_to_internal_stream_ctl<Out>(ctl: ProtectedStreamCtl) -> InternalStreamCtl<Out> where Out: 'static + Send {
    match ctl {
        ProtectedStreamCtl::Stop => InternalStreamCtl::Stop,
        ProtectedStreamCtl::Fail => InternalStreamCtl::Fail,
    }
}

struct ProducerWithSink<A, Out>
where
    A: 'static + Send,
    Out: 'static + Send
{
    producer: Box<Producer<(), A> + Send>,
    sink: Sink<A, Out>,
    phantom: PhantomData<Out>
}

struct StreamController<Out> where Out: Send {
    stream: Option<Stream<Out>>,
    state: Arc<AtomicCell<Option<Out>>>,
    produced: bool,
    stream_stopped: bool,
}

impl<Out> Actor<InternalStreamCtl<Out>> for StreamController<Out> where Out: Send {
    fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<InternalStreamCtl<Out>>) {
        match signal {
            Signal::Started => {
                self.stream.take().unwrap().run(ctx);
            }

            _ => {}
        }
    }

    fn receive(&mut self, msg: InternalStreamCtl<Out>, ctx: &mut ActorContext<InternalStreamCtl<Out>>) {
        if self.stream_stopped {
            return;
        }

        match msg {
            InternalStreamCtl::Stop => {}

            InternalStreamCtl::Fail => {}

            InternalStreamCtl::FromSink(DownstreamStageMsg::SetUpstream(upstream)) => {
                println!("pulling 1 from the sink");
                upstream.tell(UpstreamStageMsg::Pull(1));
            }


            InternalStreamCtl::FromSink(DownstreamStageMsg::Produce(value)) => {
                let _ = self.state.swap(Some(value));

                self.produced = true;

                println!("sink got final value");
            }

            InternalStreamCtl::FromSink(DownstreamStageMsg::Complete) => {
                println!("sink completed");

                assert!(self.produced, "pantomime bug: sink did not produce a value");

                ctx.stop();
            }

            InternalStreamCtl::SetDownstream(downstream) => {
                // downstream in this context is actually the origin
                // of the stream -- i.e. the most upstream stage, aka
                // the source.
                //
                // a source's upstream is immediately completed, and
                // its up to the source's logic to do what it wishes
                // with that event, typically to ignore it and complete
                // its downstream at some point in the future

                println!("telling downstream (which is the source) we're done");

                downstream.tell(DownstreamStageMsg::Complete);
            }

            InternalStreamCtl::FromSource(UpstreamStageMsg::Pull(value)) => {
                println!("source pulled {}", value);
                // nothing to do, we already sent a completed
            }

            InternalStreamCtl::FromSource(UpstreamStageMsg::Cancel) => {
                // nothing to do, we already sent a completed
            }

            InternalStreamCtl::FromSink(_) => {
                println!("sink got unexpected value");
            }
        }
    }
}

trait RunnableStream<Out> where Out: Send {
    fn run(self: Box<Self>, context: &mut ActorContext<InternalStreamCtl<Out>>);
}

impl<A, Out> RunnableStream<Out> for ProducerWithSink<A, Out>
where
    A: 'static + Send,
    Out: 'static + Send,
{
    fn run(self: Box<Self>, context: &mut ActorContext<InternalStreamCtl<Out>>) {
        let actor_ref_for_sink = context.actor_ref().convert(downstream_stage_msg_to_internal_stream_ctl);
        let actor_ref_for_source = context.actor_ref().convert(upstream_stage_msg_to_internal_stream_ctl);
        let mut context_for_upstream = context.spawn_context();

        let upstream_ref = self
            .producer
            .spawn(self.sink.logic.spawn(actor_ref_for_sink, &mut context_for_upstream), &mut context_for_upstream);

        upstream_ref.tell(DownstreamStageMsg::SetUpstream(actor_ref_for_source));

        context.actor_ref().tell(InternalStreamCtl::SetDownstream(upstream_ref));
    }
}

struct StreamComplete<Out> where Out: 'static + Send {
    controller_ref: ActorRef<InternalStreamCtl<Out>>,
    state: Arc<AtomicCell<Option<Out>>>
}

impl<Out> StreamComplete<Out> where Out: 'static + Send {
    fn pipe_to(self, actor_ref: &ActorRef<Out>) {
        let actor_ref = actor_ref.clone();

    }
}


struct Stream<Out> {
    runnable_stream: Box<RunnableStream<Out> + Send>,
}

impl<Out> Stream<Out> where Out: 'static + Send {
    fn run(self, context: &mut ActorContext<InternalStreamCtl<Out>>) {
        self.runnable_stream.run(context)
    }
}

/// A `Sink` is a stage that accepts a single output, and outputs a
/// terminal value.
///
/// The logic supplied for a `Sink` will be pulled immediately when
/// the stream is spawned. Once the stream has finished, the logic's
/// stop handler will be invoked, and a conforming implementation must
/// at some point push out a single value.
struct Sink<A, Out> where Out: 'static + Send {
    logic: Box<LogicContainerFacade<A, Out, InternalStreamCtl<Out>> + Send>,
    phantom: PhantomData<Out>
}

impl<A> Sink<A, ()>
where
    A: 'static + Send,
{
    fn for_each<F: FnMut(A) -> ()>(for_each_fn: F) -> Sink<A, ()>
    where
        F: 'static + Send,
    {
        Self {
            logic: Box::new(LogicContainer {
                logic: ForEachSinkLogic {
                    for_each_fn,
                    pulled: false,
                    phantom: PhantomData,
                },
                phantom: PhantomData,
            }),
            phantom: PhantomData
        }
    }
}

impl<A, Out> Sink<A, Out>
where
    A: 'static + Send + Clone,
    Out: 'static + Send,
{
    #[must_use]
    fn from_broadcast_hub(hub_ref: &ActorRef<Sink<A, Out>>) -> Self {
        unimplemented!()
    }
}

trait Producer<In, Out>
where
    In: 'static + Send,
    Out: 'static + Send,
{
    fn spawn(
        self: Box<Self>,
        downstream: ActorRef<DownstreamStageMsg<Out>>,
        context: &mut ActorSpawnContext,
    ) -> ActorRef<DownstreamStageMsg<In>>;
}

struct SourceLike<A, M, L>
where
    L: Logic<(), A, M>,
    A: Send,
    M: Send,
{
    logic: L,
    phantom: PhantomData<(A, M)>,
}

impl<A, M, L> Producer<(), A> for SourceLike<A, M, L>
where
    L: 'static + Logic<(), A, M> + Send,
    A: 'static + Send,
    M: 'static + Send,
{
    fn spawn(
        self: Box<Self>,
        downstream: ActorRef<DownstreamStageMsg<A>>,
        context: &mut ActorSpawnContext,
    ) -> ActorRef<DownstreamStageMsg<()>> {
        let buffer_size = self.logic.buffer_size().unwrap_or_else(|| {
            context
                .system_context()
                .config()
                .default_streams_buffer_size
        });

        let upstream = context.spawn(Stage {
            logic: self.logic,
            buffer: VecDeque::with_capacity(buffer_size),
            phantom: PhantomData,
            downstream: downstream.clone(),
            state: StageState::Waiting(None),
            pulled: false,
            upstream_stopped: false,
            midstream_stopped: false,
            downstream_demand: 0,
            upstream_demand: 0,
        });

        downstream.tell(DownstreamStageMsg::SetUpstream(
            upstream.convert(upstream_stage_msg_to_stage_msg),
        ));

        upstream.convert(downstream_stage_msg_to_stage_msg)
    }
}

struct ProducerWithFlow<A, B, C> {
    producer: Box<Producer<A, B> + Send>,
    flow: Flow<B, C>,
}

impl<A, B, C> Producer<A, C> for ProducerWithFlow<A, B, C>
where
    A: 'static + Send,
    B: 'static + Send,
    C: 'static + Send,
{
    fn spawn(
        self: Box<Self>,
        downstream: ActorRef<DownstreamStageMsg<C>>,
        context: &mut ActorSpawnContext,
    ) -> ActorRef<DownstreamStageMsg<A>> {
        let flow_ref = self.flow.logic.spawn(downstream, context);

        let upstream = self.producer.spawn(flow_ref.clone(), context);

        // @TODO double conversion here..
        flow_ref.tell(DownstreamStageMsg::SetUpstream(
            upstream.convert(upstream_stage_msg_to_downstream_stage_msg),
        ));

        upstream
    }
}

struct Source<A> {
    producer: Box<Producer<(), A> + Send>,
}

impl<A> Producer<(), A> for Source<A>
where
    A: 'static + Send,
{
    fn spawn(
        self: Box<Self>,
        downstream: ActorRef<DownstreamStageMsg<A>>,
        context: &mut ActorSpawnContext,
    ) -> ActorRef<DownstreamStageMsg<()>> {
        let upstream = self.producer.spawn(downstream.clone(), context);

        // @TODO double conversion here..
        downstream.tell(DownstreamStageMsg::SetUpstream(
            upstream.convert(upstream_stage_msg_to_downstream_stage_msg),
        ));

        upstream
    }
}

impl Source<()> {
    fn iterator<A, I: Iterator<Item = A>>(iterator: I) -> Source<A>
    where
        A: 'static + Send,
        I: 'static + Send,
    {
        Source {
            producer: Box::new(SourceLike {
                logic: IteratorSourceLogic { iterator },
                phantom: PhantomData,
            }),
        }
    }

    fn repeat<A>(el: A) -> Source<A>
    where
        A: 'static + Clone + Send,
    {
        Source {
            producer: Box::new(SourceLike {
                logic: RepeatSourceLogic { el },
                phantom: PhantomData,
            }),
        }
    }
}

struct SourceQueue<A> {
    phantom: PhantomData<A>
}

impl<A> SourceQueue<A> {
    fn new() -> Self {
        Self {
            phantom: PhantomData
        }
    }
}

struct MergeHub<A> {
    phantom: PhantomData<A>,
}

impl<A> MergeHub<A> {
    fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl<A> Actor<Source<A>> for MergeHub<A>
where
    A: 'static + Send,
{
    fn receive(&mut self, source: Source<A>, _: &mut ActorContext<Source<A>>) {}
}

struct BroadcastHub<A> {
    phantom: PhantomData<A>,
}

impl<A> BroadcastHub<A> {
    fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl<A> Actor<Sink<A, ()>> for BroadcastHub<A>
where
    A: 'static + Send + Clone,
{
    fn receive(&mut self, source: Sink<A, ()>, _: &mut ActorContext<Sink<A, ()>>) {}
}

impl<A> Source<A>
where
    A: 'static + Send,
{
    #[must_use]
    fn from_merge_hub(hub_ref: &ActorRef<Source<A>>) -> Source<A> {
        // we can register ourselves with the hub ref so that
        // when our logic is dropped, hub ref is stopped
        unimplemented!()
    }

    fn merge(self, other: Source<A>) -> Source<A> {
        unimplemented!();
    }

    fn to<Out>(self, sink: Sink<A, Out>) -> Stream<Out> where Out: 'static + Send {
        Stream {
            runnable_stream: Box::new(ProducerWithSink {
                producer: self.producer,
                sink,
                phantom: PhantomData
            }),
        }
    }

    fn filter<F: FnMut(&A) -> bool>(self, filter: F) -> Source<A>
    where
        F: 'static + Send,
    {
        Source {
            producer: Box::new(ProducerWithFlow {
                producer: self.producer,
                flow: Flow {
                    logic: Box::new(LogicContainer {
                        logic: FilterFlowLogic::new(filter),
                        phantom: PhantomData,
                    }),
                },
            }),
        }
    }

    fn via<B>(self, flow: Flow<A, B>) -> Source<B>
    where
        B: 'static + Send,
    {
        Source {
            producer: Box::new(ProducerWithFlow {
                producer: self.producer,
                flow,
            }),
        }
    }
}

struct Flow<A, B> {
    logic: Box<LogicContainerFacade<A, B, ProtectedStreamCtl> + Send>,
}

impl<A> Flow<A, A>
where
    A: 'static + Send,
{
    fn new() -> Self {
        Self {
            logic: Box::new(LogicContainer {
                logic: IdentityFlowLogic {
                    phantom: PhantomData,
                },
                phantom: PhantomData,
            }),
        }
    }
}

impl<A, B> Flow<A, B>
where
    A: 'static + Send,
    B: 'static + Send,
{
    fn from_logic<Msg, L: Logic<A, B, Msg>>(logic: L) -> Self
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

    fn map<F: FnMut(A) -> B>(map_fn: F) -> Self
    where
        F: 'static + Send,
    {
        Self::from_logic(MapFlowLogic::new(map_fn))
    }
}

impl<Msg, Out> crate::actor::Spawnable<Stream<Out>, (ActorRef<StreamCtl>, StreamComplete<Out>)> for ActorContext<Msg> where Msg: 'static + Send, Out: 'static + Send {
    fn perform_spawn(&mut self, stream: Stream<Out>) -> (ActorRef<StreamCtl>, StreamComplete<Out>) {
        let state = Arc::new(AtomicCell::new(None));

        let controller_ref = self.spawn(StreamController {
            stream: Some(stream),
            state: state.clone(),
            produced: false,
            stream_stopped: false
        });

        (controller_ref.convert(stream_ctl_to_internal_stream_ctl), StreamComplete { controller_ref, state })
    }
}

impl<Msg, Out, F: Fn(Out) -> Msg> Watchable<StreamComplete<Out>, Out, Msg, F> for ActorContext<Msg> where Msg: 'static + Send, Out: 'static + Send, F: 'static + Send {
    fn perform_watch(&mut self, subject: StreamComplete<Out>, convert: F) {
        self.watch2(
            &subject.controller_ref.clone(),
            move |reason: StopReason| {
                // @TODO inspect reason

                match subject.state.swap(None) {
                    Some(value) => {
                        convert(value)
                    }

                    None => {
                        panic!("TODO");
                        // @TODO
                    }
                }
            }
        );
    }
}

#[test]
fn test() {
    use crate::actor::*;
    use std::task::Poll;

    fn double_slowly(n: u64) -> u64 {
        let start = std::time::Instant::now();

        while start.elapsed().as_millis() < 250 {}

        n * 2
    }

    struct TestReaper {
        n: usize
    }

    impl TestReaper {
        fn new() -> Self {
            Self { n: 0 }
        }
    }

    impl Actor<()> for TestReaper {
        fn receive(&mut self, _: (), ctx: &mut ActorContext<()>) {
            self.n += 1;

            println!("n={}", self.n);

            if self.n == 2 {
                ctx.stop();
            }
        }

        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
            match signal {
                Signal::Started => {
                    let (stream_ref, result) = ctx.spawn(
                        Source::iterator(1..=20)
                            .via(Flow::from_logic(DelayFlowLogic::new(Duration::from_millis(50))))
                            .to(Sink::for_each(|n| println!("got {}", n))),
                    );

                    ctx.watch2(stream_ref, |_: StopReason| ());
                    ctx.watch2(result, |value| value);
                }

                _ => {}
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper::new()).is_ok());
}
