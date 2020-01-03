use crate::actor::{
    Actor, ActorContext, ActorRef, ActorSpawnContext, FailureReason, Signal, StopReason, Watchable,
};
use crate::stream::flow::Fused;
use crate::stream::{
    Action, Logic, LogicEvent, StageRef, Stream, StreamComplete, StreamContext,
    StreamContextAction, StreamContextType, StreamCtl,
};
use crossbeam::atomic::AtomicCell;
use std::any::Any;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;

// @TODO config
const MAX_CALLS: usize = 10;

pub(in crate::stream) enum LogicType<Up, Down> {
    Spawnable(Box<dyn LogicContainerFacade<Up, Down> + Send>),
    Fusible(Box<dyn ContainedLogic<Up, Down> + Send>),
}

pub(in crate::stream) trait ContainedLogic<Up, Down>
where
    Up: Send,
    Down: Send,
{
    fn fusible(&self) -> bool;

    fn into_facade(self: Box<Self>) -> Box<dyn LogicContainerFacade<Up, Down> + Send>;

    fn receive(
        &mut self,
        event: LogicEvent<Up, Box<dyn Any + Send>>,
        ctx: &mut StreamContext<Up, Down, Box<dyn Any + Send>>,
    ) -> Action<Down, Box<dyn Any + Send>>;
}

pub(in crate::stream) struct ContainedLogicImpl<Up, Down, Ctl, L: Logic<Up, Down, Ctl = Ctl>>
where
    Up: Send,
    Down: Send,
    Ctl: Send,
{
    logic: L,
    actions: VecDeque<StreamContextAction<Down, Ctl>>, // @TODO too much memory usage
    stage_ref: StageRef<Ctl>,
    phantom: PhantomData<(Up, Down, Ctl)>,
}

impl<Up, Down, Ctl, L: Logic<Up, Down, Ctl = Ctl>> ContainedLogicImpl<Up, Down, Ctl, L>
where
    Up: Send,
    Down: Send,
    Ctl: 'static + Send,
{
    pub(in crate::stream) fn new(logic: L) -> Self {
        Self {
            logic,
            actions: VecDeque::new(),
            stage_ref: StageRef::empty(),
            phantom: PhantomData,
        }
    }

    fn logic_receive(
        &mut self,
        event: LogicEvent<Up, Ctl>,
        ctx: &mut StreamContext<Up, Down, Box<dyn Any + Send>>,
    ) -> Action<Down, Box<dyn Any + Send>> {
        if let LogicEvent::Started = event {
            self.stage_ref = ctx.stage_ref().convert(|msg| Box::new(msg));
        }

        let mut stream_ctx = StreamContext {
            ctx: StreamContextType::Fused(&mut self.actions, &self.stage_ref),
            calls: ctx.calls,
        };

        let result = Self::convert_action(self.logic.receive(event, &mut stream_ctx));

        drop(stream_ctx);

        while let Some(a) = self.actions.pop_front() {
            let mut stream_ctx = StreamContext {
                ctx: StreamContextType::Fused(&mut self.actions, &self.stage_ref),
                calls: ctx.calls,
            };

            let next_result: Action<Down, Box<dyn Any + Send>> = match a {
                StreamContextAction::Action(Action::Pull) => {
                    Self::convert_action(self.logic.receive(LogicEvent::Pulled, &mut stream_ctx))
                }

                StreamContextAction::Action(Action::Push(el)) => Action::Push(el),

                StreamContextAction::Action(Action::None) => Action::None,

                StreamContextAction::Action(Action::Forward(msg)) => Self::convert_action(
                    self.logic
                        .receive(LogicEvent::Forwarded(msg), &mut stream_ctx),
                ),

                StreamContextAction::Action(Action::Cancel) => {
                    Self::convert_action(self.logic.receive(LogicEvent::Cancelled, &mut stream_ctx))
                }

                StreamContextAction::Action(Action::PushAndStop(el, reason)) => {
                    Action::PushAndStop(el, reason)
                }

                StreamContextAction::Action(Action::Stop(reason)) => Action::Stop(reason),

                StreamContextAction::ScheduleDelivery(name, duration, msg) => {
                    ctx.schedule_delivery(name, duration, Box::new(msg)); // @TODO namespace name

                    Action::None
                }
            };

            if let Action::None = next_result {
            } else {
                ctx.tell(next_result);
            }
        }

        result
    }

    fn convert_action(action: Action<Down, Ctl>) -> Action<Down, Box<dyn Any + Send>> {
        match action {
            Action::Pull => Action::Pull,
            Action::Push(el) => Action::Push(el),
            Action::None => Action::None,
            Action::Forward(msg) => Action::Forward(Box::new(msg)),
            Action::Cancel => Action::Cancel,
            Action::PushAndStop(el, reason) => Action::PushAndStop(el, reason),
            Action::Stop(reason) => Action::Stop(reason),
        }
    }
}

impl<Up, Down, Ctl, L: Logic<Up, Down, Ctl = Ctl>> ContainedLogic<Up, Down>
    for ContainedLogicImpl<Up, Down, Ctl, L>
where
    Up: 'static + Send,
    Down: 'static + Send,
    Ctl: 'static + Send,
    L: 'static + Send,
{
    fn fusible(&self) -> bool {
        self.logic.fusible()
    }

    fn into_facade(self: Box<Self>) -> Box<dyn LogicContainerFacade<Up, Down> + Send> {
        Box::new(IndividualLogic { logic: self.logic })
    }

    fn receive(
        &mut self,
        event: LogicEvent<Up, Box<dyn Any + Send>>,
        ctx: &mut StreamContext<Up, Down, Box<dyn Any + Send>>,
    ) -> Action<Down, Box<dyn Any + Send>> {
        match event {
            LogicEvent::Pulled => self.logic_receive(LogicEvent::Pulled, ctx),

            LogicEvent::Pushed(element) => self.logic_receive(LogicEvent::Pushed(element), ctx),

            LogicEvent::Stopped => self.logic_receive(LogicEvent::Stopped, ctx),

            LogicEvent::Cancelled => self.logic_receive(LogicEvent::Cancelled, ctx),

            LogicEvent::Started => self.logic_receive(LogicEvent::Started, ctx),

            LogicEvent::Forwarded(msg) => match msg.downcast() {
                Ok(msg) => self.logic_receive(LogicEvent::Forwarded(*msg), ctx),

                Err(_e) => {
                    panic!("TODO {:?}", _e.type_id());
                }
            },
        }
    }
}

pub(in crate::stream) trait LogicContainerFacade<A, B>
where
    A: 'static + Send,
    B: 'static + Send,
{
    fn spawn(
        self: Box<Self>,
        downstream: ActorRef<DownstreamStageMsg<B>>,
        context: &mut ActorSpawnContext,
    ) -> ActorRef<DownstreamStageMsg<A>>;
}

pub(in crate::stream) struct IndividualLogic<L> {
    pub(in crate::stream) logic: L,
}

pub(in crate::stream) struct UnionLogic<A, B, C> {
    pub(in crate::stream) upstream: Box<dyn LogicContainerFacade<A, B> + Send>,
    pub(in crate::stream) downstream: Box<dyn LogicContainerFacade<B, C> + Send>,
}

impl<A, B, C> LogicContainerFacade<A, C> for UnionLogic<A, B, C>
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
        let downstream = self.downstream.spawn(downstream, context);

        self.upstream.spawn(downstream, context)
    }
}

pub(in crate::stream) enum UpstreamStageMsg {
    Cancel,
    Pull(u64),
}

pub(in crate::stream) enum DownstreamStageMsg<A>
where
    A: 'static + Send,
{
    Produce(A),
    Complete(Option<FailureReason>),
    SetUpstream(ActorRef<UpstreamStageMsg>),
}

pub(in crate::stream) enum StageMsg<A, B, Msg>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    SetUpstream(ActorRef<UpstreamStageMsg>),
    Pull(u64),
    Cancel,
    Stopped(Option<FailureReason>),
    Consume(A),
    Action(Action<B, Msg>),
}

fn downstream_stage_msg_to_stage_msg<A, B, Msg>(msg: DownstreamStageMsg<A>) -> StageMsg<A, B, Msg>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    match msg {
        DownstreamStageMsg::Produce(el) => StageMsg::Consume(el),
        DownstreamStageMsg::Complete(reason) => StageMsg::Stopped(reason),
        DownstreamStageMsg::SetUpstream(upstream) => StageMsg::SetUpstream(upstream),
    }
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
        UpstreamStageMsg::Cancel => StageMsg::Cancel,
    }
}

pub(in crate::stream) struct Stage<A, B, Msg, L: Logic<A, B, Ctl = Msg>>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    logic: L,
    logic_actions: VecDeque<Action<B, Msg>>,
    logic_pulled: bool,
    buffer: VecDeque<A>,
    downstream: ActorRef<DownstreamStageMsg<B>>,
    state: StageState<A, B, Msg>,
    pulled: bool,
    upstream_stopped: bool,
    midstream_stopped: bool,
    downstream_demand: u64,
    upstream_demand: u64,
    calls: usize,
    cancelled: bool,
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

impl<A, B, Msg, L: Logic<A, B, Ctl = Msg>> Stage<A, B, Msg, L>
where
    L: 'static + Send,
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    fn check_upstream_demand(&mut self) {
        if self.upstream_stopped || self.midstream_stopped {
            return;
        }

        let capacity = self.buffer.capacity() as u64;
        let available = capacity - self.upstream_demand;

        /*
        println!(
            "{} c={}, capacity={}, available={}, len={}, updemand={}",
            self.logic.name(),
            self.buffer.capacity(),
            capacity,
            available,
            self.buffer.len(),
            self.upstream_demand
        );*/

        if available >= capacity / 2 {
            if let StageState::Running(ref upstream) = self.state {
                upstream.tell(UpstreamStageMsg::Pull(available));
                self.upstream_demand += available;
            }
        }
    }

    fn receive_logic_event(
        &mut self,
        event: LogicEvent<A, Msg>,
        ctx: &mut ActorContext<StageMsg<A, B, Msg>>,
    ) {
        let action = {
            let mut ctx = StreamContext {
                ctx: StreamContextType::Spawned(ctx),
                calls: self.calls,
            };

            self.logic.receive(event, &mut ctx)
        };

        // @TODO stack overflow possible?

        self.receive_action(action, ctx);

        while let Some(event) = self.logic_actions.pop_front() {
            self.receive_action(event, ctx);
        }
    }

    fn receive_action(
        &mut self,
        action: Action<B, Msg>,
        ctx: &mut ActorContext<StageMsg<A, B, Msg>>,
    ) {
        self.calls += 1;

        if self.midstream_stopped {
            //println!("{} Dropped Action (midstream stopped)", self.logic.name());
            return;
        }

        if self.calls >= MAX_CALLS {
            ctx.actor_ref().tell(StageMsg::Action(action));

            return;
        }

        match action {
            Action::Pull => {
                //println!("{} Action::Pull", self.logic.name());

                if self.pulled {
                    //println!("already pulled, this is a bug");
                    // @TODO must fail as this is a bug
                } else {
                    match self.buffer.pop_front() {
                        Some(el) => {
                            self.upstream_demand -= 1;
                            self.check_upstream_demand();

                            self.receive_logic_event(LogicEvent::Pushed(el), ctx);
                        }

                        None if self.upstream_stopped => {
                            self.receive_logic_event(LogicEvent::Stopped, ctx);
                        }

                        None => {
                            self.pulled = true;
                        }
                    }
                }
            }

            Action::Push(el) => {
                //println!("{} Action::Push", self.logic.name());

                if self.downstream_demand == 0 {
                    // @TODO must fail - logic has violated the rules
                } else {
                    self.downstream.tell(DownstreamStageMsg::Produce(el));
                    self.downstream_demand -= 1;
                    self.logic_pulled = false;

                    if self.downstream_demand > 0 {
                        self.logic_pulled = true;
                        self.receive_logic_event(LogicEvent::Pulled, ctx);
                    }
                }
            }

            Action::Forward(msg) => {
                //println!("{} Action::Forward", self.logic.name());
                self.receive_logic_event(LogicEvent::Forwarded(msg), ctx);
            }

            Action::Cancel => {
                //println!("{} Action::Cancel", self.logic.name());
                self.cancelled = true;

                if !self.upstream_stopped {
                    if let StageState::Running(ref upstream) = self.state {
                        upstream.tell(UpstreamStageMsg::Cancel);
                    }
                }
            }

            Action::Stop(reason) => {
                //println!("{} Action::Stop", self.logic.name());
                self.downstream.tell(DownstreamStageMsg::Complete(reason));

                self.midstream_stopped = true;

                ctx.stop();
            }

            Action::PushAndStop(el, reason) => {
                //println!("{} Action::PushAndStop", self.logic.name());
                self.receive_action(Action::Push(el), ctx);
                //println!("did push");
                self.receive_action(Action::Stop(reason), ctx);
            }

            Action::None => {}
        }
    }
}

impl<A, B, Msg, L: Logic<A, B, Ctl = Msg>> Actor for Stage<A, B, Msg, L>
where
    L: 'static + Send,
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    type Msg = StageMsg<A, B, Msg>;

    fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<StageMsg<A, B, Msg>>) {
        self.calls = 0;

        if let Signal::Started = signal {
            self.downstream.tell(DownstreamStageMsg::SetUpstream(
                ctx.actor_ref().convert(upstream_stage_msg_to_stage_msg),
            ));
        }
    }

    fn receive(&mut self, msg: StageMsg<A, B, Msg>, ctx: &mut ActorContext<StageMsg<A, B, Msg>>) {
        self.calls = 0;

        if self.midstream_stopped {
            return;
        }

        match self.state {
            StageState::Running(_) => {
                match msg {
                    StageMsg::Pull(demand) => {
                        //println!("{} StageMsg::Pull({})", self.logic.name(), demand);
                        // our downstream has requested more elements

                        self.downstream_demand += demand;

                        //println!("pull! downstream demand is now {}", self.downstream_demand);

                        if self.downstream_demand > 0 && !self.logic_pulled {
                            self.logic_pulled = true;

                            self.receive_logic_event(LogicEvent::Pulled, ctx);
                        }
                    }

                    StageMsg::Consume(el) => {
                        //println!("{} StageMsg::Consume", self.logic.name());

                        // our upstream has produced this element

                        if !self.upstream_stopped {
                            if self.pulled {
                                self.upstream_demand -= 1;
                                self.pulled = false;

                                // @TODO assert buffer is empty

                                self.receive_logic_event(LogicEvent::Pushed(el), ctx);

                                self.check_upstream_demand();
                            } else {
                                //println!("{} Consume Buffered", self.logic.name());
                                self.buffer.push_back(el);
                            }
                        }
                    }

                    StageMsg::Action(action) => {
                        self.receive_action(action, ctx);
                    }

                    StageMsg::Cancel => {
                        //println!("{} StageMsg::Cancel", self.logic.name());
                        // downstream has cancelled us

                        // @TODO verify this, as this defers cancellation entirely to the logic
                        //if !self.upstream_stopped {
                        //    upstream.tell(UpstreamStageMsg::Cancel);
                        //}

                        self.receive_logic_event(LogicEvent::Cancelled, ctx);
                    }

                    StageMsg::Stopped(_reason) => {
                        //println!("{} StageMsg::Stopped (cancelled={})", self.logic.name(), self.cancelled);
                        // @TODO reason
                        // upstream has stopped, need to drain buffers and be done
                        // this solution has a rare race though and freezes

                        self.upstream_stopped = true;

                        // TODO here's the problem, see #66
                        if self.buffer.is_empty() || self.cancelled {
                            self.receive_logic_event(LogicEvent::Stopped, ctx);
                            self.buffer.drain(..);
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

                        self.receive_logic_event(LogicEvent::Started, ctx);

                        self.check_upstream_demand();
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

                    self.receive_logic_event(LogicEvent::Started, ctx);

                    self.check_upstream_demand();

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

impl<A, B, Msg, L> LogicContainerFacade<A, B> for IndividualLogic<L>
where
    L: 'static + Logic<A, B, Ctl = Msg> + Send,
    A: 'static + Send,
    B: 'static + Send,
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
            logic_actions: VecDeque::with_capacity(2),
            logic_pulled: false,
            buffer: VecDeque::with_capacity(buffer_size),
            downstream: downstream.clone(),
            state: StageState::Waiting(None),
            pulled: false,
            upstream_stopped: false,
            midstream_stopped: false,
            downstream_demand: 0,
            upstream_demand: 0,
            calls: 0,
            cancelled: false,
        });

        upstream.convert(downstream_stage_msg_to_stage_msg)
    }
}

pub(in crate::stream) enum InternalStreamCtl<Out>
where
    Out: 'static + Send,
{
    Stop,
    Fail,
    FromSink(DownstreamStageMsg<Out>),
    FromSource(UpstreamStageMsg),
    SetDownstream(ActorRef<DownstreamStageMsg<()>>),
}

fn stream_ctl_to_internal_stream_ctl<Out>(ctl: StreamCtl) -> InternalStreamCtl<Out>
where
    Out: 'static + Send,
{
    match ctl {
        StreamCtl::Stop => InternalStreamCtl::Stop,
        StreamCtl::Fail => InternalStreamCtl::Fail,
    }
}

pub(in crate::stream) trait RunnableStream<Out>
where
    Out: Send,
{
    fn run(self: Box<Self>, context: &mut ActorContext<InternalStreamCtl<Out>>);
}

impl<A, Out> RunnableStream<Out> for Fused<(), A, Out>
where
    A: 'static + Send,
    Out: 'static + Send,
{
    fn run(self: Box<Self>, context: &mut ActorContext<InternalStreamCtl<Out>>) {
        // @TODO use the stream ctl actor instead of a new one

        let actor_ref_for_sink = context.actor_ref().convert(InternalStreamCtl::FromSink);

        let mut context_for_upstream = context.spawn_context();

        let logic = Box::new(IndividualLogic { logic: *self });

        let stream = logic.spawn(actor_ref_for_sink, &mut context_for_upstream);

        context
            .actor_ref()
            .tell(InternalStreamCtl::SetDownstream(stream));
    }
}

impl<A, Out> RunnableStream<Out> for UnionLogic<(), A, Out>
where
    A: 'static + Send,
    Out: 'static + Send,
{
    fn run(self: Box<Self>, context: &mut ActorContext<InternalStreamCtl<Out>>) {
        let actor_ref_for_sink = context.actor_ref().convert(InternalStreamCtl::FromSink);

        let mut context_for_upstream = context.spawn_context();

        let stream = self.upstream.spawn(
            self.downstream
                .spawn(actor_ref_for_sink, &mut context_for_upstream),
            &mut context_for_upstream,
        );

        context
            .actor_ref()
            .tell(InternalStreamCtl::SetDownstream(stream));
    }
}

pub(in crate::stream) struct SourceLike<A, M, L>
where
    L: Logic<(), A, Ctl = M>,
    A: Send,
    M: Send,
{
    pub(in crate::stream) logic: L,
    pub(in crate::stream) phantom: PhantomData<(A, M)>,
}

impl<A, M, L> LogicContainerFacade<(), A> for SourceLike<A, M, L>
where
    L: 'static + Logic<(), A, Ctl = M> + Send,
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

        let stage = Stage {
            logic: self.logic,
            logic_actions: VecDeque::with_capacity(2),
            logic_pulled: false,
            buffer: VecDeque::with_capacity(buffer_size),
            downstream,
            state: StageState::Waiting(None),
            pulled: false,
            upstream_stopped: false,
            midstream_stopped: false,
            downstream_demand: 0,
            upstream_demand: 0,
            calls: 0,
            cancelled: false,
        };

        let midstream = context.spawn(stage);

        midstream.convert(downstream_stage_msg_to_stage_msg)
    }
}

impl<Msg, Out> crate::actor::Spawnable<Stream<Out>, (ActorRef<StreamCtl>, StreamComplete<Out>)>
    for ActorContext<Msg>
where
    Msg: 'static + Send,
    Out: 'static + Send,
{
    fn perform_spawn(&mut self, stream: Stream<Out>) -> (ActorRef<StreamCtl>, StreamComplete<Out>) {
        let state = Arc::new(AtomicCell::new(None));

        let controller_ref = self.spawn(StreamController {
            stream: Some(stream),
            state: state.clone(),
            produced: false,
            stream_stopped: false,
        });

        (
            controller_ref.convert(stream_ctl_to_internal_stream_ctl),
            StreamComplete {
                controller_ref,
                state,
            },
        )
    }
}

impl<Msg, Out, F: Fn(Out) -> Msg> Watchable<StreamComplete<Out>, Out, Msg, F> for ActorContext<Msg>
where
    Msg: 'static + Send,
    Out: 'static + Send,
    F: 'static + Send,
{
    fn perform_watch(&mut self, subject: StreamComplete<Out>, convert: F) {
        self.watch(
            &subject.controller_ref.clone(),
            move |_reason: StopReason| {
                // @TODO inspect reason

                match subject.state.swap(None) {
                    Some(value) => convert(value),

                    None => {
                        panic!("TODO");
                        // @TODO
                    }
                }
            },
        );
    }
}

struct StreamController<Out>
where
    Out: Send,
{
    stream: Option<Stream<Out>>,
    state: Arc<AtomicCell<Option<Out>>>,
    produced: bool,
    stream_stopped: bool,
    // @TODO
    //downstream: Option<(StageRef<DownstreamStageMsg<()>>, SpecialContext<DownstreamStageMsg<()>>)>,
}

impl<Out> StreamController<Out> where Out: Send {}

impl<Out> Actor for StreamController<Out>
where
    Out: 'static + Send,
{
    type Msg = InternalStreamCtl<Out>;

    fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<InternalStreamCtl<Out>>) {
        if let Signal::Started = signal {
            self.stream.take().unwrap().run(ctx);
        }
    }

    fn receive(
        &mut self,
        msg: InternalStreamCtl<Out>,
        ctx: &mut ActorContext<InternalStreamCtl<Out>>,
    ) {
        if self.stream_stopped {
            return;
        }

        match msg {
            InternalStreamCtl::Stop => {}

            InternalStreamCtl::Fail => {}

            InternalStreamCtl::FromSink(DownstreamStageMsg::SetUpstream(upstream)) => {
                upstream.tell(UpstreamStageMsg::Pull(1));
            }

            InternalStreamCtl::FromSink(DownstreamStageMsg::Produce(value)) => {
                let _ = self.state.swap(Some(value));

                self.produced = true;
            }

            InternalStreamCtl::FromSink(DownstreamStageMsg::Complete(_reason)) => {
                // @TODO reason
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

                let actor_ref_for_source = ctx
                    .actor_ref()
                    .convert(upstream_stage_msg_to_internal_stream_ctl);

                downstream.tell(DownstreamStageMsg::SetUpstream(actor_ref_for_source));

                downstream.tell(DownstreamStageMsg::Complete(None));
            }

            InternalStreamCtl::FromSource(UpstreamStageMsg::Pull(_)) => {
                // nothing to do, we already sent a completed
            }

            InternalStreamCtl::FromSource(UpstreamStageMsg::Cancel) => {
                // nothing to do, we already sent a completed
            }
        }
    }
}
