use crate::actor::{
    Actor, ActorContext, ActorRef, ActorSpawnContext, FailureReason, Signal, Spawnable, StopReason,
    Watchable,
};
use crate::stream::flow::Flow;
use crate::stream::sink::Sink;
use crate::stream::source::Source;
use crate::stream::{Action, Logic, LogicEvent, Stream, StreamComplete, StreamContext, StreamCtl};
use crossbeam::atomic::AtomicCell;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;

pub(in crate::stream) trait LogicContainerFacade<A, B>
where
    A: 'static + Send,
    B: 'static + Send,
{
    fn spawn(
        self: Box<Self>,
        downstream: Downstream<B>,
        context: &mut ActorSpawnContext,
    ) -> Downstream<A>;
}

pub(in crate::stream) struct LogicContainer<L, Msg> {
    pub(in crate::stream) logic: L,
    pub(in crate::stream) phantom: PhantomData<Msg>,
}

pub(in crate::stream) struct FlowWithFlow<A, B, C> {
    pub(in crate::stream) one: Flow<A, B>,
    pub(in crate::stream) two: Flow<B, C>,
}

impl<A, B, C> LogicContainerFacade<A, C> for FlowWithFlow<A, B, C>
where
    A: 'static + Send,
    B: 'static + Send,
    C: 'static + Send,
{
    fn spawn(
        self: Box<Self>,
        downstream: Downstream<C>,
        context: &mut ActorSpawnContext,
    ) -> Downstream<A> {
        let downstream = self.two.logic.spawn(downstream, context);

        self.one.logic.spawn(downstream, context)
    }
}

pub(in crate::stream) struct FlowWithSink<A, B, Out>
where
    A: 'static + Send,
    B: 'static + Send,
    Out: 'static + Send {
    pub(in crate::stream) flow: Flow<A, B>,
    pub(in crate::stream) sink: Sink<B, Out>,
}

impl<A, B, Out> LogicContainerFacade<A, Out> for FlowWithSink<A, B, Out>
where
    A: 'static + Send,
    B: 'static + Send,
    Out: 'static + Send
{
    fn spawn(
        self: Box<Self>,
        downstream: Downstream<Out>,
        context: &mut ActorSpawnContext,
    ) -> Downstream<A> {
        if self.sink.fused {
            unimplemented!()
        } else {
            let sink = self.sink.logic.spawn(downstream, context);

            self.flow.logic.spawn(sink, context)
        }
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
    SetUpstream(Upstream),
    Converted(UpstreamStageMsg),
}

pub(in crate::stream) enum StageMsg<A, B, Msg>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    SetUpstream(Upstream),
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
        DownstreamStageMsg::Converted(UpstreamStageMsg::Pull(demand)) => StageMsg::Pull(demand),
        DownstreamStageMsg::Converted(UpstreamStageMsg::Cancel) => StageMsg::Cancel,
    }
}

fn downstream_stage_msg_to_internal_stream_ctl<A>(
    msg: DownstreamStageMsg<A>,
) -> InternalStreamCtl<A>
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
        UpstreamStageMsg::Cancel => StageMsg::Cancel,
    }
}

fn upstream_stage_msg_to_downstream_stage_msg<A>(msg: UpstreamStageMsg) -> DownstreamStageMsg<A>
where
    A: 'static + Send,
{
    DownstreamStageMsg::Converted(msg)
}

pub(in crate::stream) struct Stage<A, B, Msg, L: Logic<A, B, Ctl = Msg>>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    logic: L,
    logic_actions: VecDeque<Action<B, Msg>>,
    buffer: VecDeque<A>,
    phantom: PhantomData<(A, B, Msg)>,
    downstream: Downstream<B>,
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
    Running(Upstream),
}

pub(in crate::stream) enum Upstream {
    Spawned(ActorRef<UpstreamStageMsg>),
    Fused(Box<dyn Actor<Msg = UpstreamStageMsg> + Send>)
}

impl<A, B, Msg, L: Logic<A, B, Ctl = Msg>> Stage<A, B, Msg, L>
where
    L: 'static + Send,
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

        println!(
            "c={}, capacity={}, available={}, len={}, updemand={}",
            self.buffer.capacity(),
            capacity,
            available,
            self.buffer.len(),
            self.upstream_demand
        );

        if available >= capacity / 2 {
            match self.state {
                StageState::Running(Upstream::Spawned(ref upstream)) => {
                    upstream.tell(UpstreamStageMsg::Pull(available));
                    self.upstream_demand += available;
                }

                StageState::Running(Upstream::Fused(ref upstream)) => {
                    unimplemented!();
                }

                StageState::Waiting(_) => {}
            }
        }
    }

    fn receive_logic_event(
        &mut self,
        event: LogicEvent<A, Msg>,
        ctx: &mut ActorContext<StageMsg<A, B, Msg>>,
    ) {
        self.logic
            .receive(event, &mut StreamContext::new(ctx, &mut self.logic_actions));

        // @TODO stack overflow possible when we implement fusion -- should bound
        //       this and fall back to telling ourselves after some amount

        while let Some(event) = self.logic_actions.pop_front() {
            self.receive_action(event, ctx);
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

                            self.receive_logic_event(LogicEvent::Pushed(el), ctx);
                        }

                        None if self.upstream_stopped => {
                            self.receive_logic_event(LogicEvent::Stopped, ctx);
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
                    match self.downstream {
                        Downstream::Spawned(ref downstream) => {
                            downstream.tell(DownstreamStageMsg::Produce(el));
                            self.downstream_demand -= 1;

                            if self.downstream_demand > 0 {
                                self.receive_logic_event(LogicEvent::Pulled, ctx);
                            }
                        }

                        Downstream::Fused(ref downstream) => {
                            unimplemented!();
                        }
                    }
                }
            }

            Action::Forward(msg) => {
                println!("{} Action::Forward", self.logic.name());
                self.receive_logic_event(LogicEvent::Forwarded(msg), ctx);
            }

            Action::Cancel => {
                if !self.upstream_stopped {
                    match self.state {
                        StageState::Running(Upstream::Spawned(ref upstream)) => {
                            upstream.tell(UpstreamStageMsg::Cancel);
                        }

                        StageState::Running(Upstream::Fused(ref upstream)) => {
                            unimplemented!();
                        }

                        StageState::Waiting(_) => {
                            unimplemented!();
                        }
                    }
                }
            }

            Action::Complete(reason) => {
                match self.downstream {
                    Downstream::Spawned(ref downstream) => {
                        downstream.tell(DownstreamStageMsg::Complete(reason));

                        self.midstream_stopped = true;

                        ctx.stop();
                    }

                    Downstream::Fused(ref fused) => {
                        unimplemented!();
                    }
                }
            }
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
        if let Signal::Started = signal {
            match self.downstream {
                Downstream::Spawned(ref downstream) => {
                    println!("telling upstream");
                    downstream.tell(DownstreamStageMsg::SetUpstream(
                        Upstream::Spawned(ctx.actor_ref().convert(upstream_stage_msg_to_stage_msg)),
                    ));
                }

                Downstream::Fused(ref downstream) => {
                    unimplemented!();
                }
            }
        }
    }

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
                            self.receive_logic_event(LogicEvent::Pulled, ctx);
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

                                self.receive_logic_event(LogicEvent::Pushed(el), ctx);

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
                            match self.state {
                                StageState::Running(Upstream::Spawned(ref upstream)) => {
                                    upstream.tell(UpstreamStageMsg::Cancel);
                                }

                                StageState::Running(Upstream::Fused(ref upstream)) => {
                                    unimplemented!();
                                }

                                StageState::Waiting(_) => {
                                    unimplemented!();
                                }
                            }
                        }

                        self.receive_logic_event(LogicEvent::Cancelled, ctx);
                    }

                    StageMsg::Stopped(reason) => {
                        // upstream has stopped, need to drain buffers and be done

                        println!("{} UPSTREAM HAS STOPPED", self.logic.name());
                        self.upstream_stopped = true;

                        if self.buffer.is_empty() {
                            self.receive_logic_event(LogicEvent::Stopped, ctx);
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

                    self.receive_logic_event(LogicEvent::Started, ctx);

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

impl<A, B, Msg, L> LogicContainerFacade<A, B> for LogicContainer<L, Msg>
where
    L: 'static + Logic<A, B, Ctl = Msg> + Send,
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    fn spawn(
        self: Box<Self>,
        downstream: Downstream<B>,
        context: &mut ActorSpawnContext,
    ) -> Downstream<A> {
        if /*self.fused */ false {
            unimplemented!()
        } else {
                let buffer_size = self.logic.buffer_size().unwrap_or_else(|| {
                    context
                        .system_context()
                        .config()
                        .default_streams_buffer_size
                });

                let upstream = context.spawn(Stage {
                    logic: self.logic,
                    logic_actions: VecDeque::with_capacity(2),
                    buffer: VecDeque::with_capacity(buffer_size),
                    phantom: PhantomData,
                    downstream,
                    state: StageState::Waiting(None),
                    pulled: false,
                    upstream_stopped: false,
                    midstream_stopped: false,
                    downstream_demand: 0,
                    upstream_demand: 0,
                });

                Downstream::Spawned(upstream.convert(downstream_stage_msg_to_stage_msg))
        }
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

pub(in crate::stream) struct ProducerWithSink<A, Out>
where
    A: 'static + Send,
    Out: 'static + Send,
{
    pub(in crate::stream) producer: Box<Producer<(), A> + Send>,
    pub(in crate::stream) sink: Sink<A, Out>,
    pub(in crate::stream) phantom: PhantomData<Out>,
}

pub(in crate::stream) trait RunnableStream<Out>
where
    Out: Send,
{
    fn run(self: Box<Self>, context: &mut ActorContext<InternalStreamCtl<Out>>);
}

impl<A, Out> RunnableStream<Out> for ProducerWithSink<A, Out>
where
    A: 'static + Send,
    Out: 'static + Send,
{
    fn run(self: Box<Self>, context: &mut ActorContext<InternalStreamCtl<Out>>) {
        let actor_ref_for_sink = context
            .actor_ref()
            .convert(downstream_stage_msg_to_internal_stream_ctl);
        let actor_ref_for_source = context
            .actor_ref()
            .convert(upstream_stage_msg_to_internal_stream_ctl);

        let mut context_for_upstream = context.spawn_context();

        let upstream = self.producer.spawn(
            self.sink
                .logic
                .spawn(Downstream::Spawned(actor_ref_for_sink), &mut context_for_upstream),
            &mut context_for_upstream,
        );

        match upstream {
            Downstream::Spawned(upstream) => {
                upstream.tell(DownstreamStageMsg::SetUpstream(Upstream::Spawned(actor_ref_for_source)));

                context.actor_ref()
                       .tell(InternalStreamCtl::SetDownstream(upstream));
            }

            Downstream::Fused(_) => {
                unimplemented!();
            }
        }

    }
}

pub(in crate::stream) trait Producer<In, Out>
where
    In: 'static + Send,
    Out: 'static + Send,
{
    fn spawn(
        self: Box<Self>,
        downstream: Downstream<Out>,
        context: &mut ActorSpawnContext,
    ) -> Downstream<In>;
}

pub(in crate::stream) trait FusedStage<A> {
}

pub(in crate::stream) enum Downstream<A> where A: 'static + Send {
    Spawned(ActorRef<DownstreamStageMsg<A>>),
    Fused(Box<dyn Actor<Msg = DownstreamStageMsg<A>> + Send>)
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

impl<A, M, L> Producer<(), A> for SourceLike<A, M, L>
where
    L: 'static + Logic<(), A, Ctl = M> + Send,
    A: 'static + Send,
    M: 'static + Send,
{
    fn spawn(
        self: Box<Self>,
        downstream: Downstream<A>,
        context: &mut ActorSpawnContext,
    ) -> Downstream<()> {
        let buffer_size = self.logic.buffer_size().unwrap_or_else(|| {
            context
                .system_context()
                .config()
                .default_streams_buffer_size
        });

        let stage = Stage {
            logic: self.logic,
            logic_actions: VecDeque::with_capacity(2),
            buffer: VecDeque::with_capacity(buffer_size),
            phantom: PhantomData,
            downstream,
            state: StageState::Waiting(None),
            pulled: false,
            upstream_stopped: false,
            midstream_stopped: false,
            downstream_demand: 0,
            upstream_demand: 0,
        };

        let upstream = context.spawn(stage);


        Downstream::Spawned(upstream.convert(downstream_stage_msg_to_stage_msg))
    }
}

struct FusedFlow<A, B> where B: 'static + Send {
    flow: Flow<A, B>,
    downstream: Downstream<B>
}

impl<A, B> FusedStage<A> for FusedFlow<A, B> where B: 'static + Send {}

struct DoubleFusedFlow<A, B> where B: 'static + Send {
    flow: Flow<A, B>,
    downstream: Downstream<B>
}

impl<A, B> FusedStage<A> for DoubleFusedFlow<A, B> where B: 'static + Send {}

pub struct ProducerWithFlow<A, B, C> {
    pub(in crate::stream) producer: Box<Producer<A, B> + Send>,
    pub(in crate::stream) flow: Flow<B, C>,
}

impl<A, B, C> Producer<A, C> for ProducerWithFlow<A, B, C>
where
    A: 'static + Send,
    B: 'static + Send,
    C: 'static + Send,
{
    fn spawn(
        self: Box<Self>,
        downstream: Downstream<C>,
        context: &mut ActorSpawnContext,
    ) -> Downstream<A> {
        let flow = self.flow.logic.spawn(downstream, context);

        self.producer.spawn(flow, context)
    }
}

impl<A> Producer<(), A> for Source<A>
where
    A: 'static + Send,
{
    fn spawn(
        self: Box<Self>,
        downstream: Downstream<A>,
        context: &mut ActorSpawnContext,
    ) -> Downstream<()>{
        self.producer().spawn(downstream, context)
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
            move |reason: StopReason| {
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
}

impl<Out> Actor for StreamController<Out>
where
    Out: 'static + Send,
{
    type Msg = InternalStreamCtl<Out>;

    fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<InternalStreamCtl<Out>>) {
        match signal {
            Signal::Started => {
                self.stream.take().unwrap().run(ctx);
            }

            _ => {}
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
                match upstream {
                    Upstream::Spawned(upstream) => {
                        println!("pulling 1 from the sink");
                        upstream.tell(UpstreamStageMsg::Pull(1));
                    }

                    Upstream::Fused(upstream) => {
                        unimplemented!()
                    }
                }
            }

            InternalStreamCtl::FromSink(DownstreamStageMsg::Produce(value)) => {
                let _ = self.state.swap(Some(value));

                self.produced = true;

                println!("sink got final value");
            }

            InternalStreamCtl::FromSink(DownstreamStageMsg::Complete(reason)) => {
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

                downstream.tell(DownstreamStageMsg::Complete(None));
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
