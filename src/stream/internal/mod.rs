use crate::actor::{
    Actor, ActorContext, ActorRef, ActorSpawnContext, FailureReason, Signal, Spawnable, StopReason, Watchable,
};
use crate::stream::flow::Flow;
use crate::stream::sink::Sink;
use crate::stream::source::Source;
use crate::stream::{Action, Logic, Stream, StreamComplete, StreamContext, StreamCtl};
use crossbeam::atomic::AtomicCell;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;

pub(in crate::stream) trait LogicContainerFacade<A, B, Ctl>
where
    A: 'static + Send,
    B: 'static + Send,
    Ctl: 'static + Send,
{
    fn spawn(
        self: Box<Self>,
        downstream: ActorRef<DownstreamStageMsg<B>>,
        context: &mut ActorSpawnContext,
    ) -> ActorRef<DownstreamStageMsg<A>>;
}

pub(in crate::stream) struct LogicContainer<A, B, Msg, L>
where
    L: Logic<A, B, Msg>,
    A: Send,
    B: Send,
    Msg: Send,
{
    pub(in crate::stream) logic: L,
    pub(in crate::stream) phantom: PhantomData<(A, B, Msg)>,
}

pub(in crate::stream) struct FlowWithFlow<A, B, C> {
    pub(in crate::stream) one: Flow<A, B>,
    pub(in crate::stream) two: Flow<B, C>,
}

impl<A, B, C> LogicContainerFacade<A, C, ProtectedStreamCtl> for FlowWithFlow<A, B, C>
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
        let two = self.two.logic.spawn(downstream, context);
        let one = self.one.logic.spawn(two, context);

        one
    }
}


/*
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

*/

















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
    Converted(UpstreamStageMsg),
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

        println!(
            "c={}, capacity={}, available={}, len={}, updemand={}",
            self.buffer.capacity(),
            capacity,
            available,
            self.buffer.len(),
            self.upstream_demand
        );

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

                            if let Some(action) = self.logic.pushed(el, &mut StreamContext { ctx })
                            {
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

            Action::Complete(reason) => {
                self.downstream.tell(DownstreamStageMsg::Complete(reason));

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

                                if let Some(action) =
                                    self.logic.pushed(el, &mut StreamContext { ctx })
                                {
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

                    StageMsg::Stopped(reason) => {
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

pub(in crate::stream) enum ProtectedStreamCtl {
    Stop,
    Fail,
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

fn protected_stream_ctl_to_internal_stream_ctl<Out>(
    ctl: ProtectedStreamCtl,
) -> InternalStreamCtl<Out>
where
    Out: 'static + Send,
{
    match ctl {
        ProtectedStreamCtl::Stop => InternalStreamCtl::Stop,
        ProtectedStreamCtl::Fail => InternalStreamCtl::Fail,
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

        let upstream_ref = self.producer.spawn(
            self.sink
                .logic
                .spawn(actor_ref_for_sink, &mut context_for_upstream),
            &mut context_for_upstream,
        );

        upstream_ref.tell(DownstreamStageMsg::SetUpstream(actor_ref_for_source));

        context
            .actor_ref()
            .tell(InternalStreamCtl::SetDownstream(upstream_ref));
    }
}

pub(in crate::stream) trait Producer<In, Out>
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

pub(in crate::stream) struct SourceLike<A, M, L>
where
    L: Logic<(), A, M>,
    A: Send,
    M: Send,
{
    pub(in crate::stream) logic: L,
    pub(in crate::stream) phantom: PhantomData<(A, M)>,
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

impl<A> Producer<(), A> for Source<A>
where
    A: 'static + Send,
{
    fn spawn(
        self: Box<Self>,
        downstream: ActorRef<DownstreamStageMsg<A>>,
        context: &mut ActorSpawnContext,
    ) -> ActorRef<DownstreamStageMsg<()>> {
        let upstream = self.producer().spawn(downstream.clone(), context);

        // @TODO double conversion here..
        downstream.tell(DownstreamStageMsg::SetUpstream(
            upstream.convert(upstream_stage_msg_to_downstream_stage_msg),
        ));

        upstream
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

impl<Out> Actor<InternalStreamCtl<Out>> for StreamController<Out>
where
    Out: Send,
{
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
                println!("pulling 1 from the sink");
                upstream.tell(UpstreamStageMsg::Pull(1));
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
