use crate::actor::{Actor, ActorContext, ActorRef, Signal, SystemActorRef};
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::time::Duration;

enum Action<A, Msg> {
    Stop,
    Pull,
    Push(A),
    Forward(Msg),
}

trait Logic<A, B, Msg>
where
    A: Send,
    B: Send,
    Msg: Send,
{
    fn buffer_size(&self) -> Option<usize> {
        None
    }

    #[must_use]
    fn pulled(&mut self) -> Option<Action<B, Msg>>;

    #[must_use]
    fn pushed(&mut self, el: A) -> Option<Action<B, Msg>>;

    #[must_use]
    fn started(&mut self, stream_ctx: StreamContext<A, B, Msg>) -> Option<Action<B, Msg>>;

    fn stopped(&mut self);

    #[must_use]
    fn forwarded(&mut self, msg: Msg) -> Option<Action<B, Msg>>;
}

trait LogicContainerFacade<A, B>
where
    A: 'static + Send,
    B: 'static + Send,
{
    fn spawn(
        self: Box<Self>,
        downstream: ActorRef<DownstreamStageMsg<B>>,
        context: &mut ActorContext<InternalStreamCtl>,
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
    Stop,
    Pull(u64),
}

enum DownstreamStageMsg<A>
where
    A: 'static + Send,
{
    Produce(A),
    SetUpstream(ActorRef<UpstreamStageMsg>),

    Stop,
    Pull(u64),
}

enum StageMsg<A, B, Msg>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    SetUpstream(ActorRef<UpstreamStageMsg>),
    Pull(u64),
    Consume(A),
    Action(Action<B, Msg>),
    ScheduleDelivery(String, Duration, Msg),
}

fn downstream_stage_msg_to_stage_msg<A, B, Msg>(msg: DownstreamStageMsg<A>) -> StageMsg<A, B, Msg>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    match msg {
        DownstreamStageMsg::Produce(el) => StageMsg::Consume(el),
        DownstreamStageMsg::SetUpstream(upstream) => StageMsg::SetUpstream(upstream),

        DownstreamStageMsg::Pull(demand) => StageMsg::Pull(demand),
        DownstreamStageMsg::Stop => StageMsg::Action(Action::Stop),
    }
}

fn upstream_stage_msg_to_stage_msg<A, B, Msg>(msg: UpstreamStageMsg) -> StageMsg<A, B, Msg>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    match msg {
        UpstreamStageMsg::Pull(demand) => StageMsg::Pull(demand),
        UpstreamStageMsg::Stop => StageMsg::Action(Action::Stop),
    }
}

fn upstream_stage_msg_to_downstream_stage_msg<A>(msg: UpstreamStageMsg) -> DownstreamStageMsg<A>
where
    A: 'static + Send,
{
    match msg {
        UpstreamStageMsg::Pull(demand) => DownstreamStageMsg::Pull(demand),
        UpstreamStageMsg::Stop => DownstreamStageMsg::Stop,
    }
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
    upstream: Option<ActorRef<UpstreamStageMsg>>,
    downstream: ActorRef<DownstreamStageMsg<B>>,
    state: StageState<A, B, Msg>,
    pulled: bool,
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
        let capacity = self.buffer.capacity() as u64;
        let available = capacity - self.upstream_demand;

        //println!("capacity={}, available={}, len={}, updemand={}", capacity, available, self.buffer.len(), self.upstream_demand);

        if self.buffer.len() > 100 {
            //std::process::exit(1);
        }

        if available >= capacity / 2 {
            if let StageState::Running(ref upstream) = self.state {
                //println!("telling upstream we want {}", available);
                upstream.tell(UpstreamStageMsg::Pull(available));
                self.upstream_demand += available;
            }
        }
    }

    fn receive_action(
        &mut self,
        action: Action<B, Msg>,
        context: &mut ActorContext<StageMsg<A, B, Msg>>,
    ) {
        match action {
            Action::Pull => {
                //println!("Action::Pull");

                if self.pulled {
                    //println!("already pulled, this is a bug");
                    // @TODO must fail as this is a bug
                } else {
                    match self.buffer.pop_front() {
                        Some(el) => {
                            self.upstream_demand -= 1;
                            self.check_upstream_demand(context);

                            if let Some(action) = self.logic.pushed(el) {
                                self.receive_action(action, context);
                            }
                        }

                        None => {
                            self.pulled = true;
                        }
                    }
                }
            }

            Action::Push(el) => {
                //println!("Action::Push");
                if self.downstream_demand == 0 {
                    // @TODO must fail - logic has violated the rules
                } else {
                    self.downstream.tell(DownstreamStageMsg::Produce(el));
                    self.downstream_demand -= 1;

                    if self.downstream_demand > 0 {
                        if let Some(action) = self.logic.pulled() {
                            self.receive_action(action, context);
                        }
                    }
                }
            }

            Action::Forward(msg) => {
                //println!("Action::Forward");
                if let Some(action) = self.logic.forwarded(msg) {
                    self.receive_action(action, context);
                }
            }

            Action::Stop => {
                //println!("Action::Stop");
                // @TODO
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
        match self.state {
            StageState::Running(_) => {
                match msg {
                    StageMsg::Pull(demand) => {
                        //println!("StageMsg::Pull");
                        // our downstream has requested more elements

                        self.downstream_demand += demand;

                        //println!("pull! downstream demand is now {}", self.downstream_demand);

                        if self.downstream_demand > 0 {
                            if let Some(action) = self.logic.pulled() {
                                self.receive_action(action, ctx);
                            }
                        }
                    }

                    StageMsg::Consume(el) => {
                        //println!("StageMsg::Consume");

                        // our upstream has produced this element

                        self.check_upstream_demand(ctx);

                        if self.pulled {
                            self.upstream_demand -= 1;
                            self.pulled = false;

                            // @TODO assert buffer is empty

                            if let Some(action) = self.logic.pushed(el) {
                                self.receive_action(action, ctx);
                            }
                        } else {
                            //////println!("PRE PUSH, LEN={}", self.buffer.len());
                            self.buffer.push_back(el);
                            //println!("POST PUSH, LEN={}", self.buffer.len());
                        }
                    }

                    StageMsg::Action(action) => {
                        self.receive_action(action, ctx);
                    }

                    StageMsg::ScheduleDelivery(name, timeout, msg) => {
                        ctx.schedule_delivery(
                            &name,
                            timeout,
                            StageMsg::Action(Action::Forward(msg)),
                        );
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

                        let stream_ctx = StreamContext::new(ctx.actor_ref().clone());

                        if let Some(action) = self.logic.started(stream_ctx) {
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

                    let stream_ctx = StreamContext::new(ctx.actor_ref().clone());

                    if let Some(action) = self.logic.started(stream_ctx) {
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

impl<A, B, Msg, L> LogicContainerFacade<A, B> for LogicContainer<A, B, Msg, L>
where
    L: 'static + Logic<A, B, Msg> + Send,
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    fn spawn(
        self: Box<Self>,
        downstream: ActorRef<DownstreamStageMsg<B>>,
        context: &mut ActorContext<InternalStreamCtl>,
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
            upstream: None,
            downstream: downstream.clone(),
            state: StageState::Waiting(None),
            pulled: false,
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

struct Ignore<A>
where
    A: 'static + Send,
{
    phantom: PhantomData<A>,
}

impl<A> Logic<A, (), ()> for Ignore<A>
where
    A: 'static + Send,
{
    fn pulled(&mut self) -> Option<Action<(), ()>> {
        None
    }

    fn pushed(&mut self, el: A) -> Option<Action<(), ()>> {
        Some(Action::Pull)
    }

    fn started(&mut self, mut stream_ctx: StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        Some(Action::Pull)
    }

    fn stopped(&mut self) {}

    fn forwarded(&mut self, msg: ()) -> Option<Action<(), ()>> {
        None
    }
}

struct ForEach<A, F>
where
    F: FnMut(A) -> (),
    A: 'static + Send,
{
    for_each_fn: F,
    phantom: PhantomData<A>,
}

impl<A, F> Logic<A, (), ()> for ForEach<A, F>
where
    F: FnMut(A) -> (),
    A: 'static + Send,
{
    fn pulled(&mut self) -> Option<Action<(), ()>> {
        None
    }

    fn pushed(&mut self, el: A) -> Option<Action<(), ()>> {
        (self.for_each_fn)(el);

        Some(Action::Pull)
    }

    fn started(&mut self, mut stream_ctx: StreamContext<A, (), ()>) -> Option<Action<(), ()>> {
        Some(Action::Pull)
    }

    fn stopped(&mut self) {}

    fn forwarded(&mut self, msg: ()) -> Option<Action<(), ()>> {
        None
    }
}

//
// FLOWS
//

struct IdentityLogic<A>
where
    A: 'static + Send,
{
    phantom: PhantomData<A>,
}

impl<A> Logic<A, A, ()> for IdentityLogic<A>
where
    A: 'static + Send,
{
    fn pulled(&mut self) -> Option<Action<A, ()>> {
        Some(Action::Pull)
    }

    fn pushed(&mut self, el: A) -> Option<Action<A, ()>> {
        Some(Action::Push(el))
    }

    fn started(&mut self, stream_ctx: StreamContext<A, A, ()>) -> Option<Action<A, ()>> {
        None
    }

    fn stopped(&mut self) {}

    fn forwarded(&mut self, msg: ()) -> Option<Action<A, ()>> {
        None
    }
}

enum DelayLogicMsg {
    Ready,
}

struct DelayLogic<A>
where
    A: 'static + Send,
{
    delay: Duration,
    pulled: bool,
    pushable: bool,
    stream_ctx: StreamContext<A, A, DelayLogicMsg>,
}

impl<A> DelayLogic<A>
where
    A: 'static + Send,
{
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            pulled: false,
            pushable: true,
            stream_ctx: StreamContext::empty(),
        }
    }
}

impl<A> Logic<A, A, DelayLogicMsg> for DelayLogic<A>
where
    A: 'static + Send,
{
    fn pulled(&mut self) -> Option<Action<A, DelayLogicMsg>> {
        self.pulled = true;

        if self.pushable {
            Some(Action::Pull)
        } else {
            None
        }
    }

    fn pushed(&mut self, el: A) -> Option<Action<A, DelayLogicMsg>> {
        self.pulled = false;
        self.pushable = false;

        self.stream_ctx
            .schedule_delivery("ready", self.delay.clone(), DelayLogicMsg::Ready);

        Some(Action::Push(el))
    }

    fn started(
        &mut self,
        stream_ctx: StreamContext<A, A, DelayLogicMsg>,
    ) -> Option<Action<A, DelayLogicMsg>> {
        self.stream_ctx = stream_ctx;

        None
    }

    fn stopped(&mut self) {}

    fn forwarded(&mut self, msg: DelayLogicMsg) -> Option<Action<A, DelayLogicMsg>> {
        match msg {
            DelayLogicMsg::Ready => {
                self.pushable = true;

                if self.pulled {
                    Some(Action::Pull)
                } else {
                    None
                }
            }
        }
    }
}

struct FilterLogic<A, F: FnMut(&A) -> bool>
where
    A: 'static + Send,
    F: 'static + Send,
{
    filter: F,
    phantom: PhantomData<A>,
}

impl<A, F: FnMut(&A) -> bool> FilterLogic<A, F>
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

impl<A, F: FnMut(&A) -> bool> Logic<A, A, ()> for FilterLogic<A, F>
where
    A: 'static + Send,
    F: 'static + Send,
{
    fn pulled(&mut self) -> Option<Action<A, ()>> {
        Some(Action::Pull)
    }

    fn pushed(&mut self, el: A) -> Option<Action<A, ()>> {
        Some(if (self.filter)(&el) {
            Action::Push(el)
        } else {
            Action::Pull
        })
    }

    fn started(&mut self, stream_ctx: StreamContext<A, A, ()>) -> Option<Action<A, ()>> {
        None
    }

    fn stopped(&mut self) {}

    fn forwarded(&mut self, msg: ()) -> Option<Action<A, ()>> {
        None
    }
}

struct MapLogic<A, B, F: FnMut(A) -> B>
where
    A: Send,
    B: Send,
{
    map_fn: F,
    phantom: PhantomData<(A, B)>,
}

impl<A, B, F: FnMut(A) -> B> MapLogic<A, B, F>
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

impl<A, B, F> Logic<A, B, ()> for MapLogic<A, B, F>
where
    F: FnMut(A) -> B,
    A: 'static + Send,
    B: 'static + Send,
{
    fn pulled(&mut self) -> Option<Action<B, ()>> {
        Some(Action::Pull)
    }

    fn pushed(&mut self, el: A) -> Option<Action<B, ()>> {
        Some(Action::Push((self.map_fn)(el)))
    }

    fn started(&mut self, stream_ctx: StreamContext<A, B, ()>) -> Option<Action<B, ()>> {
        None
    }

    fn stopped(&mut self) {}

    fn forwarded(&mut self, msg: ()) -> Option<Action<B, ()>> {
        None
    }
}

struct TakeWhile<A>
where
    A: 'static + Send,
{
    while_fn: Box<FnMut(&A) -> bool>, // @TODO dont box
}

impl<A> Logic<A, A, ()> for TakeWhile<A>
where
    A: 'static + Send,
{
    fn pulled(&mut self) -> Option<Action<A, ()>> {
        Some(Action::Pull)
    }

    fn pushed(&mut self, el: A) -> Option<Action<A, ()>> {
        Some(if (self.while_fn)(&el) {
            Action::Push(el)
        } else {
            Action::Pull
        })
    }

    fn started(&mut self, stream_ctx: StreamContext<A, A, ()>) -> Option<Action<A, ()>> {
        None
    }

    fn stopped(&mut self) {}

    fn forwarded(&mut self, msg: ()) -> Option<Action<A, ()>> {
        None
    }
}

//
// SOURCES
//

struct RepeatLogic<A>
where
    A: 'static + Clone + Send,
{
    el: A,
}

impl<A> Logic<(), A, ()> for RepeatLogic<A>
where
    A: 'static + Clone + Send,
{
    fn pulled(&mut self) -> Option<Action<A, ()>> {
        Some(Action::Push(self.el.clone()))
    }

    fn pushed(&mut self, el: ()) -> Option<Action<A, ()>> {
        None
    }

    fn started(&mut self, stream_ctx: StreamContext<(), A, ()>) -> Option<Action<A, ()>> {
        None
    }

    fn stopped(&mut self) {}

    fn forwarded(&mut self, msg: ()) -> Option<Action<A, ()>> {
        None
    }
}

struct IteratorLogic<A, I: Iterator<Item = A>>
where
    A: 'static + Send,
{
    iterator: I,
}

impl<A, I: Iterator<Item = A>> Logic<(), A, ()> for IteratorLogic<A, I>
where
    A: 'static + Send,
{
    fn pulled(&mut self) -> Option<Action<A, ()>> {
        Some(if let Some(element) = self.iterator.next() {
            Action::Push(element)
        } else {
            Action::Stop
        })
    }

    fn pushed(&mut self, el: ()) -> Option<Action<A, ()>> {
        None
    }

    fn started(&mut self, stream_ctx: StreamContext<(), A, ()>) -> Option<Action<A, ()>> {
        None
    }

    fn stopped(&mut self) {}

    fn forwarded(&mut self, msg: ()) -> Option<Action<A, ()>> {
        None
    }
}

#[derive(Clone)]
struct StreamContext<A, B, Msg>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    stage_ref: ActorRef<StageMsg<A, B, Msg>>,
    phantom: PhantomData<(A, B, Msg)>, // @TODO needed?
}

impl<A, B, Msg> StreamContext<A, B, Msg>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    fn empty() -> Self {
        Self {
            stage_ref: ActorRef::empty(),
            phantom: PhantomData,
        }
    }

    fn new(stage_ref: ActorRef<StageMsg<A, B, Msg>>) -> Self {
        Self {
            stage_ref,
            phantom: PhantomData,
        }
    }

    fn schedule_delivery<S: AsRef<str>>(&mut self, name: S, timeout: Duration, msg: Msg) {
        self.stage_ref.tell(StageMsg::ScheduleDelivery(
            name.as_ref().to_owned(),
            timeout,
            msg,
        ));
    }

    fn tell(&mut self, action: Action<B, Msg>) {
        self.stage_ref.tell(StageMsg::Action(action));
    }
}

enum StreamCtl {
    Stop,
    Fail,
}

enum InternalStreamCtl {
    Stop,
    Fail,
}

fn stream_ctl_to_internal_stream_ctl(ctl: StreamCtl) -> InternalStreamCtl {
    match ctl {
        StreamCtl::Stop => InternalStreamCtl::Stop,
        StreamCtl::Fail => InternalStreamCtl::Fail,
    }
}

struct ProducerWithSink<A>
where
    A: 'static + Send,
{
    producer: Box<Producer<(), A> + Send>,
    sink: Sink<A>,
}

struct StreamController {
    stream: Option<Stream>,
}

impl Actor<InternalStreamCtl> for StreamController {
    fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<InternalStreamCtl>) {
        if let Signal::Started = signal {
            self.stream.take().unwrap().run(ctx);
        }
    }

    fn receive(&mut self, msg: InternalStreamCtl, ctx: &mut ActorContext<InternalStreamCtl>) {
        match msg {
            InternalStreamCtl::Stop => {}

            InternalStreamCtl::Fail => {}
        }
    }
}

trait StreamRunner {
    fn spawn_stream(&mut self, stream: Stream) -> ActorRef<StreamCtl>;
}

trait RunnableStream {
    fn run(self: Box<Self>, context: &mut ActorContext<InternalStreamCtl>);
}

impl<A> RunnableStream for ProducerWithSink<A>
where
    A: 'static + Send,
{
    fn run(self: Box<Self>, context: &mut ActorContext<InternalStreamCtl>) {
        let upstream_ref = self
            .producer
            .spawn(self.sink.logic.spawn(ActorRef::empty(), context), context);

        upstream_ref.tell(DownstreamStageMsg::SetUpstream(ActorRef::empty()));
    }
}

impl<Msg> StreamRunner for ActorContext<Msg>
where
    Msg: 'static + Send,
{
    fn spawn_stream(&mut self, stream: Stream) -> ActorRef<StreamCtl> {
        self.spawn(StreamController {
            stream: Some(stream),
        })
        .convert(stream_ctl_to_internal_stream_ctl)
    }
}

struct Stream {
    runnable_stream: Box<RunnableStream + Send>,
}

impl Stream {
    fn run(self, context: &mut ActorContext<InternalStreamCtl>) {
        self.runnable_stream.run(context)
    }
}

struct Sink<A> {
    logic: Box<LogicContainerFacade<A, ()> + Send>,
}

impl<A> Sink<A>
where
    A: 'static + Send,
{
    fn for_each<F: FnMut(A) -> ()>(for_each_fn: F) -> Self
    where
        F: 'static + Send,
    {
        Self {
            logic: Box::new(LogicContainer {
                logic: ForEach {
                    for_each_fn,
                    phantom: PhantomData,
                },
                phantom: PhantomData,
            }),
        }
    }
}

impl<A> Sink<A>
where
    A: 'static + Send + Clone,
{
    #[must_use]
    fn from_broadcast_hub(hub_ref: &ActorRef<Sink<A>>) -> Self {
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
        context: &mut ActorContext<InternalStreamCtl>,
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
        context: &mut ActorContext<InternalStreamCtl>,
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
            upstream: None,
            downstream: downstream.clone(),
            state: StageState::Waiting(None),
            pulled: false,
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
        context: &mut ActorContext<InternalStreamCtl>,
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
        context: &mut ActorContext<InternalStreamCtl>,
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
                logic: IteratorLogic { iterator },
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
                logic: RepeatLogic { el },
                phantom: PhantomData,
            }),
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

impl<A> Actor<Sink<A>> for BroadcastHub<A>
where
    A: 'static + Send + Clone,
{
    fn receive(&mut self, source: Sink<A>, _: &mut ActorContext<Sink<A>>) {}
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

    fn to(self, sink: Sink<A>) -> Stream {
        Stream {
            runnable_stream: Box::new(ProducerWithSink {
                producer: self.producer,
                sink,
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
                        logic: FilterLogic::new(filter),
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
    logic: Box<LogicContainerFacade<A, B> + Send>,
}

impl<A> Flow<A, A>
where
    A: 'static + Send,
{
    fn new() -> Self {
        Self {
            logic: Box::new(LogicContainer {
                logic: IdentityLogic {
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
        Self::from_logic(MapLogic::new(map_fn))
    }
}

#[test]
fn temp() {
    use crate::actor::*;

    fn double_slowly(n: u64) -> u64 {
        let start = std::time::Instant::now();

        while start.elapsed().as_millis() < 250 {}

        n * 2
    }

    struct TestReaper;

    impl Actor<()> for TestReaper {
        fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}

        fn receive_signal(&mut self, _: Signal, ctx: &mut ActorContext<()>) {
            if false {
                let flow = Flow::map(|n| n);

                let sink = Sink::for_each(|n| println!("got {}", n));

                let source = Source::iterator(1..=std::u64::MAX);

                let stream = source
                    .via(flow)
                    .via(Flow::map(double_slowly))
                    .via(Flow::map(double_slowly))
                    .to(sink);

                let stream_ref = ctx.spawn_stream(stream);

                stream_ref.tell(StreamCtl::Stop);
            }

            if true {
                ctx.spawn_stream(
                    Source::iterator(1..=std::u64::MAX)
                        .via(Flow::from_logic(DelayLogic::new(Duration::from_millis(50))))
                        .to(Sink::for_each(|n| println!("got {}", n))),
                );
            }

            if false {
                let hub = ctx.spawn(MergeHub::new());
                let source = Source::from_merge_hub(&hub);

                hub.tell(Source::iterator(1..=1000));

                let stream = source
                    .via(Flow::map(|n: usize| n * 2))
                    .to(Sink::for_each(|n| println!("got {}", n)));

                ctx.spawn_stream(stream);
            }

            if false {
                let hub = ctx.spawn(BroadcastHub::new());
                let sink = Sink::from_broadcast_hub(&hub);

                ctx.spawn_stream(Source::iterator(1..=1000).filter(|n| n % 2 == 0).to(sink));

                hub.tell(Sink::for_each(|n| println!("A got {}", n)));
                hub.tell(Sink::for_each(|n| println!("B got {}", n)));
            }

            if false {
                let merge = ctx.spawn(MergeHub::new());
                let merge_source = Source::from_merge_hub(&merge);

                let broadcast = ctx.spawn(BroadcastHub::new());
                let broadcast_sink = Sink::from_broadcast_hub(&broadcast);

                ctx.spawn_stream(merge_source.to(broadcast_sink));

                broadcast.tell(Sink::for_each(|n: usize| println!("i got it {}", n)));

                merge.tell(Source::iterator(1..=10));
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper).is_ok());
}
