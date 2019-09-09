use crate::actor::{Actor, ActorRef, ActorContext, SystemActorRef};
use std::collections::VecDeque;
use std::marker::PhantomData;


const BUFFER_SIZE: usize = 16;


trait Logic<A, B, Msg> where A: Send, B: Send, Msg: Send {
    fn pulled(&mut self);

    fn pushed(&mut self, el: A);

    fn started(&mut self, stream_ctx: StreamContext<A, B, Msg>);

    fn stopped(&mut self);

}

trait LogicContainerFacade<A, B> where A: 'static + Send, B: 'static + Send {
    fn spawn(self: Box<Self>, downstream: ActorRef<DownstreamStageMsg<B>>, context: &mut ActorContext<()>) -> ActorRef<DownstreamStageMsg<A>>;
}

struct LogicContainer<A, B, Msg, L> where L: Logic<A, B, Msg>, A: Send, B: Send, Msg: Send {
    logic: L,
    phantom: PhantomData<(A, B, Msg)>
}

enum UpstreamStageMsg {
    Stop,
    Pull(usize)
}

enum DownstreamStageMsg<A> where A: 'static + Send {
    Produce(A),
    SetUpstream(ActorRef<UpstreamStageMsg>),
}

enum StageMsg<A, B, Msg> where A: 'static + Send, B: 'static + Send, Msg: 'static + Send {
    SetUpstream(ActorRef<UpstreamStageMsg>),
    Start,
    Stop,
    Grab,
    Pull(usize),
    Consume(A),
    Produce(B),
    Forward(Msg),
}

fn downstream_stage_msg_to_stage_msg<A, B, Msg>(msg: DownstreamStageMsg<A>) -> StageMsg<A, B, Msg> where A: 'static + Send, B: 'static + Send, Msg: 'static + Send {
    match msg {
        DownstreamStageMsg::Produce(el) => StageMsg::Consume(el),
        DownstreamStageMsg::SetUpstream(upstream) => StageMsg::SetUpstream(upstream)
    }
}

fn upstream_stage_msg_to_stage_msg<A, B, Msg>(msg: UpstreamStageMsg) -> StageMsg<A, B, Msg> where A: 'static + Send, B: 'static + Send, Msg: 'static + Send {
    match msg {
        UpstreamStageMsg::Pull(demand) => StageMsg::Pull(demand),
        UpstreamStageMsg::Stop => StageMsg::Stop
    }
}

struct Stage<A, B, Msg, L> where L: Logic<A, B, Msg>, A: 'static + Send, B: 'static + Send, Msg: 'static + Send {
    logic: L,
    buffer: VecDeque<A>, // @TODO use circular buffer
    phantom: PhantomData<(A, B, Msg)>,
    upstream: Option<ActorRef<UpstreamStageMsg>>,
    downstream: ActorRef<DownstreamStageMsg<B>>,
    state: StageState<A, B, Msg>,
    pulled: bool,
    demand: usize
}

enum StageState<A, B, Msg> where A: 'static + Send, B: 'static + Send, Msg: 'static + Send {
    Waiting(Option<VecDeque<StageMsg<A, B, Msg>>>),
    Running(ActorRef<UpstreamStageMsg>)
}

impl<A, B, Msg, L> Stage<A, B, Msg, L> where L: Logic<A, B, Msg> + Send, A: 'static + Send, B: 'static + Send, Msg: 'static + Send {
    fn check_buffer(&mut self) {
        let available = BUFFER_SIZE - self.buffer.len() - self.demand;

        if available >= BUFFER_SIZE / 2 {
            if let StageState::Running(ref upstream) = self.state {
                upstream.tell(UpstreamStageMsg::Pull(available));
            }
        }
    }
}

impl<A, B, Msg, L> Actor<StageMsg<A, B, Msg>> for Stage<A, B, Msg, L> where L: Logic<A, B, Msg> + Send, A: 'static + Send, B: 'static + Send, Msg: 'static + Send {
    fn receive(&mut self, msg: StageMsg<A, B, Msg>, ctx: &mut ActorContext<StageMsg<A, B, Msg>>) {
        match self.state {
            StageState::Running(ref upstream) => {
                match msg {
                    StageMsg::Grab => {
                        // our logic has requested that it receive an element

                        match self.buffer.pop_front() {
                            Some(el) => {
                                self.logic.pushed(el);
                            }

                            None => {
                                self.pulled = true;
                            }
                        }

                    }

                    StageMsg::Pull(demand) => {
                        // our downstream has requested more elements

                        self.demand += demand;
                    }

                    StageMsg::Consume(el) => {
                        // our upstream has produced this element

                        if self.pulled {
                            self.pulled = false;

                            self.logic.pushed(el);
                        }
                    }

                    StageMsg::Produce(el) => {
                        // our logic has produced this element

                    }

                    StageMsg::Forward(msg) => {
                        // our logic has produced this custom message

                    }

                    StageMsg::Stop => {
                        // our logic has requested that we stop

                    }

                    StageMsg::SetUpstream(_) => {
                        // this shouldn't be possible

                    }

                    StageMsg::Start => {
                        let stream_ctx = StreamContext::new(
                            ctx.actor_ref().clone()
                        );

                        self.logic.started(stream_ctx);
                    }
                }
            }

            StageState::Waiting(ref mut s) if s.is_some() => {
                match msg {
                    StageMsg::SetUpstream(upstream) => {
                        let mut stash = s.take().unwrap();

                        self.state = StageState::Running(upstream);

                        while let Some(msg) = stash.pop_front() {
                            self.receive(msg, ctx);
                        }
                    }

                    other => {
                        if let Some(ref mut s) = s {
                            s.push_back(other);
                        }
                    }
                }
            }

            StageState::Waiting(_) => {
                match msg {
                    StageMsg::SetUpstream(upstream) => {
                        self.state = StageState::Running(upstream);
                    }

                    other => {
                        let mut stash = VecDeque::new();
                        stash.push_back(other);
                        self.state = StageState::Waiting(Some(stash));
                    }
                }
            }
        }

        self.check_buffer();
    }
}

impl<A, B, Msg, L> LogicContainerFacade<A, B> for LogicContainer<A, B, Msg, L> where L: 'static + Logic<A, B, Msg> + Send, A: 'static + Send, B: 'static + Send, Msg: 'static + Send {
   fn spawn(self: Box<Self>, downstream: ActorRef<DownstreamStageMsg<B>>, context: &mut ActorContext<()>) -> ActorRef<DownstreamStageMsg<A>> {
        let upstream = context.spawn(Stage {
            logic: self.logic,
            buffer: VecDeque::new(),
            phantom: PhantomData,
            upstream: None,
            downstream: downstream.clone(),
            state: StageState::Waiting(None),
            pulled: false,
            demand: 0
        });

        downstream.tell(
            DownstreamStageMsg::SetUpstream(
                upstream.convert(upstream_stage_msg_to_stage_msg)
            )
        );

        upstream.convert(downstream_stage_msg_to_stage_msg)
    }
}























//
// SINKS
//

struct Ignore<A> where A: 'static + Send {
    phantom: PhantomData<A>,
    stream_ctx: StreamContext<A, (), ()>
}

impl<A> Logic<A, (), ()> for Ignore<A> where A: 'static + Send {
    fn pulled(&mut self) {}

    fn pushed(&mut self, el: A) {
        self.stream_ctx.pull();
    }

    fn started(&mut self, mut stream_ctx: StreamContext<A, (), ()>) {
        stream_ctx.pull();

        self.stream_ctx = stream_ctx;
    }

    fn stopped(&mut self) {}
}

struct ForEach<A, F> where F: FnMut(A) -> (), A: 'static + Send {
    for_each_fn: F,
    stream_ctx: StreamContext<A, (), ()>,
    phantom: PhantomData<A>
}

impl<A, F> Logic<A, (), ()> for ForEach<A, F> where F: FnMut(A) -> (), A: 'static + Send {
    fn pulled(&mut self) {}

    fn pushed(&mut self, el: A) {
        (self.for_each_fn)(el);

        self.stream_ctx.pull();
    }

    fn started(&mut self, mut stream_ctx: StreamContext<A, (), ()>) {
        stream_ctx.pull();

        self.stream_ctx = stream_ctx;
    }

    fn stopped(&mut self) {}
}























//
// FLOWS
//


struct IdentityLogic<A> where A: 'static + Send {
    stream_ctx: StreamContext<A, A, ()>,
    phantom: PhantomData<A>
}

impl<A> Logic<A, A, ()> for IdentityLogic<A> where A: 'static + Send {
    fn pulled(&mut self) {
        self.stream_ctx.pull();
    }

    fn pushed(&mut self, el: A) {
        self.stream_ctx.push(el);
    }

    fn started(&mut self, stream_ctx: StreamContext<A, A, ()>) {
        self.stream_ctx = stream_ctx;
    }

    fn stopped(&mut self) {}
}

struct FilterLogic<A> where A: 'static + Send {
    filter: Box<FnMut(&A) -> bool>,
    stream_ctx: StreamContext<A, A, ()>
}

impl<A> Logic<A, A, ()> for FilterLogic<A> where A: 'static + Send {
    fn pulled(&mut self) {
        self.stream_ctx.pull();
    }

    fn pushed(&mut self, el: A) {
        if (self.filter)(&el) {
            self.stream_ctx.push(el);
        } else {
            self.stream_ctx.pull();
        }
    }

    fn started(&mut self, stream_ctx: StreamContext<A, A, ()>) {
        self.stream_ctx = stream_ctx;
    }

    fn stopped(&mut self) {}
}

struct MapLogic<A, B, F> where F: FnMut(A) -> B, A: 'static + Send, B: 'static + Send {
    map_fn: F,
    stream_ctx: StreamContext<A, B, ()>,
    phantom: PhantomData<(A, B)>
}

impl<A, B, F> Logic<A, B, ()> for MapLogic<A, B, F> where F: FnMut(A) -> B, A: 'static + Send, B: 'static + Send {
    fn pulled(&mut self) {
        self.stream_ctx.pull();
    }

    fn pushed(&mut self, el: A) {
        self.stream_ctx.push((self.map_fn)(el));
    }

    fn started(&mut self, stream_ctx: StreamContext<A, B, ()>) {
        self.stream_ctx = stream_ctx;
    }

    fn stopped(&mut self) {}
}

struct TakeWhile<A> where A: 'static + Send {
    while_fn: Box<FnMut(&A) -> bool>,
    stream_ctx: StreamContext<A, A, ()>
}

impl<A> Logic<A, A, ()> for TakeWhile<A> where A: 'static + Send {
    fn pulled(&mut self) {
        self.stream_ctx.pull();
    }

    fn pushed(&mut self, el: A) {
        if (self.while_fn)(&el) {
            self.stream_ctx.push(el);
        } else {
            self.stream_ctx.stop();
        }
    }

    fn started(&mut self, stream_ctx: StreamContext<A, A, ()>) {
        self.stream_ctx = stream_ctx;
    }

    fn stopped(&mut self) {}
}









//
// SOURCES
//

struct RepeatLogic<A> where A: 'static + Clone + Send {
    el: A,
    stream_ctx: StreamContext<(), A, ()>
}

impl<A> Logic<(), A, ()> for RepeatLogic<A> where A: 'static + Clone + Send {
    fn pulled(&mut self) {
        self.stream_ctx.push(self.el.clone());
    }

    fn pushed(&mut self, el: ()) {}

    fn started(&mut self, stream_ctx: StreamContext<(), A, ()>) {
        self.stream_ctx = stream_ctx;
    }

    fn stopped(&mut self) {}
}



































#[derive(Clone)]
struct StreamContext<A, B, Msg> where A: 'static + Send, B: 'static + Send, Msg: 'static + Send {
    stage_ref: ActorRef<StageMsg<A, B, Msg>>,
    phantom: PhantomData<(A, B, Msg)> // @TODO needed?
}

impl<A, B, Msg> StreamContext<A, B, Msg> where A: 'static + Send, B: 'static + Send, Msg: 'static + Send {
    fn empty() -> Self {
        // @TODO should stage_ref be an option? depends on if we can
        //       fix ActorRef::empty to not allocate

        Self {
            stage_ref: ActorRef::empty(),
            phantom: PhantomData
        }
    }

    fn new(stage_ref: ActorRef<StageMsg<A, B, Msg>>) -> Self {
        Self {
            stage_ref,
            phantom: PhantomData
        }
    }

    fn stop(&mut self) {
        self.stage_ref.tell(StageMsg::Stop);
    }

    fn pull(&mut self) {
        self.stage_ref.tell(StageMsg::Grab);
    }

    fn push(&mut self, el: B) {
        self.stage_ref.tell(StageMsg::Produce(el));
    }
}

enum StreamCtl {
    Stop,
    Fail
}

enum InternalStreamCtl {
    Stop,
    Fail
}

struct ProducerWithSink<A> where A: 'static + Send {
    producer: Box<Producer<(), A>>,
    sink: Sink<A>
}

trait RunnableStream {
    fn run(self: Box<Self>, context: &mut ActorContext<()>) -> ActorRef<StreamCtl>;
}

impl<A> RunnableStream for ProducerWithSink<A> where A: 'static + Send {
    fn run(self: Box<Self>, context: &mut ActorContext<()>) -> ActorRef<StreamCtl> {
        let sink_ref = self.sink.logic.spawn(ActorRef::empty(), context);

        self.producer.spawn(sink_ref, context);

        unimplemented!()
    }
}

struct StreamRunner {}

impl Actor<InternalStreamCtl> for StreamRunner {
    fn receive(&mut self, msg: InternalStreamCtl, ctx: &mut ActorContext<InternalStreamCtl>) {

    }
}

struct Stream {
    runnable_stream: Box<RunnableStream>
}

impl Stream {
    fn run(self, context: &mut ActorContext<()>) -> ActorRef<StreamCtl> {
        self.runnable_stream.run(context)
    }
}

struct Sink<A> {
    logic: Box<LogicContainerFacade<A, ()>>,
}

impl<A> Sink<A> where A: 'static + Send {
    fn for_each<F: FnMut(A) -> ()>(for_each_fn: F) -> Self where F: 'static + Send {
        Self {
            logic: Box::new(
                LogicContainer {
                    logic: ForEach {
                        for_each_fn,
                        stream_ctx: StreamContext::empty(),
                        phantom: PhantomData
                    },
                    phantom: PhantomData
                }
            )
        }
    }
}

trait Producer<In, Out> where In: 'static + Send, Out: 'static + Send {
    fn spawn(self: Box<Self>, downstream: ActorRef<DownstreamStageMsg<Out>>, context: &mut ActorContext<()>) -> ActorRef<DownstreamStageMsg<In>>;
}

struct SourceLike<A, M, L> where L: Logic<(), A, M>, A: Send, M: Send {
    logic: L,
    phantom: PhantomData<(A, M)>
}

impl<A, M, L> Producer<(), A> for SourceLike<A, M, L> where L: 'static + Logic<(), A, M> + Send, A: 'static + Send, M: 'static + Send {
    fn spawn(self: Box<Self>, downstream: ActorRef<DownstreamStageMsg<A>>, context: &mut ActorContext<()>) -> ActorRef<DownstreamStageMsg<()>> {
        let upstream = context.spawn(Stage {
            logic: self.logic,
            buffer: VecDeque::new(),
            phantom: PhantomData,
            upstream: None,
            downstream: downstream.clone(),
            state: StageState::Waiting(None),
            pulled: false,
            demand: 0
        });

        downstream.tell(DownstreamStageMsg::SetUpstream(upstream.convert(upstream_stage_msg_to_stage_msg)));

        upstream.convert(downstream_stage_msg_to_stage_msg)
    }
}

struct ProducerWithFlow<A, B, C> {
    producer: Box<Producer<A, B>>,
    flow: Flow<B, C>
}

impl<A, B, C> Producer<A, C> for ProducerWithFlow<A, B, C> where A: 'static + Send, B: 'static + Send, C: 'static + Send {
    fn spawn(self: Box<Self>, downstream: ActorRef<DownstreamStageMsg<C>>, context: &mut ActorContext<()>) -> ActorRef<DownstreamStageMsg<A>> {
        let flow_ref = self.flow.logic.spawn(
            downstream,
            context
        );

        let upstream = self.producer.spawn(flow_ref.clone(), context);

        //flow_ref.tell(StageMsg::SetUpstream(upstream.convert(upstream_stage_msg_to_stage_msg)));

        upstream
    }
}

struct Source<A> {
    producer: Box<Producer<(), A>>
}

impl<A> Producer<(), A> for Source<A> where A: 'static + Send {
    fn spawn(self: Box<Self>, downstream: ActorRef<DownstreamStageMsg<A>>, context: &mut ActorContext<()>) -> ActorRef<DownstreamStageMsg<()>> {
        let upstream = self.producer.spawn(downstream.clone(), context);

        //downstream.tell(StageMsg::SetUpstream(upstream.convert(upstream_stage_msg_to_stage_msg)));

        upstream
    }

}

impl Source<()> {
    fn repeat<A>(el: A) -> Source<A> where A: 'static + Clone + Send {
        Source {
            producer: Box::new(
                SourceLike {
                    logic: RepeatLogic {
                        el,
                        stream_ctx: StreamContext::empty(),
                    },
                    phantom: PhantomData
                }
            )
        }
    }
}

impl<A> Source<A> where A: 'static + Send {
    fn merge(self, other: Source<A>) -> Source<A> {
        unimplemented!();
    }

    fn to(self, sink: Sink<A>) -> Stream {
        Stream {
            runnable_stream: Box::new(
                ProducerWithSink {
                    producer: self.producer,
                    sink
                }
            )
        }
    }

    fn via<B>(self, flow: Flow<A, B>) -> Source<B> where B: 'static + Send {
        Source {
            producer: Box::new(
                ProducerWithFlow {
                    producer: self.producer,
                    flow
                }
            )
        }
    }
}

struct Flow<A, B> {
    logic: Box<LogicContainerFacade<A, B>>
}

impl<A> Flow<A, A> where A: 'static + Send {
    fn new() -> Self {
        Self {
            logic: Box::new(
                LogicContainer {
                    logic: IdentityLogic {
                        stream_ctx: StreamContext::empty(),
                        phantom: PhantomData
                    },
                    phantom: PhantomData
                }
            )
        }
    }
}

impl<A, B> Flow<A, B> where A: 'static + Send, B: 'static + Send {
    fn map<F: FnMut(A) -> B>(map_fn: F) -> Self where F: 'static + Send {
        Self {
            logic: Box::new(
                LogicContainer {
                    logic: MapLogic {
                        map_fn,
                        stream_ctx: StreamContext::empty(),
                        phantom: PhantomData
                    },
                    phantom: PhantomData
                }

            )
        }
    }
}

#[test]
fn temp() {
    let context: ActorContext<()> = unimplemented!();

    let flow = Flow::map(|n| n * 2);

    let sink = Sink::for_each(|n| println!("got {}", n));

    let stream_ref = Source::repeat(1)
        .via(flow)
        .to(sink)
        .run(&mut context);

    stream_ref.tell(StreamCtl::Stop);

}
