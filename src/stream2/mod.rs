use crate::actor::{Actor, ActorRef, ActorContext, SystemActorRef};
use std::marker::PhantomData;



trait Logic<A, B, Msg> where A: Send, B: Send, Msg: Send {
    fn pulled(&mut self);

    fn pushed(&mut self, el: A);

    fn started(&mut self, stream_ctx: StreamContext<A, B, ()>);

    fn stopped(&mut self);

}


trait LogicContainerFacade<A, B> {
    fn spawn(self: Box<Self>, downstream: ActorRef<StageMsg>, context: &mut ActorContext<()>) -> ActorRef<StageMsg>;
}

struct LogicContainer<A, B, Msg, L> where L: Logic<A, B, Msg>, A: Send, B: Send, Msg: Send {
    logic: L,
    phantom: PhantomData<(A, B, Msg)>
}

enum StageMsg {
    SetUpstream(ActorRef<StageMsg>)
}

struct Stage<A, B, Msg, L> where L: Logic<A, B, Msg>, A: Send, B: Send, Msg: Send {
    logic: L,
    phantom: PhantomData<(A, B, Msg)>,
    upstream: ActorRef<StageMsg>,
    downstream: ActorRef<StageMsg>
}

impl<A, B, Msg, L> Actor<StageMsg> for Stage<A, B, Msg, L> where L: Logic<A, B, Msg> + Send, A: Send, B: Send, Msg: Send {
    fn receive(&mut self, msg: StageMsg, ctx: &mut ActorContext<StageMsg>) {
        match msg {
            StageMsg::SetUpstream(upstream) => {
                self.upstream = upstream;
            }
        }
    }
}

impl<A, B, Msg, L> LogicContainerFacade<A, B> for LogicContainer<A, B, Msg, L> where L: 'static + Logic<A, B, Msg> + Send, A: 'static + Send, B: 'static + Send, Msg: 'static + Send {
    fn spawn(self: Box<Self>, downstream: ActorRef<StageMsg>, context: &mut ActorContext<()>) -> ActorRef<StageMsg> {
        let upstream = context.spawn(Stage {
            logic: self.logic,
            phantom: PhantomData,
            upstream: ActorRef::empty(),
            downstream: downstream.clone()
        });

        downstream.tell(StageMsg::SetUpstream(upstream.clone()));

        upstream
    }
}
























//
// SINKS
//

struct Ignore<A> where A: Send {
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

struct ForEach<A, F> where F: FnMut(A) -> (), A: Send {
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


struct IdentityLogic<A> where A: Send {
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



































enum StreamStageMsg<A, B, Msg> {
    One(B),
    Two(Msg),
    Three(A),
    Stop,
    Pull,
    Push(B)
}

#[derive(Clone)]
struct StreamContext<A, B, Msg> where A: Send, B: Send, Msg: Send {
    stage_ref: ActorRef<StreamStageMsg<A, B, Msg>>
}

impl<A, B, Msg> StreamContext<A, B, Msg> where A: 'static + Send, B: 'static + Send, Msg: 'static + Send {
    fn empty() -> Self {
        // @TODO should stage_ref be an option? depends on if we can
        //       fix ActorRef::empty to not allocate

        Self {
            stage_ref: ActorRef::empty()
        }
    }

    fn stop(&mut self) {}

    fn pull(&mut self) {
        unimplemented!();
    }

    fn push(&mut self, el: B) {
        unimplemented!();
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

struct ProducerWithSink<A> {
    producer: Box<Producer<A>>,
    sink: Sink<A>
}

trait RunnableStream {
    fn run(self: Box<Self>, context: &mut ActorContext<()>) -> ActorRef<StreamCtl>;
}

impl<A> RunnableStream for ProducerWithSink<A> {
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

trait Producer<A> {
    fn spawn(self: Box<Self>, downstream: ActorRef<StageMsg>, context: &mut ActorContext<()>) -> ActorRef<StageMsg>;
}

struct SourceLike<A, M, L> where L: Logic<(), A, M>, A: Send, M: Send {
    logic: L,
    phantom: PhantomData<(A, M)>
}

impl<A, M, L> Producer<A> for SourceLike<A, M, L> where L: 'static + Logic<(), A, M> + Send, A: 'static + Send, M: 'static + Send {
    fn spawn(self: Box<Self>, downstream: ActorRef<StageMsg>, context: &mut ActorContext<()>) -> ActorRef<StageMsg> {
        let upstream = context.spawn(Stage {
            logic: self.logic,
            phantom: PhantomData,
            upstream: ActorRef::empty(),
            downstream: downstream.clone()
        });

        downstream.tell(StageMsg::SetUpstream(upstream.clone()));

        upstream

    }
}

struct ProducerWithFlow<A, B> {
    producer: Box<Producer<A>>,
    flow: Flow<A, B>
}

impl<A, B> Producer<B> for ProducerWithFlow<A, B> where A: 'static + Send, B: 'static + Send {
    fn spawn(self: Box<Self>, downstream: ActorRef<StageMsg>, context: &mut ActorContext<()>) -> ActorRef<StageMsg> {
        let flow_ref = self.flow.logic.spawn(
            downstream,
            context
        );

        let upstream = self.producer.spawn(flow_ref.clone(), context);

        flow_ref.tell(StageMsg::SetUpstream(upstream.clone()));

        upstream
    }
}

struct Source<A> {
    producer: Box<Producer<A>>
}

impl<A> Producer<A> for Source<A> {
    fn spawn(self: Box<Self>, downstream: ActorRef<StageMsg>, context: &mut ActorContext<()>) -> ActorRef<StageMsg> {
        let upstream = self.producer.spawn(downstream.clone(), context);

        downstream.tell(StageMsg::SetUpstream(upstream.clone()));

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
