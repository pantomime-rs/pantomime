use crate::actor::{
    Actor, ActorContext, ActorRef, ActorSpawnContext, FailureReason, Signal, Spawnable, StopReason,
    Watchable,
};
use crate::stream::flow::Flow;
use crate::stream::sink::Sink;
use crate::stream::{Action, Logic, LogicEvent, Stream, StreamComplete, StreamContext, StreamCtl};
use crossbeam::atomic::AtomicCell;
use std::any::Any;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

pub(in crate::stream) trait LogicContainerFacade<A, B>
where
    A: 'static + Send,
    B: 'static + Send,
{
    fn fuse(self: Box<Self>) -> Box<dyn LogicContainerFacade<A, B> + Send>;

    fn spawn(
        self: Box<Self>,
        downstream: Downstream<B>,
        context: &mut ActorSpawnContext,
    ) -> Downstream<A>;
}

pub(in crate::stream) struct IndividualLogic<L> {
    pub(in crate::stream) logic: L,
    pub(in crate::stream) fused: bool
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
    fn fuse(mut self: Box<Self>) -> Box<dyn LogicContainerFacade<A, C> + Send> {
        self.upstream = self.upstream.fuse();
        self.downstream = self.downstream.fuse();
        self
    }

    fn spawn(
        self: Box<Self>,
        downstream: Downstream<C>,
        context: &mut ActorSpawnContext,
    ) -> Downstream<A> {
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
    SetUpstream(Upstream),
    ForwardAny(Box<Any + Send>)
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
    ForwardAny(Box<dyn Any + Send>),
    Forward(DownstreamStageMsg<B>),
    ForwardUp(DownstreamStageMsg<A>)
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
        DownstreamStageMsg::ForwardAny(msg) => StageMsg::ForwardAny(msg)
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
    buffer: VecDeque<A>,
    phantom: PhantomData<(A, B, Msg)>,
    midstream_context: Option<SpecialContext<StageMsg<A, B, Msg>>>,
    downstream: Downstream<B>,
    downstream_context: Option<SpecialContext<DownstreamStageMsg<B>>>,
    state: StageState<A, B, Msg>,
    pulled: bool,
    upstream_stopped: bool,
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
    Fused(ActorRef<UpstreamStageMsg>)
}

enum InnerStageContext<'a, Msg> where Msg: 'static + Send  {
    Spawned(&'a mut ActorContext<Msg>),
    Special(&'a mut SpecialContext<Msg>)
}

enum SpecialContextAction<Msg> {
    CancelDelivery(String),
    Stop,
    ScheduleDelivery(String, Duration, Msg)
}

struct SpecialContext<Msg> where Msg: 'static + Send {
    actions: VecDeque<SpecialContextAction<Msg>>,
    actor_ref: ActorRef<Msg>
}

// HERE'S THE IDEA
//
// assume we're a fused "chain" of stages within an actor
//
// downstream is invoked synchronously from upstream, and
// can give it a context that stores the signals and then
// are processed when the method returns
//
// the challenge becomes what to do when downstream requests
// an actor ref, or schedules delivery of some messages
//
// for this, we need to introduce a StageMsg variant for
// invoking / messaging something downstream
//
// this is a recursive forward - as each further downstream
// stage's message gets wrapped in a Forward. eventually it
// gets to where it needs to go.
//
//

pub(in crate::stream) struct StageContext<'a, Msg> where Msg: 'static + Send {
    inner: InnerStageContext<'a, Msg>
}

impl<'a, Msg> StageContext<'a, Msg> where Msg: 'static + Send {
    pub fn actor_ref(&self) -> ActorRef<Msg> {
        match self.inner {
            InnerStageContext::Spawned(ref context) => {
                context.actor_ref().clone()
            }

            InnerStageContext::Special(ref context) => {
                context.actor_ref.clone()
            }
        }
    }

    pub fn cancel_delivery<S: AsRef<str>>(&mut self, name: S) {
        match self.inner {
            InnerStageContext::Spawned(ref mut context) => {
                context.cancel_delivery(name);
            }

            InnerStageContext::Special(ref mut context) => {
                context.actions.push_back(SpecialContextAction::CancelDelivery(name.as_ref().to_string()));
            }
        }
    }

    pub fn fused(&self) -> bool {
        match self.inner {
            InnerStageContext::Spawned(_) => false,
            InnerStageContext::Special(_) => true,
        }
    }

    pub fn schedule_delivery<S: AsRef<str>>(&mut self, name: S, timeout: Duration, msg: Msg) {
        match self.inner {
            InnerStageContext::Spawned(ref mut context) => {
                context.schedule_delivery(name, timeout, msg);
            }

            InnerStageContext::Special(ref mut context) => {
                context.actions.push_back(SpecialContextAction::ScheduleDelivery(name.as_ref().to_string(), timeout, msg));
            }
        }
    }

    pub fn stop(&mut self) {
        match self.inner {
            InnerStageContext::Spawned(ref mut context) => {
                context.stop();
            }

            InnerStageContext::Special(ref mut context) => {
                context.actions.push_back(SpecialContextAction::Stop);
            }
        }
    }
}

impl<'a, A, B, Msg, L: Logic<A, B, Ctl = Msg>> Stage<A, B, Msg, L>
where
    L: 'static + Send,
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    fn check_upstream_demand(&mut self, ctx: &mut StageContext<StageMsg<A, B, Msg>>) {
        if self.upstream_stopped {
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

                StageState::Running(Upstream::Fused(ref mut upstream)) => {
                    upstream.tell(UpstreamStageMsg::Pull(available));
                    self.upstream_demand += available;
                }

                StageState::Waiting(_) => {}
            }
        }
    }

    fn process_downstream_ctx(&mut self, ctx: &mut StageContext<StageMsg<A, B, Msg>>) {
        println!("process downstream ctx");
        let downstream_ctx = self.downstream_context.as_mut().expect("TODO");

        while let Some(action) = downstream_ctx.actions.pop_front() {
            match action {
                SpecialContextAction::Stop => {
                    ctx.stop();
                }

                SpecialContextAction::ScheduleDelivery(name, duration, msg) => {
                    ctx.schedule_delivery(name, duration, StageMsg::Forward(msg));
                }

                SpecialContextAction::CancelDelivery(name) => {
                    ctx.cancel_delivery(name);
                }
            }
        }
    }

    fn process_midstream_ctx(&mut self, ctx: &mut StageContext<DownstreamStageMsg<A>>) {
        println!("{} process midstream", self.logic.name());
        let mut midstream_context = self.midstream_context.take().expect("TODO");

        while let Some(action) = midstream_context.actions.pop_front() {
            match action {
                SpecialContextAction::Stop => {
                    ctx.stop();
                }

                SpecialContextAction::ScheduleDelivery(name, duration, msg) => {
                    ctx.schedule_delivery(name, duration, DownstreamStageMsg::ForwardAny(Box::new(msg)));
                }

                SpecialContextAction::CancelDelivery(name) => {
                    ctx.cancel_delivery(name);
                }
            }
        }

        self.midstream_context = Some(midstream_context);
    }

    fn receive_signal_started(&mut self, ctx: &mut StageContext<StageMsg<A, B, Msg>>) {
        match self.downstream {
            Downstream::Spawned(ref downstream) => {
                println!("telling upstream");
                downstream.tell(DownstreamStageMsg::SetUpstream(
                    Upstream::Spawned(ctx.actor_ref().convert(upstream_stage_msg_to_stage_msg)),
                ));
            }

            Downstream::Fused(ref mut downstream) => {
                println!("{} fused started", self.logic.name());

                let actor_ref = ctx.actor_ref().convert(upstream_stage_msg_to_stage_msg);

                let mut special_context = SpecialContext {
                    actions: VecDeque::new(),
                    actor_ref: ctx.actor_ref().convert(StageMsg::Forward)
                };

                let mut downstream_ctx = StageContext {
                    inner: InnerStageContext::Special(
                        &mut special_context
                    )
                };

                downstream.receive_signal_started(&mut downstream_ctx);

                println!("{}'s downstream will receive us", self.logic.name());

                downstream.receive_message(
                    DownstreamStageMsg::SetUpstream(Upstream::Fused(actor_ref)),
                    &mut downstream_ctx
                );

                self.downstream_context = Some(special_context);

                self.process_downstream_ctx(ctx);
            }
        }
    }

    fn receive_message(&mut self, msg: StageMsg<A, B, Msg>, ctx: &mut StageContext<StageMsg<A, B, Msg>>) {
        match self.state {
            StageState::Running(ref upstream) => {
                match msg {
                    StageMsg::ForwardUp(downstream_stage_msg) => {
                        println!("{} ForwardUp", self.logic.name());
                        self.receive_message(downstream_stage_msg_to_stage_msg(downstream_stage_msg), ctx);
                    }

                    StageMsg::ForwardAny(msg) => {
                        println!("{} ForwardAny", self.logic.name());
                        match msg.downcast() {
                            Ok(msg) => {
                                println!("it worked!");
                                self.receive_message(*msg, ctx);
                            }

                            Err(_) => {
                                panic!();
                            }
                        }
                    }

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

                                // @TODO this was commented out
                                self.check_upstream_demand(ctx);
                            } else {
                                self.buffer.push_back(el);
                            }
                        }
                    }

                    StageMsg::Action(action) => {
                        println!("{} Action", self.logic.name());
                        self.receive_action(action, ctx);
                    }

                    StageMsg::Forward(msg) => {
                        println!("{} Forward", self.logic.name());
                        match self.downstream {
                            Downstream::Spawned(ref downstream) => {
                                panic!("TODO");
                            }

                            Downstream::Fused(ref mut downstream) => {
                                let downstream_ctx = self.downstream_context.as_mut().unwrap();

                                downstream.receive_message(
                                    msg,
                                    &mut StageContext {
                                        inner: InnerStageContext::Special(downstream_ctx)
                                    }
                                );

                                self.process_downstream_ctx(ctx);
                            }
                        }
                    }

                    StageMsg::Cancel => {
                        // downstream has cancelled us

                        if !self.upstream_stopped {
                            match self.state {
                                StageState::Running(Upstream::Spawned(ref upstream)) => {
                                    upstream.tell(UpstreamStageMsg::Cancel);
                                }

                                StageState::Running(Upstream::Fused(ref mut upstream)) => {
                                    upstream.tell(UpstreamStageMsg::Cancel);
                                }

                                StageState::Waiting(_) => {
                                    // @TODO think about this condition
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
                        println!("{} SetUpstream (shouldnt be possible)", self.logic.name());
                        // this shouldn't be possible
                    }
                }
            }

            StageState::Waiting(ref stash) if stash.is_none() => {
                match msg {
                    StageMsg::SetUpstream(upstream) => {
                        println!("{} SetUpstream (no stash)", self.logic.name());
                        self.state = StageState::Running(upstream);

                        self.receive_logic_event(LogicEvent::Started, ctx);

                        self.check_upstream_demand(ctx);
                    }

                    StageMsg::ForwardUp(downstream_stage_msg) => {
                        println!("{} ForwardUp", self.logic.name());
                        self.receive_message(downstream_stage_msg_to_stage_msg(downstream_stage_msg), ctx);
                    }

                    other => {
                        println!("{} will stash", self.logic.name());
                        // this is rare, but it can happen depending upon
                        // timing, i.e. the stage can receive messages from
                        // upstream or downstream before it has received
                        // the ActorRef for upstream

                        let mut stash = VecDeque::new();

                        stash.push_back(other);

                        self.state = StageState::Waiting(Some(stash));
                        println!("{} now stashed", self.logic.name());
                    }
                }
            }

            StageState::Waiting(ref mut stash) => match msg {
                StageMsg::SetUpstream(upstream) => {
                    println!("{} SetUpstream", self.logic.name());
                    let mut stash = stash
                        .take()
                        .expect("pantomime bug: Option::take failed despite being Some");

                    self.state = StageState::Running(upstream);

                    self.receive_logic_event(LogicEvent::Started, ctx);

                    self.check_upstream_demand(ctx);

                    while let Some(msg) = stash.pop_front() {
                        println!("{} unstashing", self.logic.name());
                        self.receive_message(msg, ctx);
                    }
                }

                StageMsg::ForwardUp(downstream_stage_msg) => {
                    println!("{} ForwardUp", self.logic.name());
                    self.receive_message(downstream_stage_msg_to_stage_msg(downstream_stage_msg), ctx);
                }

                other => {
                    println!("{} stashing", self.logic.name());
                    let mut stash = stash
                        .take()
                        .expect("pantomime bug: Option::take failed despite being Some");

                    stash.push_back(other);
                }
            },
        }
    }

    fn receive_logic_event(
        &mut self,
        event: LogicEvent<A, Msg>,
        ctx: &mut StageContext<StageMsg<A, B, Msg>>,
    ) {
        {
            let mut ctx = StreamContext {
                ctx,
                actions: &mut self.logic_actions
            };

            self.logic
                .receive(event, &mut ctx);
        }

        // @TODO stack overflow possible when we implement fusion -- should bound
        //       this and fall back to telling ourselves after some amount

        while let Some(event) = self.logic_actions.pop_front() {
            self.receive_action(event, ctx);
        }
    }

    fn receive_action(
        &mut self,
        action: Action<B, Msg>,
        ctx: &mut StageContext<StageMsg<A, B, Msg>>,
    ) {
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
                            println!("{}   sending LogicEvent::Stopped", self.logic.name());
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
                        }

                        Downstream::Fused(ref mut downstream) => {
                            let mut downstream_ctx = self.downstream_context.as_mut().unwrap();

                            downstream.receive_message(
                                DownstreamStageMsg::Produce(el),
                                &mut StageContext {
                                    inner: InnerStageContext::Special(downstream_ctx)
                                }
                            );

                            self.process_downstream_ctx(ctx);
                        }
                    }

                    self.downstream_demand -= 1;

                    if self.downstream_demand > 0 {
                        self.receive_logic_event(LogicEvent::Pulled, ctx);
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

                        StageState::Running(Upstream::Fused(ref mut upstream)) => {
                            upstream.tell(UpstreamStageMsg::Cancel);
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
                        println!("{} telling downstream were done", self.logic.name());
                        downstream.tell(DownstreamStageMsg::Complete(reason));

                        ctx.stop();
                    }

                    Downstream::Fused(ref mut downstream) => {
                        let downstream_ctx = self.downstream_context.as_mut().unwrap();

                        downstream.receive_message(
                            DownstreamStageMsg::Complete(reason),
                            &mut StageContext {
                                inner: InnerStageContext::Special(downstream_ctx)
                            }
                        );

                        self.process_downstream_ctx(ctx);

                        // @TODO what about stopping?
                    }
                }
            }
        }
    }
}


impl<A, B, Msg, L: Logic<A, B, Ctl = Msg>> StageActor<DownstreamStageMsg<A>> for Stage<A, B, Msg, L>
where
    L: 'static + Send,
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    fn stage_receive_signal_started(&mut self, ctx: &mut StageContext<DownstreamStageMsg<A>>) {
        println!("{} receive_signal_started", self.logic.name());
        if ctx.fused() {
            self.midstream_context = Some(
                SpecialContext {
                    actions: VecDeque::new(),
                    actor_ref: ctx.actor_ref().convert(|msg| DownstreamStageMsg::ForwardAny(Box::new(msg)))
                }
            );
        }

        let mut midstream_context = self.midstream_context.take().expect("TODO");

        self.receive_signal_started(
            &mut StageContext {
                inner: InnerStageContext::Special(&mut midstream_context)
            }
        );

        self.midstream_context = Some(midstream_context);

        self.process_midstream_ctx(ctx);
    }

    fn stage_receive_message(&mut self, msg: DownstreamStageMsg<A>, ctx: &mut StageContext<DownstreamStageMsg<A>>) {
        println!("{} receive_message", self.logic.name());
        let mut midstream_context = self.midstream_context.take().expect("TODO");

        self.receive_message(
            StageMsg::ForwardUp(msg),
            &mut StageContext {
                inner: InnerStageContext::Special(&mut midstream_context)
            }
        );

        self.midstream_context = Some(midstream_context);

        self.process_midstream_ctx(ctx);
    }
}

impl<A, B, Msg, L: Logic<A, B, Ctl = Msg>> StageActor<StageMsg<A, B, Msg>> for Stage<A, B, Msg, L>
where
    L: 'static + Send,
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    fn stage_receive_signal_started(&mut self, ctx: &mut StageContext<StageMsg<A, B, Msg>>) {
        self.receive_signal_started(ctx);
    }

    fn stage_receive_message(&mut self, msg: StageMsg<A, B, Msg>, ctx: &mut StageContext<StageMsg<A, B, Msg>>) {
        self.receive_message(msg, ctx);
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
            self.receive_signal_started(
                &mut StageContext {
                    inner: InnerStageContext::Spawned(ctx)
                }
            );
        }
    }

    fn receive(&mut self, msg: StageMsg<A, B, Msg>, ctx: &mut ActorContext<StageMsg<A, B, Msg>>) {
        self.receive_message(
            msg,
            &mut StageContext {
                inner: InnerStageContext::Spawned(ctx)
            }
        );
    }
}

impl<A, B, Msg, L> LogicContainerFacade<A, B> for IndividualLogic<L>
where
    L: 'static + Logic<A, B, Ctl = Msg> + Send,
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    fn fuse(mut self: Box<Self>) -> Box<dyn LogicContainerFacade<A, B> + Send> {
        self.fused = true;
        self
    }

    fn spawn(
        self: Box<Self>,
        downstream: Downstream<B>,
        context: &mut ActorSpawnContext,
    ) -> Downstream<A> {
        if self.fused {
            let stage = Stage {
                logic: self.logic,
                logic_actions: VecDeque::with_capacity(2),
                buffer: VecDeque::with_capacity(1),
                phantom: PhantomData,
                midstream_context: None,
                downstream,
                downstream_context: None,
                state: StageState::Waiting(None),
                pulled: false,
                upstream_stopped: false,
                downstream_demand: 0,
                upstream_demand: 0,
            };

            Downstream::Fused(
                StageRef {
                    stage: Box::new(stage)
                }
            )
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
                    midstream_context: None,
                    downstream,
                    downstream_context: None,
                    state: StageState::Waiting(None),
                    pulled: false,
                    upstream_stopped: false,
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

pub(in crate::stream) trait RunnableStream<Out>
where
    Out: Send,
{
    fn run(self: Box<Self>, context: &mut ActorContext<InternalStreamCtl<Out>>);
}

impl<A, Out> RunnableStream<Out> for UnionLogic<(), A, Out>
where
    A: 'static + Send,
    Out: 'static + Send,
{
    fn run(self: Box<Self>, context: &mut ActorContext<InternalStreamCtl<Out>>) {
        let actor_ref_for_sink = context
            .actor_ref()
            .convert(InternalStreamCtl::FromSink);

        let mut context_for_upstream = context.spawn_context();

        let stream = self.upstream.spawn(
            self.downstream
                .spawn(Downstream::Spawned(actor_ref_for_sink), &mut context_for_upstream),
            &mut context_for_upstream,
        );

        match stream {
            Downstream::Spawned(stream) => {
                context.actor_ref()
                       .tell(InternalStreamCtl::SetDownstream(stream));
            }

            Downstream::Fused(stream) => {
                unimplemented!();
            }
        }

    }
}

pub(in crate::stream) trait StageActor<A> where A: 'static + Send {
    fn stage_receive_signal_started(&mut self, ctx: &mut StageContext<A>);
    fn stage_receive_message(&mut self, msg: A, ctx: &mut StageContext<A>);
}

pub(in crate::stream) struct StageRef<A> {
    stage: Box<StageActor<A> + Send>
}

impl<A> StageRef<A> where A: 'static + Send {
    fn receive_signal_started(&mut self, ctx: &mut StageContext<A>) {
        self.stage.stage_receive_signal_started(ctx);
    }

    fn receive_message(&mut self, msg: A, ctx: &mut StageContext<A>) {
        self.stage.stage_receive_message(msg, ctx);
    }
}

pub(in crate::stream) enum Downstream<A> where A: 'static + Send {
    Spawned(ActorRef<DownstreamStageMsg<A>>),
    Fused(StageRef<DownstreamStageMsg<A>>)
}

pub(in crate::stream) struct SourceLike<A, M, L>
where
    L: Logic<(), A, Ctl = M>,
    A: Send,
    M: Send,
{
    pub(in crate::stream) logic: L,
    pub(in crate::stream) fused: bool,
    pub(in crate::stream) phantom: PhantomData<(A, M)>,
}

impl<A, M, L> LogicContainerFacade<(), A> for SourceLike<A, M, L>
where
    L: 'static + Logic<(), A, Ctl = M> + Send,
    A: 'static + Send,
    M: 'static + Send,
{
    fn fuse(mut self: Box<Self>) -> Box<dyn LogicContainerFacade<(), A> + Send> {
        self.fused = true;
        self
    }

    fn spawn(
        self: Box<Self>,
        downstream: Downstream<A>,
        context: &mut ActorSpawnContext,
    ) -> Downstream<()> {
        let stage = Stage {
            logic: self.logic,
            logic_actions: VecDeque::with_capacity(2),
            buffer: VecDeque::with_capacity(0),
            phantom: PhantomData,
            midstream_context: None,
            downstream,
            downstream_context: None,
            state: StageState::Waiting(None),
            pulled: false,
            upstream_stopped: false,
            downstream_demand: 0,
            upstream_demand: 0,
        };

        if self.fused {
            Downstream::Fused(
                StageRef {
                    stage: Box::new(stage)
                }
            )
        } else {
            let midstream = context.spawn(stage);

            Downstream::Spawned(midstream.convert(downstream_stage_msg_to_stage_msg))
        }
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

                    Upstream::Fused(mut upstream) => {
                        println!("pulling 1 from the fused sink");
                        upstream.tell(UpstreamStageMsg::Pull(1));
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

                // @TODO there's a race, ive seen this fail

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

                downstream.tell(DownstreamStageMsg::SetUpstream(Upstream::Spawned(actor_ref_for_source)));

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
