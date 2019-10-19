use crate::actor::{
    Actor, ActorContext, ActorRef, ActorSpawnContext, FailureReason, Signal, Spawnable, StopReason,
    Watchable,
};
use crate::stream::flow::{Flow, Fused};
use crate::stream::sink::Sink;
use crate::stream::{Action, Logic, LogicEvent, Stream, StreamComplete, StreamContext, StreamCtl};
use crate::util::CuteRingBuffer;
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

pub(in crate::stream) struct Union2Logic<A, B, C, UpCtl, DownCtl, Up: Logic<A, B, Ctl = UpCtl>, Down: Logic<B, C, Ctl = DownCtl>>
where
    A: Send,
    B: Send,
    C: Send,
    UpCtl: Send,
    DownCtl: Send,
    Up: Send,
    Down: Send {
    pub(in crate::stream) upstream: Up,
    pub(in crate::stream) downstream: Down,
    pub(in crate::stream) phantom: PhantomData<(A, B, C, UpCtl, DownCtl)>
}

pub(in crate::stream) struct UnionLogic<A, B, C> {
    pub(in crate::stream) upstream: Box<dyn LogicContainerFacade<A, B> + Send>,
    pub(in crate::stream) downstream: Box<dyn LogicContainerFacade<B, C> + Send>,
}

impl<A, B, C, UpCtl, DownCtl, Up: Logic<A, B, Ctl = UpCtl>, Down: Logic<B, C, Ctl = DownCtl>> LogicContainerFacade<A, C> for Union2Logic<A, B, C, UpCtl, DownCtl, Up, Down>
where
    A: 'static + Send,
    B: 'static + Send,
    C: 'static + Send,
    UpCtl: 'static + Send,
    DownCtl: 'static + Send,
    Up: 'static + Send,
    Down: 'static + Send {

    fn fuse(self: Box<Self>) -> Box<dyn LogicContainerFacade<A, C> + Send> {
        panic!()
    }

    fn spawn(
        self: Box<Self>,
        downstream: Downstream<C>,
        context: &mut ActorSpawnContext,
    ) -> Downstream<A> {
        let down = Box::new(IndividualLogic {
            logic: self.downstream,
            fused: false
        });

        let up = Box::new(IndividualLogic {
            logic: self.upstream,
            fused: false
        });

        let downstream = down.spawn(downstream, context);

        up.spawn(downstream, context)
    }
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
    SpecialThing(SpecialContextAction<DownstreamStageMsg<B>>),
    ProcessLogicActions
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

pub(in crate::stream) struct FusedStage<A, B, Msg, L: Logic<A, B, Ctl = Msg>>
    where
        A: 'static + Send,
        B: 'static + Send,
        Msg: 'static + Send,
{
    logic: L,
    phantom: PhantomData<(A, B, Msg)>
}

pub(in crate::stream) struct Stage<A, B, Msg, L: Logic<A, B, Ctl = Msg>>
where
    A: 'static + Send,
    B: 'static + Send,
    Msg: 'static + Send,
{
    logic: Box<L>,
    buffer: Box<VecDeque<A>>,
    midstream_context: Option<SpecialContext<StageMsg<A, B, Msg>>>,
    downstream: Downstream<B>,
    downstream_actor_ref: Option<ActorRef<DownstreamStageMsg<B>>>,
    downstream_context: Option<SpecialContext<DownstreamStageMsg<B>>>,
    state: StageState,
    pulled: bool,
    upstream_stopped: bool,
    downstream_demand: u64,
    upstream_demand: u64,
    stash: Box<VecDeque<StageMsg<A, B, Msg>>>
}

enum StageState {
    Waiting,
    Running(Upstream),
}

pub(in crate::stream) enum Upstream {
    Spawned(ActorRef<UpstreamStageMsg>),
    Fused(ActorRef<UpstreamStageMsg>)
}

pub(in crate::stream) enum InnerStageContext<'a, Msg> where Msg: 'static + Send  {
    Spawned(&'a mut ActorContext<Msg>),
    Special(&'a mut SpecialContext<Msg>),
    Testing(&'a mut (FnMut(SpecialContextAction<Msg>) + Send), &'a ActorRef<Msg>)
}

pub(in crate::stream) enum SpecialContextAction<Msg> {
    CancelDelivery(String),
    Stop,
    ScheduleDelivery(String, Duration, Msg),
    TellUpstream(UpstreamStageMsg),
}

pub(in crate::stream) struct SpecialContext<Msg> where Msg: 'static + Send {
    actions: VecDeque<SpecialContextAction<Msg>>,
    actor_ref: ActorRef<Msg>
}

pub(in crate::stream) struct StageContext<'a, Msg> where Msg: 'static + Send {
    pub(in crate::stream) inner: InnerStageContext<'a, Msg>
}

impl<'a, Msg> StageContext<'a, Msg> where Msg: 'static + Send {
    pub(in crate::stream) fn schedule_delivery<S: AsRef<str>>(&mut self, name: S, timeout: Duration, msg: Msg) {
        match self.inner {
            InnerStageContext::Testing(ref mut f, _) => {
                f(SpecialContextAction::ScheduleDelivery(name.as_ref().to_string(), timeout, msg));
            }

            InnerStageContext::Spawned(ref mut context) => {
                context.schedule_delivery(name, timeout, msg);
            }

            InnerStageContext::Special(ref mut context) => {
                context.actions.push_back(SpecialContextAction::ScheduleDelivery(name.as_ref().to_string(), timeout, msg));
            }
        }
    }

    fn actor_ref(&self) -> ActorRef<Msg> {
        match self.inner {
            InnerStageContext::Testing(_, actor_ref) => {
                actor_ref.clone()
            }

            InnerStageContext::Spawned(ref context) => {
                context.actor_ref().clone()
            }

            InnerStageContext::Special(ref context) => {
                context.actor_ref.clone()
            }
        }
    }

    fn cancel_delivery<S: AsRef<str>>(&mut self, name: S) {
        match self.inner {
            InnerStageContext::Testing(ref mut f, _) => {
                f(SpecialContextAction::CancelDelivery(name.as_ref().to_string()));
            }

            InnerStageContext::Spawned(ref mut context) => {
                context.cancel_delivery(name);
            }

            InnerStageContext::Special(ref mut context) => {
                context.actions.push_back(SpecialContextAction::CancelDelivery(name.as_ref().to_string()));
            }
        }
    }

    fn fused(&self) -> bool {
        match self.inner {
            InnerStageContext::Testing(_, _) => true,
            InnerStageContext::Spawned(_) => false,
            InnerStageContext::Special(_) => true,
        }
    }

    fn stop(&mut self) {
        match self.inner {
            InnerStageContext::Testing(ref mut f, _) => {
                f(SpecialContextAction::Stop);
            }

            InnerStageContext::Spawned(ref mut context) => {
                context.stop();
            }

            InnerStageContext::Special(ref mut context) => {
                context.actions.push_back(SpecialContextAction::Stop);
            }
        }
    }

    fn tell(&mut self, msg: Msg) {
        match self.inner {
            InnerStageContext::Testing(_, actor_ref) => {
                actor_ref.tell(msg)
            }
            InnerStageContext::Spawned(ref context) => {
                context.actor_ref().tell(msg);
            }

            InnerStageContext::Special(ref context) => {
                //context.actions.push_back(SpecialContextAction::TellUpstream(msg));
                context.actor_ref.tell(msg);
            }
        }
    }

    fn tell_upstream(&mut self, state: &StageState, msg: UpstreamStageMsg) {
        match self.inner {
            InnerStageContext::Testing(ref mut f, _) => {
                f(SpecialContextAction::TellUpstream(msg));
            }

            InnerStageContext::Spawned(_) => {
                match state {
                    StageState::Running(Upstream::Spawned(ref upstream)) => {
                        upstream.tell(msg);
                    }

                    StageState::Running(Upstream::Fused(ref upstream)) => {
                        upstream.tell(msg);
                    }

                    StageState::Waiting => {
                        panic!("TODO");
                    }
                }
            }

            InnerStageContext::Special(ref mut context) => {
                context.actions.push_back(SpecialContextAction::TellUpstream(msg));
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

        match ctx.fused() {
            true => {
                if self.pulled && self.upstream_demand == 0 {
                    ctx.tell_upstream(&mut self.state, UpstreamStageMsg::Pull(1));
                    self.upstream_demand += 1;
                }
            }

            false => {
                /*println!(
                    "{} c={}, capacity={}, available={}, len={}, updemand={} pulled={}",
                    self.logic.name(),
                    self.buffer.capacity(),
                    capacity,
                    available,
                    self.buffer.len(),
                    self.upstream_demand,
                    self.pulled
                );*/
                let capacity = self.buffer.capacity() as u64;
                let available = capacity - self.upstream_demand;

                if available >= capacity / 2 {
                    ctx.tell_upstream(&mut self.state, UpstreamStageMsg::Pull(1));
                    self.upstream_demand += 1;
                }
            }
        }
    }

    fn process_midstream_ctx(&mut self, ctx: &mut StageContext<DownstreamStageMsg<A>>) {
        loop {
            let next = self.midstream_context.as_mut().expect("TODO").actions.pop_front();

            match next {
                Some(SpecialContextAction::TellUpstream(msg)) => {
                    ctx.tell_upstream(&mut self.state, msg);
                }

                Some(SpecialContextAction::Stop) => {
                    ctx.stop();
                }

                Some(SpecialContextAction::ScheduleDelivery(name, duration, msg)) => {
                    ctx.schedule_delivery(name, duration, DownstreamStageMsg::ForwardAny(Box::new(msg)));
                }

                Some(SpecialContextAction::CancelDelivery(name)) => {
                    ctx.cancel_delivery(name);
                }


                None => {
                    break;
                }
            }
        }
    }

    fn receive_signal_started<'z>(&'z mut self, ctx: &mut StageContext<StageMsg<A, B, Msg>>) {
        match self.downstream {
            Downstream::Spawned(ref downstream) => {
                downstream.tell(DownstreamStageMsg::SetUpstream( Upstream::Spawned(ctx.actor_ref().convert(upstream_stage_msg_to_stage_msg)),));
            }

            Downstream::Fused(ref mut downstream) => {
                self.downstream_actor_ref = Some(ctx.actor_ref().convert(StageMsg::Forward));

                let actor_ref = ctx.actor_ref().convert(upstream_stage_msg_to_stage_msg);

                let stash = &mut self.stash;

                let mut stash_push = |action: SpecialContextAction<DownstreamStageMsg<B>>| {
                    stash.push_back(StageMsg::SpecialThing(action));
                };

                let mut downstream_ctx = StageContext {
                    inner: InnerStageContext::Testing(&mut stash_push, self.downstream_actor_ref.as_ref().unwrap())
                };

                downstream.receive_signal_started(&mut downstream_ctx);

                downstream.receive_message(
                    DownstreamStageMsg::SetUpstream(Upstream::Fused(actor_ref)),
                    &mut downstream_ctx
                );
            }
        }
    }

    fn receive_single_message(&mut self, msg: StageMsg<A, B, Msg>, ctx: &mut StageContext<StageMsg<A, B, Msg>>) {
        match msg {
            StageMsg::SpecialThing(SpecialContextAction::TellUpstream(msg)) => {
                self.stash.push_back(upstream_stage_msg_to_stage_msg(msg));
            }

            StageMsg::Pull(demand) => {
                //println!("{} StageMsg::Pull({})", self.logic.name(), demand);
                // our downstream has requested more elements

                self.downstream_demand += demand;

                if self.downstream_demand > 0 {
                    // @TODO should only pull if we havent pulled already
                    self.receive_logic_event(LogicEvent::Pulled, ctx);
                }
            }

            StageMsg::Consume(el) => {
                //println!("{} StageMsg::Consume", self.logic.name());
                // our upstream has produced this element

                if self.pulled {
                    self.upstream_demand -= 1;
                    self.pulled = false;

                    //assert!(self.buffer.is_empty());

                    // @TODO assert buffer is empty

                    self.receive_logic_event(LogicEvent::Pushed(el), ctx);

                    // @TODO this was commented out
                    self.check_upstream_demand(ctx);
                } else {
                    println!("enqueue!");
                    self.buffer.push_back(el);
                }
            }

            StageMsg::Action(Action::Pull) => {
                if self.pulled {
                    // @TODO must fail as this is a bug
                } else {
                    match self.buffer.pop_front() {
                        Some(el) => {
                            self.upstream_demand -= 1;
                            self.check_upstream_demand(ctx);

                            self.receive_logic_event(LogicEvent::Pushed(el), ctx);
                        }

                        None if self.upstream_stopped => {
                            // @TODO only send this once!
                            self.receive_logic_event(LogicEvent::Stopped, ctx);
                        }

                        None => {
                            self.pulled = true;

                            if ctx.fused() {
                                self.check_upstream_demand(ctx);
                            }
                        }
                    }
                }
            }

            StageMsg::Action(Action::Push(el)) => {
                if self.downstream_demand == 0 {
                    // @TODO must fail - logic has violated the rules
                } else {
                    match self.downstream {
                        Downstream::Spawned(ref downstream) => {
                            downstream.tell(DownstreamStageMsg::Produce(el));

                            self.downstream_demand -= 1;
                        }

                        Downstream::Fused(ref mut downstream) => {
                            let stash = &mut self.stash;

                            let mut stash_push = |action: SpecialContextAction<DownstreamStageMsg<B>>| {
                                stash.push_back(StageMsg::SpecialThing(action));
                            };

                            let mut downstream_ctx = StageContext {
                                inner: InnerStageContext::Testing(&mut stash_push, self.downstream_actor_ref.as_ref().unwrap())
                            };

                            downstream.receive_message(
                                DownstreamStageMsg::Produce(el),
                                &mut downstream_ctx
                            );

                            self.downstream_demand -= 1;
                        }
                    }

                    if self.downstream_demand > 0 {
                        self.receive_logic_event(LogicEvent::Pulled, ctx);
                    }
                }
            }

            StageMsg::Action(Action::Forward(msg)) => {
                self.receive_logic_event(LogicEvent::Forwarded(msg), ctx);
            }

            StageMsg::Action(Action::Cancel) => {
                if !self.upstream_stopped {
                    ctx.tell_upstream(&mut self.state, UpstreamStageMsg::Cancel);
                }
            }

            StageMsg::Action(Action::PushAndComplete(el, reason)) => {
                self.receive_single_message(StageMsg::Action(Action::Push(el)), ctx);
                self.receive_single_message(StageMsg::Action(Action::Complete(reason)), ctx);
            }

            StageMsg::Action(Action::Complete(reason)) => {
                match self.downstream {
                    Downstream::Spawned(ref downstream) => {
                        downstream.tell(DownstreamStageMsg::Complete(reason));

                        ctx.stop();
                    }

                    Downstream::Fused(ref mut downstream) => {
                        let stash = &mut self.stash;

                        let mut stash_push = |action: SpecialContextAction<DownstreamStageMsg<B>>| {
                            stash.push_back(StageMsg::SpecialThing(action));
                        };

                        let mut downstream_ctx = StageContext {
                            inner: InnerStageContext::Testing(&mut stash_push, self.downstream_actor_ref.as_ref().unwrap())
                        };

                        downstream.receive_message(
                            DownstreamStageMsg::Complete(reason),
                            &mut downstream_ctx
                        );

                        // @TODO what about stopping?
                    }
                }
            }

            StageMsg::Action(Action::None) => {

            }

            StageMsg::ProcessLogicActions => {
                self.process_messages(ctx);
            }

            StageMsg::ForwardAny(msg) => {
                //println!("{} StageMsg::ForwardAny", self.logic.name());
                match msg.downcast() {
                    Ok(msg) => {
                        println!("{} downcast success", self.logic.name());
                        self.stash.push_back(*msg);
                    }

                    Err(_) => {
                        panic!();
                    }
                }
            }


            StageMsg::Forward(msg) => {
                //println!("{} StageMsg::Forward", self.logic.name());
                match self.downstream {
                    Downstream::Spawned(ref downstream) => {
                        panic!("TODO");
                    }

                    Downstream::Fused(ref mut downstream) => {
                        let stash = &mut self.stash;

                        let mut stash_push = |action: SpecialContextAction<DownstreamStageMsg<B>>| {
                            stash.push_back(StageMsg::SpecialThing(action));
                        };

                        let mut downstream_ctx = StageContext {
                            inner: InnerStageContext::Testing(&mut stash_push, self.downstream_actor_ref.as_ref().unwrap())
                        };

                        downstream.receive_message(
                            msg,
                            &mut downstream_ctx
                        );
                    }
                }
            }

            StageMsg::SpecialThing(SpecialContextAction::Stop) => {
                ctx.stop();
            }

            StageMsg::SpecialThing(SpecialContextAction::ScheduleDelivery(name, duration, msg)) => {
                unimplemented!();
                //ctx.schedule_delivery(name, duration, StageMsg::Action(Action::Forward(msg)));
            }

            StageMsg::SpecialThing(SpecialContextAction::CancelDelivery(name)) => {
                ctx.cancel_delivery(name);
            }

            StageMsg::Cancel => {
                //////println!("{} StageMsg::Cancel", self.logic.name());
                // downstream has cancelled us

                if !self.upstream_stopped {
                    ctx.tell_upstream(&mut self.state, UpstreamStageMsg::Cancel);
                }

                self.receive_logic_event(LogicEvent::Cancelled, ctx);
            }

            StageMsg::Stopped(reason) => {
                //println!("{} StageMsg::Stopped", self.logic.name());
                // upstream has stopped, need to drain buffers and be done

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

    fn process_messages(&mut self, ctx: &mut StageContext<StageMsg<A, B, Msg>>) {
        while let Some(msg) = self.stash.pop_front() {
            self.receive_single_message(msg, ctx);
        }
    }

    fn receive_message(&mut self, msg: StageMsg<A, B, Msg>, ctx: &mut StageContext<StageMsg<A, B, Msg>>) {
        match self.state {
            StageState::Running(_) => {
                self.process_messages(ctx);
                self.receive_single_message(msg, ctx);
                self.process_messages(ctx);
            }

            StageState::Waiting => match msg {
                StageMsg::SetUpstream(upstream) => {
                    self.state = StageState::Running(upstream);
                    self.receive_logic_event(LogicEvent::Started, ctx);
                    self.check_upstream_demand(ctx);
                    self.process_messages(ctx);
                }

                other => {
                    self.stash.push_back(other);
                }
            }
        }
    }

    #[inline(always)]
    fn receive_logic_event(
        &mut self,
        event: LogicEvent<A, Msg>,
        ctx: &mut StageContext<StageMsg<A, B, Msg>>,
    ) {
        let action = {
            let mut ctx = StreamContext {
                ctx,
                stash: &mut self.stash
            };

            self.logic.receive(event, &mut ctx)


        };

        self.receive_single_message(StageMsg::Action(action), ctx);

        //@TODO
        //self.process_messages(ctx);
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
        if ctx.fused() {
            self.midstream_context = Some(
                SpecialContext {
                    actions: VecDeque::new(),
                    actor_ref: ctx.actor_ref().convert(move |msg| {
                        DownstreamStageMsg::ForwardAny(Box::new(msg))
                    })
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
        let mut midstream_context = self.midstream_context.take().expect("TODO");

        self.receive_message(
            downstream_stage_msg_to_stage_msg(msg),
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
        let mut fused_level = 0;

        if let Signal::Started = signal {
            self.receive_signal_started(
                &mut StageContext {
                    inner: InnerStageContext::Spawned(ctx)
                }
            );
        }
    }

    fn receive(&mut self, msg: StageMsg<A, B, Msg>, ctx: &mut ActorContext<StageMsg<A, B, Msg>>) {
        let mut fused_level = 0;

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
                logic: Box::new(self.logic),
                buffer: Box::new(VecDeque::with_capacity(1)),
                midstream_context: None,
                downstream,
                downstream_actor_ref: None,
                downstream_context: None,
                state: StageState::Waiting,
                pulled: false,
                upstream_stopped: false,
                downstream_demand: 0,
                upstream_demand: 0,
                stash: Box::new(VecDeque::new())
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
                    logic: Box::new(self.logic),
                    buffer: Box::new(VecDeque::with_capacity(buffer_size)),
                    midstream_context: None,
                    downstream,
                    downstream_actor_ref: None,
                    downstream_context: None,
                    state: StageState::Waiting,
                    pulled: false,
                    upstream_stopped: false,
                    downstream_demand: 0,
                    upstream_demand: 0,
                    stash: Box::new(VecDeque::new())
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
    FromSourceAsDownstream(DownstreamStageMsg<()>),
    SetDownstream(Downstream<()>),
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
    fn fuse(self: Box<Self>) -> Box<RunnableStream<Out> + 'static + Send>;

    fn run(self: Box<Self>, context: &mut ActorContext<InternalStreamCtl<Out>>);
}

impl<A, Out> RunnableStream<Out> for UnionLogic<(), A, Out>
where
    A: 'static + Send,
    Out: 'static + Send,
{
    fn fuse(mut self: Box<Self>) -> Box<RunnableStream<Out> + 'static + Send> {
        self.upstream = self.upstream.fuse();
        self.downstream = self.downstream.fuse();
        self
    }

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

        context.actor_ref().tell(InternalStreamCtl::SetDownstream(stream));
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
            logic: Box::new(self.logic),
            buffer: Box::new(VecDeque::with_capacity(0)),
            midstream_context: None,
            downstream,
            downstream_actor_ref: None,
            downstream_context: None,
            state: StageState::Waiting,
            pulled: false,
            upstream_stopped: false,
            downstream_demand: 0,
            upstream_demand: 0,
            stash: Box::new(VecDeque::new())
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
            downstream: None,
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
    downstream: Option<(StageRef<DownstreamStageMsg<()>>, SpecialContext<DownstreamStageMsg<()>>)>,
}

impl<Out> StreamController<Out>
where Out: Send {
    fn process_downstream_ctx(&mut self, ctx: &mut ActorContext<InternalStreamCtl<Out>>) {
        loop {
            let (_, downstream_ctx) = self.downstream.as_mut().expect("TODO");

            match downstream_ctx.actions.pop_front() {
                Some(SpecialContextAction::Stop) => {
                    // @TODO think about this
                    //ctx.stop();
                }

                Some(SpecialContextAction::ScheduleDelivery(name, duration, msg)) => {
                    ctx.schedule_delivery(name, duration, InternalStreamCtl::FromSourceAsDownstream(msg));
                }

                Some(SpecialContextAction::CancelDelivery(name)) => {
                    ctx.cancel_delivery(name);
                }

                Some(SpecialContextAction::TellUpstream(msg)) => {
                    drop(msg); // there is no upstream
                }

                None => {
                    break;
                }
            }
        }
    }
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
                        upstream.tell(UpstreamStageMsg::Pull(1));
                    }

                    Upstream::Fused(upstream) => {
                        upstream.tell(UpstreamStageMsg::Pull(1));
                    }
                }
            }

            InternalStreamCtl::FromSink(DownstreamStageMsg::Produce(value)) => {
                let _ = self.state.swap(Some(value));

                self.produced = true;
            }

            InternalStreamCtl::FromSink(DownstreamStageMsg::Complete(reason)) => {
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

                match downstream {
                    Downstream::Spawned(downstream) => {
                        downstream.tell(DownstreamStageMsg::SetUpstream(Upstream::Spawned(actor_ref_for_source)));

                        downstream.tell(DownstreamStageMsg::Complete(None));
                    }

                    Downstream::Fused(mut downstream) => {
                        let actor_ref = ctx.actor_ref().convert(InternalStreamCtl::FromSource);
                        let other_actor_ref = ctx.actor_ref().convert(InternalStreamCtl::FromSourceAsDownstream);

                        let mut fused_level = 0;

                        let mut special_context = SpecialContext {
                            actions: VecDeque::new(),
                            actor_ref: other_actor_ref
                        };

                        let mut downstream_ctx = StageContext {
                            inner: InnerStageContext::Special(
                                &mut special_context
                            )
                        };

                        downstream.receive_signal_started(&mut downstream_ctx);

                        downstream.receive_message(
                            DownstreamStageMsg::SetUpstream(Upstream::Fused(actor_ref)),
                            &mut downstream_ctx
                        );

                        self.downstream = Some((downstream, special_context));

                        self.process_downstream_ctx(ctx);

                        let (downstream, special_context) = self.downstream.as_mut().expect("TODO");

                        let mut downstream_ctx = StageContext {
                            inner: InnerStageContext::Special(special_context)
                        };

                        downstream.receive_message(DownstreamStageMsg::Complete(None), &mut downstream_ctx);

                        self.process_downstream_ctx(ctx);
                    }
                }

            }

            InternalStreamCtl::FromSource(UpstreamStageMsg::Pull(value)) => {
                // nothing to do, we already sent a completed
            }

            InternalStreamCtl::FromSource(UpstreamStageMsg::Cancel) => {
                // nothing to do, we already sent a completed
            }

            InternalStreamCtl::FromSourceAsDownstream(msg) => {
                match self.downstream {
                    Some((ref mut downstream, ref mut downstream_ctx)) => {
                        let mut fused_level = 0;
                        let mut downstream_ctx = StageContext {
                            inner: InnerStageContext::Special(downstream_ctx)
                        };

                        downstream.receive_message(
                            msg,
                            &mut downstream_ctx
                        );

                        self.process_downstream_ctx(ctx);
                    }

                    _ => panic!("TODO")
                }


            }

            InternalStreamCtl::FromSink(_) => {
            }
        }
    }
}
