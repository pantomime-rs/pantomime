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

pub(in crate::stream) enum InnerStageContext<'a, Msg> where Msg: 'static + Send  {
    Spawned(&'a mut ActorContext<Msg>, &'a mut usize),
    Special(&'a mut SpecialContext<Msg>, &'a mut usize)
}

enum SpecialContextAction<Msg> {
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
            InnerStageContext::Spawned(ref mut context, _) => {
                context.schedule_delivery(name, timeout, msg);
            }

            InnerStageContext::Special(ref mut context, _) => {
                context.actions.push_back(SpecialContextAction::ScheduleDelivery(name.as_ref().to_string(), timeout, msg));
            }
        }
    }

    fn actor_ref(&self) -> ActorRef<Msg> {
        match self.inner {
            InnerStageContext::Spawned(ref context, _) => {
                context.actor_ref().clone()
            }

            InnerStageContext::Special(ref context, _) => {
                context.actor_ref.clone()
            }
        }
    }

    fn cancel_delivery<S: AsRef<str>>(&mut self, name: S) {
        match self.inner {
            InnerStageContext::Spawned(ref mut context, _) => {
                context.cancel_delivery(name);
            }

            InnerStageContext::Special(ref mut context, _) => {
                context.actions.push_back(SpecialContextAction::CancelDelivery(name.as_ref().to_string()));
            }
        }
    }

    fn fused(&self) -> bool {
        match self.inner {
            InnerStageContext::Spawned(_, _) => false,
            InnerStageContext::Special(_, _) => true,
        }
    }

    fn fused_push(&mut self) -> bool {
        match self.inner {
            InnerStageContext::Special(_, ref mut size) => {
                **size += 1;
                **size < 10 // @TODO cfg
            }

            InnerStageContext::Spawned(_, ref mut size) => {
                **size += 1;
                **size < 10 // @TODO cfg
            }
        }
    }

    fn fused_level(&mut self) -> &mut usize {
        match self.inner {
            InnerStageContext::Special(_, ref mut level) => level,
            InnerStageContext::Spawned(_, ref mut level) => level,
        }
    }

    fn fused_pop(&mut self) {
        match self.inner {
            InnerStageContext::Special(_, ref mut size) => {
                **size -= 1;
            }

            InnerStageContext::Spawned(_, ref mut size) => {
                **size -= 1;
            }
        }
    }


    fn stop(&mut self) {
        match self.inner {
            InnerStageContext::Spawned(ref mut context, _) => {
                context.stop();
            }

            InnerStageContext::Special(ref mut context, _) => {
                context.actions.push_back(SpecialContextAction::Stop);
            }
        }
    }

    fn tell(&mut self, msg: Msg) {
        match self.inner {
            InnerStageContext::Spawned(ref context, _) => {
                context.actor_ref().tell(msg);
            }

            InnerStageContext::Special(ref context, _) => {
                //context.actions.push_back(SpecialContextAction::TellUpstream(msg));
                context.actor_ref.tell(msg);
            }
        }
    }

    fn tell_upstream<A: Send, B: Send, M: Send>(&mut self, state: &mut StageState<A, B, M>, msg: UpstreamStageMsg) {
        match self.inner {
            InnerStageContext::Spawned(_, _) => {
                match state {
                    StageState::Running(Upstream::Spawned(ref upstream)) => {
                        upstream.tell(msg);
                    }

                    StageState::Running(Upstream::Fused(ref upstream)) => {
                        upstream.tell(msg);
                    }

                    StageState::Waiting(Some(ref mut stash)) => {
                        stash.push_back(upstream_stage_msg_to_stage_msg(msg));
                    }

                    StageState::Waiting(None) => {
                        let mut stash = VecDeque::new();

                        stash.push_back(upstream_stage_msg_to_stage_msg(msg));

                        *state = StageState::Waiting(Some(stash));
                    }
                }
            }

            InnerStageContext::Special(ref mut context, _) => {
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

    fn process_downstream_ctx(&mut self, ctx: &mut StageContext<StageMsg<A, B, Msg>>) {
        loop {
            let downstream_ctx = self.downstream_context.as_mut().expect("TODO");

            match downstream_ctx.actions.pop_front() {
                Some(SpecialContextAction::Stop) => {
                    ctx.stop();
                }

                Some(SpecialContextAction::ScheduleDelivery(name, duration, msg)) => {
                    ctx.schedule_delivery(name, duration, StageMsg::Forward(msg));
                }

                Some(SpecialContextAction::CancelDelivery(name)) => {
                    ctx.cancel_delivery(name);
                }

                Some(SpecialContextAction::TellUpstream(msg)) => {
                    self.receive_message(upstream_stage_msg_to_stage_msg(msg), ctx);
                }

                None => {
                    break;
                }
            }
        }
    }

    fn process_midstream_ctx(&mut self, ctx: &mut StageContext<DownstreamStageMsg<A>>) {
        loop {
            let next = self.midstream_context.as_mut().expect("TODO").actions.pop_front();
            //let mut midstream_context = self.midstream_context.as_mut().expect("TODO");

            match next {
                Some(SpecialContextAction::Stop) => {
                    ctx.stop();
                }

                Some(SpecialContextAction::ScheduleDelivery(name, duration, msg)) => {
                    ctx.schedule_delivery(name, duration, DownstreamStageMsg::ForwardAny(Box::new(msg)));
                }

                Some(SpecialContextAction::CancelDelivery(name)) => {
                    ctx.cancel_delivery(name);
                }

                Some(SpecialContextAction::TellUpstream(msg)) => {
                    ctx.tell_upstream(&mut self.state, msg);
                }

                None => {
                    break;
                }
            }
        }
    }

    fn receive_signal_started(&mut self, ctx: &mut StageContext<StageMsg<A, B, Msg>>) {
        match self.downstream {
            Downstream::Spawned(ref downstream) => {
                downstream.tell(DownstreamStageMsg::SetUpstream( Upstream::Spawned(ctx.actor_ref().convert(upstream_stage_msg_to_stage_msg)),));
            }

            Downstream::Fused(ref mut downstream) => {
                let actor_ref = ctx.actor_ref().convert(upstream_stage_msg_to_stage_msg);

                let mut special_context = SpecialContext {
                    actions: VecDeque::new(),
                    actor_ref: ctx.actor_ref().convert(StageMsg::Forward)
                };

                let mut downstream_ctx = StageContext {
                    inner: InnerStageContext::Special(
                        &mut special_context,
                        ctx.fused_level()
                    )
                };

                downstream.receive_signal_started(&mut downstream_ctx);

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
            StageState::Running(_) => {
                match msg {
                    StageMsg::ProcessLogicActions => {
                        self.process_actions(ctx);
                    }

                    StageMsg::ForwardAny(msg) => {
                        //println!("{} StageMsg::ForwardAny", self.logic.name());
                        match msg.downcast() {
                            Ok(msg) => {
                                //println!("{} downcast success", self.logic.name());
                                self.receive_message(*msg, ctx);
                            }

                            Err(_) => {
                                panic!();
                            }
                        }
                    }

                    StageMsg::Pull(demand) => {
                        //println!("{} StageMsg::Pull({})", self.logic.name(), demand);
                        // our downstream has requested more elements

                        self.downstream_demand += demand;

                        if self.downstream_demand > 0 {
                            self.receive_logic_event(LogicEvent::Pulled, ctx);
                        }
                    }

                    StageMsg::Consume(el) => {
                        //println!("{} StageMsg::Consume", self.logic.name());
                        // our upstream has produced this element

                        if self.pulled {
                            self.upstream_demand -= 1;
                            self.pulled = false;

                            assert!(self.buffer.is_empty());

                            // @TODO assert buffer is empty

                            self.receive_logic_event(LogicEvent::Pushed(el), ctx);

                            // @TODO this was commented out
                            self.check_upstream_demand(ctx);
                        } else {
                            self.buffer.push_back(el);
                        }
                    }

                    StageMsg::Action(action) => {
                        //println!("{} StageMsg::Action", self.logic.name());
                        self.receive_action(action, ctx);
                    }

                    StageMsg::Forward(msg) => {
                        //println!("{} StageMsg::Forward", self.logic.name());
                        match self.downstream {
                            Downstream::Spawned(ref downstream) => {
                                panic!("TODO");
                            }

                            Downstream::Fused(ref mut downstream) => {
                                let downstream_ctx = self.downstream_context.as_mut().unwrap();

                                downstream.receive_message(
                                    msg,
                                    &mut StageContext {
                                        inner: InnerStageContext::Special(downstream_ctx, ctx.fused_level())
                                    }
                                );

                                self.process_downstream_ctx(ctx);
                            }
                        }
                    }

                    StageMsg::Cancel => {
                        //println!("{} StageMsg::Cancel", self.logic.name());
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

            StageState::Waiting(ref stash) if stash.is_none() => {
                match msg {
                    StageMsg::SetUpstream(upstream) => {
                        self.state = StageState::Running(upstream);
                        self.receive_logic_event(LogicEvent::Started, ctx);
                        self.check_upstream_demand(ctx);
                    }

                    other => {
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
                        self.receive_message(msg, ctx);
                    }
                }

                other => {
                    let mut stash = stash
                        .take()
                        .expect("pantomime bug: Option::take failed despite being Some");

                    stash.push_back(other);

                    self.state = StageState::Waiting(Some(stash));
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

        self.process_actions(ctx);
    }

    fn process_actions(&mut self, ctx: &mut StageContext<StageMsg<A, B, Msg>>) {
        loop {
            if ctx.fused_push() {
                //println!("here!");
                if let Some(event) = self.logic_actions.pop_front() {
                    self.receive_action(event, ctx);

                    ctx.fused_pop();
                } else {
                    ctx.fused_pop();

                    return;
                }
            } else {
                ctx.tell(StageMsg::ProcessLogicActions);

                return;
            }
        }
    }

    fn receive_action(
        &mut self,
        action: Action<B, Msg>,
        ctx: &mut StageContext<StageMsg<A, B, Msg>>,
    ) {
        match action {
            Action::Pull => {
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

            Action::Push(el) => {
                if self.downstream_demand == 0 {
                    // @TODO must fail - logic has violated the rules
                } else {
                    match self.downstream {
                        Downstream::Spawned(ref downstream) => {
                            downstream.tell(DownstreamStageMsg::Produce(el));

                            self.downstream_demand -= 1;
                        }

                        Downstream::Fused(ref mut downstream) => {
                            let downstream_ctx = self.downstream_context.as_mut().unwrap();

                            downstream.receive_message(
                                DownstreamStageMsg::Produce(el),
                                &mut StageContext {
                                    inner: InnerStageContext::Special(downstream_ctx, ctx.fused_level())
                                }
                            );

                            self.downstream_demand -= 1;

                            self.process_downstream_ctx(ctx);
                        }
                    }


                    if self.downstream_demand > 0 {
                        self.receive_logic_event(LogicEvent::Pulled, ctx);
                    }
                }
            }

            Action::Forward(msg) => {
                self.receive_logic_event(LogicEvent::Forwarded(msg), ctx);
            }

            Action::Cancel => {
                if !self.upstream_stopped {
                    ctx.tell_upstream(&mut self.state, UpstreamStageMsg::Cancel);
                }
            }

            Action::Complete(reason) => {
                match self.downstream {
                    Downstream::Spawned(ref downstream) => {
                        downstream.tell(DownstreamStageMsg::Complete(reason));

                        ctx.stop();
                    }

                    Downstream::Fused(ref mut downstream) => {
                        let downstream_ctx = self.downstream_context.as_mut().unwrap();

                        downstream.receive_message(
                            DownstreamStageMsg::Complete(reason),
                            &mut StageContext {
                                inner: InnerStageContext::Special(downstream_ctx, ctx.fused_level())
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
        if ctx.fused() {
            let mut i = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            self.midstream_context = Some(
                SpecialContext {
                    actions: VecDeque::new(),
                    actor_ref: ctx.actor_ref().convert(move |msg| {
                        match msg {
                            StageMsg::ProcessLogicActions => {
                                //println!("yep its a process");
                            }

                            _ => {}
                        }
                        let v = i.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                        //println!("forward: {}", v);

                        //if v == 1789986 {
                        //    panic!();
                        //}
                        DownstreamStageMsg::ForwardAny(Box::new(msg))
                    })
                }
            );
        }

        let mut midstream_context = self.midstream_context.take().expect("TODO");

        self.receive_signal_started(
            &mut StageContext {
                inner: InnerStageContext::Special(&mut midstream_context, ctx.fused_level())
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
                inner: InnerStageContext::Special(&mut midstream_context, ctx.fused_level())
            }
        );

        self.midstream_context = Some(midstream_context);

        /*
        if rand::random() && rand::random() && rand::random() && rand::random() && rand::random() && rand::random() && rand::random() {
            panic!();
        }*/

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
                    inner: InnerStageContext::Spawned(ctx, &mut fused_level)
                }
            );
        }
    }

    fn receive(&mut self, msg: StageMsg<A, B, Msg>, ctx: &mut ActorContext<StageMsg<A, B, Msg>>) {
        let mut fused_level = 0;

        self.receive_message(
            msg,
            &mut StageContext {
                inner: InnerStageContext::Spawned(ctx, &mut fused_level)
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
                                &mut special_context,
                                &mut fused_level
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

                        let mut fused_level =  0;

                        let mut downstream_ctx = StageContext {
                            inner: InnerStageContext::Special(special_context, &mut fused_level)
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
                            inner: InnerStageContext::Special(downstream_ctx, &mut fused_level)
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
