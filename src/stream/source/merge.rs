/*
use crate::actor::ActorRef;
use crate::stream::internal::{Downstream, DownstreamStageMsg, Producer, UpstreamStageMsg};
use crate::stream::{Action, Logic, LogicEvent, StreamContext};
use std::collections::VecDeque;
use std::marker::PhantomData;

pub struct Merge<A>
where
    A: 'static + Send,
{
    producers: Vec<Box<Producer<(), A> + Send>>,
    actors: Vec<ActorRef<DownstreamStageMsg<()>>>,
    buffer: VecDeque<A>,
    pulled: bool,
    completed: usize,
    demand: Vec<u64>,
}

// @TODO visibility here...
pub enum MergeMsg<A>
where
    A: 'static + Send,
{
    FromUpstream(usize, FromUpstreamMsg<A>),
    ForUpstream(usize, ForUpstreamMsg),
}

pub struct FromUpstreamMsg<A>
where
    A: 'static + Send,
{
    msg: DownstreamStageMsg<A>,
}

pub struct ForUpstreamMsg {
    msg: UpstreamStageMsg,
}

const BUFFER_SIZE_TEMP: u64 = 16;

impl<A> Merge<A>
where
    A: Send,
{
    pub(in crate::stream) fn new(producers: Vec<Box<Producer<(), A> + Send>>) -> Self {
        let len = producers.len();

        Self {
            producers,
            actors: Vec::with_capacity(len),
            buffer: VecDeque::with_capacity(len * BUFFER_SIZE_TEMP as usize),
            pulled: false,
            completed: 0,
            demand: Vec::with_capacity(len),
        }
    }

    fn check_demand(&mut self, id: usize) {
        let available = BUFFER_SIZE_TEMP - self.demand[id];

        if available >= BUFFER_SIZE_TEMP / 2 {
            self.actors[id].tell(DownstreamStageMsg::Converted(UpstreamStageMsg::Pull(
                available,
            )));
            self.demand[id] += available;
        }
    }
}

impl<A> Logic<(), A> for Merge<A>
where
    A: 'static + Send,
{
    type Ctl = MergeMsg<A>;

    fn name(&self) -> &'static str {
        "Merge"
    }

    fn buffer_size(&self) -> Option<usize> {
        Some(0)
    }

    fn receive(
        &mut self,
        msg: LogicEvent<(), Self::Ctl>,
        ctx: &mut StreamContext<(), A, Self::Ctl>,
    ) {
        match msg {
            LogicEvent::Pulled => match self.buffer.pop_front() {
                Some(el) => {
                    ctx.tell(Action::Push(el));
                }

                None if self.completed == self.actors.len() => {
                    ctx.tell(Action::Complete(None));
                }

                None => {
                    self.pulled = true;
                }
            },

            LogicEvent::Forwarded(MergeMsg::FromUpstream(
                id,
                FromUpstreamMsg {
                    msg: DownstreamStageMsg::Produce(a),
                },
            )) => {
                if self.pulled {
                    self.pulled = false;

                    ctx.tell(Action::Push(a));
                } else {
                    self.buffer.push_back(a);
                };

                if self.demand[id] == 0 {
                    // @TODO fail
                } else {
                    self.demand[id] -= 1;
                    self.check_demand(id);
                }
            }

            LogicEvent::Forwarded(MergeMsg::FromUpstream(
                id,
                FromUpstreamMsg {
                    msg: DownstreamStageMsg::Complete(reason),
                },
            )) => {
                self.completed = self.actors.len().min(self.completed + 1);

                if self.completed == self.actors.len() && self.buffer.is_empty() {
                    ctx.tell(Action::Complete(None));
                }
            }

            LogicEvent::Forwarded(MergeMsg::FromUpstream(
                id,
                FromUpstreamMsg {
                    msg: DownstreamStageMsg::SetUpstream(upstream),
                },
            )) => {
                // @TODO not sure what this means
            }

            LogicEvent::Forwarded(MergeMsg::FromUpstream(
                id,
                FromUpstreamMsg {
                    msg: DownstreamStageMsg::Converted(UpstreamStageMsg::Pull(demand)),
                },
            )) => {
                // @TODO not sure what this means
            }

            LogicEvent::Forwarded(MergeMsg::FromUpstream(
                id,
                FromUpstreamMsg {
                    msg: DownstreamStageMsg::Converted(UpstreamStageMsg::Cancel),
                },
            )) => {
                // @TODO not sure what this means
            }

            LogicEvent::Forwarded(MergeMsg::ForUpstream(
                id,
                ForUpstreamMsg {
                    msg: UpstreamStageMsg::Pull(_),
                },
            )) => {
                // ignore
            }

            LogicEvent::Forwarded(MergeMsg::ForUpstream(
                id,
                ForUpstreamMsg {
                    msg: UpstreamStageMsg::Cancel,
                },
            )) => {
                // ignore
            }

            LogicEvent::Cancelled => {
                self.completed = self.actors.len();

                for producer in self.actors.iter() {
                    producer.tell(DownstreamStageMsg::Converted(UpstreamStageMsg::Cancel));
                }
            }

            LogicEvent::Started => {
                for (id, p) in self.producers.drain(..).enumerate() {
                    let downstream = ctx.actor_ref().convert(move |msg| {
                        Action::Forward(MergeMsg::FromUpstream(id, FromUpstreamMsg { msg }))
                    });
                    let mut spawn_ctx = ctx.ctx.spawn_context();

                    let actor_ref = p.spawn(Downstream::Spawned(downstream), &mut spawn_ctx);

                    actor_ref.tell(DownstreamStageMsg::SetUpstream(ctx.actor_ref().convert(
                        move |msg| {
                            Action::Forward(MergeMsg::ForUpstream(id, ForUpstreamMsg { msg }))
                        },
                    )));
                    actor_ref.tell(DownstreamStageMsg::Complete(None));

                    self.actors.push(actor_ref);
                    self.demand.push(0);
                }

                for id in 0..self.actors.len() {
                    self.check_demand(id);
                }

                self.producers = Vec::with_capacity(0);
            }

            LogicEvent::Pushed(()) | LogicEvent::Stopped => {}
        }
    }
}*/
