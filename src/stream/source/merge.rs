use crate::actor::ActorRef;
use crate::stream::internal::{DownstreamStageMsg, Producer, UpstreamStageMsg};
use crate::stream::{Action, Logic, StreamContext};
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
pub(in crate::stream) enum MergeMsg<A>
where
    A: 'static + Send,
{
    FromUpstream(usize, DownstreamStageMsg<A>),
    ForUpstream(usize, UpstreamStageMsg),
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

impl<A> Logic<(), A, MergeMsg<A>> for Merge<A>
where
    A: 'static + Send,
{
    fn name(&self) -> &'static str {
        "Merge"
    }

    fn buffer_size(&self) -> Option<usize> {
        Some(0)
    }

    fn started(
        &mut self,
        ctx: &mut StreamContext<(), A, MergeMsg<A>>,
    ) -> Option<Action<A, MergeMsg<A>>> {
        for (id, p) in self.producers.drain(..).enumerate() {
            let downstream = ctx
                .actor_ref()
                .convert(move |msg| Action::Forward(MergeMsg::FromUpstream(id, msg)));
            let mut spawn_ctx = ctx.ctx.spawn_context();

            let actor_ref = p.spawn(downstream, &mut spawn_ctx);

            actor_ref
                .tell(DownstreamStageMsg::SetUpstream(ctx.actor_ref().convert(
                    move |msg| Action::Forward(MergeMsg::ForUpstream(id, msg)),
                )));
            actor_ref.tell(DownstreamStageMsg::Complete(None));

            self.actors.push(actor_ref);
            self.demand.push(0);
        }

        for id in 0..self.actors.len() {
            self.check_demand(id);
        }

        self.producers = Vec::with_capacity(0);

        None
    }

    fn cancelled(
        &mut self,
        ctx: &mut StreamContext<(), A, MergeMsg<A>>,
    ) -> Option<Action<A, MergeMsg<A>>> {
        self.completed = self.actors.len();

        for producer in self.actors.iter() {
            producer.tell(
                DownstreamStageMsg::Converted(UpstreamStageMsg::Cancel)
            );
        }

        Some(Action::Complete(None))
    }

    fn pulled(
        &mut self,
        ctx: &mut StreamContext<(), A, MergeMsg<A>>,
    ) -> Option<Action<A, MergeMsg<A>>> {
        match self.buffer.pop_front() {
            Some(el) => Some(Action::Push(el)),

            None if self.completed == self.actors.len() => Some(Action::Complete(None)),

            None => {
                self.pulled = true;

                None
            }
        }
    }

    fn forwarded(
        &mut self,
        msg: MergeMsg<A>,
        ctx: &mut StreamContext<(), A, MergeMsg<A>>,
    ) -> Option<Action<A, MergeMsg<A>>> {
        match msg {
            MergeMsg::FromUpstream(id, DownstreamStageMsg::Produce(a)) => {
                let result = if self.pulled {
                    self.pulled = false;

                    Some(Action::Push(a))
                } else {
                    self.buffer.push_back(a);
                    None
                };

                if self.demand[id] == 0 {
                    // @TODO fail
                } else {
                    self.demand[id] -= 1;
                    self.check_demand(id);
                }

                result
            }

            MergeMsg::FromUpstream(id, DownstreamStageMsg::Complete(reason)) => {
                self.completed = self.actors.len().min(self.completed + 1);

                if self.completed == self.actors.len() && self.buffer.is_empty() {
                    Some(Action::Complete(None))
                } else {
                    None
                }
            }

            MergeMsg::FromUpstream(id, DownstreamStageMsg::SetUpstream(upstream)) => {
                // @TODO not sure what this means
                None
            }

            MergeMsg::FromUpstream(
                id,
                DownstreamStageMsg::Converted(UpstreamStageMsg::Pull(demand)),
            ) => {
                // @TODO not sure what this means
                None
            }

            MergeMsg::FromUpstream(id, DownstreamStageMsg::Converted(UpstreamStageMsg::Cancel)) => {
                // @TODO not sure what this means
                None
            }

            MergeMsg::ForUpstream(id, UpstreamStageMsg::Pull(_)) => {
                // ignore
                None
            }

            MergeMsg::ForUpstream(id, UpstreamStageMsg::Cancel) => {
                // ignore
                None
            }
        }
    }

    fn pushed(
        &mut self,
        el: (),
        ctx: &mut StreamContext<(), A, MergeMsg<A>>,
    ) -> Option<Action<A, MergeMsg<A>>> {
        None
    }

    fn stopped(
        &mut self,
        ctx: &mut StreamContext<(), A, MergeMsg<A>>,
    ) -> Option<Action<A, MergeMsg<A>>> {
        None
    }
}
