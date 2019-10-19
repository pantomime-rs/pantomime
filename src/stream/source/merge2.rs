use crate::actor::ActorRef;
use crate::stream::internal::{DownstreamStageMsg, UpstreamStageMsg};
use crate::stream::{
    Action, Flow, Logic, LogicEvent, LogicPortEvent, PortAction, PortRef, Source, StreamContext,
};
use std::collections::{HashMap, VecDeque};

pub struct Merge<A>
where
    A: 'static + Send,
{
    flows: Option<Vec<Flow<A, A>>>,
    handles: Vec<PortRef<A, A, MergeMsg<A>, A>>,
    buffer: VecDeque<(Option<usize>, A)>,
    pulled: bool,
}

pub enum MergeMsg<A>
where
    A: 'static + Send,
{
    LogicPortEvent(LogicPortEvent<A>),
}

impl<A> Logic<A, A> for Merge<A>
where
    A: 'static + Send,
{
    type Ctl = MergeMsg<A>;

    fn name(&self) -> &'static str {
        "Merge"
    }

    fn receive(&mut self, msg: LogicEvent<A, Self::Ctl>, ctx: &mut StreamContext<A, A, Self::Ctl>) -> Action<A, Self::Ctl> {
        match msg {
            LogicEvent::Pulled => match self.buffer.pop_front() {
                Some((Some(id), element)) => {
                    ctx.tell_port(&mut self.handles[id], PortAction::Pull);
                    ctx.tell(Action::Push(element));
                }

                Some((None, element)) => {
                    ctx.tell(Action::Pull);
                    ctx.tell(Action::Push(element));
                }

                None => {
                    self.pulled = true;
                }
            },

            LogicEvent::Pushed(element) => {
                if self.pulled {
                    ctx.tell(Action::Pull);
                    ctx.tell(Action::Push(element));

                    self.pulled = false;
                } else {
                    self.buffer.push_back((None, element));
                }
            }

            LogicEvent::Forwarded(MergeMsg::LogicPortEvent(LogicPortEvent::Pushed(
                id,
                element,
            ))) => {
                if self.pulled {
                    ctx.tell(Action::Push(element));
                    ctx.tell_port(&mut self.handles[id], PortAction::Pull);

                    self.pulled = false;
                } else {
                    self.buffer.push_back((Some(id), element));
                }
            }

            LogicEvent::Started => {
                ctx.tell(Action::Pull);

                for flow in self
                    .flows
                    .take()
                    .expect("pantomime bug: Merge::flows is None")
                    .drain(..)
                {
                    ctx.spawn_port(flow, MergeMsg::LogicPortEvent);
                }
            }

            LogicEvent::Forwarded(MergeMsg::LogicPortEvent(LogicPortEvent::Started(id))) => {
                ctx.tell_port(&mut self.handles[id], PortAction::Pull);
            }

            LogicEvent::Forwarded(MergeMsg::LogicPortEvent(LogicPortEvent::Pulled(id))) => {
                // this shouldn't happen because ports for this logic don't have an upstream,
                // i.e. they won't request any elements
                // @TODO maybe fail if it does?
            }

            LogicEvent::Forwarded(MergeMsg::LogicPortEvent(LogicPortEvent::Stopped(id))) => {
                // @TODO track completed ports (eager?)
            }

            LogicEvent::Forwarded(MergeMsg::LogicPortEvent(LogicPortEvent::Forwarded(id, any))) => {
                unimplemented!();
            }

            LogicEvent::Forwarded(MergeMsg::LogicPortEvent(LogicPortEvent::Cancelled(id))) => {
                ctx.tell_port(&mut self.handles[id], PortAction::Complete(None));
            }

            LogicEvent::Cancelled => {
                // @TODO track already completed ports

                for mut handle in self.handles.iter_mut() {
                    ctx.tell_port(&mut handle, PortAction::Complete(None));
                }

                ctx.tell(Action::Complete(None));
            }

            LogicEvent::Stopped => {
                // @TODO track completions / completed ports
            }
        }

        // @TODO
        Action::None
    }
}
