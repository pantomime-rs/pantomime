use crate::stream::{Action, Logic, LogicEvent, StreamContext};
use std::time::Duration;

pub enum DelayMsg<A> {
    Ready(A),
}

pub struct Delay {
    delay: Duration,
    state: State
}

enum State {
    Waiting,
    Pushing,
    Stopping
}

impl Delay {
    pub fn new(delay: Duration) -> Self {
        Self {
            delay,
            state: State::Waiting
        }
    }
}

impl<A: Send> Logic<A, A> for Delay {
    type Ctl = DelayMsg<A>;

    fn name(&self) -> &'static str {
        "Delay"
    }

    fn buffer_size(&self) -> Option<usize> {
        Some(0) // @FIXME should this be based on the delay duration?
    }

    fn receive(&mut self, msg: LogicEvent<A, Self::Ctl>, ctx: &mut StreamContext<A, A, Self::Ctl>) {
        match msg {
            LogicEvent::Pulled => {
                ctx.tell(Action::Pull);
            }

            LogicEvent::Pushed(element) => {
                ctx.schedule_delivery("ready", self.delay.clone(), DelayMsg::Ready(element));

                self.state = State::Pushing;
            }

            LogicEvent::Started => {}

            LogicEvent::Stopped => {
                match self.state {
                    State::Pushing => {
                        self.state = State::Stopping;
                    }

                    State::Waiting => {
                        ctx.tell(Action::Complete(None));
                    }

                    State::Stopping => {}
                }
            }

            LogicEvent::Cancelled => {
                ctx.tell(Action::Complete(None));
            }

            LogicEvent::Forwarded(DelayMsg::Ready(element)) => {
                ctx.tell(Action::Push(element));

                if let State::Stopping = self.state {
                    ctx.tell(Action::Complete(None));
                }

                self.state = State::Waiting;
            }
        }
    }
}
