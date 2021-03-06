use crate::stream::{Action, Logic, LogicEvent, StreamContext};
use std::time::Duration;

pub enum DelayMsg<A> {
    Ready(A),
}

pub struct Delay {
    delay: Duration,
    state: State,
}

enum State {
    Waiting,
    Pushing,
    Stopping,
}

impl Delay {
    pub fn new(delay: Duration) -> Self {
        Self {
            delay,
            state: State::Waiting,
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

    /// By default, Delay is not fusible, as it relies on the asynchronous nature
    /// of scheduled deliveries.
    fn fusible(&self) -> bool {
        false
    }

    fn receive(
        &mut self,
        msg: LogicEvent<A, Self::Ctl>,
        ctx: &mut StreamContext<A, A, Self::Ctl>,
    ) -> Action<A, Self::Ctl> {
        match msg {
            LogicEvent::Pulled => Action::Pull,

            LogicEvent::Pushed(element) => {
                ctx.schedule_delivery("ready", self.delay, DelayMsg::Ready(element));

                self.state = State::Pushing;

                Action::None
            }

            LogicEvent::Started => Action::None,

            LogicEvent::Stopped => match self.state {
                State::Pushing => {
                    self.state = State::Stopping;

                    Action::None
                }

                State::Waiting => Action::Stop(None),

                State::Stopping => Action::Stop(None),
            },

            LogicEvent::Cancelled => Action::Cancel,

            LogicEvent::Forwarded(DelayMsg::Ready(element)) => {
                if let State::Stopping = self.state {
                    Action::PushAndStop(element, None)
                } else {
                    self.state = State::Waiting;

                    Action::Push(element)
                }
            }
        }
    }
}
