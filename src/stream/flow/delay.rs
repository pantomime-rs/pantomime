use crate::stream::{Action, Logic, LogicEvent, StreamContext};
use std::time::Duration;

pub enum DelayMsg<A> {
    Ready(A),
}

pub struct Delay {
    delay: Duration,
    pushing: bool,
    stopped: bool,
}

impl Delay {
    pub fn new(delay: Duration) -> Self {
        Self {
            delay,
            pushing: false,
            stopped: false,
        }
    }
}

impl<A: Send> Logic<A, A> for Delay {
    type Ctl = DelayMsg<A>;

    fn buffer_size(&self) -> Option<usize> {
        Some(0) // @FIXME should this be based on the delay duration?
    }

    fn receive(&mut self, msg: LogicEvent<A, Self::Ctl>, ctx: &mut StreamContext<A, A, Self::Ctl>) {
        match msg {
            LogicEvent::Pulled => {
                ctx.tell(Action::Pull);
            }

            LogicEvent::Pushed(element) => {
                self.pushing = true;

                ctx.schedule_delivery("ready", self.delay.clone(), DelayMsg::Ready(element));
            }

            LogicEvent::Started => {}

            LogicEvent::Stopped => {
                if self.pushing {
                    self.stopped = true;
                } else {
                    ctx.tell(Action::Complete(None));
                }
            }

            LogicEvent::Cancelled => {
                ctx.tell(Action::Complete(None));
            }

            LogicEvent::Forwarded(DelayMsg::Ready(element)) => {
                ctx.tell(Action::Push(element));

                if self.stopped {
                    ctx.tell(Action::Complete(None));
                }
            }
        }
    }
}
