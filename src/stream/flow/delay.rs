use crate::stream::{Action, Logic, LogicEvent, StreamContext};
use std::marker::PhantomData;
use std::time::Duration;

pub enum DelayMsg<A> {
    Ready(A),
}

pub struct Delay<A> {
    delay: Duration,
    pushing: bool,
    stopped: bool,
    phantom: PhantomData<A>,
}

impl<A> Delay<A> {
    pub fn new(delay: Duration) -> Self {
        Self {
            delay,
            pushing: false,
            stopped: false,
            phantom: PhantomData,
        }
    }
}

impl<A> Logic for Delay<A>
where
    A: 'static + Send,
{
    type In = A;
    type Out = A;
    type Msg = DelayMsg<A>;

    fn buffer_size(&self) -> Option<usize> {
        Some(0) // @FIXME should this be based on the delay duration?
    }

    fn receive(
        &mut self,
        msg: LogicEvent<Self::In, Self::Msg>,
        ctx: &mut StreamContext<Self::In, Self::Out, Self::Msg, Self>,
    ) {
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
