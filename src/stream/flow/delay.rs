use crate::stream::{Action, Logic, StreamContext};
use std::marker::PhantomData;
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

impl<A> Logic<A, A, DelayMsg<A>> for Delay
where
    A: 'static + Send,
{
    fn name(&self) -> &'static str {
        "Delay"
    }

    fn buffer_size(&self) -> Option<usize> {
        Some(0) // @FIXME should this be based on the delay duration?
    }

    fn pulled(
        &mut self,
        ctx: &mut StreamContext<A, A, DelayMsg<A>>,
    ) -> Option<Action<A, DelayMsg<A>>> {
        Some(Action::Pull)
    }

    fn pushed(
        &mut self,
        el: A,
        ctx: &mut StreamContext<A, A, DelayMsg<A>>,
    ) -> Option<Action<A, DelayMsg<A>>> {
        self.pushing = true;

        ctx.schedule_delivery("ready", self.delay.clone(), DelayMsg::Ready(el));

        None
    }

    fn stopped(
        &mut self,
        ctx: &mut StreamContext<A, A, DelayMsg<A>>,
    ) -> Option<Action<A, DelayMsg<A>>> {
        if self.pushing {
            self.stopped = true;

            None
        } else {
            Some(Action::Complete(None))
        }
    }

    fn forwarded(
        &mut self,
        msg: DelayMsg<A>,
        ctx: &mut StreamContext<A, A, DelayMsg<A>>,
    ) -> Option<Action<A, DelayMsg<A>>> {
        match msg {
            DelayMsg::Ready(el) => {
                if self.stopped {
                    ctx.tell(Action::Complete(None));
                }

                Some(Action::Push(el))
            }
        }
    }
}
