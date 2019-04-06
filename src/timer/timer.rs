use super::ticker::{ActiveTicker, Ticker};
use super::TimerThunk;
use crate::actor::*;
use std::time;

pub(crate) enum TimerMsg {
    Schedule {
        after: time::Duration,
        thunk: TimerThunk,
    },
    Stop,
}

pub(crate) struct Timer {
    tick_interval: time::Duration,
    ticker: Option<ActiveTicker>,
}

impl Timer {
    pub(crate) fn new(tick_interval: time::Duration) -> Self {
        Self {
            tick_interval,
            ticker: None,
        }
    }
}

impl Actor<TimerMsg> for Timer {
    fn receive(&mut self, message: TimerMsg, context: &mut ActorContext<TimerMsg>) {
        match message {
            TimerMsg::Schedule { after, thunk } => {
                if self.ticker.is_none() {
                    self.ticker = Some(
                        Ticker::new(self.tick_interval)
                            .with_dispatcher(context.system_context().dispatcher().clone())
                            .run(),
                    );
                }

                if let Some(ref ticker) = self.ticker {
                    ticker.schedule(after, thunk);
                } else {
                    panic!("pantomime bug: couldn't reference timer even though logically it's initialized");
                }
            }

            TimerMsg::Stop => {
                if let Some(ticker) = self.ticker.take() {
                    ticker.stop();
                }
            }
        }
    }
}
