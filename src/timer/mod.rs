//! Timers are used to schedule work to be performed in the future
mod ticker;
mod timer;
mod timer_wheel;

use crate::dispatcher::{Dispatcher, ThunkWithSync};

/// A unit of work to be executed with an optional dispatcher to run it on
pub(crate) struct TimerThunk {
    thunk: ThunkWithSync,
    dispatcher: Option<Box<Dispatcher + Send + 'static>>,
}

impl TimerThunk {
    pub(crate) fn new(thunk: ThunkWithSync) -> Self {
        Self {
            thunk,
            dispatcher: None,
        }
    }
}

pub(crate) use self::timer::{Timer, TimerMsg};
