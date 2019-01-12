use super::timer_wheel::TimerWheel;
use super::TimerThunk;
use crate::dispatcher::WorkStealingDispatcher;
use crossbeam::channel;
use std::thread;
use std::time;

enum TickerEvent {
    Schedule {
        after: time::Duration,
        thunk: TimerThunk,
    },
    Stop,
}

pub(crate) struct ActiveTicker {
    sender: channel::Sender<TickerEvent>,
}

impl ActiveTicker {
    pub(crate) fn schedule(&self, after: time::Duration, thunk: TimerThunk) {
        if let Some(e) = self
            .sender
            .send(TickerEvent::Schedule { after, thunk })
            .err()
        {
            error!("failed to schedule event: {}", e)
        }
    }

    pub(crate) fn stop(self) {
        if let Some(e) = self.sender.send(TickerEvent::Stop).err() {
            error!("failed to stop ticker: {}", e)
        }
    }
}

pub(crate) struct Ticker {
    dispatcher: Option<WorkStealingDispatcher>,
    interval: time::Duration,
}

/// A ticker is a loop that messages the timer actor a periodic tick.
/// This is not intended to be a high precision timer -- with the default
/// configuration on a modern linux kernel, ~10ms is the target. It's
/// also perfectly acceptable for it to drift over time.
impl Ticker {
    pub(crate) fn new(interval: time::Duration) -> Self {
        Self {
            interval,
            dispatcher: None,
        }
    }

    pub(crate) fn with_dispatcher(mut self, dispatcher: WorkStealingDispatcher) -> Self {
        self.dispatcher = Some(dispatcher);
        self
    }

    pub(crate) fn run(mut self) -> ActiveTicker {
        let (sender, receiver) = channel::unbounded();

        let mut wheel = TimerWheel::new(self.interval);

        if let Some(d) = self.dispatcher.take() {
            wheel.set_dispatcher(Some(d));
        }

        thread::spawn(move || loop {
            wheel.tick();

            loop {
                match receiver.try_recv() {
                    Ok(TickerEvent::Schedule { after, thunk }) => {
                        wheel.register_thunk(after, thunk);

                        // since we process all scheduled messages, thus
                        // starving the wheel of ticks, for fairness
                        // we also induce a tick to ensure we don't
                        // faill behind if there's a large scheduling
                        // load
                        //
                        // note that the wheel will only process the
                        // tick if the interval has actually elapsed
                        // thus it is safe to call it every time we
                        // schedule something

                        wheel.tick();
                    }

                    Err(channel::TryRecvError::Empty) => {
                        break;
                    }

                    Ok(TickerEvent::Stop) => {
                        return;
                    }

                    Err(channel::TryRecvError::Disconnected) => {
                        return;
                    }
                }
            }

            thread::sleep(self.interval);
        });

        ActiveTicker { sender }
    }
}
