use super::TimerThunk;
use crate::dispatcher::{Dispatcher, WorkStealingDispatcher};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

const TIMER_WHEEL_GEARS: usize = 4;
const TIMER_WHEEL_GEAR_SPROCKETS: usize = 128;

/// A `TimerWheel` consists of 4 gears, each with 128 sprockets.
///
/// The first wheel contains tasks that are to run within the next 128 ticks.
///
/// The second wheel contains tasks that are to run within the next
/// (128 * 128) ticks. On each full rotation of the first wheel, all tasks in
/// the current sprocket of the second wheel are moved into the first, and the
/// second wheel advances by one.
///
/// This behavior extends to the third and fourth wheels as well.
///
/// With the recommended tick duration of 10ms, this allows tasks to be
/// efficiently scheduled ~30 days into the future. If a task is scheduled
/// beyond that, it will stay in the last sprocket of the last wheel until
/// enough time has elapsed for it to move.
pub(crate) struct TimerWheel {
    dispatcher: Option<WorkStealingDispatcher>,
    gears: [[VecDeque<(TimerThunk, u64)>; TIMER_WHEEL_GEAR_SPROCKETS]; TIMER_WHEEL_GEARS],
    positions: [usize; TIMER_WHEEL_GEARS],
    start: Instant,
    tick_duration: u64,
    current_tick: u64,
}

impl TimerWheel {
    /// Creates a new `TimerWheel` with the specified `tick_duration`.
    #[rustfmt::skip]
    pub(crate) fn new(tick_duration: Duration) -> Self {
        // this is fine

        Self {
            dispatcher: None,
            gears: [
                [
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                ],
                [
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                ],
                [
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                ],
                [
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                    VecDeque::new(), VecDeque::new(), VecDeque::new(), VecDeque::new(),
                ],
            ],
            positions: [0, 32, 64, 96], // these are staggered to amortize moving tasks
            start: Instant::now(),
            tick_duration: Self::duration_ms(tick_duration),
            current_tick: 0
        }
    }

    /// Configure a dispatcher to run scheduled tasks on. This is used to
    /// allow separating the scheduling workload from the task workload.
    ///
    /// If one is not configured, the current thread is used instead.
    pub(crate) fn set_dispatcher(&mut self, dispatcher: Option<WorkStealingDispatcher>) {
        self.dispatcher = dispatcher;
    }

    /// Register a thunk to execute after `delay` time has passed.
    ///
    /// There is no maximum delay, but given a 10ms interval, the wheel is
    /// more efficient if all tasks are scheduled within the next ~30 days.
    pub(crate) fn register_thunk(&mut self, delay: Duration, thunk: TimerThunk) {
        let tick = self.tick_number(Self::duration_ms(self.start.elapsed() + delay));

        self.register_at_tick(thunk, tick);
    }

    /// Signal a tick from an external source. This will result
    /// in 0 or more internal ticks being processed, and all
    /// overdue jobs being run. In effect, this drives the
    /// wheel and should be called periodically, e.g. every
    /// 10 milliseconds.
    pub(crate) fn tick(&mut self) {
        let end_tick = self.tick_number(Self::duration_ms(self.start.elapsed()));

        if end_tick > self.current_tick {
            for tick in self.current_tick + 1..=end_tick {
                self.current_tick = tick;

                self.ticked();
            }
        }
    }

    /// Calculates the position of the sprocket that is `n` positions past the
    /// current position for the specified gear.
    fn next_sprocket(&self, gear: usize, n: usize) -> usize {
        (self.positions[gear] + n) % TIMER_WHEEL_GEAR_SPROCKETS
    }

    /// For the specified gear and sprocket, reregister all tasks, causing
    /// them to move "up" the wheels.
    fn redistribute(&mut self, gear: usize, sprocket: usize) {
        while let Some((thunk, tick)) = self.gears[gear][sprocket].pop_front() {
            self.register_at_tick(thunk, tick);
        }
    }

    /// Register the supplied thunk to be run at the absolute tick value. If
    /// the tick has already elapsed, it will be run on the next tick.
    fn register_at_tick(&mut self, thunk: TimerThunk, tick: u64) {
        let (gear, pos) = self.tick_position(tick);

        self.gears[gear][pos].push_back((thunk, tick));
    }

    /// Runs all of the currently scheduled thunks by popping them from the
    /// current sprocket of the first gear.
    fn run(&mut self) {
        while let Some((thunk, _)) = self.gears[0][self.positions[0]].pop_front() {
            if let Some(ref d) = thunk.dispatcher {
                d.execute(thunk.thunk);
            } else if let Some(ref d) = self.dispatcher {
                d.execute(thunk.thunk);
            } else {
                thunk.thunk.apply();
            }
        }
    }

    /// Given an absolute tick number (which is relative to the start of the
    /// `TimerWheel`), returns the gear and sprocket position it should be
    /// placed in at this point in time.
    fn tick_position(&self, tick: u64) -> (usize, usize) {
        let ticks_from_now = if tick <= self.current_tick {
            1
        } else {
            tick - self.current_tick
        };

        let mut t = ticks_from_now;
        let mut p = 0;

        loop {
            if t < TIMER_WHEEL_GEAR_SPROCKETS as u64 {
                return if p < TIMER_WHEEL_GEARS {
                    (p, self.next_sprocket(p, t as usize))
                } else {
                    // we've been tasked with scheduling
                    // something too far into the future, i.e.
                    // given default settings longer than ~1m out
                    //
                    // we'll insert it into the last gear, and
                    // given that we use the same registration
                    // algorithm to shift tasks, it'll stay there
                    // until time has progressed

                    (TIMER_WHEEL_GEARS - 1, self.positions[TIMER_WHEEL_GEARS - 1])
                };
            }

            t /= TIMER_WHEEL_GEAR_SPROCKETS as u64;
            p += 1;
        }
    }

    /// Signify that an internal tick has occurred. This may be called more
    /// than once if the wheel needs to "catch up" e.g. due to being suspended
    /// by the OS.
    fn ticked(&mut self) {
        let mut g = 0;

        while g < TIMER_WHEEL_GEARS {
            self.positions[g] = self.next_sprocket(g, 1);

            if g == 0 {
                self.run();
            } else {
                self.redistribute(g, self.positions[g]);
            }

            if self.positions[g] % TIMER_WHEEL_GEAR_SPROCKETS != 0 {
                return;
            }

            g += 1;
        }
    }

    /// Converts a millisecond value into the absolute ticker number that is
    /// used internally.
    fn tick_number(&self, ms: u64) -> u64 {
        ms / self.tick_duration
    }

    /// Converts the supplied `time::Duration` to a u64 value representing the
    /// number of milliseconds since the wheel was started.
    fn duration_ms(duration: Duration) -> u64 {
        (duration.as_secs() * 1_000) + (duration.subsec_nanos() / 1_000_000) as u64
    }
}

#[cfg(test)]
mod tests {
    use crate::dispatcher::WorkStealingDispatcher;
    use crate::testkit::*;
    use crate::timer::timer_wheel::*;
    use crate::timer::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_duration_ms() {
        for i in 0..=10000 {
            assert_eq!(TimerWheel::duration_ms(Duration::from_millis(i)), i);
        }
    }

    #[test]
    fn test_default_dispatcher() {
        let mut wheel = TimerWheel::new(Duration::from_millis(10));

        wheel.set_dispatcher(Some(WorkStealingDispatcher::new(8, true)));

        let scheduled = Arc::new(AtomicBool::new(false));
        let executed = Arc::new(AtomicBool::new(false));

        {
            let scheduled = scheduled.clone();
            let executed = executed.clone();

            wheel.register_thunk(
                Duration::from_nanos(0),
                TimerThunk::new(Box::new(move || {
                    while !scheduled.load(Ordering::SeqCst) {
                        thread::sleep(Duration::from_millis(1));
                    }

                    executed.store(true, Ordering::SeqCst);
                })),
            );
        }

        // two ticks are needed because sometimes the scheduling
        // can end up on a boundary
        wheel.current_tick += 1;
        wheel.ticked();
        wheel.current_tick += 1;
        wheel.ticked();

        // if the custom dispatcher wasn't used, we wouldn't get here as
        // we'd be in an infinite loop

        scheduled.store(true, Ordering::SeqCst);

        eventually(Duration::from_secs(10), || executed.load(Ordering::SeqCst));
    }

    #[test]
    fn test_thunk_dispatcher() {
        let mut wheel = TimerWheel::new(Duration::from_millis(10));

        let dispatcher = WorkStealingDispatcher::new(8, true);

        let scheduled = Arc::new(AtomicBool::new(false));
        let executed = Arc::new(AtomicBool::new(false));

        {
            let executed = executed.clone();
            let scheduled = scheduled.clone();

            wheel.register_thunk(
                Duration::from_nanos(0),
                TimerThunk::new(Box::new(move || {
                    while !scheduled.load(Ordering::SeqCst) {
                        thread::sleep(Duration::from_millis(1));
                    }

                    executed.store(true, Ordering::SeqCst);
                }))
                .with_dispatcher(Box::new(dispatcher)),
            );
        }

        // two ticks are needed because sometimes the scheduling
        // can end up on a boundary
        wheel.current_tick += 1;
        wheel.ticked();
        wheel.current_tick += 1;
        wheel.ticked();

        // if the custom dispatcher wasn't used, we wouldn't get here as
        // we'd be in an infinite loop

        scheduled.store(true, Ordering::SeqCst);

        eventually(Duration::from_secs(10), || executed.load(Ordering::SeqCst));
    }

    /// Tests that the timer can successfully process its entire range.
    /// This takes a long time to run, so by default it is ignored, and
    /// only run on CI.
    #[test]
    #[ignore]
    fn test_wheel_entire_range() {
        let mut wheel = TimerWheel::new(Duration::from_millis(10));

        let executed = Arc::new(AtomicBool::new(false));

        {
            let executed = executed.clone();

            wheel.register_thunk(
                Duration::from_secs(86400),
                TimerThunk::new(Box::new(move || {
                    executed.store(true, Ordering::SeqCst);
                })),
            );
        }

        for _ in 0..TIMER_WHEEL_GEAR_SPROCKETS.pow(TIMER_WHEEL_GEARS as u32) {
            wheel.current_tick += 1;
            wheel.ticked();
        }

        assert!(executed.load(Ordering::SeqCst));

        assert_eq!(wheel.positions, [0, 32, 64, 96]);
    }

    #[test]
    fn test_wheel_atomic_sum() {
        let sum = Arc::new(AtomicUsize::new(0));

        let mut wheel = TimerWheel::new(Duration::from_millis(10));
        let mut known_sum = 0;

        for i in 0..100 {
            let sum = sum.clone();

            known_sum += i;

            wheel.register_thunk(
                Duration::from_millis(20 * i as u64),
                TimerThunk::new(Box::new(move || {
                    sum.fetch_add(i, Ordering::SeqCst);
                })),
            );
        }

        for _ in 0..400 {
            wheel.current_tick += 1;
            wheel.ticked();
        }

        // assert everything ran

        assert_eq!(sum.load(Ordering::SeqCst), known_sum);
    }
}
