use super::{DispatcherLogic, Thunk, Trampoline, TrampolineStep};
use crossbeam::deque::{self as deque, Injector, Steal, Stealer, Worker};
use rand::rngs::SmallRng;
use rand::{FromEntropy, Rng};
use std::cell::RefCell;
use std::sync::Arc;
use std::thread;
use std::time;

#[derive(Debug)]
enum PollSchedule {
    Aggressive(usize),
    Moderate(usize),
    Conservative(usize),
    UltraConservative,
}

/// A work stealing dispatcher that is backed by crossbeam_deque.
///
/// This powers the core messaging behind Pantomime and has been found to be
/// highly performant and scalable.
///
/// A number of worker threads are created, and jobs can be submitted to it. Each
/// thread manages its own dequeue and has stealers for each other thread.
///
/// If a worker thread submits tasks to itself, they are inserted into its
/// own deque and potentially stolen by other idle threads (internal execution).
///
/// If an external thread submits tasks to the dispatcher, it is placed in an
/// injector queue that is checked by the workers.
///
/// If a thread panics while executing, a new thread is spawned to take
/// its place.
pub struct WorkStealingDispatcher {
    injector: Arc<Injector<WorkStealingDispatcherMessage>>,
    num_workers: usize,
}

impl WorkStealingDispatcher {
    thread_local! {
        static WORKER: RefCell<Option<Worker<WorkStealingDispatcherMessage>>> = RefCell::new(None);
    }

    /// Creates a new dispatcher with the given parameters.
    ///
    /// # Arguments
    ///
    /// * `parallelism` - Number of threads to start
    /// * `task_queue_fifo` - If true, execute tasks in FIFO order. In general, LIFO is more
    ///                       performant given cache locality, but FIFO provides greater
    ///                       fairness.
    ///
    /// # Remarks
    ///
    /// This creates a new WorkStealingDispatcher that dynamically sizes a thread pool
    /// according to the number of CPUs. The number of threads in the pool is calculated
    /// as follows:
    ///
    /// threads = min(parallelism_max, max(parallelism_min, cpus * parallelism_factor))
    pub fn new(parallelism: usize, task_queue_fifo: bool) -> Self {
        let mut stealers = Vec::new();
        let mut workers = Vec::new();

        for _ in 0..parallelism {
            let w = if task_queue_fifo {
                deque::Worker::new_fifo()
            } else {
                deque::Worker::new_lifo()
            };
            let s = w.stealer();

            workers.push(w);
            stealers.push(s);
        }

        let injector = Arc::new(Injector::new());

        for i in (0..parallelism).rev() {
            let injector = injector.clone();

            let mut stealers: Vec<Stealer<WorkStealingDispatcherMessage>> =
                stealers.to_vec();

            let w = workers.remove(i);
            let _ = stealers.remove(i);

            Self::spawn_worker(w, stealers, injector);
        }

        Self {
            injector,
            num_workers: parallelism,
        }
    }

    fn spawn_worker(
        worker: Worker<WorkStealingDispatcherMessage>,
        stealers: Vec<Stealer<WorkStealingDispatcherMessage>>,
        injector: Arc<Injector<WorkStealingDispatcherMessage>>,
    ) {
        struct Panicking {
            stealers: Vec<Stealer<WorkStealingDispatcherMessage>>,
            injector: Arc<Injector<WorkStealingDispatcherMessage>>,
        }

        impl Drop for Panicking {
            fn drop(&mut self) {
                if thread::panicking() {
                    WorkStealingDispatcher::WORKER.with(|w| {
                        if let Some(worker) = w.replace(None) {
                            let mut stealers = Vec::new();

                            if let Some(stealer) = self.stealers.pop() {
                                stealers.push(stealer);
                            }

                            let injector = self.injector.clone();

                            WorkStealingDispatcher::spawn_worker(worker, stealers, injector);
                        }
                    });
                }
            }
        }

        thread::spawn(move || {
            let p = Panicking {
                stealers: stealers.clone(),
                injector: injector.clone(),
            };

            Self::WORKER.with(|w| {
                {
                    *w.borrow_mut() = Some(worker);
                }

                // we've chosen SmallRng because the main requirement
                // is to be fast, not secure
                let mut small_rng = SmallRng::from_entropy();

                let mut schedule_state = PollSchedule::Aggressive(0);

                let inline_steal_every = 1000;
                let inline_steal_injector_limit = 100;
                let inline_steal_stealers_limit = 1;

                let trampoline_limit = 10;

                let schedule_aggressive_attempts = 100;
                let schedule_moderate_attempts = 100;
                let schedule_conservative_attempts = 20;

                let schedule_moderate = time::Duration::from_micros(50);
                let schedule_conservative = time::Duration::from_millis(10);
                let schedule_ultra_conservative = time::Duration::from_millis(100);

                let l = stealers.len();

                match *w.borrow() {
                    Some(ref worker) => {
                        loop {
                            let mut work_available = true;

                            if work_available {
                                let mut i = 0;

                                loop {
                                    match worker.pop() {
                                        Some(work) => {
                                            // @TODO this is duplicated below
                                            match work {
                                                WorkStealingDispatcherMessage::Execute(thunk) => {
                                                    thunk.apply();
                                                }

                                                WorkStealingDispatcherMessage::ExecuteTrampoline(trampoline) => {
                                                    let mut i = 0;
                                                    let mut step = trampoline.step;

                                                    while let TrampolineStep::Bounce(next_step) = step {
                                                        if i == trampoline_limit {
                                                            worker.push(
                                                                WorkStealingDispatcherMessage::ExecuteTrampoline(
                                                                    Trampoline { step: TrampolineStep::Bounce(next_step) }
                                                                )
                                                            );

                                                            break;
                                                        } else {
                                                            i += 1;
                                                            step = next_step.apply().step;
                                                        }
                                                    }
                                                }

                                                WorkStealingDispatcherMessage::Shutdown => {
                                                    return;
                                                }
                                            }

                                            i += 1;

                                            if i % inline_steal_every == 0 {
                                                // we occasionally pull in some tasks that may
                                                // be sitting there from the injector and add
                                                // them to the queue. they may then be stolen
                                                // by other idle workers
                                                for _ in 0..inline_steal_injector_limit {
                                                    let mut stolen = false;

                                                    loop {
                                                        match injector.steal() {
                                                            Steal::Success(work) => {
                                                                stolen = true;
                                                                worker.push(work);
                                                                break;
                                                            }

                                                            Steal::Empty => {
                                                                break;
                                                            }

                                                            Steal::Retry => {}
                                                        }
                                                    }

                                                    if !stolen {
                                                        break;
                                                    }
                                                }

                                                if l > 0 {
                                                    for _ in 0..inline_steal_stealers_limit {
                                                        let r: usize = small_rng.gen();
                                                        let mut stolen = false;

                                                        loop {
                                                            match stealers[r % l].steal() {
                                                                Steal::Success(work) => {
                                                                    stolen = true;
                                                                    worker.push(work);
                                                                    break;
                                                                }

                                                                Steal::Empty => {
                                                                    break;
                                                                }

                                                                Steal::Retry => {}
                                                            }
                                                        }

                                                        if !stolen {
                                                            break;
                                                        }
                                                    }
                                                }

                                                // as well as attempt to steal a bit before
                                                // working on our tasks some more
                                            }
                                        }

                                        None => {
                                            // our worker is empty, and since only this thread
                                            // can insert items into it, it's time to start
                                            // stealing

                                            work_available = false;
                                            break;
                                        }
                                    }
                                }
                            }

                            // next, steal as much as we can from the injector

                            loop {
                                match injector.steal() {
                                    Steal::Success(work) => {
                                        worker.push(work);
                                        work_available = true;
                                        break;
                                    }

                                    Steal::Empty => {
                                        break;
                                    }

                                    Steal::Retry => {}
                                }
                            }

                            // our worker is either empty, or reached max for fairness,
                            // so now let's try to steal from people randomly

                            if !work_available && l > 0 {
                                let r: usize = small_rng.gen();

                                loop {
                                    match stealers[r % l].steal() {
                                        Steal::Success(work) => {
                                            // @TODO this is duplicated above
                                            match work {
                                                WorkStealingDispatcherMessage::Execute(thunk) => {
                                                    thunk.apply();
                                                }

                                                WorkStealingDispatcherMessage::ExecuteTrampoline(trampoline) => {
                                                    let mut i = 0;
                                                    let mut step = trampoline.step;

                                                    while let TrampolineStep::Bounce(next_step) = step {
                                                        if i == trampoline_limit {
                                                            worker.push(
                                                                WorkStealingDispatcherMessage::ExecuteTrampoline(
                                                                    Trampoline { step: TrampolineStep::Bounce(next_step) }
                                                                )
                                                            );

                                                            break;
                                                        } else {
                                                            i += 1;
                                                            step = next_step.apply().step;
                                                        }
                                                    }
                                                }

                                                WorkStealingDispatcherMessage::Shutdown => {
                                                    return;
                                                }
                                            }

                                            work_available = true;
                                            break;
                                        }

                                        Steal::Empty => {
                                            break;
                                        }

                                        Steal::Retry => {}
                                    }
                                }
                            }

                            // if any work was performed, immediately continue, otherwise'
                            // we go into a backoff to avoid spinning

                            if !work_available {
                                match schedule_state {
                                    PollSchedule::Aggressive(n)
                                        if n < schedule_aggressive_attempts =>
                                    {
                                        schedule_state = PollSchedule::Aggressive(n + 1);
                                        thread::yield_now();
                                    }

                                    PollSchedule::Aggressive(_) => {
                                        schedule_state = PollSchedule::Moderate(1);
                                        thread::park_timeout(schedule_moderate);
                                    }

                                    PollSchedule::Moderate(n) if n < schedule_moderate_attempts => {
                                        schedule_state = PollSchedule::Moderate(n + 1);
                                        thread::park_timeout(schedule_moderate);
                                    }

                                    PollSchedule::Moderate(_) => {
                                        schedule_state = PollSchedule::Conservative(1);
                                        thread::park_timeout(schedule_conservative);
                                    }

                                    PollSchedule::Conservative(n)
                                        if n < schedule_conservative_attempts =>
                                    {
                                        schedule_state = PollSchedule::Conservative(n + 1);
                                        thread::park_timeout(schedule_conservative);
                                    }

                                    PollSchedule::Conservative(_) => {
                                        schedule_state = PollSchedule::UltraConservative;
                                        thread::park_timeout(schedule_ultra_conservative);
                                    }

                                    PollSchedule::UltraConservative => {
                                        thread::park_timeout(schedule_ultra_conservative);
                                    }
                                }
                            } else {
                                schedule_state = PollSchedule::Aggressive(0);
                            }
                        }
                    }

                    None => {
                        panic!("pantomime bug: cannot initialize WorkStealingDispatcher thread");
                    }
                }
            });

            drop(p);
        });

        // @TODO unset the thread local storage here
    }
}

enum WorkStealingDispatcherMessage {
    Execute(Thunk),
    ExecuteTrampoline(Trampoline),
    Shutdown,
}

impl DispatcherLogic for WorkStealingDispatcher {
    fn clone_box(&self) -> Box<DispatcherLogic + 'static + Send + Sync> {
        Box::new(Self {
            injector: self.injector.clone(),
            num_workers: self.num_workers,
        })
    }

    fn execute(&self, thunk: Thunk) {
        Self::WORKER.with(|w| match *w.borrow() {
            Some(ref worker) => {
                worker.push(WorkStealingDispatcherMessage::Execute(thunk));
            }

            None => {
                self.injector
                    .push(WorkStealingDispatcherMessage::Execute(thunk));
            }
        });
    }

    fn execute_trampoline(&self, trampoline: Trampoline) {
        Self::WORKER.with(|w| match *w.borrow() {
            Some(ref worker) => {
                worker.push(WorkStealingDispatcherMessage::ExecuteTrampoline(trampoline));
            }

            None => {
                self.injector
                    .push(WorkStealingDispatcherMessage::ExecuteTrampoline(trampoline));
            }
        });
    }

    fn shutdown(self: Box<Self>) {
        for _ in 0..self.num_workers {
            self.injector.push(WorkStealingDispatcherMessage::Shutdown);
        }
    }

    fn throughput(&self) -> usize {
        10
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn simple_test_fifo() {
        let counter = Arc::new(AtomicUsize::new(0));

        let dispatcher = WorkStealingDispatcher::new(8, false);

        for _ in 0..100 {
            let counter = counter.clone();

            dispatcher.execute(Box::new(move || {
                counter.fetch_add(10, Ordering::SeqCst);
            }));
        }

        eventually(Duration::from_millis(3000), move || {
            counter.load(Ordering::SeqCst) == 1000
        });
    }

    #[test]
    fn simple_test_lifo() {
        let counter = Arc::new(AtomicUsize::new(0));

        let dispatcher = WorkStealingDispatcher::new(8, false);

        for _ in 0..100 {
            let counter = counter.clone();

            dispatcher.execute(Box::new(move || {
                counter.fetch_add(10, Ordering::SeqCst);
            }));
        }

        eventually(Duration::from_millis(3000), move || {
            counter.load(Ordering::SeqCst) == 1000
        });
    }

    #[test]
    fn test_panic() {
        let counter = Arc::new(AtomicUsize::new(0));

        let dispatcher = WorkStealingDispatcher::new(8, false);

        for _ in 0..16 {
            dispatcher.execute(Box::new(move || {
                panic!();
            }));
        }

        for _ in 0..16 {
            let counter = counter.clone();

            dispatcher.execute(Box::new(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            }));
        }

        eventually(Duration::from_millis(3000), move || {
            counter.load(Ordering::SeqCst) == 16
        });
    }
}
