use super::*;
use crate::dispatcher::{
    Dispatcher, DispatcherLogic, SingleThreadedDispatcher, WorkStealingDispatcher,
};
use crate::io::{IoCoordinator, IoCoordinatorMsg};
use crate::mailbox::{
    CrossbeamChannelMailboxLogic, CrossbeamSegQueueMailboxLogic, VecDequeMailboxLogic,
};
use crossbeam::atomic::AtomicCell;
use crossbeam::channel;
use fern::colors::{Color, ColoredLevelConfig};
use std::collections::{HashMap, VecDeque};
use std::io::{Error, ErrorKind};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Once};
use std::{cmp, time, usize};

static INITIALIZE_ONCE: Once = Once::new();

/// The top level type that contains references for running actors.
///
/// Among other things, each `ActorSystem` is comprised of a number of
/// shards that actors are assigned to, as well as a global dispatcher
/// for scheduling and executing actors.
///
/// Effectively, an `ActorSystem` is a collection of references to the
/// various datastructures that support the execution of its actors.
#[derive(Default)]
pub struct ActorSystem {
    config: Option<Config>,
}

pub struct ActorSystemContext {
    inner: Arc<ActorSystemContextInner>,
}

impl ActorSystemContext {
    pub fn config(&self) -> &ActorSystemConfig {
        &self.inner.config
    }

    pub fn dispatcher(&self) -> &Dispatcher {
        &self.inner.dispatcher
    }

    pub fn stop(&self) {
        self.tell_reaper_monitor(ReaperMsg::Stop);
    }

    /// Schedule a function to be invoked after the timeout has elapsed.
    ///
    /// The supplied function will be executed on the system dispatcher.
    ///
    /// Internally, this uses a wheel-based timer that by default can
    /// schedule tasks upto a granularity of 10 milliseconds (by default).
    pub fn schedule_thunk<F: FnOnce()>(&self, timeout: std::time::Duration, f: F)
    where
        F: 'static + Send + Sync, // @TODO why Sync?
    {
        if let Some(ref timer_ref) = self.inner.timer_ref {
            timer_ref.tell(TimerMsg::Schedule {
                after: timeout,
                thunk: TimerThunk::new(Box::new(f)),
            });
        } else {
            panic!("pantomime bug: schedule_thunk called on internal context");
        }
    }

    pub(crate) fn io_coordinator_ref(&self) -> Option<&ActorRef<IoCoordinatorMsg>> {
        self.inner.io_coordinator_ref.as_ref()
    }

    pub(crate) fn spawn<M, A: Actor<M>>(&self, actor: A) -> ActorRef<M>
    where
        A: 'static + Send,
        M: 'static + Send,
    {
        /////////////////////////////////////////////////////////////////////////////////////////
        // NOTE: this is quite similiar to ActorContext::spawn, and changes should be mirrored //
        /////////////////////////////////////////////////////////////////////////////////////////

        use crate::actor::actor_ref::*;

        let empty_ref = ActorRef::empty();

        let dispatcher = actor
            .config_dispatcher(&self)
            .unwrap_or_else(|| self.new_actor_dispatcher());

        let mailbox = actor
            .config_mailbox(&self)
            .unwrap_or_else(|| self.new_actor_mailbox());

        let throughput = actor
            .config_throughput(&self)
            .unwrap_or(self.inner.config.default_actor_throughput);

        let mut spawned_actor = SpawnedActor {
            actor: Box::new(actor),
            context: ActorContext {
                actor_ref: empty_ref.clone(),
                children: HashMap::new(),
                deliveries: HashMap::new(),
                dispatcher: dispatcher.clone(),
                pending_stop: None,
                state: SpawnedActorState::Active,
                system_context: self.clone(),
            },
            dispatcher,
            execution_state: Arc::new(AtomicCell::new(SpawnedActorExecutionState::Running)),
            mailbox,
            parent_ref: empty_ref.system_ref(),
            stash: VecDeque::new(),
            throughput,
            watchers: Vec::new(),
        };

        let actor_ref = ActorRef {
            inner: Arc::new(Box::new(ActorRefCell {
                id: self.new_actor_id(),
                state: spawned_actor.execution_state.clone(),
                mailbox_appender: spawned_actor.mailbox.appender(),
            })),
        };

        spawned_actor.context.actor_ref = actor_ref.clone();

        spawned_actor
            .execution_state
            .clone()
            .store(SpawnedActorExecutionState::Idle(Box::new(spawned_actor)));

        actor_ref.tell_system(SystemMsg::Signaled(Signal::Started));

        actor_ref
    }

    pub(in crate::actor) fn new_actor_id(&self) -> usize {
        self.inner.next_actor_id.fetch_add(1, Ordering::SeqCst)
    }

    pub(in crate::actor) fn new_actor_dispatcher(&self) -> Dispatcher {
        self.inner.dispatcher.clone()
    }

    pub(in crate::actor) fn new_actor_mailbox<Msg>(&self) -> Mailbox<Msg>
    where
        Msg: 'static + Send,
    {
        let mailbox_logic: Box<MailboxLogic<Msg> + Send + Sync> =
            match self.inner.config.default_mailbox_logic.as_str() {
                "crossbeam-seg-queue" => Box::new(CrossbeamSegQueueMailboxLogic::new()),
                "crossbeam-channel" => Box::new(CrossbeamChannelMailboxLogic::new()),
                "vecdeque" => Box::new(VecDequeMailboxLogic::new()),
                other => {
                    panic!(format!("pantomime bug: unknown mailbox logic {}", other));
                }
            };

        Mailbox::new_boxed(mailbox_logic)
    }

    pub(in crate::actor) fn tell_reaper_monitor(&self, msg: ReaperMsg) {
        let _ = self.inner.sender.send(ActorSystemMsg::Forward(msg));
    }

    pub(in crate::actor) fn timer_ref(&self) -> Option<&ActorRef<TimerMsg>> {
        self.inner.timer_ref.as_ref()
    }

    pub fn done(&self) {
        let _ = self.inner.sender.send(ActorSystemMsg::Done);
    }
}

enum ActorSystemMsg {
    Forward(ReaperMsg),
    Done,
}

impl Clone for ActorSystemContext {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

struct ActorSystemContextInner {
    config: ActorSystemConfig,
    dispatcher: Dispatcher,
    next_actor_id: AtomicUsize,
    sender: channel::Sender<ActorSystemMsg>,
    timer_ref: Option<ActorRef<TimerMsg>>,
    io_coordinator_ref: Option<ActorRef<IoCoordinatorMsg>>,
}

pub struct ActiveActorSystem {
    context: ActorSystemContext,
    receiver: channel::Receiver<ActorSystemMsg>,
}

impl ActiveActorSystem {
    fn join(self, failed: &AtomicBool, reaper_monitor_ref: &ActorRef<ReaperMsg>) {
        let context = self.context.clone();

        #[allow(unused_mut)]
        let mut exit_code = 0;

        #[cfg(all(feature = "posix-signals-support", target_family = "unix"))]
        use signal_hook::iterator::Signals;

        #[cfg(all(feature = "posix-signals-support", target_family = "unix"))]
        let signals = Signals::new(&self.context.config().posix_signals)
            .expect("pantomime bug: cannot setup POSIX signal handling");

        // this provides a mechanism to occasionally check for signals that
        // have arrived (if posix signal support is enabled). alternatively,
        // an extra thread could be spawned, but that doesn't seem worth it
        // at the moment
        //
        // it's not currently configurable as there shouldn't really be
        // a desire to do so. if lower resolution latency is required,
        // please open an issue explaining the use-case

        let time_duration = time::Duration::from_millis(100);

        loop {
            #[cfg(all(feature = "posix-signals-support", target_family = "unix"))]
            for signal in signals.pending() {
                reaper_monitor_ref.tell(ReaperMsg::ReceivedPosixSignal(signal));

                if context.config().posix_shutdown_signals.contains(&signal) {
                    reaper_monitor_ref.tell(ReaperMsg::Stop);
                    exit_code = 128 + signal;
                }
            }

            match self.receiver.recv_timeout(time_duration) {
                Err(channel::RecvTimeoutError::Timeout) => {
                    // nothing to do
                }

                Ok(ActorSystemMsg::Forward(msg)) => {
                    reaper_monitor_ref.tell(msg);
                }

                Ok(ActorSystemMsg::Done) | Err(channel::RecvTimeoutError::Disconnected) => {
                    // our reaper has indicated that all non-system actors have stopped
                    // so we're done. we have to stop the timer as it's a special case and
                    // has its own thread that it needs to stop. in general, if other
                    // "special" actors are added in the future, they should be stopped here
                    // too. stopping is on a best-effort basis but should usually succeed

                    if let Some(ref timer_ref) = self.context.timer_ref() {
                        timer_ref.tell(TimerMsg::Stop);
                    }

                    if failed.load(Ordering::Acquire) && exit_code == 0 {
                        exit_code = 1;
                    }

                    break;
                }
            }
        }

        #[cfg(test)]
        let _ = exit_code;

        #[cfg(not(test))]
        let _ = {
            if self.context.config().process_exit {
                ::std::process::exit(exit_code);
            }

            0
        };
    }

    pub fn spawn<M: 'static + Send, A: 'static + Send>(&mut self, actor: A) -> ActorRef<M>
    where
        A: Actor<M>,
    {
        self.context.spawn(actor)
    }
}

pub(in crate::actor) enum ReaperMsg {
    Stop,

    #[cfg(all(feature = "posix-signals-support", target_family = "unix"))]
    ReceivedPosixSignal(i32),

    #[cfg(all(feature = "posix-signals-support", target_family = "unix"))]
    WatchPosixSignals(SystemActorRef),
}

struct ReaperMonitor<M, A: Actor<M>>
where
    M: 'static + Send,
    A: 'static + Send,
{
    actor: Option<A>,
    failed: Arc<AtomicBool>,
    reaper_id: usize,

    #[cfg(all(feature = "posix-signals-support", target_family = "unix"))]
    posix_signals_watchers: HashMap<usize, SystemActorRef>,

    phantom: PhantomData<M>,
}

impl<M, A: Actor<M>> ReaperMonitor<M, A>
where
    M: 'static + Send,
    A: 'static + Send,
{
    fn new(actor: A, failed: &Arc<AtomicBool>) -> Self {
        Self {
            actor: Some(actor),
            failed: failed.clone(),
            reaper_id: 0,

            #[cfg(all(feature = "posix-signals-support", target_family = "unix"))]
            posix_signals_watchers: HashMap::new(),

            phantom: PhantomData,
        }
    }
}

impl<M, A: Actor<M>> Actor<ReaperMsg> for ReaperMonitor<M, A>
where
    M: 'static + Send,
    A: 'static + Send,
{
    fn receive(&mut self, msg: ReaperMsg, ctx: &mut ActorContext<ReaperMsg>) {
        match msg {
            ReaperMsg::Stop => {
                ctx.stop();
            }

            #[cfg(all(feature = "posix-signals-support", target_family = "unix"))]
            ReaperMsg::ReceivedPosixSignal(signal) => {
                for watcher in self.posix_signals_watchers.values() {
                    watcher.tell_system(SystemMsg::Signaled(Signal::PosixSignal(signal)));
                }
            }

            #[cfg(all(feature = "posix-signals-support", target_family = "unix"))]
            ReaperMsg::WatchPosixSignals(system_ref) => {
                ctx.watch_system(&system_ref);

                self.posix_signals_watchers
                    .insert(system_ref.id(), system_ref);
            }
        }
    }

    fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<ReaperMsg>) {
        match signal {
            Signal::Started => {
                let actor_ref = ctx.spawn(
                    self.actor
                        .take()
                        .expect("pantomime bug: ReaperMonitor cannot get actor"),
                );

                self.reaper_id = actor_ref.id();

                ctx.watch(&actor_ref);
            }

            Signal::ActorStopped(actor, StopReason::Failed) => {
                let actor_id = actor.id();

                #[cfg(all(feature = "posix-signals-support", target_family = "unix"))]
                self.posix_signals_watchers.remove(&actor_id);

                if actor_id == self.reaper_id {
                    self.failed.store(true, Ordering::Release);
                    ctx.system_context().done();
                }
            }

            Signal::ActorStopped(actor, _) => {
                let actor_id = actor.id();

                #[cfg(all(feature = "posix-signals-support", target_family = "unix"))]
                self.posix_signals_watchers.remove(&actor_id);

                if actor_id == self.reaper_id {
                    ctx.system_context().done();
                }
            }

            _ => {}
        }
    }
}

impl ActorSystem {
    pub fn new() -> Self {
        Self { config: None }
    }

    pub fn with_config(mut self, config: &Config) -> Self {
        self.config = Some(config.clone());
        self
    }

    /// Spawns an ActorSystem on the current thread. A reaper actor must
    /// be provided whose job is to spawn other actors and optionally
    /// watch them.
    ///
    /// If the test configuration is active, this returns a result indicating
    /// whether the reaper terminated successfully.
    ///
    /// Otherwise, if Pantomime is configured to call exit, this function
    /// will never return and will instead exit the process with a relevant
    /// status code.
    ///
    /// A suggested pattern is to spawn and watch all of your top level actors
    /// with the reaper, and react to any termination signals of those actors
    /// via the `receive_signal` method.
    pub fn spawn<M: 'static + Send, A: 'static + Send>(mut self, actor: A) -> Result<(), Error>
    where
        A: Actor<M>,
    {
        INITIALIZE_ONCE.call_once(|| {
            if let Some(log_err) = Self::setup_logger().err() {
                panic!("pantomime bug: cannot initialize logger; {}", log_err);
            }
        });

        let config = ActorSystemConfig::new(&self.config.take().unwrap_or_default())?;
        let _ = self.validate_default_dispatcher_logic(&config);
        let _ = self.validate_default_mailbox_logic(&config);
        let failed = Arc::new(AtomicBool::new(false));

        let dispatcher_logic: Box<DispatcherLogic + Sync + Send> =
            match config.default_dispatcher_logic.as_ref() {
                "work-stealing" => {
                    let default_dispatcher_parallelism = cmp::min(
                        config.default_dispatcher_logic_work_stealing_parallelism_max,
                        cmp::max(
                            config.default_dispatcher_logic_work_stealing_parallelism_min,
                            (config.num_cpus as f32
                                * config.default_dispatcher_logic_work_stealing_parallelism_factor)
                                as usize,
                        ),
                    );

                    Box::new(WorkStealingDispatcher::new(
                        default_dispatcher_parallelism,
                        config.default_dispatcher_logic_work_stealing_task_queue_fifo,
                    ))
                }

                "single-threaded" => Box::new(SingleThreadedDispatcher::new()),

                other => {
                    panic!(format!("pantomime bug: unknown dispatcher logic {}", other));
                }
            };

        let dispatcher = Dispatcher::new_boxed(dispatcher_logic);

        let (sender, receiver) = channel::unbounded();

        let ticker_interval = time::Duration::from_millis(config.ticker_interval_ms);

        let internal_context = ActorSystemContext {
            inner: Arc::new(ActorSystemContextInner {
                config: config.clone(),
                dispatcher: dispatcher.clone(),
                next_actor_id: AtomicUsize::new(0),
                sender: sender.clone(),
                timer_ref: None,
                io_coordinator_ref: None,
            }),
        };

        let poller = crate::io::Poller::new();
        let poll = poller.poll.clone();
        let io_coordinator_ref = internal_context.spawn(IoCoordinator::new(poll));
        let timer_ref = internal_context.spawn(Timer::new(ticker_interval));

        poller.run(&io_coordinator_ref);

        let context = ActorSystemContext {
            inner: Arc::new(ActorSystemContextInner {
                config,
                dispatcher,
                next_actor_id: AtomicUsize::new(100), // we reserve < 100 as an internal id, i.e. special. in practice, we currently only need 2
                sender,
                timer_ref: Some(timer_ref),
                io_coordinator_ref: Some(io_coordinator_ref),
            }),
        };

        let reaper_monitor_ref = context.spawn(ReaperMonitor::new(actor, &failed));

        if context.config().log_config_on_start {
            info!("configuration: {:?}", context.config());
        }

        let system = ActiveActorSystem { context, receiver };

        system.join(&failed, &reaper_monitor_ref);

        if failed.load(Ordering::Acquire) {
            Err(Error::new(ErrorKind::Other, "TODO"))
        } else {
            Ok(())
        }
    }

    fn validate_default_dispatcher_logic(&self, config: &ActorSystemConfig) -> Result<(), Error> {
        match config.default_dispatcher_logic.as_str() {
            "work-stealing" => Ok(()),
            "single-threaded" => Ok(()),
            other => Err(Error::new(
                ErrorKind::Other,
                format!("unknown dispatcher logic: {}", other),
            )),
        }
    }

    fn validate_default_mailbox_logic(&self, config: &ActorSystemConfig) -> Result<(), Error> {
        match config.default_mailbox_logic.as_str() {
            "crossbeam-channel" => Ok(()),
            "crossbeam-seg-queue" => Ok(()),
            "vecdeque" => Ok(()),
            other => Err(Error::new(
                ErrorKind::Other,
                format!("unknown mailbox logic: {}", other),
            )),
        }
    }

    fn setup_logger() -> Result<(), fern::InitError> {
        let mut colors = ColoredLevelConfig::new();
        colors.info = Color::Blue;
        let tty = atty::is(atty::Stream::Stderr);

        fern::Dispatch::new()
            .format(move |out, message, record| {
                if tty {
                    out.finish(format_args!(
                        "{} {} [{}] {}",
                        chrono::Local::now().to_rfc3339(),
                        colors.color(record.level()),
                        record.target(),
                        message
                    ))
                } else {
                    out.finish(format_args!(
                        "{} {} [{}] {}",
                        chrono::Local::now().to_rfc3339(),
                        record.level(),
                        record.target(),
                        message
                    ))
                }
            })
            .level(log::LevelFilter::Info)
            .chain(std::io::stderr())
            .apply()?;
        Ok(())
    }
}
