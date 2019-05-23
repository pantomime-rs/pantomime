use self::actor_watcher::{ActorWatcher, ActorWatcherMessage};
use super::*;
use crate::dispatcher::{Dispatcher, WorkStealingDispatcher};
use crate::io::{IoCoordinator, IoCoordinatorMsg};
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
    config: Arc<ActorSystemConfig>,
    dispatcher: Dispatcher,
    next_actor_id: Arc<AtomicUsize>,
    sender: channel::Sender<ActorSystemMsg>,
    timer_ref: Option<ActorRef<TimerMsg>>,
    io_coordinator_ref: Option<ActorRef<IoCoordinatorMsg>>,
    watcher_ref: Option<ActorRef<ActorWatcherMessage>>,
}

impl ActorSystemContext {
    pub fn dispatcher(&self) -> &Dispatcher {
        &self.dispatcher
    }

    pub fn stop(&self) {
        let _ = self.sender.send(ActorSystemMsg::Stop);
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
        if let Some(ref timer_ref) = self.timer_ref() {
            timer_ref.tell(TimerMsg::Schedule {
                after: timeout,
                thunk: TimerThunk::new(Box::new(f)),
            });
        } else {
            panic!("pantomime bug: schedule_thunk called on internal context");
        }
    }

    pub(crate) fn io_coordinator_ref(&self) -> Option<&ActorRef<IoCoordinatorMsg>> {
        self.io_coordinator_ref.as_ref()
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

        let mut spawned_actor = SpawnedActor {
            actor: Box::new(actor),
            context: ActorContext {
                actor_ref: empty_ref.clone(),
                children: HashMap::new(),
                deliveries: HashMap::new(),
                dispatcher: dispatcher.clone(),
                system_context: self.clone(),
            },
            dispatcher,
            execution_state: Arc::new(AtomicCell::new(SpawnedActorExecutionState::Running)),
            mailbox,
            parent_ref: empty_ref.system_ref(),
            stash: VecDeque::new(),
            state: SpawnedActorState::Spawned,
        };

        let actor_ref = ActorRef {
            inner: Arc::new(Box::new(ActorRefCell {
                id: self.new_actor_id(),
                state: spawned_actor.execution_state.clone(),
                mailbox_appender: spawned_actor.mailbox.appender(),
            })),
        };

        spawned_actor.context.actor_ref = actor_ref.clone();

        if let Some(ref watcher_ref) = self.watcher_ref() {
            watcher_ref.tell(ActorWatcherMessage::Started(
                actor_ref.id(),
                actor_ref.system_ref(),
                true,
            ));
        } else {
            // these are internal actors, so they cannot be watched
            spawned_actor.state = SpawnedActorState::Active;
        }

        spawned_actor
            .execution_state
            .clone()
            .store(SpawnedActorExecutionState::Idle(Box::new(spawned_actor)));

        actor_ref
    }

    pub(in crate::actor) fn new_actor_id(&self) -> usize {
        self.next_actor_id.fetch_add(1, Ordering::SeqCst)
    }

    pub(in crate::actor) fn new_actor_dispatcher(&self) -> Dispatcher {
        self.dispatcher.clone()
    }

    pub(in crate::actor) fn new_actor_mailbox<Msg>(&self) -> Mailbox<Msg>
    where
        Msg: 'static + Send,
    {
        Mailbox::new(CrossbeamSegQueueMailboxLogic::new())
    }

    pub(in crate::actor) fn timer_ref(&self) -> Option<&ActorRef<TimerMsg>> {
        self.timer_ref.as_ref()
    }

    pub(in crate::actor) fn watcher_ref(&self) -> Option<&ActorRef<ActorWatcherMessage>> {
        self.watcher_ref.as_ref()
    }
}

impl Clone for ActorSystemContext {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            dispatcher: self.dispatcher.clone(),
            next_actor_id: self.next_actor_id.clone(),
            sender: self.sender.clone(),
            timer_ref: self.timer_ref.clone(),
            io_coordinator_ref: self.io_coordinator_ref.clone(),
            watcher_ref: self.watcher_ref.clone(),
        }
    }
}

// @TODO signaled
enum ActorSystemMsg {
    Stop,
    Done,
}

pub struct ActiveActorSystem {
    context: ActorSystemContext,
    receiver: channel::Receiver<ActorSystemMsg>,
    sender: channel::Sender<ActorSystemMsg>,
}

impl ActiveActorSystem {
    /// Process system messages, taking over the thread in the process.
    ///
    /// For most applications, this will be the final call in main.
    ///
    /// This will return once the actor system has stopped, which happens
    /// after a request to stop has occurred, and all root actors
    /// have terminated.
    ///
    /// If your application requires some special processing on main thread
    /// (e.g. some graphics libraries), this should be called in its own
    /// thread instead.
    pub fn join(self) {
        #[allow(unused_mut)]
        let mut exit_code = 0;

        #[cfg(feature = "posix-signals-support")]
        use signal_hook::iterator::Signals;

        #[cfg(feature = "posix-signals-support")]
        let signals = Signals::new(&self.context.config.posix_signals)
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
            #[cfg(feature = "posix-signals-support")]
            for signal in signals.pending() {
                if let Some(ref watcher_ref) = self.context.watcher_ref() {
                    watcher_ref.tell(ActorWatcherMessage::ReceivedPosixSignal(signal));
                }

                if self.context.config.posix_shutdown_signals.contains(&signal) {
                    let _ = self.sender.send(ActorSystemMsg::Stop);
                    exit_code = 128 + signal;
                }
            }

            match self.receiver.recv_timeout(time_duration) {
                Err(channel::RecvTimeoutError::Timeout) => {
                    // nothing to do
                }

                Err(channel::RecvTimeoutError::Disconnected) => {
                    break;
                }

                Ok(ActorSystemMsg::Stop) => {
                    if let Some(ref watcher_ref) = self.context.watcher_ref() {
                        let sender = self.sender.clone();

                        watcher_ref.tell(ActorWatcherMessage::StopSystem(Box::new(move || {
                            if let Err(e) = sender.send(ActorSystemMsg::Done) {
                                // there's nothing to do here -- the receiver has been dropped
                                // since the message was sent to the watcher

                                drop(e);
                            }
                        })));
                    } else {
                        panic!(
                            "pantomime bug: received stop request for actor without watcher_ref"
                        );
                    }
                }

                Ok(ActorSystemMsg::Done) => {
                    // our watcher has indicated that all non-system actors have stopped,
                    // so we're done. we have to stop the timer as it's a special case and
                    // has its own thread that it needs to stop. in general, if other
                    // "special" actors are added in the future, they should be stopped here
                    // too. stopping is on a best-effort basis but should usually succeed

                    if let Some(ref timer_ref) = self.context.timer_ref() {
                        timer_ref.tell(TimerMsg::Stop);
                    }

                    break;
                }
            }
        }

        #[cfg(test)]
        let _ = exit_code;

        #[cfg(not(test))]
        let _ = {
            if self.context.config.process_exit {
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

struct ReaperMonitor<M, A: Actor<M>>
where
    M: 'static + Send,
    A: 'static + Send,
{
    actor: Option<A>,
    failed: Arc<AtomicBool>,
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
            phantom: PhantomData,
        }
    }
}

impl<M, A: Actor<M>> Actor<()> for ReaperMonitor<M, A>
where
    M: 'static + Send,
    A: 'static + Send,
{
    fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}

    fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
        match signal {
            Signal::Started => {
                let actor_ref = ctx.spawn(
                    self.actor
                        .take()
                        .expect("pantomime bug: ReaperMonitor cannot get actor"),
                );

                ctx.watch(&actor_ref);
            }

            Signal::ActorStopped(_, StopReason::Failed) => {
                self.failed.store(true, Ordering::Release);
                ctx.system_context().stop();
            }

            Signal::ActorStopped(_, _) => {
                ctx.system_context().stop();
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
    /// Returns whether the reaper terminated successfully or not.
    ///
    /// A suggested pattern is to spawn and watch all of your top level actors
    /// with the reaper, and react to any termination signals of those actors
    /// via the `receive_signal` method.
    pub fn spawn<M: 'static + Send, A: 'static + Send>(mut self, actor: A) -> Result<(), Error>
    where
        A: Actor<M>,
    {
        let config = ActorSystemConfig::new(&self.config.take().unwrap_or_default())?;
        let mut system = self.start(config);

        let failed = Arc::new(AtomicBool::new(false));

        system.spawn(ReaperMonitor::new(actor, &failed));

        system.join();

        if failed.load(Ordering::Acquire) {
            Err(Error::new(ErrorKind::Other, "TODO"))
        } else {
            Ok(())
        }
    }

    fn start(self, config: ActorSystemConfig) -> ActiveActorSystem {
        INITIALIZE_ONCE.call_once(|| {
            if let Some(log_err) = Self::setup_logger().err() {
                panic!("pantomime bug: cannot initialize logger; {}", log_err);
            }
        });

        let config = Arc::new(config);

        let default_dispatcher_parallelism = cmp::min(
            config.default_dispatcher_parallelism_max,
            cmp::max(
                config.default_dispatcher_parallelism_min,
                (config.num_cpus as f32 * config.default_dispatcher_parallelism_factor) as usize,
            ),
        );

        let dispatcher_logic = WorkStealingDispatcher::new(
            default_dispatcher_parallelism,
            config.default_dispatcher_task_queue_fifo,
        );

        let dispatcher = Dispatcher::new(dispatcher_logic);

        let (sender, receiver) = channel::unbounded();

        let internal_system_context = ActorSystemContext {
            config: config.clone(),
            dispatcher: dispatcher.clone(),
            next_actor_id: Arc::new(AtomicUsize::new(1)),
            sender: sender.clone(),
            timer_ref: None,
            io_coordinator_ref: None,
            watcher_ref: None,
        };

        let ticker_interval = time::Duration::from_millis(config.ticker_interval_ms);

        let timer_ref = internal_system_context.spawn(Timer::new(ticker_interval));

        let actor_watcher_ref = internal_system_context.spawn(ActorWatcher::new());

        let poller = crate::io::Poller::new();

        let poll = poller.poll.clone();

        let io_coordinator_ref = internal_system_context.spawn(IoCoordinator::new(poll));

        poller.run(&io_coordinator_ref);

        let context = ActorSystemContext {
            config,
            dispatcher,
            next_actor_id: Arc::new(AtomicUsize::new(100)), // we reserve < 100 as an internal id, i.e. special. in practice, we currently only need 2
            sender: sender.clone(),
            timer_ref: Some(timer_ref.clone()),
            io_coordinator_ref: Some(io_coordinator_ref),
            watcher_ref: Some(actor_watcher_ref),
        };

        if context.config.log_config_on_start {
            info!("configuration: {:?}", context.config);
        }

        ActiveActorSystem {
            context,
            receiver,
            sender,
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
