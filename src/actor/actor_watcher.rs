use crate::actor::*;
use crate::dispatcher::ThunkWithSync;
use std::collections::{HashMap, HashSet};

pub(in crate::actor) enum ActorWatcherMessage {
    Subscribe(SystemActorRef, SystemActorRef),
    Started(usize, SystemActorRef),
    Stopped(SystemActorRef),
    Failed(SystemActorRef),

    #[cfg(feature = "posix-signals-support")]
    ReceivedPosixSignal(i32),
    #[cfg(feature = "posix-signals-support")]
    SubscribePosixSignals(SystemActorRef),

    DrainSystem(ThunkWithSync),
    StopSystem(ThunkWithSync),
}

/// Tracks the state of all actors in the system.
///
/// This is a system actor that is tightly coupled to the actor system
/// as it is sent messages when actors are started, stopped, or failed.
///
/// If POSIX signal support is enabled, this is also responsible for
/// forwarding signals to interested actors.
///
/// The `ActorWatcher` is also responsible for stopping all actors in
/// the system, via draining or stopping.
pub(in crate::actor) struct ActorWatcher {
    watchers: HashMap<usize, Vec<SystemActorRef>>,
    system_refs: HashSet<usize>,
    root_system_refs: HashMap<usize, SystemActorRef>,
    when_stopped: Option<ThunkWithSync>,

    #[cfg(feature = "posix-signals-support")]
    posix_signals_watchers: HashMap<usize, SystemActorRef>,
}

impl ActorWatcher {
    pub(in crate::actor) fn new() -> Self {
        Self {
            watchers: HashMap::new(),
            system_refs: HashSet::new(),
            root_system_refs: HashMap::new(),
            when_stopped: None,
            #[cfg(feature = "posix-signals-support")]
            posix_signals_watchers: HashMap::new(),
        }
    }

    fn check_stopped(&mut self) {
        if let Some(thunk) = self.when_stopped.take() {
            thunk.apply();
        }
    }
}

impl Actor<ActorWatcherMessage> for ActorWatcher {
    fn receive(
        &mut self,
        message: ActorWatcherMessage,
        _context: &mut ActorContext<ActorWatcherMessage>,
    ) {
        match message {
            ActorWatcherMessage::Subscribe(watcher, watching) => {
                if self.system_refs.contains(&watching.id) {
                    let entry = self
                        .watchers
                        .entry(watching.id)
                        .or_insert_with(|| Vec::with_capacity(1));

                    entry.push(watcher);
                } else {
                    watcher.tell_system(SystemMsg::Signaled(Signal::ActorStopped(
                        watching,
                        StopReason::AlreadyStopped,
                    )));
                }
            }

            ActorWatcherMessage::Started(id, system_ref) => {
                self.system_refs.insert(id);

                system_ref.tell_system(SystemMsg::Signaled(Signal::Started));

                if system_ref.actor_type() == ActorType::Root {
                    self.root_system_refs.insert(id, system_ref);
                }
            }

            ActorWatcherMessage::Stopped(system_ref) => {
                // Note that under normal execution is is very possible to
                // get this more than once for a given system_ref, but
                // we ensure that watchers will never be notified more
                // than once
                //
                // The status AtomicUsize is used to track the final status
                // and be able to distinguish between stop/failure for
                // new subscription attempts.
                //
                // These fields will be dropped when all ActorRef references
                // are dropped.

                self.system_refs.remove(&system_ref.id);
                self.root_system_refs.remove(&system_ref.id);
                self.system_refs.shrink_to_fit();
                self.root_system_refs.shrink_to_fit();

                if let Some(watchers) = self.watchers.remove(&system_ref.id) {
                    self.watchers.shrink_to_fit();

                    for watcher in watchers {
                        watcher.tell_system(SystemMsg::Signaled(Signal::ActorStopped(
                            system_ref.clone(),
                            StopReason::Stopped,
                        )));
                    }
                }

                #[cfg(feature = "posix-signals-support")]
                self.posix_signals_watchers.remove(&system_ref.id);

                self.check_stopped();
            }

            ActorWatcherMessage::Failed(system_ref) => {
                // Note that the same message above applies to this case as well

                self.system_refs.remove(&system_ref.id);
                self.root_system_refs.remove(&system_ref.id);
                self.system_refs.shrink_to_fit();
                self.root_system_refs.shrink_to_fit();

                if let Some(watchers) = self.watchers.remove(&system_ref.id) {
                    self.watchers.shrink_to_fit();
                    for watcher in watchers {
                        watcher.tell_system(SystemMsg::Signaled(Signal::ActorStopped(
                            system_ref.clone(),
                            StopReason::Failed,
                        )));
                    }
                }

                #[cfg(feature = "posix-signals-support")]
                self.posix_signals_watchers.remove(&system_ref.id);

                self.check_stopped();
            }

            ActorWatcherMessage::DrainSystem(done) => {
                if self.when_stopped.is_none() {
                    for actor in self.root_system_refs.values() {
                        actor.drain();
                    }

                    self.when_stopped = Some(done);
                }
            }

            ActorWatcherMessage::StopSystem(done) => {
                if self.when_stopped.is_none() {
                    for actor in self.root_system_refs.values() {
                        actor.stop();
                    }

                    self.when_stopped = Some(done);
                }
            }

            #[cfg(feature = "posix-signals-support")]
            ActorWatcherMessage::ReceivedPosixSignal(signal) => {
                for watcher in self.posix_signals_watchers.values() {
                    watcher.tell_system(SystemMsg::Signaled(Signal::PosixSignal(signal)));
                }
            }

            #[cfg(feature = "posix-signals-support")]
            ActorWatcherMessage::SubscribePosixSignals(system_ref) => {
                self.posix_signals_watchers
                    .insert(system_ref.id, system_ref);
            }
        }
    }
}
