//! Core messaging

mod actor_ref;
mod actor_watcher;
mod cell;
mod probe;
mod shard;
mod supervisor;
mod system;

#[cfg(test)]
mod tests;

use self::actor_watcher::ActorWatcherMessage;
use crate::cfg::*;
use crate::dispatcher::Dispatcher;
use crate::mailbox::*;
use crate::timer::*;
use downcast_rs::Downcast;
use std::sync::{Arc, Weak};

pub use self::actor_ref::{ActorContext, ActorRef, SystemActorRef};
pub use self::probe::{Probe, SpawnProbe};
pub use self::supervisor::Supervisor;
pub use self::system::{ActiveActorSystem, ActorSystem, ActorSystemContext};

pub(self) use self::actor_ref::*;
pub(self) use self::cell::*;
pub(self) use self::shard::*;

#[derive(Clone, Copy, PartialEq)]
enum ActorType {
    Child,
    Root,
    System,
}

/// Actors can receive signals which are messages that concern the lifecycle
/// of itself or watched actors.
pub enum Signal {
    Started,
    Stopped,
    Failed,
    ActorStopped(SystemActorRef, StopReason),

    #[cfg(feature = "posix-signals-support")]
    PosixSignal(i32),
}

/// Describes the reason that an actor has stopped.
pub enum StopReason {
    /// Signifies that the actor was stopped normally.
    Stopped,

    /// Signifies that the actor was stopped due to a failure.
    Failed,

    /// Signifies that the actor was already stopped but the cause is unknown.
    AlreadyStopped,
}

/// An actor receives messages of a particular type and can modify
/// its own state, spawn new actors, and send messages to other
/// actors.
pub trait Actor<M: 'static + Send> {
    fn config_dispatcher(
        &self,
        _context: &ActorSystemContext,
    ) -> Option<Box<'static + Dispatcher + Send + Sync>> {
        None
    }

    fn config_mailbox(
        &self,
        _context: &ActorSystemContext,
    ) -> Option<Box<Mailbox<M> + Send + Sync>> {
        None
    }

    fn receive(&mut self, message: M, context: &mut ActorContext<M>);

    fn receive_signal(&mut self, signal: Signal, context: &mut ActorContext<M>) {
        let _ = signal;

        let _ = context;
    }
}

enum SystemMsg {
    Drain,
    Stop { failure: bool },
    ChildStopped(usize),
    Signaled(Signal),
}

trait ActorWithMessage {
    fn apply(self: Box<Self>);
}

trait ActorWithSystemMessage {
    fn apply(self: Box<Self>);
}

enum ActorShardEvent {
    Messaged,
    Scheduled,
}
