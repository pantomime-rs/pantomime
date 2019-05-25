//! Common types necessary for most applications

pub use log::{debug, error, info, trace, warn};

pub use crate::actor::{
    Actor, ActorContext, ActorRef, ActorSystem, FailureError, FailureReason, Signal, SystemActorRef,
};
pub use crate::cfg::Config;
pub use crate::dispatcher::*;
