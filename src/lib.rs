//! # Pantomime
//!
//! Pantomime is an [Actor model](https://en.wikipedia.org/wiki/Actor_model) library
//! for [Rust](https://www.rust-lang.org/). The Actor model defines a powerful computation
//! abstraction that elevates the realities of distributed systems to empower you to build
//! highly concurrent and performant systems.

extern crate atty;
extern crate chrono;
extern crate conqueue;
extern crate crossbeam;
extern crate fern;
extern crate mio;
extern crate parking_lot;
extern crate rand;

#[macro_use]
extern crate downcast_rs;

#[macro_use]
extern crate log;

#[cfg(feature = "futures-support")]
extern crate futures;

#[cfg(all(feature = "posix-signals-support", target_family = "unix"))]
extern crate signal_hook_shim as signal_hook;

#[cfg(feature = "tokio-support")]
extern crate tokio as ext_tokio;

#[cfg(feature = "tokio-support")]
extern crate tokio_executor;

#[cfg(feature = "tokio-support")]
extern crate tokio_reactor;

#[cfg(feature = "tokio-support")]
extern crate tokio_threadpool;

#[cfg(feature = "tokio-support")]
extern crate tokio_timer;

pub mod actor;
pub mod cfg;
pub mod dispatcher;
pub mod io;
pub mod mailbox;
pub mod prelude;
pub mod stream;
pub mod timer;
pub mod util;

#[cfg(feature = "posix-signals-support")]
pub mod posix_signals;

#[cfg(any(feature = "testkit", test))]
pub mod testkit;
